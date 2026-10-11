import copy
import json
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import CommandError, ManagementUtility, call_command
from django.db import connection, connections, transaction
from django.db.models import F
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import _waiting, actions
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.models import (
    OxpullBatch,
    OxpullBatchMember,
    OxpullUniqueLock,
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
    OxpullWorkflowScan,
)
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_workflows import drain, flow_worker, reconcile
from .test_workflows_reconciler import (
    EDGE_TABLE,
    LEVELS,
    NODE_TABLE,
    WORKFLOW_TABLE,
    events_named,
    every_connection_at,
    every_statement,
    flagged_step,
    lose_a_deadlock_once,
    ordered_lock_of,
    record_of,
    serialization_failures_at,
)

pytestmark = pytest.mark.django_db


def _prune(*args):
    out = StringIO()
    call_command("oxpull_prune", *args, stdout=out)
    return out.getvalue()


def test_prune_deletes_stale_locks_only(worker):
    finished = tasks.unique_echo.enqueue("finished")
    assert worker.run_once() is True
    assert OxTask.objects.get(id=finished.id).status == OxTask.Status.SUCCESSFUL

    pruned_away = tasks.unique_echo.enqueue("row-pruned")
    OxTask.objects.filter(id=pruned_away.id).delete()

    live = tasks.unique_echo.enqueue("live")

    assert OxpullUniqueLock.objects.count() == 3
    # batch size 1 drives the bounded-delete loop through several
    # passes; the result must not change.
    output = _prune("--batch-size", "1")
    assert "Deleted 2 stale unique lock(s)." in output
    lock = OxpullUniqueLock.objects.get()
    assert str(lock.task_id) == live.id


def test_prune_dry_run_deletes_nothing(worker):
    tasks.unique_echo.enqueue("finished")
    assert worker.run_once() is True

    output = _prune("--dry-run")
    assert "Would delete 1 stale unique lock(s)." in output
    assert OxpullUniqueLock.objects.count() == 1


def test_prune_rejects_non_positive_batch_size():
    with pytest.raises(CommandError):
        call_command("oxpull_prune", "--batch-size", "0")


# -- workflow and batch rows ------------------------------------------------
#
# Node and edge rows outlive the task rows they name, and the edge table is
# read inside every claim on a gating backend, so a long-lived install would
# grow it without bound if nothing reaped it. oxpull_prune deletes a node and
# the edges into it only once no retry can move the node again, and it never
# deletes a batch member.
#
# Shortcuts, each named where it is taken: completed_at moved back stands in
# for waiting past --older-than, and ox_prune's finished_at moved back for
# waiting past its own cutoff.

LONG_AGO = timedelta(days=30)


def _complete(row, ago):
    """Mark a batch COMPLETE as of `ago` before now (shortcut)."""
    type(row).objects.filter(pk=row.pk).update(
        status=type(row).Status.COMPLETE, completed_at=timezone.now() - ago
    )


def completed_long_ago(flow, ago=LONG_AGO):
    """Shortcut: move a COMPLETE workflow's completed_at back."""
    row = OxpullWorkflow.objects.get(pk=flow.id)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    OxpullWorkflow.objects.filter(pk=flow.id).update(
        completed_at=row.completed_at - ago
    )


def run_to_completion(flow):
    """A worker drains it and the reconciler ticks until it completes."""
    worker = flow_worker()
    for _ in range(4):
        drain(worker)
        reconcile()
        if OxpullWorkflow.objects.get(pk=flow.id).status == "COMPLETE":
            return
    raise AssertionError("the workflow did not complete")


def rows_of(flow):
    return (
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count(),
        OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count(),
    )


def status_of(result):
    return OxTask.objects.values_list("status", flat=True).get(pk=result.id)


def two_nodes(name, second=tasks.step):
    flow = workflows.create(name)
    a = flow.add("a", tasks.step, f"{name}-a")
    b = flow.add("b", second, f"{name}-b", depends_on="a")
    flow.seal()
    return flow, a, b


def test_prune_reaps_the_nodes_and_edges_of_a_long_completed_workflow():
    flow, _, _ = two_nodes("old")
    run_to_completion(flow)
    completed_long_ago(flow)

    output = _prune()

    assert rows_of(flow) == (0, 0)
    assert "Deleted 2 workflow node row(s)." in output
    assert "Deleted 1 workflow edge row(s)." in output
    # The workflow row itself stays: one row per workflow is bounded by
    # how many an operator creates, and it is the record of what ran.
    row = OxpullWorkflow.objects.get(pk=flow.id)
    assert row.metadata_retired_at is not None
    assert row.completions["0"]["succeeded"] == 2


def test_prune_leaves_a_sealed_workflow_alone():
    """
    A SEALED workflow's edges are read by every claim on a gating
    backend. Reaping them would un-gate live nodes.
    """
    flow, _, _ = two_nodes("live")
    _prune("--older-than", "0")
    assert rows_of(flow) == (2, 1)
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is None


def test_prune_respects_older_than_for_workflows():
    flow, _, _ = two_nodes("recent")
    run_to_completion(flow)
    completed_long_ago(flow, timedelta(hours=1))

    _prune()  # default 7d
    assert rows_of(flow) == (2, 1)
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is None
    _prune("--older-than", "30m")
    assert rows_of(flow) == (0, 0)


def test_prune_keeps_batch_members():
    batch = batches.create("old-batch")
    batch.add(tasks.plain_echo, "one")
    batch.seal()
    assert OxpullBatchMember.objects.count() == 1
    _complete(OxpullBatch.objects.get(pk=batch.id), LONG_AGO)
    output = _prune("--older-than", "0")
    assert OxpullBatchMember.objects.count() == 1
    assert "batch" not in output.lower()
    assert OxpullBatch.objects.filter(pk=batch.id).exists()


def test_dry_run_counts_what_would_go_without_deleting_or_stamping():
    flow, _, _ = two_nodes("counted")
    run_to_completion(flow)
    completed_long_ago(flow)

    out = _prune("--dry-run")
    assert "Would delete 2 workflow node row(s)." in out
    assert "Would delete 1 workflow edge row(s)." in out
    assert rows_of(flow) == (2, 1)
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is None


@pytest.mark.parametrize("dead", ["failed", "lost"])
def test_a_failed_or_lost_node_stays_gated_until_ox_prune_removes_its_row(dead):
    """
    b settled without succeeding. Its node and the edge into it stay while its
    row is there, so an operator's retry of b is still gated. a succeeded, so
    its node goes, and the edge a -> b stays marked satisfied. Once ox_prune
    --include-failed deletes b's row, b's node and edge go too.
    """
    flow = workflows.create(f"dead-{dead}")
    flow.add("a", tasks.step, "a")
    if dead == "failed":
        b = flow.add("b", tasks.failing_step, "b", depends_on="a")
        flow.seal()
        run_to_completion(flow)
    else:
        b = flow.add("b", tasks.step, "b", depends_on="a")
        flow.seal()
        assert drain(flow_worker(), limit=1) == 1  # a
        reaper = flow_worker()
        for _ in range(3):
            claimed = flow_worker().claim_one()
            assert claimed is not None and str(claimed.id) == str(b.id)
            assert actions.expire_lease(b.id) is True
            assert reaper.reap() == 1
        run_to_completion(flow)
    assert status_of(b) == dead.upper()
    completed_long_ago(flow)

    output = _prune()

    assert rows_of(flow) == (1, 1), output
    assert list(
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values_list(
            "name", flat=True
        )
    ) == ["b"]
    assert OxpullWorkflowEdge.objects.get(workflow_id=flow.id).satisfied is True
    assert "Kept 1 workflow node row(s) and 1 workflow edge row(s)" in output

    OxTask.objects.filter(pk=b.id).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    call_command("ox_prune", "--include-failed", stdout=StringIO())
    assert not OxTask.objects.filter(pk=b.id).exists()
    _prune()
    assert rows_of(flow) == (0, 0)


def test_a_node_whose_row_went_before_anything_recorded_it_is_kept():
    """
    b's row is gone and nothing recorded how b finished. Its node is never
    deleted, and neither is the edge into it.
    """
    flow, _, b = two_nodes("unrecorded")
    drain(flow_worker())
    OxTask.objects.filter(pk=b.id).delete()  # shortcut: a prune by hand
    OxpullWorkflowNode.objects.filter(task_id=b.id).update(
        outcome=None, outcome_epoch=None, succeeded=False
    )
    run_to_completion(flow)
    completed_long_ago(flow)

    _prune()

    assert list(
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values_list(
            "name", flat=True
        )
    ) == ["b"]
    assert rows_of(flow) == (1, 1)


def test_a_workflow_with_a_node_retried_after_completion_keeps_every_row():
    """
    An operator retried b after the workflow completed, and b has not run
    again. b is pending, so nothing of the workflow is deleted, the stamp is
    set, and the next run tries again.
    """
    flow, _, b = two_nodes("retried", second=tasks.failing_step)
    run_to_completion(flow)
    assert actions.retry(b.id) is True
    completed_long_ago(flow)

    output = _prune()

    assert rows_of(flow) == (2, 1)
    assert "Skipped 1 workflow(s) with a node still pending." in output
    stamped = OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at
    assert stamped is not None

    drain(flow_worker())
    assert status_of(b) == OxTask.Status.FAILED
    _prune()
    assert rows_of(flow) == (1, 1)
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at == stamped


def test_prune_repairs_a_record_no_worker_wrote_before_it_deletes():
    """
    b succeeded under a writer that recorded nothing, and marked nothing. The
    prune records it first, and b's node goes with the edge into it.
    """
    flow, _, b = two_nodes("repair-first")
    run_to_completion(flow)
    OxpullWorkflowNode.objects.filter(task_id=b.id).update(
        outcome=None, outcome_epoch=None, succeeded=False
    )
    completed_long_ago(flow)
    _prune()
    assert rows_of(flow) == (0, 0)


def test_an_unmarked_edge_into_a_kept_node_is_marked_before_its_upstream_goes():
    """
    a succeeded, b failed, and the edge a -> b was never marked (shortcut: the
    mark cleared by hand, as a writer of another version leaves it). a's node is
    the record of a's success once ox_prune deletes a's row. The prune marks the
    edge in the transaction that deletes that node, so an operator's retry of b
    still runs after a's row is gone.
    """
    flow = workflows.create("mark-first")
    a = flow.add("a", tasks.step, "a")
    b = flow.add("b", flagged_step, "b", depends_on="a")
    flow.seal()
    tasks.STATE["fail:b"] = True
    run_to_completion(flow)
    assert status_of(b) == OxTask.Status.FAILED
    OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).update(satisfied=False)
    completed_long_ago(flow)

    _prune()
    assert OxpullWorkflowEdge.objects.get(workflow_id=flow.id).satisfied is True

    OxTask.objects.filter(pk=a.id).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    call_command("ox_prune", stdout=StringIO())
    assert not OxTask.objects.filter(pk=a.id).exists()
    tasks.STATE["fail:b"] = False
    assert actions.retry(b.id) is True
    assert drain(flow_worker()) == 1
    assert status_of(b) == OxTask.Status.SUCCESSFUL


def test_a_delete_marks_the_edges_out_of_a_node_that_succeeded():
    """
    The delete transaction's mark, called on its own: the edges out of a
    successful node that goes, into a node that stays, are marked in the
    transaction that deletes the node.
    """
    flow = workflows.create("mark-in-delete")
    flow.add("a", tasks.step, "a")
    b = flow.add("b", tasks.failing_step, "b", depends_on="a")
    flow.seal()
    run_to_completion(flow)
    OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).update(satisfied=False)
    alias = oxpull_db_alias()
    workflows._stamp_retired(flow.id, alias)
    chunk = list(
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id)
        .order_by("pk")
        .values_list("pk", "task_id", "outcome")
    )
    nodes, edges, marked = workflows._retire_chunk(flow.id, chunk, alias)
    assert (nodes, edges) == (1, 0)
    assert marked == {uuid.UUID(str(b.id))}
    assert OxpullWorkflowEdge.objects.get(workflow_id=flow.id).satisfied is True


def test_a_node_whose_record_is_gone_keeps_the_edges_into_it():
    """
    A node the delete cannot take keeps whatever gates it.

    The delete takes recorded nodes only. If it took the edges into an
    unrecorded node anyway, that node would be left with nothing gating it,
    which is the one thing the claim gate exists to prevent. Nothing in this
    release clears a record, so this plants one: b's record is cleared, its
    task row still reads SUCCESSFUL, and b's node and the edge into it both
    have to survive the chunk that deletes a.
    """
    flow = workflows.create("record-cleared")
    flow.add("a", tasks.step, "a")
    b = flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    run_to_completion(flow)
    alias = oxpull_db_alias()
    workflows._stamp_retired(flow.id, alias)
    OxpullWorkflowNode.objects.filter(workflow_id=flow.id, task_id=b.id).update(
        outcome=None, outcome_epoch=None
    )
    chunk = list(
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id)
        .order_by("pk")
        .values_list("pk", "task_id", "outcome")
    )
    nodes, edges, _ = workflows._retire_chunk(flow.id, chunk, alias)

    assert (nodes, edges) == (1, 0)
    kept = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values_list(
        "task_id", flat=True
    )
    assert [uuid.UUID(str(pk)) for pk in kept] == [uuid.UUID(str(b.id))]
    gating = OxpullWorkflowEdge.objects.filter(
        workflow_id=flow.id, downstream_task_id=b.id
    )
    assert gating.count() == 1, "b was left with nothing gating it"


@pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite serialises writers, so nothing lands inside the delete",
)
@pytest.mark.django_db(transaction=True)
def test_a_node_revived_before_the_delete_locks_its_row_is_kept():
    """
    b was cancelled, and its node is recorded DISCARDED. The delete reads the
    chunk's rows, and before it locks them b is revived to WAITING. The delete
    reads b's row again under its lock, finds it WAITING, and keeps its node
    and the edge into it.
    """
    flow = workflows.create("revived-inside")
    flow.add("a", tasks.failing_step, "a")
    b = flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    run_to_completion(flow)
    assert status_of(b) == OxTask.Status.DISCARDED
    epoch = OxTask.objects.get(pk=b.id).lease_epoch
    completed_long_ago(flow)
    fired = []
    task_table = OxTask._meta.db_table.upper()

    def revive_on_another_connection():
        try:
            fired.append(_waiting.revive_many([(b.id, epoch)], using=oxpull_db_alias()))
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        if (
            not fired
            and text.startswith("SELECT")
            and task_table in text
            and "FOR UPDATE" in text
        ):
            thread = threading.Thread(target=revive_on_another_connection)
            thread.start()
            thread.join(30)
        return execute(sql, params, many, context)

    with connection.execute_wrapper(pause):
        _prune()

    assert fired and list(fired[0].values()) == [_waiting.Revival.REVIVED]
    assert status_of(b) == OxTask.Status.WAITING
    names = set(
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values_list(
            "name", flat=True
        )
    )
    assert "b" in names, names
    assert OxpullWorkflowEdge.objects.filter(downstream_task_id=b.id).count() == 1


def test_prune_cleans_the_scan_rows_of_complete_workflows():
    """
    The backstop visits OPEN and SEALED workflows, and their scan rows hold its
    skip queue and pass position. Only a COMPLETE workflow's scan row goes.
    """
    done, _, _ = two_nodes("scan-done")
    run_to_completion(done)
    sealed, _, _ = two_nodes("scan-sealed")
    still_open = workflows.create("scan-open")
    still_open.add("x", tasks.step, "scan-open-x")
    OxpullWorkflowScan.objects.create(workflow_id=done.id, skipped=["x"])
    OxpullWorkflowScan.objects.create(workflow_id=sealed.id, skipped=["y"])
    OxpullWorkflowScan.objects.create(workflow_id=still_open.id, skipped=["z"])

    output = _prune()

    assert "Deleted 1 workflow scan row(s)." in output
    kept = OxpullWorkflowScan.objects.values_list("workflow_id", flat=True)
    assert {uuid.UUID(str(pk)) for pk in kept} == {
        uuid.UUID(str(sealed.id)),
        uuid.UUID(str(still_open.id)),
    }


def test_retained_rows_count_what_prune_keeps():
    flow, _, _ = two_nodes("retained", second=tasks.failing_step)
    run_to_completion(flow)
    completed_long_ago(flow)
    assert workflows.retained_rows() == workflows.RetainedRows(0, 0, 0)
    _prune()
    assert workflows.retained_rows() == workflows.RetainedRows(1, 1, 1)


@pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite serialises writers, so no stamp waits on another",
)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_stamp_that_waited_on_another_prune_runs_again(caplog, level):
    """
    Two oxpull_prune runs reach the same workflow. The other one has stamped it
    and not yet committed, so this run's stamp waits for the workflow row. At
    PostgreSQL REPEATABLE READ the row it finds after that commit changed after
    its snapshot, and the stamp raises a serialization failure. The stamp owns
    its transaction, so it runs it again, finds the workflow stamped, keeps that
    stamp, and the run deletes the rows as usual. At the other levels the stamp
    reads the row as committed and nothing runs again.
    """
    flow, _, _ = two_nodes(f"stamp-waits-{level}")
    run_to_completion(flow)
    completed_long_ago(flow)
    other_stamp = timezone.now() - timedelta(minutes=5)
    held = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def stamp_on_another_connection():
        try:
            with transaction.atomic():
                stamped = OxpullWorkflow.objects.filter(
                    pk=flow.id, metadata_retired_at__isnull=True
                ).update(metadata_retired_at=other_stamp)
                assert stamped == 1
                held.set()
                release.wait(60)
        except BaseException as exc:
            errors.append(exc)
            held.set()
        finally:
            connections.close_all()

    def commit_the_other_while_the_stamp_waits(execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        if (
            not release.is_set()
            and text.startswith("UPDATE")
            and WORKFLOW_TABLE in text
            and "METADATA_RETIRED_AT" in text
        ):
            threading.Timer(0.5, release.set).start()
        return execute(sql, params, many, context)

    with every_connection_at(level):
        other = threading.Thread(target=stamp_on_another_connection)
        other.start()
        assert held.wait(30) and not errors, errors
        try:
            with (
                caplog.at_level("INFO", logger="oxpull"),
                connection.execute_wrapper(commit_the_other_while_the_stamp_waits),
            ):
                output = _prune()
        finally:
            release.set()
            other.join(60)

    reissued = events_named(caplog, "workflow_reconcile_conflict_retry")
    facts = f"level={level} reissued={reissued} output={output!r}"
    assert not errors, errors
    if serialization_failures_at(level):
        assert len(reissued) == 1, facts
    else:
        assert reissued == [], facts
    assert rows_of(flow) == (0, 0), facts
    row = OxpullWorkflow.objects.get(pk=flow.id)
    assert row.metadata_retired_at == other_stamp, facts


# -- what a run costs once workflows are retired --------------------------------


def prune_statements(*args):
    """How many statements one oxpull_prune run sends, and what it prints."""
    with CaptureQueriesContext(connection) as captured:
        output = _prune(*args)
    return len(captured.captured_queries), output


def deletes_from(sql, table):
    """Whether a statement is a DELETE of `table`."""
    words = sql.split()
    return words[:2] == ["DELETE", "FROM"] and words[2] == connection.ops.quote_name(
        table
    )


def emptied_and_kept(label):
    """
    Two workflows past the cutoff. The first run empties one. It keeps the
    other's failed node and the edge into it.
    """
    done, _, _ = two_nodes(f"{label}-done")
    kept, _, failed = two_nodes(f"{label}-kept", second=tasks.failing_step)
    for flow in (done, kept):
        run_to_completion(flow)
        completed_long_ago(flow)
    return done, kept, failed


def test_a_run_costs_the_same_however_many_workflows_are_already_retired():
    """
    A retired workflow whose node rows are all gone gives a later run nothing to
    do, and neither does one that keeps only a recorded FAILED node. oxpull_prune
    runs on a schedule and never deletes a workflow row, so a run that visited
    every one of them would grow slower with every workflow that ever completed.
    A run over four of each sends what a run over one of each sends, and so does
    the dry run.
    """
    done, kept, _ = emptied_and_kept("first")
    _prune()
    assert (rows_of(done), rows_of(kept)) == ((0, 0), (1, 1))
    once, _ = prune_statements()
    dry_once, _ = prune_statements("--dry-run")

    for i in range(3):
        emptied_and_kept(f"more-{i}")
    _prune()
    assert workflows.retained_rows() == workflows.RetainedRows(8, 4, 4)
    again, output = prune_statements()
    dry_again, _ = prune_statements("--dry-run")

    counts = f"statements: run {once} then {again}, dry run {dry_once} then {dry_again}"
    assert (again, dry_again) == (once, dry_once), counts
    assert "Deleted 0 workflow node row(s)." in output
    assert "Kept 4 workflow node row(s) and 4 workflow edge row(s)" in output


WORKFLOW_TABLES = (OxpullWorkflow._meta.db_table, NODE_TABLE, EDGE_TABLE)
PLAN_TABLES = (OxTask._meta.db_table, *WORKFLOW_TABLES)


def _tables_by_alias(sql, quote):
    """Each workflow or task table in the SQL, and each alias of one, to the table."""
    found = {table: table for table in PLAN_TABLES}
    for table, alias in re.findall(
        rf"{quote}(\w+){quote}(?: AS)? ({quote}?[A-Za-z_]\w*{quote}?)", sql
    ):
        if table in PLAN_TABLES:
            found[alias.strip(quote)] = table
    return found


def _postgresql_rereads(plan, *, per_row=False):
    """
    Whole reads of a table under a SubPlan that is not hashed, or on the inner
    side of a nested loop. PostgreSQL runs those again for each outer row.
    """
    kind = plan.get("Node Type", "")
    whole = kind == "Seq Scan" or (
        kind in ("Index Scan", "Index Only Scan") and "Index Cond" not in plan
    )
    found = []
    if per_row and whole and plan.get("Relation Name") in PLAN_TABLES:
        found.append(f"{kind} on {plan['Relation Name']}")
    for child in plan.get("Plans", []):
        relation = child.get("Parent Relationship")
        again = (
            per_row
            or (
                relation == "SubPlan"
                and not child.get("Subplan Name", "").startswith("hashed")
            )
            or (relation == "Inner" and kind == "Nested Loop")
        )
        found.extend(_postgresql_rereads(child, per_row=again))
    return found


MYSQL_WHOLE_SCAN = r"-> (?:Table scan|Covering index scan|Index scan) on (\w+)"


def _mysql_rereads(text, sql):
    """
    Whole reads of a table inside a dependent subquery or a materialisation
    redone per row, or on a nested loop's inner side with no materialisation or
    hash between. MySQL runs those again for each outer row.
    """
    tables = _tables_by_alias(sql, "`")
    lines = text.splitlines()
    found = []
    for index, line in enumerate(lines):
        match = re.search(MYSQL_WHOLE_SCAN, line)
        if not match or tables.get(match.group(1)) not in PLAN_TABLES:
            continue
        depth = len(line) - len(line.lstrip())
        child = index
        buffered = False
        per_row = False
        for above in range(index - 1, -1, -1):
            text_above = lines[above]
            above_depth = len(text_above) - len(text_above.lstrip())
            if above_depth >= depth:
                continue
            if "dependent)" in text_above or "invalidate on row" in text_above:
                per_row = True
            if "Materialize" in text_above or "-> Hash" in text_above:
                buffered = True
            if "Nested loop" in text_above and not buffered and child != above + 1:
                per_row = True
            depth, child = above_depth, above
        if per_row:
            found.append(line.strip())
    return found


def _sqlite_rereads(rows, sql):
    """
    SCAN steps under a CORRELATED subquery, or after an earlier SCAN or SEARCH
    of the same parent, which makes them a join's inner loop. SQLite runs those
    again for each outer row.
    """
    tables = _tables_by_alias(sql, '"')
    parents = {row[0]: row[1] for row in rows}
    details = {row[0]: row[3] for row in rows}
    found = []
    for node, parent, _, detail in rows:
        match = re.match(r"SCAN (\w+)", detail)
        if not match or tables.get(match.group(1)) not in PLAN_TABLES:
            continue
        per_row = any(
            re.match(r"(SCAN|SEARCH) ", other[3])
            for other in rows
            if other[1] == parent and other[0] < node
        )
        up = parent
        while up in details:
            per_row = per_row or details[up].startswith("CORRELATED")
            up = parents[up]
        if per_row:
            found.append(detail)
    return found


def rereads(sql):
    """
    The steps of a SELECT's plan that read a whole workflow or task table again
    for each row of another table, and the plan as text.
    """
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("EXPLAIN (FORMAT JSON) " + sql)
            plan = cursor.fetchone()[0]
            if isinstance(plan, str):
                plan = json.loads(plan)
            return _postgresql_rereads(plan[0]["Plan"]), json.dumps(plan, indent=1)
        if connection.vendor == "mysql":
            cursor.execute("EXPLAIN FORMAT=TREE " + sql)
            text = cursor.fetchone()[0]
            return _mysql_rereads(text, sql), text
        cursor.execute("EXPLAIN QUERY PLAN " + sql)
        rows = cursor.fetchall()
        return _sqlite_rereads(rows, sql), "\n".join(str(row) for row in rows)


@contextmanager
def little_memory_for_the_planner():
    """
    On PostgreSQL, give this connection's planner the least memory it takes, so
    that a hash of thousands of rows does not fit, as a hash of a node table of
    millions of rows would not fit the default. Elsewhere, nothing.
    """
    if connection.vendor != "postgresql":
        yield
        return
    with connection.cursor() as cursor:
        cursor.execute("SET work_mem = '64kB'")
        cursor.execute("SET hash_mem_multiplier = 1")
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute("RESET work_mem")
            cursor.execute("RESET hash_mem_multiplier")


@pytest.mark.django_db(transaction=True)
def test_a_run_reads_no_table_again_for_each_retired_workflow():
    """
    Hundreds of retired workflows are already emptied, one keeps a failed node,
    and an open workflow has 20,000 waiting nodes. On PostgreSQL the planner has
    too little memory to hash the node table, as on a large database. Every
    SELECT of a workflow table that a run and a dry run send is explained with
    the database's EXPLAIN. No plan may read a whole workflow, node, edge or
    task table again for each row of another. A read of the node table for each
    retired workflow made a run's cost the number of retired workflows times the
    number of node rows.
    """
    _, kept, failed = emptied_and_kept("plans")
    _prune()
    assert rows_of(kept) == (1, 1)
    long_ago = timezone.now() - LONG_AGO
    OxpullWorkflow.objects.bulk_create(
        [
            OxpullWorkflow(
                name=f"emptied-{i}",
                status=OxpullWorkflow.Status.COMPLETE,
                created_at=long_ago,
                sealed_at=long_ago,
                completed_at=long_ago,
                metadata_retired_at=long_ago,
            )
            for i in range(300)
        ],
        batch_size=500,
    )
    open_flow = workflows.create("plans-open")
    template = OxTask.objects.get(pk=failed.id)
    rows = []
    for _ in range(20000):
        row = copy.copy(template)
        row.pk = uuid.uuid4()
        row.status = OxTask.Status.WAITING
        rows.append(row)
    OxTask.objects.bulk_create(rows, batch_size=1000)
    OxpullWorkflowNode.objects.bulk_create(
        [
            OxpullWorkflowNode(
                workflow_id=open_flow.id,
                name=f"n{i}",
                name_key=workflows.name_key(f"n{i}"),
                task_id=row.pk,
                created_at=timezone.now(),
            )
            for i, row in enumerate(rows)
        ],
        batch_size=1000,
    )
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            for table in PLAN_TABLES:
                cursor.execute(f"ANALYZE TABLE {connection.ops.quote_name(table)}")
                cursor.fetchall()
        else:
            cursor.execute("ANALYZE")

    bad = []
    shown = []
    with little_memory_for_the_planner():
        for args in ((), ("--dry-run",)):
            with CaptureQueriesContext(connection) as captured:
                output = _prune(*args)
            for query in captured.captured_queries:
                sql = query["sql"]
                if not sql.lstrip().upper().startswith("SELECT"):
                    continue
                if not any(table in sql for table in WORKFLOW_TABLES):
                    continue
                found, plan = rereads(sql)
                shown.append(f"{' '.join(sql.split())[:300]}\n{plan[:3000]}")
                bad.extend(f"{step} in {' '.join(sql.split())[:300]}" for step in found)
            assert "Kept 1 workflow node row(s) and 1 workflow edge row(s)" in output

    assert bad == [], "\n".join(bad) + "\n\n" + "\n\n".join(shown)


def test_a_run_passes_over_a_workflow_that_keeps_only_a_node_gone_unrecorded():
    """
    b's task row is gone and nothing recorded its outcome, so nothing ever can.
    Shortcut: b ran, then its row was deleted and its record cleared, as when a
    writer outside this version settles it and ox_prune deletes the row before
    a repair. The first run keeps b's node and the edge into it. After that the
    workflow is not one a run acts on.
    """
    flow, _, b = two_nodes("gone-unrecorded")
    drain(flow_worker())
    OxTask.objects.filter(pk=b.id).delete()
    OxpullWorkflowNode.objects.filter(task_id=b.id).update(
        outcome=None, outcome_epoch=None, succeeded=False
    )
    run_to_completion(flow)
    completed_long_ago(flow)

    _prune()

    assert rows_of(flow) == (1, 1)
    alias = oxpull_db_alias()
    assert workflows.retirable_workflows(timezone.now(), alias) == []


def test_a_workflow_another_run_stamps_mid_read_is_counted_once():
    """
    A run reads the workflows past the cutoff that are not yet stamped, then the
    stamped ones that still have nodes. Another run stamps the workflow between
    the two reads, so both find it. The dry run still counts its two nodes and
    one edge once.
    """
    flow, _, _ = two_nodes("stamped-mid-read")
    run_to_completion(flow)
    completed_long_ago(flow)
    stamped: list[str] = []

    def stamp_before_the_stamped_read(execute, sql, params, many, context):
        if not stamped and "IS NOT NULL" in sql and "EXISTS" in sql:
            stamped.append(sql)
            with connections["default"].cursor() as cursor:
                cursor.execute(
                    f"UPDATE {connection.ops.quote_name(OxpullWorkflow._meta.db_table)}"  # noqa: S608
                    " SET metadata_retired_at = completed_at WHERE id = %s",
                    [OxpullWorkflow._meta.pk.get_db_prep_value(flow.id, connection)],
                )
        return execute(sql, params, many, context)

    with connection.execute_wrapper(stamp_before_the_stamped_read):
        output = _prune("--dry-run")

    assert stamped, output
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is not None
    assert "Would delete 2 workflow node row(s)." in output, output
    assert "Would delete 1 workflow edge row(s)." in output, output


def test_a_retained_workflow_counts_again_once_a_retry_moves_its_node():
    """
    The first run keeps b's failed node. An operator retries b: the next run
    finds it pending and deletes nothing. b then succeeds, and the run after
    that deletes b's node and the edge into it.
    """
    flow = workflows.create("retried-after-retirement")
    flow.add("a", tasks.step, "a")
    b = flow.add("b", flagged_step, "b", depends_on="a")
    flow.seal()
    tasks.STATE["fail:b"] = True
    run_to_completion(flow)
    completed_long_ago(flow)
    _prune()
    assert rows_of(flow) == (1, 1)

    tasks.STATE["fail:b"] = False
    assert actions.retry(b.id) is True
    output = _prune()
    assert "Skipped 1 workflow(s) with a node still pending." in output
    assert rows_of(flow) == (1, 1)

    assert drain(flow_worker()) == 1
    assert status_of(b) == OxTask.Status.SUCCESSFUL
    _prune()
    assert rows_of(flow) == (0, 0)


def test_a_retained_node_whose_record_fell_behind_its_row_is_repaired():
    """
    The first run keeps b's failed node, recorded FAILED. An operator retries b,
    which grants one more attempt, and its worker is lost, so the reaper writes
    LOST at a later epoch. The reaper records nothing on the node. The next run
    repairs the record to LOST, as completion would, and keeps the node.
    """
    _, flow, b = emptied_and_kept("record-behind")
    _prune()
    assert record_of(b.id)[0] == OxTask.Status.FAILED
    assert actions.retry(b.id) is True
    claimed = flow_worker().claim_one()
    assert claimed is not None and str(claimed.id) == str(b.id)
    assert actions.expire_lease(b.id) is True
    assert flow_worker().reap() == 1
    row = OxTask.objects.values_list("status", "lease_epoch").get(pk=b.id)
    assert row[0] == OxTask.Status.LOST
    assert record_of(b.id) != row

    _prune()
    assert record_of(b.id) == row
    assert rows_of(flow) == (1, 1)


def test_a_completed_workflow_with_no_nodes_is_stamped():
    """A workflow sealed empty completes, and a run past its cutoff stamps it."""
    flow = workflows.create("empty")
    flow.seal()
    reconcile()
    assert OxpullWorkflow.objects.get(pk=flow.id).status == "COMPLETE"
    completed_long_ago(flow)
    _prune()
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is not None


def test_batch_size_bounds_the_rows_each_workflow_delete_names():
    """
    d depends on a, b and c. With --batch-size 2 the nodes go two at a time, and
    the three edges into d go in DELETEs of at most two rows. So do the scan
    rows of three completed workflows.
    """
    flow = workflows.create("batch-size")
    for name in ("a", "b", "c"):
        flow.add(name, tasks.step, name)
    flow.add("d", tasks.step, "d", depends_on=["a", "b", "c"])
    flow.seal()
    run_to_completion(flow)
    completed_long_ago(flow)
    done = [flow]
    for i in range(2):
        empty = workflows.create(f"batch-size-empty-{i}")
        empty.seal()
        done.append(empty)
    reconcile()
    for row in done:
        OxpullWorkflowScan.objects.create(workflow_id=row.id, skipped=["x"])
    scan_table = OxpullWorkflowScan._meta.db_table
    deletes: dict[str, list[int]] = {NODE_TABLE: [], EDGE_TABLE: [], scan_table: []}

    def count_ids(execute, sql, params, many, context):
        for table, sizes in deletes.items():
            if deletes_from(sql, table):
                sizes.append(len(params or ()))
        return execute(sql, params, many, context)

    with connection.execute_wrapper(count_ids):
        output = _prune("--batch-size", "2")

    assert rows_of(flow) == (0, 0), output
    assert "Deleted 3 workflow scan row(s)." in output, output
    assert sum(deletes[NODE_TABLE]) == 4 and sum(deletes[EDGE_TABLE]) == 3, deletes
    assert sum(deletes[scan_table]) == 3, deletes
    assert max(size for sizes in deletes.values() for size in sizes) <= 2, deletes


@pytest.mark.skipif(connection.vendor == "sqlite", reason="SQLite has no locking reads")
@pytest.mark.django_db(transaction=True)
def test_the_delete_locks_task_node_and_edge_rows_in_key_order():
    """
    Every locking read of task, node or edge rows a prune run sends is ordered
    by key, as every multi-row writer of those tables locks, and every DELETE of
    several node or edge rows by key follows such a read of its table. a, b, c
    and e go with the two edges into e. d failed and stays with its three.
    """
    flow = workflows.create("prune-lock-order")
    for name in ("a", "b", "c"):
        flow.add(name, tasks.step, name)
    flow.add("e", tasks.step, "e", depends_on=["a", "b"])
    flow.add("d", tasks.failing_step, "d", depends_on=["a", "b", "c"])
    flow.seal()
    run_to_completion(flow)
    completed_long_ago(flow)
    quoted = connection.ops.quote_name
    tables = (OxTask._meta.db_table, NODE_TABLE, EDGE_TABLE)

    with every_statement() as log:
        _prune()

    assert rows_of(flow) == (1, 3), log
    locks = [
        (table, sql)
        for sql in log
        for table in tables
        if sql.startswith("SELECT")
        and f"FROM {quoted(table)}" in sql
        and sql.endswith("FOR UPDATE")
    ]
    assert {table for table, _ in locks} == set(tables), log
    assert all(ordered_lock_of(sql, table) for table, sql in locks), locks
    for at, sql in enumerate(log):
        for table in (NODE_TABLE, EDGE_TABLE):
            if deletes_from(sql, table):
                assert any(ordered_lock_of(s, table) for s in log[:at]), sql


# -- conflicts ------------------------------------------------------------------


@pytest.mark.skipif(connection.vendor == "sqlite", reason="SQLite raises no deadlock")
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "attempt", ["_stamp_retired_once", "_retire_chunk", "_clean_scan_chunk"]
)
def test_each_prune_transaction_runs_again_after_a_deadlock(
    monkeypatch, caplog, attempt
):
    """
    The stamp, a delete and the scan row cleanup each own their transaction in
    oxpull_prune. One that loses a deadlock runs again, and the run finishes as
    if nothing had happened.
    """
    flow, _, _ = two_nodes(f"deadlock-{attempt}")
    run_to_completion(flow)
    completed_long_ago(flow)
    OxpullWorkflowScan.objects.create(workflow_id=flow.id, skipped=["x"])
    calls = lose_a_deadlock_once(monkeypatch, attempt)

    with caplog.at_level("INFO", logger="oxpull"):
        output = _prune()

    reissued = [
        record.conflict
        for record in caplog.records
        if getattr(record, "event", "") == "workflow_reconcile_conflict_retry"
    ]
    assert len(calls) == 2, output
    assert reissued == ["deadlock"], output
    assert rows_of(flow) == (0, 0), output
    assert not OxpullWorkflowScan.objects.filter(workflow_id=flow.id).exists()
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is not None


@pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite has no row locks. The next test covers its write lock",
)
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_scan_row_cleanup_that_waited_on_the_skip_queue_runs_again(caplog, level):
    """
    A COMPLETE workflow still has a scan row, and the backstop's skip queue is
    taking ids out of it on another connection by compare-and-set. The cleanup's
    DELETE waits for that row. At PostgreSQL REPEATABLE READ the row it finds
    after the commit changed after its snapshot, and the DELETE raises a
    serialization failure. The cleanup runs in autocommit, so it owns what the
    database rolled back. It reads the statuses again, runs again and deletes
    the row. At the other levels nothing runs again.
    """
    flow, _, _ = two_nodes(f"scan-waits-{level}")
    run_to_completion(flow)
    OxpullWorkflowScan.objects.create(workflow_id=flow.id, skipped=["x"])
    scan_table = OxpullWorkflowScan._meta.db_table.upper()
    held = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def take_the_queue_on_another_connection():
        try:
            with transaction.atomic():
                taken = OxpullWorkflowScan.objects.filter(workflow_id=flow.id).update(
                    skipped=[], version=F("version") + 1
                )
                assert taken == 1
                held.set()
                release.wait(60)
        except BaseException as exc:
            errors.append(exc)
            held.set()
        finally:
            connections.close_all()

    def commit_the_queue_while_the_delete_waits(execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        if not release.is_set() and text.startswith("DELETE") and scan_table in text:
            threading.Timer(0.5, release.set).start()
        return execute(sql, params, many, context)

    output = ""
    with every_connection_at(level):
        other = threading.Thread(target=take_the_queue_on_another_connection)
        other.start()
        assert held.wait(30) and not errors, errors
        try:
            with (
                caplog.at_level("INFO", logger="oxpull"),
                connection.execute_wrapper(commit_the_queue_while_the_delete_waits),
            ):
                output = _prune()
        finally:
            release.set()
            other.join(60)

    reissued = events_named(caplog, "workflow_reconcile_conflict_retry")
    facts = f"level={level} reissued={reissued} output={output!r}"
    assert not errors, errors
    if serialization_failures_at(level):
        assert len(reissued) == 1, facts
    else:
        assert reissued == [], facts
    assert "Deleted 1 workflow scan row(s)." in output, facts
    assert not OxpullWorkflowScan.objects.filter(workflow_id=flow.id).exists()


@pytest.mark.skipif(connection.vendor != "sqlite", reason="SQLite's write lock")
@pytest.mark.django_db(transaction=True)
def test_a_scan_row_cleanup_beside_another_write_finishes_on_sqlite():
    """
    A COMPLETE workflow still has a scan row. As the cleanup sends its DELETE,
    another connection has just started an ordinary write, as a worker's claim
    or outcome does. SQLite lets one connection write at a time. A cleanup that
    had read the statuses inside its own transaction could not then take the
    write lock, and SQLite refused it at once with "database is locked", after
    the run's node and edge deletes had committed. The cleanup reads and deletes
    in autocommit, so its DELETE waits for the other write and the run finishes.
    """
    flow, a, _ = two_nodes("scan-beside-a-write")
    run_to_completion(flow)
    OxpullWorkflowScan.objects.create(workflow_id=flow.id, skipped=["x"])
    scan_table = OxpullWorkflowScan._meta.db_table.upper()
    writers: list[threading.Thread] = []
    other: dict[str, object] = {}

    def write_on_another_connection():
        try:
            other["updated"] = OxTask.objects.filter(pk=a.id).update(priority=1)
        except BaseException as exc:
            other["error"] = exc
        finally:
            connections.close_all()

    def start_a_write_before_the_delete(execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        if not writers and text.startswith("DELETE") and scan_table in text:
            writer = threading.Thread(target=write_on_another_connection)
            writers.append(writer)
            writer.start()
            time.sleep(0.5)
        return execute(sql, params, many, context)

    try:
        with connection.execute_wrapper(start_a_write_before_the_delete):
            output = _prune()
    finally:
        for writer in writers:
            writer.join(60)

    facts = f"writers={len(writers)} other={other}"
    assert writers and other == {"updated": 1}, facts
    assert "Deleted 1 workflow scan row(s)." in output, facts
    assert "Kept 0 workflow node row(s)" in output, facts
    assert not OxpullWorkflowScan.objects.filter(workflow_id=flow.id).exists()


# -- out-of-range --older-than ----------------------------------------------

OUT_OF_RANGE = ["3000000d", "1000000000d", "99999999999999d", "9" * 5000 + "d"]


def prunable_rows(worker):
    """A stale unique lock and a long-completed workflow: rows a prune deletes."""
    flow, _, _ = two_nodes("out-of-range")
    run_to_completion(flow)
    completed_long_ago(flow)
    tasks.unique_echo.enqueue("finished")
    assert worker.run_once() is True
    assert OxpullUniqueLock.objects.count() == 1
    return flow


def assert_nothing_pruned(flow):
    assert OxpullUniqueLock.objects.count() == 1
    assert rows_of(flow) == (2, 1)
    assert OxpullWorkflow.objects.get(pk=flow.id).metadata_retired_at is None


@pytest.mark.django_db(transaction=True)
class TestOutOfRangeDuration:
    """A retention too large to convert or to subtract is an argument error."""

    @pytest.mark.parametrize("dry_run", [False, True])
    @pytest.mark.parametrize("value", OUT_OF_RANGE)
    def test_is_a_command_error_and_deletes_nothing(self, worker, value, dry_run):
        flow = prunable_rows(worker)
        args = [f"--older-than={value}", *(["--dry-run"] if dry_run else [])]

        with (
            CaptureQueriesContext(connection) as queries,
            pytest.raises(CommandError) as info,
        ):
            _prune(*args)

        assert queries.captured_queries == []
        assert str(info.value) == f"Invalid duration {value!r}; it is out of range."
        assert_nothing_pruned(flow)

    def test_exits_1_without_a_traceback_from_the_command_line(self, worker, capsys):
        flow = prunable_rows(worker)
        argv = ["manage.py", "oxpull_prune", "--older-than=3000000d", "--skip-checks"]

        with pytest.raises(SystemExit) as info:
            ManagementUtility(argv).execute()

        assert info.value.code == 1
        err = capsys.readouterr().err
        assert "Invalid duration '3000000d'; it is out of range." in err
        assert "Traceback" not in err
        assert_nothing_pruned(flow)

    def test_traceback_flag_still_raises(self):
        argv = [
            "manage.py",
            "oxpull_prune",
            "--older-than=3000000d",
            "--skip-checks",
            "--traceback",
        ]
        with pytest.raises(CommandError) as info:
            ManagementUtility(argv).execute()
        assert str(info.value) == "Invalid duration '3000000d'; it is out of range."
