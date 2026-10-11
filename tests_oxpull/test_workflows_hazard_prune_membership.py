"""
What oxpull_prune may delete: never batch membership, and a workflow's
structure only once nothing can move it again.

oxpull 1.2.0's oxpull_prune deleted stale unique locks and nothing else.
The workflows branch first ran the same command line and also deleted the
OxpullBatchMember rows of a batch, plus every OxpullWorkflowNode and
OxpullWorkflowEdge row of a workflow, once the parent had been COMPLETE for
longer than --older-than. Now it never deletes a batch member, and deletes
a workflow's node, with the edges into it, only once no retry can move it.

What a user is owed:

(i)  Batch membership is kept, whatever the cutoff. Batch.progress() of a
     complete batch reads its member task rows as it did on 1.2.0, with
     1.2.0's arithmetic: a member whose task row ox_prune deleted counts
     as unattributed, not as succeeded.
(ii) A workflow's nodes and edges are deleted only when the workflow is
     COMPLETE and completed before the cutoff. An edge is deleted only
     when its downstream task row is SUCCESSFUL, DISCARDED or gone, the
     states no retry can move. An edge into a FAILED node stays, because
     an operator can still retry that node.

Everything runs through public paths: a real worker's claim and execution,
the real reconcilers, and the real ox_prune and oxpull_prune commands. The
one shortcut is the clock. Instead of waiting out a cutoff, move_back()
moves timestamps a hundred years into the past: a parent's created_at,
sealed_at and completed_at, its membership rows' created_at, or a task
row's finished_at. It changes no status and adds or deletes no row. Each
prune runs with the default command line and with an explicit
--older-than. The only thing assumed about the default cutoff is that it
is longer than the few seconds a test takes.
"""

import threading
import uuid
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection, connections

from django_ox import actions
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.models import (
    OxpullBatch,
    OxpullBatchMember,
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
)

from . import tasks
from .test_batches import reconcile as reconcile_batches
from .test_batches import stored as stored_batch
from .test_workflows import drain, flow_worker, order
from .test_workflows import reconcile as reconcile_workflows
from .test_workflows import stored as stored_flow

pytestmark = pytest.mark.django_db

LONG_AGO = timedelta(days=36525)  # a hundred years

# Between a workflow that completed a moment ago and one moved back a century.
WORKFLOW_CUTOFF = "30d"


def show(*parts):
    """Print evidence: these tests exist to be read when red."""
    print(*parts)  # noqa: T201


def oxpull_prune(*args):
    out = StringIO()
    call_command("oxpull_prune", *args, stdout=out)
    return out.getvalue()


def ox_prune(*args):
    out = StringIO()
    call_command("ox_prune", *args, stdout=out)
    return out.getvalue()


def move_back(queryset, *fields, by=LONG_AGO):
    """The clock shortcut: move these timestamps back, and nothing else."""
    for row in list(queryset.values("pk", *fields)):
        moved = {field: row[field] - by for field in fields if row[field] is not None}
        if moved:
            queryset.model.objects.filter(pk=row["pk"]).update(**moved)


def move_batch_back(batch):
    parent = OxpullBatch.objects.filter(pk=batch.id)
    assert parent.get().completed_at is not None, "only a batch that completed"
    move_back(parent, "created_at", "sealed_at", "completed_at")
    move_back(OxpullBatchMember.objects.filter(batch_id=batch.id), "created_at")


def move_workflow_back(flow):
    move_back(
        OxpullWorkflow.objects.filter(pk=flow.id),
        "created_at",
        "sealed_at",
        "completed_at",
    )
    move_back(OxpullWorkflowNode.objects.filter(workflow_id=flow.id), "created_at")
    move_back(OxpullWorkflowEdge.objects.filter(workflow_id=flow.id), "created_at")


def member_rows(batch):
    return OxpullBatchMember.objects.filter(batch_id=batch.id).count()


def node_rows(flow):
    return OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count()


def edge_rows(flow):
    return OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count()


def status_of(task_id):
    return OxTask.objects.values_list("status", flat=True).get(id=task_id)


# -- (i) Batch.progress() after oxpull_prune ---------------------------


def _completed_batch(worker):
    """Two members that succeed and one that fails, drained and reconciled."""
    batch = batches.create("nightly-invoices")
    batch.add(tasks.plain_echo, 1)
    batch.add(tasks.plain_echo, 2)
    batch.add(tasks.plain_fail)
    batch.seal()
    ran = 0
    while worker.run_once():
        ran += 1
    assert ran == 5  # two successes, three attempts of the failure
    assert reconcile_batches() == 1
    row = stored_batch(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.failed_count == 1
    assert member_rows(batch) == 3
    return batch


def test_oxpull_prune_leaves_an_old_batchs_progress_as_1_2_0_did(worker):
    """
    1.2.0's oxpull_prune never touched membership, so progress() of a
    COMPLETE batch read the member task rows for as long as they existed.
    That has to hold however long ago the batch completed and whatever
    cutoff the command is given.
    """
    batch = _completed_batch(worker)
    before = batch.progress()
    assert (before.total, before.pending, before.succeeded, before.failed) == (
        3,
        0,
        2,
        1,
    )

    move_batch_back(batch)
    show(oxpull_prune())  # the operator's cron line, unchanged since 1.2.0
    show(oxpull_prune("--older-than", "0"))  # and a cutoff every batch is past

    after = batch.progress()
    show(f"before={before} after={after} member rows={member_rows(batch)}")
    assert after == before
    assert member_rows(batch) == 3


def test_a_failed_member_stays_attributed_under_both_default_prunes(worker):
    """
    django-ox's own ox_prune deletes SUCCESSFUL rows past its cutoff. It
    does not delete FAILED rows unless --include-failed is passed: they
    hold the tracebacks. So an install running both prunes on their
    defaults kept progress().failed readable on 1.2.0 indefinitely, and
    counted the two pruned successes as unattributed. Both parts of that
    arithmetic are the released contract: a pruned member is not reported
    as succeeded.
    """
    batch = _completed_batch(worker)
    move_batch_back(batch)
    move_back(OxTask.objects.exclude(finished_at=None), "finished_at")

    show(ox_prune())
    assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 0
    show(oxpull_prune())
    show(oxpull_prune("--older-than", "0"))

    progress = batch.progress()
    show(
        f"after both prunes: {progress} unattributed={progress.unattributed} "
        f"member rows={member_rows(batch)}"
    )
    assert (progress.total, progress.pending, progress.succeeded, progress.failed) == (
        3,
        0,
        0,
        1,
    )
    assert progress.unattributed == 2  # the two SUCCESSFUL rows ox_prune deleted
    assert member_rows(batch) == 3


# -- (ii) which workflow rows oxpull_prune may delete ------------------

EVERY_EDGE = {("root", "ok"), ("root", "bad"), ("bad", "doomed"), ("root", "gone")}


def _mixed_workflow(label):
    """
    One sealed workflow whose edges end in every downstream state:

        root -> ok      ok runs and ends SUCCESSFUL
        root -> bad     bad fails three attempts and ends FAILED
        bad  -> doomed  doomed is cancelled by the reconciler: DISCARDED
        root -> gone    gone ends SUCCESSFUL, then ox_prune deletes its row

    Returns the workflow and {node name: task id}.
    """
    flow = workflows.create(label)
    results = {
        "root": flow.add("root", tasks.step, f"{label}-root"),
        "ok": flow.add("ok", tasks.step, f"{label}-ok", depends_on="root"),
        "bad": flow.add("bad", tasks.failing_step, f"{label}-bad", depends_on="root"),
        "doomed": flow.add("doomed", tasks.step, f"{label}-doomed", depends_on="bad"),
        "gone": flow.add("gone", tasks.step, f"{label}-gone", depends_on="root"),
    }
    flow.seal()
    return flow, {name: _pk(result) for name, result in results.items()}


def edges_by_name(flow, ids):
    names = {task_id: name for name, task_id in ids.items()}
    rows = OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).values_list(
        "upstream_task_id", "downstream_task_id"
    )
    return {
        (names.get(_pk(up), str(up)), names.get(_pk(down), str(down)))
        for up, down in rows
    }


def settle():
    """A running install: workers drain and the reconciler ticks, until quiet."""
    worker = flow_worker()
    for _ in range(3):
        drain(worker)
        reconcile_workflows()


def test_oxpull_prune_deletes_only_the_workflow_rows_no_retry_can_move():
    """
    Four workflows, one prune:

    - retired: the mixed workflow, completed a hundred years ago. Only
      the edges into ok (SUCCESSFUL), doomed (DISCARDED) and gone (row
      deleted) may go. The edge into bad (FAILED) stays.
    - recent: the same workflow, created, sealed and given its nodes and
      edges a hundred years ago like the others, but completed a moment
      ago, inside the cutoff. Nothing goes, so the cutoff is read from
      when a workflow completed, not from any other timestamp.
    - clean: a -> b, both SUCCESSFUL, completed a hundred years ago.
      Every node and edge goes, so the prune still reaps.
    - unsealed: a -> b, both SUCCESSFUL, created a hundred years ago and
      never sealed, so never COMPLETE. Nothing goes.
    """
    retired, retired_ids = _mixed_workflow("retired")
    recent, recent_ids = _mixed_workflow("recent")
    clean = workflows.create("clean")
    clean.add("a", tasks.step, "clean-a")
    clean.add("b", tasks.step, "clean-b", depends_on="a")
    clean.seal()
    unsealed = workflows.create("unsealed")
    unsealed.add("a", tasks.step, "unsealed-a")
    unsealed.add("b", tasks.step, "unsealed-b", depends_on="a")

    settle()
    show(f"ran: {order()}")
    for ids in (retired_ids, recent_ids):
        assert status_of(ids["root"]) == OxTask.Status.SUCCESSFUL
        assert status_of(ids["ok"]) == OxTask.Status.SUCCESSFUL
        assert status_of(ids["bad"]) == OxTask.Status.FAILED
        assert status_of(ids["doomed"]) == OxTask.Status.DISCARDED
        assert status_of(ids["gone"]) == OxTask.Status.SUCCESSFUL
    assert "retired-doomed" not in order() and "recent-doomed" not in order()
    for flow in (retired, recent, clean):
        assert stored_flow(flow).status == OxpullWorkflow.Status.COMPLETE
    assert stored_flow(unsealed).status == OxpullWorkflow.Status.OPEN
    assert OxTask.objects.filter(args=["unsealed-b"]).get().status == (
        OxTask.Status.SUCCESSFUL
    )

    # django-ox's own prune deletes the two `gone` rows, and only those.
    gone = [retired_ids["gone"], recent_ids["gone"]]
    move_back(OxTask.objects.filter(id__in=gone), "finished_at")
    show(ox_prune("--older-than", WORKFLOW_CUTOFF))
    assert not OxTask.objects.filter(id__in=gone).exists()
    assert OxTask.objects.filter(args=["retired-bad"]).exists()
    assert edges_by_name(retired, retired_ids) == EVERY_EDGE
    assert edges_by_name(recent, recent_ids) == EVERY_EDGE

    for flow in (retired, clean, unsealed):
        move_workflow_back(flow)
    # Everything about recent is as old as retired except its completion.
    move_back(OxpullWorkflow.objects.filter(pk=recent.id), "created_at", "sealed_at")
    move_back(OxpullWorkflowNode.objects.filter(workflow_id=recent.id), "created_at")
    move_back(OxpullWorkflowEdge.objects.filter(workflow_id=recent.id), "created_at")
    show(oxpull_prune())  # the operator's cron line
    show(oxpull_prune("--older-than", WORKFLOW_CUTOFF))

    left = edges_by_name(retired, retired_ids)
    show(
        f"retired: edges left {sorted(left)}, nodes left {node_rows(retired)}; "
        f"recent: {edge_rows(recent)} edges, {node_rows(recent)} nodes; "
        f"clean: {edge_rows(clean)} edges, {node_rows(clean)} nodes; "
        f"unsealed: {edge_rows(unsealed)} edges, {node_rows(unsealed)} nodes"
    )
    assert ("root", "bad") in left, (
        "oxpull_prune deleted the edge root -> bad of a workflow that completed "
        "a hundred years ago, although bad is FAILED and an operator can still "
        "retry it"
    )
    assert left == {("root", "bad")}, (
        "only the edge into the FAILED node may stay; the edges into a "
        f"SUCCESSFUL, a DISCARDED and a deleted row must go, and {sorted(left)} "
        "were left"
    )
    retired_nodes = set(
        OxpullWorkflowNode.objects.filter(workflow_id=retired.id).values_list(
            "name", flat=True
        )
    )
    assert retired_nodes == {"bad"}, (
        "only the FAILED node may stay; the nodes of a SUCCESSFUL, a DISCARDED "
        f"and a deleted row must go, and {sorted(retired_nodes)} were left"
    )
    assert edges_by_name(recent, recent_ids) == EVERY_EDGE, (
        "a workflow that completed inside the cutoff lost edges"
    )
    assert node_rows(recent) == 5, "a workflow inside the cutoff lost nodes"
    assert (edge_rows(clean), node_rows(clean)) == (0, 0), (
        "a clean workflow that completed a hundred years ago kept its nodes or edges"
    )
    assert (edge_rows(unsealed), node_rows(unsealed)) == (1, 2), (
        "a workflow that never completed lost nodes or edges"
    )


@pytest.mark.django_db(transaction=True)
def test_the_completion_window_alone_leaves_nothing_for_prune_to_ungate():
    """
    The completion window alone: an operator retry committing inside
    workflows._complete_if_finished leaves a READY node on a COMPLETE
    workflow, and prune then deletes its edges.

    Expected to PASS. It is here to show what the window does and does
    not reach. The completing transaction locks the FAILED and LOST node
    rows before it classifies them again, so on PostgreSQL and MySQL the
    retry waits for it to commit (asserted below), as it does on SQLite.
    The end state is the same either way. The only rows retry() can move
    are FAILED and LOST, and a node in either state has been claimed, which
    the gate allowed only once every edge into it was satisfied. The node
    the window leaves READY is pending, so oxpull_prune deletes nothing of
    that workflow, and the node that was really waiting on the failure was
    DISCARDED by the reconciler before completion, which is terminal.
    """
    flow = workflows.create(
        "window",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    root = flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    assert drain(flow_worker()) == 3
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.FAILED

    retried = {}
    inside = {}
    real_enqueue_callback = workflows._enqueue_callback

    def retry_on_another_connection():
        try:
            retried["result"] = actions.retry(root.id)
        finally:
            connections.close_all()

    def enqueue_callback_with_a_retry_landing(*args, **kwargs):
        # Called inside the completing transaction, after the CAS and
        # after the counts were re-read under the workflow row lock.
        thread = threading.Thread(target=retry_on_another_connection)
        thread.start()
        thread.join(timeout=3)
        inside["retry_committed"] = not thread.is_alive()
        inside["thread"] = thread
        return real_enqueue_callback(*args, **kwargs)

    workflows._enqueue_callback = enqueue_callback_with_a_retry_landing
    try:
        assert reconcile_workflows() == 1
    finally:
        workflows._enqueue_callback = real_enqueue_callback
    inside["thread"].join(timeout=30)
    assert retried["result"] is True
    show(
        f"vendor={connection.vendor} retry committed inside the window: "
        f"{inside['retry_committed']}"
    )
    if connection.vendor != "sqlite":
        # PostgreSQL and MySQL: the completion holds the FAILED root's row,
        # so the retry lands just after the commit. SQLite serialises
        # writers, so there it lands just after the commit too.
        assert inside["retry_committed"] is False

    row = stored_flow(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.READY
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.DISCARDED

    move_workflow_back(flow)
    show(oxpull_prune())
    show(oxpull_prune("--older-than", WORKFLOW_CUTOFF))
    # root is READY, so the workflow has a pending node and nothing goes.
    assert (edge_rows(flow), node_rows(flow)) == (1, 2)

    tasks.STATE.clear()
    drain(flow_worker())
    show(f"ran after prune: {order()}")
    assert "child" not in order()


def _pk(result_or_id):
    value = getattr(result_or_id, "id", result_or_id)
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
