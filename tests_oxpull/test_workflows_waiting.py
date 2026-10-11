"""
Workflow nodes that wait in django-ox's WAITING status.

A node with dependencies is inserted WAITING, which no worker claims, and
it moves to READY only once every edge into it is marked satisfied. Only a
writer that saw the upstream row SUCCESSFUL marks an edge. The claim still
checks the edges, and a READY row behind an unmarked edge whose upstream row
is absent or not SUCCESSFUL is not claimed.

These tests pin that and the schema it rests on. The helpers django-ox
exposes for WAITING are private, so their signatures are pinned here too.
"""

import hashlib
import inspect
import logging
import threading
import time
import uuid
from datetime import timedelta

import pytest
from django.conf import settings
from django.db import (
    DatabaseError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.migrations.exceptions import IrreversibleError
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder
from django.db.models import ProtectedError
from django.test import override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import _waiting, actions
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.exceptions import UnknownWorkflowNode, WorkflowNodeExists
from oxpull.models import (
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
    OxpullWorkflowScan,
)
from oxpull.routing import oxpull_db_alias
from oxpull.worker import OxpullWorker

from . import tasks
from .test_batches import session_isolation
from .test_workflows import drain, flow_worker, free_worker, order, reconcile, stored
from .test_workflows_hazard_postgresql_repeatable_read_concurrent_add import (
    level_of_a_new_transaction,
    new_connections_at,
    wait_for_lock_waiters,
)

pytestmark = pytest.mark.django_db

READY = OxTask.Status.READY
WAITING = OxTask.Status.WAITING


def status_of(result):
    return OxTask.objects.get(pk=result.id).status


def chain(name="chain", **using):
    """parent -> child on the "flows" backend, left OPEN."""
    flow = workflows.create(name)
    parent = flow.add("parent", tasks.step, "parent")
    child_task = tasks.step.using(**using) if using else tasks.step
    child = flow.add("child", child_task, "child", depends_on="parent")
    return flow, parent, child


def edge_marks(result):
    return sorted(
        OxpullWorkflowEdge.objects.filter(downstream_task_id=result.id).values_list(
            "satisfied", flat=True
        )
    )


# -- the django-ox helpers this package calls -----------------------------

# Match django-ox's waiting-status definitions. django_ox._waiting is a private
# module, so changes there may be unannounced. This compatibility test must catch
# a mismatch before it reaches an application.
WAITING_HELPER_SIGNATURES = {
    "enqueue": (
        "(task: 'Task[P, R]', args: 'Sequence[Any]', kwargs: 'Mapping[str, Any]', "
        "*, using: 'str') -> 'TaskResult[P, R]'"
    ),
    "release": "(task_id: 'TaskId', *, lease_epoch: 'int', using: 'str') -> 'bool'",
    "release_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') -> 'tuple[int, int]'"
    ),
    "cancel_many": "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') -> 'int'",
    "revive_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') "
        "-> 'dict[uuid.UUID, Revival]'"
    ),
}


@pytest.mark.parametrize("name", sorted(WAITING_HELPER_SIGNATURES))
def test_the_waiting_helpers_keep_the_signatures_this_package_calls(name):
    assert (
        str(inspect.signature(getattr(_waiting, name)))
        == (WAITING_HELPER_SIGNATURES[name])
    )


def test_every_status_django_ox_writes_is_pending_or_settled():
    """
    WAITING, the django-ox status for blocked workflow tasks, holds a workflow open.
    """
    assert WAITING in workflows.PENDING_STATUSES
    assert set(workflows.PENDING_STATUSES) | set(workflows.SETTLED_STATUSES) == set(
        OxTask.Status
    )


# -- add() ------------------------------------------------------------------


def test_a_root_is_enqueued_ready_and_a_node_with_dependencies_waiting():
    flow, parent, child = chain()
    assert (status_of(parent), status_of(child)) == (READY, WAITING)
    assert edge_marks(child) == [False]
    assert flow.progress().blocked == 1


def test_a_node_whose_upstreams_all_succeeded_is_released_inside_the_add():
    """
    Both upstreams have succeeded before the child is added. The child's
    edges go in marked, and the child is READY before add()'s transaction
    commits, so it never needs the check after commit.
    """
    flow = workflows.create("late-add")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b")
    assert drain(flow_worker()) == 2
    with transaction.atomic():
        child = flow.add("child", tasks.step, "child", depends_on=["a", "b"])
        assert status_of(child) == READY
    assert edge_marks(child) == [True, True]
    assert flow_worker().run_once() is True
    assert order() == ["a", "b", "child"]


def test_a_node_waits_until_its_last_upstream_succeeds():
    flow = workflows.create("one-of-two")
    flow.add("a", tasks.step.using(priority=5), "a")
    flow.add("b", tasks.step.using(priority=0), "b")
    assert flow_worker().run_once() is True  # a
    child = flow.add("child", tasks.step, "child", depends_on=["a", "b"])
    assert status_of(child) == WAITING
    assert edge_marks(child) == [False, True]
    assert flow_worker().run_once() is True  # b
    assert status_of(child) == READY
    assert edge_marks(child) == [True, True]
    assert flow_worker().run_once() is True
    assert order() == ["a", "b", "child"]


def settle_without_success(result, how):
    if how == "discarded":
        actions.discard(result.id)
        assert status_of(result) == OxTask.Status.DISCARDED
        return
    OxTask.objects.filter(pk=result.id).update(
        status=OxTask.Status.FAILED if how == "failed" else OxTask.Status.LOST,
        finished_at=timezone.now(),
    )


@pytest.mark.parametrize("how", ["discarded", "failed", "lost"])
def test_a_node_added_after_its_upstream_did_not_succeed_is_never_released(how):
    """
    add() marks an edge satisfied only when it reads the upstream row
    SUCCESSFUL. An upstream that was discarded, failed or lost leaves the
    edge unmarked, so the node waits and no worker runs it. A marked edge
    would release the node inside the add and pass the claim's check too.
    """
    flow = workflows.create(f"after-{how}")
    parent = flow.add("parent", tasks.step, "parent")
    settle_without_success(parent, how)
    child = flow.add("child", tasks.step, "child", depends_on="parent")
    assert status_of(child) == WAITING
    assert edge_marks(child) == [False]
    assert flow_worker().run_once() is False
    assert free_worker().run_once() is False
    flow.seal()
    reconcile()
    assert status_of(child) != READY
    assert order() == []
    if how != "lost":
        assert status_of(child) == OxTask.Status.DISCARDED
        assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


def first_statement(statements, prefix):
    return next(
        (index for index, sql in enumerate(statements) if sql.startswith(prefix)),
        None,
    )


@pytest.mark.parametrize("depends", [False, True], ids=["root", "dependent"])
def test_add_writes_the_workflow_row_before_it_inserts_the_task_row(depends):
    """
    Every workflow operation locks the workflow row first. add() takes that
    lock with its first write, so it inserts no task row while seal() or
    another add() holds the workflow.
    """
    flow = workflows.create("lock-order")
    flow.add("parent", tasks.step, "parent")
    quote = connection.ops.quote_name
    with CaptureQueriesContext(connection) as captured:
        flow.add("node", tasks.step, "node", depends_on="parent" if depends else None)
    statements = [query["sql"] for query in captured.captured_queries]
    workflow_write = first_statement(
        statements, f"UPDATE {quote(OxpullWorkflow._meta.db_table)}"
    )
    task_insert = first_statement(
        statements, f"INSERT INTO {quote(OxTask._meta.db_table)}"
    )
    assert workflow_write is not None and task_insert is not None, statements
    assert workflow_write < task_insert, statements


def test_a_callers_run_after_stays_on_the_waiting_row_and_through_the_release():
    later = timezone.now() + timedelta(hours=1)
    _, _, child = chain("scheduled", run_after=later)
    row = OxTask.objects.get(pk=child.id)
    assert row.status == WAITING
    assert abs(row.run_after - later) < timedelta(seconds=1)

    assert flow_worker().run_once() is True  # the parent
    row = OxTask.objects.get(pk=child.id)
    assert row.status == READY
    assert abs(row.run_after - later) < timedelta(seconds=1)
    assert flow_worker().run_once() is False
    assert free_worker().run_once() is False


def test_add_counts_each_node_it_commits_on_the_workflow_row():
    flow = workflows.create("counted")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    with pytest.raises(WorkflowNodeExists):
        flow.add("a", tasks.step, "again")
    with pytest.raises(UnknownWorkflowNode):
        flow.add("c", tasks.step, "c", depends_on="missing")
    nodes = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count()
    assert (stored(flow).node_count, nodes) == (2, 2)


def test_a_node_name_is_its_exact_spelling():
    """
    name_key is the SHA-256 digest of the name's UTF-8 bytes, so names that
    differ by case, an accent or a trailing space are different nodes on
    every database, and depends_on finds the one spelled that way.
    """
    names = ["Extract", "extract", "resume", "résumé", "load", "load "]
    flow = workflows.create("spelling")
    added = {name: flow.add(name, tasks.step, name) for name in names}
    child = flow.add("child", tasks.step, "child", depends_on="load ")
    keys = dict(
        OxpullWorkflowNode.objects.filter(
            workflow_id=flow.id, name__in=names
        ).values_list("task_id", "name_key")
    )
    assert {str(task_id): key for task_id, key in keys.items()} == {
        str(added[name].id): hashlib.sha256(name.encode("utf-8")).hexdigest()
        for name in names
    }
    edge = OxpullWorkflowEdge.objects.get(downstream_task_id=child.id)
    assert str(edge.upstream_task_id) == str(added["load "].id)


@pytest.mark.django_db(transaction=True)
def test_an_upstream_success_the_add_did_not_see_is_caught_after_commit(monkeypatch):
    """
    add() reads its upstreams inside its own transaction, and an upstream can
    succeed after that read and before the node's edges are visible to the
    upstream's worker. A read that saw nothing stands in for that one here.
    The node commits WAITING, and the check after commit marks the edge and
    releases it.
    """
    flow = workflows.create("stale-read")
    flow.add("parent", tasks.step, "parent")
    assert flow_worker().run_once() is True
    monkeypatch.setattr(workflows, "_succeeded_among", lambda ids, alias: set())
    with transaction.atomic():
        child = flow.add("child", tasks.step, "child", depends_on="parent")
        assert status_of(child) == WAITING
    assert status_of(child) == READY
    assert edge_marks(child) == [True]
    assert flow_worker().run_once() is True
    assert order() == ["parent", "child"]


# -- the claim --------------------------------------------------------------


class SelectThenUpdateWorker(OxpullWorker):
    """
    The same dependency check through the SELECT ... FOR UPDATE SKIP LOCKED
    claim, which PostgreSQL uses when a worker's claim_filter_sql() is empty.
    """

    def claim_filter_sql(self):
        return "", {}


WORKERS = {
    "default-claim": flow_worker,
    "select-then-update-claim": lambda: SelectThenUpdateWorker(
        backend_alias="flows", backoff_initial=0, poll_interval=0.01
    ),
}


@pytest.mark.parametrize("claimer", sorted(WORKERS))
def test_a_ready_node_whose_upstream_row_is_gone_is_not_claimed(claimer):
    """
    Nothing recorded that the upstream succeeded, and its row is gone. The
    node has been put back to READY, the shape a django-ox retry or an
    application's own UPDATE leaves. A missing row says nothing about how
    the upstream finished, so the claim refuses the node.
    """
    _, parent, child = chain(f"pruned-{claimer}")
    OxTask.objects.filter(pk=parent.id).delete()
    OxTask.objects.filter(pk=child.id).update(status=READY)
    assert WORKERS[claimer]().claim_one() is None
    assert status_of(child) == READY


@pytest.mark.parametrize("claimer", sorted(WORKERS))
def test_a_node_whose_edge_was_marked_runs_after_the_upstream_row_is_gone(claimer):
    _, parent, child = chain(f"marked-{claimer}")
    assert WORKERS[claimer]().run_once() is True  # parent, which marks and releases
    assert edge_marks(child) == [True]
    OxTask.objects.filter(pk=parent.id).delete()
    assert WORKERS[claimer]().run_once() is True
    assert order() == ["parent", "child"]


# -- completion, cancellation and the reconciler ----------------------------


def test_a_sealed_workflow_whose_unfinished_node_waits_stays_sealed():
    """
    The upstream row is gone with nothing recorded, so the child waits and
    nothing releases it. WAITING is pending: the workflow holds, and the
    waiting node shows in progress().
    """
    flow, parent, child = chain("held")
    flow.seal()
    OxTask.objects.filter(pk=parent.id).delete()
    reconcile()
    reconcile()
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    assert status_of(child) == WAITING
    progress = flow.progress()
    assert (progress.pending, progress.blocked) == (1, 1)
    assert free_worker().claim_one() is None


def test_the_reconciler_cancels_a_waiting_cone():
    flow = workflows.create("doomed-waiting")
    flow.add("root", tasks.failing_step, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    leaf = flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.seal()
    worker = flow_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert (status_of(mid), status_of(leaf)) == (WAITING, WAITING)
    assert reconcile() == 1
    assert (status_of(mid), status_of(leaf)) == (
        OxTask.Status.DISCARDED,
        OxTask.Status.DISCARDED,
    )
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


def test_the_reconciler_releases_a_node_no_worker_released():
    """
    A SUCCESSFUL written by something other than an OxpullWorker releases
    nothing on its own. One reconciler pass marks the edge and releases the
    node, in an OPEN workflow as well as a sealed one.
    """
    _, parent, child = chain("out-of-band")
    OxTask.objects.filter(pk=parent.id).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    assert status_of(child) == WAITING
    assert flow_worker().run_once() is False
    reconcile()
    assert edge_marks(child) == [True]
    assert status_of(child) == READY
    assert flow_worker().run_once() is True
    assert order() == ["child"]


def test_a_release_that_fails_after_the_outcome_is_left_to_the_reconciler(
    monkeypatch, caplog
):
    _, parent, child = chain("release-fails")

    def fails(task_ids, db_alias):
        raise DatabaseError("simulated: the release statement failed")

    with monkeypatch.context() as patch:
        patch.setattr("oxpull.worker.release_downstream", fails)
        with caplog.at_level(logging.WARNING, logger="oxpull"):
            assert flow_worker().run_once() is True
    assert status_of(parent) == OxTask.Status.SUCCESSFUL
    assert status_of(child) == WAITING
    # The edge was marked with the outcome. Only the release after it failed.
    assert edge_marks(child) == [True]
    assert [
        record
        for record in caplog.records
        if getattr(record, "event", "") == "workflow_release_failed"
    ]
    reconcile()
    assert status_of(child) == READY


def test_the_reconciler_holds_a_waiting_node_whose_upstream_row_is_gone():
    _, parent, child = chain("unknown")
    OxTask.objects.filter(pk=parent.id).delete()
    reconcile()
    assert edge_marks(child) == [False]
    assert status_of(child) == WAITING


# -- a release that loses a lock conflict -------------------------------------


class SimulatedDriverError(Exception):
    """The driver error a lock conflict carries as its cause."""


def lock_conflict():
    """An OperationalError this database's release reads as a lost lock conflict."""
    if connection.vendor == "mysql":
        return OperationalError(1213, "Deadlock found when trying to get lock")
    cause = SimulatedDriverError("simulated lock conflict")
    if connection.vendor == "postgresql":
        cause.sqlstate = "40001"
    else:
        cause.sqlite_errorcode = 5  # SQLITE_BUSY
    error = OperationalError("simulated lock conflict")
    error.__cause__ = cause
    return error


def not_a_lock_conflict():
    return OperationalError("simulated: not a lock conflict")


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", "") == name]


def children_ready_to_release(name, count):
    """`count` WAITING children of one SUCCESSFUL parent, every edge marked."""
    flow = workflows.create(name)
    parent = flow.add("parent", tasks.step, "parent")
    children = [
        flow.add(f"c{index}", tasks.step, f"c{index}", depends_on="parent")
        for index in range(count)
    ]
    OxTask.objects.filter(pk=parent.id).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    OxpullWorkflowEdge.objects.filter(upstream_task_id=parent.id).update(satisfied=True)
    assert {status_of(child) for child in children} == {WAITING}
    return sorted(uuid.UUID(str(child.id)) for child in children)


def conflicting_release_many(monkeypatch, losing, conflicts, error=lock_conflict):
    """
    django-ox's release_many, except that a call carrying any id in `losing`
    raises `error()` instead, the first `conflicts` times. Returns the ids of
    every call.
    """
    real = _waiting.release_many
    calls = []
    left = [conflicts]

    def release_many(rows, *, using):
        rows = list(rows)
        ids = {uuid.UUID(str(pk)) for pk, _ in rows}
        calls.append(ids)
        if ids & losing and left[0] > 0:
            left[0] -= 1
            raise error()
        return real(rows, using=using)

    monkeypatch.setattr(workflows._waiting, "release_many", release_many)
    return calls


def statuses(ids):
    found = dict(OxTask.objects.filter(pk__in=ids).values_list("pk", "status"))
    return [found[pk] for pk in ids]


@pytest.mark.django_db(transaction=True)
def test_a_release_that_loses_a_lock_conflict_decides_again_from_fresh_reads(
    monkeypatch, caplog
):
    ids = children_ready_to_release("conflict-once", 1)
    calls = conflicting_release_many(monkeypatch, set(ids), conflicts=1)
    with caplog.at_level(logging.WARNING, logger="oxpull"):
        assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
    assert calls == [set(ids), set(ids)]
    assert statuses(ids) == [READY]
    assert not events(caplog, "workflow_release_deferred")


@pytest.mark.django_db(transaction=True)
def test_a_release_chunk_that_loses_twice_is_left_to_the_reconciler(
    monkeypatch, caplog
):
    monkeypatch.setattr(workflows, "ID_CHUNK_SIZE", 1)
    ids = children_ready_to_release("conflict-twice", 2)
    calls = conflicting_release_many(monkeypatch, {ids[0]}, conflicts=2)
    with caplog.at_level(logging.WARNING, logger="oxpull"):
        assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
    assert calls == [{ids[0]}, {ids[0]}, {ids[1]}]
    assert statuses(ids) == [WAITING, READY]
    assert len(events(caplog, "workflow_release_deferred")) == 1
    reconcile()
    assert statuses(ids) == [READY, READY]


def skip_queue(flow):
    """The ids this workflow's backstop is holding to try again."""
    row = OxpullWorkflowScan.objects.filter(workflow_id=flow.id).first()
    return [] if row is None else list(row.skipped)


def record_of(result):
    return OxpullWorkflowNode.objects.values_list(
        "outcome", "outcome_epoch", "succeeded"
    ).get(task_id=result.id)


@pytest.mark.django_db(transaction=True)
def test_a_workers_release_that_loses_twice_keeps_everything_but_the_release(
    monkeypatch, caplog
):
    """
    The deferral as a worker meets it, which is the only way a user meets
    it. The test above calls the release directly; this one runs a task.

    A parent with two children, one id to a chunk, and the chunk that would
    free c1 loses both of its tries. Everything except that release has to
    survive it: the task body ran, its outcome is committed on the row, the
    node's record carries it, both edges are marked satisfied, c0 is READY
    from the chunk that won and c1 is WAITING, c1's id is in its workflow's
    skip queue for the reconciler, and the product logged one deferral
    naming how many rows it could not release.

    A chunk that lost a lock conflict is not a chunk that took the work down
    with it, and a mixed call is not all-or-nothing: that is what the two
    statuses side by side say.
    """
    monkeypatch.setattr(workflows, "ID_CHUNK_SIZE", 1)
    flow = workflows.create("worker-defers")
    parent = flow.add("parent", tasks.step, "parent")
    c0 = flow.add("c0", tasks.step, "c0", depends_on="parent")
    c1 = flow.add("c1", tasks.step, "c1", depends_on="parent")
    flow.seal()
    losing = uuid.UUID(str(c1.id))
    calls = conflicting_release_many(monkeypatch, {losing}, conflicts=2)

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        assert flow_worker().run_once() is True

    assert order() == ["parent"]
    assert status_of(parent) == OxTask.Status.SUCCESSFUL
    epoch = OxTask.objects.get(pk=parent.id).lease_epoch
    assert record_of(parent) == (OxTask.Status.SUCCESSFUL, epoch, True)
    assert (edge_marks(c0), edge_marks(c1)) == ([True], [True])
    # The chunk that won released its node; the chunk that lost did not.
    assert (status_of(c0), status_of(c1)) == (READY, WAITING)
    assert len(calls) == 3 and calls.count({losing}) == 2, calls
    deferred = events(caplog, "workflow_release_deferred")
    assert len(deferred) == 1, deferred
    assert deferred[0].task_count == 1
    assert skip_queue(flow) == [str(c1.id)]


# Reconciler passes the recovery below is allowed, with a task run after each.
# One is what it takes, from the skip queue and from the round alike, and both
# are measured: the bound passes with this set to 1. The second is slack for a
# tick that stopped between phases, and nothing here is large enough to make a
# tick stop at all.
RECOVERY_PASSES = 2


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("queued", [True, False], ids=["queued", "dropped"])
def test_the_reconciler_recovers_a_deferred_release_and_runs_nothing_twice(
    monkeypatch, caplog, queued
):
    """
    The other half of the contract: what the reconciler owes a deferral.

    A worker's release loses both tries against both children at once, so
    they stay WAITING with their edges marked. The contention then goes away,
    as it does the moment the other writer commits, and nothing but the
    ordinary reconciler and an ordinary worker runs after that: no hand
    release, no repair, no unbounded polling. Within RECOVERY_PASSES passes
    every eligible node is runnable and has run, exactly once each and after
    its parent.

    `dropped` empties the skip queue first. The product documents that a
    queue at its cap drops ids with a log line, so a node whose only route
    back was that queue is a node the reconciler can lose forever. Run that
    way, this test passes only if the round finds the node from the persisted
    edge marks alone.

    A node that is genuinely blocked stays blocked throughout: its upstream
    is running elsewhere, so no pass may release it and no pass may cancel
    it either.
    """
    flow = workflows.create("recovers")
    flow.add("parent", tasks.step, "parent")
    c0 = flow.add("c0", tasks.step, "c0", depends_on="parent")
    c1 = flow.add("c1", tasks.step, "c1", depends_on="parent")
    stuck = flow.add("stuck", tasks.step, "stuck")
    blocked = flow.add("blocked", tasks.step, "blocked", depends_on="stuck")
    flow.seal()
    OxTask.objects.filter(pk=stuck.id).update(
        status=OxTask.Status.RUNNING, locked_by="elsewhere", locked_at=timezone.now()
    )
    children = {uuid.UUID(str(c0.id)), uuid.UUID(str(c1.id))}
    # One chunk holds both children, and it loses both of its tries.
    conflicting_release_many(monkeypatch, children, conflicts=2)

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        assert flow_worker().run_once() is True
    assert len(events(caplog, "workflow_release_deferred")) == 1
    assert (status_of(c0), status_of(c1)) == (WAITING, WAITING)
    assert sorted(skip_queue(flow)) == sorted([str(c0.id), str(c1.id)])
    if not queued:
        OxpullWorkflowScan.objects.filter(workflow_id=flow.id).update(skipped=[])

    passes = 0
    while {status_of(c0), status_of(c1)} != {OxTask.Status.SUCCESSFUL}:
        assert passes < RECOVERY_PASSES, (
            f"after {passes} reconciler passes c0 is {status_of(c0)} and c1 is "
            f"{status_of(c1)}, with their edges marked "
            f"{(edge_marks(c0), edge_marks(c1))} and the skip queue holding "
            f"{skip_queue(flow)}"
        )
        reconcile()
        drain(flow_worker())
        passes += 1

    ran = order()
    assert sorted(ran) == ["c0", "c1", "parent"], ran
    assert ran[0] == "parent", ran
    assert skip_queue(flow) == []
    # The blocked node is still blocked: not released, and not cancelled
    # either, because its upstream has not settled.
    assert (status_of(blocked), edge_marks(blocked)) == (WAITING, [False])
    assert status_of(stuck) == OxTask.Status.RUNNING
    # Nothing is owed any more, and another pass finds nothing to redo.
    assert workflows._release_backstop(oxpull_db_alias()) == 0
    assert drain(flow_worker()) == 0
    assert order() == ran


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("inside_a_transaction", [True, False])
def test_a_release_decides_again_only_after_a_lock_conflict_in_its_own_transaction(
    monkeypatch, inside_a_transaction
):
    """
    Inside a caller's transaction a lock conflict propagates at once, because
    the caller's work is lost with it. An error that is not a lock conflict
    propagates at once everywhere.
    """
    ids = children_ready_to_release("conflict-propagates", 1)
    error = lock_conflict if inside_a_transaction else not_a_lock_conflict
    calls = conflicting_release_many(monkeypatch, set(ids), conflicts=2, error=error)
    with pytest.raises(OperationalError):
        if inside_a_transaction:
            with transaction.atomic():
                workflows.release_downstream(ids, oxpull_db_alias())
        else:
            workflows.release_downstream(ids, oxpull_db_alias())
    assert calls == [set(ids)]
    assert statuses(ids) == [WAITING]


@pytest.mark.django_db(transaction=True)
def test_at_repeatable_read_a_release_that_waited_on_another_release_still_releases(
    monkeypatch,
):
    """
    Two releases at REPEATABLE READ. One has released a row and not yet
    committed. The other's chunk holds that row and a second one, and waits on
    the first's lock. On PostgreSQL the waiting chunk loses a serialization
    failure when the first commits. It decides again from fresh reads and
    releases the second row. On MySQL the locking read is a current read, and
    the chunk goes through on its first attempt.
    """
    if connection.vendor not in ("postgresql", "mysql"):
        pytest.skip("SQLite has no isolation level and no row locks")
    # The first release commits only once the second is seen waiting, however
    # long a busy machine takes to see it, so the release's two bounds are
    # moved out of the way. The wait is inside a locking read, which the
    # statement bound cuts short as the lock wait bound does, on both engines.
    # Past either one the chunk is decided again with the first release still
    # uncommitted. test_workflows_outcomes.py tests the bounds.
    monkeypatch.setattr(workflows, "RELEASE_LOCK_TIMEOUT", 30.0)
    monkeypatch.setattr(workflows, "RELEASE_STATEMENT_TIMEOUT", 30.0)
    alias = oxpull_db_alias()
    ids = children_ready_to_release("two-releases", 2)
    epoch = OxTask.objects.get(pk=ids[0]).lease_epoch
    calls = conflicting_release_many(monkeypatch, set(), conflicts=0)
    held, go = threading.Event(), threading.Event()
    box = {}

    def hold():
        try:
            box["holder level"] = level_of_a_new_transaction()
            with transaction.atomic(using=alias):
                box["held"] = _waiting.release(ids[0], lease_epoch=epoch, using=alias)
                held.set()
                go.wait(30)
        except BaseException as exc:
            box["holder error"] = exc
        finally:
            held.set()
            connections.close_all()

    def release():
        try:
            box["releaser level"] = level_of_a_new_transaction()
            box["released"] = workflows.release_downstream(ids, alias)
        except BaseException as exc:
            box["releaser error"] = exc
        finally:
            connections.close_all()

    with new_connections_at("repeatable-read-options"):
        holder = threading.Thread(target=hold)
        holder.start()
        assert held.wait(30)
        releaser = threading.Thread(target=release)
        releaser.start()
        waiters = wait_for_lock_waiters(1)
        go.set()
        holder.join(30)
        releaser.join(30)

    assert "holder error" not in box and "releaser error" not in box, box
    assert box["holder level"] == box["releaser level"] == "repeatable read", box
    assert box["held"] is True
    assert waiters >= 1, "the second release never waited on the first"
    assert box["released"] == 1
    assert statuses(ids) == [READY, READY]
    if connection.vendor == "postgresql":
        assert calls == [set(ids), {ids[1]}], calls
    else:
        assert calls == [set(ids)], calls


def doomed_sealed_workflow(name):
    flow = workflows.create(name)
    root = flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    OxTask.objects.filter(pk=root.id).update(
        status=OxTask.Status.FAILED, finished_at=timezone.now()
    )
    flow.seal()
    return flow, child


def test_a_tick_whose_release_backstop_fails_still_cancels_and_completes(
    monkeypatch, caplog
):
    flow, child = doomed_sealed_workflow("beside-a-failed-backstop")

    def fails(db_alias):
        raise DatabaseError("simulated: the release backstop failed")

    monkeypatch.setattr(workflows, "_release_backstop", fails)
    with caplog.at_level(logging.ERROR, logger="oxpull"):
        # The root was failed by hand, so nothing recorded it. The first tick
        # cancels the child and records the root. The second completes.
        assert reconcile() == 0
        assert status_of(child) == OxTask.Status.DISCARDED
        assert reconcile() == 1
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE
    assert len(events(caplog, "workflow_release_backstop_failed")) == 2


def test_a_task_timeout_in_the_release_backstop_ends_the_tick(monkeypatch):
    _flow, child = doomed_sealed_workflow("timed-out-backstop")

    def times_out(db_alias):
        raise TaskTimeout("simulated")

    monkeypatch.setattr(workflows, "_release_backstop", times_out)
    with pytest.raises(TaskTimeout):
        reconcile()
    assert status_of(child) == WAITING


def test_a_workflow_row_that_has_nodes_cannot_be_deleted():
    flow, _, _ = chain("protected")
    with pytest.raises(ProtectedError):
        OxpullWorkflow.objects.filter(pk=flow.id).delete()
    assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 2
    assert OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count() == 1


# -- the second workflow backend ----------------------------------------------


def test_the_second_workflow_backend_holds_a_node_like_the_first():
    flow = workflows.create("leftover")
    flow.add("parent", tasks.deferred_step, "parent")
    child = flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    row = OxTask.objects.get(pk=child.id)
    assert (row.status, row.run_after) == (WAITING, None)


def database_check(databases):
    """
    oxpull's database-tagged check, called as `check --database` calls it.

    Called directly rather than through checks.run_checks(), whose other
    registered checks depend on which modules earlier tests imported.
    """
    from oxpull.checks import check_workflow_schema

    return check_workflow_schema(databases=databases)


def test_waiting_nodes_with_no_workflows_backend_are_a_database_check_warning():
    _, _, child = chain("orphaned")
    assert database_check(None) == []
    assert not [m for m in database_check(["default"]) if m.id == "oxpull.W007"]
    without = {
        alias: {
            **config,
            "OPTIONS": {
                key: value
                for key, value in config.get("OPTIONS", {}).items()
                if key != "WORKFLOWS"
            },
        }
        for alias, config in settings.TASKS.items()
    }
    with override_settings(TASKS=without):
        found = [m for m in database_check(["default"]) if m.id == "oxpull.W007"]
    assert status_of(child) == WAITING
    assert len(found) == 1
    assert found[0].msg.startswith("1 workflow node(s)")


# -- migrating back ---------------------------------------------------------


def _migrate_to(targets):
    executor = MigrationExecutor(connection)
    executor.loader.build_graph()
    executor.migrate(targets)


def _leaves():
    executor = MigrationExecutor(connection)
    executor.loader.build_graph()
    return [
        (app, name)
        for app, name in executor.loader.graph.leaf_nodes()
        if app in ("oxpull", "django_ox")
    ]


WORKFLOW_TABLES = [
    model._meta.db_table
    for model in (OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode)
]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "target",
    [("oxpull", "0003_oxpullratewindow"), ("django_ox", "0007_oxschedule")],
    ids=lambda target: target[0],
)
def test_migrating_back_past_workflows_refuses_while_a_workflow_exists(target):
    """
    Migrating either app back unapplies oxpull's 0005 and then 0004 first.
    Each refuses while any workflow row exists, before it undoes anything, so
    no table or index is dropped and every migration stays applied.
    """
    leaves = _leaves()
    workflows.create("keeps-its-tables")
    try:
        with pytest.raises(IrreversibleError, match=r"0004_workflows"):
            _migrate_to([target])
        tables = connection.introspection.table_names()
        assert [table for table in WORKFLOW_TABLES if table not in tables] == []
        applied = MigrationRecorder(connection).applied_migrations()
        assert ("oxpull", "0005_workflow_edge_upstream_idx") in applied
        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(
                cursor, OxpullWorkflowEdge._meta.db_table
            )
        assert "oxpull_wf_edge_up_idx" in constraints
    finally:
        _migrate_to(leaves)


@pytest.mark.django_db(transaction=True)
def test_migrating_back_to_0004_names_what_unapplying_0005_drops():
    """
    A target of 0004_workflows unapplies only 0005. Its refusal says what 0005
    drops, the index, and does not say the tables go.
    """
    leaves = _leaves()
    workflows.create("keeps-its-index")
    try:
        with pytest.raises(IrreversibleError) as refused:
            _migrate_to([("oxpull", "0004_workflows")])
        message = str(refused.value)
        assert message.startswith(
            "Cannot migrate oxpull back past 0005_workflow_edge_upstream_idx."
        ), message
        assert "Unapplying it drops the index" in message, message
        applied = MigrationRecorder(connection).applied_migrations()
        assert ("oxpull", "0005_workflow_edge_upstream_idx") in applied
    finally:
        _migrate_to(leaves)


@pytest.mark.django_db(transaction=True)
def test_0004_refuses_on_its_own_once_0005_is_unapplied():
    """
    0005's refusal fires first on the way back, so 0004's is reached only when
    0005 was unapplied before the first workflow. Then 0004 still refuses, and
    the tables stay.
    """
    leaves = _leaves()
    try:
        _migrate_to([("oxpull", "0004_workflows")])
        workflows.create("after-0005-went")
        with pytest.raises(IrreversibleError) as refused:
            _migrate_to([("oxpull", "0003_oxpullratewindow")])
        message = str(refused.value)
        assert message.startswith(
            "Cannot migrate oxpull back past 0004_workflows. 1 workflow(s)"
        ), message
        tables = connection.introspection.table_names()
        assert [table for table in WORKFLOW_TABLES if table not in tables] == []
        applied = MigrationRecorder(connection).applied_migrations()
        assert ("oxpull", "0004_workflows") in applied
    finally:
        _migrate_to(leaves)


@pytest.mark.django_db(transaction=True)
def test_migrating_back_past_workflows_is_allowed_before_the_first_workflow():
    leaves = _leaves()
    try:
        _migrate_to([("oxpull", "0003_oxpullratewindow")])
        tables = connection.introspection.table_names()
        assert [table for table in WORKFLOW_TABLES if table in tables] == []
    finally:
        _migrate_to(leaves)


# -- MySQL REPEATABLE READ: a held claim and the WAITING key range --------------

BLOCKED = 400


@pytest.mark.django_db(transaction=True)
def test_on_mysql_repeatable_read_a_held_claim_locks_no_waiting_row_and_add_does_not_wait():  # noqa: E501
    """
    A claim at REPEATABLE READ holds next-key locks on every index entry it
    scans until its transaction ends. Blocked nodes that were READY sat in
    that range, so the claim locked them all and an add() inserting another
    one waited on it. WAITING rows are outside the range every claim scans.

    The claim is held open while performance_schema.data_locks is read from
    another connection, and while add() calls on other connections insert
    WAITING nodes ahead of, inside and behind the existing ones with a 2 s
    lock wait.
    """
    if connection.vendor != "mysql":
        pytest.skip("MySQL only: data_locks and next-key locks are InnoDB's")
    flow = workflows.create("locks")
    root = flow.add("root", tasks.step, "root")
    # Running elsewhere, so nothing downstream of it can be released.
    OxTask.objects.filter(pk=root.id).update(
        status=OxTask.Status.RUNNING, locked_by="elsewhere"
    )
    blocked = [
        flow.add(
            f"b{i:04d}", tasks.step.using(priority=10), f"b{i:04d}", depends_on="root"
        )
        for i in range(BLOCKED)
    ]
    for i in range(3):
        tasks.step.using(priority=0).enqueue(f"runnable-{i}")
    with connection.cursor() as cursor:
        cursor.execute(
            f"ANALYZE TABLE {OxTask._meta.db_table}, "
            f"{OxpullWorkflowEdge._meta.db_table}, {OxpullWorkflowNode._meta.db_table}"
        )
    blocked_hex = {uuid.UUID(str(result.id)).hex for result in blocked}
    schema = connection.settings_dict["NAME"]

    def locks_now():
        observer = connections.create_connection("default")
        try:
            with observer.cursor() as cursor:
                cursor.execute(
                    "SELECT INDEX_NAME, LOCK_MODE, LOCK_DATA FROM "
                    "performance_schema.data_locks WHERE OBJECT_SCHEMA = %s "
                    "AND OBJECT_NAME = %s",
                    [schema, OxTask._meta.db_table],
                )
                return cursor.fetchall()
        finally:
            observer.close()

    def add_with_a_short_wait(name, priority):
        outcome = {}

        def run():
            try:
                with connections["default"].cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 2")
                started = time.monotonic()
                try:
                    flow.add(
                        name,
                        tasks.step.using(priority=priority),
                        name,
                        depends_on="root",
                    )
                    outcome["result"] = "added"
                except OperationalError as exc:
                    outcome["result"] = f"{exc.args[0]} {exc}"
                outcome["seconds"] = round(time.monotonic() - started, 2)
            finally:
                connections.close_all()

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(30)
        return outcome

    worker = flow_worker()
    with session_isolation("REPEATABLE READ"), transaction.atomic():
        claimed = worker.claim_one()
        held = locks_now()
        adds = {
            priority: add_with_a_short_wait(f"late-{priority}", priority)
            for priority in (100, 10, -100)
        }
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@transaction_isolation")
            (isolation,) = cursor.fetchone()

    assert isolation.upper().replace("_", "-") == "REPEATABLE-READ"
    assert claimed is not None and claimed.args[0].startswith("runnable-"), claimed
    on_blocked = [
        row for row in held if any(key in (row[2] or "") for key in blocked_hex)
    ]
    on_waiting = [row for row in held if "'WAITING'" in (row[2] or "")]
    assert (on_blocked, on_waiting) == ([], []), (
        f"the held claim locked {len(on_blocked)} of {BLOCKED} blocked rows and "
        f"{len(on_waiting)} WAITING index entries; all its locks: {held[:12]}"
    )
    assert {priority: outcome.get("result") for priority, outcome in adds.items()} == {
        100: "added",
        10: "added",
        -100: "added",
    }, adds


# -- MySQL: the task-id columns follow OxTask.id's collation ---------------------


def _mysql_collation(table, column):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLLATION_NAME FROM information_schema.COLUMNS"
            " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
            " AND COLUMN_NAME = %s",
            [table, column],
        )
        row = cursor.fetchone()
    return None if row is None else row[0]


@pytest.mark.django_db(transaction=True)
def test_on_mysql_migrate_puts_the_task_id_columns_on_the_task_ids_collation():
    """
    The database's default collation is changed after django-ox's tables
    were created, as a dump restored into a newer server leaves it, and then
    `migrate oxpull` creates the workflow tables. Every column that holds a
    task id ends up on OxTask.id's collation, so no claim meets MySQL's
    illegal mix of collations.
    """
    if connection.vendor != "mysql":
        pytest.skip("MySQL only: a uuid column has no collation elsewhere")
    from django.core.management import call_command

    from .test_upgrade import _latest_migration, _migrate

    task_table = OxTask._meta.db_table
    task_collation = _mysql_collation(task_table, OxTask._meta.pk.column)
    new_default = (
        "utf8mb4_0900_ai_ci"
        if task_collation != "utf8mb4_0900_ai_ci"
        else "utf8mb4_general_ci"
    )
    name = connection.settings_dict["NAME"]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA"
            " WHERE SCHEMA_NAME = DATABASE()"
        )
        (original_default,) = cursor.fetchone()
    columns = [
        (OxpullWorkflowEdge._meta.db_table, "upstream_task_id"),
        (OxpullWorkflowEdge._meta.db_table, "downstream_task_id"),
        (OxpullWorkflowNode._meta.db_table, "task_id"),
        (OxpullWorkflowNode._meta.db_table, "cancelled_by_task_id"),
    ]
    latest = _latest_migration()
    _migrate("0003_oxpullratewindow")
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER DATABASE `{name}` COLLATE {new_default}")
        call_command("migrate", "oxpull", skip_checks=True, verbosity=0)
        found = {f"{t}.{c}": _mysql_collation(t, c) for t, c in columns}
    finally:
        _migrate("0003_oxpullratewindow")
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER DATABASE `{name}` COLLATE {original_default}")
        _migrate(latest)
    assert found == dict.fromkeys(found, task_collation), (
        f"the database defaulted to {new_default} and task ids are on "
        f"{task_collation}; migrate left the workflow columns on {found}"
    )
