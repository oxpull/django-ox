"""
What a workflow node's outcome writes, and the release that follows it.

OxpullWorker writes a node's SUCCESSFUL or FAILED, the node's record and, on
success, the marks on the edges out of it, in one transaction. It releases the
nodes those edges held once that transaction has committed. The claim says
which rows are nodes, so a task that is not one pays for nothing more.

These tests pin that transaction and what it does when it loses a lock
conflict, the table that decides whether a record accepts a write, the flag
each claim path carries, and the bounds, second try and skip queue of the
release.
"""

import logging
import re
import statistics
import threading
import time
import uuid
from contextlib import contextmanager

import pytest
from django.db import (
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.models import Q
from django.utils import timezone

import oxpull.worker as oxpull_worker
from django_ox.compat import task_finished
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.models import OxpullWorkflowEdge, OxpullWorkflowNode, OxpullWorkflowScan
from oxpull.routing import oxpull_db_alias
from oxpull.worker import OxpullWorker

from . import tasks
from .test_batches import session_isolation
from .test_upgrade import _latest_migration, _migrate
from .test_workflows import drain, flow_worker, reconcile
from .test_workflows_hazard_mixed_fleet_migration import (
    before_workflows_migration,  # noqa: F401 - a fixture
)
from .test_workflows_waiting import (
    SimulatedDriverError,
    chain,
    children_ready_to_release,
    conflicting_release_many,
    edge_marks,
    events,
    lock_conflict,
    not_a_lock_conflict,
    status_of,
    statuses,
)

pytestmark = pytest.mark.django_db

READY = OxTask.Status.READY
RUNNING = OxTask.Status.RUNNING
WAITING = OxTask.Status.WAITING
SUCCESSFUL = OxTask.Status.SUCCESSFUL
FAILED = OxTask.Status.FAILED
LOST = OxTask.Status.LOST
DISCARDED = OxTask.Status.DISCARDED

TASK_TABLE = OxTask._meta.db_table
NODE_TABLE = OxpullWorkflowNode._meta.db_table
EDGE_TABLE = OxpullWorkflowEdge._meta.db_table

# The attribute a claimed row carries, spelled out so this module imports on a
# package without it and each test fails on its own.
NODE_FLAG = "ox_wf_node"


def quoted(name):
    return connection.ops.quote_name(name)


def record_of(result):
    return OxpullWorkflowNode.objects.values_list(
        "outcome", "outcome_epoch", "succeeded"
    ).get(task_id=result.id)


def epoch_of(result):
    return OxTask.objects.get(pk=result.id).lease_epoch


def node_lock_read(sql, params):
    """The outcome's read of its node row: the first statement after the task row."""
    node, column, alias = (re.escape(quoted(name)) for name in (NODE_TABLE, "id", "pk"))
    # S608: a pattern matched against SQL, not a query.
    return re.match(rf"\s*SELECT {node}\.{column}(?: AS {alias})? FROM {node} ", sql)  # noqa: S608


def updates_of(table):
    def match(sql, params):
        return sql.lstrip().startswith(f"UPDATE {quoted(table)}")

    return match


@contextmanager
def statements(match=None, error=None, times=None):
    """
    Every statement this thread's connection runs in the block, as (sql,
    params), and the ones `match` accepts. With `error`, each accepted
    statement raises error() instead of running, the first `times` of them, or
    all of them when `times` is None.
    """
    log, seen = [], []
    left = [times]

    def wrapper(execute, sql, params, many, context):
        log.append((sql, list(params) if isinstance(params, (list, tuple)) else params))
        if match is not None and match(sql, params):
            seen.append(sql)
            if error is not None and (left[0] is None or left[0] > 0):
                if left[0] is not None:
                    left[0] -= 1
                raise error()
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        yield log, seen


@contextmanager
def finished_signals():
    sent = []

    def receiver(sender, task_result, **kwargs):
        sent.append(str(task_result.id))

    task_finished.connect(receiver)
    try:
        yield sent
    finally:
        task_finished.disconnect(receiver)


def outcome_writes(log, status):
    """The worker's outcome UPDATEs of the task table that write `status`."""
    task = quoted(TASK_TABLE)
    return [
        sql
        for sql, params in log
        if sql.lstrip().startswith(f"UPDATE {task}")
        and isinstance(params, list)
        and str(status) in [str(value) for value in params]
    ]


# -- the transition table -----------------------------------------------------

# (record outcome, record epoch), (written status, written epoch), accepted.
TRANSITIONS = [
    pytest.param((None, None), (SUCCESSFUL, 1), True, id="none-any"),
    pytest.param((None, None), (FAILED, 0), True, id="none-epoch-0"),
    pytest.param((None, 4), (SUCCESSFUL, 4), True, id="cleared-equal"),
    pytest.param((None, 4), (FAILED, 5), True, id="cleared-greater"),
    pytest.param((None, 4), (SUCCESSFUL, 3), False, id="cleared-lesser"),
    pytest.param((FAILED, 3), (SUCCESSFUL, 4), True, id="failed-greater"),
    pytest.param((SUCCESSFUL, 3), (FAILED, 4), True, id="successful-greater"),
    pytest.param((DISCARDED, 3), (LOST, 4), True, id="discarded-greater-lost"),
    pytest.param((SUCCESSFUL, 3), (FAILED, 2), False, id="successful-lesser"),
    pytest.param((LOST, 3), (SUCCESSFUL, 2), False, id="lost-lesser"),
    pytest.param((LOST, 3), (SUCCESSFUL, 3), True, id="lost-equal-successful"),
    pytest.param((LOST, 3), (FAILED, 3), True, id="lost-equal-failed"),
    pytest.param((LOST, 3), (DISCARDED, 3), True, id="lost-equal-discarded"),
    pytest.param((LOST, 3), (LOST, 3), False, id="lost-equal-lost"),
    pytest.param((FAILED, 3), (DISCARDED, 3), False, id="failed-equal-discarded"),
    pytest.param((FAILED, 3), (LOST, 3), False, id="failed-equal-lost"),
    pytest.param((SUCCESSFUL, 3), (FAILED, 3), False, id="successful-equal-failed"),
    pytest.param((SUCCESSFUL, 3), (SUCCESSFUL, 3), False, id="successful-equal-same"),
    pytest.param((SUCCESSFUL, 3), (LOST, 3), False, id="successful-equal-lost"),
    pytest.param((DISCARDED, 3), (SUCCESSFUL, 3), False, id="discarded-equal-success"),
    # A settled outcome with no epoch is not a state any writer leaves. It is
    # corrupt, and it accepts anything, so the next observation repairs it.
    pytest.param((SUCCESSFUL, None), (FAILED, 0), True, id="corrupt-no-epoch"),
]


def a_node(name):
    flow = workflows.create(name)
    result = flow.add("only", tasks.step, "only")
    return OxpullWorkflowNode.objects.get(task_id=result.id)


def set_record(node, record, succeeded):
    OxpullWorkflowNode.objects.filter(pk=node.pk).update(
        outcome=record[0], outcome_epoch=record[1], succeeded=succeeded
    )


def stored_record(node):
    return OxpullWorkflowNode.objects.values_list(
        "outcome", "outcome_epoch", "succeeded"
    ).get(pk=node.pk)


@pytest.mark.parametrize(("record", "write", "accepted"), TRANSITIONS)
def test_a_record_takes_a_write_exactly_where_the_transition_table_says(
    record, write, accepted
):
    node = a_node("transitions")
    set_record(node, record, succeeded=record[0] == SUCCESSFUL)

    took = workflows.record_outcome(node.pk, write[0], write[1], oxpull_db_alias())

    assert took is accepted
    if accepted:
        assert stored_record(node) == (write[0], write[1], write[0] == SUCCESSFUL)
    else:
        assert stored_record(node) == (record[0], record[1], record[0] == SUCCESSFUL)


@pytest.mark.parametrize(("record", "write", "accepted"), TRANSITIONS)
def test_succeeded_changes_only_when_the_record_takes_the_write(
    record, write, accepted
):
    """
    succeeded starts out saying the opposite of the record's outcome, which no
    writer leaves behind. A refused write must leave it that way, and a write
    the record takes must set it from the written status.
    """
    node = a_node("succeeded")
    marker = record[0] != SUCCESSFUL
    set_record(node, record, succeeded=marker)

    workflows.record_outcome(node.pk, write[0], write[1], oxpull_db_alias())

    succeeded = stored_record(node)[2]
    assert succeeded is ((write[0] == SUCCESSFUL) if accepted else marker)


def test_a_record_refuses_a_status_that_is_not_settled():
    node = a_node("unsettled")
    for status in (READY, RUNNING, WAITING):
        with pytest.raises(ValueError, match="not a settled status"):
            workflows.record_outcome(node.pk, status, 1, oxpull_db_alias())
    assert stored_record(node) == (None, None, False)


# -- what the outcome writes ----------------------------------------------------


def test_a_node_that_succeeds_records_it_and_marks_the_edges_out_of_it():
    _, parent, child = chain("succeeds")
    assert flow_worker().run_once() is True
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert edge_marks(child) == [True]
    assert status_of(child) == READY
    assert record_of(child) == (None, None, False)


def test_a_node_that_fails_records_the_failure_and_marks_nothing():
    flow = workflows.create("fails")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    worker = flow_worker()
    assert worker.run_once() is True
    assert status_of(root) == READY, "the first attempt should be retried"
    assert record_of(root) == (None, None, False), "a retry write recorded something"
    drain(worker)
    assert status_of(root) == FAILED
    assert record_of(root) == (FAILED, epoch_of(root), False)
    assert edge_marks(child) == [False]
    assert status_of(child) == WAITING


def test_a_worker_on_a_backend_without_workflows_records_and_releases():
    """
    "limited" declares no WORKFLOWS, and it shares the "default" queue with
    "flows". Any backend on the database declaring WORKFLOWS is enough.
    """
    _, parent, child = chain("limited-peer")
    worker = OxpullWorker(
        backend_alias="limited", backoff_initial=0, poll_interval=0.01
    )
    assert worker._workflows is False
    assert worker.run_once() is True
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert edge_marks(child) == [True]
    assert status_of(child) == READY


@pytest.mark.django_db(transaction=True)
def test_a_worker_started_before_the_workflow_migration_records_once_it_is_applied(
    before_workflows_migration,  # noqa: F811
    monkeypatch,
):
    """
    "limited" declares no WORKFLOWS, so it records only because "flows" does.
    Before 0004_workflows its claim cannot read the node table, and it runs a
    plain task as it did before. Once the migration is applied it records.
    """
    monkeypatch.setattr(oxpull_worker, "NODE_TABLE_RECHECK", 0.0, raising=False)
    worker = OxpullWorker(
        backend_alias="limited", backoff_initial=0, poll_interval=0.01
    )
    loose = tasks.step.enqueue("loose")
    assert worker.run_once() is True
    assert status_of(loose) == SUCCESSFUL

    _migrate(_latest_migration())
    _, parent, child = chain("after-the-migration")
    assert worker.run_once() is True
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert status_of(child) == READY


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("failing", ["node record", "edge marks"])
def test_a_failure_part_way_through_a_node_outcome_leaves_the_task_row_running(
    failing, caplog
):
    """
    The outcome, the record and the marks are one transaction. A statement of
    the record or the marks that fails takes the task row's write back with it,
    and the worker reports no outcome: the row stays RUNNING for the reaper.
    """
    _, parent, child = chain(f"part-way-{failing.replace(' ', '-')}")
    table = NODE_TABLE if failing == "node record" else EDGE_TABLE
    with (
        caplog.at_level(logging.WARNING, logger="oxpull"),
        finished_signals() as finished,
        statements(updates_of(table), error=not_a_lock_conflict) as (_, seen),
    ):
        assert flow_worker().run_once() is True

    assert status_of(parent) == RUNNING, (
        f"the task row's outcome committed although its {failing} failed"
    )
    assert len(seen) == 1, "an error that is not a lock conflict was tried again"
    assert record_of(parent) == (None, None, False)
    assert edge_marks(child) == [False]
    assert status_of(child) == WAITING
    assert finished == []
    assert len(events(caplog, "workflow_outcome_unrecorded")) == 1


@pytest.mark.django_db(transaction=True)
def test_a_node_outcome_that_loses_a_lock_conflict_runs_its_whole_transaction_again(
    caplog,
):
    _, parent, child = chain("conflict-once")
    with (
        caplog.at_level(logging.INFO, logger="oxpull"),
        finished_signals() as finished,
        statements(node_lock_read, error=lock_conflict, times=1) as (log, seen),
    ):
        assert flow_worker().run_once() is True

    assert len(seen) == 2
    assert len(outcome_writes(log, SUCCESSFUL)) == 2, "the fence was not written again"
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert edge_marks(child) == [True]
    assert status_of(child) == READY
    assert finished == [str(parent.id)]
    assert len(events(caplog, "workflow_outcome_retry")) == 1
    assert not events(caplog, "workflow_outcome_unrecorded")


@pytest.mark.django_db(transaction=True)
def test_a_stuck_thread_handover_that_loses_a_lock_conflict_keeps_its_epoch():
    """
    The handover writes FAILED at the next epoch. The try that lost had already
    copied that epoch onto the instance, and the second try must fence on the
    epoch the execution holds, not on the one the lost try wrote.
    """
    _, parent, child = chain("handover")
    OxTask.objects.filter(pk=parent.id).update(max_attempts=1)
    worker = flow_worker()
    claimed = worker.claim_one()
    assert str(claimed.pk) == str(parent.id)
    epoch = claimed.lease_epoch

    with statements(node_lock_read, error=lock_conflict, times=1) as (_, seen):
        landed = worker._handle_failure(claimed, RuntimeError("stuck"), 0, release=True)

    assert landed is True
    assert len(seen) == 2
    row = OxTask.objects.get(pk=parent.id)
    assert (row.status, row.lease_epoch) == (FAILED, epoch + 1)
    assert claimed.lease_epoch == epoch + 1
    assert record_of(parent) == (FAILED, epoch + 1, False)
    assert edge_marks(child) == [False]


@pytest.mark.django_db(transaction=True)
def test_a_node_outcome_that_keeps_losing_lock_conflicts_fails_closed(caplog):
    _, parent, child = chain("conflict-always")
    worker = flow_worker()
    claimed = worker.claim_one()
    epoch = claimed.lease_epoch
    with (
        caplog.at_level(logging.INFO, logger="oxpull"),
        finished_signals() as finished,
        statements(node_lock_read, error=lock_conflict) as (_, seen),
    ):
        worker.execute(claimed)

    assert len(seen) == oxpull_worker.OUTCOME_ATTEMPTS
    row = OxTask.objects.get(pk=parent.id)
    assert (row.status, row.lease_epoch) == (RUNNING, epoch)
    assert claimed.status == RUNNING, "the instance kept a status that never committed"
    assert finished == []
    assert len(events(caplog, "workflow_outcome_retry")) == len(seen) - 1
    assert len(events(caplog, "workflow_outcome_unrecorded")) == 1
    assert record_of(parent) == (None, None, False)
    assert status_of(child) == WAITING


@pytest.mark.django_db(transaction=True)
def test_a_node_outcome_stops_trying_once_the_lease_is_nearly_spent(caplog):
    """
    A try holds the task row, which stalls the lease's renewal. With a lease of
    0.6 s renewed every 0.2 s, a try may start only in the first 0.2 s.
    """
    _, parent, _child = chain("lease-spent")
    worker = flow_worker(lock_timeout=0.6, renew_interval=0.2)

    def slow_conflict():
        time.sleep(0.3)
        return lock_conflict()

    with (
        caplog.at_level(logging.INFO, logger="oxpull"),
        statements(node_lock_read, error=slow_conflict) as (_, seen),
    ):
        assert worker.run_once() is True

    assert len(seen) == 1
    assert status_of(parent) == RUNNING
    assert len(events(caplog, "workflow_outcome_unrecorded")) == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("caller", ["atomic", "autocommit-off"])
def test_a_node_outcome_inside_a_callers_transaction_propagates_a_lock_conflict(
    caller,
):
    """
    Whether the call owns its transaction is read from the connection: an open
    atomic block is the caller's, and so is a transaction the caller started
    by turning autocommit off.
    """
    _, parent, _child = chain(f"caller-{caller}")
    worker = flow_worker()
    claimed = worker.claim_one()
    with (
        statements(node_lock_read, error=lock_conflict) as (_, seen),
        pytest.raises(OperationalError),
    ):
        if caller == "atomic":
            with transaction.atomic():
                worker.execute(claimed)
        else:
            connection.set_autocommit(False)
            try:
                worker.execute(claimed)
            finally:
                connection.rollback()
                connection.set_autocommit(True)
    assert len(seen) == 1
    assert status_of(parent) == RUNNING


def recording_release_many(monkeypatch, seen):
    """Record every release_many call, and what another connection sees then."""
    real = workflows._waiting.release_many

    def release_many(rows, *, using):
        rows = list(rows)

        def look():
            try:
                return (
                    [status_of(OxTask.objects.get(pk=pk)) for pk, _ in rows],
                    OxpullWorkflowEdge.objects.filter(
                        downstream_task_id__in=[pk for pk, _ in rows]
                    ).values_list("satisfied", flat=True)[::1],
                )
            finally:
                connections.close_all()

        box = {}
        thread = threading.Thread(target=lambda: box.setdefault("seen", look()))
        thread.start()
        thread.join(30)
        seen.append(box.get("seen"))
        return real(rows, using=using)

    monkeypatch.setattr(workflows._waiting, "release_many", release_many)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("caller", ["atomic", "autocommit-off"])
def test_inside_a_callers_transaction_the_release_waits_for_its_commit(
    caller, monkeypatch
):
    """
    A worker's pass inside a caller's transaction writes the outcome, the record
    and the marks there, and releases nothing yet. The release runs once the
    caller commits, and from another connection it then sees the child's edge
    marked.
    """
    _, parent, child = chain(f"release-waits-for-the-commit-{caller}")
    worker = flow_worker()
    claimed = worker.claim_one()
    seen = []
    recording_release_many(monkeypatch, seen)

    if caller == "atomic":
        with transaction.atomic():
            worker.execute(claimed)
            inside = (status_of(parent), status_of(child), edge_marks(child), seen[:])
    else:
        connection.set_autocommit(False)
        try:
            worker.execute(claimed)
            inside = (status_of(parent), status_of(child), edge_marks(child), seen[:])
            connection.commit()
        finally:
            connection.set_autocommit(True)

    assert inside == (SUCCESSFUL, WAITING, [True], [])
    assert seen == [([WAITING], [True])]
    assert status_of(child) == READY


@pytest.mark.django_db(transaction=True)
def test_a_caller_that_rolls_back_releases_nothing(monkeypatch):
    _, parent, child = chain("caller-rolls-back")
    worker = flow_worker()
    claimed = worker.claim_one()
    seen = []
    recording_release_many(monkeypatch, seen)

    with pytest.raises(RuntimeError), transaction.atomic():
        worker.execute(claimed)
        raise RuntimeError("the caller's own work failed")

    assert seen == []
    assert (status_of(parent), status_of(child), edge_marks(child)) == (
        RUNNING,
        WAITING,
        [False],
    )


@pytest.mark.django_db(transaction=True)
def test_a_release_failure_after_the_callers_commit_cannot_reach_its_transaction(
    monkeypatch, caplog
):
    """
    Before, the release ran last inside a caller's transaction, and a release
    error that ended that transaction had to propagate, because it took the
    outcome with it. The release now runs after the caller's commit, in
    transactions of its own. The same error there ends only the release's own
    transaction: the outcome and the marks stay committed, task_finished was
    sent, and the child goes to its workflow's skip queue for the reconciler.
    """
    flow, parent, child = chain("release-error-after-the-commit")
    worker = flow_worker()
    claimed = worker.claim_one()

    def ends_the_transaction(rows, *, using):
        with connections[using].cursor() as cursor:
            cursor.execute("ROLLBACK")
        raise lock_conflict()

    monkeypatch.setattr(workflows._waiting, "release_many", ends_the_transaction)
    with (
        caplog.at_level(logging.WARNING, logger="oxpull"),
        finished_signals() as sent,
        transaction.atomic(),
    ):
        worker.execute(claimed)

    assert len(sent) == 1
    assert (status_of(parent), status_of(child), edge_marks(child)) == (
        SUCCESSFUL,
        WAITING,
        [True],
    )
    assert len(events(caplog, "workflow_release_deferred")) == 1
    assert OxpullWorkflowScan.objects.get(workflow_id=flow.id).skipped == [
        str(child.id)
    ]


@pytest.mark.commit_callbacks_wait
def test_a_node_outcome_in_a_callers_transaction_registers_its_release_on_commit(
    django_capture_on_commit_callbacks,
):
    """
    The release is a commit callback on the database the record was written to.
    Inside the block nothing is released. When the block runs the callbacks,
    the child is released. A success with nothing downstream, a final failure
    and a task that is not a node register none.

    This test runs inside one transaction and carries commit_callbacks_wait, so
    tests_oxpull/conftest.py leaves the worker's callbacks to this block.
    """
    _, parent, child = chain("on-commit")
    with django_capture_on_commit_callbacks(
        using=oxpull_db_alias(), execute=True
    ) as callbacks:
        assert flow_worker().run_once() is True
        inside = (status_of(parent), status_of(child), edge_marks(child))
    assert inside == (SUCCESSFUL, WAITING, [True])
    assert len(callbacks) == 1
    assert status_of(child) == READY

    flow = workflows.create("on-commit-failure")
    bad = flow.add("bad", tasks.failing_step, "bad")
    flow.add("after-bad", tasks.step, "after-bad", depends_on="bad")
    plain = tasks.plain_echo.enqueue("not a node")
    with django_capture_on_commit_callbacks(using=oxpull_db_alias()) as callbacks:
        # The child, bad's three attempts, and the plain task on a queue the
        # flows backend shares.
        assert drain(flow_worker()) == 5
    assert (status_of(child), status_of(bad)) == (SUCCESSFUL, FAILED)
    assert OxTask.objects.get(pk=plain.id).status == SUCCESSFUL
    assert callbacks == []


@pytest.mark.django_db(transaction=True)
def test_the_release_after_a_node_outcome_sees_the_outcome_committed(monkeypatch):
    _, parent, child = chain("after-commit")
    seen = {}
    real = oxpull_worker.release_downstream

    def look_from_another_connection(task_ids, db_alias):
        def look():
            try:
                seen["parent"] = status_of(parent)
                seen["marks"] = edge_marks(child)
            finally:
                connections.close_all()

        reader = threading.Thread(target=look)
        reader.start()
        reader.join(30)
        seen["ids"] = sorted(str(task_id) for task_id in task_ids)
        return real(task_ids, db_alias)

    monkeypatch.setattr(
        oxpull_worker, "release_downstream", look_from_another_connection, raising=False
    )
    assert flow_worker().run_once() is True
    assert seen == {"parent": SUCCESSFUL, "marks": [True], "ids": [str(child.id)]}
    assert status_of(child) == READY


@pytest.mark.django_db(transaction=True)
def test_a_node_outcome_waits_on_a_held_node_row_for_its_bound_then_tries_again(
    monkeypatch, caplog
):
    """
    Another transaction holds the upstream's node row, as an add() naming it
    does. The outcome waits for the lock no longer than its bound, then runs
    its transaction again, and lands once the holder commits.
    """
    if connection.vendor not in ("postgresql", "mysql"):
        pytest.skip("SQLite has no row locks")
    monkeypatch.setattr(oxpull_worker, "OUTCOME_LOCK_TIMEOUT", 1.0, raising=False)
    _, parent, child = chain("held-node")
    held = threading.Event()
    box = {}

    def hold():
        try:
            with transaction.atomic():
                list(
                    OxpullWorkflowNode.objects.select_for_update()
                    .filter(task_id=parent.id)
                    .values_list("pk", flat=True)
                )
                held.set()
                time.sleep(3.5)
        except BaseException as exc:
            box["error"] = exc
        finally:
            held.set()
            connections.close_all()

    holder = threading.Thread(target=hold)
    holder.start()
    assert held.wait(30)
    started = time.monotonic()
    with caplog.at_level(logging.INFO, logger="oxpull"):
        assert flow_worker().run_once() is True
    elapsed = time.monotonic() - started
    holder.join(30)

    assert box == {}
    assert elapsed >= 1.0, elapsed
    assert len(events(caplog, "workflow_outcome_retry")) >= 1
    assert not events(caplog, "workflow_outcome_unrecorded")
    assert status_of(parent) == SUCCESSFUL
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert status_of(child) == READY


@contextmanager
def probing_at(match, probe):
    """Run `probe(cursor)` on this connection before each statement `match` accepts."""
    found = []
    inside = [False]

    def wrapper(execute, sql, params, many, context):
        if match(sql, params) and not inside[0]:
            inside[0] = True
            try:
                with connection.cursor() as cursor:
                    found.append(probe(cursor))
            finally:
                inside[0] = False
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        yield found


def mysql_lock_wait(cursor):
    cursor.execute("SELECT @@SESSION.innodb_lock_wait_timeout")
    return int(cursor.fetchone()[0])


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("outcome", ["lands", "fails closed"])
def test_on_mysql_a_node_outcome_bounds_its_lock_waits_and_puts_the_session_back(
    outcome,
):
    if connection.vendor != "mysql":
        pytest.skip("MySQL's lock wait bound is a session variable")
    _, parent, _child = chain(f"mysql-bounds-{outcome.replace(' ', '-')}")
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 37")
    error = lock_conflict if outcome == "fails closed" else None
    try:
        with (
            probing_at(node_lock_read, mysql_lock_wait) as during,
            statements(node_lock_read, error=error),
        ):
            assert flow_worker().run_once() is True
        with connection.cursor() as cursor:
            after = mysql_lock_wait(cursor)
    finally:
        connection.close()
    expected_tries = oxpull_worker.OUTCOME_ATTEMPTS if error else 1
    assert during == [5] * expected_tries
    assert after == 37
    assert status_of(parent) == (RUNNING if error else SUCCESSFUL)


@pytest.mark.django_db(transaction=True)
def test_on_mysql_a_session_that_cannot_be_put_back_is_closed(caplog):
    if connection.vendor != "mysql":
        pytest.skip("MySQL's lock wait bound is a session variable")
    _, parent, child = chain("mysql-restore-fails")
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 37")
    closed = []
    real_close = connection.close

    def close():
        closed.append(True)
        real_close()

    def restoring_37(sql, params):
        return sql.startswith("SET SESSION innodb_lock_wait_timeout") and list(
            params or []
        )[:1] == [37]

    try:
        connection.close = close
        with (
            caplog.at_level(logging.WARNING, logger="oxpull"),
            statements(restoring_37, error=not_a_lock_conflict, times=1) as (_, seen),
        ):
            assert flow_worker().run_once() is True
    finally:
        del connection.close
        connection.close()
    assert len(seen) == 1
    assert closed, "the connection was not closed"
    assert len(events(caplog, "workflow_session_bounds_not_restored")) == 1
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert status_of(child) == READY


@pytest.mark.django_db(transaction=True)
def test_on_postgresql_each_try_of_a_node_outcome_bounds_its_lock_waits():
    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL bounds lock waits per transaction")
    _, parent, _child = chain("pg-bounds")

    def lock_timeout(cursor):
        cursor.execute("SHOW lock_timeout")
        return cursor.fetchone()[0]

    with (
        probing_at(node_lock_read, lock_timeout) as during,
        statements(node_lock_read, error=lock_conflict, times=1),
    ):
        assert flow_worker().run_once() is True
    with connection.cursor() as cursor:
        after = lock_timeout(cursor)
    assert during == ["5s", "5s"]
    assert after == "0"
    assert status_of(parent) == SUCCESSFUL


# -- the flag the claim carries ---------------------------------------------------

# OxpullWorker copies django-ox's PostgreSQL claim statement and the method that
# formats it, to add the node flag. One distribution now ships both, so a
# digest of the copied text would only say the text changed, not whether the
# copy still behaves like the original. These tests hold the behaviour: the
# row a claim leaves, what the claimed object carries, the fence on the lease,
# and the outcome a failure or a success writes must be the same whichever of
# the two workers took the task. On PostgreSQL the claim these workers take
# is the single statement; claim_path() names the path each database runs.

WORKERS = {
    "free": lambda: Worker(backoff_initial=0, poll_interval=0.01),
    "oxpull": flow_worker,
}


def test_the_node_flag_name_is_the_one_the_claim_sets():
    assert workflows.NODE_FLAG == NODE_FLAG


def stored(task_id):
    return OxTask.objects.get(pk=task_id)


def shape(row):
    """What a claim leaves on a row, with the values that differ per row left out."""
    return {
        "status": row.status,
        "attempts": row.attempts,
        "lease_epoch": row.lease_epoch,
        "worker_ids": len(row.worker_ids),
        "locked_by": bool(row.locked_by),
        "locked_at": row.locked_at is not None,
        "started_at": row.started_at is not None,
        "last_attempted_at": row.last_attempted_at is not None,
        "finished_at": row.finished_at is not None,
        "lease_seconds": round((row.lease_expires_at - row.locked_at).total_seconds()),
    }


@pytest.mark.parametrize("kind", sorted(WORKERS))
def test_a_claim_leaves_a_running_row_with_a_lease_and_a_new_epoch(kind):
    queued = tasks.plain_echo.enqueue("claimed")
    assert stored(queued.id).lease_epoch == 0
    claimed = WORKERS[kind]().claim_one()
    assert str(claimed.pk) == str(queued.id)
    assert shape(stored(queued.id)) == {
        "status": RUNNING,
        "attempts": 1,
        "lease_epoch": 1,
        "worker_ids": 1,
        "locked_by": True,
        "locked_at": True,
        "started_at": True,
        "last_attempted_at": True,
        "finished_at": False,
        "lease_seconds": shape(stored(queued.id))["lease_seconds"],
    }
    assert shape(stored(queued.id))["lease_seconds"] > 0


def test_both_workers_leave_the_same_row_behind():
    shapes = {}
    for kind in sorted(WORKERS):
        queued = tasks.plain_echo.enqueue(kind)
        WORKERS[kind]().claim_one()
        shapes[kind] = shape(stored(queued.id))
    assert shapes["free"] == shapes["oxpull"]


@pytest.mark.parametrize("kind", sorted(WORKERS))
def test_the_claimed_object_is_the_row_as_the_database_now_holds_it(kind):
    queued = tasks.plain_echo.enqueue(
        "reloaded",
    )
    claimed = WORKERS[kind]().claim_one()
    fresh = stored(queued.id)
    differing = {
        field.attname: (getattr(claimed, field.attname), getattr(fresh, field.attname))
        for field in OxTask._meta.concrete_fields
        if getattr(claimed, field.attname) != getattr(fresh, field.attname)
    }
    assert differing == {}


@pytest.mark.parametrize("kind", sorted(WORKERS))
def test_a_worker_reaped_off_its_task_cannot_write_the_outcome(kind):
    queued = tasks.plain_echo.enqueue("fenced")
    worker = WORKERS[kind]()
    claimed = worker.claim_one()
    # What the reaper and a second worker do between this worker's claim and
    # its finish: the epoch moves on and another holder has the row.
    OxTask.objects.filter(pk=claimed.pk).update(
        lease_epoch=claimed.lease_epoch + 1, locked_by="the-new-holder"
    )
    worker.execute(claimed)
    row = stored(queued.id)
    assert (row.status, row.return_value, row.locked_by, row.finished_at) == (
        RUNNING,
        None,
        "the-new-holder",
        None,
    )
    assert row.lease_epoch == claimed.lease_epoch + 1


def outcome(row):
    return {
        "status": row.status,
        "attempts": row.attempts,
        "errors": len(row.errors),
        "finished": row.finished_at is not None,
        "locked_by": row.locked_by,
        "lease_expires_at": row.lease_expires_at,
    }


def test_a_success_writes_the_same_outcome_from_either_worker():
    written = {}
    for kind in sorted(WORKERS):
        queued = tasks.plain_echo.enqueue(f"value-{kind}")
        assert WORKERS[kind]().run_once() is True
        row = stored(queued.id)
        assert row.return_value == f"value-{kind}"
        written[kind] = outcome(row)
    assert written["free"] == written["oxpull"]
    assert written["free"] == {
        "status": SUCCESSFUL,
        "attempts": 1,
        "errors": 0,
        "finished": True,
        "locked_by": None,
        "lease_expires_at": None,
    }


def test_a_failure_retries_and_then_fails_the_same_from_either_worker():
    history = {}
    for kind in sorted(WORKERS):
        queued = tasks.plain_fail.enqueue()
        worker = WORKERS[kind]()
        seen = []
        while worker.run_once():
            row = outcome(stored(queued.id))
            seen.append(
                (row["status"], row["attempts"], row["errors"], row["finished"])
            )
        history[kind] = seen
    assert history["free"] == history["oxpull"]
    assert history["free"] == [
        (READY, 1, 1, False),
        (READY, 2, 2, False),
        (FAILED, 3, 3, True),
    ]


class NarrowedWorker(OxpullWorker):
    """A subclass that narrows claim_filter_q() alone, and so has no SQL fragment."""

    def claim_filter_q(self):
        condition = super().claim_filter_q()
        narrowed = Q(queue_name="default")
        return narrowed if condition is None else condition & narrowed

    def claim_filter_sql(self):
        return "", {}


CLAIMERS = {
    "workflow-backend": flow_worker,
    "claim-filter-q-only": lambda: NarrowedWorker(
        backend_alias="flows", backoff_initial=0, poll_interval=0.01
    ),
    "backend-without-workflows": lambda: OxpullWorker(
        backend_alias="limited", backoff_initial=0, poll_interval=0.01
    ),
}


def claim_path(worker):
    """Which django-ox claim path this worker takes on this database."""
    if connection.vendor == "sqlite":
        return "compare-and-set"
    if connection.vendor == "postgresql" and (
        worker.claim_filter_sql()[0] or worker.claim_filter_q() is None
    ):
        return "single-statement"
    return "skip-locked"


@pytest.mark.parametrize("claimer", sorted(CLAIMERS))
def test_every_claim_path_flags_whether_the_claimed_task_is_a_node(claimer):
    plain = tasks.plain_echo.enqueue("plain")
    flow = workflows.create(f"flag-{claimer}")
    node = flow.add("node", tasks.step, "node")
    worker = CLAIMERS[claimer]()
    path = claim_path(worker)

    with statements() as (log, _):
        first = worker.claim_one()
        second = worker.claim_one()

    flags = {str(task.pk): getattr(task, NODE_FLAG) for task in (first, second)}
    assert flags == {str(plain.id): False, str(node.id): True}
    assert None not in flags.values()
    flagged = [sql for sql, _ in log if NODE_FLAG in sql]
    assert len(flagged) == 2, flagged
    for sql in flagged:
        if path == "single-statement":
            assert sql.lstrip().startswith("UPDATE") and "RETURNING *" in sql
        elif path == "skip-locked":
            assert "SKIP LOCKED" in sql
        else:
            assert sql.lstrip().startswith("SELECT") and "SKIP LOCKED" not in sql


@pytest.mark.django_db(transaction=True)
def test_a_task_the_claim_flagged_as_no_node_writes_its_outcome_alone():
    plain = tasks.step.enqueue("plain")
    with statements() as (log, _):
        assert flow_worker().run_once() is True
    assert status_of(plain) == SUCCESSFUL
    node_reads = [sql for sql, _ in log if NODE_TABLE in sql and NODE_FLAG not in sql]
    assert node_reads == []
    bounds = [sql for sql, _ in log if "set_config" in sql or "SET SESSION" in sql]
    assert bounds == []


@pytest.mark.django_db(transaction=True)
def test_a_row_fetched_outside_the_claim_is_looked_up():
    plain = tasks.step.enqueue("plain")
    _, parent, child = chain("fetched")
    worker = flow_worker()
    for expected in (plain, parent):
        claimed = worker.claim_one()
        assert str(claimed.pk) == str(expected.id)
        fetched = OxTask.objects.get(pk=claimed.pk)
        assert not hasattr(fetched, NODE_FLAG)
        worker.execute(fetched)
    assert status_of(plain) == SUCCESSFUL
    assert not OxpullWorkflowNode.objects.filter(task_id=plain.id).exists()
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert edge_marks(child) == [True]
    assert status_of(child) == READY


def data_locks_on(table):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM performance_schema.data_locks "
            "WHERE OBJECT_SCHEMA = DATABASE() AND OBJECT_NAME = %s",
            [table],
        )
        return int(cursor.fetchone()[0])


def lock_rows_on(table):
    """Lock rows on `table` in this database, as (index, type, mode, locked data)."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT INDEX_NAME, LOCK_TYPE, LOCK_MODE, LOCK_DATA "
            "FROM performance_schema.data_locks "
            "WHERE OBJECT_SCHEMA = DATABASE() AND OBJECT_NAME = %s",
            [table],
        )
        return sorted(
            tuple("" if value is None else str(value) for value in row)
            for row in cursor.fetchall()
        )


def holds_the_gap_only(lock_row):
    """True when a lock row covers the gap before a record and not the record."""
    return "GAP" in lock_row[2].split(",")


def holds_record_and_gap(lock_row):
    """True when a lock row is a next-key lock, over a record and the gap before it."""
    modes = lock_row[2].split(",")
    return "GAP" not in modes and "REC_NOT_GAP" not in modes


def entry_of(lock_row):
    """The index entry a lock row names, as (index, locked data)."""
    return lock_row[0], lock_row[3]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ"])
def test_on_mysql_a_held_claim_that_flags_a_node_locks_no_node_row(level):
    if connection.vendor != "mysql":
        pytest.skip("performance_schema.data_locks is MySQL's")
    _, parent, _child = chain(f"held-claim-{level.replace(' ', '-')}")
    with session_isolation(level):
        worker = flow_worker()
        with transaction.atomic():
            unflagged = (
                Worker._ready_queryset(worker)
                .select_for_update(skip_locked=True)
                .first()
            )
            assert str(unflagged.pk) == str(parent.id)
            unflagged_task_locks = lock_rows_on(TASK_TABLE)
        with transaction.atomic():
            candidate = (
                worker._ready_queryset().select_for_update(skip_locked=True).first()
            )
            assert str(candidate.pk) == str(parent.id)
            assert getattr(candidate, NODE_FLAG) is True
            task_locks = lock_rows_on(TASK_TABLE)
            node_locks = data_locks_on(NODE_TABLE)
    assert task_locks, "the claim held no lock at all, so nothing was measured"
    # Compare the locks that decide whether another session waits. A second
    # claim skips a locked record and a worker's own UPDATE waits on one, so a
    # record the flag locked and django-ox's read did not would cost another
    # worker its row. A locking read also locks index records purge has not
    # removed yet, so the later read can hold fewer of them than the earlier.
    held = [row for row in task_locks if not holds_the_gap_only(row)]
    unflagged_held = [
        row for row in unflagged_task_locks if not holds_the_gap_only(row)
    ]
    assert set(held) <= set(unflagged_held), (held, unflagged_held)
    # The gaps themselves are not the same gaps between the two reads: purge
    # removes a delete-marked entry the read had locked and hands its gap to
    # the successor, so a gap-only lock appears that neither read took. It
    # widens nothing, because the claim already holds that record and the gap
    # before it, which is what this asserts.
    next_key = {entry_of(row) for row in held if holds_record_and_gap(row)}
    inherited = [row for row in task_locks if holds_the_gap_only(row)]
    assert all(entry_of(row) in next_key for row in inherited), (inherited, held)
    assert [row for row in task_locks if row[2].startswith("S")] == []
    assert node_locks == 0


def mysql_only_read_view():
    if connection.vendor != "mysql":
        pytest.skip("MySQL answers a locking read's subqueries from a read view")


@pytest.mark.django_db(transaction=True)
def test_on_mysql_a_node_added_after_the_claims_read_view_opened_records_its_outcome():
    """
    MySQL answers the claim's subqueries from a read view that can open before
    the row the claim returns was committed. In a real claim the gate on a row
    the scan passes first opens it. Here a plain read inside the claim's
    transaction opens it at REPEATABLE READ, and add() commits a root and its
    child from another connection before the locking read runs.
    """
    mysql_only_read_view()
    flow = workflows.create("view-before-node")
    added = {}

    def add_from_another_connection():
        try:
            handle = workflows.get(flow.id)
            added["root"] = handle.add("root", tasks.step, "root")
            added["kid"] = handle.add("kid", tasks.step, "kid", depends_on="root")
        finally:
            connections.close_all()

    def open_the_view_then_add(execute, sql, params, many, context):
        if "SKIP LOCKED" in sql and not added:
            with connection.cursor() as cursor:
                # S608: a table name from the model's _meta.
                cursor.execute(f"SELECT COUNT(*) FROM {quoted(NODE_TABLE)}")  # noqa: S608
            adder = threading.Thread(target=add_from_another_connection)
            adder.start()
            adder.join(30)
        return execute(sql, params, many, context)

    with session_isolation("REPEATABLE READ"):
        worker = flow_worker()
        with connection.execute_wrapper(open_the_view_then_add):
            claimed = worker.claim_one()
        root, kid = added["root"], added["kid"]
        assert str(claimed.pk) == str(root.id)
        worker.execute(claimed)

    assert status_of(root) == SUCCESSFUL
    assert record_of(root) == (SUCCESSFUL, epoch_of(root), True)
    assert edge_marks(kid) == [True]
    assert status_of(kid) == READY
    assert getattr(claimed, NODE_FLAG) is None


# Rows the gate refuses ahead of every root in claim order. The claim probes the
# edge index for each one, so their number is how long a scan runs, and a root is
# claimed from outside the scan's read view only if its COMMIT lands inside that
# time. Measured on MySQL 8.4 held to two CPUs and to half of one, 800 rows make
# a scan 6 to 17 ms against a COMMIT of 2 to 10 ms, and 19 rounds in 20 are hits.
REFUSED_AHEAD = 800
# Rows added after a COMMIT that landed behind the scan it was aimed at.
REFUSED_GROWTH = 800
# Roots claimed from outside their claim's read view before the adding stops, and
# the rounds it may take to get them.
HITS_WANTED = 5
MAX_ROUNDS = 200
# How many of the latest scans, and of the latest COMMITs, a median is taken over.
MEDIAN_OVER = 9
# No thread waits on the other for longer than RENDEZVOUS_SECONDS, and no round
# starts after LOOP_SECONDS.
RENDEZVOUS_SECONDS = 30
LOOP_SECONDS = 300


@contextmanager
def every_connection_at(level):
    """
    DATABASES OPTIONS["isolation_level"] for every connection opened in the
    block, in any thread. MySQL only.
    """
    alias = oxpull_db_alias()
    targets = {
        id(options): options
        for options in (
            connections.settings[alias].setdefault("OPTIONS", {}),
            connections[alias].settings_dict.setdefault("OPTIONS", {}),
        )
    }
    missing = object()
    saved = {
        key: options.get("isolation_level", missing) for key, options in targets.items()
    }
    for options in targets.values():
        options["isolation_level"] = level.lower()
    connections[alias].close()
    try:
        yield
    finally:
        for key, options in targets.items():
            if saved[key] is missing:
                options.pop("isolation_level", None)
            else:
                options["isolation_level"] = saved[key]
        connections[alias].close()


def refused_rows_ahead(count):
    """
    `count` READY plain tasks the claim's gate refuses, each held by an
    unsatisfied edge from a FAILED node, enqueued before anything else.
    """
    blocker = workflows.create("refused-ahead")
    upstream = blocker.add("upstream", tasks.step, "upstream")
    OxTask.objects.filter(pk=upstream.id).update(
        status=FAILED, finished_at=timezone.now()
    )
    with transaction.atomic():
        refused = [tasks.plain_echo.enqueue(f"refused-{i}").id for i in range(count)]
    now = timezone.now()
    OxpullWorkflowEdge.objects.bulk_create(
        [
            OxpullWorkflowEdge(
                workflow_id=blocker.id,
                upstream_task_id=upstream.id,
                downstream_task_id=task_id,
                satisfied=False,
                created_at=now,
            )
            for task_id in refused
        ],
        batch_size=500,
    )


class Aim:
    """
    How far into the claimer's scan a root's COMMIT is aimed, and what each
    round's verdict does to the next round.

    hit: the claim could not flag the root. It committed after the view of the
    scan that claimed it opened, and before that scan reached it.
    early: the signalled scan claimed the root and flagged it exactly, so the
    COMMIT was in before the scan's view opened. The next COMMIT waits longer.
    late: a later scan claimed the root and flagged it exactly, so the COMMIT
    landed behind the signalled scan and between two views. The caller adds
    refused rows, which makes every scan after it longer.
    """

    # Fractions of a scan: where a COMMIT is aimed, how much later each early
    # round puts the next one, and the latest it is ever aimed. The last is
    # under 1, so however many rounds were early the wait ends inside the scan.
    START = 0.4
    STEP = 0.15
    CEILING = 0.9

    def __init__(self):
        self.later = 0.0
        self.rounds = []

    @property
    def hits(self):
        return self.rounds.count("hit")

    def delay(self, scan, commit):
        """
        Seconds to wait after the claimer's signal before starting the COMMIT.

        A COMMIT that takes `commit` seconds is aimed to land START of the way
        into a scan of `scan` seconds. What the early rounds added comes on top
        and does not shrink with `commit`: a COMMIT slower than the scan leaves
        nothing to wait for, and an early round must still move the next one.
        """
        return max(0.0, self.START * scan - commit) + self.later * scan

    def judge(self, *, flag, claimed_by, signalled):
        if flag is None:
            verdict = "hit"
        elif claimed_by == signalled:
            verdict = "early"
            self.later = min(self.later + self.STEP, self.CEILING - self.START)
        else:
            verdict = "late"
        self.rounds.append(verdict)
        return verdict


def test_an_early_round_makes_the_next_commit_wait_longer_and_a_late_one_does_not():
    aim = Aim()
    scan, commit = 0.010, 0.002
    first = aim.delay(scan, commit)
    assert first == pytest.approx(Aim.START * scan - commit)
    assert aim.delay(scan, 2 * scan) == 0.0

    assert aim.judge(flag=True, claimed_by=7, signalled=7) == "early"
    second = aim.delay(scan, commit)
    assert second == pytest.approx(first + Aim.STEP * scan)
    assert aim.delay(scan, 2 * scan) == pytest.approx(Aim.STEP * scan)

    assert aim.judge(flag=True, claimed_by=9, signalled=8) == "late"
    assert aim.judge(flag=None, claimed_by=9, signalled=9) == "hit"
    assert aim.judge(flag=None, claimed_by=11, signalled=10) == "hit"
    assert aim.delay(scan, commit) == second
    assert aim.rounds == ["early", "late", "hit", "hit"]
    assert aim.hits == 2

    for _ in range(10):
        aim.judge(flag=True, claimed_by=1, signalled=1)
    assert aim.delay(scan, 0.0) == pytest.approx(Aim.CEILING * scan)
    assert aim.delay(scan, 0.0) < scan


class RootsAddedMidScan:
    """
    One thread claims and runs whatever is runnable. Another adds roots to
    `flow`, each with one child, and commits each pair while a claim's scan is
    under way.

    A root is claimed from outside a read view only if it commits while the
    scan that claims it runs, so the claimer must never be behind the adder. One
    pair is in flight at a time: a round starts once the pair before it has
    finished and a claim has come back empty. add(root) and add(child) run in
    one transaction, and SKIP LOCKED passes over both rows until it commits. The
    claimer signals as it is about to send its next locking read, and the adder
    commits Aim.delay() after the signal. The claim's flag and the scan that
    claimed the root make the round's verdict, and the verdict aims the next
    round (Aim).

    Each of the first `forced_early` rounds holds the signalled scan back until
    the COMMIT is in. The scan's view then has the root, so the flag is exact
    and the verdict is "early".

    Every wait ends by RENDEZVOUS_SECONDS, or as soon as the other thread has
    stopped. A wait on the other thread that runs out is an error. A wait on the
    product, for a committed root to be claimed and for the pair to finish, ends
    the rounds and leaves the verdict to the caller's assertions.
    """

    def __init__(self, flow, *, hits_wanted=HITS_WANTED, forced_early=0):
        self.flow = flow
        self.hits_wanted = hits_wanted
        self.forced_early = forced_early
        self.alias = oxpull_db_alias()
        self.aim = Aim()
        self.added, self.levels, self.errors, self.log = [], [], [], []
        self.claimed, self.finished = {}, set()
        self.scan_seconds, self.commit_seconds = [], []
        self.scans, self.signalled, self.grown = 0, None, 0
        self.stalled = None
        self.armed, self.fired, self.hold, self.committed = (
            threading.Event() for _ in range(4)
        )
        self.idle, self.park, self.parked, self.stop = (
            threading.Event() for _ in range(4)
        )

    def run(self):
        """Both threads to their end. Raises what either of them raised."""
        threads = [
            threading.Thread(target=target, name=target.__name__, daemon=True)
            for target in (self.adder, self.claimer)
        ]
        for thread in threads:
            thread.start()
        # The last round starts by LOOP_SECONDS and has six waits in it.
        deadline = time.monotonic() + LOOP_SECONDS + 6 * RENDEZVOUS_SECONDS
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        self.stop.set()
        if self.errors:
            raise AssertionError(self.report()) from self.errors[0][1]
        alive = [thread.name for thread in threads if thread.is_alive()]
        assert alive == [], self.report()

    def report(self):
        def ms(seconds):
            recent = seconds[-MEDIAN_OVER:]
            return f"{1000 * statistics.median(recent):.1f}" if recent else "-"

        return (
            f"rounds={''.join(verdict[0] for verdict in self.aim.rounds) or '-'}"
            f" hits={self.aim.hits}/{self.hits_wanted} later={self.aim.later:.2f}"
            f" refused_rows_grown={self.grown} scans={self.scans}"
            f" scan_ms={ms(self.scan_seconds)} commit_ms={ms(self.commit_seconds)}"
            f" stalled={self.stalled}"
            f" errors={[(who, repr(exc)) for who, exc in self.errors]}"
            f" last_rounds={self.log[-10:]}"
        )

    def until(self, condition):
        """
        Whether `condition`, an Event or a callable, came true before the
        deadline and before either thread stopped.
        """
        event = condition if isinstance(condition, threading.Event) else None
        holds = condition if event is None else event.is_set
        deadline = time.monotonic() + RENDEZVOUS_SECONDS
        while not holds():
            if self.stop.is_set() or time.monotonic() > deadline:
                return holds()
            if event is None:
                time.sleep(0.001)
            else:
                event.wait(0.05)
        return True

    def rendezvous(self, condition, what):
        if not self.until(condition):
            why = (
                "the other thread stopped"
                if self.stop.is_set()
                else f"waited {RENDEZVOUS_SECONDS} s"
            )
            raise AssertionError(f"no {what}: {why}")

    def on_scan(self, execute, sql, params, many, context):
        if "SKIP LOCKED" not in sql:
            return execute(sql, params, many, context)
        self.scans += 1
        if self.armed.is_set():
            self.armed.clear()
            self.signalled = self.scans
            self.fired.set()
            if self.hold.is_set():
                self.rendezvous(self.committed, "COMMIT for the held scan")
        started = time.monotonic()
        try:
            return execute(sql, params, many, context)
        finally:
            self.scan_seconds.append(time.monotonic() - started)

    def claimer(self):
        worker = flow_worker()
        try:
            with connections[self.alias].cursor() as cursor:
                cursor.execute("SELECT @@SESSION.transaction_isolation")
                self.levels.append(cursor.fetchone()[0])
            with connections[self.alias].execute_wrapper(self.on_scan):
                while not self.stop.is_set():
                    if self.park.is_set():
                        self.parked.set()
                        self.rendezvous(
                            lambda: not self.park.is_set(), "end to the adder's park"
                        )
                        self.parked.clear()
                        continue
                    task = worker.claim_one()
                    if task is None:
                        self.idle.set()
                        time.sleep(0.002)
                        continue
                    self.claimed[str(task.pk)] = (getattr(task, NODE_FLAG), self.scans)
                    worker.execute(task)
                    self.finished.add(str(task.pk))
        except BaseException as exc:
            self.errors.append(("claimer", exc))
            self.stop.set()
        finally:
            connections.close_all()

    def adder(self):
        deadline = time.monotonic() + LOOP_SECONDS
        try:
            handle = workflows.get(self.flow.id)
            self.rendezvous(
                lambda: len(self.scan_seconds) >= MEDIAN_OVER, "scans to time"
            )
            for number in range(MAX_ROUNDS):
                if self.aim.hits >= self.hits_wanted and number >= self.forced_early:
                    break
                if time.monotonic() > deadline:
                    raise AssertionError(f"no round may start after {LOOP_SECONDS} s")
                if not self.round(handle, number, hold=number < self.forced_early):
                    break
        except BaseException as exc:
            self.errors.append(("adder", exc))
        finally:
            self.stop.set()
            connections.close_all()

    def grow(self):
        """
        REFUSED_GROWTH more refused rows, added while the claimer stands still.

        Only the gate holds a refused row back, and the gate reads its edge from
        the scan's view. A scan under way when the rows commit would find one of
        them READY with no edge in its view, and claim it.
        """
        self.park.set()
        try:
            self.rendezvous(self.parked, "claimer standing still")
            refused_rows_ahead(REFUSED_GROWTH)
        finally:
            self.park.clear()
        self.grown += REFUSED_GROWTH

    def round(self, handle, number, *, hold):
        """One pair, added, committed and finished. False where it did not finish."""
        self.idle.clear()
        self.rendezvous(self.idle, "claim that came back empty")
        # Until one COMMIT has been timed there is nothing to aim with, and the
        # first one starts at the signal.
        delay = (
            0.0
            if hold or not self.commit_seconds
            else self.aim.delay(
                statistics.median(self.scan_seconds[-MEDIAN_OVER:]),
                statistics.median(self.commit_seconds[-MEDIAN_OVER:]),
            )
        )
        self.committed.clear()
        if hold:
            self.hold.set()
        else:
            self.hold.clear()
        landed = []
        with transaction.atomic(using=self.alias):
            # Registered ahead of add()'s own callbacks, so it runs as soon as
            # COMMIT returns and times nothing but the COMMIT.
            transaction.on_commit(
                lambda: landed.append(time.monotonic()), using=self.alias
            )
            root = handle.add(f"root{number}", tasks.step, f"root{number}")
            kid = handle.add(
                f"kid{number}", tasks.step, f"kid{number}", depends_on=f"root{number}"
            )
            self.fired.clear()
            self.armed.set()
            self.rendezvous(self.fired, "scan sent by the claimer")
            time.sleep(delay)
            started = time.monotonic()
        self.commit_seconds.append(landed[0] - started)
        self.committed.set()
        pair = (str(root.id), str(kid.id))
        self.added.append(pair)
        if not self.until(lambda: pair[0] in self.claimed):
            self.stalled = f"round {number}: the root was not claimed"
            return False
        flag, claimed_by = self.claimed[pair[0]]
        verdict = self.aim.judge(
            flag=flag, claimed_by=claimed_by, signalled=self.signalled
        )
        self.log.append(
            f"{number}:{verdict} flag={flag}"
            f" scans_behind={claimed_by - self.signalled}"
            f" delay_ms={1000 * delay:.1f}"
            f" commit_ms={1000 * self.commit_seconds[-1]:.1f}"
        )
        if not self.until(lambda: set(pair) <= self.finished):
            self.stalled = f"round {number}: the pair did not finish"
            return False
        if verdict == "late":
            self.grow()
            seen = len(self.scan_seconds)
            self.rendezvous(
                lambda: len(self.scan_seconds) >= seen + MEDIAN_OVER,
                "scans of the new length",
            )
        return True


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ"])
def test_on_mysql_roots_added_while_claims_scan_past_refused_rows_record_their_outcomes(
    level,
):
    """
    The view opening as it does in production. Every claim scans past
    REFUSED_AHEAD rows its gate refuses, and the gate on the first one opens the
    view. Another thread adds roots, each with one child, and commits each pair
    part-way through a scan (RootsAddedMidScan), until HITS_WANTED roots have
    been claimed from outside their claim's view. Every root must still record
    its outcome and release its child.
    """
    mysql_only_read_view()
    with every_connection_at(level):
        refused_rows_ahead(REFUSED_AHEAD)
        flow = workflows.create("added-while-claims-scan")
        loop = RootsAddedMidScan(flow)
        loop.run()

    added, report = loop.added, loop.report()
    assert loop.levels == [level.replace(" ", "-")]
    roots = [root for root, _ in added]
    records = {
        str(task_id): outcome
        for task_id, outcome in OxpullWorkflowNode.objects.filter(
            workflow_id=flow.id
        ).values_list("task_id", "outcome")
    }
    status = {
        str(pk): value
        for pk, value in OxTask.objects.filter(
            pk__in=[task_id for pair in added for task_id in pair]
        ).values_list("pk", "status")
    }
    unrecorded = [
        root for root in roots if status[root] == SUCCESSFUL and records[root] is None
    ]
    stranded = [
        kid
        for root, kid in added
        if status[root] == SUCCESSFUL and status[kid] == WAITING
    ]
    assert unrecorded == [], report
    assert stranded == [], report
    assert set(status.values()) == {SUCCESSFUL}, report
    # The scans were as long as the refused rows make them only if none of those
    # rows was claimed.
    assert set(loop.claimed) == {task_id for pair in added for task_id in pair}, report
    unseen = [root for root in roots if loop.claimed[root][0] is None]
    assert len(unseen) >= HITS_WANTED, (
        f"{len(unseen)} roots were claimed from outside their claim's read view, "
        f"and {HITS_WANTED} are wanted, so too little was provoked: {report}"
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ"])
def test_on_mysql_a_root_committed_before_its_claims_scan_is_sent_reads_as_early(level):
    """
    What separates a hit from a miss. A root that is in its claim's view is
    flagged exactly, so a flag of None is not what every claim carries, and the
    round it ends makes the next COMMIT wait longer.
    """
    mysql_only_read_view()
    with every_connection_at(level):
        refused_rows_ahead(50)
        flow = workflows.create("committed-before-the-scan")
        loop = RootsAddedMidScan(flow, hits_wanted=0, forced_early=2)
        loop.run()

    report = loop.report()
    assert loop.aim.rounds == ["early", "early"], report
    assert [loop.claimed[root][0] for root, _ in loop.added] == [True, True], report
    assert loop.aim.later == pytest.approx(2 * Aim.STEP), report
    assert loop.stalled is None, report


# -- the release ----------------------------------------------------------------


def skip_queue(workflow_id):
    row = OxpullWorkflowScan.objects.filter(workflow_id=workflow_id).first()
    return [] if row is None else list(row.skipped)


def workflow_of(task_id):
    return OxpullWorkflowNode.objects.get(task_id=task_id).workflow_id


@pytest.mark.django_db(transaction=True)
def test_an_owned_release_decides_and_moves_each_chunk_inside_one_transaction(
    monkeypatch,
):
    monkeypatch.setattr(workflows, "ID_CHUNK_SIZE", 1)
    ids = children_ready_to_release("one-transaction", 2)
    inside = []

    def wrapper(execute, sql, params, many, context):
        if quoted(TASK_TABLE) in sql or quoted(EDGE_TABLE) in sql:
            inside.append((connection.in_atomic_block, sql))
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        assert workflows.release_downstream(ids, oxpull_db_alias()) == 2
    assert inside
    assert [sql for atomic, sql in inside if not atomic] == []
    assert statuses(ids) == [READY, READY]


@pytest.mark.django_db(transaction=True)
def test_an_owned_release_bounds_its_lock_waits_and_statements():
    if connection.vendor == "sqlite":
        pytest.skip("SQLite's only bound is the connection's busy timeout")
    ids = children_ready_to_release("release-bounds", 1)
    if connection.vendor == "postgresql":

        def probe(cursor):
            cursor.execute("SHOW lock_timeout")
            lock = cursor.fetchone()[0]
            cursor.execute("SHOW statement_timeout")
            return lock, cursor.fetchone()[0]

        expected, reset = ("1s", "2s"), ("0", "0")
    else:
        with connection.cursor() as cursor:
            cursor.execute(
                "SET SESSION innodb_lock_wait_timeout = 37, max_execution_time = 0"
            )

        def probe(cursor):
            cursor.execute(
                "SELECT @@SESSION.innodb_lock_wait_timeout, "
                "@@SESSION.max_execution_time"
            )
            lock, statement = cursor.fetchone()
            return int(lock), int(statement)

        expected, reset = (1, 2000), (37, 0)
    try:
        with probing_at(updates_of(TASK_TABLE), probe) as during:
            assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
        with connection.cursor() as cursor:
            after = probe(cursor)
    finally:
        connection.close()
    assert during == [expected]
    assert after == reset


@pytest.mark.django_db(transaction=True)
def test_a_release_inside_a_callers_transaction_sets_no_bounds():
    ids = children_ready_to_release("caller-no-bounds", 1)
    with statements() as (log, _), transaction.atomic():
        assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
    assert [sql for sql, _ in log if "set_config" in sql or "SET SESSION" in sql] == []


@pytest.mark.django_db(transaction=True)
def test_a_release_behind_a_held_row_gives_up_at_its_bound_and_queues_the_row(caplog):
    """
    Another transaction holds a waiting node's task row. The release waits for
    it no longer than its bound, twice, then leaves the row in its workflow's
    skip queue. The reconciler releases it from there once the row is free.
    """
    if connection.vendor not in ("postgresql", "mysql"):
        pytest.skip("SQLite has no row locks")
    ids = children_ready_to_release("held-row", 1)
    held, done = threading.Event(), threading.Event()
    box = {}

    def hold():
        try:
            with transaction.atomic():
                list(
                    OxTask.objects.select_for_update()
                    .filter(pk=ids[0])
                    .values_list("pk", flat=True)
                )
                held.set()
                done.wait(15)
        except BaseException as exc:
            box["error"] = exc
        finally:
            held.set()
            connections.close_all()

    holder = threading.Thread(target=hold)
    holder.start()
    assert held.wait(30)
    started = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="oxpull"):
        released = workflows.release_downstream(ids, oxpull_db_alias())
    elapsed = time.monotonic() - started
    done.set()
    holder.join(30)

    assert box == {}
    assert elapsed < 8, f"the release waited {elapsed:.1f}s behind the held row"
    assert released == 0
    assert len(events(caplog, "workflow_release_deferred")) == 1
    workflow_id = workflow_of(ids[0])
    assert skip_queue(workflow_id) == [str(ids[0])]
    reconcile()
    assert statuses(ids) == [READY]
    assert skip_queue(workflow_id) == []


@pytest.mark.django_db(transaction=True)
def test_a_release_chunk_that_loses_twice_leaves_its_rows_in_the_skip_queue(
    monkeypatch, caplog
):
    monkeypatch.setattr(workflows, "ID_CHUNK_SIZE", 1)
    ids = children_ready_to_release("lost-twice-queued", 2)
    conflicting_release_many(monkeypatch, {ids[0]}, conflicts=2)
    with caplog.at_level(logging.WARNING, logger="oxpull"):
        assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
    assert statuses(ids) == [WAITING, READY]
    assert len(events(caplog, "workflow_release_deferred")) == 1
    assert skip_queue(workflow_of(ids[0])) == [str(ids[0])]


def conflict_with_code(code):
    """An OperationalError carrying one engine's code for a lost lock conflict."""
    if connection.vendor == "mysql":
        return OperationalError(code, "simulated")
    cause = SimulatedDriverError("simulated")
    if connection.vendor == "postgresql":
        cause.sqlstate = code
    else:
        cause.sqlite_errorcode = code
    error = OperationalError("simulated")
    error.__cause__ = cause
    return error


CONFLICT_CODES = {
    "postgresql": ["40001", "40P01", "55P03", "57014"],
    "mysql": [1205, 1213, 3024, 1969],
    "sqlite": [5, 5 | (2 << 8)],
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("code", CONFLICT_CODES[connection.vendor], ids=str)
def test_every_lock_conflict_and_timeout_is_decided_again_once(monkeypatch, code):
    ids = children_ready_to_release(f"code-{code}", 1)
    calls = conflicting_release_many(
        monkeypatch, set(ids), conflicts=1, error=lambda: conflict_with_code(code)
    )
    assert workflows.release_downstream(ids, oxpull_db_alias()) == 1
    assert calls == [set(ids), set(ids)]
    assert statuses(ids) == [READY]


def nodes_without_rows(workflow, count):
    """`count` node rows on `workflow` whose task ids name no task row."""
    now = timezone.now()
    task_ids = [uuid.uuid4() for _ in range(count)]
    OxpullWorkflowNode.objects.bulk_create(
        [
            OxpullWorkflowNode(
                workflow_id=workflow.id,
                name=f"n{index}",
                name_key=workflows.name_key(f"n{index}"),
                task_id=task_id,
                created_at=now,
            )
            for index, task_id in enumerate(task_ids)
        ]
    )
    return task_ids


def test_the_skip_queue_keeps_each_node_once_oldest_first_up_to_its_limit(caplog):
    flow = workflows.create("queue-limit")
    limit = workflows.SKIP_QUEUE_LIMIT
    task_ids = nodes_without_rows(flow, limit + 50)
    alias = oxpull_db_alias()
    first = sorted(task_ids[:10])

    workflows.queue_skipped(first, alias)
    workflows.queue_skipped([*first, uuid.uuid4()], alias)
    assert skip_queue(flow.id) == [str(task_id) for task_id in first]

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        workflows.queue_skipped(task_ids, alias)
    queued = skip_queue(flow.id)
    assert len(queued) == limit
    assert len(set(queued)) == limit
    assert queued[:10] == [str(task_id) for task_id in first]
    (overflow,) = events(caplog, "workflow_release_skip_overflow")
    assert overflow.dropped_count == 50


@pytest.mark.django_db(transaction=True)
def test_the_reconciler_retries_the_skip_queue_before_its_pass_over_waiting_nodes(
    monkeypatch,
):
    ids = children_ready_to_release("queue-first", 3)
    alias = oxpull_db_alias()
    workflows.queue_skipped(ids[1:], alias)
    calls = []
    real = workflows.release_downstream

    def recording(task_ids, db_alias):
        calls.append(sorted(uuid.UUID(str(task_id)) for task_id in task_ids))
        return real(task_ids, db_alias)

    monkeypatch.setattr(workflows, "release_downstream", recording)
    reconcile()
    assert calls[0] == ids[1:]
    assert statuses(ids) == [READY, READY, READY]
    assert skip_queue(workflow_of(ids[0])) == []


@pytest.mark.django_db(transaction=True)
def test_a_worker_marks_edges_only_inside_its_outcome_transaction():
    """
    On a worker's path, every edge mark is an UPDATE inside the outcome's own
    transaction, after the task row's outcome and before that transaction's
    commit. After the commit the worker writes no edge row: the release moves
    task rows only. So the mark is covered by the outcome's second try after a
    lock conflict (the next test), and no mark on this path runs outside it.
    """
    flow = workflows.create("marks-inside-the-outcome")
    parent = flow.add("parent", tasks.step, "parent")
    children = [
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="parent") for i in range(3)
    ]
    marker = "-- commit --"
    real_commit = connection.commit

    def commit():
        log.append((marker, None))
        return real_commit()

    connection.commit = commit
    try:
        with statements() as (log, _):
            assert flow_worker().run_once() is True
    finally:
        del connection.commit

    sql = [text for text, _ in log]
    edge_updates = [
        at for at, text in enumerate(sql) if updates_of(EDGE_TABLE)(text, None)
    ]
    (outcome,) = [
        at for at, entry in enumerate(log) if outcome_writes([entry], SUCCESSFUL)
    ]
    commit_after = next(
        at for at, text in enumerate(sql) if text == marker and at > outcome
    )
    assert edge_updates, sql
    assert outcome < edge_updates[0] and edge_updates[-1] < commit_after, sql
    assert all(edge_marks(child) == [True] for child in children)
    assert record_of(parent)[0] == SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_a_node_outcome_whose_edge_mark_loses_a_lock_conflict_runs_again(caplog):
    """
    The mark itself loses the conflict, as a serialization failure at PostgreSQL
    REPEATABLE READ would make it. Simulated: no writer in this package updates
    an edge row while an outcome holds it, because every other writer marks an
    edge only once it sees the upstream SUCCESSFUL, which this outcome's commit
    is the first to show.
    """
    _, parent, child = chain("mark-conflict-once")
    with (
        caplog.at_level(logging.INFO, logger="oxpull"),
        statements(updates_of(EDGE_TABLE), error=lock_conflict, times=1) as (log, seen),
    ):
        assert flow_worker().run_once() is True

    assert len(seen) == 2
    assert len(outcome_writes(log, SUCCESSFUL)) == 2, "the fence was not written again"
    assert record_of(parent) == (SUCCESSFUL, epoch_of(parent), True)
    assert edge_marks(child) == [True]
    assert status_of(child) == READY
    assert len(events(caplog, "workflow_outcome_retry")) == 1
