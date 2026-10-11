"""
A workflow node's outcome, written while the server ends the worker's session.

django-ox writes an attempt's outcome once more on a new connection when the
first write finds its connection gone (Worker._write_outcome_reconnecting).
OxpullWorker writes a node's SUCCESSFUL or FAILED in a transaction of its own,
with the node's record and the marks on the edges out of it. Before 1.5.0 it
failed that transaction closed on every database error, a lost connection's
included, so django-ox never made its second write: the row stayed RUNNING,
the reaper took it back once its lease ran out, and the node's body ran again.

Every test here has the server end the worker's session for real, with
pg_terminate_backend or KILL from a second connection, at one point of the
outcome:

- before: after the worker's pre-write check, which drops only a connection a
  failed statement flagged, and before the node transaction's first statement;
- inside: after the fenced write of the task row, inside the transaction;
- committed: after the transaction committed, before the worker hears back;
- before-release: after the commit came back, before the downstream release.

The worker is a real OxpullWorker claiming and running the node, on PostgreSQL
and on MySQL. SQLite has no server session to end.
"""

import logging
import threading
import time
import warnings
from contextlib import contextmanager, suppress

import pytest
from django.db import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.tasks import task

from django_ox import actions
from django_ox.compat import task_finished
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.routing import oxpull_db_alias
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, flow_worker
from .test_workflows_hazard_a_node_that_builds_workflow_structure_is import (
    reconciler_tick,
)
from .test_workflows_outcomes import node_lock_read
from .test_workflows_waiting import events

pytestmark = pytest.mark.django_db(transaction=True)

READY = OxTask.Status.READY
RUNNING = OxTask.Status.RUNNING
WAITING = OxTask.Status.WAITING
SUCCESSFUL = OxTask.Status.SUCCESSFUL
FAILED = OxTask.Status.FAILED

TASK_TABLE = OxTask._meta.db_table

needs_a_server = pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="has the server end a session, with pg_terminate_backend or KILL",
)

# Bodies run per label, the points armed by the next body, the points armed
# now, and what each hook saw. Module state, because the worker resolves the
# task by its dotted path, and the pool thread in one test shares it.
RUNS: dict[str, int] = {}
PLAN: dict[str, int] = {}
ARMED: dict[str, int] = {}
SEEN: dict[str, list] = {}


@pytest.fixture(autouse=True)
def _reset(transactional_db):
    for state in (RUNS, PLAN, ARMED, SEEN):
        state.clear()
    yield
    for state in (RUNS, PLAN, ARMED, SEEN):
        state.clear()
    # Before the database is flushed, whatever a test left of its connection.
    forget_the_connection()


def forget_the_connection():
    """
    Drop this thread's connection for "default", whatever state a broken
    transaction left it in. Django leaves in_atomic_block set when an atomic
    block cannot create its savepoint on a dead connection, and close() then
    keeps the dead driver connection. connect() resets the rest.
    """
    conn = connections["default"]
    driver = conn.connection
    with suppress(Exception):
        if driver is not None:
            driver.close()
    # A pooled connection goes back to its pool even closed, which throws it
    # away and opens another. One it never gets back is gone from it for good,
    # and a pool of four is empty after four tests.
    pool = getattr(driver, "_pool", None)
    if pool is not None:
        with suppress(Exception):
            pool.putconn(driver)
    conn.connection = None
    conn.in_atomic_block = False
    conn.closed_in_transaction = False


# -- ending a session ------------------------------------------------------------


def raw_session_id(conn):
    """The server session of `conn`'s driver connection, asked past Django."""
    conn.ensure_connection()
    cursor = conn.connection.cursor()
    try:
        cursor.execute(
            "SELECT pg_backend_pid()"
            if conn.vendor == "postgresql"
            else "SELECT CONNECTION_ID()"
        )
        return int(cursor.fetchone()[0])
    finally:
        with suppress(Exception):
            cursor.close()


def end_session(session):
    """
    Have the server end `session` from a connection of another thread, and
    return once it has gone.
    """
    failed = []

    def run():
        try:
            other = connections["default"]
            with other.cursor() as cursor:
                if other.vendor == "postgresql":
                    cursor.execute("SELECT pg_terminate_backend(%s, 10000)", [session])
                    if not cursor.fetchone()[0]:
                        raise AssertionError(f"backend {session} did not exit")
                    return
                cursor.execute(f"KILL {int(session)}")
                deadline = time.monotonic() + 10
                while True:
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.processlist "
                        "WHERE id = %s",
                        [session],
                    )
                    if not cursor.fetchone()[0]:
                        return
                    if time.monotonic() > deadline:
                        raise AssertionError(f"connection {session} did not end")
                    time.sleep(0.02)
        except BaseException as exc:
            failed.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, name="reconnect-test-other")
    thread.start()
    thread.join(30)
    if failed:
        raise failed[0]


def end_own_session():
    """End this thread's session for "default", and return its id."""
    session = raw_session_id(connections["default"])
    end_session(session)
    return session


def fire(point):
    """End this thread's session if `point` is armed, and disarm it once."""
    left = ARMED.get(point, 0)
    if not left:
        return False
    ARMED[point] = left - 1
    SEEN.setdefault(point, []).append(end_own_session())
    return True


def settle_connection():
    """
    Drop this thread's connection if a worker pass left it unusable, as the
    next request or task of a real process would, so the test reads the rows.
    """
    connections["default"].close_if_unusable_or_obsolete()


def from_another_connection(read):
    box = {}

    def run():
        try:
            box["value"] = read()
        except BaseException as exc:
            box["error"] = exc
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, name="reconnect-test-reader")
    thread.start()
    thread.join(30)
    if "error" in box:
        raise box["error"]
    return box["value"]


# -- the node, the worker and the hooks --------------------------------------------


@task(backend="flows")
def node_body(label, *, fail=False):
    """Counts its runs, uses its connection, arms the planned points."""
    RUNS[label] = RUNS.get(label, 0) + 1
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT 1")
    ARMED.update(PLAN)
    PLAN.clear()
    if fail:
        raise ValueError(f"{label} failed")
    return label


@task(backend="flows")
def offline_body(label):
    """
    Query, then have the server end this session while the body works on
    without the database, and return with no further query. Nothing on this
    thread sees the error.
    """
    RUNS[label] = RUNS.get(label, 0) + 1
    SEEN.setdefault("offline", []).append(end_own_session())
    return label


@task(backend="flows")
def grafting_body(workflow_id, label):
    """Discovers its work, adds a child that depends on itself, then returns."""
    RUNS[label] = RUNS.get(label, 0) + 1
    flow = workflows.get(workflow_id)
    flow.add(f"{label}-child", tasks.step, f"{label}-child", depends_on=label)
    ARMED.update(PLAN)
    PLAN.clear()
    return label


class HookedWorker(OxpullWorker):
    """OxpullWorker, with the test's hooks between the steps of an outcome."""

    def _discard_unusable_connections(self):
        super()._discard_unusable_connections()
        fire("before")

    def _release_after_outcome(self, db_task, downstream):
        fire("before-release")
        super()._release_after_outcome(db_task, downstream)


def hooked_worker(**kwargs):
    return HookedWorker(
        backend_alias="flows", backoff_initial=0, poll_interval=0.01, **kwargs
    )


def is_outcome_write(sql, params):
    return (
        sql.lstrip().startswith(f"UPDATE {connection.ops.quote_name(TASK_TABLE)}")
        and isinstance(params, (list, tuple))
        and any(str(value) in (SUCCESSFUL, FAILED) for value in params)
    )


@contextmanager
def outcome_hooks():
    """
    Log this thread's outcome writes of the task row; end the session after one
    inside a transaction when "inside" is armed; and when "committed" is armed,
    let the next commit after an outcome write land on the server, end the
    session, and have the commit raise the error the dead session gives, as a
    reply lost after the commit does.
    """
    log = []
    since_armed = []
    conn = connections["default"]

    def wrapper(execute, sql, params, many, context):
        write = is_outcome_write(sql, params)
        if write:
            log.append(sql)
            if ARMED.get("committed"):
                since_armed.append(sql)
        result = execute(sql, params, many, context)
        if write and conn.in_atomic_block:
            fire("inside")
        return result

    real_commit = conn._commit

    def commit_then_lose_the_reply():
        result = real_commit()
        if since_armed and fire("committed"):
            since_armed.clear()
            cursor = conn.connection.cursor()
            with conn.wrap_database_errors:
                cursor.execute("SELECT 1")
        return result

    conn._commit = commit_then_lose_the_reply
    try:
        with conn.execute_wrapper(wrapper):
            yield log
    finally:
        del conn._commit


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


@contextmanager
def between_the_tries(monkeypatch):
    """
    What another connection sees when django-ox asks whether the row already
    holds the write, which it does only on its second try.
    """
    seen = []
    real = Worker._outcome_already_written

    def observed(self, db_task, status, fields):
        pk = db_task.pk
        seen.append(
            from_another_connection(
                lambda: {
                    "row": OxTask.objects.values_list("status", "lease_epoch").get(
                        pk=pk
                    ),
                    "record": OxpullWorkflowNode.objects.values_list(
                        "outcome", "outcome_epoch"
                    ).get(task_id=pk),
                    "marks": sorted(
                        OxpullWorkflowEdge.objects.filter(
                            upstream_task_id=pk
                        ).values_list("satisfied", flat=True)
                    ),
                }
            )
        )
        return real(self, db_task, status, fields)

    monkeypatch.setattr(Worker, "_outcome_already_written", observed)
    yield seen


def logs(caplog):
    caplog.set_level(logging.INFO)
    caplog.set_level(logging.INFO, logger="django_ox")
    caplog.set_level(logging.INFO, logger="oxpull")


def row(result):
    return OxTask.objects.get(pk=result.id)


def record_of(result):
    return OxpullWorkflowNode.objects.values_list(
        "outcome", "outcome_epoch", "succeeded"
    ).get(task_id=result.id)


def marks_into(result):
    return sorted(
        OxpullWorkflowEdge.objects.filter(downstream_task_id=result.id).values_list(
            "satisfied", flat=True
        )
    )


def a_chain(name, *, fail=False):
    """parent -> child, OPEN; the parent's failure is final on its first attempt."""
    flow = workflows.create(name)
    parent = flow.add("parent", node_body, "parent", fail=fail)
    child = flow.add("child", tasks.step, "child", depends_on="parent")
    if fail:
        OxTask.objects.filter(pk=parent.id).update(max_attempts=1)
    return flow, parent, child


def reap_what_the_lease_left():
    """
    What production does once the lease runs out: expire it and run a reaper.
    Returns whether the reaper took a row back.
    """
    return any(
        actions.expire_lease(pk)
        for pk in OxTask.objects.filter(status=RUNNING).values_list("pk", flat=True)
    ) and bool(flow_worker().reap())


# -- the reproducers ---------------------------------------------------------------

POINTS = ["before", "inside", "committed"]
STATUSES = ["SUCCESSFUL", "FAILED"]


@needs_a_server
@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("point", POINTS)
def test_a_node_outcome_whose_connection_the_server_ends_is_written_once_on_another(
    point, status, monkeypatch, caplog
):
    logs(caplog)
    fail = status == "FAILED"
    _, parent, child = a_chain(f"reconnect-{point}-{status}", fail=fail)
    PLAN[point] = 1
    with (
        between_the_tries(monkeypatch) as between,
        finished_signals() as finished,
        outcome_hooks() as writes,
    ):
        worker = hooked_worker()
        claimed = worker.claim_one()
        epoch = claimed.lease_epoch
        worker.execute(claimed)
    settle_connection()

    written = SUCCESSFUL if not fail else FAILED
    first = {
        "row": (row(parent).status, row(parent).lease_epoch, row(parent).locked_by),
        "record": record_of(parent),
        "marks": marks_into(child),
        "child": row(child).status,
        "finished": list(finished),
        "writes": len(writes),
        "reconnected": [
            getattr(r, "already_written", None)
            for r in events(caplog, "task_outcome_reconnected")
        ],
        "unrecorded": len(events(caplog, "workflow_outcome_unrecorded"))
        + len(events(caplog, "task_outcome_unrecorded")),
    }
    expected = {
        "row": (written, epoch, None),
        "record": (written, epoch, not fail),
        "marks": [not fail],
        # Before and inside, the second try's transaction commits and its
        # release runs. After a commit whose reply was lost, the second try
        # finds the row written and releases nothing: the reconciler does.
        "child": READY if (not fail and point != "committed") else WAITING,
        "finished": [str(parent.id)],
        # Before, the first try fails ahead of its write; inside, it wrote and
        # rolled back; committed, the second try writes nothing.
        "writes": {"before": 1, "inside": 2, "committed": 1}[point],
        "reconnected": [point == "committed"],
        "unrecorded": 0,
    }
    problems = [
        f"{key}: {first[key]!r}, expected {expected[key]!r}"
        for key in expected
        if first[key] != expected[key]
    ]
    if SEEN.get(point, []) == []:
        problems.append(f"the session was never ended at {point}")
    if point == "committed":
        # The first transaction committed whole before its reply was lost.
        whole = {"row": (written, epoch), "record": (written, epoch)}
    else:
        # Nothing of the first transaction is left: no partial record or mark.
        whole = {"row": (RUNNING, epoch), "record": (None, None)}
    if between != [{**whole, "marks": [point == "committed" and not fail]}]:
        problems.append(
            f"between the tries another connection saw {between}, expected one "
            f"look at {whole} with its marks"
        )

    # Production, once the lease would have run out, and the reconciler tick
    # every workflow deployment schedules.
    reaped = reap_what_the_lease_left()
    reconciler_tick()
    drain(flow_worker())
    if reaped:
        problems.append("the reaper took the row back")
    if RUNS.get("parent") != 1:
        problems.append(f"the parent's body ran {RUNS.get('parent')} time(s)")
    if row(parent).status != written:
        problems.append(f"the parent ended {row(parent).status}")
    if not fail and row(child).status != SUCCESSFUL:
        problems.append(f"the child is {row(child).status} after the reconciler")
    assert not problems, "\n".join(
        [f"disconnect {point}, {status}", *problems, f"first pass: {first}"]
    )


@needs_a_server
def test_worker_pool_body_returns_without_query_after_session_ends(caplog):
    """
    The node's session ends while its body works on without the database
    on the worker's pool thread, and the body returns with no further query.
    """
    logs(caplog)
    flow = workflows.create("pool-offline")
    node = flow.add("only", offline_body, "only")
    flow.seal()
    with finished_signals() as finished:
        OxpullWorker(
            backend_alias="flows",
            concurrency=1,
            poll_interval=0.05,
            lock_timeout=2,
            max_tasks=1,
            backoff_initial=0,
        ).run()
    first = (row(node).status, row(node).attempts, RUNS.get("only"))
    reaped = reap_what_the_lease_left()
    drain(flow_worker())
    assert SEEN.get("offline"), "the session was never ended"
    assert (first, reaped) == ((SUCCESSFUL, 1, 1), False)
    assert RUNS.get("only") == 1
    assert finished == [str(node.id)]
    assert len(events(caplog, "task_outcome_reconnected")) == 1
    assert not events(caplog, "workflow_outcome_unrecorded")


@needs_a_server
def test_a_node_that_grafts_onto_its_own_workflow_keeps_its_child_and_runs_once(
    caplog,
):
    """
    The shape the hazard test calls not re-executable: a node that adds a child
    depending on itself. Its outcome write finds the connection gone. Before the
    fix the reaper ran it again, the second add() raised, and the node ended
    FAILED with the child it had committed discarded.
    """
    logs(caplog)
    flow = workflows.create(
        "graft-reconnect",
        on_success=tasks.record_callback,
        on_success_args=["graft-success"],
    )
    root = flow.add("root", grafting_body, str(flow.id), "root")
    PLAN["before"] = 1
    assert hooked_worker().run_once() is True
    settle_connection()
    after_first = row(root).status
    reap_what_the_lease_left()
    flow.seal()
    drain(flow_worker())
    reconciler_tick()
    drain(flow_worker())
    reconciler_tick()

    nodes = flow.node_ids()
    stored = OxpullWorkflow.objects.get(pk=flow.id)
    child_runs = tasks.STATE.get("order", []).count("root-child")
    problems = []
    if not SEEN.get("before"):
        problems.append("the session was never ended")
    if after_first != SUCCESSFUL:
        problems.append(f"root was {after_first} after the worker's pass")
    if RUNS.get("root") != 1:
        problems.append(f"root's body ran {RUNS.get('root')} time(s)")
    if row(root).status != SUCCESSFUL:
        problems.append(f"root ended {row(root).status}")
    if set(nodes) != {"root", "root-child"}:
        problems.append(f"nodes {sorted(nodes)}")
    elif OxTask.objects.get(pk=nodes["root-child"]).status != SUCCESSFUL:
        problems.append(
            f"root-child is {OxTask.objects.get(pk=nodes['root-child']).status}"
        )
    if child_runs != 1:
        problems.append(f"root-child ran {child_runs} time(s)")
    if (
        stored.status != OxpullWorkflow.Status.COMPLETE
        or stored.failed_count
        or stored.cancelled_count
        or stored.success_task_id is None
    ):
        problems.append(
            f"workflow {stored.status}, failed_count={stored.failed_count}, "
            f"cancelled_count={stored.cancelled_count}, on_success "
            f"{'withheld' if stored.success_task_id is None else 'enqueued'}"
        )
    assert not problems, "\n".join(problems)


@needs_a_server
def test_a_second_try_that_loses_its_connection_too_leaves_the_row_to_the_reaper(
    caplog,
):
    logs(caplog)
    _, parent, child = a_chain("reconnect-exhausted")
    PLAN["inside"] = 2
    with finished_signals() as finished, outcome_hooks() as writes:
        worker = hooked_worker()
        claimed = worker.claim_one()
        epoch = claimed.lease_epoch
        worker.execute(claimed)
    settle_connection()

    assert len(SEEN.get("inside", [])) == 2
    assert len(writes) == 2, "the second try was not made"
    assert (row(parent).status, row(parent).lease_epoch) == (RUNNING, epoch)
    assert claimed.status == RUNNING, "the instance kept a status that never committed"
    assert record_of(parent) == (None, None, False)
    assert marks_into(child) == [False]
    assert finished == []
    assert len(events(caplog, "task_outcome_unrecorded")) == 1
    assert not events(caplog, "task_outcome_reconnected")
    assert not events(caplog, "task_succeeded")
    # The reaper takes it back once the lease runs out.
    assert actions.expire_lease(parent.id) is True
    assert flow_worker().reap() == 1
    assert row(parent).status == READY


# -- Downstream release after a lost reply ----------------------------------------


@needs_a_server
@pytest.mark.parametrize("point", ["committed", "before-release"])
def test_the_reconciler_releases_what_a_committed_outcome_held(point, caplog):
    """
    The outcome committed, and the worker's release never ran: after a reply
    lost with the commit, django-ox's second try finds the row written and runs
    no release; when the session ends after the commit came back, the release
    fails on it and is logged. The edges are marked in the committed
    transaction, and the reconciler, which every workflow deployment has to
    schedule, releases the child, from a worker that is not the one that wrote
    the outcome. The parent does not run again.
    """
    logs(caplog)
    flow, parent, child = a_chain(f"release-{point}")
    flow.add("grandchild", tasks.step, "grandchild", depends_on="child")
    PLAN[point] = 1
    with finished_signals() as finished, outcome_hooks():
        assert hooked_worker().run_once() is True
    settle_connection()
    assert SEEN.get(point), "the session was never ended"
    assert row(parent).status == SUCCESSFUL
    assert marks_into(child) == [True]
    assert row(child).status == WAITING, "the release was not skipped"
    assert finished == [str(parent.id)]

    reconciler_tick()
    assert row(child).status == READY
    drain(flow_worker())
    assert row(child).status == SUCCESSFUL
    assert OxTask.objects.get(pk=flow.node_ids()["grandchild"]).status == SUCCESSFUL
    assert RUNS.get("parent") == 1
    assert row(parent).attempts == 1


# -- what must not change -----------------------------------------------------------


def simulated(kind):
    def make():
        if kind == "OperationalError":
            return OperationalError("simulated: not a lost connection")
        if kind == "InterfaceError":
            return InterfaceError("simulated: not a lost connection")
        return DatabaseError("simulated: not a lost connection")

    return make


@pytest.mark.parametrize(
    "kind", ["OperationalError", "InterfaceError", "DatabaseError"]
)
def test_an_error_on_a_connection_that_still_answers_fails_closed(kind, caplog):
    """
    Only a lost connection goes to django-ox's second try. The same classes of
    error on a connection that answers fail closed after one try, as before.
    """
    logs(caplog)
    _, parent, child = a_chain(f"not-lost-{kind}")
    raised = []

    def wrapper(execute, sql, params, many, context):
        if not raised and node_lock_read(sql, params):
            raised.append(sql)
            raise simulated(kind)()
        return execute(sql, params, many, context)

    with (
        finished_signals() as finished,
        outcome_hooks() as writes,
        connection.execute_wrapper(wrapper),
    ):
        assert flow_worker().run_once() is True

    assert len(raised) == 1
    assert len(writes) == 1
    assert row(parent).status == RUNNING
    assert record_of(parent) == (None, None, False)
    assert marks_into(child) == [False]
    assert finished == []
    assert len(events(caplog, "workflow_outcome_unrecorded")) == 1
    assert not events(caplog, "task_outcome_reconnected")


def test_an_interface_error_in_the_release_after_the_commit_is_logged(caplog):
    """
    The release after a node's committed success logs its failure and leaves
    the nodes to the reconciler. Django's InterfaceError is not a
    DatabaseError: raised by a release statement on a connection that still
    answers, it escaped execute(), and no task_finished was sent for a row
    that was SUCCESSFUL.
    """
    logs(caplog)
    _, parent, child = a_chain("release-interface-error")
    releasing = []
    raised = []
    real = HookedWorker._release_after_outcome

    def marked(self, db_task, downstream):
        releasing.append(db_task.pk)
        try:
            real(self, db_task, downstream)
        finally:
            releasing.clear()

    def wrapper(execute, sql, params, many, context):
        if releasing and not raised:
            raised.append(sql)
            raise simulated("InterfaceError")()
        return execute(sql, params, many, context)

    escaped = []
    with (
        pytest.MonkeyPatch.context() as patch,
        finished_signals() as finished,
        connection.execute_wrapper(wrapper),
    ):
        patch.setattr(HookedWorker, "_release_after_outcome", marked)
        worker = hooked_worker()
        claimed = worker.claim_one()
        epoch = claimed.lease_epoch
        try:
            worker.execute(claimed)
        except InterfaceError as exc:
            escaped.append(repr(exc))

    first = {
        "escaped": escaped,
        "release statements failed": len(raised),
        "row": row(parent).status,
        "record": record_of(parent),
        "marks": marks_into(child),
        "child": row(child).status,
        "finished": list(finished),
        "release failed": len(events(caplog, "workflow_release_failed")),
    }
    assert first == {
        "escaped": [],
        "release statements failed": 1,
        "row": SUCCESSFUL,
        "record": (SUCCESSFUL, epoch, True),
        "marks": [True],
        "child": WAITING,
        "finished": [str(parent.id)],
        "release failed": 1,
    }

    reconciler_tick()
    assert row(child).status == READY
    drain(flow_worker())
    assert row(child).status == SUCCESSFUL
    assert RUNS.get("parent") == 1
    assert row(parent).attempts == 1


@needs_a_server
@pytest.mark.parametrize("caller", ["atomic", "autocommit-off"])
def test_inside_a_callers_transaction_a_lost_connection_reaches_the_caller(
    caller, caplog
):
    """
    A caller's transaction is theirs, whatever ended it: the error reaches them,
    and nothing writes a second time.
    """
    logs(caplog)
    _, parent, _child = a_chain(f"caller-lost-{caller}")
    worker = hooked_worker()
    claimed = worker.claim_one()
    PLAN["before"] = 1
    with outcome_hooks() as writes, pytest.raises((OperationalError, InterfaceError)):
        if caller == "atomic":
            with transaction.atomic():
                worker.execute(claimed, inline=True)
        else:
            connection.set_autocommit(False)
            worker.execute(claimed, inline=True)
    forget_the_connection()
    assert SEEN.get("before"), "the session was never ended"
    assert len(writes) <= 1
    assert row(parent).status == RUNNING
    assert not events(caplog, "task_outcome_reconnected")


@needs_a_server
def test_a_second_try_is_fenced_when_another_worker_took_the_row(caplog):
    """
    The lease ran out while the connection was going, and another worker holds
    the row at a later epoch. The second try matches nothing, and nothing is
    recorded, sent or released for an outcome this execution no longer owns.
    """
    logs(caplog)
    _, parent, child = a_chain("reconnect-fenced")
    worker = hooked_worker()
    claimed = worker.claim_one()
    other = {}

    def take_over_then_end():
        def take():
            thief = flow_worker()
            assert actions.expire_lease(parent.id) is True
            assert thief.reap() == 1
            taken = thief.claim_one()
            return (taken.pk, taken.lease_epoch, thief.worker_id)

        other["claim"] = from_another_connection(take)
        end_own_session()

    real = HookedWorker._discard_unusable_connections

    def hook(self):
        real(self)
        if RUNS.get("parent") and "claim" not in other:
            take_over_then_end()

    with (
        pytest.MonkeyPatch.context() as patch,
        finished_signals() as finished,
        outcome_hooks() as writes,
    ):
        patch.setattr(HookedWorker, "_discard_unusable_connections", hook)
        worker.execute(claimed)
    settle_connection()

    pk, epoch, thief = other["claim"]
    assert str(pk) == str(parent.id)
    assert epoch > claimed.lease_epoch
    # The first try fails ahead of its write; the second is fenced out.
    assert len(writes) == 1
    stored = row(parent)
    assert (stored.status, stored.lease_epoch, stored.locked_by) == (
        RUNNING,
        epoch,
        thief,
    )
    assert record_of(parent) == (None, None, False)
    assert marks_into(child) == [False]
    assert finished == []
    assert len(events(caplog, "task_lease_lost")) == 1
    assert not events(caplog, "task_outcome_reconnected")
    assert not events(caplog, "workflow_outcome_unrecorded")


@needs_a_server
def test_a_stuck_thread_handover_on_a_lost_connection_leaves_the_row_as_it_was():
    """
    The watchdog's record of a stuck attempt calls the outcome write directly,
    with no second try. A lost connection there is raised, which the watchdog
    logs as task_stuck_unrecorded before it recycles the worker, and the row and
    the instance are left at the epoch the execution holds.
    """
    _, parent, child = a_chain("handover-lost")
    OxTask.objects.filter(pk=parent.id).update(max_attempts=1)
    worker = flow_worker()
    claimed = worker.claim_one()
    epoch = claimed.lease_epoch
    end_own_session()
    with pytest.raises((OperationalError, InterfaceError)):
        worker._handle_failure(claimed, RuntimeError("stuck"), 0, release=True)
    forget_the_connection()
    assert claimed.lease_epoch == epoch
    assert claimed.status == RUNNING
    assert (row(parent).status, row(parent).lease_epoch) == (RUNNING, epoch)
    assert record_of(parent) == (None, None, False)
    assert marks_into(child) == [False]


# -- MySQL session bounds and the driver's liveness check -------------------------

needs_mysql = pytest.mark.skipif(
    connection.vendor != "mysql", reason="MySQL's lock wait bound is a session value"
)


def restoring(value):
    def match(sql, params):
        return sql.startswith("SET SESSION innodb_lock_wait_timeout") and list(
            params or []
        )[:1] == [value]

    return match


@contextmanager
def statements_matching(match, error=None):
    seen = []

    def wrapper(execute, sql, params, many, context):
        if match(sql, params):
            seen.append(sql)
            if error is not None:
                raise error()
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        yield seen


class TheBlocksOwnError(Exception):
    pass


@needs_mysql
@pytest.mark.parametrize("restore_error", ["InterfaceError", "OperationalError"])
def test_on_mysql_a_bound_that_cannot_be_put_back_never_replaces_the_blocks_error(
    restore_error, caplog
):
    logs(caplog)
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 37")
    error = InterfaceError if restore_error == "InterfaceError" else OperationalError
    with (
        statements_matching(restoring(37), lambda: error("restore failed")) as seen,
        pytest.raises(TheBlocksOwnError),
        workflows.session_timeouts(oxpull_db_alias(), lock_wait=5),
    ):
        raise TheBlocksOwnError
    assert len(seen) == 1
    assert connection.connection is None, "the session was not closed"
    assert len(events(caplog, "workflow_session_bounds_not_restored")) == 1


@needs_mysql
@pytest.mark.parametrize("where", ["outside a transaction", "inside a transaction"])
def test_on_mysql_nothing_is_put_back_on_a_session_the_server_ended(where, caplog):
    """
    The block's error is the lost connection's own, and it is what leaves the
    block: no statement goes to put back bounds that went with the session.
    Outside a transaction the dead connection is closed; inside one Django has
    already dropped it and opened another, which never carried the bounds.
    """
    logs(caplog)
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 37")
    with (
        statements_matching(restoring(37)) as restores,
        pytest.raises(OperationalError),
        workflows.session_timeouts(oxpull_db_alias(), lock_wait=5),
    ):
        if where == "inside a transaction":
            with transaction.atomic(), connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                end_own_session()
                cursor.execute("SELECT 1")
        else:
            end_own_session()
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
    assert restores == []
    assert not events(caplog, "workflow_session_bounds_not_restored")
    if where == "outside a transaction":
        assert connection.connection is None, "the dead connection was left open"
    connection.close()


@needs_mysql
@pytest.mark.parametrize("restore_error", ["OperationalError", "InterfaceError"])
def test_on_mysql_a_bound_that_cannot_be_put_back_after_a_clean_block_is_reported(
    restore_error, caplog
):
    """
    With no error leaving the block, a failure to put the bounds back is never
    silent: it is logged and the session closed, and one that is not a database
    error is raised as well.
    """
    logs(caplog)
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 37")
    error = InterfaceError if restore_error == "InterfaceError" else OperationalError
    raised = []
    with statements_matching(restoring(37), lambda: error("restore failed")):
        try:
            with workflows.session_timeouts(oxpull_db_alias(), lock_wait=5):
                pass
        except InterfaceError as exc:
            raised.append(exc)
    assert connection.connection is None
    assert len(events(caplog, "workflow_session_bounds_not_restored")) == 1
    assert len(raised) == (1 if restore_error == "InterfaceError" else 0)


@needs_mysql
def test_on_mysql_the_drivers_liveness_check_does_not_reconnect():
    """
    django-ox's check that a connection is lost is Django's is_usable(), which
    on MySQL is the driver's ping() with its default arguments. A ping that
    reconnected would answer for a session the server ended, and the outcome
    would never be written a second time. The last part shows these assertions
    tell a reconnecting ping apart.
    """
    driver = connection.Database
    name = f"{driver.__name__} {getattr(driver, 'VERSION_STRING', '')}".strip()
    connection.ensure_connection()
    raw = connection.connection
    ended = end_own_session()
    assert connection.is_usable() is False, f"{name}: is_usable() answered"
    assert connection.connection is raw
    assert not raw.open, f"{name}: ping() opened a new session"
    with pytest.raises(driver.Error):
        raw.ping()
    assert not raw.open
    assert flow_worker()._outcome_connection_lost() is True
    with suppress(Exception):
        connection.close()

    connection.ensure_connection()
    raw = connection.connection
    ended = end_own_session()
    with warnings.catch_warnings():
        # PyMySQL deprecates the argument; this is the one call that wants it.
        warnings.simplefilter("ignore", DeprecationWarning)
        raw.ping(reconnect=True)
    assert raw.open
    assert raw_session_id(connection) != ended
    connection.close()
