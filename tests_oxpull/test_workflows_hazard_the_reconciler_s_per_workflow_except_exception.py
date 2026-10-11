"""
The reconcilers' per-item `except Exception` swallows TaskTimeout.

django-ox raises TaskTimeout inside a sync task when its TASK_TIMEOUT
expires, and records that attempt as failed. It documents one exception:
a task that swallows the TaskTimeout "has its attempt recorded as whatever
it goes on to do", and one still running TASK_TIMEOUT_GRACE later is
recorded failed by the backstop, which then recycles the worker.

`workflows.reconcile` and `batches.reconcile` are ordinary tasks on the
default queue, so whichever worker claims one applies its backend's
TASK_TIMEOUT. Both sweeps wrap each item in `except Exception`, and
TaskTimeout subclasses TimeoutError. A sweep past its deadline therefore
catches its own timeout, logs "could not be reconciled", and carries on.

The timeout does not always arrive bare. A delivery that lands inside a
database driver comes out of it as the driver's error, chained from the
TaskTimeout (django-ox's worker._timeout_in follows that chain for
exactly this reason). The connection it leaves behind can be unusable,
and the swallowed sweep goes on using it.

The fix lets the timeout out wherever it sits in that chain. It also
stops a tick between items when the next item would probably not finish
in time, and visits items in a new order each tick, so a tick that ends
early cannot leave the same items behind every time. Tests 8 and 9 switch
the stop off so the timeout still lands mid-sweep; the tests at the end
of the file cover the stop and the order.

Every test here asserts what django-ox promises for a timed-out attempt.
Each worker test built on a controlled delay first runs a plain busy
task through an identically configured worker and requires django-ox to
record that attempt as a timeout without recycling. A failing result
therefore cannot come from the timeout injection not working.
"""

import logging
import math
import random
import sys
import threading
import time
from contextlib import suppress
from datetime import timedelta
from inspect import iscoroutinefunction

import pytest
from django.db import Error, OperationalError, connection, connections
from django.tasks import task
from django.utils import timezone

from django_ox import actions
from django_ox import worker as worker_module
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import batches, workflows
from oxpull.models import OxpullBatch, OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker

TIMEOUT_PATH = "django_ox.exceptions.TaskTimeout"
VENDOR = connection.vendor


def _busy(seconds):
    """Pure Python, so an injected TaskTimeout lands inside it."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass


@task
def busy(seconds):
    _busy(seconds)
    return "done"


def default_worker(**kwargs):
    return Worker(backoff_initial=0, poll_interval=0.01, **kwargs)


def _chain(exc):
    """The exceptions Python's traceback would print, outermost first."""
    found, seen = [], set()
    while exc is not None and id(exc) not in seen:
        found.append(exc)
        seen.add(id(exc))
        if exc.__cause__ is not None:
            exc = exc.__cause__
        elif exc.__suppress_context__:
            exc = None
        else:
            exc = exc.__context__
    return found


def _timeout_in_chain(exc):
    return any(isinstance(link, TaskTimeout) for link in _chain(exc))


def _chain_names(exc):
    return "<-".join(type(link).__name__ for link in _chain(exc))


def _swallowed_timeouts(records, event):
    return [
        r
        for r in records
        if getattr(r, "event", None) == event
        and r.exc_info
        and _timeout_in_chain(r.exc_info[1])
    ]


def _errors(row):
    return [e["exception_class_path"] for e in row.errors]


def _reset_connection():
    """
    What django-ox's Worker._discard_connections does after a timeout, so
    the test can read the row back. It only matters when the attempt left
    this thread's connection unusable, which is part of what is under test.
    """
    for conn in connections.all(initialized_only=True):
        conn.in_atomic_block = False
        conn.savepoint_ids = []
        conn.atomic_blocks = []
        conn.needs_rollback = False
        conn.closed_in_transaction = False
        with suppress(Error):
            conn.close()


class _EndIfWedged:
    """
    Turns a wedged attempt into a failure of this test instead of a hang.

    Seen once while probing this hazard on MySQL: after a swallowed
    delivery, the sweep blocked for ten minutes in PyMySQL's rollback()
    reading an OK packet that never came, its server session idle. After
    `after` seconds this ends the database's other sessions from the
    server side. It never fires on an attempt that finishes.
    """

    def __init__(self, after):
        self.after = after
        self.fired = False
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._done.set()
        self._thread.join(60)
        return False

    def _watch(self):
        if self._done.wait(self.after):
            return
        self.fired = True
        conn = connections["default"]
        try:
            with conn.cursor() as cursor:
                if conn.vendor == "mysql":
                    cursor.execute(
                        "SELECT id FROM information_schema.processlist "
                        "WHERE db = DATABASE() AND id <> CONNECTION_ID()"
                    )
                    for (pid,) in cursor.fetchall():
                        with suppress(Error):
                            cursor.execute(f"KILL {int(pid)}")
                elif conn.vendor == "postgresql":
                    cursor.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = current_database() "
                        "AND pid <> pg_backend_pid()"
                    )
        finally:
            connections.close_all()


def watching_monitoring_tools():
    """
    Registered sys.monitoring tool ids with events enabled, by id. A
    reserved id with no events runs no callback, so it watches nothing.
    """
    return {
        tool_id: name
        for tool_id in range(6)
        if (name := sys.monitoring.get_tool(tool_id)) is not None
        and sys.monitoring.get_events(tool_id)
    }


@pytest.fixture
def interruptible_attempts(monkeypatch):
    """
    Run each attempt on a thread the worker will raise TaskTimeout inside.

    django-ox leaves a watched thread alone on purpose. An exception raised
    between two bytecodes can land inside a tracing tool's own callback, so
    the worker declines to inject one and the grace backstop becomes the
    whole enforcement. Coverage measurement is such a tool. The sqlite job
    runs `pytest -q --cov`, so without this every test below would lose the
    interruption it is about and read red on its own control.

    The trace hook is per thread, so it comes off for the length of the
    attempt and goes back afterwards, and the attempt then runs unwatched
    for real. A sys.monitoring tool cannot be taken off one thread, so a
    session measured that way stubs the worker's probe instead. Every other
    thread, and the rest of the test, is measured as usual.

    django-ox carries the same fixture, for the same reason, over the same
    private names. The `_call_task` monkeypatch runs on every session, so a
    rename there raises and these tests say so. The `_active_tracer` one is
    reached only on a sys.monitoring session, so a rename of that name is
    caught only there, not under a sys.settrace session.
    """
    original = Worker._call_task

    def call_task(self, task, db_task, task_result, timeout):
        if iscoroutinefunction(task.func):
            return original(self, task, db_task, task_result, timeout)
        installed = sys.gettrace()
        sys.settrace(None)
        try:
            return original(self, task, db_task, task_result, timeout)
        finally:
            sys.settrace(installed)

    monkeypatch.setattr(Worker, "_call_task", call_task)
    if watching_monitoring_tools():
        monkeypatch.setattr(worker_module, "_active_tracer", lambda: None)


def require_timeouts_enforced(make_worker, timeout, grace):
    """
    The control. A plain task that would run past timeout + grace, on a
    worker built exactly like the one under test, is stopped by
    TaskTimeout: recorded with that error, requeued, and no recycle.
    """
    seconds = timeout + min(grace, 1.0) + 0.7
    result = busy.enqueue(seconds)
    worker = make_worker(task_timeout=timeout, task_timeout_grace=grace)
    started = time.monotonic()
    assert worker.run_once()
    elapsed = time.monotonic() - started
    row = OxTask.objects.get(id=result.id)
    control = (
        f"control: status={row.status} errors={_errors(row)} "
        f"recycling={worker.recycling} elapsed={elapsed:.2f}s"
    )
    print(control)  # noqa: T201
    assert _errors(row)[-1:] == [TIMEOUT_PATH], control
    assert row.status == OxTask.Status.READY, control
    assert not worker.recycling, control
    assert elapsed < seconds, control
    # Out of the way, so the next claim takes the reconciler's row.
    assert actions.discard(result.id)


def sealed_finished_workflows(count):
    flows = []
    for i in range(count):
        flow = workflows.create(f"finished-{i}")
        flow.add("n", tasks.step, f"n{i}")
        flow.seal()
        flows.append(flow)
    assert drain(flow_worker()) == count
    return flows


def workflow_statuses(flows):
    return [OxpullWorkflow.objects.get(pk=f.id).status for f in flows]


# -- the sweep itself --------------------------------------------------


@pytest.mark.django_db
def test_the_workflow_sweep_lets_a_task_timeout_out(monkeypatch):
    flow = workflows.create("unit")
    flow.add("n", tasks.step, "n")
    flow.seal()

    def timed_out(workflow_pk, db_alias):
        raise TaskTimeout

    monkeypatch.setattr(workflows, "_cancel_unreachable", timed_out)
    with pytest.raises(TaskTimeout):
        workflows._reconcile()


@pytest.mark.django_db
def test_the_batch_sweep_lets_a_task_timeout_out(monkeypatch):
    batch = batches.create("unit")
    batch.add(tasks.plain_echo, 1)
    batch.seal()

    def timed_out(batch_pk, db_alias):
        raise TaskTimeout

    monkeypatch.setattr(batches, "_complete_if_finished", timed_out)
    with pytest.raises(TaskTimeout):
        batches._reconcile()


def _lost_connection_raised_by_a_timeout(*args):
    """
    The shape a delivery inside PyMySQL's socket read takes, as measured:
    PyMySQL catches OSError around the read, TaskTimeout is a
    TimeoutError, so the driver closes the socket and raises
    OperationalError(2013) while handling it; Django re-raises that from
    the driver's exception. On MySQL, 266 of 300 unplaced deliveries
    reached the sweep this way and 6 arrived bare.
    """
    try:
        raise TaskTimeout
    except TaskTimeout:
        try:
            raise OSError(2013, "Lost connection to MySQL server during query")
        except OSError as driver_error:
            raise OperationalError(*driver_error.args) from driver_error


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("module", "item_step"),
    [(workflows, "_cancel_unreachable"), (batches, "_complete_if_finished")],
    ids=["workflows", "batches"],
)
def test_a_driver_error_raised_by_a_timeout_is_let_out_too(
    monkeypatch, module, item_step
):
    if module is workflows:
        flow = workflows.create("unit")
        flow.add("n", tasks.step, "n")
        flow.seal()
    else:
        batch = batches.create("unit")
        batch.add(tasks.plain_echo, 1)
        batch.seal()
    monkeypatch.setattr(module, item_step, _lost_connection_raised_by_a_timeout)
    with pytest.raises((OperationalError, TaskTimeout)) as raised:
        module._reconcile()
    assert _timeout_in_chain(raised.value), _chain_names(raised.value)


# -- (a) a timed-out sweep recorded SUCCESSFUL -------------------------


@pytest.mark.django_db(transaction=True)
def test_a_workflow_reconcile_past_its_timeout_is_recorded_as_timed_out(
    monkeypatch, caplog, interruptible_attempts
):
    flows = sealed_finished_workflows(2)
    timeout, grace = 0.3, 30
    require_timeouts_enforced(flow_worker, timeout, grace)

    original = workflows._cancel_unreachable
    calls = []

    def slow_first(workflow_pk, db_alias):
        calls.append(workflow_pk)
        if len(calls) == 1:
            _busy(1.5)
        return original(workflow_pk, db_alias)

    monkeypatch.setattr(workflows, "_cancel_unreachable", slow_first)
    # The release backstop runs first in the same attempt, and its reads come
    # out of the same 0.3 s. It is not what this test is about, so it is
    # switched off: the sweep then reaches its first workflow one read after
    # the attempt starts, as the batch sweep below does, and the timeout
    # lands in the delay and not before it.
    monkeypatch.setattr(workflows, "_release_backstop", lambda db_alias: 0)
    result = workflows.reconcile.enqueue()
    worker = flow_worker(task_timeout=timeout, task_timeout_grace=grace)
    with caplog.at_level(logging.ERROR, logger="oxpull.workflows"):
        assert worker.run_once()
    row = OxTask.objects.get(id=result.id)
    swallowed = _swallowed_timeouts(caplog.records, "workflow_reconcile_failed")
    seen = (
        f"status={row.status} return_value={row.return_value!r} "
        f"errors={_errors(row)} workflows={workflow_statuses(flows)} "
        f"swallowed_timeouts={len(swallowed)} recycling={worker.recycling}"
    )
    print(seen)  # noqa: T201
    assert calls, "the sweep never reached a sealed workflow; nothing was tested"
    assert row.status != OxTask.Status.SUCCESSFUL, seen
    assert _errors(row)[-1:] == [TIMEOUT_PATH], seen


@pytest.mark.django_db(transaction=True)
def test_a_batch_reconcile_past_its_timeout_is_recorded_as_timed_out(
    monkeypatch, caplog, interruptible_attempts
):
    made = []
    for i in range(2):
        batch = batches.create(f"finished-{i}")
        batch.add(tasks.plain_echo, i)
        batch.seal()
        made.append(batch)
    assert drain(default_worker()) == 2
    timeout, grace = 0.3, 30
    require_timeouts_enforced(default_worker, timeout, grace)

    original = batches._complete_if_finished
    calls = []

    def slow_first(batch_pk, db_alias):
        calls.append(batch_pk)
        if len(calls) == 1:
            _busy(1.5)
        return original(batch_pk, db_alias)

    monkeypatch.setattr(batches, "_complete_if_finished", slow_first)
    result = batches.reconcile.enqueue()
    worker = default_worker(task_timeout=timeout, task_timeout_grace=grace)
    with caplog.at_level(logging.ERROR, logger="oxpull.batches"):
        assert worker.run_once()
    row = OxTask.objects.get(id=result.id)
    swallowed = _swallowed_timeouts(caplog.records, "batch_reconcile_failed")
    seen = (
        f"status={row.status} return_value={row.return_value!r} "
        f"errors={_errors(row)} "
        f"batches={[OxpullBatch.objects.get(pk=b.id).status for b in made]} "
        f"swallowed_timeouts={len(swallowed)} recycling={worker.recycling}"
    )
    print(seen)  # noqa: T201
    assert calls, "the sweep never reached a sealed batch; nothing was tested"
    assert row.status != OxTask.Status.SUCCESSFUL, seen
    assert _errors(row)[-1:] == [TIMEOUT_PATH], seen


# -- (b) a timed-out sweep that outlives the grace recycles the worker --


@pytest.mark.django_db(transaction=True)
def test_a_workflow_reconcile_past_its_timeout_does_not_recycle_the_worker(
    monkeypatch, interruptible_attempts
):
    flows = sealed_finished_workflows(4)
    # The two outcomes are seconds apart on any machine. A sweep that lets
    # the timeout out ends when it is delivered, 0.3 s in, with the whole
    # grace still ahead of it. One that swallows it has at least three more
    # workflows to visit at 1.5 s each, a delay that a slow machine can only
    # lengthen, so it is still running when the grace ends 3 s later.
    timeout, grace = 0.3, 3.0
    require_timeouts_enforced(flow_worker, timeout, grace)

    original = workflows._cancel_unreachable

    def slow_each(workflow_pk, db_alias):
        _busy(1.5)
        return original(workflow_pk, db_alias)

    monkeypatch.setattr(workflows, "_cancel_unreachable", slow_each)
    result = workflows.reconcile.enqueue()
    worker = flow_worker(task_timeout=timeout, task_timeout_grace=grace)
    started = time.monotonic()
    assert worker.run_once()
    elapsed = time.monotonic() - started
    row = OxTask.objects.get(id=result.id)
    seen = (
        f"status={row.status} epoch={row.lease_epoch} errors={_errors(row)} "
        f"recycling={worker.recycling} run_once={elapsed:.2f}s "
        f"workflows={workflow_statuses(flows)}"
    )
    print(seen)  # noqa: T201
    assert not worker.recycling, seen
    assert _errors(row)[-1:] == [TIMEOUT_PATH], seen


# -- a real sweep that is simply longer than its timeout -----------------


def without_the_cooperative_stop(monkeypatch):
    """
    Switch off the sweep's stop between items, so the timeout lands inside
    the sweep and the test exercises the backstop: a TaskTimeout let out
    of the per-item except, wherever it was delivered. The stop itself is
    covered at the end of this file.

    raising=False, so a tree without the stop reads red for the test's own
    reason. If the stop ever reads its deadline from somewhere else, this
    patch no longer reaches it, and the sweep test below fails on a
    SUCCESSFUL row instead of passing quietly.
    """
    monkeypatch.setattr(batches, "remaining", lambda: None, raising=False)


def dead_chains(count, prefix, depth=4):
    """
    Sealed workflows whose root an operator discarded, each with a chain
    of `depth` nodes behind it. One reconciler pass cancels the chain a
    level per transaction (depth + 1 cancel passes) and then completes
    the workflow. The shape the hazard calls a long sweep.
    """
    flows = []
    for i in range(count):
        flow = workflows.create(f"{prefix}-{i}")
        root = flow.add("n0", tasks.step, f"{prefix}{i}-0")
        for level in range(1, depth + 1):
            flow.add(
                f"n{level}",
                tasks.step,
                f"{prefix}{i}-{level}",
                depends_on=f"n{level - 1}",
            )
        flow.seal()
        assert actions.discard(root.id)
        flows.append(flow)
    return flows


@pytest.mark.django_db(transaction=True)
def test_a_real_sweep_longer_than_its_timeout_is_recorded_as_timed_out(
    monkeypatch, caplog, interruptible_attempts
):
    timeout, grace = 0.15, 30
    # Calibrate on this machine and database, so the sweep under test is
    # roughly twelve timeouts long wherever it runs.
    dead_chains(10, "calibrate")
    started = time.monotonic()
    assert workflows._reconcile() == 10
    per_workflow = (time.monotonic() - started) / 10
    count = min(max(math.ceil(12 * timeout / per_workflow), 20), 2000)
    flows = dead_chains(count, "sweep")
    require_timeouts_enforced(flow_worker, timeout, grace)
    without_the_cooperative_stop(monkeypatch)

    result = workflows.reconcile.enqueue()
    worker = flow_worker(task_timeout=timeout, task_timeout_grace=grace)
    raised = None
    started = time.monotonic()
    with (
        caplog.at_level(logging.ERROR, logger="oxpull.workflows"),
        _EndIfWedged(timeout + grace + 3) as rescue,
    ):
        try:
            worker.run_once()
        except Exception as exc:
            raised = exc
    elapsed = time.monotonic() - started
    _reset_connection()
    row = OxTask.objects.get(id=result.id)
    swallowed = _swallowed_timeouts(caplog.records, "workflow_reconcile_failed")
    statuses = workflow_statuses(flows)
    seen = (
        f"workflows={count} per_workflow={per_workflow * 1000:.2f}ms "
        f"run_once={elapsed:.2f}s raised={raised!r} wedged={rescue.fired} "
        f"status={row.status} return_value={row.return_value!r} "
        f"errors={_errors(row)} swallowed_timeouts={len(swallowed)} "
        f"swallowed_chains={sorted({_chain_names(r.exc_info[1]) for r in swallowed})} "
        f"complete={statuses.count(OxpullWorkflow.Status.COMPLETE)} "
        f"sealed={statuses.count(OxpullWorkflow.Status.SEALED)}"
    )
    print(seen)  # noqa: T201
    delivered = bool(swallowed) or TIMEOUT_PATH in _errors(row)
    assert delivered, f"the sweep finished inside its timeout; nothing tested. {seen}"
    assert raised is None, seen
    assert row.status != OxTask.Status.SUCCESSFUL, seen
    assert _errors(row)[-1:] == [TIMEOUT_PATH], seen


# -- (c) a timeout that lands in the driver ------------------------------


@pytest.mark.django_db(transaction=True)
def test_a_timeout_landing_anywhere_in_a_sweep_leaves_the_attempt_an_outcome(
    monkeypatch, caplog, interruptible_attempts
):
    """
    No placement: the watchdog's own delivery, at a TASK_TIMEOUT drawn at
    random inside a sweep of real sealed workflows. The only patch
    switches off the stop between workflows, which would otherwise end
    most trials before the delivery that this test is about.

    Each workflow holds an operator-discarded node (so every tick opens a
    cancel transaction on it) and a node deferred a month (so it never
    completes), which makes the sweep repeatable. The correct outcome of
    every trial is the one django-ox promises: run_once returns, and the
    attempt is recorded, never left RUNNING for the reaper, and never
    recycles the worker. Stops at the first trial that breaks it.
    """
    if connection.vendor == "sqlite":
        pytest.skip(
            "0 of 350 unplaced deliveries on SQLite left the connection "
            "unusable while this hazard was probed; sqlite3 runs its protocol "
            "in C, where psycopg and PyMySQL run theirs in Python."
        )
    far = timezone.now() + timedelta(days=30)
    for i in range(30):
        flow = workflows.create(f"repeatable-{i}")
        root = flow.add("a", tasks.step, f"a{i}")
        flow.add("b", tasks.step.using(run_after=far), f"b{i}")
        flow.seal()
        assert actions.discard(root.id)
    samples = []
    for _ in range(5):
        started = time.monotonic()
        assert workflows._reconcile() == 0
        samples.append(time.monotonic() - started)
    sweep = sorted(samples)[2]

    without_the_cooperative_stop(monkeypatch)
    grace = 3.0
    rng = random.Random(20260913)  # noqa: S311
    delivered = 0
    broken = []
    for trial in range(40):
        for stale in OxTask.objects.filter(
            task_path="oxpull.workflows.reconcile", status=OxTask.Status.READY
        ).values_list("id", flat=True):
            actions.discard(stale)
        caplog.clear()
        timeout = rng.uniform(0.05, 0.95) * sweep
        result = workflows.reconcile.enqueue()
        worker = flow_worker(task_timeout=timeout, task_timeout_grace=grace)
        raised = None
        with (
            caplog.at_level(logging.ERROR, logger="oxpull.workflows"),
            _EndIfWedged(timeout + grace + 3) as rescue,
        ):
            try:
                worker.run_once()
            except Exception as exc:
                raised = exc
        _reset_connection()
        row = OxTask.objects.get(id=result.id)
        not_reconciled = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "workflow_reconcile_failed"
        ]
        if _swallowed_timeouts(
            not_reconciled, "workflow_reconcile_failed"
        ) or TIMEOUT_PATH in _errors(row):
            delivered += 1
        if (
            raised is not None
            or row.status == OxTask.Status.RUNNING
            or worker.recycling
            or rescue.fired
        ):
            chains = sorted(
                {_chain_names(r.exc_info[1]) for r in not_reconciled if r.exc_info}
            )
            broken.append(
                f"trial {trial}: timeout {timeout * 1000:.1f}ms into a "
                f"{sweep * 1000:.0f}ms sweep on {connection.vendor}; run_once "
                f"raised {raised!r}; reconcile row {row.status} "
                f"errors={_errors(row)}; recycling={worker.recycling} "
                f"wedged={rescue.fired}; {len(not_reconciled)} of 30 workflows "
                f"logged 'could not be reconciled', chains {chains}"
            )
            break
    print(  # noqa: T201
        f"trials run={trial + 1} delivered={delivered} sweep={sweep * 1000:.0f}ms "
        f"broken={broken}"
    )
    assert not broken, broken[0] if broken else ""
    assert delivered, "no trial delivered a TaskTimeout; nothing was tested"


# -- the stop between items, and a new order every tick ------------------

ITEM_STEPS = pytest.mark.parametrize(
    ("module", "item_step"),
    [(workflows, "_cancel_unreachable"), (batches, "_complete_if_finished")],
    ids=["workflows", "batches"],
)


def finished_units(module, count):
    """
    The ids of `count` sealed workflows or batches whose tasks have all
    run, oldest first, so each needs only its completion.
    """
    if module is workflows:
        return [flow.id for flow in sealed_finished_workflows(count)]
    ids = []
    for i in range(count):
        batch = batches.create(f"finished-{i}")
        batch.add(tasks.plain_echo, i)
        batch.seal()
        ids.append(batch.id)
    assert drain(default_worker()) == count
    return ids


def unit_statuses(module, ids):
    model = OxpullWorkflow if module is workflows else OxpullBatch
    rows = {str(pk): status for pk, status in model.objects.values_list("pk", "status")}
    return [rows[str(pk)] for pk in ids]


def complete_status(module):
    model = OxpullWorkflow if module is workflows else OxpullBatch
    return model.Status.COMPLETE


class SteppedClock:
    """
    Stands in for the `time` module inside batches and workflows, with a
    monotonic() that moves only when the test moves it, so a tick timed on
    it spends the same time on any machine. Every other name is the real
    module's, which keeps the retry pauses in both modules sleeping.
    """

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds

    def __getattr__(self, name):
        return getattr(time, name)


def time_a_tick_on(monkeypatch, clock, budget):
    """
    Put every clock a reconciler tick reads under `clock`, with `budget`
    seconds until the task timeout, or no timeout when it is None.

    A tick reads two: remaining() for what the timeout leaves, and
    time.monotonic() for how long a step took. The sweep reads both through
    batches, for workflows as well. The release backstop, which a workflow
    tick runs first, reads both through workflows. All four names go to the
    one clock, because a tick timed partly on the real one would again
    depend on the machine.

    remaining is set with raising=False, so a tree without the stop reads
    red for the test's own reason.
    """
    deadline = None if budget is None else clock.monotonic() + budget

    def remaining():
        return None if deadline is None else deadline - clock.monotonic()

    for reader in (batches, workflows):
        monkeypatch.setattr(reader, "time", clock)
        monkeypatch.setattr(reader, "remaining", remaining, raising=False)


@pytest.mark.django_db
@ITEM_STEPS
def test_an_item_that_times_out_every_tick_does_not_hold_back_the_rest(
    monkeypatch, caplog, module, item_step
):
    """
    A tick now ends at the item it timed out on. Visited oldest first, the
    items after one that always overruns would never be reached. Here the
    oldest raises TaskTimeout on every visit, which is that item without a
    real deadline per tick, and every other item must still complete
    within a bounded number of ticks, none of them logged as failing.
    """
    ids = finished_units(module, 6)
    broken, healthy = str(ids[0]), ids[1:]
    original = getattr(module, item_step)
    visits = []

    def times_out_on_the_oldest(pk, db_alias):
        visits[-1] += 1
        if str(pk) == broken:
            raise TaskTimeout
        return original(pk, db_alias)

    monkeypatch.setattr(module, item_step, times_out_on_the_oldest)
    complete = complete_status(module)
    kind = "workflow" if module is workflows else "batch"
    ticks = timed_out = 0
    done_before = []
    with caplog.at_level(logging.ERROR, logger=module.__name__):
        while ticks < 60:
            done = unit_statuses(module, healthy).count(complete)
            if done == len(healthy):
                break
            done_before.append(done)
            visits.append(0)
            ticks += 1
            try:
                module._reconcile()
            except TaskTimeout:
                timed_out += 1
    failed = [
        r.getMessage()
        for r in caplog.records
        if getattr(r, "event", None) == f"{kind}_reconcile_failed"
    ]
    statuses = unit_statuses(module, ids)
    seen = (
        f"ticks={ticks} timed_out={timed_out} done_before_each={done_before} "
        f"visited_each={visits} failed={failed} statuses={statuses}"
    )
    print(seen)  # noqa: T201
    assert not failed, seen
    assert timed_out, f"no tick let the TaskTimeout out. {seen}"
    assert statuses[1:] == [complete] * len(healthy), seen
    assert statuses[0] != complete, seen


@pytest.mark.django_db
@ITEM_STEPS
def test_a_tick_short_of_time_stops_between_items_and_the_next_tick_finishes(
    monkeypatch, caplog, module, item_step
):
    """
    The stop, on a clock this test moves (SteppedClock): each item takes
    0.1 s of it and the tick has 1 s. The tick must stop between items with
    items left, return what it completed, and log what it did not visit. A
    tick with no deadline then completes the rest.
    """
    count = 20
    ids = finished_units(module, count)
    original = getattr(module, item_step)
    clock = SteppedClock()

    def slow(pk, db_alias):
        clock.advance(0.1)
        return original(pk, db_alias)

    monkeypatch.setattr(module, item_step, slow)
    kind = "workflow" if module is workflows else "batch"
    time_a_tick_on(monkeypatch, clock, budget=1.0)
    with caplog.at_level(logging.WARNING, logger=module.__name__):
        completed = module._reconcile()
    stops = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == f"{kind}_reconcile_stopped"
    ]
    complete = complete_status(module)
    done = unit_statuses(module, ids).count(complete)
    seen = (
        f"completed={completed} done={done} clock={clock.now:.2f}s "
        f"stops={[(r.visited_count, r.unvisited_count) for r in stops]} "
        f"left={[round(r.remaining_seconds, 3) for r in stops]}"
    )
    print(seen)  # noqa: T201
    assert len(stops) == 1, seen
    assert stops[0].remaining_seconds > 0, f"stopped after the deadline. {seen}"
    assert 1 <= completed < count, seen
    assert done == completed == stops[0].visited_count, seen
    assert stops[0].unvisited_count == count - completed, seen

    time_a_tick_on(monkeypatch, clock, budget=None)
    assert module._reconcile() == count - completed
    assert unit_statuses(module, ids).count(complete) == count


@pytest.mark.django_db(transaction=True)
def test_a_real_sweep_longer_than_its_timeout_stops_in_time_over_several_ticks(
    caplog, interruptible_attempts
):
    """
    The sweep test 8 runs, with the stop left on and nothing patched:
    about six timeouts of work, one reconcile task per tick through a
    worker with that timeout, until every workflow is complete.

    A tick may end either way the fix allows: stopped between workflows
    and SUCCESSFUL, or recorded as a timeout when one workflow overran the
    margin. It may not swallow a timeout, raise out of run_once, leave the
    row RUNNING, or recycle the worker. The first tick must be cut short,
    and at least one tick must end with the stop.
    """
    timeout, grace = 0.4, 30
    dead_chains(10, "calibrate")
    started = time.monotonic()
    assert workflows._reconcile() == 10
    per_workflow = (time.monotonic() - started) / 10
    count = min(max(math.ceil(6 * timeout / per_workflow), 20), 2000)
    chains = dead_chains(count, "ticks")
    require_timeouts_enforced(flow_worker, timeout, grace)

    outcomes, broken = [], []
    for tick in range(60):
        statuses = workflow_statuses(chains)
        if statuses.count(OxpullWorkflow.Status.COMPLETE) == count:
            break
        for stale in OxTask.objects.filter(
            task_path="oxpull.workflows.reconcile", status=OxTask.Status.READY
        ).values_list("id", flat=True):
            actions.discard(stale)
        caplog.clear()
        result = workflows.reconcile.enqueue()
        worker = flow_worker(task_timeout=timeout, task_timeout_grace=grace)
        raised = None
        with (
            caplog.at_level(logging.WARNING, logger="oxpull.workflows"),
            _EndIfWedged(timeout + grace + 3) as rescue,
        ):
            try:
                worker.run_once()
            except Exception as exc:
                raised = exc
        _reset_connection()
        row = OxTask.objects.get(id=result.id)
        swallowed = _swallowed_timeouts(caplog.records, "workflow_reconcile_failed")
        stopped = any(
            getattr(r, "event", None) == "workflow_reconcile_stopped"
            for r in caplog.records
        )
        if stopped:
            outcomes.append("stopped")
        elif TIMEOUT_PATH in _errors(row):
            outcomes.append("timed out")
        else:
            outcomes.append(str(row.status))
        if (
            raised is not None
            or row.status == OxTask.Status.RUNNING
            or worker.recycling
            or rescue.fired
            or swallowed
        ):
            broken.append(
                f"tick {tick}: run_once raised {raised!r}; row {row.status} "
                f"errors={_errors(row)}; recycling={worker.recycling} "
                f"wedged={rescue.fired}; swallowed={len(swallowed)}"
            )
            break
    statuses = workflow_statuses(chains)
    seen = (
        f"workflows={count} per_workflow={per_workflow * 1000:.2f}ms "
        f"ticks={len(outcomes)} outcomes={outcomes} broken={broken} "
        f"complete={statuses.count(OxpullWorkflow.Status.COMPLETE)}"
    )
    print(seen)  # noqa: T201
    assert not broken, seen
    assert outcomes[:1] in (["stopped"], ["timed out"]), seen
    assert statuses.count(OxpullWorkflow.Status.COMPLETE) == count, seen
    assert "stopped" in outcomes, seen
