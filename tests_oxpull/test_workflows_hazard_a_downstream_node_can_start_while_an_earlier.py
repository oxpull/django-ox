"""
A downstream node starts while an earlier, fenced-off
execution of its upstream is still running.

The claim gate reads the upstream's row. django-ox 1.2.0 fences rows, not
executions: when a lease is handed over with attempts left, the row goes
back to READY with the epoch moved on while the first execution's thread
keeps going. A second execution then succeeds, the row reads SUCCESSFUL,
and the child is claimable, while the first execution is still inside the
upstream's body producing that node's side effects.

Two production routes, both through the real worker:

- test_..._while_a_requeued_upstream_execution_is_live: the reaper requeues
  a live execution. Either an operator shortens the lease with
  actions.expire_lease (documented "It does not stop the task") and another
  worker reaps, or the worker's own renewal UPDATE cannot reach the
  database for longer than LOCK_TIMEOUT and its own run() loop's reaper
  takes the row. The upstream is a truncate-and-reload; the first execution
  is slow to start its reload and the child reads the table mid-reload.

- test_..._while_a_stuck_upstream_execution_is_live: the TASK_TIMEOUT grace
  backstop. The node blocks in a C call past the deadline and the grace; the
  watchdog writes READY with the epoch moved and recycles the worker, which
  then drains a healthy sibling task while the stuck thread is known to be
  alive. Nothing is injected: the only inputs are OPTIONS and a body that
  blocks on a lock.

Each test asserts what the Workflows copy promises ("a node becomes
claimable only once every node it depends on has succeeded"): no execution
of a downstream node begins while any execution of its upstream is still
inside the upstream's body, and the child eventually runs. A fix that makes
the child wait passes; a fix that never runs the child does not.
"""

import threading
import time

import pytest
from django.conf import settings
from django.db import OperationalError, connections
from django.tasks import task
from django.test import override_settings

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, flow_worker, reconcile
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db(transaction=True)

WAIT = 30.0
ROWS = 5
FACTORIES = {"flows": flow_worker, "flows_deferred": deferring_worker}

# Shared between worker threads and the test. Module state rather than
# tasks.STATE because a worker thread resolves the node by its dotted path.
_GUARD = threading.Lock()
TABLE: list[int] = []  # what the upstream loads and the child reads
LIVE: dict[str, set[int]] = {}  # label -> executions inside the body now
CALLS: dict[str, int] = {}  # label -> executions started
SEEN: list[dict] = []  # one entry per downstream execution, taken at entry
CTL: dict = {}


@pytest.fixture(autouse=True)
def _shared_state():
    _reset()
    yield
    for key in ("before_truncate", "finish_reload", "sibling_release"):
        if key in CTL:
            CTL[key].set()
    if "c_gate" in CTL and CTL["c_gate"].locked():
        CTL["c_gate"].release()
    _reset()


def _reset():
    TABLE.clear()
    LIVE.clear()
    CALLS.clear()
    SEEN.clear()
    CTL.clear()


@task(backend="flows")
def reload_table(label):
    """
    Truncate-and-reload. Execution 1 is the slow one: in "python" mode it
    stalls before its truncate and again after the first row (a slow source
    read); in "c" mode it blocks in a C call, where TaskTimeout cannot land.
    Every later execution reloads straight through.
    """
    with _GUARD:
        n = CALLS[label] = CALLS.get(label, 0) + 1
        LIVE.setdefault(label, set()).add(n)
    try:
        if n == 1:
            CTL["exec1_started"].set()
            if CTL["mode"] == "c":
                CTL["c_gate"].acquire(timeout=WAIT)
            else:
                CTL["before_truncate"].wait(WAIT)
                TABLE.clear()
                TABLE.append(0)
                CTL["mid_reload"].set()
                CTL["finish_reload"].wait(WAIT)
                TABLE.extend(range(1, ROWS))
        else:
            TABLE.clear()
            TABLE.extend(range(ROWS))
        return tasks._record(label)
    finally:
        with _GUARD:
            LIVE[label].discard(n)


@task(backend="flows")
def read_table(label):
    """The child: reads what its upstream loaded, and notes what was live."""
    with _GUARD:
        live = {name: sorted(ns) for name, ns in LIVE.items() if ns}
    probe = CTL.get("probe")
    SEEN.append(
        {
            "label": label,
            "live": live,
            "table": list(TABLE),
            "context": probe() if probe else None,
        }
    )
    return tasks._record(label)


@task(backend="flows", queue_name="emails")
def sibling_step(label):
    """Another task in flight on the same worker; its queue has no timeout."""
    CTL["sibling_started"].set()
    CTL["sibling_release"].wait(WAIT)
    return label


class Runner:
    """A target on its own thread, the way a pool thread would run it."""

    def __init__(self, target):
        self.errors: list[BaseException] = []
        self.thread = threading.Thread(target=self._run, args=(target,), daemon=True)

    def _run(self, target):
        try:
            target()
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            connections.close_all()

    def start(self):
        self.thread.start()
        return self


def wait_until(predicate, what, timeout=WAIT):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for: {what}"
        time.sleep(0.01)


def chain(alias, name):
    flow = workflows.create(name)
    root = flow.add("root", reload_table.using(backend=alias), "root")
    child = flow.add(
        "child", read_table.using(backend=alias), "child", depends_on="root"
    )
    flow.seal()
    return flow, root, child


def row(result):
    return OxTask.objects.get(id=result.id)


def settle(factory, rounds=3):
    """Give the child every chance a fix might use to run it later."""
    for _ in range(rounds):
        reconcile()
        drain(factory())


def assert_no_child_overlapped(child):
    child_runs = [seen for seen in SEEN if seen["label"] == "child"]
    assert child_runs, "the child never ran"
    assert row(child).status == OxTask.Status.SUCCESSFUL
    for seen in child_runs:
        assert not seen["live"], (
            "the child started while execution(s) "
            f"{seen['live'].get('root')} of its upstream 'root' were still "
            f"inside root's body; the child read {seen['table']} "
            f"(a full load is {list(range(ROWS))}); context {seen['context']}"
        )
        assert seen["table"] == list(range(ROWS)), seen


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A downstream node starts while an earlier execution of its upstream, "
        "whose lease was handed to another worker, is still running the "
        "upstream's body."
    ),
)
@pytest.mark.parametrize("alias", sorted(FACTORIES))
@pytest.mark.parametrize("how", ["operator_expires_lease", "renewals_fail"])
def test_a_downstream_node_never_starts_while_a_requeued_upstream_execution_is_live(
    alias, how
):
    factory = FACTORIES[alias]
    CTL.update(
        mode="python",
        exec1_started=threading.Event(),
        before_truncate=threading.Event(),
        mid_reload=threading.Event(),
        finish_reload=threading.Event(),
    )
    _flow, root, child = chain(alias, f"overlap-{how}")

    # Execution 1 of root, on a real worker with a real lease.
    refused = []
    if how == "renewals_fail":
        holder = factory(lock_timeout=1.0)

        def cannot_reach_the_database():
            refused.append(time.monotonic())
            raise OperationalError("injected: renewal cannot reach the database")

        holder.renew_leases = cannot_reach_the_database
        runner = Runner(holder.run).start()
    else:
        holder = factory()
        runner = Runner(holder.run_once).start()
    try:
        assert CTL["exec1_started"].wait(WAIT), "execution 1 never started"
        claimed = row(root)
        assert claimed.status == OxTask.Status.RUNNING
        assert claimed.attempts == 1 < claimed.max_attempts
        assert claimed.lease_expires_at is not None
        epoch1 = claimed.lease_epoch
        assert epoch1 >= 1

        # The lease is handed over while execution 1 is alive.
        if how == "renewals_fail":
            wait_until(
                lambda: row(root).status != OxTask.Status.RUNNING,
                "the holder's own reaper to requeue root",
            )
            assert refused, "no renewal was attempted"
        else:
            assert actions.expire_lease(root.id) is True
            assert factory().reap() == 1
        requeued = row(root)
        assert requeued.status == OxTask.Status.READY
        assert requeued.lease_epoch == epoch1 + 1
        assert runner.thread.is_alive() and LIVE["root"] == {1}

        # Execution 2 of root, on another worker, succeeds. The child is
        # still gated, so root is the only row this claim can take.
        other = factory()
        assert other.run_once() is True
        assert CALLS["root"] == 2
        done = row(root)
        assert done.status == OxTask.Status.SUCCESSFUL
        assert done.lease_epoch == epoch1 + 2
        assert list(range(ROWS)) == TABLE
        # The precondition for the hazard, independent of the child:
        # root's row is SUCCESSFUL while execution 1 is inside the body.
        assert runner.thread.is_alive() and LIVE["root"] == {1}

        # Execution 1 resumes: it truncates and has loaded one row.
        CTL["before_truncate"].set()
        assert CTL["mid_reload"].wait(WAIT)
        CTL["probe"] = lambda: {"execution_1_thread_alive": runner.thread.is_alive()}

        # Whatever a worker can claim now. Not asserted: a fix may refuse it.
        other.run_once()
    finally:
        CTL["before_truncate"].set()
        CTL["finish_reload"].set()
        if how == "renewals_fail":
            holder.request_stop()
        runner.thread.join(WAIT)
    assert not runner.thread.is_alive()
    assert runner.errors == []
    assert CALLS["root"] == 2 and LIVE["root"] == set()
    # Execution 1's outcome write was refused by the lease: the row is
    # execution 2's.
    assert row(root).status == OxTask.Status.SUCCESSFUL
    assert row(root).lease_epoch == epoch1 + 2

    settle(factory)
    assert_no_child_overlapped(child)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A downstream node starts while an upstream execution that overran its "
        "timeout and grace period is still stuck inside the upstream's body."
    ),
)
@pytest.mark.parametrize("alias", sorted(FACTORIES))
def test_a_downstream_node_never_starts_while_a_stuck_upstream_execution_is_live(
    alias,
):
    """
    The backstop path. The overlap here is not an interleaving the test
    picks: from the moment the backstop writes READY until the recycling
    process exits, the stuck thread is alive by the worker's own account,
    and the drain keeps the process up for as long as a sibling needs, up to
    recycle_drain_budget (LOCK_TIMEOUT, 300 s by default).
    """
    factory = FACTORIES[alias]
    backend = settings.TASKS[alias]
    timeouts = {
        **settings.TASKS,
        alias: {
            **backend,
            "OPTIONS": {
                **backend["OPTIONS"],
                "TASK_TIMEOUTS": {"default": 0.5},
                "TASK_TIMEOUT_GRACE": 0.5,
            },
        },
    }
    c_gate = threading.Lock()
    c_gate.acquire()
    CTL.update(
        mode="c",
        c_gate=c_gate,
        exec1_started=threading.Event(),
        sibling_started=threading.Event(),
        sibling_release=threading.Event(),
    )
    with override_settings(TASKS=timeouts):
        _flow, root, child = chain(alias, "overlap-backstop")
        sibling = sibling_step.using(backend=alias).enqueue("sibling")

        # The production worker loop, concurrency 2, a real (non-zero)
        # backoff, the default drain budget rule (= LOCK_TIMEOUT).
        holder = OxpullWorker(
            backend_alias=alias,
            concurrency=2,
            poll_interval=0.01,
            backoff_initial=0.2,
            lock_timeout=60,
        )
        assert holder.timeouts.for_queue("default") == 0.5
        assert holder.timeouts.for_queue("emails") is None
        runner = Runner(holder.run).start()
        try:
            assert CTL["exec1_started"].wait(WAIT), "execution 1 never started"
            assert CTL["sibling_started"].wait(WAIT), "the sibling never started"
            epoch1 = row(root).lease_epoch
            assert epoch1 >= 1

            # The backstop: TaskTimeout could not land in the C call, the
            # grace passed, the attempt is recorded failed with attempts
            # left, the epoch moved, and the worker is recycling.
            wait_until(
                lambda: row(root).status == OxTask.Status.READY,
                "the grace backstop to hand root back",
            )
            handed = row(root)
            assert handed.lease_epoch == epoch1 + 1
            assert handed.attempts == 1 < handed.max_attempts
            assert "did not stop" in handed.errors[-1]["traceback"]
            wait_until(lambda: holder.recycling, "the holder to start recycling")
            # The drain is holding the process open on the sibling, and
            # execution 1 is still inside root's body.
            assert runner.thread.is_alive()
            assert row(sibling).status == OxTask.Status.RUNNING
            assert LIVE["root"] == {1}

            CTL["probe"] = lambda: {
                "holder_recycling": holder.recycling,
                "holder_run_returned": not runner.thread.is_alive(),
                "execution_1_live": sorted(LIVE.get("root", ())),
            }

            # Another worker takes root once its backoff passes.
            other = factory()
            wait_until(
                lambda: (
                    other.run_once() or row(root).status == OxTask.Status.SUCCESSFUL
                ),
                "another worker to run execution 2 of root",
            )
            assert row(root).status == OxTask.Status.SUCCESSFUL
            assert CALLS["root"] == 2
            assert row(root).lease_epoch == epoch1 + 2
            # Root's row is SUCCESSFUL while the recycling process still runs
            # execution 1.
            assert LIVE["root"] == {1}
            assert runner.thread.is_alive()

            # Whatever a worker can claim now. Not asserted: a fix may refuse it.
            other.run_once()
        finally:
            if c_gate.locked():
                c_gate.release()
            CTL["sibling_release"].set()
            runner.thread.join(WAIT)
        assert not runner.thread.is_alive()
        assert runner.errors == []
        assert holder.recycling
        # The stuck thread returns from the C call; its pending TaskTimeout
        # lands, its write is refused, and it leaves the body.
        wait_until(lambda: LIVE["root"] == set(), "execution 1 to leave the body")
        assert row(root).status == OxTask.Status.SUCCESSFUL
        assert row(sibling).status == OxTask.Status.SUCCESSFUL

        settle(factory)
    assert_no_child_overlapped(child)
