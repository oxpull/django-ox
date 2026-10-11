"""
A workflow node the reaper marks LOST while its execution is still running
must hold its downstream nodes, not discard them, and that execution's late
SUCCESSFUL must release them.

django-ox 1.2.0 lets the late write land on purpose. WRITABLE_STATUSES is
(RUNNING, LOST), and the reaper's exhausted branch leaves lease_epoch alone
so the holder of the lease can still supply the outcome. LOST says the
outcome was never observed, not that the work failed. So a workflow treats
it as unknown: the nodes downstream of it wait, gated, until the execution
reports or someone acts on the node or the workflow. FAILED and DISCARDED
still discard what depends on them; that is not what these tests cover.

Everything below runs through the production path: a real OxpullWorker
executing a node whose body blocks, a lease that really expires, the real
reaper, the reconciler run as the task a SCHEDULES entry enqueues and a
worker claims, and the straggler's own outcome write. Two ways of losing
the lease are covered:

- renewals_fail: the worker's full run() loop with a one-second lease,
  its renewal UPDATE raising as it does when that worker cannot reach the
  database, and the loop's own reaper taking the row. The fault is
  injected at renew_leases, so the real _renewal_loop handles it.
- operator_expires_lease: django_ox.actions.expire_lease on a live
  worker's task, which that function documents as "It does not stop the
  task", then another worker's reaper.

The earlier attempts fail for real so that the blocking attempt is the
last one, which is what sends the reaper down its LOST branch. No status,
lease_epoch or other state column is written by hand. The one hand-written
change is a move of timestamps into the past, described in phase 1; the
straggler's outcome write matches on status and lease_epoch, never on a
timestamp, so the move cannot change whether it lands.

Each test runs root -> child -> grandchild in three phases:

1. While root is LOST and its execution is alive, five reconciler passes
   run: one at real time, then one each with Django's clock
   (django.utils.timezone.now) an hour, eight days and 400 days ahead,
   then one more at real time after every timestamp on the workflow's
   rows has been moved 400 days into the past in the database: the node
   task rows (all but run_after), the workflow row, and its node and edge
   rows. The last pass is what a hold measured on the database clock sees
   400 days later. After every pass root is still LOST, child and
   grandchild have neither finished nor started, the workflow has not
   completed, and no callback has been enqueued.
2. The clock goes back to real time and the straggler writes SUCCESSFUL.
3. Reconciler passes and drains run until the workflow completes, then one
   more of each. All three nodes are SUCCESSFUL and ran once, in order.
   The workflow is COMPLETE with nothing failed or cancelled. on_complete
   and on_success were each enqueued exactly once and each ran once.
   Nothing here retries the workflow, so the whole run is one generation,
   and a second on_complete is a defect.

Every check is collected before the one assertion, so a failure names
each rule it broke.

Implementations these tests reject:

- Discard on LOST, as the reconciler does today: the first pass in phase 1
  sees child DISCARDED and the workflow complete.
- Discard the LOST node itself first, so its late write matches nothing:
  phase 1 sees root no longer LOST, and phase 3 sees the chain unrun.
- Hold for a grace period, then discard. A pass that treats a LOST row as
  dead once its finished_at is older than some interval sees child
  DISCARDED in a later pass of phase 1, whichever clock it reads: against
  timezone.now() the passes with Django's clock ahead catch it, and
  against the database's own now (Now() in a query) or a now() bound at
  import the pass after the timestamps move catches it. The same holds for
  an interval measured on any other timestamp of those rows. Counting
  passes instead fails the same way if it gives up within five.
- Discard on LOST, then put the discarded nodes back and reopen the
  workflow when the late SUCCESSFUL lands: phase 1 sees the discard, and
  phase 3 counts two on_complete callbacks.
- Treat LOST as satisfied: child starts during phase 1.

What the passes cannot reach: a hold that gives up only after more than
five passes, or one that expires on a clock neither move touches, such as
a monotonic timer kept in the reconciler's process. Such a hold is caught
only if it expires within the few seconds phase 1 really takes.
"""

import threading
import time
from datetime import timedelta

import pytest
from django.db import OperationalError, connections
from django.db.models import DateTimeField, F
from django.tasks import task
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import drain, flow_worker, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db(transaction=True)

WAIT = 30.0

LEASE_LOSS = ["renewals_fail", "operator_expires_lease"]

# The reconciler passes that run while root is LOST, and how far Django's
# clock is ahead of real time for each.
HOLD_PASSES = (
    ("at real time", timedelta(0)),
    ("with the clock an hour ahead", timedelta(hours=1)),
    ("with the clock eight days ahead", timedelta(days=8)),
    ("with the clock 400 days ahead", timedelta(days=400)),
)

# How far the last pass of phase 1 moves the workflow's timestamps back.
AGED_BY = timedelta(days=400)

FINISHED = (
    OxTask.Status.SUCCESSFUL,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    OxTask.Status.DISCARDED,
)

# label -> the stalling node's controls. Module state rather than
# tasks.STATE because a worker thread resolves the task by its dotted path
# and must reach the same Events the test holds.
_CONTROL: dict[str, dict] = {}


@pytest.fixture(autouse=True)
def _control():
    _CONTROL.clear()
    yield
    for ctl in _CONTROL.values():
        ctl["release"].set()
    _CONTROL.clear()


class Clock:
    """
    django.utils.timezone.now, moved ahead on demand.

    Replaced on the module, so every caller that looks it up when it calls
    it sees the move, django-ox included. django-ox's leases run on the
    database clock, which this does not move, so moving it neither expires
    nor extends a lease.
    """

    def __init__(self, monkeypatch):
        self.ahead = timedelta(0)
        real_now = timezone.now
        monkeypatch.setattr(timezone, "now", lambda: real_now() + self.ahead)


@pytest.fixture
def clock(monkeypatch):
    return Clock(monkeypatch)


def _stall(label):
    """
    Fail every attempt before the last, then block on the last until the
    test releases it, then succeed.
    """
    ctl = _CONTROL[label]
    ctl["calls"] += 1
    if ctl["calls"] < ctl["stall_on"]:
        raise RuntimeError(f"{label}: attempt {ctl['calls']} fails")
    ctl["started"].set()
    ctl["released"] = ctl["release"].wait(timeout=60)
    tasks._record(label)
    return label


@task(backend="flows")
def stalling_step(label):
    return _stall(label)


@task(backend="flows_deferred")
def stalling_deferred_step(label):
    return _stall(label)


class Straggler:
    """One node execution that outlives its lease, then finishes."""

    def __init__(self, factory, root_id, label, how):
        self.factory = factory
        self.root_id = root_id
        self.how = how
        self.ctl = _CONTROL[label] = {
            "calls": 0,
            "stall_on": OxTask.objects.get(id=root_id).max_attempts,
            "started": threading.Event(),
            "release": threading.Event(),
            "released": None,
        }
        self.renewals_refused = 0
        self.errors: list[BaseException] = []
        self.thread: threading.Thread | None = None
        self.executor = None
        self.claimed_epoch = None
        self.lost_epoch = None

    def _renewal_cannot_reach_the_database(self):
        self.renewals_refused += 1
        raise OperationalError(
            "injected: this worker's renewal cannot reach the database"
        )

    def _run(self, target):
        try:
            target()
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            connections.close_all()

    def start(self):
        if self.how == "renewals_fail":
            # The whole poll loop: claiming, retrying, renewing and reaping
            # are all the worker's own.
            self.executor = self.factory(lock_timeout=1.0)
            self.executor.renew_leases = self._renewal_cannot_reach_the_database
            target = self.executor.run
        else:
            self.executor = self.factory()
            for _ in range(1, self.ctl["stall_on"]):
                assert self.executor.run_once() is True
                assert OxTask.objects.get(id=self.root_id).status == OxTask.Status.READY
            target = self.executor.run_once
        self.thread = threading.Thread(target=self._run, args=(target,), daemon=True)
        self.thread.start()
        assert self.ctl["started"].wait(WAIT), "the node's last attempt never started"
        row = OxTask.objects.get(id=self.root_id)
        assert row.status == OxTask.Status.RUNNING
        assert row.attempts == row.max_attempts
        # A 1.2.0 lease: stored expiry and a bumped epoch, not the
        # NULL-expiry fallback the existing crash tests reap.
        assert row.lease_expires_at is not None
        assert row.lease_epoch >= 1
        self.claimed_epoch = row.lease_epoch

    def lose_lease(self):
        if self.how == "renewals_fail":
            deadline = time.monotonic() + WAIT
            while OxTask.objects.get(id=self.root_id).status == OxTask.Status.RUNNING:
                assert time.monotonic() < deadline, (
                    "the worker's reaper never took the row"
                )
                time.sleep(0.05)
            assert self.renewals_refused >= 1
        else:
            assert actions.expire_lease(self.root_id) is True
            assert self.factory().reap() == 1
        row = OxTask.objects.get(id=self.root_id)
        assert row.status == OxTask.Status.LOST
        assert self.thread.is_alive(), "the execution must outlive its row's LOST"
        self.lost_epoch = row.lease_epoch

    def finish(self):
        self.ctl["release"].set()
        if self.how == "renewals_fail":
            # run() drains the in-flight attempt, outcome write included,
            # before it returns.
            self.executor.request_stop()
        self.thread.join(WAIT)
        assert not self.thread.is_alive()
        assert self.errors == []
        assert self.ctl["released"] is True, "the body timed out rather than finishing"

    def close(self):
        self.ctl["release"].set()
        if self.executor is not None:
            self.executor.request_stop()
        if self.thread is not None:
            self.thread.join(WAIT)


def chain(name, step_task, stalling_task):
    """root -> child -> grandchild, with both callbacks."""
    flow = workflows.create(
        name,
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    root = flow.add("root", stalling_task, "root")
    flow.add("child", step_task, "child", depends_on="root")
    flow.add("grandchild", step_task, "grandchild", depends_on="child")
    flow.seal()
    return flow, root


def _timestamps(model, *, keep=()):
    return [
        field.name
        for field in model._meta.concrete_fields
        if isinstance(field, DateTimeField) and field.name not in keep
    ]


def age_every_timestamp(flow):
    """
    Move every timestamp on this workflow's rows AGED_BY into the past, in
    the database: the node task rows (all but run_after, which decides
    claims), the workflow row, and its node and edge rows. NULL stays
    NULL. Nothing but timestamps is written.
    """
    task_ids = list(flow.node_ids().values())
    targets = (
        (OxTask, {"id__in": task_ids}, ("run_after",)),
        (OxpullWorkflow, {"pk": flow.id}, ()),
        (OxpullWorkflowNode, {"workflow_id": flow.id}, ()),
        (OxpullWorkflowEdge, {"workflow_id": flow.id}, ()),
    )
    for model, match, keep in targets:
        rows = model.objects.filter(**match)
        fields = _timestamps(model, keep=keep)
        assert rows.exists() and fields, model
        rows.update(**{name: F(name) - AGED_BY for name in fields})


def reconciler_tick(factory):
    """
    One reconciler tick, run the way a SCHEDULES entry runs it: enqueued
    as the task, then claimed and executed by a worker.
    """
    result = workflows.reconcile.enqueue()
    runner = factory()
    deadline = time.monotonic() + WAIT
    while OxTask.objects.get(id=result.id).status in (
        OxTask.Status.READY,
        OxTask.Status.RUNNING,
    ):
        assert time.monotonic() < deadline, "no worker ran the reconciler"
        if not runner.run_once():
            time.sleep(0.01)
    row = OxTask.objects.get(id=result.id)
    assert row.status == OxTask.Status.SUCCESSFUL, row.errors
    return row.return_value


def settle(factory, flow, at_least=3, at_most=8):
    """
    Give the late success every chance to take effect: reconciler ticks
    and drains until the workflow completes, then one more of each, so a
    workflow that reopens gets the chance to complete and call back again.
    """
    for done in range(1, at_most + 1):
        reconciler_tick(factory)
        drain(factory())
        if done >= at_least and stored(flow).status == OxpullWorkflow.Status.COMPLETE:
            break
    reconciler_tick(factory)
    drain(factory())


def node_rows(flow):
    ids = flow.node_ids()
    rows = {row.id: row for row in OxTask.objects.filter(id__in=list(ids.values()))}
    return {name: rows[tid] for name, tid in ids.items()}


def callback_rows(label):
    """Every record_callback task row enqueued with this label."""
    return [
        row
        for row in OxTask.objects.filter(task_path=tasks.record_callback.module_path)
        if row.args[:1] == [label]
    ]


def describe(flow):
    nodes = {
        name: (
            f"{row.status} attempts={row.attempts} epoch={row.lease_epoch} "
            f"run_after={row.run_after and row.run_after.isoformat()}"
        )
        for name, row in node_rows(flow).items()
    }
    row = stored(flow)
    fields = ("status", "failed_count", "cancelled_count")
    return {
        "nodes": nodes,
        "workflow": {field: getattr(row, field, None) for field in fields},
        "callbacks enqueued": {
            label: len(callback_rows(label)) for label in ("complete", "success")
        },
    }


def held_problems(flow):
    """What the workflow must never show while root is LOST."""
    rows = node_rows(flow)
    problems = []
    if rows["root"].status != OxTask.Status.LOST:
        problems.append(
            f"root is {rows['root'].status}, although only its own "
            "still-running execution may settle it"
        )
    for name in ("child", "grandchild"):
        row = rows[name]
        if row.status in FINISHED:
            problems.append(
                f"{name!r} is {row.status}; it must wait for its upstream "
                "to report, not finish"
            )
        if row.status == OxTask.Status.RUNNING or row.attempts:
            problems.append(
                f"{name!r} started (status={row.status}, "
                f"attempts={row.attempts}) before its upstream reported"
            )
    if stored(flow).status == OxpullWorkflow.Status.COMPLETE:
        problems.append("the workflow completed")
    for label in ("complete", "success"):
        enqueued = len(callback_rows(label))
        if enqueued:
            problems.append(f"on_{label} was enqueued {enqueued} time(s)")
    return problems


def settled_problems(flow):
    """What must hold once the late success has been given every chance."""
    rows = node_rows(flow)
    problems = [
        f"{name!r} ended {row.status}; its upstream succeeded, so it must "
        "have run and succeeded"
        for name, row in rows.items()
        if row.status != OxTask.Status.SUCCESSFUL
    ]
    ran = [label for label in tasks.STATE.get("order", []) if label in rows]
    if ran != ["root", "child", "grandchild"]:
        problems.append(f"the nodes ran as {ran}, not once each in order")
    row = stored(flow)
    if row.status != OxpullWorkflow.Status.COMPLETE:
        problems.append(f"the workflow did not complete: {row.status}")
    elif row.failed_count or row.cancelled_count:
        problems.append(
            f"COMPLETE with failed_count={row.failed_count} and "
            f"cancelled_count={row.cancelled_count}, although nothing failed "
            "and nothing needed cancelling"
        )
    for label in ("complete", "success"):
        enqueued = len(callback_rows(label))
        if enqueued != 1:
            problems.append(
                f"on_{label} was enqueued {enqueued} time(s) in one "
                "generation; it must be enqueued exactly once"
            )
        executed = tasks.STATE.get(label, 0)
        if executed != 1:
            problems.append(
                f"on_{label} ran {executed} time(s); it must run exactly once"
            )
    return problems


def _scenario(how, factory, step_task, stalling_task, name, clock):
    flow, root = chain(name, step_task, stalling_task)
    straggler = Straggler(factory, root.id, "root", how)
    problems: list[str] = []
    seen: set[str] = set()
    passes: list[str] = []

    def hold_pass(when):
        completed = reconciler_tick(factory)
        passes.append(
            f"pass {when} completed {completed} workflow(s): {describe(flow)}"
        )
        # Each rule reported once, at the first pass that broke it.
        for problem in held_problems(flow):
            if problem not in seen:
                seen.add(problem)
                problems.append(f"while root is LOST, after the pass {when}: {problem}")

    try:
        straggler.start()
        straggler.lose_lease()
        for when, ahead in HOLD_PASSES:
            clock.ahead = ahead
            hold_pass(when)
        clock.ahead = timedelta(0)
        age_every_timestamp(flow)
        assert OxTask.objects.get(id=root.id).status == OxTask.Status.LOST
        hold_pass("at real time with the workflow's timestamps 400 days in the past")
        straggler.finish()
    finally:
        clock.ahead = timedelta(0)
        straggler.close()
    after_write = describe(flow)
    settle(factory, flow)
    problems.extend(
        f"after the late SUCCESSFUL and settling: {problem}"
        for problem in settled_problems(flow)
    )
    report = "\n".join(
        [
            f"lease lost via {how}",
            *problems,
            (
                f"epoch at claim={straggler.claimed_epoch}, "
                f"at LOST={straggler.lost_epoch}, "
                f"renewals refused={straggler.renewals_refused}"
            ),
            *passes,
            f"after the straggler's write: {after_write}",
            f"after settling: {describe(flow)}",
            (
                f"root's body ran {straggler.ctl['calls']} time(s); "
                f"order={tasks.STATE.get('order')}; "
                f"on_complete ran {tasks.STATE.get('complete', 0)}, "
                f"on_success ran {tasks.STATE.get('success', 0)}"
            ),
        ]
    )
    return problems, report


@pytest.mark.xfail(
    strict=True,
    reason=(
        "When a node is marked LOST while its execution is still running, "
        "the reconciler discards the nodes downstream of it and completes the "
        "workflow instead of holding them until that execution reports. The "
        "late success then lands, and the discarded nodes never run."
    ),
)
@pytest.mark.parametrize("how", LEASE_LOSS)
def test_a_late_success_over_lost_never_leaves_its_cone_discarded(how, clock):
    problems, report = _scenario(
        how, flow_worker, tasks.step, stalling_step, f"late-success-{how}", clock
    )
    assert not problems, report


@pytest.mark.xfail(
    strict=True,
    reason=(
        "On a backend that defers blocked nodes, when a node is marked LOST "
        "while its execution is still running, the reconciler discards the "
        "nodes downstream of it and completes the workflow instead of holding "
        "them until that execution reports. The late success then lands, and "
        "there is nothing left for it to release."
    ),
)
@pytest.mark.parametrize("how", LEASE_LOSS)
def test_a_late_success_over_lost_never_leaves_its_deferred_cone_discarded(how, clock):
    """
    On a backend that defers blocked nodes, the downstream nodes wait
    behind a far-future run_after, and the late SUCCESSFUL releases them
    through the worker's outcome write. They must still be there to
    release.
    """
    problems, report = _scenario(
        how,
        deferring_worker,
        tasks.deferred_step,
        stalling_deferred_step,
        f"late-deferred-success-{how}",
        clock,
    )
    assert not problems, report
