"""
A DAG that runs longer than ox_prune --older-than.

The gate and completion read the same missing upstream row two ways. The
gate (runnable_filter_q / runnable_filter_sql) reads an absent upstream as
satisfied, so the DAG keeps running after ox_prune deleted a finished
stage. Completion (_complete_if_finished) counts only the node rows still
present, so the same absence becomes "outcome unknown": on_success is
withheld on a run where every node succeeded, and a FAILED row that was
pruned or discarded before completion is not counted in failed_count.

A DAG settles its stages one after another, so its root finishes first
and its leaf last. Any root-to-leaf time longer than --older-than puts the
root's row in reach of the retention command while the leaf still waits.

Every test asserts the behaviour a user expects through real paths.
OxpullWorker.run_once() claims and runs every node, django-ox's own
ox_prune management command deletes rows, django_ox.actions.discard is
the admin's "Discard selected tasks", and the reconciler is
workflows._reconcile(), the scheduled task's body. Callbacks are then
executed by a django-ox Worker, so "on_success fired" means the callback
ran, not only that an id was stored.

Time is advanced two ways. "wallclock" waits 1.5 s and prunes with
--older-than 1s, so no row is written by hand. "backdated" moves
finished_at back two hours with one UPDATE and prunes with --older-than
1h, standing in for a DAG whose root-to-leaf time exceeds an hour.

The delayed-node test has two times to keep apart, the reminder's run_after
and the prune cutoff, so it moves the clock instead of waiting on it
(HeldClock). finished_at, ox_prune's cutoff and the claim's run_after cutoff
are all read from django.utils.timezone.now in the process that acts, so
the one move carries all three and no row is written by hand there either.
"""

import logging
import time
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows

from . import tasks
from .test_workflows import drain, flow_worker, order, reconcile, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

HORIZONS = ["wallclock", "backdated"]


class HeldClock:
    """
    django.utils.timezone.now, held at one instant until the test moves it.

    Replaced on the module, so every caller that looks it up when it calls
    it reads the held instant, django-ox's worker and its ox_prune included.
    django-ox's leases run on the database clock, which this does not move,
    so moving it neither expires nor extends a lease.
    """

    def __init__(self, monkeypatch):
        self.at = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: self.at)


@pytest.fixture
def held_clock(monkeypatch):
    return HeldClock(monkeypatch)


def _with_callbacks(name):
    return workflows.create(
        name,
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )


def _status(task_id):
    return OxTask.objects.filter(pk=task_id).values_list("status", flat=True).first()


def _prune_past(horizon, task_ids, *extra):
    """
    Put these finished rows past the prune cutoff and run ox_prune.

    Asserts the rows are really gone, so nothing downstream can pass or
    fail because a prune quietly did nothing.
    """
    if horizon == "wallclock":
        time.sleep(1.5)
        args = ["--older-than", "1s"]
    else:
        OxTask.objects.filter(pk__in=task_ids).update(
            finished_at=timezone.now() - timedelta(hours=2)
        )
        args = ["--older-than", "1h"]
    out = StringIO()
    call_command("ox_prune", *args, *extra, stdout=out)
    for task_id in task_ids:
        assert not OxTask.objects.filter(pk=task_id).exists(), out.getvalue()
    return out.getvalue()


def _run_callbacks():
    """Execute enqueued callbacks as a production django-ox worker would."""
    return drain(Worker(backoff_initial=0, poll_interval=0.01))


def _record(flow, caplog, prune_output=""):
    row = stored(flow)
    progress = flow.progress()
    levels = sorted(
        {r.levelname for r in caplog.records if r.name.startswith("oxpull")}
    )
    messages = [r.getMessage() for r in caplog.records if r.name.startswith("oxpull")]
    return (
        f"status={row.status} total={row.total_count} "
        f"failed_count={row.failed_count} cancelled_count={row.cancelled_count} "
        f"on_complete id={row.callback_task_id} ran={tasks.STATE.get('complete')} "
        f"on_success id={row.success_task_id} ran={tasks.STATE.get('success')}; "
        f"progress total={progress.total} succeeded={progress.succeeded} "
        f"failed={progress.failed} unattributed={progress.unattributed}; "
        f"order={order()}; oxpull log levels={levels} messages={messages}; "
        f"prune said {prune_output.strip()!r}"
    )


@pytest.mark.parametrize("horizon", HORIZONS)
@pytest.mark.parametrize("backend", ["gate", "deferred"])
def test_a_dag_longer_than_the_prune_horizon_still_fires_on_success(
    backend, horizon, caplog
):
    """
    root -> leaf, every node SUCCESSFUL. The root's row is pruned by the
    default ox_prune statuses while the leaf still waits; a healthy
    reconciler ticks in between. The leaf runs, because the gate reads the
    pruned root as SUCCESSFUL. The workflow must then complete clean.
    """
    caplog.set_level(logging.INFO, logger="oxpull")
    step = tasks.step if backend == "gate" else tasks.deferred_step
    worker = flow_worker() if backend == "gate" else deferring_worker()

    flow = _with_callbacks(f"long-clean-{backend}-{horizon}")
    root = flow.add("root", step, "root")
    leaf = flow.add("leaf", step, "leaf", depends_on="root")
    flow.seal()

    assert worker.run_once() is True
    assert order() == ["root"]
    assert _status(root.id) == OxTask.Status.SUCCESSFUL
    # A reconciler on its once-a-minute schedule: nothing to complete yet.
    assert reconcile() == 0

    said = _prune_past(horizon, [root.id])
    assert _status(leaf.id) == OxTask.Status.READY

    # The gate reads the pruned root as satisfied and runs the leaf.
    assert worker.run_once() is True
    assert order() == ["root", "leaf"]
    assert _status(leaf.id) == OxTask.Status.SUCCESSFUL

    assert reconcile() == 1
    _run_callbacks()
    row = stored(flow)
    assert row.failed_count == 0
    assert tasks.STATE.get("complete") == 1
    # Completion reads the same absence as "outcome unknown".
    assert row.success_task_id is not None and tasks.STATE.get("success") == 1, (
        "every node SUCCESSFUL and the leaf ran on the gate's reading of the "
        "pruned root as SUCCESSFUL, yet on_success was withheld: "
        + _record(flow, caplog, said)
    )


def test_a_delayed_node_past_the_prune_horizon_still_fires_on_success(
    caplog, held_clock
):
    """
    signup -> reminder, with the reminder scheduled after its upstream via
    run_after: the shape of a drip campaign, scaled from days to seconds.
    No row is written by hand. At the default --older-than 7d the same
    sequence is a reminder scheduled more than seven days after signup.

    The clock stands still while signup runs, so the reminder is not due
    however long that takes, and is then moved past the reminder's time,
    which puts signup's finished_at more than the prune horizon behind it.
    """
    caplog.set_level(logging.INFO, logger="oxpull")
    worker = flow_worker()
    due = held_clock.at + timedelta(seconds=2)

    flow = _with_callbacks("drip")
    signup = flow.add("signup", tasks.step, "signup")
    reminder = flow.add(
        "reminder",
        tasks.step.using(run_after=due),
        "reminder",
        depends_on="signup",
    )
    flow.seal()

    assert worker.run_once() is True
    assert order() == ["signup"]
    assert worker.run_once() is False  # the reminder is not due yet
    assert reconcile() == 0

    held_clock.at = due + timedelta(milliseconds=200)
    out = StringIO()
    call_command("ox_prune", "--older-than", "1s", stdout=out)
    assert not OxTask.objects.filter(pk=signup.id).exists(), out.getvalue()

    assert worker.run_once() is True
    assert order() == ["signup", "reminder"]
    assert _status(reminder.id) == OxTask.Status.SUCCESSFUL

    assert reconcile() == 1
    _run_callbacks()
    row = stored(flow)
    assert row.success_task_id is not None and tasks.STATE.get("success") == 1, (
        "both nodes SUCCESSFUL, on_success withheld: "
        + _record(flow, caplog, out.getvalue())
    )


@pytest.mark.parametrize("horizon", HORIZONS)
def test_a_failed_branch_pruned_while_a_sibling_waits_is_counted_as_failed(
    horizon, caplog
):
    """
    root -> {bad, good}. bad fails every attempt while good is still
    READY; a healthy reconciler ticks and cannot complete the workflow.
    ox_prune --include-failed then deletes bad's FAILED row, good runs, and
    the workflow completes. The workflow's record must still say a node
    failed.
    """
    caplog.set_level(logging.INFO, logger="oxpull")
    worker = flow_worker()

    flow = _with_callbacks(f"long-failed-{horizon}")
    flow.add("root", tasks.step, "root")
    bad = flow.add("bad", tasks.failing_step, "bad", depends_on="root")
    # Lower priority, so the claim takes every attempt of bad first and
    # good is still READY when bad's row is pruned.
    good = flow.add("good", tasks.step.using(priority=-5), "good", depends_on="root")
    flow.seal()

    for _ in range(4):
        assert worker.run_once() is True
    assert order() == ["root", "bad", "bad", "bad"]
    assert _status(bad.id) == OxTask.Status.FAILED
    assert _status(good.id) == OxTask.Status.READY
    assert reconcile() == 0

    said = _prune_past(horizon, [bad.id], "--include-failed")
    assert _status(good.id) == OxTask.Status.READY

    assert worker.run_once() is True
    assert order() == ["root", "bad", "bad", "bad", "good"]
    assert reconcile() == 1
    _run_callbacks()
    row = stored(flow)
    assert row.success_task_id is None  # withheld, correctly
    assert row.failed_count == 1, (
        "a node FAILED all 3 attempts, and the completed workflow records "
        "failed_count=0: " + _record(flow, caplog, said)
    )


def test_an_operator_discard_of_a_failed_node_is_still_counted_as_failed(caplog):
    """
    root -> {bad, good}. bad fails every attempt while good is still
    READY. An operator triaging the failure discards bad (the django-ox
    admin's "Discard selected tasks"), so default ox_prune can reclaim it.
    good runs and the workflow completes.
    """
    caplog.set_level(logging.INFO, logger="oxpull")
    worker = flow_worker()

    flow = _with_callbacks("acknowledged")
    flow.add("root", tasks.step, "root")
    bad = flow.add("bad", tasks.failing_step, "bad", depends_on="root")
    good = flow.add("good", tasks.step.using(priority=-5), "good", depends_on="root")
    flow.seal()

    for _ in range(4):
        assert worker.run_once() is True
    assert _status(bad.id) == OxTask.Status.FAILED
    assert _status(good.id) == OxTask.Status.READY
    assert reconcile() == 0

    assert actions.discard(bad.id) is True
    assert worker.run_once() is True
    assert order() == ["root", "bad", "bad", "bad", "good"]
    assert reconcile() == 1
    _run_callbacks()
    row = stored(flow)
    assert row.success_task_id is None  # withheld, correctly
    assert row.failed_count == 1, (
        "a node FAILED all 3 attempts and was discarded afterwards; the "
        "completed workflow records failed_count=0: " + _record(flow, caplog)
    )
