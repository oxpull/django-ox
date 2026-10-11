"""
Pruning an upstream that never succeeded must not let its downstream run.

ox_prune deletes finished task rows, and some of them never succeeded:

(i)  The default invocation deletes a DISCARDED row once it is seven days
     old. An operator discards a stuck node from the admin, and a week
     later the row is gone.
(ii) ox_prune --include-failed deletes FAILED and LOST rows the same way.

The gate used to read an edge whose upstream row is missing as satisfied,
so after either prune a gating worker ran the child. The reconciler did not
step in first: it cancels cones in SEALED workflows only, and only while
the dead row is still there to find. A blocked node now waits in WAITING,
and leaves it only once every edge into it is marked satisfied by a writer
that saw its upstream succeed. The claim also blocks on an unmarked edge
whose upstream row is missing.

Every test asserts the promise, not one particular fix: the child of an
upstream that never succeeded never starts. It has no attempt, it was
never RUNNING, and its body never ran. Its status is not one a claim
leads to (RUNNING, SUCCESSFUL, FAILED or LOST); READY, DISCARDED and any
waiting status django-ox may add all pass. Two ends keep that promise,
and most tests accept either one:

- held: the child waits;
- cancelled: the child is DISCARDED without running.

Which end is right depends on what was recorded before the row went. A
recorded failure cancels the cone. A missing row with nothing recorded
holds it, because a missing row no longer says whether the upstream
succeeded, and cancelling there would throw away work downstream of a
success. One test pins each end: a failure a worker recorded before the
prune must cancel the child and let the workflow complete, and a discard
nothing recorded must hold the child, not cancel it.

A control runs alongside them. When the pruned upstream really did
succeed, its child still runs. So a gate that blocks on every missing row
cannot pass this file.

Each test drives the upstream to its status through real paths: a gating
worker's claim and execution, django_ox.actions, or the real reaper. It
runs the real ox_prune management command and the real reconciler pass,
then lets a real gating worker run everything it can claim.

One shortcut, used only where a test relies on ox_prune's default 7-day
cutoff: age() moves the upstream's finished_at back eight days with one
UPDATE, standing in for a week passing. No other column is touched.
"""

from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, order, reconcile, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

PAST_THE_DEFAULT_CUTOFF = timedelta(days=8)

# Only a claim leads to these. A child that never started is in none of
# them: READY while held, DISCARDED once cancelled, or a waiting status.
CLAIMED_STATUSES = (
    OxTask.Status.RUNNING,
    OxTask.Status.SUCCESSFUL,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
)


def ox_prune(*args):
    """The real django-ox command, as a cron line would run it."""
    out = StringIO()
    call_command("ox_prune", *args, stdout=out)
    return out.getvalue()


def age(task_id, by=PAST_THE_DEFAULT_CUTOFF):
    """Stand-in for time passing: move finished_at back, and nothing else."""
    finished_at = OxTask.objects.values_list("finished_at", flat=True).get(id=task_id)
    assert finished_at is not None
    OxTask.objects.filter(id=task_id).update(finished_at=finished_at - by)


def assert_pruned(task_id, output):
    """Precondition: the prune really did delete the upstream row."""
    assert not OxTask.objects.filter(id=task_id).exists(), (
        f"precondition not reached: ox_prune left the upstream row; it said {output!r}"
    )


def status_of(task_id):
    return OxTask.objects.values_list("status", flat=True).get(id=task_id)


def assert_never_started(task_id, label, upstream_status, when):
    """
    No worker ever took this row, and its body never ran.

    A claim is the only way into RUNNING. Every claim adds an attempt,
    stamps started_at and appends a worker id, and nothing in django-ox
    clears any of the three. So they show whether any worker ever took
    the row, even after it finished.
    """
    row = OxTask.objects.values("status", "attempts", "started_at", "worker_ids").get(
        id=task_id
    )
    started = (
        row["status"] in CLAIMED_STATUSES
        or row["attempts"] != 0
        or row["started_at"] is not None
        or bool(row["worker_ids"])
        or label in order()
    )
    assert not started, (
        f"{label!r} started {when}, although its upstream settled "
        f"{upstream_status} and never succeeded: status={row['status']}, "
        f"attempts={row['attempts']}, started_at={row['started_at']}, "
        f"worker_ids={row['worker_ids']!r}, execution order {order()}. "
        "A gate that reads a missing upstream row as satisfied lets it through."
    )


def run_everything_then_assert_never_started(worker, child_id, upstream_status):
    """The last step of every test: the worker runs all it can claim."""
    drain(worker)
    assert_never_started(
        child_id, "child", upstream_status, "after ox_prune deleted its upstream"
    )


def exhaust_leases(root_id):
    """
    Three workers in turn claim the root with a real lease and die holding
    it. expire_lease brings each lease's expiry forward instead of waiting
    LOCK_TIMEOUT; the reaper does the rest, requeueing twice and then, with
    no attempts left, marking the row LOST.
    """
    reaper = flow_worker()
    for attempt in (1, 2, 3):
        claimed = flow_worker().claim_one()
        assert claimed is not None
        assert str(claimed.id) == str(root_id)
        assert claimed.attempts == attempt
        assert actions.expire_lease(root_id) is True
        assert reaper.reap() == 1
    assert status_of(root_id) == OxTask.Status.LOST


# -- (i) OPEN workflow, DISCARDED upstream, default ox_prune --------------


@pytest.mark.parametrize("closed", ["while-ready", "after-failing"])
def test_open_workflow_child_of_a_discarded_upstream_never_starts_after_default_prune(
    closed,
):
    flow = workflows.create(f"open-discarded-{closed}")
    upstream = tasks.step if closed == "while-ready" else tasks.failing_step
    root = flow.add("root", upstream, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    worker = flow_worker()
    if closed == "after-failing":
        assert drain(worker) == 3
        assert status_of(root.id) == OxTask.Status.FAILED

    # An operator closes the upstream from the admin.
    assert actions.discard(root.id) is True
    # The scheduled reconciler ticks. It may hold the child or cancel it.
    # Either way the child must not start.
    reconcile()
    assert stored(flow).status == OxpullWorkflow.Status.OPEN
    assert worker.claim_one() is None  # control: nothing runs before the prune
    assert_never_started(child.id, "child", "DISCARDED", "before the prune")

    age(root.id)
    output = ox_prune()  # the default invocation: SUCCESSFUL + DISCARDED, 7d
    assert_pruned(root.id, output)
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "DISCARDED")


# -- (ii) ox_prune --include-failed ----------------------------------------


def test_open_workflow_child_of_a_failed_upstream_never_starts_after_include_failed():
    flow = workflows.create("open-failed")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    worker = flow_worker()
    assert drain(worker) == 3
    assert status_of(root.id) == OxTask.Status.FAILED

    reconcile()
    assert worker.claim_one() is None  # control: nothing runs before the prune
    assert_never_started(child.id, "child", "FAILED", "before the prune")

    age(root.id)
    output = ox_prune("--include-failed")
    assert_pruned(root.id, output)
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "FAILED")
    assert order() == ["root", "root", "root"]


def test_open_workflow_child_of_a_lost_upstream_never_starts_after_include_failed():
    flow = workflows.create("open-lost")
    root = flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    exhaust_leases(root.id)

    worker = flow_worker()
    reconcile()
    assert worker.claim_one() is None  # control: nothing runs before the prune
    assert_never_started(child.id, "child", "LOST", "before the prune")

    age(root.id)
    output = ox_prune("--include-failed")
    assert_pruned(root.id, output)
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "LOST")
    assert order() == []


@pytest.mark.parametrize(
    ("prune_args", "aged"),
    [
        pytest.param(
            ["--include-failed"], True, id="reconciler-silent-past-the-default-cutoff"
        ),
        pytest.param(
            ["--include-failed", "--older-than", "0"],
            False,
            id="prune-runs-before-the-next-reconciler-tick",
        ),
    ],
)
def test_sealed_workflow_child_of_a_failed_upstream_never_starts_when_prune_comes_first(
    prune_args, aged
):
    flow = workflows.create("sealed-failed")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()
    assert drain(worker) == 3
    assert status_of(root.id) == OxTask.Status.FAILED
    assert worker.claim_one() is None  # control: the gate holds the child

    # No reconciler tick between the failure and the prune.
    if aged:
        age(root.id)
    output = ox_prune(*prune_args)
    assert_pruned(root.id, output)
    # The reconciler's tick arrives after the prune.
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "FAILED")
    assert order() == ["root", "root", "root"]


@pytest.mark.parametrize(
    ("prune_args", "aged"),
    [
        pytest.param(
            ["--include-failed"], True, id="reconciler-silent-past-the-default-cutoff"
        ),
        pytest.param(
            ["--include-failed", "--older-than", "0"],
            False,
            id="prune-runs-before-the-next-reconciler-tick",
        ),
    ],
)
def test_a_missing_upstream_whose_failure_was_recorded_cancels_its_child(
    prune_args, aged
):
    """
    The same sequence as the test above, with the other end pinned.

    The worker that ran root's last attempt wrote FAILED, so how root
    finished was recorded before ox_prune deleted the row. A missing
    upstream with a recorded failure is dead, not unknown: the child is
    cancelled without running, and the sealed workflow completes, so
    on_complete can fire. A fix that only holds (it blocks on the missing
    row and never cancels) leaves the child WAITING and the workflow SEALED
    for ever, and fails here.

    Up to three reconciler ticks are allowed after the prune, for a
    cancellation and a completion that land on separate ticks.
    """
    flow = workflows.create("recorded-failure")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()
    assert drain(worker) == 3
    assert status_of(root.id) == OxTask.Status.FAILED
    assert worker.claim_one() is None  # control: the gate holds the child

    # No reconciler tick between the failure and the prune.
    if aged:
        age(root.id)
    output = ox_prune(*prune_args)
    assert_pruned(root.id, output)

    for _ in range(3):
        reconcile()
        drain(worker)
        if stored(flow).status == OxpullWorkflow.Status.COMPLETE:
            break

    assert_never_started(
        child.id, "child", "FAILED", "after ox_prune deleted its upstream"
    )
    assert order() == ["root", "root", "root"]
    row = stored(flow)
    assert (status_of(child.id), row.status) == (
        OxTask.Status.DISCARDED,
        OxpullWorkflow.Status.COMPLETE,
    ), (
        f"the child is {status_of(child.id)} and the workflow {row.status}, "
        "although a worker recorded its upstream FAILED before the prune: the "
        "child should be cancelled and the workflow complete"
    )


def test_deferred_open_workflow_child_of_a_failed_upstream_is_not_released_by_prune():
    """
    The child waits, so right after the prune it is still out of reach.
    The reconciler's release backstop, which is not scoped to sealed
    workflows, runs next, and it must not release a child whose upstream
    never succeeded.
    """
    flow = workflows.create("deferred-open-failed")
    root = flow.add("root", tasks.failing_deferred_step, "root")
    child = flow.add("child", tasks.deferred_step, "child", depends_on="root")
    assert status_of(child.id) == OxTask.Status.WAITING
    worker = deferring_worker()
    assert drain(worker) == 3
    assert status_of(root.id) == OxTask.Status.FAILED

    reconcile()
    assert worker.claim_one() is None  # control: nothing runs before the prune
    assert_never_started(child.id, "child", "FAILED", "before the prune")

    age(root.id)
    output = ox_prune("--include-failed")
    assert_pruned(root.id, output)
    assert worker.claim_one() is None  # nothing runs the child until a tick
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "FAILED")
    assert order() == ["root", "root", "root"]


# -- a missing row with nothing recorded holds -----------------------------


def test_child_of_an_upstream_pruned_before_its_outcome_was_recorded_is_held():
    """
    An operator discards the upstream, and the prune deletes the row
    before any reconciler tick or worker has seen it. Nothing is left
    that says how it finished.

    The child must not run, and it must not be cancelled either: it is
    held. From a missing row with no record, a discard looks the same as a
    success, so a rule that cancels here also cancels the downstream of
    an upstream that succeeded. That is lost work. Holding is stuck work
    that an operator can see and act on.

    This depends on django-ox 1.2.0's discard recording nothing oxpull can
    read. If a later django-ox discard API does record the discard, the
    upstream is no longer unrecorded and the right end becomes DISCARDED;
    this test then needs changing, not the implementation bending to fit.
    """
    flow = workflows.create("unrecorded")
    root = flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()

    assert actions.discard(root.id) is True
    assert worker.claim_one() is None  # control: the gate holds the child

    # No reconciler tick between the discard and the prune.
    output = ox_prune("--older-than", "0")
    assert_pruned(root.id, output)
    reconcile()

    run_everything_then_assert_never_started(worker, child.id, "DISCARDED")
    assert status_of(child.id) in (OxTask.Status.READY, OxTask.Status.WAITING), (
        f"the child was cancelled (status={status_of(child.id)}) although "
        "nothing recorded how its missing upstream finished. It should be "
        "held: without a record, cancelling here would also cancel work "
        "downstream of an upstream that succeeded."
    )


# -- what a user sees -------------------------------------------------------


def test_end_to_end_a_node_downstream_of_a_pruned_failure_never_runs():
    """
    The user's view of (ii) on an OPEN workflow: nodes are added as data
    arrives and the workflow is sealed at the end, a reconciler is scheduled,
    and ox_prune --include-failed runs from cron with its default cutoff.
    """
    flow = workflows.create(
        "nightly",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    extract = flow.add("extract", tasks.failing_step, "extract")
    load = flow.add("load", tasks.step, "load", depends_on="extract")
    worker = flow_worker()
    assert drain(worker) == 3
    assert status_of(extract.id) == OxTask.Status.FAILED
    reconcile()

    age(extract.id)
    output = ox_prune("--include-failed")
    assert_pruned(extract.id, output)
    drain(worker)
    flow.seal()
    reconcile()

    row = stored(flow)
    progress = flow.progress()
    assert "load" not in order(), (
        f"'load' ran after 'extract' FAILED (execution order {order()}). The "
        f"workflow then completed as status={row.status}, "
        f"failed_count={row.failed_count}, cancelled_count={row.cancelled_count}, "
        f"on_complete enqueued={row.callback_task_id is not None}, "
        f"on_success enqueued={row.success_task_id is not None}; "
        f"progress total={progress.total} succeeded={progress.succeeded} "
        f"failed={progress.failed} unattributed={progress.unattributed}"
    )
    assert_never_started(
        load.id, "load", "FAILED", "after ox_prune deleted its upstream"
    )


# -- control: a pruned upstream that succeeded still satisfies its edge ----


@pytest.mark.parametrize("variant", ["open", "sealed", "deferred"])
def test_control_a_child_whose_pruned_upstream_succeeded_still_runs(variant):
    """
    A fan-in: 'first' succeeds and ox_prune deletes it a week later while
    'second' is still waiting. Then 'second' succeeds, and the child must
    run. Its missing upstream really did succeed.

    This passes today, and it must keep passing. A fix that blocks on every
    missing upstream row, or cancels the cone of one, fails it. So does a
    release sweep that never clears a deferral once an upstream is gone.
    """
    deferred = variant == "deferred"
    step = tasks.deferred_step if deferred else tasks.step
    worker = deferring_worker() if deferred else flow_worker()
    flow = workflows.create(f"control-{variant}")
    # The claim orders by priority first, so 'first' runs alone below.
    first = flow.add("first", step.using(priority=10), "first")
    flow.add("second", step, "second")
    child = flow.add("child", step, "child", depends_on=["first", "second"])
    if variant == "sealed":
        flow.seal()

    assert drain(worker, limit=1) == 1
    assert order() == ["first"]
    assert status_of(first.id) == OxTask.Status.SUCCESSFUL
    reconcile()

    age(first.id)
    output = ox_prune()  # the default invocation: SUCCESSFUL + DISCARDED, 7d
    assert_pruned(first.id, output)
    reconcile()
    assert order() == ["first"]

    drain(worker)
    assert order() == ["first", "second", "child"], (
        f"the child did not run (execution order {order()}, "
        f"status={status_of(child.id)}) although both its upstreams "
        "succeeded. One of them was pruned after it succeeded, and that "
        "must not block or cancel the child."
    )
    assert status_of(child.id) == OxTask.Status.SUCCESSFUL
