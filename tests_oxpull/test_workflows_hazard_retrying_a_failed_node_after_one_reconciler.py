"""
Retrying a failed node after one reconciler tick.

django-ox's supported recovery for a FAILED task is actions.retry, or the
admin's "Retry selected tasks", which calls actions.retry_many. The
workflow reconciler runs once a minute, and its first tick after a node
FAILED discards that node's whole downstream cone. When the cone was the
last pending work, the same tick completes the workflow with
failed_count=1 and fires on_complete.

An operator who fixes the cause and retries after that tick gets the
upstream rerun and succeeding, and nothing else: the cone is DISCARDED,
DISCARDED is not retryable, the reconciler never looks at a COMPLETE
workflow again, and the stored row keeps the failure count from the
first completion.

Every test here asserts the state a correct design ends in, whichever fix
is chosen (revive the cone, hold the cone, or anything else):

  * a node whose upstream nodes all end SUCCESSFUL runs, and is not left
    DISCARDED;
  * a COMPLETE workflow's stored counts agree with its nodes, and
    on_success fires once every node succeeded.

Nothing asserts the intermediate state after the tick, so a design that
never discards the cone passes too. That state is printed in every
failure message, which shows the path was provoked.

Real paths throughout: nodes fail and succeed inside a real worker's
run_once(), the reconciler is enqueued as the task a SCHEDULES entry
enqueues and is claimed and executed by a worker, and recovery goes
through django-ox's public actions. No task row is written by hand.

Gap, recorded rather than closed: a fix of the form "wait N minutes before
cancelling a FAILED node's cone" would pass these tests, because a test
cannot let a wall-clock grace elapse, and would still lose the cone of a
retry that arrives after the grace.

Two controls at the bottom pass today. One runs the same scenario with
the retry landing before any tick: the end-state assertions are
satisfiable by the shipped code when the ordering is favourable. The other
runs the red sequence exactly, retry after the tick included, on a
workflow that was never sealed: the reconciler sweeps SEALED workflows
only, so the same late retry resumes the DAG. Whether an operator's retry
recovers the work turns on whether seal() had been called.
"""

import time
from datetime import timedelta

import pytest
from django.tasks import task
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge

from . import tasks
from .test_workflows import drain, flow_worker, order, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

WAIT = 10


def _transient(label):
    """
    Fails until the operator has fixed the cause, then succeeds. The fix
    is a flag in the per-test state, the stand-in for a deploy, a
    restored credential or a dependency coming back.
    """
    tasks._record(label)
    if not tasks.STATE.get("cause_fixed"):
        raise RuntimeError(f"{label}: transient cause not fixed yet")
    return label


@task(backend="flows")
def transient_step(label):
    return _transient(label)


@task(backend="flows_deferred")
def transient_deferred_step(label):
    return _transient(label)


# backend alias -> (worker factory, the root's task, a plain node task)
BACKENDS = {
    "flows": (flow_worker, transient_step, tasks.step),
    "flows_deferred": (deferring_worker, transient_deferred_step, tasks.deferred_step),
}


def retry_the_node(flow, root_id):
    """An operator calls actions.retry on the one FAILED node."""
    assert actions.retry(root_id) is True, "actions.retry did not requeue the root"


def retry_the_selection(flow, root_id):
    """
    An operator ticks every row of the workflow in the admin and runs
    "Retry selected tasks", which calls actions.retry_many on the queryset.
    """
    ids = list(flow.node_ids().values())
    changed, skipped = actions.retry_many(OxTask.objects.filter(id__in=ids))
    tasks.STATE["retry_many"] = (changed, skipped)
    assert changed == 1, f"retry_many requeued {changed}, skipped {skipped}"


RETRIES = {"actions.retry": retry_the_node, "admin.retry_many": retry_the_selection}


def build(name, backend, cone_is_last, *, seal=True):
    """
    root -> child -> grandchild, both callbacks, sealed unless seal=False.

    With cone_is_last False, an unrelated node scheduled an hour out keeps
    the workflow SEALED past the tick, so the cancellation happens without
    the completion.
    """
    _factory, root_task, step_task = BACKENDS[backend]
    flow = workflows.create(
        name,
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    root = flow.add("root", root_task, "root")
    flow.add("child", step_task, "child", depends_on="root")
    flow.add("grandchild", step_task, "grandchild", depends_on="child")
    if not cone_is_last:
        flow.add(
            "later",
            step_task.using(run_after=timezone.now() + timedelta(hours=1)),
            "later",
        )
    if seal:
        flow.seal()
    return flow, root


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


def settle(factory, rounds=3):
    """Give any recovery a fix might add its chance: ticks and drains."""
    for _ in range(rounds):
        reconciler_tick(factory)
        drain(factory())


def fail_the_root(flow, root, factory):
    """Drain until the root has spent every attempt. Preconditions are hard."""
    drain(factory())
    row = OxTask.objects.get(id=root.id)
    assert row.status == OxTask.Status.FAILED, row.status
    assert row.attempts == row.max_attempts == 3, (row.attempts, row.max_attempts)
    assert order() == ["root"] * 3, order()


def describe(flow):
    ids = flow.node_ids()
    rows = {row.id: row for row in OxTask.objects.filter(id__in=list(ids.values()))}
    nodes = {name: str(rows[task_id].status) for name, task_id in sorted(ids.items())}
    row = stored(flow)
    progress = flow.progress()
    return (
        f"nodes={nodes}; workflow={row.status} failed_count={row.failed_count} "
        f"cancelled_count={row.cancelled_count} "
        f"on_complete_enqueued={row.callback_task_id is not None} "
        f"on_success_enqueued={row.success_task_id is not None}; "
        f"progress(total={progress.total}, pending={progress.pending}, "
        f"succeeded={progress.succeeded}, failed={progress.failed}, "
        f"unattributed={progress.unattributed}); "
        f"callbacks_ran(complete={tasks.STATE.get('complete', 0)}, "
        f"success={tasks.STATE.get('success', 0)})"
    )


def cone_problems(flow, names=("child", "grandchild")):
    """Nodes left DISCARDED although every upstream they wait on succeeded."""
    ids = flow.node_ids()
    by_id = {task_id: name for name, task_id in ids.items()}
    statuses = dict(
        OxTask.objects.filter(id__in=list(ids.values())).values_list("id", "status")
    )
    problems = []
    for name, task_id in sorted(ids.items()):
        if statuses.get(task_id) != OxTask.Status.DISCARDED:
            continue
        upstream = list(
            OxpullWorkflowEdge.objects.filter(downstream_task_id=task_id).values_list(
                "upstream_task_id", flat=True
            )
        )
        if upstream and all(
            statuses.get(up) == OxTask.Status.SUCCESSFUL for up in upstream
        ):
            problems.append(
                f"{name!r} is DISCARDED although every upstream it waits on "
                f"({sorted(by_id[up] for up in upstream)}) is SUCCESSFUL"
            )
    for name in names:
        status = statuses.get(ids[name])
        if status != OxTask.Status.SUCCESSFUL:
            problems.append(f"{name!r} ended {status}, not SUCCESSFUL")
        if name not in order():
            problems.append(f"{name!r} never ran (order={order()})")
    return problems


def outcome_problems(flow):
    """A COMPLETE workflow whose record contradicts its nodes."""
    row = stored(flow)
    progress = flow.progress()
    ids = list(flow.node_ids().values())
    discarded = OxTask.objects.filter(
        id__in=ids, status=OxTask.Status.DISCARDED
    ).count()
    every_node_succeeded = progress.succeeded == progress.total
    problems = []
    if row.status != OxpullWorkflow.Status.COMPLETE:
        problems.append(f"workflow is {row.status}, not COMPLETE, after settling")
        return problems
    if row.failed_count != progress.failed:
        problems.append(
            f"stored failed_count={row.failed_count} but progress().failed="
            f"{progress.failed}"
        )
    if row.cancelled_count != discarded:
        problems.append(
            f"stored cancelled_count={row.cancelled_count} but {discarded} node(s) "
            "are DISCARDED"
        )
    if every_node_succeeded and row.success_task_id is None:
        problems.append("every node SUCCESSFUL, yet on_success was never enqueued")
    if every_node_succeeded and tasks.STATE.get("success", 0) != 1:
        problems.append(
            f"every node SUCCESSFUL, yet on_success ran "
            f"{tasks.STATE.get('success', 0)} time(s)"
        )
    return problems


def run_retry_after_tick(backend, retry, cone_is_last, name):
    factory = BACKENDS[backend][0]
    flow, root = build(name, backend, cone_is_last)
    fail_the_root(flow, root, factory)

    # The scheduled tick lands before the operator does.
    completed = reconciler_tick(factory)
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.FAILED
    after_tick = describe(flow)

    # The operator fixes the cause and retries through django-ox.
    tasks.STATE["cause_fixed"] = True
    RETRIES[retry](flow, root.id)
    drain(factory())
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.SUCCESSFUL
    after_rerun = describe(flow)

    settle(factory)

    ids = flow.node_ids()
    child_retryable = actions.retry(ids["child"])
    context = (
        f"\n  after the tick (completed {completed}): {after_tick}"
        f"\n  after retry and rerun: {after_rerun}"
        f"\n  after 3 more ticks and drains: {describe(flow)}"
        f"\n  actions.retry(child) now -> {child_retryable}"
        f"\n  retry_many (changed, skipped) -> {tasks.STATE.get('retry_many')}"
        f"\n  order={order()}"
    )
    return flow, context


@pytest.mark.xfail(
    strict=True,
    reason=(
        "retrying a failed node after a reconciler tick reruns only that node; "
        "the downstream nodes the tick discarded stay discarded and never run"
    ),
)
@pytest.mark.parametrize("cone_is_last", [True, False], ids=["cone_last", "sibling"])
@pytest.mark.parametrize("retry", sorted(RETRIES))
@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_a_node_retried_after_its_cone_was_cancelled_runs_its_cone(
    backend, retry, cone_is_last
):
    flow, context = run_retry_after_tick(
        backend, retry, cone_is_last, "retry-after-tick"
    )
    problems = cone_problems(flow)
    assert not problems, "; ".join(problems) + context


@pytest.mark.xfail(
    strict=True,
    reason=(
        "a workflow retried after a reconciler tick completed it keeps "
        "failed_count from that first completion although no node is failed"
    ),
)
@pytest.mark.parametrize("retry", sorted(RETRIES))
@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_a_workflow_recovered_by_retry_keeps_no_stale_outcome(backend, retry):
    flow, context = run_retry_after_tick(
        backend, retry, cone_is_last=True, name="stale-outcome"
    )
    problems = outcome_problems(flow)
    assert not problems, "; ".join(problems) + context


# -- control: passes today ---------------------------------------------


@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_control_a_retry_before_any_tick_recovers_the_workflow(backend):
    """
    The same scenario with the retry landing before the reconciler's
    first tick. Holds on the shipped code, and proves the assertions the
    red tests make are satisfiable.
    """
    factory = BACKENDS[backend][0]
    flow, root = build("control", backend, cone_is_last=True)
    fail_the_root(flow, root, factory)

    tasks.STATE["cause_fixed"] = True
    retry_the_node(flow, root.id)
    drain(factory())
    settle(factory)

    context = f"\n  {describe(flow)}\n  order={order()}"
    problems = cone_problems(flow) + outcome_problems(flow)
    assert not problems, "; ".join(problems) + context
    assert stored(flow).failed_count == 0
    assert stored(flow).cancelled_count == 0


@pytest.mark.parametrize("backend", sorted(BACKENDS))
def test_control_an_unsealed_workflow_resumes_after_the_same_late_retry(backend):
    """
    The red tests' sequence step for step, the tick before the retry
    included, on a workflow nobody has sealed. The reconciler reads only
    SEALED workflows, so the tick leaves the cone WAITING, the retry
    resumes it, and sealing afterwards completes cleanly with on_success.
    """
    factory = BACKENDS[backend][0]
    flow, root = build("unsealed", backend, cone_is_last=True, seal=False)
    fail_the_root(flow, root, factory)

    reconciler_tick(factory)
    after_tick = describe(flow)
    ids = flow.node_ids()
    assert stored(flow).status == OxpullWorkflow.Status.OPEN, after_tick
    for name in ("child", "grandchild"):
        assert OxTask.objects.get(id=ids[name]).status == OxTask.Status.WAITING, (
            name,
            after_tick,
        )

    tasks.STATE["cause_fixed"] = True
    retry_the_node(flow, root.id)
    drain(factory())
    assert order() == ["root"] * 4 + ["child", "grandchild"], order()
    flow.seal()
    settle(factory)

    context = f"\n  after the tick: {after_tick}\n  end: {describe(flow)}"
    problems = cone_problems(flow) + outcome_problems(flow)
    assert not problems, "; ".join(problems) + context
    assert stored(flow).failed_count == 0, context
    assert stored(flow).cancelled_count == 0, context
