"""
Deleting a workflow row used to cascade its edges away, and the gate then
let every blocked node through.

OxpullWorkflowNode.workflow and OxpullWorkflowEdge.workflow were
on_delete=CASCADE, and the claim gate was NOT EXISTS over edge rows, so a
node with no edges left was runnable. Workflows ships no cancel API and no
admin, and the reconciler sweeps SEALED workflows only. That makes an ORM
delete of the parent row the obvious cleanup for an abandoned or stuck
workflow. Both foreign keys are now PROTECT, so the delete is refused while
any node or edge remains, and a node that waits for an upstream is WAITING,
which no claim reads.

Every test asserts the promise that should survive that cleanup: a node
never runs ahead of an upstream that has not succeeded. A fix that
refuses the delete, or discards the workflow's READY nodes as part of
it, passes these tests.

Real paths throughout. A real OxpullWorker makes the attempts, the
failure write, the retry backoff and the claim. The reconciler runs as
the scheduled task's body. The cleanup is the ORM delete a support
engineer would type into `manage.py shell`.
"""

import pytest
from django.db.models import ProtectedError, RestrictedError

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, flow_worker, order, reconcile
from .test_workflows_deferred import deferring_worker, run_after

pytestmark = pytest.mark.django_db


def status_of(task_id):
    return OxTask.objects.filter(id=task_id).values_list("status", flat=True).first()


def operator_deletes(flow):
    """
    The cleanup: delete the workflow row through the ORM.

    A refused delete is a correct outcome, so it is reported rather than
    failed on. A delete that goes through must really remove the row, so
    no test below can pass because the delete matched nothing.
    """
    try:
        OxpullWorkflow.objects.filter(pk=flow.id).delete()
    except (ProtectedError, RestrictedError):
        return False
    assert not OxpullWorkflow.objects.filter(pk=flow.id).exists()
    return True


def aftermath(flow, **nodes):
    statuses = {name: status_of(node.id) for name, node in nodes.items()}
    try:
        workflows.get(flow.id)
        record = "workflows.get() still finds it"
    except OxpullWorkflow.DoesNotExist:
        record = "workflows.get() raises DoesNotExist"
    callbacks = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.record_callback"
    ).count()
    edges = OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count()
    members = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count()
    return (
        f"statuses {statuses}; execution order {order()}; "
        f"edge rows left {edges}, node rows left {members}; "
        f"{record}; callbacks enqueued {callbacks}"
    )


def assert_dependency_order(ran, depends_on):
    """Every node that ran did so after each of its upstreams had run."""
    for position, name in enumerate(ran):
        for upstream in depends_on.get(name, []):
            assert upstream in ran[:position], (
                f"{name!r} ran before its upstream {upstream!r} (execution order {ran})"
            )


# -- an upstream that FAILED -------------------------------------------


def test_deleting_an_open_workflow_does_not_run_a_child_whose_upstream_failed():
    """
    The builder died between add() and seal(). The workflow stays OPEN,
    its nodes run gated anyway, and no reconciler tick ever cancels the
    dead cone, so the child is blocked for good. Deleting the row is the
    cleanup, and it must not turn "blocked for good" into "runs".
    """
    flow = workflows.create("abandoned-open")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")

    worker = flow_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert status_of(root.id) == OxTask.Status.FAILED
    reconcile()  # a healthy tick; it does not look at OPEN workflows
    assert worker.run_once() is False  # the gate holds the child
    assert order() == ["root", "root", "root"]

    deleted = operator_deletes(flow)
    reconcile()
    drain(worker)

    assert "child" not in order(), (
        f"a gating worker ran 'child' although its only upstream FAILED; "
        f"delete went through: {deleted}; {aftermath(flow, root=root, child=child)}"
    )


def test_deleting_a_sealed_workflow_before_cancellation_does_not_run_the_cone():
    """
    SEALED, and the root has just FAILED. No reconciler tick has cancelled
    the cone yet: either the delete lands inside the tick interval, or
    nothing schedules the reconciler (oxpull.W003, a warning only).
    """
    flow = workflows.create(
        "sealed",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    root = flow.add("root", tasks.failing_step, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    leaf = flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.seal()

    worker = flow_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert status_of(root.id) == OxTask.Status.FAILED
    assert worker.run_once() is False  # the gate holds mid and leaf

    deleted = operator_deletes(flow)
    reconcile()
    drain(worker)

    downstream = [name for name in order() if name != "root"]
    assert downstream == [], (
        f"a gating worker ran {downstream} downstream of a FAILED root; "
        f"delete went through: {deleted}; "
        f"{aftermath(flow, root=root, mid=mid, leaf=leaf)}"
    )


# -- an upstream that has not finished ---------------------------------


def test_deleting_a_workflow_waiting_on_a_retry_does_not_run_the_child_early():
    """
    A healthy reconciler cannot help here: nothing is dead, so there is
    nothing to cancel. The root's first attempt failed and its retry is
    due in ten minutes. The workflow looks stuck and is deleted, and the
    child must still wait for a root that has not succeeded.
    """
    flow = workflows.create("waiting")
    root = flow.add("root", tasks.failing_step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()

    worker = OxpullWorker(
        backend_alias="flows", backoff_initial=600, poll_interval=0.01
    )
    assert worker.run_once() is True  # attempt 1 fails; retry backs off 600s
    assert status_of(root.id) == OxTask.Status.READY
    assert reconcile() == 0
    assert worker.run_once() is False  # root not due yet, child gated

    deleted = operator_deletes(flow)
    reconcile()
    drain(worker)

    root_row = OxTask.objects.get(id=root.id)
    assert "child" not in order(), (
        f"a gating worker ran 'child' while its upstream was {root_row.status} "
        f"with attempts {root_row.attempts}/{root_row.max_attempts} and a retry "
        f"due at {root_row.run_after}; delete went through: {deleted}; "
        f"{aftermath(flow, root=root, child=child)}"
    )


def test_deleting_an_abandoned_open_chain_keeps_whatever_runs_in_dependency_order():
    """
    The priorities make claim order the reverse of dependency order, as in
    test_workflows.diamond, so only the gate can put this chain in order.
    """
    flow = workflows.create("abandoned-chain")
    flow.add("extract", tasks.step.using(priority=0), "extract")
    flow.add(
        "transform", tasks.step.using(priority=5), "transform", depends_on="extract"
    )
    flow.add("load", tasks.step.using(priority=10), "load", depends_on="transform")
    # never sealed: the builder crashed after its last add()

    operator_deletes(flow)
    reconcile()
    drain(flow_worker())

    assert_dependency_order(order(), {"transform": ["extract"], "load": ["transform"]})


# -- DEFER_BLOCKED_NODES -----------------------------------------------


def test_deferred_the_reconciler_does_not_release_a_child_whose_upstream_failed():
    """
    On the backend that still carries the leftover DEFER_BLOCKED_NODES
    option, which changes nothing now. The child waits, so the delete alone
    could not make it claimable. The reconciler's release backstop runs on
    the next tick, and it must still not release the child.
    """
    flow = workflows.create("abandoned-deferred")
    root = flow.add("root", tasks.failing_deferred_step, "root")
    child = flow.add("child", tasks.deferred_step, "child", depends_on="root")

    worker = deferring_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert status_of(root.id) == OxTask.Status.FAILED
    reconcile()
    assert worker.run_once() is False

    deleted = operator_deletes(flow)
    deferred_after_delete = run_after("child")
    claimed_before_tick = worker.run_once()
    reconcile()  # the scheduled reconciler's next tick
    drain(worker)

    assert "child" not in order(), (
        f"'child' ran although its only upstream FAILED. Right after the "
        f"delete its run_after was {deferred_after_delete} and a claim "
        f"returned {claimed_before_tick}; one reconciler tick later it ran. "
        f"delete went through: {deleted}; {aftermath(flow, root=root, child=child)}"
    )
