"""
A workflow build that raises part-way.

Each add() is its own atomic block (workflows.py:419), so outside a
caller's transaction every node commits the moment it is added. The
module docstring's own example (workflows.py:5-12) builds with no
transaction, and nothing tells a caller to wrap one. When a later add()
raises, or code between two adds raises, the committed prefix is READY
with its gates satisfied, and workers claim it at once.

A caller then has three things to do, and each one goes wrong:

1. leave the workflow OPEN: the prefix runs, and a dead root's cone
   stays READY for good, because the reconciler sweeps SEALED only;
2. run the build again: create() makes a second workflow, so the
   prefix runs twice;
3. seal() it as cleanup: total_count freezes at the prefix and, once
   the prefix succeeds, on_success fires for nodes that were never added.

Every test asserts the behaviour a user needs, so it fails today.
All of them run with real commits (transaction=True) and a real
OxpullWorker. The controls at the bottom pass today, and show the
instrument can see a node run and a callback fire.
"""

import contextlib
import time
from pprint import pformat

import pytest
from django.db import transaction
from django.tasks import task

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.exceptions import UnknownWorkflowNode, WorkflowSealed
from oxpull.models import OxpullWorkflow
from oxpull.routing import oxpull_db_alias

from . import tasks
from .racing import race_all
from .test_workflows import drain, flow_worker, order, reconcile

pytestmark = pytest.mark.django_db(transaction=True)

RECONCILER_TICKS = 3


def build_checkout(flow, first_node=tasks.step):
    """
    The documented build, with a typo in the third node's depends_on.

    UnknownWorkflowNode is one of several things that can raise here;
    WorkflowNodeExists, InvalidTask, a database error or a killed
    request leave the same committed prefix.
    """
    flow.add("charge", first_node, "charge")
    flow.add("ship", tasks.step, "ship", depends_on="charge")
    flow.add("email", tasks.step, "email", depends_on=["ship", "chrage"])
    flow.seal()


def customer_cleanup_after_a_failed_build(flow):
    """
    What a caller does once the build has raised.

    Today there is nothing to call: workflows.__all__ (workflows.py:105-118)
    has no abort or cancel for an OPEN workflow. A fix that adds one calls
    it here. A fix that rolls a failed build back needs nothing here.
    """


def on_its_own_connection(target):
    """Run target on another thread, which sees only committed rows."""
    results, errors = race_all([target])
    assert errors == [], errors
    return results[0]


def settle(worker, rounds=3):
    """
    Drain, pausing so a zero-backoff retry is eligible, until idle.

    django-ox notes that on SQLite a retry written with no backoff can be
    ineligible for up to a millisecond, which would end one drain early.
    """
    ran = 0
    for _ in range(rounds):
        ran += drain(worker)
        time.sleep(0.02)
    return ran


def workflow_row(flow):
    return OxpullWorkflow.objects.filter(pk=flow.id).values().first()


# -- exit 1: leave it OPEN --------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Each add() commits on its own, so a build that raises part-way leaves "
        "READY nodes that a worker claims and runs"
    ),
)
def test_a_build_that_raises_part_way_leaves_nothing_a_worker_can_claim():
    """
    Brief's fix criterion: claim_one() is None after a failed build.

    The claim runs on another connection, so it sees only what add()
    committed, which is what a worker process sees.
    """
    flow = workflows.create(
        "checkout",
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    with pytest.raises(UnknownWorkflowNode):
        build_checkout(flow)
    customer_cleanup_after_a_failed_build(flow)

    def claim_and_run():
        worker = flow_worker()
        first = worker.claim_one()
        claimed = None if first is None else list(first.args)
        if first is not None:
            worker.execute(first, inline=True)
        return claimed, drain(worker)

    claimed, drained = on_its_own_connection(claim_and_run)
    observed = {"first_claim": claimed, "ran": order()}
    context = {
        "drained_after_first": drained,
        "reconcile": reconcile(),
        "workflow": workflow_row(flow),
    }
    assert observed == {"first_claim": None, "ran": []}, pformat(context)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "In a build left OPEN after an error, a node whose upstream failed stays "
        "pending forever, because the reconciler only cancels in sealed workflows"
    ),
)
def test_an_abandoned_build_whose_root_failed_does_not_hold_a_ready_node_forever():
    """
    The committed root fails for good. Its child can never run, and
    nothing ends it: the reconciler cancels cones in SEALED workflows
    only, so the child stays pending (WAITING) and ox_prune, which deletes
    settled rows, never reaches it.
    """
    flow = workflows.create("checkout-failing-root")
    with pytest.raises(UnknownWorkflowNode):
        build_checkout(flow, first_node=tasks.failing_step)
    customer_cleanup_after_a_failed_build(flow)
    committed = flow.node_ids()

    ran = on_its_own_connection(lambda: settle(flow_worker()))
    ticks = [reconcile() for _ in range(RECONCILER_TICKS)]

    statuses = dict(
        OxTask.objects.filter(id__in=committed.values()).values_list("id", "status")
    )
    by_name = {name: statuses.get(task_id) for name, task_id in committed.items()}
    left_ready = sorted(
        name
        for name, status in by_name.items()
        if status in (OxTask.Status.READY, OxTask.Status.WAITING)
    )
    context = {
        "tasks_run": ran,
        "order": order(),
        "reconcile_ticks": ticks,
        "node_status": by_name,
        "workflow_status": (workflow_row(flow) or {}).get("status"),
        "progress_blocked": flow.progress().blocked if workflow_row(flow) else None,
    }
    assert left_ready == [], pformat(context)


# -- exit 2: run the build again ---------------------------------------


@task
def build_checkout_in_a_task(order_id):
    """
    A build run as a task, which is how a build usually runs.

    The first attempt dies between two adds, as a request or a task does
    when a lookup times out. django-ox's retry runs the body again, and a
    retry is exactly what a transient failure asks for.
    """
    attempts = int(tasks.STATE.get("build_attempts", 0)) + 1
    tasks.STATE["build_attempts"] = attempts
    flow = workflows.create(
        f"checkout-{order_id}",
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    flow.add("charge", tasks.step, "charge")
    flow.add("ship", tasks.step, "ship", depends_on="charge")
    if attempts <= int(tasks.STATE.get("fail_attempts", 0)):
        raise TimeoutError("address lookup timed out")
    flow.add("email", tasks.step, "email", depends_on="ship")
    flow.seal()
    return str(flow.id)


def run_build_task_and_everything_it_enqueues():
    build_checkout_in_a_task.enqueue(42)
    worker = flow_worker()
    ran = on_its_own_connection(lambda: settle(worker))
    ticks = [reconcile() for _ in range(RECONCILER_TICKS)]
    # Callbacks are ordinary tasks; run them the way a worker would.
    ran += on_its_own_connection(lambda: settle(flow_worker()))
    return ran, ticks


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Retrying a build that raised creates a second workflow, so the nodes "
        "added before the error run twice"
    ),
)
def test_retrying_a_build_that_raised_runs_each_node_once():
    """
    The build task raises on attempt 1 and succeeds on attempt 2. The
    checkout must charge once.
    """
    tasks.STATE["fail_attempts"] = 1
    ran, ticks = run_build_task_and_everything_it_enqueues()

    ran_order = order()
    observed = {name: ran_order.count(name) for name in ("charge", "ship", "email")}
    context = {
        "build_attempts": tasks.STATE.get("build_attempts"),
        "tasks_run": ran,
        "order": ran_order,
        "reconcile_ticks": ticks,
        "workflows": sorted(
            OxpullWorkflow.objects.values_list("status", "total_count")
        ),
        "on_success_runs": tasks.STATE.get("success"),
    }
    assert observed == {"charge": 1, "ship": 1, "email": 1}, pformat(context)


# -- exit 3: seal() as cleanup -----------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Sealing a build that raised part-way completes it with only the nodes "
        "that were added, and on_success fires"
    ),
)
def test_sealing_a_failed_build_does_not_fire_on_success():
    """
    Brief's fix criterion: success_task_id is None after sealing a failed
    build. The DAG the caller declared had three nodes; one was never
    added. on_success says the run was clean, and it was not.

    seal() raising for a failed build, or the workflow row no longer
    existing, are both acceptable ways to refuse; what is not acceptable
    is on_success firing.
    """
    flow = workflows.create(
        "checkout-sealed-as-cleanup",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    with pytest.raises(UnknownWorkflowNode):
        build_checkout(flow)
    seal_refused = True
    with contextlib.suppress(WorkflowSealed, OxpullWorkflow.DoesNotExist):
        flow.seal()
        seal_refused = False

    ran = on_its_own_connection(lambda: settle(flow_worker()))
    ticks = [reconcile() for _ in range(RECONCILER_TICKS)]
    ran += on_its_own_connection(lambda: settle(flow_worker()))

    row = workflow_row(flow) or {}
    observed = {
        "success_task_id": row.get("success_task_id"),
        "on_success_runs": tasks.STATE.get("success"),
    }
    context = {
        "seal_refused": seal_refused,
        "tasks_run": ran,
        "order": order(),
        "reconcile_ticks": ticks,
        "status": row.get("status"),
        "total_count": row.get("total_count"),
        "callback_task_id": row.get("callback_task_id"),
        "on_complete_runs": tasks.STATE.get("complete"),
    }
    assert observed == {"success_task_id": None, "on_success_runs": None}, pformat(
        context
    )


# -- controls: pass today ----------------------------------------------


def test_control_the_same_build_inside_one_transaction_leaves_nothing():
    """
    The mechanism, shown by its absence: the identical build inside
    transaction.atomic(using=oxpull_db_alias()) commits nothing, so a
    worker on another connection finds nothing and nothing is left over.
    """
    with (
        pytest.raises(UnknownWorkflowNode),
        transaction.atomic(using=oxpull_db_alias()),
    ):
        flow = workflows.create(
            "checkout-atomic",
            on_success=tasks.record_callback,
            on_success_args=["success"],
        )
        build_checkout(flow)

    claimed = on_its_own_connection(lambda: flow_worker().claim_one())
    assert claimed is None
    assert order() == []
    assert OxTask.objects.count() == 0
    assert OxpullWorkflow.objects.count() == 0


def test_control_a_build_task_that_does_not_raise_runs_each_node_once():
    """
    The same task path with no failure: every node runs once, the
    workflow completes and on_success runs. So the instrument sees node
    runs and callbacks, and test_retrying... and test_sealing... are not
    reading zeros they could never have observed.
    """
    tasks.STATE["fail_attempts"] = 0
    _, ticks = run_build_task_and_everything_it_enqueues()

    ran_order = order()
    assert tasks.STATE.get("build_attempts") == 1
    assert {name: ran_order.count(name) for name in ("charge", "ship", "email")} == {
        "charge": 1,
        "ship": 1,
        "email": 1,
    }
    assert ticks[0] == 1
    (row,) = OxpullWorkflow.objects.values("status", "total_count", "success_task_id")
    assert row["status"] == OxpullWorkflow.Status.COMPLETE
    assert row["total_count"] == 3
    assert row["success_task_id"] is not None
    assert tasks.STATE.get("success") == 1


def test_control_the_build_runs_on_a_thread_that_sees_committed_rows_only():
    """
    on_its_own_connection really is another connection: a row inside an
    uncommitted transaction on this thread is invisible to it.
    """
    with transaction.atomic(using=oxpull_db_alias()):
        tasks.step.enqueue("uncommitted")
        seen = on_its_own_connection(
            lambda: OxTask.objects.filter(args=["uncommitted"]).exists()
        )
    assert seen is False
    assert OxTask.objects.filter(args=["uncommitted"]).exists()
