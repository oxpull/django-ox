"""
The guarantees a workflow makes that no single query can enforce.

Each test here is written against a property an operator depends on: that
an unscheduled reconciler is reported rather than silently tolerated, that
blocked rows do not become permanent, that an idle sealed workflow costs no
write on a tick, that a worker without the gate never runs a node twice,
and that cancellation survives a concurrent operator retry.

One property is accepted rather than enforced and is marked
`xfail(strict)`, so it stays visible without holding the suite red. Strict
turns an unexpected pass into a failure, so changing that behaviour forces
this file to be revisited.
"""

import pytest
from django.db import connections
from django.tasks import task_backends
from django.test import override_settings
from django.test.utils import CaptureQueriesContext

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import (
    diamond,
    flow_worker,
    put_back_to_ready,
    reconcile,
    stored,
    without_the_workflow_reconciler,
)

pytestmark = pytest.mark.django_db


# The reconciler schedule, and what happens without one


def test_a_workflow_backend_with_no_reconciler_schedule_is_flagged():
    """
    A backend with workflows enabled and no `oxpull.workflows.reconcile`
    schedule warns, as oxpull.W003.

    Unflagged, nothing cancels an unreachable
    cone and nothing ever completes a workflow, so callbacks never fire
    and blocked READY rows accumulate in the claim's candidate set
    permanently. The claim cost is linear in that count, which makes it a
    silent, unbounded throughput regression on the hot path.

    _check_workflow_reconciler now covers it, following the idiom
    _check_unique_schedules already set for reading OPTIONS['SCHEDULES'].
    """
    with override_settings(TASKS=without_the_workflow_reconciler()):
        backend = task_backends["flows"]
        # the 'flows' backend schedules nothing
        assert "SCHEDULES" not in backend.options
        errors = backend.check()
    ids = {e.id for e in errors}
    assert "oxpull.W003" in ids or any(
        "reconcile" in str(e.msg).lower() for e in errors
    ), (
        "no check warns that oxpull.workflows.reconcile is unscheduled; "
        f"checks returned {[(e.id, e.msg) for e in errors]}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Accepted, not fixed: with no reconciler scheduled there is no "
        "bounded-time recovery. _check_workflow_reconciler warns at "
        "manage.py check, and detection is not recovery. Fixing this means "
        "recovering without a schedule; if that ever lands, strict xfail "
        "turns the unexpected pass into a failure and this test is rewritten."
    ),
)
def test_without_the_reconciler_blocked_rows_are_permanent():
    """
    The consequence, measured. A failed root leaves its whole cone WAITING
    forever if no reconciler ever runs, and the workflow never completes.

    Everything up to the final assertion is true of the shipped code and
    is worth pinning. The final assertion is the state we would want and
    do not have.
    """
    flow = diamond("no-reconciler")
    ids = flow.node_ids()
    OxTask.objects.filter(id=ids["extract"]).update(status=OxTask.Status.FAILED)

    worker = flow_worker()
    assert worker.claim_one() is None, "nothing should be claimable"
    blocked = flow.progress().blocked
    assert blocked == 3, blocked
    # No reconciler runs. The rows still wait, indefinitely.
    assert (
        OxTask.objects.filter(id__in=ids.values(), status=OxTask.Status.WAITING).count()
        == 3
    )
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    # The expected failure. See the xfail reason above.
    assert blocked == 0, (
        "3 workflow nodes wait forever, and the workflow never completes"
    )


# What a reconciler tick costs when nothing is cancellable


def test_an_idle_sealed_workflow_costs_no_write_on_a_reconciler_tick():
    """
    Eight sealed workflows with nothing cancellable produce no UPDATE on
    the workflow table.

    Opening a transaction and issuing a real UPDATE before reading
    anything would cost a write transaction per sealed workflow per tick,
    whether or not anything was cancellable. At a once-a-minute cron that
    is a write per workflow per minute forever, and on SQLite those
    serialise against the workers. Both the cancel path and
    _complete_if_finished read a cheap filter first.
    """
    flows = []
    for i in range(8):
        f = workflows.create(f"idle-{i}")
        f.add("only", tasks.step, f"n{i}")
        f.seal()
        flows.append(f)
    # Nothing is doomed: every node is simply still READY.
    conn = connections["default"]
    with CaptureQueriesContext(conn) as ctx:
        reconcile()
    writes = [
        q
        for q in ctx.captured_queries
        if q["sql"].lstrip().upper().startswith("UPDATE")
    ]
    updates_on_workflow = [
        q for q in writes if OxpullWorkflow._meta.db_table in q["sql"]
    ]
    assert len(updates_on_workflow) == 0, (
        f"{len(updates_on_workflow)} no-op UPDATE(s) on the workflow table for "
        f"8 idle sealed workflows, one write transaction each, every tick:\n"
        + "\n".join(q["sql"][:120] for q in updates_on_workflow[:3])
    )


# What an ungated worker on the same queue does


@pytest.mark.django_db(transaction=True)
def test_an_ungated_worker_runs_nodes_out_of_order_but_never_twice():
    """
    A worker on an ungated backend draining the same queue violates the
    DAG order. It never runs a node twice.

    Both halves matter. The claim is a compare-and-set on one row on
    every path, so no arrangement of workers can double-execute; the gate
    is only ever an extra conjunct on which rows are candidates, so a
    worker without it simply ignores the ordering. The failure mode of a
    mixed fleet is correctness of the DAG, not duplication.
    """
    from django_ox.worker import Worker

    tasks.STATE["order"] = []
    # Put back to READY, the shape a django-ox retry leaves, so the ungated
    # worker can reach the nodes the WAITING status would hold from it.
    put_back_to_ready(diamond("mixed-fleet"))
    gated, ungated = flow_worker(), Worker(backoff_initial=0, poll_interval=0.01)

    ran = 0
    while ran < 50:
        # Alternate: the ungated worker wins some claims, the gated one others.
        if ungated.run_once():
            ran += 1
            continue
        if gated.run_once():
            ran += 1
            continue
        break

    order = list(tasks.STATE.get("order", []))
    assert ran == 4, ran
    # Never twice: exactly one execution per node.
    assert sorted(order) == ["extract", "load", "transform-a", "transform-b"]
    assert len(order) == len(set(order)), f"a node ran twice: {order}"
    # Out of order: the ungated worker took `load` first, before its parents.
    assert order[0] == "load", (
        f"expected the ungated worker to violate the DAG, got {order}"
    )


# The cancel path's lock ordering under contention


@pytest.mark.django_db(transaction=True)
def test_cancel_survives_a_concurrent_operator_retry_under_contention():
    """
    Six threads alternating reconciler passes and operator retries against
    one workflow, on file-backed SQLite, raise no OperationalError.

    The cancel path opens with a write to take the lock before reading.
    This hammers that ordering rather than trusting it: papering over
    shows up as
    OperationalError('database is locked'), correct lock ordering shows up
    as waits.
    """
    import contextlib
    import threading

    from django.db import OperationalError
    from django.db import connections as conns

    from django_ox import actions

    flow = diamond("contended")
    ids = flow.node_ids()
    OxTask.objects.filter(id=ids["extract"]).update(status=OxTask.Status.FAILED)

    errors, barrier = [], threading.Barrier(6)

    def reconciler():
        barrier.wait()
        try:
            for _ in range(15):
                workflows._reconcile()
        except OperationalError as exc:  # the failure this guards against
            errors.append(("reconcile", str(exc)))
        finally:
            conns.close_all()

    def retrier():
        barrier.wait()
        try:
            for _ in range(15):
                row = OxTask.objects.filter(id=ids["extract"]).first()
                if row and row.status == OxTask.Status.FAILED:
                    # A losing racer is the point; only a lock error matters.
                    with contextlib.suppress(Exception):
                        actions.retry(row.id)
                OxTask.objects.filter(id=ids["extract"]).update(
                    status=OxTask.Status.FAILED
                )
        except OperationalError as exc:
            errors.append(("retry", str(exc)))
        finally:
            conns.close_all()

    threads = [threading.Thread(target=reconciler) for _ in range(3)] + [
        threading.Thread(target=retrier) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"lock contention surfaced as errors: {errors[:3]}"
