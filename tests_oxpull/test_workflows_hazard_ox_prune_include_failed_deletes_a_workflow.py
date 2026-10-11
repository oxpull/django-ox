"""
ox_prune --include-failed deletes a workflow node an operator
has already retried, and the gate then lets its child run.

django-ox 1.2.0's ox_prune deletes each batch with
``prunable.filter(pk__in=batch).delete()``, and its comment says the DELETE
re-applies the selection predicate. It does not. OxScheduleTick.task is a
SET_NULL foreign key to OxTask, so QuerySet.delete() cannot fast-delete and
takes Django's Collector path: an unlocked SELECT of the rows matching
status and finished_at, then ``UPDATE <tick table> SET task_id = NULL``,
then ``DELETE FROM <task table> WHERE id IN (<pks>)`` with no status
predicate. An actions.retry() that commits between that SELECT and that
DELETE does not save the row.

For a workflow node that mattered twice. runnable_filter_q() read an absent
upstream as satisfied, so the child became claimable although its parent
never succeeded. And if a worker had already claimed the retried parent,
that execution ran on with no row: its renewal and its outcome write matched
nothing, and the child could run beside it. The django-ox this package pins
re-applies the status predicate in that DELETE, and the claim's dependency
check now blocks on an absent upstream with nothing recorded.

The window is widened deterministically with connection.execute_wrapper on
ox_prune's own connection. Just before ox_prune's first UPDATE or DELETE
(after the deletion collector has selected its rows), the retry, and where
named a claim or a whole worker execution, runs on other threads with their
own connections through django-ox's public paths. Every statement ox_prune
issues is its real statement, from the real management command. Nothing in
this file measures how often the unwidened window is hit in production.

Shapes:
  open                 an OPEN workflow with the reconciler run before the
                       prune, as a scheduled one would be. The reconciler
                       sweeps SEALED workflows only, so it never cancels
                       the child here.
  sealed-unreconciled  a SEALED workflow whose reconciler has not run
                       (nothing schedules it: oxpull.W003 is a warning,
                       and create() and seal() do not refuse).
  sealed-reconciled    a SEALED workflow whose reconciler ran after the
                       root failed, so the child is already DISCARDED.
                       Only the lost retry survives this shape; it pins
                       how far the hazard reaches with a working reconciler.

Every test asserts the correct behaviour. The in-window tests fail on
django-ox 1.2.0 and pass with the current django-ox implementation. The control
(retry before the prune) passes on both.
"""

import threading
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection, connections
from django.tasks import task
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows

from . import tasks
from .test_workflows import flow_worker, reconcile
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db(transaction=True)

SHAPES = ["open", "sealed-unreconciled", "sealed-reconciled"]

# Past ox_prune's default --older-than of 7d.
PRUNE_AGE = timedelta(days=8)


@task(backend="flows")
def parent_that_fails_then_holds(label):
    """
    Fails every attempt until the test arms STATE["started"]; after that,
    records that it started, signals, and holds until STATE["finish"] is
    set, then succeeds. Lets a real worker be mid-execution of the retried
    parent while ox_prune deletes its row.
    """
    state = tasks.STATE
    if "started" not in state:
        tasks._record(label)
        raise ValueError(f"{label} boom")
    tasks._record(f"{label}:started")
    state["started"].set()  # type: ignore[attr-defined]
    if not state["finish"].wait(timeout=30):  # type: ignore[attr-defined]
        raise RuntimeError("the test never let the parent finish")
    tasks._record(f"{label}:finished")
    return label


# django-ox's ox_prune takes a locking read of each batch before its
# DELETE on PostgreSQL and MySQL. The in-window work here runs inside that
# transaction, so a retry of a selected row waits for ox_prune to commit and
# cannot land inside the window this harness means to widen.
lock_holds_the_window = pytest.mark.xfail(
    connection.vendor in ("postgresql", "mysql"),
    strict=True,
    reason="django-ox's ox_prune locks the rows it selected, so the "
    "retry this harness makes inside its transaction waits for the commit and "
    "never lands in the window; the window has to move before that read",
)


# -- harness -----------------------------------------------------------


def elsewhere(fn):
    """Run fn on its own thread and its own connection; return its result."""
    result, error = [], []

    def run():
        try:
            result.append(fn())
        except BaseException as exc:
            error.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=30)
    assert not thread.is_alive(), "in-window work did not finish"
    if error:
        raise error[0]
    return result[0]


def prune_with(in_window):
    """
    The real `ox_prune --include-failed`, calling in_window() once, on this
    thread, immediately before ox_prune's first UPDATE or DELETE. By then
    the deletion collector has run its SELECT and fixed the pk list the
    DELETE will use. Returns (stdout, statements ox_prune issued).
    """
    statements = []
    fired = []

    def pause_before_first_write(execute, sql, params, many, context):
        head = sql.lstrip().split(None, 1)[0].upper()
        statements.append(" ".join(sql.split())[:90])
        if head in ("UPDATE", "DELETE") and not fired:
            fired.append(True)
            in_window()
        return execute(sql, params, many, context)

    out = StringIO()
    with connection.execute_wrapper(pause_before_first_write):
        call_command("ox_prune", "--include-failed", stdout=out)
    assert fired, "ox_prune issued no write; the window was never reached"
    return out.getvalue().strip(), statements


def prepare(
    shape,
    *,
    root_task=tasks.failing_step,
    child_task=None,
    worker_factory=flow_worker,
):
    """
    root -> child, root run through every attempt to FAILED by a real
    worker, the reconciler run or not per shape, and root's finished_at
    backdated past the prune cutoff.

    The backdate is the one row written by hand. Production reaches the
    same state by the root having failed eight days ago.
    """
    if child_task is None:
        child_task = tasks.step.using(priority=9)
    flow = workflows.create(f"prune-{shape}")
    root = flow.add("root", root_task, "root")
    child = flow.add("child", child_task, "child", depends_on="root")
    if shape != "open":
        flow.seal()
    worker = worker_factory()
    for _ in range(OxTask.objects.get(id=root.id).max_attempts):
        worker.run_once()
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.FAILED
    if shape != "sealed-unreconciled":
        reconcile()
    OxTask.objects.filter(id=root.id).update(finished_at=timezone.now() - PRUNE_AGE)
    connections.close_all()
    return flow, root, child


def is_node(claimed, node):
    return claimed is not None and str(claimed.pk) == str(node.id)


def status_of(node):
    row = OxTask.objects.filter(id=node.id).first()
    return row.status if row is not None else "DELETED"


def report(**facts):
    statements = facts.pop("statements", [])
    print(  # noqa: T201
        f"\nS3 vendor={connection.vendor} "
        + " ".join(f"{k}={v!r}" for k, v in facts.items())
        + "".join(f"\n  ox_prune: {s}" for s in statements)
    )


# -- the control -------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES)
def test_control_a_node_retried_before_ox_prune_survives_and_gates_its_child(shape):
    _, root, child = prepare(shape)

    retried = elsewhere(lambda: actions.retry(root.id))
    out = StringIO()
    call_command("ox_prune", "--include-failed", stdout=out)
    root_after = status_of(root)
    claimed = flow_worker().claim_one()

    report(
        shape=shape,
        retried=retried,
        prune=out.getvalue().strip(),
        root_after=root_after,
        child=status_of(child),
        next_claim=claimed.args if claimed else None,
    )
    assert retried is True
    assert root_after != "DELETED"
    assert not is_node(claimed, child)


# -- the hazard --------------------------------------------------------


@lock_holds_the_window
@pytest.mark.parametrize("shape", SHAPES)
def test_a_node_retried_inside_ox_prune_keeps_its_row_and_its_child_stays_gated(
    shape,
):
    _, root, child = prepare(shape)
    retried = []

    out, statements = prune_with(
        lambda: retried.append(elsewhere(lambda: actions.retry(root.id)))
    )
    root_after = status_of(root)
    claimed = flow_worker().claim_one()

    report(
        shape=shape,
        retried=retried,
        prune=out,
        root_after=root_after,
        child=status_of(child),
        next_claim=claimed.args if claimed else None,
        statements=statements,
    )
    assert retried == [True], "precondition: the retry must land inside the window"
    assert root_after == OxTask.Status.READY, (
        "ox_prune deleted a node an operator had already retried"
    )
    assert not is_node(claimed, child), (
        "the child was claimed although its upstream never succeeded"
    )


@lock_holds_the_window
@pytest.mark.parametrize("shape", SHAPES)
def test_a_node_retried_and_claimed_inside_ox_prune_keeps_its_row_and_gates_its_child(
    shape,
):
    _, root, child = prepare(shape)
    retried, claimed_in_window = [], []

    def operator_then_worker():
        retried.append(elsewhere(lambda: actions.retry(root.id)))
        got = elsewhere(lambda: flow_worker().claim_one())
        claimed_in_window.append(got.args if got else None)

    out, statements = prune_with(operator_then_worker)
    root_after = status_of(root)
    claimed = flow_worker().claim_one()

    report(
        shape=shape,
        retried=retried,
        claimed_in_window=claimed_in_window,
        prune=out,
        root_after=root_after,
        child=status_of(child),
        next_claim=claimed.args if claimed else None,
        statements=statements,
    )
    assert retried == [True], "precondition: the retry must land inside the window"
    assert claimed_in_window == [["root"]], "precondition: a worker holds the root"
    assert root_after == OxTask.Status.RUNNING, (
        "ox_prune deleted a node a worker was running after an operator retry"
    )
    assert not is_node(claimed, child), (
        "the child was claimed while its upstream was still running"
    )


@lock_holds_the_window
@pytest.mark.parametrize("shape", SHAPES)
def test_a_retried_node_executing_across_ox_prune_keeps_its_outcome_and_its_child_waits(
    shape, caplog
):
    """
    The whole execution, on a real worker: retried and claimed inside the
    window, still running when ox_prune commits. The child must not run
    until the parent succeeds, and the parent's success must land.
    """
    _, root, child = prepare(shape, root_task=parent_that_fails_then_holds)
    started, finish = threading.Event(), threading.Event()
    tasks.STATE["started"] = started
    tasks.STATE["finish"] = finish

    holder = flow_worker()
    held, errors, retried = [], [], []

    def hold():
        try:
            held.append(holder.run_once())
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    holder_thread = threading.Thread(target=hold)

    def operator_then_worker():
        retried.append(elsewhere(lambda: actions.retry(root.id)))
        holder_thread.start()
        assert started.wait(timeout=30), "the worker never started the retried root"

    try:
        out, statements = prune_with(operator_then_worker)
        root_while_running = status_of(root)
        renewed = holder.renew_leases()
        child_ran = flow_worker().run_once()
        order_while_running = list(tasks.STATE.get("order", []))
    finally:
        finish.set()
        if holder_thread.ident is not None:
            holder_thread.join(timeout=30)
            assert not holder_thread.is_alive(), "the holding worker never finished"
    assert not errors, errors
    root_after = status_of(root)
    lease_lost = [
        record.getMessage()
        for record in caplog.records
        if getattr(record, "event", None) == "task_lease_lost"
    ]

    report(
        shape=shape,
        retried=retried,
        prune=out,
        root_while_running=root_while_running,
        renewals_that_matched=renewed,
        child_ran_while_root_running=child_ran,
        order_while_running=order_while_running,
        holder_ran=held,
        root_after=root_after,
        child=status_of(child),
        lease_lost_logged=lease_lost,
        statements=statements,
    )
    assert retried == [True], "precondition: the retry must land inside the window"
    assert not child_ran, "the child ran while its retried upstream was still executing"
    assert root_after == OxTask.Status.SUCCESSFUL, (
        "the retried root succeeded but its outcome write matched no row"
    )


@lock_holds_the_window
@pytest.mark.parametrize("shape", ["open", "sealed-unreconciled"])
def test_deferral_sweep_keeps_the_child_of_a_node_retried_inside_ox_prune(shape):
    """
    The child waits, so the deleted parent alone does not make it claimable.
    The reconciler's release backstop runs next, and it must not release the
    child of a parent that never succeeded. (sealed-reconciled is left out:
    the cancellation pass has already discarded the child, and the lost
    retry is covered above.)
    """
    _, root, child = prepare(
        shape,
        root_task=tasks.failing_deferred_step,
        child_task=tasks.deferred_step.using(priority=9),
        worker_factory=deferring_worker,
    )
    assert OxTask.objects.get(id=child.id).status == OxTask.Status.WAITING
    retried = []

    out, statements = prune_with(
        lambda: retried.append(elsewhere(lambda: actions.retry(root.id)))
    )
    root_after = status_of(root)
    child_deferred_after_prune = (
        OxTask.objects.get(id=child.id).status == OxTask.Status.WAITING
    )
    reconcile()
    child_deferred_after_reconcile = (
        OxTask.objects.get(id=child.id).status == OxTask.Status.WAITING
    )
    claimed = deferring_worker().claim_one()

    report(
        shape=shape,
        retried=retried,
        prune=out,
        root_after=root_after,
        child_deferred_after_prune=child_deferred_after_prune,
        child_deferred_after_reconcile=child_deferred_after_reconcile,
        next_claim=claimed.args if claimed else None,
        statements=statements,
    )
    assert retried == [True], "precondition: the retry must land inside the window"
    assert root_after == OxTask.Status.READY, (
        "ox_prune deleted a node an operator had already retried"
    )
    assert child_deferred_after_reconcile, (
        "the reconciler released a child whose upstream never succeeded"
    )
    assert not is_node(claimed, child), (
        "the child was claimed although its upstream never succeeded"
    )
