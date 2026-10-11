"""
An abandoned OPEN workflow (or batch) is sealed by hand in SQL.

A builder that dies between add() and seal() leaves its workflow OPEN for
good: the reconciler sweeps SEALED rows only. The obvious support fix is
one statement, UPDATE ... SET status = 'SEALED'. That leaves total_count
NULL, because seal() is its only writer, and the completing transaction
reads `(row.total_count or 0)`, turning an unknown denominator into zero.
Every node that is DISCARDED, LOST or pruned then stops counting as
unattributed, and on_success fires for a run that was not clean.

Each scenario runs twice:

- hand_sealed_sql: the literal UPDATE a support engineer would issue.
  These assert the correct behaviour and fail today.
- control_public_seal: the same state sealed through workflows.get(id).seal()
  (or batches.get(id).seal()) from a fresh handle, the public way to finish
  an abandoned build from another process. These pass today. They show the
  assertions can pass, and that the only difference is the NULL
  total_count.

A sealed row with no total now completes on its live count, which is the
total seal() would have stored, and the completion writes it. The
assertions ask for that much and no more: the row completes and enqueues
on_complete, and nothing enqueues on_success or records a completion
whose unattributed count disagrees with progress(). Refusing to complete
would strand the row SEALED, so it fails them too.
"""

import logging
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connections
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.models import OxpullBatch, OxpullWorkflow
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_batches import reconcile as reconcile_batches
from .test_workflows import drain, flow_worker
from .test_workflows import reconcile as reconcile_workflows

pytestmark = pytest.mark.django_db

HAND_SEALED = "hand_sealed_sql"
CONTROL = "control_public_seal"
SEAL_HOW = [HAND_SEALED, CONTROL]

CALLBACKS = {
    "on_complete": tasks.record_callback,
    "on_complete_args": ["complete"],
    "on_success": tasks.record_callback,
    "on_success_args": ["success"],
}

# Enough ticks for any fix that needs more than one pass to settle.
TICKS = 3


def hand_seal(model, name):
    """
    The support fix, as literal SQL:

        UPDATE <table> SET status = 'SEALED', sealed_at = <now>
        WHERE name = <name> AND status = 'OPEN'

    Keyed on the name, which is what an engineer reading the table has.
    """
    connection = connections[oxpull_db_alias()]
    qn = connection.ops.quote_name
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE {qn(model._meta.db_table)} "  # noqa: S608
            f"SET {qn('status')} = %s, {qn('sealed_at')} = %s "
            f"WHERE {qn('name')} = %s AND {qn('status')} = %s",
            [
                "SEALED",
                connection.ops.adapt_datetimefield_value(timezone.now()),
                name,
                "OPEN",
            ],
        )
        assert cursor.rowcount == 1


def seal(how, module, model, handle, members):
    """Seal an abandoned build one of two ways, and prove which state it left."""
    if how == HAND_SEALED:
        hand_seal(model, handle.name)
    else:
        # A fresh handle, as a shell in another process would build it.
        module.get(handle.id).seal()
    row = model.objects.get(pk=handle.id)
    assert row.status == "SEALED"
    if how == HAND_SEALED:
        assert row.total_count is None  # the state the hazard needs
    else:
        assert row.total_count == members


def lose_to_the_reaper(worker, task_id):
    """
    Drive one task to LOST through django-ox 1.2.0's own lease path.

    Each attempt is a real claim (a stored lease expiry and a bumped
    epoch), then actions.expire_lease, then the real reaper. Attempts 1
    and 2 are requeued; attempt 3 is exhausted and written LOST. No task
    row is written by hand.
    """
    max_attempts = OxTask.objects.get(id=task_id).max_attempts
    for attempt in range(1, max_attempts + 1):
        claimed = worker.claim_one()
        assert claimed is not None and str(claimed.id) == str(task_id)
        held = OxTask.objects.get(id=task_id)
        assert held.status == OxTask.Status.RUNNING
        assert held.lease_expires_at is not None
        assert held.lease_epoch >= attempt
        assert actions.expire_lease(task_id) is True
        assert worker.reap() == 1
    lost = OxTask.objects.get(id=task_id)
    assert lost.status == OxTask.Status.LOST
    assert lost.attempts == lost.max_attempts


def judge(*, how, model, handle, reconcile, logger_name, event, caplog):
    """
    Run the reconciler and return what a correct design must not do.

    The run is not clean by construction, and progress() says so: that
    is asserted first, so a zero below cannot come from a scenario that
    provoked nothing. A workflow's blocked child is still pending here;
    only the reconciler's cancellation settles it.
    """
    before = handle.progress()
    assert before.failed + before.unattributed > 0, before

    with caplog.at_level(logging.INFO, logger=logger_name):
        passes = [reconcile() for _ in range(TICKS)]
    row = model.objects.get(pk=handle.id)
    after = handle.progress()
    logged = [
        record.unattributed_count
        for record in caplog.records
        if getattr(record, "event", None) == event
    ]
    success_rows = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.record_callback", args=["success"]
    ).count()
    observed = {
        "sealed_by": how,
        "reconcile_passes": passes,
        "status": row.status,
        "total_count": row.total_count,
        "failed_count": row.failed_count,
        "cancelled_count": getattr(row, "cancelled_count", "n/a"),
        "callback_task_id": row.callback_task_id,
        "success_task_id": row.success_task_id,
        "on_success_task_rows": success_rows,
        "logged_unattributed": logged,
        "progress": {
            "total": after.total,
            "succeeded": after.succeeded,
            "failed": after.failed,
            "unattributed": after.unattributed,
        },
    }

    problems = []
    if row.status != "COMPLETE" or row.callback_task_id is None:
        problems.append(
            f"the sealed row did not complete: it is {row.status} with "
            f"callback_task_id {row.callback_task_id}"
        )
    if row.success_task_id is not None or success_rows:
        problems.append(
            f"on_success was enqueued for a run that was not clean: "
            f"progress() reports {after.failed} failed and "
            f"{after.unattributed} unattributed of {after.total}"
        )
    if logged and logged != [after.unattributed]:
        problems.append(
            f"the completion recorded unattributed={logged} while "
            f"progress() reports {after.unattributed}"
        )
    return problems, observed


def report(problems, observed):
    return "\n".join([*problems, f"observed: {observed}"])


# -- workflows ---------------------------------------------------------


@pytest.mark.parametrize("how", SEAL_HOW)
def test_hand_sealed_workflow_with_a_discarded_root_withholds_on_success(how, caplog):
    """
    root -> child. The builder dies before seal(); an operator discards
    root, the abandoned work. Hand-sealed, the reconciler cancels child
    and completes, and on_success fires over two DISCARDED nodes.
    """
    flow = workflows.create(f"abandoned-discarded-{how}", **CALLBACKS)
    root = flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    assert actions.discard(root.id) is True

    seal(how, workflows, OxpullWorkflow, flow, members=2)
    problems, observed = judge(
        how=how,
        model=OxpullWorkflow,
        handle=flow,
        reconcile=reconcile_workflows,
        logger_name="oxpull.workflows",
        event="workflow_completed",
        caplog=caplog,
    )
    assert not problems, report(problems, observed)


@pytest.mark.parametrize("how", SEAL_HOW)
def test_hand_sealed_workflow_with_a_lost_root_withholds_on_success(how, caplog):
    """
    root -> child. The builder dies before seal(); root's worker dies on
    every attempt and the reaper writes it LOST. Hand-sealed, the
    reconciler cancels child and completes, and on_success fires for a
    run whose root was never observed to finish and whose child never ran.
    """
    flow = workflows.create(f"abandoned-lost-{how}", **CALLBACKS)
    root = flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    lose_to_the_reaper(flow_worker(), root.id)

    seal(how, workflows, OxpullWorkflow, flow, members=2)
    problems, observed = judge(
        how=how,
        model=OxpullWorkflow,
        handle=flow,
        reconcile=reconcile_workflows,
        logger_name="oxpull.workflows",
        event="workflow_completed",
        caplog=caplog,
    )
    assert not problems, report(problems, observed)


@pytest.mark.parametrize("how", SEAL_HOW)
def test_hand_sealed_workflow_with_a_pruned_failed_node_withholds_on_success(
    how, caplog
):
    """
    Two independent nodes. The builder dies before seal(); one node fails
    all three attempts, the other succeeds. The workflow sits OPEN until
    ox_prune --include-failed deletes the FAILED row. Hand-sealed, it
    completes with failed_count 0 and on_success fires over a failure.
    """
    flow = workflows.create(f"abandoned-pruned-{how}", **CALLBACKS)
    doomed = flow.add("fails", tasks.failing_step, "fails")
    kept = flow.add("succeeds", tasks.step, "succeeds")
    assert drain(flow_worker()) == 4  # fails x3, succeeds
    assert OxTask.objects.get(id=doomed.id).status == OxTask.Status.FAILED
    assert OxTask.objects.get(id=kept.id).status == OxTask.Status.SUCCESSFUL

    # Stands in for the week the abandoned workflow sat OPEN.
    OxTask.objects.filter(id=doomed.id).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    call_command("ox_prune", "--include-failed", stdout=StringIO())
    assert not OxTask.objects.filter(id=doomed.id).exists()
    assert OxTask.objects.filter(id=kept.id).exists()

    seal(how, workflows, OxpullWorkflow, flow, members=2)
    problems, observed = judge(
        how=how,
        model=OxpullWorkflow,
        handle=flow,
        reconcile=reconcile_workflows,
        logger_name="oxpull.workflows",
        event="workflow_completed",
        caplog=caplog,
    )
    assert not problems, report(problems, observed)


# -- the same expression in batches.py -----------------------------------


@pytest.mark.parametrize("how", SEAL_HOW)
def test_hand_sealed_batch_with_a_discarded_member_withholds_on_success(
    how, worker, caplog
):
    batch = batches.create(f"abandoned-batch-discarded-{how}", **CALLBACKS)
    doomed = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    assert actions.discard(doomed.id) is True
    assert worker.run_once() is True
    assert worker.run_once() is False

    seal(how, batches, OxpullBatch, batch, members=2)
    problems, observed = judge(
        how=how,
        model=OxpullBatch,
        handle=batch,
        reconcile=reconcile_batches,
        logger_name="oxpull.batches",
        event="batch_completed",
        caplog=caplog,
    )
    assert not problems, report(problems, observed)


@pytest.mark.parametrize("how", SEAL_HOW)
def test_hand_sealed_batch_with_a_lost_member_withholds_on_success(how, worker, caplog):
    batch = batches.create(f"abandoned-batch-lost-{how}", **CALLBACKS)
    doomed = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    lose_to_the_reaper(worker, doomed.id)
    assert worker.run_once() is True
    assert worker.run_once() is False

    seal(how, batches, OxpullBatch, batch, members=2)
    problems, observed = judge(
        how=how,
        model=OxpullBatch,
        handle=batch,
        reconcile=reconcile_batches,
        logger_name="oxpull.batches",
        event="batch_completed",
        caplog=caplog,
    )
    assert not problems, report(problems, observed)
