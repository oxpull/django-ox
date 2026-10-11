"""
A node status the workflow reconciler does not know.

Workflow completion and progress() must not decide finished-ness by
treating every status outside PENDING_STATUSES (READY, RUNNING) as
finished. `oxpull` 1.2.0's batches use explicit SETTLED_STATUSES, so a
status the package has never seen holds a batch open
(tests_oxpull/test_batches.py,
test_a_status_the_reconciler_does_not_know_holds_the_batch_open).
The tests here require the same conservative handling for workflows.

How a row gets a status this package does not know, in production. With
the current django-ox task statuses; no supported code path writes an unknown one: every
django-ox ref carries the same six statuses, and the admin cannot edit
status. The sequences that do produce it are:

- a rolling upgrade to a later django-ox release that adds a status.
  The new processes write it while a process still on this version runs
  the scheduled reconciler (any worker in the fleet can pick that task up);
- a rollback after such an upgrade, which leaves the new rows behind,
  because migrations do not rewrite statuses;
- an out-of-band write (SQL, a custom operator tool).

None of those can be driven from this version's code, so the one shortcut
in this file is the UPDATE that writes the unknown value. Everything around
it uses the real paths: nodes run, fail and succeed under a real
OxpullWorker, the LOST row comes from the real reaper after
actions.expire_lease, and the DISCARDED row from actions.discard.
"""

import threading
import time

import pytest
from django.db import connections

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, order, reconcile, stored

pytestmark = pytest.mark.django_db

# Not a status in django-ox 1.2.0. Ten characters at most, the status
# column's max_length, so PostgreSQL and strict-mode MySQL store it exactly
# as SQLite does. The same value the batches test uses.
UNKNOWN = "ZOMBIE"

# What a sealed one-node workflow must do at the next reconciler pass, for
# each status its node can be in. A status missing from this table is a
# decision nobody has taken yet, and test 1 fails on it.
COMPLETES_THE_WORKFLOW = {
    OxTask.Status.READY: False,
    OxTask.Status.RUNNING: False,
    OxTask.Status.SUCCESSFUL: True,
    OxTask.Status.FAILED: True,
    OxTask.Status.LOST: True,
    OxTask.Status.DISCARDED: True,
    OxTask.Status.WAITING: False,
    UNKNOWN: False,
}


def callback_flow(name):
    """An OPEN workflow carrying both callbacks, so a completion is visible."""
    return workflows.create(
        name,
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )


def write_unknown_status(task_id, from_status):
    """
    The shortcut: the write a process on a later release would make.

    Compare-and-set on the status the row is in, the shape django-ox's own
    actions use, so it cannot land on a row the test did not expect.
    """
    return OxTask.objects.filter(pk=task_id, status=from_status).update(status=UNKNOWN)


def put_node_in(status, task_id):
    """Drive a one-node workflow's node into `status` through the real path."""
    worker = flow_worker()
    if status == OxTask.Status.READY:
        pass
    elif status == OxTask.Status.RUNNING:
        claimed = worker.claim_one()
        assert claimed is not None and str(claimed.pk) == str(task_id)
    elif status == OxTask.Status.SUCCESSFUL:
        assert drain(worker) == 1
    elif status == OxTask.Status.FAILED:
        assert drain(worker) == 3  # MAX_ATTEMPTS on "flows"
    elif status == OxTask.Status.LOST:
        # Two failed attempts, then a third claimed and never finished: the
        # reaper's attempts-exhausted branch, with a real 1.2.0 lease.
        assert worker.run_once() is True
        assert worker.run_once() is True
        claimed = worker.claim_one()
        assert claimed is not None and str(claimed.pk) == str(task_id)
        assert claimed.attempts == claimed.max_attempts
        assert actions.expire_lease(task_id) is True
        assert worker.reap() == 1
    elif status == OxTask.Status.DISCARDED:
        assert actions.discard(task_id) is True
    elif status == OxTask.Status.WAITING:
        # A one-node workflow's node has no upstream, so add() enqueues it
        # READY. This write gives it the status add() gives a node that
        # waits for one.
        assert (
            OxTask.objects.filter(pk=task_id, status=OxTask.Status.READY).update(
                status=OxTask.Status.WAITING
            )
            == 1
        )
    else:
        assert write_unknown_status(task_id, OxTask.Status.READY) == 1
    assert OxTask.objects.get(pk=task_id).status == status


def callback_rows():
    return OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()


# -- the contract with django-ox ---------------------------------------


def test_workflow_status_sets_cover_every_django_ox_status():
    """
    The workflow reconciler classifies a node status three ways: pending
    (holds completion), SUCCESSFUL (satisfies the gate), dead (cancels the
    downstream cone). The three sets must be disjoint and cover every status
    django-ox defines, and the table above must name every status. A new
    django-ox status must fail this test until the sets and table account
    for it.

    Completion reads SETTLED_STATUSES, shared with batches: a status
    outside it is pending. SETTLED_STATUSES must be exactly the gate's
    SUCCESSFUL plus the cancellation's dead statuses, and together with
    PENDING_STATUSES it must cover the enum. This is the batches contract
    (test_the_reconciler_knows_every_django_ox_status). Set coverage alone
    cannot detect completion treating every status outside
    PENDING_STATUSES as finished; the tests below catch that behaviour.
    """
    pending = set(workflows.PENDING_STATUSES)
    settled = set(workflows.SETTLED_STATUSES)
    succeeded = {OxTask.Status.SUCCESSFUL}
    dead = set(workflows.DEAD_STATUSES)
    assert pending & succeeded == set()
    assert pending & dead == set()
    assert succeeded & dead == set()
    assert pending | succeeded | dead == set(OxTask.Status)
    assert pending & settled == set()
    assert pending | settled == set(OxTask.Status)
    assert settled == succeeded | dead
    assert set(workflows.UNATTRIBUTED_STATUSES) <= dead
    assert set(OxTask.Status) <= set(COMPLETES_THE_WORKFLOW)
    assert UNKNOWN not in set(OxTask.Status)


@pytest.mark.parametrize("status", list(COMPLETES_THE_WORKFLOW), ids=str)
def test_every_node_status_has_one_completion_reading(status):
    """
    Read the same contract through reconcile() rather than the module's
    constants: seal a one-node workflow, drive its node into each status,
    then run one reconciler pass. The six known django-ox statuses guard
    against over-correction: LOST and DISCARDED must still complete.
    The unknown status must hold the workflow open.
    """
    flow = workflows.create(f"contract-{status.lower()}")
    task = (
        tasks.failing_step
        if status in (OxTask.Status.FAILED, OxTask.Status.LOST)
        else tasks.step
    )
    node = flow.add("only", task, "only")
    flow.seal()
    put_node_in(status, node.id)

    completes = COMPLETES_THE_WORKFLOW[status]
    observed = (reconcile(), stored(flow).status)
    expected = (
        1 if completes else 0,
        OxpullWorkflow.Status.COMPLETE if completes else OxpullWorkflow.Status.SEALED,
    )
    assert observed == expected


# -- the hazard ----------------------------------------------------------


def test_a_sealed_workflow_whose_node_status_is_unknown_stays_sealed():
    """
    A node whose row carries a status this package cannot read
    must not count as finished, and must not be attributed to on_success.
    It stays pending, and the workflow stays SEALED, until a release that
    knows the status reads it. Every observable is collected before the one
    comparison, so a failure shows what the reconciler did.
    """
    flow = callback_flow("unknown-status")
    node = flow.add("only", tasks.step, "only")
    flow.seal()
    put_node_in(UNKNOWN, node.id)

    progress = flow.progress()
    completed = reconcile()
    row = stored(flow)
    observed = {
        "progress (total, pending, finished)": (
            progress.total,
            progress.pending,
            progress.finished,
        ),
        "reconcile() completed": completed,
        "workflow status": row.status,
        "on_complete enqueued": row.callback_task_id is not None,
        "on_success enqueued": row.success_task_id is not None,
        "callback task rows": callback_rows(),
    }
    assert observed == {
        "progress (total, pending, finished)": (1, 1, 0),
        "reconcile() completed": 0,
        "workflow status": OxpullWorkflow.Status.SEALED,
        "on_complete enqueued": False,
        "on_success enqueued": False,
        "callback task rows": 0,
    }


@pytest.mark.django_db(transaction=True)
def test_an_unknown_status_over_a_running_leaf_does_not_complete_the_workflow():
    """
    The mid-upgrade shape, with a real worker thread holding a real lease.

    root -> leaf. root succeeds under a real OxpullWorker. A second worker,
    in its own thread, claims leaf and starts executing it. While it runs,
    a process on a later release writes a status this one does not know
    onto the RUNNING row. The holder's finish write is then refused
    (WRITABLE_STATUSES is RUNNING and LOST), so the row keeps the unknown
    status although the task body ran. No SUCCESSFUL was ever recorded for
    leaf, so on_success ("the run was clean") must not fire, and the
    workflow must not complete while leaf's status is unreadable.
    """
    flow = callback_flow("unknown-under-lease")
    flow.add("root", tasks.step, "root")
    leaf = flow.add("leaf", tasks.slow_step, "leaf", seconds=1.5, depends_on="root")
    flow.seal()

    assert flow_worker().run_once() is True
    assert order() == ["root"]

    outcome = {}

    def run_leaf():
        try:
            outcome["ran"] = flow_worker().run_once()
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
        finally:
            connections.close_all()

    holder = threading.Thread(target=run_leaf)
    holder.start()
    deadline = time.monotonic() + 10
    while not OxTask.objects.filter(pk=leaf.id, status=OxTask.Status.RUNNING).exists():
        assert time.monotonic() < deadline, "leaf was never claimed"
        time.sleep(0.01)
    assert write_unknown_status(leaf.id, OxTask.Status.RUNNING) == 1
    holder.join(timeout=30)

    # Preconditions: the holder ran the body and its finish write was refused.
    assert not holder.is_alive()
    assert "error" not in outcome
    assert order() == ["root", "leaf"]
    assert OxTask.objects.get(pk=leaf.id).status == UNKNOWN

    progress = flow.progress()
    completed = reconcile()
    row = stored(flow)
    observed = {
        "progress (total, pending, succeeded)": (
            progress.total,
            progress.pending,
            progress.succeeded,
        ),
        "reconcile() completed": completed,
        "workflow status": row.status,
        "on_complete enqueued": row.callback_task_id is not None,
        "on_success enqueued": row.success_task_id is not None,
        "callback task rows": callback_rows(),
    }
    assert observed == {
        "progress (total, pending, succeeded)": (2, 1, 1),
        "reconcile() completed": 0,
        "workflow status": OxpullWorkflow.Status.SEALED,
        "on_complete enqueued": False,
        "on_success enqueued": False,
        "callback task rows": 0,
    }
