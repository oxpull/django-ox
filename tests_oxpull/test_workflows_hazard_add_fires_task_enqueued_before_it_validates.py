"""
Workflow.add() sends task_enqueued before it
validates the caller's input.

add() takes the workflow row first, so a workflow that is no longer OPEN
refuses before anything is enqueued and no signal is sent. Then it
enqueues, and django-ox's enqueue sends task_enqueued right after its
INSERT (add() sends it itself for a node born WAITING, at the same point).
Only then does add() resolve depends_on and insert the node row. When one
of those refuses, add() rolls its block back, the task row with it, and the
receiver has already been handed an id that never commits.

The regression tests assert what a caller would expect: an add() that
raises UnknownWorkflowNode, WorkflowNodeExists or WorkflowSealed sends
no task_enqueued, and every id a receiver was handed can be read back
once the caller's transaction commits.

Two controls pass today and are meant to. They bound the consequence:
* A receiver that defers its effect with transaction.on_commit, as
  `docs/features/unique-tasks.md` and `docs/features/batches.md` advise,
  sees none of the refused adds, both when add() owns the transaction
  and inside a caller's atomic block that catches the refusal and
  commits.
* add() refuses a unique task as a node before it enqueues anything, so
  that refusal sends no signal and writes no row.
"""

from contextlib import contextmanager, suppress

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.tasks import task, task_backends
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.exceptions import UnknownWorkflowNode, WorkflowNodeExists, WorkflowSealed
from oxpull.models import OxpullWorkflow
from oxpull.unique import unique

from . import tasks
from .test_workflows import drain, flow_worker, reconcile, stored


@unique
@task(backend="flows")
def unique_step(label):
    return label


@contextmanager
def enqueued_ids(*, deferred=False):
    """
    The ids task_enqueued hands receivers while the block runs.

    deferred=False records at send time, as a receiver with an immediate
    external effect would. deferred=True records from transaction.on_commit,
    the pattern documented in `docs/features/unique-tasks.md` and
    `docs/features/batches.md`.
    """
    seen = []

    def receiver(sender, task_result, **kwargs):
        if deferred:
            transaction.on_commit(lambda: seen.append(str(task_result.id)))
        else:
            seen.append(str(task_result.id))

    task_enqueued.connect(receiver, weak=False, dispatch_uid="add-enqueued-ids")
    try:
        yield seen
    finally:
        task_enqueued.disconnect(dispatch_uid="add-enqueued-ids")


# -- the refused adds ---------------------------------------------------
#
# Each builds its workflow before any receiver is connected and returns
# the refused call and the exception it raises.


def _unknown_dependency():
    flow = workflows.create("unknown-dependency")
    flow.add("extract", tasks.step, "extract")
    # A typo in depends_on.
    return (
        lambda: flow.add("load", tasks.step, "load", depends_on="extarct"),
        UnknownWorkflowNode,
    )


def _duplicate_node_name():
    flow = workflows.create("duplicate-name")
    flow.add("extract", tasks.step, "extract")
    return (
        lambda: flow.add("extract", tasks.step, "extract-again"),
        WorkflowNodeExists,
    )


def _add_after_seal():
    flow = workflows.create("sealed")
    flow.add("extract", tasks.step, "extract")
    flow.seal()
    return (lambda: flow.add("late", tasks.step, "late"), WorkflowSealed)


def _add_after_complete():
    """COMPLETE through the real path: a worker runs it, the reconciler completes."""
    flow = workflows.create("complete")
    flow.add("extract", tasks.step, "extract")
    flow.seal()
    assert drain(flow_worker()) == 1
    assert reconcile() == 1
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE
    return (lambda: flow.add("late", tasks.step, "late"), WorkflowSealed)


REFUSALS = [
    pytest.param(_unknown_dependency, id="unknown-dependency"),
    pytest.param(_duplicate_node_name, id="duplicate-node-name"),
    pytest.param(_add_after_seal, id="add-after-seal"),
    pytest.param(_add_after_complete, id="add-after-complete"),
]


def refusals_marked(reason):
    """
    REFUSALS, with the refusals that still send a signal marked xfail.

    add() checks the workflow's status before it enqueues, so the sealed and
    complete cases send nothing. A bad depends_on or a duplicate name is
    found after the enqueue.
    """
    still_sends = pytest.mark.xfail(strict=True, reason=reason)
    return [
        pytest.param(_unknown_dependency, id="unknown-dependency", marks=still_sends),
        pytest.param(_duplicate_node_name, id="duplicate-node-name", marks=still_sends),
        pytest.param(_add_after_seal, id="add-after-seal"),
        pytest.param(_add_after_complete, id="add-after-complete"),
    ]


# transaction=True so add() owns a real transaction, as a bare call does.
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "refusal",
    refusals_marked(
        "add() sends task_enqueued before it resolves depends_on and inserts the "
        "node, so a refused add hands receivers the id of a task that is rolled "
        "back"
    ),
)
def test_a_refused_add_sends_no_task_enqueued(refusal):
    attempt, refused_with = refusal()
    rows_before = OxTask.objects.count()

    with enqueued_ids() as seen, pytest.raises(refused_with):
        attempt()

    # (signals sent, task rows added). The rollback already keeps the
    # second at 0; the first is the phantom.
    assert (len(seen), OxTask.objects.count() - rows_before) == (0, 0)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "refusal",
    refusals_marked(
        "A refused add() still sends task_enqueued, and the id it hands "
        "receivers does not exist after the caller commits"
    ),
)
def test_every_id_a_receiver_was_handed_exists_after_the_caller_commits(refusal):
    """
    The consequence as a receiver meets it. The caller handles its own
    mistake inside its transaction and commits the rest of its work, and
    a receiver that stored the id it was handed later asks for the
    result.
    """
    attempt, refused_with = refusal()

    with enqueued_ids() as seen, transaction.atomic():
        with suppress(refused_with):
            attempt()
        kept = tasks.default_step.enqueue("the caller's other work")

    # The backend's lookup by id, as a receiver that kept only the id
    # would make it; Task.get_result would also check the task path.
    missing = []
    for task_id in seen:
        try:
            task_backends["flows"].get_result(task_id)
        except TaskResultDoesNotExist:
            missing.append(task_id)
    assert str(kept.id) in seen
    assert missing == []


# -- controls: these pass today -----------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("inside_caller_atomic", [False, True])
@pytest.mark.parametrize("refusal", REFUSALS)
def test_a_receiver_deferred_with_on_commit_sees_no_refused_add(
    refusal, inside_caller_atomic
):
    attempt, refused_with = refusal()
    control = workflows.create("control")

    with enqueued_ids(deferred=True) as committed:
        if inside_caller_atomic:
            with transaction.atomic():
                with suppress(refused_with):
                    attempt()
                good = control.add("ok", tasks.step, "ok")
        else:
            with pytest.raises(refused_with):
                attempt()
            good = control.add("ok", tasks.step, "ok")

    # The receiver is not silent: it reports the add that committed, and
    # only that one.
    assert committed == [str(good.id)]


@pytest.mark.django_db
def test_a_unique_task_refused_as_a_node_sends_no_task_enqueued():
    flow = workflows.create("unique")
    flow.add("first", tasks.step, "k")
    rows_before = OxTask.objects.count()

    with (
        enqueued_ids() as seen,
        pytest.raises(ImproperlyConfigured, match="unique"),
    ):
        flow.add("second", unique_step, "k")

    assert (len(seen), OxTask.objects.count() - rows_before) == (0, 0)
