"""
The stored cancelled count is the number of nodes the workflow's own
cancellation changed, and nothing else.

One definition, shared with
tests_oxpull/test_workflows.py::test_an_operator_discarding_a_node_cancels_its_cone.
OxpullWorkflow.cancelled_count is the reconciler's own row count: the
READY or WAITING node rows the workflow's cancellation moved to DISCARDED
because an
upstream of theirs settled without succeeding, added up over every
reconciler tick and stored when the workflow completes. It records what
the workflow did, so:

- a node a person discarded (actions.discard, discard_many, the admin's
  "Discard selected tasks") is never counted, whether or not it has
  edges; it shows only as unattributed;
- a node the workflow cancelled still counts after ox_prune deletes its
  task row;
- a workflow that cancelled nothing stores 0.

Today the completing transaction stores the DISCARDED rows it can still
see instead. That counts people's discards and drops pruned
cancellations.

Wrong implementations these tests reject:

- counting DISCARDED rows at completion, which is today's code: every
  test marked as an expected failure;
- counting DISCARDED nodes that have an upstream, or a dead upstream:
  the operator-discards test, where a person also discards a dependent
  node, and the pruning test;
- keeping one tick's count instead of the total (the tick that
  completes, or the last tick that cancelled anything): the pruning
  test, and the unmarked test whose cancellations land in two ticks.

Adding each tick's row count onto the workflow row passes, and so does
counting oxpull node rows that record a cancellation.
"""

import io
import logging
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import order, stored

pytestmark = pytest.mark.django_db

COMPLETE = OxpullWorkflow.Status.COMPLETE
SEALED = OxpullWorkflow.Status.SEALED
DISCARDED = OxTask.Status.DISCARDED


def worker_on(alias):
    return OxpullWorker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)


def reconcile_through_a_worker(worker):
    """
    One reconciler tick the way production runs it: the task a schedule
    enqueues, claimed and executed by a worker. Top priority so the worker
    takes it ahead of any READY node.
    """
    result = workflows.reconcile.using(priority=100).enqueue()
    assert worker.run_once() is True
    row = OxTask.objects.get(pk=result.id)
    assert row.status == OxTask.Status.SUCCESSFUL, (row.status, row.errors)


def reconcile_until_complete(worker, flow):
    for _ in range(3):
        reconcile_through_a_worker(worker)
        if stored(flow).status == COMPLETE:
            break


def status_of(ids, name):
    return OxTask.objects.filter(pk=ids[name]).values_list("status", flat=True).first()


def logged_cancellations(caplog, flow):
    """What the reconciler reported cancelling in this workflow, for messages."""
    return sum(
        record.cancelled_count
        for record in caplog.records
        if getattr(record, "event", None) == "workflow_nodes_cancelled"
        and record.workflow_id == str(flow.id)
    )


def describe(flow, caplog):
    row = stored(flow)
    ids = flow.node_ids()
    statuses = {name: status_of(ids, name) for name in ids}
    return (
        f"status={row.status} failed_count={row.failed_count} "
        f"cancelled_count={row.cancelled_count} "
        f"reconciler_logged={logged_cancellations(caplog, flow)} nodes={statuses}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The stored cancelled count includes nodes a person discarded; it should "
        "count only the nodes the workflow itself cancelled"
    ),
)
@pytest.mark.parametrize(
    ("alias", "step"),
    [("flows", tasks.step), ("flows_deferred", tasks.deferred_step)],
    ids=["gate", "deferred"],
)
def test_operator_discards_are_not_counted_as_nodes_the_workflow_cancelled(
    caplog, alias, step
):
    """
    a -> b, a -> c, plus an independent leaf. A person discards the leaf,
    the dependent node b, and then a. The workflow cancels c, and only c,
    so the stored count is 1.

    b is the case that separates the definition from "DISCARDED nodes
    with an upstream": b has an upstream, and a dead one, and it is still
    a person's discard.
    """
    caplog.set_level(logging.INFO, logger="oxpull.workflows")
    flow = workflows.create("operator-discards")
    flow.add("a", step, "a")
    flow.add("b", step, "b", depends_on="a")
    flow.add("c", step, "c", depends_on="a")
    flow.add("leaf", step, "leaf")
    flow.seal()
    ids = flow.node_ids()

    assert actions.discard(ids["leaf"]) is True
    assert actions.discard(ids["b"]) is True
    assert actions.discard(ids["a"]) is True
    assert status_of(ids, "c") == OxTask.Status.READY

    worker = worker_on(alias)
    reconcile_until_complete(worker, flow)

    row = stored(flow)
    assert row.status == COMPLETE, describe(flow, caplog)
    assert order() == []  # nothing ran
    assert status_of(ids, "c") == DISCARDED  # the workflow's one cancellation
    assert row.failed_count == 0
    assert flow.progress().unattributed == 4  # all four stay visible there
    assert row.cancelled_count == 1, (
        "cancelled_count should count only c, the node the workflow cancelled; "
        "leaf, b and a are a person's discards. " + describe(flow, caplog)
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A workflow that cancelled nothing stores a non-zero cancelled count when "
        "a person discarded one of its nodes"
    ),
)
def test_a_workflow_that_cancelled_nothing_stores_no_cancellations(caplog):
    """
    No edges anywhere, so there is nothing for the workflow to cancel. A
    person closes one node through discard_many, which is what the
    django-ox admin's "Discard selected tasks" action calls; the other runs.
    """
    caplog.set_level(logging.INFO, logger="oxpull.workflows")
    flow = workflows.create("nothing-cancelled")
    flow.add("work", tasks.step, "work")
    flow.add("leaf", tasks.step, "leaf")
    flow.seal()
    ids = flow.node_ids()

    assert actions.discard_many([ids["leaf"]]) == (1, 0)
    worker = worker_on("flows")
    assert worker.run_once() is True
    assert order() == ["work"]
    reconcile_until_complete(worker, flow)

    row = stored(flow)
    assert row.status == COMPLETE, describe(flow, caplog)
    assert row.failed_count == 0
    assert flow.progress().unattributed == 1
    assert row.cancelled_count == 0, (
        "the workflow cancelled nothing; a person discarded one node. "
        + describe(flow, caplog)
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The stored cancelled count drops a node the workflow cancelled once "
        "pruning deletes that node's task row before the workflow completes"
    ),
)
def test_a_cancelled_node_pruned_before_the_workflow_completes_still_counts(caplog):
    """
    root fails permanently and the workflow cancels child in one tick. An
    independent sibling keeps the workflow SEALED. The default ox_prune
    deletes child's DISCARDED row once it is older than 7 days. A later
    tick completes the workflow and cancels nothing.

    Shortcut: child's finished_at is backdated 8 days. In production the
    same state is a sibling that stays pending for longer than ox_prune's
    --older-than after the cancellation (a backlog, a stopped fleet, a
    long-deferred node) while ox_prune runs on its usual schedule.
    """
    caplog.set_level(logging.INFO, logger="oxpull.workflows")
    flow = workflows.create("pruned-cancellation")
    flow.add("root", tasks.failing_step.using(priority=10), "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.add("sibling", tasks.step, "sibling")
    flow.seal()
    ids = flow.node_ids()

    worker = worker_on("flows")
    for _ in range(3):
        assert worker.run_once() is True
    assert order() == ["root", "root", "root"]
    assert status_of(ids, "root") == OxTask.Status.FAILED

    reconcile_through_a_worker(worker)
    assert status_of(ids, "child") == DISCARDED  # the workflow cancelled it
    assert stored(flow).status == SEALED  # sibling is still READY

    OxTask.objects.filter(pk=ids["child"]).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    call_command("ox_prune", stdout=io.StringIO())
    assert not OxTask.objects.filter(pk=ids["child"]).exists()

    assert worker.run_once() is True
    assert order()[-1] == "sibling"
    reconcile_until_complete(worker, flow)

    row = stored(flow)
    assert row.status == COMPLETE, describe(flow, caplog)
    assert row.failed_count == 1
    assert row.cancelled_count == 1, (
        "the workflow cancelled child; ox_prune deleting the row afterwards "
        "does not undo that. " + describe(flow, caplog)
    )


def test_cancellations_in_separate_ticks_add_up(caplog):
    """
    Control: passes today and must keep passing. Two branches fail at
    different times. The workflow cancels two nodes in one tick and one
    in a later tick, then completes, and the stored count is the total of
    3, not either tick's count.
    """
    caplog.set_level(logging.INFO, logger="oxpull.workflows")
    flow = workflows.create("two-ticks")
    flow.add("first", tasks.failing_step.using(priority=10), "first")
    flow.add("first-child", tasks.step, "first-child", depends_on="first")
    flow.add(
        "first-grandchild", tasks.step, "first-grandchild", depends_on="first-child"
    )
    flow.add("second", tasks.failing_step.using(priority=5), "second")
    flow.add("second-child", tasks.step, "second-child", depends_on="second")
    flow.seal()
    ids = flow.node_ids()

    worker = worker_on("flows")
    for _ in range(3):
        assert worker.run_once() is True
    assert order() == ["first"] * 3
    assert status_of(ids, "first") == OxTask.Status.FAILED

    reconcile_through_a_worker(worker)
    assert status_of(ids, "first-child") == DISCARDED, describe(flow, caplog)
    assert status_of(ids, "first-grandchild") == DISCARDED, describe(flow, caplog)
    assert status_of(ids, "second-child") == OxTask.Status.WAITING
    assert stored(flow).status == SEALED  # second is still READY

    for _ in range(3):
        assert worker.run_once() is True
    assert order()[3:] == ["second"] * 3
    assert status_of(ids, "second") == OxTask.Status.FAILED

    reconcile_until_complete(worker, flow)
    row = stored(flow)
    assert row.status == COMPLETE, describe(flow, caplog)
    assert status_of(ids, "second-child") == DISCARDED
    assert row.failed_count == 2
    assert row.cancelled_count == 3, (
        "two cancelled in the first tick and one in a later tick. "
        + describe(flow, caplog)
    )
