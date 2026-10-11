"""
django-ox's per-task policy, through every `oxpull` app path.

django-ox's PolicyTask gives a task max_attempts, backoff and timeout of its
own. The budget is written onto the row at enqueue, by the OxBackend._row that
every enqueue in this package reaches, and from then on the stored value is
the one the worker and the reaper decide on. Backoff and timeout are not
stored: the worker reads them from the code it imports for each attempt.
None of that is `oxpull` app code, and the `oxpull` app changes none of it.
What is pinned here is that it holds on the `oxpull` app's own paths, where a
row is written some other way or a failure is recorded through the `oxpull`
app's outcome hook:

- the budget reaches the row through unique enqueue, bulk enqueue, batch
  members and callbacks, workflow roots and WAITING nodes, and survives
  using() routing and rebinding;
- a unique duplicate returns the holder, whose row keeps its stored budget,
  because policy is not part of the key;
- a throttled claim spends nothing, and every admitted retry spends a claim
  and an admission;
- a workflow node's retry is not an outcome, and a backoff that answers None,
  a timeout and a spent budget each record FAILED through the outcome hook,
  while a release never touches a retry's backoff;
- a task's own timeout reaches the remaining() the reconcilers read, and the
  reconcilers, built lazily as plain tasks, keep the backend's defaults.

On Django 6.0 the tasks in tests_oxpull/policy_tasks.py are built without the
decorator spelling, which 6.0 does not have; everything below still runs.
"""

import dataclasses
import logging
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import default_task_backend, task_backends
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_finished
from django.utils import timezone

from django_ox.bulk import enqueue_many
from django_ox.models import OxTask
from django_ox.tasks import PolicyTask
from django_ox.worker import Worker
from oxpull import batches, workflows
from oxpull.backend import OxpullBackend
from oxpull.models import OxpullBatch, OxpullWorkflow, OxpullWorkflowNode

from . import policy_tasks, tasks
from .test_rate_limits import oxpull_worker, rate_limits, window
from .test_workflows import drain, flow_worker
from .test_workflows_hazard_the_reconciler_s_per_workflow_except_exception import (
    interruptible_attempts,  # noqa: F401 - a fixture
)
from .test_workflows_waiting import edge_marks

pytestmark = pytest.mark.django_db

READY = OxTask.Status.READY
RUNNING = OxTask.Status.RUNNING
WAITING = OxTask.Status.WAITING
FAILED = OxTask.Status.FAILED
LOST = OxTask.Status.LOST
DISCARDED = OxTask.Status.DISCARDED
SUCCESSFUL = OxTask.Status.SUCCESSFUL

TIMEOUT_PATH = "django_ox.exceptions.TaskTimeout"

# The backend-wide budget every test backend in tests_oxpull/settings.py declares.
BACKEND_BUDGET = 3


def row(result):
    return OxTask.objects.get(pk=result.id)


def budget(result):
    return row(result).max_attempts


def record_of(result):
    return OxpullWorkflowNode.objects.values_list(
        "outcome", "outcome_epoch", "succeeded"
    ).get(task_id=result.id)


def reconcile_batches():
    return batches.reconcile.call()


def reconcile_workflows():
    return workflows._reconcile()


def notes():
    return list(tasks.STATE.get("notes", []))


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@contextmanager
def finished_signals():
    sent = []

    def receiver(sender, task_result, **kwargs):
        sent.append(task_result)

    task_finished.connect(receiver)
    try:
        yield sent
    finally:
        task_finished.disconnect(receiver)


@contextmanager
def remaining_as_the_reconcilers_read_it(monkeypatch):
    """
    Every value django-ox's remaining() gave the two reconcilers, through the
    names they read it by. The real function answers; this only listens.
    """
    seen = []
    for module in (batches, workflows):
        real = module.remaining

        def listening(real=real, module=module):
            value = real()
            seen.append((module.__name__, value))
            return value

        monkeypatch.setattr(module, "remaining", listening)
    yield seen


# -- the budget reaches the row on every `oxpull` app path ----------------


def test_a_unique_task_stores_its_declared_budget():
    declared = policy_tasks.unique_with_budget.enqueue("a")
    inherited = tasks.unique_echo.enqueue("a")

    assert budget(declared) == 5
    assert declared.task == policy_tasks.unique_with_budget
    assert budget(inherited) == BACKEND_BUDGET
    # The result carries the task as declared; the budget is the row's.
    assert inherited.task == tasks.unique_echo
    assert inherited.task.max_attempts is None


def test_a_unique_duplicate_returns_the_holder_with_its_stored_budget():
    """
    A duplicate returns the holder's result, and the holder's row keeps its
    stored budget, which is the one the worker and the reaper decide on. A
    row can hold another budget than the code now declares: an operator
    retry rewrites it, and a row enqueued before a deploy keeps the one it
    was written with. The result's task is the task as the code declares
    it, not that budget. Policy is not part of the key, so a copy that
    declares yet another budget coalesces onto the same holder too.
    """
    holder = policy_tasks.unique_with_budget.enqueue("a")
    OxTask.objects.filter(pk=holder.id).update(max_attempts=9)

    duplicate = policy_tasks.unique_with_budget.enqueue("a")
    replaced = dataclasses.replace(policy_tasks.unique_with_budget, max_attempts=2)
    other_policy = replaced.enqueue("a")

    assert duplicate.id == other_policy.id == holder.id
    assert OxTask.objects.count() == 1
    assert budget(holder) == budget(duplicate) == budget(other_policy) == 9
    assert duplicate.task == policy_tasks.unique_with_budget
    assert other_policy.task == policy_tasks.unique_with_budget
    fetched = default_task_backend.get_result(holder.id).task
    assert fetched == policy_tasks.unique_with_budget


def test_the_reaper_decides_a_unique_task_on_its_stored_budget(worker):
    """
    The reaper's requeue-or-LOST is SQL on the row's max_attempts. With three
    attempts spent, the backend's budget of 3 would make the row LOST and
    free the key; the task's own budget of 5 puts it back, and the key stays
    with it.
    """
    first = policy_tasks.unique_with_budget.enqueue("a")
    stale = timezone.now() - timedelta(seconds=9999)
    OxTask.objects.filter(pk=first.id).update(
        status=RUNNING, attempts=3, locked_by="dead-worker", locked_at=stale
    )

    assert worker.reap() == 1
    assert row(first).status == READY
    assert policy_tasks.unique_with_budget.enqueue("a").id == first.id

    OxTask.objects.filter(pk=first.id).update(
        status=RUNNING, attempts=5, locked_by="dead-worker", locked_at=stale
    )
    assert worker.reap() == 1
    assert row(first).status == LOST
    second = policy_tasks.unique_with_budget.enqueue("a")
    assert second.id != first.id
    assert budget(second) == 5


def test_bulk_enqueue_stores_the_declared_budget_on_every_row():
    calls = [((value,), {}) for value in range(3)]

    declared = enqueue_many(policy_tasks.declares_everything, calls)
    inherited = enqueue_many(tasks.plain_echo, calls)

    assert isinstance(policy_tasks.declares_everything.get_backend(), OxpullBackend)
    assert [budget(result) for result in declared] == [5, 5, 5]
    assert [result.task for result in declared] == [
        policy_tasks.declares_everything
    ] * 3
    assert [budget(result) for result in inherited] == [BACKEND_BUDGET] * 3
    assert [result.task for result in inherited] == [tasks.plain_echo] * 3


def test_bulk_enqueue_still_refuses_a_unique_task_that_declares_policy():
    with pytest.raises(InvalidTask, match="is unique"):
        enqueue_many(policy_tasks.unique_with_budget, [(("a",), {})])
    assert OxTask.objects.count() == 0


def test_batch_members_store_their_own_budgets():
    batch = batches.create("budgets")
    declared = batch.add(policy_tasks.declares_everything, 1)
    routed = batch.add(
        policy_tasks.declares_everything.using(queue_name="emails", priority=3), 2
    )
    inherited = batch.add(tasks.plain_echo, 3)

    assert [budget(declared), budget(routed), budget(inherited)] == [
        5,
        5,
        BACKEND_BUDGET,
    ]
    assert (row(routed).queue_name, row(routed).priority) == ("emails", 3)
    assert declared.task.max_attempts == routed.task.max_attempts == 5


def test_batch_callbacks_are_enqueued_with_their_declared_budget(worker):
    batch = batches.create(
        "callbacks",
        on_complete=policy_tasks.callback_with_budget,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    batch.add(tasks.plain_echo, 1)
    batch.seal()
    assert worker.run_once() is True
    assert reconcile_batches() == 1

    stored = OxpullBatch.objects.get(pk=batch.id)
    complete = OxTask.objects.get(pk=stored.callback_task_id)
    success = OxTask.objects.get(pk=stored.success_task_id)
    assert complete.task_path == "tests_oxpull.policy_tasks.callback_with_budget"
    assert complete.max_attempts == 6
    assert success.max_attempts == BACKEND_BUDGET


@pytest.mark.parametrize("module", [batches, workflows], ids=["batch", "workflow"])
def test_a_callback_whose_policy_was_replaced_per_call_is_refused(module):
    """
    A callback is stored as its dotted path, and the path imports the
    declared task, so a per-call replace() of its policy would be dropped in
    silence at completion. It is refused on the create() line, as a using()
    override is.
    """
    replaced = dataclasses.replace(policy_tasks.callback_with_budget, max_attempts=2)
    with pytest.raises(ImproperlyConfigured, match="does not import back to itself"):
        module.create("replaced", on_complete=replaced)
    assert not OxpullBatch.objects.exists()
    assert not OxpullWorkflow.objects.exists()


def test_workflow_roots_waiting_nodes_and_callbacks_store_their_own_budgets():
    flow = workflows.create(
        "budgets",
        on_complete=policy_tasks.callback_with_budget,
        on_complete_args=["complete"],
    )
    root = flow.add("root", policy_tasks.node_with_budget, "root")
    waiting = flow.add(
        "waiting", policy_tasks.node_with_budget, "waiting", depends_on="root"
    )
    inherited = flow.add("inherited", tasks.step, "inherited", depends_on="root")
    flow.seal()

    assert (row(root).status, budget(root)) == (READY, 5)
    assert (row(waiting).status, budget(waiting)) == (WAITING, 5)
    assert (row(inherited).status, budget(inherited)) == (WAITING, BACKEND_BUDGET)
    assert root.task == waiting.task == policy_tasks.node_with_budget
    assert inherited.task == tasks.step

    assert drain(flow_worker()) == 3
    # The release moves the status and nothing else on the row.
    assert [budget(r) for r in (root, waiting, inherited)] == [5, 5, BACKEND_BUDGET]
    assert {row(r).status for r in (root, waiting, inherited)} == {SUCCESSFUL}

    reconcile_workflows()
    stored = OxpullWorkflow.objects.get(pk=flow.id)
    assert stored.status == OxpullWorkflow.Status.COMPLETE
    assert OxTask.objects.get(pk=stored.callback_task_id).max_attempts == 6


# -- routing and rebinding keep the policy -----------------------------------


def test_using_keeps_the_policy_and_the_row_keeps_the_routing():
    declared = policy_tasks.declares_everything
    routed = declared.using(queue_name="emails", priority=4)
    rebound = policy_tasks.node_with_budget.using(backend="flows_deferred")

    assert (routed.max_attempts, routed.backoff, routed.timeout) == (
        5,
        policy_tasks.retry_now,
        30,
    )
    assert isinstance(rebound, PolicyTask)
    assert rebound.max_attempts == 5

    first = routed.enqueue(1)
    second = rebound.enqueue("x")
    third = policy_tasks.unique_with_budget.using(queue_name="emails").enqueue("a")

    assert (row(first).queue_name, row(first).priority, budget(first)) == (
        "emails",
        4,
        5,
    )
    assert (row(second).backend_name, budget(second)) == ("flows_deferred", 5)
    # A unique task routed elsewhere is another key, with the same budget.
    assert (row(third).queue_name, budget(third)) == ("emails", 5)
    assert third.id != policy_tasks.unique_with_budget.enqueue("a").id

    # Read back, the result carries the row's routing and the live policy.
    back = default_task_backend.get_result(first.id).task
    assert (back.queue_name, back.priority) == ("emails", 4)
    assert (back.max_attempts, back.backoff, back.timeout) == (
        5,
        policy_tasks.retry_now,
        30,
    )
    back = task_backends["flows_deferred"].get_result(second.id).task
    assert (back.backend, back.max_attempts) == ("flows_deferred", 5)


def test_a_plain_task_rebound_onto_an_oxpull_backend_inherits_the_backend_budget():
    """
    A task declared on a backend whose tasks carry no policy keeps its own
    class when using() rebinds it here, and declares nothing, so it takes
    the backend's budget. Its result carries that task, still plain; the
    budget is on the row.
    """
    rebound = tasks.immediate_echo.using(backend="default")
    assert not isinstance(rebound, PolicyTask)

    result = rebound.enqueue("x")

    assert budget(result) == BACKEND_BUDGET
    assert not isinstance(result.task, PolicyTask)
    assert result.task == rebound
    fetched = default_task_backend.get_result(result.id).task
    assert not isinstance(fetched, PolicyTask)
    assert fetched == rebound


# -- rate limits -------------------------------------------------------------


def test_a_throttled_claim_spends_nothing_and_every_admitted_retry_spends_one():
    """
    The limiter decides before the claim, so a throttled task keeps its
    attempts and its lease epoch. Each retry the limiter admits is a claim
    like the first, and it spends one attempt of the task's own budget and
    one admission of the limit's window.
    """
    path = "tests_oxpull.policy_tasks.fails_with_budget_four"
    result = policy_tasks.fails_with_budget_four.enqueue()

    with rate_limits({"budget": {"limit": 2, "per": 600, "tasks": [path]}}):
        worker = oxpull_worker()
        for attempt in (1, 2):
            assert worker.run_once() is True  # fails, and its backoff retries at once
            assert (row(result).status, row(result).attempts) == (READY, attempt)
            assert window("budget").used == attempt

        for _ in range(3):
            assert worker.run_once() is False
        throttled = row(result)
        assert throttled.status == READY
        assert (throttled.attempts, throttled.lease_epoch) == (2, 2)
        assert throttled.locked_by is None
        assert window("budget").used == 2

    with rate_limits({"budget": {"limit": 10, "per": 600, "tasks": [path]}}):
        worker = oxpull_worker()
        assert worker.run_once() is True  # attempt 3: the backend's budget ends here
        assert (row(result).status, row(result).attempts) == (READY, 3)
        assert worker.run_once() is True  # attempt 4, the task's last
        assert worker.run_once() is False

    spent = row(result)
    assert spent.status == FAILED
    assert (spent.attempts, spent.max_attempts, spent.lease_epoch) == (4, 4, 4)
    assert window("budget").used == 4


# -- workflow nodes: a retry is not an outcome --------------------------------


def test_a_node_retry_is_not_an_outcome_until_its_own_budget_is_spent():
    """
    A retry is django-ox's write to READY, which the `oxpull` app's hook passes through
    and records nothing for. The node's budget of 5 outlives the backend's
    3, so attempts 3 and 4 are still retries, and only the fifth failure is
    the node's outcome.
    """
    flow = workflows.create("retrying")
    root = flow.add("root", policy_tasks.failing_node_with_budget_five, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()

    for attempt in range(1, 5):
        assert worker.run_once() is True
        assert (row(root).status, row(root).attempts) == (READY, attempt)
        assert record_of(root) == (None, None, False)
        reconcile_workflows()
        assert row(child).status == WAITING
        assert (
            OxpullWorkflow.objects.get(pk=flow.id).status
            == OxpullWorkflow.Status.SEALED
        )

    with finished_signals() as finished:
        assert worker.run_once() is True

    spent = row(root)
    assert (spent.status, spent.attempts, spent.max_attempts) == (FAILED, 5, 5)
    assert record_of(root) == (FAILED, spent.lease_epoch, False)
    assert edge_marks(child) == [False]
    assert [(str(r.id), r.task.max_attempts) for r in finished] == [(root.id, 5)]
    reconcile_workflows()
    assert row(child).status == DISCARDED


def test_a_backoff_that_answers_none_fails_the_node_through_the_outcome_hook(caplog):
    flow = workflows.create("declined")
    root = flow.add("root", policy_tasks.failing_node_that_stops, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()

    with (
        caplog.at_level(logging.ERROR, logger="django_ox"),
        finished_signals() as finished,
    ):
        assert flow_worker().run_once() is True

    failed = row(root)
    assert (failed.status, failed.attempts, failed.max_attempts) == (FAILED, 1, 5)
    assert record_of(root) == (FAILED, failed.lease_epoch, False)
    assert notes() == [{"callback": "stop", "attempts": 1, "max_attempts": 5}]
    assert [(str(r.id), r.task.max_attempts) for r in finished] == [(root.id, 5)]
    (declined,) = events(caplog, "task_failed")
    assert declined.reason == "backoff_declined"
    assert edge_marks(child) == [False]
    reconcile_workflows()
    assert row(child).status == DISCARDED


@pytest.mark.django_db(transaction=True)
def test_a_timeout_retries_the_node_and_the_last_one_fails_it_through_the_hook(
    interruptible_attempts,  # noqa: F811
):
    """
    The node declares a timeout of 1 s and a budget of 2, and nothing in the
    options sets a timeout. Its first timed-out attempt is a retry, which
    records nothing; its second spends the budget and records FAILED.
    """
    flow = workflows.create("timed-out")
    root = flow.add("root", policy_tasks.node_past_its_timeout, "root", 30)
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()
    assert not worker.timeouts.enabled

    assert worker.run_once() is True
    first = row(root)
    assert (first.status, first.attempts) == (READY, 1)
    assert [e["exception_class_path"] for e in first.errors] == [TIMEOUT_PATH]
    assert record_of(root) == (None, None, False)

    assert worker.run_once() is True
    spent = row(root)
    assert (spent.status, spent.attempts) == (FAILED, 2)
    assert [e["exception_class_path"] for e in spent.errors] == [TIMEOUT_PATH] * 2
    assert record_of(root) == (FAILED, spent.lease_epoch, False)
    assert not worker.recycling
    assert row(child).status == WAITING


def test_a_release_leaves_a_retrying_nodes_own_backoff_alone():
    """
    The worker's own backoff here is zero, so a run_after an hour out can
    only be the node's backoff answering. The release moves only WAITING
    rows, so a second success of the upstream, and the reconciler, leave the
    retrying node's hour where it is.
    """
    flow = workflows.create("backoff")
    parent = flow.add("parent", tasks.step, "parent")
    child = flow.add(
        "child",
        policy_tasks.failing_node_that_waits_an_hour,
        "child",
        depends_on="parent",
    )
    flow.seal()
    worker = flow_worker()
    assert worker.backoff_initial == 0

    assert worker.run_once() is True  # the parent succeeds and releases the child
    assert row(child).status == READY
    before = timezone.now()
    assert worker.run_once() is True  # the child fails; its backoff asks for an hour
    backoff = row(child).run_after
    assert backoff is not None
    assert backoff >= before + timedelta(minutes=59)
    assert notes() == [{"callback": "hour", "attempts": 1}]

    OxTask.objects.filter(pk=parent.id).update(status=READY)
    assert worker.run_once() is True  # the parent succeeds again
    reconcile_workflows()

    assert (row(child).status, row(child).run_after) == (READY, backoff)
    assert record_of(child) == (None, None, False)
    assert worker.run_once() is False


def test_a_direct_failure_record_on_a_node_keeps_its_contract_and_asks_no_backoff():
    """
    _handle_failure(db_task, exc, duration_ms, *, release=True) is called
    directly, by the stuck-thread path and by this package's tests. Neither
    call runs inside an attempt, so neither asks the node's backoff: the
    budget on the row and the worker's backoff decide, and a node with
    attempts left is a retry that records nothing.
    """
    flow = workflows.create("direct")
    root = flow.add("root", policy_tasks.failing_node_that_stops, "root")
    flow.seal()
    worker = flow_worker()

    claimed = worker.claim_one()
    assert (
        worker._handle_failure(claimed, RuntimeError("stuck"), 0, release=True) is True
    )
    assert (row(root).status, row(root).attempts) == (READY, 1)

    claimed = worker.claim_one()
    assert worker._handle_failure(claimed, ValueError("again"), 0) is True
    assert (row(root).status, row(root).attempts) == (READY, 2)

    assert notes() == []
    assert record_of(root) == (None, None, False)


# -- timeouts and the reconcilers ---------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_a_tasks_own_timeout_reaches_the_remaining_the_reconcilers_read(
    monkeypatch, worker
):
    """
    The reconcilers budget a tick from django-ox's remaining(), which reads
    the deadline the worker publishes for the attempt. A task that declares
    its own timeout publishes one with nothing in the options, and both
    reconcilers, run inside that attempt, see it.
    """
    batch = batches.create("swept")
    batch.add(tasks.plain_echo, 1)
    batch.seal()
    assert worker.run_once() is True  # the member
    assert not worker.timeouts.enabled
    policy_tasks.reconciles_under_its_own_timeout.enqueue()

    with remaining_as_the_reconcilers_read_it(monkeypatch) as seen:
        assert worker.run_once() is True

    readers = {module for module, _ in seen}
    assert readers == {"oxpull.batches", "oxpull.workflows"}, seen
    assert all(value is not None and 0 < value <= 8 for _, value in seen), seen
    # A quarter of what is left, under the five seconds a tick gets without one.
    assert 0 < tasks.STATE["backstop_budget"] <= 2, tasks.STATE
    assert tasks.STATE["reconciled"][0] == 1
    assert OxpullBatch.objects.get(pk=batch.id).status == OxpullBatch.Status.COMPLETE


def a_sealed_batch_to_sweep(name):
    """
    A sealed batch with no members, so the next tick's sweep visits it and
    reads remaining() on the way in. The workflow tick reads it whatever is
    open, for its backstop budget.
    """
    batches.create(name).seal()


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("module", [batches, workflows], ids=["batch", "workflow"])
def test_the_lazily_built_reconcilers_keep_the_backend_defaults(monkeypatch, module):
    """
    The reconcilers are plain tasks built on access, so they declare no
    policy of their own: the backend's budget goes on their row, and their
    attempt runs under the worker's timeout, or none. A task with a timeout
    of its own that ran just before, on the same worker and thread, leaves
    nothing behind for them.
    """
    reconciler = module.reconcile
    assert isinstance(reconciler, PolicyTask)
    assert (reconciler.max_attempts, reconciler.backoff, reconciler.timeout) == (
        None,
        None,
        None,
    )

    unlimited = Worker(backoff_initial=0, poll_interval=0.01)
    a_sealed_batch_to_sweep("unlimited")
    result = reconciler.enqueue()
    assert budget(result) == BACKEND_BUDGET
    assert result.task == reconciler
    with remaining_as_the_reconcilers_read_it(monkeypatch) as seen:
        assert unlimited.run_once() is True
    assert row(result).status == SUCCESSFUL
    assert seen
    assert {value for _, value in seen} == {None}, seen

    limited = Worker(backoff_initial=0, poll_interval=0.01, task_timeout=60)
    policy_tasks.reconciles_under_its_own_timeout.enqueue()
    assert limited.run_once() is True
    assert 0 < tasks.STATE["remaining_after"] <= 8
    a_sealed_batch_to_sweep("limited")
    result = module.reconcile.enqueue()
    with remaining_as_the_reconcilers_read_it(monkeypatch) as seen:
        assert limited.run_once() is True
    assert row(result).status == SUCCESSFUL
    assert seen
    assert all(value is not None and 8 < value <= 60 for _, value in seen), seen
