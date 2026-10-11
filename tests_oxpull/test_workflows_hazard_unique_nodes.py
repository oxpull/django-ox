"""
A unique task as a workflow node.

A unique task deduplicates while a task with the same key is READY or
RUNNING: enqueueing it again returns the pending task. Workflow.add()
enqueues through the backend, so a unique node that add() accepted would
be a row that an application call or another workflow already holds, or
a row that a later enqueue of the same call is deduplicated onto. Every
holder's edges gate that one row, and every holder's reconciler can
discard it. `oxpull` 1.2.0 refuses a unique task as a batch or workflow
callback (batches._reject_unique). The tests below also require node
handling to prevent the hazards described in their docstrings.

What is required: add() refuses a unique task as a node. It decides with
the same metadata the enqueue itself uses (the @unique decorator, or
UNIQUE_TASKS on the backend the task enqueues through), whether or not
anything holds the key, and it refuses before anything happens: no call to
the backend's enqueue(), no task_enqueued signal, no task, lock, node or
edge row, and no change to the workflow row. add_node() checks all of it.

Every test then drives a real worker (claim_one, execute, run_once), the
reconciler's own body and public APIs, and asserts, separately, the
contract each party was given:

  * a workflow runs a node only after every upstream it declared has
    succeeded, and a workflow in which nothing failed runs every node and
    completes with on_success;
  * an ordinary enqueue that was accepted runs, unless somebody discards it.

Each test collects every problem before it asserts, so a failure shows the
accepted node and the damage that followed it together.

Two fixes that look right fail here. One refuses a unique node only when
its key is already held: in the node-first test the key is free when add()
runs, so that fix accepts the node, and the application's later enqueue of
the same call is discarded with it. The other enqueues first and refuses
afterwards: its rollback leaves no rows, but add_node() sees the enqueue()
call and the task_enqueued signal.

The "not-unique" parameter runs the same shapes with an ordinary task,
which add() must accept. It passed before the refusal existed, which
showed that the assertions can be satisfied and that uniqueness was the
cause of every failure next to it.
"""

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest import mock

import pytest
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.tasks import task
from django.tasks.exceptions import TaskException
from django.tasks.signals import task_enqueued
from django.test import override_settings

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.backend import OxpullBackend
from oxpull.models import (
    OxpullUniqueLock,
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
)
from oxpull.unique import unique
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, order, reconcile, stored

pytestmark = pytest.mark.django_db

NOT_UNIQUE = "not-unique"

# What a refusal may raise. The message must also say the task is unique,
# so an unrelated ValueError or configuration error does not count.
REFUSALS = (ImproperlyConfigured, TaskException, ValueError)


# -- tasks -------------------------------------------------------------
#
# Module-level, as django.tasks requires, and resolved by the worker from
# this module's dotted path.


@unique
@task(backend="flows")
def unique_step(label):
    return tasks._record(label)


@unique
@task(backend="flows_deferred")
def unique_deferred_step(label):
    return tasks._record(label)


# Unique only through OPTIONS["UNIQUE_TASKS"], which the `shape` fixture
# sets for its "UNIQUE_TASKS" parameter.
@task(backend="flows")
def listed_step(label):
    return tasks._record(label)


@task(backend="flows_deferred")
def listed_deferred_step(label):
    return tasks._record(label)


# alias -> (ordinary node, failing node, @unique task, UNIQUE_TASKS task)
BACKENDS = {
    "flows": (tasks.step, tasks.failing_step, unique_step, listed_step),
    "flows_deferred": (
        tasks.deferred_step,
        tasks.failing_deferred_step,
        unique_deferred_step,
        listed_deferred_step,
    ),
}


def unique_tasks_on(listed):
    """settings.TASKS with UNIQUE_TASKS set per alias, from {alias: [task]}."""
    configured = dict(settings.TASKS)
    for alias, listed_tasks in listed.items():
        config = settings.TASKS[alias]
        configured[alias] = {
            **config,
            "OPTIONS": {
                **config["OPTIONS"],
                "UNIQUE_TASKS": [each.module_path for each in listed_tasks],
            },
        }
    return configured


@dataclass(frozen=True)
class Shape:
    alias: str
    marking: str
    step: Any
    failing: Any
    # The task each test is about: unique, except in the control.
    shared: Any

    def worker(self):
        return OxpullWorker(
            backend_alias=self.alias, backoff_initial=0, poll_interval=0.01
        )


@pytest.fixture(params=["flows", "flows_deferred"])
def backend_alias(request):
    return request.param


@pytest.fixture(params=["decorator", "UNIQUE_TASKS", NOT_UNIQUE])
def shape(request, backend_alias):
    step, failing, decorated, listed = BACKENDS[backend_alias]
    marking = request.param
    if marking != "UNIQUE_TASKS":
        shared = decorated if marking == "decorator" else step
        yield Shape(backend_alias, marking, step, failing, shared)
        return
    # Each listed task is listed on its own alias only, so a refusal that
    # reads UNIQUE_TASKS from one fixed backend fails on the other alias.
    listed_on_own_alias = {
        alias: [listed_task] for alias, (*_, listed_task) in BACKENDS.items()
    }
    with override_settings(TASKS=unique_tasks_on(listed_on_own_alias)):
        yield Shape(backend_alias, marking, step, failing, listed)


# -- helpers -----------------------------------------------------------


@contextmanager
def enqueues_of(node_task):
    """
    Collect, as descriptions, every enqueue of `node_task` while the block
    runs: calls to the backend's enqueue() and task_enqueued signals.
    """
    seen = []
    path = node_task.module_path
    original = OxpullBackend.enqueue

    def spy(backend, enqueued_task, args, kwargs):
        if enqueued_task.module_path == path:
            seen.append(f"a call to the {backend.alias!r} backend's enqueue()")
        return original(backend, enqueued_task, args, kwargs)

    def receiver(sender, task_result, **kwargs):
        if task_result.task.module_path == path:
            seen.append(f"a task_enqueued signal for row {str(task_result.id)[:8]}")

    task_enqueued.connect(receiver, weak=False)
    try:
        with mock.patch.object(OxpullBackend, "enqueue", spy):
            yield seen
    finally:
        task_enqueued.disconnect(receiver)


def footprint(flow):
    """Everything a refused add() must leave exactly as it found it."""
    return {
        "task rows": OxTask.objects.count(),
        "unique lock rows": OxpullUniqueLock.objects.count(),
        "node rows": OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count(),
        "edge rows": OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count(),
        "workflow row": OxpullWorkflow.objects.filter(pk=flow.id).values().get(),
    }


def add_node(problems, shape, flow, name, node_task, *args, **kwargs):
    """
    flow.add() of the task the test is about. Returns its result, or None
    when add() refused it.

    An ordinary task must be accepted. A unique task must be refused, and
    refused before anything happened; anything else is appended to
    `problems`.
    """
    if shape.marking == NOT_UNIQUE:
        result = flow.add(name, node_task, *args, **kwargs)
        assert flow.node_ids()[name] == uuid.UUID(str(result.id))
        return result

    where = f"{flow.name}.add({name!r}, {node_task.module_path})"
    before = footprint(flow)
    with enqueues_of(node_task) as enqueued:
        try:
            result = flow.add(name, node_task, *args, **kwargs)
        except REFUSALS as exc:
            refusal = exc
        else:
            refusal = None
    if refusal is None:
        problems.append(
            f"{where} accepted a unique task as a node, as row "
            f"{str(result.id)[:8]}; add() must refuse it"
        )
        return result
    if "unique" not in str(refusal).lower():
        problems.append(
            f"{where} raised {type(refusal).__name__} without saying the task "
            f"is unique: {refusal}"
        )
    if enqueued:
        problems.append(
            f"{where} refused only after {' and '.join(enqueued)}; it must "
            "refuse before enqueueing anything"
        )
    after = footprint(flow)
    changed = [
        f"{label} {before[label]} became {after[label]}"
        for label in before
        if after[label] != before[label]
    ]
    if changed:
        problems.append(f"{where} refused, but changed: " + "; ".join(changed))
    return None


def settle(worker, passes=4):
    """Drain and reconcile until nothing moves: a fleet plus the schedule."""
    for _ in range(passes):
        drain(worker)
        reconcile()


def _row(task_id):
    return OxTask.objects.filter(pk=task_id).first()


def _state(task_id):
    row = _row(task_id)
    if row is None:
        return "absent"
    return f"{row.status} attempts={row.attempts}"


def describe(*flows):
    """Every workflow row and node, with a row id prefix to show sharing."""
    lines = []
    for flow in flows:
        row = stored(flow)
        lines.append(
            f"{row.name}: status={row.status} failed_count={row.failed_count} "
            f"cancelled_count={row.cancelled_count} "
            f"on_success={'enqueued' if row.success_task_id else 'not enqueued'}"
        )
        for name, task_id in flow.node_ids().items():
            lines.append(f"  {name} -> row {str(task_id)[:8]} {_state(task_id)}")
    lines.append(f"execution order: {order()}")
    return "\n".join(lines)


def ran_past_an_unsucceeded_upstream(flow):
    """
    Edges of `flow` whose downstream row has been claimed although its
    upstream row exists and has not succeeded: this workflow's own ordering
    contract, broken.
    """
    broken = []
    edges = OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).values_list(
        "upstream_task_id", "downstream_task_id"
    )
    for upstream_id, downstream_id in edges:
        up, down = _row(upstream_id), _row(downstream_id)
        if down is None or down.attempts == 0 or up is None:
            continue
        if up.status != OxTask.Status.SUCCESSFUL:
            broken.append(
                f"{flow.name}: row {str(downstream_id)[:8]} was claimed "
                f"({down.status}) while its upstream is {up.status}"
            )
    return broken


def not_run_clean(flow):
    """
    What is wrong with `flow`, which has no failing node of its own: every
    node should have succeeded and the workflow completed with on_success.
    """
    row = stored(flow)
    problems = []
    if row.status != OxpullWorkflow.Status.COMPLETE:
        problems.append(f"{row.name} is {row.status}, not COMPLETE")
    else:
        if row.failed_count or row.cancelled_count:
            problems.append(
                f"{row.name} completed with failed_count={row.failed_count} "
                f"cancelled_count={row.cancelled_count}"
            )
        if row.success_task_id is None:
            problems.append(f"{row.name}'s on_success was not enqueued")
    for name, task_id in flow.node_ids().items():
        node = _row(task_id)
        if node is None or node.status != OxTask.Status.SUCCESSFUL:
            problems.append(f"{row.name}.{name} is {_state(task_id)}")
    return problems


def not_run_for_the_application(queued):
    """
    What is wrong with a call the application enqueued on its own: it
    should have run, on a row that belongs to no workflow.
    """
    problems = []
    holders = OxpullWorkflowNode.objects.filter(task_id=queued.id).values_list(
        "workflow_id", "name"
    )
    for workflow_id, name in holders:
        workflow_name = OxpullWorkflow.objects.get(pk=workflow_id).name
        problems.append(
            f"the application's own enqueue, row {queued.id[:8]}, is also "
            f"node {workflow_name}.{name}"
        )
    own = _row(queued.id)
    if own is None or own.status != OxTask.Status.SUCCESSFUL:
        problems.append(
            f"the application's own enqueue, row {queued.id[:8]}, is "
            f"{_state(queued.id)}: it never ran"
        )
    return problems


def report(problems, *flows, extra=()):
    return "\n".join([*problems, "", *extra, describe(*flows)])


# -- controls ----------------------------------------------------------


def test_each_parameter_marks_the_task_as_it_says(shape):
    """
    A control, and it passes today. It pins that the unique parameters
    really deduplicate and the not-unique one really does not, and what
    1.2.0 already refuses: a unique task as a workflow callback.
    """
    first = shape.shared.enqueue("key")
    second = shape.shared.enqueue("key")
    if shape.marking == NOT_UNIQUE:
        assert first.id != second.id
        return
    assert first.id == second.id
    with pytest.raises(ImproperlyConfigured, match="unique"):
        workflows.create("callback", on_complete=shape.shared, on_complete_args=["k"])


def test_add_accepts_a_task_that_is_unique_only_on_another_backend():
    """
    A control, and it passes today. UNIQUE_TASKS belongs to one backend, and
    so does the enqueue's deduplication. listed_step is listed on "flows"
    only, so through "flows_deferred" it is an ordinary task: its enqueue
    writes a new row every time, and add() must accept it there. A refusal
    that asks whether any backend lists the task refuses it, and fails here.
    """
    with override_settings(TASKS=unique_tasks_on({"flows": [listed_step]})):
        elsewhere = listed_step.using(backend="flows_deferred")
        # Preconditions: unique on its own backend, ordinary on the other.
        assert listed_step.enqueue("key").id == listed_step.enqueue("key").id
        assert elsewhere.enqueue("key").id != elsewhere.enqueue("key").id

        flow = workflows.create("elsewhere")
        first = flow.add("first", elsewhere, "node")
        second = flow.add("second", elsewhere, "node", depends_on="first")

    assert first.id != second.id
    assert flow.node_ids() == {
        "first": uuid.UUID(first.id),
        "second": uuid.UUID(second.id),
    }


# -- two workflows share one unique node --------------------------------


def test_a_failure_in_one_workflow_does_not_cancel_a_node_of_another(shape):
    """
    doomed: bad (fails) -> shared.  healthy: shared -> after.

    Both add the same unique call. Today healthy's `shared` is doomed's row,
    held by doomed's edge from `bad`. When `bad` fails, doomed's
    cancellation discards that row, and healthy's reconciler then cancels
    `after` because its upstream is DISCARDED. Nothing in healthy failed,
    yet none of its nodes runs. It completes with failed_count=0 and
    on_success withheld, and nothing says which workflow did it.
    """
    problems = []
    doomed = workflows.create("doomed")
    doomed.add("bad", shape.failing, "bad")
    add_node(
        problems, shape, doomed, "shared", shape.shared, "shared", depends_on="bad"
    )
    doomed.seal()

    healthy = workflows.create(
        "healthy", on_success=tasks.record_callback, on_success_args=["healthy"]
    )
    shared = add_node(problems, shape, healthy, "shared", shape.shared, "shared")
    healthy.add("after", shape.step, "after", depends_on="shared" if shared else None)
    healthy.seal()

    settle(shape.worker())

    problems += not_run_clean(healthy) + ran_past_an_unsucceeded_upstream(doomed)
    assert not problems, report(problems, doomed, healthy)


def test_an_unsealed_workflows_failure_does_not_hang_another_workflow(shape):
    """
    stalled (never sealed): bad (fails) -> shared.  waiting: shared -> after.

    The workflow that owns the failure is OPEN, because the process building
    it died before seal(). Its nodes run anyway. The reconciler sweeps
    SEALED workflows only, so nothing cancels the shared row. waiting's own
    sweep finds no dead node among its own nodes, because `bad` is not
    one of them. Today the shared row stays READY and gated forever, waiting
    stays SEALED forever, and no callback fires.
    """
    problems = []
    stalled = workflows.create("stalled")
    stalled.add("bad", shape.failing, "bad")
    add_node(
        problems, shape, stalled, "shared", shape.shared, "shared", depends_on="bad"
    )

    waiting = workflows.create(
        "waiting", on_success=tasks.record_callback, on_success_args=["waiting"]
    )
    shared = add_node(problems, shape, waiting, "shared", shape.shared, "shared")
    waiting.add("after", shape.step, "after", depends_on="shared" if shared else None)
    waiting.seal()

    settle(shape.worker())

    problems += not_run_clean(waiting) + ran_past_an_unsucceeded_upstream(stalled)
    progress = waiting.progress()
    assert not problems, report(
        problems,
        stalled,
        waiting,
        extra=[
            (
                f"waiting.progress(): total={progress.total} "
                f"pending={progress.pending} blocked={progress.blocked}"
            )
        ],
    )


# -- a unique node and the application's own enqueue of the same call ---


def test_a_workflow_does_not_cancel_a_task_the_application_enqueued(shape):
    """
    The application comes first. It enqueues the unique call on its own,
    say from a view. A workflow then adds the same call behind an upstream
    that fails.

    Today add() returns the application's pending row. The workflow's edge
    gates it, so it waits on `bad`'s retries (under DEFER_BLOCKED_NODES
    add() also writes DEFERRED_UNTIL onto it). When `bad` fails, the
    reconciler discards it. The task the application enqueued, which was
    never part of any workflow, never runs.
    """
    problems = []
    queued = shape.shared.enqueue("shared")

    flow = workflows.create("adopter")
    flow.add("bad", shape.failing, "bad")
    add_node(problems, shape, flow, "shared", shape.shared, "shared", depends_on="bad")
    flow.seal()

    settle(shape.worker())

    problems += ran_past_an_unsucceeded_upstream(flow)
    problems += not_run_for_the_application(queued)
    assert not problems, report(
        problems, flow, extra=[f"application enqueue: row {queued.id[:8]}"]
    )


def test_an_application_enqueue_made_after_a_unique_node_was_added_runs(shape):
    """
    The node comes first. A workflow adds a unique call behind an upstream
    that fails, while nothing holds that call's key. Later the application
    enqueues the same call on its own.

    Today add() takes the free key for the node. The application's enqueue
    is deduplicated onto the node's row and returns the node's id, so the
    application's call waits behind `bad`, a node of a workflow it never
    joined. When `bad` fails, the reconciler discards that row with 0
    attempts, and the application's call never runs.

    A refusal that checks whether the key is already held accepts this
    node, because the key is free when add() runs, and fails here twice: at
    add(), and when the application's call is discarded.
    """
    problems = []
    flow = workflows.create("first")
    flow.add("bad", shape.failing, "bad")
    add_node(problems, shape, flow, "shared", shape.shared, "shared", depends_on="bad")
    flow.seal()

    queued = shape.shared.enqueue("shared")

    settle(shape.worker())

    problems += ran_past_an_unsucceeded_upstream(flow)
    problems += not_run_for_the_application(queued)
    assert not problems, report(
        problems, flow, extra=[f"application enqueue: row {queued.id[:8]}"]
    )


def test_a_node_does_not_adopt_an_execution_that_began_before_its_upstream(
    shape,
):
    """
    extract -> transform (unique) -> load, where a worker has already
    claimed the application's own `transform` call when the workflow is
    built.

    Today add() returns that RUNNING row as the `transform` node. Its
    execution began before `extract` existed and finishes SUCCESSFUL, which
    satisfies load's edge, so `load` runs before `extract`. The workflow
    then completes clean, with on_success enqueued.
    """
    queued = shape.shared.enqueue("transform")
    worker = shape.worker()
    in_flight = worker.claim_one()
    # Precondition, true under any fix: no workflow exists yet.
    assert in_flight is not None
    assert uuid.UUID(str(in_flight.id)) == uuid.UUID(queued.id)

    problems = []
    flow = workflows.create(
        "late", on_success=tasks.record_callback, on_success_args=["late"]
    )
    flow.add("extract", shape.step, "extract")
    transform = add_node(
        problems,
        shape,
        flow,
        "transform",
        shape.shared,
        "transform",
        depends_on="extract",
    )
    flow.add(
        "load",
        shape.step.using(priority=10),
        "load",
        depends_on="transform" if transform else "extract",
    )
    flow.seal()

    # The rest of what run_once() does with the row it claimed.
    worker.execute(in_flight, inline=True)
    nodes = flow.node_ids()
    extract = _row(nodes["extract"])
    if transform is not None:
        node = _row(nodes["transform"])
        if node.attempts and extract.status != OxTask.Status.SUCCESSFUL:
            problems.append(
                f"late.transform is {node.status} (attempts={node.attempts}) "
                f"while its upstream extract is {extract.status} "
                f"(attempts={extract.attempts})"
            )

    settle(worker)

    ran = order()
    if "load" not in ran or "extract" not in ran:
        problems.append(f"load or extract never ran: {ran}")
    elif ran.index("load") < ran.index("extract"):
        problems.append(f"load ran before extract: {ran}")
    problems += not_run_clean(flow)
    assert not problems, report(
        problems, flow, extra=[f"application enqueue: row {queued.id[:8]}"]
    )
