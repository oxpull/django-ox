"""
Unique nodes let two workflows build a dependency cycle.

Workflow.add() documents that a cycle is unconstructible because every
edge points backwards in insertion order. That holds per workflow. The
gate, though, reads the global edge table keyed by task id, and a unique
task deduplicates to whichever row already holds its key. So:

    W1: a = unique_echo(1)            -> T1
        b = unique_add(1, 2) after a  -> T2, edge T1 -> T2
    W2: x = unique_add(1, 2)          -> dedups to T2
        y = unique_echo(1) after x    -> dedups to T1, edge T2 -> T1

T1 and T2 now each wait on the other. Neither is dead, so the reconciler
cancels nothing, and neither workflow ever completes.

Every test asserts the correct behaviour and accepts either fix: add()
refuses unique nodes (every one, as it now does, or only the one that
would close the cycle), or the workflows run to completion anyway.
A refusal counts only when it says the task is unique. Both outcomes
are checked through the public API, the real claim path and the real
reconciler pass. Accepting the nodes without preventing the cycle fails.
"""

import threading
import time

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.tasks.exceptions import TaskException

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, flow_worker, reconcile, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

# A refusal at add() is a correct fix, so it is caught. Nothing broader
# is: a database error must still fail the test.
REFUSALS = (TaskException, ImproperlyConfigured, ValueError)


def edge_cycle():
    """A cycle in the global edge table, as a list of task ids, or None."""
    graph = {}
    for up, down in OxpullWorkflowEdge.objects.values_list(
        "upstream_task_id", "downstream_task_id"
    ):
        graph.setdefault(up, set()).add(down)
    state, path = {}, []

    def visit(node):
        state[node] = "open"
        path.append(node)
        for nxt in graph.get(node, ()):
            if state.get(nxt) == "open":
                return [*path[path.index(nxt) :], nxt]
            if nxt not in state:
                found = visit(nxt)
                if found:
                    return found
        path.pop()
        state[node] = "done"
        return None

    for node in list(graph):
        if node not in state:
            found = visit(node)
            if found:
                return found
    return None


def settle(worker, rounds=5):
    """
    What a production install does while it waits: claim and run
    whatever the gate lets through, reap, and tick the reconciler.
    """
    for _ in range(rounds):
        drain(worker)
        worker.reap()
        reconcile()


def describe(flows):
    lines = [f"edge cycle: {edge_cycle()}"]
    for flow in flows:
        row = stored(flow)
        lines.append(f"{flow.name}: {row.status} {flow.progress()}")
    rows = OxTask.objects.values_list("task_path", "args", "status", "run_after")
    lines.append(f"task rows: {sorted(rows, key=str)}")
    return "\n".join(lines)


def assert_every_workflow_completed(flows):
    detail = describe(flows)
    assert edge_cycle() is None, (
        "the global edge table holds a cycle, which add() documents as "
        f"unconstructible\n{detail}"
    )
    for flow in flows:
        row = stored(flow)
        assert row.status == OxpullWorkflow.Status.COMPLETE, (
            f"{flow.name} never completes: its nodes wait on each other "
            f"across workflows and none of them is dead\n{detail}"
        )
        assert row.callback_task_id is not None, (
            f"{flow.name} completed without enqueueing on_complete\n{detail}"
        )


def refused_for_uniqueness(refusals):
    """Every refusal add() raised must say the task is unique."""
    others = [exc for exc in refusals if "unique" not in str(exc).lower()]
    assert not others, f"add() refused a node for another reason: {others!r}"


def try_add(flow, refusals, name, node_task, *args, depends_on=None):
    """flow.add(), with a refusal recorded rather than raised. None if refused."""
    try:
        return flow.add(name, node_task, *args, depends_on=depends_on)
    except REFUSALS as exc:
        refusals.append(exc)
        return None


def build_opposite_order(unique_a, unique_b):
    """The hazard's own sequence. Returns (w1, w2, refusals)."""
    refusals = []
    w1 = workflows.create(
        "w1", on_complete=tasks.record_callback, on_complete_args=["w1"]
    )
    a = try_add(w1, refusals, "a", unique_a, 1)
    b = try_add(w1, refusals, "b", unique_b, 1, 2, depends_on="a" if a else None)
    w2 = workflows.create(
        "w2", on_complete=tasks.record_callback, on_complete_args=["w2"]
    )
    x = try_add(w2, refusals, "x", unique_b, 1, 2)
    y = try_add(w2, refusals, "y", unique_a, 1, depends_on="x" if x else None)
    if None not in (a, b, x, y):
        # Not a precondition of the fix, but proof the run provoked the
        # hazard rather than passing because dedup never happened.
        tasks.STATE["dedup"] = (str(x.id) == str(b.id), str(y.id) == str(a.id))
    w1.seal()
    w2.seal()
    refused_for_uniqueness(refusals)
    return w1, w2, refusals


def test_two_workflows_sharing_unique_nodes_in_opposite_order_both_complete():
    w1, w2, _refusals = build_opposite_order(
        tasks.unique_echo.using(backend="flows"),
        tasks.unique_add.using(backend="flows"),
    )
    worker = flow_worker()
    if OxpullWorkflowNode.objects.exists():
        # add() accepted a node, so a worker on the gated backend must be
        # able to start on one.
        first = worker.claim_one()
        assert first is not None, (
            "a worker on the gated backend can claim nothing: every node of "
            f"both workflows waits on another\ndedup={tasks.STATE.get('dedup')}\n"
            f"{describe([w1, w2])}"
        )
        worker.execute(first, inline=True)
    settle(worker)
    assert_every_workflow_completed([w1, w2])


def test_a_ring_of_three_workflows_sharing_unique_nodes_all_complete():
    """
    The same hazard one workflow wider, so a fix that only looks for the
    reverse of the new edge does not pass.

        W1: echo(1) -> echo(2)
        W2: echo(2) -> echo(3)
        W3: echo(3) -> echo(1)
    """
    node = tasks.unique_echo.using(backend="flows")
    flows, refusals = [], []
    for i in range(3):
        head, tail = i + 1, (i + 1) % 3 + 1
        flow = workflows.create(
            f"ring-{i}", on_complete=tasks.record_callback, on_complete_args=[i]
        )
        if try_add(flow, refusals, "head", node, head):
            try_add(flow, refusals, "tail", node, tail, depends_on="head")
        flow.seal()
        flows.append(flow)
    refused_for_uniqueness(refusals)
    settle(flow_worker())
    assert_every_workflow_completed(flows)


def test_the_same_cycle_on_the_deferring_backend_both_complete():
    """
    DEFER_BLOCKED_NODES takes a different path through add():
    _defer_if_blocked runs on the id the enqueue returned. On a dedup hit
    that is another workflow's committed, visible row, and today it
    defers T1, which had been claimable, so the claim never even sees
    either node and the release sweep never clears either.
    """
    w1, w2, _refusals = build_opposite_order(
        tasks.unique_echo.using(backend="flows_deferred"),
        tasks.unique_add.using(backend="flows_deferred"),
    )
    settle(deferring_worker())
    assert_every_workflow_completed([w1, w2])


@pytest.mark.django_db(transaction=True)
def test_a_running_worker_completes_both_workflows_sharing_unique_nodes():
    """
    The real run() loop in its own thread: its own connection, its own
    reap on the first pass, the gated claim. The reconciler ticks beside
    it, as the schedule would on another worker.
    """
    w1, w2, _refusals = build_opposite_order(
        tasks.unique_echo.using(backend="flows"),
        tasks.unique_add.using(backend="flows"),
    )
    worker = OxpullWorker(backend_alias="flows", backoff_initial=0, poll_interval=0.01)

    def run():
        try:
            worker.run()
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        # A ceiling: the loop leaves as soon as both workflows are COMPLETE.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            reconcile()
            if all(
                stored(flow).status == OxpullWorkflow.Status.COMPLETE
                for flow in (w1, w2)
            ):
                break
            time.sleep(0.1)
    finally:
        worker.request_stop()
        thread.join(timeout=30)
    assert not thread.is_alive()
    assert_every_workflow_completed([w1, w2])
