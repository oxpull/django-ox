"""
A waiting node is released by the `oxpull.worker.OxpullWorker` that ran its upstream,
whatever backend alias that worker is on.

add() used to park a blocked node at a far-future run_after only when the
node's own backend declared DEFER_BLOCKED_NODES, and only an OxpullWorker
whose own backend declared the option released it. A claim is scoped to
queue names, not to backend aliases, so a parent could be claimed and
succeeded by an OxpullWorker whose alias did not defer, and nothing
released the children until the reconciler's sweep, which released at most
a thousand per tick, install-wide.

Every node with dependencies now waits in django-ox's WAITING status, and
the option is gone. Nothing runs out of order either way: this is liveness
only. What the tests assert is the fast path: the `oxpull.worker.OxpullWorker`
that wrote the parent's SUCCESSFUL releases the child, and live worker loops
finish a chain with no reconciler tick.

A worker on a backend that declares WORKFLOWS releases, and so does a worker
on a backend that does not, "limited", once any backend on the database
declares it. The test_control_* tests show the red is not vacuous:
the same sequence with the workflow backend's own worker releases at once,
the reconciler releases what the peer left, and live loops finish the chain
well inside WAIT.
"""

import threading
import time

import pytest
from django.core import checks
from django.db import connections
from django.tasks import task_backends

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import flow_worker, reconcile
from .test_workflows_deferred import deferring_worker

# Committed state throughout, so a worker loop on its own thread and
# connection sees exactly what a worker process would.
pytestmark = pytest.mark.django_db(transaction=True)

WAIT = 10.0

WAITING = OxTask.Status.WAITING

# OxpullWorker aliases that share the "default" queue with flows_deferred. flows
# declares WORKFLOWS; limited declares none, only a rate limit on tasks this
# test never enqueues.
PEERS = {
    "flows": flow_worker,
    "limited": lambda: OxpullWorker(
        backend_alias="limited", backoff_initial=0, poll_interval=0.01
    ),
}


def _task(result):
    return OxTask.objects.get(pk=result.id)


def _waiting(ids):
    return OxTask.objects.filter(pk__in=ids, status=WAITING)


def _errors(alias):
    return [
        message.id
        for message in task_backends[alias].check()
        if message.level >= checks.ERROR
    ]


def _chain_on_the_deferring_backend(name, length):
    flow = workflows.create(name)
    results, previous = [], None
    for i in range(length):
        label = f"n{i}"
        depends_on = None if previous is None else previous
        results.append(
            flow.add(label, tasks.deferred_step, label, depends_on=depends_on)
        )
        previous = label
    flow.seal()
    return results


# -- one alias per workflow, a peer `oxpull.worker.OxpullWorker` runs the parent ---


@pytest.mark.parametrize("peer", ["flows", "limited"])
def test_a_deferred_child_is_released_by_the_oxpull_worker_that_ran_its_parent(peer):
    """
    Every node of this workflow is on flows_deferred. The user mixed
    nothing inside the workflow; two backends merely share a queue name,
    which is Django's default (QUEUES defaults to ["default"]).
    """
    assert _errors("flows_deferred") == []
    assert _errors(peer) == []
    parent, child = _chain_on_the_deferring_backend(f"one-alias-{peer}", 2)
    assert _task(child).status == WAITING

    worker = PEERS[peer]()
    assert worker.run_once() is True
    ran = _task(parent)
    assert ran.status == OxTask.Status.SUCCESSFUL
    assert worker.worker_id in ran.worker_ids, "the peer did not run the parent"

    parked = _task(child).status
    claimed = deferring_worker().claim_one()
    assert claimed is not None and str(claimed.pk) == str(child.id), (
        f"parent SUCCESSFUL, written by an OxpullWorker on {peer!r}, and its "
        f"child is not claimable: it is {parked}; only the reconciler will "
        "release it"
    )


def test_control_the_deferring_worker_releases_the_same_child_at_once():
    parent, child = _chain_on_the_deferring_backend("one-alias-control", 2)
    assert _task(child).status == WAITING
    worker = deferring_worker()
    assert worker.run_once() is True
    assert _task(parent).status == OxTask.Status.SUCCESSFUL
    claimed = deferring_worker().claim_one()
    assert claimed is not None and str(claimed.pk) == str(child.id)


def test_control_the_reconciler_releases_what_the_peer_left():
    parent, child = _chain_on_the_deferring_backend("one-alias-reconciled", 2)
    assert PEERS["limited"]().run_once() is True
    assert _task(parent).status == OxTask.Status.SUCCESSFUL
    # Whether the child is still waiting here is deliberately not asserted.
    # One reconciler pass must leave it claimable either way.
    reconcile()
    assert _task(child).status == OxTask.Status.READY
    claimed = deferring_worker().claim_one()
    assert claimed is not None and str(claimed.pk) == str(child.id)


# -- one workflow spanning two workflow backends ------------------------------


def test_a_fan_out_on_a_deferring_backend_is_released_when_its_root_succeeds():
    """
    The root is on flows, the children on flows_deferred, and add() accepts
    all of them into one workflow. Wider than the reconciler's release page,
    so the failure message can report how many ticks the backstop needs.
    """
    width = workflows.BACKSTOP_PAGE + 500
    flow = workflows.create("mixed-aliases")
    root = flow.add("root", tasks.step, "root")
    children = [
        flow.add(f"c{i:04d}", tasks.deferred_step, f"c{i:04d}", depends_on="root")
        for i in range(width)
    ]
    flow.seal()
    ids = [child.id for child in children]
    assert _waiting(ids).count() == width

    worker = flow_worker()
    assert worker.run_once() is True
    ran = _task(root)
    assert ran.status == OxTask.Status.SUCCESSFUL
    assert worker.worker_id in ran.worker_ids

    still_parked = _waiting(ids).count()
    claimable_now = deferring_worker().claim_one() is not None
    ticks, per_tick = 0, []
    while _waiting(ids).count() and ticks < 10:
        reconcile()
        ticks += 1
        per_tick.append(_waiting(ids).count())
    assert still_parked == 0, (
        f"root SUCCESSFUL, written by an OxpullWorker on 'flows'; {still_parked} "
        f"of {width} children still WAITING; a worker on flows_deferred could "
        f"claim one: {claimable_now}; still waiting after each reconciler pass: "
        f"{per_tick} ({ticks} passes to release them all)"
    )


# -- live worker loops, no reconciler ----------------------------------------


class Loop:
    """A worker's production run() loop on its own thread and connection."""

    def __init__(self, worker):
        self.worker = worker
        self.errors = []
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.worker.run()
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            connections.close_all()

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.worker.request_stop()
        self.thread.join(timeout=WAIT + 30)
        assert not self.thread.is_alive()
        assert not self.errors, self.errors


def _wait_for(predicate, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _statuses(results):
    return [
        (row.args[0], row.status, row.run_after)
        for row in OxTask.objects.filter(pk__in=[r.id for r in results]).order_by(
            "args"
        )
    ]


def _all_succeeded(results):
    return OxTask.objects.filter(
        pk__in=[r.id for r in results], status=OxTask.Status.SUCCESSFUL
    ).count() == len(results)


def _run_chain_under_live_loops(name, parent_runner):
    """
    Start `parent_runner`'s loop alone until it has run n0, then a loop on
    flows_deferred beside it, and give the chain WAIT seconds. Neither flows
    nor flows_deferred schedules anything, so no reconciler tick can happen.
    """
    chain = _chain_on_the_deferring_backend(name, 3)
    loops = [Loop(parent_runner()).start()]
    try:
        assert _wait_for(
            lambda: _task(chain[0]).status == OxTask.Status.SUCCESSFUL, WAIT
        ), _statuses(chain)
        assert loops[0].worker.worker_id in _task(chain[0]).worker_ids
        loops.append(Loop(deferring_worker()).start())
        started = time.monotonic()
        finished = _wait_for(lambda: _all_succeeded(chain), WAIT)
        elapsed = time.monotonic() - started
    finally:
        for loop in loops:
            loop.stop()
    return chain, finished, elapsed


def test_a_chain_on_a_deferring_backend_finishes_under_live_loops_without_a_tick():
    """
    Two ox_worker-shaped loops share the "default" queue: one on flows and
    one on flows_deferred. The flows loop happens to claim the first node
    (here, by starting first; in production, by polling first or by the
    other process restarting). With no reconciler dispatched, the rest of
    the chain must still run.
    """
    chain, finished, elapsed = _run_chain_under_live_loops("live-peer", flow_worker)
    assert finished, (
        f"n0 run by the 'flows' loop; after {elapsed:.1f}s with a "
        f"flows_deferred loop polling beside it, the chain reads "
        f"{_statuses(chain)}"
    )


def test_control_the_same_chain_finishes_when_the_deferring_loop_runs_it():
    chain, finished, elapsed = _run_chain_under_live_loops(
        "live-control", deferring_worker
    )
    assert finished, _statuses(chain)
    assert elapsed < WAIT / 2, elapsed
