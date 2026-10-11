"""
A gated claim whose scan spans an add() commit.

The gate's invariant (Workflow.add's docstring) is that a worker cannot
observe a node's task row without also observing every edge that gates
it. add() keeps its half: the task row, the node and the edges commit
together. The claim is where it can break on MySQL. There the claim is
one SELECT ... FOR UPDATE SKIP LOCKED with the gate as nested NOT EXISTS
subqueries. The locking clause makes InnoDB read the task table as a
current read, which sees rows committed while the scan runs. The
subqueries over the edge table stay consistent reads, answered from a
read view fixed when the statement first reads them (READ COMMITTED) or
at the transaction's first consistent read (REPEATABLE READ). A node
whose add() commits after that view began, and whose row the outer scan
reaches afterwards, is READY and unlocked to the outer read and has no
edge to the subquery. NOT EXISTS is true and the node is claimed while
its upstream is still running.

Both tests build one workflow with two roots. "doomed" is claimed and
held RUNNING through the window, then fails. "done" has already
succeeded. What a fix has to do to pass:

  * No child of doomed ever starts: no claim takes one while doomed is
    running, and nothing runs one after doomed has failed, the
    reconciler's pass included. The fix inserts every blocked node
    WAITING in the transaction that adds it, so the claim's own locking
    read, which reads only READY rows, excludes the node whatever its
    subqueries can see.
  * Claims keep working. Children of done are free to run, and a worker
    must run every one of them: during the race in the first test, and
    from inside the pinned read view in the second. They sit at a lower
    priority than every child of doomed, so a claim reaches them only
    after passing every blocked node, which keeps the window as wide as
    the blocked nodes alone make it. A fix that stops claiming, blocks
    every workflow node, or returns nothing once the first candidate it
    meets is blocked fails here.

Both tests failed on MySQL before blocked nodes were inserted WAITING.
Only the checks that a child of doomed never started are plain asserts.
Everything else calls pytest.fail: a setup step that went wrong (the held
claim, done's run, doomed still running, a thread that raised, the edge
count), a drain that never settles, and every check that claims keep
working. An implementation that stops claiming therefore fails loudly.
PostgreSQL (one snapshot per statement) and SQLite (serialised writers)
are the controls.
"""

from __future__ import annotations

import threading
import time

import pytest
from django.db import connections, transaction

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflowEdge

from . import tasks
from .racing import race_all
from .test_batches import session_isolation
from .test_workflows import flow_worker, order, reconcile

pytestmark = pytest.mark.django_db

# Blocked READY nodes ahead of each late node in claim order. The scan
# walks all of them before it reaches the late node, so this is what
# widens the window. 4000 costs about 17 s of real add() calls on MySQL,
# and a claim over it takes about 20 ms there.
BACKLOG = 4000
CLAIMERS = 2
# How long the adds race the claims. The race ends early at the first
# child of doomed that runs.
RACE_SECONDS = 10.0
# Bound on draining the workflow afterwards, so a claim that never
# settles fails the test rather than hanging it.
SETTLE_SECONDS = 180.0

# Node names, which are also the label each node records when it runs.
# Children of doomed carry the first prefix, children of done the second.
BLOCKED = "blocked-"
RUNNABLE = "runnable-"
# Below the default priority of every child of doomed. The claim orders
# by priority first, so it passes every blocked node before it reaches a
# runnable one.
RUNNABLE_PRIORITY = -1
# Ids per query when reading nodes back, under SQLite's bound-variable
# limit however many nodes the race added.
CHUNK = 500


def require(condition, message):
    """
    A check that is not the defect under test. pytest.fail raises
    Failed, which is not an AssertionError, so the MySQL marker never
    turns it into an expected failure.
    """
    if not condition:
        pytest.fail(message)


def doomed_is_running(held):
    return OxTask.objects.get(id=held.id).status == OxTask.Status.RUNNING


def two_roots(name):
    """
    An OPEN workflow with two roots, and the worker holding one of them.

    "doomed" is claimed and held, so it stays RUNNING until settle() lets
    it fail. "done" is added after that claim and run to SUCCESSFUL.
    """
    flow = workflows.create(name)
    flow.add("doomed", tasks.failing_step, "doomed")
    worker = flow_worker()
    held = worker.claim_one()
    require(
        held is not None and held.args == ["doomed"],
        f"setup: the worker's first claim returned "
        f"{None if held is None else held.args}, not doomed, the only READY node",
    )
    flow.add("done", tasks.step, "done")
    require(
        worker.run_once() is True and order() == ["done"],
        f"setup: the worker did not run done, a root with no upstream; ran {order()}",
    )
    require(doomed_is_running(held), "setup: doomed is not RUNNING")
    return flow, worker, held


def add_runnable(flow, name):
    flow.add(
        name,
        tasks.step.using(priority=RUNNABLE_PRIORITY),
        name,
        depends_on="done",
    )


def tasks_named(flow, prefix):
    """The task rows of every node whose name starts with prefix."""
    ids = sorted(
        str(task_id)
        for name, task_id in flow.node_ids().items()
        if name.startswith(prefix)
    )
    found = []
    for start in range(0, len(ids), CHUNK):
        found.extend(OxTask.objects.filter(id__in=ids[start : start + CHUNK]))
    return found


def ran(prefix):
    return [label for label in order() if label.startswith(prefix)]


def blocked_children_that_started():
    """Children of doomed that any worker ran, in the order they ran."""
    return ran(BLOCKED)


def blocked_children_claimed(flow):
    """Children of doomed whose task row shows a claim, with that row's state."""
    return sorted(
        (row.args[0], row.status, row.attempts)
        for row in tasks_named(flow, BLOCKED)
        if row.attempts or row.started_at is not None
    )


def run_until_idle(worker, held, deadline):
    """Run whatever the worker claims until it claims nothing and doomed is settled."""
    while True:
        require(
            time.monotonic() < deadline,
            f"the worker was still finding work after {SETTLE_SECONDS} s",
        )
        if worker.run_once():
            continue
        if OxTask.objects.get(id=held.id).status != OxTask.Status.READY:
            return
        # doomed's retry is written with no backoff, and on SQLite it can
        # take a millisecond to become eligible.
        time.sleep(0.01)


def settle(flow, worker, held):
    """
    Let doomed fail on the worker that holds it, then run everything a
    worker will run, seal, reconcile once, and run again.

    Every attempt of doomed fails, so it ends FAILED. The reconciler's
    pass comes after that failure, so a fix whose backstop hands a child
    of a failed node back to the claim is caught by the second drain.
    """
    deadline = time.monotonic() + SETTLE_SECONDS
    worker.execute(held)
    run_until_idle(worker, held, deadline)
    flow.seal()
    reconcile()
    run_until_idle(worker, held, deadline)
    status = OxTask.objects.get(id=held.id).status
    require(
        status == OxTask.Status.FAILED,
        f"setup: doomed ended {status}, not FAILED",
    )


def require_runnable_children_all_ran(flow):
    """Claims keep working: every child of done ran to SUCCESSFUL."""
    rows = tasks_named(flow, RUNNABLE)
    require(rows, "setup: the test added no child of done")
    names = sorted(row.args[0] for row in rows)
    not_successful = sorted(
        (row.args[0], row.status)
        for row in rows
        if row.status != OxTask.Status.SUCCESSFUL
    )
    require(
        not_successful == [] and sorted(ran(RUNNABLE)) == names,
        f"{len(not_successful)} of {len(names)} nodes whose only upstream, "
        f"done, had succeeded did not run to SUCCESSFUL: "
        f"{not_successful[:5]}. Ran: {len(ran(RUNNABLE))}",
    )


@pytest.mark.django_db(transaction=True)
def test_a_node_added_while_workers_scan_a_blocked_backlog_waits_for_its_upstream():
    """
    Adding to an OPEN workflow while workers drain it, with the backend's
    default options.

    Real worker claims (run_once, which also executes what it claims) on
    CLAIMERS threads, and real add() calls on one more, over a backlog of
    BACKLOG children of doomed built with add(). The adding thread adds
    pairs: a child of doomed, then a child of done. doomed stays RUNNING
    for the whole race and fails afterwards, so a correct gate never
    starts a child of it, and the children of done give the workers real
    work throughout.
    """
    flow, worker, held = two_roots("fan-out-while-draining")
    for i in range(BACKLOG):
        name = f"{BLOCKED}{i:05d}"
        flow.add(name, tasks.step, name, depends_on="doomed")

    stop = threading.Event()

    def claimer():
        claiming = flow_worker()
        while not stop.is_set():
            if claiming.run_once() and blocked_children_that_started():
                stop.set()
        return "claimer"

    def adder():
        deadline = time.monotonic() + RACE_SECONDS
        pairs = 0
        while not stop.is_set() and time.monotonic() < deadline:
            blocked = f"{BLOCKED}late{pairs:05d}"
            flow.add(blocked, tasks.step, blocked, depends_on="doomed")
            add_runnable(flow, f"{RUNNABLE}{pairs:05d}")
            pairs += 1
        stop.set()
        return pairs

    results, errors = race_all([claimer] * CLAIMERS + [adder])

    require(errors == [], f"a thread raised: {errors[:2]}")
    pairs = [result for result in results if result != "claimer"]
    require(
        len(results) == CLAIMERS + 1 and len(pairs) == 1,
        f"setup: the race returned {results}",
    )
    ran_while_adding = ran(RUNNABLE)
    require(doomed_is_running(held), "setup: doomed stopped running during the race")

    settle(flow, worker, held)

    assert blocked_children_that_started() == [], (
        f"after {pairs[0]} pairs of add() calls racing {CLAIMERS} workers over "
        f"{BACKLOG} blocked nodes, a worker ran "
        f"{blocked_children_that_started()} although their only upstream, "
        f"doomed, never succeeded. Claimed rows (name, status, attempts): "
        f"{blocked_children_claimed(flow)}"
    )
    assert blocked_children_claimed(flow) == []
    require(
        ran_while_adding,
        f"no worker ran a child of done while {pairs[0]} pairs of add() calls "
        f"raced them: claims stopped",
    )
    require_runnable_children_all_ran(flow)


@pytest.mark.django_db(transaction=True)
def test_a_gated_claim_does_not_admit_a_node_committed_after_its_read_view_began():
    """
    The same window, made deterministic at REPEATABLE READ.

    Under REPEATABLE READ the transaction's first consistent read fixes
    the read view the gate's subqueries are answered from. The edge read
    below fixes it, a concurrent add() then commits a child of doomed and
    its edge, and a worker's own claim_one() runs. The outer locking read
    sees that child, and the subquery does not see its edge. A child of
    done, committed before the view began and behind the late child in
    claim order, is what the claim must return instead.

    Pinning the view with an earlier statement stands in for the scan
    itself: at READ COMMITTED the first subquery probe fixes the view,
    and the outer scan keeps walking the backlog after it. The first test
    reproduces that form through real worker threads.
    """
    if connections["default"].vendor == "sqlite":
        pytest.skip(
            "a reading SQLite transaction holds its shared lock, so the "
            "concurrent add() could not commit under it"
        )
    flow, worker, held = two_roots("late-under-a-read-view")
    runnable = f"{RUNNABLE}early"
    add_runnable(flow, runnable)
    late = f"{BLOCKED}late"

    def add_and_commit():
        flow.add(late, tasks.step, late, depends_on="doomed")
        return "added"

    claiming = flow_worker()
    with session_isolation("REPEATABLE READ"), transaction.atomic():
        # The transaction's first consistent read; the read view starts here.
        edges_seen = OxpullWorkflowEdge.objects.values_list("pk").count()
        require(edges_seen == 1, f"setup: the read view saw {edges_seen} edges")
        results, errors = race_all([add_and_commit])
        require(
            (results, errors) == (["added"], []),
            f"setup: the concurrent add() returned {results}, raised {errors[:2]}",
        )
        claimed = claiming.claim_one()

    edges = OxpullWorkflowEdge.objects.count()
    require(edges == 2, f"setup: {edges} edges after the add, not 2")
    require(doomed_is_running(held), "setup: doomed is not RUNNING")
    assert claimed is None or claimed.args != [late], (
        f"claim_one() returned {late} with status {claimed.status} while its "
        f"only upstream, doomed, was RUNNING"
    )
    require(
        claimed is not None and claimed.args == [runnable],
        f"claim_one() returned {None if claimed is None else claimed.args} "
        f"although {runnable}, whose only upstream had succeeded, was READY",
    )

    claiming.execute(claimed)
    settle(flow, worker, held)

    assert blocked_children_that_started() == []
    assert blocked_children_claimed(flow) == [], (
        f"a child of doomed shows a claim although doomed never succeeded: "
        f"{blocked_children_claimed(flow)}"
    )
    require_runnable_children_all_ran(flow)
