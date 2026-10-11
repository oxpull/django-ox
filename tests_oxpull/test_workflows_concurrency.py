"""
The gate and the reconciler under real concurrency, at sizes the rest of the
suite does not reach.

The existing race tests are thorough but small: two-parent fan-ins, two
workers, five-node diamonds. These use the same harness on wider and deeper
graphs, and put the reconciler and the workers on the same workflow at the
same time, which nothing else does.

What is being defended, in one line each:

  * the DAG order survives many workers, not just one;
  * a node never runs twice however many workers race for it;
  * cancelling a doomed cone while workers are draining it does not let a
    discarded node run, and does not lose a node that was still runnable.
"""

from __future__ import annotations

import logging

import pytest

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .racing import THREADS, race, race_all
from .test_workflows import (
    deferrals,
    drain,
    flow_worker,
    reconcile,
    settle,
    stored,
)

pytestmark = pytest.mark.django_db


def chain(name: str, length: int):
    """
    A chain whose priorities run backwards, so claim order is the reverse of
    dependency order and only the gate can produce the right sequence. A
    chain declared in topological order drains correctly with no gate at
    all, which is the trap diamond() documents.
    """
    flow = workflows.create(name)
    previous = None
    for i in range(length):
        node = f"n{i:03d}"
        task = tasks.step.using(priority=length - i)
        if previous is None:
            flow.add(node, task, node)
        else:
            flow.add(node, task, node, depends_on=previous)
        previous = node
    flow.seal()
    return flow


def wide_fan_in(name: str, parents: int):
    """
    `parents` roots, all of which must succeed before one child runs.

    The child carries the highest priority, so an ungated claim reaches it
    first and only the dependency conjunct holds it back. Without this the
    test passes with no gate at all, because the child was enqueued last.
    """
    flow = workflows.create(name)
    names = [f"p{i:03d}" for i in range(parents)]
    for node in names:
        flow.add(node, tasks.step.using(priority=0), node)
    flow.add("child", tasks.step.using(priority=9), "child", depends_on=names)
    flow.seal()
    return flow


@pytest.mark.django_db(transaction=True)
def test_a_deep_chain_keeps_its_order_under_many_workers(caplog):
    """
    Order is a property of the gate, not of there being one worker. Eight
    workers race down a chain far longer than any other test's graph; the
    only sequence any of them can produce is the declared one.

    Forty links is forty releases, each of them taken with seven other
    workers claiming against the same SQLite file, so this is the most
    releases the suite puts under contention. A link whose release loses
    both its tries leaves the next node WAITING and every worker comes back
    empty short of the end, which is the reconciler's to finish: the run
    ends on settle(), and both drains are counted. Order is what is under
    test, and it is asserted over the whole run whichever drain ran which
    node.
    """
    depth = 40
    flow = chain("deep-race", depth)

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        results, errors = race(lambda: drain(flow_worker(), depth))
        settled = settle(flow, depth)

    assert not errors, f"workers raised: {errors[:2]}"
    ran = list(tasks.STATE.get("order", []))
    assert ran == [f"n{i:03d}" for i in range(depth)], (
        f"the chain ran out of order under {len(results)} workers"
    )
    assert len(ran) == len(set(ran)), f"a node ran twice: {ran}"
    assert sum(results) + settled == depth
    assert settled == 0 or deferrals(caplog), (
        f"the reconciler had {settled} task(s) to run and the product logged "
        "no deferred or failed release, so a wakeup went missing in silence"
    )


@pytest.mark.django_db(transaction=True)
def test_a_wide_fan_in_releases_its_child_exactly_once(caplog):
    """
    Every parent finishing at once is the moment a counter-based design
    double-fires or hangs. There is no counter, but the claim still has to
    admit the child exactly once with many workers asking together.

    Twenty-four parents finishing on eight workers is also the most lock
    contention this suite puts on one release, and the release that frees
    the child is the one that meets it: about once in a hundred runs it
    loses both its tries and leaves the child to the reconciler. That is the
    product's documented answer to a lost lock conflict, so the run ends on
    `settle` rather than at the last empty claim, and both counts are added
    up: the child is run exactly once, by a worker, whichever of the two
    released it. A run that left the reconciler work must also have said so
    in the log, which is what separates a documented deferral from a lost
    wakeup.
    """
    parents = 24
    flow = wide_fan_in("wide-in", parents)

    total = parents + 1
    with caplog.at_level(logging.WARNING, logger="oxpull"):
        results, errors = race(lambda: drain(flow_worker(), total))
        settled = settle(flow, total)

    assert not errors, f"workers raised: {errors[:2]}"
    ran = list(tasks.STATE.get("order", []))
    assert ran.count("child") == 1, f"the child ran {ran.count('child')} times"
    assert ran[-1] == "child", "the child ran before its last parent"
    assert len(ran) == total
    assert sum(results) + settled == total
    assert settled == 0 or deferrals(caplog), (
        f"the reconciler had {settled} task(s) to run and the product logged "
        "no deferred or failed release, so a wakeup went missing in silence"
    )


@pytest.mark.django_db(transaction=True)
def test_cancelling_a_doomed_cone_while_workers_drain_it_runs_nothing_discarded():
    """
    The reconciler and the workers on one workflow at the same instant.

    Half the graph is doomed by a failed root and half is independent and
    still runnable. Whatever interleaving happens, a discarded node must not
    have run, and the runnable half must not be lost.
    """
    flow = workflows.create("half-doomed")
    flow.add("bad-root", tasks.step, "bad-root")
    for i in range(8):
        flow.add(f"doomed{i}", tasks.step, f"doomed{i}", depends_on="bad-root")
    for i in range(8):
        flow.add(f"free{i}", tasks.step, f"free{i}")
    flow.seal()
    ids = flow.node_ids()

    OxTask.objects.filter(id=ids["bad-root"]).update(status=OxTask.Status.FAILED)

    # Four reconcilers and four workers, released together.
    targets = [reconcile] * 4 + [lambda: drain(flow_worker(), 20)] * 4
    _, errors = race_all(targets)
    assert not errors, f"raised: {errors[:2]}"

    for _ in range(5):
        reconcile()

    ran = set(tasks.STATE.get("order", []))
    discarded_names = {
        args[0]
        for args in OxTask.objects.filter(
            id__in=ids.values(), status=OxTask.Status.DISCARDED
        ).values_list("args", flat=True)
        if args
    }

    # Without these two the assertion below is vacuous: nothing cancelled, or
    # nothing run, and "no overlap" is true for uninteresting reasons.
    assert discarded_names, "the reconcilers cancelled nothing, so this proves nothing"
    assert ran, "no worker ran anything, so this proves nothing"
    assert not (ran & discarded_names), (
        f"these ran and were also discarded: {sorted(ran & discarded_names)}"
    )
    assert {f"free{i}" for i in range(8)} <= ran, (
        f"independent nodes were lost: {sorted({f'free{i}' for i in range(8)} - ran)}"
    )
    assert len(list(tasks.STATE.get("order", []))) == len(ran), "a node ran twice"


@pytest.mark.django_db(transaction=True)
def test_racing_reconcilers_complete_a_wide_workflow_once():
    """
    Completion is a compare-and-set. Eight reconcilers on a finished
    workflow must produce one completion, not eight.
    """
    parents = 12
    flow = wide_fan_in("wide-complete", parents)
    drain(flow_worker(), parents + 1)

    results, errors = race(reconcile)
    assert not errors, f"reconcilers raised: {errors[:2]}"
    assert len(results) == THREADS, (
        f"only {len(results)} of {THREADS} reconcilers ran; a race between one "
        "thread and itself proves nothing"
    )

    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE
    assert sum(results) == 1, (
        f"{sum(results)} reconcilers each claimed the completion; expected 1"
    )
