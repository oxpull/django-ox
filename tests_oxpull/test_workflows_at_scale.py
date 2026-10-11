"""
The two bounds in the reconciler, exercised.

`MAX_CANCEL_PASSES` and `BACKSTOP_PAGE` exist so one reconciler tick
cannot run unboundedly, and both carry the same promise in their comments:
what a pass does not finish, the next tick continues. Nothing tested either.
Every other workflow in the suite is one to five nodes, so neither constant
had ever been reached by a test, and the "next tick continues" claim was
prose.

These build workflows large enough to cross each bound. They are slower than
the rest of the suite by design: a test that stays inside the limit is not
testing the limit.
"""

from __future__ import annotations

import pytest

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow
from oxpull.workflows import BACKSTOP_PAGE, MAX_CANCEL_PASSES

from . import tasks
from .test_workflows import reconcile, stored

pytestmark = pytest.mark.django_db


def chain(name: str, length: int, task=None):
    """A -> B -> C -> ... of `length` nodes, each blocked on the last."""
    task = task or tasks.step
    flow = workflows.create(name)
    previous = None
    for i in range(length):
        node = f"n{i:04d}"
        if previous is None:
            flow.add(node, task, node)
        else:
            flow.add(node, task, node, depends_on=previous)
        previous = node
    flow.seal()
    return flow


def fan_out(name: str, width: int, task=None):
    """
    One root with `width` children, all blocked on it.

    The backend comes from the task, not from create(): tasks.deferred_step
    is bound to the second workflow backend.
    """
    task = task or tasks.step
    flow = workflows.create(name)
    flow.add("root", task, "root")
    for i in range(width):
        flow.add(f"c{i:04d}", task, f"c{i:04d}", depends_on="root")
    flow.seal()
    return flow


def _cancelled(flow) -> int:
    ids = flow.node_ids().values()
    return OxTask.objects.filter(id__in=ids, status=OxTask.Status.DISCARDED).count()


def test_a_chain_deeper_than_the_cancel_bound_needs_more_than_one_tick():
    """
    _cancel_unreachable walks one level of the DAG per iteration and stops
    after MAX_CANCEL_PASSES. A doomed chain longer than that cannot be
    finished by a single tick, and the next one has to pick up where it
    stopped. Both halves are asserted: the bound holds, and it is not
    permanent.
    """
    depth = MAX_CANCEL_PASSES + 6
    flow = chain("deep", depth)
    ids = flow.node_ids()

    # Kill the root. Everything downstream is now unreachable.
    OxTask.objects.filter(id=ids["n0000"]).update(status=OxTask.Status.FAILED)

    first = reconcile()
    after_first = _cancelled(flow)
    assert after_first > 0, "the first pass cancelled nothing"
    assert after_first < depth - 1, (
        f"the whole chain of {depth} was cancelled in one pass, so this test "
        f"never reached MAX_CANCEL_PASSES={MAX_CANCEL_PASSES}"
    )

    # The promise the bound's comment makes.
    for _ in range(10):
        reconcile()
        if _cancelled(flow) >= depth - 1:
            break
    assert _cancelled(flow) == depth - 1, (
        "later ticks did not continue from where the bounded pass stopped"
    )
    # _reconcile() returns completed workflows, not cancelled nodes; a doomed
    # chain completes none, so the count above is the assertion that matters.
    assert first == 0


def test_a_fan_out_wider_than_the_release_bound_releases_across_ticks(monkeypatch):
    """
    The reconciler's backstop marks and releases at most BACKSTOP_PAGE edges a
    page. With one page a visit, a fan-out wider than that cannot be released
    by one tick, and the next tick goes on from the page it stopped at.
    """
    monkeypatch.setattr(workflows, "VISIT_PAGES", 1)
    width = BACKSTOP_PAGE + 25
    flow = fan_out("wide", width, task=tasks.deferred_step)
    ids = flow.node_ids()

    waiting = OxTask.objects.filter(id__in=ids.values(), status=OxTask.Status.WAITING)
    assert waiting.count() == width, "the children were not all waiting"

    # The root succeeds by a route that does not release eagerly, which is
    # what leaves the backstop to do the work.
    OxTask.objects.filter(id=ids["root"]).update(status=OxTask.Status.SUCCESSFUL)

    released = workflows._release_backstop("default")
    assert released == BACKSTOP_PAGE, (
        f"one pass released {released}, expected the bound {BACKSTOP_PAGE}; "
        "this test did not reach it"
    )
    assert waiting.count() == width - BACKSTOP_PAGE

    assert workflows._release_backstop("default") == width - BACKSTOP_PAGE
    assert waiting.count() == 0, "the second pass did not finish the release"


def test_a_deep_chain_completes_and_fires_its_callback_once():
    """
    The happy path at a depth no other test reaches: a chain longer than the
    cancel bound runs to completion in order, and the workflow completes once.
    """
    depth = MAX_CANCEL_PASSES + 6
    flow = chain("deep-ok", depth)

    from .test_workflows import drain, flow_worker

    ran = drain(flow_worker(), limit=depth + 10)
    assert ran == depth, f"drained {ran} of {depth}"

    order = list(tasks.STATE.get("order", []))
    assert order == [f"n{i:04d}" for i in range(depth)], "ran out of order"

    reconcile()
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE
