"""
What the dependency gate costs a claim, measured rather than argued.

The gate is on the claim path, so a regression in what it costs is a
regression in throughput. These tests pin that cost inside the repository,
where the suite reaches it.

Two decisions about method.

**Rows examined, not wall-clock.** These run on whatever machine happens to
be free, and a loaded machine changes a wall-clock reading without changing
the query. `EXPLAIN (ANALYZE)` reports rows touched server-side, which a
loaded laptop does not change.

**A ratio against a control on the same table, never an absolute number.**
An absolute figure against an empty baseline measures "the table grew by
50,000 rows" and "the gate is expensive" together, and attributes both to
the gate. Every assertion here compares two queries over identical data,
differing only in the conjunct under test.

What is asserted, and what is only recorded:

  * **Asserted:** a claim's cost does not grow with the number of blocked
    nodes in front of it. They wait in django-ox's WAITING status, which no
    claim reads, and it is the property that would regress silently.
  * **Asserted, generously:** what the gate costs per READY row it judges.
    The gate still guards READY rows nothing released, such as a django-ox
    retry, so the blocked rows are put back to READY for that measurement.
"""

from __future__ import annotations

import pytest
from django.db import connection

from django_ox.models import OxTask
from oxpull import workflows

# transaction=True, not the default wrapper: VACUUM refuses to run inside a
# transaction, and it reaches only rows no open transaction can still see.
pytestmark = pytest.mark.django_db(transaction=True)

postgres_only = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="EXPLAIN ANALYZE row counts are read from the PostgreSQL plan",
)


def vacuum_and_analyze() -> None:
    """
    Settle the two tables the claim reads, before measuring.

    Without this the measurement is order-dependent, and the order it
    depends on is the whole suite's. Most of the suite runs each test inside
    a transaction that is rolled back, and on PostgreSQL the rows of a
    rolled-back test stay in the table and in every index as dead entries
    until a vacuum reaches them. A bitmap index scan counts each dead entry
    as a row it examined, so the same claim reports 3 rows on its own and
    1,549 after those tests, and whether autovacuum has been by in between
    is timing.

    ANALYZE alone corrects the column statistics and leaves the dead entries
    where they are. VACUUM removes them and sets the visibility map, and the
    ANALYZE with it re-sizes the tables, so every input to the plan is a row
    this test wrote. That is what makes a ratio comparable between runs at
    all.
    """
    with connection.cursor() as cursor:
        cursor.execute("VACUUM (ANALYZE) django_ox_oxtask")
        cursor.execute("VACUUM (ANALYZE) oxpull_oxpullworkflowedge")


def rows_examined(queryset) -> int:
    """
    Total rows touched by the plan, server-side.

    Summed across every node of the plan rather than read off the top,
    because the gate's cost is in the anti-join underneath, not in the
    number of rows the query finally returns.
    """
    plan = queryset.explain(analyze=True, format="JSON")
    if isinstance(plan, str):
        import json

        plan = json.loads(plan)
    root = plan[0]["Plan"] if isinstance(plan, list) else plan["Plan"]

    total = 0

    def walk(node: dict) -> None:
        nonlocal total
        total += int(node.get("Actual Rows", 0)) * int(node.get("Actual Loops", 1))
        for child in node.get("Plans", []):
            walk(child)

    walk(root)
    return total


def build(blocked: int, task) -> None:
    """One never-succeeding root with `blocked` children waiting on it."""
    flow = workflows.create(f"cost-{blocked}-{task.name}")
    flow.add("root", task, "root")
    for i in range(blocked):
        flow.add(f"b{i:05d}", task, f"b{i:05d}", depends_on="root")
    flow.seal()


def claim_candidates(*, gated: bool):
    """
    The shape of a worker's claim: the next row it would actually take.

    The status and run_after conjuncts are part of the claim in django-ox,
    and status is what keeps a waiting node out of the candidate set.
    """
    from django.db.models import Q
    from django.utils import timezone

    qs = OxTask.objects.filter(status=OxTask.Status.READY).filter(
        Q(run_after__isnull=True) | Q(run_after__lte=timezone.now())
    )
    if gated:
        qs = qs.filter(workflows.runnable_filter_q())
    return qs.order_by("-priority", "enqueued_at")[:1]


def claim_candidate_count() -> int:
    """The READY rows a claim chooses among, counted rather than planned."""
    from django.db.models import Q
    from django.utils import timezone

    return (
        OxTask.objects.filter(status=OxTask.Status.READY)
        .filter(Q(run_after__isnull=True) | Q(run_after__lte=timezone.now()))
        .count()
    )


@postgres_only
@pytest.mark.parametrize("blocked", [50, 400])
def test_the_gate_costs_a_bounded_multiple_per_candidate_row(blocked):
    """
    The gate's cost per row a claim chooses among, over the same table.

    The control is the candidate count and not the ungated claim's plan.
    django-ox 1.1.0 rebuilt ox_dequeue_idx as (status, -priority,
    enqueued_at), so the ungated claim reads rows in claim order and stops
    at the first one: it examines 2 rows whatever the table holds, and a
    ratio against it measures how many nodes are blocked rather than what
    the gate costs. Against django-ox 1.0.0's index the ungated claim
    sorted every candidate, which is what the ratio used to be taken
    against. The ungated figure is still printed, because the gap between
    it and the gated one is what a gated backend gives up on PostgreSQL.
    """
    from . import tasks

    build(blocked, tasks.step)
    # The blocked nodes wait, and a waiting node is no candidate at all. Put
    # back to READY, as a django-ox retry leaves a row, they are what the gate
    # has to judge.
    OxTask.objects.filter(status=OxTask.Status.WAITING).update(
        status=OxTask.Status.READY
    )

    vacuum_and_analyze()
    candidates = claim_candidate_count()
    ungated = rows_examined(claim_candidates(gated=False))
    gated = rows_examined(claim_candidates(gated=True))

    assert candidates > 0, "no candidate rows, so the ratio is meaningless"
    per_candidate = gated / candidates

    # The measurement is the point of this test: a run that passes silently
    # tells you nothing about whether the ratio moved.
    print(  # noqa: T201
        f"\n  [cost] blocked={blocked} candidates={candidates} "
        f"ungated={ungated} gated={gated} per_candidate={per_candidate:.1f} "
        f"vs_ungated={gated / max(ungated, 1):.1f}x"
    )

    # Generous on purpose. This is a regression tripwire for the shape of the
    # cost, not a performance budget: it should catch the gate becoming an
    # order of magnitude worse, and should not fail on planner variation.
    assert per_candidate < 60, (
        f"the gate examined {per_candidate:.1f} rows per candidate row"
    )


@postgres_only
def test_waiting_nodes_keep_the_claim_flat_as_they_pile_up():
    """
    The property the waiting status exists for, and the one that would
    regress without anyone noticing.

    Blocked nodes wait, so they are outside every worker's candidate set,
    and a claim costs the same with four hundred of them in front of it as
    with fifty. Measured at both sizes.

    Two assertions, because the two halves of that sentence fail apart. The
    candidate count says whether the blocked nodes stayed out of the set,
    counted rather than planned: each build() leaves its root READY and
    everything under it WAITING, so the set holds one row after the first
    build and two after the second. The cost is then taken per candidate
    row, as the gate's own measurement above takes it, because the second
    root is a candidate the first reading did not have and a claim that
    chooses among two rows reads both.
    """
    from . import tasks

    build(50, tasks.deferred_step)
    vacuum_and_analyze()
    small_candidates = claim_candidate_count()
    small = rows_examined(claim_candidates(gated=True))

    build(400, tasks.deferred_step)
    vacuum_and_analyze()
    large_candidates = claim_candidate_count()
    large = rows_examined(claim_candidates(gated=True))

    print(  # noqa: T201
        f"\n  [cost] waiting: 50 blocked={small} rows over {small_candidates} "
        f"candidates, 450 blocked={large} rows over {large_candidates}"
    )

    assert (small_candidates, large_candidates) == (1, 2), (
        f"the two roots are the only candidates, but a claim chooses among "
        f"{small_candidates} rows of 51 and {large_candidates} of 452: the "
        "blocked nodes are in the candidate set"
    )
    assert small > 0
    growth = (large / large_candidates) / (small / small_candidates)
    assert growth < 3, (
        f"claim cost grew {growth:.1f}x per candidate row when blocked nodes "
        "went from 50 to 450; waiting nodes are supposed to be outside the "
        "candidate set"
    )
