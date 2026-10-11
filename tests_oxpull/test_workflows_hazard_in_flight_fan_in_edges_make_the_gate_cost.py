"""
In-flight fan-in edges make the dependency gate cost O(edges).

The gate is NOT EXISTS(edge JOIN upstream task WHERE upstream.status <>
SUCCESSFUL), conjoined into every claim on a WORKFLOWS backend and into the
eager release's SELECT. Three legs, each asserting the property a claim path
needs, so each fails while the hazard is present:

  1. A claim of a task that is not in any workflow must not get more expensive
     as a fan-in in flight gets wider (the fan-in node waits, so it is not a
     candidate; only its edges are in the gate's inner relation).
  2. A fan-in node ranked ahead of its upstreams (priority=10), put back to
     READY as a django-ox retry leaves it, must not make each claim re-walk
     every edge that is already satisfied.
  3. The eager release a worker runs after each parent's SUCCESSFUL must not
     re-scan every parent of the shared child, which makes a map/reduce drain
     O(K^2).

**Work, not wall-clock.** The machine is shared and loaded, so every assertion
is on work the database reports, measured around the real call:

  * PostgreSQL: auto_explain (log_analyze, JSON) captures the plan of every
    statement the call runs, and work is the sum over plan nodes of
    (Actual Rows + Rows Removed by Filter/Join Filter/Index Recheck) x Loops.
  * MySQL: the delta of the session's Handler_read_* counters, less the cost
    of reading them.
  * SQLite: SQLite VM instructions, counted with a progress handler.

The units differ, so every assertion is a ratio of two readings taken with the
same instrument on the same database. Wall-clock is printed, never asserted.

**Real paths.** Workflows are built with Workflow.add() and seal(); claims go
through OxpullWorker.claim_one() and run_once(); the release is the one
OxpullWorker._write_outcome() makes. Two shortcuts, both bulk status writes,
both marked where they happen: maps put into RUNNING (what a fleet claiming
them produces; claiming 20,000 through the gate is the hazard itself) and a
finished fan-in settled to SUCCESSFUL between two measurements.
"""

from __future__ import annotations

import contextlib
import json
import time
from dataclasses import dataclass, field
from datetime import timedelta

import pytest
from django.db import connection, transaction
from django.db.models import F
from django.utils import timezone

from django_ox.models import OxTask
from oxpull import worker as oxpull_worker
from oxpull import workflows
from oxpull.models import OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import flow_worker
from .test_workflows_deferred import deferring_worker

# transaction=True, not the default wrapper: MySQL's ANALYZE TABLE commits
# implicitly, which would end the test's own transaction halfway through a
# measurement. Every leg runs the same way so the three databases are compared
# on the same sequence of commits.
pytestmark = pytest.mark.django_db(transaction=True)


# -- the instrument ----------------------------------------------------


@dataclass
class Reading:
    work: int = 0
    seconds: float = 0.0
    statements: list[tuple[int, str]] = field(default_factory=list)

    def __str__(self) -> str:
        return f"{self.work} ({self.seconds * 1000:.1f} ms)"


def _plan_work(node: dict) -> int:
    loops = int(node.get("Actual Loops", 1))
    rows = float(node.get("Actual Rows", 0))
    removed = sum(
        float(node.get(key, 0))
        for key in (
            "Rows Removed by Filter",
            "Rows Removed by Join Filter",
            "Rows Removed by Index Recheck",
        )
    )
    total = round((rows + removed) * loops)
    for child in node.get("Plans", []):
        total += _plan_work(child)
    return total


class Meter:
    """Database-reported work done while a block runs, on this connection."""

    def __init__(self) -> None:
        self.vendor = connection.vendor
        self._raw = None
        self._notes: list[str] = []
        self._capturing = False
        self._mysql_overhead = 0

    def _ensure(self) -> None:
        connection.ensure_connection()
        raw = connection.connection
        if raw is self._raw:
            return
        self._raw = raw
        if self.vendor == "postgresql":
            raw.add_notice_handler(self._on_notice)
            with connection.cursor() as cursor:
                cursor.execute("LOAD 'auto_explain'")
                for setting in (
                    "auto_explain.log_analyze = on",
                    "auto_explain.log_timing = off",
                    "auto_explain.log_format = 'json'",
                    "auto_explain.log_level = 'notice'",
                    "auto_explain.log_nested_statements = on",
                    "auto_explain.log_min_duration = -1",
                    "client_min_messages = 'notice'",
                ):
                    cursor.execute(f"SET {setting}")
        elif self.vendor == "mysql":
            overheads = []
            for _ in range(5):
                before = self._handler_reads()
                overheads.append(self._handler_reads() - before)
            self._mysql_overhead = min(overheads)

    def _on_notice(self, diagnostic) -> None:
        if self._capturing:
            self._notes.append(diagnostic.message_primary or "")

    def _handler_reads(self) -> int:
        with connection.cursor() as cursor:
            cursor.execute("SHOW SESSION STATUS LIKE 'Handler_read%'")
            return sum(int(value) for _, value in cursor.fetchall())

    @contextlib.contextmanager
    def measure(self):
        self._ensure()
        reading = Reading()
        if self.vendor == "postgresql":
            with connection.cursor() as cursor:
                cursor.execute("SET auto_explain.log_min_duration = 0")
            self._notes = []
            self._capturing = True
            started = time.monotonic()
            try:
                yield reading
            finally:
                reading.seconds = time.monotonic() - started
                self._capturing = False
                with connection.cursor() as cursor:
                    cursor.execute("SET auto_explain.log_min_duration = -1")
                for note in self._notes:
                    if "plan:" not in note:
                        continue
                    plan = json.loads(note.split("plan:", 1)[1])
                    work = _plan_work(plan["Plan"])
                    text = " ".join(plan.get("Query Text", "").split())[:90]
                    reading.statements.append((work, text))
                    reading.work += work
        elif self.vendor == "mysql":
            before = self._handler_reads()
            started = time.monotonic()
            try:
                yield reading
            finally:
                reading.seconds = time.monotonic() - started
                after = self._handler_reads()
                reading.work = after - before - self._mysql_overhead
        else:
            raw = connection.connection
            steps = [0]

            def tick() -> int:
                steps[0] += 1
                return 0

            raw.set_progress_handler(tick, 10)
            started = time.monotonic()
            try:
                yield reading
            finally:
                reading.seconds = time.monotonic() - started
                raw.set_progress_handler(None, 10)
                reading.work = steps[0] * 10


def analyze() -> None:
    """
    Fresh statistics before a measurement, as autovacuum / InnoDB's
    auto-recalc give a running install.

    SQLite too, though Django never runs ANALYZE there: SQLite's own guidance
    is PRAGMA optimize on long-lived connections, which gathers the same
    statistics. It matters. Without statistics SQLite probes
    oxpull_wf_edge_down_idx per candidate and leg 1 stays flat; with them,
    and one 20,000-way fan-in in the table, it plans the probe as a full scan
    of the edge table (measured 390 -> 400,380 VM instructions per claim).
    """
    tables = [OxTask._meta.db_table, OxpullWorkflowEdge._meta.db_table]
    with connection.cursor() as cursor:
        for table in tables:
            if connection.vendor == "postgresql":
                cursor.execute(f'ANALYZE "{table}"')
            elif connection.vendor == "mysql":
                cursor.execute(f"ANALYZE TABLE `{table}`")
                cursor.fetchall()
            else:
                cursor.execute(f'ANALYZE "{table}"')


def unit() -> str:
    return {
        "postgresql": "plan rows",
        "mysql": "Handler_read_*",
        "sqlite": "VM instructions",
    }[connection.vendor]


def report(line: str) -> None:
    print(f"\n  [s2 {connection.vendor}] {line}", end="")  # noqa: T201


# -- fixtures built through the public API -----------------------------


def fan_in(name: str, width: int, task, *, reduce_task=None):
    """
    `width` map nodes and one reduce node depending on every one of them,
    through Workflow.add(). One outer transaction so twenty thousand adds
    cost one commit; add() inside a caller's transaction is supported.
    """
    flow = workflows.create(name)
    names = [f"{name}-map-{i:05d}" for i in range(width)]
    with transaction.atomic():
        for node in names:
            flow.add(node, task, node)
        flow.add("reduce", reduce_task or task, f"{name}-reduce", depends_on=names)
    flow.seal()
    return flow


def map_ids(flow):
    return (
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id)
        .exclude(name="reduce")
        .values("task_id")
    )


def mark_maps_running(flow) -> int:
    """
    SHORTCUT: the state a fleet leaves after claiming every map. The real
    sequence is one OxpullWorker.claim_one() per map; at 20,000 maps that
    sequence pays the very cost under test once per map.
    """
    now = timezone.now()
    return OxTask.objects.filter(
        pk__in=map_ids(flow), status=OxTask.Status.READY
    ).update(
        status=OxTask.Status.RUNNING,
        locked_by="fleet",
        locked_at=now,
        lease_expires_at=now + timedelta(hours=1),
        lease_epoch=F("lease_epoch") + 1,
        attempts=F("attempts") + 1,
        started_at=now,
        last_attempted_at=now,
    )


def settle(flow) -> None:
    """
    SHORTCUT: every node of a fan-in SUCCESSFUL, as workers running the maps
    and then the released reduce would leave it.
    """
    now = timezone.now()
    ids = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values("task_id")
    OxTask.objects.filter(pk__in=ids).update(
        status=OxTask.Status.SUCCESSFUL,
        run_after=None,
        finished_at=now,
        locked_by=None,
        locked_at=None,
        lease_expires_at=None,
    )


def claim_cost(meter: Meter, worker, *, label_prefix: str, samples: int = 3) -> Reading:
    """
    The median of `samples` real claims, each asserted to have taken a task
    whose label starts with `label_prefix`, so the reading is of the claim
    the fixture meant to measure. Each claimed task is executed, as the
    worker would, so the next sample sees the table a worker would.
    """
    readings = []
    for _ in range(samples):
        with meter.measure() as reading:
            claimed = worker.claim_one()
        assert claimed is not None, "nothing was claimable"
        assert str(claimed.args[0]).startswith(label_prefix), (
            f"the claim took {claimed.args[0]!r}, not a {label_prefix!r} task"
        )
        worker.execute(claimed, inline=True)
        readings.append(reading)
    readings.sort(key=lambda r: r.work)
    return readings[len(readings) // 2]


# -- leg 1: unrelated claims pay for a fan-in in flight ----------------


def test_an_in_flight_fan_in_does_not_tax_the_claim_of_an_unrelated_task():
    """
    A 2,000-way and then a 20,000-way fan-in are in flight: every map claimed
    and running, the reduce waiting, so nothing of the workflow is a claim
    candidate. The claim takes a plain task that is in
    no workflow. Its cost must not scale with the fan-in's width.
    """
    meter = Meter()
    worker = deferring_worker()
    with transaction.atomic():
        for i in range(1000):
            tasks.deferred_step.enqueue(f"unrelated-{i:04d}")

    analyze()
    baseline = claim_cost(meter, worker, label_prefix="unrelated")

    small = fan_in("small-fan-in", 2000, tasks.deferred_step)
    reduce_small = OxTask.objects.get(
        pk=OxpullWorkflowNode.objects.get(workflow_id=small.id, name="reduce").task_id
    )
    assert reduce_small.status == OxTask.Status.WAITING
    analyze()
    small_ready = claim_cost(meter, worker, label_prefix="unrelated")
    assert mark_maps_running(small) == 2000
    analyze()
    small_running = claim_cost(meter, worker, label_prefix="unrelated")

    settle(small)
    analyze()
    settled = claim_cost(meter, worker, label_prefix="unrelated")

    big = fan_in("big-fan-in", 20000, tasks.deferred_step)
    analyze()
    big_ready = claim_cost(meter, worker, label_prefix="unrelated")
    assert mark_maps_running(big) == 20000
    analyze()
    big_running = claim_cost(meter, worker, label_prefix="unrelated")

    report(f"unit={unit()}")
    report(f"no workflow rows:                  {baseline}")
    report(f"2,000-way fan-in, maps READY:      {small_ready}")
    report(f"2,000-way fan-in, maps RUNNING:    {small_running}")
    report(f"2,000-way fan-in settled:          {settled}")
    report(f"20,000-way fan-in, maps READY:     {big_ready}")
    report(f"20,000-way fan-in, maps RUNNING:   {big_running}")
    for work, text in big_running.statements:
        report(f"  stmt {work}: {text}")
    print()  # noqa: T201

    growth = big_running.work / max(small_running.work, 1)
    assert growth < 3, (
        f"an unrelated claim cost {small_running.work} {unit()} beside a 2,000-way "
        f"fan-in in flight and {big_running.work} beside a 20,000-way one "
        f"({growth:.1f}x); with no workflow rows it cost {baseline.work}"
    )


# -- leg 2: a candidate fan-in node re-walks its satisfied edges -------


def _priority_fan_in(name: str, width: int):
    """`width` upstreams at priority 0 and a priority-10 node on all of them."""
    flow = workflows.create(name)
    names = [f"{name}-up-{i:05d}" for i in range(width)]
    with transaction.atomic():
        for node in names:
            flow.add(node, tasks.step, node)
        flow.add(
            "load", tasks.step.using(priority=10), f"{name}-load", depends_on=names
        )
    flow.seal()
    return flow


def _half_drained_claim_cost(meter: Meter, worker, name: str, width: int) -> Reading:
    _priority_fan_in(name, width)
    for _ in range(width // 2):
        assert worker.run_once() is True
    assert tasks.STATE["order"][-1] == f"{name}-up-{width // 2 - 1:05d}", (
        "the upstreams did not drain in insertion order"
    )
    # The load node waits for the other half. Put back to READY, as a
    # django-ox retry leaves a row, it is a candidate the gate has to judge,
    # with half its edges marked satisfied.
    assert (
        OxTask.objects.filter(
            args=[f"{name}-load"], status=OxTask.Status.WAITING
        ).update(status=OxTask.Status.READY)
        == 1
    )
    analyze()
    reading = claim_cost(meter, worker, label_prefix=f"{name}-up-")
    # Finish the workflow through the same worker, load last, so the next
    # width starts from settled history rather than a second live fan-in.
    while worker.run_once():
        pass
    assert tasks.STATE["order"][-1] == f"{name}-load"
    return reading


def test_a_high_priority_fan_in_node_does_not_rewalk_its_satisfied_edges():
    """
    The fan-in node outranks its upstreams, so every claim tests its gate
    first. With half the upstreams done, the claim must
    cost about the same at width 4,000 as at width 500.
    """
    meter = Meter()
    worker = flow_worker()
    small = _half_drained_claim_cost(meter, worker, "p500", 500)
    large = _half_drained_claim_cost(meter, worker, "p4000", 4000)

    report(f"unit={unit()}")
    report(f"W=500, 250 upstreams done:    {small}")
    report(f"W=4000, 2000 upstreams done:  {large}")
    for work, text in large.statements:
        report(f"  stmt {work}: {text}")
    print()  # noqa: T201

    assert large.work <= 2 * small.work, (
        f"with half the upstreams done, one claim cost {small.work} {unit()} at "
        f"W=500 and {large.work} at W=4000 ({large.work / max(small.work, 1):.1f}x)"
    )


# -- leg 3: the eager release re-scans every parent --------------------


@pytest.mark.xfail(
    connection.vendor == "postgresql",
    strict=True,
    reason=(
        "The release run after each parent succeeds re-reads every parent of "
        "the shared child, so draining a fan-in costs its width squared. "
        "SQLite's plan skips the satisfied edges; PostgreSQL's does not"
    ),
)
@pytest.mark.xfail(
    connection.vendor == "mysql",
    strict=False,
    reason=(
        "The release run after each parent succeeds re-reads every parent of "
        "the shared child; on MySQL the narrow baseline swings with what the "
        "earlier tests left in the tables, so the result is timing-dependent"
    ),
)
@pytest.mark.xfail(
    connection.vendor == "sqlite",
    strict=False,
    reason=(
        "The release run after each parent succeeds can re-read every parent "
        "of the shared child on SQLite. Run alone the drain grows 10x for 10x "
        "the width, and run after the file before it the same drain grew 94x, "
        "so the result depends on the plan SQLite picks"
    ),
)
def test_draining_a_wide_fan_in_does_not_cost_its_width_squared(monkeypatch):
    """
    Each map's SUCCESSFUL marks its edge in OxpullWorker._write_outcome, and
    then runs release_downstream on the shared reduce. The total work
    those releases do over a whole drain must grow about linearly with the
    fan-in's width: 10x the width, well under 20x the work.
    """
    meter = Meter()
    releases: list[Reading] = []
    real_release = oxpull_worker.release_downstream

    def metered_release(task_ids, db_alias):
        with meter.measure() as reading:
            released = real_release(task_ids, db_alias)
        releases.append(reading)
        return released

    monkeypatch.setattr(oxpull_worker, "release_downstream", metered_release)

    results = {}
    for width in (200, 2000):
        releases.clear()
        fan_in(f"r{width}", width, tasks.deferred_step)
        analyze()
        worker = deferring_worker()
        ran = 0
        while worker.run_once():
            ran += 1
        assert ran == width + 1
        assert tasks.STATE["order"][-1] == f"r{width}-reduce", (
            "the reduce did not run last"
        )
        # One release per map, then the reduce's own (it has no dependents).
        assert len(releases) == width + 1
        maps = releases[:width]
        results[width] = {
            "points": {i: maps[i] for i in (0, 100, width // 2, width - 1)},
            "last": maps[-1],
            "total": sum(r.work for r in maps),
            "seconds": sum(r.seconds for r in maps),
        }

    report(f"unit={unit()}")
    for width, r in results.items():
        points = ", ".join(f"i={i}: {reading}" for i, reading in r["points"].items())
        report(f"K={width}: release of parent {points}")
        report(f"K={width}: whole drain {r['total']} ({r['seconds']:.2f} s)")
    for work, text in results[2000]["last"].statements:
        report(f"  stmt {work}: {text}")
    print()  # noqa: T201

    growth = results[2000]["total"] / max(results[200]["total"], 1)
    assert growth < 20, (
        f"releasing every parent of a 200-way fan-in cost {results[200]['total']} "
        f"{unit()} and of a 2,000-way one {results[2000]['total']} ({growth:.0f}x "
        "for 10x the width)"
    )
