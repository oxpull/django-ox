"""
The reconciler's release backstop must not read the READY backlog on every
tick.

The backstop runs first in every reconciler pass, whatever the backends
declare. Development builds swept `status = READY AND run_after = <sentinel>`
across the whole task table. No django-ox index contains run_after, so the
database reached those rows only through status (or the primary key), and an
install with few or no blocked nodes never filled the LIMIT: the sweep read
every READY row, or on MySQL at some shapes every row in the table, to
release nothing. The backstop now reads the unsatisfied edges and the
WAITING nodes of OPEN and SEALED workflows, which no READY backlog enlarges.

Every test here builds an idle tick through real paths:

  * a workflow in flight, built with `create()`, `add()` and `seal()`, whose
    root a real `OxpullWorker.claim_one()` holds RUNNING under a lease, so the
    reconciler has a SEALED workflow to look at and nothing to release;
  * a backlog of ordinary READY tasks written by django-ox's public
    `django_ox.bulk.enqueue_many`, the bulk form of `task.enqueue()`;
  * one real `workflows._reconcile()` pass, the function the scheduled
    `oxpull.workflows.reconcile` task runs.

The tick is measured server-side, never by wall clock:

  * PostgreSQL: heap tuples read from the task table during the tick
    (`seq_tup_read + idx_tup_fetch` from `pg_stat_user_tables`, parallel
    workers included), with the tick in autocommit as a worker runs it.
  * MySQL: the session's `Handler_read_*` counters around the tick, less the
    cost of reading them.
  * SQLite: virtual-machine instructions executed during the tick, from the
    connection's progress handler.
"""

from __future__ import annotations

import contextlib
import json
import os
import time
from dataclasses import dataclass

import pytest
from django.db import connection
from django.db.models import Exists, OuterRef
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.bulk import enqueue_many
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.worker import OxpullWorker
from oxpull.workflows import BACKSTOP_PAGE

from . import tasks
from .test_workflows import reconcile, stored
from .test_workflows_cost import rows_examined

pytestmark = pytest.mark.django_db(transaction=True)

# Backlog sizes for the growth test. Overridable so a smoke run can use small
# ones.
SIZES = tuple(
    int(n) for n in os.environ.get("OX_SWEEP_SIZES", "20000,200000").split(",")
)
# The MySQL whole-table shape: (SUCCESSFUL history rows, READY backlog rows).
MYSQL_SHAPE = tuple(
    int(n) for n in os.environ.get("OX_SWEEP_MYSQL_SHAPE", "70000,20000").split(",")
)
WIDTH = 10
ENQUEUE_CHUNK = 10_000

row_counting = pytest.mark.skipif(
    connection.vendor not in ("mysql", "postgresql"),
    reason="the bound is in rows; SQLite's counter is VM instructions, and the "
    "growth test covers SQLite",
)


@dataclass
class TickCost:
    reads: int
    unit: str
    ms: float
    completed: int


# -- configuration -----------------------------------------------------


@contextlib.contextmanager
def configured(backend: str):
    """
    Yield (node task, worker alias) for the workflow backend under test:
    "flows", or "flows_deferred", which carries the removed
    DEFER_BLOCKED_NODES option and must cost the same.
    """
    if backend == "flows":
        yield tasks.step, "flows"
    elif backend == "flows_deferred":
        yield tasks.deferred_step, "flows_deferred"
    else:  # pragma: no cover
        raise AssertionError(backend)


def in_flight_workflow(task, alias: str):
    """
    A SEALED workflow whose root a real worker holds RUNNING, with WIDTH
    children waiting on it. The reconciler has something to look at and
    nothing it may release or complete.
    """
    flow = workflows.create(f"in-flight-{alias}")
    flow.add("root", task, "root")
    for i in range(WIDTH):
        flow.add(f"c{i:02d}", task, f"c{i:02d}", depends_on="root")
    flow.seal()
    worker = OxpullWorker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)
    claimed = worker.claim_one()
    assert claimed is not None, "the worker claimed nothing"
    assert str(claimed.id) == str(flow.node_ids()["root"]), "the worker took a child"
    return flow


def enqueue_backlog(count: int, start: int = 0) -> None:
    """`count` ordinary READY tasks, through django-ox's public bulk enqueue."""
    done = 0
    while done < count:
        n = min(ENQUEUE_CHUNK, count - done)
        enqueue_many(tasks.plain_echo, [((start + done + i,), {}) for i in range(n)])
        done += n


def refresh_statistics() -> None:
    """
    Fresh planner statistics, so a plan is not an artefact of stale ones.

    PostgreSQL also gets VACUUM. History rows were READY before they were
    finished, and until autovacuum reaches them their dead READY versions
    stay in the status indexes and inflate what an index scan visits. The
    figure after VACUUM is the conservative one.

    SQLite is left as Django leaves it: nothing runs ANALYZE there, so a
    production SQLite install plans without sqlite_stat1.
    """
    tables = [OxTask._meta.db_table, "oxpull_oxpullworkflowedge"]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            for table in tables:
                cursor.execute(f'VACUUM (ANALYZE) "{table}"')
        elif connection.vendor == "mysql":
            cursor.execute("ANALYZE TABLE " + ", ".join(f"`{t}`" for t in tables))
            cursor.fetchall()


# -- measurement -------------------------------------------------------


def _pg_task_rows_read() -> int:
    """
    Heap tuples read from the task table so far, parallel workers included.

    Not pg_stat_xact_user_tables: that counts the leader process only, and
    at 200,000 READY rows the sweep plans as a parallel Seq Scan, so it
    reported about a third of the rows scanned. The cumulative view includes
    what each parallel worker reports at exit. Forcing a flush makes this
    backend's own pending counts visible first; nothing else connects to the
    test database.
    """
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_stat_force_next_flush()")
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_stat_clear_snapshot()")
        cursor.execute(
            "SELECT COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0) "
            "FROM pg_stat_user_tables WHERE relname = %s",
            [OxTask._meta.db_table],
        )
        row = cursor.fetchone()
    return int(row[0]) if row else 0


def _mysql_handler_reads() -> int:
    with connection.cursor() as cursor:
        cursor.execute("SHOW SESSION STATUS LIKE 'Handler_read%'")
        return sum(int(value) for _name, value in cursor.fetchall())


def measure(fn) -> tuple[int, str, float, object]:
    """Run fn() once and return (server-side reads, unit, ms, fn's result)."""
    vendor = connection.vendor
    connection.ensure_connection()
    if vendor == "postgresql":
        before = _pg_task_rows_read()
        started = time.perf_counter()
        result = fn()
        ms = (time.perf_counter() - started) * 1000
        after = _pg_task_rows_read()
        return after - before, "task-table heap tuples read", ms, result
    if vendor == "mysql":
        first = _mysql_handler_reads()
        second = _mysql_handler_reads()
        overhead = second - first
        started = time.perf_counter()
        result = fn()
        ms = (time.perf_counter() - started) * 1000
        after = _mysql_handler_reads()
        return after - second - overhead, "Handler_read_* rows", ms, result
    if vendor == "sqlite":
        raw = connection.connection
        step = 100
        calls = [0]

        def tick_handler() -> int:
            calls[0] += 1
            return 0

        raw.set_progress_handler(tick_handler, step)
        try:
            started = time.perf_counter()
            result = fn()
            ms = (time.perf_counter() - started) * 1000
        finally:
            raw.set_progress_handler(None, step)
        return calls[0] * step, "SQLite VM instructions", ms, result
    pytest.skip(f"no server-side read counter for {vendor}")  # pragma: no cover


def measure_tick() -> TickCost:
    reads, unit, ms, completed = measure(reconcile)
    return TickCost(reads=reads, unit=unit, ms=ms, completed=int(completed))


def sweep_queryset():
    """
    The task-table read behind the backstop's release page: the waiting rows
    among a page of open nodes of the open workflows, by primary key. The page
    of nodes is read first, as _release_page reads it.
    """
    live = OxpullWorkflow.objects.filter(
        status__in=(OxpullWorkflow.Status.OPEN, OxpullWorkflow.Status.SEALED)
    )
    page = list(
        OxpullWorkflowNode.objects.filter(
            workflow_id__in=live.values("pk"), succeeded=False
        )
        .order_by("pk")
        .values_list("task_id", flat=True)[:BACKSTOP_PAGE]
    )
    return (
        OxTask.objects.filter(status=OxTask.Status.WAITING, pk__in=page)
        .exclude(
            Exists(
                OxpullWorkflowEdge.objects.filter(
                    downstream_task_id=OuterRef("pk"), satisfied=False
                )
            )
        )
        .order_by("pk")
        .values_list("pk", flat=True)
    )


def sweep_evidence() -> str:
    """The sweep statement's plan and its own read cost, for the log."""
    qs = sweep_queryset()
    vendor = connection.vendor
    lines = []
    if vendor == "postgresql":
        plan = qs.explain(analyze=True, format="JSON")
        if isinstance(plan, str):
            plan = json.loads(plan)
        root = plan[0]["Plan"]

        scanned = [0]

        def walk(node, depth=0):
            label = node["Node Type"]
            if "Index Name" in node:
                label += f" on {node['Index Name']}"
            if "Relation Name" in node:
                label += f" [{node['Relation Name']}]"
            loops = int(node.get("Actual Loops", 1))
            removed = int(node.get("Rows Removed by Filter", 0)) + int(
                node.get("Rows Removed by Index Recheck", 0)
            )
            if "Relation Name" in node:
                scanned[0] += (int(node.get("Actual Rows", 0)) + removed) * loops
            lines.append(
                f"      {'  ' * depth}{label}: rows={node.get('Actual Rows')} "
                f"removed_by_filter={removed} loops={loops}"
            )
            for child in node.get("Plans", []):
                walk(child, depth + 1)

        walk(root)
        # rows_examined sums rows each node emits, so a scan that filters
        # everything out counts as 0; the second figure adds what the scan
        # nodes threw away.
        lines.insert(
            0,
            f"    sweep EXPLAIN ANALYZE: rows x loops = {rows_examined(qs)}; "
            f"relation rows scanned incl. removed by filter = {scanned[0]}",
        )
    elif vendor == "mysql":
        reads, _unit, ms, found = measure(lambda: list(qs))
        lines.append(
            f"    sweep alone: Handler_read_* = {reads}, returned {len(found)}, "
            f"{ms:.1f} ms"
        )
        for line in qs.explain(analyze=True).splitlines():
            lines.append(f"      {line}")
    else:
        reads, _unit, ms, found = measure(lambda: list(qs))
        lines.append(
            f"    sweep alone: VM instructions = {reads}, returned {len(found)}, "
            f"{ms:.1f} ms"
        )
        for line in qs.explain().splitlines():
            lines.append(f"      {line}")
    with CaptureQueriesContext(connection) as captured:
        reconcile()
    first = captured.captured_queries[0]["sql"] if captured.captured_queries else ""
    lines.append(
        f"    tick issued {len(captured.captured_queries)} statement(s); first: "
        f"{first[:400]}"
    )
    return "\n".join(lines)


def assert_idle(flow, waiting: int, ready: int, cost: TickCost) -> None:
    """The tick under test had nothing to release, cancel or complete."""
    assert cost.completed == 0, "the tick completed a workflow; it was not idle"
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    assert OxTask.objects.filter(status=OxTask.Status.WAITING).count() == waiting, (
        "the tick released a waiting node; it was not idle"
    )
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == ready


# -- the tests ---------------------------------------------------------


@pytest.mark.parametrize("backend", ["flows", "flows_deferred"])
def test_an_idle_reconciler_tick_costs_the_same_whatever_the_ready_backlog(backend):
    """
    An idle tick's cost must not track the READY backlog.

    With nothing to release, the correct cost is O(waiting nodes), bounded
    by BACKSTOP_PAGE. Growing the ordinary READY backlog tenfold, from
    SIZES[0] to SIZES[1], must leave it roughly where it was.
    """
    small, large = SIZES
    assert large > small
    with configured(backend) as (task, alias):
        flow = in_flight_workflow(task, alias)
        waiting = WIDTH
        assert OxTask.objects.filter(status=OxTask.Status.WAITING).count() == waiting, (
            "precondition: the in-flight workflow's children are not waiting"
        )

        started = time.perf_counter()
        enqueue_backlog(small)
        built_small = time.perf_counter() - started
        refresh_statistics()
        at_small = measure_tick()
        assert_idle(flow, waiting, small, at_small)

        started = time.perf_counter()
        enqueue_backlog(large - small, start=small)
        built_large = time.perf_counter() - started
        refresh_statistics()
        at_large = measure_tick()
        assert_idle(flow, waiting, large, at_large)

        evidence = sweep_evidence()

    print(  # noqa: T201
        f"\n  [s1 sweep] {connection.vendor} backend={backend} "
        f"waiting_rows={waiting} unit={at_small.unit}\n"
        f"    READY={small:>7} (built {built_small:.1f}s): "
        f"tick reads={at_small.reads:>8} in {at_small.ms:.1f} ms\n"
        f"    READY={large:>7} (built {built_large:.1f}s): "
        f"tick reads={at_large.reads:>8} in {at_large.ms:.1f} ms\n"
        f"{evidence}"
    )

    assert at_large.reads < 2 * at_small.reads + BACKSTOP_PAGE, (
        f"an idle reconciler tick read {at_small.reads} ({at_small.unit}) over "
        f"{small} READY rows and {at_large.reads} over {large}, with {waiting} "
        "waiting row(s) and nothing to release: its cost tracks the READY backlog"
    )


@row_counting
def test_an_idle_reconciler_tick_over_a_mostly_finished_table_reads_few_rows():
    """
    A table that is mostly history, with a READY backlog and nothing to
    release: an idle tick over MYSQL_SHAPE must stay under 10,000 row reads.

    The old sweep's `ORDER BY pk LIMIT 1000` could plan on MySQL as a walk of
    the primary key that never reached its limit, reading SUCCESSFUL history
    as well as the READY backlog. The log records whether the reads exceed
    the READY row count.
    """
    history, backlog = MYSQL_SHAPE
    with configured("flows") as (task, alias):
        flow = in_flight_workflow(task, alias)

        started = time.perf_counter()
        enqueue_backlog(history)
        # Stands in for workers having run them: django-ox's own worker writes
        # exactly this status on each row it finishes.
        now = timezone.now()
        finished = OxTask.objects.filter(
            status=OxTask.Status.READY, task_path=tasks.plain_echo.module_path
        ).update(
            status=OxTask.Status.SUCCESSFUL,
            attempts=1,
            started_at=now,
            last_attempted_at=now,
            finished_at=now,
        )
        assert finished == history
        enqueue_backlog(backlog, start=history)
        built = time.perf_counter() - started
        refresh_statistics()

        cost = measure_tick()
        assert_idle(flow, WIDTH, backlog, cost)
        evidence = sweep_evidence()

    total = OxTask.objects.count()
    print(  # noqa: T201
        f"\n  [s1 sweep] whole-table shape: rows={total} READY={backlog} "
        f"SUCCESSFUL={history} (built {built:.1f}s)\n"
        f"    tick reads={cost.reads} ({cost.unit}) in {cost.ms:.1f} ms; "
        f"reads beyond the READY row count: {cost.reads - backlog}\n"
        f"{evidence}"
    )

    assert cost.reads < 10_000, (
        f"an idle reconciler tick read {cost.reads} rows ({cost.unit}) over a table "
        f"of {total} rows with {backlog} READY and nothing to release"
    )
