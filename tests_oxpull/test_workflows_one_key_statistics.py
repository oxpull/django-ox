"""
Statistics that show one key for a whole index do not turn the edge reads
into scans.

One wide fan-in alone in the edge table gives oxpull_wf_edge_down_idx the
same (downstream_task_id, satisfied) key on every row, and one wide fan-out
does the same to oxpull_wf_edge_up_idx. ANALYZE then writes a statistics line
whose numbers are all the row count. From 3.45 to 3.49 SQLite marks such an
index when a connection loads the line, and does not probe a marked index
with =, IN or IS. 3.45 applies that under INDEXED BY too, where the only plan
left is a read of the whole named index. The mark stays on the connection
until its schema is read again, whatever statistics are gathered later.

Six reads look an edge up by one of those two columns with no workflow in
hand, and run for every claim, every node or every reconciler page: the
claim's dependency check, the same check unhinted in Workflow.progress(), the
release's read of the edges into the nodes it was given, the outcome's read of
the edges out of the node that succeeded, the read that marks the edges into a
chunk of nodes, and the record repair's read of the edges out of a chunk of
them. Each has a test here that builds the state on the test's own connection,
keeps that connection, grows the unrelated workflow tenfold, and asserts the
read's work stays what it was and its plan seeks the index. Two more tests ask
a marked connection what the reads decide, and the last ones read the SQL each
database is sent.

A read given a chunk of ids sends one range an id. Being allowed to probe the
index is not the same as being planned to: a read of columns the index does
not carry looks dearer as a branch an id than as a read of the table, and a
planner that takes the table tests every row against every range. So those
three reads name their index, and two of the tests below are given a chunk
long enough to show it.

**The state is built, never assumed.** On a SQLite that marks indexes,
each test first shows a plain equality on the column planned as a whole
read, which is the mark at work, and shows it again after the table grew.
A test whose own rows give the index a second key, where that equality
has another index to take, shows the mark to an equality that names the
index instead. A newer SQLite cannot be put into that state, so there
the same tests assert a seek and bounded work and say nothing about the
mark. Compatibility coverage includes SQLite 3.45 and 3.46.

Work is SQLite VM instructions, counted by the hazard suite's Meter. Plans
are read with the statistics suite's parser.
"""

from __future__ import annotations

import re
import sqlite3
import uuid

import pytest
from django.db import DEFAULT_DB_ALIAS, connection, transaction
from django.db.models import Exists, OuterRef, Subquery, Value
from django.db.models.functions import Coalesce
from django.db.utils import ConnectionHandler
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.models import OxTask
from oxpull import worker as oxpull_worker
from oxpull import workflows
from oxpull.models import OxpullWorkflowEdge
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_workflows import flow_worker
from .test_workflows_hazard_in_flight_fan_in_edges_make_the_gate_cost import (
    Meter,
    Reading,
    claim_cost,
    fan_in,
    mark_maps_running,
)
from .test_workflows_hazard_unknown_status import UNKNOWN
from .test_workflows_statistics import EDGE, gather_statistics, whole_reads

pytestmark = pytest.mark.django_db(transaction=True)

sqlite_only = pytest.mark.skipif(
    connection.vendor != "sqlite",
    reason="the index mark, the VM instruction count and the plan text are SQLite's",
)

# The releases that do not probe a marked index with =, IN or IS, and the ones
# among them that hold to it under INDEXED BY.
MARKS_INDEXES = (3, 45, 0) <= sqlite3.sqlite_version_info < (3, 50, 0)
MARKS_UNDER_A_HINT = (3, 45, 0) <= sqlite3.sqlite_version_info < (3, 46, 0)

# Every index with more than about a hundred rows can be marked, so the narrow
# workflow is already wide enough, and the wide one is ten times it.
NARROW = 400
WIDE = 4000

# Ids in one list read. Long enough that a planner counting one-key statistics
# makes more of a branch an id than of a read of the whole table.
LISTED = 100

DOWN = ("oxpull_wf_edge_down_idx", "downstream_task_id")
UP = ("oxpull_wf_edge_up_idx", "upstream_task_id")


# -- the state ---------------------------------------------------------


def fan_out(name: str, width: int):
    """One root and `width` nodes that each wait for it, through Workflow.add()."""
    flow = workflows.create(name)
    with transaction.atomic():
        flow.add("root", tasks.step, f"{name}-root")
        for i in range(width):
            flow.add(
                f"leaf-{i:05d}", tasks.step, f"{name}-leaf-{i:05d}", depends_on="root"
            )
    flow.seal()
    return flow


def statistics_line(index: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute("SELECT stat FROM sqlite_stat1 WHERE idx = %s", [index])
        return cursor.fetchone()[0]


def plain_equality_plan(column: str) -> str:
    """How this connection plans `column = ?` on the edge table, with no hint."""
    with connection.cursor() as cursor:
        # S608: a column name from this file, in a statement that is only explained.
        cursor.execute(
            f'EXPLAIN QUERY PLAN SELECT "id" FROM "{EDGE}" WHERE "{column}" = %s',  # noqa: S608
            [uuid.uuid4().hex],
        )
        return " | ".join(row[3] for row in cursor.fetchall())


def assert_the_mark_is_in_force(index: str, column: str) -> None:
    """
    On a SQLite that marks indexes, show this connection's mark on `index`: a
    plain equality on its leading column reads a table or an index whole. A
    SQLite that has no mark has nothing to show, and the caller goes on to
    assert the seek and the bounded work there.
    """
    if not MARKS_INDEXES:
        return
    plan = plain_equality_plan(column)
    assert plan.startswith("SCAN "), (
        f"SQLite {sqlite3.sqlite_version} should hold a mark on {index} after "
        f"statistics {statistics_line(index)!r}, and plans a plain equality on "
        f"{column} as: {plan}"
    )


def named_index_equality_plan(index: str, column: str) -> str:
    """How this connection plans `column = ?` on the edge table for a read
    that names `index`, which is how the list reads meet the mark."""
    with connection.cursor() as cursor:
        # S608: an index and a column name from this file, in a statement that
        # is only explained.
        cursor.execute(
            f'EXPLAIN QUERY PLAN SELECT "id" FROM "{EDGE}" '  # noqa: S608
            f'INDEXED BY "{index}" WHERE "{column}" = %s',
            [uuid.uuid4().hex],
        )
        return " | ".join(row[3] for row in cursor.fetchall())


def assert_the_mark_is_in_force_under_a_hint(index: str, column: str) -> None:
    """
    Show the mark to a read that names `index`: a plain equality there is
    planned as a read of the whole named index. From 3.46 on a named index is
    exempt, so there is nothing to show and the caller goes on to assert the
    seek and the bounded work.

    For a read whose own rows give the index a second key, where the plain
    equality assert_the_mark_is_in_force sends has another index to take.
    """
    if not MARKS_UNDER_A_HINT:
        return
    plan = named_index_equality_plan(index, column)
    assert plan.startswith("SCAN "), (
        f"SQLite {sqlite3.sqlite_version} should hold a mark on {index} after "
        f"statistics {statistics_line(index)!r}, and plans an equality on "
        f"{column} that names it as: {plan}"
    )


class KeptConnection:
    """The test's connection, and a check that it is still the one it was."""

    def __init__(self) -> None:
        connection.ensure_connection()
        self.raw = connection.connection

    def check(self) -> None:
        assert connection.connection is self.raw, (
            "the connection was replaced, and the mark went with the old one"
        )


def edge_plans(captured, column: str, *, gate: bool = False) -> list[tuple[list, str]]:
    """
    The whole reads and the plan of every captured statement that looks an
    edge up by `column`: in the dependency check's subquery, which Django
    aliases V0, or in a statement of its own on the edge table, with or
    without the index its FROM item names.
    """
    # S608: a pattern that captured SQL is matched against, and never sent anywhere.
    if gate:
        wanted = re.compile(rf'FROM "{EDGE}" "?V0"?.* WHERE .*V0"?\."{column}"')
    else:
        wanted = re.compile(
            rf'^SELECT .* FROM "{EDGE}"( INDEXED BY "\w+")? WHERE '  # noqa: S608
            rf'.*"{EDGE}"\."{column}"'
        )
    return [
        whole_reads(query["sql"])
        for query in captured.captured_queries
        if wanted.search(query["sql"])
    ]


def assert_a_bounded_seek(
    read: str,
    plans: list[tuple[list, str]],
    readings: list[Reading],
    index: str,
    column: str,
    *,
    hinted: bool,
) -> None:
    """
    Every plan seeks `index` on `column` and reads no edge table whole, and the
    work beside the wide workflow is what it was beside the narrow one. Where
    this SQLite marks the index for this kind of read, the seek is the range:
    the equality is not allowed. Everything wrong is reported together, so a
    failure shows the plan and what the plan cost.
    """
    assert plans, "no statement read the edge table by that column"
    ranged = MARKS_UNDER_A_HINT if hinted else MARKS_INDEXES
    how = rf"\({column}>\? AND {column}<\?\)" if ranged else rf"\({column}[=>]"
    seek = re.compile(rf"SEARCH \w+ USING (COVERING )?INDEX {index} {how}")
    wrong = []
    for reads, plan in plans:
        whole = [read for read in reads if read.table == EDGE]
        if whole or not seek.search(plan):
            expected = "a range seek" if ranged else "a seek"
            wrong.append(
                f"expected {expected} of {index}, whole reads {whole}:\n{plan}"
            )
    narrow, wide = readings
    print(  # noqa: T201
        f"\n  [one key, SQLite {sqlite3.sqlite_version}] {read}: {narrow.work} VM "
        f"instructions beside {NARROW} edges, {wide.work} beside {WIDE}",
        end="",
    )
    if wide.work > 2 * narrow.work:
        wrong.append(
            f"{read} cost {narrow.work} VM instructions beside a {NARROW}-wide "
            f"workflow and {wide.work} beside a {WIDE}-wide one "
            f"({wide.work / max(narrow.work, 1):.1f}x)"
        )
    assert wrong == [], f"SQLite {sqlite3.sqlite_version}:\n" + "\n".join(wrong)


def small_flow(name: str, *, priority: int = 0):
    """
    `first`, at `priority`, and a `second` that waits for it: the workflow the
    reads are about.
    """
    flow = workflows.create(name)
    flow.add("first", tasks.step.using(priority=priority), f"{name}-first")
    second = flow.add("second", tasks.step, f"{name}-second", depends_on="first")
    return flow, second


def nodes_no_worker_recorded(name: str, count: int):
    """
    A workflow of `count` nodes that succeeded with no record written, each
    with a node below it. What a writer that died between the task row and the
    record leaves, and what the record repair is given.
    """
    flow = workflows.create(name)
    with transaction.atomic():
        for i in range(count):
            flow.add(f"up-{i:03d}", tasks.step, f"{name}-up-{i:03d}")
            flow.add(
                f"down-{i:03d}",
                tasks.step,
                f"{name}-down-{i:03d}",
                depends_on=f"up-{i:03d}",
            )
    flow.seal()
    ids = flow.node_ids()
    OxTask.objects.filter(pk__in=[ids[f"up-{i:03d}"] for i in range(count)]).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    return flow


# -- the six reads, beside a workflow that grows -----------------------


@sqlite_only
def test_a_claim_seeks_the_edge_index_under_one_key_statistics():
    """
    The claim's dependency check, hinted, once per candidate row. The claims
    take plain tasks enqueued first, so every candidate is outside the fan-in.
    """
    meter = Meter()
    worker = flow_worker()
    for i in range(30):
        tasks.step.enqueue(f"unrelated-{i:02d}")
    kept = KeptConnection()
    readings, plans = [], []
    for width in (NARROW, WIDE):
        fan_in(f"fan-in-{width}", width, tasks.step)
        gather_statistics()
        assert_the_mark_is_in_force(*DOWN)
        readings.append(claim_cost(meter, worker, label_prefix="unrelated"))
        with CaptureQueriesContext(connection) as captured:
            assert worker.claim_one() is not None
        plans += edge_plans(captured, DOWN[1], gate=True)
    kept.check()
    assert_a_bounded_seek("a claim", plans, readings, *DOWN, hinted=True)


@sqlite_only
def test_progress_seeks_the_edge_index_under_one_key_statistics():
    """
    The same check with no hint, for each READY node of the workflow asked
    about. That workflow is added once the first statistics are gathered: an
    edge of its own in them would be a second key.
    """
    meter = Meter()
    flow = None
    kept = KeptConnection()
    readings, plans = [], []
    for width in (NARROW, WIDE):
        fan_in(f"fan-in-{width}", width, tasks.step)
        gather_statistics()
        assert_the_mark_is_in_force(*DOWN)
        flow = flow or small_flow("asked-about")[0]
        with meter.measure() as reading, CaptureQueriesContext(connection) as captured:
            progress = flow.progress()
        assert (progress.total, progress.pending, progress.blocked) == (2, 2, 1)
        readings.append(reading)
        plans += edge_plans(captured, DOWN[1], gate=True)
    kept.check()
    assert_a_bounded_seek(
        "progress() of a two-node workflow", plans, readings, *DOWN, hinted=False
    )


@sqlite_only
def test_a_release_seeks_the_edge_index_under_one_key_statistics():
    """
    release_downstream's read of the edges into the nodes it was given. The
    node still waits for its upstream, so the read is the whole of the call.
    Its workflow is added once the first statistics are gathered: an edge of
    its own in them would be a second key.
    """
    meter = Meter()
    alias = oxpull_db_alias()
    second = None
    kept = KeptConnection()
    readings, plans = [], []
    for width in (NARROW, WIDE):
        fan_in(f"fan-in-{width}", width, tasks.step)
        gather_statistics()
        assert_the_mark_is_in_force(*DOWN)
        second = second or small_flow("released")[1]
        with meter.measure() as reading, CaptureQueriesContext(connection) as captured:
            assert workflows.release_downstream([second.id], alias) == 0
        readings.append(reading)
        plans += edge_plans(captured, DOWN[1])
    kept.check()
    assert_a_bounded_seek(
        "the release of one waiting node", plans, readings, *DOWN, hinted=False
    )


@sqlite_only
def test_an_outcome_seeks_the_edge_index_under_one_key_statistics(monkeypatch):
    """
    record_node_outcome's read of the edges out of the node that succeeded,
    inside the worker's own outcome transaction. A fan-out is what gives the
    upstream index one key. The node that runs outranks the fan-out's root.
    """
    meter = Meter()
    worker = flow_worker()
    outcomes: list[Reading] = []
    real_outcome = oxpull_worker.record_node_outcome

    def metered_outcome(*args):
        with meter.measure() as reading:
            downstream = real_outcome(*args)
        outcomes.append(reading)
        return downstream

    monkeypatch.setattr(oxpull_worker, "record_node_outcome", metered_outcome)
    kept = KeptConnection()
    plans = []
    for width in (NARROW, WIDE):
        fan_out(f"fan-out-{width}", width)
        gather_statistics()
        assert_the_mark_is_in_force(*UP)
        _, second = small_flow(f"ran-beside-{width}", priority=10)
        with CaptureQueriesContext(connection) as captured:
            assert worker.run_once() is True
        assert tasks.STATE["order"][-1] == f"ran-beside-{width}-first"
        assert OxTask.objects.get(pk=second.id).status == OxTask.Status.READY
        plans += edge_plans(captured, UP[1])
    kept.check()
    assert len(outcomes) == 2
    assert_a_bounded_seek(
        "recording one node's success", plans, outcomes, *UP, hinted=False
    )


@sqlite_only
def test_the_read_that_marks_edges_seeks_the_edge_index_under_one_key_statistics():
    """
    _satisfy_edges_into's read of the edges into the nodes it was given, a
    reconciler page of them at a time. It selects columns the index does not
    carry, which is where a planner weighing a range an id against a read of
    the table takes the table. The ids name no edge, as a page of nodes
    already released does, so the read is the whole of the call.
    """
    meter = Meter()
    alias = oxpull_db_alias()
    given = sorted(uuid.uuid4() for _ in range(LISTED))
    kept = KeptConnection()
    readings, plans = [], []
    for width in (NARROW, WIDE):
        fan_in(f"fan-in-{width}", width, tasks.step)
        gather_statistics()
        assert_the_mark_is_in_force(*DOWN)
        with meter.measure() as reading, CaptureQueriesContext(connection) as captured:
            assert workflows._satisfy_edges_into(given, alias) == 0
        readings.append(reading)
        plans += edge_plans(captured, DOWN[1])
    kept.check()
    assert_a_bounded_seek(
        f"the read that marks the edges into {LISTED} nodes",
        plans,
        readings,
        *DOWN,
        hinted=True,
    )


@sqlite_only
def test_a_record_repair_seeks_the_edge_index_under_one_key_statistics():
    """
    _repair_records_once's read of the edges out of the nodes whose outcome no
    worker recorded, on the other column and with the same shape. A fan-out is
    what gives the upstream index one key. Each pass repairs a workflow of its
    own, because a repair marks the edges it found and a second pass over the
    same workflow would find none. Both are added once the first statistics
    are gathered: an edge of their own in them would be a second key.

    A hundred nodes that succeeded are a hundred upstream keys, so the mark is
    shown to a read that names the index rather than to a plain equality,
    which by the second pass has another index to take.
    """
    meter = Meter()
    alias = oxpull_db_alias()
    waiting: list = []
    kept = KeptConnection()
    readings, plans = [], []
    for width in (NARROW, WIDE):
        fan_out(f"fan-out-{width}", width)
        gather_statistics()
        assert_the_mark_is_in_force_under_a_hint(*UP)
        waiting = waiting or [
            nodes_no_worker_recorded(f"repaired-{i}", LISTED) for i in range(2)
        ]
        flow = waiting.pop(0)
        with meter.measure() as reading, CaptureQueriesContext(connection) as captured:
            downstream = workflows._repair_records_once(flow.id, alias)
        assert len(downstream) == LISTED
        readings.append(reading)
        plans += edge_plans(captured, UP[1])
    kept.check()
    assert_a_bounded_seek(
        f"the repair of {LISTED} nodes' records",
        plans,
        readings,
        *UP,
        hinted=True,
    )


# -- what the reads decide, on a marked connection ---------------------


def chain(name: str):
    """`up` and a `down` that waits for it. Returns the two task ids."""
    flow = workflows.create(name)
    flow.add("up", tasks.step, f"{name}-up")
    down = flow.add("down", tasks.step, f"{name}-down", depends_on="up")
    return flow.node_ids()["up"], uuid.UUID(str(down.id))


@sqlite_only
def test_the_gate_fails_closed_under_one_key_statistics():
    """
    The range beside the equality decides nothing the equality did not. On a
    connection whose edge index is marked, a READY node behind an open edge
    runs only when its upstream row is there and SUCCESSFUL, and a satisfied
    edge lets its node run with the upstream row gone. Asked three ways: the
    claim's condition, the unhinted check progress() uses, and real claims.
    """
    wide = fan_in("fan-in", NARROW, tasks.step)
    assert mark_maps_running(wide) == NARROW
    gather_statistics()
    assert_the_mark_is_in_force(*DOWN)
    kept = KeptConnection()

    done_up, after_done = chain("open-edge-upstream-successful")
    OxTask.objects.filter(pk=done_up).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    running_up, after_running = chain("open-edge-upstream-running")
    OxTask.objects.filter(pk=running_up).update(status=OxTask.Status.RUNNING)
    gone_up, after_gone = chain("open-edge-upstream-gone")
    OxTask.objects.filter(pk=gone_up).delete()
    unknown_up, after_unknown = chain("open-edge-upstream-unknown")
    OxTask.objects.filter(pk=unknown_up).update(status=UNKNOWN)
    marked_up, after_marked = chain("satisfied-edge-upstream-gone")
    OxpullWorkflowEdge.objects.filter(upstream_task_id=marked_up).update(satisfied=True)
    OxTask.objects.filter(pk=marked_up).delete()

    downs = [after_done, after_running, after_gone, after_unknown, after_marked]
    assert OxTask.objects.filter(pk__in=downs, status=OxTask.Status.WAITING).update(
        status=OxTask.Status.READY
    ) == len(downs)
    runnable = {after_done, after_marked}

    ready = OxTask.objects.filter(pk__in=downs, status=OxTask.Status.READY)
    hinted = set(
        ready.filter(workflows.runnable_filter_q()).values_list("pk", flat=True)
    )
    unhinted = set(
        ready.filter(~Exists(workflows._blocking_edges(OuterRef("pk")))).values_list(
            "pk", flat=True
        )
    )
    assert hinted == runnable
    assert unhinted == runnable

    worker = flow_worker()
    claimed = set()
    while (task := worker.claim_one()) is not None:
        claimed.add(task.pk)
    assert claimed & set(downs) == runnable
    kept.check()
    assert_the_mark_is_in_force(*DOWN)


@sqlite_only
def test_the_list_reads_take_exactly_the_ids_they_were_given_under_one_key_statistics():
    """
    The list reads hold a range per id, and a range must not take in an id
    that sorts between two of the list. Seven nodes wait behind an upstream
    that succeeded with its edge never marked, as a missed mark leaves them.
    Every other one is named, among ids of nothing at all, first to the read
    that marks edges and then to the release. The named ones are marked and
    released. The ones that sort between them, a node behind an upstream that
    has not run, and the wide fan-in's own node are left as they were.
    """
    alias = oxpull_db_alias()
    wide = fan_in("fan-in", NARROW, tasks.step)
    gather_statistics()
    assert_the_mark_is_in_force(*DOWN)
    kept = KeptConnection()

    missed = sorted((chain(f"missed-mark-{i}") for i in range(7)), key=lambda c: c[1])
    OxTask.objects.filter(pk__in=[up for up, _ in missed]).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    waiting = [down for _, down in missed]
    listed, between = waiting[::2], waiting[1::2]
    _, held = chain("upstream-not-run")
    reduce_id = wide.node_ids()["reduce"]
    given = [*listed, held, *(uuid.uuid4() for _ in range(40))]

    in_the_list = set(
        OxpullWorkflowEdge.objects.filter(
            downstream_task_id__probe_in=given
        ).values_list("downstream_task_id", flat=True)
    )
    assert in_the_list == {*listed, held}

    assert workflows._satisfy_edges_into(given, alias) == len(listed)
    marked = OxpullWorkflowEdge.objects.filter(satisfied=True)
    assert set(marked.values_list("downstream_task_id", flat=True)) == set(listed)

    assert workflows.release_downstream(given, alias) == len(listed)
    status = dict(OxTask.objects.values_list("pk", "status"))
    assert {status[pk] for pk in listed} == {OxTask.Status.READY}
    assert {status[pk] for pk in (*between, held, reduce_id)} == {OxTask.Status.WAITING}
    kept.check()
    assert_the_mark_is_in_force(*DOWN)


# -- what each database is sent ----------------------------------------


def _gate(edges) -> str:
    """A claim-shaped query over the task table with `edges` as its check."""
    return OxTask.objects.filter(~workflows._EdgeProbe(edges)).query


def _exact_edges(downstream):
    """workflows._blocking_edges, written with Django's own `exact`."""
    upstream_status = Subquery(
        OxTask.objects.filter(pk=OuterRef("upstream_task_id")).values("status")[:1]
    )
    return (
        OxpullWorkflowEdge.objects.filter(
            downstream_task_id=downstream, satisfied=False
        )
        .alias(upstream_status=Coalesce(upstream_status, Value("")))
        .exclude(upstream_status=OxTask.Status.SUCCESSFUL)
    )


def compiles_for(vendor: str):
    """
    A connection that compiles SQL as `vendor` does: the test's own where that
    is its vendor, and otherwise a PostgreSQL one that never connects. MySQL's
    compiler asks the server whether it is MariaDB before it binds a UUID, so
    MySQL's SQL is compared on a MySQL run alone.
    """
    if connection.vendor == vendor:
        return connection
    if vendor == "mysql":
        pytest.skip("compiling a UUID for MySQL asks a server which MySQL it is")
    engine = f"django.db.backends.{vendor}"
    handler = ConnectionHandler(
        {DEFAULT_DB_ALIAS: {"ENGINE": engine, "NAME": "unused"}}
    )
    return handler[DEFAULT_DB_ALIAS]


@pytest.mark.parametrize("vendor", ["postgresql", "mysql"])
def test_the_other_databases_are_sent_what_exact_and_in_send_them(vendor):
    """
    The ranges, the index a read names and the plus that puts a term beyond it
    are SQLite's alone. MySQL's claim is planned from FORCE INDEX and
    NO_SEMIJOIN over this exact text and PostgreSQL's from its column
    statistics, so the three list reads are compiled here as the source builds
    them, with the claim's check, and compared byte for byte with the same
    querysets under Django's own lookups.
    """
    database = compiles_for(vendor)
    ids = [uuid.UUID(int=n) for n in (3, 1, 2)]
    edges = OxpullWorkflowEdge.objects
    down, up = DOWN[0], UP[0]
    pairs = [
        (
            edges.filter(downstream_task_id__probe=ids[0], satisfied=False),
            edges.filter(downstream_task_id=ids[0], satisfied=False),
        ),
        (
            edges.filter(upstream_task_id__probe=ids[0], satisfied=False),
            edges.filter(upstream_task_id=ids[0], satisfied=False),
        ),
        (
            workflows._reading_index(
                edges.filter(
                    downstream_task_id__probe_in=ids, satisfied__aside=False
                ).values_list("pk", "upstream_task_id", "downstream_task_id"),
                down,
            ),
            edges.filter(downstream_task_id__in=ids, satisfied=False).values_list(
                "pk", "upstream_task_id", "downstream_task_id"
            ),
        ),
        (
            workflows._reading_index(
                edges.filter(
                    downstream_task_id__probe_in=ids, satisfied__aside=False
                ).values_list("downstream_task_id", flat=True),
                down,
            ),
            edges.filter(downstream_task_id__in=ids, satisfied=False).values_list(
                "downstream_task_id", flat=True
            ),
        ),
        (
            workflows._reading_index(
                edges.filter(
                    upstream_task_id__probe_in=ids, satisfied__aside=False
                ).values_list("pk", "downstream_task_id"),
                up,
            ),
            edges.filter(upstream_task_id__in=ids, satisfied=False).values_list(
                "pk", "downstream_task_id"
            ),
        ),
    ]
    for probe, builtin in pairs:
        ours = probe.query.get_compiler(connection=database).as_sql()
        theirs = builtin.query.get_compiler(connection=database).as_sql()
        assert ours == theirs
        assert "BETWEEN" not in ours[0]
        assert "INDEXED BY" not in ours[0]
        assert " +" not in ours[0]

    ours = _gate(workflows._blocking_edges(OuterRef("pk")))
    theirs = _gate(_exact_edges(OuterRef("pk")))
    ours = ours.get_compiler(connection=database).as_sql()
    assert ours == theirs.get_compiler(connection=database).as_sql()
    assert "BETWEEN" not in ours[0]
    if vendor == "mysql":
        assert "FORCE INDEX (`oxpull_wf_edge_down_idx`)" in ours[0]
        assert "EXISTS(SELECT /*+ NO_SEMIJOIN() */ " in ours[0]


@sqlite_only
def test_sqlite_is_sent_the_equality_and_a_range_that_holds_the_same_key():
    # Django quotes the subquery's alias from 6.1 on and did not before.
    outer = r'\("django_ox_oxtask"\."id"\)'
    column = r'"?V0"?\."downstream_task_id"'
    both = rf"\({column} = {outer} AND {column} BETWEEN {outer} AND {outer}\)"
    hinted = str(_gate(workflows._blocking_edges(OuterRef("pk"))))
    unhinted = str(
        OxTask.objects.filter(Exists(workflows._blocking_edges(OuterRef("pk")))).query
    )
    assert re.search(both, hinted), hinted
    assert re.search(both, unhinted), unhinted
    assert ' INDEXED BY "oxpull_wf_edge_down_idx" WHERE ' in hinted
    assert "INDEXED BY" not in unhinted

    one = uuid.UUID(int=7)
    sql, params = (
        OxpullWorkflowEdge.objects.filter(upstream_task_id__probe=one)
        .values("pk")
        .query.sql_with_params()
    )
    column = f'"{EDGE}"."upstream_task_id"'
    assert f"({column} = %s AND {column} BETWEEN %s AND %s)" in sql
    assert params == (one.hex,) * 3


@sqlite_only
def test_sqlite_is_sent_one_range_for_each_id_in_a_list():
    ids = [uuid.UUID(int=n) for n in (3, 1, 2, 3)]
    sql, params = (
        OxpullWorkflowEdge.objects.filter(downstream_task_id__probe_in=[*ids, None])
        .values("pk")
        .query.sql_with_params()
    )
    column = f'"{EDGE}"."downstream_task_id"'
    one = f"{column} BETWEEN %s AND %s"
    assert f"WHERE ({one} OR ({one} OR {one}))" in sql
    assert " IN " not in sql
    assert params == tuple(uuid.UUID(int=n).hex for n in (3, 3, 1, 1, 2, 2))


@sqlite_only
def test_sqlite_is_sent_the_index_a_list_read_takes_and_a_plus_beside_it():
    """
    A list read names its index in its FROM item, and its term on the index's
    second column carries the plus that puts it beyond the index. Django spells
    `satisfied=False` as an equality from 6.1 on and negated the column before,
    and SQLite is sent the one spelling whichever it is.
    """
    ids = [uuid.UUID(int=n) for n in (1, 2)]
    sql, params = workflows._reading_index(
        OxpullWorkflowEdge.objects.filter(
            downstream_task_id__probe_in=ids, satisfied__aside=False
        ).values_list("downstream_task_id", flat=True),
        workflows.EDGE_DOWNSTREAM_INDEX,
    ).query.sql_with_params()
    assert f'FROM "{EDGE}" INDEXED BY "{DOWN[0]}" WHERE ' in sql
    assert f'AND +"{EDGE}"."satisfied" = %s)' in sql
    assert params[-1] is False

    out = workflows._reading_index(
        OxpullWorkflowEdge.objects.filter(
            upstream_task_id__probe_in=ids, satisfied__aside=False
        ).values_list("pk", "downstream_task_id"),
        workflows.EDGE_UPSTREAM_INDEX,
    )
    assert f'FROM "{EDGE}" INDEXED BY "{UP[0]}" WHERE ' in str(out.query)

    nothing = OxpullWorkflowEdge.objects.filter(downstream_task_id__probe_in=[])
    assert list(nothing) == []
    by_subquery = OxpullWorkflowEdge.objects.filter(
        downstream_task_id__probe_in=OxTask.objects.values("pk")
    )
    assert " IN (SELECT " in str(by_subquery.query)


@sqlite_only
def test_a_full_chunk_of_ids_is_one_statement_sqlite_accepts():
    """
    A flat OR of a full chunk is deeper than SQLite's expression limit and
    binds more parameters than an old default build allows. The ranges nest
    as a tree, and a chunk is sized for two parameters an id.
    """
    alias = oxpull_db_alias()
    _, held = chain("held")
    ids = [uuid.uuid4() for _ in range(workflows.ID_CHUNK_SIZE - 1)]
    found = OxpullWorkflowEdge.objects.filter(downstream_task_id__probe_in=[*ids, held])
    assert [edge.downstream_task_id for edge in found] == [held]

    connection.ensure_connection()
    raw = connection.connection
    limit = sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER
    previous = raw.setlimit(limit, 999)
    try:
        assert workflows._id_chunk_size(alias) == (999 - 8) // 2
        assert workflows.release_downstream([*ids, held], alias) == 0
        assert connection.connection is raw and raw.getlimit(limit) == 999
    finally:
        raw.setlimit(limit, previous)
    assert workflows._id_chunk_size(alias) == workflows.ID_CHUNK_SIZE
