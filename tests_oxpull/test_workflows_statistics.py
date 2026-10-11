"""
Planner statistics taken during a wide fan-in do not take the workflow paths off
their indexes.

A 20,000-way fan-in fills the edge table with edges into one node. Statistics
gathered then say a downstream id matches thousands of edges. SQLite then
planned the claim's dependency check as a scan of the whole edge table for each
candidate row, and PostgreSQL and MySQL read the whole edge table once per claim.
A candidate has no edges in, or a few, so the claim cost the fan-in's width.

The test builds the fan-in through Workflow.add(), gathers statistics the way
each database does (ANALYZE, ANALYZE TABLE), and then runs the real paths:

- the claim of a map, through OxpullWorker.claim_one();
- a map's run through OxpullWorker.run_once(): its outcome, its record, the
  mark on its edge, and the release it tries after the commit;
- release_downstream() of the node the fan-in waits on;
- the cancellation, once a map has failed for good.

Every statement they send to a workflow or task table is explained with the
database's own EXPLAIN, and no plan may read a whole table where a row is looked
up for each row of another: inside a correlated subquery, on the inner side of a
join, or materialised for a join. The task table is never read whole. A read of
one workflow's own edges, or of every edge into one node, may read the table
when that is most of it, as here.

The plans are printed, so a failure shows the plan that read the table.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_workflows import drain, flow_worker
from .test_workflows_cost import claim_candidates

pytestmark = pytest.mark.django_db(transaction=True)

WIDTH = 20000
TASK = OxTask._meta.db_table
EDGE = OxpullWorkflowEdge._meta.db_table
NODE = OxpullWorkflowNode._meta.db_table
TABLES = (TASK, EDGE, NODE)


def gather_statistics() -> None:
    with connection.cursor() as cursor:
        if connection.vendor == "sqlite":
            cursor.execute("ANALYZE")
            return
        for table in TABLES:
            if connection.vendor == "postgresql":
                cursor.execute(f"ANALYZE {connection.ops.quote_name(table)}")
            else:
                cursor.execute(f"ANALYZE TABLE {connection.ops.quote_name(table)}")
                cursor.fetchall()


@dataclass(frozen=True)
class WholeRead:
    """A plan step that reads a whole table."""

    table: str
    repeated: bool
    step: str


def whole_reads(sql: str) -> tuple[list[WholeRead], str]:
    """The whole-table reads in this statement's plan, and the plan as text."""
    with connection.cursor() as cursor:
        if connection.vendor == "sqlite":
            cursor.execute("EXPLAIN QUERY PLAN " + sql)
            return _sqlite_reads(cursor.fetchall(), sql)
        if connection.vendor == "postgresql":
            cursor.execute("EXPLAIN (FORMAT JSON) " + sql)
            plan = cursor.fetchone()[0]
            if isinstance(plan, str):
                plan = json.loads(plan)
            return _postgresql_reads(plan[0]["Plan"]), json.dumps(plan, indent=1)
        cursor.execute("EXPLAIN FORMAT=TREE " + sql)
        text = cursor.fetchone()[0]
        return _mysql_reads(text, sql), text


def _aliases(sql: str, quote: str) -> dict[str, str]:
    """Table aliases in the SQL, mapped to the table, and each table to itself."""
    found = {table: table for table in TABLES}
    for table, alias in re.findall(
        rf"{quote}(\w+){quote}(?: AS)? ({quote}?[A-Za-z_]\w*{quote}?)", sql
    ):
        if table in TABLES:
            found[alias.strip(quote)] = table
    return found


def _sqlite_reads(rows, sql) -> tuple[list[WholeRead], str]:
    """
    EXPLAIN QUERY PLAN rows are (id, parent, unused, detail). A step runs again
    for each row of another when it sits under a CORRELATED subquery, or when
    an earlier step at its level reads a table, which makes it a join's inner
    side.
    """
    aliases = _aliases(sql, '"')
    parents = {row[0]: row[1] for row in rows}
    details = {row[0]: row[3] for row in rows}
    reads = []
    for node_id, parent, _, detail in rows:
        match = re.match(r"SCAN (\w+)", detail)
        if not match or match.group(1) not in aliases:
            continue
        repeated = False
        up = parent
        while up in details:
            repeated = repeated or details[up].startswith("CORRELATED")
            up = parents[up]
        earlier = [row for row in rows if row[1] == parent and row[0] < node_id]
        repeated = repeated or any(re.match(r"(SCAN|SEARCH) ", r[3]) for r in earlier)
        reads.append(WholeRead(aliases[match.group(1)], repeated, detail))
    text = "\n".join(f"{row[0]}<{row[1]} {row[3]}" for row in rows)
    return reads, text


def _postgresql_reads(node, *, repeated=False) -> list[WholeRead]:
    reads = []
    kind = node.get("Node Type", "")
    if kind == "Seq Scan" and node.get("Relation Name") in TABLES:
        reads.append(WholeRead(node["Relation Name"], repeated, kind))
    children = node.get("Plans", [])
    for child in children:
        relation = child.get("Parent Relationship", "")
        again = (
            repeated
            or relation == "SubPlan"
            or (relation == "Inner" and kind.startswith("Nested Loop"))
            or kind == "Materialize"
            or (kind == "Hash" and node.get("Parent Relationship") == "Inner")
        )
        reads.extend(_postgresql_reads(child, repeated=again))
    return reads


def _mysql_reads(text: str, sql: str) -> list[WholeRead]:
    aliases = _aliases(sql, "`")
    lines = text.splitlines()
    reads = []
    for index, line in enumerate(lines):
        match = re.search(r"-> (Table scan|Index scan) on (\w+)", line)
        if not match or match.group(2) not in aliases:
            continue
        depth = len(line) - len(line.lstrip())
        repeated = False
        for above in reversed(lines[:index]):
            above_depth = len(above) - len(above.lstrip())
            if above_depth >= depth:
                continue
            depth = above_depth
            if "dependent)" in above or "Materialize" in above or "antijoin" in above:
                repeated = True
        reads.append(WholeRead(aliases[match.group(2)], repeated, line.strip()))
    return reads


def explained(label: str, captured) -> list[tuple[str, WholeRead]]:
    """Explain every workflow or task statement captured, and return the bad reads."""
    bad = []
    print(f"\n==== {label} ({connection.vendor})")  # noqa: T201
    for query in captured.captured_queries:
        sql = query["sql"]
        if not sql.lstrip().upper().startswith(("SELECT", "UPDATE", "DELETE")):
            continue
        if not any(table in sql for table in TABLES):
            continue
        reads, text = whole_reads(sql)
        print("  SQL:", " ".join(sql.split())[:300])  # noqa: T201
        print("  " + text.replace("\n", "\n  ")[:4000])  # noqa: T201
        for read in reads:
            if read.repeated or read.table == TASK:
                bad.append((" ".join(sql.split())[:200], read))
    return bad


def test_statistics_from_a_wide_fan_in_keep_every_path_on_its_indexes():
    flow = workflows.create("statistics")
    names = [f"m{i:05d}" for i in range(WIDTH)]
    with transaction.atomic():
        # The first map runs first and fails for good, for the cancellation.
        flow.add(names[0], tasks.failing_step.using(priority=10), names[0])
        for name in names[1:]:
            flow.add(name, tasks.step, name)
        waiting = flow.add("reduce", tasks.step, "reduce", depends_on=names)
    flow.seal()
    gather_statistics()
    alias = oxpull_db_alias()
    worker = flow_worker()
    bad = []

    with CaptureQueriesContext(connection) as captured:
        claimed = worker.claim_one()
    assert claimed is not None and claimed.args == [names[0]]
    bad += explained("the claim", captured)
    worker.execute(claimed, inline=True)
    assert drain(worker, limit=2) == 2
    assert OxTask.objects.get(pk=claimed.id).status == OxTask.Status.FAILED

    with CaptureQueriesContext(connection) as captured:
        assert worker.run_once() is True
    bad += explained("a map's run, record, mark and release", captured)

    with CaptureQueriesContext(connection) as captured:
        assert workflows.release_downstream([waiting.id], alias) == 0
    bad += explained("release_downstream of the node the fan-in waits on", captured)

    with CaptureQueriesContext(connection) as captured:
        assert workflows._cancel_unreachable(flow.id, alias) == 1
    bad += explained("the cancellation", captured)
    assert OxTask.objects.get(pk=waiting.id).status == OxTask.Status.DISCARDED

    assert bad == [], "\n".join(f"{read} in {sql}" for sql, read in bad)


def test_the_claim_names_the_edge_index_and_postgresql_keeps_its_estimate():
    """
    The cheap half of the proof, for every run: the claim's SQL carries the
    hint on SQLite and MySQL, and the PostgreSQL column carries its n_distinct.
    """
    tasks.step.enqueue("plain")
    with CaptureQueriesContext(connection) as captured:
        assert flow_worker().claim_one() is not None
    edge_reads = [q["sql"] for q in captured.captured_queries if EDGE in q["sql"]]
    assert edge_reads, captured.captured_queries
    index = connection.ops.quote_name(workflows.EDGE_DOWNSTREAM_INDEX)
    if connection.vendor == "sqlite":
        assert f"INDEXED BY {index}" in edge_reads[0], edge_reads[0]
    elif connection.vendor == "mysql":
        assert f"FORCE INDEX ({index})" in edge_reads[0], edge_reads[0]
        assert "EXISTS(SELECT /*+ NO_SEMIJOIN() */ " in edge_reads[0], edge_reads[0]
    else:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT attoptions FROM pg_attribute WHERE attrelid = %s::regclass"
                " AND attname = 'downstream_task_id'",
                [EDGE],
            )
            assert cursor.fetchone()[0] == ["n_distinct=-0.5"]


def index_hint() -> str:
    """The hint this connection's engine takes, unquoted by any surrounding."""
    index = connection.ops.quote_name(workflows.EDGE_DOWNSTREAM_INDEX)
    if connection.vendor == "sqlite":
        return f"INDEXED BY {index}"
    return f"FORCE INDEX ({index})"


@pytest.mark.skipif(
    connection.vendor == "postgresql", reason="PostgreSQL takes no index hint"
)
def test_the_edge_hint_comes_last_in_the_from_item_it_applies_to():
    """
    Where the hint sits, which the check above cannot see.

    Both engines read the hint as part of a FROM item, so it has to follow
    the table and any alias on it. Django renders that alias differently
    across versions: quoted on some, bare on others. A hint written into
    the gap between table and alias is a syntax error, and the claim then
    takes nothing at all. Looking for the hint's text anywhere in the
    statement stays green while that is true, so this reads the FROM item
    itself and asserts the hint ends it.

    The SQL is compiled and not run, so a failure names the bad FROM item
    rather than the error the engine raises on it.
    """
    sql = str(claim_candidates(gated=True).query)
    opening = f"FROM {connection.ops.quote_name(EDGE)}"
    start = sql.index(opening)
    item = sql[start : sql.index(" WHERE ", start)]
    assert item.endswith(" " + index_hint()), item


@pytest.mark.skipif(
    connection.vendor == "postgresql", reason="PostgreSQL takes no index hint"
)
def test_an_edge_table_with_no_alias_keeps_the_hint_out_of_the_where():
    """
    A FROM item with no alias takes the hint too, and keeps its WHERE.

    Nothing compiles this shape today, because the probe's subquery is
    always aliased. It is asserted so that a later reading of the alias
    cannot quietly treat the WHERE keyword as one.
    """
    table = connection.ops.quote_name(EDGE)
    # S608: text handed to a string function, and never sent anywhere.
    sql = f'EXISTS(SELECT 1 AS "a" FROM {table} WHERE ("d" = 1) LIMIT 1)'  # noqa: S608
    pinned = workflows.pin_edge_probe(sql, connection)
    assert f"FROM {table} {index_hint()} WHERE " in pinned, pinned
