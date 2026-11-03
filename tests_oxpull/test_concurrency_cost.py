"""
What the concurrency count costs, read from each database's own plan.

The count runs before every claim that follows a covered claim or outcome, so
its plan is on the claim path. Over a task table of half a million rows,
nearly all of them finished or waiting, the count must find the few RUNNING
rows through an index that leads with status, and never read the table whole.
And it must stay one statement however many limits are configured: one SELECT
counts every limit.

The statement explained is the one _read_running() sends, captured with its
parameters, not a copy of its query. Each run also explains the same count
without its status filter and requires the same check to refuse that plan, so
a check that cannot see a full read fails here rather than passing quietly.
The plans are printed, so a failure shows the plan it judged.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.db import connection, connections
from django.db.models import Count
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.models import OxTask

from .test_concurrency_limits import (
    ECHO,
    GAUGED,
    UNGAUGED,
    concurrency_limits,
    oxpull_worker,
    running_row,
)

# transaction=True, not the default wrapper: VACUUM refuses to run inside a
# transaction, and ANALYZE TABLE commits implicitly on MySQL.
pytestmark = pytest.mark.django_db(transaction=True)

TABLE = OxTask._meta.db_table
STATUS_INDEXES = frozenset({"ox_reaper_idx", "ox_reaper_expiry_idx", "ox_dequeue_idx"})
ROWS = 500_000
SEED = 1024
DOUBLINGS = 9  # 1024 * 2**9 = 524,288 finished or waiting rows

# Each max is the number of covered rows fill() leaves RUNNING, so a count
# that read the right rows blocks every limit.
ONE_LIMIT = {"g": {"max": 1, "tasks": [GAUGED]}}
FIVE_LIMITS = {
    "g": {"max": 1, "tasks": [GAUGED]},
    "u": {"max": 1, "tasks": [UNGAUGED]},
    "mail": {"max": 2, "queues": ["emails"]},
    "both": {"max": 2, "tasks": [ECHO], "queues": ["emails"]},
    "paths": {"max": 3, "tasks": [GAUGED, UNGAUGED, ECHO]},
}


def new_id_sql() -> str:
    """A fresh primary key in the column's own storage, per database."""
    if connection.vendor == "postgresql":
        return "gen_random_uuid()"
    if connection.vendor == "mysql":
        return "REPLACE(UUID(), '-', '')"
    return "lower(hex(randomblob(16)))"


def fill() -> None:
    """
    Over half a million rows that are not RUNNING, then a few RUNNING rows,
    covered and not, then statistics as each database gathers them.

    A seed of finished, failed and waiting rows is copied into itself with new
    primary keys until the table is over ROWS. The RUNNING rows are created
    after the copying, so there are six of them, not six times 512.
    """
    now = timezone.now()
    statuses = (
        [OxTask.Status.SUCCESSFUL] * 800
        + [OxTask.Status.FAILED] * 112
        + [OxTask.Status.READY] * 112
    )
    assert len(statuses) == SEED
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path=(GAUGED, UNGAUGED, ECHO)[i % 3],
                queue_name=("default", "emails")[i % 2],
                backend_name="default",
                status=status,
                enqueued_at=now - timedelta(seconds=i),
                finished_at=None if status == OxTask.Status.READY else now,
            )
            for i, status in enumerate(statuses)
        ],
        batch_size=1000,
    )
    quote = connection.ops.quote_name
    columns = [field.column for field in OxTask._meta.concrete_fields]
    pk = OxTask._meta.pk.column
    others = ", ".join(quote(column) for column in columns if column != pk)
    copy = (
        f"INSERT INTO {quote(TABLE)} ({quote(pk)}, {others}) "  # noqa: S608
        f"SELECT {new_id_sql()}, {others} FROM {quote(TABLE)}"
    )
    with connection.cursor() as cursor:
        for _ in range(DOUBLINGS):
            cursor.execute(copy)
    running_row(GAUGED)
    running_row(UNGAUGED, queue="emails")
    running_row(ECHO, queue="emails")
    running_row("tests_oxpull.tasks.plain_fail")
    running_row("tests_oxpull.tasks.plain_flaky")
    running_row("tests_oxpull.tasks.unique_echo")
    assert OxTask.objects.count() > ROWS
    assert OxTask.objects.filter(status=OxTask.Status.RUNNING).count() == 6

    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(f"VACUUM (ANALYZE) {quote(TABLE)}")
        elif connection.vendor == "mysql":
            cursor.execute(f"ANALYZE TABLE {quote(TABLE)}")
            cursor.fetchall()
        else:
            cursor.execute("ANALYZE")


@contextmanager
def sent_statements(alias):
    """Every statement sent on `alias`, with its parameters."""
    sent = []

    def wrapper(execute, sql, params, many, context):
        sent.append((sql, params))
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(wrapper):
        yield sent


def plan_of(sql, params) -> tuple[list[str], list[str], str]:
    """
    (status-leading index reads, full reads, plan text) for one statement.

    A status-leading index read is a probe of one of STATUS_INDEXES on the
    status column. A full read is any read of the task table that is not
    bounded by an index condition: a sequential scan, a scan of a whole index,
    or MySQL's ALL and index access types.
    """
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("EXPLAIN (FORMAT JSON) " + sql, params)
            plan = cursor.fetchone()[0]
            if isinstance(plan, str):
                plan = json.loads(plan)
            used, full = _postgresql(plan[0]["Plan"])
            return used, full, json.dumps(plan, indent=1)
        if connection.vendor == "mysql":
            cursor.execute("EXPLAIN FORMAT=JSON " + sql, params)
            plan = json.loads(cursor.fetchone()[0])
            used, full = _mysql(plan)
            return used, full, json.dumps(plan, indent=1)
        cursor.execute("EXPLAIN QUERY PLAN " + sql, params)
        rows = cursor.fetchall()
        used, full = _sqlite([row[3] for row in rows])
        return used, full, "\n".join(row[3] for row in rows)


def _postgresql(node) -> tuple[list[str], list[str]]:
    used, full = [], []
    kind = node.get("Node Type", "")
    if kind == "Seq Scan" and node.get("Relation Name") == TABLE:
        full.append(kind)
    if kind in {"Index Scan", "Index Only Scan", "Bitmap Index Scan"}:
        name = node.get("Index Name", "")
        condition = node.get("Index Cond", "")
        if name in STATUS_INDEXES and "status" in condition:
            used.append(f"{kind} using {name} on {condition}")
        elif not condition and (
            node.get("Relation Name") == TABLE or name.startswith("ox_")
        ):
            full.append(f"{kind} using {name} with no condition")
    for child in node.get("Plans", []):
        child_used, child_full = _postgresql(child)
        used += child_used
        full += child_full
    return used, full


def _mysql(node) -> tuple[list[str], list[str]]:
    used, full = [], []
    if isinstance(node, dict):
        if node.get("table_name") == TABLE:
            access = node.get("access_type")
            key = node.get("key")
            parts = node.get("used_key_parts", [])
            if access in {"ALL", "index"}:
                full.append(f"access_type {access} on key {key}")
            elif key in STATUS_INDEXES and parts[:1] == ["status"]:
                used.append(f"{access} on {key} {parts}")
        children = node.values()
    elif isinstance(node, list):
        children = node
    else:
        children = ()
    for child in children:
        child_used, child_full = _mysql(child)
        used += child_used
        full += child_full
    return used, full


def _sqlite(details) -> tuple[list[str], list[str]]:
    used, full = [], []
    for detail in details:
        if re.match(rf"SCAN {TABLE}\b", detail):
            full.append(detail)
        match = re.match(
            rf"SEARCH {TABLE} USING (?:COVERING )?INDEX (\w+) \(status=", detail
        )
        if match and match.group(1) in STATUS_INDEXES:
            used.append(detail)
    return used, full


def the_count(config):
    """The statement one _read_running() sends under `config`, and its params."""
    with concurrency_limits(config):
        worker = oxpull_worker()
        with sent_statements(worker._db_alias) as sent:
            worker._read_running()
    assert len(sent) == 1, sent
    return sent[0], worker


def test_the_count_probes_a_status_leading_index_over_half_a_million_rows():
    """
    The count for one limit and for five, explained over 524,294 rows of which
    six are RUNNING. Each plan reads the task table through a status-leading
    index on status and nowhere reads it whole. The same count without its
    status filter is explained too, and the same check must call it a full
    read.
    """
    fill()
    for config in (ONE_LIMIT, FIVE_LIMITS):
        (sql, params), worker = the_count(config)
        assert worker._concurrency_blocked == frozenset(config), "a wrong count"
        used, full, text = plan_of(sql, params)
        print(f"\n  [plan] {len(config)} limit(s):\n{text}")  # noqa: T201
        assert used, f"no status-leading index read in:\n{text}"
        assert full == [], f"a full read of {TABLE}: {full}\n{text}"

    # The control: the same aggregate with the status filter removed.
    with concurrency_limits(FIVE_LIMITS):
        limits = list(oxpull_worker()._concurrency.values())
    with sent_statements(connection.alias) as sent:
        OxTask.objects.using(connection.alias).aggregate(
            **{
                f"running_{index}": Count("pk", filter=limit.covered_q())
                for index, limit in enumerate(limits)
            }
        )
    ((sql, params),) = sent
    used, full, text = plan_of(sql, params)
    print(f"\n  [plan] control, no status filter:\n{text}")  # noqa: T201
    assert full, f"the check did not see a full read in:\n{text}"
    assert used == []


@pytest.mark.parametrize("config", [ONE_LIMIT, FIVE_LIMITS], ids=["one", "five"])
def test_one_statement_counts_every_limit(config):
    """One _read_running() sends one statement on the write alias, for any count."""
    running_row(GAUGED)
    with concurrency_limits(config):
        worker = oxpull_worker()
        with CaptureQueriesContext(connections[worker._db_alias]) as captured:
            worker._read_running()
    assert len(captured.captured_queries) == 1, captured.captured_queries
    assert "running_0" in captured.captured_queries[0]["sql"]
    assert f"running_{len(config) - 1}" in captured.captured_queries[0]["sql"]
