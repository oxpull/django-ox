"""
MySQL: a wide fan-out or cone goes to the database as one
literal IN list, and past the range optimizer's memory budget that
statement reads the whole task table.

The release (the worker, right after a SUCCESSFUL write lands)
and _cancel_one_level (the reconciler, inside the transaction holding the
workflow row lock and FOR UPDATE on the dead upstreams) each put every id
of the fan-out or cone into a single `id IN (...)`. django-ox's own bulk
actions chunk at UPDATE_CHUNK_SIZE = 1000 "so that one statement does not
hold a long lock over a large table". MySQL charges every IN element
against range_optimizer_max_mem_size (8 MiB by default); past it the
optimizer raises warning 3170 and plans a scan of django_ox_oxtask, so
the cost follows the retained table, not the cone.

The MySQL tests assert the behaviour a wide workflow needs: the
statements that carry the cone read it by primary-key range, and the
eager release reads fewer than three rows per released node by table scan.
The release reads the fan-out's edges to find the children and writes them
to mark them satisfied, and where one chunk of primary keys covers the
whole edge table MySQL scans it for each, which is two rows per node that
follow the fan-out, not the retained task table. Rows are
asserted as well, and on MySQL they end up correct; there it is a cost
cliff, not a wrong answer.

On PostgreSQL and SQLite the same single list meets a hard cap on bound
parameters instead, and past it the statement is refused, not slow. The
last two tests assert that a fan-out or cone wider than the cap is still
released by the worker and cancelled by the reconciler.

The release and the cancellation now send the ids in chunks of at most
workflows.ID_CHUNK_SIZE, fewer where the database's own parameter limit
is lower, and every test here passes.

Shortcuts, each producing the rows a real sequence produces:
- The first REAL_ADDS children are added with Workflow.add(); the rest
  are bulk-enqueued with django_ox.bulk.enqueue_many, moved to WAITING,
  and given the node and edge rows add() writes, and the workflow row's
  node_count is raised by the same number, as add() raises it. _build asserts
  every bulk child has the same stored shape as the real ones, and that seal()
  counted every child. Real add() is about 6 ms
  per node on MySQL, which is four minutes per 40,000-node leg.
- The retained history is enqueue_many plus an UPDATE to SUCCESSFUL,
  standing in for a week of default-backend tasks that ran and that
  ox_prune has not yet deleted.
- The failing root's retry backoff is cleared between attempts, as
  test_workflows_deferred does, instead of waiting it out.
- The parameter cap is applied to the test's own connection around the
  worker or reconciler call: a PostgreSQL connection with
  OPTIONS["server_side_binding"] = True swapped in for "default", or
  setlimit() on SQLite's. A project gets the first from its DATABASES
  setting and the second from a SQLite built with the default
  SQLITE_MAX_VARIABLE_NUMBER.
"""

from __future__ import annotations

import contextlib
import logging
import re
import sqlite3
import time
import uuid

import pytest
from django.db import DEFAULT_DB_ALIAS, connection, connections
from django.db.models import F
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.bulk import enqueue_many
from django_ox.compat import task_finished
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import flow_worker
from .test_workflows_deferred import deferring_worker

mysql_only = pytest.mark.skipif(
    connection.vendor != "mysql",
    reason="range_optimizer_max_mem_size and warning 3170 are MySQL's",
)

pytestmark = pytest.mark.django_db(transaction=True)

WIDTHS = [1000, 10000, 40000]
RETAINED_HISTORY = 60000
REAL_ADDS = 100
CHUNK = 1000

TASK_TABLE = OxTask._meta.db_table
EDGE_TABLE = OxpullWorkflowEdge._meta.db_table
NODE_TABLE = OxpullWorkflowNode._meta.db_table

# A literal list of task ids, not a subquery: `django_ox_oxtask`.`id` IN ('..', ..)
LITERAL_ID_LIST = re.compile(rf"`{TASK_TABLE}`\.`id` IN \((?!SELECT)([^)]*)\)")
SHAPE = (
    "status",
    "run_after",
    "backend_name",
    "queue_name",
    "priority",
    "max_attempts",
    "attempts",
    "lease_epoch",
    "task_path",
)


def _children(flow):
    return OxTask.objects.filter(
        id__in=OxpullWorkflowNode.objects.filter(workflow_id=flow.id)
        .exclude(name="root")
        .values("task_id")
    )


def _build(width, *, root_task, child_task):
    """root -> `width` children, sealed. Returns (flow, root id)."""
    flow = workflows.create(f"wide-{width}")
    flow.add("root", root_task, "root")
    labels = [f"c{i:06d}" for i in range(width)]
    for label in labels[:REAL_ADDS]:
        flow.add(label, child_task, label, depends_on="root")
    root_id = uuid.UUID(str(flow.node_ids()["root"]))

    rest = labels[REAL_ADDS:]
    results = enqueue_many(child_task, [((label,), {}) for label in rest])
    bulk_ids = [uuid.UUID(str(r.id)) for r in results]
    for i in range(0, len(bulk_ids), CHUNK):
        OxTask.objects.filter(pk__in=bulk_ids[i : i + CHUNK]).update(
            status=OxTask.Status.WAITING
        )
    now = timezone.now()
    OxpullWorkflowNode.objects.bulk_create(
        [
            OxpullWorkflowNode(
                workflow_id=flow.id,
                name=label,
                name_key=workflows.name_key(label),
                task_id=tid,
                created_at=now,
            )
            for label, tid in zip(rest, bulk_ids, strict=True)
        ],
        batch_size=2000,
    )
    OxpullWorkflowEdge.objects.bulk_create(
        [
            OxpullWorkflowEdge(
                workflow_id=flow.id,
                upstream_task_id=root_id,
                downstream_task_id=tid,
                created_at=now,
            )
            for tid in bulk_ids
        ],
        batch_size=2000,
    )
    OxpullWorkflow.objects.filter(pk=flow.id).update(
        node_count=F("node_count") + len(rest)
    )
    flow.seal()
    assert OxpullWorkflow.objects.get(pk=flow.id).total_count == width + 1

    real = set(
        OxTask.objects.filter(
            id__in=OxpullWorkflowNode.objects.filter(
                workflow_id=flow.id, name__in=labels[:REAL_ADDS]
            ).values("task_id")
        )
        .values_list(*SHAPE)
        .distinct()
    )
    every = set(_children(flow).values_list(*SHAPE).distinct())
    assert len(real) == 1, f"add() wrote children of more than one shape: {real}"
    assert every == real, f"bulk children differ from add()'s: {every - real}"
    assert _children(flow).count() == width
    assert OxpullWorkflowEdge.objects.filter(upstream_task_id=root_id).count() == width
    return flow, root_id


def _retained_history(count=RETAINED_HISTORY):
    """Settled default-backend rows, as ox_prune's 7-day retention keeps them."""
    for start in range(0, count, 20000):
        results = enqueue_many(
            tasks.default_step,
            [((f"h{i}",), {}) for i in range(start, min(count, start + 20000))],
        )
        ids = [r.id for r in results]
        for i in range(0, len(ids), CHUNK):
            OxTask.objects.filter(pk__in=ids[i : i + CHUNK]).update(
                status=OxTask.Status.SUCCESSFUL,
                finished_at=timezone.now(),
                attempts=1,
            )


def _analyze():
    with connection.cursor() as c:
        c.execute(f"ANALYZE TABLE {TASK_TABLE}, {EDGE_TABLE}, {NODE_TABLE}")
        c.fetchall()


def _rnd_next():
    with connection.cursor() as c:
        c.execute("SHOW SESSION STATUS LIKE 'Handler_read_rnd_next'")
        return int(c.fetchone()[1])


def _status_read_overhead():
    """What reading the counter itself adds, so it can be subtracted."""
    first = _rnd_next()
    return _rnd_next() - first


def _explain_id_lists(captured):
    """
    EXPLAIN every captured statement that carries a literal list of task
    ids. Returns (examined, problems): one entry per such statement, and
    the ones that scan the task table or drew warning 3170.
    """
    examined, problems = [], []
    for query in captured:
        sql = query["sql"]
        match = LITERAL_ID_LIST.search(sql)
        if match is None:
            continue
        ids = match.group(1).count(",") + 1
        with connection.cursor() as c:
            c.execute("EXPLAIN " + sql)
            columns = [d[0] for d in c.description]
            plan = [dict(zip(columns, row, strict=True)) for row in c.fetchall()]
            c.execute("SHOW WARNINGS")
            warned = [w[2] for w in c.fetchall() if int(w[1]) == 3170]
        access = [
            (p["type"], p["key"], p["rows"]) for p in plan if p["table"] == TASK_TABLE
        ]
        entry = {
            "statement": sql.split(" ", 1)[0],
            "ids": ids,
            "access": access,
            "secs": query["time"],
        }
        examined.append(entry)
        if warned or any(kind in ("ALL", "index") for kind, _, _ in access):
            problems.append({**entry, "warning": [w[:90] for w in warned]})
    return examined, problems


@mysql_only
@pytest.mark.parametrize("width", WIDTHS)
def test_a_worker_finishing_a_wide_root_releases_its_children_by_range(width):
    """
    One root, `width` waiting children, and the retained history ox_prune
    keeps. A real worker runs the root; its
    outcome write releases the children before task_finished is sent.
    """
    flow, root_id = _build(
        width,
        root_task=tasks.deferred_step,
        child_task=tasks.deferred_step,
    )
    children = _children(flow)
    assert children.filter(status=OxTask.Status.WAITING).count() == width
    _retained_history()
    _analyze()

    worker = deferring_worker()
    signalled = []

    def on_finished(sender, task_result, **kwargs):
        signalled.append(time.perf_counter())

    task_finished.connect(on_finished, weak=False)
    try:
        overhead = _status_read_overhead()
        before = _rnd_next()
        started = time.perf_counter()
        with CaptureQueriesContext(connection) as ctx:
            ran = worker.run_once()
        after = _rnd_next()
    finally:
        task_finished.disconnect(on_finished)

    assert ran is True
    assert tasks.STATE.get("order") == ["root"]
    assert OxTask.objects.get(pk=root_id).status == OxTask.Status.SUCCESSFUL
    released = children.filter(status=OxTask.Status.READY).count()
    assert released == width, "the rows are wrong, not just slow"

    rnd_next = after - before - overhead
    examined, problems = _explain_id_lists(ctx.captured_queries)
    waited = (signalled[0] - started) if signalled else None
    print(  # noqa: T201
        f"\n[release] width={width} retained={RETAINED_HISTORY} "
        f"table_rows~{OxTask.objects.count()} released={released} "
        f"Handler_read_rnd_next={rnd_next} "
        f"run_once_to_task_finished_s={waited and round(waited, 3)} "
        f"id_list_statements={examined}"
    )
    assert examined, "no statement carried the fan-out; the instrument saw nothing"
    failures = []
    if problems:
        failures.append(f"statements over the fan-out scan {TASK_TABLE}: {problems}")
    if rnd_next >= 3 * width:
        failures.append(
            f"releasing {width} children read {rnd_next} rows by table scan "
            f"(bound {3 * width}) in a table of ~{OxTask.objects.count()} rows"
        )
    assert not failures, "\n".join(failures)


@mysql_only
@pytest.mark.parametrize("width", WIDTHS)
def test_the_reconciler_cancels_a_wide_cone_by_range(width):
    """
    A root that fails every attempt on a real worker, `width` WAITING
    children blocked on it, and one real reconciler tick.
    """
    flow, root_id = _build(width, root_task=tasks.failing_step, child_task=tasks.step)
    children = _children(flow)
    _retained_history()
    _analyze()

    worker = flow_worker()
    for _ in range(10):
        if not worker.run_once():
            break
        OxTask.objects.filter(pk=root_id, status=OxTask.Status.READY).update(
            run_after=None
        )
    assert OxTask.objects.get(pk=root_id).status == OxTask.Status.FAILED
    assert tasks.STATE.get("order") == ["root", "root", "root"]
    assert children.filter(status=OxTask.Status.WAITING).count() == width

    overhead = _status_read_overhead()
    before = _rnd_next()
    with CaptureQueriesContext(connection) as ctx:
        workflows._reconcile()
    after = _rnd_next()

    cancelled = children.filter(status=OxTask.Status.DISCARDED).count()
    assert cancelled == width, "the rows are wrong, not just slow"

    examined, problems = _explain_id_lists(ctx.captured_queries)
    print(  # noqa: T201
        f"\n[cancel] width={width} retained={RETAINED_HISTORY} "
        f"table_rows~{OxTask.objects.count()} cancelled={cancelled} "
        f"tick_Handler_read_rnd_next={after - before - overhead} "
        f"id_list_statements={examined}"
    )
    assert examined, "no statement carried the cone; the instrument saw nothing"
    assert not problems, (
        f"statements over the cone scan {TASK_TABLE} inside the cancelling "
        f"transaction: {problems}"
    )


# -- past a bind-parameter cap: PostgreSQL and SQLite ------------------
#
# PostgreSQL's protocol carries at most 65,535 bound parameters. Django's
# psycopg backend binds client-side by default, where no cap applies, and
# server-side under OPTIONS["server_side_binding"] = True. SQLite's
# compile-time SQLITE_MAX_VARIABLE_NUMBER defaults to 32,766 since 3.32.0;
# this build raises it to 250,000, so the test lowers it to the default.

PARAMETER_CAPS = {"postgresql": 65535, "sqlite": 32766}

capped_vendors = pytest.mark.skipif(
    connection.vendor not in PARAMETER_CAPS,
    reason="MySQL's drivers interpolate parameters client-side; no bind cap",
)


@contextlib.contextmanager
def _parameter_cap(capped):
    """
    Put the vendor's bind-parameter cap on the default connection. Yields
    a check that the cap is still in force, so a reconnect inside the
    worker cannot turn this into a quiet pass.
    """
    if not capped:
        yield lambda: None
        return
    if connection.vendor == "postgresql":
        original = connections[DEFAULT_DB_ALIAS]
        # Unpooled: Django keeps one pool per alias, built from the OPTIONS
        # it first saw, so a checkout from it would bind client-side
        # whatever these say.
        options = {
            name: value
            for name, value in original.settings_dict["OPTIONS"].items()
            if name != "pool"
        }
        settings_dict = {
            **original.settings_dict,
            "OPTIONS": {**options, "server_side_binding": True},
        }
        swapped = original.__class__(settings_dict, alias=DEFAULT_DB_ALIAS)
        connections[DEFAULT_DB_ALIAS] = swapped

        def still_capped():
            assert connections[DEFAULT_DB_ALIAS] is swapped
            with connection.cursor() as c:
                assert type(c.cursor).__name__ == "ServerBindingCursor"

        try:
            yield still_capped
        finally:
            swapped.close()
            connections[DEFAULT_DB_ALIAS] = original
        return
    connection.ensure_connection()
    raw = connection.connection
    limit = sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER
    previous = raw.setlimit(limit, PARAMETER_CAPS["sqlite"])

    def still_capped():
        assert connection.connection is raw
        assert raw.getlimit(limit) == PARAMETER_CAPS["sqlite"]

    try:
        yield still_capped
    finally:
        raw.setlimit(limit, previous)


def _logged_failures(caplog):
    return [
        (r.getMessage()[:70], str(r.exc_info[1])[:90] if r.exc_info else None)
        for r in caplog.records
        if r.name.startswith("oxpull") and r.levelno >= logging.WARNING
    ]


@capped_vendors
@pytest.mark.parametrize(
    "capped",
    [pytest.param(False, id="uncapped"), pytest.param(True, id="capped")],
)
def test_a_worker_releases_a_fan_out_wider_than_the_parameter_cap(capped, caplog):
    caplog.set_level(logging.WARNING, logger="oxpull")
    width = PARAMETER_CAPS[connection.vendor] + 5000
    flow, root_id = _build(
        width,
        root_task=tasks.deferred_step,
        child_task=tasks.deferred_step,
    )
    children = _children(flow)
    worker = deferring_worker()
    with _parameter_cap(capped) as still_capped:
        ran = worker.run_once()
        still_capped()

    assert ran is True
    assert OxTask.objects.get(pk=root_id).status == OxTask.Status.SUCCESSFUL
    released = children.filter(status=OxTask.Status.READY).count()
    logged = _logged_failures(caplog)
    print(  # noqa: T201
        f"\n[cap-release] vendor={connection.vendor} capped={capped} "
        f"width={width} released={released} logged={logged}"
    )
    assert released == width, (
        f"the worker that finished root released {released} of {width} children "
        f"and logged {logged}; the rest wait for the reconciler, "
        f"{workflows.BACKSTOP_PAGE} per tick"
    )


@capped_vendors
@pytest.mark.parametrize(
    "capped",
    [pytest.param(False, id="uncapped"), pytest.param(True, id="capped")],
)
def test_the_reconciler_cancels_a_cone_wider_than_the_parameter_cap(capped, caplog):
    caplog.set_level(logging.WARNING, logger="oxpull")
    width = PARAMETER_CAPS[connection.vendor] + 5000
    flow, root_id = _build(width, root_task=tasks.failing_step, child_task=tasks.step)
    children = _children(flow)
    worker = flow_worker()
    for _ in range(10):
        if not worker.run_once():
            break
        OxTask.objects.filter(pk=root_id, status=OxTask.Status.READY).update(
            run_after=None
        )
    assert OxTask.objects.get(pk=root_id).status == OxTask.Status.FAILED
    caplog.clear()

    ticks = 3
    with _parameter_cap(capped) as still_capped:
        for _ in range(ticks):
            workflows._reconcile()
        still_capped()

    cancelled = children.filter(status=OxTask.Status.DISCARDED).count()
    status = OxpullWorkflow.objects.get(pk=flow.id).status
    logged = _logged_failures(caplog)
    print(  # noqa: T201
        f"\n[cap-cancel] vendor={connection.vendor} capped={capped} "
        f"width={width} ticks={ticks} cancelled={cancelled} workflow={status} "
        f"logged={logged}"
    )
    assert (cancelled, status) == (width, OxpullWorkflow.Status.COMPLETE), (
        f"after {ticks} reconciler ticks {cancelled} of {width} doomed nodes are "
        f"cancelled and the workflow is {status}; logged {logged}"
    )
