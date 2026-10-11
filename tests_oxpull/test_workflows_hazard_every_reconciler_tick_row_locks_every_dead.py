"""
Every reconciler tick row-locks every dead node of a sealed
workflow that still has pending work.

The state: a SEALED workflow with one FAILED node, the cone below it
already DISCARDED by an earlier tick, and one unrelated node still RUNNING
(a long or rate-limited branch), so the workflow cannot complete. Nothing
in it is cancellable any more. Every tick from then on is idle for this
workflow, and every such tick should cost what an idle tick costs: no
write, no row lock, nothing another connection has to wait for.

What the shipped reconciler does instead, per tick, for as long as the
workflow stays SEALED: _has_dead_node() answers only "is any node dead",
which stays true forever, so _cancel_one_level() opens a transaction,
takes the workflow row with a no-op UPDATE, and runs SELECT ... FOR UPDATE
over every FAILED, LOST and DISCARDED node row, including every row its
own earlier cancellation discarded. Only then does it find nothing
cancellable and commit.

Every test asserts the idle-tick property, whatever the fix:

  * an idle tick issues no write statement (all three databases);
  * an idle tick issues no locking read (PostgreSQL, MySQL);
  * an idle tick leaves the dead node tuples' xmax untouched (PostgreSQL,
    where a row lock is a tuple-header write);
  * an idle tick holds a bounded number of InnoDB record locks on the task
    table, independent of how many nodes are dead (MySQL);
  * an operator write on a dead node, from another connection, is not
    blocked at any point inside an idle tick (PostgreSQL, MySQL): the real
    ox_prune over the old DISCARDED cone, and actions.retry on the FAILED
    node. Both run inside a transaction that is rolled back, so each probe
    leaves the state exactly as it found it.

A fix that keeps the lock for a tick that does cancel something passes all
of them. So does one that never opens a transaction on an idle tick: with
no transaction, no lock outlives a statement, and the mid-tick probes have
nowhere to fire.

Real paths: nodes are enqueued through Workflow.add(); the RUNNING node is
held by a real OxpullWorker.claim_one() lease; the root fails through a
real worker's run_once() until its attempts are exhausted; the cone is
cancelled by a reconciler pass; ox_prune is the django-ox management
command and actions.retry is the django-ox action.

Shortcuts, each named where it is taken:

  * The reconciler pass is workflows._reconcile() called inline, as every
    workflow test calls it. The scheduled task runs the same function.
  * The prune probe backdates the cone's finished_at by eight days. It
    stands in for the RUNNING branch keeping the workflow SEALED for over a
    week, which is what makes the cone old enough for default ox_prune.
  * The mid-tick probes fire from a connection execute wrapper, after a
    statement returns and before the next one runs, while the reconciler's
    connection is inside a transaction. That pause lengthens the tick; it
    does not add a lock. The lock timeout (700 ms on PostgreSQL, 1 s on
    MySQL) is what turns a wait into a failure the test can see. Without
    one, production waits for the tick to commit instead.
"""

import io
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.db import OperationalError, connections, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import flow_worker, reconcile, stored

# transaction=True throughout: each reconciler pass must be its own real
# transaction, as it is in production. Inside the test-case transaction a
# second FOR UPDATE by the same transaction id writes nothing new to the
# tuple, and the PostgreSQL xmax test would pass for the wrong reason.
pytestmark = pytest.mark.django_db(transaction=True)

# Two sizes by default, so the logs show the cost growing with the dead set.
# A measurement run sets OX_LOCK_WIDTHS=20000.
WIDTHS = [int(w) for w in os.environ.get("OX_LOCK_WIDTHS", "40,400").split(",")]

# The most InnoDB record locks on the task table an idle tick may hold at
# any instant. A constant, independent of the dead set's size.
IDLE_TASK_ROW_LOCK_BOUND = 4

# How many statements inside the tick's transactions get a probe after them.
PROBE_LIMIT = 6


def vendor():
    return connections["default"].vendor


needs_row_locks = pytest.mark.skipif(
    "vendor() == 'sqlite'",
    reason="SQLite has no row locks; the no-write test covers its database lock",
)


# -- the state ---------------------------------------------------------


def sealed_with_a_dead_cone(name, width):
    """
    root (fails) -> `width` children, plus an unrelated node `long` held
    RUNNING by a real worker's lease. Sealed. One reconciler pass then
    discards the whole cone, and the workflow stays SEALED because `long`
    is still running.

    Returns (flow, node ids, the first tick's captured queries).
    """
    flow = workflows.create(name)
    root = flow.add("root", tasks.failing_step, "root")
    long = flow.add("long", tasks.step.using(priority=10), "long")
    for i in range(width):
        flow.add(f"c{i:05d}", tasks.step, f"c{i:05d}", depends_on="root")
    flow.seal()

    # `long` outranks the root, so the first claim takes it and holds it.
    holder = flow_worker()
    held = holder.claim_one()
    assert held is not None and str(held.id) == str(long.id), held
    assert OxTask.objects.get(id=long.id).status == OxTask.Status.RUNNING

    # The root exhausts its three attempts in a real worker.
    worker = flow_worker()
    for _ in range(3):
        worker.run_once()
    root_row = OxTask.objects.get(id=root.id)
    assert root_row.status == OxTask.Status.FAILED, root_row.status
    assert root_row.attempts == root_row.max_attempts == 3

    with CaptureQueriesContext(connections["default"]) as first_tick:
        assert reconcile() == 0
    ids = flow.node_ids()
    assert discarded(ids) == width, "the first tick did not cancel the cone"
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    return flow, ids, first_tick.captured_queries


def discarded(ids):
    return OxTask.objects.filter(
        id__in=ids.values(), status=OxTask.Status.DISCARDED
    ).count()


def dead_ids(ids):
    return list(
        OxTask.objects.filter(
            id__in=ids.values(), status__in=workflows.DEAD_STATUSES
        ).values_list("pk", flat=True)
    )


def assert_the_tick_was_idle(flow, ids, width):
    """Preconditions: the tick under test cancelled and completed nothing."""
    assert discarded(ids) == width
    assert OxTask.objects.get(id=ids["root"]).status == OxTask.Status.FAILED
    assert OxTask.objects.get(id=ids["long"]).status == OxTask.Status.RUNNING
    assert stored(flow).status == OxpullWorkflow.Status.SEALED


def locking_reads(queries):
    return [q["sql"] for q in queries if "FOR UPDATE" in q["sql"].upper()]


def writes(queries):
    return [
        q["sql"]
        for q in queries
        if q["sql"].lstrip().split(None, 1)[0].upper() in {"UPDATE", "INSERT", "DELETE"}
    ]


def short(sql, n=160):
    return " ".join(sql.split())[:n]


# -- probes from another connection ------------------------------------


class _RolledBack(Exception):
    """Raised to roll a probe's transaction back once it has done its write."""


def on_another_connection(action, *, lock_wait=True):
    """
    Run `action` on its own thread and connection, inside a transaction
    that is always rolled back. With lock_wait, the session's lock wait is
    shortened first, so a write blocked by the tick fails instead of
    waiting for it.
    """
    outcome = {}

    def run():
        try:
            conn = connections["default"]
            if lock_wait:
                with conn.cursor() as cursor:
                    if conn.vendor == "postgresql":
                        cursor.execute("SET lock_timeout = '700ms'")
                    elif conn.vendor == "mysql":
                        cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
            started = time.monotonic()
            try:
                with transaction.atomic():
                    outcome["result"] = action()
                    raise _RolledBack
            except _RolledBack:
                outcome["ok"] = True
            except OperationalError as exc:
                outcome["ok"] = False
                cause = exc.__cause__ or exc
                outcome["error"] = f"{type(cause).__name__}: {short(str(exc), 90)}"
            outcome["seconds"] = round(time.monotonic() - started, 3)
        except BaseException as exc:  # a crash is reported, not swallowed
            outcome["ok"] = False
            outcome["crash"] = repr(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=60)
    assert not thread.is_alive(), "the probe thread did not finish"
    assert "crash" not in outcome, outcome
    return outcome


@contextmanager
def after_each_statement_inside_a_transaction(callback, limit=PROBE_LIMIT):
    """
    Call `callback` after each statement the default connection runs while
    it is inside an atomic block, up to `limit` times, and collect what it
    returns. The statement has returned and its locks are held; the next
    statement has not started.
    """
    conn = connections["default"]
    fired = []
    busy = []

    def wrapper(execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        if not busy and conn.in_atomic_block and len(fired) < limit:
            busy.append(True)
            try:
                fired.append({"after": short(sql, 70), **callback()})
            finally:
                busy.clear()
        return result

    with conn.execute_wrapper(wrapper):
        yield fired


def prune_old_rows():
    """The real ox_prune, default arguments. Returns how many task rows it deleted."""
    out = io.StringIO()
    call_command("ox_prune", stdout=out)
    match = re.search(r"Deleted (\d+) \S+ task row", out.getvalue())
    assert match, out.getvalue()
    return int(match.group(1))


# -- an idle tick writes nothing ---------------------------------------


@pytest.mark.parametrize("width", WIDTHS[:1])
def test_an_idle_tick_over_a_workflow_with_dead_nodes_writes_nothing(width):
    """
    The property test_an_idle_sealed_workflow_costs_no_write_on_a_reconciler_tick
    pins for a workflow with no dead node, extended to one whose dead
    nodes are all already dealt with. A write here is a write transaction
    per such workflow per tick, for as long as it stays SEALED; on SQLite
    that is the database write lock, serialised against every worker.
    """
    flow, ids, _ = sealed_with_a_dead_cone("idle-writes", width)

    with CaptureQueriesContext(connections["default"]) as tick:
        assert reconcile() == 0
    assert_the_tick_was_idle(flow, ids, width)

    written = writes(tick.captured_queries)
    assert written == [], (
        f"an idle tick over a SEALED workflow with {width + 1} dead nodes and "
        f"nothing cancellable issued {len(written)} write(s), every tick: "
        + " | ".join(short(sql) for sql in written)
    )


# -- an idle tick locks nothing ----------------------------------------


@needs_row_locks
@pytest.mark.parametrize("width", WIDTHS)
def test_an_idle_tick_issues_no_locking_read(width):
    flow, ids, first_tick = sealed_with_a_dead_cone("idle-locking-read", width)

    started = time.monotonic()
    with CaptureQueriesContext(connections["default"]) as tick:
        assert reconcile() == 0
    elapsed_ms = round((time.monotonic() - started) * 1000)
    assert_the_tick_was_idle(flow, ids, width)

    locking = locking_reads(tick.captured_queries)
    assert locking == [], (
        f"an idle tick ({elapsed_ms} ms) issued {len(locking)} locking read(s) "
        f"over a workflow with {width + 1} dead nodes and nothing cancellable "
        f"(the cancelling tick before it issued {len(locking_reads(first_tick))}): "
        + " | ".join(short(sql, 400) for sql in locking)
    )


def _xmax(pks):
    table = OxTask._meta.db_table
    with connections["default"].cursor() as cursor:
        cursor.execute(
            f'SELECT id::text, xmax::text FROM "{table}" WHERE id = ANY(%s)',  # noqa: S608
            [list(pks)],
        )
        return dict(cursor.fetchall())


def _explain_wal(sql):
    """
    EXPLAIN (ANALYZE, WAL) of one captured locking read, in a transaction
    that is rolled back. Diagnostic only: it goes into the failure message.
    """
    try:
        with transaction.atomic():
            with connections["default"].cursor() as cursor:
                cursor.execute("EXPLAIN (ANALYZE, WAL, BUFFERS, FORMAT JSON) " + sql)
                plan = cursor.fetchone()[0]
            raise _RolledBack
    except _RolledBack:
        pass
    except Exception as exc:  # the message must not hide the assertion
        return f"EXPLAIN failed: {exc!r}"
    top = plan[0]["Plan"] if isinstance(plan, list) else plan
    return (
        f"{top['Node Type']} rows={top.get('Actual Rows')} "
        f"WAL records={top.get('WAL Records')} bytes={top.get('WAL Bytes')} "
        f"FPI={top.get('WAL FPI')} shared dirtied={top.get('Shared Dirtied Blocks')} "
        f"time={round(plan[0].get('Execution Time', 0), 1)} ms"
    )


@pytest.mark.skipif(
    "vendor() != 'postgresql'",
    reason="xmax is PostgreSQL's tuple header; MySQL is measured by data_locks",
)
@pytest.mark.parametrize("width", WIDTHS)
def test_an_idle_tick_leaves_the_dead_node_tuples_unwritten_on_postgresql(width):
    """
    On PostgreSQL a row lock is written into the tuple header: xmax takes
    the locking transaction's id, the page is dirtied and the lock is
    WAL-logged. An idle tick should leave every dead node's xmax as it was.
    """
    flow, ids, _ = sealed_with_a_dead_cone("idle-xmax", width)
    dead = dead_ids(ids)
    assert len(dead) == width + 1

    before = _xmax(dead)
    started = time.monotonic()
    with CaptureQueriesContext(connections["default"]) as tick:
        assert reconcile() == 0
    elapsed_ms = round((time.monotonic() - started) * 1000)
    after = _xmax(dead)
    assert_the_tick_was_idle(flow, ids, width)

    relocked = [pk for pk, xmax in before.items() if after[pk] != xmax]
    if relocked:
        wal = [_explain_wal(sql) for sql in locking_reads(tick.captured_queries)]
    assert relocked == [], (
        f"an idle tick ({elapsed_ms} ms) wrote a new xmax into {len(relocked)} of "
        f"{len(dead)} dead node tuples, e.g. {before[str(relocked[0])]} -> "
        f"{after[str(relocked[0])]}. The tick's locking read, re-run under "
        f"EXPLAIN (ANALYZE, WAL): {wal}"
    )


def _data_locks(thread_id):
    table = OxTask._meta.db_table
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "SELECT OBJECT_NAME, INDEX_NAME, LOCK_TYPE, LOCK_MODE, COUNT(*)"
            " FROM performance_schema.data_locks WHERE THREAD_ID = %s"
            " GROUP BY OBJECT_NAME, INDEX_NAME, LOCK_TYPE, LOCK_MODE",
            [thread_id],
        )
        rows = cursor.fetchall()
    return {
        "task_row_locks": sum(
            n for obj, _i, kind, _m, n in rows if obj == table and kind == "RECORD"
        ),
        "locks": [
            f"{obj}.{idx} {kind} {mode} x{n}" for obj, idx, kind, mode, n in rows
        ],
    }


@pytest.mark.skipif(
    "vendor() != 'mysql'", reason="performance_schema.data_locks is MySQL's"
)
@pytest.mark.parametrize("width", WIDTHS)
def test_an_idle_tick_holds_a_bounded_number_of_row_locks_on_mysql(width):
    """
    Read from another connection while the tick is inside its transaction:
    the InnoDB record locks the reconciler's thread holds on the task table
    must not grow with the number of dead nodes.
    """
    flow, ids, _ = sealed_with_a_dead_cone("idle-data-locks", width)
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT PS_CURRENT_THREAD_ID(), @@transaction_isolation")
        thread_id, isolation = cursor.fetchone()

    with after_each_statement_inside_a_transaction(
        lambda: on_another_connection(lambda: _data_locks(thread_id), lock_wait=False)[
            "result"
        ]
    ) as fired:
        assert reconcile() == 0
    assert_the_tick_was_idle(flow, ids, width)

    peak = max((f["task_row_locks"] for f in fired), default=0)
    assert peak <= IDLE_TASK_ROW_LOCK_BOUND, (
        f"{isolation}: mid idle tick the reconciler held {peak} record locks on "
        f"{OxTask._meta.db_table} for a workflow with {width + 1} dead nodes "
        f"(bound {IDLE_TASK_ROW_LOCK_BOUND}). Per statement: "
        + " || ".join(f"after [{f['after']}]: {f['locks']}" for f in fired)
    )


# -- an idle tick blocks no operator write -----------------------------


def _prune_probe(ids, width):
    # Shortcut: the cone was discarded eight days ago, which is what a
    # RUNNING branch holding the workflow SEALED for over a week produces.
    cone = [pk for name, pk in ids.items() if name.startswith("c")]
    OxTask.objects.filter(id__in=cone, status=OxTask.Status.DISCARDED).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    return prune_old_rows, width


def _retry_probe(ids, width):
    root = ids["root"]
    return (lambda: int(actions.retry(root))), 1


PROBES = {"ox_prune": _prune_probe, "actions.retry": _retry_probe}


@needs_row_locks
@pytest.mark.parametrize("probe", list(PROBES))
@pytest.mark.parametrize("width", WIDTHS[:1])
def test_an_idle_tick_blocks_no_operator_write_on_a_dead_node(probe, width):
    """
    From another connection, at every statement boundary inside the idle
    tick's transactions: the operator write succeeds. ox_prune deletes the
    old DISCARDED cone; actions.retry requeues the FAILED root. Both are
    rolled back, so every probe sees the same state.
    """
    flow, ids, _ = sealed_with_a_dead_cone(f"idle-blocks-{probe}", width)
    action, expected = PROBES[probe](ids, width)

    # Control: with no tick in flight the probe succeeds and does its whole
    # write. Without this a probe that touched nothing would pass below.
    control = on_another_connection(action)
    assert control.get("ok") and control["result"] == expected, control

    with after_each_statement_inside_a_transaction(
        lambda: on_another_connection(action)
    ) as fired:
        assert reconcile() == 0
    assert_the_tick_was_idle(flow, ids, width)

    blocked = [f for f in fired if not f.get("ok")]
    assert blocked == [], (
        f"{probe} from another connection was blocked at {len(blocked)} of "
        f"{len(fired)} statement boundaries inside an idle tick over "
        f"{width + 1} dead nodes: "
        + " || ".join(
            f"after [{f['after']}]: "
            + (
                f"blocked {f['seconds']} s, {f['error']}"
                if not f.get("ok")
                else f"ok, result {f['result']}"
            )
            for f in fired
        )
    )
