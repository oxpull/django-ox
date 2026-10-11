"""
Blocked nodes stay out of the claim scan.

DEFER_BLOCKED_NODES used to leave a deferred node READY with run_after in the
year 3000 and its original enqueued_at. Neither ox_dequeue_idx nor
ox_dequeue_queue_idx holds run_after, so a claim walked READY rows in
(priority DESC, enqueued_at) order and rejected each deferred row with a row
filter. A blocked node is now WAITING, and status leads both indexes, so no
claim's key range reaches it. The steady state is ordinary: a root is
RUNNING, its cone was added before later work, and the later work sits
behind the cone.

Each test asserts the property the waiting status delivers:

  * a claim's cost does not grow with the deferred nodes queued ahead of it
    (all three databases, measured on the worker's real claim_one());
  * the shipped cost instrument counts the rows a plan filtered out
    (PostgreSQL; tests/test_workflows_cost.rows_examined does not);
  * on MySQL at REPEATABLE READ, a claim holds no row lock on a deferred
    node it scanned past.

The assertions are written against "blocked nodes" (an unsatisfied edge, not
RUNNING), not against run_after, so a fix that stops using run_after is
judged by the same tests.

Shortcut, stated once: the MySQL lock test holds the claim's own
`_ready_queryset().select_for_update(skip_locked=True).first()` open inside
atomic(). In production that transaction lasts for the scan plus the claim
UPDATE; holding it open makes the lock set observable deterministically
instead of by racing a scan.
"""

from __future__ import annotations

import contextlib
import threading

import pytest
from django.db import connection, connections, transaction

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows_cost import build, claim_candidates, postgres_only, rows_examined
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db(transaction=True)

mysql_only = pytest.mark.skipif(
    connection.vendor != "mysql", reason="InnoDB row locks are MySQL's"
)

SMALL = 50
LARGE = 4000
LOCK_SIZE = 400


def refresh_statistics() -> None:
    """
    Fresh planner statistics, as the shipped cost tests take them.

    MySQL's ANALYZE TABLE commits implicitly, which is why every test here
    is transaction=True: inside a rolled-back test transaction it would
    leak rows into the next test. SQLite is left as Django leaves it.
    """
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("ANALYZE django_ox_oxtask")
            cursor.execute("ANALYZE oxpull_oxpullworkflowedge")
        elif connection.vendor == "mysql":
            for table in ("django_ox_oxtask", "oxpull_oxpullworkflowedge"):
                cursor.execute(f"ANALYZE TABLE {table}")
                cursor.fetchall()


def _pg_task_rows_read() -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0) "
            "FROM pg_stat_xact_user_tables WHERE relname = 'django_ox_oxtask'"
        )
        row = cursor.fetchone()
    return int(row[0]) if row else 0


def _mysql_handler_reads() -> int:
    with connection.cursor() as cursor:
        cursor.execute("SHOW SESSION STATUS LIKE 'Handler_read%'")
        return sum(int(value) for _, value in cursor.fetchall())


def claim_cost(claim):
    """
    Run `claim` and return (its result, what it cost server-side).

    PostgreSQL: task-table heap tuples read (seq_tup_read + idx_tup_fetch)
    in this transaction, which counts rows a filter rejected. MySQL: the
    session's Handler_read_* delta, less what reading the counter costs.
    SQLite: VM instructions, from the progress handler. None of the three
    moves with machine load.
    """
    if connection.vendor == "postgresql":
        with transaction.atomic():
            before = _pg_task_rows_read()
            result = claim()
            return result, _pg_task_rows_read() - before
    if connection.vendor == "mysql":
        idle_first = _mysql_handler_reads()
        overhead = _mysql_handler_reads() - idle_first
        before = _mysql_handler_reads()
        result = claim()
        return result, _mysql_handler_reads() - before - overhead
    connection.ensure_connection()
    raw = connection.connection
    steps = 0

    def count() -> int:
        nonlocal steps
        steps += 1
        return 0

    raw.set_progress_handler(count, 1)
    try:
        result = claim()
    finally:
        raw.set_progress_handler(None, 1)
    return result, steps


def full_rows_examined(plan) -> int:
    """
    Rows every plan node touched: returned plus removed by a filter.

    PostgreSQL reports both per loop, so both are multiplied by loops.
    """
    if isinstance(plan, str):
        import json

        plan = json.loads(plan)
    root = plan[0]["Plan"] if isinstance(plan, list) else plan["Plan"]
    total = 0

    def walk(node: dict) -> None:
        nonlocal total
        loops = int(node.get("Actual Loops", 1))
        total += int(node.get("Actual Rows", 0)) * loops
        for removed in (
            "Rows Removed by Filter",
            "Rows Removed by Join Filter",
            "Rows Removed by Index Recheck",
        ):
            total += int(node.get(removed, 0)) * loops
        for child in node.get("Plans", []):
            walk(child)

    walk(root)
    return total


def blocked_nodes():
    """
    Task rows the gate holds back and no worker is running: the rows a
    claim has no reason to touch, however a release marks them.
    """
    return OxTask.objects.exclude(workflows.runnable_filter_q()).exclude(
        status=OxTask.Status.RUNNING
    )


def cone_behind_a_running_root(size: int, later: str | None, task=None) -> None:
    """
    The steady state: a root claimed by a real worker, `size` children
    waiting on it, and (if `later` is given) ordinary work enqueued after.
    """
    task = task or tasks.deferred_step
    build(size, task)
    claimed = OxpullWorker(backend_alias=task.backend, backoff_initial=0).claim_one()
    assert claimed is not None and claimed.args == ["root"], claimed
    if later is not None:
        task.enqueue(later)


# -- cost ----------------------------------------------------------------


def test_a_claim_behind_deferred_nodes_costs_no_more_as_they_pile_up():
    """
    Blocked nodes are outside the candidate set, so a claim behind 4,000 of
    them costs what a claim behind 50 does. Measured on the worker's real
    claim, which on PostgreSQL is the single-statement UPDATE ... SKIP LOCKED.
    """
    cone_behind_a_running_root(SMALL, "later-small")
    refresh_statistics()
    assert blocked_nodes().count() == SMALL
    assert claim_candidates(gated=True).first().args == ["later-small"]
    small_claim, small = claim_cost(deferring_worker().claim_one)
    assert small_claim.args == ["later-small"]

    # A second workflow in flight, its root claimed the same way, so the
    # blocked nodes ahead of the next ordinary task are now LARGE.
    cone_behind_a_running_root(LARGE - SMALL, "later-large")
    refresh_statistics()
    assert blocked_nodes().count() == LARGE
    assert (
        OxTask.objects.filter(args=["root"], status=OxTask.Status.RUNNING).count() == 2
    )
    assert claim_candidates(gated=True).first().args == ["later-large"]
    large_claim, large = claim_cost(deferring_worker().claim_one)
    assert large_claim.args == ["later-large"]

    print(  # noqa: T201
        f"\n  [s2 cost] {connection.vendor}: claim behind {SMALL} deferred={small}, "
        f"behind {LARGE} deferred={large} ({large / max(small, 1):.1f}x)"
    )
    assert large <= 2 * small, (
        f"a claim behind {LARGE} deferred nodes cost {large} against {small} "
        f"behind {SMALL}; deferred nodes are still in the claim's scan"
    )


@postgres_only
@pytest.mark.xfail(
    connection.vendor == "postgresql",
    strict=True,
    reason=(
        "The shared cost helper rows_examined leaves out rows a plan read and "
        "then filtered out, so it under-reports what a claim touched"
    ),
)
@pytest.mark.parametrize("root", ["claimed", "as-shipped"])
def test_the_cost_instrument_counts_rows_the_plan_filtered_out(root):
    """
    tests/test_workflows_cost.rows_examined sums Actual Rows only, so a
    plan that reads a row and rejects it with a filter reports nothing for
    it. Both metrics are taken from one and the same plan. "as-shipped" is
    the deferred-flatness test's own fixture (root never claimed).
    """
    if root == "claimed":
        cone_behind_a_running_root(LOCK_SIZE, "later")
    else:
        build(LOCK_SIZE, tasks.deferred_step)
    # A waiting node is filtered by nothing: the claim's index range never
    # reaches it. Put back to READY, the blocked nodes are rows the plan reads
    # and the gate rejects, which is what the instrument has to count.
    OxTask.objects.filter(status=OxTask.Status.WAITING).update(
        status=OxTask.Status.READY
    )
    refresh_statistics()
    with transaction.atomic():
        plan = claim_candidates(gated=True).explain(analyze=True, format="JSON")

    class SamePlan:
        def explain(self, **kwargs):
            return plan

    shipped = rows_examined(SamePlan())
    touched = full_rows_examined(plan)
    if touched == shipped:
        pytest.fail(
            "the claim's plan removed no rows by a filter, so it cannot show "
            f"whether the instrument counts them (rows_examined={shipped}, "
            f"with removed rows={touched})"
        )
    print(  # noqa: T201
        f"\n  [s2 instrument] root={root}: rows_examined={shipped}, "
        f"with rows removed by filter={touched}"
    )
    assert shipped == touched, (
        f"the shipped instrument reported {shipped} rows on a plan that "
        f"touched {touched}"
    )


# -- MySQL locks -----------------------------------------------------------


@contextlib.contextmanager
def holding_a_claim_scan(alias: str, level: str):
    """
    Another connection, at `level`, runs the claim's own candidate select
    inside atomic() and holds the transaction until the block exits.
    Yields {"thread": performance_schema thread id, "candidate": OxTask|None}.
    """
    ready = threading.Event()
    release = threading.Event()
    info: dict = {}

    def hold() -> None:
        try:
            session = connections["default"]
            with session.cursor() as cursor:
                cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
                cursor.execute("SELECT PS_CURRENT_THREAD_ID()")
                info["thread"] = cursor.fetchone()[0]
            worker = OxpullWorker(backend_alias=alias, backoff_initial=0)
            with transaction.atomic():
                info["candidate"] = (
                    worker._ready_queryset().select_for_update(skip_locked=True).first()
                )
                ready.set()
                release.wait(60)
        except BaseException as exc:
            info["error"] = exc
            ready.set()
        finally:
            connections.close_all()

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert ready.wait(60), "the holder never ran its scan"
        assert "error" not in info, info.get("error")
        yield info
    finally:
        release.set()
        thread.join(60)
        assert not thread.is_alive()


def record_locks(thread_id: int) -> list[tuple[str, str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT INDEX_NAME, LOCK_MODE, LOCK_DATA FROM performance_schema.data_locks"
            " WHERE THREAD_ID = %s AND OBJECT_NAME = 'django_ox_oxtask'"
            " AND LOCK_TYPE = 'RECORD'",
            [thread_id],
        )
        return list(cursor.fetchall())


@mysql_only
def test_a_claim_holds_no_row_lock_on_the_deferred_nodes_it_scanned_past():
    """
    SKIP LOCKED claims one row; the deferred nodes it rejected on the way
    are not its business. At REPEATABLE READ InnoDB keeps a lock on every
    row a locking read scanned, so each deferred node ahead of the claimed
    row is one more lock the claim holds until it commits, and any write to
    that node (a release, a cancellation, an operator's discard) waits.

    Only locks on live blocked nodes count. The supremum pseudo-record and
    delete-marked rows a previous test's flush left for purge are not
    blocked nodes, so they do not fail this test.

    Not asserted here, and why:
      * READ COMMITTED, deferred: InnoDB already releases the rejected rows.
      * The gated backend (no deferral): its rows stay locked at both
        levels, but no fix to deferral can change that.
      * An enqueue waiting on the scan's gap locks: a priority-5 enqueue
        waits behind a REPEATABLE READ claim with no blocked nodes at all,
        so a deterministic test cannot tell this hazard from InnoDB.
    """
    cone_behind_a_running_root(LOCK_SIZE, "later")
    refresh_statistics()
    blocked = {pk.hex for pk in blocked_nodes().values_list("pk", flat=True)}
    assert len(blocked) == LOCK_SIZE

    with holding_a_claim_scan("flows_deferred", "REPEATABLE READ") as info:
        candidate = info["candidate"]
        assert candidate is not None and candidate.args == ["later"]
        locks = record_locks(info["thread"])

    on_blocked = [
        lock for lock in locks if any(f"'{pk}'" in (lock[2] or "") for pk in blocked)
    ]
    shape: dict[tuple[str, str], int] = {}
    for index, mode, _ in on_blocked:
        shape[(index, mode)] = shape.get((index, mode), 0) + 1
    print(  # noqa: T201
        f"\n  [s2 locks] flows_deferred at REPEATABLE READ: {len(locks)} record "
        f"locks, {len(on_blocked)} on the {LOCK_SIZE} blocked nodes: {shape}"
    )
    assert on_blocked == [], (
        f"the claim holds {len(on_blocked)} record locks on deferred nodes it "
        f"did not take ({shape})"
    )
