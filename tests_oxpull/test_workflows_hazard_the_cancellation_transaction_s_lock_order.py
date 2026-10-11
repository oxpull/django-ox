"""
The cancellation transaction's lock order.

workflows._cancel_one_level takes its row locks in three steps inside one
transaction: the workflow row (the no-op SEALED UPDATE), then every dead
node row of the workflow in primary-key order (SELECT ... FOR UPDATE), then
the READY rows it discards (a second statement, UPDATE ... WHERE pk IN).
The docstring's deadlock guards cover two reconcilers taking the same
locks in the same order. They do not cover a second writer that takes the
same rows in a different order:

  * django_ox.actions.discard_many, the function behind the admin's
    "Discard selected tasks", is one UPDATE over the selected ids. Given a
    failed workflow's rows it locks a READY child before the FAILED root
    and then waits on the root, while the cancel pass holds the root and
    waits on that child.

  * ox_prune deletes old DISCARDED rows in batches, and an idle reconciler
    tick on a sealed workflow with a dead node locks every one of those
    DISCARDED rows with its FOR UPDATE, in its own order.

Each test asserts the correct behaviour, which is that neither party is
killed by the database's deadlock detector (PostgreSQL 40P01, InnoDB 1213),
so each fails wherever the cycle closes. Every statement on both sides is
the production statement: the real worker fails the root, the real
reconciler entry point (_reconcile, what the scheduled task runs) cancels,
and the operator side is the public actions.discard_many and the real
ox_prune management command. The only interventions are a pause (Django's
public connection.execute_wrapper on the reconciler's connection) so the
operator's statement lands inside the cancel transaction, and in the
prune test a backdated finished_at standing in for eight days passing.

SQLite is skipped, not passed: it has no row locks, and its writers are
serialised, so no cycle can form.
"""

from __future__ import annotations

import io
import logging
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest
from django.core.management import call_command
from django.db import DatabaseError, connections, transaction
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowNode
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_workflows import drain, flow_worker, reconcile, stored

pytestmark = pytest.mark.django_db(transaction=True)

TASK_TABLE = OxTask._meta.db_table
WORKFLOW_LOGGER = workflows.__name__

# The width of the prune race.
PRUNE_WIDTH = 6000

VENDOR = connections[oxpull_db_alias()].vendor


@pytest.fixture(autouse=True)
def _row_locks_only():
    if connections[oxpull_db_alias()].vendor == "sqlite":
        pytest.skip(
            "SQLite has no row locks and serialises writers, so no lock cycle "
            "between the cancel pass and another writer can form"
        )


# -- harness -----------------------------------------------------------


def deadlock_code(exc: BaseException | None) -> str | int | None:
    """40P01 or 1213 if `exc` or anything in its cause chain is a deadlock."""
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if getattr(exc, "sqlstate", None) == "40P01":
            return "40P01"
        args = getattr(exc, "args", ())
        if args and args[0] == 1213:
            return 1213
        exc = exc.__cause__ or exc.__context__
    return None


def describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}(deadlock={deadlock_code(exc)!r}): {exc}"


def on_own_connection(step: Callable[[], Any], timeout: float = 30) -> Any:
    """Run `step` on its own thread and so its own connection, and return its value."""
    out: list[Any] = []
    err: list[BaseException] = []

    def run() -> None:
        try:
            out.append(step())
        except BaseException as exc:
            err.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=timeout)
    assert not thread.is_alive(), "a probe hung"
    if err:
        raise err[0]
    return out[0]


def session_id() -> int:
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SELECT pg_backend_pid()")
        else:
            cursor.execute("SELECT CONNECTION_ID()")
        return int(cursor.fetchone()[0])


def is_waiting_on_a_lock(session: int) -> bool:
    """Is that session blocked on a lock now? Read on this thread's connection."""
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                [session],
            )
            row = cursor.fetchone()
            return bool(row) and row[0] == "Lock"
        cursor.execute(
            "SELECT trx_state FROM information_schema.INNODB_TRX "
            "WHERE trx_mysql_thread_id = %s",
            [session],
        )
        row = cursor.fetchone()
        return bool(row) and row[0] == "LOCK WAIT"


def row_is_locked(pk: Any) -> bool:
    """A NOWAIT probe: is another transaction holding this task row's lock?"""
    alias = oxpull_db_alias()
    try:
        with transaction.atomic(using=alias):
            list(
                OxTask.objects.using(alias)
                .select_for_update(nowait=True)
                .filter(pk=pk)
                .values_list("pk", flat=True)
            )
    except DatabaseError:
        return True
    return False


class AfterTheCancelLock:
    """
    Run `step` once, on the reconciler's thread, right after the cancel
    pass's SELECT ... FOR UPDATE on the task table has executed: the moment
    it holds the workflow row and the dead node rows and has not yet read
    or written a READY node.
    """

    def __init__(self, step: Callable[[], None]) -> None:
        self.step = step
        self.fired = False

    def __call__(self, execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        if not self.fired and "FOR UPDATE" in sql.upper() and TASK_TABLE in sql:
            self.fired = True
            self.step()
        return result


def pause_cancel_passes(monkeypatch, hook: AfterTheCancelLock) -> None:
    original = workflows._cancel_one_level

    def cancel_one_level(workflow_pk, db_alias):
        with connections[db_alias].execute_wrapper(hook):
            return original(workflow_pk, db_alias)

    monkeypatch.setattr(workflows, "_cancel_one_level", cancel_one_level)


def reconciler_failures(caplog) -> list[logging.LogRecord]:
    """What _reconcile logged and swallowed: its per-workflow exception handler."""
    return [
        record
        for record in caplog.records
        if record.name == WORKFLOW_LOGGER and record.exc_info
    ]


def node_statuses(flow) -> dict[str, str]:
    ids = flow.node_ids()
    by_id = dict(OxTask.objects.filter(id__in=ids.values()).values_list("id", "status"))
    return {name: by_id.get(pk, "<pruned>") for name, pk in ids.items()}


# -- (a) an operator's discard_many against the cancel pass ---------------


@pytest.mark.xfail(
    VENDOR in ("postgresql", "mysql"),
    strict=True,
    reason=(
        "the cancel pass and an operator's discard_many lock the same rows in "
        "opposite order, and the database kills one of them as a deadlock"
    ),
)
def test_an_operator_discarding_a_failed_workflow_does_not_deadlock_its_cancel_pass(
    monkeypatch, caplog
):
    """
    A sealed workflow whose root failed, with twelve READY children waiting
    on it. The reconciler's cancel pass locks the FAILED root. An operator
    who is cleaning up the same failed workflow selects its rows and runs
    discard_many (the admin's "Discard selected tasks") in that instant.
    Both must finish: the reconciler cancels the children or finds them
    already discarded, and the operator's discard of the root lands.
    """
    caplog.set_level(logging.INFO, logger=WORKFLOW_LOGGER)
    alias = oxpull_db_alias()

    flow = workflows.create("discard-many")
    flow.add("root", tasks.failing_step, "root")
    root_pk = flow.node_ids()["root"]
    added = 0
    # The cycle needs a child that sorts below the root: the discard's
    # UPDATE locks it before reaching the root. Twelve children, and more
    # only in the rare case that none of the twelve sorts below.
    while added < 12 or not any(
        pk < root_pk for name, pk in flow.node_ids().items() if name != "root"
    ):
        flow.add(f"c{added:02d}", tasks.step, f"c{added:02d}", depends_on="root")
        added += 1
    flow.seal()

    drain(flow_worker())
    before = node_statuses(flow)
    assert before["root"] == OxTask.Status.FAILED, before
    assert {s for n, s in before.items() if n != "root"} == {OxTask.Status.READY}

    ids = flow.node_ids()
    root_id = ids["root"]
    low = sorted(pk for name, pk in ids.items() if name != "root" and pk < root_id)
    selection = list(ids.values())

    operator: dict[str, Any] = {}
    operator_ready = threading.Event()

    def discard_many() -> None:
        try:
            operator["session"] = session_id()
            operator_ready.set()
            operator["result"] = actions.discard_many(selection)
        except BaseException as exc:
            operator["error"] = exc
        finally:
            operator_ready.set()
            connections.close_all()

    operator_thread = threading.Thread(target=discard_many)
    seen: dict[str, Any] = {}

    def operator_runs_discard_many() -> None:
        operator_thread.start()
        assert operator_ready.wait(10)
        deadline = time.monotonic() + 5
        waiting = False
        while time.monotonic() < deadline and operator_thread.is_alive():
            if "session" in operator and on_own_connection(
                lambda: is_waiting_on_a_lock(operator["session"])
            ):
                waiting = True
                break
            time.sleep(0.01)
        seen["operator_waiting"] = waiting
        seen["operator_finished_before_resume"] = not operator_thread.is_alive()
        seen["low_children_locked_by_operator"] = on_own_connection(
            lambda: sum(row_is_locked(pk) for pk in low)
        )

    hook = AfterTheCancelLock(operator_runs_discard_many)
    pause_cancel_passes(monkeypatch, hook)

    reconcile_error: BaseException | None = None
    try:
        reconcile()
    except BaseException as exc:  # _reconcile swallows per-workflow errors
        reconcile_error = exc
    if operator_thread.is_alive() or operator_thread.ident is not None:
        operator_thread.join(timeout=60)
    assert not operator_thread.is_alive(), "the operator's discard_many never returned"

    assert hook.fired, (
        "the cancel pass never issued its FOR UPDATE, so the operator's "
        "discard never overlapped it and this test proved nothing"
    )

    failures = [
        f"reconciler: {record.getMessage()} -> {describe(record.exc_info[1])}"
        for record in reconciler_failures(caplog)
    ]
    if reconcile_error is not None:
        failures.append(f"reconciler raised: {describe(reconcile_error)}")
    if "error" in operator:
        failures.append(f"operator discard_many raised: {describe(operator['error'])}")
    after = node_statuses(flow)
    assert not failures, (
        f"a lock cycle between the cancel pass and discard_many: {failures}; "
        f"db={connections[alias].vendor}, children below root={len(low)} of "
        f"{len(ids) - 1}, operator waiting on a lock when the reconciler "
        f"resumed={seen.get('operator_waiting')}, low children the operator "
        f"held={seen.get('low_children_locked_by_operator')}; after: "
        f"root={after['root']}, children="
        f"{sorted({s for n, s in after.items() if n != 'root'})}, "
        f"workflow={stored(flow).status}, discard_many returned "
        f"{operator.get('result')}"
    )
    # Both intents landed.
    assert after["root"] == OxTask.Status.DISCARDED, after
    assert {s for n, s in after.items() if n != "root"} == {OxTask.Status.DISCARDED}


# -- (b) ox_prune against idle reconciler ticks ----------------------------


def test_ox_prune_does_not_deadlock_idle_reconciler_ticks(caplog):
    """
    A sealed workflow that cannot complete yet: its root failed, the
    reconciler has already cancelled the six thousand children, and one
    independent node is still RUNNING. Every reconciler tick on it locks
    the root and all six thousand DISCARDED rows and then finds nothing to
    do. A week later ox_prune deletes those DISCARDED rows in batches while
    the reconciler keeps ticking. Neither may be killed as a deadlock
    victim: the reconciler that loses one logs the workflow and skips it
    for that tick, and the prune run that loses one dies part-way and
    leaves the rest of the rows.
    """
    caplog.set_level(logging.INFO, logger=WORKFLOW_LOGGER)
    alias = oxpull_db_alias()

    flow = workflows.create("prune")
    started = time.monotonic()
    # The public add(), inside one caller's transaction so six thousand
    # nodes commit once.
    with transaction.atomic(using=alias):
        flow.add("holder", tasks.step.using(priority=10), "holder")
        flow.add("root", tasks.failing_step, "root")
        for i in range(PRUNE_WIDTH):
            flow.add(f"c{i:05d}", tasks.step, f"c{i:05d}", depends_on="root")
    flow.seal()
    build_seconds = time.monotonic() - started

    # The holder: a real claim, RUNNING under a live lease, so the
    # workflow stays SEALED however many ticks run.
    holder = flow_worker().claim_one()
    ids = flow.node_ids()
    assert holder is not None and holder.id == ids["holder"]
    drain(flow_worker(), limit=10)
    assert OxTask.objects.get(id=ids["root"]).status == OxTask.Status.FAILED

    reconcile()
    nodes = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values("task_id")
    discarded = OxTask.objects.filter(id__in=nodes, status=OxTask.Status.DISCARDED)
    assert discarded.count() == PRUNE_WIDTH
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    assert not reconciler_failures(caplog), "the setup tick itself failed"

    # Eight days pass.
    OxTask.objects.filter(
        id__in=nodes,
        status__in=[OxTask.Status.DISCARDED, OxTask.Status.FAILED],
    ).update(finished_at=timezone.now() - timedelta(days=8))

    stop = threading.Event()
    first_tick = threading.Event()
    # Each tick as the span it occupied, not the instant it ended: a tick
    # that began before the prune and was still running when it finished is
    # the deepest overlap of the two there is, and its end is outside the
    # prune's window.
    ticks: list[tuple[float, float]] = []
    tick_errors: list[BaseException] = []
    prune_errors: list[BaseException] = []
    prune_out = io.StringIO()
    prune_window: dict[str, float] = {}

    def reconciler() -> None:
        try:
            while not stop.is_set():
                started = time.monotonic()
                reconcile()
                ticks.append((started, time.monotonic()))
                first_tick.set()
        except BaseException as exc:
            tick_errors.append(exc)
        finally:
            first_tick.set()
            connections.close_all()

    def prune() -> None:
        prune_window["start"] = time.monotonic()
        try:
            call_command("ox_prune", "--batch-size", "500", stdout=prune_out)
        except BaseException as exc:
            prune_errors.append(exc)
        finally:
            prune_window["end"] = time.monotonic()
            connections.close_all()

    ticking = threading.Thread(target=reconciler)
    ticking.start()
    assert first_tick.wait(60)
    pruning = threading.Thread(target=prune)
    pruning.start()
    pruning.join(timeout=300)
    stop.set()
    ticking.join(timeout=120)
    assert not pruning.is_alive() and not ticking.is_alive()

    ticks_during_prune = sum(
        started <= prune_window["end"] and ended >= prune_window["start"]
        for started, ended in ticks
    )
    assert ticks_during_prune, (
        f"no reconciler tick overlapped the prune run: {len(ticks)} tick(s), "
        f"the prune ran for {prune_window['end'] - prune_window['start']:.2f}s"
    )

    failures = [
        f"reconciler: {record.getMessage()} -> {describe(record.exc_info[1])}"
        for record in reconciler_failures(caplog)
    ]
    failures += [f"reconciler loop raised: {describe(e)}" for e in tick_errors]
    failures += [f"ox_prune raised: {describe(e)}" for e in prune_errors]
    remaining = discarded.count()
    assert not failures, (
        f"{len(failures)} failure(s) while ox_prune raced idle reconciler ticks "
        f"on {connections[alias].vendor}: {failures[:5]}; ticks during the prune="
        f"{ticks_during_prune}, prune took "
        f"{prune_window['end'] - prune_window['start']:.2f}s, DISCARDED rows left="
        f"{remaining}, prune said {prune_out.getvalue().strip()!r}; "
        f"(fixture built in {build_seconds:.1f}s)"
    )
    assert remaining == 0, prune_out.getvalue()
