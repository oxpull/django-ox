"""
The cancellation UPDATE in
workflows._cancel_one_level discards on `status=READY` alone.

What these tests found, stated before the tests so the reader knows what
fails and why:

_cancel_one_level locks the upstream rows that are dead *when its
SELECT ... FOR UPDATE runs*, then reads the READY nodes blocked by a dead
upstream in a second, unlocked SELECT, then discards them. The docstring's
"There is no interleaving" argument holds only for an upstream that was
already dead at the locking read. An upstream that dies after the lock and
before the second SELECT is read as dead without being locked, so an
operator retry of it is not made to wait, and it can commit before the
discard UPDATE. The discard then lands on a node whose only upstream is no
longer dead.

An (status, lease_epoch) compare-and-set on the discarded node, which is
what django_ox.actions.discard does, does not close that: retry() bumps
the upstream's epoch, not the downstream's, and the downstream is READY
before and after. It closes only the narrower variant in the second test,
where the downstream is also claimed and requeued inside the window.

Each test asserts the correct behaviour, so each fails today on the
databases with row locks. Timing is forced with Django's public
connection.execute_wrapper on the reconciler's own connection: every
statement the reconciler issues is the production statement, and the
interleaved steps are the real worker, the real reaper and the real
operator actions, each on its own thread and connection.

SQLite is skipped, not passed: the reconciler's transaction opens with a
write, SQLite serialises writers, so neither the upstream's failure write
nor the retry can commit inside it.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest
from django.db import connections
from django.tasks import task

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_workflows import flow_worker, order, reconcile, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db(transaction=True)

TASK_TABLE = OxTask._meta.db_table

# Read at collection, as _row_locks_only reads it at run time: the expected
# failures exist where the database has row locks, and SQLite skips.
ROW_LOCKS = connections[oxpull_db_alias()].vendor != "sqlite"


# -- tasks -------------------------------------------------------------
#
# A node that fails while its label is flagged and succeeds once the flag
# is cleared: the shape of an upstream that dies and then succeeds when an
# operator retries it. tests_oxpull/tasks.py has flaky tasks only on the default
# backend, which add() refuses for a workflow node.


def _flagged(label: str) -> str:
    tasks._record(label)
    if tasks.STATE.get(f"fail:{label}"):
        raise ValueError(f"{label} fails while flagged")
    return label


@task(backend="flows")
def flagged_step(label):
    return _flagged(label)


@task(backend="flows_deferred")
def flagged_deferred_step(label):
    return _flagged(label)


@dataclass(frozen=True)
class Shape:
    """One workflow backend and the node tasks that live on it."""

    alias: str
    worker: Callable[[], Any]
    dead: Any
    flagged: Any
    plain: Any


SHAPES = [
    pytest.param(
        Shape("flows", flow_worker, tasks.failing_step, flagged_step, tasks.step),
        id="flows",
    ),
    pytest.param(
        Shape(
            "flows_deferred",
            deferring_worker,
            tasks.failing_deferred_step,
            flagged_deferred_step,
            tasks.deferred_step,
        ),
        id="flows_deferred",
    ),
]


@pytest.fixture(autouse=True)
def _row_locks_only():
    if connections[oxpull_db_alias()].vendor == "sqlite":
        pytest.skip(
            "SQLite serialises writers and the cancel transaction opens with a "
            "write, so no upstream write can commit inside it"
        )


# -- harness -----------------------------------------------------------


def in_thread(step: Callable[[], Any], timeout: float = 30) -> Any:
    """Run `step` on its own thread and connection, as a separate process would."""
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
    if thread.is_alive():
        raise RuntimeError(
            "the interleaved step is waiting on the reconciler's transaction"
        )
    if err:
        raise err[0]
    return out[0]


class Interleave:
    """
    Pause the reconciler at two points inside _cancel_one_level:

    after_lock:  after the SELECT ... FOR UPDATE on the dead upstreams,
                 before the unlocked SELECT of cancellable nodes;
    after_read:  after that SELECT, before whatever statement comes next.
                 Today that is the discard UPDATE. It is deliberately not
                 pinned to the UPDATE: a correct cancel pass may have
                 nothing to discard, and the step must still run.

    One-shot: later cancellation levels and the completion pass run
    untouched. Errors are recorded, not raised, because _reconcile logs
    and swallows every exception from a workflow's pass.
    """

    def __init__(self, after_lock, after_read):
        self.steps = {"after_lock": after_lock, "after_read": after_read}
        self.fired: list[str] = []
        self.before: dict[str, str] = {}
        self.results: dict[str, Any] = {}
        self.errors: list[tuple[str, BaseException]] = []
        self._locked = False
        self._read = False

    def _fire(self, name: str, sql: str) -> None:
        self.fired.append(name)
        self.before[name] = " ".join(sql.split())[:48]
        try:
            # Never on the reconciler's own connection: that would run the
            # step inside its transaction, which no other process can do.
            self.results[name] = in_thread(self.steps[name])
        except BaseException as exc:
            self.errors.append((name, exc))

    def __call__(self, execute, sql, params, many, context):
        if not self._locked:
            result = execute(sql, params, many, context)
            if "FOR UPDATE" in sql.upper() and TASK_TABLE in sql:
                self._locked = True
            return result
        if not self.fired and sql.lstrip()[:6].upper() == "SELECT":
            self._fire("after_lock", sql)
            result = execute(sql, params, many, context)
            self._read = True
            return result
        if self._read and self.fired == ["after_lock"]:
            self._fire("after_read", sql)
        return execute(sql, params, many, context)


def reconcile_interleaved(interleave: Interleave) -> int:
    connection = connections[oxpull_db_alias()]
    with connection.execute_wrapper(interleave):
        completed = reconcile()
    assert interleave.errors == [], interleave.errors
    assert interleave.fired == ["after_lock", "after_read"], (
        f"the reconciler never reached both points: {interleave.fired}"
    )
    return completed


def status(task_id) -> str:
    return OxTask.objects.get(id=task_id).status


def run_until(worker, predicate, limit: int = 20) -> None:
    """run_once until predicate() holds; a zero backoff is still a timestamp."""
    for _ in range(limit):
        if predicate():
            return
        if not worker.run_once():
            time.sleep(0.01)
    assert predicate()


def sibling_dead_workflow(shape: Shape, name: str):
    """
    dead (already FAILED) -> dead-child   and   upstream -> child

    `dead` and its child are what make the reconciler open its cancel
    transaction and take a lock: a sealed workflow with a node that settled
    without succeeding and a node that depends on it. The reconciler reads
    first whether anything is cancellable, and a dead node with nothing
    downstream of it opens no transaction. `upstream` is on its last attempt
    and flagged to fail, and `child` depends on it alone.
    """
    flow = workflows.create(name)
    dead = flow.add("dead", shape.dead, "dead")
    flow.add("dead-child", shape.plain, "dead-child", depends_on="dead")
    upstream = flow.add("upstream", shape.flagged, "upstream")
    child = flow.add("child", shape.plain, "child", depends_on="upstream")
    flow.seal()
    tasks.STATE["fail:upstream"] = True

    worker = shape.worker()
    run_until(worker, lambda: status(dead.id) == OxTask.Status.FAILED)
    # Two of the upstream's three attempts, so the next one is its last.
    run_until(worker, lambda: OxTask.objects.get(id=upstream.id).attempts == 2)
    assert status(upstream.id) == OxTask.Status.READY
    assert status(child.id) == OxTask.Status.WAITING
    assert "child" not in order()
    return flow, dead.id, upstream.id, child.id


# -- the tests ---------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES)
def test_a_retry_of_an_upstream_that_died_after_the_lock_does_not_lose_its_child(
    shape,
):
    """
    1. The reconciler locks the dead set, which is {dead}.
    2. The upstream's last attempt fails: a real worker, a real FAILED write.
    3. The reconciler reads the child as blocked by a dead upstream.
    4. An operator retries the upstream. Nothing makes it wait: the upstream
       was not in the locked set.
    5. The reconciler discards the child on status=READY.

    Correct: a node is discarded only while an upstream is dead. At step 5
    the child's only upstream is READY, and once the retry succeeds the
    child must run.
    """
    flow, _dead, upstream, child = sibling_dead_workflow(shape, "retry-after-lock")

    def upstream_dies():
        shape.worker().run_once()
        return OxTask.objects.get(id=upstream).status

    def operator_retries():
        return actions.retry(upstream)

    interleave = Interleave(upstream_dies, operator_retries)
    reconcile_interleaved(interleave)
    assert interleave.results["after_lock"] == OxTask.Status.FAILED
    assert interleave.results["after_read"] is True

    child_after_pass = OxTask.objects.get(id=child)
    upstream_after_pass = status(upstream)

    # The retry was meant to work. Let it.
    tasks.STATE["fail:upstream"] = False
    run_until(shape.worker(), lambda: status(upstream) == OxTask.Status.SUCCESSFUL)
    for _ in range(3):
        shape.worker().run_once()
    reconcile()
    row = stored(flow)

    facts = (
        f"retry landed before: {interleave.before['after_read']!r}; "
        f"after the pass: child={child_after_pass.status} "
        f"(run_after={child_after_pass.run_after}), upstream={upstream_after_pass}; "
        f"after draining: upstream={status(upstream)}, child={status(child)}, "
        f"child ran={'child' in order()}, workflow={row.status} "
        f"failed_count={row.failed_count} cancelled_count={row.cancelled_count}"
    )
    assert child_after_pass.status != OxTask.Status.DISCARDED, (
        "the reconciler discarded a node whose only upstream had been retried "
        "before the discard: " + facts
    )
    assert status(child) == OxTask.Status.SUCCESSFUL, facts


@pytest.mark.parametrize("shape", SHAPES)
def test_a_child_claimed_and_requeued_inside_the_window_is_not_discarded(shape):
    """
    The narrower interleaving: the reaper requeues the discarded row
    between the reconciler's SELECT and its UPDATE.

    For the child to be claimed at all inside the window its gate must
    pass, so its upstream has to leave the dead set first: the same retry
    as the test above, and then the upstream succeeds, the child is
    claimed, its lease is expired (actions.expire_lease brings the
    reaper's clock forward; the production trigger is the reconciler
    stalling between two statements for longer than LOCK_TIMEOUT), and
    the real reaper requeues it with a bumped epoch.

    The stalled first execution then finishes. Its outcome write is
    refused by the epoch, as it should be.

    Correct: the child's upstream is SUCCESSFUL and it has attempts left,
    so it is not discarded, and its second attempt runs. This is the one
    variant a (status, lease_epoch) compare-and-set would catch.
    """
    flow, _dead, upstream, child = sibling_dead_workflow(shape, "requeue-in-window")

    def upstream_dies():
        shape.worker().run_once()
        return OxTask.objects.get(id=upstream).status

    def retry_succeed_claim_reap():
        assert actions.retry(upstream) is True
        tasks.STATE["fail:upstream"] = False
        worker = shape.worker()
        assert worker.run_once() is True
        assert OxTask.objects.get(id=upstream).status == OxTask.Status.SUCCESSFUL
        claimed = worker.claim_one()
        assert claimed is not None
        assert str(claimed.id) == str(child)
        assert actions.expire_lease(child) is True
        assert worker.reap() == 1
        requeued = OxTask.objects.get(id=child)
        assert requeued.status == OxTask.Status.READY
        assert requeued.lease_epoch == claimed.lease_epoch + 1
        return claimed

    interleave = Interleave(upstream_dies, retry_succeed_claim_reap)
    reconcile_interleaved(interleave)
    assert interleave.results["after_lock"] == OxTask.Status.FAILED
    stale = interleave.results["after_read"]

    child_after_pass = OxTask.objects.get(id=child)
    # The discard leaves no lease behind: the reaper's requeue had already
    # cleared it, and the cancel UPDATE clears locked_by and locked_at.
    assert child_after_pass.locked_by is None
    assert child_after_pass.locked_at is None
    assert child_after_pass.lease_expires_at is None

    # The first execution was only stalled. It finishes now and is fenced.
    shape.worker().execute(stale)
    ran_before_drain = order().count("child")
    fenced_status = status(child)

    for _ in range(3):
        shape.worker().run_once()
    reconcile()
    row = stored(flow)

    facts = (
        f"requeue landed before: {interleave.before['after_read']!r}; "
        f"after the pass: child={child_after_pass.status} "
        f"epoch={child_after_pass.lease_epoch} attempts={child_after_pass.attempts}; "
        f"stale execution ran the body {ran_before_drain}x and left child="
        f"{fenced_status}; after draining: child={status(child)}, "
        f"child body runs={order().count('child')}, workflow={row.status} "
        f"failed_count={row.failed_count} cancelled_count={row.cancelled_count}"
    )
    assert child_after_pass.status != OxTask.Status.DISCARDED, (
        "the reconciler discarded a requeued node whose upstream had succeeded: "
        + facts
    )
    assert status(child) == OxTask.Status.SUCCESSFUL, facts
    assert row.status == OxpullWorkflow.Status.COMPLETE, facts
