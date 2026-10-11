"""
Cancellation locks the upstreams that are dead when its locking
read runs, but decides with a later read that treats any upstream dead
*now* as dead.

workflows._cancel_one_level, in one transaction:

  1. UPDATE the workflow row under SEALED (takes the workflow row lock);
  2. SELECT ... FOR UPDATE the workflow's dead task rows ("doomed");
  3. SELECT the READY nodes blocked by a dead upstream ("cancellable"),
     re-evaluating every upstream's current status, not only the locked
     ids; a new statement, so a new snapshot at READ COMMITTED;
  4. UPDATE those nodes to DISCARDED;
  5. COMMIT.

Its docstring argues "There is no interleaving": a retry either commits
before step 2 (the upstream is not dead) or waits on the step-2 lock
until step 5. That holds only for an upstream already dead at step 2. An
upstream X that is RUNNING at step 2 and writes its final FAILED before
step 3 is read as dead but was never locked, so django_ox.actions.retry(X)
does not wait, and it can commit anywhere between step 3 and step 5.

What the test asserts, and why the precondition matters:

The bare predicate "not (X READY and child DISCARDED)" is not by
itself something the design promises. When X is already dead at step 2,
retry(X) waits for step 5 and then lands, which also ends in X READY and
the child DISCARDED, and the existing
test_a_retry_cannot_interleave_with_the_cancellation_that_would_discard_it
accepts exactly that. What the design does promise, in the docstring and in
that test ("a torn state -- the parent successfully retried *and* the child
discarded in the same pass -- is not reachable"), is that a retry which
commits *while the pass's transaction is still open* does not lose its
child to that pass. So the test first proves, as checked preconditions,
that X was RUNNING when the dead rows were locked, that its FAILED write
committed inside the pass, and that the retry returned True without
waiting while the pass had not committed. Only then does it assert the
predicate, and after it the user-visible outcome: once X is retried
and succeeds its child runs, and with the other failure retried too the
workflow completes clean, with on_success.

Every step is a production path: a real worker drives Y to FAILED, a real
worker claims X for its last attempt and runs it (claim_one + execute),
the operator action is actions.retry, and the pass is workflows._reconcile,
the scheduled task's body. No task row is written by hand.

Timing is forced from outside the statements, located by what the cancel
transaction sends to the database rather than by which function sends it,
so the same test reaches the same points in a changed implementation.
The first point is before the first statement after the FOR UPDATE.
In the implementation that exposed the race, this was the cancellable
SELECT, equivalent to wrapping _blocked_by_dead_q; the retry's
"after the cancellable read" landing was the discard UPDATE, equivalent
to a shim on timezone.now(). The second landing is inside that
transaction's commit, after the discard UPDATE. Each interleaved step
runs on its own thread and connection.

SQLite is skipped, not passed: the pass opens with a write and SQLite
serialises writers, so X's FAILED write cannot commit inside it.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
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
WORKFLOW_TABLE = OxpullWorkflow._meta.db_table
LOCK = "SELECT dead task rows FOR UPDATE"

# How long a hook lets an interleaved step run before the pass moves on.
# The steps here take milliseconds; a step still running after this is
# waiting on a lock the paused pass holds, and is recorded as such.
STEP_WAIT = 10.0


# -- tasks -------------------------------------------------------------
#
# A node that fails while its label is flagged and succeeds once the flag
# is cleared: an upstream that dies and then succeeds when retried.
# tests_oxpull/tasks.py has no such task on the workflow backends.


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
    alias: str
    worker: Callable[[], Any]
    flagged: Any
    plain: Any


SHAPES = [
    pytest.param(Shape("flows", flow_worker, flagged_step, tasks.step), id="flows"),
    pytest.param(
        Shape(
            "flows_deferred",
            deferring_worker,
            flagged_deferred_step,
            tasks.deferred_step,
        ),
        id="flows_deferred",
    ),
]

# Where the retry lands inside the pass's transaction.
AFTER_THE_CANCELLABLE_READ = "after_the_cancellable_read"
BEFORE_THE_COMMIT = "before_the_commit"
LANDINGS = [AFTER_THE_CANCELLABLE_READ, BEFORE_THE_COMMIT]


# Read at collection. The test skips itself on SQLite, so it is expected to
# fail only where the upstream's write can commit inside the pass.
HAS_ROW_LOCKS = connections[oxpull_db_alias()].vendor != "sqlite"


def skip_without_row_locks() -> None:
    if connections[oxpull_db_alias()].vendor == "sqlite":
        pytest.skip(
            "SQLite serialises writers and the cancel transaction opens with a "
            "write, so no upstream write can commit inside it"
        )


# -- harness -----------------------------------------------------------


class Step:
    """One interleaved step on its own thread and connection."""

    def __init__(self, name: str, fn: Callable[[], Any]):
        self.name = name
        self.fn = fn
        self.result: Any = None
        self.error: BaseException | None = None
        self.elapsed: float | None = None
        self.finished_inside_pass: bool | None = None
        self._thread = threading.Thread(target=self._run, name=name)

    def _run(self) -> None:
        started = time.monotonic()
        try:
            self.result = self.fn()
        except BaseException as exc:
            self.error = exc
        finally:
            self.elapsed = time.monotonic() - started
            connections.close_all()

    def run_inside_pass(self, wait: float) -> None:
        self._thread.start()
        self._thread.join(timeout=wait)
        self.finished_inside_pass = not self._thread.is_alive()

    def join(self, timeout: float = 60) -> None:
        if self._thread.ident is not None:
            self._thread.join(timeout=timeout)
            assert not self._thread.is_alive(), f"{self.name} never finished"

    def describe(self) -> str:
        elapsed = None if self.elapsed is None else round(self.elapsed, 3)
        return (
            f"{self.name}: result={self.result!r} error={self.error!r} "
            f"elapsed={elapsed}s finished_inside_pass={self.finished_inside_pass}"
        )


@dataclass
class PassRecord:
    events: list[str] = field(default_factory=list)
    cancel_returned: Any = None
    cancel_error: BaseException | None = None
    reconcile_returned: Any = None
    isolation: str = ""

    def timeline(self) -> str:
        return " | ".join(self.events)


def isolation_level() -> str:
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SHOW transaction_isolation")
        elif connection.vendor == "mysql":
            cursor.execute("SELECT @@transaction_isolation")
        else:
            return connection.vendor
        return f"{connection.vendor}:{cursor.fetchone()[0]}"


def reconcile_with_steps(
    monkeypatch: pytest.MonkeyPatch,
    *,
    after_the_lock: Step | None,
    retry: Step | None,
    landing: str,
    wait: float = STEP_WAIT,
) -> PassRecord:
    """
    Run one real reconciler pass (workflows._reconcile) on this thread and
    interleave up to two steps into the first transaction that locks dead
    task rows:

      after_the_lock   before the first statement after the locking read;
      retry at AFTER_THE_CANCELLABLE_READ
                       before the second statement after it, or inside
                       the commit if no second statement comes;
      retry at BEFORE_THE_COMMIT
                       inside that transaction's commit.

    Each fires once, on the reconciler's own connection only. On SQLite,
    which has no FOR UPDATE, the locking read is the first task-table
    SELECT after the workflow row UPDATE.
    """
    record = PassRecord(isolation=isolation_level())
    connection = connections[oxpull_db_alias()]
    sqlite = connection.vendor == "sqlite"
    state = {
        "workflow_update": False,
        "locked": False,
        "after_lock": 0,
        "hook2": False,
        "done": False,
    }

    def log(entry: str) -> None:
        record.events.append(entry)

    def fire(step: Step) -> None:
        log(f"hook: {step.name} begins")
        step.run_inside_pass(wait)
        log(
            f"hook: {step.name} "
            + ("returned" if step.finished_inside_pass else "still waiting")
        )

    def classify(text: str) -> str:
        upper = text.upper()
        if upper.startswith("SELECT") and TASK_TABLE in text:
            if "FOR UPDATE" in upper:
                return LOCK
            if sqlite and state["workflow_update"] and not state["locked"]:
                return LOCK
        if upper.startswith("UPDATE") and WORKFLOW_TABLE in text:
            return "UPDATE workflow row"
        if upper.startswith("UPDATE") and TASK_TABLE in text:
            return "UPDATE task rows"
        return text[:40]

    def statements(execute, sql, params, many, context):
        label = classify(" ".join(sql.split()))
        if state["locked"] and not state["done"]:
            if state["after_lock"] == 0 and after_the_lock is not None:
                log(
                    f"hook1 context: in_atomic_block={connection.in_atomic_block} "
                    f"next={label}"
                )
                fire(after_the_lock)
            elif (
                state["after_lock"] == 1
                and retry is not None
                and landing == AFTER_THE_CANCELLABLE_READ
                and not state["hook2"]
            ):
                state["hook2"] = True
                log(
                    f"hook2 context: in_atomic_block={connection.in_atomic_block} "
                    f"next={label}"
                )
                fire(retry)
            state["after_lock"] += 1
        result = execute(sql, params, many, context)
        log(f"sql: {label}")
        if label == "UPDATE workflow row":
            state["workflow_update"] = True
        if label == LOCK and not state["done"]:
            state["locked"] = True
        return result

    original_commit = connection._commit

    def commit():
        if state["locked"] and not state["done"]:
            state["done"] = True
            if retry is not None and not state["hook2"]:
                state["hook2"] = True
                log("hook2 context: inside the commit")
                fire(retry)
        result = original_commit()
        log("commit")
        return result

    original_cancel = workflows._cancel_unreachable

    def cancel_unreachable(workflow_pk, db_alias):
        try:
            record.cancel_returned = original_cancel(workflow_pk, db_alias)
        except BaseException as exc:
            record.cancel_error = exc
            raise
        return record.cancel_returned

    monkeypatch.setattr(workflows, "_cancel_unreachable", cancel_unreachable)
    monkeypatch.setattr(connection, "_commit", commit)
    try:
        with connection.execute_wrapper(statements):
            record.reconcile_returned = reconcile()
    finally:
        monkeypatch.undo()
    for step in (after_the_lock, retry):
        if step is not None:
            step.join()
    assert record.cancel_error is None, record.cancel_error
    return record


def status(task_id) -> str:
    return OxTask.objects.get(id=task_id).status


def run_until(worker, predicate, limit: int = 30) -> None:
    """run_once until predicate() holds; a zero backoff is still a timestamp."""
    for _ in range(limit):
        if predicate():
            return
        if not worker.run_once():
            time.sleep(0.01)
    assert predicate()


@dataclass
class Torn:
    flow: Any
    y: Any
    x: Any
    d: Any
    worker: Any
    claimed: Any


def torn_workflow(shape: Shape, name: str) -> Torn:
    """
    y (FAILED) -> ey      x (RUNNING, last attempt) -> d (WAITING, blocked)

    y is the other dead node, and ey depends on it. Together they are what
    make the pass open its cancel transaction at all: the reconciler reads
    first whether anything is cancellable, and a dead node with nothing
    downstream opens no transaction. x is claimed by a real worker on its
    last attempt and flagged to fail, so its outcome write will be FAILED.
    """
    flow = workflows.create(
        name,
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    y = flow.add("y", shape.flagged, "y")
    flow.add("ey", shape.plain, "ey", depends_on="y")
    x = flow.add("x", shape.flagged, "x")
    d = flow.add("d", shape.plain, "d", depends_on="x")
    flow.seal()
    tasks.STATE["fail:y"] = True
    tasks.STATE["fail:x"] = True

    worker = shape.worker()
    run_until(worker, lambda: status(y.id) == OxTask.Status.FAILED)
    run_until(worker, lambda: OxTask.objects.get(id=x.id).attempts == 2)
    assert status(x.id) == OxTask.Status.READY
    claimed = worker.claim_one()
    assert claimed is not None
    assert str(claimed.id) == str(x.id)
    row = OxTask.objects.get(id=x.id)
    assert row.status == OxTask.Status.RUNNING
    assert row.attempts == row.max_attempts == 3
    assert status(d.id) == OxTask.Status.WAITING
    assert "d" not in order()
    return Torn(flow, y, x, d, worker, claimed)


def upstream_last_attempt_fails(torn: Torn) -> Step:
    def run():
        torn.worker.execute(torn.claimed)
        return OxTask.objects.get(id=torn.x.id).status

    return Step("x's last attempt runs and writes its outcome", run)


def operator_retries_x(torn: Torn) -> Step:
    def run():
        retried = actions.retry(torn.x.id)
        return {
            "retry": retried,
            "x_seen_by_retry": status(torn.x.id),
            "d_seen_by_retry": status(torn.d.id),
        }

    return Step("operator retries x", run)


# -- the test ----------------------------------------------------------


@pytest.mark.parametrize("landing", LANDINGS)
@pytest.mark.parametrize("shape", SHAPES)
def test_a_retry_that_commits_inside_the_pass_does_not_lose_its_child(
    shape, landing, monkeypatch
):
    """
    1. The pass locks the dead set: {y}. x is RUNNING and not in it.
    2. x's last attempt fails: the real worker's FAILED write commits.
    3. The pass reads d as blocked by a dead upstream (x).
    4. The operator retries x. Nothing makes it wait: x was never locked.
       It commits while the pass is still open (before the discard UPDATE,
       or after it and before COMMIT).
    5. The pass discards d and commits.

    Correct: a retry that committed while the pass was open does not lose
    its child to that pass; once x succeeds d runs, and with y retried too
    the workflow completes. ey stays cancelled: y was dead when the pass
    locked it, and nothing here revives a cancelled node, so the workflow
    completes with that one cancellation and without on_success.
    """
    skip_without_row_locks()
    torn = torn_workflow(shape, f"torn-{landing}")
    hook1 = upstream_last_attempt_fails(torn)
    hook2 = operator_retries_x(torn)

    record = reconcile_with_steps(
        monkeypatch, after_the_lock=hook1, retry=hook2, landing=landing
    )

    x_after_pass = status(torn.x.id)
    d_after_pass = OxTask.objects.get(id=torn.d.id)

    # Preconditions: the interleaving really happened. If any of these
    # fails the test did not reach the hazard and says nothing about it.
    steps = f"{hook1.describe()}; {hook2.describe()}"
    timeline = record.timeline()
    assert hook1.error is None and hook2.error is None, steps
    hook1_at = next(
        i for i, e in enumerate(record.events) if e.startswith("hook1 context:")
    )
    assert record.events[hook1_at - 1] == f"sql: {LOCK}", timeline
    assert "in_atomic_block=True" in record.events[hook1_at], timeline
    assert hook1.finished_inside_pass is True, steps
    assert hook1.result == OxTask.Status.FAILED, steps
    hook2_at = next(
        i for i, e in enumerate(record.events) if e.startswith("hook2 context:")
    )
    assert hook1_at < hook2_at < record.events.index("commit", hook1_at), timeline
    assert hook2.finished_inside_pass is True, (
        "the retry waited on the pass, which is the designed interlock: " + steps
    )
    assert hook2.result["retry"] is True, steps
    assert hook2.result["x_seen_by_retry"] == OxTask.Status.READY, steps
    # The pass had not committed when the retry did: the retry's own
    # connection still reads d as WAITING, not DISCARDED.
    assert hook2.result["d_seen_by_retry"] == OxTask.Status.WAITING, steps
    assert x_after_pass == OxTask.Status.READY, timeline

    # Let the retry do what it was for, and retry the other failure too.
    tasks.STATE["fail:x"] = False
    tasks.STATE["fail:y"] = False
    assert actions.retry(torn.y.id) is True
    drainer = shape.worker()
    run_until(
        drainer,
        lambda: (
            status(torn.x.id) == OxTask.Status.SUCCESSFUL
            and status(torn.y.id) == OxTask.Status.SUCCESSFUL
        ),
    )
    for _ in range(3):
        drainer.run_once()
    reconcile()
    row = stored(torn.flow)

    facts = (
        f"[{record.isolation}] timeline: {timeline}; steps: {steps}; "
        f"cancel pass returned {record.cancel_returned}; "
        f"after the pass: x={x_after_pass}, d={d_after_pass.status} "
        f"(run_after={d_after_pass.run_after}); "
        f"after retrying y and draining: x={status(torn.x.id)}, "
        f"y={status(torn.y.id)}, d={status(torn.d.id)}, d ran={'d' in order()}, "
        f"workflow={row.status} failed_count={row.failed_count} "
        f"cancelled_count={row.cancelled_count} "
        f"on_complete enqueued={row.callback_task_id is not None} "
        f"on_success enqueued={row.success_task_id is not None}"
    )
    # Printed as well as asserted: a passing run (a fixed implementation)
    # still shows what it did, under pytest -rP.
    print("FACTS", facts)  # noqa: T201

    assert not (
        x_after_pass == OxTask.Status.READY
        and d_after_pass.status == OxTask.Status.DISCARDED
    ), (
        "the pass discarded d although its only upstream was retried by a "
        "commit that landed inside the pass: " + facts
    )
    assert status(torn.d.id) == OxTask.Status.SUCCESSFUL, facts
    assert row.status == OxpullWorkflow.Status.COMPLETE, facts
    assert row.cancelled_count == 1, facts
    assert row.success_task_id is None, facts
