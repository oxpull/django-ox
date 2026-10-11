"""
A unique enqueue inside an open transaction can freeze the
workflow reconciler on PostgreSQL and MySQL.

The state. A unique task ran as a node of a SEALED workflow (W1) and ended
FAILED or LOST, and no reconciler tick has cancelled W1's cone yet. The
key's OxpullUniqueLock row still names that dead task row.

The trigger. Application code enqueues the same key inside a transaction
it keeps open: a view under ATOMIC_REQUESTS, Workflow.add() or Batch.add()
inside atomic(), or a bare enqueue inside atomic(). The enqueue meets the
stale lock, and conflict resolution (OxpullBackend._resolve_conflict)
re-reads the dead holder with SELECT ... FOR UPDATE before it releases the
key. That row lock belongs to the caller's transaction and lives until the
caller commits.

The stall. The next reconciler tick sweeps SEALED workflows serially,
oldest first. It reaches W1, takes W1's workflow row lock in
_cancel_one_level, and then blocks locking the same dead row. Every SEALED
workflow behind W1 waits with it: nothing is cancelled or completed, and no
callback is enqueued.

Each test asserts what a correct reconciler does. An unrelated sealed
workflow (W2), whose only node has succeeded, is completed within a few
seconds of the tick starting, whatever some caller's transaction holds.
The dead row and the new enqueue come from real paths: a real worker runs
the attempts, and the operator's expire_lease plus a real reaper produce
LOST. The two controls run the identical harness and pass today: a
non-unique enqueue inside atomic(), and the unique enqueue with no
transaction open.

add() now refuses a unique task as a node, so no unique lock row can name
a node's row and the state above cannot be built. The tests still try the
hazard's own build first. When add() refuses the unique root, saying it is
unique, the dead unique row is the application's own enqueue of that call,
taken to FAILED or LOST by the same real paths before W1 exists, and W1's
root is an ordinary task that dies the same way. Workflow.add() inside
atomic() is refused before it enqueues, so that holder's transaction is
open and holds nothing of the key. Every other holder still meets the dead
lock inside its transaction, and the tick must still complete W2 in time.
Both tests catch the PostgreSQL and MySQL failure caused by accepting the
unique root without preventing this lock interaction.

SQLite is skipped. It has no row locks, so _resolve_conflict takes none
there, and the lock this hazard is about cannot exist.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError, connections, transaction
from django.http import HttpRequest, HttpResponse
from django.tasks.exceptions import TaskException
from django.test import Client, override_settings
from django.urls import path

from django_ox import actions
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.models import OxpullUniqueLock, OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, reconcile, stored

pytestmark = pytest.mark.django_db(transaction=True)

# How long one reconciler tick is given to reach and complete W2. A tick
# over two small workflows takes milliseconds; three seconds is generous
# under load and far below either database's lock wait.
TICK_BOUND = 3.0

DEAD = ["failed", "lost"]

# What add() may raise when it refuses a unique node. The message must also
# say the task is unique, so an unrelated error does not count.
REFUSALS = (ImproperlyConfigured, TaskException, ValueError)


@pytest.fixture(autouse=True)
def _row_locking_database():
    if not connections["default"].features.has_select_for_update:
        pytest.skip(
            "SQLite has no row locks: _resolve_conflict takes no FOR UPDATE "
            "there, so the lock this hazard is about is never taken."
        )


def _unique_node():
    """tests_oxpull.tasks.unique_fail_always (@unique), on the gated backend."""
    return tasks.unique_fail_always.using(backend="flows")


# -- the state ----------------------------------------------------------


def _kill(task_id: Any, dead: str) -> None:
    """Take the one claimable row, `task_id`, to FAILED or LOST with real workers."""
    if dead == "failed":
        # Three real attempts, each raising.
        drain(flow_worker())
        return
    # Two real failing attempts, then a claim whose worker goes quiet.
    # The operator expires its lease, and another worker's reaper takes
    # the row, attempts exhausted, to LOST.
    worker = flow_worker()
    assert worker.run_once() and worker.run_once()
    claimed = worker.claim_one()
    assert claimed is not None and str(claimed.id) == str(task_id)
    assert claimed.attempts == claimed.max_attempts
    assert actions.expire_lease(claimed.id)
    assert flow_worker().reap() == 1


def _doomed_workflow(dead: str) -> tuple[Any, str, str, str]:
    """
    W1: root -> child, sealed, with the root driven to FAILED or LOST by
    real workers, and a dead unique row whose lock row still names it. No
    reconciler tick has run, so the child is still WAITING.

    Returns (W1, root id, child id, dead unique row id). The hazard's own
    build makes the unique task W1's root, and the last two ids are one
    row. When add() refuses that root, the unique row is the application's
    own enqueue of the call, killed before W1 is built, and W1's root is an
    ordinary failing task.
    """
    expected = OxTask.Status.FAILED if dead == "failed" else OxTask.Status.LOST
    flow = workflows.create("w1-doomed")
    try:
        root = flow.add("root", _unique_node())
    except REFUSALS as exc:
        assert "unique" in str(exc).lower(), f"add() refused the root: {exc!r}"
        held = _unique_node().enqueue()
        _kill(held.id, dead)
        assert OxTask.objects.get(id=held.id).status == expected
        root = flow.add("root", tasks.failing_step, "root")
    else:
        held = root
    child = flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    _kill(root.id, dead)
    assert OxTask.objects.get(id=root.id).status == expected
    assert OxTask.objects.get(id=child.id).status == OxTask.Status.WAITING
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    # The key still names the dead row, so the next enqueue of that key
    # goes through conflict resolution rather than a clean insert.
    assert OxpullUniqueLock.objects.filter(task_id=held.id).exists()
    return flow, str(root.id), str(child.id), str(held.id)


def _healthy_workflow() -> Any:
    """W2: one node, succeeded, sealed. Created after W1, so swept after it."""
    flow = workflows.create("w2-healthy")
    only = flow.add("only", tasks.step, "only")
    flow.seal()
    assert flow_worker().run_once()
    assert OxTask.objects.get(id=only.id).status == OxTask.Status.SUCCESSFUL
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    return flow


# -- the application transaction ---------------------------------------


class Hold:
    """One application transaction, parked on an Event until released."""

    def __init__(self) -> None:
        self.holding = threading.Event()
        self.release = threading.Event()
        self.out: dict[str, Any] = {}

    def enqueued(self, result: Any) -> None:
        self.out["new_id"] = str(result.id)

    def refused(self, exc: BaseException) -> None:
        self.out["refused"] = exc

    def park(self) -> None:
        self.out["transaction_open"] = connections["default"].in_atomic_block
        self.holding.set()
        self.out["released"] = self.release.wait(180)


_VIEW_HOLD: dict[str, Hold] = {}


def enqueue_and_park(request: HttpRequest) -> HttpResponse:
    hold = _VIEW_HOLD["hold"]
    hold.enqueued(_unique_node().enqueue())
    hold.park()
    return HttpResponse("queued")


urlpatterns = [path("enqueue-and-park/", enqueue_and_park)]


def _atomic_request(hold: Hold) -> None:
    """A real request through Django's handler, with ATOMIC_REQUESTS on."""
    _VIEW_HOLD["hold"] = hold
    database = connections.settings["default"]
    previous = database.get("ATOMIC_REQUESTS", False)
    database["ATOMIC_REQUESTS"] = True
    try:
        response = Client().get("/enqueue-and-park/")
    finally:
        database["ATOMIC_REQUESTS"] = previous
    hold.out["status_code"] = response.status_code


def _enqueue_in_atomic(hold: Hold) -> None:
    with transaction.atomic():
        hold.enqueued(_unique_node().enqueue())
        hold.park()


def _workflow_add_in_atomic(hold: Hold) -> None:
    flow = workflows.create("w3-open")
    with transaction.atomic():
        try:
            hold.enqueued(flow.add("again", _unique_node()))
        except REFUSALS as exc:
            # Refused before it enqueued: the transaction stays open and
            # holds nothing of the key.
            hold.refused(exc)
        hold.park()


def _batch_add_in_atomic(hold: Hold) -> None:
    batch = batches.create("b-open")
    with transaction.atomic():
        hold.enqueued(batch.add(_unique_node()))
        hold.park()


def _plain_enqueue_in_atomic(hold: Hold) -> None:
    with transaction.atomic():
        hold.enqueued(tasks.failing_step.enqueue("again"))
        hold.park()


def _unique_enqueue_without_a_transaction(hold: Hold) -> None:
    hold.enqueued(_unique_node().enqueue())
    hold.park()


# id -> (holder, enqueues the dead node's unique key, caller's transaction open)
HOLDERS: dict[str, tuple[Callable[[Hold], None], bool, bool]] = {
    "atomic-requests-view": (_atomic_request, True, True),
    "enqueue-in-atomic": (_enqueue_in_atomic, True, True),
    "workflow-add-in-atomic": (_workflow_add_in_atomic, True, True),
    "batch-add-in-atomic": (_batch_add_in_atomic, True, True),
    "control-plain-enqueue-in-atomic": (_plain_enqueue_in_atomic, False, True),
    "control-unique-enqueue-no-transaction": (
        _unique_enqueue_without_a_transaction,
        True,
        False,
    ),
}


def _start_holder(target: Callable[[Hold], None], hold: Hold) -> threading.Thread:
    def run() -> None:
        try:
            target(hold)
        except BaseException as exc:
            hold.out["error"] = exc
        finally:
            hold.holding.set()
            connections.close_all()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def _start_reconciler(out: dict[str, Any]) -> threading.Thread:
    def run() -> None:
        started = time.monotonic()
        try:
            out["completed"] = reconcile()
        except BaseException as exc:
            out["error"] = exc
        finally:
            out["elapsed"] = time.monotonic() - started
            connections.close_all()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


# -- observations (reported in failure messages, never asserted) --------


def _row_lock_held(task_id: str) -> bool:
    """Does another transaction hold this task row's lock right now?"""
    try:
        with transaction.atomic():
            list(
                OxTask.objects.select_for_update(nowait=True)
                .filter(id=task_id)
                .values_list("id", flat=True)
            )
    except DatabaseError:
        return True
    return False


def _lock_waits() -> list[str]:
    """The statements in this test database that are waiting on a lock."""
    connection = connections["default"]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                "SELECT wait_event || ': ' || query FROM pg_stat_activity "
                "WHERE datname = current_database() AND wait_event_type = 'Lock' "
                "AND pid <> pg_backend_pid()"
            )
        elif connection.vendor == "mysql":
            cursor.execute(
                "SELECT t.trx_query FROM information_schema.innodb_trx t "
                "JOIN information_schema.processlist p "
                "ON p.id = t.trx_mysql_thread_id "
                "WHERE t.trx_state = 'LOCK WAIT' AND p.db = DATABASE()"
            )
        else:
            return []
        return [" ".join(str(row[0]).split())[:220] for row in cursor.fetchall()]


# -- the tests -----------------------------------------------------------


@pytest.mark.parametrize("dead", DEAD)
@pytest.mark.parametrize("holder", list(HOLDERS))
def test_a_transaction_that_reenqueued_a_dead_unique_node_does_not_stall_the_reconciler(
    holder, dead
):
    target, is_unique, transaction_open = HOLDERS[holder]
    w1, _root_id, child_id, held_id = _doomed_workflow(dead)
    w2 = _healthy_workflow()
    assert stored(w1).created_at < stored(w2).created_at

    hold = Hold()
    tick: dict[str, Any] = {}
    tick_thread = None
    started = None
    released_after = None
    with override_settings(ROOT_URLCONF=__name__):
        holder_thread = _start_holder(target, hold)
        try:
            assert hold.holding.wait(30), "the holder never reached its park"
            assert "error" not in hold.out, f"the holder raised {hold.out['error']!r}"
            # Preconditions. The variant's transaction is open or not as it
            # says, and a unique enqueue released the dead key and queued new
            # work, rather than handing back the dead row.
            assert hold.out["transaction_open"] is transaction_open
            if "refused" in hold.out:
                assert "unique" in str(hold.out["refused"]).lower()
            elif is_unique:
                assert hold.out["new_id"] != held_id
            held_locked = _row_lock_held(held_id)

            started = time.monotonic()
            tick_thread = _start_reconciler(tick)
            tick_thread.join(TICK_BOUND)
            stalled = tick_thread.is_alive()
            waits = _lock_waits() if stalled else []
            w2_during = stored(w2).status
            child_during = OxTask.objects.get(id=child_id).status
        finally:
            hold.release.set()
            if started is not None:
                released_after = time.monotonic() - started
            holder_thread.join(60)
            if tick_thread is not None:
                tick_thread.join(120)

    assert not holder_thread.is_alive(), "the holder did not finish after release"
    assert "error" not in hold.out, f"the holder raised {hold.out['error']!r}"
    assert tick_thread is not None and not tick_thread.is_alive()
    assert "error" not in tick, f"the reconciler raised {tick['error']!r}"
    if holder == "atomic-requests-view":
        assert hold.out["status_code"] == 200

    print(  # noqa: T201 - the observation is the evidence, read with -s
        f"\n[observe] {connections['default'].vendor} {holder}/{dead}: "
        f"dead unique row lock held by the holder={held_locked}; tick stalled past "
        f"{TICK_BOUND}s={stalled}; tick took {tick['elapsed']:.2f}s, holder "
        f"released {released_after:.2f}s after the tick started; W2 during="
        f"{w2_during}; W1 child during={child_during}; waiting={waits}"
    )

    # After the holder commits, the next tick finishes W1 as any correct
    # design must: its child is cancelled and it completes.
    reconcile()
    problems = []
    if stalled:
        problems.append(
            f"the reconciler tick was still running {TICK_BOUND:.0f}s after it "
            f"started, behind an application transaction ({holder}, dead root "
            f"{dead}); it returned only {tick['elapsed']:.2f}s in, after the "
            f"holder was released at {released_after:.2f}s. Dead unique row "
            f"locked by the holder: {held_locked}. Waiting on a lock: {waits}"
        )
    if w2_during != OxpullWorkflow.Status.COMPLETE:
        problems.append(
            f"unrelated workflow W2, whose only node had SUCCEEDED, was still "
            f"{w2_during} {TICK_BOUND:.0f}s into the tick; it sits behind W1 "
            f"in the sweep"
        )
    if stored(w2).status != OxpullWorkflow.Status.COMPLETE:
        problems.append(f"W2 is {stored(w2).status} after release and a second tick")
    child_after = OxTask.objects.get(id=child_id).status
    if (stored(w1).status, child_after) != (
        OxpullWorkflow.Status.COMPLETE,
        OxTask.Status.DISCARDED,
    ):
        problems.append(
            f"W1 is {stored(w1).status} with child {child_after} after release "
            f"and a second tick"
        )
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("dead", DEAD)
def test_a_second_reconciler_does_not_queue_behind_a_stalled_one(dead):
    """
    Running more reconcilers must not be the thing that fails too. A second
    tick started while the first is blocked has to be able to finish W2.
    """
    w1, _root_id, _child_id, held_id = _doomed_workflow(dead)
    w2 = _healthy_workflow()
    assert stored(w1).created_at < stored(w2).created_at

    hold = Hold()
    first: dict[str, Any] = {}
    second: dict[str, Any] = {}
    threads: list[threading.Thread] = []
    holder_thread = _start_holder(_enqueue_in_atomic, hold)
    try:
        assert hold.holding.wait(30), "the holder never reached its park"
        assert "error" not in hold.out, f"the holder raised {hold.out['error']!r}"
        assert hold.out["transaction_open"] is True
        assert hold.out["new_id"] != held_id

        threads.append(_start_reconciler(first))
        # Let the first tick reach W1. If nothing blocks it, it simply
        # finishes and the loop ends.
        deadline = time.monotonic() + TICK_BOUND
        while (
            time.monotonic() < deadline and threads[0].is_alive() and not _lock_waits()
        ):
            # Not faster: InnoDB refreshes information_schema.innodb_trx only
            # after 100 ms without a read, so a tighter poll keeps reading the
            # snapshot taken before the first tick blocked.
            time.sleep(0.25)
        first_waits = _lock_waits()

        threads.append(_start_reconciler(second))
        threads[1].join(TICK_BOUND)
        first_alive, second_alive = (thread.is_alive() for thread in threads)
        waits = _lock_waits() if (first_alive or second_alive) else []
        w2_during = stored(w2).status
    finally:
        hold.release.set()
        holder_thread.join(60)
        for thread in threads:
            thread.join(120)

    assert not holder_thread.is_alive()
    assert "error" not in hold.out, f"the holder raised {hold.out['error']!r}"
    assert all(not thread.is_alive() for thread in threads)
    assert "error" not in first and "error" not in second

    print(  # noqa: T201 - the observation is the evidence, read with -s
        f"\n[observe] {connections['default'].vendor} two reconcilers/{dead}: "
        f"first waiting when the second started={first_waits}; after "
        f"{TICK_BOUND}s first alive={first_alive}, second alive={second_alive}; "
        f"first took {first['elapsed']:.2f}s, second {second['elapsed']:.2f}s; "
        f"W2 during={w2_during}; waiting={waits}"
    )

    problems = []
    if second_alive:
        problems.append(
            f"a second reconciler started beside the stalled one was still "
            f"running {TICK_BOUND:.0f}s later (took {second['elapsed']:.2f}s); "
            f"waiting on a lock: {waits}"
        )
    if w2_during != OxpullWorkflow.Status.COMPLETE:
        problems.append(
            f"W2 was still {w2_during} with two reconcilers running; neither reached it"
        )
    assert not problems, "\n".join(problems)
