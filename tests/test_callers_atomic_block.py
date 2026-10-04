"""
run_once() inside its caller's atomic block, on a Worker other callers use.

Inside an atomic block the claim is the caller's uncommitted write, the task
runs inside the call, and the outcome is written in the same transaction. No
other connection can see the row's lease, and the caller's transaction holds
its lock, so no renewal names the row, Worker._renewable, and every other
row the Worker has in flight is renewed as if the call were not there. In
every other respect the row is in flight.

tests/test_inline_path.py covers two concurrent run_once() calls.
Here: what a renewal names, the loop, what a reaper did with the lease that
went unrenewed, what the caller's transaction publishes however the call
ends, and what is left on the Worker afterwards. Last, an override and a
wrapper of execute() written to its 1.7.0 signature, which run_once() calls
as 1.7.0 did: it tells execute() of its claim through an entry of its own,
which execute() takes in the step that puts the row in flight.
"""

import copy
import inspect
import logging
import threading
import time
import traceback
from datetime import timedelta

import pytest
from django.db import (
    DatabaseError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.worker import Worker, _Watch

from . import tasks
from .conftest import start_worker_thread, wait_for
from .dead_connection_tasks import from_another_connection
from .test_inline_path import renewal_loops

pytestmark = pytest.mark.django_db(transaction=True)

#: No test here waits on a lease unless it says so.
LOCK_TIMEOUT = 300.0

#: How long a thread here is waited for. Far past what a loaded machine needs.
LIMIT = 60.0

#: A row nobody has claimed, as (status, attempts, lease_epoch).
UNCLAIMED = (OxTask.Status.READY, 0, 0)

not_on_sqlite = pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite has one writer: while a caller's transaction is open, every "
    "other connection's write waits for it, whatever a renewal names",
)


class _Undo(Exception):
    """Raised at the end of a caller's block to roll it back."""


def _budget(settings, attempts):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"MAX_ATTEMPTS": attempts},
        }
    }


def _row(result):
    return OxTask.objects.get(id=result.id)


def _state(result):
    row = _row(result)
    return (row.status, row.attempts, row.lease_epoch)


def _as_others_see(result):
    """The row's state as a connection other than the caller's reads it."""
    seen = []

    def read(other):
        seen.append(
            OxTask.objects.using(other.alias)
            .values_list("status", "attempts", "lease_epoch")
            .get(id=result.id)
        )

    from_another_connection(read)
    return seen[0]


def _runs(label):
    return tasks.STATE.get("order", []).count(label)


def _events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def _names(sql, result):
    """Whether a statement names the row, however the database spells its key."""
    return str(result.id).replace("-", "") in sql.replace("-", "")


class Witness:
    """
    Stands in for a Worker's _in_flight_lock, and at every release checks
    what renewal relies on: an execution in flight whose claim was made
    inside a caller's atomic block is recorded as one, and nothing is
    recorded that is not in flight. `inside` is the rows the test claims
    inside an atomic block, by primary key, and the test may change it.
    `most_waiting` is the most entries run_once() had waiting for execute()
    at any release.
    """

    def __init__(self, worker):
        self._lock = threading.Lock()
        self.worker = worker
        self.inside = set()
        self.checks = 0
        self.recorded = set()
        self.most_waiting = 0
        self.violations = []

    def _check(self):
        worker = self.worker
        self.checks += 1
        self.recorded |= worker._in_callers_atomic_block
        self.most_waiting = max(
            self.most_waiting, len(worker._claimed_in_callers_atomic_block)
        )
        named = {
            pair
            for pair in worker._in_flight - worker._in_callers_atomic_block
            if pair[0] in self.inside
        }
        stray = worker._in_callers_atomic_block - worker._in_flight
        if named or stray:
            where = "".join(traceback.format_stack(limit=6))
            self.violations.append(f"named {named}, stray {stray}\n{where}")

    def acquire(self, *args, **kwargs):
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        self._check()
        self._lock.release()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        self.release()

    def settled(self):
        """No release of the lock saw either fault, and nothing is left now."""
        assert self.violations == [], self.violations[0]
        assert self.checks
        worker = self.worker
        assert (worker._in_flight, worker._in_callers_atomic_block) == (set(), set())
        assert worker._claimed_in_callers_atomic_block == {}


@pytest.fixture
def worker(settings):
    _budget(settings, 3)
    return Worker(backoff_initial=0, lock_timeout=LOCK_TIMEOUT)


@pytest.fixture
def witness(worker):
    witness = Witness(worker)
    worker._in_flight_lock = witness
    return witness


# -- what a renewal names --------------------------------------------------------


def test_a_renewal_made_while_the_call_runs_names_nothing(worker):
    """
    The row is in flight while its task runs, and no renewal names it: the
    statement another call's renewal thread, or the loop's, would make at
    that instant is not made at all. In 1.7.0 a renewal made here, on the
    caller's own connection, where the uncommitted claim can be seen,
    renewed the row.
    """
    result = tasks.with_hook.enqueue("inside")
    seen = {}

    def during():
        with worker._in_flight_lock:
            seen["in flight"] = set(worker._in_flight)
        with CaptureQueriesContext(connection) as statements:
            seen["renewed"] = worker.renew_leases()
        seen["statements"] = len(statements)

    tasks.STATE["hooks"] = {"inside": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert seen == {
        "in flight": {(_row(result).pk, 1)},
        "renewed": 0,
        "statements": 0,
    }
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


def test_a_renewal_names_every_other_row_in_flight(worker):
    """
    One task is running on the Worker outside any transaction when a call
    inside an atomic block claims another. A renewal made then names the
    first row and not the second.
    """
    other = tasks.record.enqueue("other")
    claimed = worker.claim_one()
    held = (claimed.pk, claimed.lease_epoch)
    with worker._in_flight_lock:
        worker._handed_off.discard(held)
        worker._in_flight.add(held)
    result = tasks.with_hook.enqueue("inside")
    seen = {}

    def during():
        with CaptureQueriesContext(connection) as statements:
            seen["renewed"] = worker.renew_leases()
        seen["sql"] = [statement["sql"] for statement in statements]

    tasks.STATE["hooks"] = {"inside": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert seen["renewed"] == 1
    (sql,) = seen["sql"]
    assert _names(sql, other)
    assert not _names(sql, result)


def test_a_row_claimed_again_inside_an_atomic_block_is_left_out_for_both(worker):
    """
    A row can be in flight twice on one Worker: an execution that has lost
    it and is still running, and the claim inside a caller's atomic block
    that took it since. The row is locked by that caller's transaction
    whichever of the two it is named for, so it is not named.
    """
    tasks.with_hook.enqueue("again")
    seen = {}

    def during():
        with worker._in_flight_lock:
            (pair,) = worker._in_flight
            lost = (pair[0], pair[1] - 1)
            worker._in_flight.add(lost)
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            worker._in_flight.discard(lost)

    tasks.STATE["hooks"] = {"again": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert seen == {"named": set()}


def test_a_later_claim_of_a_row_is_renewed_whatever_an_earlier_one_left(worker):
    """
    The reverse: an earlier claim of the row, made inside an atomic block,
    is still recorded when a later claim of it runs outside one. The later
    one holds the row, and the row is named.
    """
    result = tasks.with_hook.enqueue("later")
    seen = {}

    def during():
        with worker._in_flight_lock:
            (pair,) = worker._in_flight
            earlier = (pair[0], pair[1] - 1)
            worker._in_flight.add(earlier)
            worker._in_callers_atomic_block.add(earlier)
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            worker._in_flight.discard(earlier)
            worker._in_callers_atomic_block.discard(earlier)

    tasks.STATE["hooks"] = {"later": during}
    assert worker.run_once() is True

    assert seen == {"named": {_row(result).pk}}


def test_a_record_left_behind_does_not_leave_out_the_next_claim_of_the_pair(worker):
    """
    A claim that is rolled back gives its epoch back, so the next claim of
    the row is granted the same pair. Were the first one's record still
    there, the second, outside any transaction, would go unrenewed for as
    long as it ran. It takes the record back as it starts.
    """
    result = tasks.with_hook.enqueue("same pair")
    pk = _row(result).pk
    with worker._in_flight_lock:
        worker._in_callers_atomic_block.add((pk, 1))
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["recorded"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"same pair": during}
    assert worker.run_once() is True

    assert seen == {"named": {pk}, "recorded": set()}


# -- the Worker is shared ------------------------------------------------------------


def _held(*labels):
    """Events for tasks.hold: each label's task says it began, and waits."""
    began = tasks.STATE["hold_began"] = {label: threading.Event() for label in labels}
    release = tasks.STATE["hold_release"] = {
        label: threading.Event() for label in labels
    }
    return began, release


def _renewals_landing(result, count=2):
    """
    How many of the next `count` renewals of the row landed, each waited
    for. A renewal already under way when another row joined the Worker's
    can land once more without having had it to consider, so only the
    second proves a renewal that had both.
    """
    landed = 0
    for _ in range(count):
        seen = _row(result).locked_at
        if not wait_for(lambda seen=seen: _row(result).locked_at > seen):
            break
        landed += 1
    return landed


def _inside_an_atomic_block(worker, returned, label="inside"):
    """A thread that calls worker.run_once() inside a block of its own."""

    def call():
        try:
            with transaction.atomic():
                returned[label] = worker.run_once()
        except BaseException as exc:
            returned[label] = exc
        finally:
            connections.close_all()

    return threading.Thread(target=call, name=f"caller-{label}", daemon=True)


@not_on_sqlite
def test_the_loops_task_keeps_its_lease_while_a_call_is_inside_an_atomic_block(
    settings,
):
    """
    run() has a task in flight on its pool, and another thread calls
    run_once() on the same Worker inside an atomic block. The loop's
    renewal thread is there for the loop's task, and nothing the call
    holds may keep that renewal from landing.
    """
    _budget(settings, 3)
    # A lease far longer than the test, so none expires and only renewal
    # moves locked_at; an interval short enough to see several land.
    worker = Worker(
        backoff_initial=0,
        lock_timeout=60,
        renew_interval=0.05,
        poll_interval=0.05,
        concurrency=1,
    )
    began, release = _held("loop", "inside")
    on_the_loop = tasks.hold.enqueue("loop")
    inside = None
    returned = {}
    caller = _inside_an_atomic_block(worker, returned)
    loop = start_worker_thread(worker)
    landed = 0
    try:
        assert began["loop"].wait(LIMIT), "the loop's task never began"
        # The loop's one slot is taken, so it claims nothing more, and the
        # call claims this.
        inside = tasks.hold.enqueue("inside")
        caller.start()
        assert began["inside"].wait(LIMIT), "the call's task never began"
        landed = _renewals_landing(on_the_loop)
    finally:
        # Release the call inside the transaction first so cleanup can finish
        # even if a renewal regression makes other writes wait on its lock.
        release["inside"].set()
        if caller.ident is not None:
            caller.join(LIMIT)
        release["loop"].set()
        worker.request_stop()
        loop.join(LIMIT)

    assert landed == 2, (
        "the loop's task stopped having its lease renewed while the call's "
        "transaction was open"
    )
    assert returned == {"inside": True}
    assert sorted(tasks.STATE["order"]) == ["inside", "loop"]
    for result in (on_the_loop, inside):
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


@not_on_sqlite
def test_a_task_that_calls_run_once_in_a_block_of_its_own_keeps_its_lease(settings):
    """
    The same on one thread: a task the loop is running opens a block and
    calls run_once() on the Worker that runs it. The second claim is that
    block's uncommitted write on the task thread's connection. The first is
    committed, and is the one the loop's renewal is there for.
    """
    _budget(settings, 3)
    worker = Worker(
        backoff_initial=0,
        lock_timeout=60,
        renew_interval=0.05,
        poll_interval=0.05,
        concurrency=1,
    )
    began, release = _held("inner")
    returned = []

    def call_inside_a_block():
        with transaction.atomic():
            returned.append(worker.run_once())

    tasks.STATE["hooks"] = {"outer": call_inside_a_block}
    # The higher priority is claimed first, by the loop, whose one slot is
    # then taken: the task's own call claims the other.
    outer = tasks.with_hook.using(priority=2).enqueue("outer")
    inner = tasks.hold.using(priority=1).enqueue("inner")
    loop = start_worker_thread(worker)
    landed = 0
    try:
        assert began["inner"].wait(LIMIT), "the task's own call never claimed"
        landed = _renewals_landing(outer)
    finally:
        release["inner"].set()
        worker.request_stop()
        loop.join(LIMIT)

    assert landed == 2, (
        "the task stopped having its lease renewed while the block it "
        "called run_once() in was open"
    )
    assert returned == [True]
    assert tasks.STATE["order"] == ["outer", "inner"]
    for result in (outer, inner):
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


@not_on_sqlite
def test_a_reaper_finds_nothing_to_take_from_the_other_call(settings):
    """
    What the renewal that never landed cost in 1.7.0: the lease of the call
    outside the transaction ran out under its running task, a reaper
    requeued the row, and another worker ran the body a second time.

    No clock is waited on. The lease is far longer than the test, and the
    test ages it itself, as if the lock timeout had passed since the claim.
    A renewal that lands puts that right within one interval, and then a
    second Worker reaps, and claims whatever it can.

    The row claimed inside the atomic block is the one that sorts first by
    primary key. A renewal that names both, as 1.7.0's did, waits on it
    before it locks the other, so neither the ageing here nor the reaper's
    requeue waits behind that renewal. With the rows the other way round
    both would first wait out the renewal's own lock wait, most of a minute
    on MySQL's defaults.
    """
    _budget(settings, 3)
    worker = Worker(backoff_initial=0, lock_timeout=60, renew_interval=0.05)
    reaper = Worker(backoff_initial=0, lock_timeout=60)
    labels = ("inside", "outside")
    began, release = _held(*labels)
    first, second = sorted(
        (tasks.hold.enqueue("either"), tasks.hold.enqueue("either")),
        key=lambda result: str(result.id).replace("-", ""),
    )
    rows = {"inside": first, "outside": second}
    # The higher priority is claimed first, so each call claims its own.
    for label, priority in (("inside", 2), ("outside", 1)):
        OxTask.objects.filter(id=rows[label].id).update(priority=priority, args=[label])
    returned = {}

    def call_outside():
        try:
            returned["outside"] = worker.run_once()
        except BaseException as exc:
            returned["outside"] = exc
        finally:
            connections.close_all()

    threads = {
        "inside": _inside_an_atomic_block(worker, returned),
        "outside": threading.Thread(target=call_outside, daemon=True),
    }
    renewed = reclaimed = ran_again = None
    try:
        for label in labels:
            threads[label].start()
            assert began[label].wait(LIMIT), f"the {label} call never began"
        stale = timezone.now() - timedelta(seconds=120)
        OxTask.objects.filter(id=rows["outside"].id).update(
            locked_at=stale, lease_expires_at=stale
        )
        renewed = wait_for(lambda: _row(rows["outside"]).locked_at > stale)
        reclaimed = reaper.reap()
    finally:
        release["outside"].set()
        if threads["outside"].ident is not None:
            threads["outside"].join(LIMIT)
        # Its release is set, so a second run of the body returns at once.
        ran_again = reaper.run_once()
        release["inside"].set()
        if threads["inside"].ident is not None:
            threads["inside"].join(LIMIT)

    # Together, so a failure shows the requeue and the second run it led to.
    assert (renewed, reclaimed, ran_again, sorted(tasks.STATE["order"])) == (
        True,
        0,
        False,
        ["inside", "outside"],
    ), (
        "the lease of the call outside the transaction was not renewed while "
        "the other call's transaction was open: a reaper requeued the row "
        "and its body ran again"
    )
    assert returned == {"inside": True, "outside": True}
    for label in labels:
        assert _state(rows[label]) == (OxTask.Status.SUCCESSFUL, 1, 1), label


@not_on_sqlite
def test_another_workers_claim_inside_an_atomic_block_is_not_this_workers_to_name(
    settings,
):
    """
    Two Workers, as in a fleet where one process runs an earlier release:
    the rows a call inside an atomic block writes are the same either way.
    One has a claim open inside a caller's atomic block, and the other's
    task is renewed, because a Worker names only rows of its own.
    """
    _budget(settings, 3)
    theirs = Worker(backoff_initial=0, lock_timeout=60)
    mine = Worker(backoff_initial=0, lock_timeout=60, renew_interval=0.05)
    began, release = _held("inside", "outside")
    rows = {
        label: tasks.hold.using(priority=priority).enqueue(label)
        for label, priority in (("inside", 2), ("outside", 1))
    }
    returned = {}

    def call_outside():
        try:
            returned["outside"] = mine.run_once()
        except BaseException as exc:
            returned["outside"] = exc
        finally:
            connections.close_all()

    threads = {
        "inside": _inside_an_atomic_block(theirs, returned),
        "outside": threading.Thread(target=call_outside, daemon=True),
    }
    landed = 0
    try:
        for label in ("inside", "outside"):
            threads[label].start()
            assert began[label].wait(LIMIT), f"the {label} call never began"
        landed = _renewals_landing(rows["outside"])
    finally:
        for label in ("inside", "outside"):
            release[label].set()
            if threads[label].ident is not None:
                threads[label].join(LIMIT)

    assert landed == 2
    assert returned == {"inside": True, "outside": True}
    for label in ("inside", "outside"):
        assert _state(rows[label]) == (OxTask.Status.SUCCESSFUL, 1, 1), label


# -- what the caller's transaction publishes -----------------------------------------


def test_a_claim_two_blocks_deep_is_published_with_its_outcome_and_not_before(
    worker, witness
):
    """
    Nested blocks: the claim and the outcome are written two blocks deep,
    and neither leaving the inner block nor anything before the outer one
    commits shows another connection a claim. What the commit publishes is
    the finished row.
    """
    result = tasks.with_hook.enqueue("nested")
    pk = _row(result).pk
    witness.inside = {pk}
    seen = {}

    def during():
        seen["while it runs"] = _as_others_see(result)
        seen["named"] = worker._renewable()

    tasks.STATE["hooks"] = {"nested": during}
    with transaction.atomic():
        with transaction.atomic():
            assert worker.run_once() is True
            assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
            seen["after the call"] = _as_others_see(result)
        seen["after the inner block"] = _as_others_see(result)

    assert seen == {
        "while it runs": UNCLAIMED,
        "named": set(),
        "after the call": UNCLAIMED,
        "after the inner block": UNCLAIMED,
    }
    assert _as_others_see(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert witness.recorded == {(pk, 1)}
    witness.settled()


def test_a_savepoint_rolled_back_takes_the_claim_and_the_outcome_with_it(
    worker, witness
):
    """
    The inner block rolls back after the call returned: the claim and the
    outcome go together, the row is as it was before the claim, and that is
    what the outer commit publishes. The next claim of the row is granted
    the same epoch, outside any transaction this time, and is renewed.
    """
    result = tasks.with_hook.enqueue("undone")
    pk = _row(result).pk
    witness.inside = {pk}
    with transaction.atomic():
        with pytest.raises(_Undo), transaction.atomic():
            assert worker.run_once() is True
            assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
            raise _Undo
        assert _state(result) == UNCLAIMED
        assert _as_others_see(result) == UNCLAIMED
    assert _as_others_see(result) == UNCLAIMED
    witness.settled()
    assert worker._unsettled == set()

    witness.inside = set()
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        seen["in flight"] = set(worker._in_flight)

    tasks.STATE["hooks"] = {"undone": during}
    assert worker.run_once() is True

    assert seen == {"named": {pk}, "in flight": {(pk, 1)}}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert _runs("undone") == 2
    witness.settled()


#: How an attempt ends with its outcome recorded: what the task raises, the
#: attempts the row may have, and the row the outcome leaves.
ENDS = {
    "succeeds": (None, 3, (OxTask.Status.SUCCESSFUL, 1, 1)),
    "fails and is retried": (ValueError, 3, (OxTask.Status.READY, 1, 1)),
    "fails for good": (ValueError, 1, (OxTask.Status.FAILED, 1, 1)),
    "times out": (TaskTimeout, 3, (OxTask.Status.READY, 1, 1)),
}


@pytest.mark.parametrize("end", list(ENDS))
def test_the_commit_publishes_the_outcome_and_never_the_claim(end, settings):
    """
    However the attempt ends, its outcome is written in the caller's
    transaction before run_once() returns. No other connection sees the
    claim while the task runs or after the call, and the commit publishes
    the row with its outcome on it.
    """
    raises, attempts, outcome = ENDS[end]
    _budget(settings, attempts)
    worker = Worker(backoff_initial=0, lock_timeout=LOCK_TIMEOUT)
    witness = worker._in_flight_lock = Witness(worker)
    result = tasks.with_hook.enqueue("ends")
    pk = _row(result).pk
    witness.inside = {pk}
    seen = {}

    def during():
        seen["while it runs"] = _as_others_see(result)
        seen["named"] = worker._renewable()
        if raises is not None:
            raise raises("raised by the task")

    tasks.STATE["hooks"] = {"ends": during}
    with transaction.atomic():
        assert worker.run_once() is True
        assert _state(result) == outcome
        seen["after the call"] = _as_others_see(result)

    assert seen == {
        "while it runs": UNCLAIMED,
        "named": set(),
        "after the call": UNCLAIMED,
    }
    assert _as_others_see(result) == outcome
    assert _runs("ends") == 1
    assert witness.recorded == {(pk, 1)}
    assert worker._unsettled == set()
    witness.settled()


def test_a_retry_claimed_in_the_same_block_is_left_out_under_its_new_epoch(
    worker, witness
):
    """
    The first attempt fails and the row is READY again inside the caller's
    transaction, where the next call claims it at the next epoch. Each claim
    is left out while it runs, neither is ever seen from outside, and the
    commit publishes the second attempt's outcome.
    """
    result = tasks.with_hook.enqueue("retried")
    pk = _row(result).pk
    witness.inside = {pk}
    attempts = []

    def during():
        with worker._in_flight_lock:
            recorded = set(worker._in_callers_atomic_block)
        attempts.append((recorded, worker._renewable(), _as_others_see(result)))
        if len(attempts) == 1:
            raise RuntimeError("the first attempt fails")

    tasks.STATE["hooks"] = {"retried": during}
    with transaction.atomic():
        assert worker.run_once() is True
        assert _state(result) == (OxTask.Status.READY, 1, 1)
        assert worker.run_once() is True
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 2, 2)
        assert _as_others_see(result) == UNCLAIMED

    assert attempts == [
        ({(pk, 1)}, set(), UNCLAIMED),
        ({(pk, 2)}, set(), UNCLAIMED),
    ]
    assert _as_others_see(result) == (OxTask.Status.SUCCESSFUL, 2, 2)
    witness.settled()


@pytest.mark.parametrize("how", ["commit", "rollback", "set_autocommit"])
def test_django_refuses_a_task_that_would_end_the_callers_transaction(
    how, worker, witness
):
    """
    What keeps the claim uncommitted for as long as its task runs: inside
    an atomic block Django refuses a commit, a rollback and a change of
    autocommit. The attempt fails with that refusal, recorded in the
    caller's transaction like any failure, and no other connection sees a
    claim at any point.
    """
    result = tasks.with_hook.enqueue("ends it")
    witness.inside = {_row(result).pk}
    seen = {}

    def during():
        try:
            if how == "set_autocommit":
                transaction.set_autocommit(True)
            else:
                getattr(transaction, how)()
        finally:
            seen["while it runs"] = _as_others_see(result)

    tasks.STATE["hooks"] = {"ends it": during}
    with transaction.atomic():
        assert worker.run_once() is True
        seen["after the call"] = _as_others_see(result)

    assert seen == {"while it runs": UNCLAIMED, "after the call": UNCLAIMED}
    row = _row(result)
    assert (row.status, row.attempts, row.lease_epoch) == (OxTask.Status.READY, 1, 1)
    assert row.errors[-1]["exception_class_path"] == (
        "django.db.transaction.TransactionManagementError"
    )
    witness.settled()


class RecordsNoOutcome(Worker):
    """
    While `how` is set, the outcome write does not complete:
    "declined" simulates an override that fails closed without writing;
    "raised" simulates an error that leaves the connection usable.
    """

    how = None

    def _write_outcome(self, db_task, **kwargs):
        if self.how == "declined":
            return False
        if self.how == "raised":
            raise DatabaseError("refused, and the connection still answers")
        return super()._write_outcome(db_task, **kwargs)


#: A call that ends with no outcome on the row, and what run_once() raises.
NO_OUTCOME = {
    "declined": None,
    "raised": DatabaseError,
    "interrupted": KeyboardInterrupt,
    "exited": SystemExit,
}


@pytest.mark.parametrize("how", list(NO_OUTCOME))
def test_a_claim_published_without_an_outcome_is_nobodys_running_task(
    how, settings, caplog
):
    """
    The ways the caller's commit can publish a claim with no outcome on it:
    the worker's write declined, as an override that fails closed does; the
    write raised and the caller carried on; or the task raised
    KeyboardInterrupt or SystemExit and the caller caught it inside its
    block. In each the call is over, and so is the body. The row is RUNNING
    under this Worker with the lease its claim granted, which nothing
    renews, no look may refund it, and a reaper requeues it once that lease
    has run out. The next claim of it on the same Worker, outside any
    transaction, is renewed.

    The write that raised is raised here by the override. An error the
    server raised would leave PostgreSQL refusing the commit as well.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    worker = RecordsNoOutcome(backoff_initial=0, lock_timeout=LOCK_TIMEOUT)
    witness = worker._in_flight_lock = Witness(worker)
    result = tasks.with_hook.enqueue("unrecorded")
    pk = _row(result).pk
    witness.inside = {pk}
    raised = NO_OUTCOME[how]

    def during():
        if how in ("interrupted", "exited"):
            raise raised("aimed at the process")

    tasks.STATE["hooks"] = {"unrecorded": during}
    worker.how = how
    with transaction.atomic():
        if raised is None:
            assert worker.run_once() is True
        else:
            with pytest.raises(raised):
                worker.run_once()
        witness.settled()
        assert worker._unsettled == {(pk, 1)}
        assert _as_others_see(result) == UNCLAIMED

    row = _row(result)
    assert (row.status, row.locked_by, row.attempts, row.lease_epoch) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
        1,
    )
    assert _runs("unrecorded") == 1
    # Nothing renews it: its execution is over.
    assert worker._renewable() == set()
    assert worker.renew_leases() == 0
    assert _row(result).locked_at == row.locked_at
    # No look refunds it: its body ran.
    worker._claim_unconfirmed_at = time.monotonic()
    assert worker._recover_claims() is True
    assert _events(caplog, "worker_claim_released") == []
    assert _state(result) == (OxTask.Status.RUNNING, 1, 1)
    # A reaper requeues it once the lease its claim granted has run out.
    stale = timezone.now() - timedelta(seconds=LOCK_TIMEOUT + 10)
    OxTask.objects.filter(pk=pk).update(locked_at=stale, lease_expires_at=stale)
    assert Worker(lock_timeout=LOCK_TIMEOUT).reap() == 1
    assert _state(result) == (OxTask.Status.READY, 1, 2)

    worker.how = None
    witness.inside = set()
    seen = {}

    def again():
        seen["named"] = worker._renewable()
        seen["in flight"] = set(worker._in_flight)

    tasks.STATE["hooks"] = {"unrecorded": again}
    assert worker.run_once() is True

    assert seen == {"named": {pk}, "in flight": {(pk, 3)}}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 2, 3)
    assert _runs("unrecorded") == 2
    witness.settled()


@pytest.mark.parametrize("aimed", [KeyboardInterrupt, SystemExit])
def test_an_exception_aimed_at_the_process_takes_the_claim_with_it(
    aimed, worker, witness
):
    """
    Raised by the task, it reaches the caller, and on its way out of their
    block it rolls the claim back: nothing is published. The next claim of
    the row is granted the same pair, outside any transaction this time,
    and is renewed.
    """
    result = tasks.with_hook.enqueue("aimed")
    pk = _row(result).pk
    witness.inside = {pk}

    def during():
        raise aimed("aimed at the process")

    tasks.STATE["hooks"] = {"aimed": during}
    with pytest.raises(aimed), transaction.atomic():
        worker.run_once()

    assert _as_others_see(result) == UNCLAIMED
    assert worker._unsettled == {(pk, 1)}
    witness.settled()

    witness.inside = set()
    seen = {}

    def again():
        seen["named"] = worker._renewable()

    tasks.STATE["hooks"] = {"aimed": again}
    assert worker.run_once() is True

    assert seen == {"named": {pk}}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert worker._unsettled == set()
    witness.settled()


# -- the looks and the watchdog still see the row ------------------------------------


@not_on_sqlite
def test_a_look_made_while_the_call_runs_releases_only_the_orphan(worker, caplog):
    """
    A look for a claim that raised, made on another thread while the call's
    task runs. The call's row is in flight, so the look knows it; the row a
    claim of this Worker's left RUNNING without returning is the only one
    it releases.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    orphan = tasks.record.enqueue("orphan")
    claimed = worker.claim_one()
    assert str(claimed.id) == str(orphan.id)
    with worker._in_flight_lock:
        worker._handed_off.clear()
    worker._claim_unconfirmed_at = time.monotonic()
    inside = tasks.with_hook.enqueue("inside")
    looked = []

    def during():
        with worker._in_flight_lock:
            looked.append(set(worker._in_flight))
        from_another_connection(lambda other: looked.append(worker._recover_claims()))

    tasks.STATE["hooks"] = {"inside": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert looked == [{(_row(inside).pk, 1)}, True]
    released = [r.task_id for r in _events(caplog, "worker_claim_released")]
    assert released == [str(orphan.id)]
    assert _state(orphan) == (OxTask.Status.READY, 0, 2)
    assert _state(inside) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert _runs("inside") == 1
    assert worker._claim_unconfirmed_at is None


def test_an_attempt_the_watchdog_gave_up_on_is_taken_out_of_both_sets(
    worker, monkeypatch
):
    """
    The watchdog gives up on an attempt whose thread is still in the body,
    inside the caller's transaction. It takes the attempt out of the
    in-flight set, and its record of being inside a caller's atomic block
    with it; the row stays known as unsettled. When the thread comes back
    its own record stands, in the caller's transaction.

    Worker._handle_stuck is called here as the watchdog's thread calls it
    once a grace has passed, so no timeout is waited for. Its stuck record
    is a write on a connection of the watchdog's own, which waits on the
    caller's transaction, on MySQL and SQLite for the whole of their lock
    waits; here it fails at once, as that wait ends.
    """
    record = worker._handle_failure

    def handle_failure(db_task, exc, duration_ms, *, release=False):
        if release:
            raise OperationalError("the stuck record could not be written")
        return record(db_task, exc, duration_ms)

    monkeypatch.setattr(worker, "_handle_failure", handle_failure)
    result = tasks.with_hook.enqueue("stuck")
    began, release = threading.Event(), threading.Event()
    seen = {}

    def during():
        seen["ident"] = threading.get_ident()
        # The row as the caller's transaction holds it: claimed.
        seen["db_task"] = _row(result)
        began.set()
        release.wait(LIMIT)

    tasks.STATE["hooks"] = {"stuck": during}
    returned = {}
    caller = _inside_an_atomic_block(worker, returned, "stuck")
    caller.start()
    try:
        assert began.wait(LIMIT), "the call's task never began"
        db_task = seen["db_task"]
        held = (db_task.pk, db_task.lease_epoch)
        with worker._in_flight_lock:
            before = (set(worker._in_flight), set(worker._in_callers_atomic_block))
        now = time.monotonic()
        worker._handle_stuck(
            _Watch(
                ident=seen["ident"],
                db_task=copy.copy(db_task),
                attempt=held,
                timeout=1.0,
                started=now,
                deadline=now,
                deadline_at=timezone.now(),
                injectable=True,
            )
        )
        with worker._in_flight_lock:
            after = (
                set(worker._in_flight),
                set(worker._in_callers_atomic_block),
                set(worker._unsettled),
            )
    finally:
        release.set()
        caller.join(LIMIT)

    assert held[1] == 1
    assert before == ({held}, {held})
    assert after == (set(), set(), {held})
    assert worker.recycling
    assert returned == {"stuck": True}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert (worker._in_flight, worker._in_callers_atomic_block) == (set(), set())
    assert worker._unsettled == set()


# -- an override or a wrapper of execute() with its 1.7.0 signature ------------------


def test_execute_keeps_its_1_7_0_signature():
    """
    Worker.execute(db_task, *, inline=False), as in 1.7.0. An override run
    as WORKER_CLASS, or a wrapper, written to it is never called with an
    argument it does not take.
    """
    empty = inspect.Parameter.empty
    parameters = inspect.signature(Worker.execute).parameters.values()
    assert [(p.name, p.kind, p.default) for p in parameters] == [
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD, empty),
        ("db_task", inspect.Parameter.POSITIONAL_OR_KEYWORD, empty),
        ("inline", inspect.Parameter.KEYWORD_ONLY, False),
    ]


class _Refused(Exception):
    """Raised by an override, before or after the base execute()."""


class Overrides(Worker):
    """
    A Worker subclass, as WORKER_CLASS runs one, whose execute() has exactly
    the 1.7.0 signature and forwards to the base execute(). It notes each
    call. A test may set `ahead`, called with the row before the base
    execute() and standing for an override that returns without calling it
    when it returns True, and `behind`, called with the row after it.
    """

    ahead = None
    behind = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []

    def execute(self, db_task, *, inline=False):
        self.calls.append((db_task.pk, inline))
        if self.ahead is not None and self.ahead(db_task):
            return
        super().execute(db_task, inline=inline)
        if self.behind is not None:
            self.behind(db_task)


@pytest.fixture
def overrides(settings):
    _budget(settings, 3)
    return Overrides(backoff_initial=0, lock_timeout=LOCK_TIMEOUT)


def test_an_override_with_the_1_7_0_signature_runs_the_claim_and_leaves_it_out(
    overrides,
):
    """
    run_once() inside an atomic block on the worker's database, through the
    override. The task runs, inline, and while it runs its row is in flight
    and left out: a renewal made then makes no statement at all. Nothing is
    left waiting afterwards.
    """
    worker = overrides
    witness = worker._in_flight_lock = Witness(worker)
    result = tasks.with_hook.enqueue("override")
    pk = _row(result).pk
    witness.inside = {pk}
    seen = {}

    def during():
        with worker._in_flight_lock:
            seen["in flight"] = set(worker._in_flight)
            seen["left out"] = set(worker._in_callers_atomic_block)
            seen["waiting"] = dict(worker._claimed_in_callers_atomic_block)
        with CaptureQueriesContext(connection) as statements:
            seen["renewed"] = worker.renew_leases()
        seen["statements"] = len(statements)

    tasks.STATE["hooks"] = {"override": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert seen == {
        "in flight": {(pk, 1)},
        "left out": {(pk, 1)},
        "waiting": {},
        "renewed": 0,
        "statements": 0,
    }
    assert worker.calls == [(pk, True)]
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    witness.settled()


@pytest.fixture
def wrapped_execute(monkeypatch):
    """
    Wrap Worker.execute using its 1.7.0 signature. Run the real method
    inside captureOnCommitCallbacks so its commit callbacks run when it
    returns, without committing the test transaction. Returns the calls it
    saw.
    """
    real = Worker.execute
    calls = []

    def execute(self, db_task, *, inline=False):
        calls.append((db_task.pk, inline))
        with TestCase.captureOnCommitCallbacks(using=self._db_alias, execute=True):
            return real(self, db_task, inline=inline)

    monkeypatch.setattr(Worker, "execute", execute)
    return calls


@pytest.mark.django_db
def test_a_wrapper_with_the_1_7_0_signature_under_the_default_db_fixture(
    wrapped_execute, settings
):
    """
    pytest-django's default database fixture, not this module's: the test
    runs inside an atomic block on the worker's database, so run_once()
    claims inside its caller's atomic block with no block of the test's
    own. The wrapper is called as in 1.7.0, the commit callback the task
    registered runs as the pass returns, and the row is left out while the
    task runs.
    """
    assert connection.in_atomic_block, "pytest-django's own block is not open"
    _budget(settings, 3)
    worker = Worker(backoff_initial=0, lock_timeout=LOCK_TIMEOUT)
    result = tasks.with_hook.enqueue("wrapped")
    pk = _row(result).pk
    seen = {}

    def during():
        transaction.on_commit(lambda: seen.setdefault("on commit", "ran"))
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["left out"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"wrapped": during}
    assert worker.run_once() is True

    assert wrapped_execute == [(pk, True)]
    assert seen == {"named": set(), "left out": {(pk, 1)}, "on commit": "ran"}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert worker._claimed_in_callers_atomic_block == {}
    assert (worker._in_flight, worker._in_callers_atomic_block) == (set(), set())


class Lockstep:
    """
    Stands in for a Worker's _in_flight_lock, for calls made on the thread
    that built it. Each time that thread lets the lock go, another thread
    renews before the first can go on: it asks for the rows a renewal
    names, Worker._renewable, as the renewal thread of another call or of
    run() would, then reads what is in flight and what run_once() has
    waiting for execute(). So a renewal is attempted at every instant at
    which another thread could see the call's bookkeeping, and nothing
    moves while it is. `seen` is what each saw.
    """

    def __init__(self, worker):
        self.worker = worker
        self.caller = threading.get_ident()
        self.seen = []
        self.late = 0
        self._lock = threading.Lock()
        self._turn = threading.Semaphore(0)
        self._done = threading.Semaphore(0)
        self._over = False
        self._renewer = threading.Thread(target=self._renew, daemon=True)
        self._renewer.start()

    def acquire(self, *args, **kwargs):
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        self._lock.release()
        if threading.get_ident() == self.caller and not self._over:
            self._turn.release()
            if not self._done.acquire(timeout=LIMIT):
                self.late += 1

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()

    def _renew(self):
        worker = self.worker
        while True:
            self._turn.acquire()
            if self._over:
                return
            try:
                named = worker._renewable()
                with self:
                    in_flight = set(worker._in_flight)
                    waiting = set(worker._claimed_in_callers_atomic_block.values())
                self.seen.append((named, in_flight, waiting))
            except Exception as exc:
                self.seen.append(exc)
            finally:
                self._done.release()

    def close(self):
        self._over = True
        self._turn.release()
        self._renewer.join(LIMIT)


def test_a_renewal_at_any_instant_of_the_call_never_names_its_row(overrides):
    """
    Controlled: a renewal is attempted on another thread after every step
    in which the call's thread held the in-flight lock, from the claim's
    hand-off to the drop of the call's entry, and the call waits for it.
    One row of the Worker's is in flight outside any transaction throughout,
    and every renewal names it. None names the call's row: not while its
    entry waits for execute(), not after the step that puts it in flight,
    in which the entry is taken, and not after it is out of flight. No
    renewal saw the row in flight with its entry still waiting, or out of
    flight with its entry gone, between those two.
    """
    worker = overrides
    tasks.record.enqueue("other")
    other = worker.claim_one()
    with worker._in_flight_lock:
        worker._handed_off.discard((other.pk, other.lease_epoch))
        worker._in_flight.add((other.pk, other.lease_epoch))
    result = tasks.with_hook.enqueue("stepped")
    pk = _row(result).pk
    lockstep = worker._in_flight_lock = Lockstep(worker)
    try:
        with transaction.atomic():
            assert worker.run_once() is True
    finally:
        lockstep.close()

    assert lockstep.late == 0
    assert all(seen[0] == {other.pk} for seen in lockstep.seen), lockstep.seen
    # Each instant as (the call's row in flight, entries waiting), once per
    # change: the hand-off, the entry added, the step that takes it and puts
    # the row in flight, the step that takes the row out.
    entry = (threading.get_ident(), pk, 1)
    states = [
        ((pk, 1) in in_flight, waiting) for _, in_flight, waiting in lockstep.seen
    ]
    changes = [
        state for i, state in enumerate(states) if not i or states[i - 1] != state
    ]
    assert changes == [
        (False, set()),
        (False, {entry}),
        (True, set()),
        (False, set()),
    ]
    assert worker.calls == [(pk, True)]
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert worker._claimed_in_callers_atomic_block == {}


def _ahead_returns(db_task):
    return True


def _refuse(db_task):
    raise _Refused("refused by the override")


#: How a call through the override can end: the override's `ahead` and
#: `behind`, what the task raises, and what run_once() raises.
OVERRIDE_ENDS = {
    "returns before the base execute()": (_ahead_returns, None, None, None),
    "raises before the base execute()": (_refuse, None, None, _Refused),
    "raises after the base execute()": (None, _refuse, None, _Refused),
    "the task raises": (None, None, ValueError, None),
    "the task raises past execute()": (
        None,
        None,
        KeyboardInterrupt,
        KeyboardInterrupt,
    ),
}


@pytest.mark.parametrize("end", list(OVERRIDE_ENDS))
def test_however_the_override_ends_no_entry_outlives_the_call(end, overrides):
    """
    The call's entry is gone once it returns or raises, whether the base
    execute() took it, the override never called it, or it raised. The
    caller's block is rolled back, so the row's next claim, on this thread
    and outside any transaction, is granted the same pair: it is renewed
    while it runs, which an entry left behind would have prevented.
    """
    ahead, behind, task_raises, raised = OVERRIDE_ENDS[end]
    worker = overrides
    witness = worker._in_flight_lock = Witness(worker)
    result = tasks.with_hook.enqueue("ends")
    pk = _row(result).pk
    witness.inside = {pk}

    def during():
        if task_raises is not None:
            raise task_raises("raised by the task")

    tasks.STATE["hooks"] = {"ends": during}
    worker.ahead, worker.behind = ahead, behind
    with pytest.raises(_Undo), transaction.atomic():
        if raised is None:
            assert worker.run_once() is True
        else:
            with pytest.raises(raised):
                worker.run_once()
        with worker._in_flight_lock:
            left = (
                dict(worker._claimed_in_callers_atomic_block),
                set(worker._in_flight),
                set(worker._in_callers_atomic_block),
            )
        raise _Undo
    assert left == ({}, set(), set())
    assert _state(result) == UNCLAIMED
    witness.settled()

    worker.ahead = worker.behind = None
    witness.inside = set()
    seen = {}

    def again():
        seen["named"] = worker._renewable()
        seen["in flight"] = set(worker._in_flight)

    tasks.STATE["hooks"] = {"ends": again}
    assert worker.run_once() is True

    assert seen == {"named": {pk}, "in flight": {(pk, 1)}}
    assert worker.calls == [(pk, True), (pk, True)]
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    witness.settled()


def test_a_call_from_inside_the_task_takes_its_own_entry(overrides):
    """
    Nested dispatch: the task of a call made inside an atomic block opens a
    block of its own and calls run_once() again, on the same thread. The
    outer entry was taken as the outer row went in flight, the inner call
    adds its own and its execution takes it, and both rows are left out
    while the inner task runs.
    """
    worker = overrides
    witness = worker._in_flight_lock = Witness(worker)
    outer = tasks.with_hook.using(priority=2).enqueue("outer")
    inner = tasks.with_hook.using(priority=1).enqueue("inner")
    pks = (_row(outer).pk, _row(inner).pk)
    witness.inside = set(pks)
    seen = {}

    def call_again():
        with transaction.atomic():
            seen["returned"] = worker.run_once()

    def inside():
        with worker._in_flight_lock:
            seen["in flight"] = set(worker._in_flight)
            seen["left out"] = set(worker._in_callers_atomic_block)
            seen["waiting"] = dict(worker._claimed_in_callers_atomic_block)
        seen["named"] = worker._renewable()

    tasks.STATE["hooks"] = {"outer": call_again, "inner": inside}
    with transaction.atomic():
        assert worker.run_once() is True

    both = {(pk, 1) for pk in pks}
    assert seen == {
        "returned": True,
        "in flight": both,
        "left out": both,
        "waiting": {},
        "named": set(),
    }
    assert worker.calls == [(pks[0], True), (pks[1], True)]
    for result in (outer, inner):
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    witness.settled()


def test_a_call_nested_ahead_of_the_base_execute_leaves_the_outer_entry_alone(
    overrides,
):
    """
    Nested dispatch while an entry waits: the override calls run_once()
    again, in the same block, before it calls the base execute() for its
    own claim. The nested call adds an entry of its own beside the outer
    one, its execution takes that one, and it drops nothing of the outer
    call's. The outer execution then takes its own, and each row is left
    out while it runs.
    """
    worker = overrides
    witness = worker._in_flight_lock = Witness(worker)
    outer = tasks.with_hook.using(priority=2).enqueue("outer")
    inner = tasks.with_hook.using(priority=1).enqueue("inner")
    pks = {"outer": _row(outer).pk, "inner": _row(inner).pk}
    witness.inside = set(pks.values())
    me = threading.get_ident()
    seen = {}

    def waiting():
        with worker._in_flight_lock:
            return set(worker._claimed_in_callers_atomic_block.values())

    def ahead(db_task):
        if "before" not in seen:
            seen["before"] = waiting()
            seen["returned"] = worker.run_once()
            seen["after"] = waiting()
        return False

    def during(label):
        def hook():
            seen[label] = (waiting(), worker._renewable())

        return hook

    worker.ahead = ahead
    tasks.STATE["hooks"] = {label: during(label) for label in pks}
    with transaction.atomic():
        assert worker.run_once() is True

    outer_entry = (me, pks["outer"], 1)
    assert seen == {
        "before": {outer_entry},
        "inner": ({outer_entry}, set()),
        "returned": True,
        "after": {outer_entry},
        "outer": (set(), set()),
    }
    assert worker.calls == [(pks["outer"], True), (pks["inner"], True)]
    assert witness.most_waiting == 2
    assert witness.recorded == {(pks["outer"], 1), (pks["inner"], 1)}
    for result in (outer, inner):
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    witness.settled()


@not_on_sqlite
def test_two_calls_at_once_on_one_worker_each_take_their_own_entry(overrides):
    """
    Two threads share the Worker, as request threads sharing a module-level
    Worker do, and each calls run_once() inside a block of its own. Both
    hand their claims over before either execution begins, so two entries
    wait at once; each execution takes its own thread's, and neither row is
    named by a renewal while its task runs.
    """
    worker = overrides
    witness = worker._in_flight_lock = Witness(worker)
    labels = ("first", "second")
    rows = [tasks.with_hook.enqueue(label) for label in labels]
    witness.inside = {_row(result).pk for result in rows}
    both = threading.Barrier(2, timeout=LIMIT)
    named = {}

    def ahead(db_task):
        both.wait()
        return False

    def during(label):
        def hook():
            named[label] = worker._renewable()

        return hook

    worker.ahead = ahead
    tasks.STATE["hooks"] = {label: during(label) for label in labels}
    returned = {}
    callers = [_inside_an_atomic_block(worker, returned, label) for label in labels]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(LIMIT)

    assert returned == {"first": True, "second": True}
    assert named == {"first": set(), "second": set()}
    assert witness.most_waiting == 2
    assert witness.recorded == {(_row(result).pk, 1) for result in rows}
    for result in rows:
        assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)
    witness.settled()


def _another_threads_ident():
    idents = []
    thread = threading.Thread(target=lambda: idents.append(threading.get_ident()))
    thread.start()
    thread.join(LIMIT)
    return idents[0]


#: Entries that are not the call's own, and are there when it starts: as
#: (thread ident, pk, lease_epoch), from this thread's ident, the row's pk
#: and the epoch its claim is granted.
NOT_THE_CALLS = {
    "another thread's, for the same pair": lambda me, pk, epoch: (
        _another_threads_ident(),
        pk,
        epoch,
    ),
    "this thread's, for the row at another epoch": lambda me, pk, epoch: (
        me,
        pk,
        epoch + 1,
    ),
}


@pytest.mark.parametrize("block", ["inside a block", "outside any block"])
@pytest.mark.parametrize("whose", list(NOT_THE_CALLS))
def test_an_entry_that_is_not_the_calls_own_is_neither_taken_nor_dropped(
    whose, block, overrides
):
    """
    An entry waits that this call's execution must not take: one another
    thread's call made for the same pair, or one for the row at another
    epoch. Inside a block the call's own entry is taken, and the row is
    left out; outside one the call has none, and the row is renewed. The
    other entry is where it was afterwards, under its own key.
    """
    worker = overrides
    result = tasks.with_hook.enqueue("own")
    pk = _row(result).pk
    theirs = object()
    entry = NOT_THE_CALLS[whose](threading.get_ident(), pk, 1)
    with worker._in_flight_lock:
        worker._claimed_in_callers_atomic_block[theirs] = entry
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["left out"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"own": during}
    if block == "inside a block":
        with transaction.atomic():
            assert worker.run_once() is True
        assert seen == {"named": set(), "left out": {(pk, 1)}}
    else:
        assert worker.run_once() is True
        assert seen == {"named": {pk}, "left out": set()}
    assert worker._claimed_in_callers_atomic_block == {theirs: entry}
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


def test_an_outer_calls_entry_for_the_same_pair_is_left_to_it(overrides):
    """
    Two entries for one thread and one pair, as a call nested inside another
    would leave them: the outer call's, waiting, and the inner call's own.
    The inner execution takes the later, its own, and drops nothing else,
    so the outer entry is still there under its own key for the outer
    execution to take.
    """
    worker = overrides
    result = tasks.with_hook.enqueue("inner")
    pk = _row(result).pk
    outer = object()
    entry = (threading.get_ident(), pk, 1)
    with worker._in_flight_lock:
        worker._claimed_in_callers_atomic_block[outer] = entry
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["waiting"] = dict(worker._claimed_in_callers_atomic_block)

    tasks.STATE["hooks"] = {"inner": during}
    with transaction.atomic():
        assert worker.run_once() is True

    assert seen == {"named": set(), "waiting": {outer: entry}}
    assert worker._claimed_in_callers_atomic_block == {outer: entry}


# -- renewal elsewhere is as it was ----------------------------------------------------


def test_a_call_outside_any_block_through_the_override_is_renewed(
    overrides, monkeypatch
):
    """
    No block on the worker's database: the claim commits, a renewal thread
    runs for the call, and the row is named while its task runs.
    """
    worker = overrides
    started = renewal_loops(monkeypatch)
    result = tasks.with_hook.enqueue("outside")
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["left out"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"outside": during}
    assert worker.run_once() is True

    pk = _row(result).pk
    assert seen == {"named": {pk}, "left out": set()}
    assert started == ["ox-renew-inline"]
    assert worker.calls == [(pk, True)]
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
def test_a_block_on_another_database_leaves_the_row_renewed(overrides, monkeypatch):
    """
    The caller's block is on a database the worker does not use: its claim
    commits on its own, a renewal thread runs, and the row is named.
    """
    worker = overrides
    started = renewal_loops(monkeypatch)
    result = tasks.with_hook.enqueue("elsewhere")
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["left out"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"elsewhere": during}
    with transaction.atomic(using="alt"):
        assert not connections[worker._db_alias].in_atomic_block
        assert worker.run_once() is True

    pk = _row(result).pk
    assert seen == {"named": {pk}, "left out": set()}
    assert started == ["ox-renew-inline"]
    assert _state(result) == (OxTask.Status.SUCCESSFUL, 1, 1)


def test_the_loop_renews_the_rows_the_override_runs(settings):
    """
    run() through the override: every row its pool runs is named by
    renewal while its task runs, and none is left out.
    """
    _budget(settings, 3)
    worker = Overrides(
        backoff_initial=0,
        lock_timeout=LOCK_TIMEOUT,
        poll_interval=0.05,
        concurrency=1,
    )
    result = tasks.with_hook.enqueue("loop")
    seen = {}

    def during():
        seen["named"] = worker._renewable()
        with worker._in_flight_lock:
            seen["left out"] = set(worker._in_callers_atomic_block)

    tasks.STATE["hooks"] = {"loop": during}
    loop = start_worker_thread(worker)
    try:
        assert wait_for(
            lambda: _state(result)[0] == OxTask.Status.SUCCESSFUL, timeout=LIMIT
        )
    finally:
        worker.request_stop()
        loop.join(LIMIT)

    pk = _row(result).pk
    assert seen == {"named": {pk}, "left out": set()}
    assert worker.calls == [(pk, False)]
