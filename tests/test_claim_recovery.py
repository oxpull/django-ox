"""
What a worker does about a claim that raised though it may have committed.

tests/test_lost_commit_reply.py proves the outcome for one task. These prove
the rules the look lives by: which rows it may release and which it must
never touch, how the release is written, when the look runs, and what it
does when its own statements fail. tests/lost_reply.py loses the replies.
"""

import copy
import logging
import math
import sqlite3
import threading
import time
import traceback
from contextlib import contextmanager, nullcontext, suppress

import pytest
from django.db import (
    DatabaseError,
    Error,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    close_old_connections,
    connection,
    connections,
    transaction,
)
from django.db.backends.signals import connection_created
from django.db.models import Q
from django.db.models.expressions import RawSQL
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.actions import expire_lease
from django_ox.compat import task_started
from django_ox.models import OxTask
from django_ox.testing import run_tasks
from django_ox.worker import (
    MYSQL_RECOVERY_TIMEOUT,
    Worker,
    _answering_by,
    _connection_pool,
    _lost,
    _pool_options,
    _Seen,
    _sweep_pool,
    _Watch,
)

from . import tasks
from .conftest import start_worker_thread, wait_for
from .dead_connection_tasks import (
    end_connection,
    from_another_connection,
    gives_its_connection_back,
    queries_after_its_session_ended,
    restart_every_other_session,
    works_on_through_a_restart_in_process,
)
from .lost_reply import COMMITTED_CLAIM_WINDOWS, LookReads, Seams, is_the_release
from .unanswering import Unanswering

pytestmark = pytest.mark.django_db(transaction=True)

#: No test here waits on a lease: the reaper is only ever run on purpose.
LOCK_TIMEOUT = 300.0
LIMIT = 60.0

pooled_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql" or _pool_options("default") is None,
    reason="Django's connection pool: run with a pooled PostgreSQL settings module",
)


def _pool_can_drain():
    try:
        from psycopg_pool import ConnectionPool
    except ImportError:
        return False
    return hasattr(ConnectionPool, "drain")


#: The sweep discards a pool's idle connections with drain(), which
#: psycopg_pool has from 3.3. An older pool is swept by testing them, as in
#: 1.7.0, wait on a silent connection included, and these tests are about
#: what drain() changes.
drains_its_pool = pytest.mark.skipif(
    connection.vendor == "postgresql"
    and _pool_options("default") is not None
    and not _pool_can_drain(),
    reason="needs psycopg_pool 3.3 or later, which has ConnectionPool.drain(); "
    "an older pool is swept with check(), as in 1.7.0",
)


@pytest.fixture(autouse=True)
def no_dead_connection_outlives_the_test():
    """
    A test here ends sessions, the test thread's own included. Before the
    database is flushed, drop any connection that no longer answers, so the
    next test does not inherit it, whatever the code under test did.
    """
    yield
    for conn in connections.all(initialized_only=True):
        if (
            conn.connection is not None
            and not conn.in_atomic_block
            and not conn.is_usable()
        ):
            conn.close()


@pytest.fixture
def lose_the_reply():
    seams = Seams()
    yield seams.arm
    seams.remove_all()


@pytest.fixture
def started():
    """task_started per task id, for every execution in the test."""
    seen = {}
    lock = threading.Lock()

    def receiver(sender, task_result, **kwargs):
        with lock:
            seen[str(task_result.id)] = seen.get(str(task_result.id), 0) + 1

    task_started.connect(receiver, dispatch_uid="test_claim_recovery.started")
    yield seen
    task_started.disconnect(dispatch_uid="test_claim_recovery.started")


def _budget(settings, attempts):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"MAX_ATTEMPTS": attempts},
        }
    }


def _events(caplog, name, worker=None):
    return [
        r
        for r in caplog.records
        if getattr(r, "event", None) == name
        and (worker is None or getattr(r, "worker_id", None) == worker.worker_id)
    ]


def _released_ids(caplog, worker):
    return sorted(r.task_id for r in _events(caplog, "worker_claim_released", worker))


def _runs(label):
    return tasks.STATE.get("order", []).count(label)


def _row(result):
    return OxTask.objects.get(id=result.id)


def _enqueue(*labels):
    """One record task per label, claimed in the order given."""
    return [
        tasks.record.using(priority=len(labels) - i).enqueue(label)
        for i, label in enumerate(labels)
    ]


def _run_until(worker, done, limit=LIMIT):
    """Run the loop on a thread until done() or `limit`; stop it and join."""
    thread = start_worker_thread(worker)
    try:
        wait_for(done, timeout=limit)
    finally:
        worker.request_stop()
        thread.join(timeout=60)
    assert not thread.is_alive(), "the worker did not stop"


def _orphan(worker, label="orphan"):
    """
    A row RUNNING under `worker`'s id that no claim of its returned, and a
    look pending: the state a claim that raised after committing leaves.
    The claim is a real one; only the registration it made is taken back.
    """
    result = tasks.record.enqueue(label)
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == str(result.id)
    with worker._in_flight_lock:
        worker._handed_off.clear()
    worker._claim_unconfirmed_at = time.monotonic()
    return result


@contextmanager
def _watching(alias="default"):
    """
    What a pass of the loop keeps, and an outcome's first write: every
    connection this thread holds or opens for `alias` in the block, _Seen.
    """
    seen = _Seen(alias)
    seen.watch()
    try:
        yield seen
    finally:
        seen.unwatch()


def _a_failed_pass_then_its_look(worker):
    """
    What run() does, on the test's thread: a claim that raises, the failed
    pass's handling of the connection, with the pool swept when the pass
    lost its connection, and the look at the head of the next pass. The
    claim's error is returned.
    """
    with _watching(worker._db_alias) as seen:
        with pytest.raises(DatabaseError) as raised:
            worker._claim()
        lost = seen.lost()
    close_old_connections()
    _sweep_pool(connections[worker._db_alias], lost=lost)
    worker._recover_claims()
    return raised.value


# -- which rows a look may release ----------------------------------------------


class HeldBeforeRegistering(Worker):
    """Holds every claimed row on its pool thread until the gate opens."""

    def __init__(self, *args, gate, **kwargs):
        super().__init__(*args, **kwargs)
        self.gate = gate
        self.waiting = 0
        self._waiting_lock = threading.Lock()

    def _execute_in_thread(self, db_task):
        with self._waiting_lock:
            self.waiting += 1
        self.gate.wait(timeout=LIMIT)
        super()._execute_in_thread(db_task)


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_only_the_orphan_is_released_while_earlier_claims_wait_to_register(
    window, settings, caplog, started, lose_the_reply
):
    """
    Astra's test. Concurrency 3: two claims return and are submitted, and
    their pool threads are held before execute() registers them; the third
    claim commits and raises. The look must release the third alone. Then
    every body runs once, each row records one attempt by this worker, and
    task_started fires once per execution.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    gate = threading.Event()
    worker = HeldBeforeRegistering(
        concurrency=3, lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, gate=gate
    )
    at_the_kill = {}

    def look(other):
        with worker._in_flight_lock:
            at_the_kill["in_flight"] = set(worker._in_flight)
            at_the_kill["handed_off"] = set(worker._handed_off)

    seam = lose_the_reply(window, nth=3, also=look)
    first, second, third = _enqueue("c1", "c2", "c3")

    def third_claimed_again():
        return (
            worker.waiting == 3
            and _row(third).status == OxTask.Status.RUNNING
            and _row(third).lease_epoch == 3
        )

    thread = start_worker_thread(worker)
    try:
        assert wait_for(third_claimed_again, timeout=LIMIT), (
            f"the third claim was never released and claimed again; released "
            f"{_released_ids(caplog, worker)}"
        )
        # The instrument: the earlier two were claimed, and neither had
        # registered, when the third claim's session ended.
        assert seam.fired
        assert seam.seen == [(OxTask.Status.RUNNING, worker.worker_id, 1, 1)] * 3
        assert at_the_kill["in_flight"] == set()
        assert {pk for pk, _ in at_the_kill["handed_off"]} == {
            _row(first).pk,
            _row(second).pk,
        }
        assert _released_ids(caplog, worker) == [str(third.id)]
        gate.set()
        assert wait_for(
            lambda: all(
                _row(r).status == OxTask.Status.SUCCESSFUL
                for r in (first, second, third)
            ),
            timeout=LIMIT,
        )
    finally:
        gate.set()
        worker.request_stop()
        thread.join(timeout=60)

    assert [_runs(label) for label in ("c1", "c2", "c3")] == [1, 1, 1]
    for result, epoch in ((first, 1), (second, 1), (third, 3)):
        row = _row(result)
        assert (row.attempts, row.lease_epoch, row.worker_ids) == (
            1,
            epoch,
            [worker.worker_id],
        )
        assert started.get(str(result.id)) == 1
    assert _released_ids(caplog, worker) == [str(third.id)]


class Witness:
    """
    Stands in for a worker's _in_flight_lock, and on every release checks
    that each claim that returned is in _in_flight, _handed_off or
    _unsettled. In this test no outcome is ever recorded, so none may leave
    all three.
    """

    def __init__(self, worker, returned):
        self._lock = threading.Lock()
        self.worker = worker
        self.returned = returned
        self.checks = 0
        self.violations = []
        self.seen_in = {"handed_off": set(), "in_flight": set(), "unsettled": set()}

    def _check(self):
        worker = self.worker
        self.checks += 1
        for name in self.seen_in:
            self.seen_in[name] |= getattr(worker, f"_{name}")
        known = worker._in_flight | worker._handed_off | worker._unsettled
        missing = [pair for pair in list(self.returned) if pair not in known]
        if missing:
            self.violations.append((missing, "".join(traceback.format_stack(limit=6))))

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


class OutcomesNeverLand(Worker):
    """Every outcome write fails on both tries, as after an outage."""

    def __init__(self, *args, returned, **kwargs):
        super().__init__(*args, **kwargs)
        self.returned = returned

    def claim_one(self):
        db_task = super().claim_one()
        if db_task is not None:
            self.returned.append((db_task.pk, db_task.lease_epoch))
        return db_task

    def _write_outcome(self, db_task, **kwargs):
        raise OperationalError("the outcome write lost its connection")

    def _outcome_connection_lost(self):
        return True


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_returned_claim_is_in_one_set_at_every_instant(
    window, settings, caplog, lose_the_reply
):
    """
    The registration transfer, checked at every release of the lock that
    guards it. A claim moves from _handed_off to _in_flight when execute()
    starts, and from _in_flight to _unsettled when it ends without an
    outcome; a look snapshots the union under the same lock. Were either
    move two steps, a look between them would find the row in no set and
    release a row whose body is about to run, or has run.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    returned = []
    worker = OutcomesNeverLand(
        concurrency=2,
        lock_timeout=LOCK_TIMEOUT,
        poll_interval=0.05,
        returned=returned,
    )
    witness = Witness(worker, returned)
    worker._in_flight_lock = witness
    seam = lose_the_reply(window, nth=3)
    results = _enqueue("w1", "w2", "w3", "w4")

    def all_ran():
        return (
            all(_runs(label) == 1 for label in ("w1", "w2", "w3", "w4"))
            and len(worker._unsettled) == 4
        )

    _run_until(worker, all_ran)

    assert seam.fired
    assert witness.violations == [], witness.violations[0][1]
    assert witness.checks >= 3 * len(returned)
    # Each claim was seen in every set it passes through.
    for pair in returned:
        for name in ("handed_off", "in_flight", "unsettled"):
            assert pair in witness.seen_in[name], (pair, name)
    assert _released_ids(caplog, worker) == [str(results[2].id)]
    assert len(_events(caplog, "task_outcome_unrecorded", worker)) == 4
    for result in results:
        assert _row(result).status == OxTask.Status.RUNNING


class OneOutcomeUnrecorded(Worker):
    """The first task's outcome is never recorded, in the way `how` says."""

    def __init__(self, *args, how, label, **kwargs):
        super().__init__(*args, **kwargs)
        self.how = how
        self.label = label

    def _mine(self, db_task):
        return db_task.args == [self.label]

    def _write_outcome(self, db_task, **kwargs):
        if not self._mine(db_task):
            return super()._write_outcome(db_task, **kwargs)
        if self.how == "declined":
            # An override that fails closed: nothing written, and False, as
            # Oxpull Pro answers when a workflow node's record fails.
            return False
        if self.how == "raised":
            raise DatabaseError("refused, and the connection still answers")
        raise OperationalError("the outcome write lost its connection")

    def _outcome_connection_lost(self):
        return True


@pytest.mark.parametrize("how", ["unrecorded", "declined", "raised"])
@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_an_execution_whose_outcome_is_not_recorded_is_never_released(
    window, how, settings, caplog, lose_the_reply
):
    """
    A task ran and its outcome could not be recorded: both tries failed
    (task_outcome_unrecorded), an override declined without writing, or the
    write raised out of the attempt. Its row is RUNNING under this worker,
    in no in-flight set, exactly like a claim that raised. The next claim
    commits and raises; the look must release that one alone, because the
    other one's body ran.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    worker = OneOutcomeUnrecorded(
        concurrency=1,
        lock_timeout=LOCK_TIMEOUT,
        poll_interval=0.05,
        how=how,
        label="ran",
    )
    seam = lose_the_reply(window, nth=2)
    ran, orphan = _enqueue("ran", "orphan")

    _run_until(worker, lambda: _row(orphan).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    assert seam.seen == sorted(
        [(OxTask.Status.RUNNING, worker.worker_id, 1, 1)] * 2, key=repr
    )
    assert (_runs("ran"), _runs("orphan")) == (1, 1)
    row = _row(ran)
    assert (row.status, row.locked_by, row.attempts, row.lease_epoch) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
        1,
    ), "the row of a task that ran was released as a claim that never did"
    assert _released_ids(caplog, worker) == [str(orphan.id)]
    assert (_row(orphan).attempts, _row(orphan).lease_epoch) == (1, 3)


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_another_workers_row_is_untouched(window, settings, caplog, lose_the_reply):
    """
    A row RUNNING under another worker's id: a 1.5.0 worker's claim that
    raised, taken by the same claim code, which nothing runs. The look never
    reads it, and releases only this worker's own.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    theirs_result, mine = _enqueue("theirs", "mine")
    other = Worker(lock_timeout=LOCK_TIMEOUT)
    theirs = other.claim_one()
    assert str(theirs.id) == str(theirs_result.id)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    seam = lose_the_reply(window)

    _run_until(worker, lambda: _row(mine).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    row = _row(theirs_result)
    assert (
        row.status,
        row.locked_by,
        row.attempts,
        row.lease_epoch,
        row.worker_ids,
    ) == (OxTask.Status.RUNNING, other.worker_id, 1, 1, [other.worker_id])
    assert _released_ids(caplog, worker) == [str(mine.id)]
    assert _events(caplog, "worker_claim_release_refused") == []
    assert _runs("theirs") == 0


def test_an_attempt_the_watchdog_took_off_its_thread_is_never_released(
    settings, monkeypatch
):
    """
    The backstop takes a stuck attempt out of _in_flight to stop renewing
    it. Its body is still running, so the row must stay excluded, whether or
    not the stuck record that follows lands; here it fails.
    """
    _budget(settings, 3)
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("stuck")
    db_task = worker.claim_one()
    held = (db_task.pk, db_task.lease_epoch)
    with worker._in_flight_lock:
        worker._handed_off.discard(held)
        worker._in_flight.add(held)
    now = time.monotonic()
    watch = _Watch(
        ident=threading.get_ident(),
        db_task=copy.copy(db_task),
        attempt=held,
        timeout=1.0,
        started=now,
        deadline=now,
        deadline_at=timezone.now(),
        injectable=True,
    )

    def record_fails(*args, **kwargs):
        raise OperationalError("the stuck record could not be written")

    monkeypatch.setattr(worker, "_handle_failure", record_fails)
    monkeypatch.setattr(worker, "_recycle", lambda db_task: None)
    worker._handle_stuck(watch)
    worker._claim_unconfirmed_at = time.monotonic()
    worker._recover_claims()

    row = _row(result)
    assert (row.status, row.locked_by, row.attempts) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
    )


def test_a_claim_returned_by_claim_one_directly_is_never_released():
    """
    A caller that claims through claim_one() itself, outside the worker's
    entry points, and executes later: its row is registered before the claim
    returns, so a look made in between leaves it alone.
    """
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("direct")
    db_task = worker.claim_one()
    worker._claim_unconfirmed_at = time.monotonic()

    worker._recover_claims()

    assert _row(result).status == OxTask.Status.RUNNING
    worker.execute(db_task)
    assert (_row(result).status, _row(result).attempts) == (
        OxTask.Status.SUCCESSFUL,
        1,
    )


def test_a_look_drops_entries_whose_rows_are_no_longer_this_workers():
    """
    An entry excludes a row only while that row is RUNNING under this worker
    at its epoch; once it is not, it never can be again, and the next look
    drops the entry, so the sets do not grow for the life of the process.
    """
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    gone, still = tasks.record.enqueue("gone"), tasks.record.enqueue("still")
    for _ in range(2):
        db_task = worker.claim_one()
        held = (db_task.pk, db_task.lease_epoch)
        with worker._in_flight_lock:
            worker._handed_off.discard(held)
            worker._unsettled.add(held)
    OxTask.objects.filter(id=gone.id).update(
        status=OxTask.Status.SUCCESSFUL, locked_by=None
    )
    worker._claim_unconfirmed_at = time.monotonic()

    worker._recover_claims()

    assert worker._unsettled == {(_row(still).pk, 1)}
    assert _row(still).status == OxTask.Status.RUNNING


# -- the release ----------------------------------------------------------------


@pytest.mark.parametrize("release_window", ["before", "statement"])
def test_a_release_whose_reply_is_lost_refunds_once(
    release_window, settings, caplog, lose_the_reply
):
    """
    The look's own write loses its reply. Whether it landed or not, the next
    look reads the row again rather than assuming either: a landed release
    leaves nothing to do, and one that did not is made again. The attempt is
    refunded once, the task runs once. Only a release that was confirmed is
    logged as one.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    claim_window = {"mysql": "commit", "postgresql": "statement"}.get(
        connection.vendor, "statement"
    )
    claim = lose_the_reply(claim_window)
    release = lose_the_reply(release_window, statement="release")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    result = tasks.record.enqueue("once")

    _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)

    assert claim.fired and release.fired
    if release.landed:
        assert release.seen == [(OxTask.Status.READY, None, 0, 2)]
    else:
        assert release.seen == [(OxTask.Status.RUNNING, worker.worker_id, 1, 1)]
    row = _row(result)
    assert (row.status, row.attempts, row.lease_epoch, row.worker_ids) == (
        OxTask.Status.SUCCESSFUL,
        1,
        3,
        [worker.worker_id],
    )
    assert _runs("once") == 1
    confirmed = 0 if release.landed else 1
    assert len(_events(caplog, "worker_claim_released", worker)) == confirmed


class RaceTheRelease:
    """Runs `action` on another connection just before the release UPDATE."""

    def __init__(self, action):
        self.action = action
        self.fired = False

    def __call__(self, execute, sql, params, many, context):
        if not self.fired and is_the_release(sql, params):
            self.fired = True
            from_another_connection(self.action, context["connection"].alias)
        return execute(sql, params, many, context)


@pytest.mark.parametrize("reaper", ["requeued", "lost"])
@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_reaper_that_gets_there_first_keeps_its_decision(
    window, reaper, settings, caplog, lose_the_reply
):
    """
    Between the look's read and its release, the row's lease expires and
    another worker's reaper takes it: back to READY with the epoch moved and
    the attempt kept (an attempt to spare), or LOST with the epoch kept (the
    last attempt). The pinned release matches nothing and is not a release;
    the reaper's decision stands.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 2 if reaper == "requeued" else 1)
    seam = lose_the_reply(window, on=connections["default"])
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    result = tasks.record.enqueue("raced")

    def reap_first(other):
        assert expire_lease(result.id)
        Worker(lock_timeout=LOCK_TIMEOUT).reap()

    race = RaceTheRelease(reap_first)

    def install(sender, connection, **kwargs):
        # The look reconnects the thread's wrapper after the failed pass.
        if race not in connection.execute_wrappers:
            connection.execute_wrappers.append(race)

    install(None, connections["default"])
    connection_created.connect(install, weak=False)
    try:
        _a_failed_pass_then_its_look(worker)
    finally:
        connection_created.disconnect(install)
        if race in connections["default"].execute_wrappers:
            connections["default"].execute_wrappers.remove(race)

    assert seam.fired and race.fired
    assert _events(caplog, "worker_claim_released", worker) == []
    assert worker._claim_unconfirmed_at is None
    row = _row(result)
    if reaper == "requeued":
        assert (row.status, row.locked_by, row.attempts, row.lease_epoch) == (
            OxTask.Status.READY,
            None,
            1,
            2,
        )
        assert worker.run_once() is True
        row = _row(result)
        assert (row.status, row.attempts, _runs("raced")) == (
            OxTask.Status.SUCCESSFUL,
            2,
            1,
        )
    else:
        assert (row.status, row.locked_by, row.attempts, row.lease_epoch) == (
            OxTask.Status.LOST,
            None,
            1,
            1,
        )
        assert _runs("raced") == 0


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_refund_leaves_an_earlier_attempts_record_as_it_was(
    window, settings, caplog, lose_the_reply
):
    """
    The claim that raised was the task's second: the first ran and failed.
    The release takes back the second attempt and its history entry, and
    keeps started_at, which the first attempt set; the retry's error stays.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, backoff_initial=0)
    result = tasks.flaky.enqueue(2)
    assert worker.run_once() is True
    before = _row(result)
    assert (before.status, before.attempts, before.lease_epoch) == (
        OxTask.Status.READY,
        1,
        1,
    )
    assert before.started_at is not None
    seam = lose_the_reply(window, on=connections["default"])

    _a_failed_pass_then_its_look(worker)

    assert seam.fired
    row = _row(result)
    assert (row.status, row.attempts, row.lease_epoch, row.worker_ids) == (
        OxTask.Status.READY,
        1,
        3,
        [worker.worker_id],
    )
    assert row.started_at == before.started_at
    assert row.last_attempted_at is not None
    assert len(row.errors) == 1
    assert worker.run_once() is True
    row = _row(result)
    assert (row.status, row.attempts) == (OxTask.Status.SUCCESSFUL, 2)
    assert tasks.STATE["flaky_calls"] == 2


def test_a_row_whose_history_is_not_one_entry_per_attempt_is_not_released(caplog):
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    OxTask.objects.filter(id=result.id).update(worker_ids=["somebody-else"])

    worker._recover_claims()

    row = _row(result)
    assert (row.status, row.locked_by, row.attempts, row.worker_ids) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
        ["somebody-else"],
    )
    (refused,) = _events(caplog, "worker_claim_release_refused", worker)
    assert refused.task_id == str(result.id)
    assert refused.levelno == logging.ERROR
    assert _events(caplog, "worker_claim_released") == []
    assert worker._claim_unconfirmed_at is None


# -- when a look runs -----------------------------------------------------------


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_batch_does_not_end_while_a_claim_is_unconfirmed(
    window, settings, caplog, lose_the_reply
):
    caplog.set_level(logging.INFO, logger="django_ox")
    _budget(settings, 1)
    seam = lose_the_reply(window)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, batch=True)
    result = tasks.record.enqueue("batch")

    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)
    worker.request_stop()
    thread.join(timeout=60)

    assert seam.fired
    assert _events(caplog, "worker_batch_empty", worker)
    row = _row(result)
    assert (row.status, row.attempts, _runs("batch")) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    ), "the batch ended with a claim that raised still RUNNING"


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_max_tasks_counts_only_the_claims_that_returned(
    window, settings, caplog, lose_the_reply
):
    """
    --max-tasks 2 and two tasks, the second claim commits and raises. It
    uses no slot: it is released, claimed again, runs, and that claim is the
    second.
    """
    caplog.set_level(logging.INFO, logger="django_ox")
    _budget(settings, 1)
    seam = lose_the_reply(window, nth=2)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, max_tasks=2)
    first, second = _enqueue("m1", "m2")

    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)
    worker.request_stop()
    thread.join(timeout=60)

    assert seam.fired
    (reached,) = _events(caplog, "worker_max_tasks_reached", worker)
    assert reached.claimed == 2
    assert (_runs("m1"), _runs("m2")) == (1, 1)
    for result in (first, second):
        row = _row(result)
        assert (row.status, row.attempts) == (OxTask.Status.SUCCESSFUL, 1)


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_worker_that_stops_after_a_claim_raised_looks_once_more(
    window, settings, caplog, lose_the_reply
):
    """
    The stop comes while the failed pass waits: there is no next pass. The
    look is made on the way out, and the row is left READY for any worker.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=30.0)
    seam = lose_the_reply(window, also=lambda other: worker.request_stop())
    result = tasks.record.enqueue("stopped")

    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)

    assert not thread.is_alive()
    assert seam.fired
    row = _row(result)
    assert (row.status, row.locked_by, row.attempts, row.lease_epoch) == (
        OxTask.Status.READY,
        None,
        0,
        2,
    )
    assert row.worker_ids == []
    assert _released_ids(caplog, worker) == [str(result.id)]
    assert _runs("stopped") == 0


@pytest.mark.parametrize("window", COMMITTED_CLAIM_WINDOWS)
def test_a_look_that_fails_at_stop_does_not_hold_the_stop(
    window, settings, caplog, monkeypatch, lose_the_reply
):
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=30.0)
    seam = lose_the_reply(window, also=lambda other: worker.request_stop())
    result = tasks.record.enqueue("stopped")

    real = worker._recover_claims

    def look_fails(**kwargs):
        if worker._claim_unconfirmed_at is None:
            return real(**kwargs)
        raise OperationalError("the database is still gone")

    monkeypatch.setattr(worker, "_recover_claims", look_fails)
    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)

    assert not thread.is_alive()
    assert seam.fired
    (failed,) = _events(caplog, "worker_claim_recovery_failed", worker)
    assert failed.claim_recovery == "expired"
    assert _row(result).status == OxTask.Status.RUNNING
    (poll,) = _events(caplog, "worker_poll_failed", worker)
    assert poll.claim_recovery == "pending"


@pytest.mark.parametrize("step", ["reap", "dispatch_schedules"])
def test_a_pass_that_failed_outside_the_claim_starts_no_look(
    step, settings, caplog, monkeypatch
):
    """
    The loop's handler catches the database errors of reap and dispatch as
    well as the claim's. Only the claim's can leave a row RUNNING under this
    worker's id, so only the claim's starts a look.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, batch=True)
    real = getattr(worker, step)
    # A DatabaseError out of dispatch is its own handler's; an InterfaceError
    # is not a DatabaseError and reaches the loop's.
    error = OperationalError if step == "reap" else InterfaceError
    failed = []

    def fails_once(*args, **kwargs):
        if not failed:
            failed.append(step)
            raise error(f"{step} lost its connection")
        return real(*args, **kwargs)

    monkeypatch.setattr(worker, step, fails_once)
    looks = []
    real_look = worker._recover_claims

    def look(**kwargs):
        if worker._claim_unconfirmed_at is not None:
            looks.append(kwargs)
        return real_look(**kwargs)

    monkeypatch.setattr(worker, "_recover_claims", look)
    result = tasks.record.enqueue("plain")

    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)
    worker.request_stop()
    thread.join(timeout=60)

    assert failed == [step]
    (poll,) = _events(caplog, "worker_poll_failed", worker)
    assert poll.claim_recovery is None
    assert looks == []
    assert _row(result).status == OxTask.Status.SUCCESSFUL


def test_past_the_window_the_look_is_not_made(caplog):
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    worker._claim_unconfirmed_at = time.monotonic() - LOCK_TIMEOUT - 1

    with CaptureQueriesContext(connection) as queries:
        worker._recover_claims()

    assert len(queries) == 0
    assert _row(result).status == OxTask.Status.RUNNING
    assert worker._claim_unconfirmed_at is None
    (expired,) = _events(caplog, "worker_claim_recovery_expired", worker)
    assert expired.claim_recovery == "expired"


def test_a_row_whose_lease_has_expired_is_the_reapers(caplog):
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    assert expire_lease(result.id)

    worker._recover_claims()

    row = _row(result)
    assert (row.status, row.locked_by, row.attempts) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
    )
    (expired,) = _events(caplog, "worker_claim_recovery_expired", worker)
    assert expired.task_id == str(result.id)
    assert expired.claim_recovery == "expired"
    assert _events(caplog, "worker_claim_released") == []
    worker.reap()
    row = _row(result)
    assert (row.status, row.attempts, row.lease_epoch) == (
        OxTask.Status.READY,
        1,
        2,
    )


def test_a_look_is_never_made_inside_a_callers_transaction():
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)

    with transaction.atomic(), CaptureQueriesContext(connection) as queries:
        worker._recover_claims()
    assert len(queries) == 0

    transaction.set_autocommit(False)
    try:
        with CaptureQueriesContext(connection) as queries:
            worker._recover_claims()
        assert len(queries) == 0
    finally:
        transaction.rollback()
        transaction.set_autocommit(True)

    assert worker._claim_unconfirmed_at is not None
    assert _row(result).status == OxTask.Status.RUNNING
    worker._recover_claims()
    assert _row(result).status == OxTask.Status.READY


def test_an_ordinary_run_makes_no_look(settings, caplog):
    """Nothing pending, nothing read: the look costs the ordinary path nothing."""
    caplog.set_level(logging.INFO, logger="django_ox")
    _budget(settings, 1)
    looks = []

    def count(execute, sql, params, many, context):
        if "ox_lease_abandoned" in sql:
            looks.append(sql)
        return execute(sql, params, many, context)

    def install(sender, connection, **kwargs):
        if count not in connection.execute_wrappers:
            connection.execute_wrappers.append(count)

    connection_created.connect(install, weak=False)
    try:
        results = _enqueue("n1", "n2", "n3")
        worker = Worker(
            concurrency=2, lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, batch=True
        )
        thread = start_worker_thread(worker)
        thread.join(timeout=LIMIT)
        worker.request_stop()
        thread.join(timeout=60)
    finally:
        connection_created.disconnect(install)
    assert all(_row(r).status == OxTask.Status.SUCCESSFUL for r in results)
    assert looks == []
    assert worker._handed_off == set()
    assert worker._unsettled == set()


# -- Django's PostgreSQL pool ---------------------------------------------------


def _fill_the_pool(size=3):
    """Leave `size` connections idle in the pool, as a busy process does."""
    barrier = threading.Barrier(size)

    def hold():
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 1")
        barrier.wait(timeout=30)
        connections.close_all()

    threads = [threading.Thread(target=hold) for _ in range(size)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)


def _noting_the_sweeps(monkeypatch):
    """
    Every check() and drain() of Django's pool, as (name, thread), called
    through. Noted on the class, which monkeypatch puts back as it was; on
    the pool itself it would leave a method behind that a later test could
    not take away.
    """
    pool_class = type(connections["default"].pool)
    touched = []

    def noting(name):
        real = getattr(pool_class, name)

        def call_through(self, *args, **kwargs):
            touched.append((name, threading.current_thread().name))
            return real(self, *args, **kwargs)

        monkeypatch.setattr(pool_class, name, call_through)

    noting("check")
    if hasattr(pool_class, "drain"):
        noting("drain")
    return touched


def _read_after_the_restart(read):
    """The test's own connection died with the rest; read on a new one."""
    connections["default"].close()
    _sweep_pool(connections["default"])
    return read()


@pooled_postgresql
def test_after_every_pooled_connection_died_the_look_still_lands(
    settings, caplog, lose_the_reply
):
    """
    The claim's reply is lost as the server restarts: every session ends,
    the pool's idle connections with it. The look must run on a connection
    that answers, not on the dead one the claim used nor on a dead one the
    pool still holds.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    result = tasks.record.enqueue("pooled")
    _fill_the_pool()
    seam = lose_the_reply("statement", also=restart_every_other_session)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    thread = start_worker_thread(worker)
    try:
        wait_for(
            lambda: _released_ids(caplog, worker) == [str(result.id)],
            timeout=LIMIT,
        )
        _read_after_the_restart(lambda: None)
        wait_for(
            lambda: _row(result).status == OxTask.Status.SUCCESSFUL,
            timeout=LIMIT,
        )
    finally:
        worker.request_stop()
        thread.join(timeout=60)

    assert seam.fired
    assert _released_ids(caplog, worker) == [str(result.id)], (
        "no look landed after the restart"
    )
    assert _events(caplog, "worker_claim_recovery_failed", worker) == []
    row = _row(result)
    assert (row.status, row.attempts, _runs("pooled")) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    )


# -- a caller's transaction ------------------------------------------------------


class ClaimFails(Worker):
    """A claim that raises a real database error while `failing` is set."""

    failing = False

    def claim_filter_q(self):
        base = super().claim_filter_q()
        if not self.failing:
            return base
        # A fixed statement naming a column the table does not have.
        missing = Q(pk__in=RawSQL("SELECT no_such_column FROM django_ox_oxtask", ()))
        return missing if base is None else base & missing


def _looks(queries):
    return [q for q in queries if "ox_lease_abandoned" in q["sql"]]


def test_a_claim_in_a_callers_manual_transaction_starts_no_look():
    """
    The caller took the connection out of autocommit, with no atomic block,
    and a look is pending from an earlier claim of this worker's. The claim
    of this call raises. It was the caller's claim, so no look is made: the
    caller's transaction and its uncommitted write are still there to
    commit, and the pending look waits for a connection the worker owns.
    """
    worker = ClaimFails(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    pending = worker._claim_unconfirmed_at
    transaction.set_autocommit(False)
    try:
        own = tasks.record.enqueue("the caller's own write")
        driver = connection.connection
        worker.failing = True
        with (
            CaptureQueriesContext(connection) as queries,
            pytest.raises(DatabaseError, match="no_such_column"),
        ):
            worker.run_once()
        worker.failing = False
        assert connection.connection is driver
        assert not connection.get_autocommit()
        assert OxTask.objects.filter(id=own.id).exists()
        transaction.commit()
    finally:
        transaction.set_autocommit(True)

    assert OxTask.objects.filter(id=own.id).exists()
    assert _looks(queries) == []
    assert worker._claim_unconfirmed_at == pending
    assert _row(orphan).status == OxTask.Status.RUNNING
    worker._recover_claims()
    assert _row(orphan).status == OxTask.Status.READY


@pytest.mark.parametrize("block", ["manual", "atomic"])
def test_a_look_that_fails_leaves_a_callers_transaction_alone(
    block, monkeypatch, caplog
):
    """
    Whatever makes a look fail, the connection it would close is left as it
    is while it may hold a caller's transaction: out of autocommit, or
    inside an atomic block.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    _orphan(worker)

    def look_fails(**kwargs):
        raise OperationalError("the database is still gone")

    monkeypatch.setattr(worker, "_recover_claims", look_fails)

    def the_callers(write):
        driver = connection.connection
        worker._recover_claims_once()
        assert connection.connection is driver
        assert OxTask.objects.filter(id=write.id).exists()

    if block == "manual":
        transaction.set_autocommit(False)
        try:
            own = tasks.record.enqueue("the caller's own write")
            the_callers(own)
            assert not connection.get_autocommit()
            transaction.commit()
        finally:
            transaction.set_autocommit(True)
    else:
        with transaction.atomic():
            own = tasks.record.enqueue("the caller's own write")
            the_callers(own)
    assert OxTask.objects.filter(id=own.id).exists()


# -- entries a look cannot see ---------------------------------------------------


class _RolledBack(Exception):
    pass


class CallersBlock:
    """
    work() inside a caller's atomic block on a thread of its own, held open
    until end() commits or rolls it back: the gap in which the block's
    writes are invisible to every other connection. The callers_block
    fixture rolls back any block a failing test left open.
    """

    def __init__(self, work):
        self.value = None
        self._error = None
        self._opened = threading.Event()
        self._end = threading.Event()
        self._commit = True
        self._thread = threading.Thread(target=self._run, args=(work,))
        self._thread.start()
        assert self._opened.wait(LIMIT)
        if self._error is not None:
            raise self._error

    def _run(self, work):
        try:
            with transaction.atomic():
                self.value = work()
                self._opened.set()
                self._end.wait(LIMIT)
                if not self._commit:
                    raise _RolledBack
        except _RolledBack:
            pass
        except Exception as exc:
            self._error = exc
            self._opened.set()
        finally:
            connections.close_all()

    def end(self, *, commit):
        if self._end.is_set():
            return
        self._commit = commit
        self._end.set()
        self._thread.join(LIMIT)
        assert not self._thread.is_alive()
        if self._error is not None:
            raise self._error


@pytest.fixture
def callers_block():
    blocks = []

    def start(work):
        blocks.append(CallersBlock(work))
        return blocks[-1]

    yield start
    for block in blocks:
        block.end(commit=False)


def _look(worker):
    """A look, as the worker makes one after a claim of its own raised."""
    worker._claim_unconfirmed_at = time.monotonic()
    worker._recover_claims()
    assert worker._claim_unconfirmed_at is None


class OutcomeNeverRecorded(Worker):
    """An override may answer False for an outcome it could not record."""

    def _write_outcome(self, db_task, **kwargs):
        return False


def test_a_claim_in_a_callers_block_keeps_its_entry_until_it_commits(
    callers_block, caplog
):
    """
    One Worker shared by two threads. claim_one() inside a caller's atomic
    block registers its row at once, and until the caller commits, no other
    connection can see the claim. Looks made in that gap keep the entry;
    after the commit the row is RUNNING under this worker, still excluded,
    and the caller runs it once.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("direct")
    block = callers_block(worker.claim_one)
    held = (block.value.pk, block.value.lease_epoch)
    for _ in range(3):
        _look(worker)
        assert held in worker._handed_off
    block.end(commit=True)
    _look(worker)
    assert held in worker._handed_off
    row = _row(result)
    assert (row.status, row.lease_epoch) == (OxTask.Status.RUNNING, held[1])

    worker.execute(block.value)

    assert _released_ids(caplog, worker) == []
    assert _runs("direct") == 1
    assert (_row(result).status, _row(result).attempts) == (
        OxTask.Status.SUCCESSFUL,
        1,
    )


def test_a_claim_rolled_back_in_a_callers_block_keeps_its_entry_until_it_is_over(
    callers_block, caplog
):
    """
    The caller rolls its block back: the row is READY at the epoch before
    the claim, which is no evidence the claim is over, so looks keep the
    entry. Once another worker's claim has moved the row on at that epoch,
    the next look drops it.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("rolled back")
    block = callers_block(worker.claim_one)
    held = (block.value.pk, block.value.lease_epoch)
    for _ in range(2):
        _look(worker)
        assert held in worker._handed_off
    block.end(commit=False)
    for _ in range(2):
        _look(worker)
        assert held in worker._handed_off
    row = _row(result)
    assert (row.status, row.lease_epoch) == (OxTask.Status.READY, held[1] - 1)

    assert Worker(lock_timeout=LOCK_TIMEOUT).run_once() is True
    _look(worker)

    assert held not in worker._handed_off
    assert _released_ids(caplog, worker) == []
    assert _runs("rolled back") == 1


@pytest.mark.parametrize("commit", [True, False])
def test_a_claim_of_a_row_enqueued_in_the_same_block_keeps_its_entry(
    commit, callers_block, caplog
):
    """
    The caller enqueues and claims in one atomic block, so until it commits
    no other connection can see the row at all. An absent row is no evidence
    that the claim is over: looks keep the entry, and after a commit the row
    is RUNNING under this worker and still excluded. After a rollback the
    row never existed, and the entry stays.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)

    def enqueue_and_claim():
        result = tasks.record.enqueue("same block")
        db_task = worker.claim_one()
        assert str(db_task.id) == str(result.id)
        return db_task

    block = callers_block(enqueue_and_claim)
    held = (block.value.pk, block.value.lease_epoch)
    for _ in range(2):
        _look(worker)
        assert held in worker._handed_off
    block.end(commit=commit)
    for _ in range(2):
        _look(worker)
        assert held in worker._handed_off
    assert _released_ids(caplog, worker) == []
    if not commit:
        assert not OxTask.objects.filter(pk=held[0]).exists()
        return
    row = OxTask.objects.get(pk=held[0])
    assert (row.status, row.lease_epoch) == (OxTask.Status.RUNNING, held[1])
    worker.execute(block.value)
    assert _runs("same block") == 1
    assert OxTask.objects.get(pk=held[0]).status == OxTask.Status.SUCCESSFUL


@pytest.mark.parametrize("commit", [True, False])
def test_an_unrecorded_run_once_in_a_callers_block_is_never_released(
    commit, callers_block, caplog
):
    """
    run_once() inside a caller's atomic block runs the body, and the
    outcome goes unrecorded: the row is _unsettled's. Looks made before the
    caller's block ends keep the entry, and so do looks after it, committed
    or rolled back, until a read shows the claim is over.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = OutcomeNeverRecorded(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("inline")
    block = callers_block(worker.run_once)
    assert block.value is True
    (held,) = worker._unsettled
    for _ in range(2):
        _look(worker)
        assert held in worker._unsettled
    block.end(commit=commit)
    for _ in range(2):
        _look(worker)
        assert held in worker._unsettled
    row = _row(result)
    if commit:
        assert (row.status, row.lease_epoch) == (OxTask.Status.RUNNING, held[1])
    else:
        assert (row.status, row.lease_epoch) == (OxTask.Status.READY, held[1] - 1)
    assert _released_ids(caplog, worker) == []
    assert _runs("inline") == 1

    if commit:
        OxTask.objects.filter(id=result.id).update(
            status=OxTask.Status.FAILED, locked_by=None
        )
    else:
        assert Worker(lock_timeout=LOCK_TIMEOUT).run_once() is True
    _look(worker)
    assert held not in worker._unsettled


# -- the look a stopping worker makes ----------------------------------------------


postgresql_only = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="the stopping look's deadline is PostgreSQL's (_answering_by)",
)


@pytest.fixture
def unanswering(monkeypatch):
    """
    Every connection to the test database opened from here on goes through
    a relay that can stop answering (tests/unanswering.py). detach() puts
    the settings back, for reading the outcome afterwards.
    """
    relay = Unanswering(
        connection.settings_dict["HOST"] or "127.0.0.1",
        connection.settings_dict["PORT"] or 5432,
    )
    connection.close()
    monkeypatch.setitem(connection.settings_dict, "HOST", "127.0.0.1")
    monkeypatch.setitem(connection.settings_dict, "PORT", str(relay.port))

    def detach():
        relay.close()
        connection.close()
        monkeypatch.undo()
        connection.close()

    relay.detach = detach
    yield relay
    detach()


def _stop(worker):
    """
    Run a worker asked to stop before it started, which makes only the
    stop-time look, and return how long it took. A stop still waiting at
    LIMIT fails the test.
    """
    worker.request_stop()
    started = time.monotonic()
    thread = start_worker_thread(worker)
    thread.join(timeout=LIMIT)
    took = time.monotonic() - started
    assert not thread.is_alive(), f"the stop was still waiting after {took:.1f}s"
    return took


def _given_up(caplog, worker, result):
    (failed,) = _events(caplog, "worker_claim_recovery_failed", worker)
    assert failed.claim_recovery == "expired"
    assert _released_ids(caplog, worker) == []
    row = _row(result)
    assert (row.status, row.locked_by, row.attempts) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
    )


@postgresql_only
@pytest.mark.parametrize(
    "connect_timeout, bound", [(None, 5.0), (0, 5.0), (2, 2.0), (30, 5.0)]
)
def test_a_stopping_look_gives_up_on_a_connect_never_answered(
    connect_timeout, bound, unanswering, monkeypatch, caplog
):
    """
    The database is gone behind a live address: a connection is accepted
    and never answered. The look gives up after five seconds, or after a
    shorter positive connect_timeout, never after none.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    if connect_timeout is not None:
        options = {**connection.settings_dict.get("OPTIONS", {})}
        options["connect_timeout"] = connect_timeout
        monkeypatch.setitem(connection.settings_dict, "OPTIONS", options)
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    connection.close()
    unanswering.black_hole()

    took = _stop(worker)

    assert unanswering.unanswered_connects >= 1
    assert bound - 0.5 <= took < bound + 3.0
    unanswering.detach()
    _given_up(caplog, worker, result)


@postgresql_only
def test_a_stopping_look_gives_up_on_a_reply_never_sent(unanswering, caplog):
    """
    The database stops answering once the look's connection is open: its
    read goes out and no reply comes back. The same five seconds bound it,
    connecting included.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    connection.close()

    def stall(sender, connection, **kwargs):
        unanswering.stall()

    connection_created.connect(stall, weak=False)
    try:
        took = _stop(worker)
    finally:
        connection_created.disconnect(stall)

    assert unanswering.unanswered_bytes > 0
    assert 4.5 <= took < 8.0
    unanswering.detach()
    _given_up(caplog, worker, result)


mysql_only = pytest.mark.skipif(
    connection.vendor != "mysql",
    reason="the stopping look's MySQL timeouts (_answering_by)",
)

MYSQL_TIMEOUTS = ("connect_timeout", "read_timeout", "write_timeout")


def _mysql_read_bound():
    """
    The longest a read on the look's connection may wait. mysqlclient may
    retry a read that timed out, up to three attempts in all; PyMySQL
    does not retry.
    """
    retries = 1 if connection.Database.__name__ == "pymysql" else 3
    return retries * MYSQL_RECOVERY_TIMEOUT


@pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="the stopping look has a connection of its own on PostgreSQL and MySQL",
)
def test_a_stopping_look_has_a_connection_of_its_own():
    """
    The look runs on a connection of its own, from settings that are a
    copy: on MySQL, one carrying the three timeouts. The thread's
    connection, and the settings every other wrapper for the alias reads,
    are left as they were, and the look's connection is closed afterwards.
    """
    if connection.vendor == "postgresql":
        # With psycopg2 the look runs on the thread's connection.
        pytest.importorskip("psycopg")
    thread_wrapper = connections["default"]
    thread_wrapper.ensure_connection()
    driver = thread_wrapper.connection
    settings_before = copy.deepcopy(thread_wrapper.settings_dict)

    with _answering_by("default", time.monotonic() + 5.0) as bounded:
        assert bounded is True
        own = connections["default"]
        assert own is not thread_wrapper
        if connection.vendor == "mysql":
            options = own.settings_dict["OPTIONS"]
            timeouts = {name: options[name] for name in MYSQL_TIMEOUTS}
            assert timeouts == dict.fromkeys(MYSQL_TIMEOUTS, MYSQL_RECOVERY_TIMEOUT)
        with own.cursor() as cursor:
            cursor.execute("SELECT 1")
        assert own.connection is not driver

    assert connections["default"] is thread_wrapper
    assert thread_wrapper.connection is driver
    assert thread_wrapper.settings_dict == settings_before
    assert own.connection is None


@mysql_only
@pytest.mark.parametrize("configured", [None, 30])
def test_a_stopping_look_on_mysql_gives_up_on_a_connect_never_answered(
    configured, unanswering, monkeypatch, caplog
):
    """
    The database is gone behind a live address: a connection is accepted
    and no greeting ever comes. Without a timeout PyMySQL waits for one as
    long as the socket is open; the look's own connect timeout, or read
    timeout, ends the wait after five seconds, and a longer one configured
    does not lengthen it.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    if configured is not None:
        options = {**connection.settings_dict.get("OPTIONS", {})}
        options.update(dict.fromkeys(MYSQL_TIMEOUTS, configured))
        monkeypatch.setitem(connection.settings_dict, "OPTIONS", options)
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    connection.close()
    unanswering.black_hole()

    took = _stop(worker)

    assert unanswering.unanswered_connects >= 1
    assert MYSQL_RECOVERY_TIMEOUT - 0.5 <= took < MYSQL_RECOVERY_TIMEOUT + 3.0
    unanswering.detach()
    _given_up(caplog, worker, result)


@mysql_only
def test_a_stopping_look_on_mysql_gives_up_on_a_reply_never_sent(unanswering, caplog):
    """
    The database stops answering once the look's connection is open: its
    read goes out and no reply comes back. The look's read timeout ends
    the wait.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    connection.close()

    def stall(sender, connection, **kwargs):
        unanswering.stall()

    connection_created.connect(stall, weak=False)
    try:
        took = _stop(worker)
    finally:
        connection_created.disconnect(stall)

    assert unanswering.unanswered_bytes > 0
    assert MYSQL_RECOVERY_TIMEOUT - 0.5 <= took < _mysql_read_bound() + 3.0
    unanswering.detach()
    _given_up(caplog, worker, result)


def test_a_stopping_look_given_up_closes_nothing(monkeypatch, caplog):
    """
    A stopping look given up leaves the thread's connection as it is, and
    Django's pool with it: there is no next statement for a closed
    connection or a swept pool to serve.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)

    def gave_up(deadline):
        raise OperationalError("no reply from the server by the deadline")

    monkeypatch.setattr(worker, "_recover_claims_by", gave_up)
    driver = connection.connection
    assert driver is not None
    worker._recover_claims_once()
    assert connection.connection is driver
    _given_up(caplog, worker, result)


def test_a_stopping_look_does_not_wait_past_its_deadline_for_a_claim_in_flight(
    monkeypatch, caplog
):
    """
    Another thread sharing the Worker has a claim in flight, on its way
    back. The stopping look waits for it no longer than its deadline, and
    gives up.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    in_flight = threading.Event()
    release = threading.Event()

    def claim_in_flight():
        in_flight.set()
        release.wait(LIMIT)

    monkeypatch.setattr(worker, "_claim_one", claim_in_flight)
    claimer = threading.Thread(
        target=lambda: (worker.claim_one(), connections.close_all())
    )
    claimer.start()
    try:
        assert in_flight.wait(LIMIT)
        took = _stop(worker)
    finally:
        release.set()
        claimer.join(LIMIT)

    assert 4.5 <= took < 8.0
    _given_up(caplog, worker, result)


# -- run_once() and run_tasks() make no look -----------------------------------------


@pytest.fixture
def look_reads():
    looks = LookReads()
    yield looks.reads
    looks.remove()


def _call(call, worker):
    return worker.run_once() if call == "run_once" else run_tasks()


RECOVERY_EVENTS = (
    "worker_claim_released",
    "worker_claim_recovery_failed",
    "worker_claim_recovery_expired",
)


def _recovery_events(caplog):
    return [r for name in RECOVERY_EVENTS for r in _events(caplog, name)]


@pytest.mark.parametrize("call", ["run_once", "run_tasks"])
def test_an_inline_claim_error_makes_no_look(call, monkeypatch, caplog, look_reads):
    """
    A claim of run_once()'s or run_tasks()'s raises on a connection the
    worker owns. The error is raised at once and nothing looks for the
    claim, then or later: a look would have raised here.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    tasks.record.enqueue("never claimed")
    looks = []
    real_look = Worker._recover_claims

    def look(self, **kwargs):
        looks.append(self.worker_id)
        return real_look(self, **kwargs)

    def claim(self):
        raise OperationalError("the reply to the claim was lost")

    monkeypatch.setattr(Worker, "_recover_claims", look)
    monkeypatch.setattr(Worker, "claim_one", claim)
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    with pytest.raises(OperationalError, match="reply to the claim was lost"):
        _call(call, worker)

    assert looks == []
    assert look_reads == []
    assert _recovery_events(caplog) == []
    assert worker._claim_unconfirmed_at is None


def test_run_once_makes_no_look_that_run_noted(caplog, look_reads):
    """
    A look run() noted is pending on the Worker when run_once() is called
    on it. run_once() neither makes it nor clears it: the orphan's row stays
    RUNNING and the next task is claimed and run. run()'s next pass is where
    the look is made.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    pending = worker._claim_unconfirmed_at
    after = tasks.record.using(priority=-1).enqueue("after")

    assert worker.run_once() is True

    assert look_reads == []
    assert worker._claim_unconfirmed_at == pending
    assert _row(orphan).status == OxTask.Status.RUNNING
    assert _row(after).status == OxTask.Status.SUCCESSFUL
    worker._recover_claims()
    assert _row(orphan).status == OxTask.Status.READY
    assert _released_ids(caplog, worker) == [str(orphan.id)]


#: What each database's session keeps, and how a claim is made to fail on a
#: connection that still answers: another session holds the task table, and
#: the caller's session waits for it no longer than a moment.
_SESSION = {
    "postgresql": {
        "setup": [
            "SELECT pg_try_advisory_lock(4242)",
            "CREATE TEMPORARY TABLE ox_callers_scratch (x integer)",
            "SET lock_timeout = '300ms'",
        ],
        "checks": {
            "session": "SELECT pg_backend_pid()",
            "advisory lock": (
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND objid = 4242 AND pid = pg_backend_pid() AND granted"
            ),
            "temporary table": "SELECT count(*) FROM ox_callers_scratch",
            "setting": "SHOW lock_timeout",
        },
        "teardown": [
            "SELECT pg_advisory_unlock(4242)",
            "DROP TABLE ox_callers_scratch",
            "RESET lock_timeout",
        ],
    },
    "mysql": {
        "setup": [
            "SELECT GET_LOCK('ox_callers_lock', 0)",
            "CREATE TEMPORARY TABLE ox_callers_scratch (x integer)",
            "SET SESSION lock_wait_timeout = 1",
        ],
        "checks": {
            "session": "SELECT CONNECTION_ID()",
            "advisory lock": "SELECT IS_USED_LOCK('ox_callers_lock') = CONNECTION_ID()",
            "temporary table": "SELECT count(*) FROM ox_callers_scratch",
            "setting": "SELECT @@SESSION.lock_wait_timeout",
        },
        "teardown": [
            "SELECT RELEASE_LOCK('ox_callers_lock')",
            "DROP TEMPORARY TABLE ox_callers_scratch",
            "SET SESSION lock_wait_timeout = DEFAULT",
        ],
    },
    "sqlite": {
        "setup": [
            "CREATE TEMPORARY TABLE ox_callers_scratch (x integer)",
            "PRAGMA busy_timeout = 300",
        ],
        "checks": {
            "temporary table": "SELECT count(*) FROM ox_callers_scratch",
            "setting": "PRAGMA busy_timeout",
        },
        "teardown": ["DROP TABLE ox_callers_scratch"],
    },
}


def _session_state():
    state = {}
    with connection.cursor() as cursor:
        for name, sql in _SESSION[connection.vendor]["checks"].items():
            cursor.execute(sql)
            state[name] = cursor.fetchone()[0]
    return state


class _TableHeld:
    """Another session holds the task table, as a migration that alters it does."""

    def __init__(self):
        table = OxTask._meta.db_table
        if connection.vendor == "sqlite":
            self._other = sqlite3.connect(
                connection.settings_dict["NAME"], isolation_level=None
            )
            self._other.execute("BEGIN EXCLUSIVE")
            return
        self._other = connections.create_connection("default")
        if connection.vendor == "postgresql":
            self._other.set_autocommit(False)
            with self._other.cursor() as cursor:
                cursor.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
        else:
            with self._other.cursor() as cursor:
                cursor.execute(f"LOCK TABLES {table} WRITE")

    def release(self):
        if connection.vendor == "sqlite":
            self._other.execute("ROLLBACK")
            self._other.close()
            return
        if connection.vendor == "postgresql":
            self._other.rollback()
            self._other.set_autocommit(True)
        else:
            with self._other.cursor() as cursor:
                cursor.execute("UNLOCK TABLES")
        self._other.close()


@pytest.mark.parametrize("call", ["run_once", "run_tasks"])
def test_an_inline_claim_error_keeps_the_callers_session(
    call, settings, monkeypatch, caplog, look_reads
):
    """
    The caller's session holds what a session can: an advisory lock, a
    temporary table, a setting. A claim of run_once()'s or run_tasks()'s
    then fails on that connection while it still answers: another session
    holds the task table, and the caller waits for it no longer than its
    own setting allows. The claim's error is raised at once, and the
    caller's connection is the one it had, with all of that still on it.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    claim_errors = _claim_errors(monkeypatch)
    result = tasks.record.enqueue("held")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    session = _SESSION[connection.vendor]
    with connection.cursor() as cursor:
        for sql in session["setup"]:
            cursor.execute(sql)
            if "LOCK(" in sql.upper():
                assert cursor.fetchone()[0], "another session holds the lock"
    driver = connection.connection
    before = _session_state()
    try:
        held = _TableHeld()
        try:
            with pytest.raises(DatabaseError) as raised:
                _call(call, worker)
        finally:
            held.release()
        assert raised.value is claim_errors[0]
        assert connection.connection is driver, "the caller's connection was closed"
        assert _session_state() == before
    finally:
        if connection.connection is driver:
            with connection.cursor() as cursor:
                for sql in session["teardown"]:
                    cursor.execute(sql)
        connection.close()
        if connection.vendor == "postgresql" and _pool_options("default"):
            # A connection closed with the session on it went back to the
            # pool, its advisory lock and temporary table with it.
            connection.close_pool()

    assert look_reads == []
    assert _recovery_events(caplog) == []
    assert _row(result).status == OxTask.Status.READY


@pytest.mark.skipif(
    connection.vendor != "postgresql", reason="psycopg2 is PostgreSQL's driver"
)
def test_with_psycopg2_the_loop_leaves_a_claim_that_raised_to_the_reaper(
    settings, monkeypatch, caplog, lose_the_reply, look_reads
):
    """
    psycopg2, emulated: psycopg_any.is_psycopg3 reads False when the worker
    asks, as with psycopg2 installed, while Django's backend, which read it
    at import, goes on with psycopg 3. The loop's claim commits and loses
    its reply. It is not noted: no pass looks for it and nor does the stop,
    and the row is RUNNING under the worker, the attempt charged, for the
    reaper. The loop goes on claiming.
    """
    from django.db.backends.postgresql import psycopg_any

    monkeypatch.setattr(psycopg_any, "is_psycopg3", False)
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    seam = lose_the_reply("statement")
    lost, after = _enqueue("lost", "after")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    _run_until(worker, lambda: _row(after).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    assert look_reads == []
    assert _recovery_events(caplog) == []
    assert worker._claim_unconfirmed_at is None
    (poll,) = _events(caplog, "worker_poll_failed", worker)
    assert poll.claim_recovery is None
    row = _row(lost)
    assert (row.status, row.locked_by, row.attempts) == (
        OxTask.Status.RUNNING,
        worker.worker_id,
        1,
    )
    assert (_runs("lost"), _runs("after")) == (0, 1)


# -- a database that stops answering -----------------------------------------------


postgresql_relay = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="a relay in front of PostgreSQL; on MySQL the claim itself can wait on "
    "a server that stopped answering, in its transaction's exit, as it always "
    "could",
)

#: Before, the look after a claim error waited at least 40 s on a server that
#: had stopped answering; an inline call makes no look now.
BOUNDED_WELL_UNDER = 20.0


@pytest.fixture
def relay(monkeypatch):
    """
    Every connection to the test database opened from here on, Django's
    pool's included, goes through a relay that can stop answering
    (tests/unanswering.py). go_dark() stops it, and detach() puts
    everything back, for reading the outcome afterwards.
    """
    conn = connections["default"]
    pooled = conn.vendor == "postgresql" and _pool_options("default") is not None
    relay = Unanswering(
        conn.settings_dict["HOST"] or "127.0.0.1", conn.settings_dict["PORT"] or 5432
    )
    conn.close()
    if pooled:
        conn.close_pool()
    monkeypatch.setitem(conn.settings_dict, "HOST", "127.0.0.1")
    monkeypatch.setitem(conn.settings_dict, "PORT", str(relay.port))
    receivers = []
    detached = []

    def go_dark(dark):
        """
        "gone": the connections open now, the pool's idle ones among them,
        never answer again, and a new one is accepted and never answered.
        "hung": the same for the connections open now, and a new one is
        set up and stops answering as soon as Django has it.
        """
        if dark == "gone":
            relay.stall()
            relay.black_hole()
            return
        relay.stall_open()

        def stall_it(sender, connection, **kwargs):
            relay.stall_open()

        connection_created.connect(stall_it, weak=False)
        receivers.append(stall_it)

    def detach():
        if detached:
            return
        detached.append(True)
        for receiver in receivers:
            connection_created.disconnect(receiver)
        relay.close()
        conn.close()
        if pooled:
            conn.close_pool()
        monkeypatch.undo()
        conn.close()

    relay.pooled = pooled
    relay.go_dark = go_dark
    relay.detach = detach
    yield relay
    detach()


class Caller:
    """
    A caller of run_once() or run_tasks() on a thread of its own, as a
    request handler would be. `setup` runs there first, so the connection
    the call starts with is that thread's and already open; the call waits
    for start().
    """

    def __init__(self, setup, call):
        self.ready = threading.Event()
        self._go = threading.Event()
        self.outcome = {}
        self.thread = threading.Thread(target=self._run, args=(setup, call))
        self.thread.daemon = True
        self.thread.start()
        assert self.ready.wait(LIMIT), "the caller's setup did not finish"
        assert "setup_error" not in self.outcome, self.outcome["setup_error"]

    def _run(self, setup, call):
        try:
            try:
                setup()
            except BaseException as exc:
                self.outcome["setup_error"] = exc
                return
            finally:
                self.ready.set()
            self._go.wait(LIMIT)
            started = time.monotonic()
            try:
                self.outcome["value"] = call()
            except BaseException as exc:
                self.outcome["error"] = exc
            finally:
                self.outcome["took"] = time.monotonic() - started
        finally:
            connections.close_all()

    def start(self):
        self._go.set()

    def returned_within(self, seconds):
        self.thread.join(seconds)
        return not self.thread.is_alive()


def _claim_errors(monkeypatch):
    """Every exception a claim raises, as raised."""
    raised = []
    real = Worker.claim_one

    def claim_one(self):
        try:
            return real(self)
        except BaseException as exc:
            raised.append(exc)
            raise

    monkeypatch.setattr(Worker, "claim_one", claim_one)
    return raised


def _open_a_connection():
    OxTask.objects.exists()


@postgresql_relay
@pytest.mark.parametrize("dark", ["gone", "hung"])
@pytest.mark.parametrize("call", ["run_once", "run_tasks"])
def test_an_inline_claim_error_on_a_stalled_database_raises_at_once(
    call, dark, relay, settings, monkeypatch, caplog, lose_the_reply
):
    """
    The database stops answering right after a claim's reply is lost: every
    connection already open, the pool's idle ones included, never answers
    again, and a new one is either never answered or set up and then never
    answered. The call raises the claim's own error without waiting on the
    server: no look, so no connection opened for one and no test of the
    pool's idle connections. The row is RUNNING, the attempt charged, for
    the reaper. Before, the call made a look first and waited on it.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    claim_errors = _claim_errors(monkeypatch)
    if relay.pooled:
        _fill_the_pool()
    untouched = tasks.record.using(priority=-1).enqueue("never claimed")
    result = tasks.record.enqueue("dark")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    seam = lose_the_reply("statement", also=lambda other: relay.go_dark(dark))
    caller = Caller(_open_a_connection, lambda: _call(call, worker))
    caller.start()
    returned = caller.returned_within(BOUNDED_WELL_UNDER)
    relay.detach()
    caller.thread.join(LIMIT)

    assert returned, f"the call was still waiting after {BOUNDED_WELL_UNDER:g}s"
    assert seam.fired
    assert caller.outcome.get("error") is claim_errors[0], caller.outcome
    assert _recovery_events(caplog) == []
    assert worker._claim_unconfirmed_at is None
    row = _row(result)
    assert (row.status, row.attempts) == (OxTask.Status.RUNNING, 1)
    row = _row(untouched)
    assert (row.status, row.attempts) == (OxTask.Status.READY, 0)


@drains_its_pool
@pooled_postgresql
@pytest.mark.parametrize("call", ["run_once", "run_tasks"])
def test_an_inline_claim_error_leaves_the_pool_alone(
    call, settings, monkeypatch, caplog, lose_the_reply
):
    """
    Nothing on the caller's thread touches the connections Django's pool
    holds idle after a claim error, lost connection or not: none is tested
    and none is discarded. The claim's error is raised at once and the pool
    is the caller's process's, as its connection is. Only the loop's failed
    pass sweeps the pool, and only when it lost its connection.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    tasks.record.enqueue("swept")
    touched = _noting_the_sweeps(monkeypatch)
    # The instrument sees a sweep.
    _sweep_pool(connections["default"])
    assert touched == [("drain", threading.current_thread().name)]
    touched.clear()
    seam = lose_the_reply("statement", on=connections["default"])
    worker = Worker(lock_timeout=LOCK_TIMEOUT)

    with pytest.raises(DatabaseError):
        _call(call, worker)

    assert seam.fired
    assert touched == []
    assert _recovery_events(caplog) == []


# -- a stop while the pool's idle connections never answer -------------------------

#: How long the tests below give a worker, on a database that has stopped
#: answering in the way each arranges, to stop or to be back at work. It is
#: the tests' allowance, not a bound the worker holds: one whose every
#: connection is silent waits in its next statement as it always did. The
#: look a stopping worker makes gives up after five seconds, and the rest is
#: room for whatever comes before it on a slow machine. The relay stays dark
#: until the outcome has been judged, so a wait with no limit of its own
#: lasts exactly as long as the test lets it: no machine is fast enough to
#: pass by accident.
STOPS_WITHIN = 20.0


@drains_its_pool
@pooled_postgresql
@pytest.mark.parametrize("stop", ["before-the-sweep", "during-the-sweep"])
def test_a_stop_is_honoured_while_the_pools_idle_connections_never_answer(
    stop, relay, settings, monkeypatch, caplog, lose_the_reply
):
    """
    The claim's reply is lost as the database goes dark: the connections
    Django's pool holds idle stay open and never answer again, and a new
    connection is accepted and never answered. The failed pass lost its
    connection, so it sweeps the pool, and a sweep that tested each idle
    connection would wait for a reply that never comes. It discards them
    untested, so a stop asked for before that pass handles its error, or as
    its sweep begins, is read once the pass is over: the worker makes its
    look for the claim that raised, which gives up after five seconds, and
    run() returns while the database is still dark.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    result = tasks.record.enqueue("dark")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    swept = []

    def sweep(conn, **told):
        swept.append(conn.alias)
        if stop == "during-the-sweep":
            # On the loop's own thread, so the stop is always asked for after
            # the pass has come to its sweep and before the pool is touched.
            worker.request_stop()
        _sweep_pool(conn, **told)

    def go_dark(other):
        relay.go_dark("gone")
        if stop == "before-the-sweep":
            # Before the claim's error reaches the loop.
            worker.request_stop()

    monkeypatch.setattr("django_ox.worker._sweep_pool", sweep)
    seam = lose_the_reply("statement", also=go_dark)

    thread = start_worker_thread(worker)
    assert wait_for(lambda: seam.fired, timeout=LIMIT)
    thread.join(STOPS_WITHIN)
    stopped = not thread.is_alive()
    relay.detach()
    thread.join(LIMIT)

    if stop == "during-the-sweep":
        # The stop is asked for from inside the sweep: no sweep, no stop.
        assert swept == ["default"], "the failed pass never reached its sweep"
    assert stopped, (
        f"the worker was still stopping {STOPS_WITHIN:g}s after it was asked "
        "to, on a database that had stopped answering"
    )
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1
    _given_up(caplog, worker, result)


# -- what the driver says of a connection -------------------------------------------

#: The SQLSTATE of each failure these tests provoke that leaves its connection
#: answering: none is a lost connection.
ANSWERING = {
    "missing-table": "42P01",
    "missing-column": "42703",
    "lock-timeout": "55P03",
}

#: psycopg 3 says of a connection whether it went bad. No other driver here
#: does, psycopg2 included, and none of them has a pool for a failed pass to
#: sweep.
PSYCOPG3 = connection.vendor == "postgresql" and (
    connection.Database.__name__ == "psycopg"
)
says_when_lost = pytest.mark.skipif(
    not PSYCOPG3,
    reason="psycopg 3 reports a connection that went bad; no other driver does",
)


@says_when_lost
def test_a_session_the_server_ended_is_a_lost_connection():
    """
    The statement fails on a session the server ended, and the driver says
    the connection is lost while the wrapper still holds it, before any
    cleanup has closed it.
    """
    connection.ensure_connection()
    held = connection.connection
    with _watching() as seen:
        assert not seen.lost()

        with pytest.raises(OperationalError):
            end_connection()

        assert connection.connection is held
        assert _lost(held)
        assert seen.lost()


@says_when_lost
def test_asking_whether_a_silent_connection_is_lost_does_not_wait(relay):
    """
    The connection is open and the server behind it has stopped answering.
    Asking the driver about it returns at once, where a statement, Django's
    is_usable() among them, would wait for a reply that never comes. It is
    not a lost connection either: nothing has failed on it.
    """
    conn = connections["default"]
    conn.ensure_connection()
    held = conn.connection
    relay.stall()
    answer = []
    asker = threading.Thread(target=lambda: answer.append(_lost(held)), daemon=True)
    asker.start()
    asker.join(5)
    relay.detach()

    assert answer == [False]


@says_when_lost
@pytest.mark.parametrize("began", ["holding-a-connection", "with-no-connection"])
def test_a_session_ended_inside_a_transaction_is_lost_though_django_replaced_it(
    began,
):
    """
    The session ends inside an atomic block. The block's exit cannot roll
    back, closes the connection and opens another, all before the error
    reaches the caller: the connection the caller finds is a new one that
    answers, with no error flagged on it. The one that was lost still says
    so, and it was kept: because it was held when the watch began, or, when
    none was, because the block's own entry opened it, and that is when it
    was kept.
    """
    if began == "with-no-connection":
        connection.close()
    else:
        connection.ensure_connection()
    held = connection.connection
    with _watching() as seen:
        with pytest.raises(DatabaseError), transaction.atomic():
            inside = connection.connection
            end_connection()
        replacement = connection.connection

        assert (held is None) is (began == "with-no-connection")
        assert replacement is not None
        assert replacement is not inside
        assert not _lost(replacement)
        assert connection.errors_occurred is False
        assert _lost(inside)
        assert [raw is inside for raw in seen.raws] == [True, False]
        assert seen.raws[1] is replacement
        assert seen.lost()


@says_when_lost
@pytest.mark.parametrize("failure", ["missing-table", "lock-timeout"])
@pytest.mark.parametrize("inside", [False, True], ids=["autocommit", "transaction"])
def test_an_error_that_leaves_the_connection_answering_is_not_a_lost_one(
    failure, inside
):
    """
    The statement is refused and the connection answers the next one. The
    driver does not call it lost, though Django has flagged an error on it,
    as it flags a lost one: the flag cannot tell the two apart.
    """
    tasks.record.enqueue("locked")
    connection.ensure_connection()
    held = connection.connection
    lock = _TableHeld() if failure == "lock-timeout" else None
    try:
        with _watching() as seen:
            with (
                pytest.raises(DatabaseError) as raised,
                transaction.atomic() if inside else nullcontext(),
                connection.cursor() as cursor,
            ):
                if lock is not None:
                    cursor.execute("SET lock_timeout = '50ms'")
                    cursor.execute(f"SELECT count(*) FROM {OxTask._meta.db_table}")  # noqa: S608
                else:
                    cursor.execute("SELECT * FROM ox_no_such_table")
            flagged = connection.errors_occurred
            lost = seen.lost()
    finally:
        if lock is not None:
            lock.release()
            with connection.cursor() as cursor:
                cursor.execute("RESET lock_timeout")

    expected = OperationalError if lock is not None else ProgrammingError
    assert isinstance(raised.value, expected)
    assert raised.value.__cause__.sqlstate == ANSWERING[failure]
    assert connection.connection is held
    assert not lost
    # Flagged outside a transaction; inside one the rollback cleared it.
    assert flagged is not inside
    assert _row_count() == 1


def _row_count():
    return OxTask.objects.count()


@says_when_lost
def test_a_connection_closed_on_purpose_or_never_opened_is_not_a_lost_one(
    monkeypatch,
):
    """
    A connection Django closed, as its cleanup closes one past its age, did
    not go bad, and neither did one that could not be had: the pool timed
    out, or the server refused. Nothing was lost, whatever Django flagged,
    and after the failed connect nothing was even seen.
    """
    import psycopg

    connection.ensure_connection()
    with _watching() as seen:
        held = connection.connection
        connection.close()
        assert connection.connection is None
        assert not _lost(held)
        assert not seen.lost()

    def refused(conn_params):
        raise psycopg.OperationalError("couldn't get a connection after 30.00 sec")

    with _watching() as seen:
        monkeypatch.setattr(connection, "get_new_connection", refused)
        with pytest.raises(OperationalError):
            connection.ensure_connection()
        monkeypatch.undo()
        assert connection.connection is None
        assert connection.errors_occurred is True
        assert seen.raws == []
        assert not seen.lost()


@pytest.mark.skipif(
    PSYCOPG3, reason="MySQL, SQLite and psycopg2: drivers that do not say"
)
def test_a_driver_that_does_not_say_reports_no_connection_lost():
    """
    MySQL's and SQLite's drivers and psycopg2 have nothing to ask, so no
    pass of the loop ever counts as having lost its connection there, even
    one that did; and there is no pool on them for it to sweep.
    """
    connection.ensure_connection()
    held = connection.connection
    with _watching() as seen:
        if connection.vendor != "sqlite":
            with pytest.raises(DatabaseError):
                end_connection()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
            assert not connection.is_usable()
        assert not _lost(held)
        assert not _lost(connection.connection)
        assert not seen.lost()
    assert _connection_pool(connection) is None
    connection.close()


# -- which failed passes sweep Django's pool ----------------------------------------


def _other_sessions(cursor=None):
    """The backends of every other session on the test database."""
    sql = (
        "SELECT pid FROM pg_stat_activity "
        "WHERE datname = current_database() AND pid <> pg_backend_pid()"
    )
    if cursor is not None:
        return {pid for (pid,) in cursor.execute(sql).fetchall()}
    with connection.cursor() as own:
        own.execute(sql)
        return {pid for (pid,) in own.fetchall()}


def _opened_since(before):
    """The connections Django's pool has opened since `before`, its stats then."""
    now = connections["default"].pool.get_stats()
    return now.get("connections_num", 0) - before.get("connections_num", 0)


def _a_connection_made_now(conn):
    """
    A psycopg connection of the test's own to `conn`'s database, through
    whatever `conn`'s settings name, a relay included. Made after the relay
    silenced what was open, it is answered, which the test thread's Django
    connection, open before, no longer is.
    """
    import psycopg

    params = conn.get_connection_params()
    return psycopg.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        dbname=params["dbname"],
        autocommit=True,
    )


@drains_its_pool
@pooled_postgresql
def test_a_pass_that_lost_its_connection_discards_the_idle_connections_untested(
    settings, caplog, lose_the_reply
):
    """
    The server ends the loop's session and no other: the connections the
    pool holds idle would still answer. The failed pass does not ask them.
    It discards every one, and the pool opens others; the claim that raised
    is released on a new connection and its task runs once. A sweep that
    tested the idle connections kept the ones that answered.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    result = tasks.record.enqueue("reset")
    _fill_the_pool()
    held_idle = _other_sessions()
    assert len(held_idle) >= 3
    seam = lose_the_reply("statement")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1
    assert _released_ids(caplog, worker) == [str(result.id)]
    row = _row(result)
    assert (row.status, row.attempts, _runs("reset")) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    )
    # One was the loop's, which the server ended; the pool closed the rest.
    assert wait_for(lambda: not (_other_sessions() & held_idle), timeout=10), (
        "the pool still holds a connection it held idle when the loop lost its own"
    )


@drains_its_pool
@postgresql_relay
def test_a_failed_pass_asks_nothing_of_a_connection_that_never_answers(
    relay, monkeypatch, caplog
):
    """
    The loop's connection is open and has stopped being answered, and the
    pass fails without any statement having failed on it: the error is
    another connection's. The handler asks the driver whether the loop's
    connection is lost, which it is not, and asks the server nothing: the
    pass is over at once, the stop is read, and run() returns with the
    connection still silent. A handler that probed the connection waited
    on it for as long as it stayed silent.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    def reap():
        connections[worker._db_alias].ensure_connection()
        relay.stall()
        worker.request_stop()
        raise OperationalError("the error of a connection that is not the loop's")

    monkeypatch.setattr(worker, "reap", reap)

    thread = start_worker_thread(worker)
    thread.join(STOPS_WITHIN)
    stopped = not thread.is_alive()
    relay.detach()
    thread.join(LIMIT)

    assert stopped, (
        f"the worker was still in its failed pass {STOPS_WITHIN:g}s after it "
        "failed, on a connection that had stopped answering"
    )
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1


class ClaimsInATransaction(Worker):
    """
    A queryset claim filter without its SQL, which on PostgreSQL gives up
    the single-statement claim for the one inside a transaction.
    """

    def claim_filter_q(self):
        return Q(pk__isnull=False)


@drains_its_pool
@pooled_postgresql
def test_a_connection_lost_inside_the_claims_transaction_still_sweeps_the_pool(
    settings, caplog, lose_the_reply
):
    """
    The claim runs in a transaction, and the server ends the session before
    its commit. Django's exit from the block closes that connection and
    checks another out of the pool before the error reaches the loop, so
    the connection the failed pass finds answers. The pass still lost the
    one it began on: it discards the pool's idle connections, and the
    second task, whose claim rolled back, is claimed again and runs once.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    first, second = _enqueue("t1", "t2")
    _fill_the_pool()
    held_idle = _other_sessions()
    assert len(held_idle) >= 3
    # The second claim, so the pass it fails in began on a connection. One
    # task at a time, so the first has finished by then.
    seam = lose_the_reply("before", nth=2)
    worker = ClaimsInATransaction(
        concurrency=1, lock_timeout=LOCK_TIMEOUT, poll_interval=0.05
    )

    _run_until(
        worker,
        lambda: all(
            _row(r).status == OxTask.Status.SUCCESSFUL for r in (first, second)
        ),
    )

    assert seam.fired
    # The claim that lost its session had not committed: it rolled back.
    assert seam.seen == [
        (OxTask.Status.READY, None, 0, 0),
        (OxTask.Status.SUCCESSFUL, None, 1, 1),
    ]
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1
    assert (_runs("t1"), _runs("t2")) == (1, 1)
    assert wait_for(lambda: not (_other_sessions() & held_idle), timeout=10), (
        "the pool still holds a connection it held idle when the claim lost its own"
    )


@drains_its_pool
@pooled_postgresql
def test_a_pass_that_began_with_no_connection_and_lost_one_in_a_transaction_sweeps(
    settings, caplog, lose_the_reply
):
    """
    The first pass of a worker begins with no connection, as does the pass
    after any that failed. This one never reaps, so its claim is the first
    thing to reach the database: the connection is opened for the claim,
    the claim runs in a transaction, and the server ends the session before
    its commit. Django's exit from the block replaces the connection before
    the error reaches the loop. Nothing the pass began on says a connection
    was lost, and the one in hand answers; the one that was lost was kept
    when it was opened, and the pass discards the pool's idle connections.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    result = tasks.record.enqueue("first")
    _fill_the_pool()
    held_idle = _other_sessions()
    assert len(held_idle) >= 3
    seam = lose_the_reply("before")
    worker = ClaimsInATransaction(
        concurrency=1,
        lock_timeout=LOCK_TIMEOUT,
        poll_interval=0.05,
        reap_interval=math.inf,
    )

    _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    # The claim that lost its session had not committed: it rolled back.
    assert seam.seen == [(OxTask.Status.READY, None, 0, 0)]
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1
    row = _row(result)
    assert (row.status, row.attempts, _runs("first")) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    )
    assert wait_for(lambda: not (_other_sessions() & held_idle), timeout=10), (
        "the pool still holds a connection it held idle when the claim lost its own"
    )


class ClaimMissesItsTable(Worker):
    """A claim that names a table the database does not have."""

    def claim_filter_q(self):
        return Q(pk__in=RawSQL("SELECT id FROM ox_no_such_table", ()))


#: Failed passes each of those tests lets by before it judges the pool.
PASSES = 20


@pooled_postgresql
@pytest.mark.parametrize("failure", list(ANSWERING))
def test_a_pass_that_failed_on_a_connection_that_answers_leaves_the_pool_alone(
    failure, settings, caplog
):
    """
    Every pass fails and the loop's connection answers throughout: the
    claim names a table or a column that is not there, or the pass waits on
    a lock for longer than its session allows. Twenty failed passes later
    the pool has opened no connection: it replaces every one it closes, so
    it closed none. A sweep on each of those passes discarded the idle
    connections, healthy as they were, and opened the pool again every poll
    interval.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    stats = connections["default"].pool.get_stats()
    held = None
    impatient = None
    if failure == "missing-table":
        worker = ClaimMissesItsTable(lock_timeout=LOCK_TIMEOUT, poll_interval=0.02)
    elif failure == "missing-column":
        worker = ClaimFails(lock_timeout=LOCK_TIMEOUT, poll_interval=0.02)
        worker.failing = True
    else:
        worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.02)

        def impatient(sender, connection, **kwargs):
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '50ms'")

        connection_created.connect(impatient, weak=False)
        held = _TableHeld()

    def failed():
        return _events(caplog, "worker_poll_failed", worker)

    thread = start_worker_thread(worker)
    try:
        assert wait_for(lambda: len(failed()) >= PASSES, timeout=LIMIT)
        opened = _opened_since(stats)
    finally:
        if held is not None:
            # First, or the look a stopping worker makes waits out its five
            # seconds on the table.
            held.release()
            connection_created.disconnect(impatient)
        worker.request_stop()
        thread.join(timeout=60)
        if held is not None:
            # The sessions that carry the setting go with the pool.
            connection.close()
            connection.close_pool()
    assert not thread.is_alive(), "the worker did not stop"

    states = {r.exc_info[1].__cause__.sqlstate for r in failed()[:PASSES]}
    assert states == {ANSWERING[failure]}
    assert opened == 0, f"the pool opened {opened} connection(s) in {PASSES} passes"


@drains_its_pool
@pooled_postgresql
def test_after_a_failover_that_resets_nothing_the_worker_is_back_at_work(
    relay, settings, caplog, lose_the_reply
):
    """
    The claim's reply is lost, and from then on the connections that were
    open, the pool's idle ones among them, stay open and are never answered
    again, while a new connection works: a failover that reset nothing. The
    failed pass discards the idle connections without asking them anything,
    so the next pass runs on a new one: the lost claim is released and both
    tasks run, while the old connections are still silent. A sweep that
    tested them waited on the first for as long as it stayed silent.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 3)
    _fill_the_pool()
    lost = tasks.record.enqueue("lost")
    later = tasks.record.using(priority=-1).enqueue("later")
    ids = [str(lost.id), str(later.id)]
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    seam = lose_the_reply("statement", also=lambda other: relay.stall_open())

    thread = start_worker_thread(worker)
    assert wait_for(lambda: seam.fired, timeout=LIMIT)
    with _a_connection_made_now(connections["default"]) as fresh:

        def rows():
            return fresh.execute(
                "SELECT status, attempts FROM django_ox_oxtask "
                "WHERE id = ANY(%s::uuid[]) ORDER BY priority DESC",
                [ids],
            ).fetchall()

        done = [(OxTask.Status.SUCCESSFUL, 1)] * 2
        back_at_work = wait_for(lambda: rows() == done, timeout=STOPS_WITHIN)
        seen = rows()
        worker.request_stop()
        thread.join(STOPS_WITHIN)
        stopped = not thread.is_alive()
    relay.detach()
    thread.join(LIMIT)

    assert back_at_work, (
        f"the tasks were not both done {STOPS_WITHIN:g}s after the claim's reply "
        f"was lost, with the old connections still silent: {seen}"
    )
    assert stopped, "the worker did not stop while the old connections were silent"
    assert (_runs("lost"), _runs("later")) == (1, 1)
    assert _released_ids(caplog, worker) == [str(lost.id)]
    assert len(_events(caplog, "worker_poll_failed", worker)) == 1


@drains_its_pool
@pooled_postgresql
def test_a_task_that_lost_its_connection_is_recorded_beside_silent_idle_ones(
    relay, settings, caplog
):
    """
    The sweep on a task's thread. A task queries, its session is ended, and
    from that moment every connection open then is silent, the pool's idle
    ones among them, while a new connection works. The task's next query
    fails; it catches the error and returns. Before the outcome is written
    the dead connection is dropped and the pool swept, and the write lands
    on a new connection while the idle ones are still silent. A sweep that
    tested them waited on the first, the row stayed RUNNING, and its lease
    kept being renewed.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    conn = connections["default"]
    ended = threading.Event()

    def end_session(own):
        relay.stall_open()
        with _a_connection_made_now(conn) as fresh:
            fresh.execute("SELECT pg_terminate_backend(%s, 10000)", [own])
        ended.set()

    tasks.STATE["end_session"] = end_session
    result = queries_after_its_session_ended.enqueue()
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    thread = start_worker_thread(worker)
    # A connection made before the relay silenced what was open would be
    # silent too.
    assert ended.wait(LIMIT), "the task never had its session ended"
    with _a_connection_made_now(conn) as fresh:

        def row():
            return fresh.execute(
                "SELECT status, attempts, return_value FROM django_ox_oxtask "
                "WHERE id = %s",
                [str(result.id)],
            ).fetchone()

        recorded = (OxTask.Status.SUCCESSFUL, 1, "succeeded anyway")
        written = wait_for(lambda: row() == recorded, timeout=STOPS_WITHIN)
        seen = row()
    relay.detach()
    worker.request_stop()
    thread.join(LIMIT)

    assert tasks.STATE.get("flagged") is True
    assert written, (
        f"the outcome was not recorded {STOPS_WITHIN:g}s after the task returned, "
        f"with the pool's idle connections still silent: {seen}"
    )
    assert not thread.is_alive(), "the worker did not stop"
    assert _events(caplog, "task_outcome_unrecorded", worker) == []


# -- which outcome writes sweep Django's pool ---------------------------------------


@drains_its_pool
@pooled_postgresql
def test_an_outcome_write_whose_checkout_timed_out_discards_no_connection(
    settings, monkeypatch, caplog
):
    """
    The task ends holding no connection, and the pool has none to give its
    outcome write in time: the checkout times out, as it does on a pool too
    small for the worker. That earns the write its second try, which lands.
    No connection was lost, so the pool is left alone: it opens none, and a
    pool that is only too small is not discarded at every such write.
    """
    from psycopg_pool import PoolTimeout

    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    pool = connections["default"].pool
    touched = _noting_the_sweeps(monkeypatch)
    armed, timed_out = [], []
    real_getconn = type(pool).getconn

    def getconn(self, *args, **kwargs):
        if armed and not timed_out:
            timed_out.append(threading.current_thread().name)
            raise PoolTimeout("couldn't get a connection after 30.00 sec")
        return real_getconn(self, *args, **kwargs)

    monkeypatch.setattr(type(pool), "getconn", getconn)
    tasks.STATE["gave_it_back"] = lambda: armed.append(True)
    result = gives_its_connection_back.enqueue()
    stats = pool.get_stats()
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=30.0)

    _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)

    assert len(timed_out) == 1 and timed_out[0].startswith("ox_")
    (reconnected,) = _events(caplog, "task_outcome_reconnected", worker)
    assert reconnected.already_written is False
    assert "PoolTimeout" not in reconnected.getMessage()
    assert "couldn't get a connection" in reconnected.getMessage()
    assert _row(result).attempts == 1
    assert touched == []
    assert _opened_since(stats) == 0


class WritesInATransaction(Worker):
    """
    Records each outcome inside a transaction of its own and, when the
    connection goes there, closes the one Django opened in its place before
    the error goes on, so that none is open and the write is
    tried again.
    """

    def _write_outcome(self, db_task, **kwargs):
        conn = connections[self._db_alias]
        if not conn.get_autocommit() or conn.in_atomic_block:
            return super()._write_outcome(db_task, **kwargs)
        driver = conn.connection
        try:
            with transaction.atomic(using=self._db_alias):
                return super()._write_outcome(db_task, **kwargs)
        except Error:
            if conn.connection is not None and conn.connection is not driver:
                with suppress(Error):
                    conn.close()
            raise


@drains_its_pool
@pooled_postgresql
@pytest.mark.parametrize(
    "give_back", [False, True], ids=["held-before-the-write", "opened-by-the-write"]
)
def test_an_outcome_written_in_a_transaction_lands_after_a_restart(
    give_back, settings, monkeypatch, caplog
):
    """
    Every session ends while the task works on, and its outcome is written
    by an override that uses a transaction. The write
    fails on a dead connection, which the task held or which the write
    itself checked out of the pool; Django replaces it on leaving the
    block, with another dead one from the pool; and the override closes
    that, leaving none open. None open is also what a checkout that timed
    out leaves, and that discards nothing. Here a connection was lost, and
    it was kept when it was opened: the pool's idle connections are
    discarded, and the second try lands on a new one. The loop sleeps
    through all of it, so no failed pass of its own sweeps the pool first.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    touched = _noting_the_sweeps(monkeypatch)
    tasks.STATE["ready"] = threading.Event()
    tasks.STATE["go"] = threading.Event()
    result = works_on_through_a_restart_in_process.enqueue(give_back)
    worker = WritesInATransaction(
        concurrency=1, lock_timeout=LOCK_TIMEOUT, poll_interval=30.0
    )

    thread = start_worker_thread(worker)
    try:
        assert tasks.STATE["ready"].wait(LIMIT)
        assert touched == []
        ended = restart_every_other_session(connection)
        tasks.STATE["go"].set()
        recorded = wait_for(
            lambda: _row(result).status != OxTask.Status.RUNNING, timeout=LIMIT
        )
        swept = list(touched)
    finally:
        tasks.STATE["go"].set()
        worker.request_stop()
        thread.join(timeout=60)

    assert not thread.is_alive(), "the worker did not stop"
    assert ended >= 3
    assert recorded
    assert _events(caplog, "task_outcome_unrecorded", worker) == []
    row = _row(result)
    assert (row.status, row.attempts, row.return_value) == (
        OxTask.Status.SUCCESSFUL,
        1,
        "succeeded anyway",
    )
    (reconnected,) = _events(caplog, "task_outcome_reconnected", worker)
    assert reconnected.already_written is False
    # The outcome's own sweep, on the task's thread. The loop wakes once the
    # task is done, finds its own connection dead and sweeps as well.
    assert [name for name, thread in swept if thread.startswith("ox_")] == ["drain"]
    assert "check" not in [name for name, _ in swept]


# -- a pool that cannot discard its idle connections ---------------------------------


@pytest.fixture
def psycopg_pool_before_3_3(monkeypatch):
    """
    Django's pool as psycopg_pool had it before 3.3, with no drain(), in a
    process in which no alias has been told so yet.
    """
    monkeypatch.delattr("psycopg_pool.ConnectionPool.drain", raising=False)
    monkeypatch.setattr("django_ox.worker._cannot_drain", set())
    assert not hasattr(connections["default"].pool, "drain")


@pooled_postgresql
def test_a_pool_that_cannot_drain_is_tested_and_keeps_the_connections_that_answer(
    psycopg_pool_before_3_3, settings, monkeypatch, caplog, lose_the_reply
):
    """
    On psycopg_pool before 3.3 the sweep is 1.7.0's. The server ends the
    loop's session and no other; the failed pass tests the pool's idle
    connections, and the ones that answer are the ones it holds
    afterwards. The claim that raised is released and its task runs once,
    and it is said once that this pool cannot be drained.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    result = tasks.record.enqueue("legacy")
    _fill_the_pool()
    held_idle = _other_sessions()
    touched = _noting_the_sweeps(monkeypatch)
    seam = lose_the_reply("statement")
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)

    assert seam.fired
    assert [name for name, _ in touched] == ["check"]
    assert _released_ids(caplog, worker) == [str(result.id)]
    assert (_row(result).attempts, _runs("legacy")) == (1, 1)
    answering = held_idle - {seam._session}
    assert len(answering) >= 2
    assert answering <= _other_sessions()
    (said,) = _events(caplog, "connection_pool_cannot_drain")
    assert said.database == "default"


@pooled_postgresql
def test_a_pool_that_cannot_drain_is_tested_on_every_failed_pass_as_before(
    psycopg_pool_before_3_3, settings, monkeypatch, caplog
):
    """
    1.7.0 tested the pool after every failed pass, whatever failed, and a
    pool that cannot drain is still treated exactly so: here the claim
    names a column that is not there, on a connection that answers. Its
    idle connections answer too, are kept, and none is opened.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    _fill_the_pool()
    touched = _noting_the_sweeps(monkeypatch)
    stats = connections["default"].pool.get_stats()
    worker = ClaimFails(lock_timeout=LOCK_TIMEOUT, poll_interval=0.02)
    worker.failing = True

    def failed():
        return _events(caplog, "worker_poll_failed", worker)

    thread = start_worker_thread(worker)
    try:
        assert wait_for(lambda: len(failed()) >= 5, timeout=LIMIT)
        passes, tested = len(failed()), [name for name, _ in touched]
        opened = _opened_since(stats)
    finally:
        worker.request_stop()
        thread.join(timeout=60)
    assert not thread.is_alive(), "the worker did not stop"

    assert set(tested) == {"check"}
    assert passes - 1 <= len(tested) <= passes + 1
    assert opened == 0
