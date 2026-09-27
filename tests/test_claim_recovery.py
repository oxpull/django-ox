"""
What a worker does about a claim that raised though it may have committed.

tests/test_lost_commit_reply.py proves the outcome for one task. These prove
the rules the look lives by: which rows it may release and which it must
never touch, how the release is written, when the look runs, and what it
does when its own statements fail. tests/lost_reply.py loses the replies.
"""

import copy
import logging
import threading
import time
import traceback

import pytest
from django.db import (
    DatabaseError,
    InterfaceError,
    OperationalError,
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
    _pool_options,
    _sweep_pool,
    _Watch,
)

from . import tasks
from .conftest import start_worker_thread, wait_for
from .dead_connection_tasks import from_another_connection, restart_every_other_session
from .lost_reply import COMMITTED_CLAIM_WINDOWS, Seams, is_the_release
from .unanswering import Unanswering

pytestmark = pytest.mark.django_db(transaction=True)

#: No test here waits on a lease: the reaper is only ever run on purpose.
LOCK_TIMEOUT = 300.0
LIMIT = 60.0

pooled_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql" or _pool_options("default") is None,
    reason="Django's connection pool: run with a pooled PostgreSQL settings module",
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


def test_a_look_waits_for_a_claim_on_its_way_back(monkeypatch):
    """
    One claimer per worker. A claim has committed on one thread and is on
    its way back, not yet registered; a look started on another thread must
    wait for it, and then leaves its row alone.
    """
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = tasks.record.enqueue("in-transit")
    committed = threading.Event()
    go_on = threading.Event()
    real = worker._claim_one

    def claim_then_pause():
        db_task = real()
        committed.set()
        go_on.wait(timeout=LIMIT)
        return db_task

    monkeypatch.setattr(worker, "_claim_one", claim_then_pause)
    reads = []

    def count(execute, sql, params, many, context):
        if "ox_lease_abandoned" in sql:
            reads.append(sql)
        return execute(sql, params, many, context)

    def install(sender, connection, **kwargs):
        connection.execute_wrappers.append(count)

    claimed = []
    claimer = threading.Thread(
        target=lambda: (claimed.append(worker._claim()), connections.close_all())
    )
    connection_created.connect(install, weak=False)
    try:
        claimer.start()
        assert committed.wait(timeout=LIMIT)
        worker._claim_unconfirmed_at = time.monotonic()
        looker = threading.Thread(
            target=lambda: (worker._recover_claims(), connections.close_all())
        )
        looker.start()
        looker.join(timeout=3)
        assert reads == [], "the look read while a claim was on its way back"
        go_on.set()
        claimer.join(timeout=LIMIT)
        looker.join(timeout=LIMIT)
    finally:
        go_on.set()
        connection_created.disconnect(install)
    assert len(reads) == 1
    assert str(claimed[0].id) == str(result.id)
    assert _row(result).status == OxTask.Status.RUNNING


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


@pytest.mark.parametrize("entry", ["loop", "run_once"])
@pytest.mark.parametrize("release_window", ["before", "statement"])
def test_a_release_whose_reply_is_lost_refunds_once(
    release_window, entry, settings, caplog, lose_the_reply
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
    on = connections["default"] if entry == "run_once" else None
    claim = lose_the_reply(claim_window, on=on)
    release = lose_the_reply(release_window, statement="release", on=on)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)
    result = tasks.record.enqueue("once")

    if entry == "loop":
        _run_until(worker, lambda: _row(result).status == OxTask.Status.SUCCESSFUL)
    else:
        with pytest.raises(DatabaseError) as raised:
            worker.run_once()
        # As any caller does after a database error: drop a connection that
        # died.
        close_old_connections()
        # The claim's own error, never the look's.
        frames = traceback.format_exception(raised.value)
        assert "_release_claim" not in "".join(frames)
        assert "claim_one" in "".join(frames)
        assert _row(result).status == (
            OxTask.Status.READY if release.landed else OxTask.Status.RUNNING
        )
        failed = _events(caplog, "worker_claim_recovery_failed", worker)
        assert failed
        # Recovery stays due: the next call looks again before claiming.
        assert {event.claim_recovery for event in failed} == {"pending"}
        assert worker.run_once() is True

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
    connections["default"].execute_wrappers.append(race)
    try:
        with pytest.raises(DatabaseError):
            worker.run_once()
    finally:
        connections["default"].execute_wrappers.remove(race)
    close_old_connections()

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

    with pytest.raises(DatabaseError):
        worker.run_once()
    close_old_connections()

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


def _read_after_the_restart(read):
    """The test's own connection died with the rest; read on a new one."""
    connections["default"].close()
    _sweep_pool(connections["default"])
    return read()


@pooled_postgresql
@pytest.mark.parametrize("entry", ["loop", "run_once"])
def test_after_every_pooled_connection_died_the_look_still_lands(
    entry, settings, caplog, lose_the_reply
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
    on = connections["default"] if entry == "run_once" else None
    seam = lose_the_reply("statement", on=on, also=restart_every_other_session)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05)

    if entry == "loop":
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
    else:
        with pytest.raises(DatabaseError):
            worker.run_once()
        row = _read_after_the_restart(lambda: _row(result))
        assert (row.status, row.attempts) == (OxTask.Status.READY, 0)
        assert worker.run_once() is True

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


@pytest.mark.parametrize("stopping", [False, True])
@pytest.mark.parametrize("block", ["manual", "atomic"])
def test_a_look_that_fails_leaves_a_callers_transaction_alone(
    block, stopping, monkeypatch, caplog
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
        worker._recover_claims_once(stopping=stopping)
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
    A stopping look given up leaves the thread's connection as it is: there
    is no next statement for it, and closing a pooled one sweeps the pool,
    which can wait on the very server the look gave up on.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)

    def gave_up(deadline):
        raise OperationalError("no reply from the server by the deadline")

    monkeypatch.setattr(worker, "_recover_claims_by", gave_up)
    driver = connection.connection
    assert driver is not None
    worker._recover_claims_once(stopping=True)
    assert connection.connection is driver
    _given_up(caplog, worker, result)


def test_a_stopping_look_does_not_wait_past_its_deadline_for_the_claimer(caplog):
    """
    Another thread sharing the Worker holds the claimer lock, as a claim on
    its way back does. The stopping look waits for it no longer than its
    deadline, and gives up.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    result = _orphan(worker)
    holding = threading.Event()
    release = threading.Event()

    def hold():
        with worker._claimer:
            holding.set()
            release.wait(LIMIT)

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert holding.wait(LIMIT)
        took = _stop(worker)
    finally:
        release.set()
        holder.join(LIMIT)

    assert 4.5 <= took < 8.0
    _given_up(caplog, worker, result)


# -- what a failed look says next --------------------------------------------------


LOOKS_AGAIN = (
    "Claim outcome is unknown. This Worker will look again before its next claim "
    "while the LOCK_TIMEOUT recovery window remains open. Recovery is not "
    "guaranteed; an unrecovered claim may be reaped with its attempt spent."
)
NO_LATER_LOOK = (
    "Claim outcome is unknown. Recovery looks are limited to this run_tasks() "
    "call; pending recovery state is not retained for later calls. Recovery is "
    "not guaranteed; an unrecovered claim may be reaped with its attempt spent."
)


@pytest.mark.parametrize("caller", ["run_once", "run_tasks"])
def test_a_failed_look_says_what_happens_next_for_its_caller(
    caller, monkeypatch, caplog
):
    """
    run_once()'s Worker is the caller's and looks again before its next
    claim, so recovery is still pending; run_tasks() builds one per call,
    and nothing looks later, so it has expired.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    tasks.record.enqueue("never claimed")
    real_look = Worker._recover_claims

    def look(self, **kwargs):
        if self._claim_unconfirmed_at is not None:
            raise OperationalError("the database is still gone")
        return real_look(self, **kwargs)

    def claim(self):
        raise OperationalError("the reply to the claim was lost")

    monkeypatch.setattr(Worker, "_recover_claims", look)
    monkeypatch.setattr(Worker, "claim_one", claim)
    with pytest.raises(OperationalError, match="reply to the claim was lost"):
        if caller == "run_once":
            Worker(lock_timeout=LOCK_TIMEOUT).run_once()
        else:
            run_tasks()

    (failed,) = _events(caplog, "worker_claim_recovery_failed")
    message = failed.getMessage()
    said, not_said = (
        (LOOKS_AGAIN, NO_LATER_LOOK)
        if caller == "run_once"
        else (NO_LATER_LOOK, LOOKS_AGAIN)
    )
    assert message.endswith(". " + said)
    assert not_said not in message
    assert "the database is still gone" in message
    assert failed.claim_recovery == ("pending" if caller == "run_once" else "expired")
