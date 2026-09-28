"""
One Worker shared by threads: claims, and looks for a claim that raised.

A claim counts itself in flight while it runs and registers what it returned
before it counts itself out; a look makes no read while a claim is in flight,
and releases nothing if a claim began while it read. Nothing is held across a
claim, so a claim that waits on the database, an override's statements after
the base claim included, holds up no other claim of the Worker's, which is
what a module-level Worker in a threaded web process needs when a view claims
inside its own transaction. Worker._claiming says how.
"""

import collections
import ctypes
import logging
import random
import sys
import threading
import time
import traceback

import pytest
from django.db import (
    DatabaseError,
    OperationalError,
    close_old_connections,
    connection,
    connections,
    transaction,
)
from django.db.backends.signals import connection_created
from django.db.models import F

from django_ox import worker as worker_module
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .dead_connection_tasks import end_session, from_another_connection
from .lost_reply import is_the_release, session_of
from .test_timeouts import interruptible_attempts  # noqa: F401 (a fixture)

pytestmark = pytest.mark.django_db(transaction=True)

#: No test here waits on a lease unless it says so.
LOCK_TIMEOUT = 300.0
LIMIT = 60.0

#: How long a call that is waiting on nothing may take here, far above what
#: any of them takes and far below a wait that never ends.
PROMPT = 5.0

#: A deadlock is called one once both threads have waited this long.
DEADLOCK = 20.0


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


def _state(result):
    row = _row(result)
    return (row.status, row.attempts, row.lease_epoch)


def _orphan(worker, label="orphan"):
    """
    A row RUNNING under `worker`'s id that no claim of its returned, and a
    look pending: what a claim that raised after committing leaves.
    """
    result = tasks.record.enqueue(label)
    # The base claim, whatever the class under test overrides.
    db_task = Worker.claim_one(worker)
    assert db_task is not None and str(db_task.id) == str(result.id)
    with worker._in_flight_lock:
        worker._handed_off.clear()
    worker._claim_unconfirmed_at = time.monotonic()
    return result


def _on_a_thread(target, name):
    """target() on a thread of its own, and so a connection of its own."""
    out = {}

    def run():
        try:
            out["returned"] = target()
        except BaseException as exc:
            out["raised"] = exc
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.start()
    return thread, out


# -- a claim that waits on the database holds up no other claim --------------------


class CountsAfterClaiming(Worker):
    """
    Counts every claim it makes on a row of its own after the base claim has
    returned, as Oxpull Pro counts a rate-limited admission: an UPDATE whose
    row lock a caller's transaction holds until it commits.
    """

    counter = None

    def claim_one(self):
        db_task = super().claim_one()
        if db_task is not None:
            OxTask.objects.using(self._db_alias).filter(pk=self.counter).update(
                priority=F("priority") + 1
            )
        return db_task


def _waiting_on_a_row_lock(other):
    with other.cursor() as cursor:
        if other.vendor == "postgresql":
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = "
                "current_database() AND wait_event_type = 'Lock'"
            )
        else:
            cursor.execute(
                "SELECT count(*) FROM information_schema.innodb_trx "
                "WHERE trx_state = 'LOCK WAIT'"
            )
        return cursor.fetchone()[0] > 0


def _two_callers(worker, *, wait_for_a, pause=1.0):
    """
    B calls run_once() twice inside one atomic() block; A calls run_once() on
    the same Worker after B's first call, while B's transaction is open. B
    makes its second call once wait_for_a() says A is waiting on B, or after
    `pause` when it is None. A deadlock is broken after DEADLOCK seconds by
    ending B's session, and reported as blocked.
    """
    out = {}
    b_first = threading.Event()
    a_started = threading.Event()

    def b():
        try:
            connection.ensure_connection()
            if connection.vendor != "sqlite":
                out["b_session"] = session_of(connection)
            with transaction.atomic():
                out["b1"] = worker.run_once()
                b_first.set()
                a_started.wait(LIMIT)
                if wait_for_a is None:
                    time.sleep(pause)
                else:
                    out["a_waited"] = wait_for(wait_for_a, timeout=10.0, interval=0.1)
                began = time.monotonic()
                out["b2"] = worker.run_once()
                out["b2_took"] = time.monotonic() - began
            out["b"] = "committed"
        except BaseException as exc:
            out["b"] = f"raised {exc!r}"
        finally:
            connections.close_all()

    def a():
        try:
            b_first.wait(LIMIT)
            a_started.set()
            began = time.monotonic()
            try:
                out["a"] = worker.run_once()
            except DatabaseError as exc:
                out["a"] = f"raised {exc!r}"
            out["a_took"] = time.monotonic() - began
        finally:
            connections.close_all()

    tb = threading.Thread(target=b, name="caller-b", daemon=True)
    ta = threading.Thread(target=a, name="caller-a", daemon=True)
    started = time.monotonic()
    tb.start()
    ta.start()
    tb.join(DEADLOCK)
    ta.join(max(DEADLOCK - (time.monotonic() - started), 0.1))
    blocked = {"a": ta.is_alive(), "b": tb.is_alive()}
    if any(blocked.values()) and "b_session" in out:
        from_another_connection(lambda other: end_session(other, out["b_session"]))
    tb.join(LIMIT)
    ta.join(LIMIT)
    assert not (tb.is_alive() or ta.is_alive()), "a caller never returned"
    return out, blocked


@pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="PostgreSQL and MySQL: a row lock the override's UPDATE waits on; "
    "on SQLite the claim itself waits, test_a_claim_waiting_on_sqlites_write_lock",
)
def test_an_override_waiting_on_a_callers_transaction_holds_up_no_claim(settings):
    """
    X3-1 without Oxpull Pro. One Worker shared by two threads, whose
    claim_one() override updates a row after the base claim. B's transaction
    holds that row's lock after B's first run_once(); A's run_once() claims
    and then waits for it; B's second run_once() must not wait for A, or
    neither ever finishes: the database cannot see a lock held in Python.
    """
    _budget(settings, 3)
    counter = tasks.record.enqueue("counter")
    OxTask.objects.filter(id=counter.id).update(status=OxTask.Status.SUCCESSFUL)
    worker = CountsAfterClaiming(lock_timeout=LOCK_TIMEOUT, backoff_initial=0)
    worker.counter = _row(counter).pk
    results = [tasks.record.enqueue(f"t{i}") for i in range(4)]

    def a_is_waiting():
        seen = []
        from_another_connection(
            lambda other: seen.append(_waiting_on_a_row_lock(other))
        )
        return seen[0]

    out, blocked = _two_callers(worker, wait_for_a=a_is_waiting)

    # The instrument: A was waiting on B's row lock when B claimed again.
    assert out.get("a_waited") is True, out
    assert blocked == {"a": False, "b": False}, f"deadlock: {out}"
    assert out["b"] == "committed", out
    assert (out["b1"], out["b2"], out["a"]) == (True, True, True), out
    assert out["b2_took"] < PROMPT, out
    assert _row(counter).priority == 3
    ran = [r for r in results if _row(r).status == OxTask.Status.SUCCESSFUL]
    assert len(ran) == 3
    assert sorted(_runs(f"t{i}") for i in range(4)) == [0, 1, 1, 1]


def test_a_claim_waiting_on_sqlites_write_lock_holds_up_no_claim(settings, monkeypatch):
    """
    X3-2. On SQLite, B's transaction holds the write lock after its first
    run_once(); A's claim waits for it in the busy handler; B's second
    run_once() must go ahead, so that B can commit and A's claim can land,
    rather than wait for A until A's busy timeout gives up. PostgreSQL and
    MySQL claims skip locked rows, so A never waits there, and the same
    calls must finish there too.
    """
    _budget(settings, 3)
    if connection.vendor == "sqlite":
        # A's claim gives up after this, where the suite's twenty seconds
        # would only make the failure slower.
        monkeypatch.setitem(connection.settings_dict["OPTIONS"], "timeout", 5)
        connections.close_all()
    worker = Worker(lock_timeout=LOCK_TIMEOUT, backoff_initial=0)
    for i in range(4):
        tasks.record.enqueue(f"t{i}")

    out, blocked = _two_callers(worker, wait_for_a=None)
    connections.close_all()

    assert blocked == {"a": False, "b": False}, out
    assert out["b"] == "committed", out
    assert (out["b1"], out["b2"], out["a"]) == (True, True, True), out
    assert out["b2_took"] < PROMPT, out
    if connection.vendor == "sqlite":
        # The instrument: A's claim did wait, for B's commit.
        assert out["a_took"] >= 0.8, out
    assert sorted(_runs(f"t{i}") for i in range(4)) == [0, 1, 1, 1]


# -- a claim and a look on the same Worker ---------------------------------------


class ClaimsWithoutTheBase(Worker):
    """An override that claims a row itself and never calls the base."""

    def claim_one(self):
        return self._claim_one()


#: How the claim is made: the base claim_one() called directly, as
#: run_tasks() and a launcher do; run()'s and run_once()'s _claim() over the
#: base; and _claim() over an override that never calls the base.
CLAIMS = {
    "claim_one": (Worker, lambda worker: worker.claim_one()),
    "_claim": (Worker, lambda worker: worker._claim()),
    "no_super": (ClaimsWithoutTheBase, lambda worker: worker._claim()),
}


def _paused_claims(monkeypatch, worker, *, committed, go_on):
    """The worker's next claim commits, says so, and waits for go_on."""
    real = worker._claim_one

    def claim_then_pause():
        db_task = real()
        committed.set()
        go_on.wait(LIMIT)
        return db_task

    monkeypatch.setattr(worker, "_claim_one", claim_then_pause)


class LookReadGate:
    """
    On every connection opened while installed, the test thread's included:
    counts the look's read, and before the first one runs calls before().
    """

    def __init__(self, before=None):
        self.reads = 0
        self.before = before
        self._lock = threading.Lock()
        self._installed = []
        connection_created.connect(self._install, weak=False)
        self._install(sender=None, connection=connections["default"])

    def _wrap(self, execute, sql, params, many, context):
        if "ox_lease_abandoned" in sql:
            with self._lock:
                self.reads += 1
                first = self.reads == 1
            if first and self.before is not None:
                self.before()
        return execute(sql, params, many, context)

    def _install(self, sender, connection, **kwargs):
        if self._wrap not in connection.execute_wrappers:
            connection.execute_wrappers.append(self._wrap)
            self._installed.append(connection)

    def remove(self):
        connection_created.disconnect(self._install)
        for conn in self._installed:
            if self._wrap in conn.execute_wrappers:
                conn.execute_wrappers.remove(self._wrap)


@pytest.fixture
def gates():
    installed = []

    def install(before=None):
        gate = LookReadGate(before)
        installed.append(gate)
        return gate

    yield install
    for gate in installed:
        gate.remove()


@pytest.mark.parametrize("how", list(CLAIMS))
def test_a_look_that_starts_during_a_claim_makes_no_read(
    how, monkeypatch, caplog, gates
):
    """
    A claim has committed on one thread and is on its way back, not yet
    registered; a look starts on another. It must neither read nor wait:
    it stays pending and returns. Once the claim has returned, the next
    look releases the row whose claim raised and leaves the other alone.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    cls, claim = CLAIMS[how]
    worker = cls(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    in_transit = tasks.record.enqueue("in-transit")
    committed, go_on = threading.Event(), threading.Event()
    _paused_claims(monkeypatch, worker, committed=committed, go_on=go_on)
    gate = gates()
    claimer, claimed = _on_a_thread(lambda: claim(worker), "claimer")
    try:
        assert committed.wait(LIMIT)
        looker, looked = _on_a_thread(worker._recover_claims, "looker")
        looker.join(PROMPT)
        assert not looker.is_alive(), "the look waited for the claim"
        assert looked == {"returned": False}, looked
        assert gate.reads == 0, "the look read while a claim was on its way back"
        assert worker._claim_unconfirmed_at is not None
    finally:
        go_on.set()
        claimer.join(LIMIT)
    assert str(claimed["returned"].id) == str(in_transit.id)
    assert worker._claims_in_flight == 0

    assert worker._recover_claims() is True

    assert gate.reads == 1
    assert _released_ids(caplog, worker) == [str(orphan.id)]
    assert _state(orphan) == (OxTask.Status.READY, 0, 2)
    assert _state(in_transit) == (OxTask.Status.RUNNING, 1, 1)
    assert worker._claim_unconfirmed_at is None
    worker.execute(claimed["returned"])
    assert _state(in_transit) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert _runs("in-transit") == 1


@pytest.mark.parametrize("look", ["pass", "stop"])
@pytest.mark.parametrize("how", list(CLAIMS))
def test_a_claim_that_starts_during_a_look_makes_it_release_nothing(
    how, look, monkeypatch, caplog, gates
):
    """
    A look has checked that no claim is in flight and taken its snapshot;
    before its read, a claim on another thread commits a row that it has
    not yet registered. The read sees that row RUNNING under this worker's
    id and in none of the sets, exactly like the orphan. The look must
    release neither and stay pending: a look at the head of a pass returns,
    and a stopping look gives up and says so. The claim's row is never
    released, and runs once.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    cls, claim = CLAIMS[how]
    worker = cls(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    in_transit = tasks.record.enqueue("in-transit")
    committed, go_on = threading.Event(), threading.Event()
    _paused_claims(monkeypatch, worker, committed=committed, go_on=go_on)
    started = []
    in_time = []

    def claim_before_the_read():
        # Not raised here: a look that fails is what a stopping look reports.
        claimer, claimed = _on_a_thread(lambda: claim(worker), "claimer")
        started.append((claimer, claimed))
        in_time.append(committed.wait(10.0))

    gate = gates(before=claim_before_the_read)
    if look == "pass":
        looker, looked = _on_a_thread(worker._recover_claims, "looker")
    else:
        looker, looked = _on_a_thread(worker._recover_claims_once, "looker")
    try:
        looker.join(LIMIT)
        assert not looker.is_alive()
    finally:
        go_on.set()
        for claimer, _ in started:
            claimer.join(LIMIT)

    assert gate.reads == 1
    assert in_time == [True], "the claim did not commit while the look read"
    (claimer, claimed) = started[0]
    assert "raised" not in looked, looked["raised"]
    assert str(claimed["returned"].id) == str(in_transit.id)
    assert _released_ids(caplog, worker) == []
    assert _state(orphan) == (OxTask.Status.RUNNING, 1, 1)
    assert _state(in_transit) == (OxTask.Status.RUNNING, 1, 1)
    if look == "pass":
        assert looked == {"returned": False}
        assert worker._claim_unconfirmed_at is not None
        # The next look has the claim's row registered, and releases only
        # the orphan.
        assert worker._recover_claims() is True
        assert _released_ids(caplog, worker) == [str(orphan.id)]
        assert _state(orphan) == (OxTask.Status.READY, 0, 2)
        assert worker._claim_unconfirmed_at is None
    else:
        (failed,) = _events(caplog, "worker_claim_recovery_failed", worker)
        assert failed.claim_recovery == "expired"
    assert _state(in_transit) == (OxTask.Status.RUNNING, 1, 1)
    worker.execute(claimed["returned"])
    assert _state(in_transit) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert _runs("in-transit") == 1


def test_a_claim_that_raises_while_a_look_releases_keeps_it_pending(
    monkeypatch, caplog
):
    """
    A claim that begins while a look is making its releases committed after
    the look's read, so its row is not among them; but it may raise, and
    then it notes a look of its own. The look must not clear that note.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    worker = Worker(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    raised_at = []

    def claim_raises():
        raise OperationalError("the reply to this claim was lost")

    def raising_claim():
        monkeypatch.setattr(worker, "_claim_one", claim_raises)
        try:
            worker._claim()
        except OperationalError:
            raised_at.append(worker._claim_unconfirmed_at)

    fired = []

    def before_the_release(execute, sql, params, many, context):
        if is_the_release(sql, params) and not fired:
            fired.append(True)
            thread, _ = _on_a_thread(raising_claim, "claimer")
            thread.join(10.0)
        return execute(sql, params, many, context)

    connection.ensure_connection()
    connection.execute_wrappers.append(before_the_release)
    try:
        returned = worker._recover_claims()
    finally:
        connection.execute_wrappers.remove(before_the_release)

    assert fired and raised_at, "the claim did not raise during the release"
    assert _released_ids(caplog, worker) == [str(orphan.id)]
    assert returned is False
    assert worker._claim_unconfirmed_at == raised_at[0] is not None


class RaisesAfterTheBase(Worker):
    """An override whose own work after the base claim raises."""

    def claim_one(self):
        db_task = super().claim_one()
        raise RuntimeError(f"the override failed after claiming {db_task}")


#: A claim that raises: the base claim's database error, through claim_one()
#: and through _claim(); and an override's own error after the base claim.
RAISING = {
    "claim_one": (Worker, lambda worker: worker.claim_one(), DatabaseError),
    "_claim": (Worker, lambda worker: worker._claim(), DatabaseError),
    "override": (RaisesAfterTheBase, lambda worker: worker._claim(), RuntimeError),
}


@pytest.mark.parametrize("how", list(RAISING))
def test_a_claim_that_raised_counts_itself_out(how, monkeypatch, caplog):
    """
    However a claim ends, it stops counting as in flight: after a claim that
    raised, a look is made, and releases the orphan at once, and a stopping
    look does not wait out its deadline.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    cls, claim, error = RAISING[how]
    worker = cls(lock_timeout=LOCK_TIMEOUT)
    orphan = _orphan(worker)
    tasks.record.enqueue("next")
    if error is DatabaseError:

        def claim_raises():
            raise DatabaseError("the claim failed")

        monkeypatch.setattr(worker, "_claim_one", claim_raises)
    for _ in range(3):
        with pytest.raises(error):
            claim(worker)

    began = time.monotonic()
    worker._recover_claims_once()
    took = time.monotonic() - began

    assert took < PROMPT
    assert _events(caplog, "worker_claim_recovery_failed", worker) == []
    assert _released_ids(caplog, worker) == [str(orphan.id)]
    assert _state(orphan) == (OxTask.Status.READY, 0, 2)
    assert worker._claim_unconfirmed_at is None


# -- the loop and run_once() threads on one Worker, replies lost -----------------


class LosesSomeReplies(Worker):
    """
    Every `every`-th claim commits and then raises, as a claim whose reply
    was lost does: RUNNING under this worker's id, and never returned. A
    pause after the others widens the moment a claim is on its way back.
    tests/test_lost_commit_reply.py loses real replies, one at a time.
    """

    def __init__(self, *args, every, **kwargs):
        super().__init__(*args, **kwargs)
        self.every = every
        self._count_lock = threading.Lock()
        self.claims = 0
        self.lost = collections.Counter()
        self.looks = collections.Counter()

    def _claim_one(self):
        db_task = super()._claim_one()
        if db_task is None:
            return None
        with self._count_lock:
            self.claims += 1
            lose = self.claims % self.every == 0
        caller = (
            "run_once" if threading.current_thread().name.startswith("once") else "run"
        )
        if lose:
            self.lost[caller] += 1
            raise OperationalError("the reply to a claim that committed was lost")
        time.sleep(random.uniform(0, 0.01))
        return db_task

    def _recover_claims(self):
        made = super()._recover_claims()
        self.looks[made] += 1
        return made


def test_the_loop_and_run_once_threads_on_one_worker_run_every_body_once(
    settings, caplog
):
    """
    run() and two threads calling run_once() share one Worker while one claim
    in five loses its reply. The loop looks for its own; its look meets the
    other threads' claims on their way back. A row whose claim raised under
    run_once() is the reaper's, or a later look's. Every body runs, and none
    twice.
    """
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 10)
    n = 60
    labels = [f"s{i}" for i in range(n)]
    results = [tasks.record.enqueue(label) for label in labels]
    worker = LosesSomeReplies(
        every=5,
        concurrency=2,
        lock_timeout=2.0,
        poll_interval=0.02,
        reap_interval=0.25,
        backoff_initial=0,
    )
    done = threading.Event()

    def call_run_once():
        while not done.is_set():
            try:
                ran = worker.run_once()
            except DatabaseError:
                ran = True
            finally:
                close_old_connections()
                connections.close_all()
            if not ran:
                time.sleep(0.02)

    threads = [
        threading.Thread(target=call_run_once, name=f"once-{i}", daemon=True)
        for i in range(2)
    ]
    loop = start_worker_thread(worker)
    for thread in threads:
        thread.start()

    def all_ran():
        return OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == n

    try:
        finished = wait_for(all_ran, timeout=90.0, interval=0.1)
    finally:
        done.set()
        for thread in threads:
            thread.join(LIMIT)
        worker.request_stop()
        loop.join(LIMIT)

    statuses = collections.Counter(OxTask.objects.values_list("status", flat=True))
    twice = {label: _runs(label) for label in labels if _runs(label) > 1}
    never = [label for label in labels if _runs(label) == 0]
    summary = (
        f"claims={worker.claims} lost={dict(worker.lost)} looks={dict(worker.looks)} "
        f"released={len(_released_ids(caplog, worker))} statuses={dict(statuses)}"
    )
    logging.getLogger(__name__).warning("shared worker: %s", summary)
    assert finished, f"not every task ran: {summary}"
    assert twice == {}, f"{twice} {summary}"
    assert never == [], f"{never} {summary}"
    # The instrument: replies were lost under both callers.
    assert worker.lost["run"] >= 1 and worker.lost["run_once"] >= 1, summary
    for result in results:
        assert _row(result).status == OxTask.Status.SUCCESSFUL


# -- an exception delivered inside a claim leaves the fence free -----------------

needs_delivery = pytest.mark.skipif(
    worker_module._inject_async_exc is None,
    reason="delivering an exception to another thread needs PyThreadState_SetAsyncExc",
)


def _deliver(thread, exc):
    """
    Raise `exc` in `thread` at its next instruction that checks for one, as
    the watchdog raises TaskTimeout and a signal handler KeyboardInterrupt.
    """
    set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc
    assert set_async_exc(ctypes.c_ulong(thread.ident), ctypes.py_object(exc)) == 1


def _free(lock, wait=0.0):
    """
    Whether `lock` is free, or comes free within `wait` seconds. A
    Condition's acquire and release are its lock's.
    """
    if lock.acquire(timeout=wait) if wait else lock.acquire(blocking=False):
        lock.release()
        return True
    return False


def _stack(thread):
    frame = sys._current_frames().get(thread.ident)
    return [] if frame is None else [f.name for f in traceback.extract_stack(frame)]


#: Where a thread waits for one of the fence's locks, whichever tree.
WAITS = {"_claiming", "_hand_off", "_recover_claims", "_recover_claims_by", "__enter__"}


def _waiting_on_a_lock(thread):
    stack = _stack(thread)
    return bool(stack) and stack[-1] in WAITS


def _let_go(worker, thread):
    """
    Release a fence lock nothing holds any more, which is what a delivery
    that leaked it leaves, until `thread` is done with the Worker.
    """
    for _ in range(50):
        thread.join(0.2)
        if not thread.is_alive():
            return
        # Held across the join, which no claim's own hold ever is.
        if not _free(worker._fence) and not _free(worker._fence, wait=0.2):
            worker._fence.release()


@needs_delivery
@pytest.mark.usefixtures("interruptible_attempts")
def test_a_timeout_delivered_inside_a_claim_leaves_a_shared_workers_fence_free(
    settings, caplog
):
    """
    A task under a timeout calls run_once() on a Worker shared between tasks,
    as a module-level Worker is, and its claim waits for the fence's lock
    past the task's deadline. The watchdog's TaskTimeout is pending when the
    wait ends, and lands as the acquire returns. That attempt fails; the
    next task's same call returns, and the shared Worker's fence is free and
    counts no claim.
    """
    caplog.set_level(logging.INFO, logger="django_ox")
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "other"],
            "OPTIONS": {"MAX_ATTEMPTS": 1},
        }
    }
    shared = Worker(queues=["other"], lock_timeout=LOCK_TIMEOUT)
    tasks.STATE["shared_worker"] = shared
    worker = Worker(
        queues=["default"],
        concurrency=1,
        task_timeout=2,
        # Past every wait here, so no attempt is given up as stuck.
        task_timeout_grace=LIMIT,
        poll_interval=0.05,
        lock_timeout=LOCK_TIMEOUT,
    )
    first = tasks.claim_on_a_shared_worker.enqueue("first")
    shared._fence.acquire()
    holding = True
    loop = start_worker_thread(worker)
    try:
        assert wait_for(lambda: tasks.STATE.get("shared_began"), timeout=LIMIT)
        # The deadline has passed and the watchdog has fired: its TaskTimeout
        # waits in the claim's acquire until the lock is free.
        assert wait_for(
            lambda: any(watch.fired for watch in list(worker._watches.values())),
            timeout=LIMIT,
        )
        shared._fence.release()
        holding = False
        assert wait_for(lambda: _row(first).status == OxTask.Status.FAILED, PROMPT)
        assert [r.task_id for r in _events(caplog, "task_timed_out")] == [str(first.id)]

        second = tasks.claim_on_a_shared_worker.enqueue("second")
        done = wait_for(lambda: _row(second).status == OxTask.Status.SUCCESSFUL, LIMIT)
        assert done, f"the next call waited on the fence: {_row(second).status}"
        assert tasks.STATE.get("shared_done") == ["second"]
        assert _free(shared._fence)
        assert shared._claims_in_flight == 0
    finally:
        if holding:
            shared._fence.release()
        worker.request_stop()
        _let_go(shared, loop)


class PausesInsideTheFence(Worker):
    """
    Pauses the claim of the thread named "victim" at `pause_at`: in its
    claim_one() override before the base claim ("before_base") or after it
    ("after_base"), or inside the base's fence before the claim itself
    ("inside_base"), so a test can take the lock the claim needs next.
    """

    pause_at = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arrived = threading.Event()
        self.go_on = threading.Event()

    def _pause(self, where):
        if where == self.pause_at and threading.current_thread().name == "victim":
            self.arrived.set()
            self.go_on.wait(LIMIT)

    def claim_one(self):
        self._pause("before_base")
        db_task = super().claim_one()
        self._pause("after_base")
        return db_task

    def _claim_one(self):
        self._pause("inside_base")
        return super()._claim_one()


def _base_claim(worker):
    return worker.claim_one()


def _run_once_call(worker):
    return worker.run_once()


def _stopping_look(worker):
    worker._claim_unconfirmed_at = time.monotonic()
    return worker._recover_claims_by(time.monotonic() + LIMIT)


def _look(worker):
    worker._claim_unconfirmed_at = time.monotonic()
    return worker._recover_claims()


#: Each acquire of the fence's lock, or of the lock a claim registers under,
#: on the way through a claim or a look: the call, the lock, where the call
#: pauses first so the test can take that lock (None: the test takes it
#: before the call), and whether a row is due so the claim returns one.
BOUNDARIES = {
    "claim_one count in": (_base_claim, "fence", None, False),
    "claim_one register": (_base_claim, "in_flight", "inside_base", True),
    "claim_one count out": (_base_claim, "fence", "inside_base", False),
    "run_once count in": (_run_once_call, "fence", None, False),
    "run_once base count in": (_run_once_call, "fence", "before_base", False),
    "run_once base register": (_run_once_call, "in_flight", "inside_base", True),
    "run_once base count out": (_run_once_call, "fence", "inside_base", False),
    "run_once register": (_run_once_call, "in_flight", "after_base", True),
    "run_once count out": (_run_once_call, "fence", "after_base", False),
    "look": (_look, "fence", None, False),
    "stopping look": (_stopping_look, "fence", None, False),
}


@needs_delivery
@pytest.mark.parametrize("exc", [TaskTimeout, KeyboardInterrupt])
@pytest.mark.parametrize("boundary", list(BOUNDARIES))
def test_an_exception_delivered_as_a_fence_lock_is_taken_leaves_it_free(boundary, exc):
    """
    The call waits for a lock the test holds; the test delivers an exception
    to it and lets the lock go, so the exception lands as the acquire
    returns, at the first instruction there that checks for one. The call
    raises it. Both locks are free, no claim counts as in flight, and the
    Worker claims again at once.
    """
    call, which, pause_at, row_due = BOUNDARIES[boundary]
    if row_due:
        tasks.record.enqueue("due")
    worker = PausesInsideTheFence(lock_timeout=LOCK_TIMEOUT)
    worker.pause_at = pause_at
    lock = worker._fence if which == "fence" else worker._in_flight_lock
    holding = False
    if pause_at is None:
        lock.acquire()
        holding = True
    thread, out = _on_a_thread(lambda: call(worker), "victim")
    try:
        if pause_at is not None:
            assert worker.arrived.wait(LIMIT)
            lock.acquire()
            holding = True
            worker.go_on.set()
        assert wait_for(lambda: _waiting_on_a_lock(thread), PROMPT), _stack(thread)
        # Into the acquire itself, a C call that checks for nothing.
        time.sleep(0.2)
        _deliver(thread, exc)
        lock.release()
        holding = False
        thread.join(PROMPT)

        assert not thread.is_alive(), f"it did not return: {_stack(thread)}"
        assert isinstance(out.get("raised"), exc), out
        assert _free(worker._fence)
        assert _free(worker._in_flight_lock)
        assert worker._claims_in_flight == 0
        worker.pause_at = None
        again, again_out = _on_a_thread(worker.claim_one, "again")
        again.join(PROMPT)
        assert not again.is_alive() and "raised" not in again_out, again_out
    finally:
        worker.go_on.set()
        if holding:
            lock.release()
        _let_go(worker, thread)
