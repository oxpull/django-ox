"""
What the dispatch pass does with an exception depends on what it left
behind, not on its class.

One schedule's own failure, database or not, is logged against that
schedule and the rest of the pass continues: a task that will not enqueue,
a row that raises, arguments the database refuses, a statement that fails
while the connection stays usable. A failure that leaves the connection
unusable is not one schedule's: it ends the pass and reaches run(), which
logs `schedule_dispatch_failed`, drops a connection that is no longer
usable, and goes on to the claim.
"""

import logging
import threading
import time
from datetime import timedelta

import pytest
from django.db import (
    DatabaseError,
    Error,
    IntegrityError,
    OperationalError,
    connection,
    transaction,
)
from django.utils import timezone

from django_ox.compat import task_enqueued
from django_ox.models import OxScheduleTick, OxTask
from django_ox.schedules import lock_contention
from django_ox.worker import Worker

from .isolation import kill

pytestmark = pytest.mark.django_db

TWO = {
    name: {"task": "tests.tasks.add", "cron": "* * * * *", "args": [1, 2]}
    for name in ("one", "two")
}


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULES": TWO},
        }
    }
    return Worker(
        backoff_initial=0, poll_interval=0.02, reap_interval=0.0, schedule_interval=0.0
    )


@pytest.fixture
def frozen_now(monkeypatch):
    """Pin the clock mid-minute, so every pass in a test sees one due tick."""
    fixed = timezone.now().replace(second=30, microsecond=0)
    monkeypatch.setattr(timezone, "now", lambda: fixed)
    return fixed


def with_history():
    # A tick of history each, so the pass fires rather than anchors.
    now = timezone.now()
    for name in TWO:
        OxScheduleTick.objects.create(
            schedule_name=name,
            scheduled_for=now.replace(second=0, microsecond=0) - timedelta(minutes=1),
            task_id=None,
            created_at=now,
        )


def raising_on_tick_insert(exc, times=None):
    """An execute wrapper that raises `exc` from the tick INSERT, `times` times."""
    raised = []

    def wrapper(execute, sql, params, many, context):
        is_tick_insert = sql.lstrip().upper().startswith("INSERT") and (
            "oxscheduletick" in sql
        )
        if is_tick_insert and (times is None or len(raised) < times):
            raised.append(exc)
            raise exc
        return execute(sql, params, many, context)

    return wrapper


def losing_the_connection_at_tick_insert(times=1):
    """
    An execute wrapper that loses the connection for real just before the
    tick INSERT goes out, `times` times: the INSERT, the rollback after it
    and the check after that all meet a session that is gone.
    """
    lost = []

    def wrapper(execute, sql, params, many, context):
        is_tick_insert = sql.lstrip().upper().startswith("INSERT") and (
            "oxscheduletick" in sql
        )
        if is_tick_insert and len(lost) < times:
            lost.append(1)
            kill(context["connection"])
        return execute(sql, params, many, context)

    return wrapper


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


class TestTheConnectionDecidesNotTheClass:
    @pytest.mark.django_db(transaction=True)
    def test_a_connection_lost_at_the_tick_insert_ends_the_pass(self, worker, caplog):
        # Transactional, so the block is the outermost transaction and its
        # rollback meets the dead session the way it does in a worker.
        with_history()
        with (
            caplog.at_level(logging.ERROR, logger="django_ox"),
            connection.execute_wrapper(losing_the_connection_at_tick_insert()),
            pytest.raises(Error),
        ):
            worker.dispatch_schedules()
        assert not events(caplog, "schedule_dispatch_error"), (
            "a lost connection was reported as one schedule's failure"
        )
        assert not OxTask.objects.exists()
        assert not OxScheduleTick.objects.exclude(task_id=None).exists()

    def test_the_same_class_on_a_usable_connection_is_one_schedules(
        self, worker, caplog
    ):
        # The error an outage raises, from a statement that failed on a
        # connection that is still there: what MySQL does with arguments
        # it refuses (3140) is an OperationalError too. The class cannot
        # tell those apart, and the connection can.
        with_history()
        failed = OperationalError("server closed the connection unexpectedly")
        with (
            caplog.at_level(logging.ERROR, logger="django_ox"),
            connection.execute_wrapper(raising_on_tick_insert(failed, times=1)),
        ):
            assert worker.dispatch_schedules() == 1, "the other schedule must fire"
        (reported,) = events(caplog, "schedule_dispatch_error")
        assert reported.schedule == "one"
        assert reported.error == "OperationalError"
        assert reported.exc_info is not None
        assert OxTask.objects.count() == 1

    def test_an_integrity_error_before_the_claim_is_not_always_a_lost_race(
        self, worker, caplog
    ):
        # Only a duplicate key on the tick INSERT is another worker winning.
        # Any other integrity failure there is a fault, and silence would
        # retry it forever with nothing in the log.
        with_history()
        not_a_race = IntegrityError(
            "NOT NULL constraint failed: django_ox_oxscheduletick.created_at"
        )
        with (
            caplog.at_level(logging.ERROR, logger="django_ox"),
            connection.execute_wrapper(raising_on_tick_insert(not_a_race, times=1)),
        ):
            assert worker.dispatch_schedules() == 1, "the other schedule must fire"
        (reported,) = events(caplog, "schedule_dispatch_error")
        assert reported.schedule == "one"
        assert reported.error == "IntegrityError"

    def test_one_schedules_own_failure_is_still_isolated(self, worker, caplog):
        with_history()
        own = RuntimeError("this schedule's task will not enqueue")
        with (
            caplog.at_level(logging.ERROR, logger="django_ox"),
            connection.execute_wrapper(raising_on_tick_insert(own, times=1)),
        ):
            assert worker.dispatch_schedules() == 1, "the other schedule must fire"
        assert len(events(caplog, "schedule_dispatch_error")) == 1
        assert OxTask.objects.count() == 1


@pytest.mark.django_db(transaction=True)
class TestRunReportsAFailedPassAndCarriesOn:
    def test_the_next_pass_dispatches(self, worker, caplog, frozen_now):
        # Transactional: run() polls on its own thread with its own
        # connection, and the wrapper has to be installed on that one.
        with_history()

        def run_losing_the_connection_once():
            with connection.execute_wrapper(losing_the_connection_at_tick_insert()):
                worker.run()

        thread = threading.Thread(target=run_losing_the_connection_once, daemon=True)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if events(caplog, "schedule_dispatch_failed") and (
                    len(events(caplog, "schedule_dispatched")) == len(TWO)
                ):
                    break
                time.sleep(0.02)
            worker.request_stop()
            thread.join(timeout=5)

        assert not thread.is_alive(), "run() did not return after the stop"
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.exc_info is not None, "the first report of an outage is in full"
        assert len(events(caplog, "schedule_dispatched")) == len(TWO), (
            "the pass after the failure did not dispatch"
        )
        assert not events(caplog, "schedule_dispatch_error")


class _PgLockNotAvailable(Exception):
    """The shape psycopg gives a lock_timeout: an SQLSTATE on the cause."""

    sqlstate = "55P03"


def _with_cause(exc, cause):
    exc.__cause__ = cause
    return exc


LOCK_CONTENTION = {
    "mysql-lock-wait": OperationalError(
        1205, "Lock wait timeout exceeded; try restarting transaction"
    ),
    "mysql-deadlock": OperationalError(
        1213, "Deadlock found when trying to get lock; try restarting transaction"
    ),
    "sqlite-busy": OperationalError("database is locked"),
    "postgres-lock-timeout": _with_cause(
        OperationalError("canceling statement due to lock timeout"),
        _PgLockNotAvailable(),
    ),
}


class TestALockTheDatabaseGaveUpOnIsContentionNotAFault:
    @pytest.mark.parametrize("shape", list(LOCK_CONTENTION), ids=list(LOCK_CONTENTION))
    def test_the_classifier_reads_each_databases_shape(self, shape):
        assert lock_contention(LOCK_CONTENTION[shape])

    @pytest.mark.parametrize(
        "other",
        [
            OperationalError("server closed the connection unexpectedly"),
            OperationalError(2006, "MySQL server has gone away"),
            OperationalError("no such table: django_ox_oxscheduletick"),
        ],
        ids=["postgres-gone", "mysql-gone", "sqlite-missing-table"],
    )
    def test_anything_else_is_not(self, other):
        assert not lock_contention(other)

    @pytest.mark.parametrize("shape", list(LOCK_CONTENTION), ids=list(LOCK_CONTENTION))
    def test_it_is_one_warning_without_a_traceback_and_the_pass_goes_on(
        self, worker, caplog, shape
    ):
        with_history()
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            connection.execute_wrapper(
                raising_on_tick_insert(LOCK_CONTENTION[shape], times=1)
            ),
        ):
            assert worker.dispatch_schedules() == 1, "the other schedule must fire"
        warned = events(caplog, "schedule_lock_unavailable")
        assert len(warned) == 1
        assert warned[0].exc_info is None, "contention is not a traceback"
        assert not events(caplog, "schedule_dispatch_error")
        assert not events(caplog, "schedule_dispatch_failed")


@pytest.mark.django_db(transaction=True)
class TestAMySQLLockWaitTimeoutOnTheTickRow:
    """
    The real thing on MySQL: the winner holds its transaction past the
    loser's lock-wait timeout, so the loser's unique INSERT ends in 1205
    rather than in the IntegrityError the loop reads as a lost race.
    """

    def test_the_loser_warns_once_and_the_winners_task_stands(self, worker, caplog):
        if connection.vendor != "mysql":
            pytest.skip("innodb_lock_wait_timeout is MySQL's")
        with_history()
        winner_inserted, release = threading.Event(), threading.Event()
        out = {}

        def hold_after_the_tick_insert(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            if sql.lstrip().upper().startswith("INSERT") and "oxscheduletick" in sql:
                winner_inserted.set()
                release.wait(timeout=10)
            return result

        def winner():
            try:
                with connection.execute_wrapper(hold_after_the_tick_insert):
                    out["winner"] = Worker(backoff_initial=0).dispatch_schedules()
            except BaseException as exc:
                out["winner"] = exc
            finally:
                connection.close()

        def loser():
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                out["loser"] = Worker(backoff_initial=0).dispatch_schedules()
            except BaseException as exc:
                out["loser"] = exc
            finally:
                connection.close()

        first = threading.Thread(target=winner)
        second = threading.Thread(target=loser)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            first.start()
            assert winner_inserted.wait(timeout=10)
            second.start()
            second.join(timeout=30)
            release.set()
            first.join(timeout=30)
        assert not first.is_alive() and not second.is_alive()
        raised = [v for v in out.values() if isinstance(v, BaseException)]
        assert not raised, raised
        # The loser timed out on the first schedule's tick and went on to
        # the second, which it won while the winner was still held; each
        # tick fired exactly once between them.
        assert out["winner"] + out["loser"] == len(TWO), out
        assert OxTask.objects.count() == len(TWO)
        warned = events(caplog, "schedule_lock_unavailable")
        assert [r.schedule for r in warned] == ["one"], [r.getMessage() for r in warned]
        assert warned[0].exc_info is None, "contention is not a traceback"
        assert not events(caplog, "schedule_dispatch_error")


@pytest.mark.django_db(transaction=True)
class TestAPostCommitCallbackThatRaises:
    """
    Django runs transaction.on_commit callbacks at the outermost exit, after
    the commit, and does not guard them by default. One registered by a
    task_enqueued receiver that raises therefore leaves the dispatch block
    after the task is committed. That is the callback's failure, not the
    dispatch's: the task exists, the tick is recorded, and both are counted.

    Transactional so the block is the outermost transaction and the callback
    actually runs; inside a test transaction it would be deferred.
    """

    @pytest.mark.parametrize(
        "raised",
        [
            RuntimeError("the callback failed after the commit"),
            IntegrityError("duplicate key in the receiver's own table"),
            OperationalError("server closed the connection unexpectedly"),
            OperationalError(
                1205, "Lock wait timeout exceeded; try restarting transaction"
            ),
            DatabaseError("the receiver's own statement failed"),
        ],
        ids=["plain", "integrity", "operational", "contention-shaped", "database"],
    )
    def test_the_dispatch_is_counted_and_the_callback_reported(
        self, worker, caplog, raised
    ):
        # Every class, not only a plain one. A callback that does database
        # work of its own raises database errors, and read by class those
        # are a lost race, contention, or a fault that ends the pass: the
        # committed task went uncounted, or the rest of the pass was lost.
        with_history()

        def failing_after_commit():
            raise raised

        def receiver(sender, task_result, **kwargs):
            transaction.on_commit(failing_after_commit)

        task_enqueued.connect(receiver)
        try:
            with caplog.at_level(logging.INFO, logger="django_ox"):
                dispatched = worker.dispatch_schedules()
        finally:
            task_enqueued.disconnect(receiver)

        assert dispatched == len(TWO), "a task that exists went uncounted"
        assert OxTask.objects.count() == len(TWO)
        assert OxScheduleTick.objects.exclude(task_id=None).count() == len(TWO)
        failed = events(caplog, "schedule_dispatch_callback_failed")
        assert len(failed) == len(TWO)
        assert all(r.exc_info is not None for r in failed), "the cause is wanted"
        assert {str(t) for t in OxTask.objects.values_list("id", flat=True)} == {
            r.task_id for r in failed
        }
        assert len(events(caplog, "schedule_dispatched")) == len(TWO)
        assert not events(caplog, "schedule_dispatch_error")

    def test_a_commit_that_fails_is_not_a_callback_failure(
        self, worker, caplog, monkeypatch
    ):
        # The body ran to its end and then the COMMIT itself failed, on a
        # connection that is still usable once it has rolled back. Nothing
        # is committed, so nothing is counted and nothing is a callback's:
        # the failure is that schedule's, and the pass goes on to the next.
        with_history()
        commit = connection._commit

        def failing_commit():
            monkeypatch.setattr(connection, "_commit", commit)
            raise OperationalError("server closed the connection unexpectedly")

        monkeypatch.setattr(connection, "_commit", failing_commit)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        (dispatched,) = events(caplog, "schedule_dispatched")
        assert dispatched.schedule == "two"
        (reported,) = events(caplog, "schedule_dispatch_error")
        assert reported.schedule == "one"
        assert not events(caplog, "schedule_dispatch_callback_failed")
        assert OxTask.objects.count() == 1
        assert OxScheduleTick.objects.exclude(task_id=None).count() == 1

    def test_a_commit_that_loses_the_connection_ends_the_pass(
        self, worker, caplog, monkeypatch
    ):
        # The session goes away under the COMMIT. The rollback after it
        # fails too, Django drops the connection, and the pass is over.
        with_history()
        commit = connection._commit

        def commit_on_a_dead_session():
            monkeypatch.setattr(connection, "_commit", commit)
            kill(connection)
            return commit()

        monkeypatch.setattr(connection, "_commit", commit_on_a_dead_session)
        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            pytest.raises(Error),
        ):
            worker.dispatch_schedules()
        assert not events(caplog, "schedule_dispatched")
        assert not events(caplog, "schedule_dispatch_callback_failed")
        assert not events(caplog, "schedule_dispatch_error")
        assert not OxTask.objects.exists()
