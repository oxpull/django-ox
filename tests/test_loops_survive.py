"""
Every loop in the worker outlives a database that went away.

The published envelope says the database may go away and come back. A loop
that dies instead takes something with it: the poll loop takes the worker,
and five worker deaths in a minute stop the supervisor, so a blip the worker
was built to ride out leaves the whole fleet down. The watchdog thread takes
the timeout backstop for every attempt already armed, silently.
"""

import logging
import threading
import time

import pytest
from django.db import DataError, InterfaceError, OperationalError
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .test_schedules import MINUTELY_ADD, backdate_anchor, tasks_setting

pytestmark = pytest.mark.django_db


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, poll_interval=0.02, reap_interval=0.0)


class TestThePollLoopSurvivesTheDatabase:
    @pytest.mark.parametrize(
        "failure",
        [
            OperationalError("server closed the connection unexpectedly"),
            InterfaceError("connection already closed"),
        ],
        ids=["operational", "interface"],
    )
    def test_run_keeps_polling_after_one_failure(
        self, failure, worker, monkeypatch, caplog
    ):
        calls = []
        real_reap = worker.reap

        def reap_once_broken():
            calls.append(1)
            if len(calls) == 1:
                raise failure
            return real_reap()

        monkeypatch.setattr(worker, "reap", reap_once_broken)

        thread = threading.Thread(target=worker.run, daemon=True)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            thread.start()
            deadline = time.monotonic() + 3
            while len(calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
            worker.request_stop()
            thread.join(timeout=5)

        assert not thread.is_alive(), "run() did not return after the stop"
        assert len(calls) >= 3, (
            f"the poll loop stopped after {len(calls)} pass(es); one database "
            "error ended run() and the supervisor would replace the worker"
        )
        assert events(caplog, "worker_poll_failed"), "the failure was not reported"

    @pytest.mark.django_db(transaction=True)
    def test_a_task_still_runs_after_the_blip(self, worker, monkeypatch):
        # Transactional: run() polls on its own thread with its own connection,
        # so it cannot see a row this test has not committed.
        calls = []
        real_claim = worker.claim_one

        def claim_once_broken():
            calls.append(1)
            if len(calls) == 1:
                raise OperationalError("gone")
            return real_claim()

        monkeypatch.setattr(worker, "claim_one", claim_once_broken)
        result = tasks.add.enqueue(1, 2)

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL:
                break
            time.sleep(0.02)
        worker.request_stop()
        thread.join(timeout=5)

        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL, (
            "the worker never got back to claiming after the database returned"
        )


class TestTheWatchdogThreadSurvivesAFailedRecord:
    def test_the_loop_carries_on_and_says_so(self, worker, monkeypatch, caplog):
        from django_ox.worker import _Watch

        # One watch, already past its grace, whose recording will fail.
        result = tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        now = time.monotonic()
        ident = threading.get_ident()
        watch = _Watch(
            ident=ident,
            db_task=claimed,
            attempt=(claimed.pk, claimed.lease_epoch),
            timeout=5,
            started=now - 6,
            deadline=now - 1,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
            grace_at=now,
        )
        worker._running_on[ident] = watch.attempt

        def broken(*args, **kwargs):
            raise OperationalError("cannot write the outcome")

        monkeypatch.setattr(worker, "_handle_failure", broken)

        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._handle_stuck(watch)

        assert worker.recycling, (
            "the record failed and the worker did not recycle, so it keeps a "
            "dead pool slot and the drain waits for the wedged thread forever"
        )
        assert worker._stuck.get(ident) == watch.attempt
        assert events(caplog, "task_stuck_unrecorded"), "the failure was silent"
        assert result is not None

    @pytest.mark.parametrize(
        "failure",
        [
            # Django raises this from adapt_datetimefield_value on a naive
            # value, and it is not a django.db.Error.
            ValueError("naive datetime while time zone support is active"),
            # A driver hitting a character the column's charset cannot hold.
            UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates not allowed"),
        ],
    )
    def test_a_failure_that_is_not_a_database_error_still_recycles(
        self, worker, monkeypatch, caplog, failure
    ):
        """
        The watchdog above catches `Exception`, so the guard here must be as
        wide: anything that escapes it skips the recycle, and the pool slot is
        then gone for the life of the process.
        """
        from django_ox.worker import _Watch

        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        now = time.monotonic()
        ident = threading.get_ident()
        watch = _Watch(
            ident=ident,
            db_task=claimed,
            attempt=(claimed.pk, claimed.lease_epoch),
            timeout=5,
            started=now - 6,
            deadline=now - 1,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
            grace_at=now,
        )
        worker._running_on[ident] = watch.attempt

        def broken(*args, **kwargs):
            raise failure

        monkeypatch.setattr(worker, "_handle_failure", broken)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._handle_stuck(watch)

        assert worker.recycling, (
            f"{type(failure).__name__} escaped the guard, so the worker kept "
            "a dead pool slot and the drain waits for the wedged thread"
        )
        assert worker._stuck.get(ident) == watch.attempt
        assert events(caplog, "task_stuck_unrecorded"), "the failure was silent"

    def test_one_bad_watch_does_not_kill_the_thread(self, worker, monkeypatch, caplog):
        """
        A raising `_handle_stuck` must not end `_watchdog_loop`. That thread is
        the whole timeout backstop for every attempt already armed, and nothing
        restarts it mid-attempt.
        """
        from django_ox.worker import _Watch

        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        now = time.monotonic()
        ident = threading.get_ident()
        # Past its grace already, so the first pass takes it out and hands it
        # to _handle_stuck.
        worker._watches[ident] = _Watch(
            ident=ident,
            db_task=claimed,
            attempt=(claimed.pk, claimed.lease_epoch),
            timeout=5,
            started=now - 6,
            deadline=now - 2,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
            grace_at=now - 1,
        )

        seen = []

        def broken(watch):
            seen.append(watch)
            raise RuntimeError("something nobody predicted")

        monkeypatch.setattr(worker, "_handle_stuck", broken)

        thread = threading.Thread(target=worker._watchdog_loop, daemon=True)
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            thread.start()
            deadline = time.monotonic() + 3
            while not seen and time.monotonic() < deadline:
                time.sleep(0.01)
            # The table is empty now, so the loop goes idle and returns.
            thread.join(timeout=5)

        assert seen, "_handle_stuck was never reached"
        assert not thread.is_alive(), (
            "a raising _handle_stuck killed the watchdog thread, so every "
            "attempt already armed lost its deadline and its grace"
        )
        assert events(caplog, "watchdog_error"), "the failure was silent"


class TestTheBackoffArithmeticStaysInRange:
    """
    `attempts` is a PositiveSmallIntegerField, so it reaches 32767. The delay
    doubled the initial backoff by `attempts - 1` and only then took the
    minimum, so a large `MAX_ATTEMPTS` raised OverflowError converting a value
    the `min()` was about to discard. It raised inside `_handle_failure`, on
    the failure path, leaving the row RUNNING until the reaper took it.
    """

    @pytest.mark.parametrize("attempts", [1, 2, 10, 64, 1100, 32767])
    def test_a_failure_is_recorded_however_many_attempts_have_gone(
        self, settings, attempts
    ):
        # A real backoff, not the zero the other fixtures use: 0 * (2 ** n) is
        # 0 whatever n is, so a worker with no backoff never reaches the
        # float conversion that overflows and the whole case is invisible.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=5.0, backoff_max=600.0)
        result = tasks.add.enqueue(1, 2)
        OxTask.objects.filter(id=result.id).update(
            max_attempts=32767, attempts=attempts
        )
        claimed = OxTask.objects.get(id=result.id)
        claimed.status = OxTask.Status.RUNNING
        claimed.locked_by = worker.worker_id
        claimed.save()

        worker._handle_failure(claimed, ValueError("nope"), 1)
        row = OxTask.objects.get(id=result.id)
        # READY with a retry, or FAILED once the attempts are spent. What
        # matters is that the arithmetic did not raise and leave it RUNNING,
        # where only the reaper could recover it.
        assert row.status in (OxTask.Status.READY, OxTask.Status.FAILED), (
            f"the failure path left the row {row.status}"
        )

    def test_the_delay_is_still_bounded_by_backoff_max(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=5.0, backoff_max=600.0)
        result = tasks.add.enqueue(1, 2)
        OxTask.objects.filter(id=result.id).update(max_attempts=32767, attempts=40)
        claimed = OxTask.objects.get(id=result.id)
        claimed.status = OxTask.Status.RUNNING
        claimed.locked_by = worker.worker_id
        claimed.save()
        before = timezone.now()
        worker._handle_failure(claimed, ValueError("nope"), 1)
        row = OxTask.objects.get(id=result.id)
        assert row.run_after is not None
        assert (row.run_after - before).total_seconds() <= worker.backoff_max + 1


class TestThePrivateDjangoAttributesStillExist:
    """
    `_discard_connections` resets five private attributes on Django's
    connection wrapper. All five exist today; the package supports Django 5.2
    LTS through 6.1, and a rename inside that window would make the reset a
    no-op silently: the connection stays closed-inside-a-transaction, the
    outcome write fails, the lease expires, and the task is retried after the
    function already ran.
    """

    @pytest.mark.parametrize(
        "attribute",
        [
            "in_atomic_block",
            "savepoint_ids",
            "atomic_blocks",
            "needs_rollback",
            "closed_in_transaction",
        ],
    )
    def test_the_attribute_is_there(self, attribute):
        from django.db import connections

        wrapper = connections["default"]
        assert hasattr(wrapper, attribute), (
            f"Django no longer has BaseDatabaseWrapper.{attribute}, so "
            "_discard_connections silently stops resetting it"
        )


class TestABatchPassMustSucceed:
    """
    `--batch` ends run() on a pass that found nothing, and a pass the
    database interrupted found out nothing: it cannot count as empty. A
    dispatch pass the database abandoned holds the batch open until a later
    pass completes, since the passes in between do not dispatch at all. A
    single schedule the database rejects no longer abandons the pass.
    """

    @pytest.fixture
    def batch_worker(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        return self.make_worker()

    @staticmethod
    def make_worker(**kwargs):
        # No schedule_interval: ox_worker never passes one, so dispatch runs
        # at most once a second while the poll is far shorter, and most
        # passes do not dispatch at all. A test that dispatched on every
        # pass could not see a failure outliving the pass that had it.
        worker = Worker(
            backoff_initial=0,
            poll_interval=0.02,
            reap_interval=0.0,
            batch=True,
            **kwargs,
        )
        assert worker.schedule_interval > worker.poll_interval
        return worker

    @staticmethod
    def break_dispatch(worker, monkeypatch, *, failures, error=OperationalError):
        """
        Make the worker's first `failures` dispatches raise `error`, or every
        one when it is None. Returns the attempts, one entry each.
        """
        calls = []
        real_dispatch = worker.dispatch_schedules

        def dispatch():
            calls.append(1)
            if failures is None or len(calls) <= failures:
                raise error("gone")
            return real_dispatch()

        monkeypatch.setattr(worker, "dispatch_schedules", dispatch)
        return calls

    def run_to_completion(self, worker, caplog):
        # Registered, so a batch that never finishes is stopped and joined
        # when the test ends instead of claiming the next test's tasks.
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            thread.join(timeout=10)
        assert not thread.is_alive(), "the batch worker never finished"

    @pytest.mark.django_db(transaction=True)
    def test_a_failed_claim_is_not_an_empty_pass(
        self, batch_worker, monkeypatch, caplog
    ):
        calls = []
        real_claim = batch_worker.claim_one

        def claim_once_broken():
            calls.append(1)
            if len(calls) == 1:
                raise OperationalError("gone")
            return real_claim()

        monkeypatch.setattr(batch_worker, "claim_one", claim_once_broken)
        self.run_to_completion(batch_worker, caplog)

        assert events(caplog, "worker_poll_failed")
        assert len(calls) >= 2, "the pass the database interrupted ended the batch"
        (done,) = events(caplog, "worker_batch_empty")
        assert done.worker_id == batch_worker.worker_id
        assert done.claimed == 0

    @pytest.mark.django_db(transaction=True)
    def test_a_failed_schedule_dispatch_is_not_an_empty_pass(
        self, batch_worker, monkeypatch, caplog
    ):
        calls = []
        real_dispatch = batch_worker.dispatch_schedules

        def dispatch_once_broken():
            calls.append(1)
            if len(calls) == 1:
                raise OperationalError("gone")
            return real_dispatch()

        monkeypatch.setattr(batch_worker, "dispatch_schedules", dispatch_once_broken)
        self.run_to_completion(batch_worker, caplog)

        assert events(caplog, "schedule_dispatch_failed")
        assert len(calls) >= 2, "the pass whose dispatch failed ended the batch"
        assert len(events(caplog, "worker_batch_empty")) == 1

    @pytest.mark.django_db(transaction=True)
    def test_a_due_tick_whose_dispatch_failed_runs_before_the_batch_ends(
        self, settings, monkeypatch, caplog
    ):
        settings.TASKS = tasks_setting(MINUTELY_ADD)
        # First sight anchors the schedule without firing it.
        Worker().dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        worker = self.make_worker()
        calls = self.break_dispatch(worker, monkeypatch, failures=1)
        self.run_to_completion(worker, caplog)

        assert len(calls) >= 2, "the batch ended without retrying the dispatch"
        (row,) = OxTask.objects.all()
        assert row.task_path == "tests.tasks.add"
        assert row.status == OxTask.Status.SUCCESSFUL
        (done,) = events(caplog, "worker_batch_empty")
        assert done.claimed == 1

    # A pass-level failure: dispatch_schedules() itself raises, as it does
    # when a shared read fails or a schedule's failure leaves the connection
    # unusable. Any DatabaseError class holds the batch; none is read as
    # transient. It stands for nothing about one schedule: a schedule the
    # database refuses is isolated inside the pass, reported, and does not
    # hold the batch (tests/test_schedule_isolation.py).
    @pytest.mark.parametrize("error", [OperationalError, DataError])
    @pytest.mark.django_db(transaction=True)
    def test_a_dispatch_that_keeps_failing_holds_the_batch_open(
        self, batch_worker, monkeypatch, caplog, error
    ):
        calls = self.break_dispatch(
            batch_worker, monkeypatch, failures=None, error=error
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(batch_worker)
            # The second attempt comes a schedule_interval after the first,
            # across passes that find nothing. A batch that ended on one of
            # those never makes it.
            wait_for(lambda: len(calls) >= 2 or not thread.is_alive(), timeout=30)
            assert thread.is_alive(), "the batch ended with a dispatch still owed"
            batch_worker.request_stop()
            thread.join(timeout=10)
        assert not thread.is_alive()
        assert len(calls) >= 2
        assert not events(caplog, "worker_batch_empty")

    @pytest.mark.django_db(transaction=True)
    def test_an_owed_dispatch_does_not_hold_back_the_task_limit(
        self, batch_worker, monkeypatch, caplog
    ):
        result = tasks.add.enqueue(1, 2)
        batch_worker.max_tasks = 1
        self.break_dispatch(batch_worker, monkeypatch, failures=None)
        self.run_to_completion(batch_worker, caplog)

        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
        (done,) = events(caplog, "worker_max_tasks_reached")
        assert done.claimed == 1
