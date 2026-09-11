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
from django.db import InterfaceError, OperationalError
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker

from . import tasks

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
        The guard used to name `django.db.Error`, and the watchdog above it
        catches `Exception`, so anything outside that taxonomy propagated
        past the recycle. The pool slot was then gone for the life of the
        process and the drain waited on a thread that will never finish.
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
