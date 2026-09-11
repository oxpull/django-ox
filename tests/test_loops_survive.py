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
