"""
Per-task timeouts: TASK_TIMEOUT and the per-queue TASK_TIMEOUTS.

A timeout is soft. The worker stops waiting, records the attempt as failed
and moves the lease epoch; the thread running the task cannot be stopped
and is left to finish on its own, with nothing to write to.
"""

import logging
import threading
import time

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import default_task_backend

from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.timeouts import task_timeouts_from_options
from django_ox.worker import Worker

from .conftest import wait_for
from .tasks import fail_always, record_thread, slow, slow_with_context

TIMEOUT_PATH = f"{TaskTimeout.__module__}.{TaskTimeout.__qualname__}"


def tasks_setting(**options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, **options},
        }
    }


def task_threads():
    return [t for t in threading.enumerate() if t.name.startswith("ox-task-")]


class TestOptions:
    def test_absent_means_no_limit(self):
        timeouts = task_timeouts_from_options({})
        assert timeouts.default is None
        assert timeouts.for_queue("default") is None
        assert not timeouts.enabled

    def test_default_and_per_queue(self):
        timeouts = task_timeouts_from_options(
            {"TASK_TIMEOUT": 30, "TASK_TIMEOUTS": {"exports": 3600, "emails": None}}
        )
        assert timeouts.for_queue("default") == 30.0
        assert timeouts.for_queue("exports") == 3600.0
        # None in the mapping exempts that queue from the global limit.
        assert timeouts.for_queue("emails") is None
        assert timeouts.enabled

    @pytest.mark.parametrize(
        "options",
        [
            {"TASK_TIMEOUT": 0},
            {"TASK_TIMEOUT": -5},
            {"TASK_TIMEOUT": "30"},
            {"TASK_TIMEOUT": True},
            {"TASK_TIMEOUT": float("nan")},
            {"TASK_TIMEOUTS": [30]},
            {"TASK_TIMEOUTS": {"exports": 0}},
            {"TASK_TIMEOUTS": {"exports": "1h"}},
            {"TASK_TIMEOUTS": {"": 30}},
        ],
    )
    def test_invalid_values_are_rejected(self, options):
        with pytest.raises(ImproperlyConfigured):
            task_timeouts_from_options(options)

    @pytest.mark.django_db
    def test_check_reports_invalid_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=-1)
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "TASK_TIMEOUT" in errors[0].msg

    @pytest.mark.django_db
    def test_check_reports_invalid_queue_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUTS={"emails": "soon"})
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "TASK_TIMEOUTS['emails']" in errors[0].msg

    @pytest.mark.django_db
    def test_check_passes_valid_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=30, TASK_TIMEOUTS={"emails": 5})
        assert default_task_backend.check() == []

    @pytest.mark.django_db
    def test_worker_init_rejects_invalid_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=0)
        with pytest.raises(ImproperlyConfigured, match="TASK_TIMEOUT"):
            Worker()

    @pytest.mark.django_db
    def test_worker_keyword_overrides_the_default_only(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=30, TASK_TIMEOUTS={"emails": 5})
        worker = Worker(task_timeout=1)
        assert worker.timeouts.for_queue("default") == 1.0
        assert worker.timeouts.for_queue("emails") == 5.0


@pytest.mark.django_db(transaction=True)
class TestTimedOutAttempt:
    def _timed_out(self, seconds=0.2, **kwargs):
        worker = Worker(backoff_initial=60, poll_interval=0.05, **kwargs)
        result = slow.enqueue(seconds * 5)
        started = time.monotonic()
        assert worker.run_once()
        elapsed = time.monotonic() - started
        return worker, result, elapsed

    def test_is_recorded_as_failed_and_retried_with_backoff(self):
        worker, result, elapsed = self._timed_out(task_timeout=0.2)
        # The worker stopped waiting at the timeout, not when the task ended.
        assert elapsed < 0.8

        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.READY
        assert db_task.attempts == 1
        assert db_task.run_after is not None
        assert db_task.locked_by is None
        error = db_task.errors[-1]
        assert error["exception_class_path"] == TIMEOUT_PATH
        assert "0.2s timeout" in error["traceback"]
        assert "keeps running until the task returns" in error["traceback"]
        assert worker.worker_id in error["traceback"]

    def test_out_of_attempts_marks_failed(self):
        worker = Worker(task_timeout=0.2, backoff_initial=0, poll_interval=0.05)
        result = slow.enqueue(1.0)
        OxTask.objects.filter(id=result.id).update(max_attempts=1)
        assert worker.run_once()

        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.finished_at is not None
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH
        result.refresh()
        assert result.errors[-1].exception_class is TaskTimeout

    def test_moves_the_lease_epoch_and_stops_renewing(self):
        worker, result, _ = self._timed_out(task_timeout=0.2)
        db_task = OxTask.objects.get(id=result.id)
        # One for the claim, one for taking the row off the timed-out thread.
        assert db_task.lease_epoch == 2
        assert worker.renew_leases() == 0
        assert worker._in_flight == set()

    def test_the_runaway_threads_late_write_is_fenced(self):
        worker, result, _ = self._timed_out(task_timeout=0.2)
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert task_threads(), "the task thread is still sleeping"

        # What the timed-out execution held: the claim's epoch, one behind
        # the row. The same write that lands for a live lease is refused.
        stale = OxTask.objects.get(id=result.id)
        stale.lease_epoch = row.lease_epoch - 1
        assert not worker._write_outcome(
            stale, status=OxTask.Status.SUCCESSFUL, duration_ms=0, return_value="x"
        )

        # And the thread itself, once it does return, writes nothing.
        assert wait_for(lambda: not task_threads(), timeout=3)
        row.refresh_from_db()
        assert row.status == OxTask.Status.READY
        assert row.return_value is None
        assert row.lease_epoch == 2

    def test_does_not_run_again_before_the_backoff(self):
        worker, result, _ = self._timed_out(task_timeout=0.2)
        # backoff_initial is 60 s, so the retry is not yet eligible.
        assert not worker.run_once()
        assert OxTask.objects.get(id=result.id).attempts == 1

    def test_logs_the_timeout_event(self, caplog):
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            _, result, _ = self._timed_out(task_timeout=0.2)
        events = [r for r in caplog.records if r.event == "task_timed_out"]
        assert len(events) == 1
        assert events[0].task_id == str(result.id)
        assert events[0].timeout_s == 0.2
        assert events[0].duration_ms >= 200
        assert [r.event for r in caplog.records if r.event == "task_retrying"]

    def test_per_queue_value_wins(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=0.2, TASK_TIMEOUTS={"emails": None})
        worker = Worker(backoff_initial=60, poll_interval=0.05)
        # Runs on the emails queue, so the global 0.2 s does not apply.
        result = slow.using(queue_name="emails").enqueue(0.4)
        assert worker.run_once()
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
class TestWithinTheTimeout:
    def test_success_is_unchanged(self):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        result = slow.enqueue(0.05)
        assert worker.run_once()
        result.refresh()
        assert result.return_value == "done"
        assert OxTask.objects.get(id=result.id).lease_epoch == 1
        assert wait_for(lambda: not task_threads(), timeout=2)

    def test_an_exception_is_the_ordinary_failure(self):
        worker = Worker(task_timeout=5, backoff_initial=60, poll_interval=0.05)
        result = fail_always.enqueue()
        assert worker.run_once()
        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.READY
        assert db_task.errors[-1]["exception_class_path"] == "builtins.ValueError"
        assert "boom" in db_task.errors[-1]["traceback"]
        # An ordinary retry keeps the epoch; only a timeout moves it.
        assert db_task.lease_epoch == 1

    def test_context_reaches_the_task(self):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        result = slow_with_context.enqueue(0.01)
        assert worker.run_once()
        result.refresh()
        assert result.return_value == 1

    def test_runs_on_its_own_thread(self, task_state):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        record_thread.enqueue()
        assert worker.run_once()
        assert task_state["thread_name"].startswith("ox-task-")

    def test_no_timeout_runs_on_the_calling_thread(self, task_state):
        worker = Worker(backoff_initial=0, poll_interval=0.05)
        record_thread.enqueue()
        assert worker.run_once()
        assert task_state["thread_name"] == threading.current_thread().name


@pytest.mark.django_db(transaction=True)
class TestDrain:
    def test_stop_does_not_wait_for_a_timed_out_thread(self):
        worker = Worker(task_timeout=0.2, backoff_initial=60, poll_interval=0.05)
        result = slow.enqueue(3.0)
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            # Claimed, timed out, and back to READY with its attempt spent.
            assert wait_for(
                lambda: OxTask.objects.get(id=result.id).attempts == 1, timeout=5
            )
            assert wait_for(
                lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.READY,
                timeout=5,
            )
            assert task_threads(), "the task thread is still sleeping"
            started = time.monotonic()
            worker.request_stop()
            thread.join(timeout=2)
            assert not thread.is_alive()
            assert time.monotonic() - started < 1.5
            # The drain did not wait for it; it is still running.
            assert task_threads()
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.READY
        assert db_task.attempts == 1

    def test_stop_waits_for_a_task_inside_its_timeout(self):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        result = slow.enqueue(0.5)
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            assert wait_for(
                lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
            )
            worker.request_stop()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            worker.request_stop()
            thread.join(timeout=5)
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
