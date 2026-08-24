"""
Task timeouts: TASK_TIMEOUT, the per-queue TASK_TIMEOUTS, and the grace
backstop TASK_TIMEOUT_GRACE.

At the deadline the worker raises TaskTimeout on the task's own thread (or
cancels the coroutine of an async task), so the thread comes back to the
pool with its connection and its locks released. A thread that is still
running TASK_TIMEOUT_GRACE seconds later is blocked outside Python; the
worker records the attempt as failed, fences the thread off the row, and
recycles itself so the thread dies with the process.

A thread a coverage or tracing tool is watching is left alone: the attempt
is registered for the grace backstop and nothing is raised inside the task.
"""

import contextlib
import logging
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from inspect import iscoroutinefunction

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connection, connections
from django.tasks import TaskResultStatus, default_task_backend
from django.utils import timezone

import django_ox
from django_ox.bulk import enqueue_many
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.supervisor import RECYCLE_EXIT_CODE, Supervisor
from django_ox.timeouts import MAX_SECONDS, task_timeouts_from_options
from django_ox.worker import WATCHDOG_MAX_WAIT, Worker, _active_tracer

from .conftest import start_worker_thread, wait_for
from .tasks import (
    add,
    async_catch_timeout,
    async_report_deadline,
    async_spin,
    atomic_write_until_released,
    busy,
    query_then_hold,
    raise_timeout,
    report_deadline,
    slow,
    spin,
    spin_in_atomic,
    swallow_then_run_on,
    swallow_timeout,
    write_loop,
    write_until_released,
)

TIMEOUT_PATH = f"{TaskTimeout.__module__}.{TaskTimeout.__qualname__}"

postgres_only = pytest.mark.skipif(
    connection.vendor != "postgresql", reason="counts pg_stat_activity sessions"
)
sqlite_only = pytest.mark.skipif(
    connection.vendor != "sqlite", reason="SQLite has one writer; this is its lock"
)


def tasks_setting(**options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, **options},
        }
    }


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


_threads_before_test: set[threading.Thread] = set()


@pytest.fixture(autouse=True)
def _snapshot_worker_threads():
    """
    A previous test's worker can still be winding down (a drain on a slow
    database outlives its test); its threads must not fail this test's
    own thread accounting.
    """
    _threads_before_test.clear()
    _threads_before_test.update(
        t for t in threading.enumerate() if t.name.startswith("ox")
    )
    yield


def monitoring_tools():
    """Registered sys.monitoring tool ids, by id."""
    return {
        tool_id: name
        for tool_id in range(6)
        if (name := sys.monitoring.get_tool(tool_id)) is not None
    }


@pytest.fixture
def interruptible_attempts(monkeypatch):
    """
    Run each attempt on a thread the worker will raise TaskTimeout inside.

    The worker leaves a watched thread alone, so under `pytest --cov` the
    attempts in a test that asks for this fixture would fall back to the
    grace backstop and the interruption the test is about would never
    happen. The trace hook is per thread, so it is taken off for the length
    of the attempt and put back afterwards, and the attempt then runs
    unwatched for real. A sys.monitoring tool cannot be taken off one
    thread, so where the session is measured that way the probe is stubbed
    instead. Every other thread, and the rest of the test, is measured as
    usual.
    """
    from django_ox import worker as worker_module

    original = Worker._call_task

    def call_task(self, task, db_task, task_result, timeout):
        if iscoroutinefunction(task.func):
            return original(self, task, db_task, task_result, timeout)
        installed = sys.gettrace()
        sys.settrace(None)
        try:
            return original(self, task, db_task, task_result, timeout)
        finally:
            sys.settrace(installed)

    monkeypatch.setattr(Worker, "_call_task", call_task)
    if monitoring_tools():
        monkeypatch.setattr(worker_module, "_active_tracer", lambda: None)


def worker_threads():
    return [
        t
        for t in threading.enumerate()
        if t.name.startswith("ox") and t not in _threads_before_test
    ]


def wait_until_no_task_is_pending(stall_timeout=60.0, budget=300.0):
    """
    True once no task is READY or RUNNING. Waits on progress rather than
    the clock, so a slow database gets the time it needs: it gives up
    after `stall_timeout` seconds in which the pending count did not
    move, or after `budget` seconds in total, so a worker that is merely
    crawling still fails its test instead of outliving the CI job.
    """
    pending = None
    started = time.monotonic()
    changed_at = started
    while True:
        now_pending = OxTask.objects.filter(
            status__in=(OxTask.Status.READY, OxTask.Status.RUNNING)
        ).count()
        if now_pending == 0:
            return True
        now = time.monotonic()
        if now - started > budget:
            return False
        if now_pending != pending:
            pending = now_pending
            changed_at = now
        elif now - changed_at > stall_timeout:
            return False
        time.sleep(0.1)


def run_workers_until_no_task_is_pending(
    make_worker, workers, threads, on_poll=None, stall_timeout=60.0, budget=300.0
):
    """
    Run a worker until no task is READY or RUNNING, starting a fresh one
    whenever the grace backstop recycles the one before -- the
    supervisor's job in production, done inline here. Progress-based like
    wait_until_no_task_is_pending: the backstop resolves a stalled batch
    within one grace, so a drain that moves nothing for `stall_timeout`
    seconds, or takes more than `budget` in total, is a genuine failure.
    Every worker and thread started lands in `workers` and `threads`, so
    the caller's cleanup sees them all however this exits.
    """

    def start():
        worker = make_worker()
        workers.append(worker)
        threads.append(start_worker_thread(worker))
        return worker, threads[-1]

    worker, thread = start()
    started = time.monotonic()
    pending = None
    changed_at = started
    while True:
        if on_poll is not None:
            on_poll()
        now_pending = OxTask.objects.filter(
            status__in=(OxTask.Status.READY, OxTask.Status.RUNNING)
        ).count()
        if now_pending == 0:
            return
        if not thread.is_alive():
            assert worker.recycling, "the worker exited without being asked"
            worker, thread = start()
            changed_at = time.monotonic()
        now = time.monotonic()
        assert now - started <= budget, (
            f"{now_pending} task(s) still pending after {budget:g}s "
            f"across {len(workers)} worker(s)"
        )
        if now_pending != pending:
            pending = now_pending
            changed_at = now
        else:
            assert now - changed_at <= stall_timeout, (
                f"no progress for {stall_timeout:g}s with {now_pending} task(s) pending"
            )
        time.sleep(0.05)


def run_in_thread(worker):
    return start_worker_thread(worker)


def row(result):
    return OxTask.objects.get(id=result.id)


# -- options and checks -----------------------------------------------------


class TestOptions:
    def test_absent_means_no_limit(self):
        timeouts = task_timeouts_from_options({})
        assert timeouts.default is None
        assert timeouts.for_queue("default") is None
        assert timeouts.grace == 30.0
        assert not timeouts.enabled

    def test_default_per_queue_and_grace(self):
        timeouts = task_timeouts_from_options(
            {
                "TASK_TIMEOUT": 30,
                "TASK_TIMEOUTS": {"exports": 3600, "emails": None},
                "TASK_TIMEOUT_GRACE": 5,
            }
        )
        assert timeouts.for_queue("default") == 30.0
        assert timeouts.for_queue("exports") == 3600.0
        # None in the mapping exempts that queue from the global limit.
        assert timeouts.for_queue("emails") is None
        assert timeouts.grace == 5.0
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
            {"TASK_TIMEOUT_GRACE": 0},
            {"TASK_TIMEOUT_GRACE": None},
            {"TASK_TIMEOUT_GRACE": "30"},
        ],
    )
    def test_invalid_values_are_rejected(self, options):
        with pytest.raises(ImproperlyConfigured):
            task_timeouts_from_options(options)

    @pytest.mark.parametrize(
        "options",
        [
            {"TASK_TIMEOUT": float("inf")},
            {"TASK_TIMEOUT": math.inf},
            {"TASK_TIMEOUT": 1e14},
            {"TASK_TIMEOUT": MAX_SECONDS + 1},
            {"TASK_TIMEOUTS": {"exports": float("inf")}},
            {"TASK_TIMEOUT_GRACE": float("inf")},
        ],
    )
    def test_infinite_and_astronomical_values_are_rejected(self, options):
        """
        inf is a plausible spelling of no limit, and it passed validation
        while the deadline arithmetic on every attempt overflowed, so each
        attempt failed with OverflowError before the task was called.
        None is the spelling; the message says so for the two options that
        take it.
        """
        with pytest.raises(ImproperlyConfigured, match="finite number of seconds"):
            task_timeouts_from_options(options)

    def test_the_infinity_message_points_at_none_where_none_is_no_limit(self):
        with pytest.raises(ImproperlyConfigured, match="None means no limit"):
            task_timeouts_from_options({"TASK_TIMEOUT": float("inf")})
        with pytest.raises(ImproperlyConfigured) as info:
            task_timeouts_from_options({"TASK_TIMEOUT_GRACE": float("inf")})
        assert "None" not in str(info.value)

    def test_the_largest_accepted_value_is_usable(self):
        timeouts = task_timeouts_from_options(
            {"TASK_TIMEOUT": MAX_SECONDS, "TASK_TIMEOUT_GRACE": MAX_SECONDS}
        )
        assert timeouts.for_queue("default") == MAX_SECONDS
        assert timeouts.grace == MAX_SECONDS
        # The arithmetic the worker does with it must not overflow.
        timezone.now() + timedelta(seconds=MAX_SECONDS)

    def test_every_problem_is_listed_not_just_the_first(self):
        with pytest.raises(ImproperlyConfigured) as info:
            task_timeouts_from_options(
                {"TASK_TIMEOUT": -1, "TASK_TIMEOUTS": {"nope": 10, "emails": "x"}},
                ["default", "emails"],
            )
        message = str(info.value)
        assert "TASK_TIMEOUT must be greater than zero" in message
        assert "names the queue 'nope'" in message
        assert "TASK_TIMEOUTS['emails'] must be a number" in message

    def test_unknown_queue_is_rejected_when_queues_are_named(self):
        with pytest.raises(ImproperlyConfigured, match="not in QUEUES"):
            task_timeouts_from_options(
                {"TASK_TIMEOUTS": {"nosuchqueue": 1}}, ["default", "emails"]
            )
        # An empty QUEUES accepts any queue name, so it accepts any key.
        assert task_timeouts_from_options({"TASK_TIMEOUTS": {"anything": 1}}, [])

    @pytest.mark.django_db
    def test_check_reports_invalid_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=-1)
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "TASK_TIMEOUT" in errors[0].msg

    @pytest.mark.django_db
    def test_check_reports_invalid_grace(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT_GRACE=0)
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "TASK_TIMEOUT_GRACE" in errors[0].msg

    @pytest.mark.django_db
    def test_check_reports_an_infinite_timeout(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=float("inf"))
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "finite" in errors[0].msg
        settings.TASKS = tasks_setting(TASK_TIMEOUTS={"emails": math.inf})
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004"]
        assert "TASK_TIMEOUTS['emails']" in errors[0].msg

    @pytest.mark.django_db
    def test_check_reports_every_problem_at_once(self, settings):
        """One run of manage.py check names them all; Django's own do."""
        settings.TASKS = tasks_setting(TASK_TIMEOUT=-1, TASK_TIMEOUTS={"nope": 10})
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E004", "django_ox.E005"]
        assert "TASK_TIMEOUT must be greater than zero" in errors[0].msg
        assert "'nope'" in errors[1].msg

    @pytest.mark.django_db
    def test_check_reports_unknown_queue(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUTS={"nosuchqueue": 1})
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E005"]
        assert "'nosuchqueue'" in errors[0].msg

    @pytest.mark.django_db
    def test_check_passes_valid_values(self, settings):
        settings.TASKS = tasks_setting(
            TASK_TIMEOUT=30, TASK_TIMEOUTS={"emails": 5}, TASK_TIMEOUT_GRACE=10
        )
        assert default_task_backend.check() == []

    @pytest.mark.django_db
    @pytest.mark.parametrize(
        ("options", "error_id"),
        [
            ('{"TASK_TIMEOUT": -1}', "django_ox.E004"),
            ('{"TASK_TIMEOUT": Infinity}', "django_ox.E004"),
            ('{"TASK_TIMEOUT_GRACE": 0}', "django_ox.E004"),
            ('{"TASK_TIMEOUTS": {"nope": 5}}', "django_ox.E005"),
            ('{"SCHEDULES": {"bad": {"task": "x", "cron": "nope"}}}', "django_ox.E002"),
        ],
    )
    def test_manage_py_check_reports_them_in_a_plain_project(self, options, error_id):
        """
        Django registers its tasks check when django.tasks is imported,
        and a project with no admin and no URLconf imports it nowhere
        before `manage.py check` runs. The app has to do that itself, or
        every django_ox check is silent exactly where there is nothing
        else to catch the mistake.
        """
        env = dict(os.environ)
        env["DJANGO_SETTINGS_MODULE"] = "tests.settings_plain"
        env["OX_TEST_TASKS_OPTIONS"] = options
        completed = subprocess.run(
            [sys.executable, "-m", "django", "check"],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert completed.returncode == 1, completed.stdout + completed.stderr
        assert error_id in completed.stderr, completed.stderr

    def test_manage_py_check_passes_in_a_plain_project(self):
        env = dict(os.environ)
        env["DJANGO_SETTINGS_MODULE"] = "tests.settings_plain"
        env["OX_TEST_TASKS_OPTIONS"] = (
            '{"TASK_TIMEOUT": 5, "TASK_TIMEOUTS": {"emails": 1}}'
        )
        completed = subprocess.run(
            [sys.executable, "-m", "django", "check"],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr

    def test_worker_init_rejects_invalid_values(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=0)
        with pytest.raises(ImproperlyConfigured, match="TASK_TIMEOUT"):
            Worker()
        # Used to start, then fail every attempt with OverflowError in the
        # deadline arithmetic until the task was FAILED.
        with pytest.raises(ImproperlyConfigured, match="finite"):
            Worker(task_timeout=float("inf"))
        with pytest.raises(ImproperlyConfigured, match="finite"):
            Worker(task_timeout=1, task_timeout_grace=float("inf"))
        settings.TASKS = tasks_setting(TASK_TIMEOUTS={"nosuchqueue": 1})
        with pytest.raises(ImproperlyConfigured, match="nosuchqueue"):
            Worker()

    @pytest.mark.django_db
    def test_worker_keywords_override_default_and_grace_only(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=30, TASK_TIMEOUTS={"emails": 5})
        worker = Worker(task_timeout=1, task_timeout_grace=2)
        assert worker.timeouts.for_queue("default") == 1.0
        assert worker.timeouts.for_queue("emails") == 5.0
        assert worker.timeouts.grace == 2.0

    def test_task_timeout_is_a_timeout_error(self):
        assert issubclass(TaskTimeout, TimeoutError)
        # Raised by class when injected, so it must construct bare.
        assert TaskTimeout().timeout is None


# -- the soft timeout -------------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestSoftTimeout:
    def _timed_out(self, task=spin, seconds=2.0, timeout=0.2, **kwargs):
        worker = Worker(backoff_initial=60, poll_interval=0.05, **kwargs)
        result = task.enqueue(seconds)
        started = time.monotonic()
        assert worker.run_once()
        elapsed = time.monotonic() - started
        return worker, result, elapsed

    def test_raised_inside_the_task_and_recorded(
        self, task_state, interruptible_attempts
    ):
        worker, result, elapsed = self._timed_out(task_timeout=0.2)
        # The task stopped at the deadline, not when its loop ended.
        assert elapsed < 1.0
        assert task_state.get("caught") is True
        assert task_state.get("finally_ran") is True

        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.attempts == 1
        assert db_task.run_after is not None
        assert db_task.locked_by is None
        # The thread came back on its own, so no handover: the epoch is the
        # claim's, and the renewal set is empty.
        assert db_task.lease_epoch == 1
        assert worker._in_flight == set()
        error = db_task.errors[-1]
        assert error["exception_class_path"] == TIMEOUT_PATH
        assert "0.2s timeout" in error["traceback"]
        assert worker.worker_id in error["traceback"]
        # The traceback is the task's own frames at the point it stopped.
        assert "_spin" in error["traceback"]

    def test_out_of_attempts_marks_failed(self, interruptible_attempts):
        worker = Worker(task_timeout=0.2, backoff_initial=0, poll_interval=0.05)
        result = spin.enqueue(2.0)
        OxTask.objects.filter(id=result.id).update(max_attempts=1)
        assert worker.run_once()

        db_task = row(result)
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.finished_at is not None
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH
        result.refresh()
        assert result.status == TaskResultStatus.FAILED
        assert result.errors[-1].exception_class is TaskTimeout

    def test_retried_on_the_backoff_and_not_before(self, interruptible_attempts):
        worker, result, _ = self._timed_out(task_timeout=0.2)
        assert not worker.run_once()
        assert row(result).attempts == 1
        OxTask.objects.filter(id=result.id).update(run_after=None)
        assert worker.run_once()
        assert row(result).attempts == 2

    def test_logs_the_timeout_event(self, caplog, interruptible_attempts):
        caplog.set_level(logging.WARNING, logger="django_ox")
        _, result, _ = self._timed_out(task_timeout=0.2)
        (timed_out,) = events(caplog, "task_timed_out")
        assert timed_out.task_id == str(result.id)
        assert timed_out.timeout_s == 0.2
        assert timed_out.duration_ms >= 200
        assert timed_out.levelno == logging.WARNING
        assert events(caplog, "task_retrying")
        assert not events(caplog, "task_stuck")
        assert not events(caplog, "worker_error")

    def test_thread_returns_to_the_pool_across_fifty_timeouts(
        self, interruptible_attempts
    ):
        worker = Worker(
            task_timeout=0.02, backoff_initial=0, poll_interval=0.05, concurrency=2
        )
        for _ in range(50):
            spin.enqueue(1.0)
        OxTask.objects.update(max_attempts=1)

        # Threads from the previous test may still be on their way out.
        assert wait_for(lambda: not worker_threads(), timeout=5)
        before = threading.active_count()
        peak = before

        def all_failed():
            nonlocal peak
            peak = max(peak, threading.active_count())
            return row_count(OxTask.Status.FAILED) == 50

        thread = run_in_thread(worker)
        try:
            assert wait_for(all_failed, timeout=30)
        finally:
            worker.request_stop()
            thread.join(timeout=10)
        assert not thread.is_alive()
        assert wait_for(lambda: not worker_threads(), timeout=5)
        # The run loop, the renewer, the watchdog and the pool, and nothing
        # else; every one of them is gone once the worker has stopped.
        assert peak <= before + 3 + worker.concurrency, peak
        assert threading.active_count() <= before, [
            t.name for t in threading.enumerate()
        ]
        assert not OxTask.objects.exclude(status=OxTask.Status.FAILED).exists()
        assert all(
            r.errors[-1]["exception_class_path"] == TIMEOUT_PATH
            for r in OxTask.objects.all()
        )

    def test_a_task_may_catch_it_and_return(self, interruptible_attempts):
        worker = Worker(task_timeout=0.2, backoff_initial=0, poll_interval=0.05)
        result = swallow_timeout.enqueue(2.0)
        started = time.monotonic()
        assert worker.run_once()
        assert time.monotonic() - started < 1.0
        result.refresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == "cleaned up"

    def test_a_task_raising_it_itself_is_a_timeout_failure(self, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(task_timeout=5, backoff_initial=60, poll_interval=0.05)
        result = raise_timeout.enqueue()
        assert worker.run_once()
        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == 1
        assert "raised by the task itself" in db_task.errors[-1]["traceback"]
        (timed_out,) = events(caplog, "task_timed_out")
        assert timed_out.timeout_s == 1.5
        assert not events(caplog, "task_stuck")

    def test_per_queue_none_exempts_the_queue(self, settings):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=0.2, TASK_TIMEOUTS={"emails": None})
        worker = Worker(backoff_initial=60, poll_interval=0.05)
        result = spin.using(queue_name="emails").enqueue(0.5)
        assert worker.run_once()
        assert row(result).status == OxTask.Status.SUCCESSFUL

    def test_per_queue_value_wins(self, settings, interruptible_attempts):
        settings.TASKS = tasks_setting(TASK_TIMEOUT=5, TASK_TIMEOUTS={"emails": 0.2})
        worker = Worker(backoff_initial=60, poll_interval=0.05)
        result = spin.using(queue_name="emails").enqueue(2.0)
        assert worker.run_once()
        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert "0.2s timeout" in db_task.errors[-1]["traceback"]

    def test_success_within_the_timeout_is_unchanged(self, task_state):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        result = spin.enqueue(0.05)
        assert worker.run_once()
        result.refresh()
        assert result.return_value == "done"
        assert row(result).lease_epoch == 1
        assert "caught" not in task_state
        assert django_ox.deadline() is None

    def test_an_ordinary_exception_is_the_ordinary_failure(self):
        from .tasks import fail_always

        worker = Worker(task_timeout=5, backoff_initial=60, poll_interval=0.05)
        result = fail_always.enqueue()
        assert worker.run_once()
        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.errors[-1]["exception_class_path"] == "builtins.ValueError"

    def test_no_timeout_leaves_the_stored_traceback_as_it_was(self):
        """
        With no timeout on the queue the worker calls the task the way it
        did before timeouts existed: the stored traceback goes straight
        from _run_attempt into django.tasks and the task, with no frame of
        the timeout machinery between them. Tooling that string-matches
        traceback frames saw two new ones otherwise.
        """
        from .tasks import fail_always

        worker = Worker(backoff_initial=60, poll_interval=0.05)
        assert worker.timeouts.for_queue("default") is None
        result = fail_always.enqueue()
        assert worker.run_once()
        traceback = row(result).errors[-1]["traceback"]
        frames = re.findall(r'File ".*?", line \d+, in (\w+)', traceback)
        assert frames == ["_run_attempt", "call", "fail_always"], traceback

    def test_a_deadline_years_out_is_waited_for_in_steps(self, monkeypatch):
        """
        The watchdog sleeps until the earliest deadline. A condition wait
        longer than the platform allows raises OverflowError, which killed
        the watchdog thread for any timeout past about 49 days on Windows
        and past the end of time_t elsewhere; the largest accepted value
        is well past both.
        """
        assert MAX_SECONDS > WATCHDOG_MAX_WAIT
        died: list[threading.ExceptHookArgs] = []
        monkeypatch.setattr(threading, "excepthook", died.append)
        worker = Worker(
            task_timeout=MAX_SECONDS, backoff_initial=60, poll_interval=0.05
        )
        result = spin.enqueue(0.3)
        assert worker.run_once()
        result.refresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert died == [], died
        # The watchdog is idling after the attempt, not dead.
        assert worker._watchdog is not None and worker._watchdog.is_alive()


def row_count(status):
    return OxTask.objects.filter(status=status).count()


# -- the cooperative helpers ------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestDeadline:
    def test_deadline_and_remaining_inside_a_task(self):
        worker = Worker(task_timeout=30, backoff_initial=0, poll_interval=0.05)
        result = report_deadline.enqueue()
        before = timezone.now()
        assert worker.run_once()
        result.refresh()
        reported = result.return_value
        deadline = datetime.fromisoformat(reported["deadline"])
        assert 29 < (deadline - before).total_seconds() <= 31
        assert 29 < reported["remaining"] <= 30

    def test_inside_an_async_task(self):
        worker = Worker(task_timeout=30, backoff_initial=0, poll_interval=0.05)
        result = async_report_deadline.enqueue()
        assert worker.run_once()
        result.refresh()
        assert 29 < result.return_value <= 30

    def test_none_without_a_timeout_and_outside_a_task(self):
        worker = Worker(backoff_initial=0, poll_interval=0.05)
        result = report_deadline.enqueue()
        assert worker.run_once()
        result.refresh()
        assert result.return_value == {"deadline": None, "remaining": None}
        assert django_ox.deadline() is None
        assert django_ox.remaining() is None


# -- locks, connections and late writes -------------------------------------


@pytest.mark.django_db(transaction=True)
class TestReleasesWhatItHeld:
    def test_atomic_block_rolls_back_and_the_outcome_write_lands(
        self, task_state, caplog, interruptible_attempts
    ):
        """
        A task inside transaction.atomic() holds the writer lock on SQLite.
        TaskTimeout unwinds the block, so the lock is gone by the time the
        worker writes the outcome; nothing waits, nothing is reaped.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(
            task_timeout=0.2, backoff_initial=60, poll_interval=0.05, lock_timeout=5
        )
        result = spin_in_atomic.enqueue(5.0)
        started = time.monotonic()
        assert worker.run_once()
        assert time.monotonic() - started < 2.0
        assert task_state.get("writer_locked") is True

        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.priority == 0, "the transaction rolled back"
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH
        assert worker.reap() == 0
        assert not events(caplog, "worker_error")
        assert not events(caplog, "lease_renew_failed")
        # The pool thread's connection is usable and outside any transaction.
        assert not connections["default"].in_atomic_block

    def test_no_write_lands_after_the_timeout(self, task_state, interruptible_attempts):
        """
        A task writing in a loop stops at the injection. The retry's row is
        not touched by the first attempt afterwards, and a write the first
        attempt might still try carries the old epoch and is refused.
        """
        worker = Worker(task_timeout=0.3, backoff_initial=0, poll_interval=0.05)
        result = write_loop.enqueue(1.5)
        assert worker.run_once()
        first = row(result)
        assert first.status == OxTask.Status.READY
        writes_at_timeout = task_state["writes"]
        assert writes_at_timeout >= 1
        # The thread is back; nothing keeps writing.
        time.sleep(0.3)
        assert task_state["writes"] == writes_at_timeout
        assert row(result).return_value["written_by"] == 1

        # Attempt 2 runs to completion on a queue without the limit. Its
        # writes are counted from zero: how many land in its window is the
        # database's speed, not the invariant; that they land at all is.
        OxTask.objects.filter(id=result.id).update(run_after=None)
        worker.timeouts.default = None
        task_state["writes"] = 0
        assert worker.run_once()
        time.sleep(0.3)
        second = row(result)
        assert second.status == OxTask.Status.SUCCESSFUL
        assert second.return_value == "loop done"
        assert task_state["writes"] >= 1, "attempt 2's writes landed"

        # And the epoch fence, for a write the stuck path would have to stop:
        # the claim's epoch is one behind the row after a handover.
        stale = row(result)
        stale.lease_epoch = second.lease_epoch - 1
        assert not worker._write_outcome(
            stale, status=OxTask.Status.SUCCESSFUL, duration_ms=0, return_value="x"
        )
        assert row(result).return_value == "loop done"

    @postgres_only
    def test_many_timeouts_do_not_pile_up_connections(self, task_state, caplog):
        """
        120 ORM tasks that each time out, at concurrency 8. Every thread
        comes back and closes its connection, so the session count stays
        within the pool plus the worker's own threads. Each task holds
        until the test releases it, so no attempt can finish before its
        delivery lands. A delivery that cannot land within the grace is
        the backstop's case, exactly as documented: the attempt is
        recorded as stuck, the worker recycles, and the replacement --
        started here the way the supervisor would -- carries on. Either
        way, every attempt ends recorded, fenced, and off its connection.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")

        def make_worker():
            # The default grace. The backstop is part of the contract
            # under test: on a runner too slow for the delivery to land,
            # it is what keeps the drain moving.
            return Worker(
                task_timeout=0.05,
                backoff_initial=0,
                poll_interval=0.02,
                concurrency=8,
                lock_timeout=2.0,
            )

        task_state["release"] = threading.Event()
        enqueue_many(query_then_hold, [((), {}) for _ in range(120)])
        peak_used = 0
        peak_total = 0

        def sessions():
            """
            (used, total) backends on this database. `used` is every
            session that has run at least one statement -- the only kind
            the worker's threads produce, since a claim, a renewal, an
            outcome write and a task body all query. A starved server
            also accumulates sessions that never got that far (connection
            attempts a delivery interrupted mid-handshake, held by the
            test process until it exits, and closed backends not yet
            reaped); those are the environment's, counted only against
            the coarse cap below.
            """
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FILTER (WHERE query <> ''), count(*) "
                    "FROM pg_stat_activity WHERE datname = current_database()"
                )
                return cursor.fetchone()

        def note_peak():
            nonlocal peak_used, peak_total
            used, total = sessions()
            peak_used = max(peak_used, used)
            peak_total = max(peak_total, total)

        workers: list[Worker] = []
        threads: list[threading.Thread] = []
        try:
            run_workers_until_no_task_is_pending(
                make_worker, workers, threads, on_poll=note_peak
            )
        finally:
            task_state["release"].set()
            for worker in workers:
                worker.request_stop()
            for thread in threads:
                thread.join(timeout=15)
        assert not any(thread.is_alive() for thread in threads)
        # A recycled worker's stuck threads outlive it until the release;
        # every one must finish and lose its fenced write before the rows
        # are judged.
        assert wait_for(lambda: not worker_threads(), timeout=60)

        db_tasks = list(OxTask.objects.all())
        assert len(db_tasks) == 120
        for db_task in db_tasks:
            assert db_task.status == OxTask.Status.FAILED, db_task.errors
            assert {e["exception_class_path"] for e in db_task.errors} == {
                TIMEOUT_PATH
            }, db_task.errors
            assert db_task.attempts == db_task.max_attempts
            assert db_task.attempts == len(db_task.worker_ids)
            assert db_task.return_value != "released", "a fenced write landed"
        # Beside each worker's pool: the run loop, the renewer, this
        # test's polling connection, and one just-closed session the
        # server has not torn down yet. A recycle strands up to a pool's
        # worth of stuck connections until the release, on top of the
        # replacement's own. 120 leaking sessions would dwarf any of it.
        assert peak_used <= 12 + 8 * (len(workers) - 1), (peak_used, len(workers))
        # The coarse cap on what the storm leaves behind on a slow
        # server: handshake debris and unreaped backends stay far from
        # the server's limit, while a genuine pileup -- counted in
        # attempts, 360 here -- would blow straight past it.
        assert peak_total < 60, peak_total
        assert not events(caplog, "worker_error")
        assert not events(caplog, "task_reclaimed")
        assert "too many clients" not in caplog.text

    @postgres_only
    @pytest.mark.parametrize(
        "loop", [write_until_released, atomic_write_until_released]
    )
    def test_a_timeout_landing_inside_a_statement_is_recorded(
        self, task_state, caplog, loop
    ):
        """
        psycopg drives a statement through Python, so the injected
        exception can land after the query was sent and before its result
        was read, leaving the thread's connection with a command in flight;
        inside atomic() the same landing makes Django's own exit raise. The
        worker must still record the attempt as a timeout on a connection
        it can use, rather than fail its outcome write and leave the row
        RUNNING for the reaper. 300 tasks writing as fast as they can,
        timed out at 50 ms, land inside a statement often enough.

        A thread waiting inside the driver's C wait is out of the
        exception's reach until the statement returns -- the documented
        limit of delivery, and the grace backstop's whole reason to
        exist. So the claim is the invariant, not the path: every attempt
        ends FAILED with TaskTimeout, recorded either where the delivery
        landed or by the backstop as stuck, every straggler write is
        fenced, and nothing is left RUNNING. Which path recorded it
        belongs to the machine the test happens to run on.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")

        def make_worker():
            # The default grace, so a delivery a slow database blocks
            # past it becomes the backstop's stuck record, not a hang.
            return Worker(
                task_timeout=0.05,
                backoff_initial=0,
                poll_interval=0.02,
                concurrency=4,
                lock_timeout=60,
            )

        task_state["release"] = threading.Event()
        enqueue_many(loop, [((), {}) for _ in range(300)])
        OxTask.objects.update(max_attempts=1)

        workers: list[Worker] = []
        threads: list[threading.Thread] = []
        try:
            run_workers_until_no_task_is_pending(make_worker, workers, threads)
        finally:
            task_state["release"].set()
            for worker in workers:
                worker.request_stop()
            for thread in threads:
                thread.join(timeout=15)
        assert not any(thread.is_alive() for thread in threads)
        # Stuck threads outlive their worker until the release; every one
        # must finish and lose its fenced write before the rows are
        # judged.
        assert wait_for(lambda: not worker_threads(), timeout=60)

        db_tasks = list(OxTask.objects.all())
        assert len(db_tasks) == 300
        landed_in_the_driver = 0
        recorded_by_backstop = 0
        for db_task in db_tasks:
            # SUCCESSFUL is impossible by construction: the release only
            # opens once nothing is pending, so every attempt was already
            # terminal, and a straggler's later outcome is refused by the
            # fence rather than recorded.
            assert db_task.status == OxTask.Status.FAILED, db_task.errors
            assert [e["exception_class_path"] for e in db_task.errors] == [
                TIMEOUT_PATH
            ], db_task.errors
            assert db_task.attempts == 1 == len(db_task.worker_ids)
            assert db_task.return_value != "released", "a fenced write landed"
            trace = db_task.errors[-1]["traceback"]
            if "did not stop" in trace:
                recorded_by_backstop += 1
            else:
                assert 'File "' in trace, trace
                if "psycopg" in trace:
                    landed_in_the_driver += 1
        assert landed_in_the_driver > 0, "no delivery landed inside a statement"
        assert recorded_by_backstop <= len(events(caplog, "task_stuck"))
        assert not events(caplog, "worker_error")

    def test_injection_never_lands_in_worker_bookkeeping(
        self, caplog, interruptible_attempts
    ):
        """
        A thousand short tasks with a timeout about equal to their runtime,
        so the deadline races the deregistration on every one of them.
        Each outcome is a clean success or a clean timeout; no exception
        escapes into the runner, and no row is left RUNNING.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        # Durations straddle the timeout by more than the GIL switch
        # interval, so some deliveries land inside the loop and some arrive
        # after it has returned.
        worker = Worker(
            task_timeout=0.01,
            backoff_initial=0,
            poll_interval=0.01,
            concurrency=4,
            lock_timeout=60,
        )
        enqueue_many(busy, [((0.005 + (i % 5) * 0.0025,), {}) for i in range(1000)])
        OxTask.objects.update(max_attempts=1)

        thread = run_in_thread(worker)
        try:
            assert wait_until_no_task_is_pending()
        finally:
            worker.request_stop()
            thread.join(timeout=15)
        assert not thread.is_alive()

        successes = row_count(OxTask.Status.SUCCESSFUL)
        failures = row_count(OxTask.Status.FAILED)
        assert successes + failures == 1000
        assert failures > 0, "no timeout fired; the race was not exercised"
        assert successes > 0, "every task timed out; the race was not exercised"
        for db_task in OxTask.objects.filter(status=OxTask.Status.FAILED):
            assert [e["exception_class_path"] for e in db_task.errors] == [TIMEOUT_PATH]
        assert not events(caplog, "worker_error")
        assert not events(caplog, "task_lease_lost")
        assert not events(caplog, "task_stuck")
        # Every timeout landed in the task body or on the instruction that
        # called it (a call's return is an eval-breaker check, so a delivery
        # that arrives as the task returns surfaces there); never in the
        # runner's bookkeeping around the call.
        landed_in = {"task": 0, "call": 0}
        for db_task in OxTask.objects.filter(status=OxTask.Status.FAILED):
            frames = [
                line
                for line in db_task.errors[-1]["traceback"].splitlines()
                if line.lstrip().startswith("File ")
            ]
            innermost = frames[-1]
            if "tests/tasks.py" in innermost:
                landed_in["task"] += 1
            elif innermost.endswith((" in invoke", " in call")):
                landed_in["call"] += 1
            else:
                raise AssertionError(innermost)
        assert landed_in["task"] > 0, landed_in


# -- async tasks ------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestAsyncTimeout:
    def test_coroutine_is_cancelled_and_recorded(self, task_state):
        worker = Worker(task_timeout=0.2, backoff_initial=60, poll_interval=0.05)
        result = async_spin.enqueue(5.0)
        started = time.monotonic()
        assert worker.run_once()
        assert time.monotonic() - started < 1.0
        assert task_state.get("cancelled") is True
        assert task_state["loop"].is_closed()

        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == 1
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH
        assert "0.2s timeout" in db_task.errors[-1]["traceback"]
        assert wait_for(lambda: not worker_threads(), timeout=5)

    def test_within_the_timeout_succeeds(self, task_state):
        worker = Worker(task_timeout=5, backoff_initial=0, poll_interval=0.05)
        result = async_spin.enqueue(0.05)
        assert worker.run_once()
        result.refresh()
        assert result.return_value == "done"
        assert "cancelled" not in task_state

    def test_the_coroutine_sees_cancelled_error_not_task_timeout(self, task_state):
        """
        asyncio delivers a cancellation, and nothing can raise a different
        class at a running coroutine's await. `except TaskTimeout` inside
        an async task never fires; the documentation says so.
        """
        worker = Worker(task_timeout=0.2, backoff_initial=60, poll_interval=0.05)
        result = async_catch_timeout.enqueue(5.0)
        assert worker.run_once()
        assert task_state.get("cancelled") is True
        db_task = row(result)
        assert db_task.status == OxTask.Status.READY
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH

    def test_the_stored_traceback_has_the_task_frames(self):
        """The record shows the await the coroutine was on, not just the worker."""
        worker = Worker(task_timeout=0.2, backoff_initial=60, poll_interval=0.05)
        result = async_spin.enqueue(5.0)
        assert worker.run_once()
        traceback = row(result).errors[-1]["traceback"]
        assert "in async_spin" in traceback, traceback
        assert "await asyncio.sleep(seconds)" in traceback, traceback
        assert "0.2s timeout" in traceback


# -- the grace backstop and the recycle -------------------------------------


@pytest.mark.django_db(transaction=True)
class TestBackstop:
    def test_stuck_thread_is_recorded_and_the_worker_recycles(
        self, caplog, interruptible_attempts
    ):
        """
        time.sleep() never returns to bytecode, so the injected TaskTimeout
        cannot land. After the grace the attempt is recorded as failed with
        a message that says so, its lease is fenced, the worker stops
        claiming, drains the other task, and run() returns with recycling
        set; the stuck thread is not waited for.
        """
        caplog.set_level(logging.INFO, logger="django_ox")
        worker = Worker(
            task_timeout=0.2,
            task_timeout_grace=0.3,
            backoff_initial=0,
            poll_interval=0.05,
            concurrency=2,
            lock_timeout=60,
        )
        stuck = slow.enqueue(2.5)
        OxTask.objects.filter(id=stuck.id).update(max_attempts=1)
        other = spin.using(queue_name="emails").enqueue(1.0)
        worker.timeouts.by_queue["emails"] = None
        third = add.enqueue(1, 2)

        thread = run_in_thread(worker)
        started = time.monotonic()
        thread.join(timeout=10)
        returned_after = time.monotonic() - started
        assert not thread.is_alive(), "run() did not return"
        assert worker.recycling
        # It returned before the stuck sleep ended: the drain skipped it.
        assert returned_after < 2.3, returned_after

        db_task = row(stuck)
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.errors[-1]["exception_class_path"] == TIMEOUT_PATH
        assert "did not stop" in db_task.errors[-1]["traceback"]
        assert "0.3s grace" in db_task.errors[-1]["traceback"]
        # The handover moved the epoch, so the thread's own write is fenced.
        assert db_task.lease_epoch == 2
        assert db_task.locked_by is None
        assert worker._in_flight == set()

        assert row(other).status == OxTask.Status.SUCCESSFUL, "drained normally"
        assert row(third).status == OxTask.Status.READY, "claimed nothing new"

        (stuck_event,) = events(caplog, "task_stuck")
        assert stuck_event.levelno == logging.ERROR
        assert stuck_event.task_id == str(stuck.id)
        assert stuck_event.grace_s == 0.3
        assert stuck_event.timeout_s == 0.2
        (recycling,) = events(caplog, "worker_recycling")
        assert recycling.levelno == logging.WARNING
        assert recycling.exit_code == RECYCLE_EXIT_CODE == 75
        assert events(caplog, "worker_stopped")

        # The thread finishes its sleep, tries to write, and is refused.
        assert wait_for(lambda: not worker_threads(), timeout=5)
        # The injection was pending all along and lands as the sleep
        # returns, so what the thread tries to record is its own timeout.
        (lost,) = events(caplog, "task_lease_lost")
        assert lost.task_id == str(stuck.id)
        assert lost.dropped_status == "FAILED"
        final = row(stuck)
        assert final.status == OxTask.Status.FAILED
        assert final.return_value is None

    def test_a_task_that_catches_it_and_runs_past_the_grace_is_stuck(
        self, caplog, task_state, interruptible_attempts
    ):
        """
        A task may catch TaskTimeout, but it has until the grace to return
        or raise. One that keeps going is indistinguishable from a thread
        the exception could not reach, and is treated the same way; the
        record says what was observed, not a cause the worker cannot see.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(
            task_timeout=0.2,
            task_timeout_grace=0.3,
            backoff_initial=0,
            poll_interval=0.05,
            lock_timeout=60,
        )
        result = swallow_then_run_on.enqueue(5.0, 1.5)
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

        thread = run_in_thread(worker)
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert worker.recycling
        assert task_state.get("caught") is True
        db_task = row(result)
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.lease_epoch == 2
        message = db_task.errors[-1]["traceback"]
        assert "did not stop within the 0.3s grace" in message
        assert "TaskTimeout was raised inside it" in message
        assert "blocked outside Python" not in message
        assert events(caplog, "task_stuck")
        assert wait_for(lambda: not worker_threads(), timeout=5)
        assert task_state.get("ran_on") is True, "the thread ran on to its end"
        assert events(caplog, "task_lease_lost")
        assert row(result).status == OxTask.Status.FAILED

    def test_backstop_alone_when_injection_is_unavailable(self, caplog, monkeypatch):
        """
        The PyPy shape: no PyThreadState_SetAsyncExc. A Python loop that
        would have been interrupted runs on until the grace, then the
        backstop takes over. The startup line says so once.
        """
        from django_ox import worker as worker_module

        caplog.set_level(logging.WARNING, logger="django_ox")
        monkeypatch.setattr(worker_module, "_inject_async_exc", None)
        worker = Worker(
            task_timeout=0.1,
            task_timeout_grace=0.2,
            backoff_initial=0,
            poll_interval=0.05,
            lock_timeout=60,
        )
        (startup,) = events(caplog, "timeouts_backstop_only")
        assert startup.grace_s == 0.2
        result = spin.enqueue(1.0)
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

        thread = run_in_thread(worker)
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert worker.recycling
        assert row(result).status == OxTask.Status.FAILED
        message = row(result).errors[-1]["traceback"]
        assert "did not stop within the 0.2s grace" in message
        # Nothing was raised inside the task; the record must not say it was.
        assert "cannot raise TaskTimeout inside a running task" in message
        assert "raised inside it" not in message
        assert "blocked outside Python" not in message
        assert events(caplog, "task_stuck")
        assert wait_for(lambda: not worker_threads(), timeout=5)

    def test_a_thread_that_came_back_before_the_grace_is_not_a_recycle(self, caplog):
        """
        The backstop's write is a compare-and-set. If the thread's own
        outcome landed first, the thread is not stuck, and the worker
        carries on.
        """
        from django_ox.worker import _Watch

        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(task_timeout=5, task_timeout_grace=1, backoff_initial=0)
        result = add.enqueue(1, 2)
        claimed = worker.claim_one()
        worker.execute(claimed)
        assert row(result).status == OxTask.Status.SUCCESSFUL

        # The watchdog's view of that attempt, firing after the outcome.
        now = time.monotonic()
        watch = _Watch(
            ident=threading.get_ident(),
            db_task=row(result),
            timeout=5,
            started=now - 6,
            deadline=now - 1,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
            grace_at=now,
        )
        watch.db_task.lease_epoch = claimed.lease_epoch
        worker._handle_stuck(watch)

        assert not worker.recycling
        assert worker._stuck == set()
        assert row(result).status == OxTask.Status.SUCCESSFUL
        assert row(result).return_value == 3
        assert events(caplog, "task_stuck"), "the watchdog did see the grace pass"
        (lost,) = events(caplog, "task_lease_lost")
        assert lost.dropped_status == "READY", "a retry, with attempts remaining"
        assert not events(caplog, "worker_recycling")


# -- under a coverage or tracing tool ---------------------------------------


def _null_trace(frame, event, arg):
    """
    A trace function that traces nothing: returning None for a call event
    leaves the frame untraced, which keeps this cheap enough to install
    for the length of a test.
    """


def tracer_seen_by_an_unwatched_thread(setup=None):
    """
    What _active_tracer() answers on a thread with no trace hook of its
    own. Under `pytest --cov` the calling thread has coverage's trace
    function installed, which would answer every one of these before the
    case under test was reached, and a thread started here inherits it, so
    the probe takes its own hook off first.
    """
    seen = []

    def probe():
        sys.settrace(None)
        if setup is not None:
            setup()
        seen.append(_active_tracer())

    thread = threading.Thread(target=probe, name="tracer-probe")
    thread.start()
    thread.join(timeout=10)
    assert not thread.is_alive()
    return seen[0]


class TestActiveTracer:
    @pytest.mark.skipif(
        bool(monitoring_tools()), reason="this session is measured through it"
    )
    def test_none_when_nothing_is_watching_the_thread(self):
        assert tracer_seen_by_an_unwatched_thread() is None

    def test_names_a_trace_function(self):
        seen = tracer_seen_by_an_unwatched_thread(
            setup=lambda: sys.settrace(_null_trace)
        )
        assert seen == "a trace function (sys.settrace)"

    def test_names_a_monitoring_tool(self):
        """
        A sys.monitoring tool watches a thread that has no trace hook at
        all, and coverage measurement uses it from Python 3.14 on. The
        debugger id is the lowest, so it is what the probe reports even in
        a session that is itself measured that way.
        """
        tool_id = sys.monitoring.DEBUGGER_ID
        if sys.monitoring.get_tool(tool_id) is not None:
            pytest.skip("something else holds the debugger tool id")
        sys.monitoring.use_tool_id(tool_id, "ox-test-tool")
        try:
            seen = tracer_seen_by_an_unwatched_thread()
        finally:
            sys.monitoring.free_tool_id(tool_id)
        assert seen == "a sys.monitoring tool (ox-test-tool)"


@pytest.mark.django_db(transaction=True)
class TestUnderATracingTool:
    def test_the_backstop_alone_while_a_trace_function_is_installed(
        self, caplog, task_state
    ):
        """
        A trace function on the worker's threads holds timeouts to the
        grace backstop: nothing is raised inside the task, the attempt is
        recorded as failed once the grace passes, and the worker recycles.
        The startup line says so once, for two attempts.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(
            task_timeout=0.1,
            task_timeout_grace=0.2,
            backoff_initial=0,
            poll_interval=0.05,
            lock_timeout=60,
            concurrency=2,
        )
        first = spin.enqueue(1.0)
        second = spin.enqueue(1.0)
        OxTask.objects.filter(id__in=[first.id, second.id]).update(max_attempts=1)

        installed = threading.gettrace()
        threading.settrace(_null_trace)
        try:
            thread = run_in_thread(worker)
            thread.join(timeout=15)
        finally:
            threading.settrace(installed)
        assert not thread.is_alive()

        assert worker.recycling
        assert row(first).status == OxTask.Status.FAILED
        message = row(first).errors[-1]["traceback"]
        assert "a tracing tool is active on this worker" in message
        assert "did not stop within the 0.2s grace" in message
        # Nothing was raised inside the task; the record must not say it was.
        assert "raised inside it" not in message
        assert task_state.get("caught") is not True
        # One line for the worker, not one per attempt.
        (degraded,) = events(caplog, "timeouts_backstop_only")
        assert degraded.reason == "tracing_tool"
        assert degraded.tracer == "a trace function (sys.settrace)"
        assert degraded.grace_s == 0.2
        assert events(caplog, "task_stuck")
        assert wait_for(lambda: not worker_threads(), timeout=10)

    def test_the_stuck_record_says_a_tool_was_active(self, caplog):
        """
        The record the backstop writes for an attempt that was left alone
        says so, and claims nothing was raised inside the task.
        """
        from django_ox.worker import _Watch

        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(task_timeout=5, task_timeout_grace=1, backoff_initial=0)
        result = add.enqueue(1, 2)
        claimed = worker.claim_one()
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

        now = time.monotonic()
        watch = _Watch(
            ident=threading.get_ident(),
            db_task=row(result),
            timeout=5,
            started=now - 6,
            deadline=now - 1,
            deadline_at=timezone.now(),
            injectable=False,
            traced=True,
            fired=True,
            grace_at=now,
        )
        watch.db_task.lease_epoch = claimed.lease_epoch
        worker._handle_stuck(watch)

        assert worker.recycling
        assert row(result).status == OxTask.Status.FAILED
        message = row(result).errors[-1]["traceback"]
        assert "a tracing tool is active on this worker" in message
        assert "did not stop within the 1s grace" in message
        assert "raised inside it" not in message
        assert "cannot raise TaskTimeout" not in message
        assert "its coroutine was cancelled" not in message

    def test_the_degraded_line_is_said_once_per_worker(self, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(task_timeout=5, task_timeout_grace=1, backoff_initial=0)
        worker._note_backstop_only("a trace function (sys.settrace)")
        worker._note_backstop_only("a sys.monitoring tool (coverage.py)")

        (degraded,) = events(caplog, "timeouts_backstop_only")
        assert degraded.reason == "tracing_tool"
        assert degraded.tracer == "a trace function (sys.settrace)"
        assert degraded.grace_s == 1

    def test_raised_inside_the_task_when_nothing_is_watching(
        self, caplog, task_state, interruptible_attempts
    ):
        """
        The other side of it: with no tool watching the attempt's thread
        the timeout is raised inside the task, and nothing is degraded.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(task_timeout=0.2, backoff_initial=60, poll_interval=0.05)
        result = spin.enqueue(2.0)
        started = time.monotonic()
        assert worker.run_once()

        assert time.monotonic() - started < 1.0
        assert task_state.get("caught") is True
        assert not events(caplog, "timeouts_backstop_only")
        error = row(result).errors[-1]
        assert error["exception_class_path"] == TIMEOUT_PATH
        assert "TaskTimeout was raised inside it" in error["traceback"]


# -- the supervisor ---------------------------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="--processes relies on POSIX signals")
class TestRecycleUnderTheSupervisor:
    def test_exit_75_restarts_the_slot_without_counting_it(self, caplog, monkeypatch):
        """
        A worker process that recycles exits 75. With the cap at zero any
        counted death would stop the supervisor; the recycle restarts the
        slot instead, and the new worker runs the next task.
        """
        from django.conf import settings

        caplog.set_level(logging.WARNING, logger="django_ox")
        monkeypatch.setenv("DJANGO_SETTINGS_MODULE", settings.SETTINGS_MODULE)
        monkeypatch.setenv("OX_TEST_DB_NAME", str(connection.settings_dict["NAME"]))
        monkeypatch.setenv(
            "OX_TEST_TASKS_OPTIONS",
            '{"TASK_TIMEOUT": 0.2, "TASK_TIMEOUT_GRACE": 0.3,'
            ' "TASK_TIMEOUTS": {"emails": null}}',
        )
        stuck = slow.enqueue(3.0)
        OxTask.objects.filter(id=stuck.id).update(max_attempts=1)
        other = spin.using(queue_name="emails").enqueue(1.0)

        supervisor = Supervisor(
            processes=1,
            worker_args=[
                "--interval",
                "0.05",
                "--concurrency",
                "2",
                "--verbosity",
                "0",
            ],
            restart_delay=0.05,
            restart_cap=0,
        )
        result: list[int] = []
        thread = threading.Thread(target=lambda: result.append(supervisor.run()))
        thread.start()
        try:
            assert wait_for(lambda: supervisor._children.get(0) is not None, timeout=20)
            first_pid = supervisor._children[0].pid
            assert wait_for(
                lambda: row(stuck).status == OxTask.Status.FAILED, timeout=20
            )
            assert wait_for(
                lambda: (
                    supervisor._children.get(0) is not None
                    and supervisor._children[0].pid != first_pid
                ),
                timeout=20,
            )
            assert thread.is_alive()
            assert not supervisor.stopping
            # The restarted slot is a working worker.
            after = add.enqueue(2, 3)
            assert wait_for(
                lambda: row(after).status == OxTask.Status.SUCCESSFUL, timeout=20
            )
        finally:
            supervisor.request_stop()
            supervisor._signal_children(signal.SIGTERM)
            thread.join(timeout=20)
        assert result == [0]

        assert row(other).status == OxTask.Status.SUCCESSFUL
        assert "did not stop" in row(stuck).errors[-1]["traceback"]
        (recycled,) = events(caplog, "worker_process_recycled")
        assert recycled.exit_code == RECYCLE_EXIT_CODE
        assert recycled.worker_index == 0
        assert not events(caplog, "supervisor_restart_cap")
        assert not events(caplog, "worker_process_restarted")

    def test_a_recycle_during_a_stop_is_not_a_failure(self):
        supervisor = Supervisor(processes=1, worker_args=[])
        supervisor._exit_codes = {0: RECYCLE_EXIT_CODE}
        supervisor.request_stop()
        assert supervisor.run() == 0


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="signals")
def test_the_command_exits_75_after_a_recycle(tmp_path):
    """The real exit code, through manage.py, as a supervisor or systemd sees it."""
    from django.conf import settings

    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    env["OX_TEST_TASKS_OPTIONS"] = '{"TASK_TIMEOUT": 0.2, "TASK_TIMEOUT_GRACE": 0.3}'
    stuck = slow.enqueue(3.0)
    OxTask.objects.filter(id=stuck.id).update(max_attempts=1)

    import subprocess

    log = (tmp_path / "worker.log").open("wb")
    proc = subprocess.Popen(
        [sys.executable, "-m", "django", "ox_worker", "--interval", "0.05"],
        env=env,
        stderr=log,
        stdout=log,
    )
    try:
        code = proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
    text = (tmp_path / "worker.log").read_text()
    assert code == RECYCLE_EXIT_CODE, text
    assert "did not stop" in text
    assert "recycling" in text
    assert "stopped" in text
    assert row(stuck).status == OxTask.Status.FAILED
