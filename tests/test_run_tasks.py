"""
django_ox.testing.run_tasks(): the configured worker's claim and execution,
on the test's own thread and connections.

Grouped as the behaviour is: what it sees, what it claims, how far it goes,
what it records, what a task body's transaction does, commit callbacks,
what it leaves out, timeouts, and what it refuses. The TestCase and
TransactionTestCase classes at the end run the transaction-sensitive parts
under Django's own test classes as well as under pytest-django's.
"""

import logging
import re
import threading
from datetime import timedelta
from unittest import mock

import pytest
from django.contrib.auth.models import Group
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    Error,
    IntegrityError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.transaction import TransactionManagementError
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from django_ox import _run_tasks, testing
from django_ox.compat import TaskResultStatus, task_finished, task_started
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.testing import run_tasks
from django_ox.worker import Worker

from . import run_tasks_tasks as t
from .tasks import STATE

pytestmark = pytest.mark.django_db

HERE = "tests.run_tasks_tasks"


def ox_tasks(**options):
    """A TASKS setting with one OxBackend and these OPTIONS on top."""
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, **options},
        }
    }


def frames(stored_traceback):
    """The function names of a stored traceback, outermost first."""
    return re.findall(r'File "[^"]+", line \d+, in (\S+)', stored_traceback)


def frame_source(stored_traceback, function):
    """
    The source lines a stored traceback shows under `function`'s frame,
    stripped, without the marker lines Python draws under them.
    """
    lines = stored_traceback.splitlines()
    heading = re.compile(rf'\s*File "[^"]+", line \d+, in {function}')
    starts = [n for n, line in enumerate(lines) if heading.fullmatch(line)]
    assert starts, f"no {function} frame in {stored_traceback}"
    shown = []
    for line in lines[starts[0] + 1 :]:
        if not line.startswith("    ") or line.lstrip().startswith("File "):
            break
        if set(line.strip()) <= set("~^"):
            continue
        shown.append(line.strip())
    return shown


def callback_functions(alias="default"):
    """The functions pending in a connection's run_on_commit, in order."""
    return [func for _sids, func, _robust in connections[alias].run_on_commit]


def status_of(result):
    return OxTask.objects.get(id=result.id).status


def events(caplog, event):
    return [r for r in caplog.records if getattr(r, "event", None) == event]


# -- what it sees ----------------------------------------------------------


class TestEnqueueVisibility:
    def test_a_rolled_back_enqueue_never_runs(self):
        with pytest.raises(ValueError, match="roll back"), transaction.atomic():
            t.note.enqueue("rolled back")
            raise ValueError("roll back")
        assert run_tasks() == []
        assert "ran" not in STATE

    def test_an_enqueue_that_survives_its_block_runs(self):
        with transaction.atomic():
            t.note.enqueue("kept")
        [result] = run_tasks()
        assert result.return_value == "kept"
        assert STATE["ran"] == ["kept"]

    def test_the_tests_uncommitted_rows_are_what_the_task_sees(self):
        Group.objects.create(name="seed")
        t.seen.enqueue("seed")
        [result] = run_tasks()
        assert result.return_value is True

    def test_it_runs_on_the_callers_thread_and_connection(self):
        driver = connection.connection
        t.where_am_i.enqueue()
        run_tasks()
        assert STATE["thread"] == threading.get_ident()
        assert STATE["driver"] == id(driver)
        assert STATE["in_atomic_block"] is True
        assert connection.connection is driver
        assert connection.in_atomic_block


# -- what it claims --------------------------------------------------------


class TestSelection:
    def test_only_the_named_backends_queues(self, settings):
        settings.TASKS = {
            **ox_tasks(),
            "other": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["other"],
                "OPTIONS": {},
            },
        }
        t.note.enqueue("default")
        t.note.using(backend="other", queue_name="other").enqueue("other")
        assert [r.return_value for r in run_tasks(backend="other")] == ["other"]
        assert [r.return_value for r in run_tasks()] == ["default"]

    def test_an_empty_list_of_queues_is_the_workers_default(self):
        t.note.enqueue("default")
        t.note_email.enqueue("email")
        assert [r.return_value for r in run_tasks(queues=[])] == ["default", "email"]

    def test_a_queue_nothing_is_on_claims_nothing(self):
        result = t.note.enqueue("default")
        assert run_tasks(queues=["nowhere"]) == []
        assert status_of(result) == OxTask.Status.READY

    def test_queues_narrow_the_claim_as_they_do_for_a_worker(self):
        t.note.enqueue("default")
        t.note_email.enqueue("email")
        assert [r.return_value for r in run_tasks(queues=["emails"])] == ["email"]
        assert [r.return_value for r in run_tasks()] == ["default"]

    def test_a_row_due_now_runs_and_one_due_a_microsecond_later_waits(self):
        now = timezone.now()
        t.note.using(run_after=now).enqueue("due")
        t.note.using(run_after=now + timedelta(microseconds=1)).enqueue("later")
        with mock.patch("django.utils.timezone.now", return_value=now):
            assert [r.return_value for r in run_tasks()] == ["due"]
        assert sorted(OxTask.objects.values_list("status", flat=True)) == [
            OxTask.Status.READY,
            OxTask.Status.SUCCESSFUL,
        ]

    def test_a_future_row_waits_until_time_moves(self):
        later = timezone.now() + timedelta(hours=1)
        t.note.using(run_after=later).enqueue("future")
        assert run_tasks() == []
        with mock.patch(
            "django.utils.timezone.now", return_value=later + timedelta(seconds=1)
        ):
            assert [r.return_value for r in run_tasks()] == ["future"]

    @pytest.mark.parametrize(
        "status",
        [
            OxTask.Status.WAITING,
            OxTask.Status.RUNNING,
            OxTask.Status.SUCCESSFUL,
            OxTask.Status.FAILED,
            OxTask.Status.LOST,
            OxTask.Status.DISCARDED,
        ],
    )
    def test_rows_a_worker_would_not_claim_are_left_alone(self, status):
        result = t.note.enqueue("never")
        OxTask.objects.filter(id=result.id).update(status=status)
        assert run_tasks() == []
        assert status_of(result) == status

    def test_the_configured_worker_class_claims(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.CountingWorker")
        t.note.enqueue("one")
        t.note.enqueue("two")
        results = run_tasks()
        assert [r.return_value for r in results] == ["one", "two"]
        # Two claims that found work and the one that found none.
        assert STATE["claims"] == 3

    def test_the_drain_class_keeps_the_configured_class_and_its_name(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.CountingWorker")
        seen = []
        real = t.CountingWorker.claim_one

        def claim_one(self):
            seen.append(type(self))
            return real(self)

        with mock.patch.object(t.CountingWorker, "claim_one", claim_one):
            run_tasks()
        [cls] = seen
        assert issubclass(cls, t.CountingWorker)
        assert cls.__name__ == "CountingWorker"
        assert cls.__mro__.index(t.CountingWorker) < cls.__mro__.index(Worker)


# -- how far it goes -------------------------------------------------------


class TestRecursionAndBounds:
    def test_a_child_the_task_enqueues_runs_in_the_same_call(self):
        t.enqueue_child.enqueue("parent")
        results = run_tasks()
        assert [r.return_value for r in results] == ["parent", "parent-child"]

    def test_a_child_enqueued_from_a_commit_callback_runs_in_the_same_call(self):
        t.enqueue_child_on_commit.enqueue("parent")
        results = run_tasks()
        assert [r.return_value for r in results] == ["parent", "parent-child"]

    def test_zero_claims_nothing(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.CountingWorker")
        result = t.note.enqueue("left")
        assert run_tasks(max_tasks=0) == []
        assert "claims" not in STATE
        assert status_of(result) == OxTask.Status.READY

    def test_an_explicit_max_tasks_steps_quietly(self):
        t.chain.enqueue(1, 3)
        assert [r.return_value for r in run_tasks(max_tasks=1)] == [1]
        assert [r.return_value for r in run_tasks(max_tasks=1)] == [2]
        assert [r.return_value for r in run_tasks(max_tasks=5)] == [3]
        assert run_tasks(max_tasks=1) == []

    def test_an_explicit_max_tasks_is_the_bound_even_above_the_ceiling(
        self, monkeypatch
    ):
        monkeypatch.setattr(_run_tasks, "SAFETY_LIMIT", 3)
        t.loop_forever.enqueue(0)
        results = run_tasks(max_tasks=5)
        assert [r.return_value for r in results] == [0, 1, 2, 3, 4]

    def test_a_self_enqueueing_task_stops_at_the_safety_limit(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.CountingWorker")
        t.loop_forever.enqueue(0)
        with pytest.raises(RuntimeError) as raised:
            run_tasks()
        assert str(raised.value) == (
            "run_tasks() reached its safety limit of 1000 attempts; due READY "
            f"work remains (for example: '{HERE}.loop_forever'). Remaining work "
            "may be gated or rate-limited. Use max_tasks=N for bounded stepping."
        )
        # The limit's own claims, and not one more to look at what is left.
        assert STATE["claims"] == 1000
        assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 1000
        assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 1

    def test_ending_exactly_at_the_limit_with_nothing_left_returns(
        self, settings, monkeypatch
    ):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.CountingWorker")
        monkeypatch.setattr(_run_tasks, "SAFETY_LIMIT", 5)
        t.chain.enqueue(1, 5)
        results = run_tasks()
        assert [r.return_value for r in results] == [1, 2, 3, 4, 5]
        assert STATE["claims"] == 5

    def test_a_gated_candidate_at_the_limit_raises_conservatively(
        self, settings, monkeypatch
    ):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.GatingWorker")
        monkeypatch.setattr(_run_tasks, "SAFETY_LIMIT", 3)
        t.gated.enqueue("held")
        t.chain.enqueue(1, 3)
        with pytest.raises(RuntimeError, match=re.escape(f"'{HERE}.gated'")):
            run_tasks()
        assert STATE["claims"] == 3
        assert "held" not in STATE["ran"]

    def test_a_gated_row_below_the_limit_returns_quietly(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.GatingWorker")
        result = t.gated.enqueue("held")
        assert run_tasks() == []
        assert status_of(result) == OxTask.Status.READY


# -- what it records -------------------------------------------------------


class TestOutcomes:
    def test_a_success_is_recorded_and_returned(self):
        enqueued = t.note.enqueue("done")
        [result] = run_tasks()
        assert result.id == enqueued.id
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == "done"
        assert status_of(result) == OxTask.Status.SUCCESSFUL
        enqueued.refresh()
        assert enqueued.status == TaskResultStatus.SUCCESSFUL

    def test_a_terminal_failure_is_recorded_not_raised(self, settings):
        settings.TASKS = ox_tasks(MAX_ATTEMPTS=1)
        t.fail_always.enqueue()
        [result] = run_tasks()
        assert result.status == TaskResultStatus.FAILED
        assert [e.exception_class_path for e in result.errors] == [
            "builtins.ValueError"
        ]
        assert status_of(result) == OxTask.Status.FAILED

    def test_a_retry_is_scheduled_with_the_worker_backoff_and_left(self):
        before = timezone.now()
        t.flaky.enqueue(succeed_on=3)
        [result] = run_tasks()
        assert result.status == TaskResultStatus.READY
        row = OxTask.objects.get(id=result.id)
        assert row.attempts == 1
        assert timedelta(seconds=4) < row.run_after - before < timedelta(seconds=10)
        assert run_tasks() == []

    def test_advancing_time_between_drains_runs_each_retry(self):
        start = timezone.now()
        t.flaky.enqueue(succeed_on=3)
        statuses = []
        for hours in (0, 1, 2):
            at = start + timedelta(hours=hours)
            with mock.patch("django.utils.timezone.now", return_value=at):
                statuses += [r.status for r in run_tasks()]
        assert statuses == [
            TaskResultStatus.READY,
            TaskResultStatus.READY,
            TaskResultStatus.SUCCESSFUL,
        ]

    def test_time_frozen_later_still_runs_one_attempt_per_call(self):
        # A retry is written for now + its delay, and "now" is the frozen
        # instant, so it is never due at that instant.
        at = timezone.now() + timedelta(hours=1)
        t.flaky.enqueue(succeed_on=3)
        with mock.patch("django.utils.timezone.now", return_value=at):
            assert len(run_tasks()) == 1
            assert run_tasks() == []

    def test_a_backoff_of_zero_retries_in_the_same_call_with_a_snapshot_each(self):
        t.flaky_retry_now.enqueue(succeed_on=3)
        results = run_tasks()
        assert [r.status for r in results] == [
            TaskResultStatus.READY,
            TaskResultStatus.READY,
            TaskResultStatus.SUCCESSFUL,
        ]
        assert [len(r.errors) for r in results] == [1, 2, 2]
        assert [len(r.worker_ids) for r in results] == [1, 2, 3]
        assert len({id(r) for r in results}) == 3
        assert results[2].return_value == 3

    def test_the_tasks_own_backoff_decides_the_delay(self):
        before = timezone.now()
        t.fail_then_wait_a_minute.enqueue()
        [result] = run_tasks()
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert timedelta(seconds=59) < row.run_after - before < timedelta(seconds=65)

    def test_the_lifecycle_signals_fire_as_on_a_worker(self):
        seen = []

        def started(sender, task_result, **kwargs):
            seen.append(("started", task_result.status))

        def finished(sender, task_result, **kwargs):
            seen.append(("finished", task_result.status))

        task_started.connect(started)
        task_finished.connect(finished)
        try:
            t.note.enqueue("signalled")
            run_tasks()
        finally:
            task_started.disconnect(started)
            task_finished.disconnect(finished)
        assert seen == [
            ("started", TaskResultStatus.RUNNING),
            ("finished", TaskResultStatus.SUCCESSFUL),
        ]

    def test_raise_failures_raises_the_original_exception_after_recording_it(self):
        t.make_then_raise.enqueue("recorded")
        t.note.enqueue("not reached")
        with pytest.raises(ValueError) as raised:
            run_tasks(raise_failures=True)
        assert raised.value is STATE["raised"]
        row = OxTask.objects.get(task_path=f"{HERE}.make_then_raise")
        # A retry was scheduled, and the call still raised and stopped.
        assert row.status == OxTask.Status.READY
        assert len(row.errors) == 1
        assert "ran" not in STATE

    def test_raise_failures_covers_a_return_value_that_cannot_be_stored(self):
        t.unserializable.enqueue()
        with pytest.raises(TypeError):
            run_tasks(raise_failures=True)
        row = OxTask.objects.get()
        assert row.status == OxTask.Status.READY
        assert row.errors[0]["exception_class_path"] == "builtins.TypeError"

    def test_a_task_that_no_longer_imports_is_recorded_then_raised(self):
        result = t.note.enqueue("moved")
        OxTask.objects.filter(id=result.id).update(task_path=f"{HERE}.gone")
        with pytest.raises(ImportError):
            run_tasks()
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert row.attempts == 1
        assert "ImportError" in row.errors[0]["exception_class_path"]

    def test_without_raise_failures_a_failure_is_only_recorded(self):
        t.make_then_raise.enqueue("recorded")
        t.note.enqueue("reached")
        results = run_tasks()
        assert [r.status for r in results] == [
            TaskResultStatus.READY,
            TaskResultStatus.SUCCESSFUL,
        ]


# -- a task body's transaction ----------------------------------------------


class TestTheBodysTransaction:
    def test_a_plain_exception_keeps_the_bodys_writes(self):
        t.make_then_raise.enqueue("kept")
        [result] = run_tasks()
        assert result.errors[0].exception_class_path == "builtins.ValueError"
        assert Group.objects.filter(name="kept").exists()
        # The caller's transaction goes on.
        Group.objects.create(name="after")
        assert connection.in_atomic_block
        assert not connection.needs_rollback

    def test_an_orm_database_error_rolls_the_attempts_writes_back(self):
        t.make_then_integrity_error.enqueue("gone")
        [result] = run_tasks()
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        assert not Group.objects.filter(name__startswith="gone").exists()
        # Recorded outside the savepoint, so the record is there to read.
        assert status_of(result) == OxTask.Status.READY
        assert not connection.needs_rollback
        Group.objects.create(name="after")

    def test_a_raw_sql_error_is_recovered_through_the_savepoint(self):
        t.make_then_raw_error.enqueue("raw")
        [result] = run_tasks()
        assert STATE["needs_rollback"] is False
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        assert status_of(result) == OxTask.Status.READY
        # PostgreSQL aborts the transaction on any error, so the attempt's
        # writes go with the savepoint. SQLite and MySQL undo the failed
        # statement alone, the connection goes on, and the writes are kept.
        kept = Group.objects.filter(name="raw").exists()
        assert kept is (connection.vendor != "postgresql")
        Group.objects.create(name="after")

    def test_a_body_that_returns_with_its_transaction_aborted_is_no_success(self):
        t.make_then_swallow_raw_error.enqueue("swallowed")
        [result] = run_tasks()
        assert STATE["swallowed"] is True
        if connection.vendor == "postgresql":
            assert result.status == TaskResultStatus.READY
            assert result.errors[0].exception_class_path == (
                "django.db.utils.InternalError"
            )
            assert not Group.objects.filter(name="swallowed").exists()
        else:
            assert result.status == TaskResultStatus.SUCCESSFUL
            assert Group.objects.filter(name="swallowed").exists()
        Group.objects.create(name="after")

    def test_a_body_that_returns_after_catching_an_orm_error_is_no_success(self):
        t.make_then_swallow_orm_error.enqueue("caught")
        [result] = run_tasks()
        assert STATE["swallowed"] is True
        assert result.status == TaskResultStatus.READY
        [error] = result.errors
        assert error.exception_class_path == (
            "django.db.transaction.TransactionManagementError"
        )
        assert (
            f"Task {HERE}.make_then_swallow_orm_error left the transaction on "
            "database 'default' marked for rollback"
        ) in error.traceback
        assert not Group.objects.filter(name="caught").exists()
        Group.objects.create(name="after")

    @pytest.mark.parametrize("aimed", [KeyboardInterrupt, SystemExit])
    def test_an_interrupt_reaches_the_caller_with_the_attempt_undone(self, aimed):
        pending = list(connection.run_on_commit)
        result = t.interrupts.enqueue("interrupted", aimed.__name__)
        with pytest.raises(aimed):
            run_tasks()
        assert not Group.objects.filter(name="interrupted").exists()
        assert "callbacks" not in STATE
        assert connection.run_on_commit == pending
        # Not recorded: aimed at the caller, as with run_once().
        assert status_of(result) == OxTask.Status.RUNNING
        assert connection.in_atomic_block
        Group.objects.create(name="after")

    def test_success_and_the_bodys_own_savepoints_behave_normally(self):
        t.nested_savepoints.enqueue("own")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == ["own-a", "own-d"]
        assert STATE["inner_rolled_back"] is True
        assert sorted(Group.objects.values_list("name", flat=True)) == [
            "own-a",
            "own-d",
        ]

    def test_a_durable_block_runs_where_it_would_without_run_tasks(self):
        t.durable_write.enqueue("durable")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert Group.objects.filter(name="durable").exists()

    def test_a_durable_block_is_refused_where_it_would_be_without_run_tasks(self):
        with transaction.atomic():
            with pytest.raises(RuntimeError, match="durable") as direct:
                t.durable_write.call("direct")
            t.durable_write.enqueue("drained")
            [result] = run_tasks()
        assert result.errors[0].exception_class_path == "builtins.RuntimeError"
        assert str(direct.value) in result.errors[0].traceback

    @pytest.mark.django_db(transaction=True)
    def test_an_unrecoverable_transaction_is_neither_cleared_nor_a_success(self):
        result = t.closes_its_connection.enqueue()
        # The outcome write is the first statement after the close, and what
        # it raises depends on the driver: nothing here catches it.
        with pytest.raises(Error), transaction.atomic():
            try:
                run_tasks()
            finally:
                # Still marked for rollback: nothing papered over it.
                assert connection.needs_rollback
                assert connection.closed_in_transaction
        # The claim and the task's write went with the transaction.
        assert status_of(result) == OxTask.Status.READY
        assert not Group.objects.filter(name="before-close").exists()

    def test_the_original_exception_and_its_stored_traceback_survive(self):
        t.make_then_raise.enqueue("identity")
        with pytest.raises(ValueError) as raised:
            run_tasks(raise_failures=True)
        assert raised.value is STATE["raised"]
        stored = OxTask.objects.get().errors[0]["traceback"]
        assert frames(stored) == ["_run_attempt", "call", "make_then_raise"]
        assert "_task_body" not in stored
        assert "_run_tasks" not in stored

    def test_a_broken_transaction_is_refused_before_any_claim(self):
        result = t.note.enqueue("never")
        Group.objects.create(name="dup")
        with transaction.atomic():
            with pytest.raises(IntegrityError):
                Group.objects.create(name="dup")
            assert connection.needs_rollback
            with pytest.raises(TransactionManagementError) as raised:
                run_tasks()
        assert str(raised.value) == (
            "run_tasks() cannot run while the transaction on database 'default' "
            "is broken: an earlier error marked it for rollback. No query can "
            "run on it until the atomic block that holds it ends."
        )
        assert status_of(result) == OxTask.Status.READY

    def test_a_callback_that_breaks_the_transaction_is_a_recorded_failure(self):
        t.breaks_the_transaction_in_a_callback.enqueue("twice")
        with transaction.atomic():
            [result] = run_tasks()
            # The callback's own savepoint took the error.
            assert not connection.needs_rollback
            Group.objects.create(name="after")
        assert result.status == TaskResultStatus.READY
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )


# The source line a stored traceback shows for the _run_attempt frame, as
# the worker stored it before the seam, for an attempt with no timeout.
CALL_LINE = "raw_return_value = task.call(*db_task.args, **db_task.kwargs)"


class TestStoredTracebackOfAWorker:
    def test_a_worker_attempt_stores_the_same_frames_as_before_the_seam(self):
        t.fail_always.enqueue()
        assert Worker(backoff_initial=0).run_once() is True
        stored = OxTask.objects.get().errors[0]["traceback"]
        assert frames(stored) == ["_run_attempt", "call", "fail_always"]
        assert frame_source(stored, "_run_attempt") == [CALL_LINE]
        assert "contextlib" not in stored
        assert "_task_body" not in stored

    def test_the_seam_changes_no_frame_and_no_exception(self):
        t.make_then_raise.enqueue("worker")
        caught = []
        real = Worker._handle_failure

        def handle_failure(self, db_task, exc, duration_ms, **kwargs):
            caught.append(exc)
            return real(self, db_task, exc, duration_ms, **kwargs)

        with mock.patch.object(Worker, "_handle_failure", handle_failure):
            Worker(backoff_initial=0).run_once()
        [exc] = caught
        assert exc is STATE["raised"]
        names = []
        tb = exc.__traceback__
        while tb is not None:
            names.append(tb.tb_frame.f_code.co_name)
            tb = tb.tb_next
        assert names == ["_run_attempt", "call", "make_then_raise"]


# -- commit callbacks ------------------------------------------------------


class TestCommitCallbacks:
    def test_a_body_callback_runs_before_the_outcome_and_is_taken_off(self):
        order = []

        def finished(sender, task_result, **kwargs):
            order.append(("finished", list(STATE.get("callbacks", []))))

        task_finished.connect(finished)
        try:
            pending = len(connection.run_on_commit)
            t.register.enqueue("body")
            run_tasks()
        finally:
            task_finished.disconnect(finished)
        assert STATE["callbacks"] == ["body"]
        assert order == [("finished", ["body"])]
        assert len(connection.run_on_commit) == pending

    def test_the_callers_own_callbacks_are_left_alone(self):
        mine = []
        transaction.on_commit(lambda: mine.append("caller"))
        before = list(connection.run_on_commit)
        t.register.enqueue("task")
        run_tasks()
        assert mine == []
        assert connection.run_on_commit == before
        assert STATE["callbacks"] == ["task"]

    def test_a_callback_on_rolled_back_work_does_not_run(self):
        t.register_in_rolled_back_block.enqueue("block")
        run_tasks()
        assert STATE["callbacks"] == ["block-kept"]

    def test_a_plain_body_exception_keeps_its_callbacks(self):
        t.register_then_raise.enqueue("kept")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.READY
        assert STATE["callbacks"] == ["kept"]

    def test_a_database_error_drops_its_callbacks_with_its_writes(self):
        t.register_then_integrity_error.enqueue("dropped")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.READY
        assert "callbacks" not in STATE

    def test_callbacks_run_in_the_order_they_were_registered(self):
        t.register_order.enqueue("n")
        run_tasks()
        assert STATE["callbacks"] == ["n-0", "n-1", "n-2"]

    def test_a_callback_registered_by_a_callback_runs_after_the_rest(self):
        t.register_chain.enqueue("c")
        run_tasks()
        assert STATE["callbacks"] == ["c-first", "c-third", "c-second"]

    def test_a_robust_callback_error_is_logged_and_the_attempt_succeeds(self, caplog):
        t.register.enqueue("robust", robust=True, fail=True)
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        [record] = events(caplog, "run_tasks_callback_failed")
        assert "callback_robust" in record.getMessage()
        assert "a robust callback's error does not fail the attempt" in (
            record.getMessage()
        )

    def test_a_non_robust_callback_error_fails_a_successful_attempt(self):
        t.register.enqueue("fragile", fail=True)
        [result] = run_tasks()
        assert result.status == TaskResultStatus.READY
        [error] = result.errors
        assert error.exception_class_path == "builtins.RuntimeError"
        assert "callback fragile failed" in error.traceback

    def test_raise_failures_raises_the_callbacks_error(self):
        t.register.enqueue("fragile", fail=True)
        with pytest.raises(RuntimeError, match="callback fragile failed"):
            run_tasks(raise_failures=True)

    def test_after_a_failed_body_the_callback_error_is_reported_separately(
        self, caplog
    ):
        t.register_then_raise.enqueue("both", fail=True)
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            [result] = run_tasks()
        [error] = result.errors
        assert error.exception_class_path == "builtins.ValueError"
        [record] = events(caplog, "run_tasks_callback_failed")
        assert "callback both failed" in record.getMessage()
        assert record.exc_info[1].args == ("callback both failed",)

    def test_an_outcome_callback_runs_after_execute(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.OutcomeCallbackWorker")
        t.register.enqueue("body")
        run_tasks()
        assert STATE["callbacks"] == ["body", "outcome-register-SUCCESSFUL"]

    def test_a_failing_outcome_callback_is_raised_and_the_outcome_stands(
        self, settings
    ):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.OutcomeCallbackWorker")
        STATE["outcome_callback_fails"] = True
        result = t.note.enqueue("recorded")
        t.note.enqueue("not reached")
        with pytest.raises(RuntimeError, match="outcome-note-SUCCESSFUL failed"):
            run_tasks()
        assert status_of(result) == OxTask.Status.SUCCESSFUL
        assert STATE["ran"] == ["recorded"]

    def test_a_robust_outcome_callback_error_is_logged(self, settings, caplog):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.OutcomeCallbackWorker")
        STATE["outcome_callback_fails"] = True
        STATE["outcome_callback_robust"] = True
        t.note.enqueue("recorded")
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert len(events(caplog, "run_tasks_callback_failed")) == 1

    def test_a_robust_callbacks_database_error_is_logged_and_the_rest_go_on(
        self, caplog
    ):
        t.writes_then_registers_a_breaking_callback.enqueue("robust", robust=True)
        t.note.enqueue("next")
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            results = run_tasks()
        assert [r.status for r in results] == [TaskResultStatus.SUCCESSFUL] * 2
        [record] = events(caplog, "run_tasks_callback_failed")
        assert isinstance(record.exc_info[1], IntegrityError)
        # The callback after it ran, and so did the next task.
        assert STATE["callbacks"] == ["robust-after"]
        assert STATE["ran"] == ["next"]
        assert Group.objects.filter(name="robust-body").exists()
        assert not Group.objects.filter(name="robust-callback").exists()
        assert not connection.needs_rollback
        Group.objects.create(name="after")

    def test_a_callbacks_database_error_is_recorded_and_the_drain_goes_on(self):
        t.writes_then_registers_a_breaking_callback.enqueue("fragile")
        t.note.enqueue("next")
        results = run_tasks()
        assert [r.status for r in results] == [
            TaskResultStatus.READY,
            TaskResultStatus.SUCCESSFUL,
        ]
        [error] = results[0].errors
        assert error.exception_class_path == "django.db.utils.IntegrityError"
        assert status_of(results[0]) == OxTask.Status.READY
        # The callbacks waiting behind it are dropped, as a commit drops them.
        assert "callbacks" not in STATE
        assert STATE["ran"] == ["next"]
        assert Group.objects.filter(name="fragile-body").exists()
        assert not Group.objects.filter(name="fragile-callback").exists()
        assert not connection.needs_rollback
        Group.objects.create(name="after")

    def test_a_callback_that_returns_after_catching_a_database_error_fails(self):
        t.registers_a_callback_that_swallows_an_error.enqueue("caught")
        [result] = run_tasks()
        assert STATE["swallowed"] is True
        assert result.status == TaskResultStatus.READY
        [error] = result.errors
        assert error.exception_class_path == (
            "django.db.transaction.TransactionManagementError"
        )
        assert (
            f"A commit callback of task {HERE}.registers_a_callback_that_swallows_"
            "an_error, registers_a_callback_that_swallows_an_error.<locals>."
            "swallows, left the transaction on database 'default' marked for "
            "rollback"
        ) in error.traceback
        assert not Group.objects.filter(name="caught-callback").exists()
        assert not connection.needs_rollback

    def test_a_failing_callbacks_writes_are_rolled_back(self, caplog):
        t.registers_a_callback_that_writes_then_raises.enqueue("gone")
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert len(events(caplog, "run_tasks_callback_failed")) == 1
        assert not Group.objects.filter(name="gone-callback").exists()
        assert STATE["callbacks"] == ["gone-after"]

    @pytest.mark.parametrize("aimed", [KeyboardInterrupt, SystemExit])
    def test_an_interrupt_from_a_callback_keeps_the_bodys_writes(self, aimed):
        before = callback_functions()
        result = t.registers_a_callback_that_interrupts.enqueue("cb", aimed.__name__)
        with pytest.raises(aimed):
            run_tasks()
        # The body's savepoint was released before the callbacks ran; only
        # the callback's own is rolled back.
        assert Group.objects.filter(name="cb-body").exists()
        assert not Group.objects.filter(name="cb-callback").exists()
        assert "callbacks" not in STATE
        assert callback_functions() == before
        assert status_of(result) == OxTask.Status.RUNNING
        assert not connection.needs_rollback

    def test_a_durable_block_in_a_callback_runs_where_it_would_without_run_tasks(
        self,
    ):
        t.registers_a_durable_callback.enqueue("durable-callback")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert Group.objects.filter(name="durable-callback").exists()


# A savepoint rolled back during an attempt rebuilds run_on_commit, and the
# caller's callbacks must come through each way it happens. Each case is the
# task, its keyword arguments, whether the test first creates the group the
# task will collide with, and the callbacks the task's own attempt runs.
ROLLBACK_PATHS = [
    pytest.param(t.rolls_back_its_own_block, {}, False, ["cb"], id="own-block"),
    pytest.param(
        t.sets_rollback_in_its_own_block, {}, False, ["cb"], id="set-rollback"
    ),
    pytest.param(
        t.inserts_a_marker_once, {}, True, ["cb-done-already"], id="marker-insert"
    ),
    pytest.param(t.register_then_integrity_error, {}, False, None, id="orm-error"),
    pytest.param(
        t.register, {"robust": True, "fail": True}, False, ["cb"], id="callback-raises"
    ),
]


class TestTheCallersCallbacks:
    @pytest.mark.parametrize(("task", "kwargs", "seed", "ran"), ROLLBACK_PATHS)
    def test_a_savepoint_rollback_in_the_attempt_leaves_them_alone(
        self, task, kwargs, seed, ran
    ):
        mine = []
        transaction.on_commit(lambda: mine.append("caller"))
        before = callback_functions()
        if seed:
            Group.objects.create(name="cb")
        task.enqueue("cb", **kwargs)
        run_tasks()
        assert mine == []
        assert callback_functions() == before
        assert STATE.get("callbacks") == ran
        assert not connection.needs_rollback
        Group.objects.create(name="after")

    @pytest.mark.parametrize(("task", "kwargs", "seed", "ran"), ROLLBACK_PATHS)
    def test_an_outer_capture_gets_them_exactly_once(
        self, django_capture_on_commit_callbacks, task, kwargs, seed, ran
    ):
        mine = []

        def caller():
            mine.append("caller")

        with django_capture_on_commit_callbacks(execute=True) as captured:
            transaction.on_commit(caller)
            if seed:
                Group.objects.create(name="cb")
            task.enqueue("cb", **kwargs)
            run_tasks()
            assert mine == []
        assert captured == [caller]
        assert mine == ["caller"]
        assert STATE.get("callbacks") == ran

    def test_the_same_function_registered_by_the_caller_and_the_task(self):
        transaction.on_commit(t.shared_callback)
        before = callback_functions()
        t.register_the_shared_callback.enqueue("shared")
        run_tasks()
        # Both of the task's registrations ran; the caller's is still there.
        assert STATE["shared"] == ["ran", "ran"]
        assert callback_functions() == before
        assert before.count(t.shared_callback) == 1

    @pytest.mark.parametrize(
        ("task", "left"),
        [
            (t.drops_the_callers_first_callback, 2),
            (t.replaces_the_callers_first_callback, 3),
        ],
        ids=["dropped", "replaced"],
    )
    def test_callbacks_changed_under_it_stop_the_drain(self, task, left):
        mine = []
        transaction.on_commit(lambda: mine.append("first"))
        transaction.on_commit(lambda: mine.append("second"))
        result = task.enqueue("task")
        t.note.enqueue("not reached")
        with pytest.raises(RuntimeError) as raised:
            run_tasks()
        name = task.func.__name__
        assert str(raised.value) == (
            "The transaction.on_commit() callbacks pending on database "
            f"'default' before task {HERE}.{name} ran are no longer the first 2 "
            "there, in the order they were registered, so run_tasks() cannot "
            "tell the task's callbacks from the caller's and neither runs nor "
            "removes any of them. Something other than on_commit() and a "
            "savepoint rollback changed the connection's run_on_commit list "
            "while the task ran."
        )
        # Nothing ran and nothing more was removed.
        assert mine == []
        assert "callbacks" not in STATE
        assert len(connection.run_on_commit) == left
        assert "ran" not in STATE
        # Raised inside the attempt, so the worker recorded it there too.
        row = OxTask.objects.get(id=result.id)
        assert row.errors[0]["exception_class_path"] == "builtins.RuntimeError"
        assert not connection.needs_rollback


# -- what it leaves out ----------------------------------------------------


class TestExecution:
    def test_an_async_task_runs_and_shares_the_tests_rows(self):
        Group.objects.create(name="async-seed")
        t.async_make_group.enqueue("async")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert STATE["async_saw"] is True
        assert Group.objects.filter(pk=result.return_value, name="async").exists()

    def test_nothing_but_the_claim_and_the_execution_runs(self, monkeypatch):
        def refuse(name):
            def called(*args, **kwargs):
                raise AssertionError(f"run_tasks() reached {name}")

            return called

        for name in (
            "run",
            "run_once",
            "reap",
            "dispatch_schedules",
            "renew_leases",
            "_renewal_loop",
            "_arm",
            "_execute_in_thread",
            "_discard_connections",
            "_close_connections_in_thread",
        ):
            monkeypatch.setattr(Worker, name, refuse(name))
        monkeypatch.setattr(
            "django_ox.worker.close_old_connections", refuse("close_old_connections")
        )
        threads = set(threading.enumerate())
        driver = connection.connection
        t.note.enqueue("plain")
        t.async_make_group.enqueue("async")
        results = run_tasks()
        assert [r.status for r in results] == [TaskResultStatus.SUCCESSFUL] * 2
        assert connection.connection is driver
        assert connection.in_atomic_block
        assert {th for th in threading.enumerate() if th.name.startswith("ox")} <= {
            th for th in threads if th.name.startswith("ox")
        }


# -- timeouts --------------------------------------------------------------


class TestTimeouts:
    def test_a_declared_timeout_is_inert_and_said_once_per_call(self, caplog):
        t.outlives_its_timeout.enqueue(1.3)
        t.outlives_its_timeout.enqueue(0)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            results = run_tasks()
        assert [r.status for r in results] == [TaskResultStatus.SUCCESSFUL] * 2
        assert STATE["deadline"] is None
        assert STATE["remaining"] is None
        # Two attempts of one task in one call: one warning.
        [record] = events(caplog, "run_tasks_timeout_inert")
        assert record.task_path == f"{HERE}.outlives_its_timeout"
        assert record.timeout_s == 1
        assert record.backend == "default"
        assert record.getMessage() == (
            f"Task {HERE}.outlives_its_timeout runs under a 1s timeout, which "
            "run_tasks() does not enforce: the task runs to the end on the "
            "caller's thread, and deadline() and remaining() return None "
            "inside it. Test the timeout against a real worker."
        )

    def test_each_call_says_it_again(self, caplog):
        # So a test asserting the warning passes whatever ran before it.
        for call in range(2):
            t.outlives_its_timeout.enqueue(0)
            t.outlives_its_timeout.enqueue(0)
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="django_ox"):
                results = run_tasks()
            assert len(results) == 2, call
            assert len(events(caplog, "run_tasks_timeout_inert")) == 1, call

    def test_a_backend_timeout_is_inert_too(self, settings, caplog):
        settings.TASKS = ox_tasks(TASK_TIMEOUT=0.1)
        t.outlives_the_backend_timeout.enqueue(0.3)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert STATE["deadline"] is None
        assert len(events(caplog, "run_tasks_timeout_inert")) == 1

    def test_a_task_with_no_timeout_says_nothing(self, caplog):
        t.note.enqueue("quiet")
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            run_tasks()
        assert events(caplog, "run_tasks_timeout_inert") == []


class TestATaskThatRaisesTaskTimeoutItself:
    @pytest.mark.django_db(transaction=True)
    def test_the_callers_transaction_survives_it(self):
        with transaction.atomic():
            Group.objects.create(name="seed")
            t.make_then_raise_task_timeout.enqueue("written")
            driver = connection.connection
            [result] = run_tasks()
            assert connection.in_atomic_block
            assert connection.connection is driver
            assert Group.objects.filter(name="seed").exists()
            assert Group.objects.filter(name="written").exists()
            assert result.status == TaskResultStatus.READY
            assert result.errors[0].exception_class_path == (
                "django_ox.exceptions.TaskTimeout"
            )
            assert status_of(result) == OxTask.Status.READY

    def test_it_is_recorded_inside_a_testcase(self):
        Group.objects.create(name="seed")
        t.make_then_raise_task_timeout.enqueue("written")
        with pytest.raises(TaskTimeout, match="raised by the task itself"):
            run_tasks(raise_failures=True)
        assert connection.in_atomic_block
        assert Group.objects.filter(name__in=["seed", "written"]).count() == 2
        assert OxTask.objects.get().status == OxTask.Status.READY


# -- what it refuses -------------------------------------------------------


class TestRefusals:
    def test_a_task_body_cannot_start_a_drain(self):
        t.calls_run_tasks.enqueue()
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        [(kind, message)] = STATE["refused"]
        assert kind is RuntimeError
        assert message.startswith(
            f"run_tasks() was called inside a running task ('{HERE}.calls_run_tasks')."
        )

    def test_a_body_callback_cannot_start_a_drain(self):
        t.calls_run_tasks_from_callback.enqueue()
        run_tasks()
        [(_kind, message)] = STATE["refused"]
        assert "inside a running task" in message

    def test_an_outcome_callback_cannot_start_a_drain(self, settings):
        settings.TASKS = ox_tasks(WORKER_CLASS=f"{HERE}.OutcomeCallbackWorker")
        STATE["outcome_callback_runs_tasks"] = True
        t.note.enqueue("recorded")
        run_tasks()
        [(kind, message)] = STATE["refused"]
        assert kind is RuntimeError
        assert message.startswith(
            "run_tasks() was called while another run_tasks() call was draining"
        )

    def test_an_async_task_cannot_start_a_drain(self):
        t.async_calls_run_tasks.enqueue()
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        [(_kind, message)] = STATE["refused"]
        assert "async_calls_run_tasks" in message

    def test_a_task_run_by_a_worker_inline_cannot_start_a_drain(self):
        t.calls_run_tasks.enqueue()
        t.async_calls_run_tasks.enqueue()
        worker = Worker(backoff_initial=0)
        assert worker.run_once() is True
        assert worker.run_once() is True
        assert [kind for kind, _ in STATE["refused"]] == [RuntimeError] * 2

    @pytest.mark.django_db(transaction=True)
    def test_a_task_on_a_worker_thread_cannot_start_a_drain(self):
        t.calls_run_tasks.enqueue()
        Worker(backoff_initial=0, batch=True, poll_interval=0.05).run()
        [(kind, message)] = STATE["refused"]
        assert kind is RuntimeError
        assert "calls_run_tasks" in message
        assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL

    def test_a_signal_receiver_during_an_attempt_cannot_start_a_drain(self):
        def finished(sender, task_result, **kwargs):
            t._try_run_tasks()

        task_finished.connect(finished)
        try:
            t.note.enqueue("signalled")
            [result] = run_tasks()
        finally:
            task_finished.disconnect(finished)
        assert result.status == TaskResultStatus.SUCCESSFUL
        [(kind, message)] = STATE["refused"]
        assert kind is RuntimeError
        assert f"inside a running task ('{HERE}.note')" in message

    def test_the_marker_does_not_outlive_the_attempt(self):
        t.note.enqueue("one")
        assert Worker(backoff_initial=0).run_once() is True
        t.note.enqueue("two")
        assert [r.return_value for r in run_tasks()] == ["two"]

    @pytest.mark.parametrize(
        ("kwargs", "error", "message"),
        [
            ({"backend": 1}, TypeError, "backend must be a TASKS alias"),
            ({"queues": "emails"}, TypeError, "not a single string"),
            ({"queues": [1]}, TypeError, "list of queue names"),
            ({"queues": 5}, TypeError, "list of queue names"),
            ({"max_tasks": -1}, ValueError, "0 or more"),
            ({"max_tasks": True}, TypeError, "whole number or None"),
            ({"max_tasks": 1.5}, TypeError, "whole number or None"),
            ({"raise_failures": 1}, TypeError, "True or False"),
        ],
    )
    def test_invalid_arguments(self, kwargs, error, message):
        result = t.note.enqueue("never")
        with pytest.raises(error, match=re.escape(message)):
            run_tasks(**kwargs)
        assert status_of(result) == OxTask.Status.READY

    def test_arguments_are_keyword_only(self):
        with pytest.raises(TypeError):
            run_tasks("default")

    def test_a_tuple_of_queues_is_accepted(self):
        t.note_email.enqueue("email")
        assert [r.return_value for r in run_tasks(queues=("emails",))] == ["email"]

    def test_an_unknown_backend(self):
        with pytest.raises(Exception, match="nope") as raised:
            run_tasks(backend="nope")
        assert type(raised.value).__name__ in {
            "InvalidTaskBackend",
            "InvalidTaskBackendError",
        }

    def test_a_backend_that_is_not_an_ox_backend(self, settings):
        settings.TASKS = {
            **ox_tasks(),
            "immediate": {"BACKEND": "django_ox.testing.ImmediateBackend"},
        }
        with pytest.raises(ImproperlyConfigured) as raised:
            run_tasks(backend="immediate")
        assert str(raised.value) == (
            "run_tasks() drains an OxBackend, and backend 'immediate' is "
            "ImmediateBackend."
        )

    def test_it_is_public(self):
        assert "run_tasks" in testing.__all__


# -- Django's own test classes ---------------------------------------------


@override_settings(TASKS=ox_tasks())
class RunTasksInATestCase(TestCase):
    databases = {"default", "alt"}

    def setUp(self):
        STATE.clear()

    def test_it_sees_the_rows_and_keeps_the_class_transaction(self):
        Group.objects.create(name="seed")
        t.seen.enqueue("seed")
        [result] = run_tasks()
        assert result.return_value is True
        assert connection.in_atomic_block
        assert connections["alt"].in_atomic_block

    def test_an_outer_capture_sees_no_callback_twice(self):
        with self.captureOnCommitCallbacks(execute=True) as captured:
            t.register.enqueue("once")
            run_tasks()
        assert STATE["callbacks"] == ["once"]
        assert captured == []

    def test_an_outer_capture_still_gets_the_callers_own_callback(self):
        with self.captureOnCommitCallbacks(execute=True) as captured:
            transaction.on_commit(lambda: STATE.setdefault("mine", []).append(1))
            t.enqueue_child_on_commit.enqueue("p")
            run_tasks()
        assert len(captured) == 1
        assert STATE["mine"] == [1]
        assert STATE["ran"] == ["p", "p-child"]

    def test_an_outer_capture_gets_the_callers_callback_once_after_a_rollback(
        self,
    ):
        with self.captureOnCommitCallbacks(execute=True) as captured:
            transaction.on_commit(lambda: STATE.setdefault("mine", []).append(1))
            t.make_then_integrity_error.enqueue("rolled")
            t.rolls_back_its_own_block.enqueue("own")
            run_tasks()
            assert "mine" not in STATE
        assert len(captured) == 1
        assert STATE["mine"] == [1]
        assert STATE["callbacks"] == ["own"]

    def test_a_callbacks_database_error_leaves_the_class_transaction_usable(self):
        Group.objects.create(name="seed")
        t.writes_then_registers_a_breaking_callback.enqueue("tc", robust=True)
        t.writes_then_registers_a_breaking_callback.enqueue("tc2")
        results = run_tasks()
        assert [r.status for r in results] == [
            TaskResultStatus.SUCCESSFUL,
            TaskResultStatus.READY,
        ]
        assert results[1].errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        assert set(Group.objects.values_list("name", flat=True)) == {
            "seed",
            "tc-body",
            "tc2-body",
        }
        assert not connection.needs_rollback

    def test_a_body_writing_to_a_second_database(self):
        t.make_on.enqueue("alt", "on-alt")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert Group.objects.using("alt").filter(name="on-alt").exists()

    def test_a_database_error_on_one_database_rolls_back_only_there(self):
        t.make_on_both_then_integrity_error_on_alt.enqueue("both")
        [result] = run_tasks()
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        assert Group.objects.filter(name="both").exists()
        assert not Group.objects.using("alt").filter(name="both").exists()
        assert not connections["alt"].needs_rollback
        Group.objects.using("alt").create(name="after")

    def test_a_callback_on_a_second_database_runs(self):
        before = len(connections["alt"].run_on_commit)
        t.register_on.enqueue("alt", "alt-callback")
        run_tasks()
        assert STATE["callbacks"] == ["alt-callback"]
        assert len(connections["alt"].run_on_commit) == before

    def test_a_savepoint_that_cannot_be_opened_is_raised_to_the_caller(self):
        result = t.note.enqueue("never ran")
        refused = OperationalError("no savepoint for you")
        with (
            mock.patch.object(connections["alt"], "savepoint", side_effect=refused),
            pytest.raises(OperationalError) as raised,
        ):
            run_tasks()
        assert raised.value is refused
        # The worker had already recorded it against the attempt, which is
        # where anything raised inside an attempt goes.
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert row.errors[0]["exception_class_path"] == (
            "django.db.utils.OperationalError"
        )
        assert "ran" not in STATE
        assert not connection.needs_rollback

    def test_a_broken_second_database_is_refused(self):
        alt = connections["alt"]
        with transaction.atomic(using="alt"):
            alt.needs_rollback = True
            with self.assertRaisesMessage(
                TransactionManagementError, "database 'alt' is broken"
            ):
                run_tasks()


CLASS_CALLBACK_RAN = []


def _class_callback():
    CLASS_CALLBACK_RAN.append("class")


@override_settings(TASKS=ox_tasks())
class CallersCallbacksFromSetUpTestData(TestCase):
    """
    A callback setUpTestData registers, as a post_save receiver on a fixture
    does, stays pending through each test's drain and into the next test.
    The tests run in name order.
    """

    @classmethod
    def setUpTestData(cls):
        transaction.on_commit(_class_callback)
        cls.pending = callback_functions()

    def setUp(self):
        STATE.clear()

    def test_a_drain_that_rolls_back_a_savepoint_leaves_it(self):
        t.rolls_back_its_own_block.enqueue("a")
        t.make_then_integrity_error.enqueue("a")
        run_tasks()
        assert STATE["callbacks"] == ["a"]
        assert CLASS_CALLBACK_RAN == []
        assert callback_functions() == self.pending

    def test_b_the_next_test_still_has_it(self):
        assert _class_callback in self.pending
        assert callback_functions() == self.pending
        assert CLASS_CALLBACK_RAN == []


@override_settings(TASKS=ox_tasks())
class RunTasksInATransactionTestCase(TransactionTestCase):
    def setUp(self):
        STATE.clear()

    def test_autocommit_is_the_workers_own_behaviour(self):
        t.make_then_integrity_error.enqueue("ttc")
        [result] = run_tasks()
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        # Autocommit: the writes before the failing statement were committed,
        # as on a worker.
        assert Group.objects.filter(name__startswith="ttc").count() == 2

    def test_a_caught_orm_error_leaves_a_success_as_on_a_worker(self):
        t.make_then_swallow_orm_error.enqueue("caught")
        [result] = run_tasks()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert Group.objects.filter(name="caught").exists()

    def test_a_callbacks_database_error_is_what_a_worker_records(self):
        # Autocommit: the callback runs as it is registered, inside the body,
        # and its first write is committed before the second fails.
        t.writes_then_registers_a_breaking_callback.enqueue("ttc")
        [result] = run_tasks()
        assert result.errors[0].exception_class_path == (
            "django.db.utils.IntegrityError"
        )
        assert Group.objects.filter(name="ttc-body").exists()
        assert Group.objects.filter(name="ttc-callback").exists()

    def test_on_commit_runs_at_once_and_the_child_runs(self):
        t.enqueue_child_on_commit.enqueue("p")
        results = run_tasks()
        assert [r.return_value for r in results] == ["p", "p-child"]

    def test_the_connection_is_the_callers(self):
        t.where_am_i.enqueue()
        driver = connection.connection
        run_tasks()
        assert STATE["thread"] == threading.get_ident()
        assert STATE["driver"] == id(driver)
        assert STATE["in_atomic_block"] is False
