"""
Per-task policy in process: what a PolicyTask accepts, what a row stores and
reports, what the worker resolves for one attempt, and the options that
back it.
"""

import asyncio
import dataclasses
import functools
import logging
from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections, router
from django.test import override_settings
from django.utils import timezone

import django_ox
from django_ox import worker as worker_module
from django_ox.bulk import enqueue_many
from django_ox.compat import (
    DUMMY_BACKEND_PATH,
    InvalidTask,
    Task,
    TaskResultStatus,
    default_task_backend,
    task,
    task_backends,
    task_enqueued,
    task_finished,
)
from django_ox.models import OxTask
from django_ox.tasks import MAX_ATTEMPTS_LIMIT, PolicyTask, validate_policy
from django_ox.timeouts import MAX_SECONDS
from django_ox.worker import Worker

from . import policy_tasks, signal_tasks
from .policy_tasks import DECORATOR_TAKES_POLICY
from .tasks import STATE, add, fail_always

#: A worker that retries at once, for in-process attempts.
NO_WAIT = {"backoff_initial": 0, "poll_interval": 0.05}


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def tasks_setting(**options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, **options},
        }
    }


def module_level(value):
    return value


async def async_backoff(exc, task_result):
    return 1


class AsyncCallable:
    async def __call__(self, exc, task_result):
        return 1


class SyncCallable:
    def __call__(self, exc, task_result):
        return 1


def assert_is_the_declared_task(got, declared):
    """The declared class, equal to the declaration, and hashed alike."""
    assert type(got) is type(declared)
    assert got == declared
    assert hash(got) == hash(declared)


#: signal_tasks' tasks: bare, and declaring only a backoff or only a timeout.
SIGNAL_TASKS = [
    "bare_succeeds",
    "bare_fails",
    "backoff_only_fails",
    "timeout_only_succeeds",
]


def build(**policy):
    """A PolicyTask for module_level on the default backend."""
    return PolicyTask(
        func=module_level,
        priority=0,
        backend="default",
        queue_name="default",
        run_after=None,
        **policy,
    )


class TestTheFieldsAreValidated:
    @pytest.mark.parametrize("value", [1, 3, MAX_ATTEMPTS_LIMIT, None])
    def test_a_budget_from_one_to_the_column_ceiling_is_accepted(self, value):
        assert build(max_attempts=value).max_attempts == value

    @pytest.mark.parametrize(
        "value", [0, -1, MAX_ATTEMPTS_LIMIT + 1, True, False, "3", 3.0, 2.5]
    )
    def test_any_other_budget_is_refused(self, value):
        with pytest.raises(InvalidTask, match="max_attempts must be a whole number"):
            build(max_attempts=value)

    @pytest.mark.parametrize("value", [1, 30, int(MAX_SECONDS), None])
    def test_a_timeout_in_whole_seconds_is_accepted(self, value):
        assert build(timeout=value).timeout == value

    @pytest.mark.parametrize(
        "value", [0, -5, True, 1.5, 30.0, "30", int(MAX_SECONDS) + 1, timedelta(1)]
    )
    def test_any_other_timeout_is_refused(self, value):
        with pytest.raises(InvalidTask, match="timeout must be a whole number"):
            build(timeout=value)

    @pytest.mark.parametrize(
        "value",
        [
            policy_tasks.retry_now,
            SyncCallable(),
            SyncCallable,
            functools.partial(policy_tasks.retry_now),
            None,
        ],
        ids=["function", "instance", "class", "partial", "none"],
    )
    def test_a_synchronous_callable_is_accepted_as_backoff(self, value):
        assert build(backoff=value).backoff is value

    @pytest.mark.parametrize(
        "value",
        [
            async_backoff,
            AsyncCallable(),
            functools.partial(async_backoff),
        ],
        ids=["coroutine-function", "async-call", "partial"],
    )
    def test_an_async_backoff_is_refused_at_declaration(self, value):
        with pytest.raises(InvalidTask, match="synchronous callable"):
            build(backoff=value)

    @pytest.mark.parametrize("value", [5, "retry_now", timedelta(seconds=5)])
    def test_a_backoff_that_is_not_callable_is_refused(self, value):
        with pytest.raises(InvalidTask, match="backoff must be a callable"):
            build(backoff=value)

    def test_every_problem_is_named_at_once(self):
        with pytest.raises(InvalidTask) as excinfo:
            build(max_attempts=0, backoff=5, timeout=0)
        message = str(excinfo.value)
        assert "max_attempts" in message
        assert "backoff" in message
        assert "timeout" in message

    def test_replace_runs_the_same_validation(self):
        declared = policy_tasks.declares_everything
        assert dataclasses.replace(declared, max_attempts=9).max_attempts == 9
        with pytest.raises(InvalidTask, match="max_attempts"):
            dataclasses.replace(declared, max_attempts=0)

    @pytest.mark.skipif(
        not DECORATOR_TAKES_POLICY, reason="Django 6.0's @task takes no policy"
    )
    def test_the_decorator_passes_the_fields_and_validates_them(self):
        declared = task(max_attempts=4, timeout=9, backoff=policy_tasks.retry_now)(
            module_level
        )
        assert type(declared) is PolicyTask
        assert (declared.max_attempts, declared.timeout) == (4, 9)
        assert declared.backoff is policy_tasks.retry_now
        with pytest.raises(InvalidTask, match="max_attempts"):
            task(max_attempts=0)(module_level)

    def test_a_bare_decorator_builds_a_policy_task_that_inherits(self):
        assert type(add) is PolicyTask
        assert (add.max_attempts, add.backoff, add.timeout) == (None, None, None)

    def test_the_package_exports_the_class_and_the_callback_type(self):
        assert django_ox.PolicyTask is PolicyTask
        assert django_ox.BackoffCallback is not None
        assert {"PolicyTask", "BackoffCallback"} <= set(django_ox.__all__)
        with pytest.raises(AttributeError):
            django_ox.NoSuchName  # noqa: B018

    def test_using_keeps_the_policy(self):
        declared = policy_tasks.declares_everything
        rerouted = declared.using(priority=5, queue_name="emails")
        assert type(rerouted) is PolicyTask
        assert (rerouted.max_attempts, rerouted.timeout) == (5, 7)
        assert rerouted.backoff is policy_tasks.retry_now

    def test_validate_policy_passes_a_plain_task(self):
        plain = Task(
            func=module_level,
            priority=0,
            backend="default",
            queue_name="default",
            run_after=None,
        )
        assert validate_policy(plain) is None


@pytest.mark.django_db
class TestTheRowStoresTheBudgetAndResultsReportTheDeclaration:
    def test_a_declared_budget_is_stored(self):
        result = policy_tasks.declares_everything.enqueue(1)
        assert OxTask.objects.get(id=result.id).max_attempts == 5
        assert result.task.max_attempts == 5

    def test_a_task_that_declares_none_stores_the_backend_value(self, settings):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=7)
        result = add.enqueue(1, 2)
        assert OxTask.objects.get(id=result.id).max_attempts == 7
        # The result carries the task as declared, None and all; the stored
        # budget is the row's.
        assert result.task is add
        assert result.task.max_attempts is None

    def test_using_carries_the_declared_budget_to_the_row(self):
        result = policy_tasks.declares_everything.using(queue_name="emails").enqueue(1)
        stored = OxTask.objects.get(id=result.id)
        assert (stored.queue_name, stored.max_attempts) == ("emails", 5)

    def test_get_result_reports_the_declaration_not_the_stored_budget(self):
        result = policy_tasks.declares_everything.enqueue(1)
        OxTask.objects.filter(id=result.id).update(max_attempts=2)
        fetched = default_task_backend.get_result(result.id)
        assert type(fetched.task) is PolicyTask
        # All three fields are the live declaration; the 2 stays on the row.
        assert fetched.task.max_attempts == 5
        assert fetched.task.timeout == 7
        assert fetched.task.backoff is policy_tasks.retry_now
        assert fetched.task == policy_tasks.declares_everything
        assert OxTask.objects.get(id=result.id).max_attempts == 2
        assert policy_tasks.declares_everything.get_result(result.id).id == result.id

    def test_a_declared_budget_does_not_get_round_an_invalid_backend(self, settings):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=-1)
        with pytest.raises(ImproperlyConfigured, match="MAX_ATTEMPTS"):
            policy_tasks.declares_everything.enqueue(1)
        assert not OxTask.objects.exists()

    def test_a_row_below_the_valid_range_is_still_readable(self):
        # A MAX_ATTEMPTS of 0 is deprecated (django_ox.W004) and still stored.
        result = add.enqueue(1, 2)
        OxTask.objects.filter(id=result.id).update(max_attempts=0)
        fetched = default_task_backend.get_result(result.id)
        assert fetched.task == add
        assert fetched.task.max_attempts is None
        assert OxTask.objects.get(id=result.id).max_attempts == 0

    def test_bulk_results_carry_the_declared_task_and_rows_the_budget(self):
        results = enqueue_many(add, [((1, 2), {}), ((3, 4), {})])
        assert [r.task for r in results] == [add, add]
        assert results[0].task is results[1].task is add
        stored = OxTask.objects.filter(id__in=[r.id for r in results])
        assert sorted(stored.values_list("max_attempts", flat=True)) == [3, 3]
        declared = enqueue_many(policy_tasks.declares_everything, [((1,), {})])
        assert declared[0].task is policy_tasks.declares_everything

    def test_the_enqueued_signal_carries_the_declared_task(self):
        seen = []

        def receiver(sender, task_result, **kwargs):
            seen.append(task_result)

        task_enqueued.connect(receiver)
        try:
            result = add.enqueue(1, 2)
        finally:
            task_enqueued.disconnect(receiver)
        (signalled,) = seen
        assert signalled.task is add
        assert OxTask.objects.get(id=result.id).max_attempts == 3

    def test_a_plain_task_rebound_onto_the_backend_stays_plain(self, settings):
        settings.TASKS = {
            **tasks_setting(),
            "dummy": {"BACKEND": DUMMY_BACKEND_PATH},
        }
        declared_elsewhere = task(backend="dummy")(module_level)
        assert type(declared_elsewhere) is Task
        rebound = declared_elsewhere.using(backend="default")
        assert type(rebound) is Task
        result = rebound.enqueue(3)
        assert type(result.task) is Task
        assert result.task == rebound
        assert result.task.func is module_level
        assert OxTask.objects.get(id=result.id).max_attempts == 3

    def test_the_worker_keeps_a_plain_task_it_imports_plain(self, settings):
        settings.TASKS = {
            **tasks_setting(),
            "dummy": {"BACKEND": DUMMY_BACKEND_PATH},
        }
        from django_ox.results import task_from_db

        result = add.enqueue(1, 2)
        stored = OxTask.objects.get(id=result.id)
        stored.task_path = "tests.test_policy.plain_on_dummy"
        rebuilt = task_from_db(stored)
        assert type(rebuilt) is Task
        assert rebuilt.backend == "default"
        assert rebuilt.func is plain_on_dummy_func
        # Nothing declared, so the worker's policy lookup inherits all three.
        assert worker_module.task_policy(rebuilt) == (None, None, None)

    def test_a_policy_task_subclass_keeps_its_class_and_fields(self):
        from django_ox.results import task_from_db

        result = add.enqueue(1, 2)
        stored = OxTask.objects.get(id=result.id)
        stored.task_path = "tests.test_policy.labelled"
        rebuilt = task_from_db(stored)
        assert type(rebuilt) is LabelledTask
        assert rebuilt.label == "kept"
        # Its own declaration, not the 3 the row stores.
        assert rebuilt.max_attempts == 9
        assert rebuilt == labelled


@pytest.mark.django_db
class TestResultsCarryTheDeclaredTask:
    """
    A result's task is the task as declared, with the routing it was
    enqueued with, whichever path built the result: its class, and, when the
    routing matches, its equality and hash. The stored budget stays on the
    row, where the worker and the reaper read it.
    """

    @pytest.mark.parametrize("name", SIGNAL_TASKS)
    def test_enqueue_get_result_and_the_enqueued_signal(self, task_state, name):
        declared = getattr(signal_tasks, name)
        result = declared.enqueue()
        assert_is_the_declared_task(result.task, declared)
        fetched = default_task_backend.get_result(result.id)
        assert_is_the_declared_task(fetched.task, declared)
        ((signal, signalled),) = task_state["signalled"]
        assert signal == "enqueued"
        assert_is_the_declared_task(signalled.task, declared)
        assert OxTask.objects.get(id=result.id).max_attempts == 3

    def test_enqueue_many(self, task_state):
        declared = signal_tasks.bare_succeeds
        results = enqueue_many(declared, [((), {}), ((), {})])
        assert len(results) == 2
        for result in results:
            assert_is_the_declared_task(result.task, declared)
            fetched = default_task_backend.get_result(result.id)
            assert_is_the_declared_task(fetched.task, declared)
        signalled = [result for _, result in task_state["signalled"]]
        assert [result.id for result in signalled] == [r.id for r in results]
        for result in signalled:
            assert_is_the_declared_task(result.task, declared)

    @pytest.mark.parametrize("name", ["bare_succeeds", "timeout_only_succeeds"])
    def test_the_routing_is_rebuilt_from_the_row(self, name):
        declared = getattr(signal_tasks, name)
        later = timezone.now().replace(microsecond=0) + timedelta(hours=1)
        rerouted = declared.using(queue_name="emails", priority=7, run_after=later)
        result = rerouted.enqueue()
        assert_is_the_declared_task(result.task, rerouted)
        fetched = default_task_backend.get_result(result.id).task
        assert_is_the_declared_task(fetched, rerouted)
        assert (fetched.queue_name, fetched.priority, fetched.run_after) == (
            "emails",
            7,
            later,
        )
        assert fetched != declared

    def test_reenqueueing_an_inherited_budget_stores_the_backends_current_one(
        self, settings
    ):
        result = fail_always.enqueue()
        assert OxTask.objects.get(id=result.id).max_attempts == 3
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=7)
        again = task_backends["default"].get_result(result.id).task.enqueue()
        assert OxTask.objects.get(id=again.id).max_attempts == 7
        assert OxTask.objects.get(id=result.id).max_attempts == 3

    def test_reenqueueing_a_declaration_stores_it_not_the_rows_budget(self):
        result = policy_tasks.declares_everything.enqueue(1)
        OxTask.objects.filter(id=result.id).update(max_attempts=2)
        again = default_task_backend.get_result(result.id).task.enqueue(1)
        assert OxTask.objects.get(id=again.id).max_attempts == 5

    @pytest.mark.parametrize("declares", [False, True], ids=["bare", "declared"])
    def test_a_legacy_budget_result_reads_and_copies_with_both_apis(self, declares):
        declared = policy_tasks.declares_everything if declares else add
        args = (1,) if declares else (1, 2)
        # 0 is what a deprecated MAX_ATTEMPTS of 0 stores on every database.
        # One above 32767 is what a deprecated MAX_ATTEMPTS stored on SQLite
        # and MySQL; PostgreSQL's smallint never held one.
        vendor = connections[router.db_for_write(OxTask)].vendor
        budgets = [0] if vendor == "postgresql" else [0, MAX_ATTEMPTS_LIMIT + 7233]
        for budget in budgets:
            result = declared.enqueue(*args)
            OxTask.objects.filter(id=result.id).update(max_attempts=budget)
            fetched = default_task_backend.get_result(result.id).task
            assert_is_the_declared_task(fetched, declared)
            moved = fetched.using(priority=5, queue_name="emails")
            assert (moved.priority, moved.queue_name) == (5, "emails")
            assert moved.max_attempts == declared.max_attempts
            replaced = dataclasses.replace(fetched, priority=6)
            assert replaced.priority == 6
            assert replaced.max_attempts == declared.max_attempts
            # Asked for outright, the stored value is still refused.
            with pytest.raises(InvalidTask, match="max_attempts"):
                dataclasses.replace(fetched, max_attempts=budget)
            assert OxTask.objects.get(id=result.id).max_attempts == budget

    def test_a_deprecated_zero_budget_result_copies(self, settings):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=0)
        result = add.enqueue(1, 2)
        assert OxTask.objects.get(id=result.id).max_attempts == 0
        assert result.task.using(priority=5).priority == 5
        fetched = task_backends["default"].get_result(result.id).task
        assert fetched.using(priority=5).priority == 5
        assert dataclasses.replace(fetched, priority=6).priority == 6


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class LabelledTask(PolicyTask):
    label: str = ""


def labelled_func():
    return "labelled"


labelled = LabelledTask(
    func=labelled_func,
    priority=0,
    backend="default",
    queue_name="default",
    run_after=None,
    max_attempts=9,
    label="kept",
)


def plain_on_dummy_func():
    return "plain"


def __getattr__(name):
    # The dummy alias exists only inside the tests that use this, so the
    # plain Task is built when the worker's import asks for it.
    if name == "plain_on_dummy":
        return task(backend="dummy")(plain_on_dummy_func)
    raise AttributeError(name)


@pytest.mark.django_db(transaction=True)
class TestTheWorkerResolvesThePolicyPerAttempt:
    def test_deadline_and_remaining_follow_the_task_timeout(self, task_state):
        worker = Worker(**NO_WAIT)
        assert not worker.timeouts.enabled
        policy_tasks.reports_deadline_in_process.enqueue()
        assert worker.run_once() is True
        assert 4 < task_state["remaining"] <= 5
        assert task_state["deadline"] is not None

    def test_the_task_timeout_wins_over_the_queue_and_the_worker(self, task_state):
        worker = Worker(task_timeout=60, **NO_WAIT)
        policy_tasks.reports_deadline_in_process.enqueue()
        assert worker.run_once() is True
        assert 4 < task_state["remaining"] <= 5

    def test_the_interpreter_notice_is_given_for_a_task_timeout(
        self, caplog, monkeypatch
    ):
        monkeypatch.setattr(worker_module, "_inject_async_exc", None)
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(**NO_WAIT)
        # Nothing in the options, so nothing to say at startup.
        assert not events(caplog, "timeouts_backstop_only")
        policy_tasks.reports_deadline_in_process.enqueue()
        policy_tasks.reports_deadline_in_process.enqueue()
        assert worker.run_once() is True
        assert worker.run_once() is True
        (notice,) = events(caplog, "timeouts_backstop_only")
        assert notice.reason == "interpreter"

    def test_a_backoff_runs_on_the_attempts_thread_after_bad_connections_go(
        self, monkeypatch
    ):
        dropped = []
        original = Worker._discard_unusable_connections

        def spy(self):
            # How many times the backoff had been asked when this ran.
            dropped.append(len(STATE.get("notes", [])))
            original(self)

        monkeypatch.setattr(Worker, "_discard_unusable_connections", spy)
        worker = Worker(**NO_WAIT)
        result = policy_tasks.fails_and_describes.enqueue()
        assert worker.run_once() is True
        # Once before the backoff, for what the task broke, and once after
        # it, for what the backoff broke, before the write.
        assert dropped == [0, 1]
        stored = OxTask.objects.get(id=result.id)
        assert stored.status == OxTask.Status.READY
        (snapshot,) = STATE["notes"]
        assert snapshot["status"] == "FAILED"
        assert snapshot["attempts"] == 1
        assert snapshot["max_attempts"] == 2

    def test_the_failed_signal_carries_the_attempts_task_without_a_reimport(
        self, monkeypatch
    ):
        seen = []

        def receiver(sender, task_result, **kwargs):
            seen.append(task_result)

        from django_ox import results

        worker = Worker(**NO_WAIT)
        result = policy_tasks.fails_and_stops.enqueue()
        imports = []
        original = results.task_from_db

        def counting(db_task):
            imports.append(db_task.pk)
            return original(db_task)

        monkeypatch.setattr(results, "task_from_db", counting)
        task_finished.connect(receiver)
        try:
            assert worker.run_once() is True
        finally:
            task_finished.disconnect(receiver)
        (finished,) = seen
        assert finished.status == TaskResultStatus.FAILED
        assert finished.task.max_attempts == 5
        # Once, for the attempt; the failure used what that import found.
        assert [str(pk) for pk in imports] == [result.id]

    @pytest.mark.parametrize(
        "path", ["tests.raises_on_import.gone", "tests.refused_declaration.refused"]
    )
    def test_an_attempt_that_never_had_a_task_is_not_imported_again(
        self, monkeypatch, caplog, path
    ):
        # A module that raises something other than ImportError while it
        # imports. The attempt fails before it has a task; recording that
        # failure must not import it again, which would raise the same thing
        # out of the failure path after the FAILED write.
        from django_ox import results

        seen = []

        def receiver(sender, task_result, **kwargs):
            seen.append(task_result)

        result = add.enqueue(1, 2)
        OxTask.objects.filter(id=result.id).update(task_path=path, max_attempts=1)
        imports = []
        original = results.task_from_db

        def counting(db_task):
            imports.append(db_task.pk)
            return original(db_task)

        monkeypatch.setattr(results, "task_from_db", counting)
        worker = Worker(**NO_WAIT)
        task_finished.connect(receiver)
        try:
            with caplog.at_level(logging.INFO, logger="django_ox"):
                assert worker.run_once() is True
        finally:
            task_finished.disconnect(receiver)
        stored = OxTask.objects.get(id=result.id)
        assert (stored.status, stored.attempts) == (OxTask.Status.FAILED, 1)
        assert [str(pk) for pk in imports] == [result.id]
        assert seen == []
        (failed,) = events(caplog, "task_failed")
        assert failed.reason == "attempts_exhausted"

    def test_a_direct_failure_record_still_signals_a_task_that_imports(self):
        # No attempt on the thread, so nothing says the task does not import:
        # it is imported, as before, and the signal is sent.
        seen = []

        def receiver(sender, task_result, **kwargs):
            seen.append(task_result)

        worker = Worker(backoff_initial=30, backoff_max=30)
        result = policy_tasks.fails_and_stops.enqueue()
        OxTask.objects.filter(id=result.id).update(max_attempts=1)
        claimed = worker.claim_one()
        task_finished.connect(receiver)
        try:
            assert worker._handle_failure(claimed, ValueError("x"), 1) is True
        finally:
            task_finished.disconnect(receiver)
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.FAILED
        (finished,) = seen
        assert finished.id == result.id
        assert finished.status == TaskResultStatus.FAILED

    def test_a_direct_failure_record_takes_the_workers_backoff(self):
        # The stuck-thread path and callers outside an attempt reach
        # _handle_failure with no attempt policy on the thread.
        worker = Worker(backoff_initial=30, backoff_max=30)
        result = policy_tasks.fails_and_stops.enqueue()
        claimed = worker.claim_one()
        assert str(claimed.pk) == result.id
        assert worker._handle_failure(claimed, ValueError("x"), 1) is True
        stored = OxTask.objects.get(id=result.id)
        assert stored.status == OxTask.Status.READY
        assert STATE.get("notes") is None

    def test_the_stuck_path_never_asks_the_backoff(self):
        worker = Worker(backoff_initial=30, backoff_max=30)
        result = policy_tasks.fails_and_stops.enqueue()
        claimed = worker.claim_one()
        policy = worker_module._AttemptPolicy(
            attempt=(claimed.pk, claimed.lease_epoch),
            task=policy_tasks.fails_and_stops,
            backoff=policy_tasks.stop_retrying,
            inline=False,
        )
        worker._attempt_local.policy = policy
        try:
            landed = worker._handle_failure(
                claimed, ValueError("stuck"), 1, release=True
            )
        finally:
            worker._attempt_local.policy = None
        assert landed is True
        stored = OxTask.objects.get(id=result.id)
        # stop_retrying would have made it FAILED; the backstop does not ask.
        assert stored.status == OxTask.Status.READY
        assert STATE.get("notes") is None

    @pytest.mark.parametrize("kind", ["cancelled", "base"])
    def test_inline_a_base_exception_from_a_backoff_reaches_the_caller(self, kind):
        # run_once() is the caller's own thread. A BaseException that is not
        # an Exception is theirs to handle, as KeyboardInterrupt and
        # SystemExit are, not a callback failure to fall back from.
        worker = Worker(**NO_WAIT)
        policy_tasks.fails_with_a_base_exception_backoff.enqueue(kind=kind)
        raised = (
            asyncio.CancelledError
            if kind == "cancelled"
            else policy_tasks.CallbackEscape
        )
        with pytest.raises(raised, match="raised by the backoff callback"):
            worker.run_once()
        assert STATE["notes"] == [{"callback": "base", "kind": kind, "attempts": 1}]

    def test_inline_an_exception_from_a_backoff_still_falls_back(self, caplog):
        caplog.set_level(logging.ERROR, logger="django_ox")
        worker = Worker(**NO_WAIT)
        result = policy_tasks.fails_with_a_misbehaving_backoff.enqueue(mode="raises")
        assert worker.run_once() is True
        stored = OxTask.objects.get(id=result.id)
        assert (stored.status, stored.attempts) == (OxTask.Status.READY, 1)
        assert [e["exception_class_path"] for e in stored.errors] == [
            "builtins.ValueError"
        ]
        (logged,) = events(caplog, "task_policy_error")
        assert logged.exc_info[0] is RuntimeError


@pytest.mark.django_db
class TestThePoolThreadBoundary:
    """
    _execute_in_thread is all a pool thread runs for a task, and the pool
    keeps its future without reading it: what leaves that frame is lost
    without a word. Everything in it, connection setup and cleanup included,
    is caught and logged as worker_error.
    """

    def claimed(self):
        result = add.enqueue(1, 2)
        return OxTask.objects.get(id=result.id)

    @pytest.mark.parametrize(
        "raised",
        [asyncio.CancelledError, policy_tasks.CallbackEscape, SystemExit],
        ids=["cancelled", "base", "exit"],
    )
    def test_anything_execute_raises_is_logged(self, monkeypatch, caplog, raised):
        caplog.set_level(logging.ERROR, logger="django_ox")
        worker = Worker(**NO_WAIT)
        db_task = self.claimed()

        def escape(db_task, *, inline=False):
            raise raised("left execute")

        monkeypatch.setattr(worker, "execute", escape)
        worker._execute_in_thread(db_task)
        (logged,) = events(caplog, "worker_error")
        assert logged.exc_info[0] is raised
        assert logged.task_id == str(db_task.id)

    @pytest.mark.parametrize("when", ["setup", "cleanup"])
    def test_a_connection_failure_on_either_side_is_logged(
        self, monkeypatch, caplog, when
    ):
        caplog.set_level(logging.ERROR, logger="django_ox")
        worker = Worker(**NO_WAIT)
        db_task = self.claimed()
        ran = []
        monkeypatch.setattr(
            worker, "execute", lambda db_task, *, inline=False: ran.append(db_task.pk)
        )
        calls = []

        def close_old_connections():
            calls.append(len(calls))
            if calls == ([0] if when == "setup" else [0, 1]):
                raise policy_tasks.CallbackEscape(f"connection {when} failed")

        monkeypatch.setattr(
            worker_module, "close_old_connections", close_old_connections
        )
        worker._execute_in_thread(db_task)
        # The cleanup runs whether or not the setup did.
        assert calls == [0, 1]
        assert ran == ([] if when == "setup" else [db_task.pk])
        (logged,) = events(caplog, "worker_error")
        assert logged.exc_info[0] is policy_tasks.CallbackEscape
        assert f"connection {when} failed" in str(logged.exc_info[1])


class TestTheTestBackends:
    @pytest.mark.parametrize(
        "backend",
        ["django_ox.testing.ImmediateBackend", "django_ox.testing.DummyBackend"],
    )
    def test_they_build_policy_tasks_and_validate_them(self, backend):
        with override_settings(TASKS={"default": {"BACKEND": backend}}):
            built = task_backends["default"].task_class
            assert built is PolicyTask
            declared = dataclasses.replace(task(module_level), max_attempts=4)
            assert declared.max_attempts == 4
            with pytest.raises(InvalidTask, match="max_attempts"):
                dataclasses.replace(task(module_level), max_attempts=0)

    def test_immediate_runs_once_and_warns_once_per_task(self, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        with override_settings(
            TASKS={"default": {"BACKEND": "django_ox.testing.ImmediateBackend"}}
        ):
            declared = dataclasses.replace(
                task(fail_always.func), max_attempts=5, timeout=1
            )
            first = declared.enqueue()
            second = declared.enqueue()
            plain = task(add.func).enqueue(1, 2)
        assert first.status == second.status == TaskResultStatus.FAILED
        assert first.attempts == 1
        assert plain.status == TaskResultStatus.SUCCESSFUL
        (warning,) = events(caplog, "task_policy_inert")
        assert warning.levelno == logging.WARNING
        assert warning.task_path == "tests.tasks.fail_always"
        assert warning.declared == ["max_attempts", "timeout"]
        assert "does not enforce" in warning.getMessage()

    def test_dummy_stores_without_running_and_warns_once(self, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        with override_settings(
            TASKS={"default": {"BACKEND": "django_ox.testing.DummyBackend"}}
        ):
            declared = dataclasses.replace(
                task(add.func), backoff=policy_tasks.retry_now
            )
            result = declared.enqueue(1, 2)
            declared.enqueue(3, 4)
        assert result.status == TaskResultStatus.READY
        (warning,) = events(caplog, "task_policy_inert")
        assert warning.declared == ["backoff"]
