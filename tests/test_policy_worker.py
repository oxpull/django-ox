"""
Per-task policy, through the real `manage.py ox_worker`.

Every test here enqueues in this process, runs the command in a subprocess
against the same test database, and reads the rows it left. What a task
declares reaches the worker only by the route production takes: the row, and
a fresh import of the task module in another process.
"""

import json
import os
import subprocess
import sys
import time
from datetime import timedelta

import pytest
from django.db import connection
from django.utils import timezone

from django_ox import actions
from django_ox.compat import InvalidTask, default_task_backend, task_backends
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask

from . import policy_tasks, signal_tasks
from .policy_tasks import notes
from .tasks import fail_always
from .test_policy import SIGNAL_TASKS, assert_is_the_declared_task, tasks_setting

TIMEOUT_PATH = f"{TaskTimeout.__module__}.{TaskTimeout.__qualname__}"
#: django.tasks' InvalidTask, which the 5.2 backport calls InvalidTaskError.
INVALID_TASK_PATH = f"{InvalidTask.__module__}.{InvalidTask.__qualname__}"

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def policy_log(tmp_path):
    return tmp_path / "policy.jsonl"


def run_worker(*args, options=None, policy_log=None, log_format=None, timeout=90):
    """
    `manage.py ox_worker` on this test database, to completion.

    Coverage's subprocess hooks are left out of the child: a thread a tracer
    watches is never interrupted, which would turn every timeout here into the
    grace backstop and a recycle.
    """
    from django.conf import settings

    env = {k: v for k, v in os.environ.items() if not k.startswith("COV_CORE")}
    env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    if log_format is not None:
        env["OX_TEST_LOG_FORMAT"] = log_format
    if options is not None:
        env["OX_TEST_TASKS_OPTIONS"] = json.dumps(options)
    if policy_log is not None:
        env["OX_TEST_POLICY_LOG"] = str(policy_log)
    started = time.monotonic()
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "django", "ox_worker", "--interval", "0.05", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    completed.elapsed = time.monotonic() - started
    return completed


def row(result):
    return OxTask.objects.get(id=result.id)


def logged_events(stderr):
    """The event of every line a worker logged with EVENT_FORMAT."""
    return [
        line.split()[1].removeprefix("event=")
        for line in stderr.splitlines()
        if line.split()[1:2] and line.split()[1].startswith("event=")
    ]


#: A log format that says which event each line is.
EVENT_FORMAT = "%(levelname)s event=%(event)s %(message)s"


def seconds_until(when, since):
    return (when - since).total_seconds()


def test_two_tasks_on_one_queue_keep_their_own_budgets(policy_log):
    one = policy_tasks.fails_with_budget_one.enqueue()
    four = policy_tasks.fails_with_budget_four.enqueue()
    assert one.task.queue_name == four.task.queue_name == "default"

    worker = run_worker("--batch", policy_log=policy_log)

    assert worker.returncode == 0, worker.stderr
    first, second = row(one), row(four)
    assert (first.status, first.attempts, first.max_attempts) == ("FAILED", 1, 1)
    assert (second.status, second.attempts, second.max_attempts) == ("FAILED", 4, 4)
    assert len(first.errors) == 1
    assert len(second.errors) == 4
    assert default_task_backend.get_result(one.id).task.max_attempts == 1
    assert default_task_backend.get_result(four.id).task.max_attempts == 4


def test_a_backoff_sets_the_delay_and_sees_the_failed_attempt(policy_log):
    result = policy_tasks.fails_and_waits_an_hour.enqueue()
    # The row's budget decides the retry; the snapshot's task is the
    # declaration, which says 3.
    OxTask.objects.filter(id=result.id).update(max_attempts=5)
    before = timezone.now()

    worker = run_worker("--max-tasks", "1", policy_log=policy_log)

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert stored.status == OxTask.Status.READY
    assert stored.attempts == 1
    # An hour, not the worker's five-second backoff, and not capped by
    # BACKOFF_MAX's ten minutes.
    assert 3590 < seconds_until(stored.run_after, before) < 3700
    assert [e["exception_class_path"] for e in stored.errors] == ["builtins.ValueError"]
    (snapshot,) = notes(policy_log)
    assert snapshot == {
        "callback": "describe",
        "exception": "ValueError",
        "message": "wait an hour",
        "status": "FAILED",
        "attempts": 1,
        "worker_ids": 1,
        "errors": ["builtins.ValueError"],
        "finished": True,
        "task_type": "PolicyTask",
        "max_attempts": 3,
        "task_id": str(result.id),
    }
    assert stored.max_attempts == 5
    assert "retrying in 3600.0s" in worker.stderr


def test_a_backoff_may_answer_in_whole_seconds():
    result = policy_tasks.fails_and_waits_two_hours.enqueue()
    before = timezone.now()

    worker = run_worker("--max-tasks", "1")

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert stored.status == OxTask.Status.READY
    assert 7190 < seconds_until(stored.run_after, before) < 7300


def test_a_backoff_answering_none_fails_the_task_now(policy_log):
    result = policy_tasks.fails_and_stops.enqueue()

    worker = run_worker("--batch", policy_log=policy_log)

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert (stored.status, stored.attempts, stored.max_attempts) == ("FAILED", 1, 5)
    assert stored.finished_at is not None
    assert [e["exception_class_path"] for e in stored.errors] == ["builtins.ValueError"]
    assert notes(policy_log) == [{"callback": "stop", "attempts": 1}]
    assert "its backoff returned None, so it is not retried" in worker.stderr


@pytest.mark.parametrize("mode", sorted(policy_tasks.MISBEHAVIOURS))
def test_a_misbehaving_backoff_falls_back_and_keeps_the_task_error(policy_log, mode):
    result = policy_tasks.fails_with_a_misbehaving_backoff.enqueue(mode=mode)
    before = timezone.now()

    worker = run_worker(
        "--max-tasks",
        "1",
        options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900},
        policy_log=policy_log,
    )

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert stored.status == OxTask.Status.READY
    # The worker's own backoff, which is what the options give it.
    assert 890 < seconds_until(stored.run_after, before) < 1000
    # The task's failure is the one recorded, never the callback's.
    (error,) = stored.errors
    assert error["exception_class_path"] == "builtins.ValueError"
    assert "the task's own failure" in error["traceback"]
    assert "the backoff callback broke" not in error["traceback"]
    assert notes(policy_log) == [{"callback": "misbehaving", "mode": mode}]
    assert "retrying on the worker's backoff instead" in worker.stderr
    assert "never awaited" not in worker.stderr
    if mode == "raises":
        assert "RuntimeError: the backoff callback broke" in worker.stderr


def test_a_task_that_no_longer_imports_takes_the_row_budget_and_worker_backoff(
    policy_log,
):
    retried = policy_tasks.moved_away.enqueue()
    exhausted = policy_tasks.moved_away.enqueue()
    OxTask.objects.filter(id=retried.id).update(
        task_path="tests.policy_tasks.not_there", max_attempts=2
    )
    OxTask.objects.filter(id=exhausted.id).update(
        task_path="tests.policy_tasks.not_there", max_attempts=1
    )
    before = timezone.now()

    worker = run_worker(
        "--max-tasks",
        "2",
        options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900},
        policy_log=policy_log,
    )

    assert worker.returncode == 0, worker.stderr
    first = row(retried)
    assert first.status == OxTask.Status.READY
    assert 890 < seconds_until(first.run_after, before) < 1000
    assert first.errors[-1]["exception_class_path"] == "builtins.ImportError"
    second = row(exhausted)
    assert (second.status, second.attempts) == ("FAILED", 1)
    # No task, so no callback: the one the task declared was never asked.
    assert notes(policy_log) == []


@pytest.mark.parametrize(
    ("path", "raised"),
    [
        ("tests.raises_on_import.gone", {"builtins.TypeError"}),
        # TypeError on Django 6.0, whose task() takes no policy keywords.
        (
            "tests.refused_declaration.refused",
            {INVALID_TASK_PATH, "builtins.TypeError"},
        ),
    ],
    ids=["type-error", "refused-declaration"],
)
def test_a_module_that_raises_on_import_ends_in_task_failed_not_worker_error(
    path, raised
):
    """
    A module that raises something other than ImportError while importing:
    TypeError from a declaration Django 6.0 cannot take, InvalidTask from one
    this release refuses. Each attempt fails before it has a task. The last
    one records FAILED and logs task_failed; it does not import the module a
    second time, which is what used to raise out of the failure path and log
    the final attempt as worker_error with no reason.
    """
    result = policy_tasks.moved_away.enqueue()
    OxTask.objects.filter(id=result.id).update(task_path=path, max_attempts=2)

    worker = run_worker(
        "--max-tasks",
        "2",
        options={"BACKOFF_INITIAL": 0.05, "BACKOFF_MAX": 0.05},
        log_format="%(levelname)s event=%(event)s %(message)s",
    )

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert (stored.status, stored.attempts) == ("FAILED", 2)
    assert len(stored.errors) == 2
    for error in stored.errors:
        assert error["exception_class_path"] in raised, error
    logged = [
        line.split()[1].removeprefix("event=")
        for line in worker.stderr.splitlines()
        if line.split()[1:2] and line.split()[1].startswith("event=")
    ]
    assert "worker_error" not in logged, worker.stderr
    assert logged.count("task_retrying") == 1, worker.stderr
    assert logged.count("task_failed") == 1, worker.stderr
    assert f"path={path} failed after 2/2 attempts" in worker.stderr


def test_a_task_timeout_applies_with_no_timeout_in_the_options():
    sync = policy_tasks.spins_past_its_timeout.enqueue(60)
    coroutine = policy_tasks.awaits_past_its_timeout.enqueue(60)

    worker = run_worker(
        "--batch",
        "--concurrency",
        "2",
        options={"TASK_TIMEOUT_GRACE": 10},
    )

    assert worker.returncode == 0, worker.stderr
    # Well short of the minute either task would take on its own.
    assert worker.elapsed < 45, worker.elapsed
    for result in (sync, coroutine):
        stored = row(result)
        assert stored.status == OxTask.Status.FAILED, (stored.status, worker.stderr)
        (error,) = stored.errors
        assert error["exception_class_path"] == TIMEOUT_PATH
        assert "past the 1s timeout" in error["traceback"]
    assert worker.stderr.count("ran past its 1s timeout") == 2, worker.stderr


def test_a_task_timeout_applies_on_a_queue_the_options_exempt():
    declared = policy_tasks.spins_past_its_timeout_on_emails.enqueue(60)
    exempt = policy_tasks.spins_on_emails.enqueue(2)

    worker = run_worker(
        "--batch",
        "--concurrency",
        "2",
        options={
            "TASK_TIMEOUT": 1,
            "TASK_TIMEOUTS": {"emails": None},
            "TASK_TIMEOUT_GRACE": 10,
        },
    )

    assert worker.returncode == 0, worker.stderr
    assert worker.elapsed < 45, worker.elapsed
    stored = row(declared)
    assert stored.status == OxTask.Status.FAILED
    assert stored.errors[-1]["exception_class_path"] == TIMEOUT_PATH
    # A task that declares nothing is still exempt on that queue: it runs
    # past TASK_TIMEOUT and succeeds.
    assert row(exempt).status == OxTask.Status.SUCCESSFUL


def test_every_retry_gets_a_fresh_deadline(policy_log):
    result = policy_tasks.reports_its_deadline_then_fails.enqueue(1.5)

    worker = run_worker("--batch", policy_log=policy_log)

    assert worker.returncode == 0, worker.stderr
    stored = row(result)
    assert (stored.status, stored.attempts) == ("FAILED", 2)
    first, second = notes(policy_log)
    # The whole three seconds at the start of each attempt, although the
    # first spent half of them before failing.
    assert 2.5 < first["remaining"] <= 3
    assert 2.5 < second["remaining"] <= 3
    assert second["deadline"] - first["deadline"] > 1.4


def test_an_operator_retry_grants_one_attempt_not_the_declared_budget():
    result = policy_tasks.fails_three_times.enqueue()
    first = run_worker("--batch")
    assert first.returncode == 0, first.stderr
    assert (row(result).status, row(result).attempts) == ("FAILED", 3)

    assert actions.retry(result.id) is True
    assert row(result).max_attempts == 4

    second = run_worker("--batch")
    assert second.returncode == 0, second.stderr
    stored = row(result)
    # One more claim, not three more from the declaration.
    assert (stored.status, stored.attempts, stored.max_attempts) == ("FAILED", 4, 4)
    # The result's task is the declaration; the granted attempt is the row's.
    assert default_task_backend.get_result(result.id).task.max_attempts == 3

    assert actions.retry_many([result.id]) == (1, 0)
    third = run_worker("--batch")
    assert third.returncode == 0, third.stderr
    assert (row(result).attempts, row(result).max_attempts) == (5, 5)


def test_the_reaper_decides_on_the_stored_budget():
    lost = policy_tasks.succeeds_with_budget_one.enqueue()
    requeued = policy_tasks.succeeds_with_budget_two.enqueue()
    long_ago = timezone.now() - timedelta(hours=1)
    # Both as a worker that died mid-attempt leaves them: one claim spent,
    # the lease long expired.
    OxTask.objects.filter(id__in=[lost.id, requeued.id]).update(
        status=OxTask.Status.RUNNING,
        attempts=1,
        lease_epoch=1,
        worker_ids=["gone-worker"],
        locked_by="gone-worker",
        locked_at=long_ago,
        lease_expires_at=long_ago,
        started_at=long_ago,
        last_attempted_at=long_ago,
    )

    worker = run_worker("--batch")

    assert worker.returncode == 0, worker.stderr
    first = row(lost)
    assert (first.status, first.attempts, first.max_attempts) == ("LOST", 1, 1)
    second = row(requeued)
    assert (second.status, second.attempts, second.max_attempts) == (
        "SUCCESSFUL",
        2,
        2,
    )


#: What each of signal_tasks' tasks ends as: status, claims, stored budget.
SIGNAL_TASK_OUTCOMES = {
    "bare_succeeds": ("SUCCESSFUL", 1, 3),
    # Its row is given a budget of 1 below, as an older row might have.
    "bare_fails": ("FAILED", 1, 1),
    "backoff_only_fails": ("FAILED", 3, 3),
    "timeout_only_succeeds": ("SUCCESSFUL", 1, 3),
}


@pytest.mark.parametrize("name", SIGNAL_TASKS)
def test_every_result_of_a_task_is_the_declared_task(policy_log, task_state, name):
    """
    enqueue(), get_result(), task_enqueued in this process and task_finished
    in the worker's all carry the task as declared: its class, and equal to
    it with an equal hash, since the routing matches.
    """
    declared = getattr(signal_tasks, name)
    result = declared.enqueue()
    assert_is_the_declared_task(result.task, declared)
    assert_is_the_declared_task(
        default_task_backend.get_result(result.id).task, declared
    )
    ((signal, enqueued),) = task_state["signalled"]
    assert signal == "enqueued"
    assert_is_the_declared_task(enqueued.task, declared)
    if name == "bare_fails":
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

    worker = run_worker("--batch", policy_log=policy_log)

    assert worker.returncode == 0, worker.stderr
    status, attempts, budget = SIGNAL_TASK_OUTCOMES[name]
    stored = row(result)
    assert (stored.status, stored.attempts, stored.max_attempts) == (
        status,
        attempts,
        budget,
    )
    # A task that retried has the retry's run_after on its row, so its
    # routing matches its declaration's only with that run_after.
    retried = attempts > 1
    assert (stored.run_after is not None) is retried
    assert notes(policy_log) == [
        {
            "signal": "finished",
            "task": name,
            "status": status,
            "class": type(declared).__qualname__,
            "declared_class": type(declared).__qualname__,
            "equal": True,
            "same_hash": True,
            "run_after_from_row": retried,
        }
    ]
    routed = declared.using(run_after=stored.run_after) if retried else declared
    assert_is_the_declared_task(default_task_backend.get_result(result.id).task, routed)


def test_rows_run_to_their_stored_budgets_and_read_as_the_declared_task():
    """
    Rows whose stored budget is not what the code would give them today, as
    an older release, a deprecated MAX_ATTEMPTS or an operator leaves them:
    each runs to its own column, and its result reads as the task declared.
    """
    budgets = {"zero": 0, "two": 2, "five": 5}
    bare = {}
    for label, budget in budgets.items():
        bare[label] = fail_always.enqueue()
        OxTask.objects.filter(id=bare[label].id).update(max_attempts=budget)
    declared = policy_tasks.fails_three_times.enqueue()
    OxTask.objects.filter(id=declared.id).update(max_attempts=1)

    # 1 + 2 + 5 claims for the bare rows, 1 for the declared one.
    worker = run_worker(
        "--max-tasks",
        "9",
        options={"BACKOFF_INITIAL": 0.05, "BACKOFF_MAX": 0.05},
    )

    assert worker.returncode == 0, worker.stderr
    # A budget of 0 gives one attempt, as it did in 1.4.0.
    for label, claims in (("zero", 1), ("two", 2), ("five", 5)):
        stored = row(bare[label])
        assert (stored.status, stored.attempts, stored.max_attempts) == (
            "FAILED",
            claims,
            budgets[label],
        ), label
        assert len(stored.errors) == claims
        # A retry writes run_after, which the result's routing comes from.
        routed = fail_always
        if stored.run_after is not None:
            routed = fail_always.using(run_after=stored.run_after)
        fetched = default_task_backend.get_result(bare[label].id).task
        assert_is_the_declared_task(fetched, routed)
        assert fetched.using(priority=5).priority == 5
    stored = row(declared)
    assert (stored.status, stored.attempts, stored.max_attempts) == ("FAILED", 1, 1)
    assert_is_the_declared_task(
        default_task_backend.get_result(declared.id).task,
        policy_tasks.fails_three_times,
    )


def test_reenqueueing_after_an_operator_retry_stores_the_task_or_backend_budget(
    settings,
):
    """
    An operator retry moves a row's budget to attempts + 1. Re-enqueueing its
    result's task stores what that task declares, or, declaring nothing, the
    backend's budget at that moment: never the retried row's.
    """
    inherits = fail_always.enqueue()
    declares = policy_tasks.fails_three_times.enqueue()
    first = run_worker(
        "--max-tasks", "6", options={"BACKOFF_INITIAL": 0.05, "BACKOFF_MAX": 0.05}
    )
    assert first.returncode == 0, first.stderr
    for result in (inherits, declares):
        assert (row(result).status, row(result).attempts) == ("FAILED", 3)
        assert actions.retry(result.id) is True
        assert row(result).max_attempts == 4

    second = run_worker("--max-tasks", "2")
    assert second.returncode == 0, second.stderr
    for result in (inherits, declares):
        stored = row(result)
        assert (stored.status, stored.attempts, stored.max_attempts) == (
            "FAILED",
            4,
            4,
        )

    settings.TASKS = tasks_setting(MAX_ATTEMPTS=7)
    backend = task_backends["default"]
    again = backend.get_result(inherits.id).task.enqueue()
    assert row(again).max_attempts == 7
    again = backend.get_result(declares.id).task.enqueue()
    assert row(again).max_attempts == 3


@pytest.mark.parametrize("kind", ["cancelled", "base"])
def test_a_backoff_raising_a_base_exception_falls_back_on_the_pool(policy_log, kind):
    """
    asyncio.CancelledError, or a BaseException of the application's own,
    raised by a backoff on a pool thread: logged as task_policy_error, the
    worker's backoff decides, and the task's own error is recorded once per
    attempt, with no wait for the lease to expire, until the budget is spent.
    """
    result = policy_tasks.fails_with_a_base_exception_backoff.enqueue(kind=kind)

    worker = run_worker(
        "--max-tasks",
        "3",
        options={"BACKOFF_INITIAL": 0.2, "BACKOFF_MAX": 0.2},
        policy_log=policy_log,
        log_format=EVENT_FORMAT,
        timeout=60,
    )

    assert worker.returncode == 0, worker.stderr
    # Three attempts with 0.2s between them, well inside the 300s lease a
    # stranded row would wait out before the reaper took it back.
    assert worker.elapsed < 30, worker.elapsed
    stored = row(result)
    assert (stored.status, stored.attempts, stored.max_attempts) == ("FAILED", 3, 3)
    assert [e["exception_class_path"] for e in stored.errors] == [
        "builtins.ValueError"
    ] * 3
    for error in stored.errors:
        assert f"the task's own failure ({kind})" in error["traceback"]
        assert "raised by the backoff callback" not in error["traceback"]
    assert notes(policy_log) == [
        {"callback": "base", "kind": kind, "attempts": 1},
        {"callback": "base", "kind": kind, "attempts": 2},
    ]
    logged = logged_events(worker.stderr)
    assert logged.count("task_policy_error") == 2, worker.stderr
    assert logged.count("task_retrying") == 2, worker.stderr
    assert logged.count("task_failed") == 1, worker.stderr
    assert "worker_error" not in logged, worker.stderr
    assert "task_reclaimed" not in logged, worker.stderr
    assert worker.stderr.count("retrying in 0.2s") == 2, worker.stderr
    assert "raised by the backoff callback" in worker.stderr
