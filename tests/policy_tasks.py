"""
Tasks that declare per-task policy, for the policy tests.

A module of their own because Django 6.0's ``task()`` accepts no extra
keyword arguments: ``@task(max_attempts=...)`` raises TypeError there at
import, and tests/tasks.py is imported by every test. On 6.0 the same tasks
are built with ``dataclasses.replace`` on a bare ``@task``, which gives the
worker the same PolicyTask to run, so the worker's handling is exercised on
every supported version while the decorator spelling is exercised where it
exists.

Tasks started in a worker subprocess report what they saw by appending JSON
lines to the file named by OX_TEST_POLICY_LOG.
"""

import asyncio
import dataclasses
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import django

import django_ox
from django_ox.compat import HAS_CORE_TASKS, task
from django_ox.timeouts import MAX_SECONDS

from .tasks import STATE, _busy

#: Whether @task forwards keyword arguments to the backend's task class.
#: Django 6.1 and the 5.2 backport do; Django 6.0 does not.
DECORATOR_TAKES_POLICY = not HAS_CORE_TASKS or django.VERSION >= (6, 1)


def policy_task(**policy):
    """``@task(**policy)`` where the decorator takes it, else the same task."""

    def wrap(func):
        if DECORATOR_TAKES_POLICY:
            return task(**policy)(func)
        routing = {
            k: policy.pop(k) for k in ("queue_name", "takes_context") if k in policy
        }
        return dataclasses.replace(task(**routing)(func), **policy)

    return wrap


def note(**fields):
    """One JSON line to OX_TEST_POLICY_LOG, or to STATE in-process."""
    path = os.environ.get("OX_TEST_POLICY_LOG")
    if path is None:
        STATE.setdefault("notes", []).append(fields)
        return
    with Path(path).open("a") as log:
        log.write(json.dumps(fields) + "\n")


def notes(path):
    """Every line a worker subprocess wrote to `path`."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def retry_now(exc, task_result):
    return 0


def describe_and_wait_an_hour(exc, task_result):
    """Record the snapshot the worker passed, then ask for an hour."""
    note(
        callback="describe",
        exception=type(exc).__qualname__,
        message=str(exc),
        status=str(task_result.status),
        attempts=task_result.attempts,
        worker_ids=len(task_result.worker_ids),
        errors=[error.exception_class_path for error in task_result.errors],
        finished=task_result.finished_at is not None,
        task_type=type(task_result.task).__qualname__,
        max_attempts=task_result.task.max_attempts,
        task_id=task_result.id,
    )
    return timedelta(hours=1)


def wait_two_hours_in_seconds(exc, task_result):
    return 7200


def stop_retrying(exc, task_result):
    note(callback="stop", attempts=task_result.attempts)


class _Awaitable:
    def __await__(self):
        yield


async def _never_awaited():
    return 5


#: What misbehaving_backoff answers, by the mode the task was called with.
MISBEHAVIOURS = {
    "raises": None,
    "negative": -1,
    "float": 2.5,
    "bool": True,
    "string": "10",
    "negative_timedelta": timedelta(seconds=-1),
    "too_long": int(MAX_SECONDS) + 1,
    "coroutine": None,
    "awaitable": None,
}


def misbehaving_backoff(exc, task_result):
    mode = task_result.kwargs["mode"]
    note(callback="misbehaving", mode=mode)
    if mode == "raises":
        raise RuntimeError("the backoff callback broke")
    if mode == "coroutine":
        return _never_awaited()
    if mode == "awaitable":
        return _Awaitable()
    return MISBEHAVIOURS[mode]


class CallbackEscape(BaseException):
    """A BaseException that is not an Exception, as an application may define."""


def raise_a_base_exception(exc, task_result):
    """Raise what ``except Exception`` does not catch, by the task's kind."""
    kind = task_result.kwargs["kind"]
    note(callback="base", kind=kind, attempts=task_result.attempts)
    if kind == "cancelled":
        raise asyncio.CancelledError("raised by the backoff callback")
    raise CallbackEscape("raised by the backoff callback")


@policy_task(max_attempts=1, backoff=retry_now)
def fails_with_budget_one():
    raise ValueError("budget one")


@policy_task(max_attempts=4, backoff=retry_now)
def fails_with_budget_four():
    raise ValueError("budget four")


@policy_task(max_attempts=3, backoff=describe_and_wait_an_hour)
def fails_and_waits_an_hour():
    raise ValueError("wait an hour")


@policy_task(max_attempts=3, backoff=wait_two_hours_in_seconds)
def fails_and_waits_two_hours():
    raise ValueError("wait two hours")


@policy_task(max_attempts=5, backoff=stop_retrying)
def fails_and_stops():
    raise ValueError("stop now")


@policy_task(max_attempts=5, backoff=misbehaving_backoff)
def fails_with_a_misbehaving_backoff(mode):
    raise ValueError(f"the task's own failure ({mode})")


@policy_task(max_attempts=5, backoff=describe_and_wait_an_hour)
def moved_away():
    raise AssertionError("the row points elsewhere, so this never runs")


@policy_task(max_attempts=1, timeout=1)
def spins_past_its_timeout(seconds):
    _busy(seconds)
    return "finished"


@policy_task(max_attempts=1, timeout=1)
async def awaits_past_its_timeout(seconds):
    await asyncio.sleep(seconds)
    return "finished"


@policy_task(max_attempts=1, timeout=1, queue_name="emails")
def spins_past_its_timeout_on_emails(seconds):
    _busy(seconds)
    return "finished"


@task(queue_name="emails")
def spins_on_emails(seconds):
    _busy(seconds)
    return "finished"


@policy_task(max_attempts=2, timeout=3, backoff=retry_now)
def reports_its_deadline_then_fails(pause):
    note(
        attempt="deadline",
        remaining=django_ox.remaining(),
        deadline=django_ox.deadline().timestamp(),
    )
    time.sleep(pause)
    raise ValueError("after the pause")


@policy_task(max_attempts=3, backoff=retry_now)
def fails_three_times():
    raise ValueError("three")


@policy_task(max_attempts=1)
def succeeds_with_budget_one():
    return "one"


@policy_task(max_attempts=2)
def succeeds_with_budget_two():
    return "two"


@policy_task(timeout=5)
def reports_deadline_in_process():
    STATE["remaining"] = django_ox.remaining()
    STATE["deadline"] = django_ox.deadline()
    return "reported"


@policy_task(max_attempts=5, timeout=7, backoff=retry_now)
def declares_everything(value):
    return value


@policy_task(max_attempts=2, backoff=describe_and_wait_an_hour)
def fails_and_describes():
    raise ValueError("described")


@policy_task(max_attempts=3, backoff=raise_a_base_exception)
def fails_with_a_base_exception_backoff(kind):
    raise ValueError(f"the task's own failure ({kind})")
