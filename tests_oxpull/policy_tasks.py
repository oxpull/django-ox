"""
Tasks that declare django-ox's per-task policy, for tests_oxpull/test_policy.py.

A module of their own because Django 6.0's ``task()`` takes no keyword
arguments beyond its own: ``@task(max_attempts=...)`` raises TypeError there
at import, and tests_oxpull/tasks.py is imported by nearly every test. On 6.0 each
task is built with ``dataclasses.replace`` on a bare ``@task`` instead. That
binds the same PolicyTask to the module-level name the worker imports, so the
worker's handling is exercised on both supported versions, while the
decorator spelling is exercised where it exists.
"""

import dataclasses
import time
from datetime import timedelta

import django
from django.tasks import task

from django_ox import remaining
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.unique import unique

from .tasks import STATE, _record

#: Whether @task forwards keyword arguments to the backend's task class.
#: Django 6.1 does; 6.0, the other version this package supports, does not.
DECORATOR_TAKES_POLICY = django.VERSION >= (6, 1)


def policy_task(**declared):
    """``@task(**declared)`` where the decorator takes policy, else the same task."""

    def wrap(func):
        if DECORATOR_TAKES_POLICY:
            return task(**declared)(func)
        routing = {
            name: declared.pop(name)
            for name in ("backend", "queue_name")
            if name in declared
        }
        return dataclasses.replace(task(**routing)(func), **declared)

    return wrap


def note(**fields):
    STATE.setdefault("notes", []).append(fields)


def _busy(seconds):
    """Pure Python, so an injected TaskTimeout lands inside it."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass


# -- backoff callbacks ---------------------------------------------------


def retry_now(exc, task_result):
    return 0


def stop_retrying(exc, task_result):
    # The stored budget is the row's; task_result.task is the declaration.
    note(
        callback="stop",
        attempts=task_result.attempts,
        max_attempts=OxTask.objects.get(pk=task_result.id).max_attempts,
    )


def wait_an_hour(exc, task_result):
    note(callback="hour", attempts=task_result.attempts)
    return timedelta(hours=1)


# -- the default backend ---------------------------------------------------


@unique
@policy_task(max_attempts=5)
def unique_with_budget(value):
    return value


@policy_task(max_attempts=5, backoff=retry_now, timeout=30)
def declares_everything(value):
    return value


@policy_task(max_attempts=4, backoff=retry_now)
def fails_with_budget_four():
    raise ValueError("four")


@policy_task(max_attempts=6)
def callback_with_budget(label):
    STATE[label] = STATE.get(label, 0) + 1
    return label


@policy_task(timeout=8)
def reconciles_under_its_own_timeout():
    """
    One tick of each reconciler inside an attempt that has a timeout of its
    own, and what the workflow backstop budgeted from it.
    """
    STATE["backstop_budget"] = workflows._backstop_budget()
    STATE["reconciled"] = [batches._reconcile(), workflows._reconcile()]
    STATE["remaining_after"] = remaining()


# -- workflow nodes, on "flows" --------------------------------------------


@policy_task(backend="flows", max_attempts=5)
def node_with_budget(label):
    return _record(label)


@policy_task(backend="flows", max_attempts=5, backoff=retry_now)
def failing_node_with_budget_five(label):
    _record(label)
    raise ValueError(f"{label} boom")


@policy_task(backend="flows", max_attempts=5, backoff=stop_retrying)
def failing_node_that_stops(label):
    _record(label)
    raise ValueError(f"{label} boom")


@policy_task(backend="flows", max_attempts=3, backoff=wait_an_hour)
def failing_node_that_waits_an_hour(label):
    _record(label)
    raise ValueError(f"{label} boom")


@policy_task(backend="flows", max_attempts=2, timeout=1)
def node_past_its_timeout(label, seconds):
    _record(label)
    _busy(seconds)
    return label
