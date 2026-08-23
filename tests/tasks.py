"""Module-level task functions; django.tasks requires module-level definitions."""

import asyncio
import time

from django.db import transaction
from django.tasks import task

import django_ox
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask

# Mutable per-test state, reset by the `task_state` fixture.
STATE: dict[str, object] = {}


def _spin(seconds, step=0.005):
    """Sleep in short slices: a loop that keeps returning to bytecode."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        time.sleep(step)


def _busy(seconds):
    """A pure Python loop with no C call in it to hide inside."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        pass


@task
def add(a, b):
    return a + b


@task
def echo(value):
    return value


@task
def fail_always():
    raise ValueError("boom")


@task
def flaky(succeed_on):
    calls = STATE.get("flaky_calls", 0) + 1
    STATE["flaky_calls"] = calls
    if calls < succeed_on:
        raise RuntimeError(f"failing on call {calls}")
    return calls


@task
def record(label):
    STATE.setdefault("order", []).append(label)
    return label


@task
def slow(seconds):
    time.sleep(seconds)
    return "done"


@task
def record_interval(seconds):
    start = time.monotonic()
    time.sleep(seconds)
    STATE.setdefault("intervals", []).append((start, time.monotonic()))
    return "done"


@task(queue_name="emails")
def send_email(to):
    return f"sent to {to}"


@task(takes_context=True)
def with_context(context):
    return {"attempt": context.attempt, "id": context.task_result.id}


@task
async def async_add(a, b):
    return a + b


# -- timeout fixtures -------------------------------------------------------


@task
def spin(seconds):
    """Runs for `seconds` in Python, so an injected TaskTimeout can land."""
    try:
        _spin(seconds)
    except TaskTimeout:
        STATE["caught"] = True
        raise
    finally:
        STATE["finally_ran"] = True
    return "done"


@task
def busy(seconds):
    _busy(seconds)
    return "done"


@task
def swallow_timeout(seconds):
    try:
        _spin(seconds)
    except TaskTimeout:
        return "cleaned up"
    return "done"


@task
def raise_timeout():
    raise TaskTimeout("raised by the task itself", timeout=1.5)


@task(takes_context=True)
def spin_in_atomic(context, seconds):
    """Hold a write lock for `seconds` inside one transaction."""
    with transaction.atomic():
        OxTask.objects.filter(pk=context.task_result.id).update(priority=7)
        STATE["writer_locked"] = True
        _spin(seconds)
    return "committed"


@task(takes_context=True)
def write_loop(context, seconds):
    """Write to the task's own row as fast as it can, for `seconds`."""
    attempt = context.attempt
    writes = 0
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        writes += 1
        OxTask.objects.filter(pk=context.task_result.id).update(
            return_value={"written_by": attempt, "writes": writes}
        )
        STATE["writes"] = writes
    return "loop done"


@task
def query_then_spin(seconds):
    """One ORM query (opening this thread's connection), then a Python loop."""
    OxTask.objects.count()
    _spin(seconds)
    return "done"


@task
def report_deadline():
    at = django_ox.deadline()
    return {
        "deadline": None if at is None else at.isoformat(),
        "remaining": django_ox.remaining(),
    }


@task
async def async_spin(seconds):
    STATE["loop"] = asyncio.get_running_loop()
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        STATE["cancelled"] = True
        raise
    return "done"


@task
async def async_report_deadline():
    return django_ox.remaining()


@task(takes_context=True)
def atomic_write_loop(context, seconds):
    """One short transaction per write, as fast as it can, for `seconds`."""
    attempt = context.attempt
    writes = 0
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        writes += 1
        with transaction.atomic():
            OxTask.objects.filter(pk=context.task_result.id).update(
                return_value={"written_by": attempt, "writes": writes}
            )
    return "loop done"


@task
def swallow_then_run_on(seconds, after):
    """Catches the timeout, then keeps running Python for `after` seconds."""
    try:
        _spin(seconds)
    except TaskTimeout:
        STATE["caught"] = True
        _spin(after)
        STATE["ran_on"] = True
        return "finished after the timeout"
    return "done"


@task
async def async_catch_timeout(seconds):
    try:
        await asyncio.sleep(seconds)
    except TaskTimeout:
        return "caught TaskTimeout"
    except asyncio.CancelledError:
        STATE["cancelled"] = True
        raise
    return "done"
