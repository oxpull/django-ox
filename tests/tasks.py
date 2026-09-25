"""Module-level task functions; django.tasks requires module-level definitions."""

import asyncio
import time
from pathlib import Path

from django.db import transaction

import django_ox
from django_ox.compat import task
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
def labelled(label, **kwargs):
    """Returns its label. A schedule names itself with it, so a task row says
    which schedule enqueued it."""
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


@task(takes_context=True)
def write_until_released(context):
    """Write to the task's own row as fast as it can until STATE["release"]."""
    attempt = context.attempt
    writes = 0
    release = STATE["release"]
    while not release.is_set():
        writes += 1
        OxTask.objects.filter(pk=context.task_result.id).update(
            return_value={"written_by": attempt, "writes": writes}
        )
    return "released"


@task
def query_then_hold():
    """One ORM query (opening this thread's connection), then hold for release."""
    OxTask.objects.count()
    release = STATE["release"]
    while not release.is_set():
        time.sleep(0.005)
    return "released"


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
def atomic_write_until_released(context):
    """One short transaction per write until the test sets STATE["release"]."""
    attempt = context.attempt
    writes = 0
    release = STATE["release"]
    while not release.is_set():
        writes += 1
        with transaction.atomic():
            OxTask.objects.filter(pk=context.task_result.id).update(
                return_value={"written_by": attempt, "writes": writes}
            )
    return "released"


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


@task()
def failing_with_long_traceback():
    """A failure whose traceback is far larger than the row it lands in."""
    raise ValueError("x" * 200_000)


def _mark(log_path, line):
    with Path(log_path).open("a") as log:
        log.write(f"{line}\n")


@task
def query_and_hold(log_path, release_path):
    """
    One ORM query, which takes this thread's connection, then keep it until
    release_path exists. Appends START, HELD and END to log_path, so a test in
    another process can see when the connection is taken and count how many
    times the body ran. Lets go after a minute regardless, so a test that
    fails before releasing it does not leave a worker holding on.
    """
    _mark(log_path, "START")
    OxTask.objects.count()
    _mark(log_path, "HELD")
    deadline = time.monotonic() + 60
    while not Path(release_path).exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    _mark(log_path, "END")
    return "released"


@task
def query_then_sleep(log_path, seconds):
    """
    One ORM query, then one sleep that TaskTimeout cannot interrupt: it lands
    at the next line of Python, and time.sleep() does not return to Python
    until it is over. The thread keeps its connection throughout.
    """
    _mark(log_path, "START")
    OxTask.objects.count()
    _mark(log_path, "HELD")
    time.sleep(seconds)
    _mark(log_path, "END")
    return "done"


@task
def enqueue_follow_up(label):
    """Enqueue another task from inside one, as a fan-out job does."""
    echo.enqueue(label)
    return label


@task
def enqueue_follow_up_once_released(label):
    """
    enqueue_follow_up, held until the test sets STATE["release"], so the test
    decides the moment the follow-up appears. Gives up after ten seconds
    rather than hold a worker thread for a test that never lets it go.
    """
    if not STATE["release"].wait(10):
        raise TimeoutError("the test never released this task")
    echo.enqueue(label)
    return label
