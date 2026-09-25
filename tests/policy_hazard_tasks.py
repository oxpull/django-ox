"""
Tasks for the adversarial policy tests in test_policy_hazards.py.

Every body says which claim is running it, by appending JSON lines to the
file named by OX_TEST_POLICY_LOG (see policy_tasks.note): the task id, the
attempt, the worker id the claim appended, the process and thread, the lease
epoch the row held when the body started, and the wall-clock time, which every
process on the machine shares. The tests hold the claim histories and outcome
writes on the rows up against those records, so a second invocation, an
overlap or a write from the wrong epoch shows up as a record that should not
be there rather than as a final status that happens to look right.

A body that has to wait for the test waits on a gate: a file beside the log
that the test creates. Every wait has a limit, so a gate that is never opened
costs a bounded wait rather than a hung worker, and the test then fails on
what the body recorded.
"""

import json
import os
import threading
import time
from contextlib import suppress
from datetime import timedelta
from pathlib import Path

from django.db import DatabaseError, connections, transaction

import django_ox
from django_ox.compat import task
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask

from .dead_connection_tasks import end_connection
from .policy_tasks import note, policy_task, retry_now
from .tasks import _busy

#: The longest any body waits for a gate or for its siblings. Far past what
#: any of these tests needs, so reaching it means the scenario did not happen.
GATE_LIMIT = 60.0

#: How many concurrent attempts the mixed-policy test runs in one worker.
MIXED = 6


def _log_path():
    return Path(os.environ["OX_TEST_POLICY_LOG"])


def gate(directory, name):
    """The file that opens gate `name`, for a log in `directory`."""
    return Path(directory) / f"gate-{name}"


def read_notes(path):
    """
    Every complete JSON line in `path`. A line another process is still
    appending is skipped: this is read while workers run.
    """
    found = []
    with suppress(FileNotFoundError):
        for line in Path(path).read_text().splitlines():
            with suppress(ValueError):
                found.append(json.loads(line))
    return found


def _wait(predicate, limit=GATE_LIMIT):
    """Sleep in short slices until `predicate()`; whether it came true."""
    end = time.monotonic() + limit
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _wait_for_gate(name, limit=GATE_LIMIT):
    path = gate(_log_path().parent, name)
    return _wait(path.exists, limit)


def _open_gate(name):
    gate(_log_path().parent, name).touch()


def _epoch(task_id):
    return OxTask.objects.filter(id=task_id).values_list("lease_epoch", flat=True).get()


def _claim(context, event="start", **fields):
    """Record which claim is running this body, and return the record."""
    result = context.task_result
    record = {
        "event": event,
        "task": result.id,
        "attempt": context.attempt,
        "worker_id": result.worker_ids[-1],
        "pid": os.getpid(),
        "thread": threading.get_ident(),
        "epoch": _epoch(result.id),
        "at": time.time(),
        **fields,
    }
    note(**record)
    return record


def _end(context, **fields):
    note(
        event="end",
        task=context.task_result.id,
        attempt=context.attempt,
        pid=os.getpid(),
        at=time.time(),
        **fields,
    )


def _drop_connections():
    """
    Close this thread's connections, whatever state a TaskTimeout left them
    in. The exception lands at the next bytecode, which can be inside the
    driver's own Python between sending a statement and reading its result.
    """
    for conn in connections.all(initialized_only=True):
        with suppress(Exception):
            conn.close()


# -- backoff callbacks --------------------------------------------------------


def _callback_note(callback, exc, task_result, **fields):
    note(
        event="callback",
        callback=callback,
        task=task_result.id,
        attempts=task_result.attempts,
        status=str(task_result.status),
        exception=type(exc).__qualname__,
        errors=[error.exception_class_path for error in task_result.errors],
        max_attempts=task_result.task.max_attempts,
        pid=os.getpid(),
        thread=threading.get_ident(),
        at=time.time(),
        **fields,
    )


def note_and_wait_an_hour(exc, task_result):
    _callback_note("hour", exc, task_result)
    return timedelta(hours=1)


def note_and_retry_now(exc, task_result):
    _callback_note("now", exc, task_result)
    return 0


def note_and_decline(exc, task_result):
    _callback_note("decline", exc, task_result)


def sleep_then_wait_an_hour(exc, task_result):
    """A callback that outlasts several lease periods before it answers."""
    _callback_note("slow", exc, task_result, phase="called")
    time.sleep(7)
    _callback_note("slow", exc, task_result, phase="answered")
    return timedelta(hours=1)


def query_then_wait_an_hour(exc, task_result):
    """Use the database from the callback, on the attempt's own thread."""
    connection = connections["default"]
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid()")
        (backend_pid,) = cursor.fetchone()
    status, epoch = (
        OxTask.objects.filter(id=task_result.id)
        .values_list("status", "lease_epoch")
        .get()
    )
    _callback_note(
        "query",
        exc,
        task_result,
        backend_pid=backend_pid,
        row_status=status,
        row_epoch=epoch,
    )
    return timedelta(hours=1)


def _end_connection_from_the_callback(task_result):
    """
    Have the server end this thread's default connection from inside the
    backoff, and note whether Django flagged it, then let the error out.
    """
    try:
        end_connection()
    except DatabaseError as error:
        note(
            event="callback-ended",
            task=task_result.id,
            caught=type(error).__qualname__,
            flagged=connections["default"].errors_occurred,
            thread=threading.get_ident(),
        )
        raise


def end_connection_then_wait_an_hour(exc, task_result):
    """End the connection, catch the error, and answer a delay all the same."""
    _callback_note("ends-then-hour", exc, task_result)
    with suppress(DatabaseError):
        _end_connection_from_the_callback(task_result)
    return timedelta(hours=1)


def end_connection_then_raise(exc, task_result):
    """End the connection and let the error the statement got out."""
    _callback_note("ends-then-raises", exc, task_result)
    _end_connection_from_the_callback(task_result)
    raise AssertionError("the statement above ends the callback")


def end_connection_then_decline(exc, task_result):
    """End the connection, catch the error, and answer None: FAILED now."""
    _callback_note("ends-then-declines", exc, task_result)
    with suppress(DatabaseError):
        _end_connection_from_the_callback(task_result)


def lock_row_then_wait_an_hour(exc, task_result):
    """
    Take the row lock the task body held when its timeout struck. NOWAIT, so
    a transaction the timeout left open on some connection fails this at once
    rather than blocking the attempt's thread.
    """
    with transaction.atomic():
        (status,) = (
            OxTask.objects.select_for_update(nowait=True)
            .filter(id=task_result.id)
            .values_list("status", flat=True)
        )
    _callback_note("lock", exc, task_result, row_status=status)
    return timedelta(hours=1)


# -- two workers claiming one queue ------------------------------------------


def _two_workers_ran():
    return len({record["pid"] for record in read_notes(_log_path())}) >= 2


def _race(context, fail_until):
    _claim(context)
    # By construction, not by luck: no body lets its slot go until a second
    # process has run one too, so both workers are claiming from the same
    # rows at the same time for the whole test.
    both = _wait(_two_workers_ran)
    time.sleep(0.05)
    _end(context, both=both)
    if context.attempt <= fail_until:
        raise ValueError(f"failing attempt {context.attempt}")
    return context.attempt


@policy_task(takes_context=True, max_attempts=2, backoff=retry_now)
def race_succeeds(context, n):
    return _race(context, fail_until=0)


@policy_task(takes_context=True, max_attempts=2, backoff=retry_now)
def race_fails_once(context, n):
    return _race(context, fail_until=1)


@policy_task(takes_context=True, max_attempts=3, backoff=retry_now)
def race_fails_thrice(context, n):
    return _race(context, fail_until=3)


@policy_task(takes_context=True, max_attempts=1)
def race_fails_once_for_good(context, n):
    return _race(context, fail_until=1)


# -- a timeout longer than the lease ------------------------------------------


@policy_task(takes_context=True, max_attempts=1, timeout=30)
def outlives_its_lease(context, seconds):
    """Watch the row's lease while running for several lease periods."""
    record = _claim(context)
    samples = []
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        locked_by, expires = (
            OxTask.objects.filter(id=record["task"])
            .values_list("locked_by", "lease_expires_at")
            .get()
        )
        samples.append([locked_by, expires.timestamp()])
        time.sleep(0.2)
    _end(context, samples=samples)
    return "outlived"


@policy_task(takes_context=True, max_attempts=2, timeout=7, backoff=retry_now)
def times_out_past_its_lease(context):
    _claim(context)
    if context.attempt > 1:
        _end(context)
        return "second"
    try:
        _busy(60)
    finally:
        _end(context)
    return "never"


@policy_task(takes_context=True, max_attempts=2, backoff=sleep_then_wait_an_hour)
def fails_into_a_slow_backoff(context):
    _claim(context)
    raise ValueError("into a slow backoff")


# -- an owner that comes back after losing its lease --------------------------


@policy_task(takes_context=True, max_attempts=3, backoff=note_and_wait_an_hour)
def late_owner(context):
    """
    Each attempt is held at a gate of its own: the first, the owner's, at
    late-owner, and the taker's at late-owner-taker, so the test decides
    which of them is running when the other writes.
    """
    _claim(context)
    if context.attempt > 1:
        opened = _wait_for_gate("late-owner-taker")
        _end(context, opened=opened)
        return "second"
    opened = _wait_for_gate("late-owner")
    _end(context, opened=opened)
    raise ValueError("the late owner's failure")


@policy_task(takes_context=True, max_attempts=1, backoff=note_and_wait_an_hour)
def answers_after_lost(context, name):
    """
    The first attempt is held at gate `name`, then fails; a retry is held at
    `name`-taker, then succeeds.
    """
    _claim(context)
    if context.attempt > 1:
        opened = _wait_for_gate(f"{name}-taker")
        _end(context, opened=opened)
        return "retried"
    opened = _wait_for_gate(name)
    _end(context, opened=opened)
    raise ValueError(f"the first owner's failure ({name})")


# -- a timeout the task swallows ----------------------------------------------


@policy_task(
    takes_context=True, max_attempts=2, timeout=1, backoff=note_and_wait_an_hour
)
def swallows_its_timeout(context):
    """
    Catch TaskTimeout and keep going until the row's lease epoch moves, which
    is the watchdog's grace backstop taking the attempt away; then return, as
    a thread that finally came back from a long call would.
    """
    record = _claim(context)
    if context.attempt > 1:
        _end(context)
        return "second"
    end = time.monotonic() + GATE_LIMIT
    while time.monotonic() < end:
        try:
            time.sleep(0.25)
            epoch = _epoch(record["task"])
            if epoch != record["epoch"]:
                note(
                    event="saw_epoch", task=record["task"], epoch=epoch, at=time.time()
                )
                _end(context)
                _open_gate("swallower-returned")
                return "late success"
        except TaskTimeout:
            note(event="swallowed", task=record["task"], at=time.time())
            _drop_connections()
    _open_gate("swallower-returned")
    return "gave up"


@policy_task(takes_context=True, max_attempts=1)
def holds_the_drain(context):
    """Keep a recycling worker draining until the swallower has returned."""
    _claim(context)
    opened = _wait_for_gate("swallower-returned")
    time.sleep(2)
    _end(context, opened=opened)
    return "drained"


@policy_task(
    takes_context=True, max_attempts=2, timeout=2, backoff=note_and_wait_an_hour
)
def stubborn(context):
    """
    Swallow TaskTimeout on every attempt. The first never comes back. The
    second waits at the stubborn-taker gate through its own timeout, which the
    taker's long grace lets it outlive, and then succeeds.
    """
    _claim(context)
    taker_gate = gate(_log_path().parent, "stubborn-taker")
    end = time.monotonic() + GATE_LIMIT
    while time.monotonic() < end:
        try:
            if context.attempt > 1 and taker_gate.exists():
                _end(context, opened=True)
                return "second"
            time.sleep(0.05)
        except TaskTimeout:
            note(
                event="swallowed",
                task=context.task_result.id,
                attempt=context.attempt,
                at=time.time(),
            )
    return "gave up"


# -- concurrent attempts with different policies in one worker ---------------


def _mixed_started():
    records = read_notes(_log_path())
    started = {
        r["task"] for r in records if r.get("event") == "start" and r["attempt"] == 1
    }
    return len(started) >= MIXED


def _mixed(context):
    remaining = django_ox.remaining()
    deadline = django_ox.deadline()
    _claim(
        context,
        remaining=remaining,
        deadline=None if deadline is None else deadline.timestamp(),
    )
    if context.attempt == 1:
        # Every one of them in flight at once, by construction.
        _wait(_mixed_started)


@policy_task(takes_context=True, max_attempts=1, timeout=1)
def mixed_times_out(context):
    try:
        _mixed(context)
        _busy(60)
    finally:
        _end(context)
    return "never"


@policy_task(takes_context=True, max_attempts=1)
def mixed_runs_long(context):
    _mixed(context)
    time.sleep(3)
    _end(context)
    return "long"


@policy_task(takes_context=True, max_attempts=3, backoff=note_and_wait_an_hour)
def mixed_waits_an_hour(context):
    _mixed(context)
    _end(context)
    raise ValueError("wait an hour")


@policy_task(takes_context=True, max_attempts=5, backoff=note_and_decline)
def mixed_declines(context):
    _mixed(context)
    _end(context)
    raise ValueError("declined")


@policy_task(takes_context=True, max_attempts=2, timeout=5, backoff=note_and_retry_now)
def mixed_retries_now(context):
    _mixed(context)
    _end(context)
    if context.attempt == 1:
        raise ValueError("retry now")
    return "second"


@task(takes_context=True)
def mixed_declares_nothing(context):
    _mixed(context)
    _end(context)
    raise ValueError("declares nothing")


# -- one pool thread, one attempt after another -------------------------------


@policy_task(takes_context=True, max_attempts=3, backoff=note_and_wait_an_hour)
def first_on_the_thread(context):
    _claim(context)
    raise ValueError("first on the thread")


@task(takes_context=True)
def next_on_the_thread(context):
    _claim(context)
    raise ValueError("next on the thread")


# -- a callback after the database connection broke --------------------------


def _terminate_own_backend():
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid()")
        (backend_pid,) = cursor.fetchone()
        note(event="backend", pid=os.getpid(), backend_pid=backend_pid)
        cursor.execute("SELECT pg_terminate_backend(pg_backend_pid())")


@policy_task(takes_context=True, max_attempts=3, backoff=query_then_wait_an_hour)
def kills_its_connection(context):
    _claim(context)
    _terminate_own_backend()
    raise AssertionError("the statement above ends this attempt")


@policy_task(takes_context=True, max_attempts=3)
def kills_its_connection_and_declares_no_backoff(context):
    _claim(context)
    _terminate_own_backend()
    raise AssertionError("the statement above ends this attempt")


@policy_task(takes_context=True, max_attempts=1, backoff=query_then_wait_an_hour)
def kills_its_connection_on_its_last_attempt(context):
    _claim(context)
    _terminate_own_backend()
    raise AssertionError("the statement above ends this attempt")


@policy_task(takes_context=True, max_attempts=3, backoff=note_and_wait_an_hour)
def kills_its_connection_and_succeeds(context):
    """Catch the error the dead connection raised, and return all the same."""
    _claim(context)
    try:
        _terminate_own_backend()
    except Exception as exc:
        note(event="caught", exception=type(exc).__qualname__)
    _end(context)
    return "succeeded anyway"


@policy_task(
    takes_context=True, max_attempts=3, timeout=1, backoff=lock_row_then_wait_an_hour
)
def times_out_holding_its_row(context):
    """Hold the row's lock in an open transaction when the timeout strikes."""
    task_id = context.task_result.id
    _claim(context)
    with transaction.atomic():
        OxTask.objects.select_for_update().filter(id=task_id).values_list("id").get()
        note(event="locked", task=task_id, at=time.time())
        _busy(60)
    return "never"


# -- a callback that ends the connection itself -------------------------------


@policy_task(
    takes_context=True, max_attempts=3, backoff=end_connection_then_wait_an_hour
)
def fails_into_a_backoff_that_ends_the_connection_and_waits(context):
    _claim(context)
    raise ValueError("fails before its backoff ends the connection")


@policy_task(takes_context=True, max_attempts=3, backoff=end_connection_then_raise)
def fails_into_a_backoff_that_ends_the_connection_and_raises(context):
    _claim(context)
    raise ValueError("fails before its backoff ends the connection")


@policy_task(takes_context=True, max_attempts=3, backoff=end_connection_then_decline)
def fails_into_a_backoff_that_ends_the_connection_and_declines(context):
    _claim(context)
    raise ValueError("fails before its backoff ends the connection")
