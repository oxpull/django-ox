"""
Soak task bodies. Every execution writes phase rows to the soak_ledger
side table, which is the measurement instrument for at-least-once vs
exactly-once behavior:

- a "started" row at body entry, a "completed" or "failed" row at body
  exit, all three sharing a per-execution nonce (uuid4);
- a started row with no exit row is an execution that died mid-body
  (SIGKILL), because rows are written on the worker thread's own
  autocommit connection and survive the process dying immediately after;
- more than one "completed" row for a task is a measured double
  execution, not an assumed one.

The ledger table itself is created by soak.py (plain SQL, no Django
model), so nothing here touches the package's migrations.
"""

import os
import time
import uuid

from django.db import connection
from django.tasks import task

LEDGER_INSERT = (
    "INSERT INTO soak_ledger (task_id, attempt, nonce, pid, phase) "
    "VALUES (%s, %s, %s, %s, %s)"
)


def _record(context, nonce, phase):
    with connection.cursor() as cursor:
        cursor.execute(
            LEDGER_INSERT,
            [str(context.task_result.id), context.attempt, nonce, os.getpid(), phase],
        )


def _sleep_task(context, seconds):
    nonce = uuid.uuid4().hex
    _record(context, nonce, "started")
    time.sleep(seconds)
    _record(context, nonce, "completed")
    return nonce


@task(takes_context=True)
def quick(context, seconds=0.02):
    """Short I/O-shaped task; the bulk of the mix."""
    return _sleep_task(context, seconds)


@task(takes_context=True)
def medium(context, seconds=0.2):
    """Mid-length task, e.g. an outbound HTTP call."""
    return _sleep_task(context, seconds)


@task(takes_context=True)
def slow(context, seconds=1.5):
    """Long task; still well under LOCK_TIMEOUT."""
    return _sleep_task(context, seconds)


@task(takes_context=True)
def slow_hold(context, seconds=8.0):
    """
    Crash-window fixture: sleeps long enough that the orchestrator can
    SIGKILL the worker while this is claimed and unfinished.
    """
    return _sleep_task(context, seconds)


@task(takes_context=True)
def flaky_once(context):
    """Fails its first attempt, succeeds on any retry."""
    nonce = uuid.uuid4().hex
    _record(context, nonce, "started")
    if context.attempt < 2:
        _record(context, nonce, "failed")
        raise RuntimeError("flaky_once failing on attempt 1")
    _record(context, nonce, "completed")
    return nonce


@task(takes_context=True)
def doomed(context):
    """Fails every attempt; must end FAILED with attempts == max_attempts."""
    nonce = uuid.uuid4().hex
    _record(context, nonce, "started")
    _record(context, nonce, "failed")
    raise RuntimeError(f"doomed attempt {context.attempt}")
