"""
Task body for the worker-death harness (killbench.py). One module for both
arms: django-tasks-db 0.13.0 rides Django core's django.tasks, so the
decorator, the body and the stored task path are identical.

Every execution writes two rows to the kill_ledger side table through the
process's own Django connection, in autocommit:

- "started" at body entry: execution nonce (uuid4), the logical task id,
  pid, the harness's worker name, the attempt number;
- "effect" with the same nonce, committed just before the body returns.

A kill between the two rows leaves a started row with no effect: a lost
partial execution. A kill after the effect row but before the backend's
outcome write leaves a completed effect on a row the backend still sees as
in flight; whatever the backend does next is measured by counting effect
rows per logical task. The table is created by killbench.py in plain SQL,
so neither backend knows it, prunes it or truncates it.
"""

import os
import time
import uuid

from django.db import connection
from django.tasks import task

LEDGER_INSERT = (
    "INSERT INTO kill_ledger (task_id, nonce, pid, worker_name, attempt, phase) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)


def _record(context, nonce, phase):
    # The ledger only measures anything if each row commits on its own. A
    # backend that ran the body inside a transaction would hold both rows
    # until the outcome write, and a kill would erase the evidence along
    # with the work. Refuse to run in that case rather than report zeros.
    if connection.in_atomic_block:
        raise RuntimeError(
            "task body is inside a transaction; ledger rows would not commit"
        )
    with connection.cursor() as cursor:
        cursor.execute(
            LEDGER_INSERT,
            [
                str(context.task_result.id),
                nonce,
                os.getpid(),
                os.environ.get("KILLBENCH_WORKER", ""),
                context.attempt,
                phase,
            ],
        )


@task(takes_context=True)
def sleep_and_record(context, seconds=0.1):
    nonce = uuid.uuid4().hex
    _record(context, nonce, "started")
    time.sleep(seconds)
    _record(context, nonce, "effect")
    return nonce
