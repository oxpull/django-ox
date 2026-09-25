"""
Tasks whose results are compared with the tasks themselves.

A TaskResult's task is the task as declared, with the routing it was enqueued
with, whichever path built the result. These are the cases the comparison
covers: a bare task that succeeds, one that fails, and tasks that declare only
a backoff or only a timeout.

Receivers for task_enqueued and task_finished connect when this module is
imported, which in a worker subprocess is when the worker imports a task from
it to run. Each notes only this module's tasks: in process, the TaskResult
itself to STATE["signalled"]; in a subprocess that sets OX_TEST_POLICY_LOG,
one JSON line saying whether the task compared and hashed equal to the one
this module declares.
"""

import json
import os
from pathlib import Path

from django_ox.compat import task, task_enqueued, task_finished

from .policy_tasks import policy_task, retry_now
from .tasks import STATE


@task
def bare_succeeds():
    return "bare"


@task
def bare_fails():
    raise ValueError("bare")


@policy_task(backoff=retry_now)
def backoff_only_fails():
    raise ValueError("backoff only")


@policy_task(timeout=30)
def timeout_only_succeeds():
    return "timeout only"


def declared_for(task_result):
    """This module's task that `task_result` ran, or None for any other."""
    func = task_result.task.func
    if getattr(func, "__module__", None) != __name__:
        return None
    return globals()[func.__name__]


def _record(signal, task_result):
    declared = declared_for(task_result)
    if declared is None:
        return
    path = os.environ.get("OX_TEST_POLICY_LOG")
    if path is None:
        STATE.setdefault("signalled", []).append((signal, task_result))
        return
    got = task_result.task
    # A retry writes the row's run_after, and the routing comes from the row,
    # so a task that retried is compared with its declaration routed alike.
    if got.run_after != declared.run_after:
        declared = declared.using(run_after=got.run_after)
    line = {
        "signal": signal,
        "task": got.func.__name__,
        "status": str(task_result.status),
        "class": type(got).__qualname__,
        "declared_class": type(declared).__qualname__,
        "equal": got == declared,
        "same_hash": hash(got) == hash(declared),
        "run_after_from_row": got.run_after is not None,
    }
    with Path(path).open("a") as log:
        log.write(json.dumps(line) + "\n")


def on_enqueued(sender, task_result, **kwargs):
    _record("enqueued", task_result)


def on_finished(sender, task_result, **kwargs):
    _record("finished", task_result)


task_enqueued.connect(on_enqueued, dispatch_uid="tests.signal_tasks.enqueued")
task_finished.connect(on_finished, dispatch_uid="tests.signal_tasks.finished")
