"""Module-level task functions; django.tasks requires module-level definitions."""

import time

from django.tasks import task

# Mutable per-test state, reset by the `task_state` fixture.
STATE: dict[str, object] = {}


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
