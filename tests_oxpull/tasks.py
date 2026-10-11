"""Module-level task functions; django.tasks requires module-level definitions."""

import time

from django.tasks import task

from oxpull.unique import unique

# Mutable per-test state, reset by the `task_state` fixture.
STATE: dict[str, object] = {}


@task
def plain_echo(value):
    return value


@unique
@task
def unique_echo(value):
    return value


@unique
@task
def unique_add(a, b):
    return a + b


@unique
@task
def unique_fail_always():
    raise ValueError("boom")


@unique
@task
def unique_flaky(succeed_on):
    calls = STATE.get("flaky_calls", 0) + 1
    STATE["flaky_calls"] = calls
    if calls < succeed_on:
        raise RuntimeError(f"failing on call {calls}")
    return calls


# Not decorated with @unique; made unique via OPTIONS["UNIQUE_TASKS"] in
# tests_oxpull/settings.py.
@task
def configured_unique(value):
    return value


@task
def plain_fail():
    raise ValueError("boom")


@task
def plain_flaky(succeed_on):
    calls = STATE.get("plain_flaky_calls", 0) + 1
    STATE["plain_flaky_calls"] = calls
    if calls < succeed_on:
        raise RuntimeError(f"failing on call {calls}")
    return calls


@task
def record_callback(label):
    """Batch callback target; records that it ran when a worker runs it."""
    STATE[label] = STATE.get(label, 0) + 1
    return label


# Runs at enqueue time on the immediate backend, so it never writes an
# OxTask row: batch membership must refuse it.
@task(backend="immediate")
def immediate_echo(value):
    return value


# The same, but it records that it ran, so a test can prove a batch
# callback on this backend never executes its side effect.
@task(backend="immediate")
def immediate_record(label):
    STATE[label] = STATE.get(label, 0) + 1
    return label


# On a second queue, so a limit attached to one queue has another queue to
# leave alone.
@task(queue_name="emails")
def emails_echo(value):
    return value


# On the "limited" backend, whose OPTIONS carry a rate limit covering both
# of these. The default backend has none, which is what the zero-cost test
# measures against.
@task(backend="limited")
def limited_echo(value):
    return value


@task(backend="limited")
def limited_add(a, b):
    return a + b


# -- workflow nodes ----------------------------------------------------
#
# On the "flows" backend, whose OPTIONS declare WORKFLOWS and name the
# worker that gates a claim on its dependencies.


def _record(label):
    """Append `label` to the recorded execution order."""
    order = STATE.setdefault("order", [])
    order.append(label)  # type: ignore[attr-defined]
    return label


@task(backend="flows")
def step(label):
    return _record(label)


@task(backend="flows")
def slow_step(label, seconds=0.2):
    time.sleep(seconds)
    return _record(label)


@task(backend="flows")
def failing_step(label):
    _record(label)
    raise ValueError(f"{label} boom")


@task(backend="flows")
def wedged_step(label, seconds=30):
    """Holds its lease long enough for the reaper to take it away."""
    _record(label)
    time.sleep(seconds)
    return label


# On the ungated backend, so add() has something real to refuse.
@task(backend="flows_ungated")
def ungated_step(label):
    return _record(label)


@task
def default_step(label):
    return _record(label)


# Workflow nodes on the backend that also defers blocked nodes
# (OPTIONS["DEFER_BLOCKED_NODES"]). A separate alias rather than a
# setting override, because the gate and the deferral are chosen per
# backend at worker construction and the tests need both live at once.


@task(backend="flows_deferred")
def deferred_step(label):
    return _record(label)


@task(backend="flows_deferred")
def failing_deferred_step(label):
    _record(label)
    raise ValueError(f"{label} boom")
