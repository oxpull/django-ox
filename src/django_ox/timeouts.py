"""
Task timeouts: the options, and the helpers task code can call.

TASK_TIMEOUT is one number of seconds for every attempt the backend runs, or
None for no limit. TASK_TIMEOUTS maps a queue name to its own value, which
wins over TASK_TIMEOUT for that queue; None there exempts the queue from a
global limit. TASK_TIMEOUT_GRACE is how long the worker gives a timed-out
attempt to stop before it treats the thread as stuck and recycles itself.

The mapping is per queue rather than per task because django.tasks gives a
Task no field a backend could read a timeout from: Task.using() accepts
priority, queue_name, run_after and backend, and nothing else. Routing a
task to a queue is the sanctioned way to give it different handling, so
that is the unit a timeout attaches to.

deadline() and remaining() are the cooperative side. The worker sets the
attempt's deadline in a context variable before it calls the task, so a
long loop can check how much time it has left instead of being interrupted
in the middle of a step.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from contextvars import ContextVar
from datetime import datetime
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

__all__ = [
    "DEFAULT_GRACE",
    "RECYCLE_EXIT_CODE",
    "TaskTimeouts",
    "deadline",
    "remaining",
    "task_timeouts_from_options",
]

DEFAULT_GRACE = 30.0

# EX_TEMPFAIL from sysexits.h: a worker that recycles itself after a stuck
# thread exits with this code, and the supervisor restarts the slot without
# counting the exit against the restart cap.
RECYCLE_EXIT_CODE = 75

# The running attempt's deadline. A context variable rather than a
# thread-local because an async task's coroutine runs on a thread of
# asgiref's choosing, with the caller's context copied across.
_deadline: ContextVar[datetime | None] = ContextVar("django_ox_deadline", default=None)


def deadline() -> datetime | None:
    """
    The moment the running attempt times out, or None when it has no limit
    or when called outside a task.
    """
    return _deadline.get()


def remaining() -> float | None:
    """
    Seconds left before the running attempt times out, negative once the
    deadline has passed, or None when there is no limit.
    """
    at = _deadline.get()
    if at is None:
        return None
    return (at - timezone.now()).total_seconds()


class TaskTimeouts:
    """The resolved timeout rule: a default, per-queue overrides, the grace."""

    __slots__ = ("by_queue", "default", "grace")

    def __init__(
        self,
        default: float | None,
        by_queue: Mapping[str, float | None],
        grace: float = DEFAULT_GRACE,
    ) -> None:
        self.default = default
        self.by_queue = dict(by_queue)
        self.grace = grace

    def for_queue(self, queue_name: str) -> float | None:
        """Seconds the tasks on this queue may run, or None for no limit."""
        return self.by_queue.get(queue_name, self.default)

    @property
    def enabled(self) -> bool:
        return self.default is not None or any(
            value is not None for value in self.by_queue.values()
        )


def _seconds(value: Any, where: str) -> float | None:
    if value is None:
        return None
    # bool is an int subclass; True as a one-second timeout is a mistake.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ImproperlyConfigured(
            f"{where} must be a number of seconds or None, got {value!r}."
        )
    if value != value or value <= 0:
        raise ImproperlyConfigured(f"{where} must be greater than zero, got {value!r}.")
    return float(value)


def task_timeouts_from_options(
    options: Mapping[str, Any], queues: Collection[str] = ()
) -> TaskTimeouts:
    """
    Validate and resolve TASK_TIMEOUT, TASK_TIMEOUTS and TASK_TIMEOUT_GRACE.

    Raises ImproperlyConfigured on a value that is not a positive number or
    None, on a TASK_TIMEOUTS that is not a mapping keyed by queue name, and
    on a TASK_TIMEOUTS key that is not one of ``queues`` when that list is
    given (an empty list means the backend accepts any queue name, and
    then every key is accepted too). The backend's system check and the
    worker's startup both call this, so a bad value fails `manage.py check`
    and refuses to start a worker.
    """
    default = _seconds(options.get("TASK_TIMEOUT"), "TASK_TIMEOUT")
    grace = _seconds(
        options.get("TASK_TIMEOUT_GRACE", DEFAULT_GRACE), "TASK_TIMEOUT_GRACE"
    )
    if grace is None:
        raise ImproperlyConfigured("TASK_TIMEOUT_GRACE must be a number of seconds.")
    raw = options.get("TASK_TIMEOUTS", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ImproperlyConfigured(
            f"TASK_TIMEOUTS must be a mapping of queue name to seconds, got {raw!r}."
        )
    by_queue: dict[str, float | None] = {}
    for queue_name, value in raw.items():
        if not isinstance(queue_name, str) or not queue_name:
            raise ImproperlyConfigured(
                f"TASK_TIMEOUTS keys must be queue names, got {queue_name!r}."
            )
        if queues and queue_name not in queues:
            raise ImproperlyConfigured(
                f"TASK_TIMEOUTS names the queue {queue_name!r}, which is not in "
                f"QUEUES {sorted(queues)!r}; the entry would never apply."
            )
        by_queue[queue_name] = _seconds(value, f"TASK_TIMEOUTS[{queue_name!r}]")
    return TaskTimeouts(default, by_queue, grace)
