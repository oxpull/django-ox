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

import math
import time
from collections.abc import Collection, Mapping
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import Any, cast

from django.core.exceptions import ImproperlyConfigured

__all__ = [
    "DEFAULT_GRACE",
    "MAX_SECONDS",
    "RECYCLE_EXIT_CODE",
    "TaskTimeouts",
    "deadline",
    "remaining",
    "task_timeout_problems",
    "task_timeouts_from_options",
]

DEFAULT_GRACE = 30.0

# The largest timeout or grace accepted: a thousand years. A deadline is a
# datetime, which stops at the year 9999, so float("inf") or a value near
# timedelta's own limit would overflow the arithmetic on every attempt and
# fail each one before the task was called. None is how to say no limit.
MAX_SECONDS = timedelta(days=365_250).total_seconds()

# EX_TEMPFAIL from sysexits.h: a worker that recycles itself after a stuck
# thread exits with this code, and the supervisor restarts the slot without
# counting the exit against the restart cap.
RECYCLE_EXIT_CODE = 75

# The running attempt's deadline. A context variable rather than a
# thread-local because an async task's coroutine runs on a thread of
# asgiref's choosing, with the caller's context copied across.
_deadline: ContextVar[datetime | None] = ContextVar("django_ox_deadline", default=None)

# The same deadline on the clock the watchdog actually enforces against.
# `deadline()` answers "when", which is a wall-clock question and has to stay a
# wall-clock answer. `remaining()` answers "how long have I got", which is the
# question the enforcer settles, and it settles it on `time.monotonic()`. Read
# off the wall clock, the two disagree for as long as an NTP step lasts: a
# backwards correction leaves a task confident it has seconds in hand after the
# watchdog has already fired, and a forwards one makes a task give up early.
_deadline_monotonic: ContextVar[float | None] = ContextVar(
    "django_ox_deadline_monotonic", default=None
)


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

    Measured on the same monotonic clock the worker enforces the deadline
    with, so a clock correction cannot put this answer and the timeout that
    actually fires on different sides of the same instant.
    """
    until = _deadline_monotonic.get()
    if until is None:
        return None
    return until - time.monotonic()


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


def _seconds(value: Any, where: str, *, unlimited: bool = True) -> float | None:
    """
    A validated number of seconds, or None where None is allowed.

    ``unlimited`` says whether None means no limit for this option, which
    decides how the messages read: TASK_TIMEOUT and a TASK_TIMEOUTS value
    may be None, TASK_TIMEOUT_GRACE may not.
    """
    if value is None:
        if unlimited:
            return None
        raise ImproperlyConfigured(f"{where} must be a number of seconds.")
    # bool is an int subclass; True as a one-second timeout is a mistake.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ImproperlyConfigured(
            f"{where} must be a number of seconds{' or None' if unlimited else ''}, "
            f"got {value!r}."
        )
    if math.isnan(value) or value <= 0:
        raise ImproperlyConfigured(f"{where} must be greater than zero, got {value!r}.")
    if value > MAX_SECONDS:
        raise ImproperlyConfigured(
            f"{where} must be a finite number of seconds, at most "
            f"{MAX_SECONDS:.0f} (a thousand years), got {value!r}"
            f"{'; None means no limit' if unlimited else ''}."
        )
    return float(value)


#: Options the worker reads as a number of seconds. A deploy that gets one of
#: them wrong stops at `manage.py check` rather than in the poll loop.
LEASE_TIMINGS = ("LOCK_TIMEOUT", "BACKOFF_INITIAL", "BACKOFF_MAX")


def lease_timing_problems(
    options: Mapping[str, Any],
) -> tuple[list[str], list[str]]:
    """
    Every way the worker's timing options are invalid or misleading.

    Returns ``(errors, warnings)``. Errors are values that are not a
    positive, finite number of seconds. Validated by the same helper as the
    timeout options: `True` is not one second and 1e300 is not a duration.

    The one relationship this helper checks is `BACKOFF_INITIAL` above
    `BACKOFF_MAX`. Both must be present and individually valid; otherwise
    the existing per-option error stands and this pair is not compared.
    Equal values are an intentional fixed delay and produce no message.
    The comparison is a warning because retries still run: every wait is
    `BACKOFF_MAX`.

    This check does not validate the renewal interval. Its default,
    max(lock_timeout / 3, 0.1) seconds, exceeds lock_timeout below 0.1 seconds.
    Backend OPTIONS does not configure the interval. The Worker constructor
    accepts a renew_interval override, including values greater than lock_timeout.
    Ticks are scheduled start-to-start. An overrun starts the next tick
    immediately, with no overlap or catch-up ticks.
    """
    problems: list[str] = []
    warnings: list[str] = []
    parsed: dict[str, float] = {}
    for name in LEASE_TIMINGS:
        if name not in options:
            continue
        try:
            # A float here: unlimited=False refuses None.
            parsed[name] = cast(
                "float", _seconds(options[name], f"OPTIONS[{name!r}]", unlimited=False)
            )
        except ImproperlyConfigured as exc:
            problems.append(str(exc))
    if (
        "BACKOFF_INITIAL" in parsed
        and "BACKOFF_MAX" in parsed
        and parsed["BACKOFF_INITIAL"] > parsed["BACKOFF_MAX"]
    ):
        initial = options["BACKOFF_INITIAL"]
        maximum = options["BACKOFF_MAX"]
        warnings.append(
            f"OPTIONS['BACKOFF_INITIAL'] is {initial!r} and "
            f"OPTIONS['BACKOFF_MAX'] is {maximum!r}; every retry waits "
            "BACKOFF_MAX."
        )
    return problems, warnings


def task_timeout_problems(
    options: Mapping[str, Any], queues: Collection[str] = ()
) -> list[str]:
    """
    Every way TASK_TIMEOUT, TASK_TIMEOUTS and TASK_TIMEOUT_GRACE are
    invalid, one sentence each, in the order the options are read; empty
    when they are valid. The backend's system check reports each one.
    """
    problems: list[str] = []

    def read(value: Any, where: str, *, unlimited: bool = True) -> None:
        try:
            _seconds(value, where, unlimited=unlimited)
        except ImproperlyConfigured as exc:
            problems.append(str(exc))

    read(options.get("TASK_TIMEOUT"), "TASK_TIMEOUT")
    read(
        options.get("TASK_TIMEOUT_GRACE", DEFAULT_GRACE),
        "TASK_TIMEOUT_GRACE",
        unlimited=False,
    )
    raw = options.get("TASK_TIMEOUTS", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        problems.append(
            f"TASK_TIMEOUTS must be a mapping of queue name to seconds, got {raw!r}."
        )
        return problems
    for queue_name, value in raw.items():
        if not isinstance(queue_name, str) or not queue_name:
            problems.append(
                f"TASK_TIMEOUTS keys must be queue names, got {queue_name!r}."
            )
            continue
        if queues and queue_name not in queues:
            problems.append(
                f"TASK_TIMEOUTS names the queue {queue_name!r}, which is not in "
                f"QUEUES {sorted(queues)!r}; the entry would never apply."
            )
        read(value, f"TASK_TIMEOUTS[{queue_name!r}]")
    return problems


def task_timeouts_from_options(
    options: Mapping[str, Any], queues: Collection[str] = ()
) -> TaskTimeouts:
    """
    Validate and resolve TASK_TIMEOUT, TASK_TIMEOUTS and TASK_TIMEOUT_GRACE.

    Raises ImproperlyConfigured, naming every problem, on a value that is
    not a positive finite number or None, on a TASK_TIMEOUTS that is not a
    mapping keyed by queue name, and on a TASK_TIMEOUTS key that is not
    one of ``queues`` when that list is given (an empty list means the
    backend accepts any queue name, and then every key is accepted too).
    The backend's system check and the worker's startup both validate
    this way, so a bad value fails `manage.py check` and refuses to start
    a worker.
    """
    problems = task_timeout_problems(options, queues)
    if problems:
        raise ImproperlyConfigured(" ".join(problems))
    default = _seconds(options.get("TASK_TIMEOUT"), "TASK_TIMEOUT")
    grace = _seconds(
        options.get("TASK_TIMEOUT_GRACE", DEFAULT_GRACE),
        "TASK_TIMEOUT_GRACE",
        unlimited=False,
    )
    raw = options.get("TASK_TIMEOUTS") or {}
    by_queue = {
        queue_name: _seconds(value, f"TASK_TIMEOUTS[{queue_name!r}]")
        for queue_name, value in raw.items()
    }
    # grace is a float here: unlimited=False refused None above.
    return TaskTimeouts(default, by_queue, DEFAULT_GRACE if grace is None else grace)
