"""
Reading the task timeout options.

TASK_TIMEOUT is one number of seconds for every task the backend runs, or
None for no limit. TASK_TIMEOUTS maps a queue name to its own value, which
wins over TASK_TIMEOUT for that queue; None there exempts the queue from a
global limit.

The mapping is per queue rather than per task because django.tasks gives a
Task no field a backend could read a timeout from: Task.using() accepts
priority, queue_name, run_after and backend, and nothing else. Routing a
task to a queue is the sanctioned way to give it different handling, so
that is the unit a timeout attaches to.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from django.core.exceptions import ImproperlyConfigured

__all__ = ["TaskTimeouts", "task_timeouts_from_options"]


class TaskTimeouts:
    """The resolved timeout rule: a default and per-queue overrides."""

    __slots__ = ("by_queue", "default")

    def __init__(
        self, default: float | None, by_queue: Mapping[str, float | None]
    ) -> None:
        self.default = default
        self.by_queue = dict(by_queue)

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


def task_timeouts_from_options(options: Mapping[str, Any]) -> TaskTimeouts:
    """
    Validate and resolve TASK_TIMEOUT and TASK_TIMEOUTS.

    Raises ImproperlyConfigured on a value that is not a positive number
    or None, or a TASK_TIMEOUTS that is not a mapping keyed by queue name.
    The backend's system check and the worker's startup both call this, so
    a bad value fails `manage.py check` and refuses to start a worker.
    """
    default = _seconds(options.get("TASK_TIMEOUT"), "TASK_TIMEOUT")
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
        by_queue[queue_name] = _seconds(value, f"TASK_TIMEOUTS[{queue_name!r}]")
    return TaskTimeouts(default, by_queue)
