"""
Test backends that accept the per-task policy fields.

Django's ImmediateBackend and DummyBackend build the stock Task, so on Django
6.1 and on the 5.2 backport a module that declares ``@task(max_attempts=5)``
fails to import under test settings that point TASKS at either of them:
``TypeError: Task.__init__() got an unexpected keyword argument``. These
subclasses build django_ox.tasks.PolicyTask instead and hold its fields to
the same validation OxBackend does, so the module imports, and an invalid
value fails the same way it would in production.

They are import-compatible test doubles, not retry or timeout simulators.
ImmediateBackend runs each enqueued task once, on the caller's thread, and
DummyBackend never runs it; neither retries, calls a backoff or enforces a
timeout. The first time each task that declares any of the three fields is
enqueued, the backend logs one WARNING saying so. Test the policy itself
against a real worker.

Use them in test settings::

    TASKS = {"default": {"BACKEND": "django_ox.testing.ImmediateBackend"}}
"""

from __future__ import annotations

import logging
from threading import Lock
from typing import Any

from .compat import DummyBackend as _StockDummyBackend
from .compat import ImmediateBackend as _StockImmediateBackend
from .compat import Task
from .tasks import PolicyTask, task_policy, validate_policy

__all__ = ["DummyBackend", "ImmediateBackend"]

logger = logging.getLogger("django_ox")


class _PolicyTolerant:
    """
    What the two test backends share: PolicyTask, its validation, and one
    warning per policy-bearing task that the policy is not enforced here.
    """

    task_class: type[Task[..., Any]] = PolicyTask
    alias: str
    #: What this backend does with a task instead, for the warning.
    runs = ""

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)  # type: ignore[call-arg]
        self._warned: set[str] = set()
        self._warned_lock = Lock()

    def validate_task(self, task: Task[..., Any]) -> None:
        super().validate_task(task)  # type: ignore[misc]
        validate_policy(task)

    def _warn_inert(self, task: Task[..., Any]) -> None:
        declared = {
            name: value
            for name, value in zip(
                ("max_attempts", "backoff", "timeout"), task_policy(task), strict=True
            )
            if value is not None
        }
        if not declared:
            return
        with self._warned_lock:
            if task.module_path in self._warned:
                return
            self._warned.add(task.module_path)
        logger.warning(
            "Task %s declares %s, which the %r backend (%s) does not enforce: "
            "it %s, with no retry, no backoff and no timeout. Test that policy "
            "against a real worker.",
            task.module_path,
            ", ".join(sorted(declared)),
            self.alias,
            type(self).__qualname__,
            self.runs,
            extra={
                "event": "task_policy_inert",
                "task_path": task.module_path,
                "backend": self.alias,
                "declared": sorted(declared),
            },
        )


class ImmediateBackend(_PolicyTolerant, _StockImmediateBackend):
    """Django's ImmediateBackend, building PolicyTask; runs each task once."""

    runs = "runs each task once, on the caller's thread"

    def enqueue(self, task: Task[..., Any], args: Any, kwargs: Any) -> Any:
        self._warn_inert(task)
        return super().enqueue(task, args, kwargs)


class DummyBackend(_PolicyTolerant, _StockDummyBackend):
    """Django's DummyBackend, building PolicyTask; never runs a task."""

    runs = "stores each task without running it"

    def enqueue(self, task: Task[..., Any], args: Any, kwargs: Any) -> Any:
        self._warn_inert(task)
        return super().enqueue(task, args, kwargs)
