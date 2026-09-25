"""
Per-task retry and timeout policy: PolicyTask, the task class OxBackend builds.

``OxBackend.task_class`` is ``PolicyTask``, so on Django 6.1 and on 5.2 with
the django-tasks 0.12 backport, where ``@task`` forwards keyword arguments to
the backend's task class, a task can carry three fields of its own::

    @task(max_attempts=5, backoff=on_failure, timeout=30)
    def sync_account(account_id): ...

Every field defaults to None, which means inherit: the backend's
``MAX_ATTEMPTS``, the worker's exponential backoff, and the queue's
``TASK_TIMEOUTS`` entry or ``TASK_TIMEOUT``. A bare ``@task`` therefore builds
a PolicyTask that behaves exactly as a plain Task did. Django 6.0's ``task()``
takes no extra keyword arguments at all, so there the kwargs raise its own
TypeError at import; the class is still what a bare ``@task`` builds.

The fields live in two places on purpose. ``max_attempts`` is written onto the
row at enqueue, into the column the worker and the reaper already decide on,
so the stored value is the budget from then on: an admin retry, a row enqueued
before a deploy, and a row enqueued by an older release all keep theirs.
``backoff`` and ``timeout`` are not stored anywhere. The worker re-imports the
task for every attempt and uses what the code it is running declares, so a
deploy that changes them changes rows already queued.

This surface is provisional. It follows the names and the two-argument
callback of Django's open new-features proposals #142 and #144, and makes no
promise of compatibility with whatever Django core eventually ships.

The class is a frozen, slotted, keyword-only dataclass like Django's own Task,
and it calls ``Task.__post_init__(self)`` explicitly: a zero-argument
``super()`` inside a slotted dataclass subclass fails on Python 3.12 and 3.13,
because ``slots=True`` builds a new class that the implicit ``__class__`` cell
does not name.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeGuard

from .compat import InvalidTask, Task, TaskResult
from .timeouts import MAX_SECONDS

__all__ = [
    "MAX_ATTEMPTS_LIMIT",
    "BackoffCallback",
    "PolicyTask",
    "validate_policy",
]

#: The largest attempt budget a row can hold. The column is a
#: PositiveSmallIntegerField, a signed 16-bit smallint on PostgreSQL, so this
#: is the largest value every supported database stores in it.
MAX_ATTEMPTS_LIMIT = 32767

#: What ``backoff`` is called with and may return. It receives the exception
#: the attempt raised and a TaskResult describing the attempt as failed, and
#: returns the delay before the next attempt (whole seconds, or a timedelta),
#: or None to stop retrying and record the task as FAILED now.
type BackoffCallback = Callable[
    [BaseException, TaskResult[Any, Any]], int | timedelta | None
]

# django.tasks.Task is generic in django-stubs and in the backport, and a plain
# dataclass at runtime on Django 6.x, where subscripting it raises. The type
# checker reads the first branch; the runtime gets a base that answers the
# subscript with the class itself and leaves the class's own generic
# parameters to the implicit typing.Generic base.
if TYPE_CHECKING or hasattr(Task, "__class_getitem__"):
    _TaskBase = Task
else:

    class _TaskBase:
        def __class_getitem__(cls, params: object) -> type[Task]:
            return Task


def _whole_number(value: object) -> TypeGuard[int]:
    # bool is an int subclass, and True as one attempt or one second is a
    # mistake rather than a value.
    return isinstance(value, int) and not isinstance(value, bool)


def max_attempts_problem(value: object, where: str, *, also: str = "") -> str | None:
    """
    Why ``value`` is not an attempt budget, or None when it is one.

    The task field's rule: a whole number from 1 to MAX_ATTEMPTS_LIMIT.
    Strings, floats and bools are refused rather than coerced, because
    coercion turns "3" into 3, 3.7 into 3 and True into 1 without a word. The
    field is new, so nothing depends on the coercion. The backend's
    MAX_ATTEMPTS option is older, and 1.4.0 coerced it: there the same values
    are deprecated rather than refused (see django_ox.backend). ``also``
    names what else the caller accepts, for the message.
    """
    if _whole_number(value) and 1 <= value <= MAX_ATTEMPTS_LIMIT:
        return None
    return (
        f"{where} must be a whole number from 1 to {MAX_ATTEMPTS_LIMIT}{also}, "
        f"got {value!r}."
    )


def _is_async(func: object) -> bool:
    """
    Whether calling ``func`` produces something to await rather than an answer.

    Identifiable cases only: a coroutine function, an async generator
    function, a partial of either (inspect unwraps partials), and an instance
    whose ``__call__`` is one. A class is not asked about its ``__call__``,
    because calling a class constructs an instance. A callable that returns
    an awaitable without being declared async is caught when it is called.
    """
    if inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func):
        return True
    if inspect.isclass(func):
        return False
    call = getattr(func, "__call__", None)  # noqa: B004
    return inspect.iscoroutinefunction(call) or inspect.isasyncgenfunction(call)


def policy_problems(
    *, max_attempts: object, backoff: object, timeout: object
) -> list[str]:
    """
    Every way the three policy fields are invalid, one sentence each; empty
    when they are valid. None is always valid, and means inherit.
    """
    problems: list[str] = []
    if max_attempts is not None:
        problem = max_attempts_problem(
            max_attempts,
            "max_attempts",
            also=", or None to use the backend's MAX_ATTEMPTS",
        )
        if problem is not None:
            problems.append(problem)
    if backoff is not None:
        if not callable(backoff):
            problems.append(
                "backoff must be a callable taking (exception, task_result), "
                f"or None to use the worker's exponential backoff, got {backoff!r}."
            )
        elif _is_async(backoff):
            problems.append(
                f"backoff must be a synchronous callable, got {backoff!r}. The "
                "worker calls it on the attempt's own thread and does not "
                "await what it returns."
            )
    if timeout is not None and not (
        _whole_number(timeout) and 0 < timeout <= MAX_SECONDS
    ):
        problems.append(
            "timeout must be a whole number of seconds greater than zero and "
            f"at most {MAX_SECONDS:.0f} (a thousand years), or None to use the "
            f"queue's timeout, got {timeout!r}."
        )
    return problems


def task_policy(
    task: Task[..., Any],
) -> tuple[int | None, BackoffCallback | None, int | None]:
    """
    ``(max_attempts, backoff, timeout)`` as the task declares them.

    A task that is not a PolicyTask declares nothing and inherits all three.
    That covers a task declared on another backend and rebound onto an
    OxBackend with ``.using(backend=...)``, which keeps its own class, and a
    Task subclass from another library: attributes of the same names on
    those are that library's, not this policy, and are not read.
    """
    if isinstance(task, PolicyTask):
        return task.max_attempts, task.backoff, task.timeout
    return None, None, None


def validate_policy(task: Task[..., Any]) -> None:
    """
    Raise InvalidTask naming every invalid policy field of ``task``.

    Shared by PolicyTask's own construction, OxBackend.validate_task and the
    test backends in django_ox.testing, so each refuses exactly the same
    values. A task that is not a PolicyTask has nothing to validate.
    """
    max_attempts, backoff, timeout = task_policy(task)
    problems = policy_problems(
        max_attempts=max_attempts, backoff=backoff, timeout=timeout
    )
    if problems:
        raise InvalidTask(" ".join(problems))


@dataclass(frozen=True, slots=True, kw_only=True)
class PolicyTask[**P, R](_TaskBase[P, R]):
    """
    A django.tasks Task with a retry budget, a retry decision and a timeout.

    ``max_attempts`` is the number of claims the task may take, the first
    included, from 1 to 32767. ``backoff`` is called after a failed attempt
    that has attempts left, and returns the delay before the next one, or
    None to fail the task now; see BackoffCallback. ``timeout`` is the whole
    number of seconds one attempt may run. None, the default for all three,
    inherits the backend's or the worker's setting.

    The fields are validated when the task is built, which for ``@task`` is
    at import. ``Task.using()`` cannot change them, and although
    ``dataclasses.replace()`` can, only ``max_attempts`` reaches the row: a
    backoff or timeout set that way is not what the worker runs.
    """

    max_attempts: int | None = None
    backoff: BackoffCallback | None = None
    timeout: int | None = None

    def __post_init__(self) -> None:
        validate_policy(self)
        # Explicit, not super(): see the module docstring.
        Task.__post_init__(self)
