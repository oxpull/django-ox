"""
Maintenance tasks for django-ox.

`prune` is built when it is looked up rather than when this module is
imported. django.tasks validates a Task against its backend the moment it is
constructed, queue name included, so a module-level `@task` here would make
`import django_ox.maintenance` raise InvalidTask in any project whose default
backend's QUEUES omits "default". The system checks import a schedule's task
path, so that one import would end `check`, `migrate`, `ox_worker` and
`ox_health` alike. Built on access, the import has no side effect, and every
lookup of `django_ox.maintenance.prune` returns a new Task, equal by value to
the last one, on a queue the default backend accepts.
"""

from __future__ import annotations

import io
import json
from typing import Any, cast

from django.conf import settings
from django.core.management import call_command

from django_ox.compat import (
    DEFAULT_TASK_BACKEND_ALIAS,
    BaseTaskBackend,
    Task,
    task,
    task_backends,
)

#: django.tasks' DEFAULT_TASK_QUEUE_NAME, spelled out because the compat
#: layer does not carry it and the value is the same on every version.
_DEFAULT_QUEUE = "default"


def _prune(
    older_than: str = "7d",
    include_failed: bool = False,  # noqa: FBT001, FBT002
    queue: str | None = None,
    dry_run: bool = False,  # noqa: FBT001, FBT002
    batch_size: int | None = None,
) -> dict[str, Any]:
    """
    Prune finished task rows and old dispatch history.

    Runs ``ox_prune`` with ``--format json`` and returns the result dictionary.

    The task is on "default" when the default backend accepts that queue,
    and otherwise on the first queue its settings declare. A schedule entry's
    `queue_name` overrides it, which is how to put it on a queue the
    project's workers run.

    `include_failed` and `dry_run` must be True or False. A schedule's
    arguments are JSON, where the string "false" is easy to write and is
    true to a truthiness test, so anything else raises TypeError before
    `ox_prune` runs rather than deleting FAILED and LOST rows.
    """
    for name, value in (("include_failed", include_failed), ("dry_run", dry_run)):
        if not isinstance(value, bool):
            raise TypeError(f"prune() {name} must be True or False, not {value!r}.")
    out = io.StringIO()
    args: list[str] = [
        "--format",
        "json",
        "--older-than",
        str(older_than),
    ]
    if include_failed:
        args.append("--include-failed")
    if queue is not None:
        args.extend(["--queue", str(queue)])
    if dry_run:
        args.append("--dry-run")
    if batch_size is not None:
        args.extend(["--batch-size", str(batch_size)])

    # The unreadable-tick warning goes to stderr; the result carries its counts.
    call_command("ox_prune", *args, stdout=out, stderr=io.StringIO())
    return cast(dict[str, Any], json.loads(out.getvalue()))


# The task row stores f"{func.__module__}.{func.__qualname__}", and both the
# schedule and the worker resolve the task by that path, so the function
# carries its public name however it is spelled above.
_prune.__name__ = _prune.__qualname__ = "prune"


def _queue_accepted_by(backend: BaseTaskBackend, params: Any) -> str:
    """
    The queue a maintenance task takes on `backend`: "default" when it
    accepts that, else the first queue its settings declare.
    """
    queues = backend.queues
    if not queues or _DEFAULT_QUEUE in queues:
        return _DEFAULT_QUEUE
    # The declared order is the operator's own; sorted() is only a fallback
    # for a backend built outside settings.TASKS.
    configured = params.get("QUEUES") if isinstance(params, dict) else None
    # Only a list or tuple has a declared order. Any other iterable, a set
    # say, takes the first name in sorted order, so every process picks the
    # same queue whatever its hash seed.
    ordered = (
        configured
        if isinstance(configured, (list, tuple)) and configured
        else sorted(queues)
    )
    return str(ordered[0])


def _build_prune_task() -> Task[..., dict[str, Any]]:
    """The prune task, on a queue the default backend accepts."""
    queue_name = _queue_accepted_by(
        task_backends[DEFAULT_TASK_BACKEND_ALIAS],
        getattr(settings, "TASKS", {}).get(DEFAULT_TASK_BACKEND_ALIAS, {}),
    )
    if queue_name == _DEFAULT_QUEUE:
        return task(_prune)
    return task(queue_name=queue_name)(_prune)


__all__ = ["prune"]

# Declared, not bound: the name has to stay out of the module namespace so
# that every lookup goes through __getattr__ below.
prune: Task[..., dict[str, Any]]


def __dir__() -> list[str]:
    """The module's names, with the lazily built `prune` among them."""
    return sorted({*globals(), "prune"})


def __getattr__(name: str) -> Any:
    """Resolve the module's lazily built public names."""
    if name == "prune":
        return _build_prune_task()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
