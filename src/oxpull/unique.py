"""
Marking tasks unique, and deriving the uniqueness key.

A task is unique-while-pending: while a task with the same key is READY
or RUNNING, enqueueing again returns the pending task's result instead of
inserting a second row. Once the pending task leaves those two statuses
(SUCCESSFUL, FAILED, LOST or DISCARDED, or its row is pruned), the key is
free again. The key follows the row's status, not the code: a LOST row
frees the key while the execution it belonged to may still be running,
and django-ox lets that execution write its outcome onto the row later.
Uniqueness deduplicates pending rows; it does not stop two executions of
one key overlapping. A LOST task frees the key even if its original
execution is still running.

django.tasks.Task is a frozen slots dataclass, so the unique flag cannot
live on the task object. It lives in a module-level registry keyed by
task.module_path, which also survives Task.using() copies (queue or
priority overrides keep the same module_path).
"""

from __future__ import annotations

# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

import hashlib
import json
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.tasks.base import Task
from django.utils.json import normalize_json

_registry: set[str] = set()


def unique[**P, R](task: Task[P, R]) -> Task[P, R]:
    """
    Mark a task as unique-while-pending. Apply above the @task decorator:

        @unique
        @task
        def send_invoice(order_id): ...
    """
    if not isinstance(task, Task):
        raise TypeError(
            "@unique must wrap a django.tasks Task; apply it above the @task decorator."
        )
    _registry.add(task.module_path)
    return task


def is_registered(task_path: str) -> bool:
    return task_path in _registry


def unique_tasks_from_options(options: dict[str, Any]) -> frozenset[str]:
    """
    UNIQUE_TASKS from a backend's OPTIONS, as a frozenset of dotted task
    paths, equivalent to decorating each listed task with @unique.

    Raises ImproperlyConfigured on an invalid shape so a bad deploy fails
    in system checks rather than silently at enqueue time; the same idiom
    as django_ox.schedules.schedules_from_options.
    """
    raw = options.get("UNIQUE_TASKS", [])
    if not (isinstance(raw, list | tuple) and all(isinstance(p, str) for p in raw)):
        raise ImproperlyConfigured("UNIQUE_TASKS must be a list of dotted task paths.")
    return frozenset(raw)


def unique_key(
    backend_alias: str, task_path: str, queue_name: str, args: Any, kwargs: Any
) -> str:
    """
    Derive the uniqueness key: SHA-256 over the JSON encoding of
    (backend_alias, task_path, queue_name, normalised args, normalised
    kwargs). Uniqueness is scoped per backend alias: the alias is the
    isolation boundary everywhere else in django.tasks, and a key shared
    across aliases would return a result whose backend contradicts the
    task it was enqueued on.

    Arguments are normalised with the same django.utils.json.normalize_json
    the backend uses to store them, so key equality is exactly equality of
    what would be written to the task row: tuples equal lists, dict order
    is irrelevant (sort_keys), and 1 and 1.0 differ just as they do in the
    stored JSON. queue_name is part of the key; the same call on two queues
    is two tasks. priority and run_after are not part of the key.

    Dict keys pass through JSON's own coercion before sorting (int keys
    become strings, exactly as the row's JSONField stores them), so a
    payload mixing int and str keys derives the key of what the row will
    actually hold instead of raising over an unsortable dict.
    """
    raw = [
        backend_alias,
        task_path,
        queue_name,
        normalize_json(args),
        normalize_json(kwargs),
    ]
    canonical = json.loads(json.dumps(raw))
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
