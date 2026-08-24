"""
Enqueue many calls of one task in a single INSERT.

``enqueue_many(task, calls)`` is the bulk form of ``task.enqueue()``. It
takes one ``django.tasks`` task and a list of ``(args, kwargs)`` pairs,
writes one row per pair with ``bulk_create`` on the default connection, and
returns the ``TaskResult`` list in the order the calls were given. Each row
is serialised exactly as ``enqueue()`` serialises it, so the worker path
does not know the difference.

The shape follows ``django.tasks`` itself: ``Task.enqueue(*args, **kwargs)``
hands the backend ``(task, args, kwargs)``, and that triple is what one
element of ``calls`` spells out. Queue, priority and ``run_after`` are
properties of the task, not of a call, so they are set once with
``task.using(...)`` and apply to every row; a mix of queues is two calls.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from django.tasks.base import Task, TaskResult
from django.tasks.exceptions import InvalidTask

from .backend import INSERT_CHUNK_SIZE, OxBackend

__all__ = ["INSERT_CHUNK_SIZE", "enqueue_many"]

# INSERT_CHUNK_SIZE, re-exported from the backend, is the number of rows per
# INSERT statement. SQLite caps the number of bound variables per statement,
# and a row binds one variable per column, so the chunk keeps each statement
# well under that cap; Django lowers it further on a SQLite built with a
# small SQLITE_MAX_VARIABLE_NUMBER. Every chunk runs inside one transaction,
# so the call still commits or rolls back whole.


def enqueue_many[**P, R](
    task: Task[P, R],
    calls: Iterable[tuple[Sequence[Any], Mapping[str, Any]]],
) -> list[TaskResult[P, R]]:
    """
    Enqueue ``task`` once per ``(args, kwargs)`` pair in ``calls``.

    Returns one ``TaskResult`` per pair, in input order. The rows are written
    with one INSERT per ``INSERT_CHUNK_SIZE`` rows inside a single
    ``transaction.atomic()`` block, so the call is all-or-nothing on its own
    and joins the caller's transaction when there is one: inside
    ``atomic()`` the tasks become visible to workers only when that
    transaction commits, the same as ``enqueue()``.

    The task is validated, and every call serialised to JSON, before the
    first row is written. A task that is not a ``django.tasks`` task, one
    bound to a backend other than ``OxBackend``, a queue the backend does
    not accept, or an argument that will not serialise all raise
    ``InvalidTask`` (or ``TypeError`` from the serialiser) with nothing
    inserted.

    An empty ``calls`` returns an empty list and touches the database only
    to open and close the transaction.
    """
    if not isinstance(task, Task):
        raise InvalidTask(
            f"enqueue_many() needs a django.tasks Task, got {type(task).__name__}."
        )
    backend = task.get_backend()
    if not isinstance(backend, OxBackend):
        raise InvalidTask(
            f"Task {task.module_path!r} is bound to backend {task.backend!r}, "
            "which is not django_ox.backend.OxBackend; enqueue_many() writes "
            "task rows and only OxBackend reads them."
        )
    return backend.enqueue_many(task, calls)
