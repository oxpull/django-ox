"""
Moves into and out of OxTask.Status.WAITING. Not public API.

django-ox itself never calls anything here. A package built on django-ox
does, under an exact version pin, to hold a task back from every worker until
something outside the worker decides it may run. The functions are private so
that holding tasks back stays that package's feature rather than a promise
this one makes; tests/test_waiting.py pins their signatures and guards, so a
change here fails this repository's suite first.

A held task is inserted WAITING, never inserted READY and then moved. status
leads both dequeue indexes, so a move out of READY leaves the row's old index
entries in the key range every claim scans, and each claim walks them until
they are cleaned up, which any older open transaction postpones. A row born
WAITING has no READY entry to leave behind, and a row released from WAITING
leaves its dead entries in the WAITING range, which no claim enters.

Every move is one compare-and-set UPDATE on status and lease_epoch, so it
either moves a row from the state its caller read or does nothing. There is
no unpinned form. A caller decides from a read, and a row cancelled and
revived after that read carries a higher epoch, so the stale decision matches
nothing.

Every function takes the database it writes to, and none falls back to a
router. A row enqueued on one database is not there to move through another,
and the return value says nothing moved.

The bulk forms sort their primary keys and issue their statements in that
order, so two callers lock rows in one order. Nothing here sends a
django.tasks signal or writes a log event.
"""

from __future__ import annotations

import enum
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from django.db import transaction
from django.db.models import (
    BigIntegerField,
    Case,
    DateTimeField,
    F,
    QuerySet,
    Value,
    When,
)
from django.utils import timezone

from . import actions
from .backend import OxBackend
from .compat import Task, TaskResult
from .models import OxTask
from .results import task_result_from_db

TaskId = str | uuid.UUID

# A task that has not run, and nothing else, can be cancelled from here.
_CANCELLABLE = (OxTask.Status.READY, OxTask.Status.WAITING)


class Revival(enum.Enum):
    """What revive_many() did with one row."""

    REVIVED = "revived"
    # No row with that primary key on the database named. It was never there,
    # or it was deleted since, the way ox_prune deletes a DISCARDED row.
    NOT_FOUND = "not found"
    # The row is there, but it is not DISCARDED at the epoch given with it.
    WRONG_STATUS_OR_EPOCH = "wrong status or epoch"


def enqueue[**P, R](
    task: Task[P, R],
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
    *,
    using: str,
) -> TaskResult[P, R]:
    """
    Insert the row OxBackend.enqueue would insert for this call, as WAITING,
    on the database named.

    Built by the same OxBackend._row, so every column but status matches, and
    written with one INSERT in the caller's transaction. The result reads READY
    through django.tasks: the task has not run.

    It sends no task_enqueued, which leaves the caller to decide when the task
    counts as enqueued, and it never goes through a backend's enqueue(), so
    nothing layered on that method sees the row.
    """
    backend = task.get_backend()
    if not isinstance(backend, OxBackend):
        raise TypeError(
            f"{task.module_path} is on the {task.backend!r} backend, which is not "
            "an OxBackend; only django-ox's own table has a waiting status."
        )
    backend.validate_task(task)
    db_task = backend._row(task, args, kwargs, timezone.now())
    db_task.status = OxTask.Status.WAITING
    db_task.save(using=using, force_insert=True)
    return cast("TaskResult[P, R]", task_result_from_db(db_task, task=task))


def release(task_id: TaskId, *, lease_epoch: int, using: str) -> bool:
    """
    Move one WAITING row to READY if it is still at lease_epoch. True when it
    moved.

    lease_epoch is the epoch the caller read when it decided to release. One
    statement keyed on the primary key, status and that epoch, so there is no
    gap between checking the row and moving it. The epoch is not bumped: no
    execution holds a waiting row, and the claim bumps it.

    run_after becomes the later of now and its own value, so a task that asked
    to run later still does, and one that did not counts its age from its
    release rather than from when it was inserted.
    """
    pk = actions._pk(task_id)
    if pk is None:
        return False
    rows = OxTask.objects.using(using).filter(
        pk=pk, status=OxTask.Status.WAITING, lease_epoch=lease_epoch
    )
    return rows.update(**_released(timezone.now())) == 1


def release_many(rows: Iterable[tuple[TaskId, int]], *, using: str) -> tuple[int, int]:
    """
    release() for many rows, each pinned to the epoch given with it.

    Returns (changed, skipped), counted as django_ox.actions counts them. An
    id given twice counts once, with the first epoch given for it. A row that
    is not WAITING at its epoch, is not there, or has a malformed id is
    skipped.

    One UPDATE per thousand rows, in primary-key order, and no transaction of
    its own. Called outside a transaction, each chunk commits by itself, so
    the call never holds every row's lock at once, and an error part-way
    leaves the chunks before it released and the rest WAITING for a later
    release to move. Called inside a transaction, every chunk belongs to that
    transaction and commits or rolls back with it.
    """
    pairs, malformed = _pinned(rows)
    released = _released(timezone.now())
    changed = 0
    for chunk in _chunks(pairs):
        changed += _pinned_rows(using, chunk, (OxTask.Status.WAITING,)).update(
            **released
        )
    skipped = len(pairs) + len({str(task_id) for task_id in malformed}) - changed
    return changed, skipped


def cancel_many(rows: Iterable[tuple[TaskId, int]], *, using: str) -> int:
    """
    Close READY or WAITING rows without running them, each pinned to the epoch
    given with it. Returns how many moved.

    The same write django_ox.actions.discard makes: DISCARDED, finished_at
    stamped, the lock columns cleared, the epoch left alone. Unlike
    discard_many it never matches a FAILED or LOST row. One transaction, so
    an error part-way moves nothing.
    """
    pairs, _ = _pinned(rows)
    now = timezone.now()
    moved = 0
    with transaction.atomic(using=using):
        for chunk in _chunks(pairs):
            moved += _pinned_rows(using, chunk, _CANCELLABLE).update(
                status=OxTask.Status.DISCARDED,
                finished_at=now,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
            )
    return moved


def revive_many(
    rows: Iterable[tuple[TaskId, int]], *, using: str
) -> dict[uuid.UUID, Revival]:
    """
    Move DISCARDED rows back to WAITING, each pinned to the epoch given with
    it. Returns what happened to each row, keyed by primary key, in key order.

    Every row given gets an entry: REVIVED, NOT_FOUND or WRONG_STATUS_OR_EPOCH.
    ox_prune deletes DISCARDED rows, so a row cancelled long enough ago can be
    gone, and the caller hears that instead of finding one row fewer in a
    count. An id given twice gets one entry, decided by the first epoch given
    for it. A malformed id names no row to report on, so it raises ValueError
    before anything moves.

    The epoch goes up, so a caller still holding a decision made from the
    discarded row matches nothing. finished_at is cleared. attempts, errors
    and run_after stay as they were. A revived row always waits, and whatever
    released it before decides again.

    One transaction, so an error part-way moves nothing. Each chunk's rows
    are read with a locking read in primary-key order before its UPDATE, so
    each entry describes the row that UPDATE then moves or leaves.
    """
    pairs, malformed = _pinned(rows)
    if malformed:
        raise ValueError(
            f"{malformed[0]!r} is not a task id, so there is no row to revive "
            "or to report on."
        )
    results: dict[uuid.UUID, Revival] = {}
    with transaction.atomic(using=using):
        for chunk in _chunks(pairs):
            found = {
                pk: (status, epoch)
                for pk, status, epoch in OxTask.objects.using(using)
                .select_for_update()
                .filter(pk__in=[pk for pk, _ in chunk])
                .order_by("pk")
                .values_list("pk", "status", "lease_epoch")
            }
            revivable: list[tuple[uuid.UUID, int]] = []
            for pk, epoch in chunk:
                if pk not in found:
                    results[pk] = Revival.NOT_FOUND
                elif found[pk] != (OxTask.Status.DISCARDED, epoch):
                    results[pk] = Revival.WRONG_STATUS_OR_EPOCH
                else:
                    results[pk] = Revival.REVIVED
                    revivable.append((pk, epoch))
            if revivable:
                _pinned_rows(using, revivable, (OxTask.Status.DISCARDED,)).update(
                    status=OxTask.Status.WAITING,
                    lease_epoch=F("lease_epoch") + 1,
                    finished_at=None,
                )
    return results


def _released(now: datetime) -> dict[str, Any]:
    # Process time, the clock _ready_queryset and the retry backoff compare
    # run_after against.
    return {
        "status": OxTask.Status.READY,
        "run_after": Case(
            When(run_after__gt=now, then=F("run_after")),
            default=Value(now, output_field=DateTimeField()),
        ),
    }


def _chunks[T](items: list[T]) -> Iterator[list[T]]:
    size = actions.UPDATE_CHUNK_SIZE
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _pinned(
    rows: Iterable[tuple[TaskId, int]],
) -> tuple[list[tuple[uuid.UUID, int]], list[TaskId]]:
    """
    The usable (primary key, epoch) pairs in primary-key order, each key once
    with the first epoch given for it, and the ids that are not UUIDs.
    """
    seen: dict[uuid.UUID, int] = {}
    malformed: list[TaskId] = []
    for task_id, epoch in rows:
        pk = actions._pk(task_id)
        if pk is None:
            malformed.append(task_id)
        else:
            seen.setdefault(pk, int(epoch))
    return sorted(seen.items()), malformed


def _pinned_rows(
    using: str,
    chunk: list[tuple[uuid.UUID, int]],
    statuses: tuple[OxTask.Status, ...],
) -> QuerySet[OxTask]:
    rows = OxTask.objects.using(using).filter(
        pk__in=[pk for pk, _ in chunk], status__in=statuses
    )
    epochs = sorted({epoch for _, epoch in chunk})
    if len(epochs) == 1:
        return rows.filter(lease_epoch=epochs[0])
    # One statement per chunk even when the epochs differ, rather than one per
    # epoch: statements split by epoch would take the chunk's rows out of
    # primary-key order whenever the epochs interleave.
    return rows.filter(
        lease_epoch=Case(
            *(
                When(pk__in=[pk for pk, e in chunk if e == epoch], then=Value(epoch))
                for epoch in epochs
            ),
            output_field=BigIntegerField(),
        )
    )
