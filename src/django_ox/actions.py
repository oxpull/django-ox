"""
Operator actions on stored tasks: retry one that is settled, or discard one
that has not run. The admin actions call these; so can a shell or a view.

Each single-row function is one compare-and-set UPDATE keyed on the row's
status and its lease epoch, so it either moves the row from the state it
read or does nothing and reports that. The _many forms do the same move
for a selection in one conditional UPDATE per thousand ids, inside one
transaction. None of them ever moves a RUNNING row: a running task
belongs to the worker holding its lease, and the reaper is the only party
that takes a lease away.

The _many forms sort the ids before they chunk them, in the order the
database keeps its keys. On a database with row locks, each chunk's UPDATE
comes after a locking read of that chunk's ids in key order, so the whole
call takes its rows in key order. That is the order ox_prune and
django_ox._waiting lock in, so a call and one of them wait for each other
instead of deadlocking over the rows they share. Sorting is what MySQL
needs, where an UPDATE already walks a chunk's ids in key order but the
chunks came in the order they were given. The locking read is what
PostgreSQL needs, where the UPDATE can visit rows in table order. It names
the ids alone, with no status, so MySQL reads it from the primary key rather
than a status index. It therefore locks every row in the chunk, whatever its
status, until the call ends. A worker writing to one of them waits for that.

A deadlock is still possible with a writer that locks the same rows in
another order. When a _many call opened the transaction itself, a deadlock
or a serialization failure starts it again from its first chunk, three
attempts in all. Inside a caller's transaction the error goes to the
caller.

Each call resolves the alias OxTask writes to once and runs every statement
on it, its reads included. Left unqualified, the read that a
compare-and-set needs would follow db_for_read: under a router that sends
reads to a replica, a call would read one database and write another, and
against a replica that is behind it would read a row it is about to
contradict, or fail to find one that is there.

The actions write the table directly and send no django.tasks signal: a
discard finishes the result without task_finished, and a retry requeues
it without task_enqueued. The worker's signals fire as usual when a
retried task next runs.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from datetime import timedelta
from typing import Any

from django.db import connections, router, transaction
from django.db.models import F, Q, QuerySet
from django.utils import timezone

from . import _contention
from .models import OxTask
from .tasks import MAX_ATTEMPTS_LIMIT

__all__ = [
    "DISCARDABLE_STATUSES",
    "RETRYABLE_STATUSES",
    "UPDATE_CHUNK_SIZE",
    "discard",
    "discard_many",
    "expire_lease",
    "retry",
    "retry_many",
]

# Ids per UPDATE. Under every backend's parameter cap; small enough that one
# statement does not hold a long lock over a large table.
UPDATE_CHUNK_SIZE = 1000

RETRYABLE_STATUSES = (OxTask.Status.FAILED, OxTask.Status.LOST)
DISCARDABLE_STATUSES = (
    OxTask.Status.READY,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    OxTask.Status.WAITING,
)

# A retry grants attempts + 1 as the new budget, so a row whose count is
# already at the column's ceiling cannot be granted one. The condition rides
# in the same UPDATE as the status check: refused in the statement that would
# have written the overflow, never after a read that could be stale.
_RETRY_FITS = Q(attempts__lt=MAX_ATTEMPTS_LIMIT)


def _rows() -> QuerySet[OxTask]:
    """OxTask on the one alias a call reads and writes."""
    return OxTask.objects.using(router.db_for_write(OxTask))


def _pk(result_id: str | uuid.UUID) -> uuid.UUID | None:
    if isinstance(result_id, uuid.UUID):
        return result_id
    try:
        return uuid.UUID(str(result_id))
    except (ValueError, TypeError, AttributeError):
        return None


def retry(result_id: str | uuid.UUID) -> bool:
    """
    Put a FAILED or LOST task back on the queue for one more attempt.

    Returns True when the row was requeued, False when it was not there or
    was in any other state, RUNNING included. Nothing is raised for a row
    that cannot be retried: the caller asked for a state change and gets
    told whether it happened.

    The rule that keeps this from running a task twice at once: the UPDATE
    is a compare-and-set on (status in RETRYABLE_STATUSES, lease_epoch as
    read), and it bumps lease_epoch in the same statement. Two retries of
    one row race for the same epoch and exactly one matches. A LOST row may
    still have its last worker alive somewhere; that worker holds the old
    epoch, so after the bump its outcome write matches nothing and is
    dropped, the same way the reaper fences a requeue. A RUNNING row is
    never matched, so a retry cannot add a second execution to one that is
    still going.

    Attempts are neither reset nor incremented here. The row keeps its
    count, its worker_ids and its per-attempt errors, so the record still
    says what happened before the retry, and max_attempts moves up to
    attempts + 1 so the next claim is the one extra attempt this grants.
    That is an operator's override, not the task's declared budget: the
    task's max_attempts decided the row's first budget at enqueue and has no
    say here. Claiming increments attempts, the same as any other claim.
    run_after is cleared so the retry is eligible at once rather than after
    a backoff written for a failure that has already been dealt with.

    A row that has already used 32767 attempts, the most the column holds,
    is refused and left exactly as it was: attempts + 1 does not fit.
    """
    pk = _pk(result_id)
    if pk is None:
        return False
    rows = _rows()
    row = (
        rows.filter(_RETRY_FITS, pk=pk, status__in=RETRYABLE_STATUSES)
        .values("lease_epoch", "attempts")
        .first()
    )
    if row is None:
        return False
    updated = rows.filter(
        _RETRY_FITS,
        pk=pk,
        status__in=RETRYABLE_STATUSES,
        lease_epoch=row["lease_epoch"],
    ).update(
        status=OxTask.Status.READY,
        lease_epoch=row["lease_epoch"] + 1,
        max_attempts=row["attempts"] + 1,
        run_after=None,
        finished_at=None,
        locked_by=None,
        locked_at=None,
        lease_expires_at=None,
    )
    return updated == 1


def expire_lease(result_id: str | uuid.UUID) -> bool:
    """
    Make a RUNNING task's lease expire now, so the next reaper pass takes it.

    For the one case the stored expiry cannot fix by itself: a lease was
    granted with a timeout that turned out to be wrong, and because the row
    carries its own deadline, changing the setting on the workers does not
    move it. A long lease outlives the mistake that configured it. This is how
    an operator shortens one.

    It does not stop the task. Whatever is holding the row keeps running, and
    the lease number still refuses its finish write once somebody else has the
    row, so this is the reaper's ordinary reclaim brought forward rather than a
    cancellation. Use `discard()` to close a task, not this.

    Returns True when the lease was expired, False when the row was not there
    or was not RUNNING. Nothing is raised: the caller asked for a state change
    and gets whether it happened.
    """
    pk = _pk(result_id)
    if pk is None:
        return False
    # Process time, not the lease clock. This is an operator's instruction
    # rather than part of the lease protocol, and any reaper comparing against
    # any clock must read it as past: a full day in the past is behind
    # every clock a fleet plausibly has.
    already_expired = timezone.now() - timedelta(days=1)
    updated = (
        _rows()
        .filter(pk=pk, status=OxTask.Status.RUNNING)
        .exclude(locked_at=None)
        .update(lease_expires_at=already_expired, locked_at=already_expired)
    )
    return updated == 1


def discard(result_id: str | uuid.UUID) -> bool:
    """
    Close a READY, WAITING, FAILED or LOST task without running it.

    Returns True when the row was marked DISCARDED, False when it was not
    there or was in any other state. A RUNNING row is never matched: the
    worker holding it keeps it, and discarding it would be a verdict on
    work still in progress.

    The UPDATE is a compare-and-set on (status in DISCARDABLE_STATUSES,
    lease_epoch as read), so a READY row that a worker claims in the same
    instant goes to exactly one of them. A WAITING row released in the same
    instant is still closed: the release leaves it READY at the epoch this
    call read. The epoch is not bumped: DISCARDED is not a state any worker
    writes onto, so there is no holder to fence out, and a LOST row's
    straggler write is already refused by status. The row keeps attempts,
    worker_ids and errors as they were.
    """
    pk = _pk(result_id)
    if pk is None:
        return False
    rows = _rows()
    epoch = (
        rows.filter(pk=pk, status__in=DISCARDABLE_STATUSES)
        .values_list("lease_epoch", flat=True)
        .first()
    )
    if epoch is None:
        return False
    updated = rows.filter(
        pk=pk, status__in=DISCARDABLE_STATUSES, lease_epoch=epoch
    ).update(
        status=OxTask.Status.DISCARDED,
        finished_at=timezone.now(),
        locked_by=None,
        locked_at=None,
        lease_expires_at=None,
    )
    return updated == 1


def _key_order(alias: str) -> Callable[[uuid.UUID], bytes]:
    """
    A sort key that puts primary keys in the order the database `alias` keeps
    them in, which is the order an ORDER BY on the key reads and locks them.

    PostgreSQL, MySQL and SQLite order a UUIDField by its bytes. On MariaDB
    10.7 and later, Django gives it MariaDB's own uuid column, which orders
    some values by their fields in reverse. Every uuid4 is one of them.
    """
    connection = connections[alias]
    if connection.vendor == "mysql" and connection.features.has_native_uuid_field:
        return _mariadb_uuid_order
    return _uuid_bytes


def _uuid_bytes(pk: uuid.UUID) -> bytes:
    return pk.bytes


def _mariadb_uuid_order(pk: uuid.UUID) -> bytes:
    # MariaDB orders a uuid value by its node, clock sequence, time-high,
    # time-mid and time-low fields, in that order, when its seventh byte is
    # 0x01 to 0x5f and its ninth byte has the top bit set. Any other value it
    # orders byte by byte.
    raw = pk.bytes
    if 0 < raw[6] < 0x60 and raw[8] & 0x80:
        return raw[10:] + raw[8:10] + raw[6:8] + raw[4:6] + raw[:4]
    return raw


def _ids(
    selection: QuerySet[OxTask] | Iterable[str | uuid.UUID],
    alias: str,
) -> tuple[list[uuid.UUID], int]:
    """
    The distinct usable ids, and how many distinct items were malformed.

    A queryset is read on `alias`, whatever it was built on, because the
    UPDATE that follows runs there. Reading the ids anywhere else would
    move rows chosen on one database by their state on another.
    """
    if isinstance(selection, QuerySet):
        raw: Iterable[str | uuid.UUID] = selection.using(alias).values_list(
            "pk", flat=True
        )
    else:
        raw = selection
    seen: dict[uuid.UUID, None] = {}
    malformed: set[str] = set()
    for item in raw:
        pk = _pk(item)
        if pk is None:
            malformed.add(str(item))
        else:
            seen.setdefault(pk, None)
    return list(seen), len(malformed)


def _move_in_key_order(
    alias: str,
    ids: list[uuid.UUID],
    statuses: tuple[OxTask.Status, ...],
    values: dict[str, Any],
    condition: Q | None = None,
) -> int:
    """
    The _many forms' write. Returns how many rows moved.

    The ids go in the database's key order, one chunk at a time. On a
    database with row locks a locking read of the chunk's ids comes first.
    The transaction is opened on `alias`, which is where every statement
    below goes, the locking read included. `condition`, when given, is one
    more thing a row must satisfy to move, checked by the same UPDATE.
    """
    where = (
        Q(status__in=statuses)
        if condition is None
        else Q(status__in=statuses) & condition
    )
    locking_read = connections[alias].features.has_select_for_update
    ordered = sorted(ids, key=_key_order(alias))

    def move() -> int:
        changed = 0
        with transaction.atomic(using=alias):
            for start in range(0, len(ordered), UPDATE_CHUNK_SIZE):
                chunk = OxTask.objects.using(alias).filter(
                    pk__in=ordered[start : start + UPDATE_CHUNK_SIZE]
                )
                if locking_read:
                    list(
                        chunk.select_for_update()
                        .order_by("pk")
                        .values_list("pk", flat=True)
                    )
                changed += chunk.filter(where).update(**values)
        return changed

    return _contention.run(alias, move)


def retry_many(
    selection: QuerySet[OxTask] | Iterable[str | uuid.UUID],
) -> tuple[int, int]:
    """
    retry() for a selection, in one conditional UPDATE per thousand rows.

    Takes a queryset or any iterable of ids and returns (changed, skipped):
    how many rows were requeued and how many were left alone because their
    status did not allow it, were not there, or were malformed ids.
    Duplicate ids count once.

    The move is the one retry() makes, and so is the fence. Each UPDATE is
    conditional on status in RETRYABLE_STATUSES and bumps lease_epoch in
    the same statement, so a LOST row's missing worker holds a number that
    matches nothing afterwards, and a concurrent retry or discard of the
    same row finds it already READY and matches zero rows. max_attempts
    becomes attempts + 1 from the row's own count, so the next claim is
    the one extra attempt this grants. A row whose attempts are already at
    32767, where attempts + 1 does not fit the column, is refused by the
    same UPDATE and counted as skipped. The whole call runs in one
    transaction: an error part-way leaves every row as it was.

    The ids go in key order, and each chunk is locked before its UPDATE, as
    the module docstring says. A deadlock or serialization failure in a
    transaction this call opened starts the call again, three attempts in all.
    """
    alias = router.db_for_write(OxTask)
    ids, malformed = _ids(selection, alias)
    changed = _move_in_key_order(
        alias,
        ids,
        RETRYABLE_STATUSES,
        {
            "status": OxTask.Status.READY,
            "lease_epoch": F("lease_epoch") + 1,
            "max_attempts": F("attempts") + 1,
            "run_after": None,
            "finished_at": None,
            "locked_by": None,
            "locked_at": None,
            "lease_expires_at": None,
        },
        _RETRY_FITS,
    )
    return changed, len(ids) + malformed - changed


def discard_many(
    selection: QuerySet[OxTask] | Iterable[str | uuid.UUID],
) -> tuple[int, int]:
    """
    discard() for a selection, in one conditional UPDATE per thousand rows.

    Takes a queryset or any iterable of ids and returns (changed, skipped),
    counted the same way as retry_many(). Each UPDATE is conditional on
    status in DISCARDABLE_STATUSES, so a READY row that a worker claims in
    the same instant goes to exactly one of them, and the epoch is not
    bumped, for the reason given on discard(). One transaction for the
    whole call, with the key order and the retries retry_many() has.
    """
    alias = router.db_for_write(OxTask)
    ids, malformed = _ids(selection, alias)
    changed = _move_in_key_order(
        alias,
        ids,
        DISCARDABLE_STATUSES,
        {
            "status": OxTask.Status.DISCARDED,
            "finished_at": timezone.now(),
            "locked_by": None,
            "locked_at": None,
            "lease_expires_at": None,
        },
    )
    return changed, len(ids) + malformed - changed
