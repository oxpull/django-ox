"""
Operator actions on stored tasks: retry one that is settled, or discard one
that has not run. The admin actions call these; so can a shell or a view.

Each function is one compare-and-set UPDATE keyed on the row's status and
its lease epoch, so it either moves the row from the state it read or does
nothing and reports that. Neither ever touches a RUNNING row: a running
task belongs to the worker holding its lease, and the reaper is the only
party that takes a lease away.
"""

from __future__ import annotations

import uuid

from django.utils import timezone

from .models import OxTask

__all__ = ["DISCARDABLE_STATUSES", "RETRYABLE_STATUSES", "discard", "retry"]

RETRYABLE_STATUSES = (OxTask.Status.FAILED, OxTask.Status.LOST)
DISCARDABLE_STATUSES = (
    OxTask.Status.READY,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
)


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
    Claiming increments attempts, the same as any other claim. run_after is
    cleared so the retry is eligible at once rather than after a backoff
    written for a failure that has already been dealt with.
    """
    pk = _pk(result_id)
    if pk is None:
        return False
    row = (
        OxTask.objects.filter(pk=pk, status__in=RETRYABLE_STATUSES)
        .values("lease_epoch", "attempts")
        .first()
    )
    if row is None:
        return False
    updated = OxTask.objects.filter(
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
    )
    return updated == 1


def discard(result_id: str | uuid.UUID) -> bool:
    """
    Close a READY, FAILED or LOST task without running it.

    Returns True when the row was marked DISCARDED, False when it was not
    there or was in any other state. A RUNNING row is never matched: the
    worker holding it keeps it, and discarding it would be a verdict on
    work still in progress.

    The UPDATE is a compare-and-set on (status in DISCARDABLE_STATUSES,
    lease_epoch as read), so a READY row that a worker claims in the same
    instant goes to exactly one of them. The epoch is not bumped: DISCARDED
    is not a state any worker writes onto, so there is no holder to fence
    out, and a LOST row's straggler write is already refused by status.
    The row keeps attempts, worker_ids and errors as they were.
    """
    pk = _pk(result_id)
    if pk is None:
        return False
    epoch = (
        OxTask.objects.filter(pk=pk, status__in=DISCARDABLE_STATUSES)
        .values_list("lease_epoch", flat=True)
        .first()
    )
    if epoch is None:
        return False
    updated = OxTask.objects.filter(
        pk=pk, status__in=DISCARDABLE_STATUSES, lease_epoch=epoch
    ).update(
        status=OxTask.Status.DISCARDED,
        finished_at=timezone.now(),
        locked_by=None,
        locked_at=None,
    )
    return updated == 1
