"""
Read-only queue metrics computed from the task table.

Every function here is a plain ORM query over OxTask: no extra state, no
signals, no worker involvement. Safe to call from a request, a shell, a
health check, or a metrics exporter, on any database django-ox supports.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from django.db.models import Count, Max, Min, Q, QuerySet
from django.db.models.functions import Coalesce
from django.utils import timezone

from .models import OxTask

__all__ = [
    "DEFAULT_WINDOW",
    "QueueStats",
    "failure_rate",
    "last_claim_age",
    "oldest_ready_age",
    "queue_stats",
    "ready_count",
    "throughput",
]

DEFAULT_WINDOW = timedelta(minutes=5)


@dataclass(frozen=True)
class QueueStats:
    """Per-status row counts for one queue."""

    queue_name: str
    ready: int
    running: int
    failed: int
    successful: int


def _for_queue(queryset: QuerySet[OxTask], queue_name: str | None) -> QuerySet[OxTask]:
    if queue_name is not None:
        queryset = queryset.filter(queue_name=queue_name)
    return queryset


def _ready(queue_name: str | None, now: datetime) -> QuerySet[OxTask]:
    # Mirrors the worker's dequeue predicate: READY and due, so a task
    # deferred to a future run_after does not count as backlog.
    queryset = OxTask.objects.filter(status=OxTask.Status.READY).filter(
        Q(run_after__isnull=True) | Q(run_after__lte=now)
    )
    return _for_queue(queryset, queue_name)


def _finished(window: timedelta, queue_name: str | None) -> QuerySet[OxTask]:
    if window <= timedelta(0):
        raise ValueError("window must be a positive timedelta.")
    queryset = OxTask.objects.filter(
        status__in=(OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED),
        finished_at__gte=timezone.now() - window,
    )
    return _for_queue(queryset, queue_name)


def queue_stats() -> list[QueueStats]:
    """
    Raw row counts per queue and status, one entry per queue that has any
    rows, ordered by queue name. Unlike ready_count(), the ready column
    counts every READY row including tasks deferred to the future.
    """
    rows = (
        OxTask.objects.values("queue_name")
        .annotate(
            ready=Count("pk", filter=Q(status=OxTask.Status.READY)),
            running=Count("pk", filter=Q(status=OxTask.Status.RUNNING)),
            failed=Count("pk", filter=Q(status=OxTask.Status.FAILED)),
            successful=Count("pk", filter=Q(status=OxTask.Status.SUCCESSFUL)),
        )
        .order_by("queue_name")
    )
    return [QueueStats(**row) for row in rows]


def ready_count(queue_name: str | None = None) -> int:
    """READY tasks currently eligible to run (run_after unset or passed)."""
    return _ready(queue_name, timezone.now()).count()


def oldest_ready_age(queue_name: str | None = None) -> timedelta | None:
    """
    Age of the oldest task waiting to run, or None when nothing waits.

    Age is measured from the moment the task became eligible: run_after
    when set (deferred tasks and retries), enqueued_at otherwise. A task
    deferred by a week does not show up as a week of backlog.
    """
    now = timezone.now()
    oldest: datetime | None = _ready(queue_name, now).aggregate(
        oldest=Min(Coalesce("run_after", "enqueued_at"))
    )["oldest"]
    if oldest is None:
        return None
    return now - oldest


def throughput(
    window: timedelta = DEFAULT_WINDOW, queue_name: str | None = None
) -> float:
    """
    Tasks that reached a terminal state (SUCCESSFUL or FAILED) per
    minute, over the trailing window.
    """
    finished = _finished(window, queue_name).count()
    return finished / (window.total_seconds() / 60.0)


def failure_rate(
    window: timedelta = DEFAULT_WINDOW, queue_name: str | None = None
) -> float | None:
    """
    Fraction of terminal outcomes in the trailing window that FAILED,
    between 0.0 and 1.0, or None when nothing finished in the window.
    Retries still pending are not outcomes and do not count.
    """
    counts = _finished(window, queue_name).aggregate(
        finished=Count("pk"),
        failed=Count("pk", filter=Q(status=OxTask.Status.FAILED)),
    )
    finished: int = counts["finished"]
    if finished == 0:
        return None
    failed: int = counts["failed"]
    return failed / finished


def last_claim_age(queue_name: str | None = None) -> timedelta | None:
    """
    Time since any worker last claimed a task (from last_attempted_at,
    which every claim writes), or None when no task was ever claimed.

    This is claim activity, not a heartbeat: workers idling over an empty
    queue record nothing, so it only signals liveness on queues with
    steady traffic.
    """
    now = timezone.now()
    latest: datetime | None = _for_queue(OxTask.objects.all(), queue_name).aggregate(
        latest=Max("last_attempted_at")
    )["latest"]
    if latest is None:
        return None
    return now - latest
