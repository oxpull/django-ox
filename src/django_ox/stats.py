"""
Read-only queue metrics computed from the task table.

Every function here is a plain ORM query over OxTask: no extra state, no
signals, no worker involvement. Safe to call from a request, a shell, a
health check, or a metrics exporter, on any database django-ox supports.

Each reading comes from the alias the task rows are written to, which
``using`` overrides. It is never the alias ``db_for_read`` points at. Under
a router that sends reads to a replica, a replica that is behind reports a
backlog that has already been worked off, or none at all, and a health
check reading it passes while the queue is stuck.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from django.db import router
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
    "waiting_counts",
]

DEFAULT_WINDOW = timedelta(minutes=5)


def _alias(using: str | None) -> str:
    """The alias to read on: the one given, or the one OxTask writes to."""
    return using if using is not None else router.db_for_write(OxTask)


def _tasks(alias: str) -> QuerySet[OxTask]:
    return OxTask.objects.using(alias)


@dataclass(frozen=True)
class QueueStats:
    """
    Per-status row counts for one queue.

    lost counts rows whose worker stopped renewing its lease with no
    attempts left. Those are settled, so they are not backlog, but nobody
    observed how they ended, so they are neither a success nor a failure
    and they are not folded into either column. A steady lost count means
    workers are being reclaimed; see the Production guide on LOCK_TIMEOUT.

    discarded counts rows closed without running. Settled, not backlog, and
    not an outcome of any attempt.

    WAITING rows are in no column. waiting_counts() reports them, so the
    fields here stay the ones code already unpacks and compares.
    """

    queue_name: str
    ready: int
    running: int
    failed: int
    successful: int
    lost: int = 0
    discarded: int = 0


def _for_queue(queryset: QuerySet[OxTask], queue_name: str | None) -> QuerySet[OxTask]:
    if queue_name is not None:
        queryset = queryset.filter(queue_name=queue_name)
    return queryset


def _ready(alias: str, queue_name: str | None, now: datetime) -> QuerySet[OxTask]:
    # Mirrors the worker's dequeue predicate: READY and due, so a task
    # deferred to a future run_after does not count as backlog.
    queryset = (
        _tasks(alias)
        .filter(status=OxTask.Status.READY)
        .filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
    )
    return _for_queue(queryset, queue_name)


def _finished(
    alias: str, window: timedelta, queue_name: str | None
) -> QuerySet[OxTask]:
    if window <= timedelta(0):
        raise ValueError("window must be a positive timedelta.")
    queryset = _tasks(alias).filter(
        status__in=(OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED),
        finished_at__gte=timezone.now() - window,
    )
    return _for_queue(queryset, queue_name)


def queue_stats(using: str | None = None) -> list[QueueStats]:
    """
    Raw row counts per queue and status, one entry per queue that has any
    rows, ordered by queue name. Unlike ready_count(), the ready column
    counts every READY row including tasks deferred to the future. A queue
    whose rows are all WAITING gets an entry of zeros, and waiting_counts()
    has its count.
    """
    return [
        QueueStats(**{name: count for name, count in row.items() if name != "waiting"})
        for row in _status_counts(_alias(using))
    ]


def waiting_counts(using: str | None = None) -> dict[str, int]:
    """
    WAITING rows per queue, for each queue that has any.

    A waiting task is held back from every worker until something releases
    it. django-ox never puts a task there by itself. It is not settled, and
    it is not backlog: ready_count() and oldest_ready_age() leave it out.
    """
    rows = (
        _tasks(_alias(using))
        .filter(status=OxTask.Status.WAITING)
        .values("queue_name")
        .annotate(n=Count("pk"))
        .order_by()
    )
    return {row["queue_name"]: row["n"] for row in rows}


def _status_counts(alias: str) -> list[Mapping[str, Any]]:
    # Every status per queue in one grouped query, ordered by queue name.
    # queue_stats() drops the waiting column, and django_ox.metrics reads
    # every column, so a scrape still costs one query for the counts.
    return list(
        _tasks(alias)
        .values("queue_name")
        .annotate(
            ready=Count("pk", filter=Q(status=OxTask.Status.READY)),
            running=Count("pk", filter=Q(status=OxTask.Status.RUNNING)),
            failed=Count("pk", filter=Q(status=OxTask.Status.FAILED)),
            successful=Count("pk", filter=Q(status=OxTask.Status.SUCCESSFUL)),
            lost=Count("pk", filter=Q(status=OxTask.Status.LOST)),
            discarded=Count("pk", filter=Q(status=OxTask.Status.DISCARDED)),
            waiting=Count("pk", filter=Q(status=OxTask.Status.WAITING)),
        )
        .order_by("queue_name")
    )


def ready_count(queue_name: str | None = None, using: str | None = None) -> int:
    """READY tasks currently eligible to run (run_after unset or passed)."""
    return _ready(_alias(using), queue_name, timezone.now()).count()


def oldest_ready_age(
    queue_name: str | None = None, using: str | None = None
) -> timedelta | None:
    """
    Age of the oldest eligible READY task, or None when there is none.

    Age is measured from the moment the task became eligible: run_after
    when set (deferred tasks and retries), enqueued_at otherwise. A task
    deferred by a week does not show up as a week of backlog.
    """
    now = timezone.now()
    oldest: datetime | None = _ready(_alias(using), queue_name, now).aggregate(
        oldest=Min(Coalesce("run_after", "enqueued_at"))
    )["oldest"]
    if oldest is None:
        return None
    return now - oldest


def throughput(
    window: timedelta = DEFAULT_WINDOW,
    queue_name: str | None = None,
    using: str | None = None,
) -> float:
    """
    Tasks that reached a terminal state (SUCCESSFUL or FAILED) per
    minute, over the trailing window.
    """
    finished = _finished(_alias(using), window, queue_name).count()
    return finished / (window.total_seconds() / 60.0)


def failure_rate(
    window: timedelta = DEFAULT_WINDOW,
    queue_name: str | None = None,
    using: str | None = None,
) -> float | None:
    """
    Fraction of terminal outcomes in the trailing window that FAILED,
    between 0.0 and 1.0, or None when nothing finished in the window.
    Retries still pending are not outcomes and do not count.
    """
    counts = _finished(_alias(using), window, queue_name).aggregate(
        finished=Count("pk"),
        failed=Count("pk", filter=Q(status=OxTask.Status.FAILED)),
    )
    finished: int = counts["finished"]
    if finished == 0:
        return None
    failed: int = counts["failed"]
    return failed / finished


def last_claim_age(
    queue_name: str | None = None, using: str | None = None
) -> timedelta | None:
    """
    Time since any worker last claimed a task (from last_attempted_at,
    which every claim writes), or None when no task was ever claimed.

    This is claim activity, not a heartbeat: workers idling over an empty
    queue record nothing, so it only signals liveness on queues with
    steady traffic.
    """
    now = timezone.now()
    latest: datetime | None = _for_queue(
        _tasks(_alias(using)).all(), queue_name
    ).aggregate(latest=Max("last_attempted_at"))["latest"]
    if latest is None:
        return None
    return now - latest


# Grouped forms of the readings above, one GROUP BY each, for callers that
# need every queue at once. django_ox.metrics reads these so a scrape's cost
# does not grow with the number of queues.


def _ready_counts(alias: str, now: datetime) -> dict[str, int]:
    rows = (
        _ready(alias, None, now).values("queue_name").annotate(n=Count("pk")).order_by()
    )
    return {row["queue_name"]: row["n"] for row in rows}


def _oldest_ready(alias: str, now: datetime) -> dict[str, datetime]:
    rows = (
        _ready(alias, None, now)
        .values("queue_name")
        .annotate(oldest=Min(Coalesce("run_after", "enqueued_at")))
        .order_by()
    )
    return {row["queue_name"]: row["oldest"] for row in rows}


def _last_claims(alias: str) -> dict[str, datetime]:
    rows = (
        _tasks(alias)
        .values("queue_name")
        .annotate(latest=Max("last_attempted_at"))
        .order_by()
    )
    return {
        row["queue_name"]: row["latest"] for row in rows if row["latest"] is not None
    }


def _finished_counts(alias: str, window: timedelta) -> dict[str, tuple[int, int]]:
    rows = (
        _finished(alias, window, None)
        .values("queue_name")
        .annotate(
            finished=Count("pk"),
            failed=Count("pk", filter=Q(status=OxTask.Status.FAILED)),
        )
        .order_by()
    )
    return {row["queue_name"]: (row["finished"], row["failed"]) for row in rows}
