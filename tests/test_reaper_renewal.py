"""
The reaper must not take a task back from a worker that is still holding it.

A row is selected because its lease looks expired, and the reclaim happens a
moment later. In between, the worker holding it can renew: it was briefly late
rather than dead, which is the case the lease exists to tolerate. Renewing
writes `locked_at` and nothing else, so an epoch compare alone cannot see it,
and reclaiming anyway hands a running task to a second worker.
"""

import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db


def _is_the_stuck_select(args, kwargs):
    """Only the reaper's stuck-select has this shape.

    It is one positional Q, the expiry-or-legacy-lock condition, plus
    `status` as the single keyword. Everything else the reaper touches is
    left alone so the interception lands on one statement.
    """
    return len(args) == 1 and set(kwargs) == {"status"}


class _RenewOnReap:
    """`OxTask.objects`, with a renewal landing inside the reaper's window.

    Only the reaper's stuck-select carries `{status, locked_at__lt}`, so this
    leaves every other query alone and wraps just that one.
    """

    def __init__(self, real, renew):
        self._real = real
        self._renew = renew

    def __getattr__(self, name):
        return getattr(self._real, name)

    def using(self, alias):
        # The reaper pins every statement to its write alias, so the
        # interception point is `objects.using(alias).filter(...)` rather than
        # `objects.filter(...)`.
        return _RenewOnReap(self._real.using(alias), self._renew)

    def filter(self, *args, **kwargs):
        queryset = self._real.filter(*args, **kwargs)
        if not _is_the_stuck_select(args, kwargs):
            return queryset
        return _RenewsBeforeActing(queryset, self._renew)


class _RenewsBeforeActing:
    """The stuck set, renewed by its holder the instant before the reaper acts.

    The reaper narrows that set and then either takes the whole of it in one
    UPDATE or walks it a row at a time. Both are the moment it acts, so the
    renewal goes immediately ahead of whichever one comes. A reclaim that
    survives this is a reclaim that resolved the lease before writing, which
    is the defect.
    """

    def __init__(self, queryset, renew):
        self._queryset = queryset
        self._renew = renew

    def _wrap(self, queryset):
        return _RenewsBeforeActing(queryset, self._renew)

    def filter(self, *args, **kwargs):
        return self._wrap(self._queryset.filter(*args, **kwargs))

    def only(self, *args, **kwargs):
        return self._wrap(self._queryset.only(*args, **kwargs))

    def order_by(self, *args, **kwargs):
        return self._wrap(self._queryset.order_by(*args, **kwargs))

    def values_list(self, *args, **kwargs):
        # The reclaim's log manifest. It reads, so the renewal goes ahead of
        # it too; what matters is that the write below still refuses.
        self._renew()
        return self._queryset.values_list(*args, **kwargs)

    def __getitem__(self, item):
        return self._wrap(self._queryset[item])

    def update(self, **kwargs):
        self._renew()
        return self._queryset.update(**kwargs)

    def __iter__(self):
        self._renew()
        return iter(self._queryset)


def a_running_task(**over):
    stale = timezone.now() - timedelta(hours=1)
    fields = {
        "task_path": "tests.tasks.add",
        "args": [1, 2],
        "kwargs": {},
        "queue_name": "default",
        "status": OxTask.Status.RUNNING,
        "locked_by": "worker-A",
        "locked_at": stale,
        "lease_expires_at": stale,
        "lease_epoch": 12,
        "attempts": 1,
        "max_attempts": 3,
        "enqueued_at": stale,
    }
    fields.update(over)
    return OxTask.objects.create(**fields)


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0)


def _reap_with_renewal(monkeypatch, worker, task, locked_by="worker-A"):
    def renew():
        # Exactly what renew_leases writes: the lock timestamp and the
        # expiry that goes with it.
        now = timezone.now()
        OxTask.objects.filter(
            pk=task.pk, status=OxTask.Status.RUNNING, locked_by=locked_by
        ).update(
            locked_at=now,
            lease_expires_at=now + timedelta(seconds=300),
        )

    monkeypatch.setattr(OxTask, "objects", _RenewOnReap(OxTask.objects, renew))
    try:
        return worker.reap()
    finally:
        monkeypatch.undo()


class TestARenewedLeaseIsNotReclaimed:
    def test_a_worker_that_renews_keeps_its_task(self, worker, monkeypatch):
        task = a_running_task()
        reclaimed = _reap_with_renewal(monkeypatch, worker, task)
        task.refresh_from_db()
        assert reclaimed == 0, "the reaper took a task whose lease was live"
        assert task.status == OxTask.Status.RUNNING
        assert task.locked_by == "worker-A", "the holder lost its claim"
        assert task.lease_epoch == 12, "the epoch moved, so a claim would be refused"

    def test_the_same_holds_on_the_exhausted_branch(self, worker, monkeypatch):
        # Out of attempts, so the reclaim would write LOST rather than READY.
        # A live worker must not have its task declared abandoned underneath it.
        task = a_running_task(attempts=3, max_attempts=3)
        reclaimed = _reap_with_renewal(monkeypatch, worker, task)
        task.refresh_from_db()
        assert reclaimed == 0
        assert task.status == OxTask.Status.RUNNING, "a live task was marked LOST"
        assert task.errors == []

    def test_a_genuinely_abandoned_task_is_still_reclaimed(self, worker):
        # The reaper's whole job, unchanged: no renewal, so it must act.
        task = a_running_task()
        assert worker.reap() == 1
        task.refresh_from_db()
        assert task.status == OxTask.Status.READY
        assert task.locked_by is None
        assert task.lease_epoch == 13

    def test_an_abandoned_task_with_no_attempts_left_is_still_lost(self, worker):
        task = a_running_task(attempts=3, max_attempts=3)
        assert worker.reap() == 1
        task.refresh_from_db()
        assert task.status == OxTask.Status.LOST
        assert task.errors, "the lost lease was not recorded"


class _SwapTheStuckSet:
    """`OxTask.objects` with the stuck set changing between read and write.

    The reaper's cutoff is a database expression re-evaluated per statement,
    so the write sees a later cutoff than the read. One row can leave the set
    (its holder renewed) while another joins it (its lease aged out), which
    leaves the count unchanged and the membership different.
    """

    def __init__(self, real, swap):
        self._real = real
        self._swap = swap

    def __getattr__(self, name):
        return getattr(self._real, name)

    def using(self, alias):
        return _SwapTheStuckSet(self._real.using(alias), self._swap)

    def filter(self, *args, **kwargs):
        queryset = self._real.filter(*args, **kwargs)
        if not _is_the_stuck_select(args, kwargs):
            return queryset
        return _SwapsBeforeTheWrite(queryset, self._swap)


class _SwapsBeforeTheWrite:
    def __init__(self, queryset, swap):
        self._queryset = queryset
        self._swap = swap

    def _wrap(self, queryset):
        return _SwapsBeforeTheWrite(queryset, self._swap)

    def filter(self, *args, **kwargs):
        return self._wrap(self._queryset.filter(*args, **kwargs))

    def only(self, *args, **kwargs):
        return self._wrap(self._queryset.only(*args, **kwargs))

    def order_by(self, *args, **kwargs):
        return self._wrap(self._queryset.order_by(*args, **kwargs))

    def __getitem__(self, item):
        return self._wrap(self._queryset[item])

    def values_list(self, *args, **kwargs):
        return self._queryset.values_list(*args, **kwargs)

    def update(self, **kwargs):
        self._swap()
        return self._queryset.update(**kwargs)

    def __iter__(self):
        return iter(self._queryset)


class TestAReclaimRecordNamesOnlyWhatItReclaimed:
    """
    This record is what an operator reads to explain a task that ran twice, so
    a false entry in it is worse than no entry. Comparing how many rows were
    read against how many were written is only a meaningful check if the set can
    shrink and not grow: one row leaving and one joining keeps the numbers
    equal and changes every name.
    """

    def test_a_set_that_swaps_members_names_nobody(self, worker, monkeypatch, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        leaves = a_running_task(locked_by="worker-late")
        joins = a_running_task(locked_by="worker-dying")
        # `joins` is inside the cutoff when the set is read, so only `leaves`
        # is on the manifest.
        OxTask.objects.filter(pk=joins.pk).update(
            locked_at=timezone.now() - timedelta(seconds=1),
            lease_expires_at=timezone.now() + timedelta(seconds=299),
        )

        def swap():
            # The late worker renews, and the dying one's lease ages out.
            now = timezone.now()
            OxTask.objects.filter(pk=leaves.pk).update(
                locked_at=now, lease_expires_at=now + timedelta(seconds=300)
            )
            OxTask.objects.filter(pk=joins.pk).update(
                locked_at=now - timedelta(hours=2),
                lease_expires_at=now - timedelta(hours=1),
            )

        monkeypatch.setattr(OxTask, "objects", _SwapTheStuckSet(OxTask.objects, swap))
        try:
            worker.reap()
        finally:
            monkeypatch.undo()

        leaves.refresh_from_db()
        assert leaves.status == OxTask.Status.RUNNING, (
            "the renewing worker lost its task"
        )
        for record in caplog.records:
            if getattr(record, "event", None) != "task_reclaimed":
                continue
            if not hasattr(record, "task_id"):
                continue  # the count-only record names nobody, which is fine
            named = OxTask.objects.get(pk=record.task_id)
            assert named.status == OxTask.Status.READY, (
                f"named {record.task_id} ({named.status}) as reclaimed; "
                f"held_by={getattr(record, 'held_by', None)}"
            )
