"""
The reaper must not take a task back from a worker that is still holding it.

A row is selected because its lease looks expired, and the reclaim happens a
moment later. In between, the worker holding it can renew: it was briefly late
rather than dead, which is the case the lease exists to tolerate. Renewing
writes `locked_at` and nothing else, so an epoch compare alone cannot see it,
and reclaiming anyway hands a running task to a second worker.
"""

from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db


class _RenewOnSelect:
    """`OxTask.objects`, with a renewal landing inside the reaper's window.

    Only the reaper's stuck-select carries `{status, locked_at__lt}`, so this
    leaves every other query alone and puts the renewal exactly between that
    selection and the compare-and-set that follows it.
    """

    def __init__(self, real, renew):
        self._real = real
        self._renew = renew

    def __getattr__(self, name):
        return getattr(self._real, name)

    def filter(self, **kwargs):
        queryset = self._real.filter(**kwargs)
        if set(kwargs) != {"status", "locked_at__lt"}:
            return queryset
        rows = list(queryset)
        renew = self._renew

        class _Selected:
            def __iter__(self):
                renew()
                return iter(rows)

        return _Selected()


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
        # Exactly what renew_leases writes: locked_at, nothing else.
        OxTask.objects.filter(
            pk=task.pk, status=OxTask.Status.RUNNING, locked_by=locked_by
        ).update(locked_at=timezone.now())

    monkeypatch.setattr(OxTask, "objects", _RenewOnSelect(OxTask.objects, renew))
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
