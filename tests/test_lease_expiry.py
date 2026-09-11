"""
The row carries its own lease deadline, and every reaper judges that.

Derived from each reaper's own LOCK_TIMEOUT, the deadline is whatever the
observing worker happens to be configured with. A rolling deploy that changes
the setting then puts two answers in one fleet: a worker renewing correctly on
the longer cadence is reclaimed mid-execution by one running the shorter, and
the lease number protects that worker's finish write rather than the work it is
doing.

NULL is a lease taken before the column existed. Those keep the old comparison
until a renewal fills the column in, which is what lets a fleet upgrade one
worker at a time with nothing for an operator to run.
"""

from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.actions import expire_lease
from django_ox.models import OxTask
from django_ox.worker import Worker, _lease_expiry, _lease_now

from . import tasks

pytestmark = pytest.mark.django_db


def a_worker(settings, **kwargs):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, **kwargs)


@pytest.fixture
def worker(settings):
    return a_worker(settings)


class TestTheClaimStampsTheExpiry:
    def test_a_claimed_row_carries_its_own_deadline(self, worker):
        tasks.add.enqueue(1, 2)
        db_task = worker.claim_one()
        db_task.refresh_from_db()
        assert db_task.lease_expires_at is not None, (
            "the claim left no expiry, so every reaper invents one again"
        )
        ahead = db_task.lease_expires_at - db_task.locked_at
        assert timedelta(seconds=worker.lock_timeout - 2) <= ahead
        assert ahead <= timedelta(seconds=worker.lock_timeout + 2)

    def test_renewing_moves_the_expiry_with_the_lock(self, worker):
        tasks.add.enqueue(1, 2)
        db_task = worker.claim_one()
        db_task.refresh_from_db()
        first = db_task.lease_expires_at
        # Pretend the lease is nearly up, then renew.
        OxTask.objects.filter(pk=db_task.pk).update(
            locked_at=timezone.now() - timedelta(seconds=worker.lock_timeout - 1),
            lease_expires_at=timezone.now() + timedelta(seconds=1),
        )
        worker._in_flight.add((db_task.pk, db_task.lease_epoch))
        assert worker.renew_leases() == 1
        db_task.refresh_from_db()
        assert db_task.lease_expires_at > first - timedelta(seconds=2), (
            "the renewal refreshed locked_at and left the row expiring"
        )


class TestTheStoredExpiryDecidesWhoIsReaped:
    def _running(self, *, expires_in, locked_ago, attempts=1):
        now = timezone.now()
        return OxTask.objects.create(
            task_path="tests.tasks.add",
            args=[1, 2],
            kwargs={},
            queue_name="default",
            status=OxTask.Status.RUNNING,
            locked_by="worker-A",
            locked_at=now - timedelta(seconds=locked_ago),
            lease_expires_at=(
                None if expires_in is None else now + timedelta(seconds=expires_in)
            ),
            lease_epoch=4,
            attempts=attempts,
            max_attempts=3,
            enqueued_at=now,
        )

    def test_a_live_expiry_survives_a_reaper_with_a_shorter_timeout(self, settings):
        # The holder took a 600s lease and is renewing on that cadence. This
        # reaper runs the new 30s setting, and the row is 60s old.
        task = self._running(expires_in=540, locked_ago=60)
        reaper = a_worker(settings, lock_timeout=30)
        assert reaper.reap() == 0, (
            "a worker renewing correctly on its own lease was reclaimed by a "
            "reaper configured with a shorter one"
        )
        task.refresh_from_db()
        assert task.status == OxTask.Status.RUNNING
        assert task.lease_epoch == 4

    def test_a_passed_expiry_is_reaped_by_a_worker_with_a_longer_timeout(
        self, settings
    ):
        # The mirror image: the row's own lease is up, and this reaper's
        # generous timeout must not keep it alive.
        task = self._running(expires_in=-5, locked_ago=10)
        reaper = a_worker(settings, lock_timeout=3600)
        assert reaper.reap() == 1, (
            "the row's lease had expired and the reaper judged it by its own "
            "timeout instead"
        )
        task.refresh_from_db()
        assert task.status == OxTask.Status.READY
        assert task.lease_epoch == 5
        assert task.lease_expires_at is None, "a released lease kept its expiry"

    def test_the_exhausted_branch_reads_the_expiry_too(self, settings):
        task = self._running(expires_in=540, locked_ago=60, attempts=3)
        assert a_worker(settings, lock_timeout=30).reap() == 0
        task.refresh_from_db()
        assert task.status == OxTask.Status.RUNNING, "a live task was marked LOST"


class TestALeaseFromBeforeTheColumnExisted:
    def _legacy(self, *, locked_ago):
        now = timezone.now()
        return OxTask.objects.create(
            task_path="tests.tasks.add",
            args=[1, 2],
            kwargs={},
            queue_name="default",
            status=OxTask.Status.RUNNING,
            locked_by="worker-old",
            locked_at=now - timedelta(seconds=locked_ago),
            lease_expires_at=None,
            lease_epoch=1,
            attempts=1,
            max_attempts=3,
            enqueued_at=now,
        )

    def test_a_fresh_legacy_lease_is_left_alone(self, worker):
        task = self._legacy(locked_ago=5)
        assert worker.reap() == 0
        task.refresh_from_db()
        assert task.status == OxTask.Status.RUNNING

    def test_a_stale_legacy_lease_is_still_reclaimed(self, worker):
        task = self._legacy(locked_ago=worker.lock_timeout + 60)
        assert worker.reap() == 1, (
            "a row upgraded into this version was never reclaimable again"
        )
        task.refresh_from_db()
        assert task.status == OxTask.Status.READY

    def test_a_renewal_fills_the_column_in(self, worker):
        task = self._legacy(locked_ago=5)
        worker._in_flight.add((task.pk, task.lease_epoch))
        worker.worker_id = "worker-old"
        assert worker.renew_leases() == 1
        task.refresh_from_db()
        assert task.lease_expires_at is not None, (
            "the fleet never converges: this row is judged by whichever "
            "reaper sees it, for as long as it lives"
        )


class TestExpiringALeaseByHand:
    def test_a_running_lease_can_be_expired_now(self, worker):
        tasks.add.enqueue(1, 2)
        db_task = worker.claim_one()
        assert expire_lease(db_task.pk) is True
        assert worker.reap() == 1, "the expired lease was not reclaimed"
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.READY

    def test_a_task_that_is_not_running_is_not_touched(self, worker):
        result = tasks.add.enqueue(1, 2)
        assert expire_lease(result.id) is False
        assert OxTask.objects.get(pk=result.id).status == OxTask.Status.READY

    def test_an_unknown_id_is_false_rather_than_an_error(self):
        assert expire_lease("not-a-uuid") is False


class TestTheExpiryFollowsTheLeaseClock:
    """
    One lease, one clock. If the expiry were stamped from the process while
    `locked_at` came from the database, or the reverse, the stored deadline
    would relocate the disagreement it exists to remove rather than removing
    it: the reaper would compare a column one clock wrote against a cutoff
    another clock produced.
    """

    def test_it_is_derived_from_lease_now(self, monkeypatch, settings):
        settings.USE_TZ = False
        moved = timezone.now() - timedelta(minutes=17)
        monkeypatch.setattr("django_ox.worker._lease_now", lambda: moved)
        assert _lease_expiry(60) == moved + timedelta(seconds=60), (
            "the expiry ignored the lease clock, so a worker whose clock moved "
            "stamps a deadline from one clock and a lock from another"
        )

    def test_with_time_zone_support_it_is_a_database_expression(self, settings):
        settings.USE_TZ = True
        expiry = _lease_expiry(60)
        assert not isinstance(expiry, type(timezone.now())), (
            "the expiry came from the worker's clock while the lease comes "
            "from the database"
        )
        assert "Now" in repr(_lease_now()), "this test no longer checks anything"
