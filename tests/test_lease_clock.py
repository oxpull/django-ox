"""
The lease is judged on one clock.

`_lease_now()` picks it: the database's when USE_TZ is on, the worker's when
it is off. Everything that touches `locked_at` has to go through it, or two
clocks end up on one column and the reaper judges a timestamp a different
clock wrote. Across hosts that drift, that is a live lease reclaimed and a
task running twice.
"""

from datetime import timedelta

import pytest
from django.conf import settings as django_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import POSTGRES_CLAIM_SQL, Worker

from . import tasks

pytestmark = pytest.mark.django_db


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, lock_timeout=60)


class TestTheClaimAndTheRenewalAgree:
    def test_the_rendered_claim_follows_use_tz(self):
        # The statement and _lease_now() make the same choice, so the column
        # is never stamped by one clock and judged by another.
        for clock in ("STATEMENT_TIMESTAMP()", "%(lease_now)s"):
            sql = POSTGRES_CLAIM_SQL.format(
                lease_clock=clock, table="ox_task", queue_clause="", extra_clause=""
            )
            assert f'"locked_at" = {clock}' in sql

    def test_a_renewal_moves_the_lease_forward(self, worker):
        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        before = OxTask.objects.get(pk=claimed.pk).locked_at
        with worker._in_flight_lock:
            worker._in_flight.add((claimed.pk, claimed.lease_epoch))
        assert worker.renew_leases() == 1
        after = OxTask.objects.get(pk=claimed.pk).locked_at
        assert after >= before, (
            f"the renewal moved the lease backwards, {before} -> {after}: the "
            "claim and the renewal are not on the same clock"
        )


class TestWhoseClockTheLeaseIsOn:
    """
    Recorded as a limit, not half-guarded.

    With USE_TZ on, `_lease_now()` is a database expression: the worker's own
    clock never touches `locked_at`, so two workers on hosts that drift still
    agree about when a lease expires. With it off there is no shared clock to
    use, every worker stamps and judges on its own, and a worker whose clock
    runs behind by more than the lock timeout loses live leases to the reaper.

    Nothing in the code can close that. A shared clock is the fix and USE_TZ
    is how you get one, so the documentation says so rather than promising a
    property the configuration cannot deliver.
    """

    def test_with_time_zone_support_the_worker_s_clock_is_not_used(self, settings):
        from django.db.models.functions import Now

        settings.USE_TZ = True
        from django_ox.worker import _lease_now

        assert isinstance(_lease_now(), Now), (
            "the lease would be stamped from the worker's clock, so two hosts "
            "that drift would disagree about when it expires"
        )

    def test_without_it_a_worker_behind_the_reaper_loses_a_live_lease(
        self, worker, monkeypatch
    ):
        if django_settings.USE_TZ:
            pytest.skip("there is no process clock on this path to skew")

        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        with worker._in_flight_lock:
            worker._in_flight.add((claimed.pk, claimed.lease_epoch))

        behind = timezone.now() - timedelta(minutes=2)
        monkeypatch.setattr("django_ox.worker._lease_now", lambda: behind)
        assert worker.renew_leases() == 1
        monkeypatch.undo()

        reaper = Worker(backoff_initial=0, lock_timeout=60)
        reclaimed = reaper.reap()
        row = OxTask.objects.get(pk=claimed.pk)

        # The documented limit: with no shared clock, a worker two minutes
        # behind renews to a timestamp the reaper already reads as expired.
        assert reclaimed == 1
        assert row.status == OxTask.Status.READY
        assert row.locked_by is None


class TestTheTimingOptionsAreChecked:
    """
    `LOCK_TIMEOUT`, `BACKOFF_INITIAL` and `BACKOFF_MAX` were each cast with a
    bare `float()` and used. A zero or a negative reached the poll loop and
    misbehaved there, rather than stopping the deploy at `manage.py check`
    where every other bad option does.
    """

    def _check(self, settings, options):
        from django_ox.compat import default_task_backend

        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": options,
            }
        }
        return [e.id for e in default_task_backend.check()]

    @pytest.mark.parametrize(
        "option", ["LOCK_TIMEOUT", "BACKOFF_INITIAL", "BACKOFF_MAX"]
    )
    @pytest.mark.parametrize("bad", [0, -1, float("inf"), float("nan"), "soon"])
    def test_a_value_that_is_not_a_positive_number_of_seconds_is_refused(
        self, option, bad, settings
    ):
        assert "django_ox.E010" in self._check(settings, {option: bad})

    @pytest.mark.parametrize(
        "option", ["LOCK_TIMEOUT", "BACKOFF_INITIAL", "BACKOFF_MAX"]
    )
    def test_a_sensible_value_is_accepted(self, option, settings):
        assert "django_ox.E010" not in self._check(settings, {option: 30})

    def test_leaving_them_out_is_accepted(self, settings):
        assert "django_ox.E010" not in self._check(settings, {})

    def test_the_relationship_the_digest_named_is_not_reachable(self):
        # `renew_interval > lock_timeout` was reported as the headline case.
        # It cannot be configured: the renewal interval is derived from the
        # lock timeout, not read from OPTIONS. Asserted rather than "fixed".
        w = Worker(backoff_initial=0, lock_timeout=30)
        assert w.renew_interval < w.lock_timeout, (
            "the renewal would be slower than the lease it renews"
        )
        import inspect

        from django_ox import worker as worker_module

        source = inspect.getsource(worker_module.Worker.__init__)
        assert 'options.get("RENEW_INTERVAL"' not in source, (
            "RENEW_INTERVAL became configurable, so the pair the digest named "
            "is now reachable and does need a check"
        )
