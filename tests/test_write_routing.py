"""
Every statement in the claim protocol goes to the write database.

A database router can send `OxTask` somewhere other than the default
connection. The worker computes that alias once and used it only for its
transaction blocks, leaving the statements themselves to route themselves.
Reads route through `db_for_read`, so with a read replica configured the
claim lands off primary or raises on a read-only standby, `_reload_claimed`
can miss a claim that succeeded, and the reaper can read a stale `locked_at`
while its update hits primary.

The raw PostgreSQL claim is the worst of them: unlike `select_for_update()`
it does not mark itself for write, so Django cannot tell the SQL is an
UPDATE even in principle.
"""

import pytest
from django.db import connections

from django_ox.worker import Worker

from . import tasks

ALT = "alt"


class _PrimaryAndReplica:
    """
    Reads to one database, writes to another, the way a read replica is set up.

    A router that sends reads and writes to the same alias cannot show this
    defect at all: an unpinned statement routes itself to the very alias the
    pinned one names, so both behave identically and the test proves nothing.
    Splitting them is what makes "which connection did this statement use"
    an answerable question.
    """

    def db_for_read(self, model, **hints):
        return "default" if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model, **hints):
        return ALT if model._meta.app_label == "django_ox" else None

    def allow_migrate(self, db, app_label, **hints):
        # Both, so the tables exist either side and a misrouted statement
        # fails on its alias rather than on a missing table.
        return None


@pytest.fixture
def spy(monkeypatch):
    """Record the alias of every statement the worker issues."""
    seen: list[str] = []
    for alias in ("default", ALT):
        wrapper = connections[alias]
        original = wrapper.cursor

        def cursor(self=wrapper, _original=original, _alias=alias):
            seen.append(_alias)
            return _original()

        monkeypatch.setattr(wrapper, "cursor", cursor)
    return seen


@pytest.mark.django_db(databases=["default", ALT])
class TestTheProtocolLandsOnTheWriteAlias:
    @pytest.fixture(autouse=True)
    def _router(self, settings):
        settings.DATABASE_ROUTERS = [_PrimaryAndReplica()]
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }

    def test_the_worker_resolved_the_routed_alias(self):
        assert Worker(backoff_initial=0)._db_alias == ALT

    def test_a_claim_touches_only_the_routed_alias(self, spy):
        worker = Worker(backoff_initial=0)
        tasks.add.enqueue(1, 2)
        spy.clear()
        claimed = worker.claim_one()
        assert claimed is not None
        assert set(spy) == {ALT}, (
            f"the claim issued statements on {sorted(set(spy))}; a read "
            "replica would take them off primary"
        )

    def test_renewal_and_the_outcome_write_stay_on_it(self, spy):
        worker = Worker(backoff_initial=0)
        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        spy.clear()
        worker.execute(claimed)
        assert set(spy) == {ALT}, f"execute() used {sorted(set(spy))}"

    def test_the_reaper_stays_on_it(self, spy):
        worker = Worker(backoff_initial=0)
        tasks.add.enqueue(1, 2)
        worker.claim_one()
        spy.clear()
        worker.reap()
        assert set(spy) == {ALT}, f"reap() used {sorted(set(spy))}"

    def test_renew_leases_stays_on_it(self, spy):
        worker = Worker(backoff_initial=0)
        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        with worker._in_flight_lock:
            worker._in_flight.add((claimed.pk, claimed.lease_epoch))
        spy.clear()
        assert worker.renew_leases() == 1
        assert set(spy) == {ALT}, f"renew_leases() used {sorted(set(spy))}"


@pytest.mark.django_db(databases=["default", ALT])
class TestEnqueueManyCommitsOnOneConnection:
    """
    `enqueue_many` promises all or nothing. An unpinned `atomic()` wraps the
    default connection while `bulk_create` routes itself, so under a router
    the block guarded a connection the INSERTs never touched and a partial
    batch could survive an error.
    """

    @pytest.fixture(autouse=True)
    def _router(self, settings):
        settings.DATABASE_ROUTERS = [_PrimaryAndReplica()]
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }

    def test_the_transaction_is_opened_on_the_write_alias(self, monkeypatch):
        from django.db import transaction as tx

        from django_ox.bulk import enqueue_many

        seen = []
        real = tx.atomic

        def spy(using=None, **kwargs):
            seen.append(using)
            return real(using=using, **kwargs)

        monkeypatch.setattr("django_ox.backend.transaction.atomic", spy)
        enqueue_many(tasks.add, [((1, 2), {}), ((3, 4), {})])

        assert ALT in seen, f"opened on {seen!r}, never on the write alias"
        assert None not in seen, (
            "a transaction was opened on the default connection while the "
            "rows were written through the routed one"
        )

    def test_the_rows_land_on_the_write_alias(self):
        from django_ox.bulk import enqueue_many
        from django_ox.models import OxTask

        enqueue_many(tasks.add, [((1, 2), {}), ((3, 4), {})])
        assert OxTask.objects.using(ALT).count() == 2
