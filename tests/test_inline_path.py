"""
`run_once()` runs a task on the caller's own thread.

It has no underscore and a plain docstring, so callers read it as public. Two
things follow that the pool path gets for free: the lease has to be renewed
while the task runs, and an exception aimed at the caller's process has to
reach the caller rather than being filed against the task.
"""

import threading
import time

import pytest
from django.db import connections, transaction

from django_ox.models import OxTask
from django_ox.worker import Worker

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
    return Worker(backoff_initial=0, lock_timeout=1)


class TestTheLeaseIsRenewedInline:
    @pytest.mark.django_db(transaction=True)
    def test_a_task_outliving_the_lock_timeout_is_not_reaped(self, worker, settings):
        # Transactional: the reaper runs on its own thread with its own
        # connection, so it cannot see a claim this test has not committed,
        # and the whole scenario would pass for the wrong reason.
        # Sleeps past the one-second lock timeout. Only run() ever started the
        # renewal thread, so nothing refreshed this lease and a reaper handed
        # the task to somebody else while this call was still inside it.
        result = tasks.slow.enqueue(2.0)

        reclaimed = []

        def reap_while_it_runs():
            reaper = Worker(backoff_initial=0, lock_timeout=1)
            deadline = time.monotonic() + 3
            try:
                while time.monotonic() < deadline:
                    reclaimed.append(reaper.reap())
                    time.sleep(0.2)
            finally:
                # Given back: on a pooled database, a thread that ends holding
                # its connection keeps it out of this process's pool for good.
                connections.close_all()

        reaper_thread = threading.Thread(target=reap_while_it_runs, daemon=True)
        reaper_thread.start()
        assert worker.run_once() is True
        reaper_thread.join(timeout=5)

        assert sum(reclaimed) == 0, (
            "a task running inline was reclaimed mid-flight and would run a "
            "second time on another worker"
        )
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL

    def test_the_renewal_thread_does_not_outlive_the_call(self, worker):
        tasks.add.enqueue(1, 2)
        before = {t.name for t in threading.enumerate()}
        assert worker.run_once() is True
        after = {t.name for t in threading.enumerate()}
        assert not (after - before), f"threads left behind: {after - before}"


def renewal_loops(monkeypatch):
    """Count the renewal loops run_once() starts, running each as it would."""
    started = []
    loop = Worker._renewal_loop

    def counted(self, stop):
        started.append(threading.current_thread().name)
        return loop(self, stop)

    monkeypatch.setattr(Worker, "_renewal_loop", counted)
    return started


class TestRenewalInsideTheCallersTransaction:
    """
    Inside an atomic block on the worker's database the claim is the
    caller's uncommitted write: no other connection can see or take the row
    before the caller commits, and the outcome is written in the same
    transaction. A renewal from another connection protects nothing there,
    and on SQLite and MySQL it waits on the caller's lock.
    """

    @pytest.mark.django_db(transaction=True)
    def test_no_renewal_thread_is_started_inside_an_atomic_block(
        self, worker, monkeypatch
    ):
        started = renewal_loops(monkeypatch)
        result = tasks.add.enqueue(1, 2)
        with transaction.atomic():
            assert worker.run_once() is True
            assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
        assert started == []
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL

    @pytest.mark.django_db(transaction=True)
    def test_a_task_outliving_the_renew_interval_returns_when_it_ends(
        self, monkeypatch
    ):
        # renew_interval is 0.5 s, so the task outlives it twice. A renewal
        # thread waiting on the caller's lock held the return up until the
        # join gave up, renew_interval + 5 s after the task ended.
        started = renewal_loops(monkeypatch)
        worker = Worker(backoff_initial=0, lock_timeout=1.5)
        result = tasks.slow.enqueue(1.2)
        with transaction.atomic():
            began = time.monotonic()
            assert worker.run_once() is True
            took = time.monotonic() - began
        assert started == []
        assert took < 1.2 + 3.0, f"run_once() took {took:.2f}s for a 1.2 s task"
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL

    @pytest.mark.django_db(transaction=True)
    def test_a_rolled_back_block_takes_the_claim_and_the_outcome_with_it(self, worker):
        result = tasks.add.enqueue(1, 2)
        with pytest.raises(RuntimeError, match="undo"), transaction.atomic():
            assert worker.run_once() is True
            raise RuntimeError("undo")
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert row.locked_by in ("", None)

    @pytest.mark.django_db(transaction=True)
    def test_renewal_still_runs_in_autocommit(self, worker, monkeypatch):
        started = renewal_loops(monkeypatch)
        tasks.add.enqueue(1, 2)
        assert worker.run_once() is True
        assert started == ["ox-renew-inline"]

    @pytest.mark.django_db(transaction=True)
    def test_renewal_still_runs_with_autocommit_off_and_no_atomic_block(
        self, worker, monkeypatch
    ):
        # The task can commit here, which makes its lease visible while it
        # is still running, so the lease is renewed as usual.
        started = renewal_loops(monkeypatch)
        result = tasks.add.enqueue(1, 2)
        transaction.set_autocommit(False)
        try:
            assert worker.run_once() is True
            transaction.commit()
        finally:
            transaction.set_autocommit(True)
        assert started == ["ox-renew-inline"]
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL

    @pytest.mark.django_db(transaction=True, databases=["default", "alt"])
    def test_an_atomic_block_on_another_database_keeps_renewal(
        self, worker, monkeypatch
    ):
        # The claim autocommits on the worker's database whatever the caller
        # holds elsewhere, so its lease is visible and is renewed.
        started = renewal_loops(monkeypatch)
        tasks.add.enqueue(1, 2)
        with transaction.atomic(using="alt"):
            assert worker.run_once() is True
        assert started == ["ox-renew-inline"]

    @pytest.mark.django_db(transaction=True, databases=["default", "alt"])
    def test_the_workers_own_database_decides_when_the_router_moves_it(
        self, settings, monkeypatch
    ):
        # The worker's database is alt: a block there holds the claim, and a
        # block on default does not, so only the second one keeps renewal.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        settings.DATABASE_ROUTERS = [ToAlt()]
        started = renewal_loops(monkeypatch)
        worker = Worker(backoff_initial=0, lock_timeout=1)
        assert worker._db_alias == "alt"
        tasks.add.enqueue(1, 2)
        with transaction.atomic(using="alt"):
            assert worker.run_once() is True
        assert started == []
        tasks.add.enqueue(3, 4)
        with transaction.atomic(using="default"):
            assert worker.run_once() is True
        assert started == ["ox-renew-inline"]


class TestAnExceptionAimedAtTheProcess:
    @pytest.mark.parametrize("aimed", [KeyboardInterrupt, SystemExit])
    def test_it_reaches_the_caller_inline(self, worker, monkeypatch, aimed):
        tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None

        def boom(*args, **kwargs):
            raise aimed("aimed at the process")

        monkeypatch.setattr(tasks.add.func, "__call__", boom, raising=False)
        monkeypatch.setattr(type(tasks.add), "call", boom)
        with pytest.raises(aimed):
            worker.execute(claimed, inline=True)

    @pytest.mark.parametrize("aimed", [KeyboardInterrupt, SystemExit])
    def test_it_stays_a_failed_attempt_on_the_pool(self, worker, monkeypatch, aimed):
        # One task calling sys.exit() must not be able to stop a fleet, and
        # raising out of a pool thread would end only that thread anyway.
        result = tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None

        def boom(*args, **kwargs):
            raise aimed("aimed at the process")

        monkeypatch.setattr(type(tasks.add), "call", boom)
        worker.execute(claimed)
        row = OxTask.objects.get(id=result.id)
        assert row.status in (OxTask.Status.READY, OxTask.Status.FAILED)


class ToAlt:
    """Sends django_ox's models, and so the worker, to the alt database."""

    def db_for_read(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None
