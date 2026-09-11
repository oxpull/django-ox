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
            while time.monotonic() < deadline:
                reclaimed.append(reaper.reap())
                time.sleep(0.2)

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
