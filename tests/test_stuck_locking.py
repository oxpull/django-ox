"""
The stuck set is read by the drain and written by the watchdog.

`_stuck_alive` iterates it while holding `_in_flight_lock`, because the drain
is what holds the process open while an abandoned attempt is still running. A
second attempt going stuck writes to the same dict from the watchdog thread. If
that write does not take the same lock, the drain's iteration can see the dict
change size and raise, on the one code path whose job is to not let the process
exit out from under live work.
"""

import threading

import pytest
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker, _Watch

pytestmark = pytest.mark.django_db


class _WitnessLock:
    """A lock that remembers whether this thread is inside it."""

    def __init__(self):
        self._lock = threading.Lock()
        self.depth = 0

    def __enter__(self):
        self._lock.acquire()
        self.depth += 1
        return self

    def __exit__(self, *exc):
        self.depth -= 1
        self._lock.release()
        return False

    @property
    def held(self):
        return self.depth > 0


class _GuardedStuck(dict):
    """Records the lock state at every write, so the test can check it."""

    def __init__(self, witness):
        super().__init__()
        self._witness = witness
        self.writes_without_the_lock = []

    def __setitem__(self, key, value):
        if not self._witness.held:
            self.writes_without_the_lock.append(key)
        super().__setitem__(key, value)


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, task_timeout=0.05)


def a_running_task():
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        args=[1, 2],
        kwargs={},
        queue_name="default",
        status=OxTask.Status.RUNNING,
        locked_by="worker-A",
        locked_at=now,
        lease_epoch=1,
        attempts=1,
        max_attempts=3,
        enqueued_at=now,
    )


class TestTheStuckSetIsWrittenUnderItsLock:
    def test_handle_stuck_takes_the_lock_it_shares_with_the_drain(self, worker):
        db_task = a_running_task()
        ident = threading.get_ident()
        attempt = (db_task.pk, db_task.lease_epoch)

        witness = _WitnessLock()
        worker._in_flight_lock = witness
        worker._stuck = _GuardedStuck(witness)
        # The thread is still inside the attempt, which is the branch that
        # records it as stuck and recycles the worker.
        worker._running_on = {ident: attempt}

        watch = _Watch(
            ident=ident,
            db_task=db_task,
            attempt=attempt,
            timeout=0.05,
            started=0.0,
            deadline=0.0,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
        )
        worker._handle_stuck(watch)

        assert worker._stuck, "the attempt was not recorded as stuck at all"
        assert not worker._stuck.writes_without_the_lock, (
            "the stuck set was written without the lock the drain iterates "
            "under, so a second stuck attempt can make the drain raise"
        )
