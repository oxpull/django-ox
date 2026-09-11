"""
The drain waits for healthy work and stops waiting for abandoned work.

Both halves of this are ways the process can exit out from under a task that
was going to finish, or hang forever on one that never will.
"""

import threading
import time
from concurrent.futures import Future

import pytest

from django_ox.worker import Worker

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
    return Worker(backoff_initial=0)


def _pending():
    """A future that is not done, standing in for a running attempt."""
    return Future()


class TestAReusedPoolThreadDoesNotLookStuck:
    """
    `_stuck` is keyed by pool-thread ident, and a pool thread is reused. Once
    an abandoned task finally returns, its ident is still alive and idle, and
    alive again under the next task. Counting idents therefore over-counts,
    and the drain stops waiting while healthy work is still running.
    """

    def test_a_thread_that_returned_stops_counting_as_stuck(self, worker):
        ident = threading.get_ident()
        attempt = ("task-a", 3)
        worker._stuck[ident] = attempt

        # Still inside the abandoned attempt: it counts.
        worker._running_on[ident] = attempt
        assert worker._stuck_alive() == 1

        # It came back on its own. The thread is alive and idle; nothing is
        # abandoned any more.
        del worker._running_on[ident]
        assert worker._stuck_alive() == 0, (
            "an idle pool thread was still counted as stuck, so the drain "
            "would stop waiting for healthy work"
        )

    def test_the_same_thread_on_a_different_attempt_does_not_count(self, worker):
        ident = threading.get_ident()
        worker._stuck[ident] = ("task-a", 3)
        # The pool handed this thread a fresh task. That task is healthy.
        worker._running_on[ident] = ("task-b", 1)
        assert worker._stuck_alive() == 0, (
            "a new task on a reused thread was counted as the abandoned one"
        )

    def test_the_drain_does_not_return_while_healthy_work_is_pending(self, worker):
        ident = threading.get_ident()
        worker._stuck[ident] = ("task-a", 3)
        del worker._running_on  # nothing running: the stuck task came back
        worker._running_on = {}
        worker._recycling = True

        healthy = _pending()
        returned = []
        drain = threading.Thread(
            target=lambda: (worker._drain({healthy}), returned.append(True)),
            daemon=True,
        )
        drain.start()
        time.sleep(0.6)  # several RECYCLE_DRAIN_POLL periods
        try:
            assert not returned, (
                "the drain returned while a healthy task was still pending; "
                "the process would exit out from under it"
            )
        finally:
            healthy.set_result(None)
            drain.join(timeout=2)
        assert returned, "the drain never returned once the task finished"


class TestARecycleStartedMidDrainIsObserved:
    def test_the_drain_re_reads_the_flag(self, worker):
        ident = threading.get_ident()
        abandoned = _pending()
        worker._recycling = False

        returned = []
        drain = threading.Thread(
            target=lambda: (worker._drain({abandoned}), returned.append(True)),
            daemon=True,
        )
        drain.start()
        time.sleep(0.3)
        assert not returned, "the drain returned before anything finished"

        # The backstop fires now, part way through an ordinary drain, and
        # gives up on the thread running this attempt.
        worker._stuck[ident] = ("task-a", 3)
        worker._running_on[ident] = ("task-a", 3)
        worker._recycling = True

        drain.join(timeout=3)
        assert returned, (
            "a recycle that started during the drain was never observed, so "
            "the drain waited unbounded on the thread it exists to abandon"
        )
        abandoned.set_result(None)

    def test_an_ordinary_drain_still_waits_for_everything(self, worker):
        healthy = _pending()
        returned = []
        drain = threading.Thread(
            target=lambda: (worker._drain({healthy}), returned.append(True)),
            daemon=True,
        )
        drain.start()
        time.sleep(0.3)
        assert not returned
        healthy.set_result(None)
        drain.join(timeout=2)
        assert returned
