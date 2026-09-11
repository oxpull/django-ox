"""
The drain waits for healthy work and stops waiting for abandoned work.

Both halves of this are ways the process can exit out from under a task that
was going to finish, or hang forever on one that never will.
"""

import threading
import time
from concurrent.futures import Future

import pytest
from django.utils import timezone

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


class TestAReaperWinDoesNotCancelTheRecycle:
    """
    The backstop's write is a compare-and-set, and it can lose for two
    unrelated reasons: the thread's own outcome landed first, or a reaper
    requeued the row underneath it. The first means the thread came back. The
    second means the thread is still wedged and the row is now claimable by
    anyone, which is the worst moment to decide not to recycle.

    The first case, a thread whose own outcome landed ahead of the backstop,
    is covered in tests/test_timeouts.py, where the attempt is really executed
    rather than synthesised.
    """

    def _watch(self, worker, claimed, ident):
        import time as clock

        from django_ox.worker import _Watch

        now = clock.monotonic()
        return _Watch(
            ident=ident,
            db_task=claimed,
            attempt=(claimed.pk, claimed.lease_epoch),
            timeout=5,
            started=now - 6,
            deadline=now - 1,
            deadline_at=timezone.now(),
            injectable=True,
            fired=True,
            grace_at=now,
        )

    def test_the_worker_recycles_even_though_the_write_matched_nothing(
        self, worker, caplog
    ):
        import logging

        from django_ox.models import OxTask

        from . import tasks

        result = tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        ident = threading.get_ident()
        watch = self._watch(worker, claimed, ident)

        # The thread is wedged: still inside this attempt.
        worker._running_on[ident] = watch.attempt

        # A reaper got there first: READY, and the epoch moved on. The
        # backstop's compare-and-set now matches zero rows.
        OxTask.objects.filter(id=result.id).update(
            status=OxTask.Status.READY,
            locked_by=None,
            locked_at=None,
            lease_epoch=claimed.lease_epoch + 1,
        )

        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._handle_stuck(watch)

        assert worker.recycling, (
            "the write lost to a reaper and the worker declined to recycle, so "
            "the wedged thread keeps its pool slot for the life of the process"
        )
        assert worker._stuck.get(ident) == watch.attempt, (
            "the wedged thread is not in the stuck set, so the drain will wait "
            "for it without bound"
        )


class TestARecycleFinishesEvenWithHealthyWorkOutstanding:
    """
    Abandoning the stuck thread is not enough on its own. The recycling worker
    still waits for that thread's healthy siblings, and a sibling on a queue
    with no timeout has no obligation to finish -- so one of them could hold a
    recycling worker open indefinitely. That made the recycle a request rather
    than a guarantee, on the one path whose premise is that this process can
    no longer be trusted with work.

    The budget is the lease: past `lock_timeout` a reaper may take these rows
    anyway, so waiting beyond it buys nothing.
    """

    def test_the_drain_gives_up_on_a_sibling_that_never_finishes(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=0, recycle_drain_budget=0.5)
        ident = threading.get_ident()
        worker._stuck[ident] = ("task-a", 3)
        worker._running_on = {ident: ("task-a", 3)}
        worker._recycling = True

        abandoned, never_finishes = _pending(), _pending()
        returned = []
        drain = threading.Thread(
            target=lambda: (
                worker._drain({abandoned, never_finishes}),
                returned.append(True),
            ),
            daemon=True,
        )
        drain.start()
        drain.join(timeout=5)
        assert returned, (
            "the recycling worker waited forever on a task that never "
            "finished, so the supervisor never got its replacement"
        )
        abandoned.set_result(None)
        never_finishes.set_result(None)

    def test_the_budget_defaults_to_the_lease(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"LOCK_TIMEOUT": 120.0},
            }
        }
        assert Worker(backoff_initial=0).recycle_drain_budget == 120.0

    def test_a_healthy_task_that_finishes_in_time_is_still_waited_for(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=0, recycle_drain_budget=30.0)
        ident = threading.get_ident()
        worker._stuck[ident] = ("task-a", 3)
        worker._running_on = {}
        worker._recycling = True

        healthy = _pending()
        returned = []
        drain = threading.Thread(
            target=lambda: (worker._drain({healthy}), returned.append(True)),
            daemon=True,
        )
        drain.start()
        time.sleep(0.6)
        try:
            assert not returned, "the budget cut a healthy task short"
        finally:
            healthy.set_result(None)
            drain.join(timeout=2)
        assert returned
