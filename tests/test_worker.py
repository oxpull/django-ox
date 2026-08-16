import threading
from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import TaskResultStatus, default_task_backend
from django.utils import timezone

from django_ox.exceptions import TaskAbandoned
from django_ox.models import OxTask
from django_ox.worker import Worker

from .conftest import wait_for
from .tasks import add, fail_always, flaky, record_interval, slow


@pytest.mark.django_db
class TestRetries:
    def test_failure_schedules_retry_with_backoff(self):
        worker = Worker(backoff_initial=30, poll_interval=0.05)
        fail_always.enqueue()

        before = timezone.now()
        assert worker.run_once() is True

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.attempts == 1
        assert len(db_task.errors) == 1
        assert db_task.run_after >= before + timedelta(seconds=30)
        assert db_task.locked_by is None
        assert db_task.locked_at is None

    def test_backoff_doubles_per_attempt(self):
        worker = Worker(backoff_initial=10, backoff_max=25, poll_interval=0.05)
        fail_always.enqueue()

        delays = []
        for _ in range(2):
            db_task = OxTask.objects.get()
            OxTask.objects.update(run_after=None)
            before = timezone.now()
            assert worker.run_once() is True
            db_task.refresh_from_db()
            delays.append((db_task.run_after - before).total_seconds())

        assert 10 <= delays[0] < 11
        # Second delay is capped at backoff_max, not 20.
        assert 20 <= delays[1] < 26

    def test_retry_until_success(self, worker):
        result = flaky.enqueue(succeed_on=3)

        assert worker.run_once() is True  # attempt 1 fails
        assert worker.run_once() is True  # attempt 2 fails
        assert worker.run_once() is True  # attempt 3 succeeds

        result.refresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 3
        assert result.attempts == 3
        assert len(result.errors) == 2

    def test_max_attempts_then_failed(self, worker):
        result = fail_always.enqueue()

        for _ in range(3):  # MAX_ATTEMPTS = 3 in test settings
            assert worker.run_once() is True
        assert worker.run_once() is False

        result.refresh()
        assert result.status == TaskResultStatus.FAILED
        assert result.is_finished
        assert result.attempts == 3
        assert len(result.errors) == 3
        assert result.errors[0].exception_class is ValueError
        assert "ValueError: boom" in result.errors[0].traceback
        with pytest.raises(ValueError, match="Task failed"):
            _ = result.return_value

        db_task = OxTask.objects.get()
        assert db_task.finished_at is not None
        assert db_task.locked_by is None


@pytest.mark.django_db
class TestClaiming:
    def test_claim_is_exclusive(self, worker):
        add.enqueue(1, 2)
        other = Worker(backoff_initial=0)

        claimed = worker.claim_one()
        assert claimed is not None
        assert claimed.status == OxTask.Status.RUNNING
        assert claimed.locked_by == worker.worker_id
        assert claimed.attempts == 1

        assert other.claim_one() is None

    def test_claim_writes_all_bookkeeping_atomically(self, worker):
        add.enqueue(1, 2)

        claimed = worker.claim_one()
        db_task = OxTask.objects.get()

        # The claim UPDATE itself wrote every per-attempt field, and the
        # returned instance matches the row without a re-fetch.
        for field in (
            "status",
            "attempts",
            "worker_ids",
            "started_at",
            "last_attempted_at",
            "locked_by",
            "locked_at",
        ):
            assert getattr(claimed, field) == getattr(db_task, field)
        assert db_task.worker_ids == [worker.worker_id]
        assert db_task.attempts == len(db_task.worker_ids)
        assert db_task.started_at is not None
        assert db_task.last_attempted_at is not None

    def test_reclaim_preserves_started_at_and_appends_worker_id(self, worker):
        fail_always.enqueue()
        assert worker.run_once() is True  # attempt 1 fails and requeues
        first = OxTask.objects.get()
        assert first.status == OxTask.Status.READY

        claimed = worker.claim_one()

        assert claimed is not None
        assert claimed.attempts == 2
        assert claimed.started_at == first.started_at
        assert claimed.last_attempted_at > first.last_attempted_at
        assert claimed.worker_ids == [worker.worker_id, worker.worker_id]

    def test_cas_claim_skips_stale_candidate(self, worker, monkeypatch):
        from django.db import connections

        add.enqueue(1, 2)
        stale = list(OxTask.objects.all())

        # Force the compare-and-set path, then simulate another worker
        # claiming and requeueing the row between fetch and UPDATE.
        connection = connections[worker._db_alias]
        monkeypatch.setattr(
            connection.features, "has_select_for_update_skip_locked", False
        )
        monkeypatch.setattr(worker, "_ready_queryset", lambda: stale)
        OxTask.objects.update(attempts=1, worker_ids=["other-worker"])

        assert worker.claim_one() is None
        db_task = OxTask.objects.get()
        assert db_task.attempts == 1
        assert db_task.worker_ids == ["other-worker"]

    def test_worker_requires_ox_backend(self, settings):
        settings.TASKS = {
            "default": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}
        }
        with pytest.raises(ImproperlyConfigured):
            Worker()


@pytest.mark.django_db
class TestReaper:
    def _make_stuck(self, worker, attempts=None):
        add.enqueue(1, 2)
        db_task = worker.claim_one()
        stale = timezone.now() - timedelta(seconds=worker.lock_timeout + 10)
        updates = {"locked_at": stale}
        if attempts is not None:
            updates["attempts"] = attempts
        OxTask.objects.filter(pk=db_task.pk).update(**updates)
        return db_task

    def test_reaper_requeues_stuck_task(self, worker):
        db_task = self._make_stuck(worker)

        assert worker.reap() == 1
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.READY
        assert db_task.locked_by is None
        assert db_task.locked_at is None
        # The consumed attempt stays counted; the task then runs normally.
        assert db_task.attempts == 1
        assert worker.run_once() is True
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.SUCCESSFUL

    def test_reaper_fails_task_with_no_attempts_left(self, worker):
        db_task = self._make_stuck(worker, attempts=3)

        assert worker.reap() == 1
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.FAILED

        result = default_task_backend.get_result(str(db_task.pk))
        assert result.errors[-1].exception_class is TaskAbandoned

    def test_reaper_ignores_fresh_locks(self, worker):
        add.enqueue(1, 2)
        worker.claim_one()
        assert worker.reap() == 0


@pytest.mark.django_db(transaction=True)
class TestRunLoop:
    def _run_in_thread(self, worker):
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        return thread

    def test_graceful_shutdown_drains_in_flight(self):
        worker = Worker(poll_interval=0.05, backoff_initial=0)
        result = slow.enqueue(0.6)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
            )
            worker.request_stop()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.SUCCESSFUL

    def test_shutdown_closes_each_pool_thread_connection(self, monkeypatch):
        closer_threads = []
        original = Worker._close_connections_in_thread

        def spy(self, barrier):
            closer_threads.append(threading.current_thread().name)
            original(self, barrier)

        monkeypatch.setattr(Worker, "_close_connections_in_thread", spy)
        worker = Worker(concurrency=2, poll_interval=0.05, backoff_initial=0)
        add.enqueue(1, 2)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)
        assert not thread.is_alive()

        # One close task per pool slot, each on a distinct pool thread —
        # the barrier is what guarantees the distinctness.
        assert len(closer_threads) == 2
        assert len(set(closer_threads)) == 2
        assert all(name.startswith("ox") for name in closer_threads)

    def test_run_loop_processes_tasks_concurrently(self, task_state):
        worker = Worker(concurrency=2, poll_interval=0.05, backoff_initial=0)
        r1 = record_interval.enqueue(0.4)
        r2 = record_interval.enqueue(0.4)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 2
                ),
                timeout=5,
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        (start_a, end_a), (start_b, end_b) = task_state["intervals"]
        # Both executions must overlap in time; serial execution cannot.
        assert max(start_a, start_b) < min(end_a, end_b)

        for result in (r1, r2):
            result.refresh()
            assert result.return_value == "done"
