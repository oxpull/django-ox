import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxTask

from .tasks import add, fail_always

STABLE_KEYS = {"event", "task_id", "task_path", "queue", "attempt", "worker_id"}


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@pytest.mark.django_db
class TestStructuredLogging:
    def test_success_lifecycle_events_carry_stable_keys(self, worker, caplog):
        caplog.set_level(logging.DEBUG, logger="django_ox")
        result = add.enqueue(1, 2)
        assert worker.run_once() is True

        for name in ("task_claimed", "task_started", "task_succeeded"):
            (record,) = events(caplog, name)
            assert set(record.__dict__) >= STABLE_KEYS
            assert record.task_id == str(result.id)
            assert record.task_path == "tests.tasks.add"
            assert record.queue == "default"
            assert record.attempt == 1
            assert record.worker_id == worker.worker_id

        (succeeded,) = events(caplog, "task_succeeded")
        assert succeeded.levelno == logging.INFO
        assert isinstance(succeeded.duration_ms, int)
        assert "succeeded" in succeeded.getMessage()

    def test_retry_and_terminal_failure_events(self, worker, caplog):
        caplog.set_level(logging.DEBUG, logger="django_ox")
        fail_always.enqueue()
        for _ in range(3):  # MAX_ATTEMPTS = 3 in test settings
            assert worker.run_once() is True

        retries = events(caplog, "task_retrying")
        assert [r.attempt for r in retries] == [1, 2]
        assert all(r.levelno == logging.WARNING for r in retries)
        assert all(r.exception == "ValueError" for r in retries)
        # The human-readable message is unchanged by the structured pass.
        assert "retrying in" in retries[0].getMessage()

        (failed,) = events(caplog, "task_failed")
        assert failed.levelno == logging.ERROR
        assert failed.attempt == 3
        assert failed.exception == "ValueError"
        assert isinstance(failed.duration_ms, int)

    def test_reclaim_event(self, worker, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        add.enqueue(1, 2)
        db_task = worker.claim_one()
        OxTask.objects.filter(pk=db_task.pk).update(
            locked_at=timezone.now() - timedelta(seconds=worker.lock_timeout + 10)
        )
        assert worker.reap() == 1

        (record,) = events(caplog, "task_reclaimed")
        assert set(record.__dict__) >= STABLE_KEYS
        assert record.task_id == str(db_task.pk)
        assert record.status == "READY"
