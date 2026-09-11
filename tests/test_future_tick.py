"""
A tick recorded in the future does not re-enqueue on every pass.

The suppression compares the due tick against the newest tick in the log and
requires that newest one to be in the past, so a clock-skewed worker's future
write never short-circuits it. Dispatch then enqueues, the unique constraint
refuses the tick row, and the whole transaction rolls back. But `enqueue()`
saves and fires `task_enqueued` before that outer block rolls back, so every
pass emits an enqueue signal for a task that will not exist.
"""

import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db

MINUTELY = {
    "minutely-add": {"task": "tests.tasks.add", "cron": "* * * * *", "args": [1, 2]}
}


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULES": MINUTELY},
        }
    }
    return Worker(backoff_initial=0)


def _tick(at, task_id=None):
    return OxScheduleTick.objects.create(
        schedule_name="minutely-add",
        scheduled_for=at,
        task_id=task_id,
        created_at=timezone.now(),
    )


class TestATickRecordedInTheFuture:
    def test_the_due_tick_is_not_enqueued_again_on_every_pass(self, worker, caplog):
        now = timezone.now().replace(second=0, microsecond=0)
        # This minute already ran, and a skewed worker wrote one an hour ahead.
        _tick(now, task_id=None)
        _tick(now + timedelta(hours=1))

        enqueued = []
        from django_ox.compat import task_enqueued

        def count(sender, task_result, **kwargs):
            enqueued.append(task_result)

        task_enqueued.connect(count)
        try:
            with caplog.at_level(logging.ERROR, logger="django_ox"):
                for _ in range(5):
                    worker.dispatch_schedules()
        finally:
            task_enqueued.disconnect(count)

        assert OxTask.objects.count() == 0, (
            "a task row survived, so the constraint did not refuse the tick"
        )
        assert enqueued == [], (
            f"{len(enqueued)} phantom task_enqueued signal(s) for tasks that "
            "were rolled back; receivers see enqueues that never happened"
        )

    def test_a_future_tick_still_does_not_suppress_a_due_one(self, worker):
        now = timezone.now().replace(second=0, microsecond=0)
        # Only the future tick exists. This minute has not run, and must.
        _tick(now + timedelta(hours=1))
        assert worker.dispatch_schedules() == 1, (
            "a tick recorded in the future suppressed one that is due now"
        )

    def test_an_ordinary_schedule_is_unaffected(self, worker):
        now = timezone.now().replace(second=0, microsecond=0)
        _tick(now - timedelta(minutes=1))
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1
