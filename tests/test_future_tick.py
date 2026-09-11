"""
A tick recorded in the future does not re-enqueue on every pass.

The suppression compares the due tick against the newest tick in the log and
requires that newest one to be in the past, so a clock-skewed worker's future
write never short-circuits it. The tick row is written before the task is
enqueued, so a pass that loses the constraint enqueues nothing and
`task_enqueued` fires only for a task that exists.
"""

import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.compat import task_enqueued
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


@pytest.fixture
def two_scheduled_workers(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULES": MINUTELY},
        }
    }
    # Two workers on the same schedule, which is the ordinary state of a
    # fleet on every tick rather than an edge case.
    return Worker(backoff_initial=0), Worker(backoff_initial=0)


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


class TestALosingWorkerAnnouncesNothing:
    """
    Every worker derives the same tick times, so on every tick all of them
    reach the dispatch and exactly one INSERT survives the unique constraint.
    The losers must do no work: `enqueue()` saves and fires `task_enqueued`
    before an outer rollback unwinds, so a loser that enqueued first announced
    a task that never existed, once per tick, for as long as the fleet ran.
    """

    def test_the_loser_fires_no_enqueue_signal(
        self, two_scheduled_workers, monkeypatch
    ):
        first, second = two_scheduled_workers
        # The schedule already has history, so this tick fires rather than
        # anchoring.
        now = timezone.now().replace(second=0, microsecond=0)
        _tick(now - timedelta(minutes=1), task_id=None)

        # Both read the tick log before either commits, which is the whole of
        # the race: without a stale snapshot the second worker simply sees the
        # first one's committed tick and never reaches the dispatch at all.
        stale = second._latest_ticks(now - timedelta(days=1))
        monkeypatch.setattr(second, "_latest_ticks", lambda since: stale)

        announced: list[str] = []

        def record(sender, task_result, **kwargs):
            announced.append(str(task_result.id))

        task_enqueued.connect(record)
        try:
            assert first.dispatch_schedules() == 1
            announced_by_winner = list(announced)
            assert second.dispatch_schedules() == 0, "both workers dispatched"
        finally:
            task_enqueued.disconnect(record)

        assert announced == announced_by_winner, (
            "the losing worker announced an enqueue; its task row was rolled "
            "back, so receivers saw a task that never existed"
        )
        surviving = {str(pk) for pk in OxTask.objects.values_list("id", flat=True)}
        for task_id in announced:
            assert task_id in surviving, (
                f"announced {task_id}, which is not in the database"
            )
