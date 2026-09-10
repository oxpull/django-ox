"""
A schedule anchors once, however many workers first see it at once.

The first worker to see a schedule with no history records the current tick
as an anchor and enqueues nothing, so the schedule does not fire for every
tick since the cron epoch. It fires at its next tick.

The decision has to come from the log inside the transaction. Taken from a
snapshot read before the loop, a second worker whose pass began a tick later
still believes it is the first sighting: it writes a second anchor, at a tick
whose boundary already existed and which should have fired, and the unique
constraint then suppresses that instant for good.
"""

from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db

MINUTELY = {
    "minutely-add": {"task": "tests.tasks.add", "cron": "* * * * *", "args": [1, 2]}
}


def tasks_setting(schedules):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULES": schedules},
        }
    }


@pytest.fixture
def two_workers(settings):
    settings.TASKS = tasks_setting(MINUTELY)
    return Worker(backoff_initial=0), Worker(backoff_initial=0)


def _at(worker, monkeypatch, moment):
    from django_ox import worker as worker_module

    monkeypatch.setattr(worker_module.timezone, "now", lambda: moment)
    return worker.dispatch_schedules()


class TestASecondAnchorDoesNotSwallowATick:
    def test_a_worker_arriving_a_tick_later_fires_rather_than_anchors(
        self, two_workers, monkeypatch
    ):
        # Worker B takes its snapshot while the schedule has no history, then
        # A completes a whole pass and commits the anchor at T0, and only then
        # does B reach its own transaction at T1. T1 must fire.
        a, b = two_workers
        t0 = timezone.now().replace(second=0, microsecond=0)
        t1 = t0 + timedelta(minutes=1)

        snapshot = b._latest_ticks()
        assert snapshot == {}, "the snapshot must predate the anchor"

        _at(a, monkeypatch, t0)
        monkeypatch.undo()
        assert OxScheduleTick.objects.count() == 1, "A did not anchor"
        assert OxTask.objects.count() == 0, "an anchor must enqueue nothing"

        monkeypatch.setattr(b, "_latest_ticks", lambda: snapshot)
        _at(b, monkeypatch, t1)
        monkeypatch.undo()

        rows = list(
            OxScheduleTick.objects.order_by("scheduled_for").values_list(
                "scheduled_for", "task_id"
            )
        )
        assert OxTask.objects.count() == 1, (
            f"the tick after the anchor was swallowed as a second anchor: {rows}"
        )

    def test_the_uncontended_sequence_is_unchanged(self, two_workers, monkeypatch):
        # The control: one worker, two ticks. Anchor then fire, as documented.
        a, _ = two_workers
        t0 = timezone.now().replace(second=0, microsecond=0)
        assert _at(a, monkeypatch, t0) == 0, "the first sighting must not fire"
        monkeypatch.undo()
        assert _at(a, monkeypatch, t0 + timedelta(minutes=1)) == 1
        monkeypatch.undo()
        assert OxTask.objects.count() == 1
        assert OxScheduleTick.objects.count() == 2

    def test_a_schedule_with_history_never_pays_for_the_check(
        self, two_workers, monkeypatch
    ):
        # The extra read exists only while a schedule has no ticks at all.
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        a, _ = two_workers
        t0 = timezone.now().replace(second=0, microsecond=0)
        _at(a, monkeypatch, t0)
        monkeypatch.undo()

        from django_ox import worker as worker_module

        monkeypatch.setattr(
            worker_module.timezone, "now", lambda: t0 + timedelta(minutes=1)
        )
        with CaptureQueriesContext(connection) as captured:
            a.dispatch_schedules()
        monkeypatch.undo()
        existence_checks = [
            q["sql"]
            for q in captured.captured_queries
            if "ox_schedule_tick" in q["sql"].lower() and " exists" in q["sql"].lower()
        ]
        assert not existence_checks, (
            f"the first-sighting check ran for a schedule with history: "
            f"{existence_checks}"
        )
