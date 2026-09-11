"""
What one reap pass costs, in statements.

Every worker reaps on its own interval, so a fleet that has just lost half its
members runs this on every survivor at once, against a database that is still
recovering. The reaper that walks the stuck set and writes a row at a time
turns one outage into two: with W workers and N abandoned rows it is W*(N+1)
statements, and nothing in it is bounded.

These tests pin the cost, not the correctness. `test_reaper_renewal.py` holds
the correctness.
"""

from datetime import timedelta

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.models import OxTask
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


def abandoned_rows(count, *, attempts=1, max_attempts=3):
    stale = timezone.now() - timedelta(hours=1)
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path="tests.tasks.add",
                args=[1, 2],
                kwargs={},
                queue_name="default",
                status=OxTask.Status.RUNNING,
                locked_by=f"worker-{i}",
                locked_at=stale,
                lease_epoch=1,
                attempts=attempts,
                max_attempts=max_attempts,
                enqueued_at=stale,
            )
            for i in range(count)
        ],
        batch_size=500,
    )


class TestTheRequeueBranchCostsOneStatement:
    def test_fifty_abandoned_rows_come_back_in_a_single_update(self, worker):
        abandoned_rows(50)
        with CaptureQueriesContext(connection) as captured:
            reclaimed = worker.reap()
        assert reclaimed == 50
        # Three: the log manifest, the single UPDATE that reclaims all 50,
        # and the select looking for exhausted rows. A reaper that reads the
        # stuck set and writes it row by row issues 51 here, and the same
        # again on every other worker that reaps.
        assert len(captured.captured_queries) == 3, [
            q["sql"][:90] for q in captured.captured_queries
        ]

    def test_the_cost_does_not_move_with_the_number_of_rows(self, worker):
        abandoned_rows(5)
        with CaptureQueriesContext(connection) as few:
            worker.reap()
        OxTask.objects.all().delete()
        abandoned_rows(200)
        with CaptureQueriesContext(connection) as many:
            assert worker.reap() == 200
        assert len(many.captured_queries) == len(few.captured_queries)


class TestTheExhaustedBranchIsBounded:
    def test_a_pass_retires_at_most_reap_batch_rows(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=0, reap_batch=10)
        abandoned_rows(50, attempts=3, max_attempts=3)
        with CaptureQueriesContext(connection) as captured:
            lost = worker.reap()
        assert lost == 10, "the pass was not capped"
        # The requeue manifest and UPDATE, the select, and one compare-and-set
        # per capped row. Unbounded, this is 51 and climbs with the backlog.
        assert len(captured.captured_queries) == 13
        assert OxTask.objects.filter(status=OxTask.Status.LOST).count() == 10

    def test_the_remainder_is_retired_by_later_passes(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {},
            }
        }
        worker = Worker(backoff_initial=0, reap_batch=10)
        abandoned_rows(25, attempts=3, max_attempts=3)
        assert [worker.reap() for _ in range(3)] == [10, 10, 5]
        assert OxTask.objects.filter(status=OxTask.Status.LOST).count() == 25
