"""
The claim must read its candidate out of the index, in order.

Cost, not correctness, and expressed as a query plan rather than a duration:
plans do not move with what else the machine is doing, and a timing taken on a
busy machine has flipped a verdict on this project before.

`ox_dequeue_idx` used to end on `run_after`, which is a range sitting behind
the two columns that carry the ORDER BY. No database could use it there.
PostgreSQL gave up on the index altogether -- bitmap-scanning the reaper's
index and sorting the result on every claim -- and SQLite built a temporary
B-tree. Ending the index on `enqueued_at` instead hands the claim its rows in
exactly the order it wants them, so the scan stops at the first runnable one.
"""

import pytest
from django.db import connection
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


def a_deferred_backlog(total=2000, deferred=1500):
    """The shape a retry backlog takes: deferred rows keep their original
    `enqueued_at`, so they sort ahead of everything that can actually run."""
    now = timezone.now()
    far = now + timezone.timedelta(days=30)
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path="tests.tasks.add",
                args=[1, 2],
                kwargs={},
                queue_name="default",
                status=OxTask.Status.READY,
                priority=0,
                enqueued_at=now - timezone.timedelta(seconds=i),
                run_after=far if i < deferred else None,
            )
            for i in range(total)
        ],
        batch_size=500,
    )
    if connection.vendor == "postgresql":
        # Fresh statistics, or the planner chooses on defaults and the plan
        # says nothing about the index. Deliberately not done on MySQL:
        # ANALYZE TABLE there commits the open transaction implicitly, so
        # these 2,000 rows would outlive the test's rollback and every test
        # after it would run against a table full of them.
        with connection.cursor() as cursor:
            cursor.execute("ANALYZE django_ox_oxtask")


def dequeue_plan(worker):
    queryset = worker._ready_queryset()[:1]
    if connection.vendor == "postgresql":
        return str(queryset.explain(analyze=True))
    return str(queryset.explain())


class TestTheClaimReadsItsCandidateFromTheIndex:
    def test_the_dequeue_index_is_the_one_chosen(self, worker):
        a_deferred_backlog()
        plan = dequeue_plan(worker)
        assert "ox_dequeue_idx" in plan, plan

    def test_nothing_sorts_a_deferred_backlog_on_every_claim(self, worker):
        a_deferred_backlog()
        plan = dequeue_plan(worker).lower()
        sort_node = {
            "postgresql": "sort",
            "mysql": "sort:",
            "sqlite": "temp b-tree",
        }[connection.vendor]
        assert sort_node not in plan, plan

    def test_the_scan_stops_at_the_first_runnable_row(self, worker):
        if connection.vendor != "postgresql":
            pytest.skip("only PostgreSQL reports rows actually read")
        a_deferred_backlog()
        plan = dequeue_plan(worker)
        scan = next(line for line in plan.split("\n") if "ox_dequeue_idx" in line)
        assert "actual" in scan and "rows=1 " in scan.split("actual")[1], scan
