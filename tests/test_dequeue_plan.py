"""
The claim must read its candidate out of the index, in order.

Cost, not correctness, and expressed as a query plan rather than a duration:
plans do not move with what else the machine is doing, and a timing on a busy
machine can invert the result.

An index that ends on `run_after` cannot serve this query. That column is a
range sitting behind the two that carry the ORDER BY, where no database can use
it, and its presence costs the planner the ordering: PostgreSQL stops choosing
the index and sorts the whole candidate set on every claim, and SQLite builds a
temporary B-tree. Ending on `enqueued_at` instead hands the claim its rows in
exactly the order it wants them, so the scan stops at the first runnable one.
"""

import pytest
from django.db import connection
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db


def a_worker_on(queues, settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": list(queues),
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, queues=list(queues) or None)


@pytest.fixture
def worker(settings):
    return a_worker_on(["default"], settings)


def a_deferred_backlog(total=2000, deferred=1500, queues=("default",)):
    """The worst shape a retry backlog takes.

    The deferred rows are the OLDEST, so they sort ahead of everything that
    can actually run and the scan has to walk past all of them. Making them
    the newest instead hides the cost: the very first index entry is already
    runnable, so a scan that never stops early looks identical to one that
    stops immediately.
    """
    now = timezone.now()
    far = now + timezone.timedelta(days=30)
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path="tests.tasks.add",
                args=[1, 2],
                kwargs={},
                queue_name=queues[i % len(queues)],
                status=OxTask.Status.READY,
                priority=0,
                enqueued_at=now - timezone.timedelta(seconds=total - i),
                run_after=far if i < deferred else None,
            )
            for i in range(total)
        ],
        batch_size=500,
    )
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("ANALYZE django_ox_oxtask")


def dequeue_plan(worker):
    queryset = worker._ready_queryset()[:1]
    if connection.vendor == "postgresql":
        return str(queryset.explain(analyze=True))
    return str(queryset.explain())


SORT_NODE = {"postgresql": "sort", "mysql": "sort:", "sqlite": "temp b-tree"}


class TestTheClaimReadsItsCandidateFromTheIndex:
    def test_the_dequeue_index_is_the_one_chosen(self, worker):
        a_deferred_backlog()
        plan = dequeue_plan(worker)
        assert "ox_dequeue" in plan, plan

    def test_nothing_sorts_a_deferred_backlog_on_every_claim(self, worker):
        a_deferred_backlog()
        plan = dequeue_plan(worker).lower()
        assert SORT_NODE[connection.vendor] not in plan, plan

    def test_the_scan_stops_at_the_first_runnable_row(self, worker):
        if connection.vendor != "postgresql":
            pytest.skip("only PostgreSQL reports rows actually read")
        a_deferred_backlog()
        plan = dequeue_plan(worker)
        scan = next(line for line in plan.split("\n") if "ox_dequeue" in line)
        assert "actual" in scan and "rows=1 " in scan.split("actual")[1], scan


class TestEveryWorkerShapeReadsFromAnIndexInOrder:
    """
    A worker names one queue, several, or none. `QUEUES: []` is the default and
    filters on no queue at all, so an index that puts `queue_name` between the
    equality and the sort columns cannot be used by it: the column is
    unconstrained, and the sort comes back for the whole candidate set. Both
    index shapes ship, and the planner picks per query.
    """

    @pytest.mark.parametrize(
        "queues",
        [
            pytest.param(["default"], id="one-queue"),
            pytest.param(["default", "emails", "reports"], id="three-queues"),
            pytest.param([], id="every-queue"),
        ],
    )
    def test_no_sort_whatever_the_worker_claims(self, queues, settings):
        worker = a_worker_on(queues, settings)
        a_deferred_backlog(queues=("default", "emails", "reports"))
        plan = dequeue_plan(worker)
        assert "ox_dequeue" in plan, plan
        assert SORT_NODE[connection.vendor] not in plan.lower(), plan
