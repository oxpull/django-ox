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

import re
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.utils import timezone

from django_ox import _waiting
from django_ox.models import OxTask
from django_ox.worker import Worker

from .tasks import add

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


def analyze():
    # PostgreSQL only. ANALYZE TABLE commits the open transaction on MySQL, and
    # would take the test's rows with it.
    if connection.vendor == "postgresql":
        with connection.cursor() as cursor:
            cursor.execute("ANALYZE django_ox_oxtask")


def rows_ahead_of_the_claim(arm, ahead=400, released=50, behind=100):
    """
    `ahead` rows in front of everything runnable in claim order, then
    `released` and `behind` runnable rows.

    born-waiting is how django_ox._waiting writes them: inserted WAITING, and
    `released` of them released inside the inserting transaction, the way a
    task whose prerequisites already finished would be. far-run-after is the
    control: READY rows deferred a year, which the claim does read past.
    """
    urgent = add.using(priority=5)
    with transaction.atomic():
        for _ in range(ahead):
            if arm == "born-waiting":
                _waiting.enqueue(urgent, [1, 2], {})
            else:
                later = timezone.now() + timedelta(days=365)
                urgent.using(run_after=later).enqueue(1, 2)
        for _ in range(released):
            _waiting.release(_waiting.enqueue(add, [1, 2], {}).id)
    now = timezone.now()
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path="tests.tasks.add",
                args=[1, 2],
                kwargs={},
                backend_name="default",
                queue_name="default",
                status=OxTask.Status.READY,
                enqueued_at=now + timedelta(seconds=i),
            )
            for i in range(behind)
        ]
    )
    analyze()


def vm_steps(worker):
    """SQLite virtual machine steps for the claim's candidate select."""
    steps = 0

    def count():
        nonlocal steps
        steps += 1
        return 0

    connection.ensure_connection()
    raw = connection.connection
    raw.set_progress_handler(count, 1)
    try:
        list(worker._ready_queryset()[:1])
    finally:
        raw.set_progress_handler(None, 1)
    return steps


def handler_reads(worker):
    """MySQL Handler_read_* for the SKIP LOCKED candidate select, net of the reading."""

    def snapshot():
        with connection.cursor() as cursor:
            cursor.execute("SHOW SESSION STATUS LIKE 'Handler_read%'")
            return {name: int(value) for name, value in cursor.fetchall()}

    first = snapshot()
    second = snapshot()
    with transaction.atomic():
        list(worker._ready_queryset().select_for_update(skip_locked=True)[:1])
    third = snapshot()
    return {
        name: (third[name] - second[name]) - (second[name] - first[name])
        for name in first
    }


class TestWaitingRowsSitOutsideTheClaimsKeyRange:
    """
    Every claim is an equality on status = 'READY', and status leads both
    dequeue indexes, so a WAITING row is in a key range the scan never enters.
    Each engine's own reading of rows or steps read says so, against a control
    whose rows the scan does walk.
    """

    @pytest.mark.parametrize("arm", ["born-waiting", "far-run-after"])
    def test_waiting_rows_ahead_of_the_claim_are_not_read(self, arm, worker):
        vendor = connection.vendor
        empty_steps = vm_steps(worker) if vendor == "sqlite" else 0
        rows_ahead_of_the_claim(arm)
        candidate = worker._ready_queryset().first()
        assert (candidate.status, candidate.priority) == (OxTask.Status.READY, 0)
        read_past = arm == "far-run-after"

        if vendor == "postgresql":
            plan = dequeue_plan(worker)
            assert "ox_dequeue" in plan, plan
            assert ("Rows Removed by Filter" in plan) is read_past, plan
        elif vendor == "mysql":
            reads = handler_reads(worker)
            if read_past:
                assert reads["Handler_read_next"] >= 400, reads
            else:
                assert reads["Handler_read_next"] == 0, reads
        else:
            steps = vm_steps(worker)
            if read_past:
                assert steps > empty_steps + 400, (empty_steps, steps)
            else:
                assert steps < empty_steps + 100, (empty_steps, steps)


@contextmanager
def a_snapshot_older_than_what_follows():
    """
    A REPEATABLE READ transaction on a second connection, holding its snapshot.
    While it is open, no row version that dies after it began can be cleaned
    up, so a claim pays for every such entry left in its key range.
    """
    other = connections.create_connection(DEFAULT_DB_ALIAS)
    assert other.settings_dict["NAME"] == connection.settings_dict["NAME"]
    try:
        with other.cursor() as cursor:
            cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT count(*) FROM django_ox_oxtask")
            try:
                yield
            finally:
                cursor.execute("ROLLBACK")
    finally:
        other.close()


def claim_buffers(worker):
    with transaction.atomic():
        plan = (
            worker._ready_queryset()
            .select_for_update(skip_locked=True)[:1]
            .explain(analyze=True, buffers=True, costs=False, timing=False)
        )
    match = re.search(r"Buffers: shared hit=(\d+)(?: read=(\d+))?", plan)
    assert match, plan
    return int(match[1]) + int(match[2] or 0)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("arm", ["born-waiting", "moved-from-ready"])
def test_a_born_waiting_row_leaves_no_dead_ready_entry(arm, worker):
    """
    What the plan above cannot show. A row inserted READY and then moved to
    WAITING leaves its READY index entries behind, because status leads both
    dequeue indexes and the move is never a heap-only update. Neither Rows
    Removed by Filter nor InnoDB's handler counters count dead entries; buffers
    do. moved-from-ready is that shape, as the control, and born-waiting is
    what django_ox._waiting writes.
    """
    if connection.vendor != "postgresql":
        pytest.skip("buffers per claim are PostgreSQL's reading")
    start = timezone.now() - timedelta(hours=1)
    OxTask.objects.bulk_create(
        [
            OxTask(
                task_path="tests.tasks.add",
                args=[1, 2],
                kwargs={},
                backend_name="default",
                queue_name="default",
                status=OxTask.Status.READY,
                enqueued_at=start + timedelta(seconds=i),
            )
            for i in range(300)
        ]
    )
    analyze()
    baseline = claim_buffers(worker)

    urgent = add.using(priority=5)
    with a_snapshot_older_than_what_follows():
        with transaction.atomic():
            if arm == "born-waiting":
                for _ in range(400):
                    _waiting.enqueue(urgent, [1, 2], {})
                for _ in range(50):
                    _waiting.release(_waiting.enqueue(add, [1, 2], {}).id)
            else:
                for _ in range(400):
                    urgent.enqueue(1, 2)
                OxTask.objects.filter(priority=5).update(status=OxTask.Status.WAITING)
        analyze()
        costs = [claim_buffers(worker) for _ in range(3)]

    if arm == "born-waiting":
        assert max(costs) <= baseline + 1, (baseline, costs)
    else:
        assert min(costs) > baseline + 10, (baseline, costs)
