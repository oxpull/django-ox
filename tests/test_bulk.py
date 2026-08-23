"""
django_ox.bulk.enqueue_many: one INSERT per chunk, input order kept, the
same rollback behaviour as enqueue(), and nothing written when the task is
rejected. Runs on every database in the matrix; the parameter-count test is
the one that matters on SQLite.
"""

from datetime import timedelta

import pytest
from django.conf import settings
from django.db import connection, transaction
from django.tasks import TaskResultStatus
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from django.test import override_settings
from django.utils import timezone

from django_ox.backend import INSERT_CHUNK_SIZE
from django_ox.bulk import enqueue_many
from django_ox.models import OxTask

from .tasks import STATE, add, record

pytestmark = pytest.mark.django_db


class CapturedInserts:
    """Every INSERT the connection ran, as (sql, params) pairs."""

    def __init__(self):
        self.statements = []

    def __call__(self, execute, sql, params, many, context):
        if sql.lstrip().upper().startswith("INSERT"):
            self.statements.append((sql, params))
        return execute(sql, params, many, context)


def test_returns_results_in_input_order():
    results = enqueue_many(add, [((i, i), {}) for i in range(50)])

    assert [r.args for r in results] == [[i, i] for i in range(50)]
    assert all(r.status == TaskResultStatus.READY for r in results)
    assert len({r.id for r in results}) == 50
    rows = {str(t.id): t for t in OxTask.objects.all()}
    assert [rows[r.id].args for r in results] == [[i, i] for i in range(50)]


def test_row_matches_enqueue_row():
    """The worker reads one format: a bulk row equals a single-enqueue row."""
    single = OxTask.objects.get(id=add.using(priority=3).enqueue(1, b=2).id)
    (bulk,) = enqueue_many(add.using(priority=3), [((1,), {"b": 2})])
    many = OxTask.objects.get(id=bulk.id)

    compared = [
        f.name for f in OxTask._meta.fields if f.name not in ("id", "enqueued_at")
    ]
    for name in compared:
        assert getattr(single, name) == getattr(many, name), name


def test_honours_queue_priority_and_run_after():
    later = timezone.now() + timedelta(hours=1)
    bound = record.using(queue_name="emails", priority=7, run_after=later)

    results = enqueue_many(bound, [(("a",), {}), (("b",), {})])

    rows = OxTask.objects.filter(id__in=[r.id for r in results])
    assert {r.queue_name for r in rows} == {"emails"}
    assert {r.priority for r in rows} == {7}
    assert {r.run_after for r in rows} == {later}


def test_sends_task_enqueued_per_row():
    received = []

    def receiver(sender, task_result, **kwargs):
        received.append(task_result.id)

    task_enqueued.connect(receiver)
    try:
        results = enqueue_many(add, [((1, 1), {}), ((2, 2), {})])
    finally:
        task_enqueued.disconnect(receiver)
    assert received == [r.id for r in results]


def test_rollback_removes_every_row():
    class Abort(Exception):
        pass

    with pytest.raises(Abort), transaction.atomic():
        calls = [((i, 1), {}) for i in range(INSERT_CHUNK_SIZE + 5)]
        results = enqueue_many(add, calls)
        assert OxTask.objects.count() == INSERT_CHUNK_SIZE + 5
        raise Abort

    assert OxTask.objects.count() == 0
    assert len(results) == INSERT_CHUNK_SIZE + 5


def test_empty_calls_write_nothing():
    assert enqueue_many(add, []) == []
    assert OxTask.objects.count() == 0


def test_non_task_is_rejected_before_any_write():
    def plain(a, b):
        return a + b

    with pytest.raises(InvalidTask):
        enqueue_many(plain, [((1, 2), {})])
    assert OxTask.objects.count() == 0


def test_unknown_queue_is_rejected_before_any_write():
    # tests.settings restricts QUEUES to default and emails.
    with pytest.raises(InvalidTask, match="not valid"):
        enqueue_many(add.using(queue_name="nope"), [((1, 2), {})])
    assert OxTask.objects.count() == 0


@override_settings(
    TASKS={
        "default": settings.TASKS["default"],
        "dummy": {"BACKEND": "django.tasks.backends.dummy.DummyBackend"},
    }
)
def test_task_on_another_backend_is_rejected():
    with pytest.raises(InvalidTask, match="not django_ox"):
        enqueue_many(add.using(backend="dummy"), [((1, 2), {})])
    assert OxTask.objects.count() == 0


def test_unserialisable_argument_writes_nothing():
    with pytest.raises(TypeError):
        enqueue_many(add, [((1, 2), {}), ((object(), 2), {})])
    assert OxTask.objects.count() == 0


def test_five_thousand_tasks_chunked_under_the_parameter_limit(worker):
    captured = CapturedInserts()
    with connection.execute_wrapper(captured):
        results = enqueue_many(record, [((i,), {}) for i in range(5000)])

    assert len(results) == 5000
    assert OxTask.objects.count() == 5000
    assert len(captured.statements) == 5000 // INSERT_CHUNK_SIZE
    if connection.vendor == "sqlite":
        # One bound variable per column per row; PostgreSQL binds one array
        # per column instead, so the count is only meaningful here.
        columns = len(OxTask._meta.fields)
        for _sql, params in captured.statements:
            assert len(params) == INSERT_CHUNK_SIZE * columns
            assert len(params) <= connection.features.max_query_params

    # An inline worker drains all of them. Every row in one call shares an
    # enqueued_at, so the claim order among them is not promised.
    drained = 0
    while worker.run_once():
        drained += 1
    assert drained == 5000
    assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 5000
    assert sorted(STATE["order"]) == list(range(5000))
