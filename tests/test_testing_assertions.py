"""
django_ox.testing.assert_task_count(): reading OxTask rows directly, with
JSON-normalised matching on args and kwargs.
"""

import pytest
from django.db import transaction
from django.test import TestCase, TransactionTestCase

from django_ox.models import OxTask
from django_ox.testing import assert_task_count, run_tasks

from . import testing_assertions_tasks as t

pytestmark = pytest.mark.django_db

PATH = "tests.testing_assertions_tasks.note"
ISOLATED_QUEUE = "isolated-recipe"


@pytest.fixture
def isolated_run_tasks():
    """docs/patterns.md's isolated-queue recipe, reproduced to test it."""
    if OxTask.objects.filter(
        backend_name="default", queue_name=ISOLATED_QUEUE
    ).exists():
        raise ValueError(
            f"queue {ISOLATED_QUEUE!r} already holds rows; reserve a queue "
            "no other test or fixture writes to"
        )

    def run(**kwargs):
        return run_tasks(queues=[ISOLATED_QUEUE], **kwargs)

    return run


class TestAssertTaskCount(TestCase):
    def test_matches_an_enqueued_task_by_path_args_and_kwargs(self):
        t.note.enqueue("hello")
        assert_task_count(path=PATH, queue="default", args=["hello"], count=1)

    def test_zero_when_nothing_matches(self):
        assert_task_count(path=PATH, queue="default", count=0)

    def test_tuple_args_match_the_stored_list(self):
        t.note.enqueue("hello")
        assert_task_count(path=PATH, queue="default", args=("hello",), count=1)

    def test_kwargs_are_matched_too(self):
        t.greet.enqueue(name="Ada", loud=True)
        assert_task_count(
            path="tests.testing_assertions_tasks.greet",
            queue="default",
            kwargs={"name": "Ada", "loud": True},
            count=1,
        )

    def test_wrong_args_do_not_match(self):
        t.note.enqueue("hello")
        assert_task_count(path=PATH, queue="default", args=["goodbye"], count=0)

    def test_count_mismatch_reports_expected_and_observed(self):
        t.note.enqueue("hello")
        t.note.enqueue("hello")
        with pytest.raises(AssertionError) as excinfo:
            assert_task_count(path=PATH, queue="default", args=["hello"], count=1)
        message = str(excinfo.value)
        assert "expected 1" in message
        assert "found 2" in message
        assert PATH in message
        assert "default" in message

    def test_a_rolled_back_enqueue_is_not_counted(self):
        with pytest.raises(ValueError, match="roll back"), transaction.atomic():
            t.note.enqueue("hello")
            raise ValueError("roll back")
        assert_task_count(path=PATH, queue="default", args=["hello"], count=0)

    def test_scoped_to_its_own_queue(self):
        t.note.using(queue_name="emails").enqueue("hello")
        assert_task_count(path=PATH, queue="emails", args=["hello"], count=1)
        assert_task_count(path=PATH, queue="default", args=["hello"], count=0)

    def test_scoped_to_its_own_backend(self):
        t.note.enqueue("hello")
        assert_task_count(
            path=PATH, queue="default", args=["hello"], backend="other", count=0
        )

    def test_a_finished_rows_path_args_and_kwargs_still_count(self):
        from django_ox.testing import run_tasks

        t.note.enqueue("hello")
        run_tasks()
        assert_task_count(path=PATH, queue="default", args=["hello"], count=1)


class TestAssertTaskCountUnderATransactionTestCase(TransactionTestCase):
    """
    TestCase above wraps every test in a transaction it rolls back; a plain
    enqueue() there never truly commits. TransactionTestCase wraps nothing,
    so the same assertion has to read a row that is actually committed, and
    still has to exclude one that a nested atomic() rolled back.
    """

    def test_a_committed_enqueue_is_counted(self):
        t.note.enqueue("hello")
        assert_task_count(path=PATH, queue="default", args=["hello"], count=1)

    def test_a_rolled_back_nested_atomic_is_not_counted(self):
        with pytest.raises(ValueError, match="roll back"), transaction.atomic():
            t.note.enqueue("hello")
            raise ValueError("roll back")
        assert_task_count(path=PATH, queue="default", args=["hello"], count=0)


class TestAssertTaskCountArguments:
    def test_rejects_a_non_string_path(self):
        with pytest.raises(TypeError, match="path"):
            assert_task_count(path=123, queue="default", count=0)

    def test_rejects_a_non_string_queue(self):
        with pytest.raises(TypeError, match="queue"):
            assert_task_count(path=PATH, queue=123, count=0)

    def test_rejects_a_non_integer_count(self):
        with pytest.raises(TypeError, match="count"):
            assert_task_count(path=PATH, queue="default", count="1")

    def test_rejects_a_negative_count(self):
        with pytest.raises(ValueError, match="count"):
            assert_task_count(path=PATH, queue="default", count=-1)

    def test_rejects_a_datetime_argument(self):
        from datetime import datetime

        with pytest.raises(TypeError, match="JSON round trip"):
            assert_task_count(
                path=PATH, queue="default", args=[datetime.now()], count=0
            )

    def test_rejects_a_uuid_keyword_argument(self):
        import uuid

        with pytest.raises(TypeError, match="JSON round trip"):
            assert_task_count(
                path=PATH, queue="default", kwargs={"id": uuid.uuid4()}, count=0
            )

    def test_rejects_a_decimal_argument(self):
        from decimal import Decimal

        with pytest.raises(TypeError, match="JSON round trip"):
            assert_task_count(
                path=PATH, queue="default", args=[Decimal("1.5")], count=0
            )


def test_isolated_run_tasks_runs_only_the_reserved_queue(isolated_run_tasks):
    """docs/patterns.md's "An isolated queue for one test" recipe."""
    t.isolated_task.enqueue("clip.mp4")
    t.note.enqueue("unrelated")
    [result] = isolated_run_tasks()
    assert result.return_value == "clip.mp4"
    assert_task_count(
        path="tests.testing_assertions_tasks.note",
        queue="default",
        args=["unrelated"],
        count=1,
    )


def test_isolated_run_tasks_refuses_a_queue_already_holding_a_row(request):
    # Simulates what setUpTestData or an earlier fixture would leave behind:
    # a row in the reserved queue before the recipe's own check ever runs.
    t.isolated_task.enqueue("leftover")
    with pytest.raises(ValueError, match="already holds"):
        request.getfixturevalue("isolated_run_tasks")
    # Refused before any claim: the leftover row is untouched.
    assert_task_count(
        path="tests.testing_assertions_tasks.isolated_task",
        queue=ISOLATED_QUEUE,
        args=["leftover"],
        count=1,
    )
