"""
django_ox.testing.assert_task_count(): reading OxTask rows directly, with
JSON-normalised matching on args and kwargs.

The isolated-queue recipe in docs/patterns.md is reproduced below exactly as
the page prints it, apart from its import lines, and
test_the_recipe_here_is_the_one_on_the_page keeps the two the same.
"""

import re
from pathlib import Path

import pytest
from django.db import router, transaction
from django.test import TestCase, TransactionTestCase

from django_ox.compat import task
from django_ox.models import OxTask
from django_ox.testing import assert_task_count, run_tasks

from . import testing_assertions_tasks as t
from .test_replica_reads import PRIMARY, REPLICA, _ReplicaReadRouter, no_statement_on

pytestmark = pytest.mark.django_db

PATH = "tests.testing_assertions_tasks.note"
PATTERNS = Path(__file__).resolve().parent.parent / "docs" / "patterns.md"

# Must appear in this backend's own QUEUES list (a sibling of OPTIONS,
# not inside it): TASKS["default"]["QUEUES"].
ISOLATED_QUEUE = "isolated-recipe"


@pytest.fixture
def isolated_run_tasks():
    # Checked once, here, before the test body enqueues anything: a row
    # from setUpTestData or an earlier fixture is caught before it is ever
    # claimed. The returned `run` does not repeat the check, since by then
    # the only row it is meant to find is the one the test just enqueued.
    # Read where enqueue() writes, which a replica router's reads are not.
    rows = OxTask.objects.using(router.db_for_write(OxTask))
    if rows.filter(backend_name="default", queue_name=ISOLATED_QUEUE).exists():
        raise ValueError(
            f"queue {ISOLATED_QUEUE!r} already holds rows; reserve a queue "
            "no other test or fixture writes to"
        )

    def run(**kwargs):
        return run_tasks(queues=[ISOLATED_QUEUE], **kwargs)

    return run


@task(queue_name=ISOLATED_QUEUE)
def convert_video(path):
    return f"converted {path}"


@pytest.mark.django_db
def test_convert_video(isolated_run_tasks):
    convert_video.enqueue("clip.mp4")
    [result] = isolated_run_tasks()
    assert result.return_value == "converted clip.mp4"


def _recipe_blocks():
    """The page's two code blocks for the recipe, without their import lines."""
    text = PATTERNS.read_text()
    section = text[text.index("### An isolated queue for one test") :]
    section = section[: section.index("\n## ")]
    blocks = re.findall(r"```python\n(.*?)```", section, re.DOTALL)
    return [
        "\n".join(
            line
            for line in block.splitlines()
            if not line.startswith(("import ", "from "))
        ).strip()
        for block in blocks
    ]


def test_the_recipe_here_is_the_one_on_the_page():
    blocks = _recipe_blocks()
    assert len(blocks) == 2
    source = Path(__file__).read_text()
    for block in blocks:
        assert block in source


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


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestAssertTaskCountReadsTheWriteAlias:
    """
    Under a router that sends reads to a replica, the count comes from the
    alias enqueue() wrote to, and the replica is never asked.
    """

    def test_a_read_replica_router_does_not_hide_the_row(self, settings):
        settings.DATABASE_ROUTERS = [_ReplicaReadRouter()]
        t.note.enqueue("hello")
        assert OxTask.objects.using(PRIMARY).filter(task_path=PATH).count() == 1
        with no_statement_on(REPLICA):
            assert_task_count(path=PATH, queue="default", args=["hello"], count=1)


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
