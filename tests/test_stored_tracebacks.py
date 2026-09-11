"""
What a failed attempt is allowed to write onto the row.

`errors` holds one entry per attempt and each carries a traceback, whose length
is decided by the failure rather than by us. A deep recursion, a long chain of
`raise ... from ...`, or a library that prints locals into its traceback can all
produce something far larger than the row it lands in, and the row is read back
by the admin and by anything polling the task's result.
"""

import pytest

from django_ox.models import OxTask
from django_ox.worker import (
    MAX_STORED_TRACEBACK,
    Worker,
    _stored_traceback,
)

from .tasks import failing_with_long_traceback

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


def an_oversized_exception(size):
    return an_oversized_exception_of("x" * size)


def an_oversized_exception_of(message):
    try:
        raise ValueError(message)
    except ValueError as exc:
        return exc


class TestATracebackIsBounded:
    def test_an_ordinary_traceback_is_stored_whole(self):
        exc = an_oversized_exception(10)
        assert _stored_traceback(exc) == "".join(
            __import__("traceback").format_exception(exc)
        )

    def test_an_oversized_one_is_cut_to_the_documented_limit(self):
        stored = _stored_traceback(an_oversized_exception(MAX_STORED_TRACEBACK * 4))
        # The whole string, marker included, fits the published cap. The cap is
        # what an operator sizes the column from, so being "a little over" it
        # is being wrong about it.
        assert len(stored.encode()) <= MAX_STORED_TRACEBACK
        assert "bytes of this traceback were not stored" in stored

    def test_the_cap_is_bytes_and_not_characters(self):
        # One emoji is four bytes. Counted in characters, a traceback carrying
        # them stores four times the published limit.
        exc = an_oversized_exception_of("\U0001f600" * MAX_STORED_TRACEBACK)
        stored = _stored_traceback(exc)
        assert len(stored.encode()) <= MAX_STORED_TRACEBACK, (
            f"stored {len(stored.encode())} bytes against a "
            f"{MAX_STORED_TRACEBACK} byte cap"
        )

    def test_a_cut_never_leaves_a_broken_character(self):
        exc = an_oversized_exception_of("\u00e9" * MAX_STORED_TRACEBACK)
        stored = _stored_traceback(exc)
        # Would raise if either end had been cut mid-sequence.
        stored.encode().decode()

    def test_both_ends_survive_the_cut(self):
        exc = an_oversized_exception(MAX_STORED_TRACEBACK * 4)
        stored = _stored_traceback(exc)
        assert stored.startswith("Traceback (most recent call last)"), (
            "the head is gone, so where the call came from is unrecoverable"
        )
        assert stored.rstrip().endswith("x"), (
            "the tail is gone, so what actually raised is unrecoverable"
        )

    def test_the_row_a_real_failure_writes_is_bounded(self, worker):
        failing_with_long_traceback.enqueue()
        db_task = worker.claim_one()
        worker.execute(db_task)
        db_task.refresh_from_db()
        (error,) = db_task.errors
        assert len(error["traceback"]) < MAX_STORED_TRACEBACK + 200, (
            "a task can write an unbounded string onto its own row"
        )
        assert db_task.status in {OxTask.Status.READY, OxTask.Status.FAILED}
