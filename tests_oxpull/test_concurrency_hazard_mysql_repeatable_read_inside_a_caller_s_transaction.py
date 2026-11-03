"""
A concurrency count inside a caller's transaction reads that transaction's view.

docs/features/concurrency-limits.md says, under the guarantee's semantics:

    Inside a caller's transaction the bound is not promised. With
    `run_once()` inside `transaction.atomic`, `run_tasks()` under a
    `TestCase`, or `AUTOCOMMIT: False`, the count reads that transaction's
    view. On MySQL's default REPEATABLE READ that is a snapshot, which hides
    other processes' claims and completions since it was taken.

This file documents that sentence on MySQL; it is not a guarantee test. The
caller opens a transaction and reads once, which fixes its read view under
REPEATABLE READ. Another connection then claims a covered row and commits.
The caller's run_once() counts from its snapshot, sees no covered row
RUNNING, and claims a second one under a ceiling of one. The claim itself is
a locking read, which is a current read on MySQL, so it skips the row the
other connection holds and takes the next.

Django's MySQL backend connects at READ COMMITTED, where each statement reads
current state. The server's default, and what isolation_level None selects,
is REPEATABLE READ. The level is set on the caller's session here as the
workflow hazard file beside this one sets it, and READ COMMITTED runs the
same sequence as the control: there the count sees the other claim and
nothing is claimed.
"""

import re
from pathlib import Path

import pytest
from django.db import connections, transaction

from django_ox.models import OxTask

from . import tasks
from .racing import race_all
from .test_concurrency_limits import GAUGED, concurrency_limits, oxpull_worker
from .test_workflows_hazard_mysql_repeatable_read_inside_a_caller_s import (
    caller_session_at,
)

pytestmark = pytest.mark.django_db(transaction=True)

DOCS = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "features"
    / "concurrency-limits.md"
)


def one_line(text):
    """Text as a reader sees it: one line, without Markdown's bold."""
    return " ".join(text.replace("**", "").split())


def test_the_docstring_quotes_the_documented_sentence():
    """The sentences above are the documentation's, word for word."""
    quoted = re.search(r"\n\n    (Inside a caller's.*?)\n\n", __doc__, re.DOTALL)
    assert quoted is not None
    assert one_line(quoted.group(1)) in one_line(DOCS.read_text())


@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ"])
def test_a_count_inside_a_callers_transaction_misses_a_claim_committed_since_its_first_read(  # noqa: E501
    level,
):
    """
    Two covered rows under a ceiling of one. Another connection claims the
    first and commits after the caller's first read. At REPEATABLE READ the
    caller's run_once() then claims and runs the second, so two covered rows
    were RUNNING at once: the one the other connection holds, which never
    leaves RUNNING here, and the one the caller ran. At READ COMMITTED the
    count sees the other claim and run_once() claims nothing.
    """
    if connections["default"].vendor != "mysql":
        pytest.skip("the snapshot this documents is MySQL REPEATABLE READ's")
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        first = tasks.gauged.enqueue(0).id
        second = tasks.gauged.enqueue(0).id

        def another_process_claims():
            claimed = oxpull_worker().claim_one()
            return None if claimed is None else str(claimed.pk)

        with caller_session_at(level), transaction.atomic():
            # The caller's first consistent read. Under REPEATABLE READ the
            # read view is fixed here, with no covered row RUNNING.
            assert not OxTask.objects.filter(status=OxTask.Status.RUNNING).exists()
            results, errors = race_all([another_process_claims])
            assert (results, errors) == ([str(first)], [])
            ran = oxpull_worker().run_once()

    held = OxTask.objects.get(pk=first)
    assert held.status == OxTask.Status.RUNNING, "the other claim never ended"
    caller = OxTask.objects.get(pk=second)
    if level == "READ COMMITTED":
        assert ran is False
        assert (caller.status, caller.attempts) == (OxTask.Status.READY, 0)
    else:
        assert ran is True, "the count saw the other claim after all"
        assert (caller.status, caller.attempts) == (OxTask.Status.SUCCESSFUL, 1)
