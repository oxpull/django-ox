"""
django_ox.testing.run_tasks() against oxpull's worker: a workflow chain whose
dependents are released by a commit callback of the outcome write, and a
rate limit that the claim enforces.

Every test here carries commit_callbacks_wait: run_tasks() runs the outcome
write's callbacks itself, and the suite's own stand-in for a commit around
Worker.execute would otherwise run them first and leave them registered.
"""

import re

import pytest
from django.db import connection

from django_ox import _run_tasks
from django_ox.compat import TaskResultStatus
from django_ox.models import OxTask
from django_ox.testing import run_tasks
from oxpull import workflows

from . import tasks

pytestmark = [pytest.mark.django_db, pytest.mark.commit_callbacks_wait]


def chain(name="chain"):
    flow = workflows.create(name)
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    flow.add("c", tasks.step, "c", depends_on="b")
    flow.seal()
    return flow


def test_a_workflow_chain_runs_in_order_inside_the_test_transaction():
    flow = chain()
    results = run_tasks(backend="flows")
    assert [r.return_value for r in results] == ["a", "b", "c"]
    assert tasks.STATE["order"] == ["a", "b", "c"]
    progress = flow.progress()
    assert progress.succeeded == 3
    assert progress.pending == 0
    assert connection.in_atomic_block


def test_stepping_a_chain_one_attempt_at_a_time():
    chain()
    assert [r.return_value for r in run_tasks(backend="flows", max_tasks=1)] == ["a"]
    assert [r.return_value for r in run_tasks(backend="flows", max_tasks=1)] == ["b"]
    assert [r.return_value for r in run_tasks(backend="flows")] == ["c"]


def test_a_failed_node_does_not_release_its_dependent():
    flow = workflows.create("fails")
    flow.add("a", tasks.failing_step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    OxTask.objects.update(max_attempts=1)
    results = run_tasks(backend="flows")
    assert [r.status for r in results] == [TaskResultStatus.FAILED]
    assert tasks.STATE["order"] == ["a"]
    assert flow.progress().failed == 1


@pytest.mark.django_db(transaction=True)
def test_a_workflow_chain_runs_in_order_in_autocommit():
    flow = chain("autocommit")
    results = run_tasks(backend="flows")
    assert [r.return_value for r in results] == ["a", "b", "c"]
    assert flow.progress().succeeded == 3


def test_a_rate_limit_holds_work_back_and_the_drain_returns():
    for value in range(3):
        tasks.limited_echo.enqueue(value)
    results = run_tasks(backend="limited")
    assert [r.return_value for r in results] == [0]
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 2


def test_a_rate_limited_row_at_the_safety_limit_raises_conservatively(monkeypatch):
    # The limit is applied when the claim runs, and the look at what is left
    # does not claim, so it counts the held row as due work: documented as
    # the conservative side of the safety limit.
    monkeypatch.setattr(_run_tasks, "SAFETY_LIMIT", 1)
    for value in range(2):
        tasks.limited_echo.enqueue(value)
    with pytest.raises(
        RuntimeError, match=re.escape("'tests_oxpull.tasks.limited_echo'")
    ):
        run_tasks(backend="limited")
    assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 1
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 1
