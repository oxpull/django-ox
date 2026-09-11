"""
A broken signal receiver is not the task's fault.

The lifecycle signals are an observability surface, and every one of them
fires somewhere that charges a raising receiver to the work: an attempt is
spent, an enqueue is refused over a row that exists, or a failure path is cut
short.
"""

import logging

import pytest

from django_ox.compat import task_enqueued, task_finished, task_started
from django_ox.models import OxTask
from django_ox.worker import Worker

from . import tasks

pytestmark = pytest.mark.django_db


class _Boom(Exception):
    pass


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


@pytest.fixture
def raising(request):
    signal = request.param

    def boom(sender, **kwargs):
        raise _Boom("a receiver nobody here wrote")

    signal.connect(boom)
    yield signal
    signal.disconnect(boom)


class TestAReceiverDoesNotConsumeTheWork:
    @pytest.mark.parametrize("raising", [task_started], indirect=True)
    def test_a_raising_task_started_does_not_spend_an_attempt(
        self, worker, raising, caplog
    ):
        result = tasks.add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert claimed is not None
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            worker.execute(claimed)

        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.SUCCESSFUL, (
            f"the task is {row.status}: a broken observability receiver was "
            "charged to the work, and the function never ran"
        )
        assert row.return_value == 3

    @pytest.mark.parametrize("raising", [task_enqueued], indirect=True)
    def test_a_raising_task_enqueued_does_not_break_enqueue(self, worker, raising):
        result = tasks.add.enqueue(1, 2)
        assert OxTask.objects.filter(id=result.id).exists()
        assert OxTask.objects.count() == 1, (
            "enqueue() raised over a row that already exists, so a caller who "
            "retries creates a duplicate task"
        )

    @pytest.mark.parametrize("raising", [task_finished], indirect=True)
    def test_a_raising_task_finished_does_not_hide_the_outcome(self, worker, raising):
        result = tasks.fail_always.enqueue()
        claimed = worker.claim_one()
        assert claimed is not None
        worker.execute(claimed)
        row = OxTask.objects.get(id=result.id)
        assert row.status in (OxTask.Status.READY, OxTask.Status.FAILED), (
            f"the failure path was cut short by a receiver: {row.status}"
        )
