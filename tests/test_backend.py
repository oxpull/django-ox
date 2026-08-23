import os
import subprocess
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from django.db import transaction
from django.tasks import TaskResultStatus, default_task_backend
from django.tasks.exceptions import TaskResultDoesNotExist, TaskResultMismatch
from django.tasks.signals import task_enqueued
from django.utils import timezone

from django_ox.models import OxTask

from .tasks import STATE, add, async_add, echo, record, send_email, with_context

pytestmark = pytest.mark.django_db


def test_enqueue_creates_ready_row():
    result = add.enqueue(1, 2)

    assert result.status == TaskResultStatus.READY
    assert result.enqueued_at is not None
    assert result.started_at is None
    assert result.attempts == 0

    db_task = OxTask.objects.get(id=result.id)
    assert db_task.status == OxTask.Status.READY
    assert db_task.task_path == "tests.tasks.add"
    assert db_task.args == [1, 2]
    assert db_task.queue_name == "default"


def test_enqueue_sends_task_enqueued_signal():
    received = []

    def receiver(sender, task_result, **kwargs):
        received.append(task_result)

    task_enqueued.connect(receiver)
    try:
        result = add.enqueue(1, 2)
    finally:
        task_enqueued.disconnect(receiver)
    assert len(received) == 1
    assert received[0].id == result.id


def test_roundtrip_enqueue_run_result(worker):
    result = add.enqueue(4, 5)

    assert worker.run_once() is True
    result.refresh()

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 9
    assert result.is_finished
    assert result.attempts == 1
    assert result.worker_ids == [worker.worker_id]
    assert result.started_at is not None
    assert result.finished_at is not None
    assert result.last_attempted_at is not None


def test_args_kwargs_serialization_roundtrip(worker):
    payload = {"a": [1, 2.5, None, True, "x"], "nested": {"k": "v"}}
    result = echo.enqueue(payload)
    worker.run_once()
    result.refresh()
    assert result.return_value == payload

    # Tuples are normalized to lists, per django.utils.json.normalize_json.
    result = echo.enqueue((1, 2))
    assert result.args == [[1, 2]]


def test_get_result_roundtrip(worker):
    result = add.enqueue(2, 3)
    worker.run_once()

    fetched = add.get_result(result.id)
    assert fetched.status == TaskResultStatus.SUCCESSFUL
    assert fetched.return_value == 5
    assert fetched.task.func is add.func


def test_get_result_unknown_id():
    with pytest.raises(TaskResultDoesNotExist):
        default_task_backend.get_result(str(uuid.uuid4()))
    with pytest.raises(TaskResultDoesNotExist):
        default_task_backend.get_result("not-a-uuid")


def test_get_result_task_mismatch():
    result = add.enqueue(1, 2)
    with pytest.raises(TaskResultMismatch):
        echo.get_result(result.id)


def test_aget_result_and_aenqueue(worker):
    from asgiref.sync import async_to_sync

    result = async_to_sync(add.aenqueue)(10, 20)
    worker.run_once()
    fetched = async_to_sync(add.aget_result)(result.id)
    assert fetched.return_value == 30


def test_transactional_enqueue_rolls_back_with_transaction():
    with pytest.raises(RuntimeError), transaction.atomic():
        add.enqueue(1, 2)
        raise RuntimeError("abort business transaction")
    assert OxTask.objects.count() == 0


def test_non_task_path_is_not_executed(worker):
    # A row whose task_path points at an arbitrary importable callable (here
    # os.system) must never be called: the worker records an ImportError
    # failure instead of executing the dotted path from the table.
    db_task = OxTask.objects.create(
        task_path="os.system",
        args=["touch /tmp/django-ox-should-never-exist"],
        kwargs={},
        queue_name="default",
        backend_name="default",
        max_attempts=1,
        enqueued_at=timezone.now(),
    )

    assert worker.run_once() is True

    db_task.refresh_from_db()
    assert db_task.status == OxTask.Status.FAILED
    assert db_task.errors[-1]["exception_class_path"] == "builtins.ImportError"
    assert not Path("/tmp/django-ox-should-never-exist").exists()


def test_non_task_path_get_result_raises(worker):
    db_task = OxTask.objects.create(
        task_path="os.system",
        args=[],
        kwargs={},
        queue_name="default",
        backend_name="default",
        enqueued_at=timezone.now(),
    )
    with pytest.raises(ImportError):
        default_task_backend.get_result(str(db_task.id))


def test_run_after_deferral(worker):
    from django.utils import timezone

    future = timezone.now() + timedelta(seconds=60)
    add.using(run_after=future).enqueue(1, 2)

    assert worker.run_once() is False

    OxTask.objects.update(run_after=timezone.now() - timedelta(seconds=1))
    assert worker.run_once() is True
    assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL


def test_priority_ordering(worker):
    record.enqueue("low")
    record.using(priority=10).enqueue("high")
    record.using(priority=-10).enqueue("lowest")

    while worker.run_once():
        pass
    assert STATE["order"] == ["high", "low", "lowest"]


def test_queue_filtering():
    from django_ox.worker import Worker

    add.enqueue(1, 2)
    send_email.enqueue("a@example.com")

    email_worker = Worker(queues=["emails"], backoff_initial=0)
    assert email_worker.run_once() is True
    assert email_worker.run_once() is False

    assert OxTask.objects.get(queue_name="emails").status == OxTask.Status.SUCCESSFUL
    assert OxTask.objects.get(queue_name="default").status == OxTask.Status.READY


def test_takes_context(worker):
    result = with_context.enqueue()
    worker.run_once()
    result.refresh()
    assert result.return_value == {"attempt": 1, "id": result.id}


def test_async_task(worker):
    result = async_add.enqueue(3, 4)
    worker.run_once()
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 7


def test_capability_flags():
    assert default_task_backend.supports_defer
    assert default_task_backend.supports_get_result
    assert default_task_backend.supports_priority
    assert default_task_backend.supports_async_task


def test_check_requires_installed_app(settings):
    assert default_task_backend.check() == []
    settings.INSTALLED_APPS = []
    errors = default_task_backend.check()
    assert [e.id for e in errors] == ["django_ox.E001"]


def test_manage_py_check_cannot_see_a_missing_app_entry():
    """
    The app's ready() is what registers the django-ox checks, so E001, the
    missing-app check, never fires from manage.py check in a project that
    imports django.tasks nowhere else. The configuration page says so.
    """
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "tests.settings_plain"
    env["OX_TEST_DROP_APP"] = "1"
    completed = subprocess.run(
        [sys.executable, "-m", "django", "check"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "E001" not in completed.stderr
