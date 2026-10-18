import json
import uuid
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from django_ox.actions import (
    MAX_ATTEMPTS_LIMIT,
)
from django_ox.models import OxTask

from .test_prune import make_task


@pytest.mark.django_db(transaction=True)
class TestOxRetryCommand:
    def test_retry_command_requeues_failed_and_lost(self):
        failed = make_task(OxTask.Status.FAILED)
        lost = make_task(OxTask.Status.LOST)
        ready = make_task(OxTask.Status.READY)

        out = StringIO()
        call_command(
            "ox_retry", str(failed.pk), str(lost.pk), str(ready.pk), stdout=out
        )
        assert "Retried 2 task(s); 1 skipped." in out.getvalue()

        failed.refresh_from_db()
        lost.refresh_from_db()
        ready.refresh_from_db()

        assert failed.status == OxTask.Status.READY
        assert lost.status == OxTask.Status.READY
        assert ready.status == OxTask.Status.READY

    def test_retry_json_format_execution(self):
        failed = make_task(OxTask.Status.FAILED)
        unknown = str(uuid.uuid4())

        out = StringIO()
        call_command(
            "ox_retry", str(failed.pk), unknown, "--format", "json", stdout=out
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 1}

    def test_retry_dry_run_preview(self):
        failed = make_task(OxTask.Status.FAILED)
        lost = make_task(OxTask.Status.LOST)
        ready = make_task(OxTask.Status.READY)

        out = StringIO()
        call_command(
            "ox_retry",
            str(failed.pk),
            str(lost.pk),
            str(ready.pk),
            "--dry-run",
            stdout=out,
        )
        assert "Would retry 2 task(s); 1 skipped." in out.getvalue()

        failed.refresh_from_db()
        assert failed.status == OxTask.Status.FAILED

    def test_retry_dry_run_json_format(self):
        failed = make_task(OxTask.Status.FAILED)
        unknown = str(uuid.uuid4())

        out = StringIO()
        call_command(
            "ox_retry",
            str(failed.pk),
            unknown,
            "--dry-run",
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"eligible": 1, "skipped": 1, "dry_run": True}

    def test_retry_queue_scoping(self):
        failed_q1 = make_task(OxTask.Status.FAILED, queue_name="q1")
        failed_q2 = make_task(OxTask.Status.FAILED, queue_name="q2")

        out = StringIO()
        call_command(
            "ox_retry",
            str(failed_q1.pk),
            str(failed_q2.pk),
            "--queue",
            "q1",
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 1}

        failed_q1.refresh_from_db()
        failed_q2.refresh_from_db()
        assert failed_q1.status == OxTask.Status.READY
        assert failed_q2.status == OxTask.Status.FAILED

    def test_retry_attempts_ceiling(self):
        failed_capped = make_task(OxTask.Status.FAILED)
        OxTask.objects.filter(pk=failed_capped.pk).update(attempts=MAX_ATTEMPTS_LIMIT)

        out = StringIO()
        call_command(
            "ox_retry",
            str(failed_capped.pk),
            "--dry-run",
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"eligible": 0, "skipped": 1, "dry_run": True}

        # Actual execution skips capped attempts
        out = StringIO()
        call_command("ox_retry", str(failed_capped.pk), "--format", "json", stdout=out)
        data = json.loads(out.getvalue())
        assert data == {"changed": 0, "skipped": 1}

    def test_retry_malformed_id_raises_usage_error(self):
        with pytest.raises(CommandError) as info:
            call_command("ox_retry", "not-a-uuid")
        assert getattr(info.value, "returncode", 1) == 2
        assert "Invalid task ID 'not-a-uuid'; must be a valid UUID." in str(info.value)

    def test_retry_duplicate_ids_counted_once(self):
        failed = make_task(OxTask.Status.FAILED)
        out = StringIO()
        call_command(
            "ox_retry",
            str(failed.pk),
            str(failed.pk),
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 0}


@pytest.mark.django_db(transaction=True)
class TestOxDiscardCommand:
    def test_discard_command_closes_eligible_tasks(self):
        ready = make_task(OxTask.Status.READY)
        waiting = make_task(OxTask.Status.WAITING)
        failed = make_task(OxTask.Status.FAILED)
        lost = make_task(OxTask.Status.LOST)
        running = make_task(OxTask.Status.RUNNING)

        out = StringIO()
        call_command(
            "ox_discard",
            str(ready.pk),
            str(waiting.pk),
            str(failed.pk),
            str(lost.pk),
            str(running.pk),
            stdout=out,
        )
        assert "Discarded 4 task(s); 1 skipped." in out.getvalue()

        ready.refresh_from_db()
        waiting.refresh_from_db()
        failed.refresh_from_db()
        lost.refresh_from_db()
        running.refresh_from_db()

        assert ready.status == OxTask.Status.DISCARDED
        assert waiting.status == OxTask.Status.DISCARDED
        assert failed.status == OxTask.Status.DISCARDED
        assert lost.status == OxTask.Status.DISCARDED
        assert running.status == OxTask.Status.RUNNING

    def test_discard_json_format_execution(self):
        ready = make_task(OxTask.Status.READY)
        unknown = str(uuid.uuid4())

        out = StringIO()
        call_command(
            "ox_discard", str(ready.pk), unknown, "--format", "json", stdout=out
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 1}

    def test_discard_dry_run_preview(self):
        ready = make_task(OxTask.Status.READY)
        running = make_task(OxTask.Status.RUNNING)

        out = StringIO()
        call_command(
            "ox_discard",
            str(ready.pk),
            str(running.pk),
            "--dry-run",
            stdout=out,
        )
        assert "Would discard 1 task(s); 1 skipped." in out.getvalue()

        ready.refresh_from_db()
        assert ready.status == OxTask.Status.READY

    def test_discard_dry_run_json_format(self):
        ready = make_task(OxTask.Status.READY)
        unknown = str(uuid.uuid4())

        out = StringIO()
        call_command(
            "ox_discard",
            str(ready.pk),
            unknown,
            "--dry-run",
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"eligible": 1, "skipped": 1, "dry_run": True}

    def test_discard_queue_scoping(self):
        ready_q1 = make_task(OxTask.Status.READY, queue_name="q1")
        ready_q2 = make_task(OxTask.Status.READY, queue_name="q2")

        out = StringIO()
        call_command(
            "ox_discard",
            str(ready_q1.pk),
            str(ready_q2.pk),
            "--queue",
            "q1",
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 1}

        ready_q1.refresh_from_db()
        ready_q2.refresh_from_db()
        assert ready_q1.status == OxTask.Status.DISCARDED
        assert ready_q2.status == OxTask.Status.READY

    def test_discard_malformed_id_raises_usage_error(self):
        with pytest.raises(CommandError) as info:
            call_command("ox_discard", "invalid-pk")
        assert getattr(info.value, "returncode", 1) == 2
        assert "Invalid task ID 'invalid-pk'; must be a valid UUID." in str(info.value)

    def test_discard_duplicate_ids_counted_once(self):
        ready = make_task(OxTask.Status.READY)
        out = StringIO()
        call_command(
            "ox_discard",
            str(ready.pk),
            str(ready.pk),
            "--format",
            "json",
            stdout=out,
        )
        data = json.loads(out.getvalue())
        assert data == {"changed": 1, "skipped": 0}
