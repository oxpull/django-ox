import json
import uuid
from io import StringIO

import pytest
from django.core.management import ManagementUtility, call_command
from django.core.management.base import CommandError
from django.utils import timezone

from django_ox.actions import (
    MAX_ATTEMPTS_LIMIT,
)
from django_ox.compat import task_enqueued, task_finished
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


def run_json(command, *args):
    out = StringIO()
    call_command(command, *args, "--format", "json", stdout=out)
    return json.loads(out.getvalue())


def snapshot():
    return {
        row["pk"]: row
        for row in OxTask.objects.values(
            "pk", "status", "attempts", "lease_epoch", "max_attempts"
        )
    }


def mixed_rows():
    """One row per status, the two ceiling cases, and a row on another queue."""
    rows = {
        "failed": make_task(OxTask.Status.FAILED),
        "lost": make_task(OxTask.Status.LOST),
        "ready": make_task(OxTask.Status.READY),
        "waiting": make_task(OxTask.Status.WAITING),
        "running": make_task(OxTask.Status.RUNNING),
        "successful": make_task(OxTask.Status.SUCCESSFUL),
        "discarded": make_task(OxTask.Status.DISCARDED),
        "nearly_capped": make_task(OxTask.Status.FAILED),
        "capped": make_task(OxTask.Status.FAILED),
        "elsewhere": make_task(OxTask.Status.FAILED, queue_name="emails"),
    }
    OxTask.objects.filter(pk=rows["nearly_capped"].pk).update(
        attempts=MAX_ATTEMPTS_LIMIT - 1
    )
    OxTask.objects.filter(pk=rows["capped"].pk).update(attempts=MAX_ATTEMPTS_LIMIT)
    return rows


RETRIES = ["failed", "lost", "nearly_capped"]
DISCARDS = ["ready", "waiting", "failed", "lost", "nearly_capped", "capped"]


@pytest.mark.django_db(transaction=True)
class TestPreviewMatchesExecution:
    @pytest.mark.parametrize(
        "command,moves,moved_to",
        [
            ("ox_retry", RETRIES, OxTask.Status.READY),
            ("ox_discard", DISCARDS, OxTask.Status.DISCARDED),
        ],
    )
    def test_every_status_the_ceiling_and_the_queue_filter(
        self, command, moves, moved_to
    ):
        rows = mixed_rows()
        # Every row, one of them twice, and an id that is not in the table.
        ids = [str(row.pk) for row in rows.values()]
        ids += [str(rows["failed"].pk), str(uuid.uuid4())]
        distinct = len(rows) + 1
        before = snapshot()

        preview = run_json(command, *ids, "--queue", "default", "--dry-run")

        assert preview == {
            "eligible": len(moves),
            "skipped": distinct - len(moves),
            "dry_run": True,
        }
        assert snapshot() == before

        result = run_json(command, *ids, "--queue", "default")

        assert result == {"changed": preview["eligible"], "skipped": preview["skipped"]}
        after = snapshot()
        for name, row in rows.items():
            expected = moved_to if name in moves else before[row.pk]["status"]
            assert after[row.pk]["status"] == expected, name
        assert run_json(command, *ids, "--queue", "default", "--dry-run") == {
            "eligible": 0,
            "skipped": distinct,
            "dry_run": True,
        }

    def test_retry_keeps_attempts_and_advances_the_lease_epoch(self):
        failed = make_task(OxTask.Status.FAILED)
        OxTask.objects.filter(pk=failed.pk).update(
            attempts=5, lease_epoch=7, run_after=timezone.now()
        )

        assert run_json("ox_retry", str(failed.pk)) == {"changed": 1, "skipped": 0}

        failed.refresh_from_db()
        assert failed.status == OxTask.Status.READY
        assert failed.attempts == 5
        assert failed.max_attempts == 6
        assert failed.lease_epoch == 8
        assert failed.run_after is None

    @pytest.mark.parametrize("command", ["ox_retry", "ox_discard"])
    def test_sends_no_task_signal(self, command):
        rows = [make_task(OxTask.Status.FAILED), make_task(OxTask.Status.LOST)]
        received = []

        def receiver(sender, **kwargs):
            received.append(kwargs)

        task_enqueued.connect(receiver)
        task_finished.connect(receiver)
        try:
            result = run_json(command, *(str(row.pk) for row in rows))
        finally:
            task_enqueued.disconnect(receiver)
            task_finished.disconnect(receiver)

        assert result == {"changed": 2, "skipped": 0}
        assert received == []


class _WritesToAlt:
    """
    Sends django_ox's writes to the alt database and its reads to the default
    one, where these tests keep no row. A lookup that follows db_for_read, or
    a hard-coded alias, finds nothing to retry or discard.
    """

    def db_for_read(self, model, **hints):
        return "default" if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
class TestRoutedWrites:
    @pytest.mark.parametrize(
        "command,moved_to",
        [("ox_retry", OxTask.Status.READY), ("ox_discard", OxTask.Status.DISCARDED)],
    )
    def test_selection_preview_and_execution_follow_db_for_write(
        self, settings, command, moved_to
    ):
        row = OxTask.objects.using("alt").create(
            task_path="tests.tasks.add",
            backend_name="default",
            queue_name="default",
            status=OxTask.Status.FAILED,
            enqueued_at=timezone.now(),
        )
        settings.DATABASE_ROUTERS = [_WritesToAlt()]
        ids = (str(row.pk), "--queue", "default")

        assert run_json(command, *ids, "--dry-run") == {
            "eligible": 1,
            "skipped": 0,
            "dry_run": True,
        }
        assert run_json(command, *ids) == {"changed": 1, "skipped": 0}
        assert OxTask.objects.using("alt").get(pk=row.pk).status == moved_to
        assert not OxTask.objects.using("default").exists()


@pytest.mark.django_db(transaction=True)
class TestUsageErrorsFromTheCommandLine:
    @pytest.mark.parametrize("command", ["ox_retry", "ox_discard"])
    def test_no_ids_exits_2(self, command, capsys):
        argv = ["manage.py", command, "--skip-checks"]

        with pytest.raises(SystemExit) as info:
            ManagementUtility(argv).execute()

        assert info.value.code == 2
        assert "the following arguments are required: TASK_ID" in (
            capsys.readouterr().err
        )

    @pytest.mark.parametrize("command", ["ox_retry", "ox_discard"])
    def test_a_malformed_id_exits_2_before_any_write(self, command, capsys):
        failed = make_task(OxTask.Status.FAILED)
        argv = ["manage.py", command, str(failed.pk), "not-a-uuid", "--skip-checks"]

        with pytest.raises(SystemExit) as info:
            ManagementUtility(argv).execute()

        assert info.value.code == 2
        err = capsys.readouterr().err
        assert "Invalid task ID 'not-a-uuid'; must be a valid UUID." in err
        assert "Traceback" not in err
        failed.refresh_from_db()
        assert failed.status == OxTask.Status.FAILED
