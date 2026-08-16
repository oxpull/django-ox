from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError
from django.utils import timezone

from django_ox.management.commands import ox_health
from django_ox.models import OxTask


def make_ready(*, queue="default", seconds_ago=0.0, run_after_seconds=None):
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        queue_name=queue,
        status=OxTask.Status.READY,
        enqueued_at=now - timedelta(seconds=seconds_ago),
        run_after=(
            now + timedelta(seconds=run_after_seconds)
            if run_after_seconds is not None
            else None
        ),
    )


def make_claimed(*, seconds_ago):
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        status=OxTask.Status.SUCCESSFUL,
        enqueued_at=now - timedelta(seconds=seconds_ago),
        last_attempted_at=now - timedelta(seconds=seconds_ago),
        finished_at=now - timedelta(seconds=seconds_ago),
    )


def health(*args):
    out = StringIO()
    call_command("ox_health", *args, stdout=out)
    return out.getvalue()


@pytest.mark.django_db
class TestHealth:
    def test_ok_with_no_flags_on_empty_database(self):
        out = health()
        assert out.startswith("OK: backlog=0 oldest_age=none last_claim_age=none")

    def test_database_unreachable_fails_with_reason(self, monkeypatch):
        def boom(queue_name=None):
            raise DatabaseError("connection refused")

        monkeypatch.setattr(ox_health.stats, "ready_count", boom)
        with pytest.raises(CommandError, match="Database unreachable"):
            health()

    def test_backlog_within_threshold_passes(self):
        make_ready()
        make_ready()
        out = health("--max-backlog=2")
        assert "backlog=2" in out

    def test_backlog_over_threshold_fails(self):
        make_ready()
        make_ready()
        with pytest.raises(CommandError, match="backlog is 2, over --max-backlog 1"):
            health("--max-backlog=1")

    def test_deferred_tasks_do_not_count_as_backlog(self):
        make_ready(run_after_seconds=3600)
        health("--max-backlog=0")

    def test_oldest_age_within_threshold_passes(self):
        make_ready(seconds_ago=120)
        health("--max-age=300")

    def test_oldest_age_over_threshold_fails(self):
        make_ready(seconds_ago=120)
        with pytest.raises(CommandError, match="over --max-age 60s"):
            health("--max-age=60")

    def test_max_age_passes_when_nothing_waits(self):
        health("--max-age=1")

    def test_worker_timeout_passes_on_recent_claim(self):
        make_claimed(seconds_ago=10)
        health("--worker-timeout=60")

    def test_worker_timeout_fails_on_stale_claim(self):
        make_claimed(seconds_ago=120)
        with pytest.raises(CommandError, match="over --worker-timeout 60s"):
            health("--worker-timeout=60")

    def test_worker_timeout_fails_when_nothing_ever_claimed(self):
        make_ready()
        with pytest.raises(CommandError, match="no task claim recorded"):
            health("--worker-timeout=60")

    def test_queue_flag_scopes_the_checks(self):
        make_ready(queue="emails")
        health("--queue=default", "--max-backlog=0")
        with pytest.raises(CommandError, match="backlog is 1"):
            health("--queue=emails", "--max-backlog=0")

    def test_multiple_failures_report_on_one_line(self):
        make_ready(seconds_ago=120)
        with pytest.raises(CommandError) as excinfo:
            health("--max-backlog=0", "--max-age=60")
        message = str(excinfo.value)
        assert "backlog is 1" in message
        assert "oldest waiting task" in message
        assert "\n" not in message

    @pytest.mark.parametrize(
        "flag",
        ["--max-backlog=-1", "--max-age=0", "--worker-timeout=-5"],
    )
    def test_rejects_bad_thresholds(self, flag):
        with pytest.raises(CommandError, match="must be"):
            health(flag)
