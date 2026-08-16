from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.utils import timezone

from django_ox.management.commands.ox_prune import parse_duration
from django_ox.models import OxScheduleTick, OxTask


def make_task(status, *, finished_days_ago=None):
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        enqueued_at=now - timedelta(days=400),
        status=status,
        finished_at=(
            now - timedelta(days=finished_days_ago)
            if finished_days_ago is not None
            else None
        ),
    )


def make_tick(name, *, scheduled_days_ago):
    when = timezone.now() - timedelta(days=scheduled_days_ago)
    return OxScheduleTick.objects.create(
        schedule_name=name, scheduled_for=when, created_at=when
    )


def prune(*args):
    out = StringIO()
    call_command("ox_prune", *args, stdout=out)
    return out.getvalue()


class TestParseDuration:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("7d", timedelta(days=7)),
            ("24h", timedelta(hours=24)),
            ("90m", timedelta(minutes=90)),
            ("45s", timedelta(seconds=45)),
            ("3600", timedelta(seconds=3600)),
            (" 7d ", timedelta(days=7)),
        ],
    )
    def test_accepted_forms(self, value, expected):
        assert parse_duration(value) == expected

    @pytest.mark.parametrize("value", ["banana", "1.5d", "-5", "", "7w", "d7", "7 d"])
    def test_rejects_garbage(self, value):
        with pytest.raises(CommandError):
            parse_duration(value)


@pytest.mark.django_db
class TestPrune:
    def test_prunes_old_successful_only_by_default(self):
        old_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        old_failed = make_task(OxTask.Status.FAILED, finished_days_ago=8)
        young_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=1)

        out = prune()

        assert not OxTask.objects.filter(pk=old_ok.pk).exists()
        assert OxTask.objects.filter(pk=old_failed.pk).exists()
        assert OxTask.objects.filter(pk=young_ok.pk).exists()
        assert "Deleted 1 SUCCESSFUL task row(s)" in out

    def test_include_failed_prunes_failed_too(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.FAILED, finished_days_ago=8)
        young_failed = make_task(OxTask.Status.FAILED, finished_days_ago=1)

        out = prune("--include-failed")

        assert set(OxTask.objects.values_list("pk", flat=True)) == {young_failed.pk}
        assert "Deleted 2 SUCCESSFUL/FAILED task row(s)" in out

    def test_ready_and_running_never_pruned(self):
        # finished_at is forced to an ancient date to prove the status
        # filter alone protects non-terminal rows, not just null-ness.
        make_task(OxTask.Status.READY, finished_days_ago=399)
        make_task(OxTask.Status.RUNNING, finished_days_ago=399)

        out = prune("--older-than=1s", "--include-failed")

        assert OxTask.objects.count() == 2
        assert "Deleted 0" in out

    def test_older_than_cutoff_boundary(self):
        newer = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=0)
        OxTask.objects.filter(pk=newer.pk).update(
            finished_at=timezone.now() - timedelta(hours=23)
        )
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=2)

        prune("--older-than=24h")

        assert set(OxTask.objects.values_list("pk", flat=True)) == {newer.pk}

    def test_dry_run_deletes_nothing(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)

        out = prune("--dry-run")

        assert "Would delete 2" in out
        assert OxTask.objects.count() == 2

    def test_deletes_in_batches(self, django_assert_num_queries):
        for _ in range(5):
            make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)

        # Each task batch (2+2+1 rows over three batches) is a pk SELECT
        # plus the deletion collector's row SELECT, tick SET NULL, and
        # DELETE, then the empty terminating SELECT; the tick pass adds its
        # schedule-name SELECT and empty pk SELECT. A single-statement
        # delete would collapse the twelve batch queries into four.
        with django_assert_num_queries(15):
            out = prune("--batch-size=2")

        assert "Deleted 5" in out
        assert OxTask.objects.count() == 0

    def test_prunes_old_ticks_but_keeps_each_schedules_latest(self):
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=20)
        latest_a = make_tick("a", scheduled_days_ago=10)
        # A rarely firing schedule keeps its only tick however old it is:
        # it is the anchor the dispatcher measures missed ticks against.
        latest_b = make_tick("b", scheduled_days_ago=40)

        out = prune()

        assert set(OxScheduleTick.objects.values_list("pk", flat=True)) == {
            latest_a.pk,
            latest_b.pk,
        }
        assert "Deleted 2 schedule tick row(s)" in out

    def test_young_ticks_are_kept(self):
        make_tick("a", scheduled_days_ago=1)
        make_tick("a", scheduled_days_ago=2)

        out = prune()

        assert OxScheduleTick.objects.count() == 2
        assert "Deleted 0 schedule tick row(s)" in out

    def test_dry_run_reports_ticks_without_deleting(self):
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=10)

        out = prune("--dry-run")

        assert "Would delete 1 schedule tick row(s)" in out
        assert OxScheduleTick.objects.count() == 2

    def test_rejects_garbage_duration(self):
        with pytest.raises(CommandError):
            call_command("ox_prune", "--older-than=fortnight")

    def test_rejects_non_positive_batch_size(self):
        with pytest.raises(CommandError):
            call_command("ox_prune", "--batch-size=0")
