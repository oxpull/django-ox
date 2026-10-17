import json
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import router
from django.utils import timezone

from django_ox.compat import default_task_backend
from django_ox.maintenance import prune
from django_ox.models import OxScheduleTick, OxTask
from django_ox.testing import run_tasks
from django_ox.worker import Worker

from .conftest import start_worker_thread
from .contention import CONTENTION, failing, simulated
from .test_inline_path import ToAlt
from .test_prune import make_old_rows, make_task
from .test_prune_unreadable import insert_tick, only_on
from .test_schedules import backdate_anchor, frozen_now, tasks_setting  # noqa: F401
from .test_supervisor import start_worker, stop_worker, wait_for


@pytest.mark.django_db(transaction=True)
class TestPruneTask:
    def test_prune_task_deletes_rows_past_cutoff(self):
        old_success_pks = make_old_rows(1, OxTask.Status.SUCCESSFUL)
        old_discarded_pks = make_old_rows(1, OxTask.Status.DISCARDED)
        old_failed_pks = make_old_rows(1, OxTask.Status.FAILED)
        old_lost_pks = make_old_rows(1, OxTask.Status.LOST)
        new_success = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=1)

        # Also create a tick row
        old_tick_time = timezone.now() - timedelta(days=10)
        new_tick_time = timezone.now() - timedelta(days=1)
        OxScheduleTick.objects.create(
            schedule_name="test-sched",
            scheduled_for=old_tick_time,
            created_at=old_tick_time,
        )
        OxScheduleTick.objects.create(
            schedule_name="test-sched",
            scheduled_for=new_tick_time,
            created_at=new_tick_time,
        )

        res_handle = prune.enqueue(older_than="7d")
        results = run_tasks()
        assert len(results) == 1
        res_handle.refresh()

        result = res_handle.return_value
        assert result["dry_run"] is False
        assert result["task_rows"] == 2  # old_success and old_discarded
        assert result["tick_rows"] == 1  # older tick deleted, newer kept as anchor
        assert result["statuses"] == ["SUCCESSFUL", "DISCARDED"]

        # Rows past cutoff deleted
        assert not OxTask.objects.filter(
            pk__in=old_success_pks + old_discarded_pks
        ).exists()
        # FAILED and LOST rows remain unless include_failed=True
        assert OxTask.objects.filter(pk__in=old_failed_pks).exists()
        assert OxTask.objects.filter(pk__in=old_lost_pks).exists()
        # New rows remain
        assert OxTask.objects.filter(pk=new_success.pk).exists()

    def test_include_failed_deletes_failed_and_lost(self):
        old_failed_pks = make_old_rows(1, OxTask.Status.FAILED)
        old_lost_pks = make_old_rows(1, OxTask.Status.LOST)

        res_handle = prune.enqueue(older_than="7d", include_failed=True)
        run_tasks()
        res_handle.refresh()

        result = res_handle.return_value
        assert result["task_rows"] == 2
        assert result["statuses"] == ["SUCCESSFUL", "DISCARDED", "FAILED", "LOST"]
        assert not OxTask.objects.filter(pk__in=old_failed_pks + old_lost_pks).exists()

    def test_prune_task_does_not_delete_ready_waiting_running_or_own_row(self):
        ready_task = make_task(OxTask.Status.READY)
        waiting_task = make_task(OxTask.Status.WAITING)
        running_task = make_task(OxTask.Status.RUNNING)

        res_handle = prune.enqueue(older_than="0s", include_failed=True)
        run_tasks()
        res_handle.refresh()

        # Check ready, waiting, running still exist
        assert OxTask.objects.filter(pk=ready_task.pk).exists()
        assert OxTask.objects.filter(pk=waiting_task.pk).exists()
        assert OxTask.objects.filter(pk=running_task.pk).exists()

        # Check prune task's own row exists and is SUCCESSFUL
        prune_row = OxTask.objects.get(id=res_handle.id)
        assert prune_row.status == OxTask.Status.SUCCESSFUL

    def test_result_structure_matches_json_output(self):
        make_old_rows(1, OxTask.Status.SUCCESSFUL)

        out = StringIO()
        call_command("ox_prune", "--format", "json", "--older-than", "7d", stdout=out)
        expected_json = json.loads(out.getvalue())

        make_old_rows(1, OxTask.Status.SUCCESSFUL)
        res_handle = prune.enqueue(older_than="7d")
        run_tasks()
        res_handle.refresh()
        task_result = res_handle.return_value

        # Keys and shapes must match
        assert set(task_result.keys()) == set(expected_json.keys())
        assert task_result["statuses"] == expected_json["statuses"]
        assert task_result["dry_run"] == expected_json["dry_run"]
        assert task_result["task_rows"] == 1

    def test_dry_run_returns_counts_without_deleting_rows(self):
        pks = make_old_rows(1, OxTask.Status.SUCCESSFUL)
        res_handle = prune.enqueue(older_than="7d", dry_run=True)
        run_tasks()
        res_handle.refresh()

        result = res_handle.return_value
        assert result["dry_run"] is True
        assert result["task_rows"] == 1
        assert OxTask.objects.filter(pk__in=pks).exists()

    def test_invalid_older_than_fails_attempt_with_duration_message(self):
        res_handle = prune.enqueue(older_than="invalid_duration")
        run_tasks()
        res_handle.refresh()

        # The attempt fails and error is captured
        assert len(res_handle.errors) >= 1
        error = res_handle.errors[0]
        assert "Invalid duration 'invalid_duration'" in error.traceback
        msg = "use forms like 7d, 24h, 90m, 45s, or a plain number of seconds"
        assert msg in error.traceback

    def test_manage_py_check_validates_schedule_entry(self, settings):
        settings.TASKS["default"]["OPTIONS"]["SCHEDULES"] = {
            "ox-prune": {
                "task": "django_ox.maintenance.prune",
                "cron": "0 3 * * *",
                "kwargs": {"older_than": "7d"},
            }
        }
        errors = default_task_backend.check()
        assert [e.id for e in errors if e.id == "django_ox.E002"] == []

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_contention_failure_fails_task_without_partial_success(
        self, kind, monkeypatch
    ):
        make_old_rows(5, OxTask.Status.FAILED)

        delete_sql = "DELETE FROM DJANGO_OX_OXTASK"
        pauses = []
        monkeypatch.setattr("django_ox._contention.pause", pauses.append)

        with failing(
            delete_sql,
            lambda: simulated(kind),
            lambda n: n >= 2,
        ):
            res_handle = prune.enqueue(
                older_than="7d", include_failed=True, batch_size=2
            )
            run_tasks()
            res_handle.refresh()

            # Task attempt must fail and not report a successful return_value
            assert res_handle.status != OxTask.Status.SUCCESSFUL
            assert res_handle._return_value is None
            assert len(res_handle.errors) >= 1
            expected = "The next batch hit a database deadlock or serialization failure"
            assert expected in res_handle.errors[0].traceback

    def test_alias_coverage_prunes_db_for_write(self, monkeypatch):
        # OxTask default write database
        alias = router.db_for_write(OxTask)
        pks = make_old_rows(1, OxTask.Status.SUCCESSFUL)
        assert OxTask.objects.using(alias).filter(pk__in=pks).exists()

        res_handle = prune.enqueue(older_than="7d")
        run_tasks()
        res_handle.refresh()

        assert res_handle.return_value["task_rows"] == 1
        assert not OxTask.objects.using(alias).filter(pk__in=pks).exists()

    def test_stdout_does_not_leak_into_worker_logs(self, capsys):
        make_old_rows(1, OxTask.Status.SUCCESSFUL)
        prune.enqueue(older_than="7d")
        run_tasks()
        captured = capsys.readouterr()
        # JSON output or text output from ox_prune should not leak into stdout/stderr
        assert "task_rows" not in captured.out
        assert "Deleted" not in captured.out

    def test_worker_scheduled_execution(self, tmp_path):
        make_old_rows(1, OxTask.Status.SUCCESSFUL)
        res_handle = prune.enqueue(older_than="7d")
        proc = start_worker(tmp_path, "--processes", "1", "--interval", "0.05")
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=res_handle.id).status
                    == OxTask.Status.SUCCESSFUL
                ),
                timeout=20,
            )
        finally:
            code, _log = stop_worker(proc, tmp_path)

        assert code == 0
        assert OxTask.objects.get(id=res_handle.id).return_value["task_rows"] == 1


PRUNE_SCHEDULE = {
    "ox-prune": {
        "task": "django_ox.maintenance.prune",
        "cron": "* * * * *",
        "kwargs": {"older_than": "7d"},
    }
}


@pytest.mark.django_db(transaction=True)
class TestPruneTaskWiring:
    def test_queue_limits_the_prune_to_that_queue(self):
        emails = make_old_rows(2, OxTask.Status.SUCCESSFUL, queue_name="emails")
        others = make_old_rows(3, OxTask.Status.SUCCESSFUL, queue_name="reports")

        res_handle = prune.enqueue(older_than="7d", queue="emails")
        run_tasks()
        res_handle.refresh()

        assert res_handle.return_value["task_rows"] == 2
        assert not OxTask.objects.filter(pk__in=emails).exists()
        assert OxTask.objects.filter(pk__in=others).count() == 3

    def test_a_schedule_entry_fires_the_prune_through_a_worker(
        self,
        settings,
        frozen_now,  # noqa: F811
    ):
        settings.TASKS = tasks_setting(PRUNE_SCHEDULE)
        old = make_old_rows(2, OxTask.Status.SUCCESSFUL)
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: OxScheduleTick.objects.filter(schedule_name="ox-prune").exists()
            )
            backdate_anchor("ox-prune", 1)
            assert wait_for(
                lambda: OxTask.objects.filter(
                    task_path="django_ox.maintenance.prune",
                    status=OxTask.Status.SUCCESSFUL,
                ).exists()
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)
        assert not thread.is_alive()

        row = OxTask.objects.get(task_path="django_ox.maintenance.prune")
        assert row.return_value["task_rows"] == 2
        assert not OxTask.objects.filter(pk__in=old).exists()

    def test_a_bad_cron_entry_is_reported_as_e002(self, settings):
        settings.TASKS = tasks_setting(
            {"ox-prune": {**PRUNE_SCHEDULE["ox-prune"], "cron": "banana"}}
        )
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E002"]

    def test_the_unreadable_tick_warning_stays_off_worker_stderr(self, capsys):
        only_on("sqlite")
        bad = insert_tick("k", "'banana'")

        res_handle = prune.enqueue(older_than="7d")
        run_tasks()
        res_handle.refresh()

        err = capsys.readouterr().err
        assert "Warning: this run encountered" not in err
        assert "--purge-unreadable-ticks" not in err
        assert res_handle.return_value["unreadable_tick_rows"] == 1
        assert OxScheduleTick.objects.filter(pk=bad).exists()

    @pytest.mark.django_db(transaction=True, databases=["default", "alt"])
    def test_the_prune_follows_the_router_write_alias(self, settings):
        on_default = make_old_rows(2, OxTask.Status.SUCCESSFUL)
        settings.DATABASE_ROUTERS = [ToAlt()]
        assert router.db_for_write(OxTask) == "alt"
        old = make_old_rows(2, OxTask.Status.SUCCESSFUL)
        assert OxTask.objects.using("alt").filter(pk__in=old).count() == 2

        res_handle = prune.enqueue(older_than="7d")
        run_tasks()
        res_handle.refresh()

        assert res_handle.return_value["task_rows"] == 2
        assert not OxTask.objects.using("alt").filter(pk__in=old).exists()
        assert OxTask.objects.using("default").filter(pk__in=on_default).count() == 2
