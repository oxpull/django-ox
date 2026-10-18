import argparse
import json
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command, execute_from_command_line
from django.core.management.base import CommandError
from django.db import DatabaseError
from django.utils import timezone

from django_ox.durations import parse_seconds
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


def health_json(*args):
    out = StringIO()
    error = None
    try:
        call_command("ox_health", "--format", "json", *args, stdout=out)
    except CommandError as exc:
        error = exc
    return json.loads(out.getvalue()), error


@pytest.mark.django_db
class TestHealth:
    def test_ok_with_no_flags_on_empty_database(self):
        out = health()
        assert out.startswith("OK: backlog=0 oldest_age=none last_claim_age=none")

    def test_json_ok_reports_the_same_figures(self):
        make_ready(seconds_ago=30)
        make_claimed(seconds_ago=60)

        report, error = health_json("--max-backlog=5")

        assert error is None
        assert report["ok"] is True
        assert report["queue"] is None
        assert report["backlog"] == 1
        assert report["oldest_age_seconds"] == pytest.approx(30, abs=5)
        assert report["last_claim_age_seconds"] == pytest.approx(60, abs=5)
        assert report["problems"] == []

    def test_json_on_empty_database_uses_null_ages(self):
        report, error = health_json("--queue", "emails")

        assert error is None
        assert report == {
            "ok": True,
            "queue": "emails",
            "backlog": 0,
            "oldest_age_seconds": None,
            "last_claim_age_seconds": None,
            "unknown_task_paths": None,
            "problems": [],
        }

    def test_json_failure_prints_the_object_and_exits_non_zero(self):
        make_ready()
        make_ready()

        report, error = health_json("--max-backlog=1", "--worker-timeout=60")

        assert isinstance(error, CommandError)
        assert report["ok"] is False
        assert report["backlog"] == 2
        assert report["problems"] == [
            "backlog is 2, over --max-backlog 1",
            "no task claim recorded (--worker-timeout 60s)",
        ]
        assert str(error) == "; ".join(report["problems"])

    def test_json_database_unreachable_still_prints_the_object(self, monkeypatch):
        def boom(queue_name=None, using=None):
            raise DatabaseError("connection refused")

        monkeypatch.setattr(ox_health.stats, "ready_count", boom)
        report, error = health_json()

        assert isinstance(error, CommandError)
        assert report["ok"] is False
        assert report["backlog"] is None
        assert report["problems"] == ["Database unreachable: connection refused"]

    def test_json_rejects_a_bad_threshold_but_still_prints_the_object(self):
        report, error = health_json("--max-backlog=-1")

        assert isinstance(error, CommandError)
        assert report == {
            "ok": False,
            "queue": None,
            "backlog": None,
            "oldest_age_seconds": None,
            "last_claim_age_seconds": None,
            "unknown_task_paths": None,
            "problems": ["--max-backlog must be zero or a positive integer."],
        }
        assert str(error) == "--max-backlog must be zero or a positive integer."

    def test_database_unreachable_fails_with_reason(self, monkeypatch):
        def boom(queue_name=None, using=None):
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

    def test_many_waiting_rows_fail_neither_backlog_nor_age(self):
        an_hour_ago = timezone.now() - timedelta(hours=1)
        OxTask.objects.bulk_create(
            [
                OxTask(
                    task_path="tests.tasks.add",
                    backend_name="default",
                    status=OxTask.Status.WAITING,
                    enqueued_at=an_hour_ago,
                )
                for _ in range(5000)
            ],
            batch_size=1000,
        )
        out = health("--max-backlog=0", "--max-age=1")
        assert out.startswith("OK: backlog=0 oldest_age=none"), out

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
        assert "oldest ready task" in message
        assert "\n" not in message

    def test_max_age_accepts_the_duration_forms_prune_accepts(self):
        make_ready(seconds_ago=120)
        with pytest.raises(CommandError, match="over --max-age 60s"):
            health("--max-age=1m")

    def test_worker_timeout_accepts_the_duration_forms_prune_accepts(self):
        make_claimed(seconds_ago=7200)
        with pytest.raises(CommandError, match="over --worker-timeout 1800s"):
            health("--worker-timeout=30m")

    def test_max_age_still_accepts_a_fractional_number_of_seconds(self):
        make_ready(seconds_ago=120)
        with pytest.raises(CommandError, match=r"over --max-age 60\.5s"):
            health("--max-age=60.5")

    def test_worker_timeout_still_accepts_a_fractional_number_of_seconds(self):
        make_claimed(seconds_ago=10)
        health("--worker-timeout=60.5")

    def test_a_duration_with_no_meaning_is_rejected_with_the_duration_message(self):
        with pytest.raises(
            CommandError, match="argument --max-age: invalid duration 'banana'"
        ):
            health("--max-age=banana")

    def test_a_bad_duration_on_the_command_line_is_a_usage_error(self, capsys):
        with pytest.raises(SystemExit) as exit_info:
            ox_health.Command().run_from_argv(
                ["manage.py", "ox_health", "--max-age=banana"]
            )
        assert exit_info.value.code == 2
        err = capsys.readouterr().err
        assert "argument --max-age: invalid duration 'banana'" in err
        assert "Traceback" not in err

    def test_a_large_plain_number_of_seconds_still_parses(self):
        make_claimed(seconds_ago=10)
        health("--worker-timeout=100000000000000")

    @pytest.mark.parametrize(
        "flag",
        ["--max-backlog=-1", "--max-age=0", "--worker-timeout=-5"],
    )
    def test_rejects_bad_thresholds(self, flag):
        with pytest.raises(CommandError, match="must be"):
            health(flag)

    @pytest.mark.parametrize(
        "flag",
        [
            "--max-age=nan",
            "--max-age=inf",
            "--max-age=1e400",
            "--worker-timeout=nan",
            "--worker-timeout=inf",
        ],
    )
    def test_rejects_a_threshold_no_figure_can_exceed(self, flag, capsys):
        """
        A task two hours old exceeds any real --max-age. It exceeds no nan
        and no inf, so the age check could not fail while the flag took
        them. --worker-timeout took them the same way.
        """
        make_ready(seconds_ago=7200)
        with pytest.raises(SystemExit) as exit_info:
            execute_from_command_line(["manage.py", "ox_health", flag])
        assert exit_info.value.code == 2
        err = capsys.readouterr().err
        assert "invalid duration" in err
        assert "Traceback" not in err


class TestParseSeconds:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("7d", 604800.0),
            ("24h", 86400.0),
            ("90m", 5400.0),
            ("45s", 45.0),
            ("3600", 3600.0),
            (" 7d ", 604800.0),
            ("120.5", 120.5),
            ("0.001", 0.001),
            ("1e3", 1000.0),
            ("-5", -5.0),
            ("100000000000000", 1e14),
        ],
    )
    def test_accepted_forms(self, value, expected):
        assert parse_seconds(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "banana",
            "",
            "7w",
            "d7",
            "7 d",
            "1.5d",
            pytest.param("9" * 400 + "d", id="overflow"),
            # float() takes all of these, and a threshold set to one of them
            # can never be exceeded.
            "nan",
            "NaN",
            "inf",
            "-inf",
            "Infinity",
            "1e400",
        ],
    )
    def test_rejects_garbage(self, value):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_seconds(value)


def make_row(
    task_path, *, status=OxTask.Status.READY, queue="default", using="default"
):
    return OxTask.objects.using(using).create(
        task_path=task_path,
        backend_name="default",
        queue_name=queue,
        status=status,
        enqueued_at=timezone.now(),
    )


class _ToAlt:
    """Sends django_ox's writes to the alt database."""

    def db_for_read(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None


@pytest.mark.django_db(databases=["default", "alt"])
class TestTaskPaths:
    def test_a_queued_row_that_resolves_to_a_task_passes(self):
        make_row("tests.tasks.add")

        report, error = health_json("--task-paths")

        assert error is None
        assert report["ok"] is True
        assert report["unknown_task_paths"] == []
        assert health("--task-paths").startswith("OK: ")
        assert "task_paths=1" in health("--task-paths")

    def test_an_unimportable_path_fails_with_its_row_count(self):
        make_row("tests.no_such_module.gone")
        make_row("tests.no_such_module.gone")
        make_row("tests.tasks.add")

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["ok"] is False
        assert report["unknown_task_paths"] == [
            {
                "task_path": "tests.no_such_module.gone",
                "rows": 2,
                "error": "ModuleNotFoundError",
            }
        ]
        assert report["problems"] == [
            (
                "task path 'tests.no_such_module.gone' does not resolve to a task "
                "(2 queued rows, ModuleNotFoundError)"
            )
        ]
        assert str(error) == report["problems"][0]

    def test_a_path_to_a_plain_function_fails_as_the_worker_would(self):
        make_row("tests.tasks._busy")

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["unknown_task_paths"] == [
            {"task_path": "tests.tasks._busy", "rows": 1, "error": "ImportError"}
        ]
        assert report["problems"] == [
            (
                "task path 'tests.tasks._busy' does not resolve to a task "
                "(1 queued row, ImportError)"
            )
        ]

    def test_a_module_that_exits_at_import_is_reported_not_obeyed(self):
        make_row("tests.exits_on_import.anything")

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["unknown_task_paths"] == [
            {
                "task_path": "tests.exits_on_import.anything",
                "rows": 1,
                "error": "SystemExit",
            }
        ]

    def test_an_interrupt_during_an_import_is_not_swallowed(self):
        make_row("tests.interrupts_on_import.anything")

        with pytest.raises(KeyboardInterrupt):
            health_json("--task-paths")

    def test_unknown_paths_are_sorted_and_counted_across_statuses(self):
        make_row("tests.gone.second")
        make_row("tests.gone.second", status=OxTask.Status.WAITING)
        make_row("tests.gone.first")

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["unknown_task_paths"] == [
            {
                "task_path": "tests.gone.first",
                "rows": 1,
                "error": "ModuleNotFoundError",
            },
            {
                "task_path": "tests.gone.second",
                "rows": 2,
                "error": "ModuleNotFoundError",
            },
        ]
        assert report["problems"][0].startswith("task path 'tests.gone.first'")
        assert report["problems"][1].startswith("task path 'tests.gone.second'")

    def test_database_error_in_the_path_read_is_reported_as_unreachable(
        self, monkeypatch
    ):
        def boom(queue, alias):
            raise DatabaseError("connection refused")

        monkeypatch.setattr(ox_health, "_pending_task_paths", boom)

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["unknown_task_paths"] is None
        assert report["problems"] == ["Database unreachable: connection refused"]

    def test_an_error_other_than_import_error_is_reported_by_class(self, capsys):
        make_row("tests.raises_on_import.anything")

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert report["unknown_task_paths"] == [
            {
                "task_path": "tests.raises_on_import.anything",
                "rows": 1,
                "error": "TypeError",
            }
        ]
        out = StringIO()
        with pytest.raises(CommandError):
            call_command("ox_health", "--task-paths", stdout=out)
        assert "Traceback" not in out.getvalue()
        assert "Traceback" not in capsys.readouterr().out

    def test_only_ready_and_waiting_rows_are_checked(self):
        make_row("tests.gone.waiting", status=OxTask.Status.WAITING)
        for status in (
            OxTask.Status.RUNNING,
            OxTask.Status.SUCCESSFUL,
            OxTask.Status.FAILED,
            OxTask.Status.LOST,
            OxTask.Status.DISCARDED,
        ):
            make_row(f"tests.gone.{status.lower()}", status=status)

        report, _ = health_json("--task-paths")

        assert [u["task_path"] for u in report["unknown_task_paths"]] == [
            "tests.gone.waiting"
        ]

    def test_queue_scopes_the_check(self):
        make_row("tests.gone.emails", queue="emails")

        report, error = health_json("--task-paths", "--queue", "default")
        assert error is None
        assert report["unknown_task_paths"] == []

        report, error = health_json("--task-paths", "--queue", "emails")
        assert isinstance(error, CommandError)
        assert [u["task_path"] for u in report["unknown_task_paths"]] == [
            "tests.gone.emails"
        ]

    def test_database_selects_the_alias_read(self):
        make_row("tests.gone.on_alt", using="alt")

        report, error = health_json("--task-paths")
        assert error is None
        assert report["unknown_task_paths"] == []

        report, error = health_json("--task-paths", "--database", "alt")
        assert isinstance(error, CommandError)
        assert [u["task_path"] for u in report["unknown_task_paths"]] == [
            "tests.gone.on_alt"
        ]

    def test_the_write_alias_is_the_default(self, settings):
        make_row("tests.gone.on_alt", using="alt")
        settings.DATABASE_ROUTERS = [_ToAlt()]

        report, error = health_json("--task-paths")

        assert isinstance(error, CommandError)
        assert [u["task_path"] for u in report["unknown_task_paths"]] == [
            "tests.gone.on_alt"
        ]

    def test_the_read_is_one_query_whatever_the_row_count(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        def task_path_queries(*flags):
            with CaptureQueriesContext(connection) as ctx:
                health_json(*flags)
            return [q["sql"] for q in ctx.captured_queries if "task_path" in q["sql"]]

        make_row("tests.tasks.add")
        one_row = task_path_queries("--task-paths")
        for _ in range(20):
            make_row("tests.tasks.add")
        many_rows = task_path_queries("--task-paths")

        assert len(one_row) == len(many_rows) == 1
        assert "GROUP BY" in many_rows[0]
        assert task_path_queries() == []

    def test_without_the_flag_nothing_is_imported_or_reported(self):
        make_row("tests.gone.never_checked")

        report, error = health_json()

        assert error is None
        assert report["unknown_task_paths"] is None
