"""
Stored values that do not read, through real `manage.py ox_worker`
processes against whichever database the suite runs on.

What is asserted is what a job runner and an operator see: the exit code,
whether the supervisor restarted anything, the committed tick rows and the
tasks they point at, and the log. Every wait is a deadlock guard with a
bound, never a measurement: a batch that exits does so on its own, and one
that does not has failed.
"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import create_schedule

from . import tasks
from .conftest import wait_for
from .isolation import daily_cron_at, twelve_hours_ago, with_history
from .test_stored_isolation import refused_as, refusing_moves_of
from .test_stored_read import (
    ONE_PER_ENGINE,
    insert_tick,
    largest_count,
    only_on,
    set_column,
)
from .test_stored_source_integrity import MISREAD, SQLITE_ONLY
from .test_supervisor import child_pids
from .test_tick_history import EARLIEST, NEWEST

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="worker processes"),
]

#: Every batch here finishes in a second or two, and a long-running worker
#: reaches what a test waits for in a few. This is how long a test waits
#: before calling one stuck.
GUARD = 30

#: Starts every record in a worker's log, followed by its event.
RECORD = "@@ "

STORED = "django_ox.stored.DatabaseScheduleSource"

#: The suite's settings module, for the workers' settings to re-export. Read
#: here, since inside a test that overrides a setting it reads as None.
SUITE_SETTINGS = settings.SETTINGS_MODULE

#: One unreadable newest tick per engine for the --processes runs: the
#: values that ended the process (SQLite, MySQL) or every pass (PostgreSQL).
REPRESENTATIVE = [
    pytest.param("sqlite", "'banana'", id="sqlite-banana"),
    pytest.param("postgresql", "'10000-01-01 00:00:00+00'", id="postgresql-year-10000"),
    pytest.param("mysql", "'9999-00-00 00:00:00'", id="mysql-zero-in-date"),
]


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    # For create_schedule in this process. The workers read the same kind
    # from SCHEDULABLE_TASKS in their settings.
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="labelled", task=tasks.labelled))


def project(tmp_path, schedules, *, source=STORED, report_every=None, use_tz=None):
    """
    A deployment's shape: its own manage.py and settings package, the
    settings re-exporting the suite's so the workers reach the test
    database. `report_every` sets the worker's interval between summaries
    of a failure that goes on, once Django is set up: at 0 every failed
    or skipped pass is a line, so a test can count passes. `use_tz` sets
    USE_TZ for the workers, for a test that has set it for itself.
    """
    root = tmp_path / "proj"
    (root / "tickproj").mkdir(parents=True)
    control = ""
    if report_every is not None:
        control = (
            "import django\n"
            "django.setup()\n"
            "import django_ox.worker\n"
            f"django_ox.worker.DISPATCH_FAILURE_REPORT_INTERVAL = {report_every!r}\n"
        )
    (root / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tickproj.settings')\n"
        f"{control}"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (root / "tickproj" / "__init__.py").write_text("")
    (root / "tickproj" / "settings.py").write_text(
        "import json, sys\n"
        f"sys.path.insert(0, {str(REPO)!r})\n"
        f"from {SUITE_SETTINGS} import *  # noqa: F403\n"
        "TASKS = {'default': {\n"
        "    'BACKEND': 'django_ox.backend.OxBackend',\n"
        "    'QUEUES': ['default', 'emails'],\n"
        "    'OPTIONS': {\n"
        f"        'SCHEDULES': json.loads({json.dumps(schedules)!r}),\n"
        f"        'SCHEDULE_SOURCE': {source!r},\n"
        "        'SCHEDULABLE_TASKS': {'labelled': 'tests.tasks.labelled'},\n"
        "    },\n"
        "}}\n" + ("" if use_tz is None else f"USE_TZ = {use_tz!r}\n")
    )
    return root


def start(root, log_path, *flags, batch=True, env=None):
    environ = dict(os.environ)
    environ["DJANGO_SETTINGS_MODULE"] = "tickproj.settings"
    environ["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    environ["OX_TEST_LOG_LEVEL"] = "INFO"
    environ["OX_TEST_LOG_FORMAT"] = RECORD + "%(event)s %(message)s"
    environ.update(env or {})
    log = log_path.open("wb")
    try:
        # The arguments that are not literals are this test's own flags.
        return subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "manage.py",
                "ox_worker",
                *(["--batch"] if batch else []),
                "--interval",
                "0.05",
                *flags,
            ],
            cwd=root,
            env=environ,
            stdout=log,
            stderr=log,
        )
    finally:
        log.close()


def finish(proc, log_path):
    """The exit code of a process that ended on its own, or a failed test."""
    try:
        return proc.wait(timeout=GUARD)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail(
            f"the worker did not finish within {GUARD}s:\n"
            f"{log_path.read_text()[-4000:]}"
        )


def run_batch(root, log_path, env=None):
    proc = start(root, log_path, env=env)
    return finish(proc, log_path), log_path.read_text()


def stop(proc, log_path):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    return finish(proc, log_path), log_path.read_text()


def lines(log, event):
    return [line for line in log.splitlines() if line.startswith(f"{RECORD}{event} ")]


def counted(log_path, event):
    """The records of `event` a running worker has written so far."""
    return lines(log_path.read_text() if log_path.exists() else "", event)


def entry(name, base):
    return {"task": "tests.tasks.labelled", "cron": daily_cron_at(base), "args": [name]}


def a_row(name, base):
    return create_schedule(
        name=name,
        task_key="labelled",
        trigger="cron",
        cron=daily_cron_at(base),
        arguments={"label": name},
        start_time=timezone.now() - timedelta(days=1),
    )


def every_second():
    """A stored schedule due every second: each tick of it is a dispatch pass."""
    return create_schedule(
        name="every-second",
        task_key="labelled",
        trigger="interval",
        every_seconds=1,
        arguments={"label": "every-second"},
        start_time=timezone.now() - timedelta(days=1),
    )


def label(task):
    return task.args[0] if task.args else task.kwargs.get("label")


def ticks_of(key):
    return sorted(
        OxScheduleTick.objects.filter(schedule_name=key).values_list("pk", flat=True)
    )


def assert_fired_once_each(expected):
    """
    Each key fired exactly one tick, that tick's task carries the schedule's
    own label and ran, and no labelled task exists that no tick points at.
    """
    pointed_at = set()
    for key, name in expected.items():
        (tick,) = OxScheduleTick.objects.filter(schedule_name=key).exclude(task_id=None)
        task = OxTask.objects.get(id=tick.task_id)
        assert label(task) == name, (key, label(task))
        assert task.status == OxTask.Status.SUCCESSFUL, (name, task.status)
        pointed_at.add(task.id)
    labelled = OxTask.objects.filter(task_path="tests.tasks.labelled")
    assert set(labelled.values_list("id", flat=True)) == pointed_at


class TestABatchWithAnUnreadableTick:
    @pytest.mark.parametrize(("vendor", "literal", "why", "use_tz"), NEWEST)
    def test_the_newest_tick_skips_its_schedule_alone(
        self, tmp_path, settings, vendor, literal, why, use_tz
    ):
        only_on(vendor, why)
        # For this process and the worker alike: both write and read the
        # same columns, and a time has a zone in them or it does not.
        settings.USE_TZ = use_tz
        base = twelve_hours_ago()
        root = project(tmp_path, {"s-healthy": entry("s-healthy", base)}, use_tz=use_tz)
        with_history("s-healthy")
        healthy = a_row("r-healthy", base)
        blocked = a_row("r-blocked", base)
        bad = insert_tick(f"db:{blocked.pk}", literal)

        code, log = run_batch(root, tmp_path / "run.log")
        assert code == 0, log
        assert_fired_once_each(
            {"s-healthy": "s-healthy", f"db:{healthy.pk}": "r-healthy"}
        )
        assert ticks_of(f"db:{blocked.pk}") == [bad], "the blocked row fired"
        (skipped, *_) = lines(log, "schedule_tick_unreadable")
        assert "Schedule r-blocked skipped" in skipped, skipped
        assert "r-blocked could not be dispatched" not in log, log
        assert "Schedule dispatch failed" not in log, log
        assert "found nothing to claim" in log

    @pytest.mark.parametrize(("vendor", "literal", "why"), EARLIEST)
    def test_the_earliest_tick_blocks_its_settings_schedule_alone(
        self, tmp_path, vendor, literal, why
    ):
        only_on(vendor, why)
        base = twelve_hours_ago()
        root = project(
            tmp_path,
            {
                "s-anchor": entry("s-anchor", base),
                "s-healthy": entry("s-healthy", base),
            },
        )
        with_history("s-healthy")
        healthy = a_row("r-healthy", base)
        bad = insert_tick("s-anchor", literal)

        code, log = run_batch(root, tmp_path / "run.log")
        assert code == 0, log
        assert_fired_once_each(
            {"s-healthy": "s-healthy", f"db:{healthy.pk}": "r-healthy"}
        )
        # Not anchored again, and no candidate tick or latch left behind.
        assert ticks_of("s-anchor") == [bad]
        (skipped, *_) = lines(log, "schedule_tick_unreadable")
        assert "Schedule s-anchor skipped" in skipped, skipped
        assert "Its earliest tick could not be read" in skipped, skipped
        assert "s-anchor could not be dispatched" not in log, log
        assert "Schedule dispatch failed" not in log, log


class TestABatchWithARowTheConverterWouldMisread:
    @pytest.mark.parametrize(("vendor", "column", "literal"), MISREAD)
    def test_the_row_is_skipped_by_field_and_never_dispatched(
        self, tmp_path, vendor, column, literal
    ):
        only_on(vendor, SQLITE_ONLY)
        base = twelve_hours_ago()
        root = project(tmp_path, {})
        healthy = a_row("r-healthy", base)
        victim = a_row("victim", base)
        set_column(victim.pk, column, literal)

        code, log = run_batch(root, tmp_path / "run.log")
        assert code == 0, log
        assert_fired_once_each({f"db:{healthy.pk}": "r-healthy"})
        assert ticks_of(f"db:{victim.pk}") == [], "the row was dispatched"
        (skipped, *_) = lines(log, "schedule_row_skipped")
        said = f"Skipping stored schedule victim (pk {victim.pk}): its {column} holds "
        assert said in skipped, skipped


class TestABatchWithARowThatDoesNotRead:
    def test_the_skip_carries_the_field_as_a_list(self, tmp_path):
        column, literal = ONE_PER_ENGINE[connection.vendor]
        base = twelve_hours_ago()
        root = project(tmp_path, {})
        healthy = a_row("r-healthy", base)
        victim = a_row("victim", base)
        set_column(victim.pk, column, literal)

        code, log = run_batch(
            root,
            tmp_path / "run.log",
            env={"OX_TEST_LOG_FORMAT": RECORD + "%(event)s %(fields)s %(message)s"},
        )
        assert code == 0, log
        assert_fired_once_each({f"db:{healthy.pk}": "r-healthy"})
        assert ticks_of(f"db:{victim.pk}") == [], "the row was dispatched"
        (skipped, *_) = lines(log, "schedule_row_skipped")
        said = (
            f"{RECORD}schedule_row_skipped ['{column}'] Skipping stored schedule "
            f"victim (pk {victim.pk}): its {column} holds "
        )
        assert skipped.startswith(said), skipped


class TestTwoProcessesWithAnUnreadableTick:
    """
    Under --processes 2 a child that a stored value ended would be
    restarted into the same value until the supervisor's restart cap
    stopped it. Both children keep their process: the same two pids before
    and after several dispatch passes, and no restart at all.
    """

    @pytest.mark.parametrize(("vendor", "literal"), REPRESENTATIVE)
    def test_no_child_is_restarted(self, tmp_path, vendor, literal):
        only_on(vendor, f"a value only {vendor} keeps")
        base = twelve_hours_ago()
        root = project(
            tmp_path, {"s-healthy": entry("s-healthy", base)}, report_every=0
        )
        with_history("s-healthy")
        healthy = a_row("r-healthy", base)
        blocked = a_row("r-blocked", base)
        bad = insert_tick(f"db:{blocked.pk}", literal)
        log_path = tmp_path / "worker.log"
        proc = start(root, log_path, "--processes", "2", batch=False)
        try:
            done = OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL)

            def skips():
                return len(counted(log_path, "schedule_tick_unreadable"))

            reached = wait_for(
                lambda: done.count() == 2 and skips() >= 4, timeout=GUARD
            )
            assert reached, log_path.read_text()[-4000:]
            before = sorted(child_pids(proc))
            seen = skips()
            # Both children through more passes, each one a line.
            assert wait_for(lambda: skips() >= seen + 4, timeout=GUARD), (
                log_path.read_text()[-4000:]
            )
            after = sorted(child_pids(proc))
        finally:
            code, log = stop(proc, log_path)
        assert code == 0, log
        assert len(before) == 2, before
        assert before == after, "a child was replaced"
        assert not lines(log, "worker_process_restarted"), log
        assert not lines(log, "supervisor_restart_cap"), log
        assert_fired_once_each(
            {"s-healthy": "s-healthy", f"db:{healthy.pk}": "r-healthy"}
        )
        assert ticks_of(f"db:{blocked.pk}") == [bad]


def failing(error, passes):
    return {"OX_TEST_SOURCE_RAISES": error, "OX_TEST_SOURCE_PASSES": passes}


class TestAFaultInADispatchReader:
    """
    A shared read of the dispatch pass that raises something no handler in
    the pass expects, as a reader added later might: tests.sources'
    FailingReadSource. The pass is lost and reported; the process is not.
    """

    def test_a_batch_claims_its_work_dispatches_and_exits_0(self, tmp_path):
        base = twelve_hours_ago()
        root = project(
            tmp_path,
            {"s-due": entry("s-due", base)},
            source="tests.sources.FailingReadSource",
        )
        with_history("s-due")
        queued = tasks.add.enqueue(1, 2)

        code, log = run_batch(
            root, tmp_path / "run.log", env=failing("AttributeError", "1")
        )
        assert code == 0, log
        assert OxTask.objects.get(id=queued.id).status == OxTask.Status.SUCCESSFUL
        assert_fired_once_each({"s-due": "s-due"})
        (failed,) = lines(log, "schedule_dispatch_failed")
        assert "unexpected exception (AttributeError)" in failed, failed
        assert "Traceback" in log
        assert lines(log, "schedule_dispatch_resumed"), log

    def test_one_on_every_pass_leaves_polling_alive_without_a_busy_loop(self, tmp_path):
        root = project(
            tmp_path,
            {"s-due": entry("s-due", twelve_hours_ago())},
            source="tests.sources.FailingReadSource",
            report_every=0,
        )
        queued = tasks.add.enqueue(1, 2)
        log_path = tmp_path / "worker.log"
        started = time.monotonic()
        proc = start(
            root, log_path, batch=False, env=failing("AttributeError", "every")
        )
        try:
            done = OxTask.objects.filter(id=queued.id, status=OxTask.Status.SUCCESSFUL)
            assert wait_for(
                lambda: (
                    done.exists()
                    and len(counted(log_path, "schedule_dispatch_failed")) >= 3
                ),
                timeout=GUARD,
            ), log_path.read_text()[-4000:]
        finally:
            code, log = stop(proc, log_path)
        elapsed = time.monotonic() - started
        assert code == 0, log
        # Each failed pass is a line here, and a pass comes at most once a
        # second (the default schedule interval): a retry at once would be
        # hundreds in the same time.
        assert len(lines(log, "schedule_dispatch_failed")) <= elapsed + 2, log
        assert log.count("Traceback") == 1, "a traceback per pass"

    def test_one_on_every_pass_holds_a_batch_open_and_its_queue_still_runs(
        self, tmp_path
    ):
        """
        A batch ends once its queue is drained and no dispatch is owed. A
        pass that fails stays owed, so a fault on every pass holds the
        batch open, as a database failing every pass does: it goes on
        claiming what is queued, tries the dispatch again once a schedule
        interval, and stays up until something stops it.
        """
        root = project(
            tmp_path,
            {"s-due": entry("s-due", twelve_hours_ago())},
            source="tests.sources.FailingReadSource",
            report_every=0,
        )
        with_history("s-due")
        first = tasks.add.enqueue(1, 2)
        log_path = tmp_path / "worker.log"

        def failed():
            return len(counted(log_path, "schedule_dispatch_failed"))

        def ran(task):
            done = OxTask.objects.filter(id=task.id, status=OxTask.Status.SUCCESSFUL)
            return done.exists()

        started = time.monotonic()
        proc = start(root, log_path, env=failing("AttributeError", "every"))
        try:
            # The queue is drained, and the dispatch has failed and failed
            # again: a batch that could end has had every chance to.
            assert wait_for(lambda: ran(first) and failed() >= 2, timeout=GUARD), (
                log_path.read_text()[-4000:]
            )
            assert proc.poll() is None, "the batch ended with its dispatch owed"
            # Still claiming: what is queued while it is held open is run.
            seen = failed()
            second = tasks.add.enqueue(3, 4)
            assert wait_for(
                lambda: ran(second) and failed() >= seen + 2, timeout=GUARD
            ), log_path.read_text()[-4000:]
            assert proc.poll() is None, "the batch ended with its dispatch owed"
        finally:
            code, log = stop(proc, log_path)
        elapsed = time.monotonic() - started
        assert code == 0, log
        assert not lines(log, "worker_batch_empty"), log
        # Each failed pass is a line here, and a pass comes at most once a
        # second (the default schedule interval): a retry at once would be
        # hundreds in the same time.
        assert len(lines(log, "schedule_dispatch_failed")) <= elapsed + 2, log
        assert log.count("Traceback") == 1, "a traceback per pass"
        # Nothing was dispatched: the schedule's history is what it was.
        assert not OxScheduleTick.objects.exclude(task_id=None).exists()
        assert not OxTask.objects.filter(task_path="tests.tasks.labelled").exists()

    def test_two_processes_lose_one_pass_each_and_no_child(self, tmp_path):
        base = twelve_hours_ago()
        root = project(
            tmp_path,
            {"s-due": entry("s-due", base)},
            source="tests.sources.FailingReadSource",
        )
        with_history("s-due")
        queued = tasks.add.enqueue(1, 2)
        log_path = tmp_path / "worker.log"
        proc = start(
            root,
            log_path,
            "--processes",
            "2",
            batch=False,
            env=failing("AttributeError", "1"),
        )
        try:
            done = OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL)
            assert wait_for(
                lambda: (
                    done.count() == 2
                    and len(counted(log_path, "schedule_dispatch_resumed")) >= 2
                ),
                timeout=GUARD,
            ), log_path.read_text()[-4000:]
            before = sorted(child_pids(proc))
        finally:
            code, log = stop(proc, log_path)
        assert code == 0, log
        assert len(before) == 2, before
        assert len(lines(log, "schedule_dispatch_failed")) == 2, log
        assert not lines(log, "worker_process_restarted"), log
        assert not lines(log, "supervisor_restart_cap"), log
        assert OxTask.objects.get(id=queued.id).status == OxTask.Status.SUCCESSFUL
        assert_fired_once_each({"s-due": "s-due"})


def processes_of(marker):
    """The pids of every process whose command line holds this marker."""
    found = subprocess.run(  # noqa: S603
        ["pgrep", "-f", str(marker)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    return [int(pid) for pid in found]


class TestACustomSourceWhoseStoreStopsAnswering:
    """
    tests.sources' UnavailableAfterStart answers when the worker is built
    and raises ConnectionError on every read after that. Dispatching
    directly raises it. A `--batch` worker does not end on it: it reports
    the lost pass and stays up, polling, until it is stopped, so a wrapper
    that starts one cannot take a prompt non-zero exit as the sign of a
    source that is down.
    """

    SOURCE = "tests.sources.UnavailableAfterStart"

    def test_direct_dispatch_raises(self, settings):
        from django_ox.worker import Worker

        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULES": {
                        "s-due": {
                            "task": "tests.tasks.labelled",
                            "cron": "* * * * *",
                            "args": ["s-due"],
                        }
                    },
                    "SCHEDULE_SOURCE": self.SOURCE,
                },
            }
        }
        worker = Worker(poll_interval=0.02, reap_interval=0.0, schedule_interval=0.0)

        with pytest.raises(ConnectionError, match="stopped answering"):
            worker.dispatch_schedules()

    def test_a_batch_reports_it_and_stays_alive_until_it_is_stopped(self, tmp_path):
        root = project(
            tmp_path,
            {"s-due": entry("s-due", twelve_hours_ago())},
            source=self.SOURCE,
            report_every=0,
        )
        queued = tasks.add.enqueue(1, 2)
        log_path = tmp_path / "worker.log"
        # A path no other process has, so that every process of this worker
        # can be found by its command line.
        marker = tmp_path / "heartbeat"
        proc = start(root, log_path, "--heartbeat-file", str(marker))
        try:
            done = OxTask.objects.filter(id=queued.id, status=OxTask.Status.SUCCESSFUL)
            assert wait_for(
                lambda: (
                    done.exists()
                    and len(counted(log_path, "schedule_dispatch_failed")) >= 2
                ),
                timeout=GUARD,
            ), log_path.read_text()[-4000:]
            # The queue is drained and the dispatch has failed more than
            # once: a batch that could end has had every chance to.
            assert proc.poll() is None, log_path.read_text()[-4000:]
            assert processes_of(marker), "the worker is not running"
        finally:
            code, log = stop(proc, log_path)
        assert code == 0, log
        (first, *_) = lines(log, "schedule_dispatch_failed")
        assert "unexpected exception (ConnectionError)" in first, first
        assert not lines(log, "worker_batch_empty"), log
        assert not OxScheduleTick.objects.exclude(task_id=None).exists()
        assert not processes_of(marker), "a process of the worker is still running"


class TestAWorkerWhoseDatabaseWillNotMoveABoundary:
    """
    A stored row retimed by SQL, on a database that refuses to let its
    boundary be moved for as long as a trigger on the table stands. The move
    is tried at the start of every dispatch pass, about once a second, and
    refused each time. The worker says so once with the traceback, and after
    that at most once a minute in a line that carries none; the move is
    still tried, and the rest is dispatched.
    """

    def test_it_is_said_in_full_once_and_the_schedule_beside_it_keeps_firing(
        self, tmp_path
    ):
        base = twelve_hours_ago()
        root = project(tmp_path, {})
        beside = every_second()
        victim = a_row("victim", base)
        set_column(victim.pk, "cron", f"'{daily_cron_at(base - timedelta(hours=1))}'")
        log_path = tmp_path / "worker.log"
        fired = OxScheduleTick.objects.filter(schedule_name=f"db:{beside.pk}").exclude(
            task_id=None
        )

        with refusing_moves_of(victim):
            proc = start(root, log_path, batch=False)
            try:
                assert wait_for(
                    lambda: counted(log_path, "schedule_boundary_heal_failed"),
                    timeout=GUARD,
                ), log_path.read_text()[-4000:]
                # Each tick of the schedule beside it is a dispatch pass, and
                # a pass tries the move before it dispatches anything.
                seen = fired.count()
                assert wait_for(lambda: fired.count() >= seen + 3, timeout=GUARD), (
                    log_path.read_text()[-4000:]
                )
            finally:
                code, log = stop(proc, log_path)
        assert code == 0, log
        said = lines(log, "schedule_boundary_heal_failed")
        in_full = f"Could not move schedule {victim.pk} onto its current timing"
        assert [line for line in said if line.endswith(f" {in_full}")] == said[:1], said
        # Any line after the first is the summary a minute on, and the
        # database's error was printed with the first alone.
        assert all(" Still could not move schedule " in line for line in said[1:]), said
        assert log.count(f"django.db.utils.{refused_as().__name__}: ") == 1, log
        assert not lines(log, "schedule_row_skipped"), log
        assert ticks_of(f"db:{victim.pk}") == [], "the row was dispatched"


class TestAWorkerBesideACountOfBoundaryWritesAtItsMaximum:
    """
    A stored row retimed by SQL whose count of boundary writes is the most
    its column can hold. Moving its boundary would add one to the count,
    which PostgreSQL and MySQL refuse and SQLite keeps as a float. So the
    worker tries no move: it names the row once, by that field, leaves it
    out and as it is, and dispatches the rest.
    """

    def test_the_row_is_named_once_and_left_as_it_is(self, tmp_path):
        base = twelve_hours_ago()
        root = project(tmp_path, {})
        beside = every_second()
        victim = a_row("victim", base)
        set_column(victim.pk, "boundary_generation", largest_count())
        set_column(victim.pk, "cron", f"'{daily_cron_at(base - timedelta(hours=1))}'")
        as_sql_left_it = OxSchedule.objects.filter(pk=victim.pk).values().get()
        log_path = tmp_path / "worker.log"
        fired = OxScheduleTick.objects.filter(schedule_name=f"db:{beside.pk}").exclude(
            task_id=None
        )

        proc = start(
            root,
            log_path,
            batch=False,
            env={"OX_TEST_LOG_FORMAT": RECORD + "%(event)s %(fields)s %(message)s"},
        )
        try:
            assert wait_for(
                lambda: counted(log_path, "schedule_row_skipped"), timeout=GUARD
            ), log_path.read_text()[-4000:]
            # Each tick of the schedule beside it is a dispatch pass.
            seen = fired.count()
            assert wait_for(lambda: fired.count() >= seen + 3, timeout=GUARD), (
                log_path.read_text()[-4000:]
            )
        finally:
            code, log = stop(proc, log_path)
        assert code == 0, log
        (skipped,) = lines(log, "schedule_row_skipped")
        assert skipped == (
            f"{RECORD}schedule_row_skipped ['boundary_generation'] Skipping stored "
            f"schedule victim (pk {victim.pk}): its boundary_generation holds "
            f"{largest_count()}, the most its column can hold, so the count cannot "
            "be raised"
        )
        # No move of its boundary was tried, so none failed and none was made.
        assert not lines(log, "schedule_boundary_heal_failed"), log
        assert not lines(log, "schedule_boundary_healed"), log
        assert "Traceback" not in log, log
        assert OxSchedule.objects.filter(pk=victim.pk).values().get() == as_sql_left_it
        if connection.vendor == "sqlite":
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT typeof(boundary_generation) FROM django_ox_oxschedule "
                    "WHERE id = %s",
                    [victim.pk],
                )
                assert cursor.fetchone() == ("integer",)
        assert ticks_of(f"db:{victim.pk}") == [], "the row was dispatched"
