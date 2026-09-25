"""
Schedule isolation through real `manage.py ox_worker` processes, `--batch`
ones and a long-running one, against whichever database the suite runs on.

The database refuses for real (tests/isolation.py). What is asserted is
what a job runner and an operator see: the exit code, the committed tick
rows and the tasks they point at, and the log. Every wait is a deadlock
guard with a bound, never a measurement: a batch that exits does so on its
own, and one that does not has failed.
"""

import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    _touch_change_row,
    create_schedule,
    delete_schedule,
    update_schedule,
)

from . import tasks
from .conftest import wait_for
from .forms import FilterArgs
from .isolation import (
    REFUSED,
    REFUSED_TEXT,
    as_stored,
    daily_cron_at,
    twelve_hours_ago,
    with_history,
)

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="worker processes"),
]

#: Every batch here finishes in a second or two. This is how long a test
#: waits before calling one stuck: the failure these tests exist for was a
#: batch that never finished.
GUARD = 30

KIND = "iso.filtered"


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    # For create_schedule in this process. The workers read the same kind
    # from SCHEDULABLE_TASKS in their settings.
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key=KIND, task=tasks.labelled, form=FilterArgs))


def project(tmp_path, schedules, *, stored=False, report_every=None):
    """
    A deployment's shape: its own manage.py and settings package, the
    settings re-exporting the suite's so the workers reach the test
    database. SCHEDULES go through json.loads, which reads the bare token
    Infinity back as the float a settings file would hold.

    `report_every` is a test-only control: the worker's interval between
    summaries of a schedule that goes on failing, set in manage.py once
    Django is set up. At 0 every failed attempt is a line, so a test can
    count them.
    """
    root = tmp_path / "proj"
    (root / "isoproj").mkdir(parents=True)
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
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'isoproj.settings')\n"
        f"{control}"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (root / "isoproj" / "__init__.py").write_text("")
    options = [f"'SCHEDULES': json.loads({json.dumps(schedules)!r})"]
    if stored:
        options.append("'SCHEDULE_SOURCE': 'django_ox.stored.DatabaseScheduleSource'")
        options.append(
            "'SCHEDULABLE_TASKS': {"
            f"{KIND!r}: {{'task': 'tests.tasks.labelled', "
            "'form': 'tests.forms.FilterArgs'}}"
        )
    (root / "isoproj" / "settings.py").write_text(
        "import json, sys\n"
        f"sys.path.insert(0, {str(REPO)!r})\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = {'default': {\n"
        "    'BACKEND': 'django_ox.backend.OxBackend',\n"
        # tests.tasks declares a task on the emails queue, and importing
        # it checks that queue against this backend.
        "    'QUEUES': ['default', 'emails'],\n"
        f"    'OPTIONS': {{{', '.join(options)}}},\n"
        "}}\n"
    )
    return root


def start(root, log_path, *, batch=True, log_format=None):
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "isoproj.settings"
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    if log_format is not None:
        env["OX_TEST_LOG_FORMAT"] = log_format
    log = log_path.open("wb")
    try:
        # The one argument that is not a literal is this test's own flag.
        return subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "manage.py",
                "ox_worker",
                *(["--batch"] if batch else []),
                "--interval",
                "0.05",
            ],
            cwd=root,
            env=env,
            stdout=log,
            stderr=log,
        )
    finally:
        log.close()


def finish(proc, log_path):
    """The exit code of a batch that ended on its own, or a failed test."""
    try:
        return proc.wait(timeout=GUARD)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail(
            f"the batch did not finish within {GUARD}s:\n{log_path.read_text()[-4000:]}"
        )


def run_batch(root, log_path):
    proc = start(root, log_path)
    return finish(proc, log_path), log_path.read_text()


def label(task):
    return task.args[0] if task.args else task.kwargs.get("label")


def dispatched(key):
    """(tick instant, task) for every tick of `key` that enqueued a task."""
    return [
        (tick.scheduled_for, OxTask.objects.get(id=tick.task_id))
        for tick in OxScheduleTick.objects.filter(schedule_name=key).exclude(
            task_id=None
        )
    ]


def assert_fired_once_each(expected):
    """
    Each key fired exactly one tick, that tick points at exactly one task,
    the task carries the schedule's own label and ran, and no task exists
    that no tick points at.
    """
    pointed_at = set()
    for key, name in expected.items():
        ((_, task),) = dispatched(key)
        assert label(task) == name, (key, label(task))
        assert task.status == OxTask.Status.SUCCESSFUL, (name, task.status)
        pointed_at.add(task.id)
    assert set(OxTask.objects.values_list("id", flat=True)) == pointed_at


def refused_tick_count(key):
    return (
        OxScheduleTick.objects.filter(schedule_name=key).exclude(task_id=None).count()
    )


def backdate_every_tick(days=1):
    for tick in OxScheduleTick.objects.all():
        tick.scheduled_for -= timedelta(days=days)
        tick.save(update_fields=["scheduled_for"])


def a_row(name, *, refused=False, **over):
    arguments = {"label": name}
    if refused:
        arguments["filters"] = REFUSED_TEXT
    fields = {
        "name": name,
        "task_key": KIND,
        "trigger": "cron",
        "cron": "* * * * *",
        "arguments": arguments,
        "start_time": timezone.now() - timedelta(days=1),
    }
    fields.update(over)
    return create_schedule(**fields)


def daily_interval_phase_at(local):
    """The phase that puts a daily interval's ticks at `local`'s time of day."""
    elapsed = local - datetime(1970, 1, 1)
    return int(elapsed.total_seconds()) % 86400


def waiting_on_locks():
    """
    Sessions of this test database blocked on a lock right now.

    Asked on a connection of its own. PostgreSQL keeps one snapshot of
    pg_stat_activity per transaction, so polling it from inside the
    barrier's transaction would read the same answer forever.
    """
    probe = connections.create_connection(DEFAULT_DB_ALIAS)
    try:
        with probe.cursor() as cursor:
            if probe.vendor == "postgresql":
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            else:
                # performance_schema rather than information_schema's
                # innodb_trx, which on MySQL 8 did not list these waiters.
                cursor.execute(
                    "SELECT COUNT(*) FROM performance_schema.data_lock_waits w "
                    "JOIN performance_schema.threads t "
                    "ON t.THREAD_ID = w.REQUESTING_THREAD_ID "
                    "WHERE t.PROCESSLIST_DB = DATABASE()"
                )
            return cursor.fetchone()[0]
    finally:
        probe.close()


def entry(name, **timing):
    return {"task": "tests.tasks.labelled", "args": [name], **timing}


class TestABatchWithARefusedSchedule:
    def test_the_second_run_dispatches_the_healthy_schedules_and_exits_0(
        self, tmp_path
    ):
        # The case the roadmap names: the first run after a deploy anchors
        # the new schedules and cannot fail yet; the next one meets the
        # refusal. It used to hold the batch open until the job timeout
        # and stop every schedule after the refused one.
        base = twelve_hours_ago()
        root = project(
            tmp_path,
            {
                "a-before": entry("a-before", cron=daily_cron_at(base)),
                "b-bad": {
                    **entry("b-bad", cron=daily_cron_at(base)),
                    "kwargs": REFUSED,
                },
                "c-after": entry(
                    "c-after", every=86400, phase=daily_interval_phase_at(base)
                ),
            },
        )
        code, first = run_batch(root, tmp_path / "run1.log")
        assert code == 0, first
        anchors = OxScheduleTick.objects.all()
        assert sorted(t.schedule_name for t in anchors) == [
            "a-before",
            "b-bad",
            "c-after",
        ]
        assert all(t.task_id is None for t in anchors), "a first sighting fired"
        assert {t.scheduled_for for t in anchors} == {as_stored(base)}
        # As if the first run was yesterday: every schedule has a due tick
        # after its anchor.
        backdate_every_tick()

        code, second = run_batch(root, tmp_path / "run2.log")
        assert code == 0, second
        assert_fired_once_each({"a-before": "a-before", "c-after": "c-after"})
        assert refused_tick_count("b-bad") == 0
        assert second.count("Schedule b-bad could not be dispatched this pass") == 1
        assert "Traceback" in second, "the first failure is reported in full"
        assert "Schedule dispatch failed" not in second, second
        assert "found nothing to claim" in second

    def test_a_refused_stored_row_between_healthy_rows(self, tmp_path):
        base = twelve_hours_ago()
        root = project(tmp_path, {}, stored=True)
        first = a_row("r1-good", cron=daily_cron_at(base))
        bad = a_row("r2-bad", refused=True, cron=daily_cron_at(base))
        third = a_row(
            "r3-good",
            trigger="interval",
            cron="",
            every_seconds=86400,
            phase_seconds=daily_interval_phase_at(base),
        )
        # An ordinary edit before the run. On PostgreSQL it moves r1 to the
        # end of the heap, behind the refused row.
        update_schedule(first, arguments={"label": "r1-good", "filters": '{"x": 1}'})

        code, log = run_batch(root, tmp_path / "run.log")
        assert code == 0, log
        assert_fired_once_each(
            {f"db:{first.pk}": "r1-good", f"db:{third.pk}": "r3-good"}
        )
        assert refused_tick_count(f"db:{bad.pk}") == 0
        assert log.count("Schedule r2-bad could not be dispatched this pass") == 1
        assert "Schedule dispatch failed" not in log, log

    def test_a_refused_settings_schedule_before_healthy_stored_rows(self, tmp_path):
        base = twelve_hours_ago()
        root = project(
            tmp_path,
            {"b-bad": {**entry("b-bad", cron=daily_cron_at(base)), "kwargs": REFUSED}},
            stored=True,
        )
        with_history("b-bad")
        row = a_row("r1-good", cron=daily_cron_at(base))

        code, log = run_batch(root, tmp_path / "run.log")
        assert code == 0, log
        assert_fired_once_each({f"db:{row.pk}": "r1-good"})
        assert refused_tick_count("b-bad") == 0
        assert log.count("Schedule b-bad could not be dispatched this pass") == 1


class TestTwoWorkers:
    def test_each_healthy_tick_is_dispatched_exactly_once(self, tmp_path):
        """
        Two batch workers over the same due ticks, with refused schedules
        among them, cron and interval triggers, a first sighting, and a
        stored row edited before the run.

        On PostgreSQL and MySQL both workers are held at the same tick by a
        database barrier: this test inserts that tick's row itself and
        keeps its transaction open until both workers' INSERTs are waiting
        on it, then rolls back. One worker's INSERT goes through; the other
        loses the race (PostgreSQL) or is chosen as the deadlock victim
        (MySQL, where two waiters on a rolled-back key deadlock). SQLite
        has one writer, so its workers take turns without a barrier.
        """
        base = twelve_hours_ago()
        # Due twelve hours ago and not again for another twelve, so both
        # workers derive the same tick whenever the test runs.
        root = project(
            tmp_path,
            {
                "a-cron": entry("a-cron", cron=daily_cron_at(base)),
                "b-bad": {
                    **entry("b-bad", cron=daily_cron_at(base)),
                    "kwargs": REFUSED,
                },
                "c-interval": entry(
                    "c-interval", every=86400, phase=daily_interval_phase_at(base)
                ),
                "d-new": entry("d-new", cron=daily_cron_at(base - timedelta(hours=1))),
            },
            stored=True,
        )
        with_history("a-cron", "b-bad", "c-interval")
        edited = a_row("r-edited", cron=daily_cron_at(base))
        update_schedule(edited, arguments={"label": "r-edited", "filters": '{"x": 1}'})
        bad_row = a_row("r-bad", refused=True, cron=daily_cron_at(base))
        due = as_stored(base)

        logs = [tmp_path / "w1.log", tmp_path / "w2.log"]
        procs = []
        barrier = connection.vendor in ("postgresql", "mysql")
        try:
            if barrier:
                with transaction.atomic():
                    OxScheduleTick.objects.create(
                        schedule_name="c-interval",
                        scheduled_for=due,
                        task_id=None,
                        created_at=timezone.now(),
                    )
                    procs = [start(root, log) for log in logs]
                    held = wait_for(lambda: waiting_on_locks() >= 2, timeout=GUARD)
                    transaction.set_rollback(True)
                assert held, "both workers never waited on the tick: " + "\n".join(
                    log.read_text()[-2000:] for log in logs
                )
            else:
                procs = [start(root, log) for log in logs]
            codes = [finish(proc, log) for proc, log in zip(procs, logs, strict=True)]
        finally:
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
        text = [log.read_text() for log in logs]
        assert codes == [0, 0], text

        assert_fired_once_each(
            {
                "a-cron": "a-cron",
                "c-interval": "c-interval",
                f"db:{edited.pk}": "r-edited",
            }
        )
        # The tick the barrier held is the one that fired.
        ((instant, _),) = dispatched("c-interval")
        assert instant == due
        # The first sighting anchored once, between two workers, and fired
        # nothing.
        (anchor,) = OxScheduleTick.objects.filter(schedule_name="d-new")
        assert anchor.task_id is None
        assert refused_tick_count("b-bad") == 0
        assert refused_tick_count(f"db:{bad_row.pk}") == 0
        # Losing the race, or the deadlock, is not a schedule's failure.
        for log in text:
            assert "c-interval could not be dispatched" not in log, log
            assert "Schedule dispatch failed" not in log, log
        assert any("b-bad could not be dispatched" in log for log in text)
        assert any("r-bad could not be dispatched" in log for log in text)


#: Starts every record in the log of a worker these tests read line by
#: line, so a record and the traceback under it stay together.
RECORD = "@@ "


def records(log_path):
    """The worker's log records so far, in order, each with its traceback."""
    return [
        RECORD + part for part in ("\n" + log_path.read_text()).split("\n" + RECORD)[1:]
    ]


def kind_of(record, name):
    """What a record says about schedule `name`, or None if it is not about it."""
    if record.startswith(f"{RECORD}Schedule {name} could not be dispatched this pass"):
        return "first"
    if record.startswith(f"{RECORD}Schedule {name} still cannot be dispatched:"):
        return "summary"
    if record.startswith(f"{RECORD}Schedule {name} dispatched again after "):
        return "recovered"
    if record.startswith(f"{RECORD}Dispatched schedule {name} tick "):
        return "dispatched"
    return None


def about(log_path, name):
    """(position in the log, kind, record) for every record about `name`."""
    return [
        (at, kind, record)
        for at, record in enumerate(records(log_path))
        if (kind := kind_of(record, name)) is not None
    ]


def failed(log_path, name):
    return [row for row in about(log_path, name) if row[1] in ("first", "summary")]


def recovered_after(record):
    """The failures a schedule_dispatch_recovered record counts."""
    return int(record.split(" dispatched again after ", 1)[1].split(" ", 1)[0])


class TestAPausedRowThroughARealWorker:
    """
    A stored row the database refuses, paused while it fails, repaired or
    not, and resumed, through one long-running `manage.py ox_worker`.

    Pausing is how a person stops a failing schedule while they repair it.
    The row still exists, so the run of failures is still its own: a repair
    made while it was paused ends the run with one recovery at the first
    dispatch after the resume, and a resume without a repair goes on
    counting the same run instead of reporting a new first failure with a
    traceback. Deleting the row is what ends a run without an event.

    Every failed attempt is a line here (`report_every=0`), so the failures
    a recovery counts can be counted in the log. Each wait is for a record
    the worker writes, bounded by GUARD as a deadlock guard. A sentinel row
    created after a write and seen to fire marks a dispatch pass that began
    after that write committed, which is how a test knows the worker has
    seen it.
    """

    NAME = "r-paused"

    @pytest.fixture
    def worker(self, tmp_path):
        root = project(tmp_path, {}, stored=True, report_every=0)
        # Every second, so a resume, which moves the boundary to the moment
        # it is made, has a tick after it within a second.
        row = a_row(
            self.NAME,
            refused=True,
            trigger="interval",
            cron="",
            every_seconds=1,
            phase_seconds=0,
        )
        log = tmp_path / "worker.log"
        proc = start(root, log, batch=False, log_format=RECORD + "%(message)s")

        def stop():
            proc.send_signal(signal.SIGTERM)
            return finish(proc, log)

        try:
            self.until(
                log, "three failed attempts", lambda: len(failed(log, self.NAME)) >= 3
            )
            yield row, log, stop
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)

    def until(self, log, what, predicate):
        if not wait_for(predicate, timeout=GUARD):
            pytest.fail(f"no {what} within {GUARD}s:\n{log.read_text()[-4000:]}")

    def seen(self, log, sentinel):
        """
        Wait for a pass that began after everything written so far.

        The sentinel row is due twelve hours ago and fires on the first pass
        whose read of the rows includes it, which is a pass that began after
        it, and so after every write before it, committed. Its record's
        position in the log is returned.
        """
        a_row(sentinel, cron=daily_cron_at(twelve_hours_ago()))
        self.until(log, f"dispatch of {sentinel}", lambda: bool(about(log, sentinel)))
        ((at, _, _),) = about(log, sentinel)
        return at

    def test_repaired_while_paused_it_recovers_once_on_resume(self, worker):
        row, log, stop = worker
        update_schedule(row, enabled=False)
        paused_at = self.seen(log, "sentinel-1")
        update_schedule(row, arguments={"label": self.NAME})
        update_schedule(row, enabled=True)
        # Two dispatches: the pass that made the first had written all of
        # its records before the second began.
        self.until(
            log,
            "two dispatches after the resume",
            lambda: sum(k == "dispatched" for _, k, _ in about(log, self.NAME)) >= 2,
        )
        assert stop() == 0

        lines = about(log, self.NAME)
        kinds = [kind for _, kind, _ in lines]
        failures = failed(log, self.NAME)
        assert all(at < paused_at for at, _, _ in failures), "failed while paused"
        assert kinds.count("first") == 1
        assert sum("Traceback" in record for _, _, record in failures) == 1
        recovered = [record for _, kind, record in lines if kind == "recovered"]
        assert len(recovered) == 1, lines
        # The run the pause interrupted, counted whole, and ended by the
        # first dispatch after the resume.
        assert recovered_after(recovered[0]) == len(failures)
        assert kinds.index("recovered") == kinds.index("dispatched") + 1
        assert {label for _, label in fired_ticks(row)} == {self.NAME}

    def test_resumed_unrepaired_it_goes_on_counting_the_same_run(self, worker):
        row, log, stop = worker
        update_schedule(row, enabled=False)
        self.seen(log, "sentinel-1")
        before = len(failed(log, self.NAME))
        update_schedule(row, enabled=True)
        self.until(
            log,
            "a failure after the resume",
            lambda: len(failed(log, self.NAME)) > before,
        )
        failures = failed(log, self.NAME)
        assert [kind for _, kind, _ in failures].count("first") == 1, (
            "the resume started a new run: " + failures[before][2]
        )
        assert "Traceback" not in failures[before][2]
        summary = f"Schedule {self.NAME} still cannot be dispatched: 1 more failure(s)"
        assert failures[before][2].startswith(RECORD + summary)

        # Repaired now: the one recovery counts the failures on both sides
        # of the pause as one run.
        update_schedule(row, arguments={"label": self.NAME})
        self.until(
            log,
            "two dispatches after the repair",
            lambda: sum(k == "dispatched" for _, k, _ in about(log, self.NAME)) >= 2,
        )
        assert stop() == 0
        failures = failed(log, self.NAME)
        recovered = [r for _, k, r in about(log, self.NAME) if k == "recovered"]
        assert len(recovered) == 1
        assert recovered_after(recovered[0]) == len(failures)

    def test_deleted_its_run_ends_without_an_event(self, worker):
        # What a run is keyed on is the row's primary key. A row restored
        # from a backup with that key after its deletion is a new row, and
        # its first refusal is reported in full: the run of the deleted row
        # was forgotten, not carried over to it.
        row, log, stop = worker
        backup = OxSchedule.objects.get(pk=row.pk)
        delete_schedule(row)
        deleted_at = self.seen(log, "sentinel-1")
        before = len(failed(log, self.NAME))
        # Restore the row and update the change marker in one transaction, row
        # first, matching the product's write paths. The separate marker transaction
        # read before writing, and this test's timing made that write overlap the
        # worker's routine writes. On SQLite, upgrading a read transaction to a
        # write fails immediately with SQLITE_BUSY if another connection is writing,
        # without waiting for its lock. Writing first avoids that upgrade.
        with transaction.atomic():
            backup.save(force_insert=True)
            _touch_change_row()
        self.until(
            log,
            "a failure of the restored row",
            lambda: len(failed(log, self.NAME)) > before,
        )
        assert stop() == 0
        failures = failed(log, self.NAME)
        at, kind, record = failures[before]
        assert at > deleted_at
        assert kind == "first", record
        assert "Traceback" in record
        assert [k for _, k, _ in failures].count("first") == 2
        assert not any(k == "recovered" for _, k, _ in about(log, self.NAME))


def fired_ticks(row):
    return [(instant, label(task)) for instant, task in dispatched(f"db:{row.pk}")]
