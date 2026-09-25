"""
One schedule the database refuses does not stop the others.

Every test here makes the database refuse for real: a non-finite float in
a schedule's arguments, which each engine rejects at the task INSERT with
an exception of a different class. Nothing is mocked where the claim is
about what the database does; where a failure has to arrive at a chosen
statement, the connection is lost for real (tests/isolation.py) or the
statement is made to fail on a connection that stays usable, and the test
says which.

tests/test_schedule_isolation_worker.py drives the same cases through real
`manage.py ox_worker` processes.
"""

import logging
import threading
from datetime import timedelta

import pytest
from django.db import DatabaseError, OperationalError, connection, transaction
from django.db.models import F
from django.utils import timezone

from django_ox import worker as worker_module
from django_ox.compat import task_enqueued
from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    _touch_change_row,
    create_schedule,
    delete_schedule,
    update_schedule,
)
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread
from .forms import FilterArgs
from .isolation import REFUSED_TEXT, cron_schedules, fired, kill, labels, with_history

pytestmark = pytest.mark.django_db

KIND = "iso.filtered"
STORED = "django_ox.stored.DatabaseScheduleSource"

#: The class each engine's driver gives the refusal, as Django re-raises
#: it. MySQL's 3140 is an OperationalError under PyMySQL; the class is
#: recorded for the report, and nothing decides by it.
REFUSAL_CLASS = {
    "postgresql": {"DataError"},
    "mysql": {"OperationalError", "DataError"},
    "sqlite": {"IntegrityError"},
}


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def configure(settings, schedules=None, *, stored=False):
    options = {"SCHEDULES": schedules or {}}
    if stored:
        options["SCHEDULE_SOURCE"] = STORED
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": options,
        }
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key=KIND, task=tasks.labelled, form=FilterArgs))


@pytest.fixture
def frozen_now(monkeypatch):
    """Pin the clock mid-minute, so every pass in a test sees one due tick."""
    fixed = timezone.now().replace(second=30, microsecond=0)
    monkeypatch.setattr(timezone, "now", lambda: fixed)
    return fixed


def a_row(name, *, refused=False, **over):
    """A stored minutely row, due now: its boundary is well in the past."""
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


def server_session():
    """This thread's connection: the driver's object and the server's id for it."""
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SELECT pg_backend_pid()")
        elif connection.vendor == "mysql":
            cursor.execute("SELECT CONNECTION_ID()")
        else:
            cursor.execute("SELECT 1")
        return connection.connection, cursor.fetchone()[0]


def assert_refusal(record):
    assert record.error in REFUSAL_CLASS[connection.vendor], record.error


class TestARefusedScheduleIsIsolated:
    def test_the_settings_schedules_after_it_fire(self, settings, caplog):
        configure(
            settings, cron_schedules("a-before", "b-bad", "c-after", refused={"b-bad"})
        )
        worker = Worker(backoff_initial=0)
        with_history("a-before", "b-bad", "c-after")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 2
        assert [label for _, label in fired("a-before")] == ["a-before"]
        assert [label for _, label in fired("c-after")] == ["c-after"]
        assert fired("b-bad") == []
        assert labels() == ["a-before", "c-after"]
        # The refused tick was rolled back whole: only the seeded history.
        assert OxScheduleTick.objects.filter(schedule_name="b-bad").count() == 1
        (reported,) = events(caplog, "schedule_dispatch_error")
        assert reported.schedule == "b-bad"
        assert reported.exc_info is not None, "the first failure is in full"
        assert_refusal(reported)

    @pytest.mark.django_db(transaction=True)
    def test_a_run_of_refused_schedules_does_not_starve_the_next(
        self, settings, caplog
    ):
        # Outermost transactions, as in a worker. Five refusals in a row on
        # one usable connection, and the sixth schedule fires on that same
        # session: nothing reconnected, and nothing gave up after N.
        refused = [f"b-bad-{i}" for i in range(5)]
        configure(settings, cron_schedules(*refused, "z-after", refused=set(refused)))
        worker = Worker(backoff_initial=0)
        with_history(*refused, "z-after")
        session = server_session()
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert server_session() == session, "the pass moved to another session"
        assert [label for _, label in fired("z-after")] == ["z-after"]
        assert labels() == ["z-after"]
        reported = events(caplog, "schedule_dispatch_error")
        assert [r.schedule for r in reported] == refused
        assert not OxScheduleTick.objects.filter(
            schedule_name__in=refused, task_id__isnull=False
        ).exists()

    def test_a_stored_row_the_database_refuses_does_not_stop_the_rows_after_it(
        self, settings, caplog
    ):
        configure(settings, stored=True)
        good_1 = a_row("r1-good")
        bad = a_row("r2-bad", refused=True)
        good_3 = a_row("r3-good")
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 2
        assert [label for _, label in fired(f"db:{good_1.pk}")] == ["r1-good"]
        assert [label for _, label in fired(f"db:{good_3.pk}")] == ["r3-good"]
        assert not OxScheduleTick.objects.filter(schedule_name=f"db:{bad.pk}").exists()
        (reported,) = events(caplog, "schedule_dispatch_error")
        assert reported.schedule == "r2-bad"
        assert_refusal(reported)

    def test_a_refused_settings_schedule_does_not_stop_the_stored_rows(
        self, settings, caplog
    ):
        configure(settings, cron_schedules("b-bad", refused={"b-bad"}), stored=True)
        row = a_row("r1-good")
        worker = Worker(backoff_initial=0)
        with_history("b-bad")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert [label for _, label in fired(f"db:{row.pk}")] == ["r1-good"]
        assert [r.schedule for r in events(caplog, "schedule_dispatch_error")] == [
            "b-bad"
        ]

    @pytest.mark.usefixtures("frozen_now")
    def test_the_refused_tick_stays_unclaimed_and_is_tried_every_pass(
        self, settings, caplog
    ):
        configure(settings, cron_schedules("a-ok", "b-bad", refused={"b-bad"}))
        worker = Worker(backoff_initial=0)
        worker._dispatch_report.clock = lambda: 0.0
        with_history("a-ok", "b-bad")
        attempts = []
        real = worker._dispatch_report.schedule_failed

        def counting(schedule, exc):
            attempts.append(schedule.name)
            real(schedule, exc)

        worker._dispatch_report.schedule_failed = counting
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
            assert worker.dispatch_schedules() == 0
            assert worker.dispatch_schedules() == 0
        # The healthy one fired once; its tick is recorded and suppressed.
        assert labels() == ["a-ok"]
        # Attempted on each pass, with no backoff and no skipped tick.
        assert attempts == ["b-bad"] * 3
        assert len(events(caplog, "schedule_dispatch_error")) == 1

    def test_a_starting_deadline_still_drops_a_refused_tick(
        self, settings, caplog, monkeypatch
    ):
        # The deadline decides before the transaction opens, so a refused
        # row stops being attempted once its tick is too late, and says so
        # as a dropped tick rather than as another failure.
        configure(settings, stored=True)
        due = timezone.now().replace(second=0, microsecond=0)
        at = [due + timedelta(seconds=30)]
        monkeypatch.setattr(timezone, "now", lambda: at[0])
        bad = a_row("r-bad", refused=True, starting_deadline_seconds=45)
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            worker.dispatch_schedules()
            at[0] = due + timedelta(seconds=50)
            worker.dispatch_schedules()
        assert [r.schedule for r in events(caplog, "schedule_dispatch_error")] == [
            "r-bad"
        ]
        (dropped,) = events(caplog, "schedule_tick_dropped")
        assert dropped.schedule == "r-bad"
        assert not OxScheduleTick.objects.filter(schedule_name=f"db:{bad.pk}").exists()

    def test_a_receiver_whose_sql_fails_costs_only_its_schedule(self, settings, caplog):
        # send_robust swallows the receiver's exception, but on PostgreSQL
        # its failed statement has aborted the dispatch transaction, and
        # the tick update after the enqueue raises InternalError. That is
        # the schedule's failure, not the pass's. Elsewhere a failed
        # statement leaves the transaction open and the dispatch stands.
        configure(settings, cron_schedules("a-before", "b-receiver", "c-after"))
        worker = Worker(backoff_initial=0)
        with_history("a-before", "b-receiver", "c-after")

        def receiver(sender, task_result, **kwargs):
            if list(task_result.args) == ["b-receiver"]:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM iso_no_such_table")

        task_enqueued.connect(receiver)
        try:
            with caplog.at_level(logging.INFO, logger="django_ox"):
                dispatched = worker.dispatch_schedules()
        finally:
            task_enqueued.disconnect(receiver)
        if connection.vendor == "postgresql":
            assert dispatched == 2
            assert labels() == ["a-before", "c-after"]
            (reported,) = events(caplog, "schedule_dispatch_error")
            assert reported.schedule == "b-receiver"
            assert reported.error == "InternalError"
        else:
            assert dispatched == 3
            assert labels() == ["a-before", "b-receiver", "c-after"]
            assert not events(caplog, "schedule_dispatch_error")


class TestReportingIsRateLimited:
    """
    A refused schedule is tried on every pass and fails the same way each
    time. The first failure is reported in full, the rest at most once a
    DISPATCH_FAILURE_REPORT_INTERVAL, and the recovery once. The clock is
    the report's own, so the intervals are exact rather than waited for.
    """

    def test_full_then_summary_then_recovered(self, settings, caplog, monkeypatch):
        configure(settings, stored=True)
        fixed = timezone.now().replace(second=30, microsecond=0)
        monkeypatch.setattr(timezone, "now", lambda: fixed)
        bad = a_row("r-bad", refused=True)
        worker = Worker(backoff_initial=0)
        clock = [1000.0]
        worker._dispatch_report.clock = lambda: clock[0]
        interval = worker_module.DISPATCH_FAILURE_REPORT_INTERVAL

        def one_pass(at):
            clock[0] = 1000.0 + at
            with caplog.at_level(logging.INFO, logger="django_ox"):
                worker.dispatch_schedules()

        one_pass(0)
        (first,) = events(caplog, "schedule_dispatch_error")
        assert first.exc_info is not None
        assert (first.failures, first.suppressed) == (1, 0)
        assert first.database == "default"

        for at in (1, interval / 2, interval - 0.001):
            one_pass(at)
        assert len(events(caplog, "schedule_dispatch_error")) == 1, "not throttled"

        one_pass(interval)
        summary = events(caplog, "schedule_dispatch_error")[-1]
        assert summary is not first
        assert summary.exc_info is None, "a summary carries no traceback"
        assert (summary.failures, summary.suppressed) == (5, 4)
        assert_refusal(summary)
        assert "4 more failure(s)" in summary.getMessage()
        assert "max_age" not in summary.getMessage(), "no arguments in a summary"

        one_pass(interval + 1)
        assert len(events(caplog, "schedule_dispatch_error")) == 2

        # Repaired through the write API. The arguments do not move the
        # boundary, so the tick that kept failing is the one that fires.
        update_schedule(bad, arguments={"label": "r-bad", "filters": '{"max_age": 1}'})
        one_pass(interval + 2)
        assert [label for _, label in fired(f"db:{bad.pk}")] == ["r-bad"]
        (recovered,) = events(caplog, "schedule_dispatch_recovered")
        assert recovered.schedule == "r-bad"
        assert recovered.failures == 6

        one_pass(interval + 3)
        assert len(events(caplog, "schedule_dispatch_recovered")) == 1

        # Broken again at the next tick: a new run, reported in full.
        update_schedule(bad, arguments={"label": "r-bad", "filters": REFUSED_TEXT})
        later = fixed + timedelta(minutes=1)
        monkeypatch.setattr(timezone, "now", lambda: later)
        one_pass(interval + 4)
        again = events(caplog, "schedule_dispatch_error")[-1]
        assert again.exc_info is not None
        assert (again.failures, again.suppressed) == (1, 0)

    def test_the_state_is_bounded_by_the_schedules_that_exist(self, settings):
        configure(settings, stored=True)
        rows = [a_row(f"r-bad-{i}", refused=True) for i in range(3)]
        worker = Worker(backoff_initial=0)
        worker.dispatch_schedules()
        assert sorted(worker._dispatch_report._schedules) == sorted(
            f"db:{row.pk}" for row in rows
        )
        for row in rows[:2]:
            delete_schedule(row)
        # Forgotten once the source stops answering them: at the next full
        # read, or, where the change marker has not visibly moved, the pass
        # after the one that found each row gone under its lock.
        worker.dispatch_schedules()
        worker.dispatch_schedules()
        assert list(worker._dispatch_report._schedules) == [f"db:{rows[2].pk}"]


class TestAPauseDoesNotEndARunOfFailures:
    """
    A paused row is not dispatched, so the source leaves it out of its
    answer, but it still exists, and its run of failures is still its own.
    Resumed after a repair, its next dispatch is the one recovery for the
    whole run; resumed without one, it goes on counting the same run. Only
    a row that is gone loses its run, without an event.

    The wall clock is stepped by hand so that each write moves the change
    marker (a marker written twice at one instant does not read as moved)
    and a resume's boundary falls before the next minute's tick. The
    report's own clock is stepped too, so the summary interval is exact.
    tests/test_schedule_isolation_worker.py drives the same through a real
    worker process.
    """

    @pytest.fixture
    def clocks(self, monkeypatch):
        fixed = timezone.now().replace(second=30, microsecond=0)
        wall = [fixed]
        monkeypatch.setattr(timezone, "now", lambda: wall[0])
        return wall, [1000.0]

    def run(self, settings, clocks, caplog):
        """A worker over one refused row that has failed three times."""
        configure(settings, stored=True)
        wall, report = clocks
        bad = a_row("r-bad", refused=True)
        worker = Worker(backoff_initial=0)
        worker._dispatch_report.clock = lambda: report[0]

        def one_pass():
            wall[0] += timedelta(seconds=1)
            report[0] += 1
            with caplog.at_level(logging.INFO, logger="django_ox"):
                worker.dispatch_schedules()

        for _ in range(3):
            one_pass()
        assert worker._dispatch_report._schedules[f"db:{bad.pk}"].failures == 3
        return bad, worker, one_pass

    def next_minute(self, clocks):
        """Past the resume's boundary: the next minutely tick is due."""
        wall, _ = clocks
        wall[0] += timedelta(minutes=1)

    def test_repaired_while_paused_it_recovers_once_on_resume(
        self, settings, clocks, caplog
    ):
        bad, worker, one_pass = self.run(settings, clocks, caplog)
        update_schedule(bad, enabled=False)
        one_pass()
        one_pass()
        assert f"db:{bad.pk}" not in {
            s.key for s in worker._schedule_source.schedules()
        }
        update_schedule(bad, arguments={"label": "r-bad"})
        update_schedule(bad, enabled=True)
        self.next_minute(clocks)
        one_pass()
        assert [label for _, label in fired(f"db:{bad.pk}")] == ["r-bad"]
        (recovered,) = events(caplog, "schedule_dispatch_recovered")
        assert recovered.failures == 3
        assert len(events(caplog, "schedule_dispatch_error")) == 1
        assert worker._dispatch_report._schedules == {}

    def test_resumed_unrepaired_it_goes_on_counting_the_same_run(
        self, settings, clocks, caplog
    ):
        bad, worker, one_pass = self.run(settings, clocks, caplog)
        update_schedule(bad, enabled=False)
        one_pass()
        update_schedule(bad, enabled=True)
        self.next_minute(clocks)
        one_pass()
        # Inside the interval: counted, not reported, and not a new first
        # failure with its traceback.
        (first,) = events(caplog, "schedule_dispatch_error")
        assert first.exc_info is not None
        assert worker._dispatch_report._schedules[f"db:{bad.pk}"].failures == 4
        _, report = clocks
        report[0] += worker_module.DISPATCH_FAILURE_REPORT_INTERVAL
        one_pass()
        summary = events(caplog, "schedule_dispatch_error")[-1]
        assert summary.exc_info is None
        assert (summary.failures, summary.suppressed) == (5, 4)

    def test_a_pause_made_outside_the_write_api_keeps_the_run(
        self, settings, clocks, caplog
    ):
        # Met under the row lock at dispatch rather than by a full read:
        # the marker has not moved, so the cached copy still has the row
        # until that lock finds it disabled.
        bad, worker, one_pass = self.run(settings, clocks, caplog)
        OxSchedule.objects.filter(pk=bad.pk).update(enabled=False)
        one_pass()
        one_pass()
        assert f"db:{bad.pk}" not in {
            s.key for s in worker._schedule_source.schedules()
        }
        assert worker._dispatch_report._schedules[f"db:{bad.pk}"].failures == 3

    def test_a_row_that_stops_building_keeps_the_run(self, settings, clocks, caplog):
        # Skipped at the read with schedule_row_skipped, as any row that no
        # longer validates is, and still a row: a repair that makes it
        # dispatch again ends the same run.
        bad, worker, one_pass = self.run(settings, clocks, caplog)
        OxSchedule.objects.filter(pk=bad.pk).update(arguments={"filters": "{}"})
        _touch_change_row()
        one_pass()
        assert events(caplog, "schedule_row_skipped")
        assert worker._dispatch_report._schedules[f"db:{bad.pk}"].failures == 3
        update_schedule(bad, arguments={"label": "r-bad"})
        one_pass()
        (recovered,) = events(caplog, "schedule_dispatch_recovered")
        assert recovered.failures == 3

    def test_a_row_deleted_outside_the_write_api_loses_the_run(
        self, settings, clocks, caplog
    ):
        # The marker has not moved, so no full read says the row is gone.
        # The row lock at dispatch does, and the pass after it forgets.
        bad, worker, one_pass = self.run(settings, clocks, caplog)
        OxSchedule.objects.filter(pk=bad.pk).delete()
        one_pass()
        one_pass()
        assert worker._dispatch_report._schedules == {}
        assert events(caplog, "schedule_dispatch_recovered") == []

    def test_another_source_bounds_the_run_by_its_answer(self, settings, caplog):
        # A source that is not the stored one cannot say what exists
        # beyond what it answers, so a schedule it stops answering is
        # forgotten, as before.
        configure(settings, cron_schedules("b-bad", refused={"b-bad"}))
        worker = Worker(backoff_initial=0)
        with_history("b-bad")
        worker.dispatch_schedules()
        assert list(worker._dispatch_report._schedules) == ["b-bad"]

        class Emptied:
            def schedules(self):
                return []

        worker._schedule_source = Emptied()
        worker.dispatch_schedules()
        assert worker._dispatch_report._schedules == {}


@pytest.mark.django_db(transaction=True)
class TestAbandonedPassesAreSummarised:
    def test_the_first_is_in_full_and_the_rest_in_summary(self, settings, caplog):
        configure(settings)
        worker = Worker(
            backoff_initial=0,
            poll_interval=0.01,
            reap_interval=3600.0,
            schedule_interval=0.0,
            batch=True,
        )
        clock = [0.0]
        worker._dispatch_report.clock = lambda: clock[0]
        calls = []

        def failing_ten_times():
            calls.append(clock[0])
            if len(calls) <= 10:
                # 15 s apart on the report's clock: 0, 15, ... 135.
                clock[0] = 15.0 * len(calls)
                raise DatabaseError("the bounded tick read failed")
            return 0

        worker.dispatch_schedules = failing_ten_times
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            thread.join(timeout=30)
        assert not thread.is_alive()
        assert len(calls) == 11
        failed = events(caplog, "schedule_dispatch_failed")
        # The report's clock reads 15, 30, ... 150 at the ten failures: the
        # first in full at 15, then a summary at 75 and at 135, each the
        # first failure a full interval after the last line.
        assert [r.exc_info is not None for r in failed] == [True, False, False]
        assert [(r.failures, r.suppressed) for r in failed] == [
            (1, 0),
            (5, 4),
            (9, 4),
        ]
        assert all(r.error == "DatabaseError" for r in failed)
        (done,) = events(caplog, "worker_batch_empty")
        assert caplog.records.index(done) > caplog.records.index(failed[-1])


class TestContentionIsNotRefusal:
    """
    A lock the database gave up waiting for is contention: a warning with
    no traceback, the tick left for a later pass. A refusal is the
    schedule's failure. Both roll back whole, and neither stops the rest.
    """

    @pytest.mark.django_db(transaction=True)
    def test_a_held_row_lock_and_a_refused_row_in_one_pass(self, settings, caplog):
        if connection.vendor == "sqlite":
            pytest.skip("SQLite has one writer; the next test covers it")
        configure(settings, stored=True)
        locked = a_row("r-locked")
        bad = a_row("r-bad", refused=True)
        good = a_row("r-good")
        worker = Worker(backoff_initial=0)
        holding, release = threading.Event(), threading.Event()

        def hold_the_row():
            try:
                with transaction.atomic():
                    OxSchedule.objects.select_for_update().get(pk=locked.pk)
                    holding.set()
                    release.wait(timeout=30)
            finally:
                connection.close()

        holder = threading.Thread(target=hold_the_row)
        holder.start()
        try:
            assert holding.wait(timeout=10)
            with connection.cursor() as cursor:
                if connection.vendor == "postgresql":
                    cursor.execute("SET lock_timeout = '300ms'")
                else:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
            with caplog.at_level(logging.INFO, logger="django_ox"):
                assert worker.dispatch_schedules() == 1
        finally:
            release.set()
            holder.join(timeout=30)
            with connection.cursor() as cursor:
                if connection.vendor == "postgresql":
                    cursor.execute("RESET lock_timeout")
                else:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = DEFAULT")
        (contended,) = events(caplog, "schedule_lock_unavailable")
        assert contended.exc_info is None, "contention is not a traceback"
        (refused,) = events(caplog, "schedule_dispatch_error")
        assert refused.schedule == "r-bad"
        assert labels() == ["r-good"]
        assert not OxScheduleTick.objects.filter(
            schedule_name__in=[f"db:{locked.pk}", f"db:{bad.pk}"]
        ).exists(), "a rolled-back schedule left a tick behind"
        assert [label for _, label in fired(f"db:{good.pk}")] == ["r-good"]

        # Released: the contended row fires on the next pass, the refused
        # one fails again and is not reported again this minute.
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="django_ox"):
            worker.dispatch_schedules()
        assert "r-locked" in labels()
        assert "r-bad" not in labels()
        assert not events(caplog, "schedule_lock_unavailable")

    @pytest.mark.django_db(transaction=True)
    def test_sqlite_deferred_contention_is_not_a_refusal(self, settings, caplog):
        if connection.vendor != "sqlite":
            pytest.skip("SQLite's single writer")
        configure(settings, cron_schedules("a-ok", "b-bad", refused={"b-bad"}))
        worker = Worker(backoff_initial=0)
        with_history("a-ok", "b-bad")
        row = OxScheduleTick.objects.filter(schedule_name="a-ok").first()
        writing, release = threading.Event(), threading.Event()

        def hold_the_write_lock():
            try:
                with transaction.atomic():
                    OxScheduleTick.objects.filter(pk=row.pk).update(
                        schedule_name=F("schedule_name")
                    )
                    writing.set()
                    release.wait(timeout=30)
            finally:
                connection.close()

        holder = threading.Thread(target=hold_the_write_lock)
        holder.start()
        try:
            assert writing.wait(timeout=10)
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA busy_timeout = 300")
            with caplog.at_level(logging.INFO, logger="django_ox"):
                assert worker.dispatch_schedules() == 0
        finally:
            release.set()
            holder.join(timeout=30)
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA busy_timeout = 20000")
        warned = events(caplog, "schedule_lock_unavailable")
        assert [r.schedule for r in warned] == ["a-ok", "b-bad"]
        assert all(r.exc_info is None for r in warned)
        assert not events(caplog, "schedule_dispatch_error"), (
            "contention was reported as a refusal"
        )
        assert not OxTask.objects.exists()
        assert OxScheduleTick.objects.count() == 2, "only the seeded history"

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert labels() == ["a-ok"]
        assert [r.schedule for r in events(caplog, "schedule_dispatch_error")] == [
            "b-bad"
        ]


@pytest.mark.django_db(transaction=True)
class TestAPassThatFailsKeepsDispatchOwed:
    """
    A pass abandoned for a shared read or a lost connection leaves dispatch
    owed, and only a pass that goes through every schedule clears it: a
    --batch worker does not finish in between.
    """

    def batch_worker(self):
        return Worker(
            backoff_initial=0,
            poll_interval=0.01,
            reap_interval=3600.0,
            schedule_interval=0.0,
            batch=True,
        )

    def run_to_completion(self, worker, wrapper, caplog):
        def run():
            with connection.execute_wrapper(wrapper):
                worker.run()

        thread = threading.Thread(target=run, daemon=True)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread.start()
            thread.join(timeout=30)
        if thread.is_alive():
            worker.request_stop()
            thread.join(timeout=10)
            pytest.fail("the batch did not finish")

    def test_a_shared_read_that_fails_three_times(self, settings, caplog):
        configure(settings, cron_schedules("a-ok", "b-bad", refused={"b-bad"}))
        with_history("a-ok", "b-bad")
        worker = self.batch_worker()
        worker._dispatch_report.clock = lambda: 0.0
        failed_reads = []

        def failing_the_tick_read(execute, sql, params, many, context):
            is_tick_read = "MAX(" in sql.upper() and "oxscheduletick" in sql
            if is_tick_read and len(failed_reads) < 3:
                failed_reads.append(1)
                raise OperationalError("the bounded tick read failed")
            return execute(sql, params, many, context)

        self.run_to_completion(worker, failing_the_tick_read, caplog)
        assert len(failed_reads) == 3
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.exc_info is not None
        (dispatched,) = events(caplog, "schedule_dispatched")
        (done,) = events(caplog, "worker_batch_empty")
        records = caplog.records
        assert records.index(failed) < records.index(dispatched) < records.index(done)
        # The refused schedule was attempted by the pass that completed,
        # and did not hold the batch open.
        assert [r.schedule for r in events(caplog, "schedule_dispatch_error")] == [
            "b-bad"
        ]
        (task,) = OxTask.objects.all()
        assert task.args == ["a-ok"]
        assert task.status == OxTask.Status.SUCCESSFUL

    def test_a_connection_lost_mid_pass(self, settings, caplog):
        configure(settings, cron_schedules("a-ok", "b-ok"))
        with_history("a-ok", "b-ok")
        worker = self.batch_worker()
        lost = []

        def losing_it_at_the_second_schedule(execute, sql, params, many, context):
            is_tick_insert = sql.lstrip().upper().startswith("INSERT") and (
                "oxscheduletick" in sql
            )
            if is_tick_insert and params and "b-ok" in params and not lost:
                lost.append(1)
                kill(context["connection"])
            return execute(sql, params, many, context)

        self.run_to_completion(worker, losing_it_at_the_second_schedule, caplog)
        assert lost == [1]
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert not events(caplog, "schedule_dispatch_error"), (
            "a lost connection was reported against a schedule"
        )
        # a-ok committed before the loss, b-ok on the pass after it; each
        # tick once.
        assert [r.schedule for r in events(caplog, "schedule_dispatched")] == [
            "a-ok",
            "b-ok",
        ]
        assert labels() == ["a-ok", "b-ok"]
        assert set(OxTask.objects.values_list("status", flat=True)) == {
            OxTask.Status.SUCCESSFUL
        }
        (done,) = events(caplog, "worker_batch_empty")
        assert caplog.records.index(done) > caplog.records.index(failed)


class TestTraversalOrder:
    def test_settings_in_declared_order_then_rows_by_primary_key_after_an_edit(
        self, settings
    ):
        configure(settings, cron_schedules("s-second", "s-first"), stored=True)
        rows = [a_row(f"r{i}") for i in (1, 2, 3)]
        # An ordinary edit. On PostgreSQL it writes a new row version at the
        # end of the heap, and an unordered read then returns r1 last.
        update_schedule(rows[0], arguments={"label": "r1", "filters": '{"x": 1}'})
        worker = Worker(backoff_initial=0)
        names = [schedule.name for schedule in worker._schedule_source.schedules()]
        assert names == ["s-second", "s-first", "r1", "r2", "r3"]

    def test_the_order_holds_when_a_worker_reads_the_rows_again(self, settings):
        # On the real clock: the edit has to move the change marker, and a
        # frozen clock writes the same instant the worker already saw.
        configure(settings, stored=True)
        rows = [a_row(f"r{i}") for i in (1, 2, 3)]
        worker = Worker(backoff_initial=0)
        worker.dispatch_schedules()
        update_schedule(rows[0], arguments={"label": "r1", "filters": '{"x": 2}'})
        worker.dispatch_schedules()
        names = [schedule.name for schedule in worker._schedule_source.schedules()]
        assert names == ["r1", "r2", "r3"]
