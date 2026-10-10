"""
A schedule whose tick history holds a value that does not read is skipped,
and every other schedule in the pass is dispatched.

The tick log is read twice on the way to a dispatch: the newest tick of
every schedule in the pass, one grouped statement per slice of keys, and,
for a settings schedule with nothing inside the pass's bound, the earliest
tick, which decides whether this pass is its first sighting. A value in
either that Django's converter cannot turn into a datetime (PostgreSQL's
year 10000, 'infinity' and '-infinity', a date SQLite holds that does not
exist or text that is not a date, a MySQL zero date) belongs to the one
schedule whose key the tick carries. That schedule is skipped with
`schedule_tick_unreadable` and never read as having no history.

A fault in code that escapes the dispatch pass costs that pass and no
more: run() reports it, keeps the dispatch owed, and goes on claiming.

Values are written by SQL, as only SQL can write them, each in the form
the engine that keeps it keeps it. A test for another engine's value
skips and says why.
"""

import logging
import sqlite3
import time
from datetime import timedelta

import pytest
from django.db import (
    DataError,
    Error,
    InterfaceError,
    OperationalError,
    connection,
)
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import worker as worker_module
from django_ox._stored_read import newest_tick_pk
from django_ox.cron import CronExpression
from django_ox.models import OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.schedules import Schedule
from django_ox.stored import create_schedule
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .isolation import daily_cron_at, kill, twelve_hours_ago, with_history
from .test_stored_read import insert_tick, only_on

#: How long a test waits for a worker thread before calling it stuck: a
#: deadlock guard, never a measurement.
GUARD = 30

#: A newest tick that does not read, per engine, as SQL: (engine, literal,
#: why only that engine, USE_TZ). Each sorts after any tick a schedule can
#: be due at, so it is the newest within any bound.
NEWEST = [
    pytest.param(
        "postgresql",
        "'10000-01-01 00:00:00+00'",
        "PostgreSQL keeps a year past 9999",
        True,
        id="postgresql-year-10000",
    ),
    pytest.param(
        "postgresql",
        "'infinity'",
        "PostgreSQL keeps 'infinity'",
        True,
        id="postgresql-infinity",
    ),
    pytest.param(
        "sqlite", "'banana'", "SQLite keeps any text", True, id="sqlite-banana"
    ),
    pytest.param(
        "sqlite",
        "'9999-02-30 00:00:00'",
        "SQLite keeps a date that does not exist",
        True,
        id="sqlite-does-not-exist",
    ),
    pytest.param(
        "sqlite",
        "CAST(X'FF61' AS TEXT)",
        "SQLite keeps text that is not UTF-8",
        True,
        id="sqlite-not-utf-8",
    ),
    pytest.param(
        "sqlite",
        "'9999-01-01 00:00:00+00:00'",
        "SQLite keeps an offset in a column read as naive",
        False,
        id="sqlite-offset-with-use-tz-off",
    ),
    pytest.param(
        "mysql",
        "'9999-00-00 00:00:00'",
        "MySQL keeps a zero month and day",
        True,
        id="mysql-zero-in-date",
    ),
]

#: An earliest tick that does not read, per engine: each sorts before any
#: tick a schedule can be due at, so no bounded read sees it and only the
#: first-sighting read does.
EARLIEST = [
    pytest.param(
        "postgresql",
        "'-infinity'",
        "PostgreSQL keeps '-infinity'",
        id="postgresql-minus-infinity",
    ),
    pytest.param(
        "sqlite",
        "'0000-02-30 00:00:00'",
        "SQLite keeps a year 0 that does not exist",
        id="sqlite-year-0",
    ),
    pytest.param(
        "sqlite",
        "'10000-01-01 00:00:00'",
        "SQLite sorts this text before any date",
        id="sqlite-year-10000",
    ),
    pytest.param(
        "mysql", "'0000-00-00 00:00:00'", "MySQL keeps a zero date", id="mysql-zero"
    ),
]

#: One newest tick that does not read on whichever engine this run is on.
ANY_NEWEST = {
    "postgresql": "'infinity'",
    "sqlite": "'banana'",
    "mysql": "'9999-00-00 00:00:00'",
}

#: And one earliest tick.
ANY_EARLIEST = {
    "postgresql": "'-infinity'",
    "sqlite": "'0000-02-30 00:00:00'",
    "mysql": "'0000-00-00 00:00:00'",
}


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="labelled", task=tasks.labelled))


def tasks_setting(schedules):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
                "SCHEDULES": schedules,
            },
        }
    }


def entry(name, base):
    """A settings schedule daily at `base`'s time of day, labelled `name`."""
    return {"task": "tests.tasks.labelled", "cron": daily_cron_at(base), "args": [name]}


def a_row(name, base):
    """A stored schedule daily at `base`'s time of day, labelled `name`."""
    return create_schedule(
        name=name,
        task_key="labelled",
        trigger="cron",
        cron=daily_cron_at(base),
        arguments={"label": name},
        start_time=timezone.now() - timedelta(days=1),
    )


def label(task):
    return task.args[0] if task.args else task.kwargs.get("label")


def fired():
    return sorted(label(task) for task in OxTask.objects.all())


def ticks_of(key):
    return sorted(
        OxScheduleTick.objects.filter(schedule_name=key).values_list("pk", flat=True)
    )


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def a_pass_with_a_blocked_row(settings, literal, *, use_tz=True):
    """
    A settings schedule and a stored row that fire, and a stored row whose
    newest tick is `literal`. Returns the blocked row and its tick's key.
    """
    settings.USE_TZ = use_tz
    base = twelve_hours_ago()
    settings.TASKS = tasks_setting({"s-healthy": entry("s-healthy", base)})
    with_history("s-healthy")
    a_row("r-healthy", base)
    blocked = a_row("r-blocked", base)
    return blocked, insert_tick(f"db:{blocked.pk}", literal)


@pytest.mark.django_db
class TestAnUnreadableNewestTick:
    @pytest.mark.parametrize(("vendor", "literal", "why", "use_tz"), NEWEST)
    def test_only_its_schedule_is_skipped(
        self, settings, caplog, vendor, literal, why, use_tz
    ):
        only_on(vendor, why)
        blocked, bad = a_pass_with_a_blocked_row(settings, literal, use_tz=use_tz)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert Worker(backoff_initial=0).dispatch_schedules() == 2
        assert fired() == ["r-healthy", "s-healthy"]
        # No candidate tick and no task: the unreadable one is all there is.
        assert ticks_of(f"db:{blocked.pk}") == [bad]
        (skipped,) = events(caplog, "schedule_tick_unreadable")
        assert skipped.schedule == "r-blocked"
        assert skipped.schedule_key == f"db:{blocked.pk}"
        assert skipped.schedule_pk == blocked.pk
        assert skipped.phase == "latest"
        assert skipped.tick_pk == bad
        assert skipped.database == "default"
        assert skipped.reason.startswith("scheduled_for ")
        assert "not treated as absent" in skipped.getMessage()
        assert skipped.exc_info is None
        # Reported once, as what it is, and not as a failed dispatch too.
        assert not events(caplog, "schedule_dispatch_error")

    @pytest.mark.parametrize(("vendor", "literal", "why", "use_tz"), NEWEST)
    def test_the_pass_after_still_skips_it_and_fires_nothing_twice(
        self, settings, vendor, literal, why, use_tz
    ):
        only_on(vendor, why)
        blocked, bad = a_pass_with_a_blocked_row(settings, literal, use_tz=use_tz)
        worker = Worker(backoff_initial=0)
        assert worker.dispatch_schedules() == 2
        assert worker.dispatch_schedules() == 0
        assert fired() == ["r-healthy", "s-healthy"]
        assert ticks_of(f"db:{blocked.pk}") == [bad]


@pytest.mark.django_db
class TestAnUnreadableEarliestTick:
    """
    A settings schedule with nothing inside the pass's bound is asked for
    its earliest tick, inside its dispatch transaction and after its
    candidate tick is written. One that does not read rolls that back: the
    schedule is neither anchored again nor fired.
    """

    @pytest.mark.parametrize(("vendor", "literal", "why"), EARLIEST)
    def test_its_schedule_is_skipped_and_nothing_of_it_is_written(
        self, settings, caplog, vendor, literal, why
    ):
        only_on(vendor, why)
        base = twelve_hours_ago()
        settings.TASKS = tasks_setting(
            {
                "s-anchor": entry("s-anchor", base),
                "s-healthy": entry("s-healthy", base),
            }
        )
        with_history("s-healthy")
        a_row("r-healthy", base)
        bad = insert_tick("s-anchor", literal)
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 2
            assert worker.dispatch_schedules() == 0
        assert fired() == ["r-healthy", "s-healthy"]
        # No anchor, no candidate tick and no latch left behind.
        assert ticks_of("s-anchor") == [bad]
        skipped = events(caplog, "schedule_tick_unreadable")
        assert len(skipped) == 1, "the second pass is counted, not reported again"
        (first,) = skipped
        assert (first.schedule, first.schedule_key) == ("s-anchor", "s-anchor")
        assert first.schedule_pk is None
        assert first.phase == "anchor"
        assert first.tick_pk == bad
        assert not events(caplog, "schedule_dispatch_error")

    def test_once_the_tick_is_removed_the_schedule_anchors_as_one_never_seen(
        self, settings, caplog
    ):
        settings.TASKS = tasks_setting(
            {"s-anchor": entry("s-anchor", twelve_hours_ago())}
        )
        bad = insert_tick("s-anchor", ANY_EARLIEST[connection.vendor])
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
            assert ticks_of("s-anchor") == [bad]
            OxScheduleTick.objects.filter(pk=bad).delete()
            assert worker.dispatch_schedules() == 0
        # With no history left this is a first sighting: the due tick is
        # recorded as the anchor and nothing is fired for it.
        (anchor,) = OxScheduleTick.objects.filter(schedule_name="s-anchor")
        assert anchor.task_id is None
        assert fired() == []
        (readable,) = events(caplog, "schedule_tick_readable")
        assert (readable.schedule, readable.schedule_pk) == ("s-anchor", None)
        assert readable.failures == 1

    def test_a_readable_old_tick_still_anchors_the_schedule_as_before(self, settings):
        # The control: history before the bound that reads is history, and
        # the due tick after it fires.
        base = twelve_hours_ago()
        settings.TASKS = tasks_setting({"s-anchor": entry("s-anchor", base)})
        with_history("s-anchor")
        assert Worker(backoff_initial=0).dispatch_schedules() == 1
        assert fired() == ["s-anchor"]


@pytest.mark.django_db
class TestTheSkipIsReportedLikeAFailure:
    """Once in full, then in summary, then once when the history reads again."""

    def test_first_then_summary_then_readable(self, settings, caplog, monkeypatch):
        blocked, bad = a_pass_with_a_blocked_row(
            settings, ANY_NEWEST[connection.vendor]
        )
        worker = Worker(backoff_initial=0)
        key = f"db:{blocked.pk}"
        with caplog.at_level(logging.INFO, logger="django_ox"):
            worker.dispatch_schedules()
            worker.dispatch_schedules()
            assert len(events(caplog, "schedule_tick_unreadable")) == 1
            monkeypatch.setattr(worker_module, "DISPATCH_FAILURE_REPORT_INTERVAL", 0)
            worker.dispatch_schedules()
            first, summary = events(caplog, "schedule_tick_unreadable")
            assert (first.failures, first.suppressed) == (1, 0)
            assert (summary.failures, summary.suppressed) == (3, 2)
            assert summary.getMessage().startswith("Schedule r-blocked still skipped")
            # The tick removed by its key, as only its key can remove it.
            OxScheduleTick.objects.filter(pk=bad).delete()
            assert worker.dispatch_schedules() == 1, "the row fires again"
        (readable,) = events(caplog, "schedule_tick_readable")
        assert (readable.schedule, readable.schedule_key) == ("r-blocked", key)
        assert readable.failures == 3
        assert fired() == ["r-blocked", "r-healthy", "s-healthy"]


def tick_reads(queries):
    """The statements of `queries` that read the tick log."""
    return [
        q["sql"]
        for q in queries.captured_queries
        if q["sql"].lstrip().upper().startswith("SELECT")
        and "django_ox_oxscheduletick" in q["sql"]
    ]


# Autocommit, as a worker reads.
@pytest.mark.django_db(transaction=True)
class TestTheUnreadableTicksRowIsNamedOnlyWhereALineIsWritten:
    """
    Which tick row it is appears in the line a skip writes, at most once a
    minute for a schedule. Passes that skip the schedule and write no line
    read the tick log once, as a pass with nothing unreadable does.
    """

    SKIPPED = 3

    def blocked_rows(self, settings):
        settings.TASKS = tasks_setting({})
        base = twelve_hours_ago()
        with_history("unrelated")
        rows = [a_row(f"r-{index}", base) for index in range(self.SKIPPED)]
        ticks = {
            row.pk: insert_tick(f"db:{row.pk}", ANY_NEWEST[connection.vendor])
            for row in rows
        }
        return rows, ticks

    def test_a_pass_that_writes_the_lines_looks_each_row_up_and_the_next_does_not(
        self, settings, caplog, monkeypatch
    ):
        _, ticks = self.blocked_rows(settings)
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            with CaptureQueriesContext(connection) as first:
                worker.dispatch_schedules()
            with CaptureQueriesContext(connection) as quiet:
                worker.dispatch_schedules()
                worker.dispatch_schedules()
            monkeypatch.setattr(worker_module, "DISPATCH_FAILURE_REPORT_INTERVAL", 0)
            with CaptureQueriesContext(connection) as summary:
                worker.dispatch_schedules()

        # The grouped read, and one lookup for each line written.
        assert len(tick_reads(first)) == 1 + self.SKIPPED
        # Two passes, one grouped read each, and no lookup.
        assert len(tick_reads(quiet)) == 2
        assert len(tick_reads(summary)) == 1 + self.SKIPPED
        lines = events(caplog, "schedule_tick_unreadable")
        assert len(lines) == 2 * self.SKIPPED
        assert {line.schedule_pk: line.tick_pk for line in lines} == ticks
        assert all(line.phase == "latest" for line in lines)

    def test_a_lookup_that_fails_leaves_the_skip_and_the_line_as_they_were(
        self, settings, caplog, monkeypatch
    ):
        rows, ticks = self.blocked_rows(settings)

        def refused(*args, **kwargs):
            raise OperationalError("the lookup was refused")

        monkeypatch.setattr(worker_module, "newest_tick_pk", refused)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            Worker(backoff_initial=0).dispatch_schedules()

        lines = events(caplog, "schedule_tick_unreadable")
        assert sorted(line.schedule_pk for line in lines) == sorted(ticks)
        assert {line.tick_pk for line in lines} == {None}
        assert all("not treated as absent" in line.getMessage() for line in lines)
        # Skipped, as when the row is named: nothing fired and nothing written.
        assert fired() == []
        for row in rows:
            assert ticks_of(f"db:{row.pk}") == [ticks[row.pk]]


def many_schedules(count):
    return [
        Schedule(
            name=f"s{index:04d}",
            task=tasks.add,
            trigger=CronExpression("* * * * *"),
            args=(),
            kwargs={},
        )
        for index in range(count)
    ]


def seed(schedules, at, every=7):
    for schedule in schedules[::every]:
        OxScheduleTick.objects.create(
            schedule_name=schedule.key, scheduled_for=at, task_id=None, created_at=at
        )
    return {schedule.key: at for schedule in schedules[::every]}


# Autocommit, as a worker reads.
@pytest.mark.django_db(transaction=True)
class TestTheHealthyReadIsOneStatementPerSlice:
    def test_beyond_sqlite_3_31s_limit(self, settings):
        only_on("sqlite", "SQLite's parameter limit")
        settings.TASKS = tasks_setting({})
        schedules = many_schedules(1200)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = seed(schedules, at)
        connection.ensure_connection()
        raw = connection.connection
        before = raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
        raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        try:
            assert connection.features.max_query_params == 999
            worker = Worker(backoff_initial=0)
            with CaptureQueriesContext(connection) as captured:
                latest = worker._latest_ticks(schedules, at - timedelta(days=1))
        finally:
            raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, before)
        # 1200 keys at 998 a statement: two statements and nothing else, no
        # savepoint and no read per key.
        statements = [q["sql"] for q in captured.captured_queries]
        assert len(statements) == 2, statements
        assert all("MAX(" in sql.upper() for sql in statements)
        assert {key: read.at for key, read in latest.items()} == expected

    def test_in_slices_of_the_connections_limit(self, settings, monkeypatch):
        settings.TASKS = tasks_setting({})
        schedules = many_schedules(10)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = seed(schedules, at, every=3)
        # Three keys and the bound per statement. On the class, and the
        # instance's cached value dropped, as test_schedules does it.
        monkeypatch.setattr(type(connection.features), "max_query_params", 4)
        connection.features.__dict__.pop("max_query_params", None)
        worker = Worker(backoff_initial=0)
        with CaptureQueriesContext(connection) as captured:
            latest = worker._latest_ticks(schedules, at - timedelta(days=1))
        statements = [q["sql"] for q in captured.captured_queries]
        assert len(statements) == 4, statements
        assert {key: read.at for key, read in latest.items()} == expected


@pytest.mark.django_db
class TestAnUnreadableKeyAnywhereInASlice:
    """
    One key's newest tick that does not read, first, in the middle or last
    of a slice, costs that key alone. Most such values are decoded per key
    from the one grouped statement; text SQLite cannot hand over as UTF-8
    fails the statement, which is read again in halves down to the key.
    """

    VALUES = [
        pytest.param("postgresql", "'infinity'", id="postgresql-infinity"),
        pytest.param("sqlite", "'9999-02-30 00:00:00'", id="sqlite-does-not-exist"),
        pytest.param("sqlite", "CAST(X'FF61' AS TEXT)", id="sqlite-not-utf-8"),
        pytest.param("mysql", "'9999-00-00 00:00:00'", id="mysql-zero-in-date"),
    ]

    @pytest.mark.parametrize("position", [0, 3, 6], ids=["first", "middle", "last"])
    @pytest.mark.parametrize(("vendor", "literal"), VALUES)
    def test_every_other_key_is_answered(self, settings, vendor, literal, position):
        only_on(vendor, f"a value only {vendor} keeps")
        settings.TASKS = tasks_setting({})
        schedules = many_schedules(7)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = seed(schedules, at, every=1)
        bad_key = schedules[position].key
        bad = insert_tick(bad_key, literal)
        latest = Worker(backoff_initial=0)._latest_ticks(
            schedules, at - timedelta(days=1)
        )
        assert latest[bad_key].unreadable
        # Which row it is is looked up where a line names it, not here.
        assert latest[bad_key].pk is None
        assert newest_tick_pk(bad_key, at - timedelta(days=1), using="default") == bad
        del expected[bad_key]
        assert {
            key: read.at for key, read in latest.items() if key != bad_key
        } == expected


def failing_before(marker, how):
    """
    An execute wrapper that fails the first statement on the tick log whose
    SQL carries `marker`: by losing the connection for real (`kill`), or by
    raising an error the database raises that is not about any value
    (`refuse`).
    """
    met = []

    def wrapper(execute, sql, params, many, context):
        if not met and "oxscheduletick" in sql and marker in sql:
            met.append(sql)
            if how == "kill":
                kill(context["connection"])
            else:
                raise OperationalError("database disk image is malformed")
        return execute(sql, params, many, context)

    return wrapper, met


@pytest.mark.django_db(transaction=True)
class TestAFailedReadIsNotAnUnreadableTick:
    """
    A read of the tick log that fails because the connection went, or
    because the database refused it for a reason that is not a value, is
    the database's and never a tick that does not read. The shared read of
    the newest ticks abandons the pass for run() to report and reconnect.
    The earliest-tick read is inside one schedule's transaction, where the
    connection decides as for any statement there: lost, the pass is
    abandoned; still answering, it is that schedule's dispatch error.
    """

    @pytest.mark.parametrize("how", ["kill", "refuse"])
    @pytest.mark.parametrize(
        ("marker", "schedules"),
        [
            pytest.param("MAX(", "stored", id="newest"),
            pytest.param("ox_stored_scheduled_for", "settings", id="earliest"),
        ],
    )
    def test_no_schedule_is_skipped_for_it(
        self, settings, caplog, marker, schedules, how
    ):
        if how == "kill" and connection.vendor == "sqlite":
            pytest.skip(
                "SQLite has no connection to lose; closing its handle under "
                "Django is not a shape a worker meets"
            )
        base = twelve_hours_ago()
        if schedules == "stored":
            settings.TASKS = tasks_setting({})
            a_row("r-due", base)
        else:
            settings.TASKS = tasks_setting({"s-new": entry("s-new", base)})
        worker = Worker(backoff_initial=0)
        wrapper, met = failing_before(marker, how)
        abandoned = how == "kill" or schedules == "stored"
        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(wrapper),
        ):
            if abandoned:
                with pytest.raises(Error):
                    worker.dispatch_schedules()
            else:
                assert worker.dispatch_schedules() == 0
        assert met, "the read was never made"
        assert not events(caplog, "schedule_tick_unreadable")
        reported = [r.schedule for r in events(caplog, "schedule_dispatch_error")]
        assert reported == ([] if abandoned else ["s-new"])
        connection.close()
        assert not OxTask.objects.exists()
        assert not OxScheduleTick.objects.exists()


# -- run(): a fault in code costs the pass ------------------------------------


class _Halt(BaseException):
    """Not an Exception: what run() must let through."""


@pytest.fixture
def due_worker(settings):
    """A worker with one settings schedule due, and history so it fires."""
    base = twelve_hours_ago()
    settings.TASKS = tasks_setting({"s-due": entry("s-due", base)})
    with_history("s-due")
    return Worker(
        backoff_initial=0, poll_interval=0.02, reap_interval=0.0, schedule_interval=0.1
    )


def a_reader_that_fails(worker, monkeypatch, error, times=1):
    """
    Make the schedule source's answer raise `error` on the first `times`
    dispatch passes, or on every one when `times` is None: a shared read of
    the pass that nothing guards, as a reader added later might be. The
    worker has asked it once already, when it was built. Returns the calls.
    """
    source = worker._schedule_source
    real = source.schedules
    calls = []

    def schedules():
        calls.append(time.monotonic())
        if times is None or len(calls) <= times:
            raise error("a shared read of the pass failed")
        return real()

    monkeypatch.setattr(source, "schedules", schedules)
    return calls


def run_until(worker, predicate, caplog):
    with caplog.at_level(logging.INFO, logger="django_ox"):
        thread = start_worker_thread(worker)
        reached = wait_for(lambda: predicate() or not thread.is_alive(), timeout=GUARD)
        alive = thread.is_alive()
        worker.request_stop()
        thread.join(timeout=GUARD)
    assert reached, "the worker never got there"
    assert alive, "run() ended on its own"
    assert not thread.is_alive(), "run() did not stop when asked"


def all_succeeded(count):
    return lambda: (
        OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() >= count
    )


@pytest.mark.django_db(transaction=True)
class TestAFaultInADispatchReaderCostsThePass:
    @pytest.mark.parametrize(
        "error",
        [AttributeError, TypeError, ValueError, KeyError, RuntimeError, LookupError],
    )
    def test_the_worker_claims_and_then_dispatches(
        self, due_worker, monkeypatch, caplog, error
    ):
        queued = tasks.add.enqueue(1, 2)
        calls = a_reader_that_fails(due_worker, monkeypatch, error)
        run_until(due_worker, all_succeeded(2), caplog)
        assert OxTask.objects.get(id=queued.id).status == OxTask.Status.SUCCESSFUL
        assert OxTask.objects.filter(args=["s-due"]).exists()
        assert len(calls) >= 2
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.category == "unexpected_exception"
        assert failed.error == error.__name__
        assert failed.exc_info is not None, "the first is reported in full"
        assert "unexpected" in failed.getMessage()
        (resumed,) = events(caplog, "schedule_dispatch_resumed")
        assert resumed.failures == 1
        assert caplog.records.index(resumed) > caplog.records.index(failed)

    def test_one_that_keeps_failing_costs_each_pass_and_no_more(
        self, due_worker, monkeypatch, caplog
    ):
        queued = tasks.add.enqueue(1, 2)
        calls = a_reader_that_fails(due_worker, monkeypatch, AttributeError, None)
        started = time.monotonic()
        run_until(
            due_worker,
            lambda: all_succeeded(1)() and len(calls) >= 4,
            caplog,
        )
        elapsed = time.monotonic() - started
        assert OxTask.objects.get(id=queued.id).status == OxTask.Status.SUCCESSFUL
        # At most one attempt a schedule_interval, and a slack of two: a
        # loop retrying at once would make far more in the same time.
        assert len(calls) <= elapsed / due_worker.schedule_interval + 2, (
            len(calls),
            elapsed,
        )
        # In full once, and no summary inside the report interval.
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.category == "unexpected_exception"
        assert not events(caplog, "schedule_dispatch_resumed")

    def test_it_holds_a_batch_open_and_the_queue_still_runs(
        self, settings, monkeypatch, caplog
    ):
        settings.TASKS = tasks_setting({})
        queued = tasks.add.enqueue(1, 2)
        worker = Worker(
            backoff_initial=0,
            poll_interval=0.02,
            reap_interval=0.0,
            schedule_interval=0.1,
            batch=True,
        )
        calls = a_reader_that_fails(worker, monkeypatch, AttributeError, None)
        started = time.monotonic()
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            # The queue drained and the dispatch failed again and again: a
            # batch that could end on a failed dispatch has had the chance.
            wait_for(
                lambda: (
                    (all_succeeded(1)() and len(calls) >= 4) or not thread.is_alive()
                ),
                timeout=GUARD,
            )
            alive = thread.is_alive()
            elapsed = time.monotonic() - started
            worker.request_stop()
            thread.join(timeout=GUARD)
        assert alive, "the batch ended with a dispatch still owed"
        assert OxTask.objects.get(id=queued.id).status == OxTask.Status.SUCCESSFUL
        # Tried again at most once a schedule_interval, with a slack of two.
        assert len(calls) <= elapsed / worker.schedule_interval + 2, (
            len(calls),
            elapsed,
        )
        assert not events(caplog, "worker_batch_empty")

    @pytest.mark.parametrize("error", [OperationalError, InterfaceError, DataError])
    def test_a_database_error_is_the_databases(
        self, due_worker, monkeypatch, caplog, error
    ):
        a_reader_that_fails(due_worker, monkeypatch, error)
        run_until(due_worker, all_succeeded(1), caplog)
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.category == "database"
        assert failed.error == error.__name__
        assert "unexpected" not in failed.getMessage()
        # Inside the dispatch handler, so the claim still ran that pass:
        # not the poll handler's, which loses the claim with it.
        assert not events(caplog, "worker_poll_failed")
        assert events(caplog, "schedule_dispatch_resumed")

    @pytest.mark.parametrize("error", [_Halt, SystemExit])
    def test_an_exception_that_is_not_an_exception_still_ends_run(
        self, due_worker, monkeypatch, error
    ):
        a_reader_that_fails(due_worker, monkeypatch, error)
        with pytest.raises(error):
            due_worker.run()


def test_the_report_keeps_the_categories_apart(caplog):
    # A fault in code during an outage is still reported in full.
    report = worker_module._DispatchReport("w", "default", clock=lambda: 0.0)
    with caplog.at_level(logging.INFO, logger="django_ox"):
        report.pass_failed(OperationalError("gone"))
        report.pass_failed(OperationalError("gone"))
        report.pass_failed(AttributeError("x"), unexpected=True)
        report.pass_completed()
        report.pass_completed()
    failed = events(caplog, "schedule_dispatch_failed")
    assert [(r.category, r.exc_info is not None) for r in failed] == [
        ("database", True),
        ("unexpected_exception", True),
    ]
    (resumed,) = events(caplog, "schedule_dispatch_resumed")
    assert resumed.failures == 3
