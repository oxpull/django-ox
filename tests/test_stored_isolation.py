"""
One stored row a worker cannot use cannot stop the worker.

A worker reads every stored row on each full read, and on PostgreSQL,
SQLite and MySQL there are values a row can hold that the read cannot turn
into Python, or that the dispatch pass cannot compare with its clock:

- PostgreSQL keeps a timestamp past year 9999 or before year 1, and
  'infinity', and psycopg refuses to read any of them back (DataError).
  `create_schedule` and `update_schedule` accepted an end of 9999-12-31
  20:00 in New York, which is year 10000 in UTC.
- SQLite keeps whatever it is given: a date that does not exist, text in an
  integer column. The converter or the digest raised ValueError or
  ValidationError.
- MySQL keeps a zero date, which PyMySQL hands back as text, and Django's
  converter raised AttributeError.
- SQLite, with USE_TZ off, keeps a time with an offset, which the dispatch
  pass then compared with its naive clock and raised TypeError.

Each of those stopped the source for every row, so `Worker()` could not be
built and a running worker died at its next full read, or abandoned every
dispatch pass. Now the writers refuse such a bound, the read converts and
checks each row inside that row's own handling, and the dispatch pass
derives each schedule's tick and compares its bounds inside that
schedule's. The row is left out and logged, named by its key; the database
failing is still the database failing.

The values are written by SQL, as only SQL can write them. Each is how one
engine holds it, and a test for another engine's value skips and says why.
"""

import logging
import re
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    connection,
    transaction,
)
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule, OxScheduleChange, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.schedules import IntervalTrigger, Schedule
from django_ox.stored import (
    DatabaseScheduleSource,
    _export_preflight,
    create_schedule,
    create_schedules,
    delete_schedule,
    update_schedule,
)
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .test_stored_read import Refusing, largest_count

ADD = "admin:django_ox_oxschedule_add"
CHANGE = "admin:django_ox_oxschedule_change"
TABLE = "django_ox_oxschedule"
MARKER = "django_ox_oxschedulechange"


def stored_tasks(**options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
                **options,
            },
        }
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.record))


@pytest.fixture
def operator(client):
    client.force_login(User.objects.create_superuser("root", "root@example.com", "pw"))
    return client


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def a_row(name, **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "interval",
        "every_seconds": 60,
        "arguments": {"label": name},
        "start_time": timezone.now() - timedelta(minutes=10),
    }
    fields.update(over)
    return create_schedule(**fields)


def write_by_sql(table, column, literal, pk=None):
    """
    Put a value in a column the way only SQL can.

    `literal` is SQL, quoted as the engine reads it. MySQL refuses a zero
    date in the strict mode the suite runs in, so its session is let off
    for the one statement and put back.
    """
    where = "" if pk is None else f" WHERE id = {int(pk)}"
    statement = f"UPDATE {table} SET {column} = {literal}{where}"  # noqa: S608
    with connection.cursor() as cursor:
        if connection.vendor != "mysql":
            cursor.execute(statement)
            return
        cursor.execute("SELECT @@SESSION.sql_mode")
        (mode,) = cursor.fetchone()
        cursor.execute("SET SESSION sql_mode = ''")
        try:
            cursor.execute(statement)
        finally:
            cursor.execute("SET SESSION sql_mode = %s", [mode])


def only_on(vendor, why):
    if connection.vendor != vendor:
        pytest.skip(f"a {vendor} value: {why}; this run is on {connection.vendor}")


#: (engine, column, SQL literal, why only that engine). Every value known to
#: stop the read, as the engine that keeps it keeps it.
UNREADABLE = [
    pytest.param(
        "sqlite",
        "every_seconds",
        "'abc'",
        "SQLite keeps text in an integer column",
        id="text in the interval",
    ),
    pytest.param(
        "sqlite",
        "phase_seconds",
        "'abc'",
        "SQLite keeps text in an integer column",
        id="text in the phase",
    ),
    pytest.param(
        "sqlite",
        "start_time",
        "'2026-02-30 00:00:00'",
        "SQLite keeps a date that does not exist",
        id="a start that does not exist",
    ),
    pytest.param(
        "sqlite",
        "end_time",
        "'2026-02-30 00:00:00'",
        "SQLite keeps a date that does not exist",
        id="an end that does not exist",
    ),
    pytest.param(
        "sqlite",
        "created_at",
        "'2026-02-30 00:00:00'",
        "SQLite keeps a date that does not exist",
        id="a creation time that does not exist",
    ),
    pytest.param(
        "sqlite",
        "task_key",
        "CAST(X'FF' AS TEXT)",
        "SQLite keeps text that is not UTF-8, and its driver cannot decode it",
        id="text that is not utf-8",
    ),
    pytest.param(
        "sqlite",
        "task_key",
        "X'626c6f62'",
        "SQLite keeps bytes in a text column",
        id="bytes as the task key",
    ),
    pytest.param(
        "postgresql",
        "end_time",
        "'infinity'",
        "PostgreSQL keeps infinity, which no datetime holds",
        id="an end of infinity",
    ),
    pytest.param(
        "postgresql",
        "start_time",
        "'-infinity'",
        "PostgreSQL keeps -infinity, which no datetime holds",
        id="a start of minus infinity",
    ),
    pytest.param(
        "postgresql",
        "end_time",
        "'10000-01-01 00:00:00+00'",
        "PostgreSQL keeps year 10000, which no datetime holds",
        id="an end in year 10000",
    ),
    pytest.param(
        "postgresql",
        "start_time",
        "'0001-01-01 00:00:00+00 BC'",
        "PostgreSQL keeps a date before year 1, which no datetime holds",
        id="a start before year 1",
    ),
    pytest.param(
        "mysql",
        "end_time",
        "'0000-00-00 00:00:00'",
        "MySQL keeps a zero date outside strict mode, and PyMySQL reads it as text",
        id="an end of zero",
    ),
    pytest.param(
        "mysql",
        "start_time",
        "'0000-00-00 00:00:00'",
        "MySQL keeps a zero date outside strict mode, and PyMySQL reads it as text",
        id="a start of zero",
    ),
]


#: One value per engine, for the worker that is already running.
WHILE_RUNNING = [
    pytest.param(
        "sqlite",
        "end_time",
        "'2026-02-30 00:00:00'",
        "SQLite keeps a date that does not exist",
        id="sqlite",
    ),
    pytest.param(
        "postgresql",
        "end_time",
        "'infinity'",
        "PostgreSQL keeps infinity",
        id="postgresql",
    ),
    pytest.param(
        "mysql",
        "end_time",
        "'0000-00-00 00:00:00'",
        "MySQL keeps a zero date",
        id="mysql",
    ),
]


#: The change marker, the same way, on each engine.
UNREADABLE_MARKER = [
    pytest.param(
        "sqlite", "'2026-02-30 00:00:00'", "SQLite keeps a date that does not exist"
    ),
    pytest.param("postgresql", "'infinity'", "PostgreSQL keeps infinity"),
    pytest.param("mysql", "'0000-00-00 00:00:00'", "MySQL keeps a zero date"),
]


# -- the writers -------------------------------------------------------------


#: Two directions over the edge of what a database keeps: an end in a zone
#: west of UTC on the last day of year 9999, which is year 10000 in UTC, and
#: a start in a zone east of it on the first day of year 1, which is year 0.
PAST_THE_EDGE = [
    pytest.param(
        "end_time",
        datetime(9999, 12, 31, 20, 0, tzinfo=ZoneInfo("America/New_York")),
        id="an end in year 10000 UTC",
    ),
    pytest.param(
        "start_time",
        datetime(1, 1, 1, 3, 0, tzinfo=ZoneInfo("Asia/Kolkata")),
        id="a start in year 0 UTC",
    ),
]


@pytest.mark.django_db
class TestEveryWriterRefusesATimeTheDatabaseCannotKeep:
    """
    Refused before anything is written, on every engine. PostgreSQL stored
    such a time and could not read it back; SQLite and MySQL raised
    OverflowError at the write, out of `create_schedule` and as an error
    page from the admin.
    """

    said = stored._OUTSIDE_THE_STORABLE_YEARS

    @pytest.fixture(autouse=True)
    def _use_tz(self, settings):
        # The years are asked with USE_TZ on; the last test turns it off.
        settings.USE_TZ = True

    @pytest.mark.parametrize(("field", "value"), PAST_THE_EDGE)
    def test_creating_a_schedule(self, field, value):
        with pytest.raises(ValidationError) as caught:
            a_row("past-the-edge", **{field: value})
        assert caught.value.message_dict == {field: [self.said]}
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists()

    @pytest.mark.parametrize(("field", "value"), PAST_THE_EDGE)
    def test_creating_a_batch_writes_nothing(self, field, value):
        fine = {"name": "fine", "task_key": "report", "trigger": "interval"}
        rows = [
            {**fine, "every_seconds": 60},
            {**fine, "name": "past-the-edge", "every_seconds": 60, field: value},
        ]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        (entry,) = caught.value.error_list
        assert entry.params["index"] == 1
        assert entry.params["field"] == field
        assert entry.params["message"] == self.said
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists()

    def test_updating_a_schedule_changes_nothing(self):
        row = a_row("kept")
        before = OxSchedule.objects.get(pk=row.pk)
        marker = OxScheduleChange.objects.get().changed_at
        end = datetime(9999, 12, 31, 20, 0, tzinfo=ZoneInfo("America/New_York"))
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, end_time=end)
        assert caught.value.message_dict == {"end_time": [self.said]}
        after = OxSchedule.objects.get(pk=row.pk)
        assert (after.end_time, after.updated_at) == (None, before.updated_at)
        assert OxScheduleChange.objects.get().changed_at == marker

    @pytest.mark.parametrize(("field", "value"), PAST_THE_EDGE)
    def test_the_importer_s_preflight(self, field, value):
        fields = {
            "name": "past-the-edge",
            "task_key": "report",
            "trigger": "interval",
            "every_seconds": 60,
            field: value,
        }
        assert _export_preflight(fields) == [(field, self.said)]

    def test_the_admin_s_add_and_change_forms(self, operator, settings):
        # The form reads the time in the current time zone, as a person in
        # New York typing the last evening of year 9999 means it.
        settings.TIME_ZONE = "America/New_York"
        posted = {
            "name": "past-the-edge",
            "task_key": "report",
            "trigger": "interval",
            "cron": "",
            "every_seconds": "60",
            "phase_seconds": "0",
            "arguments": "{}",
            "enabled": "on",
            "end_time_0": "9999-12-31",
            "end_time_1": "20:00:00",
            "starting_deadline_seconds": "",
        }
        response = operator.post(reverse(ADD), posted)
        assert response.status_code == 200
        form = response.context["adminform"].form
        assert form.errors == {"end_time": [self.said]}
        assert not OxSchedule.objects.exists()
        row = a_row("kept")
        response = operator.post(reverse(CHANGE, args=[row.pk]), posted)
        assert response.status_code == 200
        assert response.context["adminform"].form.errors == {"end_time": [self.said]}
        assert OxSchedule.objects.get(pk=row.pk).end_time is None

    def test_a_naive_time_is_read_in_the_default_time_zone(self, settings):
        # As the field reads it when it saves: with USE_TZ on, a naive time
        # is local to TIME_ZONE. 23:00 on the last day in Chicago is past
        # year 9999 in UTC, and the same in UTC is not.
        settings.TIME_ZONE = "America/Chicago"
        naive = {
            "start_time": datetime(2020, 1, 1),
            "end_time": datetime(9999, 12, 31, 23),
        }
        with pytest.raises(ValidationError) as caught:
            a_row("naive", **naive)
        assert caught.value.message_dict == {"end_time": [self.said]}
        settings.TIME_ZONE = "UTC"
        a_row("naive", **naive)

    def test_the_last_and_first_instants_in_utc_are_kept_and_read_back(self):
        # Inside the edge on both sides: stored, read back and built.
        last = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
        first = datetime(1, 1, 1, 0, 0, tzinfo=UTC)
        row = a_row("edges", start_time=first, end_time=last)
        back = OxSchedule.objects.get(pk=row.pk)
        assert (back.start_time, back.end_time) == (first, last)
        built = DatabaseScheduleSource({}, "default").schedules()
        assert [schedule.name for schedule in built] == ["edges"]

    def test_the_zone_the_database_keeps_times_in_is_the_one_asked(self, monkeypatch):
        # SQLite and MySQL keep a time in the database's own zone when it
        # names one. 20:00 UTC on the last day is year 10000 in Kolkata,
        # and the write would raise OverflowError there.
        class Kolkata:
            timezone = ZoneInfo("Asia/Kolkata")

        monkeypatch.setattr(stored, "connections", {"default": Kolkata()})
        late = datetime(9999, 12, 31, 20, 0, tzinfo=UTC)
        assert stored._outside_the_storable_years(late, "default")
        assert not stored._outside_the_storable_years(
            late - timedelta(hours=6), "default"
        )

    def test_with_use_tz_off_a_naive_time_is_kept_as_it_is(self, settings):
        # Stored as written on SQLite and MySQL, and on PostgreSQL written
        # and read back in TIME_ZONE, so the last instant fits everywhere.
        settings.USE_TZ = False
        settings.TIME_ZONE = "America/New_York"
        row = a_row(
            "naive-edge",
            start_time=datetime(1, 1, 1, 0, 0),
            end_time=datetime(9999, 12, 31, 23, 0),
        )
        back = OxSchedule.objects.get(pk=row.pk)
        assert back.end_time == datetime(9999, 12, 31, 23, 0)


@pytest.mark.django_db
class TestWithUseTzOffATimeWithAZoneIsRefused:
    """
    With USE_TZ off a worker compares naive times, so a bound with a zone
    raised TypeError in the dispatch pass. SQLite and MySQL refused it at
    the write with ValueError; PostgreSQL kept it.
    """

    @pytest.fixture(autouse=True)
    def _naive(self, settings):
        settings.USE_TZ = False

    def test_on_its_own(self):
        # A start is the bound that can stand alone: every writer gives a
        # row one, so an end always has a start beside it.
        start = datetime(2030, 1, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
        with pytest.raises(ValidationError) as caught:
            a_row("zoned", start_time=start)
        assert caught.value.message_dict == {
            "start_time": [stored._TIME_ZONE_WHILE_USE_TZ_IS_OFF]
        }
        assert not OxSchedule.objects.exists()

    def test_both_and_not_compared_with_each_other(self):
        start = datetime(2030, 1, 2, tzinfo=UTC)
        end = datetime(2030, 1, 1, tzinfo=UTC)
        with pytest.raises(ValidationError) as caught:
            a_row("zoned", start_time=start, end_time=end)
        assert caught.value.message_dict == {
            "start_time": [stored._TIME_ZONE_WHILE_USE_TZ_IS_OFF],
            "end_time": [stored._TIME_ZONE_WHILE_USE_TZ_IS_OFF],
        }

    def test_beside_a_naive_one_it_is_still_the_pair_s_message(self):
        # The rule for a mixed pair is said once, not twice.
        with pytest.raises(ValidationError) as caught:
            a_row(
                "mixed",
                start_time=datetime(2020, 1, 1),
                end_time=datetime(2030, 1, 1, tzinfo=UTC),
            )
        assert caught.value.message_dict == {"end_time": [stored._TIME_WITH_A_ZONE]}

    def test_through_the_batch_and_the_preflight(self):
        fields = {
            "name": "zoned",
            "task_key": "report",
            "trigger": "interval",
            "every_seconds": 60,
            "start_time": datetime(2030, 1, 1, tzinfo=UTC),
        }
        assert _export_preflight(fields) == [
            ("start_time", stored._TIME_ZONE_WHILE_USE_TZ_IS_OFF)
        ]
        with pytest.raises(ValidationError):
            create_schedules([fields])
        assert not OxSchedule.objects.exists()


# -- the read ----------------------------------------------------------------


@pytest.mark.django_db
class TestARowTheReadCannotConvertIsLeftOut:
    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), UNREADABLE)
    def test_the_source_reads_the_rest_and_a_worker_is_built(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, column, literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            worker = Worker(backoff_initial=0)
            fired = worker.dispatch_schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        assert fired == 1, "the schedule beside it fires"
        skipped = events(caplog, "schedule_row_skipped")
        assert {record.schedule_pk for record in skipped} == {victim.pk}
        # Named, and the reason says which column would not convert.
        first = skipped[0]
        assert first.schedule == "victim"
        assert first.reason.startswith(f"its {column} ")
        # The row still exists as far as the worker is concerned.
        assert f"db:{victim.pk}" in worker._schedule_source._stored_keys()

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), UNREADABLE)
    def test_a_paused_row_is_left_out_without_a_word(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        victim = a_row("victim", enabled=False)
        write_by_sql(TABLE, column, literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert DatabaseScheduleSource({}, "default").schedules() == []
        assert not events(caplog, "schedule_row_skipped")

    def test_a_row_whose_name_cannot_be_read_is_named_by_its_key(
        self, settings, caplog
    ):
        only_on("sqlite", "SQLite keeps text that is not UTF-8")
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, "name", "CAST(X'FF61' AS TEXT)", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule, skipped.schedule_pk) == (None, victim.pk)
        assert skipped.getMessage().startswith(
            f"Skipping stored schedule pk {victim.pk}: "
        )
        assert skipped.reason.startswith("its name could not be read: ")

    def test_a_row_whose_name_is_bytes_is_named_by_its_key(self, settings, caplog):
        only_on("sqlite", "SQLite keeps bytes in a text column")
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, "name", "X'626c6f622d6e616d65'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            assert Worker(backoff_initial=0).dispatch_schedules() == 1
        assert [schedule.name for schedule in built] == ["beside-it"]
        skipped = events(caplog, "schedule_row_skipped")
        assert {record.schedule_pk for record in skipped} == {victim.pk}
        assert {record.schedule for record in skipped} == {None}
        assert {record.reason for record in skipped} == {
            "its name holds b'blob-name', which is not text"
        }

    def test_two_bad_rows_among_many_are_each_found(self, settings, caplog):
        # The read halves a range of keys that does not read; every row
        # that does is still used, in key order.
        only_on("sqlite", "SQLite keeps a date that does not exist")
        settings.TASKS = stored_tasks()
        rows = [a_row(f"row-{i:02d}") for i in range(12)]
        for bad in (rows[3], rows[10]):
            write_by_sql(TABLE, "end_time", "'2026-02-30 00:00:00'", bad.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
        assert [s.name for s in built] == [
            row.name for row in rows if row not in (rows[3], rows[10])
        ]
        skipped = events(caplog, "schedule_row_skipped")
        assert sorted(r.schedule_pk for r in skipped) == [rows[3].pk, rows[10].pk]
        assert {r.schedule for r in skipped} == {"row-03", "row-10"}

    def test_a_row_written_around_validation_with_a_zone_and_use_tz_off(
        self, settings, caplog
    ):
        only_on("sqlite", "SQLite keeps an offset in a column read as naive")
        settings.USE_TZ = False
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, "start_time", "'2020-01-01 00:00:00+05:00'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            fired = Worker(backoff_initial=0).dispatch_schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        assert fired == 1
        # Once by each source: the one built here and the worker's own.
        skipped = events(caplog, "schedule_row_skipped")
        assert len(skipped) == 2
        assert {(r.schedule_pk, r.reason) for r in skipped} == {
            (
                victim.pk,
                (
                    "its start_time holds '2020-01-01 00:00:00+05:00', which has "
                    "a time zone, and USE_TZ is off"
                ),
            )
        }

    def test_a_bound_that_is_not_a_time_or_has_no_zone_with_use_tz_on(self, settings):
        # What the converters never hand back, and a row built in memory can.
        settings.USE_TZ = True
        source = DatabaseScheduleSource({}, "default")
        naive = OxSchedule(
            name="naive",
            task_key="report",
            trigger="interval",
            every_seconds=60,
            end_time=datetime(2030, 1, 1),
        )
        with pytest.raises(ValueError, match="no time zone"):
            source._to_schedule(naive)
        naive.end_time = "tomorrow"
        with pytest.raises(ValueError, match="is not a date and time"):
            source._to_schedule(naive)


# -- the read under the row's lock, at dispatch -------------------------------


def a_sighting(settings):
    """
    A worker's source that has met `victim` at dispatch with timing its
    boundary was not set for, changed by a route that moves no boundary:
    the source, holding the sighting its next read heals, and the row.
    """
    settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
    a_row("beside-it")
    victim = a_row("victim")
    worker = Worker(backoff_initial=0)
    source = worker._schedule_source
    source.schedules()
    OxSchedule.objects.filter(pk=victim.pk).update(every_seconds=120)
    worker.dispatch_schedules()
    assert victim.pk in source._needs_heal
    return source, victim


@pytest.mark.django_db
class TestAtDispatchARowChangedSinceTheSnapshot:
    """
    A worker dispatches from a snapshot and reads each due row again under
    its lock. A row changed by SQL after the snapshot is met there first,
    before any full read has seen it.
    """

    def _snapshot_then(self, settings, column, literal):
        settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
        a_row("beside-it")
        victim = a_row("victim")
        worker = Worker(backoff_initial=0)
        # Built from the snapshot, with no tick of either claimed yet.
        assert len(worker._schedule_source.schedules()) == 2
        write_by_sql(TABLE, column, literal, victim.pk)
        return worker, victim

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_a_value_that_cannot_be_read_is_left_out(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        worker, victim = self._snapshot_then(settings, column, literal)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule, skipped.schedule_pk) == ("victim", victim.pk)
        assert skipped.reason.startswith(f"its {column} ")
        assert not events(caplog, "schedule_dispatch_error")
        # Out of the snapshot, so it is not locked again on every pass.
        cached = [s.key for s in worker._schedule_source._cached]
        assert cached == [f"db:{OxSchedule.objects.get(name='beside-it').pk}"]
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{victim.pk}"
        ).exists()

    def test_a_value_its_field_cannot_convert_is_left_out(self, settings, caplog):
        only_on("sqlite", "SQLite keeps text in an integer column")
        worker, victim = self._snapshot_then(settings, "every_seconds", "'abc'")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert skipped.reason.startswith("its every_seconds ")
        assert len(worker._schedule_source._cached) == 1

    def test_a_paused_row_that_cannot_be_converted_is_left_out_quietly(
        self, settings, caplog
    ):
        only_on("sqlite", "SQLite keeps text in an integer column")
        worker, victim = self._snapshot_then(settings, "every_seconds", "'abc'")
        OxSchedule.objects.filter(pk=victim.pk).update(enabled=False)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert not events(caplog, "schedule_row_skipped")
        assert len(worker._schedule_source._cached) == 1
        # Its boundary could not be checked, so no heal is waiting on it.
        assert victim.pk not in worker._schedule_source._needs_heal

    def test_a_row_that_no_longer_builds_is_left_out_with_its_traceback(
        self, settings, caplog
    ):
        worker, victim = self._snapshot_then(settings, "task_key", "'only.in.new.code'")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
            worker._schedule_source._rows()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert skipped.exc_info is not None
        assert len(worker._schedule_source._cached) == 1

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_a_heal_that_meets_a_row_that_cannot_be_read(
        self, settings, caplog, vendor, column, literal, why
    ):
        """
        A row seen at dispatch with its timing changed around the functions
        is healed on the next pass. Changed again before that, so that it
        cannot be read, the heal cannot move its boundary: the sighting goes
        and the row is reported, where before the read raised for every row.
        """
        only_on(vendor, why)
        source, victim = a_sighting(settings)
        write_by_sql(TABLE, column, literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            source.schedules()
        assert victim.pk not in source._needs_heal
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule, skipped.schedule_pk) == ("victim", victim.pk)
        assert skipped.reason.startswith(f"its {column} ")
        assert not events(caplog, "schedule_boundary_heal_failed")


# -- the change marker -------------------------------------------------------


@pytest.mark.django_db
class TestAChangeMarkerThatCannotBeRead:
    @pytest.mark.parametrize(("vendor", "literal", "why"), UNREADABLE_MARKER)
    def test_the_rows_are_read_and_a_later_write_is_noticed(
        self, settings, caplog, vendor, literal, why
    ):
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        a_row("first")
        write_by_sql(MARKER, "changed_at", literal)
        # Long enough that only the marker can make the source read again.
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 3600}, "default"
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert [s.name for s in source.schedules()] == ["first"]
            assert [s.name for s in source.schedules()] == ["first"]
            unreadable = events(caplog, "schedule_source_unavailable")
            # Said once, not once a pass, and as a value that does not read.
            assert len(unreadable) == 1
            assert unreadable[0].msg == stored._MARKER_UNREADABLE
            # A write through this module replaces the marker, which it
            # could not have read, and the source notices it at once.
            a_row("second")
            assert [s.name for s in source.schedules()] == ["first", "second"]
        assert OxScheduleChange.objects.get().changed_at is not None

    def test_a_worker_is_built_with_it(self, settings):
        only_on("sqlite", "SQLite keeps a date that does not exist")
        settings.TASKS = stored_tasks()
        a_row("first")
        write_by_sql(MARKER, "changed_at", "'2026-02-30 00:00:00'")
        assert Worker(backoff_initial=0).dispatch_schedules() == 1


# -- what is still the database's -------------------------------------------


@pytest.mark.django_db
class TestTheDatabaseFailingIsNotABadRow:
    def test_a_failed_read_of_the_rows_is_raised(self, settings, caplog):
        settings.TASKS = stored_tasks()
        a_row("first")
        source = DatabaseScheduleSource({}, "default")

        def refuse(execute, sql, params, many, context):
            if TABLE in sql and MARKER not in sql:
                raise OperationalError("server closed the connection unexpectedly")
            return execute(sql, params, many, context)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refuse),
            pytest.raises(OperationalError),
        ):
            source.schedules()
        assert not events(caplog, "schedule_row_skipped")

    def test_nor_is_one_that_fails_after_a_row_would_not_read(self, settings, caplog):
        # The read that halves its way to the bad row meets the database
        # failing: that is raised, and no row is put down as bad for it.
        only_on("sqlite", "SQLite keeps a date that does not exist")
        settings.TASKS = stored_tasks()
        a_row("first")
        victim = a_row("victim")
        write_by_sql(TABLE, "end_time", "'2026-02-30 00:00:00'", victim.pk)
        source = DatabaseScheduleSource({}, "default")
        seen = []

        def refuse_after_the_first(execute, sql, params, many, context):
            if TABLE in sql and MARKER not in sql:
                seen.append(sql)
                if len(seen) > 1:
                    raise OperationalError("database disk image is malformed")
            return execute(sql, params, many, context)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refuse_after_the_first),
            pytest.raises(OperationalError),
        ):
            source.schedules()
        assert len(seen) == 2
        assert not events(caplog, "schedule_row_skipped")

    def test_a_form_that_queries_and_fails_is_not_the_row_s_failure(
        self, settings, monkeypatch
    ):
        settings.TASKS = stored_tasks()
        a_row("first")

        def lost(self, row):
            raise OperationalError("the connection was lost")

        monkeypatch.setattr(DatabaseScheduleSource, "_to_schedule", lost)
        with pytest.raises(OperationalError):
            DatabaseScheduleSource({}, "default").schedules()


@pytest.mark.django_db
class TestTheReadInsideACallersTransaction:
    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), UNREADABLE)
    def test_a_value_that_does_not_read_leaves_the_transaction_usable(
        self, settings, caplog, vendor, column, literal, why
    ):
        """
        A source may be read inside a transaction the caller owns (this
        test runs in one). The rows are read a batch at a time with each
        value decoded here, so a value that does not read fails no
        statement, and the one failure a driver raises by itself, SQLite's
        on text that is not UTF-8, comes once the database has answered.
        Either way the transaction is as it was: no savepoint is taken
        around a read, and the caller's next statement runs.
        """
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, column, literal, victim.pk)
        assert connection.in_atomic_block
        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            CaptureQueriesContext(connection) as queries,
        ):
            built = DatabaseScheduleSource({}, "default").schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        assert {r.schedule_pk for r in events(caplog, "schedule_row_skipped")} == {
            victim.pk
        }
        assert not [
            q["sql"] for q in queries.captured_queries if "SAVEPOINT" in q["sql"]
        ]
        assert OxSchedule.objects.filter(pk=victim.pk).update(name="still-here") == 1

    def test_a_read_the_server_refuses_is_raised_as_it_was(self, settings, caplog):
        """
        A read the server itself refuses ends a PostgreSQL transaction for
        every statement after it, and there is no savepoint to go back to.
        The server's error is raised as it is, for the caller that owns
        the transaction to roll it back: nothing is read after it, and no
        row is put down as one that does not read. Made to happen here by
        having the server refuse the first batch of rows.
        """
        only_on("postgresql", "only PostgreSQL aborts a transaction on an error")
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, "end_time", "'infinity'", victim.pk)
        refused = []
        after = []

        def refuse_the_first_batch(execute, sql, params, many, context):
            if refused:
                after.append(sql)
            elif TABLE in sql and "ox_stored_end_time" in sql:
                refused.append(sql)
                return execute("SELECT 1/0", None, many, context)
            return execute(sql, params, many, context)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            pytest.raises(DataError, match="division by zero"),
            transaction.atomic(),
            connection.execute_wrapper(refuse_the_first_batch),
        ):
            DatabaseScheduleSource({}, "default").schedules()
        assert refused
        assert after == []
        assert not events(caplog, "schedule_row_skipped")
        # Rolled back by its owner, the connection reads again.
        assert OxSchedule.objects.count() == 2


# -- an error the database raised is not a value ------------------------------


def raising_at(table, error):
    """
    connection.execute_wrapper() that raises `error` in place of the first
    SELECT on `table`.
    """
    met = []

    def wrapper(execute, sql, params, many, context):
        if not met and table in sql and sql.lstrip().upper().startswith("SELECT"):
            met.append(sql)
            raise error
        return execute(sql, params, many, context)

    return wrapper


@pytest.mark.django_db(transaction=True)
class TestAnErrorTheDatabaseRaisedIsNotPutDownToAValue:
    """
    The change marker and a row whose boundary is being moved are read by
    Django's own converters, and a value in either that does not convert
    fails in this process, once the database has answered. Only such a
    failure is the marker's or the row's. An error the database itself
    raised over the same statement, a DataError as much as a lock or a
    lost connection, says nothing about what is stored: the marker is not
    reported as holding a value that cannot be read, and the row is not
    left out as one that does not read.

    On PostgreSQL and MySQL the server refuses the read for real, with a
    statement it raises a DataError on; SQLite has no such statement, and
    the error is raised in the read's place. With no transaction open, as
    a worker reads.
    """

    def test_a_refused_read_of_the_marker_keeps_the_last_known_schedules(
        self, settings, caplog
    ):
        settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
        a_row("first")
        worker = Worker(backoff_initial=0)
        source = worker._schedule_source
        assert [s.name for s in source.schedules()] == ["first"]
        # Written since. A marker taken for unreadable has the rows read
        # in full, and this one would be among them.
        a_row("second")
        refusing = Refusing(MARKER)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refusing),
        ):
            fired = worker.dispatch_schedules()

        assert refusing.refused is not None, "the marker was never read"
        assert fired == 1, "the schedule it knew of fires"
        assert [s.name for s in source._cached] == ["first"]
        (unavailable,) = events(caplog, "schedule_source_unavailable")
        assert unavailable.msg != stored._MARKER_UNREADABLE
        assert "using the last known schedules" in unavailable.getMessage()
        assert Refusing.SAYS[connection.vendor] in unavailable.getMessage()
        assert not events(caplog, "schedule_row_skipped")
        # Answered on the next pass, the marker has moved and the rows are read.
        assert [s.name for s in source.schedules()] == ["first", "second"]

    @pytest.mark.parametrize("write", ["create", "change", "pause", "delete"])
    def test_a_refused_read_of_the_marker_ends_the_write_it_was_part_of(
        self, settings, write
    ):
        settings.TASKS = stored_tasks()
        row = a_row("first")
        marked = OxScheduleChange.objects.get().changed_at
        writes = {
            "create": lambda: a_row("second"),
            "change": lambda: update_schedule(row, every_seconds=120),
            "pause": lambda: update_schedule(row, enabled=False),
            "delete": lambda: delete_schedule(row),
        }
        refusing = Refusing(MARKER)

        with pytest.raises(DataError) as caught, connection.execute_wrapper(refusing):
            writes[write]()

        assert refusing.refused is not None, "the marker was never read"
        assert Refusing.SAYS[connection.vendor] in str(caught.value)
        # Not written over, and the row's own write went back with it.
        assert not [
            sql
            for sql in refusing.after
            if sql.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
        ]
        assert OxScheduleChange.objects.get().changed_at == marked
        (kept,) = OxSchedule.objects.all()
        assert (kept.name, kept.every_seconds, kept.enabled) == ("first", 60, True)

    def test_a_refused_read_under_the_lock_leaves_the_heal_for_the_next_pass(
        self, settings, caplog
    ):
        source, victim = a_sighting(settings)
        boundary = OxSchedule.objects.get(pk=victim.pk).boundary_for
        refusing = Refusing("ox_stored_start_time")

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refusing),
        ):
            source.schedules()

        assert refusing.refused is not None, "the row was never read"
        # The sighting is kept, and nothing says the row does not read.
        assert victim.pk in source._needs_heal
        assert not events(caplog, "schedule_row_skipped")
        (failed,) = events(caplog, "schedule_boundary_heal_failed")
        assert type(failed.exc_info[1]) is DataError
        assert Refusing.SAYS[connection.vendor] in str(failed.exc_info[1])
        assert OxSchedule.objects.get(pk=victim.pk).boundary_for == boundary

        caplog.clear()
        with caplog.at_level(logging.INFO, logger="django_ox"):
            source.schedules()

        assert victim.pk not in source._needs_heal
        (healed,) = events(caplog, "schedule_boundary_healed")
        assert healed.schedule_pk == victim.pk
        moved = OxSchedule.objects.get(pk=victim.pk)
        assert moved.boundary_for == stored.boundary_digest(moved) != boundary

    def test_a_refused_read_of_the_marker_takes_a_heal_back_with_it(
        self, settings, caplog
    ):
        source, victim = a_sighting(settings)
        before = OxSchedule.objects.get(pk=victim.pk)
        refusing = Refusing(MARKER)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refusing),
        ):
            source.schedules()

        # The heal writes the boundary and then the marker, in one
        # transaction: the boundary went back with the marker's refusal.
        assert victim.pk in source._needs_heal
        assert events(caplog, "schedule_boundary_heal_failed")
        assert not events(caplog, "schedule_row_skipped")
        after = OxSchedule.objects.get(pk=victim.pk)
        assert (after.boundary_for, after.boundary_generation) == (
            before.boundary_for,
            before.boundary_generation,
        )

    @pytest.mark.parametrize(
        "error", [ValueError, TypeError, AttributeError, OverflowError]
    )
    def test_a_converters_failure_on_the_marker_is_still_the_markers(
        self, settings, caplog, error
    ):
        settings.TASKS = stored_tasks()
        a_row("first")
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 3600}, "default"
        )

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(raising_at(MARKER, error("not a date"))),
        ):
            assert [s.name for s in source.schedules()] == ["first"]

        (unreadable,) = events(caplog, "schedule_source_unavailable")
        assert unreadable.msg == stored._MARKER_UNREADABLE

    @pytest.mark.parametrize(
        "error",
        [InterfaceError("connection already closed"), RuntimeError("a fault in code")],
        ids=["a-closed-connection", "a-fault-in-code"],
    )
    def test_a_failure_that_is_neither_goes_up_from_the_marker_read(
        self, settings, caplog, error
    ):
        """
        Neither a value's nor an error the database raised: a connection
        closed under the read, which the dispatch loop takes with the
        database's own, and a fault in code, which costs the pass. Each
        goes up as it is, and the marker is not reported for it.
        """
        settings.TASKS = stored_tasks()
        a_row("first")
        source = DatabaseScheduleSource({}, "default")

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(raising_at(MARKER, error)),
            pytest.raises(type(error)) as caught,
        ):
            source.schedules()

        assert caught.value is error
        assert not events(caplog, "schedule_source_unavailable")

    @pytest.mark.parametrize(
        ("error", "the_rows"),
        [
            (ValidationError("not a number"), True),
            (ValueError("not a number"), True),
            (TypeError("not a number"), True),
            (InterfaceError("connection already closed"), False),
            (RuntimeError("a fault in code"), False),
        ],
        ids=[
            "ValidationError",
            "ValueError",
            "TypeError",
            "a-closed-connection",
            "a-fault-in-code",
        ],
    )
    def test_in_a_heal_only_a_failure_known_to_be_a_values_is_the_rows(
        self, settings, caplog, monkeypatch, error, the_rows
    ):
        source, victim = a_sighting(settings)

        def fails(row):
            raise error

        monkeypatch.setattr(stored, "boundary_digest", fails)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            if the_rows:
                source.schedules()
            else:
                with pytest.raises(type(error)) as caught:
                    source.schedules()
                assert caught.value is error

        skipped = events(caplog, "schedule_row_skipped")
        assert {r.schedule_pk for r in skipped} == ({victim.pk} if the_rows else set())
        assert (victim.pk in source._needs_heal) is not the_rows
        assert not events(caplog, "schedule_boundary_heal_failed")


# -- a boundary the database will not let a worker move -----------------------


#: What the trigger below says when it refuses a write.
REFUSED_MOVE = "this boundary is not to be moved"

#: The class each database's refusal of that write reaches Django as.
MOVE_REFUSED_AS = {
    "sqlite": IntegrityError,
    "postgresql": ProgrammingError,
    "mysql": OperationalError,
}

#: The trigger's name, and on PostgreSQL its function's.
REFUSES_MOVES = "ox_test_refuses_moves"


def _statements_refusing_moves_of(pks):
    """The trigger as this run's database writes one."""
    named = ", ".join(str(int(pk)) for pk in pks)
    on_the_count = f"BEFORE UPDATE OF boundary_generation ON {TABLE}"
    if connection.vendor == "postgresql":
        function = (
            f"CREATE FUNCTION {REFUSES_MOVES}() RETURNS trigger LANGUAGE plpgsql "
            f"AS $$ BEGIN RAISE EXCEPTION '{REFUSED_MOVE}'; END $$"
        )
        trigger = (
            f"CREATE TRIGGER {REFUSES_MOVES} {on_the_count} FOR EACH ROW "
            f"WHEN (OLD.id IN ({named})) EXECUTE FUNCTION {REFUSES_MOVES}()"
        )
        return [function, trigger]
    if connection.vendor == "mysql":
        # MySQL has no trigger on one column, so this one asks whether the
        # write changes the count.
        changes_the_count = (
            f"OLD.id IN ({named}) "
            "AND NOT (NEW.boundary_generation <=> OLD.boundary_generation)"
        )
        refuse = f"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{REFUSED_MOVE}'"
        trigger = (
            f"CREATE TRIGGER {REFUSES_MOVES} BEFORE UPDATE ON {TABLE} FOR EACH ROW "
            f"BEGIN IF {changes_the_count} THEN {refuse}; END IF; END"
        )
        return [trigger]
    trigger = (
        f"CREATE TRIGGER {REFUSES_MOVES} {on_the_count} WHEN OLD.id IN ({named}) "
        f"BEGIN SELECT RAISE(ABORT, '{REFUSED_MOVE}'); END"
    )
    return [trigger]


def _statements_allowing_moves():
    if connection.vendor == "postgresql":
        return [
            f"DROP TRIGGER IF EXISTS {REFUSES_MOVES} ON {TABLE}",
            f"DROP FUNCTION IF EXISTS {REFUSES_MOVES}()",
        ]
    return [f"DROP TRIGGER IF EXISTS {REFUSES_MOVES}"]


@contextmanager
def refusing_moves_of(*rows):
    """
    A database that refuses to let these rows' boundaries be moved, for as
    long as the block lasts: a trigger of a project's own on the schedule
    table, raising on every write to a named row's count of boundary
    writes, which a move always makes. The refusal is the database's own,
    on every engine and to whatever process sends the write, a worker
    started from the test among them. Reading the rows, locking them and
    writing their other columns go through.

    For a test with no transaction around it: the trigger is made and
    dropped by statements that commit on their own.
    """

    def run(statements):
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    run(_statements_allowing_moves())
    run(_statements_refusing_moves_of([row.pk for row in rows]))
    try:
        yield
    finally:
        run(_statements_allowing_moves())


def refused_as():
    """The class this run's database refuses a move as."""
    return MOVE_REFUSED_AS[connection.vendor]


def retime_by_sql(row, seconds, count=None):
    """
    Change a row's timing by a route that moves no boundary, which is what
    leaves its boundary stale, and first its count of boundary writes when
    `count` is given.
    """
    if count is not None:
        write_by_sql(TABLE, "boundary_generation", str(count), row.pk)
    write_by_sql(TABLE, "every_seconds", str(seconds), row.pk)


def a_worker_beside(settings, *names):
    """
    A worker that has read `beside-it` and a row of each name given, all as
    they were written: the worker, its source with the clock its reports go
    by, and the rows. A row changed by SQL after this is met at the next
    dispatch, under its lock.
    """
    settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
    a_row("beside-it")
    rows = [a_row(name) for name in names]
    worker = Worker(backoff_initial=0)
    source = worker._schedule_source
    assert len(source.schedules()) == len(rows) + 1
    clock = [0.0]
    source._report.clock = lambda: clock[0]
    return worker, source, clock, rows


def moves_failed(caplog):
    return events(caplog, "schedule_boundary_heal_failed")


def said_of(caplog, row):
    """Every record about one stored row: by its key, its name or its dispatch key."""
    return [
        record
        for record in caplog.records
        if getattr(record, "schedule_pk", None) == row.pk
        or getattr(record, "schedule", None) == row.name
        or getattr(record, "schedule_key", None) == f"db:{row.pk}"
    ]


class MovesTried:
    """
    connection.execute_wrapper() that counts, for each row, the moves of its
    boundary a worker sent: the UPDATE that adds one to the row's count of
    boundary writes, which is the last thing the statement names.
    """

    def __init__(self):
        self.of = Counter()

    def __call__(self, execute, sql, params, many, context):
        adds_one = f"{connection.ops.quote_name('boundary_generation')} + "
        if sql.lstrip().upper().startswith("UPDATE") and adds_one in sql:
            self.of[params[-1]] += 1
        return execute(sql, params, many, context)


class RefusingTheLockedReadOf:
    """
    connection.execute_wrapper() for a database that refuses, every time,
    the read of each of `pks` under its lock: the SELECT that names that row
    alone. Refused as `Refusing` refuses one read, and counted for each row.
    """

    def __init__(self, *pks):
        self.pks = set(pks)
        self.refused = Counter()

    def __call__(self, execute, sql, params, many, context):
        named = tuple(params or ())
        if (
            "ox_stored_start_time" in sql
            and sql.lstrip().upper().startswith("SELECT")
            and len(named) == 1
            and named[0] in self.pks
        ):
            self.refused[named[0]] += 1
            statement = Refusing.STATEMENT.get(connection.vendor)
            if statement is None:
                raise DataError(Refusing.SAYS["sqlite"])
            return execute(statement, None, many, context)
        return execute(sql, params, many, context)


@pytest.mark.django_db(transaction=True)
class TestABoundaryTheDatabaseWillNotLetAWorkerMove:
    """
    A boundary found stale is moved at the start of the next dispatch pass.
    When the database refuses the move, the sighting stays and the move is
    tried again on every pass, about once a second, so it is reported as a
    dispatch that keeps failing is: with its traceback the first time, then
    at most once a ROW_REPORT_INTERVAL with how many times it has failed
    since, for each row and for each worker by itself. Once the boundary
    moves, or no longer has to, a failure after that is reported in full.

    The failure that lasts is the database refusing the write itself, here
    by a trigger on the table that stands until it is dropped
    (`refusing_moves_of`). The one that passes is a read under the row's
    lock that the database refuses, made to happen on every engine with
    `Refusing`. With no transaction open, as a worker reads.
    """

    def test_a_move_the_database_refuses_is_reported_once_and_then_counted(
        self, settings, caplog
    ):
        worker, source, clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120)
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            with refusing_moves_of(victim):
                # Met at dispatch, and the move is tried at the start of each
                # pass after that.
                assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
                assert victim.pk in source._needs_heal
                assert not tried.of
                for _ in range(6):
                    worker.dispatch_schedules()

                # Six passes inside one interval: one line, with its traceback.
                failed = moves_failed(caplog)
                assert [record.exc_info is not None for record in failed] == [True]
                (first,) = failed
                assert first.msg == stored._HEAL_FAILED
                assert first.levelno == logging.WARNING
                assert type(first.exc_info[1]) is refused_as()
                assert REFUSED_MOVE in str(first.exc_info[1])
                assert (first.schedule_pk, first.error) == (
                    victim.pk,
                    refused_as().__name__,
                )
                assert (first.failures, first.suppressed) == (1, 0)
                # Nothing else is said of the row, and it is not put down as
                # one that does not read.
                assert said_of(caplog, victim) == [first]
                assert not events(caplog, "schedule_row_skipped")
                # Only the reporting is held back: the move was tried each time.
                assert tried.of == {victim.pk: 6}

                # Once the interval has passed, one line more, with the count.
                clock[0] = stored.ROW_REPORT_INTERVAL
                worker.dispatch_schedules()
                first, later = moves_failed(caplog)
                assert later.msg == stored._HEAL_STILL_FAILING
                assert later.exc_info is None
                assert later.levelno == logging.WARNING
                assert later.args == (victim.pk, 6, refused_as().__name__)
                assert (later.schedule_pk, later.error) == (
                    victim.pk,
                    refused_as().__name__,
                )
                assert (later.failures, later.suppressed) == (7, 6)
                for _ in range(3):
                    worker.dispatch_schedules()
                assert said_of(caplog, victim) == [first, later]
                assert tried.of == {victim.pk: 10}

                # Still waiting to be moved, and still not dispatched.
                assert victim.pk in source._needs_heal
                kept = OxSchedule.objects.get(pk=victim.pk)
                assert kept.boundary_generation == 0
                assert kept.boundary_for != stored.boundary_digest(kept)
                assert not OxScheduleTick.objects.filter(
                    schedule_name=f"db:{victim.pk}"
                ).exists()

            # The database takes the write again, and the pass after that
            # moves the boundary.
            caplog.clear()
            worker.dispatch_schedules()
            (healed,) = events(caplog, "schedule_boundary_healed")
            assert healed.schedule_pk == victim.pk
            assert not moves_failed(caplog)
            assert not events(caplog, "schedule_row_skipped")
            assert victim.pk not in source._needs_heal
            moved = OxSchedule.objects.get(pk=victim.pk)
            assert moved.boundary_for == stored.boundary_digest(moved)
            assert moved.boundary_generation == 1

            # It fails again after that: a new failure, said in full, well
            # inside the interval the last line began.
            with refusing_moves_of(victim):
                retime_by_sql(victim, 180)
                source._last_read = None
                caplog.clear()
                worker.dispatch_schedules()
                (again,) = moves_failed(caplog)
                assert again.msg == stored._HEAL_FAILED
                assert type(again.exc_info[1]) is refused_as()
                assert (again.failures, again.suppressed) == (1, 0)

    def test_two_rows_are_each_reported_for_itself(self, settings, caplog):
        worker, source, clock, (victim, another) = a_worker_beside(
            settings, "victim", "another"
        )
        tried = MovesTried()

        def lines():
            return [
                (r.schedule_pk, r.exc_info is not None, r.failures, r.suppressed)
                for r in moves_failed(caplog)
            ]

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
            refusing_moves_of(victim, another),
        ):
            retime_by_sql(victim, 120)
            worker.dispatch_schedules()
            # Two failed moves of the first before the second row is stale.
            worker.dispatch_schedules()
            worker.dispatch_schedules()
            assert len(moves_failed(caplog)) == 1
            assert lines() == [(victim.pk, True, 1, 0)]
            # The second, found by a full read, which tries every move that
            # is waiting once more when it is done.
            retime_by_sql(another, 120)
            source._last_read = None
            worker.dispatch_schedules()
            worker.dispatch_schedules()
            assert lines() == [(victim.pk, True, 1, 0), (another.pk, True, 1, 0)]
            assert set(source._needs_heal) == {victim.pk, another.pk}

            clock[0] = stored.ROW_REPORT_INTERVAL
            worker.dispatch_schedules()
            later = {record.schedule_pk: record for record in moves_failed(caplog)[2:]}
            assert len(moves_failed(caplog)) == 4

        # Each row's line carries that row's own count: every move the
        # database refused for it, and those since its first line.
        assert tried.of == {victim.pk: 6, another.pk: 3}
        for row in (victim, another):
            assert later[row.pk].exc_info is None
            assert later[row.pk].failures == tried.of[row.pk]
            assert later[row.pk].suppressed == tried.of[row.pk] - 1
        assert not events(caplog, "schedule_row_skipped")

    def test_a_row_beside_it_is_moved_and_the_other_schedules_fire(
        self, settings, caplog
    ):
        worker, source, _clock, (victim, another) = a_worker_beside(
            settings, "victim", "another"
        )
        retime_by_sql(victim, 120)
        retime_by_sql(another, 120)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            refusing_moves_of(victim),
        ):
            assert worker.dispatch_schedules() == 1, "the schedule beside them fires"
            assert set(source._needs_heal) == {victim.pk, another.pk}
            worker.dispatch_schedules()

        (failed,) = moves_failed(caplog)
        assert failed.schedule_pk == victim.pk
        (healed,) = events(caplog, "schedule_boundary_healed")
        assert healed.schedule_pk == another.pk
        assert set(source._needs_heal) == {victim.pk}
        moved = OxSchedule.objects.get(pk=another.pk)
        assert moved.boundary_for == stored.boundary_digest(moved)
        assert OxScheduleTick.objects.filter(
            schedule_name=f"db:{OxSchedule.objects.get(name='beside-it').pk}",
            task__isnull=False,
        ).exists()

    def test_each_worker_reports_for_itself(self, settings, caplog):
        worker, _source, _clock, (victim,) = a_worker_beside(settings, "victim")
        second = Worker(backoff_initial=0)
        assert len(second._schedule_source.schedules()) == 2
        retime_by_sql(victim, 120)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            refusing_moves_of(victim),
        ):
            for _ in range(3):
                worker.dispatch_schedules()
                second.dispatch_schedules()

        # The row has failed four times, twice for each worker, and each
        # worker has said so once.
        assert len(moves_failed(caplog)) == 2
        assert [
            (r.schedule_pk, r.exc_info is not None, r.failures)
            for r in moves_failed(caplog)
        ] == [(victim.pk, True, 1), (victim.pk, True, 1)]

    def test_a_move_that_fails_once_and_then_succeeds_is_one_line_and_one_move(
        self, settings, caplog
    ):
        source, victim = a_sighting(settings)
        refusing = Refusing("ox_stored_start_time")

        with caplog.at_level(logging.INFO, logger="django_ox"):
            with connection.execute_wrapper(refusing):
                source.schedules()
            assert refusing.refused is not None, "the row was never read"
            for _ in range(3):
                source.schedules()

        (failed,) = moves_failed(caplog)
        assert failed.msg == stored._HEAL_FAILED
        assert Refusing.SAYS[connection.vendor] in str(failed.exc_info[1])
        assert (failed.schedule_pk, failed.error) == (victim.pk, "DataError")
        assert (failed.failures, failed.suppressed) == (1, 0)
        (healed,) = events(caplog, "schedule_boundary_healed")
        assert said_of(caplog, victim) == [failed, healed]
        assert victim.pk not in source._needs_heal

    def test_a_read_under_the_lock_refused_on_every_pass_is_counted_too(
        self, settings, caplog
    ):
        source, victim = a_sighting(settings)
        clock = [0.0]
        source._report.clock = lambda: clock[0]

        def a_pass_the_database_refuses(nth=1):
            refusing = Refusing("ox_stored_start_time", nth=nth)
            with connection.execute_wrapper(refusing):
                source.schedules()
            assert refusing.refused is not None, "the row was never read"

        with caplog.at_level(logging.INFO, logger="django_ox"):
            for _ in range(5):
                a_pass_the_database_refuses()
            failed = moves_failed(caplog)
            assert [record.exc_info is not None for record in failed] == [True]
            (first,) = failed
            assert type(first.exc_info[1]) is DataError
            assert Refusing.SAYS[connection.vendor] in str(first.exc_info[1])
            assert (first.failures, first.suppressed) == (1, 0)
            assert said_of(caplog, victim) == [first]
            assert victim.pk in source._needs_heal

            clock[0] = stored.ROW_REPORT_INTERVAL
            a_pass_the_database_refuses()
            first, later = moves_failed(caplog)
            assert later.msg == stored._HEAL_STILL_FAILING
            assert later.exc_info is None
            assert later.args == (victim.pk, 5, "DataError")
            assert (later.failures, later.suppressed) == (6, 5)
            assert not events(caplog, "schedule_row_skipped")

            # The database answers: the boundary moves, and that is said.
            caplog.clear()
            source.schedules()
            (healed,) = events(caplog, "schedule_boundary_healed")
            assert healed.schedule_pk == victim.pk
            assert not moves_failed(caplog)
            assert victim.pk not in source._needs_heal

            # Changed by SQL again and refused again: a new failure, said in
            # full. A full read finds the row stale and tries the move at
            # once, so the read under the lock is the second to name the row.
            retime_by_sql(victim, 180)
            source._last_read = None
            caplog.clear()
            a_pass_the_database_refuses(nth=2)
            (again,) = moves_failed(caplog)
            assert again.msg == stored._HEAL_FAILED
            assert again.exc_info is not None
            assert (again.failures, again.suppressed) == (1, 0)
            assert victim.pk in source._needs_heal

    def test_of_two_rows_refused_at_once_the_one_that_moves_ends_only_its_own_count(
        self, settings, caplog
    ):
        worker, source, clock, (victim, another) = a_worker_beside(
            settings, "victim", "another"
        )
        retime_by_sql(victim, 120)
        retime_by_sql(another, 120)
        assert worker.dispatch_schedules() == 1, "the schedule beside them fires"
        assert set(source._needs_heal) == {victim.pk, another.pk}
        both = RefusingTheLockedReadOf(victim.pk, another.pk)
        one = RefusingTheLockedReadOf(victim.pk)

        with caplog.at_level(logging.INFO, logger="django_ox"):
            with connection.execute_wrapper(both):
                for _ in range(3):
                    source.schedules()
            assert len(moves_failed(caplog)) == 2
            assert [
                (r.schedule_pk, r.exc_info is not None, r.failures, r.suppressed)
                for r in moves_failed(caplog)
            ] == [(victim.pk, True, 1, 0), (another.pk, True, 1, 0)]

            # The database answers for one of them. It is moved, and the
            # other is still refused and still counted.
            with connection.execute_wrapper(one):
                for _ in range(2):
                    source.schedules()
            (healed,) = events(caplog, "schedule_boundary_healed")
            assert healed.schedule_pk == another.pk
            assert set(source._needs_heal) == {victim.pk}
            assert len(moves_failed(caplog)) == 2

            clock[0] = stored.ROW_REPORT_INTERVAL
            with connection.execute_wrapper(one):
                source.schedules()

        _first, _other, later = moves_failed(caplog)
        refused = both.refused[victim.pk] + one.refused[victim.pk]
        assert refused > both.refused[another.pk] == 3
        assert later.schedule_pk == victim.pk
        assert later.exc_info is None
        assert (later.failures, later.suppressed) == (refused, refused - 1)
        assert not events(caplog, "schedule_row_skipped")

    def test_a_row_gone_while_its_move_kept_failing_is_forgotten(
        self, settings, caplog
    ):
        source, victim = a_sighting(settings)

        with caplog.at_level(logging.INFO, logger="django_ox"):
            with connection.execute_wrapper(Refusing("ox_stored_start_time")):
                source.schedules()
            assert len(moves_failed(caplog)) == 1
            OxSchedule.objects.filter(pk=victim.pk).delete()
            source.schedules()

        # Settled with nothing to move, and nothing is kept about the row.
        assert victim.pk not in source._needs_heal
        assert len(moves_failed(caplog)) == 1
        assert not events(caplog, "schedule_boundary_healed")
        assert ("heal", victim.pk) not in source._report._failing

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_so_is_a_row_that_stops_reading_while_its_move_kept_failing(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        source, victim = a_sighting(settings)

        with caplog.at_level(logging.INFO, logger="django_ox"):
            with connection.execute_wrapper(Refusing("ox_stored_start_time")):
                source.schedules()
            assert len(moves_failed(caplog)) == 1
            write_by_sql(TABLE, column, literal, victim.pk)
            source.schedules()

        # Left out as a row that does not read: there is no boundary to
        # move, and nothing is kept about the moves that failed.
        assert victim.pk not in source._needs_heal
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule_pk, skipped.fields) == (victim.pk, [column])
        assert len(moves_failed(caplog)) == 1
        assert ("heal", victim.pk) not in source._report._failing

    def test_and_a_row_whose_fields_the_boundary_cannot_be_taken_from(
        self, settings, caplog, monkeypatch
    ):
        source, victim = a_sighting(settings)

        def refuses(row):
            raise ValidationError("not a number")

        with caplog.at_level(logging.INFO, logger="django_ox"):
            with connection.execute_wrapper(Refusing("ox_stored_start_time")):
                source.schedules()
            assert len(moves_failed(caplog)) == 1
            monkeypatch.setattr(stored, "boundary_digest", refuses)
            source.schedules()

        assert victim.pk not in source._needs_heal
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert len(moves_failed(caplog)) == 1
        assert ("heal", victim.pk) not in source._report._failing


# -- a count of boundary writes that cannot be raised ---------------------------


def count_as_stored(row):
    """A row's count of boundary writes as the database holds it, and its type there."""
    kind = "typeof(boundary_generation)" if connection.vendor == "sqlite" else "NULL"
    statement = f"SELECT boundary_generation, {kind} FROM {TABLE} WHERE id = %s"  # noqa: S608
    with connection.cursor() as cursor:
        cursor.execute(statement, [row.pk])
        return cursor.fetchone()


def a_whole_number_still():
    """How SQLite says a value is kept as an integer; the others have no other way."""
    return "integer" if connection.vendor == "sqlite" else None


def full():
    """What a worker says of a row whose count is the most its column can hold."""
    return (
        f"its boundary_generation holds {largest_count()}, the most its column "
        "can hold, so the count cannot be raised"
    )


def assert_named_once_for_its_count(caplog, row):
    (skipped,) = events(caplog, "schedule_row_skipped")
    assert skipped.msg == stored._SKIPPING
    assert skipped.levelno == logging.WARNING
    assert (skipped.schedule, skipped.schedule_pk) == (row.name, row.pk)
    assert skipped.fields == ["boundary_generation"]
    assert skipped.reason == full()
    assert skipped.exc_info is None
    # Nothing else is said of the row.
    assert said_of(caplog, row) == [skipped]
    return skipped


@pytest.mark.django_db(transaction=True)
class TestACountOfBoundaryWritesAtItsMaximum:
    """
    A row whose count of boundary writes is the most its column can hold is
    one whose boundary cannot be moved: the move adds one to the count, and
    PostgreSQL and MySQL refuse that write every time it is tried, while
    SQLite keeps the sum as a float, a count that no longer reads. SQL writes
    such a count, or one below it, from which the next move reaches it.

    A worker leaves the row out as it leaves out any row that does not
    read, named by that field and said once, and tries no move of its
    boundary however stale the boundary is. Lowering the count by SQL, or
    deleting the row, is what ends it. With no transaction open, as a
    worker reads.
    """

    @pytest.mark.parametrize("stale", [True, False], ids=["stale", "current"])
    def test_met_at_a_full_read_it_is_named_once_and_no_move_is_tried(
        self, settings, caplog, stale
    ):
        worker, source, clock, (victim,) = a_worker_beside(settings, "victim")
        if stale:
            retime_by_sql(victim, 120)
        write_by_sql(TABLE, "boundary_generation", str(largest_count()), victim.pk)
        as_sql_left_it = OxSchedule.objects.filter(pk=victim.pk).values().get()
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            for _ in range(6):
                # Every pass reads every row again.
                source._last_read = None
                worker.dispatch_schedules()

            assert_named_once_for_its_count(caplog, victim)
            assert not moves_failed(caplog)
            assert not events(caplog, "schedule_boundary_healed")
            assert not tried.of
            assert victim.pk not in source._needs_heal

            # Past the interval, the one line a row still left out gets.
            clock[0] = stored.ROW_REPORT_INTERVAL
            source._last_read = None
            worker.dispatch_schedules()
            _first, later = events(caplog, "schedule_row_skipped")
            assert later.msg == stored._STILL_SKIPPING
            assert later.fields == ["boundary_generation"]
            assert not tried.of

        # The row is as SQL left it, its count a whole number still.
        assert OxSchedule.objects.filter(pk=victim.pk).values().get() == as_sql_left_it
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())
        # It is not dispatched, and the schedule beside it is.
        assert [s.name for s in source._cached] == ["beside-it"]
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{victim.pk}"
        ).exists()
        assert OxScheduleTick.objects.filter(
            schedule_name=f"db:{OxSchedule.objects.get(name='beside-it').pk}",
            task__isnull=False,
        ).exists()

    def test_met_at_dispatch_it_is_named_and_no_move_is_left_waiting(
        self, settings, caplog
    ):
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120, count=largest_count())
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            # Under its lock, before any full read has seen the change.
            assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
            assert victim.pk not in source._needs_heal
            for _ in range(5):
                worker.dispatch_schedules()

        assert_named_once_for_its_count(caplog, victim)
        assert not moves_failed(caplog)
        assert not events(caplog, "schedule_boundary_healed")
        assert not tried.of
        assert victim.pk not in source._needs_heal
        # Out of the snapshot, so it is not locked again on every pass.
        assert [s.name for s in source._cached] == ["beside-it"]
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())

    def test_a_move_already_waiting_is_given_up_without_being_tried(
        self, settings, caplog
    ):
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120)
        assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
        assert victim.pk in source._needs_heal
        write_by_sql(TABLE, "boundary_generation", str(largest_count()), victim.pk)
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            for _ in range(5):
                worker.dispatch_schedules()

        assert_named_once_for_its_count(caplog, victim)
        assert not moves_failed(caplog)
        assert not events(caplog, "schedule_boundary_healed")
        assert not tried.of
        assert victim.pk not in source._needs_heal
        assert ("heal", victim.pk) not in source._report._failing
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())

    def test_one_below_the_maximum_the_heal_takes_the_count_to_it(
        self, settings, caplog
    ):
        """
        SQL can leave the count one below the maximum. The move of a stale
        boundary adds one and succeeds, and the row it leaves is the one
        above: at its maximum, left out at the next read, and not one a
        later change can go on from.
        """
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120, count=largest_count() - 1)

        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
            assert victim.pk in source._needs_heal
            assert not events(caplog, "schedule_boundary_healed")
            worker.dispatch_schedules()
            (healed,) = events(caplog, "schedule_boundary_healed")
            assert healed.schedule_pk == victim.pk
            assert not moves_failed(caplog)
            assert count_as_stored(victim) == (largest_count(), a_whole_number_still())

            # The write succeeded; what it left is read as a full count.
            for _ in range(4):
                source._last_read = None
                worker.dispatch_schedules()

        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.fields == ["boundary_generation"]
        assert skipped.reason == full()
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())
        assert [s.name for s in source._cached] == ["beside-it"]

    def test_paused_it_is_left_out_without_a_word_and_still_not_moved(
        self, settings, caplog
    ):
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120, count=largest_count())
        write_by_sql(TABLE, "enabled", "FALSE", victim.pk)
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            for _ in range(4):
                source._last_read = None
                worker.dispatch_schedules()

        assert said_of(caplog, victim) == []
        assert not tried.of
        assert victim.pk not in source._needs_heal
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())

    def test_two_such_rows_are_each_named_and_the_others_fire(self, settings, caplog):
        worker, source, _clock, (victim, another) = a_worker_beside(
            settings, "victim", "another"
        )
        for row in (victim, another):
            retime_by_sql(row, 120, count=largest_count())
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            assert worker.dispatch_schedules() == 1, "the schedule beside them fires"
            for _ in range(3):
                source._last_read = None
                worker.dispatch_schedules()

        skipped = events(caplog, "schedule_row_skipped")
        assert [(r.schedule_pk, r.fields) for r in skipped] == [
            (victim.pk, ["boundary_generation"]),
            (another.pk, ["boundary_generation"]),
        ]
        assert not moves_failed(caplog)
        assert not tried.of
        assert not source._needs_heal

    def test_one_below_it_the_boundary_is_moved_once_and_the_row_is_then_full(
        self, settings, caplog
    ):
        worker, _source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120, count=largest_count() - 1)
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            for _ in range(5):
                worker.dispatch_schedules()

        # A count below the most reads as any count does: the boundary is
        # moved, which is the last write the count has room for.
        (healed,) = events(caplog, "schedule_boundary_healed")
        assert healed.schedule_pk == victim.pk
        assert tried.of == {victim.pk: 1}
        assert not moves_failed(caplog)
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule_pk, skipped.fields) == (
            victim.pk,
            ["boundary_generation"],
        )
        assert skipped.reason == full()
        assert count_as_stored(victim) == (largest_count(), a_whole_number_still())

    def test_with_the_count_lowered_by_sql_the_row_is_moved_and_built_again(
        self, settings, caplog
    ):
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        retime_by_sql(victim, 120, count=largest_count())
        with caplog.at_level(logging.INFO, logger="django_ox"):
            source._last_read = None
            worker.dispatch_schedules()
            assert_named_once_for_its_count(caplog, victim)

            write_by_sql(TABLE, "boundary_generation", "5", victim.pk)
            source._last_read = None
            caplog.clear()
            worker.dispatch_schedules()

            # Found stale by the read that could read it again, and moved by it.
            (healed,) = events(caplog, "schedule_boundary_healed")
            assert healed.schedule_pk == victim.pk
            assert not events(caplog, "schedule_row_skipped")
            assert not moves_failed(caplog)
            moved = OxSchedule.objects.get(pk=victim.pk)
            assert moved.boundary_for == stored.boundary_digest(moved)
            assert moved.boundary_generation == 6
            assert sorted(s.name for s in source._cached) == ["beside-it", "victim"]

            # Full once more: a row that was built again is said in full again.
            write_by_sql(TABLE, "boundary_generation", str(largest_count()), victim.pk)
            source._last_read = None
            caplog.clear()
            worker.dispatch_schedules()
            assert_named_once_for_its_count(caplog, victim)

    def test_on_sqlite_a_count_that_is_already_a_float_is_left_out_as_it_was(
        self, settings, caplog
    ):
        only_on("sqlite", "SQLite keeps a number past its largest integer as a float")
        worker, source, _clock, (victim,) = a_worker_beside(settings, "victim")
        # One more than SQLite's largest integer, which is what adding one to
        # a full count left behind.
        retime_by_sql(victim, 120, count=largest_count() + 1)
        tried = MovesTried()

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(tried),
        ):
            assert worker.dispatch_schedules() == 1, "the schedule beside it fires"
            for _ in range(3):
                source._last_read = None
                worker.dispatch_schedules()

        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert skipped.fields == ["boundary_generation"]
        assert skipped.reason.endswith("which is not a whole number")
        assert not moves_failed(caplog)
        assert not events(caplog, "schedule_boundary_healed")
        assert not tried.of
        assert victim.pk not in source._needs_heal
        assert count_as_stored(victim) == (float(largest_count() + 1), "real")
        assert [s.name for s in source._cached] == ["beside-it"]


# -- how it is said ----------------------------------------------------------


@pytest.mark.django_db
class TestWhatTheLogSays:
    def test_once_in_full_then_at_most_once_an_interval(self, settings, caplog):
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        write_by_sql(TABLE, "task_key", "'only.in.new.code'", victim.pk)
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 0.001}, "default"
        )
        clock = [0.0]
        source._report.clock = lambda: clock[0]
        with caplog.at_level(logging.INFO, logger="django_ox"):
            for _ in range(3):
                source._last_read = None
                source.schedules()
            assert len(events(caplog, "schedule_row_skipped")) == 1
            clock[0] = stored.ROW_REPORT_INTERVAL
            source._last_read = None
            source.schedules()
        first, later = events(caplog, "schedule_row_skipped")
        assert (first.msg, later.msg) == (stored._SKIPPING, stored._STILL_SKIPPING)
        # The row, how many times it went unsaid, and the reason.
        assert later.args == (f"victim (pk {victim.pk})", 2, first.args[1])
        assert later.schedule_pk == first.schedule_pk == victim.pk

    def test_a_row_that_builds_again_is_reported_in_full_when_it_breaks(
        self, settings, caplog
    ):
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 0.001}, "default"
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            for key in ("only.in.new.code", "report", "only.in.new.code"):
                write_by_sql(TABLE, "task_key", f"'{key}'", victim.pk)
                source._last_read = None
                source.schedules()
        first, again = events(caplog, "schedule_row_skipped")
        assert first.msg == again.msg == stored._SKIPPING
        assert first.getMessage() == again.getMessage()

    def test_a_name_is_printed_safely(self, settings, caplog):
        # A stored name is whatever was stored: an escape sequence, a line
        # break, a carriage return. None of it reaches the line as itself.
        settings.TASKS = stored_tasks()
        hostile = "\x1b]0;owned\x07\x1b[2J\r\nWARNING forged line"
        victim = a_row(hostile)
        write_by_sql(TABLE, "task_key", "'only.in.new.code'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            DatabaseScheduleSource({}, "default").schedules()
        (record,) = events(caplog, "schedule_row_skipped")
        line = record.getMessage()
        assert line.isprintable()
        assert "\x1b" not in line and "\n" not in line and "\r" not in line
        assert f"(pk {victim.pk})" in line
        assert record.schedule == stored._printable(hostile)
        assert record.schedule.isprintable()

    def test_a_name_is_printed_safely_by_a_worker_that_dispatches_it(
        self, settings, caplog
    ):
        # Stored through the write function, with no SQL, on a row that
        # reads and runs: a worker names it on every tick it dispatches.
        settings.TASKS = stored_tasks()
        hostile = "nightly\r\nCRITICAL forged line\x1b[2J"
        a_row(hostile)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            Worker(backoff_initial=0).dispatch_schedules()
        (record,) = events(caplog, "schedule_dispatched")
        line = record.getMessage()
        assert line.isprintable()
        assert "nightly\\r\\nCRITICAL forged line" in line
        assert record.schedule == stored._printable(hostile)
        assert record.schedule.isprintable()
        assert OxTask.objects.count() == 1

    def test_a_long_name_and_reason_are_cut(self, settings, caplog):
        only_on("sqlite", "SQLite keeps a name longer than its column")
        settings.TASKS = stored_tasks()
        victim = a_row("short")
        write_by_sql(TABLE, "name", "'" + "n" * 5000 + "'", victim.pk)
        write_by_sql(TABLE, "every_seconds", "'" + "9" * 5000 + "x'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            DatabaseScheduleSource({}, "default").schedules()
        (record,) = events(caplog, "schedule_row_skipped")
        assert len(record.getMessage()) < 800
        assert len(record.schedule) == stored._PRINTABLE_LIMIT
        assert record.schedule.endswith("...")

    def test_the_printable_form(self):
        assert stored._printable("nightly report") == "nightly report"
        assert stored._printable("Bericht für März") == "Bericht für März"
        assert stored._printable("a\nb") == "a\\nb"
        assert stored._printable("‮evil") == "\\u202eevil"
        assert stored._printable(12) == "12"
        assert stored._printable("x" * 10, limit=8) == "xxxxx..."


@pytest.mark.django_db
class TestASkipNamesTheFieldsAtFault:
    """
    `schedule_row_skipped` carries the fields whose values did not read as
    a list of their names, beside the reason that says the same in a
    sentence. Always a list, and empty where no field is at fault.
    """

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), UNREADABLE)
    def test_at_the_full_read(self, settings, caplog, vendor, column, literal, why):
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        write_by_sql(TABLE, column, literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            DatabaseScheduleSource({}, "default").schedules()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert type(skipped.fields) is list
        assert skipped.fields == [column]
        assert skipped.schedule_pk == victim.pk
        assert skipped.reason.startswith(f"its {column} ")

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_at_the_read_under_the_lock(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
        victim = a_row("victim")
        worker = Worker(backoff_initial=0)
        assert len(worker._schedule_source.schedules()) == 1
        write_by_sql(TABLE, column, literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.fields == [column]
        assert skipped.schedule_pk == victim.pk

    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_two_fields_are_named_in_the_models_order(
        self, settings, caplog, vendor, column, literal, why
    ):
        only_on(vendor, why)
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        # The end, and the start before it: every engine's own value is one
        # either column keeps.
        write_by_sql(TABLE, "end_time", literal, victim.pk)
        write_by_sql(TABLE, "start_time", literal, victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            DatabaseScheduleSource({}, "default").schedules()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.fields == ["start_time", "end_time"]
        assert skipped.reason.startswith("its start_time ")
        assert "; end_time " in skipped.reason

    def test_a_row_that_reads_and_cannot_be_built_names_none(self, settings, caplog):
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        write_by_sql(TABLE, "task_key", "'only.in.new.code'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            DatabaseScheduleSource({}, "default").schedules()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.fields == []
        assert skipped.schedule_pk == victim.pk
        assert "is not registered as a schedulable task" in skipped.reason

    def test_a_later_line_about_the_row_carries_them_too(self, settings, caplog):
        only_on("sqlite", "SQLite keeps text in an integer column")
        settings.TASKS = stored_tasks()
        victim = a_row("victim")
        write_by_sql(TABLE, "every_seconds", "'abc'", victim.pk)
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 0.001}, "default"
        )
        clock = [0.0]
        source._report.clock = lambda: clock[0]
        with caplog.at_level(logging.INFO, logger="django_ox"):
            source.schedules()
            clock[0] = stored.ROW_REPORT_INTERVAL
            source._last_read = None
            source.schedules()
        first, later = events(caplog, "schedule_row_skipped")
        assert (first.msg, later.msg) == (stored._SKIPPING, stored._STILL_SKIPPING)
        assert first.fields == later.fields == ["every_seconds"]
        assert first.reason == later.reason


# -- the dispatch pass -------------------------------------------------------


class _Raising:
    """A trigger whose arithmetic cannot be done: an error from its own sum."""

    def previous(self, dt):
        raise OverflowError("date value out of range")


class _Querying:
    """A trigger that asks the database, and the database fails."""

    def previous(self, dt):
        raise OperationalError("the connection was lost")


class _OneQuerying:
    def __init__(self, options, backend_alias):
        pass

    def schedules(self):
        return [
            Schedule(
                name="querying",
                task=tasks.record,
                trigger=_Querying(),
                args=("querying",),
                kwargs={},
            )
        ]


class _Mixed:
    """
    A source answering one schedule that dispatches, one whose bound its
    clock cannot be compared with, and one whose tick cannot be derived.
    Any source can answer such a schedule; the dispatch pass is what keeps
    them from the others.
    """

    def __init__(self, options, backend_alias):
        self.alias = backend_alias

    def schedules(self):
        every_second = IntervalTrigger(every=timedelta(seconds=1))
        zoned = datetime(2020, 1, 1, tzinfo=UTC)
        return [
            Schedule(
                name="fine",
                task=tasks.record,
                trigger=every_second,
                args=("fine",),
                kwargs={},
            ),
            Schedule(
                name="zoned-bound",
                task=tasks.record,
                trigger=every_second,
                args=("zoned-bound",),
                kwargs={},
                start_time=zoned,
            ),
            Schedule(
                name="raising",
                task=tasks.record,
                trigger=_Raising(),
                args=("raising",),
                kwargs={},
            ),
        ]


@pytest.mark.django_db(transaction=True)
class TestTheDispatchPassKeepsEachScheduleToItself:
    def test_a_bound_and_a_trigger_that_raise_are_their_schedules_failures(
        self, settings, caplog
    ):
        settings.USE_TZ = False
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {"SCHEDULE_SOURCE": f"{__name__}._Mixed"},
            }
        }
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            worker.dispatch_schedules()
        failed = {
            record.schedule: record
            for record in events(caplog, "schedule_dispatch_error")
        }
        assert set(failed) == {"zoned-bound", "raising"}
        assert failed["zoned-bound"].error == "TypeError"
        assert failed["raising"].error == "OverflowError"
        # The one that can be dispatched was: anchored on its first sight.
        assert OxScheduleTick.objects.filter(schedule_name="fine").exists()
        assert not OxScheduleTick.objects.exclude(schedule_name="fine").exists()

    def test_the_database_failing_there_is_still_the_pass_s_failure(self, settings):
        # Nothing in that part reads the database, but a trigger may; if it
        # does and the database fails, that is not one schedule's failure.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {"SCHEDULE_SOURCE": f"{__name__}._OneQuerying"},
            }
        }
        with pytest.raises(OperationalError):
            Worker(backoff_initial=0).dispatch_schedules()

    def test_a_running_worker_runs_what_is_queued_beside_them(
        self, settings, task_state
    ):
        settings.USE_TZ = False
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {"SCHEDULE_SOURCE": f"{__name__}._Mixed"},
            }
        }
        tasks.record.enqueue("queued")
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        thread = start_worker_thread(worker)
        try:
            # A bound on a wait: "fine" anchors on its first sight and fires
            # on the next second. A worker the others stopped does neither.
            ran = wait_for(
                lambda: {"queued", "fine"} <= set(task_state.get("order", [])),
                timeout=20,
            )
            alive = thread.is_alive()
        finally:
            worker.request_stop()
            thread.join(timeout=30)
        assert ran and alive
        assert "zoned-bound" not in task_state["order"]
        assert "raising" not in task_state["order"]


# -- a worker already running ------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestAWorkerAlreadyRunning:
    @pytest.mark.parametrize(("vendor", "column", "literal", "why"), WHILE_RUNNING)
    def test_keeps_dispatching_and_running_tasks(
        self, settings, caplog, task_state, vendor, column, literal, why
    ):
        """
        The value is written while the worker runs, between two of its full
        reads. Before, the next full read raised: on SQLite and MySQL the
        worker died with it, and on PostgreSQL every dispatch pass after it
        was abandoned, so no schedule fired again.
        """
        only_on(vendor, why)
        # Read in full every fifth of a second, so a read meets the value.
        settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=0.2)
        healthy = a_row("every-second", every_seconds=1)
        victim = a_row("victim")
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            try:
                assert wait_for(
                    lambda: OxScheduleTick.objects.filter(
                        schedule_name=f"db:{healthy.pk}", task__isnull=False
                    ).exists(),
                    timeout=20,
                )
                write_by_sql(TABLE, column, literal, victim.pk)
                written_at = timezone.now()
                tasks.record.enqueue("queued after")
                # Bounds on waits, not measurements: a full read comes every
                # 0.2 s and the healthy schedule's tick every second.
                ran = wait_for(
                    lambda: "queued after" in task_state.get("order", []), timeout=20
                )
                fired = wait_for(
                    lambda: OxScheduleTick.objects.filter(
                        schedule_name=f"db:{healthy.pk}",
                        task__isnull=False,
                        scheduled_for__gte=written_at + timedelta(seconds=2),
                    ).exists(),
                    timeout=20,
                )
                alive = thread.is_alive()
                cached = [s.key for s in worker._schedule_source._cached]
            finally:
                worker.request_stop()
                thread.join(timeout=30)
        assert ran, "the task queued after the value was written ran"
        assert fired, "the healthy schedule fired after a full read met the value"
        assert alive
        assert f"db:{victim.pk}" not in cached
        assert f"db:{healthy.pk}" in cached
        skipped = events(caplog, "schedule_row_skipped")
        assert {record.schedule_pk for record in skipped} == {victim.pk}
        assert not events(caplog, "schedule_dispatch_failed")

    def test_a_worker_built_beside_it_runs_what_is_queued(self, settings, task_state):
        only_on("postgresql", "PostgreSQL keeps year 10000 and cannot read it back")
        settings.TASKS = stored_tasks()
        a_row("every-second", every_seconds=1)
        victim = a_row("victim")
        write_by_sql(TABLE, "end_time", "'10000-01-01 00:00:00+00'", victim.pk)
        tasks.record.enqueue("queued")
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        thread = start_worker_thread(worker)
        try:
            ran = wait_for(
                lambda: {"queued", "every-second"} <= set(task_state.get("order", [])),
                timeout=20,
            )
        finally:
            worker.request_stop()
            thread.join(timeout=30)
        assert ran


def test_the_messages_name_their_placeholders():
    # The wording may change; what it is filled with may not.
    assert re.fullmatch(r"[^{}]*\{field\}[^{}]*", stored._BOUND_HAS_A_ZONE)
    assert re.fullmatch(r"[^{}]*\{field\}[^{}]*", stored._BOUND_HAS_NO_ZONE)
    assert re.fullmatch(r"[^{}]*\{field\}[^{}]*", stored._BOUND_IS_NOT_A_TIME)
    assert "{columns}" in stored._COLUMNS_UNREADABLE
    assert "{error}" in stored._COLUMNS_UNREADABLE
    assert "{error}" in stored._A_VALUE_UNREADABLE
    assert stored._SKIPPING.count("%s") == 2
    assert stored._STILL_SKIPPING.count("%") == 3
    assert stored._MARKER_UNREADABLE.count("%s") == 1
    assert stored._HEAL_FAILED.count("%") == 1
    assert stored._HEAL_STILL_FAILING.count("%") == 3
