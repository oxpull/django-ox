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
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import OperationalError, connection
from django.urls import reverse
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule, OxScheduleChange, OxScheduleTick
from django_ox.registry import ScheduleKind, register
from django_ox.schedules import IntervalTrigger, Schedule
from django_ox.stored import (
    DatabaseScheduleSource,
    _export_preflight,
    create_schedule,
    create_schedules,
    update_schedule,
)
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for

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
        assert first.reason.startswith(f"its {column} could not be read: ")
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
            (victim.pk, stored._BOUND_HAS_A_ZONE.format(field="start_time"))
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
        assert skipped.reason.startswith(f"its {column} could not be read: ")
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
        assert skipped.reason.startswith("its every_seconds could not be read: ")
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

    def test_a_heal_that_meets_a_row_that_cannot_be_read(self, settings, caplog):
        """
        A row seen at dispatch with its timing changed around the functions
        is healed on the next pass. Changed again before that, so that it
        cannot be read, the heal cannot move its boundary: the sighting goes
        and the row is reported, where before the read raised for every row.
        """
        only_on("sqlite", "SQLite keeps a date that does not exist")
        settings.TASKS = stored_tasks(SCHEDULE_RECONCILE_INTERVAL=3600)
        a_row("beside-it")
        victim = a_row("victim")
        worker = Worker(backoff_initial=0)
        source = worker._schedule_source
        source.schedules()
        OxSchedule.objects.filter(pk=victim.pk).update(every_seconds=120)
        worker.dispatch_schedules()
        assert victim.pk in source._needs_heal
        write_by_sql(TABLE, "end_time", "'2026-02-30 00:00:00'", victim.pk)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            source.schedules()
        assert victim.pk not in source._needs_heal
        assert {
            (r.schedule, r.schedule_pk) for r in events(caplog, "schedule_row_skipped")
        } == {("victim", victim.pk)}
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
            # Said once, not once a pass.
            assert len(unreadable) == 1
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
    def test_a_value_refused_by_the_server_leaves_the_transaction_usable(
        self, settings, caplog
    ):
        """
        The rows are read again in ranges, each in a savepoint, because a
        source may be read inside a transaction the caller owns (this test
        runs in one). A value psycopg cannot convert fails on the client
        and leaves the transaction as it was; one the server refuses aborts
        it, and without the savepoint every read after it in the pass would
        fail with it. Made to happen here by having the server refuse the
        first range read.
        """
        only_on("postgresql", "only PostgreSQL aborts a transaction on an error")
        settings.TASKS = stored_tasks()
        a_row("beside-it")
        victim = a_row("victim")
        write_by_sql(TABLE, "end_time", "'infinity'", victim.pk)
        refused = []

        def refuse_the_first_range(execute, sql, params, many, context):
            if TABLE in sql and '"id" >= ' in sql and not refused:
                refused.append(sql)
                return execute("SELECT 1/0", None, many, context)
            return execute(sql, params, many, context)

        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            connection.execute_wrapper(refuse_the_first_range),
        ):
            built = DatabaseScheduleSource({}, "default").schedules()
        assert refused
        assert [schedule.name for schedule in built] == ["beside-it"]
        assert {r.schedule_pk for r in events(caplog, "schedule_row_skipped")} == {
            victim.pk
        }


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
