"""
Reading stored schedules and the tick log when a value in them does not read.

Django's own read of a stored value it cannot convert raises in the middle
of a result, and takes every row of that result with it: PostgreSQL keeps
year 10000, 'infinity' and dates before year 1, SQLite keeps a date that
does not exist and text that is not UTF-8, MySQL keeps zero dates. On SQLite
some values read as something else instead, with nothing raised: a start of
'banana' reads as None, an `enabled` of 2 reads as False, text in an integer
column reads as text.

django_ox._stored_read fetches the values whose reading can fail or mislead
as the database holds them and decodes each on its own. A value Django reads
must decode to exactly what Django reads; one Django raises on or misreads
must be reported by field, beside healthy rows and keys that still read.

Every value below is written by SQL, as only SQL can write it, in the form
the engine that keeps it keeps it, and the test first shows Django's own
read raising on it or misreading it. A test for another engine's value
skips and says why.

No read takes a savepoint. One that fails in this process, on a value the
driver cannot hand over, is followed by narrower reads, in the same
transaction where there is one. One the database itself refuses is raised
as it is, with nothing read after it, inside a transaction and with none
open: its error is the database's, and no row or key is named for it.
"""

import math
import pickle
import random
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from django.core.exceptions import FieldDoesNotExist
from django.db import (
    DatabaseError,
    DataError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.models import Max
from django.db.transaction import TransactionManagementError
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import _stored_read
from django_ox._stored_read import (
    TickRead,
    TickRow,
    UnreadableRow,
    UnreadableValue,
    as_text,
    can_read_on,
    decode_boolean,
    decode_datetime,
    decode_integer,
    earliest_tick,
    is_unreadable_value,
    latest_ticks,
    lock_schedule,
    newest_tick_pk,
    read_schedules,
    read_ticks,
)
from django_ox.exceptions import StoredValueUnreadable
from django_ox.models import OxSchedule, OxScheduleTick

from .conftest import wait_for
from .dead_connection_tasks import from_another_connection
from .isolation import kill

SCHEDULES = "django_ox_oxschedule"
TICKS = "django_ox_oxscheduletick"
ALIAS = "default"


def a_row(name, **over):
    """A healthy stored schedule, written by the ORM with no validation."""
    now = timezone.now()
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "interval",
        "every_seconds": 60,
        "arguments": {"label": name},
        "start_time": now - timedelta(minutes=10),
        "created_at": now,
        "updated_at": now,
    }
    fields.update(over)
    return OxSchedule.objects.create(**fields)


def a_tick(key, scheduled_for):
    return OxScheduleTick.objects.create(
        schedule_name=key, scheduled_for=scheduled_for, created_at=scheduled_for
    )


def many_rows(count):
    """`count` healthy stored schedules, written in one go; their keys in order."""
    now = timezone.now()
    OxSchedule.objects.bulk_create(
        OxSchedule(
            name=f"row-{i}",
            task_key="report",
            trigger="interval",
            every_seconds=60,
            arguments={"label": f"row-{i}"},
            start_time=now - timedelta(minutes=10),
            created_at=now,
            updated_at=now,
        )
        for i in range(count)
    )
    return list(OxSchedule.objects.order_by("pk").values_list("pk", flat=True))


def by_sql(statement, params=()):
    """
    Run one statement the way only SQL can write what it writes.

    MySQL refuses a zero date and a NULL in a NOT NULL column in the strict
    mode the suite runs in, so its session is let off for the statement and
    put back.
    """
    with connection.cursor() as cursor:
        if connection.vendor != "mysql":
            cursor.execute(statement, params)
            return
        cursor.execute("SELECT @@SESSION.sql_mode")
        (mode,) = cursor.fetchone()
        cursor.execute("SET SESSION sql_mode = ''")
        try:
            cursor.execute(statement, params)
        finally:
            cursor.execute("SET SESSION sql_mode = %s", [mode])


def set_column(pk, column, literal):
    by_sql(f"UPDATE {SCHEDULES} SET {column} = {literal} WHERE id = {int(pk)}")  # noqa: S608


def insert_tick(key, literal):
    """A tick row whose scheduled_for is `literal`, as SQL; its primary key."""
    created = {
        "postgresql": "now()",
        "mysql": "NOW(6)",
    }.get(connection.vendor, "'2026-10-05 00:00:00'")
    by_sql(
        f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
        f"VALUES (%s, {literal}, NULL, {created})",
        [key],
    )
    return (
        OxScheduleTick.objects.filter(schedule_name=key)
        .order_by("-pk")
        .values_list("pk", flat=True)
        .first()
    )


def only_on(vendor, why):
    if connection.vendor != vendor:
        pytest.skip(f"a {vendor} value: {why}; this run is on {connection.vendor}")


def orm_read(pk):
    """Django's own read of a stored row: the row, or the exception it raised."""
    try:
        with transaction.atomic():
            return OxSchedule.objects.get(pk=pk)
    except Exception as exc:  # recorded, and asserted on by the caller
        return exc


def misread(found):
    """
    Django's own read of a date and time raised, or gave something else:
    None, which reads as no value at all, or the stored text. Which of the
    two depends on the engine and on USE_TZ.
    """
    if isinstance(found, datetime) and found.replace(tzinfo=None) in (
        datetime.min,
        datetime.max,
    ):
        # What psycopg2 hands over for PostgreSQL's '-infinity' and 'infinity'.
        return True
    return isinstance(found, Exception) or not isinstance(found, datetime)


def same(left, right):
    """Equal, and for a date and time the same zone or none, and the same fold."""
    if isinstance(left, datetime) or isinstance(right, datetime):
        return (
            isinstance(left, datetime)
            and isinstance(right, datetime)
            and left.replace(tzinfo=None) == right.replace(tzinfo=None)
            and left.tzinfo == right.tzinfo
            and left.utcoffset() == right.utcoffset()
            and left.fold == right.fold
        )
    return type(left) is type(right) and left == right


def assert_reads_as_django_does(read, pk):
    assert isinstance(read, OxSchedule), read
    expected = OxSchedule.objects.get(pk=pk)
    for model_field in OxSchedule._meta.concrete_fields:
        name = model_field.attname
        assert same(getattr(read, name), getattr(expected, name)), name
    assert read._state.db == ALIAS
    assert not read._state.adding


def assert_all_read_as_django_does(found, leaving_out=()):
    """Every stored row is in `found` as Django's own read of the table gives it."""
    expected = {row.pk: row for row in OxSchedule.objects.exclude(pk__in=leaving_out)}
    assert found.keys() - set(leaving_out) == expected.keys()
    for pk, row in expected.items():
        assert isinstance(found[pk], OxSchedule), found[pk]
        for model_field in OxSchedule._meta.concrete_fields:
            name = model_field.attname
            assert same(getattr(found[pk], name), getattr(row, name)), (pk, name)


class Reads:
    """connection.execute_wrapper() that keeps each SELECT on `table` as sent."""

    def __init__(self, table):
        self.table = table
        self.sent = []

    def __call__(self, execute, sql, params, many, context):
        if self.table in sql and sql.lstrip().upper().startswith("SELECT"):
            self.sent.append((sql, tuple(params or ())))
        return execute(sql, params, many, context)

    @property
    def parameters(self):
        """How many parameters each statement carried, in the order sent."""
        return [len(params) for _sql, params in self.sent]


@pytest.fixture
def parameter_limit(monkeypatch):
    """
    Lowers how many parameters this connection takes in a statement.

    On the features class: SQLite's is a property that reads the live
    connection, and PostgreSQL's is cached on the instance, which is cleared
    so that the class's is what is read. SQLite's own limit is lowered with
    it, so a statement over the limit is refused rather than only counted.
    """
    lowered = []

    def lower(limit):
        monkeypatch.setattr(type(connection.features), "max_query_params", limit)
        connection.features.__dict__.pop("max_query_params", None)
        if connection.vendor == "sqlite":
            raw = connection.connection
            lowered.append((raw, raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)))
            raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit)

    yield lower
    for raw, before in lowered:
        raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, before)


def selects(queries, table):
    return [
        q["sql"]
        for q in queries.captured_queries
        if q["sql"].lstrip().upper().startswith("SELECT") and table in q["sql"]
    ]


def columns_read(statement):
    """The schedule columns a SELECT takes, by name."""
    taken = statement.split(" FROM ")[0]
    return {
        model_field.column
        for model_field in OxSchedule._meta.concrete_fields
        if connection.ops.quote_name(model_field.column) in taken
    }


def savepoints(queries):
    """The statements among `queries` that make, release or go back to a savepoint."""
    return [
        q["sql"]
        for q in queries.captured_queries
        if q["sql"]
        .lstrip()
        .upper()
        .startswith(("SAVEPOINT", "RELEASE SAVEPOINT", "ROLLBACK TO"))
    ]


#: Which PostgreSQL driver Django is running on. psycopg 3 raises a DataError
#: for a timestamp it cannot load; psycopg2 raises ValueError, and loads
#: 'infinity' and '-infinity' as the largest and smallest datetime.
USES_PSYCOPG_3 = connection.vendor != "postgresql" or "psycopg2" not in (
    connection.Database.__name__
)

#: The class Django gives an error the server raised on a value it refuses to
#: convert: a DataError, except through mysqlclient, which raises MySQL's 1367
#: as an OperationalError.
REFUSED = (
    OperationalError
    if connection.vendor == "mysql" and connection.Database.__name__ == "MySQLdb"
    else DataError
)

#: Text SQLite keeps that its driver cannot decode as UTF-8.
NOT_UTF_8 = "CAST(X'62616480FF' AS TEXT)"


class Refusing:
    """
    connection.execute_wrapper() for a database that refuses one read.

    The `nth` SELECT that mentions `table` is replaced by a statement the
    server itself raises a DataError on, so the session is left as a real
    refusal leaves it: on PostgreSQL the transaction takes nothing more.
    SQLite has no such statement, so there the error is raised in the
    statement's place. Every statement sent after the refusal is kept in
    `after`.
    """

    STATEMENT = {"postgresql": "SELECT 1/0", "mysql": "SELECT 1e400"}
    #: What each database says, which is not what it says of a transaction
    #: that has already failed.
    SAYS = {
        "postgresql": "division by zero",
        "mysql": "Illegal double",
        "sqlite": "string or blob too big",
    }

    def __init__(self, table, nth=1):
        self.table = table
        self.nth = nth
        self.refused = None
        self.after = []

    def __call__(self, execute, sql, params, many, context):
        if self.refused is not None:
            self.after.append(sql)
            return execute(sql, params, many, context)
        if self.table in sql and sql.lstrip().upper().startswith("SELECT"):
            self.nth -= 1
            if not self.nth:
                self.refused = sql
                statement = self.STATEMENT.get(connection.vendor)
                if statement is None:
                    raise DataError(self.SAYS["sqlite"])
                return execute(statement, None, many, context)
        return execute(sql, params, many, context)

    def assert_raised_as_it_was(self, caught):
        """The database's own error reached the caller, and nothing followed it."""
        assert self.refused is not None, "the read was never sent"
        assert type(caught.value) is REFUSED
        assert self.SAYS[connection.vendor] in str(caught.value)
        assert self.after == []


class Failing:
    """
    connection.execute_wrapper() for a read that fails in this process, on
    one value.

    Every SELECT on `table` that takes `column` and has `having` among its
    parameters fails as a read does when a value in its answer cannot be
    turned into Python: the database answers, and the failure comes after.
    On PostgreSQL psycopg is sent a timestamp it cannot load, and on
    SQLite the driver is sent text that is not UTF-8, so each raises its
    own error for real. MySQL's driver hands every value over, and what
    fails there is Django's converter on a zero date, whose error is
    raised in the statement's place. The statements that failed are kept
    in `failed`.
    """

    STATEMENT = {
        "postgresql": "SELECT '10000-01-01 00:00:00+00'::timestamptz",
        "sqlite": f"SELECT {NOT_UTF_8}",
    }
    #: The class of what each raises: the driver's own on PostgreSQL and
    #: SQLite, and on MySQL what the converter raises.
    RAISES = {
        "postgresql": DataError if USES_PSYCOPG_3 else ValueError,
        "sqlite": OperationalError,
        "mysql": AttributeError,
    }

    def __init__(self, table, column, having):
        self.table = table
        self.column = connection.ops.quote_name(column)
        self.having = having
        self.failed = []

    def __call__(self, execute, sql, params, many, context):
        taken = sql.split(" FROM ")[0]
        if (
            self.table in sql
            and sql.lstrip().upper().startswith("SELECT")
            and self.column in taken
            and self.having in (params or ())
        ):
            self.failed.append(sql)
            statement = self.STATEMENT.get(connection.vendor)
            if statement is None:
                raise AttributeError("'str' object has no attribute 'utcoffset'")
            return execute(statement, None, many, context)
        return execute(sql, params, many, context)


def raised_by(statement):
    """
    What one statement raises on this connection, sent and fetched as a
    read is. In a block of its own, so that a transaction around the test
    outlives an error the server raised.
    """
    try:
        with transaction.atomic(), connection.cursor() as cursor:
            cursor.execute(statement)
            cursor.fetchall()
    except Exception as exc:  # handed to the caller, which asserts on it
        return exc
    raise AssertionError(f"{statement} raised nothing")


def refused_by_sqlite():
    """
    A DataError SQLite itself raises, for the tests that need the real
    thing: the longest value the connection takes is lowered, a longer one
    is asked for, and the limit is put back.
    """
    raw = connection.connection
    before = raw.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
    raw.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1000)
    try:
        return raised_by("SELECT zeroblob(2000)")
    finally:
        raw.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, before)


# -- what Django's own read does with each value --------------------------

RAISES = "raises"


def raises_or_text():
    # PyMySQL hands a zero date over as text, which Django's converter, with
    # USE_TZ on, cannot give a zone to. With it off, the text is the value.
    from django.conf import settings

    return RAISES if settings.USE_TZ else str


#: (engine, column, SQL literal, how Django's read takes it, id). The value
#: each engine keeps that Django's read raises on or misreads.
UNREADABLE_ROW_VALUES = [
    pytest.param(
        "sqlite", "end_time", "'10000-01-01 00:00:00'", None, id="sqlite-end-year-10000"
    ),
    pytest.param(
        "sqlite",
        "start_time",
        "'10000-01-01 00:00:00'",
        None,
        id="sqlite-start-year-10000",
    ),
    pytest.param("sqlite", "start_time", "'banana'", None, id="sqlite-start-banana"),
    pytest.param("sqlite", "end_time", "'banana'", None, id="sqlite-end-banana"),
    pytest.param(
        "sqlite",
        "end_time",
        "'2026-02-30 00:00:00'",
        RAISES,
        id="sqlite-end-does-not-exist",
    ),
    pytest.param(
        "sqlite",
        "created_at",
        "'2026-02-30 00:00:00'",
        RAISES,
        id="sqlite-created-does-not-exist",
    ),
    pytest.param("sqlite", "updated_at", "'banana'", None, id="sqlite-updated-banana"),
    pytest.param("sqlite", "start_time", "2.5", None, id="sqlite-start-a-fraction"),
    pytest.param("sqlite", "every_seconds", "'abc'", "abc", id="sqlite-interval-text"),
    pytest.param("sqlite", "phase_seconds", "'abc'", "abc", id="sqlite-phase-text"),
    pytest.param(
        "sqlite", "starting_deadline_seconds", "'abc'", "abc", id="sqlite-deadline-text"
    ),
    pytest.param(
        "sqlite", "boundary_generation", "'abc'", "abc", id="sqlite-generation-text"
    ),
    pytest.param(
        "sqlite", "every_seconds", "60.5", 60.5, id="sqlite-interval-fraction"
    ),
    pytest.param("sqlite", "enabled", "2", False, id="sqlite-enabled-2"),
    pytest.param("sqlite", "enabled", "'abc'", False, id="sqlite-enabled-text"),
    pytest.param(
        "sqlite",
        "name",
        NOT_UTF_8,
        RAISES,
        id="sqlite-name-not-utf-8",
    ),
    pytest.param(
        "sqlite", "name", "X'626c6f622d6e616d65'", b"blob-name", id="sqlite-name-blob"
    ),
    pytest.param(
        "sqlite", "task_key", "X'626c6f62'", b"blob", id="sqlite-task-key-blob"
    ),
    pytest.param(
        "sqlite", "boundary_for", "X'626c6f62'", b"blob", id="sqlite-boundary-for-blob"
    ),
    pytest.param(
        "postgresql",
        "end_time",
        "'10000-01-01 00:00:00+00'",
        RAISES,
        id="postgresql-end-year-10000",
    ),
    pytest.param(
        "postgresql",
        "start_time",
        "'10000-01-01 00:00:00+00'",
        RAISES,
        id="postgresql-start-year-10000",
    ),
    pytest.param(
        "postgresql",
        "updated_at",
        "'10000-01-01 00:00:00+00'",
        RAISES,
        id="postgresql-updated-year-10000",
    ),
    pytest.param(
        "postgresql", "end_time", "'infinity'", RAISES, id="postgresql-end-infinity"
    ),
    pytest.param(
        "postgresql",
        "start_time",
        "'-infinity'",
        RAISES,
        id="postgresql-start-minus-infinity",
    ),
    pytest.param(
        "postgresql",
        "start_time",
        "'0001-01-01 00:00:00+00 BC'",
        RAISES,
        id="postgresql-start-bc",
    ),
    pytest.param(
        "mysql",
        "end_time",
        "'0000-00-00 00:00:00'",
        raises_or_text,
        id="mysql-end-zero-date",
    ),
    pytest.param(
        "mysql",
        "start_time",
        "'2026-00-10 00:00:00'",
        raises_or_text,
        id="mysql-start-zero-month",
    ),
    pytest.param(
        "mysql",
        "start_time",
        "NULL",
        raises_or_text,
        id="mysql-start-null-kept-as-zero",
    ),
    pytest.param(
        "mysql",
        "created_at",
        "'0000-00-00 00:00:00'",
        raises_or_text,
        id="mysql-created-zero-date",
    ),
    pytest.param("mysql", "enabled", "2", 2, id="mysql-enabled-2"),
]


#: Values every engine keeps that read, and that validation, not the read,
#: has to refuse, as SET clauses. The reader hands them over as Django does.
READABLE_BUT_INVALID = [
    pytest.param(
        "{trigger} = 'cron', cron = 'banana', every_seconds = NULL", id="cron-text"
    ),
    pytest.param("task_key = 'nope.missing'", id="unregistered-key"),
    pytest.param("arguments = '[1, 2]'", id="arguments-not-a-mapping"),
    pytest.param("every_seconds = 0", id="interval-zero"),
    pytest.param("end_time = NULL", id="no-end"),
    pytest.param("enabled = FALSE", id="disabled"),
]


#: One value per engine for the tests that only need one.
ONE_PER_ENGINE = {
    "sqlite": ("end_time", "'2026-02-30 00:00:00'"),
    "postgresql": ("end_time", "'10000-01-01 00:00:00+00'"),
    "mysql": ("end_time", "'0000-00-00 00:00:00'"),
}


def corrupt(pk):
    column, literal = ONE_PER_ENGINE[connection.vendor]
    set_column(pk, column, literal)
    return column


def show_django_misreads(pk, column, expected):
    """
    Django's own read of the row raises, or gives the stored value as
    `expected`: the evidence that this value is one the reader must catch.
    """
    if callable(expected) and expected is not str:
        expected = expected()
    found = orm_read(pk)
    if expected is RAISES:
        # Or, on psycopg2, an 'infinity' read as the largest datetime.
        assert isinstance(found, Exception) or misread(getattr(found, column)), getattr(
            found, column
        )
        return
    assert isinstance(found, OxSchedule), found
    if expected is str:
        assert isinstance(getattr(found, column), str)
    else:
        assert same(getattr(found, column), expected), getattr(found, column)


@pytest.mark.django_db
class TestAStoredRowThatDoesNotRead:
    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "django"), UNREADABLE_ROW_VALUES
    )
    def test_is_named_by_field_and_the_rows_beside_it_read(
        self, vendor, column, literal, django
    ):
        only_on(vendor, "kept by that engine only")
        before, bad, after = a_row("before"), a_row("bad"), a_row("after")
        set_column(bad.pk, column, literal)
        show_django_misreads(bad.pk, column, django)

        found = read_schedules([before.pk, bad.pk, after.pk], using=ALIAS)

        assert list(found) == [before.pk, bad.pk, after.pk]
        assert_reads_as_django_does(found[before.pk], before.pk)
        assert_reads_as_django_does(found[after.pk], after.pk)
        unreadable = found[bad.pk]
        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.pk == bad.pk
        assert unreadable.alias == ALIAS
        assert unreadable.fields == (column,)
        assert unreadable.reason.startswith(f"{column} ")
        if column != "name":
            # Decoded on its own, not found by a read that failed whole.
            assert unreadable.reason.startswith(f"{column} holds ")
            assert isinstance(unreadable.cause, UnreadableValue)
        assert column not in unreadable.values
        assert unreadable.values["id"] == bad.pk
        if column != "name":
            assert unreadable.values["name"] == "bad"
        if column not in ("name", "task_key"):
            assert unreadable.values["task_key"] == "report"

    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "django"), UNREADABLE_ROW_VALUES
    )
    def test_raises_stored_value_unreadable_with_its_cause(
        self, vendor, column, literal, django
    ):
        only_on(vendor, "kept by that engine only")
        bad = a_row("bad")
        set_column(bad.pk, column, literal)

        unreadable = read_schedules([bad.pk], using=ALIAS)[bad.pk]
        with pytest.raises(StoredValueUnreadable) as caught:
            raise unreadable.exception()

        exc = caught.value
        assert (exc.alias, exc.model, exc.pk) == (ALIAS, "django_ox.OxSchedule", bad.pk)
        assert exc.fields == (column,)
        assert exc.reason == unreadable.reason
        assert exc.__cause__ is unreadable.cause
        assert isinstance(exc.__cause__, Exception)
        assert not isinstance(exc, DatabaseError)
        assert str(exc) == (
            f"The django_ox.OxSchedule row with primary key {bad.pk!r} in database "
            f"'default' holds a value that cannot be read: {exc.reason}"
        )

    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "django"), UNREADABLE_ROW_VALUES
    )
    def test_gives_an_instance_of_what_did_read(self, vendor, column, literal, django):
        only_on(vendor, "kept by that engine only")
        bad = a_row("bad")
        set_column(bad.pk, column, literal)

        instance = read_schedules([bad.pk], using=ALIAS)[bad.pk].deferred_instance()

        assert instance.pk == bad.pk
        assert instance._state.db == ALIAS
        assert instance.get_deferred_fields() == {column}

    @pytest.mark.parametrize("assignments", READABLE_BUT_INVALID)
    def test_a_value_that_reads_is_handed_over_as_django_reads_it(self, assignments):
        row = a_row("row", end_time=timezone.now() + timedelta(days=1))
        clause = assignments.format(trigger=connection.ops.quote_name("trigger"))
        by_sql(f"UPDATE {SCHEDULES} SET {clause} WHERE id = %s", [row.pk])  # noqa: S608

        assert_reads_as_django_does(
            read_schedules([row.pk], using=ALIAS)[row.pk], row.pk
        )

    def test_a_huge_interval_reads_and_is_left_to_validation(self):
        only_on("sqlite", "only SQLite's integer column holds 2**62")
        row = a_row("row")
        set_column(row.pk, "every_seconds", str(2**62))

        assert_reads_as_django_does(
            read_schedules([row.pk], using=ALIAS)[row.pk], row.pk
        )

    def test_a_row_that_is_gone_is_left_out(self):
        row = a_row("row")
        gone = a_row("gone")
        gone_pk = gone.pk
        gone.delete()

        assert list(read_schedules([gone_pk, row.pk], using=ALIAS)) == [row.pk]
        assert read_schedules([], using=ALIAS) == {}

    def test_a_queryset_is_read_for_its_keys(self):
        rows = [a_row(f"row-{i}") for i in range(3)]
        bad = a_row("bad")
        corrupt(bad.pk)

        found = read_schedules(OxSchedule.objects.order_by("-pk"), using=ALIAS)

        assert list(found) == [bad.pk, *[r.pk for r in reversed(rows)]]
        assert isinstance(found[bad.pk], UnreadableRow)
        for row in rows:
            assert_reads_as_django_does(found[row.pk], row.pk)


# -- a count of boundary writes that cannot be raised -----------------------

#: The most the count of boundary writes can be on each engine, written out
#: here so that what the reader goes by, Django's description of the column,
#: is checked against the number and against the database itself.
LARGEST_COUNT = {
    "postgresql": 2147483647,
    "mysql": 4294967295,
    "sqlite": 9223372036854775807,
}

COUNT = "boundary_generation"


def largest_count():
    return LARGEST_COUNT[connection.vendor]


def a_full_row(name="full"):
    """A healthy stored schedule whose count of boundary writes SQL set to the most."""
    row = a_row(name)
    set_column(row.pk, COUNT, str(largest_count()))
    return row


def says_the_count_is_full(unreadable):
    """The row is reported for its count alone, with the count's own reason."""
    assert isinstance(unreadable, UnreadableRow), unreadable
    assert unreadable.fields == (COUNT,)
    assert unreadable.reason == (
        f"{COUNT} holds {largest_count()}, the most its column can hold, so the "
        "count cannot be raised"
    )
    assert isinstance(unreadable.cause, UnreadableValue)
    return True


@pytest.mark.django_db
class TestACountOfBoundaryWritesAtItsMaximum:
    """
    The count of boundary writes is a whole number, and at the most its
    column can hold it still reads as one: Django's own read hands it over.
    What it cannot do is go up, and every write that moves a schedule's
    boundary adds one to it. PostgreSQL and MySQL refuse that write, each
    time it is tried, and SQLite keeps the sum as a float. Only SQL can put
    the count there.

    The reader answers such a row as one that does not read, by its count,
    before any write is tried. The maximum is the one Django gives for the
    column on the connection, not a number kept in the reader.
    """

    def test_django_describes_the_column_as_each_database_holds_it(self):
        field = OxSchedule._meta.get_field(COUNT)

        _, highest = connection.ops.integer_field_range(field.get_internal_type())

        assert highest == largest_count()

    def test_django_reads_it_and_the_database_does_not_add_one_to_it(self):
        row = a_full_row()
        assert OxSchedule.objects.get(pk=row.pk).boundary_generation == largest_count()
        adds_one = f"UPDATE {SCHEDULES} SET {COUNT} = {COUNT} + 1 WHERE id = {row.pk}"  # noqa: S608

        if connection.vendor != "sqlite":
            refused = raised_by(adds_one)
            # The database's own error, which is never put down to a row.
            assert type(refused) is DataError
            assert not is_unreadable_value(refused, using=ALIAS)
            return
        by_sql(adds_one)
        kept_as = f"SELECT typeof({COUNT}) FROM {SCHEDULES} WHERE id = %s"  # noqa: S608
        with connection.cursor() as cursor:
            cursor.execute(kept_as, [row.pk])
            assert cursor.fetchone() == ("real",)
        # What the sum left is a count that does not read, for its own reason.
        left = read_schedules([row.pk], using=ALIAS)[row.pk]
        assert left.fields == (COUNT,)
        assert left.reason.endswith("which is not a whole number")

    def test_is_named_by_its_field_and_the_rows_beside_it_read(self):
        before, full, after = a_row("before"), a_full_row(), a_row("after")

        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([before.pk, full.pk, after.pk], using=ALIAS)

        # Decided from what the one statement fetched, with nothing sent for it.
        assert len(queries.captured_queries) == 1
        assert list(found) == [before.pk, full.pk, after.pk]
        assert_reads_as_django_does(found[before.pk], before.pk)
        assert_reads_as_django_does(found[after.pk], after.pk)
        unreadable = found[full.pk]
        assert says_the_count_is_full(unreadable)
        assert (unreadable.pk, unreadable.alias) == (full.pk, ALIAS)
        # Every other value of the row is handed over as it read.
        every_other = {
            model_field.attname for model_field in OxSchedule._meta.concrete_fields
        } - {COUNT}
        assert set(unreadable.values) == every_other
        assert unreadable.values["name"] == "full"
        assert unreadable.deferred_instance().get_deferred_fields() == {COUNT}

    def test_raises_stored_value_unreadable_naming_the_count(self):
        full = a_full_row()

        unreadable = read_schedules([full.pk], using=ALIAS)[full.pk]
        with pytest.raises(StoredValueUnreadable) as caught:
            raise unreadable.exception()

        assert caught.value.fields == (COUNT,)
        assert caught.value.reason == unreadable.reason
        assert caught.value.__cause__ is unreadable.cause

    def test_one_below_it_reads_as_django_reads_it(self):
        row = a_row("row")
        set_column(row.pk, COUNT, str(largest_count() - 1))

        found = read_schedules([row.pk], using=ALIAS)[row.pk]

        assert_reads_as_django_does(found, row.pk)
        assert found.boundary_generation == largest_count() - 1

    def test_the_maximum_is_the_one_django_gives_for_the_connection(self, monkeypatch):
        at_it, below_it = a_row("at-it"), a_row("below-it")
        set_column(at_it.pk, COUNT, "7")
        set_column(below_it.pk, COUNT, "6")
        asked = []

        def holds_up_to_seven(internal_type):
            asked.append(internal_type)
            return (0, 7)

        monkeypatch.setattr(connection.ops, "integer_field_range", holds_up_to_seven)
        found = read_schedules([at_it.pk, below_it.pk], using=ALIAS)

        unreadable = found[at_it.pk]
        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.fields == (COUNT,)
        assert unreadable.reason.startswith(f"{COUNT} holds 7, the most its column")
        assert isinstance(found[below_it.pk], OxSchedule)
        # Asked by the type of the count's own field, once for each row.
        field = OxSchedule._meta.get_field(COUNT)
        assert asked == [field.get_internal_type()] * 2

    @pytest.mark.parametrize(
        "column", ["every_seconds", "phase_seconds", "starting_deadline_seconds"]
    )
    def test_no_other_whole_number_is_held_to_it(self, column):
        row = a_row("row")
        set_column(row.pk, column, str(largest_count()))

        assert_reads_as_django_does(
            read_schedules([row.pk], using=ALIAS)[row.pk], row.pk
        )

    def test_beside_a_value_that_does_not_read_both_are_named(self):
        full = a_full_row()
        other = corrupt(full.pk)

        unreadable = read_schedules([full.pk], using=ALIAS)[full.pk]

        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.fields == (COUNT, other)
        assert unreadable.unreadable[COUNT].startswith(f"holds {largest_count()}, ")

    def test_a_read_that_does_not_take_the_count_does_not_meet_it(self):
        full = a_full_row()

        without = read_schedules([full.pk], using=ALIAS, fields=["task_key"])[full.pk]
        with_it = read_schedules([full.pk], using=ALIAS, fields=["task_key", COUNT])[
            full.pk
        ]

        assert isinstance(without, OxSchedule)
        assert COUNT in without.get_deferred_fields()
        assert without.task_key == "report"
        assert says_the_count_is_full(with_it)
        assert with_it.values == {"id": full.pk, "task_key": "report"}

    def test_the_locked_read_names_it_too(self):
        full = a_full_row()

        with transaction.atomic():
            found = lock_schedule(full.pk, using=ALIAS)
            some = lock_schedule(full.pk, using=ALIAS, fields=["enabled", COUNT])

        assert says_the_count_is_full(found)
        assert says_the_count_is_full(some)
        assert some.values == {"id": full.pk, "enabled": True}


@pytest.mark.django_db(transaction=True)
def test_a_full_count_is_named_in_a_row_read_a_column_at_a_time():
    """
    A row the driver cannot hand over whole is read again a column at a
    time, and its count is held to the same rule there.
    """
    before, full = a_row("before"), a_full_row()
    failing = Failing(SCHEDULES, "name", full.pk)

    with connection.execute_wrapper(failing):
        found = read_schedules([before.pk, full.pk], using=ALIAS)

    assert failing.failed, "the read never failed"
    assert_reads_as_django_does(found[before.pk], before.pk)
    unreadable = found[full.pk]
    assert isinstance(unreadable, UnreadableRow)
    assert unreadable.fields == ("name", COUNT)
    assert unreadable.unreadable["name"].startswith("could not be read: ")
    assert unreadable.unreadable[COUNT] == (
        f"holds {largest_count()}, the most its column can hold, so the count "
        "cannot be raised"
    )


@pytest.mark.django_db
class TestReadingRowsInBatches:
    def test_healthy_rows_are_read_five_hundred_a_statement(self):
        pks = many_rows(1200)
        reads = Reads(SCHEDULES)

        with connection.execute_wrapper(reads):
            found = read_schedules(OxSchedule.objects.order_by("pk"), using=ALIAS)

        # The keys, which take no parameter, and then the rows, a key a
        # parameter: three statements for twelve hundred of them.
        assert reads.parameters == [0, 500, 500, 200]
        assert list(found) == pks
        assert_all_read_as_django_does(found)

    def test_a_connection_that_takes_fewer_parameters_is_read_in_smaller_batches(
        self, parameter_limit
    ):
        pks = many_rows(250)
        parameter_limit(100)
        reads = Reads(SCHEDULES)

        with connection.execute_wrapper(reads):
            found = read_schedules(pks, using=ALIAS)

        assert reads.parameters == [100, 100, 50]
        assert list(found) == pks
        assert_all_read_as_django_does(found)

    def test_a_limit_above_five_hundred_leaves_the_batch_at_five_hundred(
        self, parameter_limit
    ):
        pks = many_rows(501)
        parameter_limit(999)
        reads = Reads(SCHEDULES)

        with connection.execute_wrapper(reads):
            found = read_schedules(pks, using=ALIAS)

        assert reads.parameters == [500, 1]
        assert list(found) == pks

    @pytest.mark.parametrize("position", ["first", "middle", "last"])
    def test_a_bad_row_anywhere_loses_no_healthy_row(self, position):
        rows = [a_row(f"row-{i}") for i in range(9)]
        bad = {"first": rows[0], "middle": rows[4], "last": rows[-1]}[position]
        column = corrupt(bad.pk)

        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([r.pk for r in rows], using=ALIAS)

        # The value is decoded on its own, so the batch still reads in one.
        assert len(selects(queries, SCHEDULES)) == 1
        assert found[bad.pk].fields == (column,)
        for row in rows:
            if row is not bad:
                assert_reads_as_django_does(found[row.pk], row.pk)

    @pytest.mark.parametrize("position", ["first", "middle", "last"])
    def test_a_row_the_driver_cannot_hand_over_is_found_by_halving(self, position):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        rows = [a_row(f"row-{i}") for i in range(16)]
        bad = {"first": rows[0], "middle": rows[7], "last": rows[-1]}[position]
        set_column(bad.pk, "name", NOT_UTF_8)
        assert isinstance(orm_read(bad.pk), OperationalError)

        # Inside this test's transaction, and in no savepoint: the failure
        # is the driver's, raised once SQLite has answered, and the
        # transaction takes the narrower reads as it took the first.
        assert connection.in_atomic_block
        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([r.pk for r in rows], using=ALIAS)

        assert not savepoints(queries)
        assert found[bad.pk].fields == ("name",)
        assert "UTF-8" in found[bad.pk].reason
        assert isinstance(found[bad.pk].cause, OperationalError)
        for row in rows:
            if row is not bad:
                assert_reads_as_django_does(found[row.pk], row.pk)
        # Halving 16 rows down to one is 2 * log2(16) + 1 statements, then
        # one a column to name the field.
        columns = len(OxSchedule._meta.concrete_fields)
        assert len(selects(queries, SCHEDULES)) <= 2 * 4 + 1 + columns

    def test_inside_a_transaction_the_transaction_still_works_after(self):
        rows = [a_row(f"row-{i}") for i in range(3)]
        corrupt(rows[1].pk)
        if connection.vendor == "sqlite":
            set_column(rows[2].pk, "name", NOT_UTF_8)

        with transaction.atomic():
            with CaptureQueriesContext(connection) as queries:
                found = read_schedules([r.pk for r in rows], using=ALIAS)
            # A statement after the reads, in the same transaction.
            assert OxSchedule.objects.filter(pk=rows[0].pk).update(cron="") == 1

        assert not savepoints(queries)
        assert_reads_as_django_does(found[rows[0].pk], rows[0].pk)
        assert isinstance(found[rows[1].pk], UnreadableRow)
        if connection.vendor == "sqlite":
            assert found[rows[2].pk].fields == ("name",)

    def test_a_read_the_driver_fails_is_narrowed_in_the_same_transaction(self):
        """
        psycopg loads a timestamp in this process, and one it cannot load
        raises a DataError there, with the statement answered and the
        transaction as it was. The session says so, and the batch is read
        again in halves. Made to happen by sending a typed read in the
        first batch's place: the reads here cast every timestamp to text,
        and none of them fails this way by itself.
        """
        only_on("postgresql", "psycopg raises a DataError of its own")
        rows = [a_row(f"row-{i}") for i in range(4)]
        sent = []

        def a_typed_read_first(execute, sql, params, many, context):
            sent.append(sql)
            if len(sent) == 1:
                return execute(
                    "SELECT '10000-01-01 00:00:00+00'::timestamptz", None, many, context
                )
            return execute(sql, params, many, context)

        with transaction.atomic():
            with connection.execute_wrapper(a_typed_read_first):
                found = read_schedules([r.pk for r in rows], using=ALIAS)
            assert OxSchedule.objects.filter(pk=rows[0].pk).update(cron="") == 1

        # The batch, then its two halves.
        assert len(sent) == 3
        assert not [sql for sql in sent if "SAVEPOINT" in sql.upper()]
        for row in rows:
            assert_reads_as_django_does(found[row.pk], row.pk)

    def test_a_locking_read_locks_where_the_database_can(self):
        row = a_row("row")

        with transaction.atomic(), CaptureQueriesContext(connection) as queries:
            found = read_schedules([row.pk], using=ALIAS, lock=True)

        assert_reads_as_django_does(found[row.pk], row.pk)
        (statement,) = selects(queries, SCHEDULES)
        assert ("FOR UPDATE" in statement) == connection.features.has_select_for_update


@pytest.mark.django_db
class TestReadingSomeOfTheFields:
    """
    A caller that names the fields it needs is handed those, and its read
    takes no other column: what another column holds neither stops the
    read nor is reported by it.
    """

    FIELDS = ("task_key", "enabled", "every_seconds")
    EVERY_FIELD = frozenset(f.attname for f in OxSchedule._meta.concrete_fields)

    def test_takes_the_key_and_the_named_columns_alone(self):
        row = a_row("row")

        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([row.pk], using=ALIAS, fields=self.FIELDS)[row.pk]

        (statement,) = selects(queries, SCHEDULES)
        assert columns_read(statement) == {"id", *self.FIELDS}
        assert isinstance(found, OxSchedule)
        expected = OxSchedule.objects.get(pk=row.pk)
        for name in ("id", *self.FIELDS):
            assert same(getattr(found, name), getattr(expected, name)), name
        assert found.get_deferred_fields() == self.EVERY_FIELD - {"id", *self.FIELDS}
        assert found._state.db == ALIAS
        assert not found._state.adding

    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "django"), UNREADABLE_ROW_VALUES
    )
    def test_a_value_in_a_column_it_did_not_name_is_not_its_concern(
        self, vendor, column, literal, django
    ):
        only_on(vendor, "kept by that engine only")
        row = a_row("row")
        set_column(row.pk, column, literal)
        assert isinstance(read_schedules([row.pk], using=ALIAS)[row.pk], UnreadableRow)

        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([row.pk], using=ALIAS, fields=["trigger", "cron"])

        assert isinstance(found[row.pk], OxSchedule)
        assert found[row.pk].trigger == "interval"
        # One statement, for text SQLite's driver cannot decode as for any
        # other value: the column is not taken, so nothing fails on it.
        (statement,) = selects(queries, SCHEDULES)
        assert columns_read(statement) == {"id", "trigger", "cron"}

    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "django"), UNREADABLE_ROW_VALUES
    )
    def test_a_value_in_a_column_it_named_is_reported(
        self, vendor, column, literal, django
    ):
        only_on(vendor, "kept by that engine only")
        row = a_row("row")
        set_column(row.pk, column, literal)

        with CaptureQueriesContext(connection) as queries:
            found = read_schedules([row.pk], using=ALIAS, fields=["trigger", column])

        unreadable = found[row.pk]
        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.fields == (column,)
        # It speaks of the fields that were read, and of no other.
        assert set(unreadable.values) == {"id", "trigger"}
        instance = unreadable.deferred_instance()
        assert instance.get_deferred_fields() == self.EVERY_FIELD - {"id", "trigger"}
        statements = selects(queries, SCHEDULES)
        if literal == NOT_UTF_8:
            # The driver could not hand the row over, so it is read again
            # a column at a time: the three the read was for.
            assert len(statements) == 1 + 3
        else:
            assert len(statements) == 1
        for statement in statements:
            assert columns_read(statement) <= {"id", "trigger", column}

    def test_a_name_that_is_not_a_field_is_refused(self):
        row = a_row("row")

        with pytest.raises(FieldDoesNotExist):
            read_schedules([row.pk], using=ALIAS, fields=["task_kee"])


@pytest.mark.django_db
class TestTheLockedRead:
    def test_locks_and_reads_a_row_that_does_not_read(self):
        bad = a_row("bad")
        column = corrupt(bad.pk)
        found = orm_read(bad.pk)
        assert isinstance(found, Exception) or misread(found.end_time)

        with transaction.atomic():
            locked = lock_schedule(bad.pk, using=ALIAS)

        assert isinstance(locked, UnreadableRow)
        assert locked.fields == (column,)
        assert locked.values["name"] == "bad"

    def test_a_row_that_reads_is_read_as_django_reads_it(self):
        row = a_row("row")

        with transaction.atomic():
            locked = lock_schedule(row.pk, using=ALIAS)

        assert_reads_as_django_does(locked, row.pk)

    def test_the_read_says_whether_the_row_is_there(self):
        """
        Nothing else does: on SQLite the lock is an UPDATE, whose row count
        nothing here reads, and a statement sent only to ask would be one
        more on every dispatch of a stored schedule. A row that is gone is
        left out of the read, which is the answer.
        """
        there = a_row("there")
        gone = a_row("gone")
        gone_pk = gone.pk
        gone.delete()

        with transaction.atomic(), CaptureQueriesContext(connection) as queries:
            assert lock_schedule(gone_pk, using=ALIAS) is None
            found = lock_schedule(there.pk, using=ALIAS)

        assert_reads_as_django_does(found, there.pk)
        assert not savepoints(queries)
        statements = [
            q["sql"].lstrip().upper()
            for q in queries.captured_queries
            if SCHEDULES.upper() in q["sql"].upper()
        ]
        if connection.features.has_select_for_update:
            # One statement a row: the read is what locks.
            reads = statements
            assert len(reads) == 2
            assert all("FOR UPDATE" in read for read in reads)
        else:
            # The writer first, then the row, and nothing between them.
            assert len(statements) == 4
            assert all(write.startswith("UPDATE") for write in statements[::2])
            reads = statements[1::2]
        for read in reads:
            assert read.startswith("SELECT") and "OX_STORED_START_TIME" in read
            assert not read.startswith("SELECT 1")

    def test_reads_only_the_fields_it_is_asked_for(self):
        """
        A caller that names the fields it needs takes no other column, so
        a value that does not read in a column it did not name neither
        stops it nor is reported to it.
        """
        row = a_row("row")
        beside = corrupt(row.pk)
        assert isinstance(read_schedules([row.pk], using=ALIAS)[row.pk], UnreadableRow)

        with transaction.atomic(), CaptureQueriesContext(connection) as queries:
            locked = lock_schedule(row.pk, using=ALIAS, fields=["task_key", "enabled"])

        assert isinstance(locked, OxSchedule)
        assert (locked.pk, locked.task_key, locked.enabled) == (row.pk, "report", True)
        assert beside in locked.get_deferred_fields()
        (read,) = selects(queries, SCHEDULES)
        assert columns_read(read) == {"id", "task_key", "enabled"}
        assert ("FOR UPDATE" in read) == connection.features.has_select_for_update


@pytest.mark.django_db(transaction=True)
def test_the_locked_read_needs_a_transaction():
    row = a_row("row")

    with pytest.raises(TransactionManagementError, match="there is no transaction"):
        lock_schedule(row.pk, using=ALIAS)


@pytest.mark.django_db(transaction=True)
def test_the_locked_read_holds_off_another_writer():
    """
    A second connection that writes the row while the lock is held waits
    for it, here up to a short timeout that it then exceeds.
    """
    bad = a_row("bad")
    corrupt(bad.pk)
    outcome = {}
    locked = threading.Event()
    finished = threading.Event()

    def writer():
        try:
            # Not past a test that failed before it took the lock.
            if not locked.wait(15) or "abandoned" in outcome:
                return
            with connection.cursor() as cursor:
                if connection.vendor == "postgresql":
                    cursor.execute("SET lock_timeout = '300ms'")
                elif connection.vendor == "mysql":
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                else:
                    cursor.execute("PRAGMA busy_timeout = 300")
            OxSchedule.objects.filter(pk=bad.pk).update(cron="")
            outcome["wrote"] = True
        except OperationalError as exc:
            outcome["refused"] = exc
        finally:
            connection.close()
            finished.set()

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        with transaction.atomic():
            assert isinstance(lock_schedule(bad.pk, using=ALIAS), UnreadableRow)
            locked.set()
            assert finished.wait(15)
    finally:
        if not locked.is_set():
            outcome["abandoned"] = True
            locked.set()
        thread.join(15)

    assert "refused" in outcome, outcome
    # Released at the end of the transaction, the row takes the write.
    assert OxSchedule.objects.filter(pk=bad.pk).update(cron="") == 1


def seen_waiting_for_the_lock(limit=30.0):
    """
    Whether a session came to wait for a lock within `limit` seconds.

    PostgreSQL and MySQL say when one does. They are asked from a connection
    of its own: a session inside a transaction is shown the activity it saw
    first for as long as the transaction lasts. And no more often than every
    tenth of a second, because InnoDB renews its picture of the transactions
    only once it has gone that long unread.

    SQLite does not say. Its writer waits on a busy timeout instead, so the
    call is given a moment to reach it, and the answer is None.
    """
    if connection.vendor == "sqlite":
        time.sleep(0.3)
        return None
    query = {
        "postgresql": (
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND datname = current_database()"
        ),
        "mysql": (
            "SELECT count(*) FROM information_schema.innodb_trx "
            "WHERE trx_state = 'LOCK WAIT'"
        ),
    }[connection.vendor]

    def waiting():
        seen = []

        def count(other):
            with other.cursor() as cursor:
                cursor.execute(query)
                seen.append(cursor.fetchone()[0])

        from_another_connection(count)
        return seen[0] > 0

    return wait_for(waiting, timeout=limit, interval=0.1)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("fields", [None, ["task_key", "enabled"]], ids=["row", "some"])
def test_the_locked_read_waits_for_a_writer_and_reads_what_it_committed(fields):
    """
    The read that takes the lock is a current read. A transaction that
    has already looked at the row waits for the writer holding it, and is
    then handed the row as that writer committed it: on MySQL a plain read
    would go on answering from what the transaction saw first. The writer
    here changes the row only once this read is seen waiting, so what comes
    back was written after the read was sent.

    SQLite refuses a transaction that reads and then writes behind another
    writer instead of making it wait, so there nothing is read first, and
    the wait itself cannot be seen.
    """
    row = a_row("row")
    holding = threading.Event()
    entering = threading.Event()
    out = {}

    def writer():
        try:
            with transaction.atomic():
                lock_schedule(row.pk, using=ALIAS)
                holding.set()
                assert entering.wait(30)
                out["waited"] = seen_waiting_for_the_lock()
                OxSchedule.objects.filter(pk=row.pk).update(task_key="written-since")
        except BaseException as exc:  # handed to the test, which fails on it
            out["raised"] = exc
        finally:
            connections[ALIAS].close()

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        assert holding.wait(30)
        with transaction.atomic():
            if connection.features.has_select_for_update:
                first = OxSchedule.objects.values_list("task_key", flat=True)
                assert first.get(pk=row.pk) == "report"
            entering.set()
            locked = lock_schedule(row.pk, using=ALIAS, fields=fields)
    finally:
        entering.set()
        thread.join(60)

    assert not thread.is_alive()
    assert "raised" not in out, out
    assert out["waited"] is not False, "the read was not seen waiting for the lock"
    assert locked.task_key == "written-since"


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("in_transaction", [False, True])
@pytest.mark.parametrize("read", ["rows", "newest", "earliest"])
def test_a_lost_connection_is_raised_not_put_down_to_a_value(read, in_transaction):
    """
    A connection the server ended (SQLite: closed under Django) fails every
    read that follows. That is the database failing, and it is raised for the
    caller's database handling, never answered as a row or a tick that does
    not read.
    """
    row = a_row("row")
    now = now_ish()
    a_tick("k", now)
    calls = {
        "rows": lambda: read_schedules([row.pk], using=ALIAS),
        "newest": lambda: latest_ticks(["k"], now - timedelta(hours=1), using=ALIAS),
        "earliest": lambda: earliest_tick("k", using=ALIAS),
    }
    # Django's class for it, or on SQLite the driver's own where Django
    # does not wrap the call: reading the parameter limit, for one.
    failed = (DatabaseError, connection.Database.Error)
    try:
        with pytest.raises(failed) as caught:
            if in_transaction:
                with transaction.atomic():
                    kill(connection)
                    calls[read]()
            else:
                kill(connection)
                calls[read]()
        assert not is_unreadable_value(caught.value, using=ALIAS)
        assert not can_read_on(caught.value, using=ALIAS)
    finally:
        connection.close()


# -- a read the database refuses, and one the driver fails -------------------


def the_reads():
    """
    Rows and ticks, and each read over more than one of them, so that a
    read tried again in halves would have halves to try.
    """
    rows = [a_row(f"row-{i}") for i in range(4)]
    now = now_ish()
    since = now - timedelta(hours=1)
    keys = ["k0", "k1", "k2"]
    for key in keys:
        a_tick(key, now)
    every_tick = OxScheduleTick.objects.order_by("pk")
    return {
        "rows": (SCHEDULES, lambda: read_schedules([r.pk for r in rows], using=ALIAS)),
        "newest": (TICKS, lambda: latest_ticks(keys, since, using=ALIAS)),
        "earliest": (TICKS, lambda: earliest_tick("k0", using=ALIAS)),
        "ticks": (TICKS, lambda: read_ticks(every_tick, using=ALIAS)),
    }


@pytest.mark.django_db
@pytest.mark.parametrize("read", ["rows", "newest", "earliest", "ticks"])
def test_in_a_transaction_a_read_the_database_refuses_is_raised_as_it_was(read):
    """
    A statement the server refuses ends a PostgreSQL transaction for every
    statement after it. A narrower read sent into it raises "current
    transaction is aborted", which is an error about the second read, and
    no savepoint was taken to go back to. So the first error is raised, for
    whoever owns the transaction to roll it back, and nothing is read after
    it. It is never answered as a row or a tick that does not read, on any
    database: where the driver cannot say the transaction still stands,
    it is not assumed to.
    """
    table, call = the_reads()[read]
    refusing = Refusing(table)

    with (
        pytest.raises(REFUSED) as caught,
        transaction.atomic(),
        connection.execute_wrapper(refusing),
    ):
        call()

    refusing.assert_raised_as_it_was(caught)
    # Rolled back by its owner, the connection reads again.
    assert OxSchedule.objects.count() == 4


@pytest.mark.django_db
def test_in_a_transaction_a_refused_lookup_of_an_unreadable_ticks_row_is_raised():
    """
    The read that names an unreadable newest tick's row is a statement of
    its own, sent after a value did not decode. Refused, it is raised like
    any other: the tick's row going unnamed is not worth a statement into a
    transaction that may be over.
    """
    literal = {
        "postgresql": "'infinity'",
        "mysql": "'9999-00-00 00:00:00'",
        "sqlite": "'banana'",
    }[connection.vendor]
    now = now_ish()
    since = now - timedelta(hours=1)
    a_tick("k0", now)
    insert_tick("k0", literal)
    # The grouped read of the keys, then the lookup of the row.
    refusing = Refusing(TICKS, nth=2)

    with (
        pytest.raises(REFUSED) as caught,
        transaction.atomic(),
        connection.execute_wrapper(refusing),
    ):
        assert latest_ticks(["k0"], since, using=ALIAS)["k0"].unreadable
        newest_tick_pk("k0", since, using=ALIAS)

    refusing.assert_raised_as_it_was(caught)


@pytest.mark.django_db
def test_in_a_transaction_a_refused_read_of_one_column_is_raised():
    """
    A row the driver could not hand over is read a column at a time, and
    each of those reads is held to the same rule as the first.
    """
    only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
    bad = a_row("bad")
    set_column(bad.pk, "name", NOT_UTF_8)
    # The row whole, which the driver fails, and then its first column.
    refusing = Refusing(SCHEDULES, nth=2)

    with (
        pytest.raises(REFUSED) as caught,
        transaction.atomic(),
        connection.execute_wrapper(refusing),
    ):
        read_schedules([bad.pk], using=ALIAS)

    refusing.assert_raised_as_it_was(caught)


@pytest.mark.django_db
def test_in_a_transaction_a_refused_read_of_the_tick_rows_keys_is_raised():
    """
    Tick rows the driver could not hand over are read again as their keys
    and then by key, and each of those reads is held to the same rule.
    """
    only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
    a_tick("k", now_ish())
    insert_tick("bad", NOT_UTF_8)
    # The rows whole, which the driver fails, and then their keys.
    refusing = Refusing(TICKS, nth=2)

    with (
        pytest.raises(DataError) as caught,
        transaction.atomic(),
        connection.execute_wrapper(refusing),
    ):
        read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)

    refusing.assert_raised_as_it_was(caught)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("read", ["rows", "newest", "earliest", "ticks"])
def test_with_no_transaction_open_a_read_the_database_refuses_is_raised_as_it_was(read):
    """
    With no transaction open the refused statement was a transaction of its
    own, and nothing is left for the next one to run into. A narrower read
    is not sent all the same. What the database raised says that it
    refused the statement, not that one value in an answer could not be
    read, so there is no row or key for halves to find. Its error is
    raised as it is, for the caller's handling of a database error, and is
    never answered as a row or a tick that does not read.
    """
    table, call = the_reads()[read]
    refusing = Refusing(table)

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with pytest.raises(REFUSED) as caught, connection.execute_wrapper(refusing):
        call()

    refusing.assert_raised_as_it_was(caught)
    # The connection reads again.
    assert OxSchedule.objects.count() == 4


@pytest.mark.django_db(transaction=True)
def test_with_no_transaction_open_a_read_that_fails_on_one_row_is_narrowed_to_it():
    """
    A failure raised in this process, while a value the database sent was
    being turned into Python, is one row's. The batch is read again in
    halves until that row is alone, its columns are read one at a time to
    name the field, and every other row reads.
    """
    rows = [a_row(f"row-{i}") for i in range(4)]
    bad = rows[2]
    failing = Failing(SCHEDULES, "name", bad.pk)

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with connection.execute_wrapper(failing):
        found = read_schedules([r.pk for r in rows], using=ALIAS)

    # The batch, the half the row is in, the row alone, and its one column.
    assert len(failing.failed) == 4
    unreadable = found[bad.pk]
    assert isinstance(unreadable, UnreadableRow)
    assert unreadable.fields == ("name",)
    assert unreadable.reason.startswith("name could not be read: ")
    assert type(unreadable.cause) is Failing.RAISES[connection.vendor]
    for row in rows:
        if row is not bad:
            assert_reads_as_django_does(found[row.pk], row.pk)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("position", [0, 249, 250, 499])
def test_a_row_that_fails_in_a_batch_of_five_hundred_is_narrowed_to_itself(position):
    """
    A full batch is one statement, and one row of it that fails in this
    process fails all five hundred. Read again in halves, the batch comes
    down to that row in nine halvings at most: each sends the half the row
    is in, which fails, and the half it is not in, which reads. Then the
    row's columns, one at a time. Every other row of the batch reads.
    """
    pks = many_rows(500)
    bad = pks[position]
    failing = Failing(SCHEDULES, "name", bad)
    reads = Reads(SCHEDULES)

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with connection.execute_wrapper(reads), connection.execute_wrapper(failing):
        found = read_schedules(pks, using=ALIAS)

    assert reads.parameters[0] == 500
    halvings = math.ceil(math.log2(500))
    columns = len(OxSchedule._meta.concrete_fields)
    # The batch and each half the row was in, and then its one column.
    assert halvings + 1 <= len(failing.failed) <= halvings + 2
    assert len(reads.sent) <= 2 * halvings + 1 + columns
    assert list(found) == pks
    unreadable = found[bad]
    assert isinstance(unreadable, UnreadableRow)
    assert unreadable.fields == ("name",)
    assert unreadable.reason.startswith("name could not be read: ")
    assert type(unreadable.cause) is Failing.RAISES[connection.vendor]
    assert_all_read_as_django_does(found, leaving_out=[bad])


@pytest.mark.django_db(transaction=True)
def test_with_no_transaction_open_a_read_that_fails_on_one_key_is_narrowed_to_it():
    """
    The same for the newest ticks: the slice of keys is read again in
    halves until the key that fails is alone, that key is answered as one
    whose tick does not read, with the tick's row named, and the keys
    beside it are answered as they stand. The earliest tick is read for
    one key, which is its answer.
    """
    now = now_ish()
    keys = ["k0", "k1", "k2"]
    ticks = {key: a_tick(key, now) for key in keys}
    failing = Failing(TICKS, "scheduled_for", "k1")

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with connection.execute_wrapper(failing):
        answers = latest_ticks(keys, now - timedelta(hours=1), using=ALIAS)
        earliest = earliest_tick("k1", using=ALIAS)

    # The slice, the half the key is in, the key alone, and its earliest.
    assert len(failing.failed) == 4
    for answer in (answers["k1"], earliest):
        assert answer.unreadable
        assert answer.reason.startswith("scheduled_for could not be read: ")
        assert type(answer.cause) is Failing.RAISES[connection.vendor]
    assert answers["k1"].pk is None
    assert newest_tick_pk("k1", now - timedelta(hours=1), using=ALIAS) == ticks["k1"].pk
    assert answers["k0"] == TickRead("k0", at=now)
    assert answers["k2"] == TickRead("k2", at=now)


@pytest.mark.django_db(transaction=True)
def test_with_no_transaction_open_a_read_that_fails_on_one_tick_row_is_narrowed_to_it():
    """
    And for tick rows: the selection is read again as its keys, which
    always read, and then by key in halves until the row that fails is
    alone. That row is answered as one whose tick does not read, under the
    schedule key it carries, and every other row as it stands.
    """
    now = now_ish()
    ticks = [a_tick(f"k{i}", now - timedelta(minutes=i)) for i in range(4)]
    bad = ticks[2]
    failing = Failing(TICKS, "scheduled_for", bad.pk)
    selection = OxScheduleTick.objects.filter(pk__in=[t.pk for t in ticks])

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with connection.execute_wrapper(failing):
        rows = read_ticks(selection.order_by("pk"), using=ALIAS)

    # The selection, its rows by key, the half the row is in, the row alone.
    assert len(failing.failed) == 4
    assert [row.pk for row in rows] == [tick.pk for tick in ticks]
    assert rows[2].unreadable
    assert rows[2].key == "k2"
    assert rows[2].reason.startswith("scheduled_for could not be read: ")
    assert type(rows[2].cause) is Failing.RAISES[connection.vendor]
    for index in (0, 1, 3):
        tick = ticks[index]
        assert rows[index] == TickRow(tick.pk, f"k{index}", at=tick.scheduled_for)


@pytest.mark.django_db(transaction=True)
def test_with_no_transaction_open_text_the_driver_cannot_decode_is_narrowed_down():
    """
    The stored value that fails a whole read by itself: text SQLite keeps
    and its driver cannot decode. It is found by halving with no
    transaction open as it is inside one.
    """
    only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
    rows = [a_row(f"row-{i}") for i in range(4)]
    set_column(rows[1].pk, "name", NOT_UTF_8)
    now = now_ish()
    a_tick("k0", now)
    a_tick("k1", now)
    pk = insert_tick("k1", NOT_UTF_8)

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    found = read_schedules([r.pk for r in rows], using=ALIAS)
    answers = latest_ticks(["k0", "k1"], now - timedelta(hours=1), using=ALIAS)

    assert found[rows[1].pk].fields == ("name",)
    assert isinstance(found[rows[1].pk].cause, OperationalError)
    for row in rows:
        if row is not rows[1]:
            assert_reads_as_django_does(found[row.pk], row.pk)
    assert answers["k1"].unreadable
    assert answers["k1"].pk is None
    assert newest_tick_pk("k1", now - timedelta(hours=1), using=ALIAS) == pk
    assert answers["k0"] == TickRead("k0", at=now)


@pytest.mark.django_db(transaction=True)
def test_a_transaction_keeps_what_it_wrote_before_a_read_the_driver_failed():
    """
    The whole of it, committed: the write before the read that failed, the
    rows found by reading again in halves, and the write after them. No
    savepoint is taken anywhere in it.
    """
    only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
    rows = [a_row(f"row-{i}") for i in range(4)]
    set_column(rows[2].pk, "name", NOT_UTF_8)

    with CaptureQueriesContext(connection) as queries, transaction.atomic():
        a_tick("k", now_ish())
        found = read_schedules([r.pk for r in rows], using=ALIAS)
        OxSchedule.objects.filter(pk=rows[0].pk).update(starting_deadline_seconds=90)

    assert not savepoints(queries)
    assert found[rows[2].pk].fields == ("name",)
    assert_reads_as_django_does(found[rows[3].pk], rows[3].pk)
    assert OxScheduleTick.objects.filter(schedule_name="k").count() == 1
    assert OxSchedule.objects.get(pk=rows[0].pk).starting_deadline_seconds == 90


#: The one failure SQLite's driver raises in this process over a statement
#: the database answered, as its message alone.
UNDECODABLE = OperationalError("Could not decode to UTF-8 column 'name' with text 'b'")


def failed_in_the_driver():
    """
    The driver's own failure on a value it was sent, raised for real, or
    None on MySQL, whose driver hands every value over.
    """
    statement = Failing.STATEMENT.get(connection.vendor)
    return None if statement is None else raised_by(statement)


def refused_by_the_database():
    """A DataError the database itself raised, for real, with its code."""
    statement = Refusing.STATEMENT.get(connection.vendor)
    return refused_by_sqlite() if statement is None else raised_by(statement)


@pytest.mark.django_db
class TestWhetherAFailedReadMayBeFollowedByAnother:
    def test_not_after_a_failure_that_is_the_databases(self):
        for exc in (
            OperationalError("server closed the connection unexpectedly"),
            OperationalError("database is locked"),
            KeyError("x"),
            refused_by_the_database(),
            raised_by("SELECT * FROM no_such_table"),
        ):
            assert not can_read_on(exc, using=ALIAS), exc

    def test_in_a_transaction_the_driver_says_where_it_can(self):
        # After the driver's own failure, and after a converter's.
        expected = {
            # The session reports that no statement in it has failed.
            "postgresql": (True, True),
            # Nothing is reported, so only the failure known to be the
            # driver's own.
            "sqlite": (True, False),
            # Nothing is reported, and the driver has no failure of its own.
            "mysql": (False, False),
        }[connection.vendor]

        with transaction.atomic():
            assert OxSchedule.objects.count() == 0
            own = failed_in_the_driver() or DataError("a value that did not load")
            answers = (
                can_read_on(own, using=ALIAS),
                can_read_on(ValueError("x"), using=ALIAS),
            )

        assert answers == expected

    def test_not_once_a_postgresql_transaction_has_failed(self):
        only_on("postgresql", "its session reports a transaction that has failed")
        with pytest.raises(DataError), transaction.atomic():
            assert can_read_on(ValueError("x"), using=ALIAS)
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1/0")
            except DataError as exc:
                # Django has not marked the transaction: only the session
                # knows the server has ended it.
                assert not connection.needs_rollback
                assert not can_read_on(exc, using=ALIAS)
                assert not can_read_on(ValueError("x"), using=ALIAS)
                raise

    def test_not_in_a_transaction_django_has_marked_for_rollback(self):
        if connection.vendor == "mysql":
            pytest.skip("on mysql no failure inside a transaction is read past")
        with transaction.atomic():
            assert OxSchedule.objects.count() == 0
            own = failed_in_the_driver()
            assert can_read_on(own, using=ALIAS)
            transaction.set_rollback(True)
            try:
                assert not can_read_on(own, using=ALIAS)
            finally:
                transaction.set_rollback(False)


@pytest.mark.django_db(transaction=True)
def test_with_no_transaction_open_only_a_failure_on_a_value_is_read_past():
    connection.ensure_connection()
    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    # A converter's failure, on every database, and the driver's own where
    # it has one.
    assert can_read_on(ValueError("x"), using=ALIAS)
    own = failed_in_the_driver()
    if own is not None:
        assert can_read_on(own, using=ALIAS)
    # Nothing the database raised: a DataError no more than a lock or a
    # table that is not there.
    assert not can_read_on(refused_by_the_database(), using=ALIAS)
    assert not can_read_on(OperationalError("database is locked"), using=ALIAS)
    assert not can_read_on(raised_by("SELECT * FROM no_such_table"), using=ALIAS)


# -- the decoders ----------------------------------------------------------


def generated_instants(rng, aware):
    """
    Many dates and times a column can be given, the edges of the years a
    datetime holds and the folds and gaps of the zones among them.
    """
    zones = [
        UTC,
        ZoneInfo("America/Chicago"),
        ZoneInfo("Asia/Kolkata"),
        ZoneInfo("Australia/Lord_Howe"),
        ZoneInfo("Pacific/Kiritimati"),
    ]
    first = datetime(1, 1, 1, tzinfo=UTC)
    last = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    span = int((last - first).total_seconds())
    picked = [
        first,
        last,
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime(2026, 3, 8, 8, 30, tzinfo=UTC),
        datetime(2026, 11, 1, 6, 30, tzinfo=UTC),
        datetime(2026, 11, 1, 7, 30, tzinfo=UTC),
    ]
    for _ in range(300):
        moment = first + timedelta(seconds=rng.randrange(span))
        micro = rng.choice([0, 0, 1, 500000, 120000, 999999, rng.randrange(1000000)])
        picked.append(moment.replace(microsecond=micro))
    for _ in range(60):
        # Around now, where the zones' rules change most.
        moment = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(
            seconds=rng.randrange(366 * 86400)
        )
        picked.append(moment)
    values = []
    for moment in picked:
        zone = rng.choice(zones)
        try:
            local = moment.astimezone(zone)
        except OverflowError:
            local = moment
        if aware:
            values.append(local)
        else:
            values.append(local.replace(tzinfo=None, fold=rng.choice([0, 1])))
    return values


@pytest.mark.django_db
class TestTheDateAndTimeDecoder:
    @pytest.mark.parametrize(
        ("use_tz", "zone"),
        [
            (True, "America/Chicago"),
            (False, "America/Chicago"),
            (False, "Pacific/Kiritimati"),
        ],
    )
    def test_decodes_every_time_the_orm_writes_to_what_the_orm_reads(
        self, settings, use_tz, zone
    ):
        settings.USE_TZ = use_tz
        settings.TIME_ZONE = zone
        rng = random.Random(f"{connection.vendor}-{use_tz}-{zone}")
        values = generated_instants(rng, aware=use_tz)
        OxScheduleTick.objects.bulk_create(
            OxScheduleTick(schedule_name=f"p{i}", scheduled_for=v, created_at=v)
            for i, v in enumerate(values)
        )

        by_orm = dict(OxScheduleTick.objects.values_list("pk", "scheduled_for"))
        by_text = dict(
            OxScheduleTick.objects.annotate(t=as_text("scheduled_for")).values_list(
                "pk", "t"
            )
        )

        assert len(by_orm) == len(values)
        differ = {
            pk: (by_text[pk], decode_datetime(by_text[pk], ALIAS), read)
            for pk, read in by_orm.items()
            if not same(decode_datetime(by_text[pk], ALIAS), read)
        }
        assert not differ

    @pytest.mark.parametrize("use_tz", [True, False])
    def test_a_schedule_row_reads_as_the_orm_reads_it(self, settings, use_tz):
        settings.USE_TZ = use_tz
        settings.TIME_ZONE = "America/Chicago"
        rng = random.Random(f"rows-{connection.vendor}-{use_tz}")
        values = generated_instants(rng, aware=use_tz)[:80]
        rows = [
            a_row(
                f"row-{i}",
                start_time=start,
                end_time=None if i % 3 == 0 else end,
                created_at=start,
                updated_at=end,
                enabled=bool(i % 2),
                every_seconds=None if i % 4 == 0 else 60 + i,
                trigger="cron" if i % 4 == 0 else "interval",
                cron="* * * * *" if i % 4 == 0 else "",
                starting_deadline_seconds=None if i % 5 else i,
            )
            for i, (start, end) in enumerate(zip(values, reversed(values), strict=True))
        ]

        found = read_schedules([r.pk for r in rows], using=ALIAS)

        for row in rows:
            assert_reads_as_django_does(found[row.pk], row.pk)

    def test_with_use_tz_off_a_time_with_a_zone_is_unreadable(self, settings):
        only_on(
            "sqlite", "SQLite keeps an offset in the text, which USE_TZ off cannot read"
        )
        settings.USE_TZ = False
        pk = insert_tick("k", "'9999-01-01 00:00:00+00:00'")
        # Django reads it with its zone, which a naive clock cannot be
        # compared with.
        assert timezone.is_aware(OxScheduleTick.objects.get(pk=pk).scheduled_for)

        answer = latest_ticks(["k"], datetime(2000, 1, 1), using=ALIAS)["k"]

        assert answer.unreadable
        assert answer.pk is None
        assert newest_tick_pk("k", datetime(2000, 1, 1), using=ALIAS) == pk
        assert answer.reason == (
            "scheduled_for holds '9999-01-01 00:00:00+00:00', which has a time "
            "zone, and USE_TZ is off"
        )

    def test_with_use_tz_on_a_time_with_an_offset_reads_as_django_reads_it(
        self, settings
    ):
        only_on("sqlite", "SQLite keeps an offset in the text")
        settings.USE_TZ = True
        pk = insert_tick("k", "'2026-10-05 12:00:00+02:00'")
        django = OxScheduleTick.objects.get(pk=pk).scheduled_for

        answer = latest_ticks(["k"], datetime(2000, 1, 1, tzinfo=UTC), using=ALIAS)["k"]

        assert same(answer.at, django)
        assert answer.at == datetime(2026, 10, 5, 10, 0, tzinfo=UTC)


class TestTheTextFormsOfEachEngine:
    """
    Each engine's text, decoded as its driver would. These need no database
    of that engine: the text is what it sends, and the connection here
    supplies only the zones.
    """

    @pytest.fixture
    def conn(self, settings):
        settings.USE_TZ = True
        from django.db import connections

        return connections[ALIAS]

    def test_postgresql(self, conn):
        read = _stored_read._from_postgresql
        assert read("2026-10-05 12:34:56.5+00", conn) == datetime(
            2026, 10, 5, 12, 34, 56, 500000, tzinfo=UTC
        )
        assert read("2026-10-05 12:34:56.123456+00", conn).microsecond == 123456
        assert read("1799-12-31 18:09:24-05:50:36", conn) == datetime(
            1800, 1, 1, tzinfo=UTC
        )
        for text in ("infinity", "-infinity", "0001-01-01 00:00:00+00 BC", "banana"):
            with pytest.raises(UnreadableValue):
                read(text, conn)
        # Python's own words, which differ between versions.
        with pytest.raises(ValueError):
            read("10000-01-01 00:00:00+00", conn)

    def test_mysql(self, conn):
        read = _stored_read._from_mysql
        assert read("2026-10-05 12:34:56.000000", conn) == datetime(
            2026, 10, 5, 12, 34, 56, tzinfo=UTC
        )
        assert read("9999-12-31 23:59:59.999999", conn).microsecond == 999999
        for text in ("2026-00-10 00:00:00.000000", "0000-00-00 00:00:00.000000"):
            with pytest.raises(ValueError):
                read(text, conn)
        with pytest.raises(UnreadableValue):
            read("2026-10-05", conn)

    def test_sqlite(self, conn):
        read = _stored_read._from_sqlite
        assert read("2026-10-05 12:34:56", conn) == datetime(
            2026, 10, 5, 12, 34, 56, tzinfo=UTC
        )
        for text in ("banana", "10000-01-01 00:00:00", "2.5"):
            with pytest.raises(UnreadableValue):
                read(text, conn)
        with pytest.raises(ValueError):
            read("2026-02-30 00:00:00", conn)


class TestTheNumberDecoders:
    def test_integers(self):
        assert decode_integer(0) == 0
        assert decode_integer(2**62) == 2**62
        assert decode_integer(None, null=True) is None
        for value in ("abc", "60", 60.5, 60.0, True, b"60", None):
            with pytest.raises(UnreadableValue):
                decode_integer(value)
        with pytest.raises(
            UnreadableValue, match="holds 'abc', which is not a whole number"
        ):
            decode_integer("abc")

    def test_booleans(self):
        assert decode_boolean(value=True) is True
        assert decode_boolean(value=False) is False
        assert decode_boolean(1) is True
        assert decode_boolean(0) is False
        assert decode_boolean(None, null=True) is None
        for value in (2, -1, "abc", "1", 1.0, None):
            with pytest.raises(UnreadableValue):
                decode_boolean(value)
        with pytest.raises(
            UnreadableValue, match="holds 2, which is neither true nor false"
        ):
            decode_boolean(2)

    def test_a_value_is_quoted_to_a_length(self):
        with pytest.raises(UnreadableValue) as caught:
            decode_integer("x" * 500)
        assert len(str(caught.value)) < 120

    @pytest.mark.django_db
    def test_null_where_a_value_is_needed(self):
        with pytest.raises(
            UnreadableValue, match="is NULL, and this field needs a value"
        ):
            decode_datetime(None, ALIAS)
        assert decode_datetime(None, ALIAS, null=True) is None


@pytest.mark.django_db
class TestWhatCountsAsAnUnreadableValue:
    def test_a_converters_failure_does(self):
        for exc in (
            ValueError("x"),
            TypeError("x"),
            AttributeError("x"),
            OverflowError("x"),
            UnreadableValue("x"),
        ):
            assert is_unreadable_value(exc, using=ALIAS), exc

    def test_the_drivers_own_failure_on_a_value_does(self):
        exc = failed_in_the_driver()
        if exc is None:
            pytest.skip("MySQL's driver hands every value over; none fails in it")
        assert type(exc) is Failing.RAISES[connection.vendor]
        assert is_unreadable_value(exc, using=ALIAS)

    def test_a_data_error_the_database_raised_does_not(self):
        """
        The class is the same as the driver's own on PostgreSQL. What
        tells them apart is the code the database gives every error it
        raises, and that the driver's own has none.
        """
        exc = refused_by_the_database()
        assert type(exc) is REFUSED
        assert Refusing.SAYS[connection.vendor] in str(exc)
        assert not is_unreadable_value(exc, using=ALIAS)

    def test_each_drivers_failure_counts_on_its_own_database_alone(self):
        without_a_code = DataError("timestamp too large (after year 10K)")
        assert is_unreadable_value(UNDECODABLE, using=ALIAS) == (
            connection.vendor == "sqlite"
        )
        assert is_unreadable_value(without_a_code, using=ALIAS) == (
            connection.vendor == "postgresql"
        )

    def test_the_database_failing_does_not(self):
        from django.db import IntegrityError, InterfaceError, ProgrammingError

        for exc in (
            OperationalError("server closed the connection unexpectedly"),
            OperationalError("database is locked"),
            InterfaceError("connection already closed"),
            ProgrammingError("relation does not exist"),
            IntegrityError("duplicate key"),
            KeyError("x"),
            RuntimeError("x"),
            raised_by("SELECT * FROM no_such_table"),
        ):
            assert not is_unreadable_value(exc, using=ALIAS), exc


def test_stored_value_unreadable_pickles():
    exc = StoredValueUnreadable(
        alias="default",
        model="django_ox.OxSchedule",
        pk=5,
        fields=("end_time",),
        reason="end_time holds 'infinity', which is not a date and time",
    )

    copy = pickle.loads(pickle.dumps(exc))  # noqa: S301 - our own bytes

    assert type(copy) is StoredValueUnreadable
    assert (copy.alias, copy.model, copy.pk, copy.fields, copy.reason) == (
        exc.alias,
        exc.model,
        exc.pk,
        exc.fields,
        exc.reason,
    )
    assert str(copy) == str(exc)


# -- the tick log ------------------------------------------------------------


def now_ish():
    return timezone.now().replace(microsecond=0)


#: (engine, SQL literal, id): a tick each engine keeps that Django's grouped
#: read of the newest tick raises on, and that sorts newest.
UNREADABLE_NEWEST_TICKS = [
    pytest.param("postgresql", "'10000-01-01 00:00:00+00'", id="postgresql-year-10000"),
    pytest.param("postgresql", "'infinity'", id="postgresql-infinity"),
    pytest.param("sqlite", "'banana'", id="sqlite-banana"),
    pytest.param("sqlite", "'9999-02-30 00:00:00'", id="sqlite-does-not-exist"),
    pytest.param("mysql", "'9999-00-00 00:00:00'", id="mysql-zero-month"),
]

#: The same for the oldest tick, which the first-sighting read takes.
UNREADABLE_OLDEST_TICKS = [
    pytest.param("postgresql", "'-infinity'", id="postgresql-minus-infinity"),
    pytest.param("postgresql", "'0001-01-01 00:00:00+00 BC'", id="postgresql-bc"),
    pytest.param("sqlite", "'0000-02-30 00:00:00'", id="sqlite-year-zero"),
    pytest.param("mysql", "'0000-00-00 00:00:00'", id="mysql-zero-date"),
]


def django_earliest(key):
    try:
        with transaction.atomic():
            return (
                OxScheduleTick.objects.filter(schedule_name=key)
                .order_by("scheduled_for")
                .values_list("scheduled_for", flat=True)
                .first()
            )
    except Exception as exc:  # the caller asserts on it
        return exc


def django_newest(key, since=None):
    ticks = OxScheduleTick.objects.filter(schedule_name=key)
    if since is not None:
        ticks = ticks.filter(scheduled_for__gte=since)
    try:
        with transaction.atomic():
            return (
                ticks.values("schedule_name")
                .annotate(latest=Max("scheduled_for"))
                .get()["latest"]
            )
    except Exception as exc:  # the caller asserts on it
        return exc


@pytest.mark.django_db
class TestTheNewestTickPerKey:
    def test_answers_every_key_in_one_grouped_statement(self):
        now = now_ish()
        since = now - timedelta(hours=1)
        for i in range(5):
            for back in range(3):
                a_tick(f"k{i}", now - timedelta(minutes=10 * back + i))
        a_tick("old", now - timedelta(days=1))
        keys = [f"k{i}" for i in range(5)] + ["old", "never"]

        with CaptureQueriesContext(connection) as queries:
            answers = latest_ticks(keys, since, using=ALIAS)

        assert len(selects(queries, TICKS)) == 1
        assert list(answers) == keys
        for i in range(5):
            assert same(answers[f"k{i}"].at, django_newest(f"k{i}", since))
            assert not answers[f"k{i}"].unreadable
        # Nothing at or after `since` is no history within what was asked.
        assert answers["old"].absent
        assert answers["never"].absent
        assert answers["never"] == TickRead("never")

    def test_takes_one_statement_a_slice_of_the_parameter_limit(self):
        now = now_ish()
        since = now - timedelta(hours=1)
        if connection.features.max_query_params is None:
            # No limit: every key in one statement, however many.
            keys = [f"k{i}" for i in range(2025)]
            for key in keys[:25]:
                a_tick(key, now)
            with CaptureQueriesContext(connection) as queries:
                answers = latest_ticks(keys, since, using=ALIAS)
            assert len(selects(queries, TICKS)) == 1
            assert all(answers[key].at == now for key in keys[:25])
            assert all(answers[key].absent for key in keys[25:])
            return
        # SQLite's own limit, lowered on this connection, so that a statement
        # over it is refused rather than merely counted. A Django that
        # declares a limit of its own instead of reading SQLite's is held to
        # that one.
        raw = connection.connection
        before = raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
        raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 10)
        if connection.features.max_query_params != 10:
            raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, before)
        try:
            limit = connection.features.max_query_params
            keys = [f"k{i}" for i in range(2 * limit + 5)]
            OxScheduleTick.objects.bulk_create(
                OxScheduleTick(schedule_name=key, scheduled_for=now, created_at=now)
                for key in keys
            )
            with CaptureQueriesContext(connection) as queries:
                answers = latest_ticks(keys, since, using=ALIAS)
        finally:
            raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, before)
        # A key a parameter, and one for the bound.
        assert len(selects(queries, TICKS)) == math.ceil(len(keys) / (limit - 1)) == 3
        assert all(answers[key].at == now for key in keys)

    def test_without_a_bound_every_tick_counts(self):
        now = now_ish()
        old = now - timedelta(days=4000)
        a_tick("k", old)
        a_tick("k", old - timedelta(days=1))

        with CaptureQueriesContext(connection) as queries:
            answers = latest_ticks(["k", "never"], using=ALIAS)

        assert len(selects(queries, TICKS)) == 1
        assert answers["k"] == TickRead("k", at=old)
        assert same(answers["k"].at, django_newest("k"))
        assert answers["never"].absent

    @pytest.mark.parametrize(("vendor", "literal"), UNREADABLE_OLDEST_TICKS)
    def test_without_a_bound_an_only_tick_that_does_not_read_is_not_no_history(
        self, vendor, literal
    ):
        only_on(vendor, "kept by that engine only")
        pk = insert_tick("k", literal)

        answer = latest_ticks(["k"], using=ALIAS)["k"]

        assert answer.unreadable
        assert answer.pk is None
        assert newest_tick_pk("k", using=ALIAS) == pk

    @pytest.mark.parametrize(("vendor", "literal"), UNREADABLE_NEWEST_TICKS)
    @pytest.mark.parametrize("position", ["first", "middle", "last"])
    def test_an_unreadable_newest_tick_is_its_keys_answer_alone(
        self, vendor, literal, position
    ):
        only_on(vendor, "kept by that engine only")
        now = now_ish()
        since = now - timedelta(hours=1)
        keys = [f"k{i}" for i in range(5)]
        for key in keys:
            a_tick(key, now)
        bad = {"first": "k0", "middle": "k2", "last": "k4"}[position]
        pk = insert_tick(bad, literal)
        assert misread(django_newest(bad, since))

        with CaptureQueriesContext(connection) as queries:
            answers = latest_ticks(keys, since, using=ALIAS)

        assert answers[bad].unreadable
        assert not answers[bad].absent
        assert answers[bad].at is None
        assert answers[bad].pk is None
        assert newest_tick_pk(bad, since, using=ALIAS) == pk
        assert answers[bad].reason.startswith("scheduled_for holds ")
        assert isinstance(answers[bad].cause, UnreadableValue)
        for key in keys:
            if key != bad:
                assert answers[key] == TickRead(key, at=now)
        # The grouped statement alone: the bad tick's row is not looked up.
        assert len(selects(queries, TICKS)) == 1

    @pytest.mark.parametrize(("vendor", "literal"), UNREADABLE_NEWEST_TICKS)
    def test_several_unreadable_newest_ticks_cost_the_one_grouped_statement(
        self, vendor, literal
    ):
        only_on(vendor, "kept by that engine only")
        now = now_ish()
        since = now - timedelta(hours=1)
        keys = [f"k{i}" for i in range(6)]
        for key in keys:
            a_tick(key, now)
        bad = {key: insert_tick(key, literal) for key in ("k1", "k3", "k4")}

        with CaptureQueriesContext(connection) as queries:
            answers = latest_ticks(keys, since, using=ALIAS)

        assert len(selects(queries, TICKS)) == 1
        for key in keys:
            if key in bad:
                assert answers[key].unreadable
                assert answers[key].pk is None
            else:
                assert answers[key] == TickRead(key, at=now)
        # The row is named by asking, for a key at a time.
        for key, pk in bad.items():
            assert newest_tick_pk(key, since, using=ALIAS) == pk

    @pytest.mark.parametrize("position", ["first", "middle", "last"])
    def test_a_tick_the_driver_cannot_hand_over_is_found_by_halving_keys(
        self, position
    ):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        now = now_ish()
        keys = [f"k{i}" for i in range(8)]
        for key in keys:
            a_tick(key, now)
        bad = {"first": "k0", "middle": "k3", "last": "k7"}[position]
        pk = insert_tick(bad, NOT_UTF_8)

        # In this test's transaction, and in no savepoint.
        assert connection.in_atomic_block
        with CaptureQueriesContext(connection) as queries:
            answers = latest_ticks(keys, now - timedelta(hours=1), using=ALIAS)

        assert not savepoints(queries)
        assert answers[bad].unreadable
        assert answers[bad].pk is None
        assert newest_tick_pk(bad, now - timedelta(hours=1), using=ALIAS) == pk
        assert "UTF-8" in answers[bad].reason
        for key in keys:
            if key != bad:
                assert answers[key] == TickRead(key, at=now)

    def test_a_column_the_read_does_not_take_is_not_its_concern(self):
        only_on("postgresql", "PostgreSQL keeps year 10000")
        now = now_ish()
        tick = a_tick("k", now)
        by_sql(
            f"UPDATE {TICKS} SET created_at = '10000-01-01 00:00:00+00' WHERE id = %s",  # noqa: S608
            [tick.pk],
        )

        answers = latest_ticks(["k"], now - timedelta(hours=1), using=ALIAS)

        assert answers["k"] == TickRead("k", at=now)

    def test_inside_a_transaction_the_transaction_still_works_after(self):
        now = now_ish()
        a_tick("k0", now)
        a_tick("k1", now)
        literal = {
            "postgresql": "'infinity'",
            "mysql": "'9999-00-00 00:00:00'",
            "sqlite": NOT_UTF_8,
        }[connection.vendor]
        insert_tick("k1", literal)

        with transaction.atomic():
            with CaptureQueriesContext(connection) as queries:
                answers = latest_ticks(
                    ["k0", "k1"], now - timedelta(hours=1), using=ALIAS
                )
            assert (
                OxScheduleTick.objects.filter(schedule_name="k0").update(task=None) == 1
            )

        assert not savepoints(queries)
        assert answers["k0"].at == now
        assert answers["k1"].unreadable


@pytest.mark.django_db
class TestTheEarliestTick:
    def test_is_the_first_by_time_leaving_out_the_given_rows(self):
        now = now_ish()
        first = a_tick("k", now - timedelta(hours=2))
        second = a_tick("k", now - timedelta(hours=1))
        a_tick("other", now - timedelta(days=1))

        assert earliest_tick("k", using=ALIAS) == TickRead(
            "k", at=first.scheduled_for, pk=first.pk
        )
        assert earliest_tick("k", using=ALIAS, exclude=[first.pk]) == TickRead(
            "k", at=second.scheduled_for, pk=second.pk
        )
        assert earliest_tick("k", using=ALIAS, exclude=[first.pk, second.pk]).absent
        assert earliest_tick("never", using=ALIAS).absent
        found = earliest_tick("k", using=ALIAS).at
        django = (
            OxScheduleTick.objects.filter(schedule_name="k")
            .order_by("scheduled_for")
            .values_list("scheduled_for", flat=True)
            .first()
        )
        assert same(found, django)

    @pytest.mark.parametrize(("vendor", "literal"), UNREADABLE_OLDEST_TICKS)
    def test_an_unreadable_earliest_tick_is_not_no_history(self, vendor, literal):
        only_on(vendor, "kept by that engine only")
        now = now_ish()
        a_tick("k", now)
        pk = insert_tick("k", literal)
        assert misread(django_earliest("k"))

        answer = earliest_tick("k", using=ALIAS)

        assert answer.unreadable
        assert not answer.absent
        assert answer.pk == pk
        assert answer.reason.startswith("scheduled_for holds ")
        # Left out, the next tick is the earliest again.
        assert earliest_tick("k", using=ALIAS, exclude=[pk]).at == now

    def test_a_locking_read_where_the_database_has_one(self):
        now = now_ish()
        tick = a_tick("k", now)

        with transaction.atomic(), CaptureQueriesContext(connection) as queries:
            answer = earliest_tick("k", using=ALIAS, lock=True)

        assert answer == TickRead("k", at=now, pk=tick.pk)
        # One statement in the caller's transaction, and no savepoint.
        assert not savepoints(queries)
        (statement,) = selects(queries, TICKS)
        features = connection.features
        assert ("FOR UPDATE" in statement) == features.has_select_for_update
        assert ("SKIP LOCKED" in statement) == (
            features.has_select_for_update
            and features.has_select_for_update_skip_locked
        )


@pytest.mark.django_db(transaction=True)
def test_the_locking_earliest_read_skips_a_tick_another_transaction_holds():
    """
    The latch protocol's repeat read is a locking read that skips rows
    another transaction holds. Here one transaction holds the earliest tick
    through the same read, and a second one's read passes over it to the
    next.
    """
    if not connection.features.has_select_for_update_skip_locked:
        pytest.skip(f"{connection.vendor} has no SKIP LOCKED")
    now = now_ish()
    first = a_tick("k", now - timedelta(hours=1))
    second = a_tick("k", now)
    held = threading.Event()
    release = threading.Event()
    seen = {}

    def holder():
        try:
            with transaction.atomic():
                seen["holder"] = earliest_tick("k", using=ALIAS, lock=True)
                held.set()
                release.wait(15)
        finally:
            connection.close()

    thread = threading.Thread(target=holder)
    thread.start()
    try:
        assert held.wait(15)
        with transaction.atomic():
            seen["other"] = earliest_tick("k", using=ALIAS, lock=True)
    finally:
        release.set()
        thread.join(15)

    assert seen["holder"].pk == first.pk
    assert seen["other"].pk == second.pk


@pytest.mark.django_db
class TestAScheduleNameThatIsNotText:
    BLOB = "X'626c6f622d6e616d65'"

    def test_bytes_in_the_name_column_are_a_value_that_does_not_read(self):
        only_on("sqlite", "SQLite keeps bytes in a text column")
        row = a_row("row")
        set_column(row.pk, "name", self.BLOB)
        assert OxSchedule.objects.get(pk=row.pk).name == b"blob-name"

        unreadable = read_schedules([row.pk], using=ALIAS)[row.pk]

        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.fields == ("name",)
        assert unreadable.reason == "name holds b'blob-name', which is not text"
        assert isinstance(unreadable.cause, UnreadableValue)

    def test_bytes_that_are_text_in_a_name_are_not_taken_for_text(self):
        only_on("sqlite", "SQLite keeps bytes in a text column")
        row = a_row("row")
        set_column(row.pk, "name", "CAST('plain' AS BLOB)")

        unreadable = read_schedules([row.pk], using=ALIAS)[row.pk]

        assert unreadable.reason == "name holds b'plain', which is not text"

    def test_a_name_that_reads_is_shown_as_it_is(self):
        row = a_row("nightly")

        assert str(row) == "nightly"
        assert str(OxSchedule.objects.get(pk=row.pk)) == "nightly"
        assert str(OxSchedule(name="unsaved")) == "unsaved"

    def test_a_name_of_bytes_is_shown_as_the_primary_key(self):
        only_on("sqlite", "SQLite keeps bytes in a text column")
        row = a_row("row")
        set_column(row.pk, "name", self.BLOB)
        found = OxSchedule.objects.get(pk=row.pk)

        assert str(found) == f"pk {row.pk}"
        assert f"pk {row.pk}" in repr(found)
        # Shown, not assigned: the field still holds what the row holds.
        assert found.name == b"blob-name"

    def test_a_name_the_driver_cannot_decode_is_shown_as_the_primary_key(self):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        row = a_row("row")
        set_column(row.pk, "name", NOT_UTF_8)
        instance = read_schedules([row.pk], using=ALIAS)[row.pk].deferred_instance()
        assert "name" in instance.get_deferred_fields()

        assert str(instance) == f"pk {row.pk}"
        assert f"pk {row.pk}" in repr(instance)
        # Still deferred, so the next read of it raises as it did.
        assert "name" in instance.get_deferred_fields()
        with pytest.raises(OperationalError):
            instance.name  # noqa: B018

    def test_an_error_the_database_raised_is_not_hidden_by_the_display(
        self, monkeypatch
    ):
        row = a_row("row")
        instance = OxSchedule.objects.only("id").get(pk=row.pk)
        assert "name" in instance.get_deferred_fields()

        def locked(*args, **kwargs):
            raise OperationalError("database is locked")

        monkeypatch.setattr(OxSchedule, "refresh_from_db", locked)

        with pytest.raises(OperationalError, match="database is locked"):
            str(instance)


#: The values a project can have around the API that psycopg2 and mysqlclient
#: read as something (the largest or smallest datetime, None) where psycopg 3
#: and PyMySQL raise: (engine, column, SQL literal, how the reason quotes it).
LEGACY_ROW_VALUES = [
    pytest.param("postgresql", "end_time", "'infinity'", "'infinity'", id="pg-end"),
    pytest.param(
        "postgresql", "start_time", "'-infinity'", "'-infinity'", id="pg-start"
    ),
    pytest.param(
        "mysql", "end_time", "'0000-00-00 00:00:00'", "'0000-00-00 ", id="zero"
    ),
    pytest.param(
        "mysql", "start_time", "'2026-00-10 00:00:00'", "'2026-00-10", id="zero-month"
    ),
    pytest.param("mysql", "start_time", "NULL", "NULL", id="null-start"),
]

LEGACY_TICK_VALUES = [
    pytest.param("postgresql", "'infinity'", id="pg-infinity"),
    pytest.param("postgresql", "'-infinity'", id="pg-minus-infinity"),
    pytest.param("mysql", "'0000-00-00 00:00:00'", id="zero"),
    pytest.param("mysql", "'9999-00-00 00:00:00'", id="9999-zero-month"),
]


@pytest.mark.django_db
class TestTheLegacyValuesAreUnreadableOnEveryDriver:
    """
    One answer on every driver: a stored value that psycopg2 or mysqlclient
    would hand over as a datetime or None is unreadable here as it is with
    psycopg 3 and PyMySQL, and the healthy rows beside it read.
    """

    @pytest.mark.parametrize(
        ("vendor", "column", "literal", "shown"), LEGACY_ROW_VALUES
    )
    def test_a_schedule_row(self, vendor, column, literal, shown):
        only_on(vendor, "kept by that engine only")
        before, bad, after = a_row("before"), a_row("bad"), a_row("after")
        set_column(bad.pk, column, literal)

        found = read_schedules([before.pk, bad.pk, after.pk], using=ALIAS)

        assert_reads_as_django_does(found[before.pk], before.pk)
        assert_reads_as_django_does(found[after.pk], after.pk)
        unreadable = found[bad.pk]
        assert isinstance(unreadable, UnreadableRow)
        assert unreadable.fields == (column,)
        assert unreadable.reason.startswith(f"{column} ")
        assert shown in unreadable.reason or literal == "NULL"
        assert isinstance(unreadable.cause, UnreadableValue)

    @pytest.mark.parametrize(("vendor", "literal"), LEGACY_TICK_VALUES)
    def test_a_tick_row(self, vendor, literal):
        only_on(vendor, "kept by that engine only")
        now = now_ish()
        a_tick("healthy", now)
        insert_tick("legacy", literal)

        newest = latest_ticks(["healthy", "legacy"], using=ALIAS)
        oldest = earliest_tick("legacy", using=ALIAS)

        assert newest["healthy"] == TickRead("healthy", at=now)
        assert newest["legacy"].unreadable
        assert oldest.unreadable
        assert oldest.reason.startswith("scheduled_for holds ")
        assert earliest_tick("healthy", using=ALIAS).at == now


@pytest.mark.django_db
class TestReadingTickRows:
    def test_reads_each_row_in_the_selections_order_in_one_statement(self):
        now = now_ish()
        made = [a_tick(f"k{i % 2}", now - timedelta(minutes=i)) for i in range(5)]
        selection = OxScheduleTick.objects.order_by("scheduled_for")

        with CaptureQueriesContext(connection) as queries:
            rows = read_ticks(selection, using=ALIAS)

        assert len(selects(queries, TICKS)) == 1
        assert [row.pk for row in rows] == [tick.pk for tick in reversed(made)]
        for row in rows:
            django = OxScheduleTick.objects.get(pk=row.pk)
            assert row.key == django.schedule_name
            assert same(row.at, django.scheduled_for)
            assert not row.unreadable
        assert read_ticks(selection, using=ALIAS, limit=2) == rows[:2]
        assert read_ticks(selection[3:], using=ALIAS, limit=10) == rows[3:]

    @pytest.mark.parametrize(
        ("vendor", "literal"), UNREADABLE_NEWEST_TICKS + UNREADABLE_OLDEST_TICKS
    )
    def test_an_unreadable_tick_is_its_rows_answer_alone(self, vendor, literal):
        only_on(vendor, "kept by that engine only")
        now = now_ish()
        before = a_tick("k", now - timedelta(hours=1))
        bad = insert_tick("k", literal)
        after = a_tick("other", now)
        assert misread(django_earliest("k")) or misread(django_newest("k"))

        rows = read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)

        assert [row.pk for row in rows] == [before.pk, bad, after.pk]
        assert rows[0] == TickRow(before.pk, "k", at=before.scheduled_for)
        assert rows[2] == TickRow(after.pk, "other", at=now)
        assert rows[1].unreadable
        assert rows[1].key == "k"
        assert rows[1].at is None
        assert rows[1].reason.startswith("scheduled_for holds ")
        assert isinstance(rows[1].cause, UnreadableValue)

    @pytest.mark.parametrize("position", ["first", "middle", "last"])
    def test_a_tick_the_driver_cannot_hand_over_is_found_by_halving(self, position):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        now = now_ish()
        index = {"first": 0, "middle": 3, "last": 7}[position]
        pks = []
        for i in range(8):
            if i == index:
                pks.append(insert_tick("bad", NOT_UTF_8))
            else:
                pks.append(a_tick(f"k{i}", now - timedelta(minutes=i)).pk)

        # In this test's transaction, and in no savepoint.
        assert connection.in_atomic_block
        with CaptureQueriesContext(connection) as queries:
            rows = read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)

        assert not savepoints(queries)
        assert [row.pk for row in rows] == pks
        for i, row in enumerate(rows):
            if i == index:
                assert row.unreadable
                assert row.key == "bad"
                assert "UTF-8" in row.reason
                assert row.reason.startswith("scheduled_for could not be read")
            else:
                assert row == TickRow(pks[i], f"k{i}", at=now - timedelta(minutes=i))

    def test_a_key_the_driver_cannot_hand_over_is_its_rows_answer_alone(self):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        now = now_ish()
        before = a_tick("k", now - timedelta(hours=1))
        by_sql(
            f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
            "VALUES (CAST(X'6B80FF' AS TEXT), '2026-10-05 00:00:00', NULL, "
            "'2026-10-05 00:00:00')"
        )
        after = a_tick("other", now)
        (bad,) = set(OxScheduleTick.objects.values_list("pk", flat=True)) - {
            before.pk,
            after.pk,
        }

        rows = read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)

        assert [row.pk for row in rows] == [before.pk, bad, after.pk]
        assert rows[0] == TickRow(before.pk, "k", at=before.scheduled_for)
        assert rows[2] == TickRow(after.pk, "other", at=now)
        assert rows[1].unreadable
        assert rows[1].key == ""
        assert rows[1].at is None
        assert rows[1].reason.startswith("schedule_name could not be read")
        assert "UTF-8" in rows[1].reason

    @pytest.mark.parametrize(
        "tick",
        ["'2026-10-05 00:00:00'", "'banana'", NOT_UTF_8],
        ids=["tick-reads", "tick-does-not", "tick-not-utf-8"],
    )
    def test_a_key_that_is_bytes_is_its_rows_answer_alone(self, tick):
        # The driver hands bytes over as they are, so nothing raises: the
        # row is answered as one whose key does not read, whatever its tick.
        only_on("sqlite", "SQLite keeps bytes where a schedule key should be")
        now = now_ish()
        before = a_tick("k", now - timedelta(hours=1))
        by_sql(
            f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
            f"VALUES (X'6E696768746C79', {tick}, NULL, '2026-10-05 00:00:00')"
        )
        after = a_tick("other", now)
        (bad,) = set(OxScheduleTick.objects.values_list("pk", flat=True)) - {
            before.pk,
            after.pk,
        }

        rows = read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)

        assert [row.pk for row in rows] == [before.pk, bad, after.pk]
        assert rows[0] == TickRow(before.pk, "k", at=before.scheduled_for)
        assert rows[2] == TickRow(after.pk, "other", at=now)
        assert rows[1] == TickRow(
            bad, "", reason="schedule_name holds b'nightly', which is not text"
        )

    def test_a_column_the_read_does_not_take_is_not_its_concern(self):
        only_on("postgresql", "PostgreSQL keeps year 10000")
        now = now_ish()
        tick = a_tick("k", now)
        by_sql(
            f"UPDATE {TICKS} SET created_at = '10000-01-01 00:00:00+00' WHERE id = %s",  # noqa: S608
            [tick.pk],
        )

        assert read_ticks(OxScheduleTick.objects.all(), using=ALIAS) == [
            TickRow(tick.pk, "k", at=now)
        ]

    def test_inside_a_transaction_the_transaction_still_works_after(self):
        now = now_ish()
        a_tick("k0", now)
        literal = {
            "postgresql": "'infinity'",
            "mysql": "'9999-00-00 00:00:00'",
            "sqlite": NOT_UTF_8,
        }[connection.vendor]
        insert_tick("k1", literal)

        with transaction.atomic():
            with CaptureQueriesContext(connection) as queries:
                rows = read_ticks(OxScheduleTick.objects.order_by("pk"), using=ALIAS)
            assert (
                OxScheduleTick.objects.filter(schedule_name="k0").update(task=None) == 1
            )

        assert not savepoints(queries)
        assert [row.unreadable for row in rows] == [False, True]
