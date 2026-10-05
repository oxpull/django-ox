"""
Reading stored schedules and their tick log when a value in them may not
read. Not public API.

A row written through django_ox.stored holds values Django reads back. A
row written around it, by SQL, a fixture or a data migration, holds
whatever its columns accept, and Django's read of such a value either
raises in the middle of a result, taking every row of it along, or on
SQLite reads something else without a word: a start that is not a date
reads as None, an `enabled` of 2 reads as False. A worker that dispatched
from either was wrong, and an admin page or an API call that read one could
not go on.

So the values whose reading can fail or mislead are fetched as the database
holds them and decoded here, one value at a time. A date and time is
fetched as text, cast in the query on PostgreSQL and MySQL (SQLite holds
text already), and decoded by one rule on every driver, under USE_TZ and
the connection's time zone: the rule of psycopg 3 and PyMySQL with Django's
converters. What they read, this reads, and what they raise on or turn into
None is unreadable. The drivers Django also runs on read some of those
values as something: psycopg2 loads PostgreSQL's 'infinity' and '-infinity'
as the largest and smallest datetime, and mysqlclient loads a MySQL zero
date as None. They are unreadable here on those drivers too, so a stored
value means the same under any of them. An integer or a boolean is fetched
past Django's converters and has to be one, and a text column has to hold
text. A value that does not decode is reported by field, never raised from
the middle of a result and never read as something else.

One value that reads is reported with them: a count of boundary writes
that is the most its column can hold. Every write that moves a schedule's
boundary adds one to the count in the database, and at the column's maximum
PostgreSQL and MySQL refuse that write each time it is tried, while SQLite
keeps the sum as a float, which no longer reads. A row holding it is
answered as one that does not read before any such write is tried
(`_refuse_a_full_count`).

A read that fails as a whole, on a value the driver cannot hand over at all
(text SQLite cannot decode as UTF-8), is tried again in halves until the
row or the key that fails is alone. Only a failure known to be raised in
this process, while a value was being turned into Python, is narrowed down
that way (`is_unreadable_value` names them), and only while the connection
is known to take another statement (`can_read_on`). No read takes a
savepoint: a failure of that kind comes once the database has answered and
leaves a transaction as it was, so the reads that narrow it down run in the
same one.

An error the database itself raised is raised as it is, with nothing read
after it, inside a transaction and with none open. It does not say that
one value was at fault, so no row or key is named for it, and inside a
PostgreSQL transaction no statement is taken after it until the
transaction is rolled back, which is for whoever owns it.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, tzinfo
from datetime import timezone as fixed_offset
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.db import DatabaseError, DataError, OperationalError, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import (
    BooleanField,
    CharField,
    DateTimeField,
    F,
    Field,
    Func,
    IntegerField,
    Max,
    QuerySet,
    TextField,
)
from django.db.models.functions import Cast
from django.db.transaction import TransactionManagementError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from .exceptions import StoredValueUnreadable
from .models import OxSchedule, OxScheduleTick

#: The most stored schedules one statement reads by key: enough that every
#: row of a large table is read in a few statements, ten thousand rows in
#: twenty, and a bound on the keys any one statement names. A connection
#: that takes fewer parameters than this is read in batches of what it
#: takes (`read_schedules`). Not a setting, and nothing outside this module
#: depends on the number.
_BATCH_SIZE = 500

#: How SQLite's driver reports text it cannot decode as UTF-8. It raises an
#: OperationalError, the class of a database that failed, for one value in
#: one row, so it is told apart by its message.
_SQLITE_UNDECODABLE = "Could not decode to UTF-8 column"

#: The two of libpq's transaction states a session can be read in: no
#: transaction open, and one open in which no statement has failed. psycopg
#: and psycopg2 both report the state as `info.transaction_status`.
_PQTRANS_IDLE = 0
_PQTRANS_INTRANS = 2

#: The most characters of a stored value a reason quotes.
_SHOWN_LIMIT = 60

#: The most characters of an error's message a reason quotes.
_ERROR_LIMIT = 200

# What is wrong with a value, all of it in this block. Each follows the
# field's name: "end_time holds '10000-01-01 00:00:00+00', which is not a
# date and time". `value` is the value as stored, quoted and cut to length.

#: NULL in a column that needs a value.
_NULL = "is NULL, and this field needs a value"
#: Text a date and time cannot be read from, or a value that is not text.
_NOT_A_DATETIME = "holds {value}, which is not a date and time"
#: A date and time outside what Python's datetime holds, or one that does
#: not exist. `error` is what datetime said.
_DATETIME_OUT_OF_RANGE = (
    "holds {value}, which is not a date and time Python can hold ({error})"
)
#: A date and time with a zone, read while USE_TZ is off.
_ZONE_WITH_USE_TZ_OFF = "holds {value}, which has a time zone, and USE_TZ is off"
#: A date and time without a zone, read while USE_TZ is on.
_NO_ZONE_WITH_USE_TZ_ON = "holds {value}, which has no time zone, and USE_TZ is on"
#: Not an integer.
_NOT_A_WHOLE_NUMBER = "holds {value}, which is not a whole number"
#: Not one of the two values a boolean column holds.
_NOT_A_BOOLEAN = "holds {value}, which is neither true nor false"
#: A value that is not text in a column of text: bytes, which SQLite keeps in
#: any column.
_NOT_TEXT = "holds {value}, which is not text"
#: A count of boundary writes that is the most its column holds.
_COUNT_AT_ITS_MAXIMUM = (
    "holds {value}, the most its column can hold, so the count cannot be raised"
)
#: The driver could not hand the value over. `error` is what it raised.
_NOT_FETCHED = "could not be read: {error}"
#: A row that did not read, when no one field could be named.
_UNKNOWN_FIELD = "a value in it could not be read: {error}"


class UnreadableValue(ValueError):
    """A stored value that does not decode. The message says why, safe to print."""


def is_unreadable_value(exc: BaseException, *, using: str) -> bool:
    """
    Whether a read on `using` failed in this process, on a value it was
    turning into Python, rather than in the database.

    The failures known to be that, and no others:

    - a conversion error from a driver's or Django's converter, which is
      no database error at all: ValueError, TypeError, AttributeError,
      OverflowError;
    - on PostgreSQL, a DataError that carries no SQLSTATE. The server
      gives one to every error it raises, so this is the driver's own, on
      a value it was sent and could not load: psycopg's, for a timestamp
      outside the years a datetime holds;
    - on SQLite, the driver's OperationalError for text it cannot decode
      as UTF-8, told by its message and by carrying no SQLite error code,
      which SQLite gives to every error of its own.

    Anything else is the database's and is raised as it is, whatever its
    class. A DataError the server raised is among them on every database
    (PostgreSQL's carries its SQLSTATE and MySQL's its error number), with
    a lost connection, a lock, a deadlock, a cancelled statement, a missing
    table and a refused permission. None of those says that one value was
    at fault, so none is put down to a row or a key.
    """
    if not isinstance(exc, DatabaseError):
        return isinstance(exc, (ValueError, TypeError, AttributeError, OverflowError))
    if _database_code(exc) is not None:
        return False
    vendor = connections[using].vendor
    if vendor == "postgresql":
        return isinstance(exc, DataError)
    if vendor == "sqlite":
        return isinstance(exc, OperationalError) and _SQLITE_UNDECODABLE in str(exc)
    return False


def _database_code(exc: BaseException) -> object:
    """
    The code the database gave an error it raised, or None for one that
    carries none.

    Django raises its own exception class with the driver's exception as
    the cause. psycopg puts the SQLSTATE on its exception as `sqlstate`
    and psycopg2 as `pgcode`, each None on an error the driver raised
    itself; the SQLite driver puts SQLite's code on as `sqlite_errorcode`,
    and leaves the attribute off an error of its own.
    """
    for error in (exc, exc.__cause__):
        for name in ("sqlstate", "pgcode", "sqlite_errorcode"):
            code = getattr(error, name, None)
            if code is not None:
                return code
    return None


def can_read_on(exc: BaseException, *, using: str) -> bool:
    """
    Whether the read that raised `exc` may be followed by another on the
    same connection: it failed in this process on a value
    (`is_unreadable_value`), and the connection is known to take another
    statement.

    What follows a failed read is a narrower one, to find the row or the
    field it failed on. That is worth sending only after a failure a
    narrower read can pin on one value, which an error the database raised
    is never taken for, with a transaction open or without. And it must
    not be sent into a transaction the failure has ended. No read here
    takes a savepoint to come back to, so that second question is put to
    the connection, without a query.

    Django is asked first: a transaction it has marked for rollback takes
    no more statements. Then the driver, where it says. PostgreSQL's
    driver reports the session's transaction state, and reading goes on
    when no transaction is open or the one that is has not failed. A value
    psycopg could not load fails in this process, once the server has
    answered, and leaves the transaction as it was.

    MySQL's and SQLite's drivers report no such state. With no transaction
    open there is nothing a failed statement could have left unfinished,
    and reading goes on. Inside one it goes on only for the failure known
    to be the driver's own over a statement the database answered:
    SQLite's, refusing text that is not UTF-8. Anything else is raised as
    it stands to whoever owns the transaction. Both would often take
    another statement after it, and nothing here can tell when.
    """
    if not is_unreadable_value(exc, using=using):
        return False
    connection = connections[using]
    if connection.needs_rollback:
        return False
    if connection.vendor == "postgresql":
        state = _postgresql_transaction_state(connection)
        if state is not None:
            return state in (_PQTRANS_IDLE, _PQTRANS_INTRANS)
    if connection.get_autocommit() and not connection.in_atomic_block:
        return True
    # On SQLite the one database error that counts as a value's is the
    # driver's own, on text it could not decode.
    return connection.vendor == "sqlite" and isinstance(exc, DatabaseError)


def _postgresql_transaction_state(connection: BaseDatabaseWrapper) -> int | None:
    """
    The transaction state libpq holds for this session, or None where the
    driver does not give it. From the driver's own record, with no round
    trip: a query could not be sent to ask whether a query can be sent.
    """
    try:
        return int(connection.connection.info.transaction_status)
    except (AttributeError, TypeError, ValueError):
        # No connection, or a driver without the attribute.
        return None


class _Raw(Func):
    """
    A column as the driver hands it over, past Django's converters.

    SQLite's driver converts a value by its column's declared type, so a
    boolean column's 2 or 'abc' arrives as False before anything can look
    at it. An expression has no declared type, and unary plus is the
    expression that leaves every value as it is. Elsewhere it is the column.

    The output field is the bare Field, which no backend converts: Django
    passes an expression with an integer output through int(), which turns
    60.5 into 60 and raises on 'abc'.
    """

    arity = 1
    template = "%(expressions)s"
    output_field = Field()

    def as_sqlite(self, compiler: Any, connection: Any, **extra_context: Any) -> Any:
        return self.as_sql(
            compiler, connection, template="+%(expressions)s", **extra_context
        )


def raw(expression: Any) -> Func:
    """An integer or boolean column, or an expression, as the driver hands it over."""
    return _Raw(expression)


def as_text(expression: Any) -> Cast:
    """
    A date and time column, or an aggregate of one, as its text in the database.

    The cast is in the query, so no converter of the driver's or Django's
    sees the value: PostgreSQL's text form, MySQL's CAST AS CHAR, and on
    SQLite the text it holds. `decode_datetime` reads it.
    """
    return Cast(expression, output_field=TextField())


def _shown(value: object) -> str:
    """A stored value as a reason quotes it: its repr, escaped, cut to length."""
    text = repr(value)
    if len(text) > _SHOWN_LIMIT:
        text = text[: _SHOWN_LIMIT - 3] + "..."
    return text


def _error_text(exc: BaseException | None) -> str:
    """An exception as a reason quotes it, escaped and cut to length."""
    text = "unknown error" if exc is None else f"{type(exc).__name__}: {exc}"
    if not text.isprintable():
        text = ascii(text)[1:-1]
    if len(text) > _ERROR_LIMIT:
        text = text[: _ERROR_LIMIT - 3] + "..."
    return text


# -- decoding ---------------------------------------------------------------

#: PostgreSQL's text form of a timestamptz under the ISO DateStyle, which is
#: the only one psycopg reads: the pattern psycopg matches, so a value reads
#: here exactly when it reads there. 'infinity', '-infinity' and a date
#: before year 1 (which ends in ' BC') do not match.
_POSTGRESQL_TIMESTAMPTZ = re.compile(
    r"""
    (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)
    (?: T | [^a-z0-9] )
    (\d+) [^a-z0-9] (\d+) [^a-z0-9] (\d+)
    (?: \.(\d+) )?
    ([-+]) (\d+) (?: : (\d+) )? (?: : (\d+) )?
    """,
    re.ASCII | re.IGNORECASE | re.VERBOSE,
)

#: MySQL's text form of a DATETIME: the pattern PyMySQL converts, whole.
#: A zero date matches and then fails as a date.
_MYSQL_DATETIME = re.compile(
    r"(\d{1,4})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d{1,6}))?",
    re.ASCII,
)


def _session_zone(connection: BaseDatabaseWrapper) -> tzinfo:
    """
    The zone a PostgreSQL session renders a timestamptz in: the one Django
    sets it to, and UTC where that zone is unknown here, as psycopg falls
    back.
    """
    try:
        return ZoneInfo(connection.timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        return UTC


def _from_postgresql(text: str, connection: BaseDatabaseWrapper) -> datetime:
    """
    A timestamptz's text as psycopg loads it and Django's loader relabels it.

    psycopg reads the instant from the text and moves it to the session's
    zone; past the end of the years a datetime holds, it keeps the text's
    own wall clock and offset instead. Django then replaces the zone with
    the connection's: UTC, or none with USE_TZ off.
    """
    match = _POSTGRESQL_TIMESTAMPTZ.fullmatch(text)
    if match is None:
        raise UnreadableValue(_NOT_A_DATETIME.format(value=_shown(text)))
    year, month, day, hour, minute, second, fraction, sign, oh, om, os = match.groups()
    micro = int(fraction) if fraction else 0
    if fraction and len(fraction) < 6:
        micro *= 10 ** (6 - len(fraction))
    offset_seconds = 3600 * int(oh) + 60 * int(om or 0) + int(os or 0)
    offset = timedelta(seconds=offset_seconds if sign == "+" else -offset_seconds)
    instant = datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second),
        micro,
        tzinfo=UTC,
    )
    try:
        loaded = (instant - offset).astimezone(_session_zone(connection))
    except OverflowError:
        loaded = instant.replace(tzinfo=fixed_offset(offset))
    return loaded.replace(tzinfo=connection.timezone)


def _from_mysql(text: str, connection: BaseDatabaseWrapper) -> datetime:
    """A DATETIME's text as PyMySQL converts it and Django's converter zones it."""
    match = _MYSQL_DATETIME.fullmatch(text)
    if match is None:
        raise UnreadableValue(_NOT_A_DATETIME.format(value=_shown(text)))
    year, month, day, hour, minute, second, fraction = match.groups()
    loaded = datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second),
        int((fraction or "").ljust(6, "0")),
    )
    if settings.USE_TZ:
        loaded = timezone.make_aware(loaded, connection.timezone)
    return loaded


def _from_sqlite(text: str, connection: BaseDatabaseWrapper) -> datetime:
    """
    SQLite's text as Django's converter reads it, except that text it
    cannot read is unreadable rather than None.
    """
    loaded = parse_datetime(text)
    if loaded is None:
        raise UnreadableValue(_NOT_A_DATETIME.format(value=_shown(text)))
    if settings.USE_TZ and timezone.is_naive(loaded):
        loaded = timezone.make_aware(loaded, connection.timezone)
    return loaded


def decode_datetime(
    value: object, using: str, *, null: bool = False
) -> datetime | None:
    """
    Decode a date and time fetched as text through `as_text`.

    Use one decoding rule on every driver, following psycopg 3 and PyMySQL
    with Django's converters, under USE_TZ and the connection's time zone.
    For accepted values, preserve the instant and the time zone, or its
    absence, that this rule produces.

    PostgreSQL 'infinity' and '-infinity' are unreadable even when psycopg2
    would return the largest or smallest datetime. MySQL zero dates are
    unreadable even when mysqlclient would return None.

    Raise UnreadableValue for those values, dates outside years 1 through
    9999, nonexistent dates, and text that is not a date and time.
    Also reject NULL unless `null`, and values whose time zone does not
    match USE_TZ, including SQLite values with a zone when USE_TZ is off.
    """
    if value is None:
        if null:
            return None
        raise UnreadableValue(_NULL)
    if not isinstance(value, str):
        raise UnreadableValue(_NOT_A_DATETIME.format(value=_shown(value)))
    connection = connections[using]
    try:
        if connection.vendor == "postgresql":
            loaded = _from_postgresql(value, connection)
        elif connection.vendor == "mysql":
            loaded = _from_mysql(value, connection)
        else:
            loaded = _from_sqlite(value, connection)
    except UnreadableValue:
        raise
    except (ValueError, OverflowError) as exc:
        raise UnreadableValue(
            _DATETIME_OUT_OF_RANGE.format(value=_shown(value), error=exc)
        ) from exc
    if timezone.is_aware(loaded) and not settings.USE_TZ:
        raise UnreadableValue(_ZONE_WITH_USE_TZ_OFF.format(value=_shown(value)))
    if timezone.is_naive(loaded) and settings.USE_TZ:
        raise UnreadableValue(_NO_ZONE_WITH_USE_TZ_ON.format(value=_shown(value)))
    return loaded


def decode_integer(value: object, *, null: bool = False) -> int | None:
    """
    An integer column's value as fetched through `raw`. SQLite keeps text
    and fractions in one, which raise UnreadableValue, as NULL does unless
    `null`.
    """
    if value is None:
        if null:
            return None
        raise UnreadableValue(_NULL)
    if type(value) is int:
        return value
    raise UnreadableValue(_NOT_A_WHOLE_NUMBER.format(value=_shown(value)))


def decode_boolean(value: object, *, null: bool = False) -> bool | None:
    """
    A boolean column's value as fetched through `raw`: PostgreSQL's boolean,
    or the 0 or 1 SQLite and MySQL keep. Anything else they keep (2, 'abc')
    raises UnreadableValue, as NULL does unless `null`.
    """
    if value is None:
        if null:
            return None
        raise UnreadableValue(_NULL)
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise UnreadableValue(_NOT_A_BOOLEAN.format(value=_shown(value)))


def _projection(model_field: Field[Any, Any]) -> Any:
    """How one column is fetched: an expression, or None for the column itself."""
    if model_field.primary_key:
        return None
    if isinstance(model_field, DateTimeField):
        return as_text(model_field.attname)
    if isinstance(model_field, (BooleanField, IntegerField)):
        return raw(model_field.attname)
    return None


def _decode(model_field: Field[Any, Any], value: object, using: str) -> Any:
    """One fetched column's value, decoded as `_projection` fetched it."""
    if isinstance(model_field, DateTimeField):
        return decode_datetime(value, using, null=model_field.null)
    if isinstance(model_field, BooleanField):
        return decode_boolean(value, null=model_field.null)
    if isinstance(model_field, IntegerField):
        number = decode_integer(value, null=model_field.null)
        if model_field.name == _BOUNDARY_COUNT:
            _refuse_a_full_count(number, model_field, using)
        return number
    # SQLite keeps whatever it was given in a text column, and hands bytes
    # back as they are. Django would put them in the field as the value,
    # where every use of it as text fails.
    if (
        isinstance(model_field, (CharField, TextField))
        and value is not None
        and not isinstance(value, str)
    ):
        raise UnreadableValue(_NOT_TEXT.format(value=_shown(value)))
    return value


#: The field that counts the writes to a schedule's activation boundary.
_BOUNDARY_COUNT = "boundary_generation"


def _refuse_a_full_count(
    count: int | None, model_field: Field[Any, Any], using: str
) -> None:
    """
    Raise UnreadableValue when boundary_generation cannot increase within
    its column's range on `using`.

    The maximum is a readable integer. The guarded reader rejects it because
    every write that moves the activation boundary increments the count.
    These writes include a worker's boundary heal, a retime, and a pause
    whose boundary is stale.

    PostgreSQL and MySQL refuse an increment at the maximum. SQLite stores
    the sum as a float, which fails the integer read. SQL can set the count
    to the maximum or one below it. From one below, the next boundary write
    succeeds and reaches the maximum. Subsequent guarded reads reject the
    row, so a worker skips it without trying to move its boundary.
    Pause and delete by primary key remain available; other changes are
    refused. Neither the API nor the admin can lower the count.

    This check rejects a decoded value. It does not attribute a database
    error to a particular row.

    Use `ops.integer_field_range` for the column type on this connection:
    2147483647 on PostgreSQL, 4294967295 on MySQL, and 9223372036854775807 on
    SQLite. Reject values at or above that maximum, including values in a
    column altered to hold more than the model's declared type.
    """
    if count is None:
        return
    _, highest = connections[using].ops.integer_field_range(
        model_field.get_internal_type()
    )
    if count >= highest:
        raise UnreadableValue(_COUNT_AT_ITS_MAXIMUM.format(value=_shown(count)))


def _fetched(
    query: QuerySet[Any], fields: Sequence[Field[Any, Any]]
) -> list[tuple[Any, ...]]:
    """`fields` of every row of `query`, each as `_projection` fetches it."""
    names = []
    expressions = {}
    for model_field in fields:
        expression = _projection(model_field)
        if expression is None:
            names.append(model_field.attname)
        else:
            name = f"ox_stored_{model_field.attname}"
            expressions[name] = expression
            names.append(name)
    return list(query.annotate(**expressions).values_list(*names))


# -- schedule rows -----------------------------------------------------------


@dataclass(frozen=True)
class UnreadableRow:
    """
    A stored schedule that exists and holds a value that does not read.

    `unreadable` maps each field that did not read to why, safe to print,
    and is empty when the read could not say which field it was. `values`
    holds every field that did read, by attribute name, the primary key
    always among them. `cause` is the first exception the read met.

    A read asked for some of the fields (`read_schedules`, `fields`) says
    nothing about the others: they are in neither mapping.
    """

    alias: str
    pk: int
    unreadable: Mapping[str, str]
    values: Mapping[str, Any]
    cause: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def fields(self) -> tuple[str, ...]:
        """The names of the fields that did not read, in the model's order."""
        return tuple(self.unreadable)

    @property
    def reason(self) -> str:
        """Why the row did not read, in one line safe to print."""
        if self.unreadable:
            return "; ".join(f"{name} {why}" for name, why in self.unreadable.items())
        return _UNKNOWN_FIELD.format(error=_error_text(self.cause))

    def exception(self) -> StoredValueUnreadable:
        """The StoredValueUnreadable that says so, with `cause` as its cause."""
        exc = StoredValueUnreadable(
            alias=self.alias,
            model=OxSchedule._meta.label,
            pk=self.pk,
            fields=self.fields,
            reason=self.reason,
        )
        exc.__cause__ = self.cause
        return exc

    def deferred_instance(self) -> OxSchedule:
        """
        An OxSchedule holding the fields that read, with every other field
        deferred. Reading a deferred field asks Django's own read for it,
        which raises as it always did; saving with `update_fields` that
        name only fields set here writes nothing else.
        """
        names = [
            model_field.attname
            for model_field in OxSchedule._meta.concrete_fields
            if model_field.attname in self.values
        ]
        return OxSchedule.from_db(self.alias, names, [self.values[n] for n in names])


def read_schedules(
    pks: Iterable[int] | QuerySet[OxSchedule],
    *,
    using: str,
    lock: bool = False,
    fields: Collection[str] | None = None,
) -> dict[int, OxSchedule | UnreadableRow]:
    """
    The stored schedules with these primary keys, read from `using`.

    Each that exists maps to an OxSchedule equal to the one Django's own
    read gives, or to an UnreadableRow naming the fields that did not read.
    A key with no row is left out, so a caller can tell a row that is gone
    from one that does not read. In the order the keys were given.

    A queryset is read for its primary keys alone, which always read, and
    then as keys: `_BATCH_SIZE` rows a statement, or as many as the
    connection takes parameters for where that is fewer. A batch that fails
    as a whole, on a value the driver cannot hand over, is read again in
    halves until the row that fails is alone, and that row's columns are
    then read one at a time to name the ones that do not read. Those reads
    are made only after a failure known to be a value's, and only while the
    connection is known to take another statement (`can_read_on`): an error
    the database raised is raised as it is.

    `lock` takes each row's lock as it reads it, where the database has row
    locks, for a read that has to be current. On SQLite it does nothing:
    there `lock_schedule` is what serialises.

    `fields` names the fields to read, for a caller that needs those alone
    and must not depend on the rest of the row. The primary key is always
    read. A column that is not named is not selected, so a value in it
    that does not read cannot fail the read and is not reported: on the
    OxSchedule handed back the field is deferred, and an UnreadableRow
    speaks only of the fields that were read. Without `fields`, every one.
    """
    if isinstance(pks, QuerySet):
        pks = pks.values_list("pk", flat=True)
    wanted = list(dict.fromkeys(pks))
    selected = _selected(fields)
    # One parameter a key, and the statement has no other.
    limit = connections[using].features.max_query_params
    step = max(min(_BATCH_SIZE, limit) if limit else _BATCH_SIZE, 1)
    found: dict[int, OxSchedule | UnreadableRow] = {}
    for start in range(0, len(wanted), step):
        batch = wanted[start : start + step]
        found.update(_read_rows(batch, using, lock=lock, fields=selected))
    return {pk: found[pk] for pk in wanted if pk in found}


def _selected(fields: Collection[str] | None) -> list[Field[Any, Any]]:
    """
    The model's fields a read fetches, in the model's order: all of them,
    or the primary key and those named. A name that is not a field raises
    FieldDoesNotExist, as the model's own lookup of it does.
    """
    concrete = OxSchedule._meta.concrete_fields
    if fields is None:
        return list(concrete)
    named = {OxSchedule._meta.get_field(name) for name in fields}
    return [
        model_field
        for model_field in concrete
        if model_field.primary_key or model_field in named
    ]


def _read_rows(
    pks: list[int], using: str, *, lock: bool, fields: list[Field[Any, Any]]
) -> dict[int, OxSchedule | UnreadableRow]:
    """One batch, or its halves when it does not read as a whole."""
    query = OxSchedule.objects.using(using).filter(pk__in=pks)
    if lock and connections[using].features.has_select_for_update:
        query = query.select_for_update()
    try:
        fetched = _fetched(query, fields)
    except Exception as exc:
        if not can_read_on(exc, using=using):
            raise
        if len(pks) == 1:
            alone = _diagnose(pks[0], using, exc, fields)
            return {} if alone is None else {pks[0]: alone}
        middle = len(pks) // 2
        return {
            **_read_rows(pks[:middle], using, lock=lock, fields=fields),
            **_read_rows(pks[middle:], using, lock=lock, fields=fields),
        }
    rows = (_hydrate(dict(zip(fields, row, strict=True)), using) for row in fetched)
    return {row.pk: row for row in rows}


def _hydrate(
    fetched: Mapping[Field[Any, Any], Any], using: str
) -> OxSchedule | UnreadableRow:
    """A fetched row as an OxSchedule, or as what about it did not read."""
    values: dict[str, Any] = {}
    unreadable: dict[str, str] = {}
    cause: BaseException | None = None
    for model_field, value in fetched.items():
        try:
            values[model_field.attname] = _decode(model_field, value, using)
        except UnreadableValue as exc:
            unreadable[model_field.name] = str(exc)
            cause = cause or exc
    pk = values[OxSchedule._meta.pk.attname]
    if unreadable:
        return UnreadableRow(using, pk, unreadable, values, cause)
    names = [model_field.attname for model_field in fetched]
    return OxSchedule.from_db(using, names, [values[name] for name in names])


def _diagnose(
    pk: int, using: str, cause: BaseException, fields: list[Field[Any, Any]]
) -> UnreadableRow | None:
    """
    A row that failed to read as a whole, one column at a time, or None if
    it is gone. Paid only for a row that failed: a statement for each
    column the read was for.
    """
    values: dict[str, Any] = {}
    unreadable: dict[str, str] = {}
    exists = False
    for model_field in fields:
        query = OxSchedule.objects.using(using).filter(pk=pk)
        try:
            fetched = _fetched(query, [model_field])
        except Exception as exc:
            if not can_read_on(exc, using=using):
                raise
            exists = True
            unreadable[model_field.name] = _NOT_FETCHED.format(error=_error_text(exc))
            continue
        if not fetched:
            continue
        exists = True
        try:
            values[model_field.attname] = _decode(model_field, fetched[0][0], using)
        except UnreadableValue as exc:
            unreadable[model_field.name] = str(exc)
    if not exists:
        return None
    return UnreadableRow(using, pk, unreadable, values, cause)


def lock_schedule(
    pk: int, *, using: str, fields: Collection[str] | None = None
) -> OxSchedule | UnreadableRow | None:
    """
    Take a stored schedule's row lock and read the row under it: the row,
    what of it did not read, or None when it is gone.

    On PostgreSQL and MySQL the read is what locks, one statement: a
    locking read waits for a concurrent writer and then reads the row as
    that writer committed it, not as this transaction first saw it, and
    holds the lock to the end of the transaction. It is read as
    `read_schedules` reads, every value whose reading can fail fetched as
    the database holds it and decoded once the statement has answered. So
    the statement that takes the lock is not one a stored value fails, and
    a row holding a value that does not read is locked like any other, to
    be paused, repaired or deleted.

    SQLite has no row locks, and Django drops the locking clause there
    without raising. What serialises SQLite is being the writer, and a
    transaction that reads first starts as a reader, which is refused
    rather than made to wait when it later writes. So there a no-op UPDATE
    comes first, which makes the transaction the writer before it reads.

    `fields` names the fields to read, as for `read_schedules`: a caller
    that names what it needs selects no other column, and a value in one
    it did not name neither stops it nor is reported to it.

    Whether the row is there is what the read says, which leaves out a row
    that is gone. No statement asks, and nothing depends on the UPDATE's
    row count: a no-op UPDATE counts the rows it matched on SQLite, and on
    MySQL the rows it changed unless Django's FOUND_ROWS flag is set, which
    a project setting its own client_flag loses with no error.

    Raises TransactionManagementError outside a transaction, where the lock
    would be released as soon as it was taken.
    """
    connection = connections[using]
    if connection.get_autocommit():
        raise TransactionManagementError(
            "lock_schedule() holds a lock until the end of the transaction, "
            "and there is no transaction."
        )
    if not connection.features.has_select_for_update:
        OxSchedule.objects.using(using).filter(pk=pk).update(name=F("name"))
    return read_schedules([pk], using=using, lock=True, fields=fields).get(pk)


# -- the tick log -------------------------------------------------------------


def _fetch_failed(exc: BaseException) -> str:
    """Why a tick did not read, when the driver could not hand it over."""
    return f"scheduled_for {_NOT_FETCHED.format(error=_error_text(exc))}"


@dataclass(frozen=True)
class TickRead:
    """
    What the tick log holds for one schedule key.

    One of three answers. A tick that reads: `at` is set. No tick at all,
    within what was asked: `absent`. A tick that does not read: `reason`
    says why, safe to print, and `pk` is the tick row's key where the read
    that found it also found its key (`earliest_tick`).
    `latest_ticks` does not look for it: `newest_tick_pk` does, for a caller
    that is about to name the row. An unreadable tick is not an absent one,
    and must not be taken for one.
    """

    key: str
    at: datetime | None = None
    pk: int | None = None
    reason: str | None = None
    cause: BaseException | None = field(default=None, compare=False, repr=False)

    @property
    def absent(self) -> bool:
        return self.at is None and self.reason is None

    @property
    def unreadable(self) -> bool:
        return self.reason is not None


def latest_ticks(
    keys: Sequence[str], since: datetime | None = None, *, using: str
) -> dict[str, TickRead]:
    """
    The newest tick for each schedule key, every key answered.

    With `since`, only ticks at or after it count, which is what lets an
    index seek to them; a key whose ticks are all older is `absent`. Without
    it, the newest of all, which a page of a few keys can afford and which
    does not take a key whose only tick is unreadable and very old (a MySQL
    zero date, PostgreSQL's -infinity) for one with no history.

    One grouped statement for as many keys as the connection takes
    parameters for, with the newest tick fetched as text and decoded per
    key, so one key's unreadable tick is that key's answer and the others
    still read. A statement that fails as a whole, on a value the driver
    cannot hand over, is tried again with half the keys, down to one, while
    the connection is known to take another statement (`can_read_on`).

    The answer for a key whose newest tick does not read carries no `pk`.
    Which row it is matters to the message that reports the skip, which is
    written at most once a minute for a key, and the lookup is made then
    (`newest_tick_pk`) rather than in every pass that skips the key.
    """
    wanted = list(dict.fromkeys(keys))
    limit = connections[using].features.max_query_params
    # One parameter a key, and one for `since`.
    step = max(limit - 1, 1) if limit else max(len(wanted), 1)
    found: dict[str, TickRead] = {}
    for start in range(0, len(wanted), step):
        found.update(_latest(wanted[start : start + step], since, using))
    return {key: found[key] for key in wanted}


def _ticks(using: str, since: datetime | None) -> QuerySet[OxScheduleTick]:
    """The tick log on `using`, from `since` on where one is given."""
    ticks = OxScheduleTick.objects.using(using)
    return ticks if since is None else ticks.filter(scheduled_for__gte=since)


def _latest(keys: list[str], since: datetime | None, using: str) -> dict[str, TickRead]:
    """One slice of keys, or its halves when it does not read as a whole."""
    try:
        fetched = {
            row["schedule_name"]: row["latest"]
            for row in _ticks(using, since)
            .filter(schedule_name__in=keys)
            .values("schedule_name")
            .annotate(latest=as_text(Max("scheduled_for")))
        }
    except Exception as exc:
        if not can_read_on(exc, using=using):
            raise
        if len(keys) == 1:
            key = keys[0]
            return {key: TickRead(key, reason=_fetch_failed(exc), cause=exc)}
        middle = len(keys) // 2
        return {
            **_latest(keys[:middle], since, using),
            **_latest(keys[middle:], since, using),
        }
    answers = {}
    for key in keys:
        if key not in fetched:
            answers[key] = TickRead(key)
            continue
        try:
            at = decode_datetime(fetched[key], using)
        except UnreadableValue as exc:
            answers[key] = TickRead(key, reason=f"scheduled_for {exc}", cause=exc)
        else:
            answers[key] = TickRead(key, at=at)
    return answers


def newest_tick_pk(
    key: str, since: datetime | None = None, *, using: str
) -> int | None:
    """
    The primary key of the tick row the log holds newest for `key`, from
    `since` on where one is given, or None when there is none or the read
    that finds it fails on a value (`can_read_on`).

    One statement, for a message that names the row. It does not decide
    anything: a caller that cannot have the key goes on without it. An
    error the database raised is raised as it is.
    """
    try:
        return (
            _ticks(using, since)
            .filter(schedule_name=key)
            .order_by("-scheduled_for")
            .values_list("pk", flat=True)
            .first()
        )
    except Exception as exc:
        if not can_read_on(exc, using=using):
            raise
        return None


def earliest_tick(
    key: str, *, using: str, exclude: Collection[int] = (), lock: bool = False
) -> TickRead:
    """
    The earliest tick recorded for `key`, leaving out the rows in `exclude`.

    The read a first sighting is decided by, with its order and exclusions,
    and its tick fetched as text and decoded. An unreadable earliest tick is
    `unreadable`, never `absent`: taken for no history, it would make the
    schedule anchor again over a tick that already has one.

    `lock` makes it a locking read where the database has one, skipping
    rows another transaction holds where it can: the current read the
    first-sighting latch repeats once it holds the latch. It is one
    statement in the caller's transaction, with no savepoint around it, so
    the locks it takes and the ones the caller already holds stay held.

    A read the database refuses inside that transaction is raised as it is
    (`can_read_on`), for the caller to roll back. It is not answered as a
    tick that does not read.
    """
    query = OxScheduleTick.objects.using(using).filter(schedule_name=key)
    if exclude:
        query = query.exclude(pk__in=list(exclude))
    query = query.order_by("scheduled_for")
    features = connections[using].features
    if lock and features.has_select_for_update:
        query = query.select_for_update(
            skip_locked=features.has_select_for_update_skip_locked
        )
    try:
        fetched = list(
            query.annotate(
                ox_stored_scheduled_for=as_text("scheduled_for")
            ).values_list("pk", "ox_stored_scheduled_for")[:1]
        )
    except Exception as exc:
        if not can_read_on(exc, using=using):
            raise
        return TickRead(key, reason=_fetch_failed(exc), cause=exc)
    if not fetched:
        return TickRead(key)
    pk, text = fetched[0]
    try:
        at = decode_datetime(text, using)
    except UnreadableValue as exc:
        return TickRead(key, pk=pk, reason=f"scheduled_for {exc}", cause=exc)
    return TickRead(key, at=at, pk=pk)
