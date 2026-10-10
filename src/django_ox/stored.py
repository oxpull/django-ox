"""
Reading and writing schedules that live in the database.

The write path is here rather than on the model because Django's
``save()`` does not call ``full_clean()``. A ``clean()`` method alone
would validate everything the admin submits and nothing that
``objects.create()`` writes.

So validation lives in one place and is called from two: the model's
``clean()``, which the admin runs for free, and the functions below,
which are the supported programmatic write path. A caller who bypasses
both and writes the row directly gets an unvalidated row, and the
dispatch path is written to expect one.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextvars import ContextVar
from datetime import datetime, timedelta
from functools import partial
from typing import Any, cast

from django.conf import settings
from django.core.exceptions import (
    NON_FIELD_ERRORS,
    ImproperlyConfigured,
    PermissionDenied,
    ValidationError,
)
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import DatabaseError, DataError, connections, router, transaction
from django.db.models import F, Field, IntegerField
from django.utils import timezone

from . import registry
from ._stored_read import (
    UnreadableRow,
    is_unreadable_value,
    lock_schedule,
    read_schedules,
)
from .compat import normalize_json
from .cron import CronExpression
from .models import OxSchedule, OxScheduleChange, validate_against, validation_alias
from .schedules import MAX_INTERVAL, STORED_KEY_PREFIX, lock_contention

logger = logging.getLogger("django_ox")

#: The marker value has never been read. Distinct from None, which is what
#: an install with no schedule changes yet legitimately reads.
_UNREAD = object()

#: The marker was read and holds a value that could not be converted.
_UNREADABLE = object()

TIMING_FIELDS = frozenset({"trigger", "cron", "every_seconds", "phase_seconds"})

#: What a caller may hand `update_schedule`. Everything else on the row is
#: this module's to write. The activation boundary and the count of writes
#: to it are one mechanism: a worker that found a row stale records both
#: with its sighting, and a boundary written without the count moving is a
#: boundary the worker cannot tell has been superseded, so it heals on top
#: of it and discards the ticks in between. The two timestamps are the
#: same kind of bookkeeping.
#:
#: Naming what is accepted rather than what is refused means a column
#: added later is refused until it is listed, and a misspelled keyword is
#: reported instead of being set on the instance and silently not saved.
WRITABLE_FIELDS = frozenset(
    {
        "name",
        "task_key",
        "trigger",
        "cron",
        "every_seconds",
        "phase_seconds",
        "arguments",
        "enabled",
        "end_time",
        "starting_deadline_seconds",
    }
)

#: The same, plus the boundary, for `create_schedule`. A row that does not
#: exist yet has no pending heal to fence and no count to reset, and the
#: schedules page documents choosing the boundary a schedule starts from.
CREATABLE_FIELDS = frozenset(WRITABLE_FIELDS | {"start_time"})

#: What the clean of a new row is not asked about: the columns this module
#: fills in itself, which a caller cannot have got wrong.
_NOT_CLEANED = ("boundary_for", "boundary_generation", "created_at", "updated_at")

#: The columns the rules read that may be left blank. Django's field
#: cleaning passes over an empty value in such a column without looking at
#: it, so "", [], () and {} reach the rules as the caller gave them, and of
#: the values it calls empty only None is one the column can hold.
_MAY_BE_BLANK = ("every_seconds", "starting_deadline_seconds", "end_time")

#: The most seconds a phase or a starting deadline may be: what a timedelta
#: holds. Dispatch makes a timedelta of each, and one second past this,
#: that raises. The row was skipped at every read then, so a schedule that
#: had validated and been stored never fired, and nothing refused it. Only
#: SQLite's integer column is wide enough to take such a number;
#: PostgreSQL's and MySQL's refuse it for their own range.
_LONGEST_SECONDS = timedelta.max // timedelta(seconds=1)

#: The most seconds an interval may be: the longest whose ticks dispatch can
#: derive, `schedules.MAX_INTERVAL`, which is far less than a timedelta
#: holds. One second past it, with a phase the clock had not reached, the
#: trigger raised in the dispatch pass before the pass came to any one
#: schedule's handling. That ended the pass and the worker with it, for
#: every worker reading the row and again after each restart, so the tasks
#: queued beside it never ran. Only SQLite's integer column takes a number
#: this size.
_LONGEST_INTERVAL_SECONDS = MAX_INTERVAL // timedelta(seconds=1)

#: Whether `validate_schedule` holds a row's task_key to the registry.
#: False only while `_export_preflight` runs: the importer prints the
#: registry entries and the rows that name them together, so when it
#: validates a row the key is not registered yet, and that is the expected
#: state rather than a finding. A context variable for the reason
#: `validate_against` is one: the frame in between is Django's.
_registry_decides: ContextVar[bool] = ContextVar(
    "django_ox_registry_decides", default=True
)

# Messages, all of them in this block. Down to the separator it is
# how `create_schedules` reports a refused batch, where every other message
# is the one `create_schedule` gives for the same row. The three after it
# are `validate_schedule`'s own, and reach every path that validates. The
# last is the reason a worker logs for a row it will not build.

#: One entry of the flat list. `index` is the row's position in the list the
#: caller passed and `name` is the name as that row supplied it, because a
#: name can be missing, or the same in two rows, and the position cannot.
_ROW_FAILURE = "rows[%(index)s] (name %(name)r), %(field)s: %(message)s"
#: The same, for a failure that belongs to no one field.
_ROW_FAILURE_WITHOUT_FIELD = "rows[%(index)s] (name %(name)r): %(message)s"
#: A name an earlier row of the same batch already carries.
_REPEATED_NAME = "rows[%(first)s] has the same name."
#: A row that is not a mapping of field to value.
_NOT_A_MAPPING = "create_schedules() requires a mapping at rows[{index}]; got {kind}."
#: Which row a keyword `_only_writable` refuses came from.
_IN_ROW = " in rows[{index}]"
#: One row `user` may not schedule, in the PermissionDenied a batch raises.
#: `message` is what `check_permission` says about that row.
_ROW_DENIED = "rows[{index}] (name {name!r}): {message}"
#: What stands between two such rows in the one message.
_ROWS_DENIED_SEPARATOR = " "
#: A start or an end given without a time zone, beside one that has a zone:
#: what a naive datetime is while USE_TZ is on. Said of the one without.
_TIME_WITHOUT_A_ZONE = (
    "This time has no time zone. The schedule's other time bound has one, so "
    "they cannot be compared."
)
#: The same the other way about, with USE_TZ off. Said of the one with.
_TIME_WITH_A_ZONE = (
    "This time has a time zone. The schedule's other time bound has none, so "
    "they cannot be compared."
)
#: An interval, a phase or a starting deadline of more seconds than a
#: schedule can run on. `limit` is the field's own: for an interval
#: `_LONGEST_INTERVAL_SECONDS`, for the other two `_LONGEST_SECONDS`.
_TOO_LONG_FOR_A_SCHEDULE = "Enter a value of %(limit)s seconds or less."
#: A stored interval row whose tick cannot be derived, written around
#: `validate_schedule` or stored before it refused such a row. The
#: `reason` of the `schedule_row_skipped` line, after the row's name.
_TICKS_BEFORE_YEAR_ONE = (
    "this row's interval or phase can put a tick before year 1 when "
    "counting ticks from 1970"
)

# Messages for row isolation, all of them in this block.
# The first two are `validate_schedule`'s and reach every path that
# validates. The rest are what a worker logs about a stored row it leaves
# out, about the change marker, and about a boundary it could not move.

#: A start or an end the database at the row's destination would keep
#: outside the years 1 to 9999: PostgreSQL stores it and cannot read it
#: back, SQLite and MySQL refuse it at the write. Said of that bound.
_OUTSIDE_THE_STORABLE_YEARS = (
    "This time must fall within years 1 to 9999 in the database's time zone."
)
#: A start or an end with a time zone while USE_TZ is off, where a
#: schedule's times have none. Said of that bound.
_TIME_ZONE_WHILE_USE_TZ_IS_OFF = "This time must have no time zone when USE_TZ is off."
#: The reason a worker gives for leaving out a stored row whose start or
#: end its clock cannot be compared with. `field` is the column's name.
_BOUND_HAS_A_ZONE = "its {field} has a time zone, and USE_TZ is off"
#: The same, the other way about.
_BOUND_HAS_NO_ZONE = "its {field} has no time zone, and USE_TZ is on"
#: The same, for a bound that is not a time at all.
_BOUND_IS_NOT_A_TIME = "its {field} is not a date and time"
#: The reason for a row a value of which could not be read from the
#: database. `columns` names them, `error` is what the reading raised.
_COLUMNS_UNREADABLE = "its {columns} could not be read: {error}"
#: The same, when no single column could be named.
_A_VALUE_UNREADABLE = "a value in it could not be read: {error}"
#: The reason for a row holding a value that does not read as its field:
#: what the read said of each such field, field first ("end_time holds
#: '10000-01-01 00:00:00', which is not a date and time"). It covers what
#: Django's SQLite converters read as something else without a word: a
#: start or an end that is text and not a date, read as None, which is a
#: schedule with no boundary or no end, and an `enabled` other than true
#: or false, read as False, which is a pause nobody made.
_FIELDS_UNREADABLE = "its {reason}"
#: The reason for a row whose task_key the registry does not hold. `task_key`
#: is the key as stored, quoted and made safe to print. Not "does not
#: exist": during a rolling deploy it may be one only newer code registers.
_TASK_KEY_NOT_REGISTERED = (
    "its task_key {task_key} is not registered as a schedulable task in this deployment"
)
#: The first line about a row left out. The row's name, made safe to print,
#: then its primary key, then the reason.
_SKIPPING = "Skipping stored schedule %s: %s"
#: A later line about the same row, at most once a ROW_REPORT_INTERVAL.
_STILL_SKIPPING = (
    "Still skipping stored schedule %s (%d unreported skips since the last report): %s"
)
#: The change marker holds a value that could not be read. `%s` is the
#: error. Rows are then read in full every SCHEDULE_RECONCILE_INTERVAL, and
#: at once after the next write through this module, which replaces it.
_MARKER_UNREADABLE = (
    "The schedule change marker contains an unreadable value (%s). Stored schedules "
    "are read in full every SCHEDULE_RECONCILE_INTERVAL until a schedule is written "
    "through django_ox.stored."
)
#: The first line about a row whose boundary could not be moved because the
#: database raised an error. `%s` is the row's primary key.
_HEAL_FAILED = "Could not move schedule %s onto its current timing"
#: A later line about the same row, at most once a ROW_REPORT_INTERVAL: the
#: row's primary key, how many times the move has failed since the last
#: line, this time included, and the class of the latest error.
_HEAL_STILL_FAILING = (
    "Still could not move schedule %s onto its current timing: %d more "
    "failure(s) since it was last reported, latest error %s"
)
#: The note `create_schedules` adds to the database's own error when a row
#: of the batch is refused at the write. `index` is the row's position in
#: the rows passed, `name` the name it supplied, escaped and cut to length.
_ROW_REFUSED_AT_THE_WRITE = (
    "The database refused rows[{index}] (name {name}) during the write."
)
#: The same note, when the database raises in the check before the write:
#: validation's own query for names already taken carries the row's name,
#: and PostgreSQL refuses a NUL in it there. Added to whatever the database
#: raises in the check, a lost connection too. `index` and `name` as above.
_ROW_REFUSED_AT_THE_CHECK = (
    "The database raised this error while checking rows[{index}] (name {name}). No "
    "rows in this batch have been written."
)


#: While a stored row keeps being left out, the change marker keeps being
#: unreadable, or a row's boundary keeps failing to move, a worker says so
#: in full the first time and after that at most this often, with the
#: number of times since the last line.
ROW_REPORT_INTERVAL = 60.0

#: The most characters of a row's name or of a reason a line carries.
_PRINTABLE_LIMIT = 200


def _printable(value: Any, limit: int = _PRINTABLE_LIMIT) -> str:
    """
    Text a log line can carry whatever the database held.

    A name or a message is kept as it is when every character prints, and
    otherwise escaped the way `ascii()` escapes it, so a control character,
    an escape sequence or a line break in a stored name cannot reach a
    terminal or start a new line in a log file. Then cut to `limit`.
    """
    text = value if isinstance(value, str) else repr(value)
    if not text.isprintable():
        text = ascii(text)[1:-1]
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text


def _row_label(pk: Any, name: Any) -> str:
    """A stored row as a log line names it: its name when it has one, and its key."""
    if not isinstance(name, str) or not name:
        return f"pk {pk}"
    return f"{_printable(name)} (pk {pk})"


def _outside_the_storable_years(value: datetime, alias: str) -> bool:
    """
    Whether a time falls outside the years 1 to 9999 where the database at
    `alias` keeps it.

    With USE_TZ on, a time is stored in the connection's time zone: UTC on
    PostgreSQL, and on SQLite and MySQL UTC unless the database names a zone
    of its own. A naive time is first read in the default time zone, as the
    field reads it when it saves. PostgreSQL stores an instant past year
    9999 in UTC and then cannot read it back, which stopped every worker;
    SQLite and MySQL raise OverflowError at the write. Asked before the
    write, and with the arithmetic that raises caught rather than run.

    With USE_TZ off a naive time is kept as it is, and PostgreSQL reads it
    back in the zone it was written in, so it always fits. A time with a
    zone is refused before this is asked.
    """
    if not settings.USE_TZ:
        return False
    if timezone.is_naive(value):
        value = timezone.make_aware(value, timezone.get_default_timezone())
    try:
        value.astimezone(connections[alias].timezone)
    except OverflowError:
        return True
    return False


class _RowReport:
    """
    What a schedule source logs about something it keeps failing to use: in
    full the first time, then at most every ROW_REPORT_INTERVAL seconds with
    how many times it went unsaid.

    Keyed on what failed: a stored row by its primary key, the move of a
    row's boundary by the same, or the change marker. A row that is read and
    built again is forgotten, so if it breaks once more it is reported in
    full, and so is a boundary once it is moved or no longer has to be.
    `clock` is time.monotonic outside tests.
    """

    def __init__(self, clock: Any = time.monotonic) -> None:
        self.clock = clock
        #: When the last line was written, the times since then unsaid, and
        #: the times in all since the first line, that one counted.
        self._failing: dict[Any, list[float]] = {}

    def due(self, key: Any) -> tuple[bool, bool, int]:
        """Whether a line is due, whether it is the first, and how many went unsaid."""
        now = self.clock()
        state = self._failing.get(key)
        if state is None:
            self._failing[key] = [now, 0, 1]
            return True, True, 0
        state[2] += 1
        if now - state[0] >= ROW_REPORT_INTERVAL:
            unsaid = int(state[1])
            state[0], state[1] = now, 0
            return True, False, unsaid
        state[1] += 1
        return False, False, 0

    def times(self, key: Any) -> int:
        """How many times `key` has failed since its first line, that one counted."""
        state = self._failing.get(key)
        return 0 if state is None else int(state[2])

    def forget(self, key: Any) -> None:
        self._failing.pop(key, None)

    def keep_rows(self, pks: set[Any]) -> None:
        """Forget every row that is not among `pks`."""
        for key in [k for k in self._failing if k[0] == "row" and k[1] not in pks]:
            del self._failing[key]


def _only_writable(
    fields: Mapping[str, Any], allowed: frozenset[str], func: str, where: str = ""
) -> None:
    """Refuse a keyword this function does not write."""
    refused = sorted(set(fields) - allowed)
    if refused:
        raise TypeError(
            f"{func}() does not take {', '.join(refused)}{where}. It writes "
            f"{', '.join(sorted(allowed))}. The activation boundary, the "
            "count of writes to it and the timestamps are written by "
            "django_ox.stored."
        )


#: What the activation boundary is a boundary *for*: the columns that
#: decide which instants a schedule wants, and whether it wants any.
#:
#: `enabled` is here so that a pause made outside the write API moves the
#: boundary the way one made through `update_schedule` does. A digest still
#: cannot see a round trip: a schedule disabled and re-enabled between two
#: reads ends with the value it started with, and the boundary looks
#: current. What it can see is each state a read lands on. A pause a read
#: finds moves the boundary to that read, and the resume, at the next read,
#: moves it again, so a tick that came due inside the pause is behind the
#: boundary by the time the schedule fires again. A pause and resume with
#: no read between them are invisible, and the docs say so.
#:
#: Timing has the same round-trip property, and there the digest is right
#: to say nothing: a cron changed to something else and back again is a
#: boundary that still matches the timing it was set for.
BOUNDARY_FIELDS = frozenset(TIMING_FIELDS | {"enabled"})


def stored_value(schedule: OxSchedule, field: str) -> Any:
    """
    A field's value as the database will hold it, whatever the caller passed.

    The digest and the retime comparison both have to survive a round trip,
    and an attribute in memory need not match the column yet. `trigger` may
    hold an `OxSchedule.Trigger` member, whose `repr` is the member and
    whose column is the value; `every_seconds` may hold `"60"` from JSON or
    a form, whose column is `60`. Comparing or hashing either as it stands
    reads an unchanged schedule as retimed, and writes a boundary digest
    that nothing recomputed from a row can ever match.

    `to_python` is what the field itself would do on the way in. A str
    subclass is collapsed to `str` as well, because `to_python` returns a
    choices member unchanged and only its `repr` gives it away.
    """
    model_field = cast("Field[Any, Any]", OxSchedule._meta.get_field(field))
    value = model_field.to_python(getattr(schedule, field))
    return str(value) if isinstance(value, str) else value


def boundary_digest(schedule: OxSchedule) -> str:
    """A digest of the columns the boundary was set for."""
    material = "|".join(
        f"{field}={stored_value(schedule, field)!r}"
        for field in sorted(BOUNDARY_FIELDS)
    )
    return hashlib.sha256(material.encode()).hexdigest()[:32]


def _unconvertible_fields(schedule: OxSchedule) -> list[str]:
    """The columns of a row the digest reads that hold what their field refuses."""
    found = []
    for field in sorted(BOUNDARY_FIELDS):
        try:
            stored_value(schedule, field)
        except Exception:
            found.append(field)
    return found


def _number(schedule: OxSchedule, name: str) -> int | None:
    """
    A whole-number column as the rules can compare it, or None if they cannot.

    The rules run after each field's own cleaning, which is what puts the
    number on the instance. But Django runs them whether or not that
    cleaning passed, and where it did not, the instance still holds what the
    caller gave: "soon", or nothing at all in a column that needs something.
    Put to a comparison, such a value raises TypeError out of the whole
    validation, which loses the field's report and, in a batch, the row.

    Read through the field's own `to_python`, as `stored_value` reads, so a
    value the field would take is compared as the number it would store.
    One it would not take is reported elsewhere: by the field, or for a
    value the field passed over as empty, at the end of `validate_schedule`.
    """
    field = cast("Field[Any, Any]", OxSchedule._meta.get_field(name))
    try:
        value = field.to_python(getattr(schedule, name))
    except ValidationError:
        return None
    return value if isinstance(value, int) else None


def _range_errors(schedule: OxSchedule, alias: str) -> dict[str, list[ValidationError]]:
    """
    Hold each number of seconds to what has to hold it.

    The column, first. Django builds an integer field's range validators
    from the default connection, so a field's own cleaning judges
    `every_seconds` by the default database's column whichever alias the
    row is bound for. On one vendor the two agree. Routed to another they
    need not: the cleaning passes on the default's wider range, and the
    value goes on to a write the narrower column refuses. So each column is
    held to the range the database at `alias` gives it, built the way the
    field builds its own: the message is the field's own, and a bound the
    field already holds is not reported twice.

    Then what dispatch does with the number, which holds less than SQLite's
    column does: the tick arithmetic for an interval, and for a phase or a
    deadline the timedelta made of it. Said once as well: not of a value a
    column's range has already kept out, the destination's here or the
    field's own in its cleaning.
    """
    errors: dict[str, list[ValidationError]] = {}
    ops = connections[alias].ops
    for field in OxSchedule._meta.concrete_fields:
        if field.name not in CREATABLE_FIELDS or not isinstance(field, IntegerField):
            continue
        # None is a column left empty, or a value that is not a number.
        value = _number(schedule, field.name)
        if value is None:
            continue
        low, high = ops.integer_field_range(field.get_internal_type())
        for limit, validator in ((low, MinValueValidator), (high, MaxValueValidator)):
            # A backend may leave a side of the range open. A bound the
            # field holds already is one its cleaning has just applied.
            if limit is None or validator(limit) in field.validators:
                continue
            try:
                validator(limit)(value)
            except ValidationError as exc:
                errors.setdefault(field.name, []).extend(exc.error_list)
        longest = (
            _LONGEST_INTERVAL_SECONDS
            if field.name == "every_seconds"
            else _LONGEST_SECONDS
        )
        if value <= longest or field.name in errors:
            continue
        try:
            field.run_validators(value)
        except ValidationError:
            # Past the field's own range, which its cleaning has said.
            continue
        errors[field.name] = [
            ValidationError(
                _TOO_LONG_FOR_A_SCHEDULE,
                code="max_value",
                params={"limit": longest},
            )
        ]
    return errors


def _ticks_overflow(every: int, phase: int) -> bool:
    """
    Whether deriving this interval's tick can reach back past year 1.

    `IntervalTrigger.previous()` steps back from the epoch by as much of
    the phase as the clock has not reached, rounded up to whole intervals.
    At the epoch that is the whole phase and a later clock only shortens
    it, so a row that passes here derives its tick at every clock reading
    from the epoch on.

    `validate_schedule` keeps a phase under its interval, where the step
    is one interval and this is the interval's own limit. A row written
    around it need not, and a phase of many intervals reaches as far back
    as a long interval does.
    """
    return (
        every > _LONGEST_INTERVAL_SECONDS
        or -(-phase // every) * every > _LONGEST_INTERVAL_SECONDS
    )


def validate_schedule(schedule: OxSchedule) -> None:
    """
    Everything a stored schedule must satisfy, whoever is writing it.

    Raises ValidationError with per-field messages, so the admin renders
    them against the fields that caused them.

    What the destination will hold is part of that, and it is asked of the
    destination: an integer column is held to the range the alias this row
    is validated against gives it, where Django's own field validators know
    only the default connection's. Asked here so that creating, updating,
    the admin and the importer's preflight all get the one answer. A number
    of seconds is held to what a timedelta holds as well, since that is
    what dispatch makes of it.

    A value a field's own cleaning has refused is left to the field. It is
    not compared with anything, and it is not reported a second time. A
    value the cleaning never looked at, because the column may be blank and
    the value is empty, is refused here in the words the field would use.
    """
    errors: dict[str, str] = {}

    # Every writer but the importer's preflight, which asks before the key
    # can have been registered.
    if _registry_decides.get():
        try:
            registry.get(schedule.task_key)
        except KeyError:
            known = ", ".join(sorted(registry.kinds())) or "none"
            errors["task_key"] = (
                f"{schedule.task_key!r} is not a schedulable task. "
                f"Registered keys: {known}."
            )

    if schedule.trigger == OxSchedule.Trigger.CRON:
        if not schedule.cron:
            errors["cron"] = "A cron schedule needs a cron expression."
        else:
            try:
                CronExpression(schedule.cron)
            except ValueError as exc:
                errors["cron"] = str(exc)
        if schedule.every_seconds is not None:
            errors["every_seconds"] = "A cron schedule has no interval."
    elif schedule.trigger == OxSchedule.Trigger.INTERVAL:
        every = _number(schedule, "every_seconds")
        phase = _number(schedule, "phase_seconds")
        if schedule.every_seconds is None:
            errors["every_seconds"] = "An interval schedule needs an interval."
        elif every is not None and every < 1:
            errors["every_seconds"] = (
                "An interval below one second cannot be honoured: the dispatch "
                "loop looks about once a second and only the latest due tick "
                "fires, so faster ticks would be coalesced rather than run."
            )
        elif every is not None and phase is not None and phase >= every:
            errors["phase_seconds"] = "The phase must be less than the interval."
        if schedule.cron:
            errors["cron"] = "An interval schedule has no cron expression."
    else:
        errors["trigger"] = f"Unknown trigger {schedule.trigger!r}."

    deadline = _number(schedule, "starting_deadline_seconds")
    if deadline is not None and deadline < 1:
        errors["starting_deadline_seconds"] = (
            "A deadline below one second drops every tick, because a tick is "
            "already later than that by the time a worker sees it."
        )

    # Compare the bounds only when both are datetime instances. Any other
    # value is an absent bound or one that validation rejects elsewhere.
    start, end = schedule.start_time, schedule.end_time
    if (
        isinstance(start, datetime)
        and isinstance(end, datetime)
        and timezone.is_aware(start) != timezone.is_aware(end)
    ):
        # Python will not order a time that has a zone against one that
        # has none, and no field's cleaning objects to either on its own.
        # Reported against the one the USE_TZ setting does not expect,
        # which is the one the caller can put right.
        stray = (
            "end_time" if timezone.is_aware(end) != settings.USE_TZ else "start_time"
        )
        errors[stray] = _TIME_WITHOUT_A_ZONE if settings.USE_TZ else _TIME_WITH_A_ZONE
    # Then each on its own, as the database will hold it. A worker reads
    # every stored row, so a time it cannot read back, or cannot compare
    # with its clock, stopped the source for every schedule rather than
    # this one; refused here, it never reaches the table.
    alias = validation_alias(type(schedule))
    for name, value in (("start_time", start), ("end_time", end)):
        if name in errors or not isinstance(value, datetime):
            continue
        if not settings.USE_TZ and timezone.is_aware(value):
            errors[name] = _TIME_ZONE_WHILE_USE_TZ_IS_OFF
        elif _outside_the_storable_years(value, alias):
            errors[name] = _OUTSIDE_THE_STORABLE_YEARS
    if (
        isinstance(start, datetime)
        and isinstance(end, datetime)
        and not {"start_time", "end_time"} & errors.keys()
        and end <= start
    ):
        errors["end_time"] = "The end time must be after the start time."

    kind = registry.kinds().get(schedule.task_key)
    if not isinstance(schedule.arguments, dict):
        errors["arguments"] = "Arguments must be a mapping."
    elif kind is not None and kind.form is not None:
        form = kind.form(schedule.arguments)
        if not form.is_valid():
            errors["arguments"] = "; ".join(
                f"{field}: {' '.join(messages)}"
                for field, messages in form.errors.items()
            )
        else:
            # What the form cleans to is what reaches the task, and a task
            # is enqueued as JSON. A DateField cleans to a date and an
            # IntegerField to an int; only one of those survives the trip.
            # Checked here so the row is refused, and again at dispatch so
            # a row written any other way is skipped rather than fatal.
            try:
                normalize_json(dict(form.cleaned_data))
            except (TypeError, ValueError) as exc:
                errors["arguments"] = (
                    f"{kind.form.__name__} cleans to a value a task cannot "
                    f"carry: {exc}. Use a field whose cleaned value is JSON."
                )

    # What the numbers can be held in first and in column order, then the
    # rules: where the field's own cleaning would have put a range it held.
    found = _range_errors(schedule, validation_alias(type(schedule)))
    for field, message in errors.items():
        found.setdefault(field, []).append(ValidationError(message))
    # Last, an empty value no column can hold and no field looked at. Not
    # where a rule has refused the field already: one refusal keeps the
    # value out, and the rule's is the one that says what to do instead.
    for name in _MAY_BE_BLANK:
        column = cast("Field[Any, Any]", OxSchedule._meta.get_field(name))
        value = getattr(schedule, name)
        if name in found or value is None:
            continue
        if column.blank and value in column.empty_values:
            found[name] = [
                ValidationError(
                    column.error_messages["invalid"],
                    code="invalid",
                    params={"value": value},
                )
            ]
    if found:
        raise ValidationError(found)


def check_permission(schedule: OxSchedule, user: Any) -> None:
    """
    Enforce a registry entry's own permission, if it declares one.

    Kept here rather than only in the admin because the admin is one write
    path and these functions are the other. A permission enforced in the
    UI alone is a permission the UI enforces.

    Django's per-action admin permission hook takes no object, so this
    cannot ride on it; the shape is borrowed instead, and object-level
    backends get their chance because the schedule is passed through.
    """
    kind = registry.kinds().get(schedule.task_key)
    if kind is None or kind.permission is None:
        return
    if user is None:
        return
    if not user.has_perm(kind.permission, schedule) and not user.has_perm(
        kind.permission
    ):
        raise PermissionDenied(
            f"Scheduling {schedule.task_key!r} needs the "
            f"{kind.permission!r} permission."
        )


def schedule_db_alias() -> str:
    """
    The database the stored schedules are written through.

    Named once and threaded, rather than left to each statement's own
    routing. A transaction opened without it runs on the default
    connection while the row it means to lock is read through the routed
    one, so the lock holds nothing and the row write and the marker bump
    are not atomic. On the default router the two are the same database
    and the omission is invisible, which is why it has to be explicit.
    """
    return str(router.db_for_write(OxSchedule))


def _touch_change_row(using: str | None = None) -> None:
    """Tell every worker that the stored schedules moved."""
    alias = using or schedule_db_alias()
    now = timezone.now()
    try:
        OxScheduleChange.objects.using(alias).update_or_create(
            id=1, defaults={"changed_at": now}
        )
    except Exception as exc:
        # The marker holds a value that cannot be read back, written around
        # this module. update_or_create reads it first, in a savepoint of its
        # own, so the read is what raised and nothing was written. Written
        # over without reading it: a marker no write can move is one no
        # worker learns anything from, and every write through this module
        # would fail on it. Only after a failure known to be a value's,
        # raised in this process once the database had answered
        # (`is_unreadable_value`). An error the database itself raised says
        # nothing about what the marker holds, a DataError no more than a
        # lock or a lost connection: it is raised as it is, and the write
        # this was part of goes back with it.
        if not is_unreadable_value(exc, using=alias):
            raise
        OxScheduleChange.objects.using(alias).filter(id=1).update(changed_at=now)


def _unsaved(
    fields: Mapping[str, Any], now: datetime, func: str, where: str = ""
) -> OxSchedule:
    """
    A new schedule from a caller's fields, before anything is checked.

    `start_time` defaults to `now`, on a copy: the mapping is the caller's,
    and a batch gives every row the one reading of the clock.
    """
    _only_writable(fields, CREATABLE_FIELDS, func, where)
    return OxSchedule(created_at=now, updated_at=now, **{"start_time": now, **fields})


def _creation_errors(
    schedule: OxSchedule, alias: str, *, exporting: bool = False
) -> dict[str, list[ValidationError]]:
    """
    Everything that stops this new schedule being written to `alias`, by field.

    The one validation `create_schedule`, `create_schedules` and
    `_export_preflight` share, so a batch refuses exactly what a single
    call would, and a row the importer lets through is a row a paste
    accepts. Returned rather than raised because two of the three go on
    collecting: a batch reports every row at once, and the importer lists.

    It is the model's own `full_clean` and nothing beside it, so there is
    no rule here that `update_schedule` or the admin could be without.

    `exporting` is the importer's reading, taken before the registry entries
    it prints have been applied and possibly before the table exists. It
    leaves out what cannot be known then, the registry's word on task_key
    and whether the name is taken, and it makes no query. The check
    constraint goes with them, because Django validates one by having the
    database evaluate it. Nothing is lost by that: `validate_schedule`
    already refuses every row the constraint would.
    """
    errors: dict[str, list[ValidationError]] = {}
    token = _registry_decides.set(not exporting)
    try:
        with validate_against(alias):
            schedule.full_clean(
                exclude=_NOT_CLEANED,
                validate_unique=not exporting,
                validate_constraints=not exporting,
            )
    except ValidationError as exc:
        errors = exc.update_error_dict(errors)
    finally:
        _registry_decides.reset(token)
    return errors


def _flat(
    errors: dict[str, list[ValidationError]],
) -> list[tuple[str, str, str | None]]:
    """
    Each error as (field, message, code), in the order the clean found them.

    A failure that belongs to no one field has "" for its field.
    """
    return [
        ("" if field == NON_FIELD_ERRORS else field, message, error.code)
        for field, found in errors.items()
        for error in found
        for message in error.messages
    ]


def _row_failure(
    index: int, row: Mapping[str, Any], field: str, message: str, code: str | None
) -> ValidationError:
    """One entry of the error a refused batch raises."""
    return ValidationError(
        _ROW_FAILURE if field else _ROW_FAILURE_WITHOUT_FIELD,
        code=code,
        params={
            "index": index,
            "name": row.get("name"),
            "field": field,
            "message": message,
        },
    )


def create_schedule(*, user: Any = None, **fields: Any) -> OxSchedule:
    """
    Create a stored schedule, validated.

    `start_time` defaults to now, which is the point of writing it here:
    the boundary belongs to the moment the schedule came into existence,
    not to the moment a worker first happens to notice it.
    """
    schedule = _unsaved(fields, timezone.now(), "create_schedule")
    # Once, before the first statement, and the validation below reads it
    # too: a name checked against one database and written to another is
    # not checked at all.
    alias = schedule_db_alias()
    errors = _creation_errors(schedule, alias)
    if errors:
        raise ValidationError(errors)
    # After the clean, so the digest is over the values that will be stored.
    schedule.boundary_for = boundary_digest(schedule)
    check_permission(schedule, user)
    with transaction.atomic(using=alias):
        schedule.save(using=alias)
        _touch_change_row(alias)
    return schedule


def create_schedules(
    rows: Iterable[Mapping[str, Any]], *, user: Any = None
) -> list[OxSchedule]:
    """
    Create several stored schedules, all of them or none.

    Each row holds the keyword fields `create_schedule` takes and is held
    to what `create_schedule` holds it to. On top of that the names must
    differ within the batch.

    Every row is checked before any row is written. A non-mapping row or
    unsupported key raises TypeError immediately. Otherwise a batch with an
    invalid row in it writes nothing and raises one ValidationError listing
    every failure of every row, so two hundred rows are corrected in one
    pass rather than one refusal at a time. The list is flat rather than keyed
    by name, because a name can be missing or repeated and a position
    cannot: each entry's `params` carry the row's `index` in `rows`, the
    `name` it supplied, the `field` ("" when the failure belongs to no one
    field) and the `message`.

    `user`, when given, needs each row's registry permission, and is asked
    once the whole batch validates, as `create_schedule` asks after its
    clean. PermissionDenied names every row that was refused, and again
    nothing has been written.

    The rows share one reading of the clock, which is their `created_at`,
    their `updated_at` and the `start_time` of any row that does not bring
    its own, and the workers are told once.

    Checking the names does not reserve them. A schedule someone else
    creates between the check and the write is caught by the unique index,
    and that IntegrityError is raised with nothing of the batch left behind.
    So is any other error the database raises at the write, a value it
    refuses that validation could not know of; each carries a note naming
    the row by its index and the name it supplied. A value the database
    refuses in the check itself, a NUL in a name on PostgreSQL, raises its
    error there, before anything is written, with the same kind of note.
    """
    batch = list(rows)
    if not batch:
        return []
    alias = schedule_db_alias()
    now = timezone.now()
    schedules: list[OxSchedule] = []
    failures: list[ValidationError] = []
    first_named: dict[str, int] = {}
    for index, row in enumerate(batch):
        if not isinstance(row, Mapping):
            raise TypeError(_NOT_A_MAPPING.format(index=index, kind=type(row).__name__))
        schedule = _unsaved(row, now, "create_schedules", _IN_ROW.format(index=index))
        try:
            errors = _creation_errors(schedule, alias)
        except DatabaseError as exc:
            # A value the database refuses in validation's own query, the
            # one for names already taken, before anything is written: a
            # NUL in a name on PostgreSQL. Each row is checked by itself, so
            # the query that failed was this row's. Raised as the write
            # raises one below, with its class and its cause, and a note
            # naming the row.
            exc.add_note(
                _ROW_REFUSED_AT_THE_CHECK.format(
                    index=index, name=_printable(repr(row.get("name")))
                )
            )
            raise
        found = _flat(errors)
        # Among the names that are otherwise acceptable. A name the clean
        # refused is reported for that already, and two rows with no name
        # at all are not each other's duplicate.
        if "name" not in errors:
            first = first_named.setdefault(schedule.name, index)
            if first != index:
                found.append(
                    ("name", _REPEATED_NAME % {"first": first}, "repeated_name")
                )
        failures.extend(_row_failure(index, row, *entry) for entry in found)
        schedules.append(schedule)
    if failures:
        raise ValidationError(failures)
    # Permission after validation, as `create_schedule` orders the two, and
    # only for a batch that validates whole: a permission backend is never
    # handed a row that does not, and a refusal is PermissionDenied here as
    # it is there, rather than one more entry among the validation errors.
    denied = []
    for index, (row, schedule) in enumerate(zip(batch, schedules, strict=True)):
        # After the clean, so the digest is over the values that will be
        # stored.
        schedule.boundary_for = boundary_digest(schedule)
        try:
            check_permission(schedule, user)
        except PermissionDenied as exc:
            denied.append(
                _ROW_DENIED.format(index=index, name=row.get("name"), message=exc)
            )
    if denied:
        raise PermissionDenied(_ROWS_DENIED_SEPARATOR.join(denied))
    # Everything above only reads, and is done before the transaction is
    # opened, so the transaction holds nothing but the writes. On SQLite
    # that order is the difference between waiting and being refused. A
    # transaction that has already read is not made to wait for the write
    # lock there, so a batch that checked its rows inside its own
    # transaction would be refused at once with "database is locked" whenever
    # another connection was writing, a worker beside it for one. Opened to
    # write, it takes its turn on the busy timeout, as `create_schedule`
    # does.
    #
    # Nothing is given up for it. Reading inside the transaction does not
    # reserve a name on PostgreSQL or MySQL, and on every database the
    # unique index has the last word.
    with transaction.atomic(using=alias):
        for index, (row, schedule) in enumerate(zip(batch, schedules, strict=True)):
            try:
                schedule.save(using=alias)
            except DatabaseError as exc:
                # What validation cannot know: a name someone else took
                # since the check, or a value this database refuses, a NUL
                # in PostgreSQL text or a name equal to another under
                # MySQL's collation. The database's own error, raised as it
                # is, with its class and its cause, and the transaction takes
                # back the rows before it. A note says which row, which the
                # error itself cannot: it knows only the statement.
                exc.add_note(
                    _ROW_REFUSED_AT_THE_WRITE.format(
                        index=index, name=_printable(repr(row.get("name")))
                    )
                )
                raise
        _touch_change_row(alias)
    return schedules


def _export_preflight(fields: dict[str, Any]) -> list[tuple[str, str]]:
    """
    What a paste would refuse in one row, asked before the row is printed.

    For `ox_import_beat_schedules`, which prints rows for `create_schedules`
    and must not print one that call will refuse. This is the validation
    that call runs, against the database the row will be written to, less
    the two things the importer cannot know: whether task_key is registered,
    because the registry entries are printed alongside the rows, and whether
    the name is taken, because the table need not exist yet. For the same
    reason it makes no query.

    Returns `(field, message)` pairs, "" for a failure of no one field, and
    an empty list for a row that would pass. Only a refusal is returned. A
    keyword that is not a field is the importer's own mistake rather than
    the row's and raises TypeError, and anything else the validation itself
    raises on, it raises here, as it would at the paste.
    """
    schedule = _unsaved(fields, timezone.now(), "_export_preflight")
    errors = _creation_errors(schedule, schedule_db_alias(), exporting=True)
    return [(field, message) for field, message, _code in _flat(errors)]


def update_schedule(
    schedule: OxSchedule, *, user: Any = None, **fields: Any
) -> OxSchedule:
    """
    Change a stored schedule, moving the boundary when the timing moves.

    Retiming reschedules from the moment of the change: every tick before
    it is skipped, whatever the old definition would have done. At 15:00
    a schedule retimed from 02:00 to 14:00 does not fire today's 14:00,
    because at 14:00 today nobody could have expected a run.

    Enabling a disabled schedule moves the boundary too, so a pause does
    not accumulate a backlog that fires all at once on resume.

    Those rules are the whole of how the boundary moves here, so
    `start_time` is not a field this takes: writing it directly would move
    activation without counting the write, and the count is what tells a
    worker holding a pending heal that its sighting has been superseded.

    `enabled=False` given alone pauses the schedule without validating the
    rest of it (`_disable`), so a row that no longer validates can still be
    stopped. Anything else, enabling it again included, is validated.
    """
    _only_writable(fields, WRITABLE_FIELDS, "update_schedule")
    if _disables_only(fields):
        return _disable(schedule, user=user)
    # From the database, not from the instance. The admin hands this function
    # form.instance, which ModelForm._post_clean has already updated, so the
    # in-memory value is the new one and a re-enable through the change form
    # would never look like a transition.
    alias = schedule_db_alias()
    with transaction.atomic(using=alias):
        # The row under its own lock, and the values read from it. Reading
        # them outside the transaction and then saving every field off the
        # instance let a caller holding a stale copy write an old
        # start_time over a newer one, undoing a resume someone else had
        # just made.
        current = _lock_row(schedule.pk, alias)
        if current is None:
            raise OxSchedule.DoesNotExist(f"Schedule {schedule.pk} no longer exists.")
        # The clock once the lock is held, not before the wait for it. The
        # boundary this call may write is the moment of the change, and the
        # change is not made until the row is locked: a dispatcher that
        # derived a tick under the old definition during the wait, an
        # instant the new definition also contains, admits it against the
        # boundary, and a boundary from before the wait lets it through.
        now = timezone.now()
        # The values as the database holds them, before this call's changes.
        # Taken from the locked row rather than from the caller's instance,
        # which may be minutes old.
        previous = {field: stored_value(current, field) for field in TIMING_FIELDS}
        previously_enabled = current.enabled
        stale_boundary = current.boundary_for != boundary_digest(current)

        # Only the fields this call was given. Everything else keeps the
        # value the database holds now, not the value the caller last saw.
        for name, value in fields.items():
            setattr(current, name, value)

        retimed = any(
            stored_value(current, field) != previous[field] for field in TIMING_FIELDS
        )
        resumed = current.enabled and not previously_enabled
        # stale_boundary: the timing had already changed by a route that did
        # not move the boundary, so it is stale whatever this call changes.
        if retimed or resumed or stale_boundary:
            current.start_time = now
            # Counted here and nowhere else in this function: a call that
            # changes nothing the boundary is for rewrites boundary_for to
            # the value it already held and leaves start_time alone, which
            # is not a boundary write and must not fence a worker's
            # pending heal. Under this row's lock, so the read the
            # increment is computed from cannot move under it.
            current.boundary_generation = F("boundary_generation") + 1
        current.updated_at = now
        with validate_against(alias):
            current.full_clean(
                exclude=[
                    "boundary_for",
                    "boundary_generation",
                    "created_at",
                    "updated_at",
                ]
            )
        # After the clean, so the digest is over the values that will be stored.
        current.boundary_for = boundary_digest(current)
        check_permission(current, user)
        current.save(using=alias)
        _touch_change_row(alias)
    # The caller's instance is the one they will read from next. Read back
    # from the alias this call wrote to: unqualified it follows
    # db_for_read, and a replica that is behind either hands the caller the
    # values it has just replaced or, for a row younger than its snapshot,
    # raises DoesNotExist on a write that succeeded.
    schedule.refresh_from_db(using=alias)
    return schedule


def _disables_only(fields: Mapping[str, Any]) -> bool:
    """Whether an update asks for nothing but `enabled=False`."""
    if set(fields) != {"enabled"}:
        return False
    column = cast("Field[Any, Any]", OxSchedule._meta.get_field("enabled"))
    try:
        return column.to_python(fields["enabled"]) is False
    except ValidationError:
        return False


def _disable(schedule: OxSchedule, *, user: Any = None) -> OxSchedule:
    """
    Pause a stored schedule without asking whether the rest of it is valid.

    Disabling is how an operator stops a schedule, and a schedule that no
    longer validates is the one most likely to need stopping: a row stored
    before a rule existed, or written around this module. Validating the
    whole row first refused exactly those, so the one way to stop them was
    to delete them. Nothing else changes here, so nothing else is checked;
    enabling the row again, or any other change, is validated in full.

    Otherwise as `update_schedule`: under the row's lock, authorized by the
    task's registry permission, the boundary handled the way a pause
    handles it, and the workers told.
    """
    alias = schedule_db_alias()
    with transaction.atomic(using=alias):
        current = _lock_row(schedule.pk, alias)
        if current is None:
            raise OxSchedule.DoesNotExist(f"Schedule {schedule.pk} no longer exists.")
        check_permission(current, user)
        now = timezone.now()
        written = ["enabled", "updated_at"]
        try:
            # The digest over values the field cannot convert raises; such
            # a row cannot be built, so its boundary is left as it stands.
            stale_boundary = current.boundary_for != boundary_digest(current)
            current.enabled = False
            digest: str | None = boundary_digest(current)
        except (ValidationError, ArithmeticError, TypeError, ValueError):
            current.enabled = False
            stale_boundary, digest = False, None
        if stale_boundary:
            # As update_schedule does: the timing had moved by a route that
            # did not move the boundary, so it is stale whatever this
            # changes, and the write moves it.
            current.start_time = now
            current.boundary_generation = F("boundary_generation") + 1
            written += ["start_time", "boundary_generation"]
        if digest is not None:
            current.boundary_for = digest
            written.append("boundary_for")
        current.updated_at = now
        current.save(using=alias, update_fields=written)
        _touch_change_row(alias)
    schedule.refresh_from_db(using=alias)
    return schedule


def delete_schedule(schedule: OxSchedule) -> None:
    """
    Delete a stored schedule and tell the workers.

    A plain delete() leaves every running worker holding the schedule until
    something else changes, enqueueing and rolling back once a pass.
    """
    alias = schedule_db_alias()
    with transaction.atomic(using=alias):
        schedule.delete(using=alias)
        _touch_change_row(alias)


def _lock_row(pk: int, db_alias: str) -> OxSchedule | None:
    """
    Take this schedule's row lock and return the row as it stands.

    On PostgreSQL and MySQL a locking read is a current read: it waits for
    a concurrent writer, then reads the committed row rather than the
    transaction's snapshot.

    SQLite has neither row locks nor `SELECT ... FOR UPDATE`, and Django
    drops the clause there without raising, so a locking read alone holds
    on two databases and does nothing at all on the third. What serialises
    SQLite is being a writer, and a transaction that reads first starts as
    a reader. The no-op UPDATE makes it a writer before it reads.

    Nothing here depends on a rowcount. Reading one would rest on Django
    setting MySQL's FOUND_ROWS flag, and a project setting its own
    client_flag would lose the guarantee with no error.
    """
    rows = OxSchedule.objects.using(db_alias).filter(pk=pk)
    if connections[db_alias].features.has_select_for_update:
        return rows.select_for_update().first()
    OxSchedule.objects.using(db_alias).filter(pk=pk).update(name=F("name"))
    return rows.first()


def _paused(row: UnreadableRow) -> bool:
    """Whether a row that did not read is paused for certain: `enabled` read false."""
    return row.values.get("enabled") is False


def _current_row(pk: int, db_alias: str) -> OxSchedule | UnreadableRow | None:
    """
    Take this schedule's row lock and read the row as it stands, for a
    worker deciding whether to dispatch it or move its boundary: the row,
    what of it did not read, or None when it is gone.

    Locked as `_lock_row` locks: a locking read on PostgreSQL and MySQL,
    and on SQLite a no-op UPDATE first, which makes the transaction the
    writer before it reads. Read as `_stored_read.read_schedules` reads,
    each value decoded rather than converted, so a value that does not
    read is reported by field instead of raised, and one SQLite's
    converters would read as something else (a start that is not a date
    read as None, an `enabled` of 2 read as False) does not read either.

    The statements `_lock_row` sends and no more, on every dispatch of a
    stored schedule: the one locking read, or on SQLite the UPDATE and
    the read. No savepoint is taken around the read, and whether the row
    is still there comes from the read itself, which leaves out a row
    that is gone. A read the database refuses is raised as it is, for the
    dispatch loop to roll this schedule back on.
    """
    return lock_schedule(pk, using=db_alias)


class DatabaseScheduleSource:
    """
    Schedules read from OxSchedule rows.

    Named in a backend's OPTIONS::

        "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"}

    Rows are re-read only when the change row moves, so the steady state
    is one cheap read of one row per dispatch pass. Polling rather than
    listening on purpose: a notification channel would be a second thing
    to deploy and monitor, and not needing one is the whole point of this
    package.

    A row is input from a person, unlike a settings entry, so it is
    treated as such. One that no longer validates, or that names a key
    this deployment does not register, is skipped and logged rather than
    allowed to stop every other schedule from firing. Validation cannot
    see everything the database will refuse, so a row that passes it and
    is then refused at dispatch is isolated there instead: the worker
    rolls back that row's tick, reports it, and dispatches the rest
    (`Worker.dispatch_schedules`).

    Rows come after the settings schedules, in primary-key order. The
    order is for reproducibility and nothing else: every schedule in a
    pass is attempted whatever happens to the ones before it, and the
    tick constraint, not the order, is what coordinates workers. Without
    an explicit order a PostgreSQL table hands back rows in heap order,
    which an ordinary edit changes.

    The backend's ``OPTIONS["SCHEDULES"]`` come along as well. Naming this
    source adds the rows to the schedules a project already declared; it
    does not replace them, which would turn the switch into a silent stop
    for every schedule the settings hold.
    """

    def __init__(self, options: dict[str, Any], backend_alias: str) -> None:
        self._options = options
        self._backend_alias = backend_alias
        #: The settings schedules, built on first use rather than here.
        #: A bad SCHEDULES entry is E002's to report; building it here
        #: would report it again under E006, and the worker still fails at
        #: startup because its constructor asks for the schedules.
        self._settings: list[Any] | None = None
        self._cached: list[Any] = []
        #: The dispatch key of every row the last full read met, the
        #: disabled ones and the ones that did not build included, less any
        #: found gone under its lock since. What `_stored_keys` answers.
        self._row_keys: set[str] = set()
        self._seen_change: Any = _UNREAD
        #: Rows whose boundary was found stale, with the boundary as it
        #: stood when it was found: the digest column, the start time and
        #: the count of boundary writes.
        #: Healed on the next pass rather than in place: the dispatch
        #: transaction rolls back when a tick is refused, which would take
        #: the heal with it. The observed boundary is what the heal checks
        #: against, not whether the digest matches by then. A row disabled
        #: with queryset.update, met under the lock, and re-enabled the
        #: same way before the next pass matches its digest again, and a
        #: heal that asked only that would skip, leave the boundary where
        #: the pause found it, and fire the tick that came due inside the
        #: pause at the next full read.
        self._needs_heal: dict[Any, tuple[Any, Any, Any]] = {}
        #: When the rows were last read in full, on the monotonic clock.
        #: None means never.
        self._last_read: float | None = None
        #: What this source says about rows it leaves out, about a marker
        #: it cannot read and about a boundary the database will not let it
        #: move: once in full, then in summary.
        self._report = _RowReport()
        self._db_alias = schedule_db_alias()
        #: How often to read every row regardless of the change marker.
        raw_interval = options.get("SCHEDULE_RECONCILE_INTERVAL", 60.0)
        try:
            self._reconcile_interval = float(raw_interval)
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                "SCHEDULE_RECONCILE_INTERVAL must be a number of seconds, "
                f"not {raw_interval!r}."
            ) from exc
        if self._reconcile_interval <= 0:
            raise ImproperlyConfigured(
                "SCHEDULE_RECONCILE_INTERVAL must be greater than zero; it is "
                "the backstop that finds a row changed without this package's "
                "write functions."
            )

    def schedules(self) -> list[Any]:
        if self._settings is None:
            from .schedules import schedules_from_options

            self._settings = schedules_from_options(self._options, self._backend_alias)
        return [*self._settings, *self._rows()]

    def _stored_keys(self) -> set[str]:
        """
        The dispatch key of every row that exists, whether or not it is
        among the schedules this source answers: a paused row, and one
        that no longer builds, included. As of the last full read, less the
        rows found gone under their lock since.

        The worker keeps a failing schedule's report while its key is here
        or among `schedules()` (`Worker.dispatch_schedules`). A paused row
        is not dispatched, so `schedules()` leaves it out, but it is the
        same schedule when it is resumed, and its run of failures ends with
        a recovery or goes on, rather than starting again, only if the
        report outlives the pause. A deleted row's report goes.

        Costs no statement: the full read already reads every row, the
        disabled ones included, to find stale boundaries, and the row lock
        at dispatch already finds a row gone.
        """
        return self._row_keys

    def _rows(self) -> list[Any]:
        """The enabled rows as schedules, re-read when the marker moves."""
        if self._needs_heal:
            self._heal()
        changed_at: Any
        try:
            changed_at = (
                OxScheduleChange.objects.using(self._db_alias)
                .filter(id=1)
                .values_list("changed_at", flat=True)
                .first()
            )
        except Exception as exc:
            if not is_unreadable_value(exc, using=self._db_alias):
                # Not known to be a value's. What the database raised is the
                # database not answering, a DataError of its own as much as
                # a lost connection, and says nothing about what the marker
                # holds. Anything else is not a reading of the marker at
                # all, and goes up as it is.
                if not isinstance(exc, DatabaseError):
                    raise
                return self._marker_unavailable(exc)
            # The marker holds a value that cannot be read: written around
            # this module, since every write here replaces it. It says
            # nothing about whether the rows moved, so they are read in full
            # at the reconcile interval, and the next write through this
            # module replaces it with one that reads, which moves it.
            due, _, _ = self._report.due(("marker",))
            if due:
                logger.warning(
                    _MARKER_UNREADABLE,
                    _printable(f"{type(exc).__name__}: {exc}"),
                    extra={"event": "schedule_source_unavailable"},
                )
            changed_at = _UNREADABLE
        else:
            self._report.forget(("marker",))
        if changed_at == self._seen_change and not self._due_for_a_full_read():
            return self._cached
        self._cached = self._build()
        if self._needs_heal:
            # Found by this read, healed by this read. Deferred to the next
            # call, a row that changes again in between can match its
            # digest once more, so the heal is skipped and the boundary
            # never moves: a pause seen here and a raw resume before the
            # next call would fire the tick that came due in between. The
            # heal bumps the marker, so the next call reads again anyway.
            self._heal()
            self._cached = self._build()
        self._seen_change = changed_at
        self._last_read = time.monotonic()
        return self._cached

    def _marker_unavailable(self, exc: DatabaseError) -> list[Any]:
        """
        The database did not answer the marker read: the last known set.

        An empty list would read as "no schedules are configured", which is
        a different and much worse claim, so the last known set stands
        until the database answers again.

        The cause in the message and no traceback, the way
        schedule_lock_unavailable already reports. This is one statement on
        one small table once a dispatch pass, so while it keeps failing it
        is reported about once a second per worker, and the traceback is
        byte-identical every time: it carries nothing the message does not
        and costs about 3.4 KB a record, which is enough to crowd out the
        reports that do.
        """
        logger.warning(
            "Could not read the schedule change marker (%s); using the "
            "last known schedules",
            exc,
            extra={"event": "schedule_source_unavailable"},
        )
        return self._cached

    def _due_for_a_full_read(self) -> bool:
        """
        Has it been long enough to read every row again regardless?

        The change marker is bumped by this package's own write functions
        and by nothing else, so a row created, re-enabled or retimed with
        `queryset.update()`, a data migration or a fixture moves nothing
        that a worker watches. Without a periodic read those rows are
        invisible until something unrelated happens to bump the marker.

        A schedule that is disabled is not in the cache, so a raw re-enable
        cannot be noticed any other way: this read is what finds it.

        The marker is what makes an ordinary edit visible within a second.
        This is the backstop, and it is one indexed read of a small table.
        """
        if self._last_read is None:
            return True
        return (time.monotonic() - self._last_read) >= self._reconcile_interval

    def _heal(self) -> None:
        """
        Move the boundary of every schedule found stale at dispatch.

        A timing change made by a route that runs no model code leaves the
        boundary set for the old timing, so the schedule would fire an
        instant nothing scheduled while it was in the future. Dispatch
        refuses that tick; this moves the boundary forward so the schedule
        resumes on its new timing rather than being refused forever.

        Bumping the change marker is not incidental. A worker whose cached
        copy still holds the old timing has nothing else to tell it to
        re-read, and would go on proposing ticks the row no longer wants.
        """
        for pk, observed in list(self._needs_heal.items()):
            name = None
            try:
                with transaction.atomic(using=self._db_alias):
                    row = _current_row(pk, self._db_alias)
                    if row is None:
                        # The row is gone, so there is no boundary to move.
                        self._settled(pk)
                        continue
                    if isinstance(row, UnreadableRow):
                        # It holds a value that does not read, so its
                        # boundary cannot be moved and it cannot be built
                        # either. The sighting goes: kept, it would be
                        # retried under the row's lock on every pass. The
                        # full read reports the row and leaves it out, and
                        # finds the boundary stale again once it reads. A
                        # count of boundary writes at its column's maximum
                        # is such a value: the move below adds one to the
                        # count, so it is never tried on one.
                        self._drop_sighting(pk)
                        self._unreadable_row(row)
                        continue
                    name = row.name
                    # After the lock, per row. The boundary is the moment
                    # the change was found, and a wait for the lock is
                    # time the row can change again in: resumed while the
                    # heal waited, a tick due in that wait is inside the
                    # pause, and a boundary from before the wait is in
                    # front of it.
                    now = timezone.now()
                    if (
                        row.boundary_for,
                        row.start_time,
                        row.boundary_generation,
                    ) != observed:
                        # Someone wrote the boundary since this worker saw
                        # it stale: another worker's heal, or the write
                        # API. Their boundary stands.
                        #
                        # The generation is what makes that unconditional.
                        # A heal sets the start time to its own clock, and
                        # "sets it" is not "changes it": a healer whose
                        # clock reads the instant the boundary was written
                        # at writes the observed pair back unchanged, and
                        # a fence on the columns alone would let a second
                        # healer move the boundary again, onto its own
                        # later clock, discarding the ticks in between.
                        # Two workers' clocks disagree, a clock steps back
                        # over a correction, and a coarse clock puts the
                        # write and the heal in one granule. The count
                        # moves whatever the clock does.
                        self._settled(pk)
                        continue
                    # Whether or not the digest matches by now. The row was
                    # seen in a state its boundary was not set for, and
                    # nothing has moved the boundary since, so the state it
                    # is in now is one it reached after the boundary was
                    # set. A pause and a resume between the sighting and
                    # this lock leave the digest equal to the column, and
                    # the tick between them still has to be behind the
                    # boundary.
                    OxSchedule.objects.using(self._db_alias).filter(pk=pk).update(
                        start_time=now,
                        boundary_for=boundary_digest(row),
                        boundary_generation=F("boundary_generation") + 1,
                    )
                    _touch_change_row(self._db_alias)
                    self._settled(pk, healed=True)
            except Exception as exc:
                if isinstance(exc, ValidationError) or is_unreadable_value(
                    exc, using=self._db_alias
                ):
                    # A value the digest refused, in a row that read, or one
                    # that failed in this process where the read under the
                    # lock could not go on to name its field. The sighting
                    # goes, as for a row that does not read, and the row is
                    # reported.
                    self._drop_sighting(pk)
                    self._row_failed(pk, name, exc)
                    continue
                # What the database raised is not the row's, whatever its
                # class: the read under the lock refused, or the write. The
                # sighting stays and the move is tried again on the next
                # pass. Anything else goes up.
                if not isinstance(exc, DatabaseError):
                    raise
                self._heal_failed(pk, exc)

    def _heal_failed(self, pk: int, exc: DatabaseError) -> None:
        """
        Log a boundary the database would not let this worker move: with its
        traceback the first time, then in summary.

        The move is tried again at the start of every dispatch pass, about
        once a second, and what refused it once can refuse it every time: a
        trigger or a constraint of the project's own on the table refuses
        the write for as long as it stands, and so does a permission taken
        away. A traceback a pass would crowd every other report out of the
        log, so the failures after the first are counted, and said at most
        once a ROW_REPORT_INTERVAL, per row. Only the reporting is held
        back. The move is tried on every pass all the same, and the sighting
        stays until it succeeds or is settled, which is when the count is
        forgotten (`_drop_sighting`).

        A count of boundary writes at its column's maximum, which the move
        cannot add to, does not come here: a row holding one does not read,
        and no move of its boundary is tried (`_heal`).

        `failures` and `suppressed` are what they are on the lines about a
        dispatch that keeps failing (`worker._DispatchReport`): every failed
        move of this run, and the ones since the last line, this one
        included.
        """
        key = ("heal", pk)
        due, first, unsaid = self._report.due(key)
        if not due:
            return
        error = type(exc).__name__
        # Each record's extra is a literal dict, so the docs test's scan of
        # emitted keys can read every key it carries.
        if first:
            logger.warning(
                _HEAL_FAILED,
                pk,
                exc_info=exc,
                extra={
                    "event": "schedule_boundary_heal_failed",
                    "schedule_pk": pk,
                    "error": error,
                    "failures": 1,
                    "suppressed": 0,
                },
            )
            return
        # The ones that went unsaid, and this one, which has no line of its own.
        since = unsaid + 1
        logger.warning(
            _HEAL_STILL_FAILING,
            pk,
            since,
            error,
            extra={
                "event": "schedule_boundary_heal_failed",
                "schedule_pk": pk,
                "error": error,
                "failures": self._report.times(key),
                "suppressed": since,
            },
        )

    def _drop_sighting(self, pk: int) -> None:
        """
        Forget a sighting, and with it what was being held back about its
        move failing. A boundary that will not move after this is a new
        failure, and is reported in full.
        """
        self._needs_heal.pop(pk, None)
        self._report.forget(("heal", pk))

    def _settled(self, pk: int, *, healed: bool = False) -> None:
        """
        Forget a sighting, once the transaction that settled it commits.

        A sighting is the only record that the row was met in a state its
        boundary was not set for; the row itself keeps no trace of having
        been seen. `dispatch_schedules()` may be called inside a
        transaction the caller owns, and everything this pass decided goes
        back with that transaction if it rolls back: the boundary the heal
        wrote, and equally the read that said another writer owns the
        boundary or that the row is gone. Dropping the sighting there
        would leave nothing to re-heal the row, and a raw resume restores
        the digest, so no later read finds it stale again and the tick
        that came due inside the pause fires.

        Deferring covers the ordinary pass with the same mechanism rather
        than a second one: outside a transaction the heal's own `atomic`
        is the outermost, so the callback runs as it exits and the
        sighting is gone before this method returns.
        """

        def forget() -> None:
            self._drop_sighting(pk)
            if healed:
                logger.info(
                    "Moved schedule %s to a boundary matching its timing",
                    pk,
                    extra={"event": "schedule_boundary_healed", "schedule_pk": pk},
                )

        transaction.on_commit(forget, using=self._db_alias)

    def _build(self) -> list[Any]:
        built = []
        keys = set()
        failing = set()
        # Every row, the disabled ones included. A disabled row is not
        # dispatched, but its boundary can be stale: a pause made outside
        # the write API leaves the boundary set for the enabled state, and
        # moving it now is what stops a raw resume from firing a tick that
        # came due inside the pause. This is the only read that sees a
        # disabled row at all, which is also why it is the one that says
        # which rows exist (`_stored_keys`).
        for pk, row in self._read_every_row():
            # A row that cannot be read is still a row: it exists, so it
            # keeps its key and its failure report, and it is named by the
            # key it has rather than dropped as if it were gone.
            keys.add(f"{STORED_KEY_PREFIX}{pk}")
            if isinstance(row, UnreadableRow):
                # Left out quietly only when it is paused for certain: an
                # `enabled` that read as false. One that did not read is not
                # a pause, and is reported like any other value.
                if not _paused(row):
                    failing.add(pk)
                    self._unreadable_row(row)
                continue
            # Checked here, where every row is read whether or not a tick
            # of it is due. At dispatch it would sit behind the snapshot's
            # own filters, so a row whose cached copy said "not due" would
            # never reach it: an expired end_time, a boundary in the
            # future, or a period long enough that the next tick is months
            # away would each hide that the row had changed.
            refused: BaseException | None = None
            try:
                digest: str | None = boundary_digest(row)
            except Exception as exc:
                # A column holding what its field cannot convert. Inside
                # this row's handling: raised from here it ended the read,
                # and with it every schedule this source answers.
                digest, refused = None, exc
            if digest is not None and row.boundary_for != digest:
                self._needs_heal[row.pk] = (
                    row.boundary_for,
                    row.start_time,
                    row.boundary_generation,
                )
            if not row.enabled:
                continue
            if refused is not None:
                failing.add(pk)
                self._row_failed(
                    pk, row.name, refused, columns=partial(_unconvertible_fields, row)
                )
                continue
            try:
                built.append(self._to_schedule(row))
            except Exception as exc:
                # Every exception from the row, not a chosen list. A row is
                # input from a person, and the guarantee that one bad row
                # cannot stop the others cannot rest on predicting how a
                # row goes wrong. The database's own failures are not the
                # row's: one raised while building it (a form that queries)
                # goes up, and the read is abandoned as any other is.
                if isinstance(exc, DatabaseError) and not isinstance(exc, DataError):
                    raise
                failing.add(pk)
                self._row_failed(pk, row.name, exc)
        # Once the read has gone through. One that raises part way leaves
        # the last complete answer, as it leaves the cached schedules.
        self._report.keep_rows(failing)
        self._row_keys = keys
        return built

    def _read_every_row(self) -> list[tuple[int, OxSchedule | UnreadableRow]]:
        """
        Every row in primary-key order, each with its key: the row, or what
        of it did not read.

        Read as `_stored_read.read_schedules` reads: the keys, which always
        read, and then the rows a batch of keys at a time, each value
        decoded rather than converted. A value Django's converters raise on
        (a PostgreSQL timestamp outside the years a datetime holds, a date
        SQLite was given that does not exist, a MySQL zero date) is that
        row's, and the rest of the batch still reads. One SQLite's
        converters read as something else, a start or an end that is text
        and not a date, an `enabled` of 2, does not read either: built from
        what the converter said, the row would dispatch with no boundary or
        no end, or sit paused with nothing to say so.
        """
        rows = read_schedules(
            OxSchedule.objects.using(self._db_alias).order_by("pk"),
            using=self._db_alias,
        )
        return list(rows.items())

    def _unreadable_row(self, row: UnreadableRow) -> None:
        """Log a stored row left out because a value in it does not read."""
        reason = (
            _FIELDS_UNREADABLE.format(reason=row.reason)
            if row.unreadable
            else row.reason
        )
        self._row_failed(
            row.pk,
            row.values.get("name"),
            row.cause or ValueError(reason),
            reason=reason,
            fields=row.fields,
        )

    def _row_failed(
        self,
        pk: int,
        name: Any,
        exc: BaseException,
        *,
        columns: Callable[[], list[str]] | None = None,
        reason: str | None = None,
        fields: Sequence[str] = (),
        traceback: bool = False,
    ) -> None:
        """
        Log a stored row left out: in full the first time, then in summary.

        Named by its primary key, which cannot be anything but a number, and
        by its name made safe to print, since the name is whatever was
        stored. The reason is cut to a length and escaped the same way: an
        error message can quote the value that caused it. `columns`, for a
        row a value of which did not convert, names the columns that did not,
        and is asked only when a line is written. `reason`, where the caller
        has one, is the reason as it stands.

        The line carries the fields at fault as a list of their names as
        well, for whoever reads the record rather than the sentence: those
        in `fields`, or the ones `columns` names. The list is empty where no
        field is at fault, a row that read and could not be built, and
        where the read could not say which field it was.
        """
        due, first, unsaid = self._report.due(("row", pk))
        if not due:
            return
        if reason is not None:
            reason = _printable(reason)
        elif columns is not None:
            error = _printable(f"{type(exc).__name__}: {exc}")
            named = columns()
            fields = named
            reason = (
                _COLUMNS_UNREADABLE.format(columns=", ".join(named), error=error)
                if named
                else _A_VALUE_UNREADABLE.format(error=error)
            )
        else:
            reason = _printable(str(exc))
        label = _row_label(pk, name)
        safe_name = _printable(name) if isinstance(name, str) else None
        if first:
            logger.warning(
                _SKIPPING,
                label,
                reason,
                exc_info=exc if traceback else None,
                extra={
                    "event": "schedule_row_skipped",
                    "schedule": safe_name,
                    "schedule_pk": pk,
                    "reason": reason,
                    "fields": list(fields),
                },
            )
            return
        logger.warning(
            _STILL_SKIPPING,
            label,
            unsaid,
            reason,
            extra={
                "event": "schedule_row_skipped",
                "schedule": safe_name,
                "schedule_pk": pk,
                "reason": reason,
                "fields": list(fields),
            },
        )

    def _to_schedule(self, row: OxSchedule) -> Any:
        from .schedules import IntervalTrigger, Schedule

        # Around the lookup alone: a KeyError from anywhere later, a form's
        # own clean among them, is not this, and said to be would send
        # someone to check a registration that is fine.
        try:
            kind = registry.get(row.task_key)
        except KeyError:
            raise ValueError(
                _TASK_KEY_NOT_REGISTERED.format(task_key=_printable(repr(row.task_key)))
            ) from None
        arguments = dict(row.arguments) if isinstance(row.arguments, dict) else None
        if arguments is None:
            raise ValueError("arguments must be a mapping")
        if kind.form is not None:
            form = kind.form(arguments)
            if not form.is_valid():
                raise ValidationError(dict(form.errors))
            # The cleaned values, not the raw ones. A form declares the types
            # its task expects, and passing the raw row through would honour
            # that declaration only where the field happens to reject.
            arguments = dict(form.cleaned_data)
            # Raises, so _build and _current skip this row and log it. A
            # row can be written around validate_schedule.
            normalize_json(arguments)
        trigger: Any
        if row.trigger == OxSchedule.Trigger.CRON:
            trigger = CronExpression(row.cron)
        else:
            if not row.every_seconds:
                raise ValueError(
                    "an interval schedule needs a non-zero interval; this row "
                    "has none, and building a trigger from it would divide by "
                    "zero on the dispatch path"
                )
            if _ticks_overflow(row.every_seconds, row.phase_seconds):
                # Raised here for the same reason. The trigger would raise
                # where the dispatch pass plans every schedule's tick,
                # which is outside any one schedule's handling.
                raise ValueError(_TICKS_BEFORE_YEAR_ONE)
            trigger = IntervalTrigger(
                every=timedelta(seconds=row.every_seconds),
                phase=timedelta(seconds=row.phase_seconds),
            )
        # The bounds as the dispatch pass will compare them with its clock:
        # a time, with a zone exactly when USE_TZ is on. A row that holds
        # anything else was written around validate_schedule, and compared
        # there it raised outside any one schedule's handling.
        for field in ("start_time", "end_time"):
            bound = getattr(row, field)
            if bound is None:
                continue
            if not isinstance(bound, datetime):
                raise ValueError(_BOUND_IS_NOT_A_TIME.format(field=field))
            if timezone.is_aware(bound) != settings.USE_TZ:
                mismatch = _BOUND_HAS_NO_ZONE if settings.USE_TZ else _BOUND_HAS_A_ZONE
                raise ValueError(mismatch.format(field=field))
        pk, alias = row.pk, self._db_alias
        # Bound to this backend, exactly as schedules_from_options binds a
        # settings-declared schedule: the schedule is dispatched by this
        # backend's workers, so its enqueues belong in this backend's queue
        # whatever alias the task was declared with.
        task = (
            kind.task
            if kind.task.backend == self._backend_alias
            else kind.task.using(backend=self._backend_alias)
        )
        return Schedule(
            # A label, and what every line a worker logs about the schedule
            # carries, so made safe to print here: the write functions and
            # the admin's form store a name with a line break or an escape
            # sequence in it as it is given.
            name=_printable(row.name),
            # The row's identity, not its label. Renaming a schedule must not
            # change what its ticks are keyed on, or a worker holding the old
            # label and one holding the new would write two tick rows for the
            # same instant and the unique constraint would coordinate neither.
            dispatch_key=f"{STORED_KEY_PREFIX}{row.pk}",
            task=task,
            trigger=trigger,
            args=(),
            kwargs=arguments,
            start_time=row.start_time,
            end_time=row.end_time,
            starting_deadline=(
                timedelta(seconds=row.starting_deadline_seconds)
                if row.starting_deadline_seconds is not None
                else None
            ),
            # A row was given its boundary when it was created, so it does
            # not need a first sighting to establish one.
            anchors=False,
            refresh=lambda: self._current(pk, alias),
        )

    def _current(self, pk: int, db_alias: str) -> Any:
        """
        This schedule as it stands now, under its lock, or None.

        None means the row is gone, is disabled, or no longer describes a
        schedule this deployment can run. The dispatch loop treats all three
        the same way: it commits nothing (on SQLite the lock is a no-op
        UPDATE, rolled back with the rest), so the tick stays unclaimed and
        a worker with a current view can still act on it.
        """
        try:
            row = _current_row(pk, db_alias)
        except Exception as exc:
            # A lock-wait timeout, or SQLite reporting the database busy.
            # One schedule's contention must not end the pass for the rest,
            # and it is contention, so no traceback. Anything else goes to
            # the dispatch loop, which rolls this schedule back and decides
            # from the connection, not the class, whether the rest of the
            # pass can go on.
            if not isinstance(exc, DatabaseError) or not lock_contention(exc):
                raise
            logger.warning(
                "Could not lock stored schedule %s this pass, the database gave "
                "up waiting for a lock: %s",
                pk,
                exc,
                extra={"event": "schedule_lock_unavailable", "schedule_pk": pk},
            )
            return None
        if isinstance(row, UnreadableRow):
            # Changed around this module since the snapshot was taken, to a
            # value that does not read. Left out as the full read leaves it
            # out, from the snapshot as well, so it is not locked again on
            # every pass until that read: reported, unless it is paused for
            # certain, and with no heal, since its boundary cannot be
            # checked.
            if not _paused(row):
                self._unreadable_row(row)
            self._leave_out(pk)
            return None
        digest = None
        if row is not None:
            try:
                digest = boundary_digest(row)
            except Exception as exc:
                # A column its field cannot convert. A paused row is left as
                # a paused row is, below; its boundary cannot be checked.
                if row.enabled:
                    self._row_failed(
                        pk, row.name, exc, columns=partial(_unconvertible_fields, row)
                    )
                    self._leave_out(pk)
                    return None
        if row is None or not row.enabled:
            if row is not None and digest is not None and row.boundary_for != digest:
                # A pause made outside the write API, met at dispatch
                # before any full read found it. Healed on the next pass,
                # so the boundary sits at the pause and a raw resume cannot
                # fire a tick from inside it, whether the resume lands
                # before or after the heal.
                self._needs_heal[pk] = (
                    row.boundary_for,
                    row.start_time,
                    row.boundary_generation,
                )
            # Drop it from the snapshot too. Returning None alone would
            # leave the row in the cache, so every later pass would plan its
            # tick and take its lock again for a schedule that cannot fire.
            key = self._leave_out(pk)
            if row is None:
                # Gone, deleted without the write API or before the marker
                # read saw it: its failure report can go now rather than at
                # the next full read. A disabled row stays among the keys;
                # it is paused, not gone.
                self._row_keys.discard(key)
            return None
        if row.boundary_for != digest:
            # The timing changed without the boundary moving, so the tick
            # this worker planned belongs to a definition that no longer
            # applies. Refuse it, and heal on the next pass: the refusal
            # rolls this transaction back and would take the heal with it.
            self._needs_heal[pk] = (
                row.boundary_for,
                row.start_time,
                row.boundary_generation,
            )
            return None
        try:
            return self._to_schedule(row)
        except Exception as exc:
            # As at the full read: the database's own failure is the
            # dispatch loop's to judge, from the connection.
            if isinstance(exc, DatabaseError) and not isinstance(exc, DataError):
                raise
            self._row_failed(pk, row.name, exc, traceback=True)
            self._leave_out(pk)
            return None

    def _leave_out(self, pk: int) -> str:
        """Drop a row from the snapshot until the next full read; its key."""
        key = f"{STORED_KEY_PREFIX}{pk}"
        self._cached = [s for s in self._cached if s.dispatch_key != key]
        return key
