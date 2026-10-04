"""
Read a django-celery-beat schedule table and print what django-ox needs.

Writes nothing, anywhere. A migration is a decision about production
timing, so this prints and stops; you read it, edit it, and apply it
yourself.

A row comes out one of three ways, and each is said: as a schedule that
fires at the times it fired at under beat; as a schedule named with the
ways it will differ, which are runs around a change of the clocks, runs
beat's own schedule loading made late, and an interval's phase; or listed
by name with the reason it was not translated. No row is printed on a
guess: where what beat did with it cannot be established from the table
and the settings, it is listed. Where one row stopped beat for the whole
table, the output says so before anything else.
"""

from __future__ import annotations

import importlib.metadata
import json
import math
import re
import zoneinfo
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any, cast

from django.conf import settings
from django.core.management.base import CommandError, CommandParser
from django.core.validators import MaxValueValidator
from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
    DataError,
    connections,
    router,
)
from django.utils import timezone

from ...cron import CronExpression
from ...models import OxSchedule
from ...schedules import MAX_INTERVAL
from ...stored import _export_preflight, schedule_db_alias
from .. import _beat_cron, _beat_timing
from .._database import DatabaseCommand

# django-celery-beat's tables are read with raw SQL rather than through a
# model, because this package does not depend on it and cannot import one.
# The three names below are module constants, never arguments, so the S608
# suppressions on the queries are about interpolating a constant table name
# and not about user input reaching a statement.
BEAT_TABLE = "django_celery_beat_periodictask"
CRONTAB_TABLE = "django_celery_beat_crontabschedule"
INTERVAL_TABLE = "django_celery_beat_intervalschedule"

#: The task beat schedules for itself whenever results expire. It is in
#: the table of every such installation, and nobody put it there.
CELERY_CLEANUP_TASK = "celery.backend_cleanup"

#: What beat decodes for a row's args and kwargs when the column holds
#: nothing: `model.args or '[]'`, `model.kwargs or '{}'`.
_EMPTY_ARGUMENTS = {"args": "[]", "kwargs": "{}"}

#: _kombu_json() for a kombu from before Celery type markers.
_OLD_KOMBU = object()

#: What JSON decodes to, and so all a stored schedule's arguments hold.
_JSON_SCALARS = (str, int, float, bool, type(None))

#: The zone of a crontab read from a table that has no timezone column.
#: Distinct from an empty value in a table that has one, which beat reads
#: differently.
_NO_ZONE_COLUMN = object()

#: Every keyword timedelta takes, which is how beat uses a row's period: it
#: builds the interval as timedelta(**{period: every}) and runs whatever
#: that makes. Its model offers five of them, and does not hold a saved row
#: to its choices, so a row with weeks or milliseconds ran like any other.
#: Any other period is not a keyword timedelta takes, and beat raised on it.
PERIODS = frozenset(
    {"weeks", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds"}
)
_ONE_SECOND = timedelta(seconds=1)

#: The longest cron expression a stored schedule's column holds. Asked of
#: the model, where create_schedules will ask it.
CRON_LENGTH = cast("int", OxSchedule._meta.get_field("cron").max_length)

#: How many years of a zone's clock changes a crontab is held against. Far
#: enough to cover the rules a zone has announced, and the output says how
#: far, because no number of years makes the absence of a notice a promise.
HORIZON_YEARS = 10

# What the command says. Every line a person reads is in this block and
# nowhere below it, so the wording can change without touching a decision
# and a decision cannot quietly change what a line claims. A stored value
# reaches a line through !a and no other way.

HELP = "Print django-ox equivalents for the schedules in a django-celery-beat table."
DATABASE_HELP = "Database alias holding the django-celery-beat tables."
BEAT_TIMEZONE_HELP = (
    "Confirm the timezone the Celery app ran beat in, for example Europe/Berlin. This "
    "applies to every crontab when DJANGO_CELERY_BEAT_TZ_AWARE is False, and to "
    "crontab tables with no timezone column. It does not replace an empty timezone "
    "when DJANGO_CELERY_BEAT_TZ_AWARE is True. The command cannot verify your choice."
)

NO_TABLE = (
    "No {table} table on database {alias!r}. Point --database "
    "at the one holding your django-celery-beat schedules."
)
UNREACHABLE = "Database unreachable: {exc}"
BEAT_TIMEZONE_UNLOADABLE = (
    "--beat-timezone {zone!a} is not a timezone this Python can load. Check the value "
    "and this environment's timezone data."
)
ACCESS_ERROR = (
    "Cannot read beat schedules from database {alias!a}. Check database access."
)
STORED_VALUE_ERROR = (
    "Cannot read beat schedules from database {alias!a}. A stored value could not be "
    "converted. Nothing was printed."
)
BOUND_REFUSED = (
    "Cannot import beat schedules from database {alias!a}. The {column} of {name!a} "
    "cannot be imported as stored. Dropping it would allow runs outside the schedule's "
    "window. Nothing was printed. Fix the value, or clear it if the bound is no longer "
    "needed, then import again."
)
OUTPUT_UNREADABLE = (
    "The output generated from database {alias!a} failed the command's safety check. "
    "Nothing was printed. Report this as a defect in ox_import_beat_schedules."
)
NO_ROWS = "No periodic tasks found."

HEADER = (
    "# Translated from django-celery-beat for django-ox.\n"
    "# Run this command in beat's Python environment with beat's Django settings.\n"
    "# It uses that environment's timezone data, USE_TZ and\n"
    "# DJANGO_CELERY_BEAT_TZ_AWARE. With --beat-timezone, you confirm\n"
    "# the timezone the Celery app ran beat in.\n"
    "# The command cannot verify these. Check them before applying this output."
)
#: Said under the header where the installed beat's schedule
#: loading was not the one this command's analysis describes.
BEAT_VERSION_UNMEASURED = (
    "# django-celery-beat {version} is installed here. Its schedule loading was "
    "checked for 2.9.0 only, so whether it ran crontabs late or stopped on a bad row "
    "was not checked."
)
BEAT_NOT_INSTALLED = (
    "# django-celery-beat is not installed here, so its schedule loading was not "
    "checked: whether it ran crontabs late or stopped on a bad row is not known."
)
#: Said under the header where one row stopped beat for the
#: whole table. {ids} and {names} are literals, ten at most, and then
#: LIST_CUT.
SOURCE_STOPPED_ZONE = (
    "# beat ran nothing from this table: crontab rows {ids} have an empty timezone or "
    "one this Python cannot load. beat 2.9.0 raises on every schedule load while any "
    "such crontab exists, even if no task uses it. Applying the schedules below starts "
    "work beat was not running."
)
SOURCE_STOPPED_PROJECT_ZONE = (
    "# beat ran nothing from this table: with USE_TZ off, beat 2.9.0 raises on every "
    "schedule load because TIME_ZONE {project!a} cannot be loaded. Applying the "
    "schedules below starts work beat was not running."
)
SOURCE_STOPPED_ROWS = (
    "# beat ran nothing from this table while any of {names} was in its schedule: "
    "under these settings beat raises on every tick when it compares a listed row's "
    "start time or expiry with its clock. Applying the schedules below starts work "
    "beat was not running."
)
SOURCE_STOPPED_BUILD = (
    "# beat ran nothing from this table while any of {names} was in its schedule: beat "
    "2.9.0 raises while building a listed row's schedule, so it loads none. Applying "
    "the schedules below starts work beat was not running."
)
SOURCE_STOPPED_UNTIL = (
    "# beat ran nothing from this table until {until} UTC while any of {names} was in "
    "its schedule: under these settings beat raises on every tick while a listed row's "
    "start time is still ahead. Applying the schedules below starts work beat was not "
    "running."
)
BEAT_TIMEZONE_UNUSED = (
    "# --beat-timezone was not needed: every crontab row has its own timezone and "
    "DJANGO_CELERY_BEAT_TZ_AWARE is on."
)
SECTION_1 = "# 1. Expose these tasks. A row can only name a key you list."
SCHEDULE_SOURCE_NOTE = (
    '# Add "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource" to the same '
    "OPTIONS; without it no stored schedule ever runs."
)
SECTION_2 = "# 2. Create the schedules."
NOTHING_TRANSLATED = (
    "# No periodic tasks could be translated. The reasons follow.\n"
    "# There are no schedules to apply."
)
DIFFERENCES = "# Translated, with a difference from beat:"
HORIZON = (
    "# Clock changes were checked for ten years from this import in\n"
    "# {project!a}.\n"
    "# Within those ten years, a printed crontab with no clock-change notice\n"
    "# runs through skipped and repeated clock times at the times beat ran it.\n"
    "# Later years were not checked."
)
NOT_TRANSLATED = "# Not translated, and why:"
FOOTER = (
    "# Read before applying. Enabled states are preserved.\n"
    "# Schedules start when created. Enabling a disabled schedule resets\n"
    "# its start time.\n"
    "# Expiry bounds are imported. beat's exclusive expiry becomes an inclusive\n"
    "# end_time one microsecond earlier.\n"
    "# If an expiry passes before applying, creation may fail or leave a schedule\n"
    "# that never runs.\n"
    "# The rows were checked against known destination database constraints\n"
    "# when this command ran. Applying can still fail if a name has since been\n"
    "# taken, permissions or the schema have changed, or the database does not\n"
    "# answer. The call creates all schedules or none. If it raises, nothing\n"
    "# was created. Regenerate stale output before applying. After a successful\n"
    "# call, do not paste it again.\n"
    "# A queue or priority set on a beat task has no equivalent on a stored\n"
    "# schedule; set it on the task."
)

REPEATED_RUN = (
    "its next run in repeated clock time is on {date}. A stored schedule runs it on "
    "both passes. beat runs it on the first pass only."
)
REPEATED_ONCE = (
    "both beat and a stored schedule run a repeated clock time once with USE_TZ off. A "
    "stored schedule created now, during the second pass on {date}, still has that run "
    "ahead. beat ran it on the first pass."
)
SKIPPED_RUN = (
    "on {date} the clocks skip its run time, and a stored schedule runs it at another "
    "time, or another number of times, than beat did."
)
LOADED_LATE = (
    "beat's schedule loading leaves it out at its hour, first on {date}. It may still "
    "run on time if beat reloads soon enough. Any delay depends on when beat reloads. "
    "A stored schedule runs it at its intended hour and may not keep that delay."
)
INTERVAL_DIFFERS = (
    "a stored schedule counts this interval from a fixed instant. Celery counts it "
    "from the last run. Its run times may differ."
)

ONE_OFF = "one-off tasks have no equivalent on a stored schedule"
BACKEND_CLEANUP = (
    "celery.backend_cleanup is added by beat to clean Celery's result backend. It is "
    "not a project task to import. Use ox_prune to remove django-ox's finished rows."
)
NOT_TEXT = "its {column} is not text. Store a text value, then import again."
TWO_SCHEDULES = (
    "it has both a crontab and an interval. beat uses the interval, and a stored "
    "schedule has one trigger. Keep only the intended trigger, then import again."
)
SCHEDULE_ROW_MISSING = "its schedule row is missing"
NO_EQUIVALENT_KIND = "solar and clocked schedules have no equivalent"

AWARE_BOUND_BEAT_NAIVE = (
    "it has a start time or an expiry. With USE_TZ on and DJANGO_CELERY_BEAT_TZ_AWARE "
    "off, beat cannot compare that bound with the current time, so it cannot run this "
    "row."
)
CRONTAB_NOT_KEPT = (
    "USE_TZ is on and DJANGO_CELERY_BEAT_TZ_AWARE is off. TIME_ZONE {project!a} has "
    "timezone data that differs from UTC's. This configuration is supported only with "
    "UTC-identical data. beat stores UTC run times as local time, which shifts them "
    "wherever the UTC offset is nonzero."
)
INTERVAL_NOT_SUPPORTED = (
    "USE_TZ is on and DJANGO_CELERY_BEAT_TZ_AWARE is off. TIME_ZONE {project!a} has "
    "timezone data that differs from UTC's. Intervals are not supported for import "
    "from this configuration. beat was not run with one in the checks behind this "
    "command."
)
BEAT_STOPPED = (
    "USE_TZ is off and DJANGO_CELERY_BEAT_TZ_AWARE is on or unset. On SQLite, beat was "
    "observed to send the task, then stop with a ValueError when saving the run. It "
    "could not keep to the crontab."
)
BEAT_SAVE_REFUSED = (
    "USE_TZ is off and DJANGO_CELERY_BEAT_TZ_AWARE is on or unset. Django's {database} "
    "backend rejects the value beat saves after a run. This refusal was checked in the "
    "backend code. beat was not run on this backend in the checks behind this command."
)
BEAT_NOT_ESTABLISHED = (
    "USE_TZ is off and DJANGO_CELERY_BEAT_TZ_AWARE is on or unset. beat's crontab "
    "timing on {database} has not been established for this configuration, so the row "
    "is not imported."
)
INTERVAL_NOT_RUN = (
    "USE_TZ is off and DJANGO_CELERY_BEAT_TZ_AWARE is on or unset. beat was not run "
    "with an interval in this configuration in the checks behind this command. Its "
    "timing has not been established, so the row is not imported."
)

ZONE_FROM_ROW = "from the row"
ZONE_FROM_OPTION = "from --beat-timezone"
ZONE_IGNORED = (
    "DJANGO_CELERY_BEAT_TZ_AWARE is off, so beat ignored the row's timezone and used "
    "the Celery app's timezone. Pass --beat-timezone to confirm which timezone that "
    "was."
)
ZONE_MISSING = (
    "its crontab table has no timezone column. Pass --beat-timezone to confirm the "
    "timezone the Celery app ran beat in."
)
ZONE_EMPTY = (
    "its crontab's timezone is empty. beat 2.9.0 raises on every schedule load while "
    "any crontab has an empty timezone. Set the crontab's timezone, then import again. "
    "--beat-timezone cannot replace this empty value."
)
ZONE_COLUMN_UNLOADABLE = (
    "its crontab's timezone is {zone!a}, which this Python cannot load. beat 2.9.0 "
    "reads it when loading its schedule, even with DJANGO_CELERY_BEAT_TZ_AWARE off, "
    "and raises on every load. Fix the crontab's timezone, then import again."
)
ZONE_UNLOADABLE = (
    "its schedule runs in {zone!a} ({source}), which this Python cannot load. Check "
    "the timezone value and this environment's timezone data."
)
PROJECT_ZONE_UNLOADABLE = (
    "TIME_ZONE is {project!a}, which this Python cannot load. Check the setting and "
    "this environment's timezone data."
)
PROJECT_ZONE_COMPARISON_UNAVAILABLE = (
    "TIME_ZONE is {project!a}, which this Python can load, but its zone file or UTC's "
    "could not be found for comparison. Check this environment's TZPATH directories "
    "and tzdata package."
)
ZONE_DATA_MISSING = (
    "its schedule runs in {zone!a} ({source}). The timezone file for that zone or "
    "TIME_ZONE {project!a} could not be found, so their data could not be compared. "
    "Check this environment's timezone data."
)
ZONE_DIFFERS = (
    "its schedule runs in {zone!a} ({source}). A stored schedule uses TIME_ZONE "
    "{project!a}. The timezone files differ, so the command cannot treat them as the "
    "same zone."
)
FIELD_NOT_TEXT = (
    "its crontab's {column} is not text. Store a valid Celery crontab field as text, "
    "then import again."
)
FIELD_NOT_CELERY = (
    "its crontab's {column} is {text!a}, which Celery refuses. Fix the field, then "
    "import again."
)
BOTH_DAY_FIELDS = (
    "its day_of_month leaves out a date of a month it runs in, and its day_of_week "
    "leaves out a weekday. Celery requires both to match. A stored schedule runs when "
    "either matches."
)
CRON_UNUSABLE = (
    "its crontab would be written {cron!a}, which django-ox refuses: {problem!a}"
)
CRON_READS_DIFFERENTLY = (
    "its crontab would be written {cron!a}, which django-ox reads as different times."
)
CRON_NOT_WITHIN_LIMIT = (
    "the command found no verified crontab expression within the {limit} characters a "
    "stored schedule holds. The shortest verified expression it found has {length} "
    "characters. A shorter one may exist."
)

UNKNOWN_PERIOD = (
    "its interval period is {period!a}. It must be weeks, days, hours, minutes, "
    "seconds, milliseconds or microseconds. Other periods make beat raise."
)
EVERY_NOT_A_NUMBER = (
    "its interval's every value is {every!a}, which is not a number. Fix the value, "
    "then import again."
)
NON_FINITE_INTERVAL = "interval contains a non-finite number"
BELOW_ONE_SECOND = (
    "an interval of {every!a} {period!a} is below one "
    "second, which the dispatch loop cannot honour"
)
NOT_WHOLE_SECONDS = (
    "an interval of {every!a} {period!a} is not a whole number of seconds. A stored "
    "schedule counts in whole seconds."
)
INTERVAL_TOO_LONG = (
    "an interval of {every!a} {period!a} exceeds timedelta's limit of 999,999,999 "
    "days, so beat cannot build it. Fix the interval, then import again."
)

INVALID_JSON = (
    "its {column} contains invalid JSON. beat disables a row whose arguments it cannot "
    "decode, so it did not run this row."
)
NON_FINITE_ARGUMENT = "{column} contains a non-finite number"
TOO_DEEP_TO_DECODE = (
    "{column} is nested too deeply to decode. Reduce the nesting, then import again."
)
# What beat's own decoding makes of the
# arguments, where a stored schedule cannot carry it or beat never ran it.
ARGUMENT_TYPE_UNSUPPORTED = (
    "beat passes a non-JSON value of type {kind} in its {column}, decoded from a "
    "Celery type marker. A stored schedule's JSON arguments cannot carry that value."
)
ARGUMENT_TYPE_UNKNOWN = (
    "its {column} holds a Celery type marker {kind} that kombu here cannot decode. "
    "beat disables a row whose arguments it cannot decode, so it did not run this row."
)
ARGUMENT_MARKER_INVALID = (
    "its {column} holds a Celery {kind} type marker whose value kombu cannot decode. "
    "beat disables a row whose arguments it cannot decode, so it did not run this row."
)
ARGUMENT_NOT_LOADABLE = (
    "its {column} is stored as {kind}, which beat cannot decode: loading the schedule "
    "fails on this row."
)
ARGUMENT_MARKER_BREAKS_LOAD = (
    "its {column} holds a Celery {kind} type marker whose value kombu cannot use: "
    "loading the schedule fails on this row."
)
ARGUMENT_MARKER_UNCHECKED = (
    "its {column} holds a Celery type marker {kind}, and kombu is not installed here "
    "to tell what beat made of it."
)
POSITIONAL = (
    "it passes positional arguments, and a stored schedule takes "
    "keyword arguments only; rewrite the task signature or the row"
)

NAIVE_BOUND_BEAT_AWARE = (
    "it has a start time or an expiry. With USE_TZ off and DJANGO_CELERY_BEAT_TZ_AWARE "
    "on or unset, beat cannot compare that bound with the current time, so it cannot "
    "run this row."
)
OFFSET_BOUND = (
    "it has a start time or an expiry with an explicit UTC offset. With USE_TZ and "
    "DJANGO_CELERY_BEAT_TZ_AWARE off, beat cannot compare that bound with the current "
    "time, so it cannot run this row."
)
NAIVE_EXPIRY_NOT_UTC = (
    "it has an expiry. With USE_TZ and DJANGO_CELERY_BEAT_TZ_AWARE both off, beat "
    "compares it with UTC wall time. A stored schedule reads it in {project!a}, whose "
    "timezone data differs from UTC's."
)
START_AHEAD = (
    "its start time is still ahead. beat can run a task as soon as its start arrives. "
    "A stored schedule waits for its next tick. Import again after the start time."
)
EXPIRED = "it has expired"
TOO_SHORT = "its expiry is one microsecond away, too short for a stored schedule"
WOULD_BE_REFUSED = "create_schedules would refuse it: {problems}"
# What the database the schedules are
# written to refuses where a schedule's validation does not.
DESTINATION_REFUSES = (
    "database {alias} ({database}) cannot store its {column} as it is: {why}"
)
WHY_NUL = "it holds a NUL character, which PostgreSQL text cannot hold."
WHY_SURROGATE = "it holds a lone UTF-16 surrogate, which {database} JSON refuses."
WHY_TOO_DEEP = "it is nested more than {limit} levels deep, which MySQL JSON refuses."
WHY_BIG_INTEGER = (
    "it holds the integer {number}, outside the 64-bit range MySQL JSON keeps exactly; "
    "MySQL would store an approximate float."
)
WHY_EXPONENT = (
    "it holds the number {number}, which PostgreSQL JSON stores without its exponent "
    "and hands back as an integer."
)
NAME_TAKEN = (
    "database {alias} already has a schedule named {existing}, which equals this name "
    "under that column's comparison rules. Rename one of them, then import again."
)
#: {others} are literals, ten at most, and then LIST_CUT.
NAME_CLASH = (
    "its name equals the name of {others} under the {collation} collation of database "
    "{alias}, so create_schedules would refuse the batch. Rename all but one of them, "
    "then import again."
)
NAMES_UNCHECKED = (
    "# Database {alias} has no schedule table yet, so names already in use there were "
    "not checked."
)
# One limit for an interval, the one
# that refuses it, and what sets it.
INTERVAL_PAST_TICKS = (
    "its interval is {seconds} seconds. A stored schedule's interval can be at most "
    "{limit} seconds, the longest a worker can count ticks for."
)
INTERVAL_PAST_COLUMN = (
    "its interval is {seconds} seconds. The every_seconds column of database {alias} "
    "({database}) holds at most {limit}."
)
INTERVAL_PAST_VALIDATION = (
    "its interval is {seconds} seconds. create_schedules accepts at most {limit} in "
    "every_seconds: Django validates that field against the range of the default "
    "database ({database}), whichever database the schedules are on."
)
DESTINATION_UNREADABLE = (
    "Cannot read the schedule table of database {alias} to check the names in use "
    "there. Check database access."
)
CALL_UNREADABLE = (
    "its arguments could not be turned into a Python call that can be compiled. Check "
    "their nesting depth."
)

# What follows a quoted value that was
# cut to fit a diagnostic line.
EXCERPT_CUT = " [cut; {length} {unit} in all]"
#: What follows a list cut to its first ten, so that
#: the line is as long for a table of any size: {ids} or {names} in the
#: SOURCE_STOPPED lines, where more than ten crontabs or rows stopped beat,
#: and {others} in NAME_CLASH, where more than ten other rows share the
#: name. {more} is how many it does not name.
LIST_CUT = " and {more} more"

# What is printed as code rather than said. The whole of section 2 is one
# call, so that applying it creates every schedule or none. Its result is
# assigned: left bare, a shell echoes it, and with it every stored name as
# it is, escape sequences and carriage returns included.
SETTINGS_OPEN = '"SCHEDULABLE_TASKS": {'
SETTINGS_CLOSE = "},"
DATETIME_IMPORT = "from datetime import datetime"
STORED_IMPORT = "from django_ox.stored import create_schedules"
CALL_OPEN = "created = create_schedules(["
CALL_ROW = "    dict({row}),"
CALL_CLOSE = "])"

#: What compile() raises for a source it cannot take: text that is not
#: Python, text nested deeper than it parses, and a source too complex for
#: its parser, which is reported as a MemoryError.
_UNCOMPILABLE = (SyntaxError, RecursionError, MemoryError, ValueError)


class _Refused(Exception):
    """This row cannot be translated. The other rows still are."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _BoundRefused(Exception):
    """A row's start or expiry cannot be carried over, so nothing is imported."""

    def __init__(self, name: Any, column: str) -> None:
        super().__init__(column)
        self.name = name
        self.column = column


class _Unconvertible(Exception):
    """A stored value failed to convert while the rows of a query were read."""


class _DestinationUnreadable(Exception):
    """The database the schedules go to did not answer a question about it."""


class _Marker(Exception):
    """
    A Celery type marker in a row's arguments that kombu could not decode,
    with what it raised, or that nothing here can decode (error None).
    Not a ValueError, so json.loads lets it through as it is.
    """

    def __init__(self, kind: Any, error: BaseException | None) -> None:
        super().__init__(kind)
        self.kind = kind
        self.error = error


class Command(DatabaseCommand):
    help = HELP
    database_help = DATABASE_HELP

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "--beat-timezone", default=None, metavar="ZONE", help=BEAT_TIMEZONE_HELP
        )

    def default_database(self) -> str:
        # db_for_read, not db_for_write: this command reads a table
        # django-ox does not own and writes nothing anywhere. A replica that
        # is behind on somebody else's schedule table still prints the same
        # suggestion, so there is nothing here to pin to the primary.
        return router.db_for_read(OxSchedule)

    def handle(self, *args: Any, **options: Any) -> None:
        alias = self.database(options)
        # Before anything is read: a zone the operator mistyped would
        # otherwise surface as a refusal on every row it applies to.
        beat_timezone = options.get("beat_timezone")
        if beat_timezone is not None and not _loadable(beat_timezone):
            raise CommandError(
                BEAT_TIMEZONE_UNLOADABLE.format(zone=_Excerpt(beat_timezone))
            )
        connection = connections[alias]
        # Reading someone else's table starts by asking which tables are
        # there, so this is where a database that is not there is found.
        # One line, the way the missing-table case below is: this command
        # prints code for a person to read, and a driver traceback in the
        # middle of that is nothing they can act on.
        try:
            tables = connection.introspection.table_names()
        except DatabaseError as exc:
            raise CommandError(UNREACHABLE.format(exc=exc)) from exc
        if BEAT_TABLE not in tables:
            raise CommandError(NO_TABLE.format(table=BEAT_TABLE, alias=alias))

        # Every row is read and converted before a line is printed, and
        # three things can stop that, each with a line of its own. The
        # database refuses the introspection or a query. A stored value the
        # driver cannot convert, such as PostgreSQL's 'infinity', an
        # impossible date kept as text on SQLite or text SQLite cannot
        # decode, fails while the cursor is iterated, where no single row
        # can be set aside. Or a row's start or expiry cannot be carried
        # over as it is, which is found row by row after the fetch so the
        # line can name the row. None of them prints a traceback or half of
        # what the command would have printed, and none drops the bound.
        try:
            rows = self._read(connection)
        except _BoundRefused as refused:
            raise CommandError(
                BOUND_REFUSED.format(
                    alias=alias, column=refused.column, name=_Excerpt(refused.name)
                )
            ) from refused.__cause__
        except _Unconvertible as exc:
            raise CommandError(
                STORED_VALUE_ERROR.format(alias=alias)
            ) from exc.__cause__
        except (DataError, ValueError, OverflowError) as exc:
            # Wherever it was raised. MySQL's drivers convert while the
            # query runs rather than while its rows are read, and no driver
            # reports a database it cannot reach with one of these.
            raise CommandError(STORED_VALUE_ERROR.format(alias=alias)) from exc
        except DatabaseError as exc:
            raise CommandError(ACCESS_ERROR.format(alias=alias)) from exc
        if not rows:
            self.stdout.write(NO_ROWS)
            return
        # Every text value a reason can quote, wrapped so that the quote is
        # bounded however long or strange the value is.
        for row in rows:
            for column in ("name", "task"):
                row[column] = _source(row[column])
            for column in ("_cron", "_interval"):
                if row[column]:
                    row[column] = tuple(_source(value) for value in row[column])

        # One reading of the clock for the whole import, so whether a row
        # is translated, the reason it is not, and the bounds its call
        # carries are all decided against the same instant. Taken as that
        # instant: without USE_TZ timezone.now() is the wall clock of this
        # process's zone, which is TIME_ZONE only where Django set it (not
        # under settings.configure(), not on Windows), and only that zone
        # turns it back into the instant it was read at. beat's own clock
        # under those settings is naive UTC, so that is the form a naive
        # bound is held against.
        instant = timezone.now().astimezone(UTC)
        now = instant if settings.USE_TZ else instant.replace(tzinfo=None)
        version = _beat_version()
        zones = _Zones(
            beat_timezone, measured=version in _beat_timing.MEASURED_BEAT_VERSIONS
        )
        # Where the printed rows will be written, resolved once: the same
        # alias _export_preflight validates against.
        self._destination = _Destination()
        printed = []
        skipped = []
        try:
            for row in rows:
                try:
                    printed.append(self._row(row, now, zones, connection))
                except _Refused as refused:
                    skipped.append((row["name"], refused.reason))
        except _BoundRefused as refused:
            # A live expiry that cannot be carried over, found only once the
            # row is known not to have expired. Nothing is printed yet.
            raise CommandError(
                BOUND_REFUSED.format(
                    alias=alias, column=refused.column, name=refused.name
                )
            ) from refused.__cause__
        # Names, once every row has its own answer: two printed rows the
        # destination calls one name, or a name it already holds, would make
        # create_schedules refuse the batch. Each of them is listed.
        if printed:
            try:
                clashes = self._destination.clashes(
                    [fields["name"] for fields, _, _ in printed]
                )
            except _DestinationUnreadable as exc:
                raise CommandError(
                    DESTINATION_UNREADABLE.format(alias=self._destination.quoted_alias)
                ) from exc.__cause__
            for index in sorted(clashes):
                skipped.append((printed[index][0]["name"], clashes[index]))
            printed = [
                entry for index, entry in enumerate(printed) if index not in clashes
            ]
        # What a printed row will do that it did not do under beat. Said
        # row by row and apart from the rows that were not translated:
        # these are, and each one's difference is the reader's to accept.
        differing = [
            (fields["name"], difference)
            for fields, _, difference in printed
            if difference is not None
        ]

        settings_fragment: list[str] = []
        code: list[str] = []
        if printed:
            # From the rows that are printed and no others: a key listed
            # here is a task anyone with the admin can schedule, and a row
            # that was not translated has no schedule to justify it.
            # Literals through !a, like every stored value below: this
            # fragment is pasted into settings.
            paths = sorted({fields["task_key"] for fields, _, _ in printed})
            settings_fragment = [
                SETTINGS_OPEN,
                *(f"    {_plain(path)!a}: {_plain(path)!a}," for path in paths),
                SETTINGS_CLOSE,
                SCHEDULE_SOURCE_NOTE,
            ]
            code.append(SECTION_2)
            if any("end_time" in fields for fields, _, _ in printed):
                code.append(DATETIME_IMPORT)
            code.append(STORED_IMPORT)
            code.append("")
            code.append(CALL_OPEN)
            code.extend(CALL_ROW.format(row=written) for _, written, _ in printed)
            code.append(CALL_CLOSE)
        crontabs = any(fields["trigger"] == "cron" for fields, _, _ in printed)
        if differing or crontabs:
            code.append("")
            if differing:
                code.append(DIFFERENCES)
                code.extend(
                    f"#   {_excerpt(name)}: {difference}"
                    for name, difference in differing
                )
            # Whenever a crontab is printed, named above or not: a crontab
            # with no notice is clear for the years that were looked at,
            # and the reader is told how many that was.
            if crontabs:
                code.append(HORIZON.format(project=settings.TIME_ZONE))
        if skipped:
            code.append("")
            code.append(NOT_TRANSLATED)
            # A literal even inside a comment: a line break in a stored
            # name would end the comment and paste the rest as code.
            code.extend(f"#   {_excerpt(name)}: {reason}" for name, reason in skipped)
        if printed:
            code.append("")
            code.append(FOOTER)

        # Both pasted pieces are compiled whole before a line of either is
        # printed: the settings fragment as the dictionary entry it is, and
        # everything from section 2 on as the module it is run as. Each row
        # was compiled on its own already, so this has nothing left to
        # find; it is here so that output Python cannot read is never what
        # a person is handed, whatever went wrong in producing it.
        #
        # A reason or a difference is held to one printable ASCII line for
        # the same cause. Every stored value in one went through !a, so it
        # is one; but a line break in it would end its comment and paste
        # the rest as code, and that still compiles.
        try:
            compile("{" + "\n".join(settings_fragment) + "\n}", "<generated>", "eval")
            compile("\n".join(code) + "\n", "<generated>", "exec")
        except _UNCOMPILABLE as exc:
            raise CommandError(OUTPUT_UNREADABLE.format(alias=alias)) from exc
        if not all(
            said.isascii() and said.isprintable() for _, said in [*skipped, *differing]
        ):
            raise CommandError(OUTPUT_UNREADABLE.format(alias=alias))

        lines = [HEADER]
        # The option names the zone of the crontabs beat ran in the Celery
        # app's zone: all of them with DJANGO_CELERY_BEAT_TZ_AWARE off, and
        # those of a table from before a crontab had a zone. Where every
        # crontab has a zone of its own and beat used it, the option
        # changed nothing, and an option that does nothing is said to have
        # done nothing rather than passed over. Said of crontabs that were
        # read: of a table with none, "every crontab row" says nothing.
        if (
            beat_timezone is not None
            and _beat_is_tz_aware()
            and any(row["_cron"] for row in rows)
            and all(_Zones.own(row["_cron"][-1]) for row in rows if row["_cron"])
        ):
            lines.append(BEAT_TIMEZONE_UNUSED)
        # What this command knows of beat's schedule loading is what one
        # version does, and a different one is said not to have been looked
        # at rather than taken to behave the same.
        if version is None:
            lines.append(BEAT_NOT_INSTALLED)
        elif not zones.measured:
            lines.append(BEAT_VERSION_UNMEASURED.format(version=ascii(version)))
        else:
            lines.extend(self._stoppages(rows, now))
        if printed and not self._destination.names_checked:
            lines.append(NAMES_UNCHECKED.format(alias=self._destination.quoted_alias))
        lines.append("")
        if printed:
            lines += [SECTION_1, *settings_fragment, "", *code]
        else:
            lines += [NOTHING_TRANSLATED, *code]
        for line in lines:
            self.stdout.write(line)

    def _read(self, connection: Any) -> list[dict[str, Any]]:
        tables = connection.introspection.table_names()
        with connection.cursor() as cursor:
            beat_columns = {
                c.name
                for c in connection.introspection.get_table_description(
                    cursor, BEAT_TABLE
                )
            }
            # expires is in django-celery-beat's first migration, one_off and
            # start_time arrived in its 0007. A table older than that reads
            # them as NULL rather than failing on a column it never had.
            one_off_col = "one_off" if "one_off" in beat_columns else "NULL AS one_off"
            start_time_col = (
                "start_time" if "start_time" in beat_columns else "NULL AS start_time"
            )
            expires_col = "expires" if "expires" in beat_columns else "NULL AS expires"
            # Whether each date bound is NULL, asked of the database rather
            # than read off the decoded value: a driver can decode a stored
            # date it cannot represent as None, as mysqlclient does with
            # MySQL's zero date and Django's SQLite converter with text it
            # cannot parse, and that must not read as a row without a bound.
            nulls = ", ".join(
                f"CASE WHEN {name} IS NULL THEN 1 ELSE 0 END AS {name}_is_null"
                if name in beat_columns
                else f"1 AS {name}_is_null"
                for name in ("start_time", "expires")
            )

            columns, fetched = self._select(
                cursor,
                f"SELECT name, task, args, kwargs, queue, enabled, "  # noqa: S608
                f"crontab_id, interval_id, {one_off_col}, {start_time_col}, "
                f"{expires_col}, {nulls} FROM {BEAT_TABLE}",
            )
            rows = [dict(zip(columns, values, strict=True)) for values in fetched]

            crontabs: dict[Any, Any] = {}
            if CRONTAB_TABLE in tables:
                # django-celery-beat has carried a per-schedule timezone
                # since 2018, but an older table will not have the column
                # and reading it would be a crash rather than a migration.
                crontab_columns = {
                    c.name
                    for c in connection.introspection.get_table_description(
                        cursor, CRONTAB_TABLE
                    )
                }
                zoned = "timezone" in crontab_columns
                zone = "timezone" if zoned else "NULL"
                _, fetched = self._select(
                    cursor,
                    f"SELECT id, minute, hour, day_of_month, month_of_year, "  # noqa: S608
                    f"day_of_week, {zone} FROM {CRONTAB_TABLE}",
                )
                crontabs = {
                    r[0]: (*r[1:6], r[6] if zoned else _NO_ZONE_COLUMN) for r in fetched
                }

            intervals: dict[Any, Any] = {}
            if INTERVAL_TABLE in tables:
                _, fetched = self._select(
                    cursor,
                    f"SELECT id, every, period FROM {INTERVAL_TABLE}",  # noqa: S608
                )
                intervals = {r[0]: (r[1], r[2]) for r in fetched}

        for row in rows:
            row["_cron"] = crontabs.get(row["crontab_id"])
            row["_interval"] = intervals.get(row["interval_id"])
            row["_one_off"] = bool(row.get("one_off"))
            # After the fetch, a row at a time, so a bound that cannot be
            # carried over is reported against the row it is on.
            for column in ("start_time", "expires"):
                try:
                    row[f"_{column}"] = self._bound(row, column, connection)
                except (ValueError, OverflowError) as exc:
                    raise _BoundRefused(row["name"], column) from exc
        # Every crontab's zone, the ones no row uses included: beat 2.9.0
        # loads them all to filter its schedule, and one that does not load
        # stops it for the whole table.
        self._crontab_zones = {
            key: crontab[-1]
            for key, crontab in crontabs.items()
            if crontab[-1] is not _NO_ZONE_COLUMN
        }
        return rows

    @staticmethod
    def _select(cursor: Any, sql: str) -> tuple[list[str], list[Any]]:
        """
        Run one query and read all of its rows: the column names, and them.

        The two halves fail for different reasons. A statement the
        database refuses is a question of access. Once it has run, what
        fails while its rows are read is a stored value the driver cannot
        hand over, whatever class the driver raises for it: SQLite reports
        text it cannot decode as an OperationalError.
        """
        cursor.execute(sql)
        columns = [c[0] for c in cursor.description]
        try:
            return columns, list(cursor)
        except (DatabaseError, ValueError, OverflowError) as exc:
            raise _Unconvertible from exc

    def _bound(
        self, row: dict[str, Any], name: str, connection: Any
    ) -> datetime | None:
        """
        A row's start or expiry, which is None only where the database
        holds NULL. A stored value read as no date at all is not a missing
        bound, and dropping it would run the schedule outside its window.
        """
        value = self._parse_datetime(row[name], connection)
        if value is None and not row[f"{name}_is_null"]:
            raise ValueError(f"{name} is not NULL but was read as no date")
        return value

    @staticmethod
    def _parse_datetime(val: Any, connection: Any) -> datetime | None:
        """
        A stored start or expiry, in the form the rest of the command uses.

        With USE_TZ, Django writes a datetime to SQLite or MySQL without its
        zone, in the zone of the connection: DATABASES TIME_ZONE when set,
        UTC otherwise. So a naive value is read in the zone of the
        connection it came from, the one --database names, not in UTC or in
        TIME_ZONE. PostgreSQL returns aware values, which pass as they are.
        Without USE_TZ Django keeps naive local time, and that passes as it
        is too. A value that comes back with an offset there was stored with
        one by something other than Django. It keeps it: beat read it with
        the offset as well, and the row is listed for that (_settings), not
        read as a local time nobody stored.
        """
        if val is None:
            return None
        if isinstance(val, str):
            val = datetime.fromisoformat(val)
        if isinstance(val, datetime):
            if settings.USE_TZ and timezone.is_naive(val):
                return timezone.make_aware(val, connection.timezone)
            return val
        return None

    @staticmethod
    def _end_time(expires: datetime | None) -> datetime | None:
        """
        The last instant a stored schedule may fire at, for a beat expiry.

        Celery stops at its expiry: a tick that falls exactly on it does not
        run. A stored schedule's end_time still fires a tick that falls on
        it, so the bound moves back by the smallest step a datetime holds.
        The step is taken on the UTC instant rather than on the wall clock,
        where a zone's clock change can skip or repeat an hour: a microsecond
        before 03:00 on a day that skips from 02:00 is 01:59:59 and a
        fraction, while 02:59:59 never happens and PostgreSQL would store it
        an hour later. A naive expiry has no instant to step on, and needs
        none: it is carried over only where local time is UTC (_end), whose
        wall clock neither skips nor repeats.
        """
        if expires is None:
            return None
        step = timedelta(microseconds=1)
        if timezone.is_aware(expires):
            return (expires.astimezone(UTC) - step).astimezone(expires.tzinfo)
        return expires - step

    def _row(
        self, row: dict[str, Any], now: datetime, zones: _Zones, connection: Any
    ) -> tuple[dict[str, Any], str, str | None]:
        """
        What creates a row's schedule, as the fields and as the text printed
        for them, with what the schedule will do differently from beat if
        anything; or _Refused with why nothing creates it.

        The one place that decides, in one order, so a row is never printed
        as a call and listed as skipped, and the reason listed is the first
        check that failed.
        """
        if row["_one_off"]:
            raise _Refused(ONE_OFF)
        if row["task"] == CELERY_CLEANUP_TASK:
            raise _Refused(BACKEND_CLEANUP)
        # A NULL or a BLOB where beat keeps text. Its own columns hold
        # neither, and a task that is not text cannot be listed in section 1
        # beside ones that are.
        for column in ("name", "task"):
            if not isinstance(row[column], str):
                raise _Refused(NOT_TEXT.format(column=column))
        fields: dict[str, Any] = {"name": row["name"], "task_key": row["task"]}
        # beat reads a row with both as its interval and this command read
        # it as its crontab. beat's own validation refuses the row, so there
        # is no behaviour to carry over that anyone chose.
        if row["crontab_id"] is not None and row["interval_id"] is not None:
            raise _Refused(TWO_SCHEDULES)
        if row["_cron"]:
            fields["trigger"] = "cron"
        elif row["_interval"]:
            fields["trigger"] = "interval"
        elif row["crontab_id"] is not None or row["interval_id"] is not None:
            raise _Refused(SCHEDULE_ROW_MISSING)
        else:
            raise _Refused(NO_EQUIVALENT_KIND)
        self._settings(row, fields["trigger"], zones, connection)
        difference: str | None
        if fields["trigger"] == "cron":
            fields["cron"], difference = self._cron(row["_cron"], now, zones)
        else:
            fields["every_seconds"] = self._every_seconds(*row["_interval"])
            difference = INTERVAL_DIFFERS
        arguments = self._arguments(row)
        if arguments:
            fields["arguments"] = arguments
        end_time = self._end(row, now, zones)
        if end_time is not None:
            fields["end_time"] = end_time
        if not row["enabled"]:
            fields["enabled"] = False
        # What create_schedules will hold the row to, asked now: a name or
        # a task longer than a stored schedule's columns, an expression
        # past the length of its cron column, an interval the destination
        # database has no column for. Against the import's own reading of
        # the clock as the start, so this too is decided at that instant.
        # Arguments nested past what Python's own JSON encoder walks cannot
        # be validated at all, and the call below would not compile either.
        try:
            problems = _export_preflight({**fields, "start_time": now})
        except RecursionError:
            raise _Refused(CALL_UNREADABLE) from None
        # An interval past what the row can be written with is said once,
        # with the one limit that refuses it. Validation reports the
        # default database's range and the destination's alike, and two
        # numbers for one value are two explanations where one applies.
        if fields["trigger"] == "interval" and any(
            field == "every_seconds" for field, _ in problems
        ):
            reason = self._destination.interval_reason(fields["every_seconds"])
            if reason is not None:
                raise _Refused(reason)
        if problems:
            raise _Refused(
                WOULD_BE_REFUSED.format(
                    problems="; ".join(
                        f"{field}: {_excerpt(message)}" if field else _excerpt(message)
                        for field, message in problems
                    )
                )
            )
        # Printed text is only worth printing if Python reads it back.
        # Arguments nested a few hundred deep decode and print, and then
        # the call holding them is refused by the compiler; deeper ones
        # cannot be printed at all. Compiled inside the call it will sit
        # in, whose own brackets count towards the nesting.
        try:
            written = self._written(fields)
            compile(
                "\n".join([CALL_OPEN, CALL_ROW.format(row=written), CALL_CLOSE]),
                "<generated>",
                "exec",
            )
        except _UNCOMPILABLE:
            raise _Refused(CALL_UNREADABLE) from None
        # And what the database it goes to would refuse, or keep as
        # something else, of a row that validates and prints. After the
        # compiler, whose refusal holds wherever the output is applied.
        refusal = self._destination.refusal(fields)
        if refusal is not None:
            raise _Refused(refusal)
        return fields, written, difference

    @staticmethod
    def _settings(
        row: dict[str, Any], trigger: str, zones: _Zones, connection: Any
    ) -> None:
        """
        _Refused when beat itself did not keep to a row's schedule: for the
        two pairs of settings in which USE_TZ and beat's own
        DJANGO_CELERY_BEAT_TZ_AWARE disagree, and for a bound stored with
        an offset where both are off.

        Both pairs were run, on SQLite, with crontab rows. Where a reason
        speaks of anything else, another database or an interval, it says
        that it was not run.
        """
        use_tz = bool(settings.USE_TZ)
        bounds = [
            bound
            for bound in (row["_start_time"], row["_expires"])
            if bound is not None
        ]
        if use_tz == _beat_is_tz_aware():
            # With both off beat reads the clock as naive UTC, and a bound
            # that came back with an offset cannot be compared with it:
            # beat's check of the row raises, for a start as for an expiry,
            # past or ahead. The bound is not turned into a local time to
            # get round that. With both on every bound has a zone.
            if not use_tz and any(timezone.is_aware(bound) for bound in bounds):
                raise _Refused(OFFSET_BOUND)
            return
        if use_tz:
            # beat reads the clock as naive UTC, the bound is aware, and
            # comparing them raises on every tick, before the bound and
            # after it, in UTC as anywhere.
            if bounds:
                raise _Refused(AWARE_BOUND_BEAT_NAIVE)
            # beat writes each run as naive UTC and Django stores a naive
            # value as local time, so every run is read back off by the
            # offset: east of UTC the row fires again at each resync, west
            # of it ticks are dropped. Only where local time is UTC is the
            # crontab kept. An interval is refused on the same boundary
            # without having been run.
            if not zones.project_is_utc():
                unkept = (
                    CRONTAB_NOT_KEPT if trigger == "cron" else INTERVAL_NOT_SUPPORTED
                )
                raise _Refused(unkept.format(project=settings.TIME_ZONE))
            return
        # beat reads the clock as an aware time and the bound is naive, so
        # the same comparison raises, whatever TIME_ZONE is. A bound stored
        # with an offset is one beat can compare, so it is not the reason
        # given: the row goes on to what this configuration did to every
        # row.
        if any(timezone.is_naive(bound) for bound in bounds):
            raise _Refused(NAIVE_BOUND_BEAT_AWARE)
        if trigger != "cron":
            raise _Refused(INTERVAL_NOT_RUN)
        # After a row's first run beat saves an aware time, which Django's
        # SQLite, MySQL and Oracle backends refuse with USE_TZ off. On
        # SQLite that was seen to end beat, every time. The other two carry
        # the same refusal and were not run. PostgreSQL's takes the value,
        # and what beat then fires there has not been established.
        vendor = connection.vendor
        if vendor == "sqlite":
            raise _Refused(BEAT_STOPPED)
        database = {"mysql": "MySQL", "oracle": "Oracle"}.get(vendor)
        if database is not None:
            raise _Refused(BEAT_SAVE_REFUSED.format(database=database))
        database = "PostgreSQL" if vendor == "postgresql" else ascii(vendor)
        raise _Refused(BEAT_NOT_ESTABLISHED.format(database=database))

    @staticmethod
    def _written(fields: dict[str, Any]) -> str:
        """A row's fields as the keyword arguments of its printed call."""
        # Escape database-derived text with ascii() and reject non-finite numbers
        # before emitting supported values as Python literals.
        written = [
            f"name={_plain(fields['name'])!a}",
            f"task_key={_plain(fields['task_key'])!a}",
        ]
        if fields["trigger"] == "cron":
            written.append(f'trigger="cron", cron={fields["cron"]!a}')
        else:
            written.append(
                f'trigger="interval", every_seconds={fields["every_seconds"]}'
            )
        if "arguments" in fields:
            written.append(f"arguments={fields['arguments']!a}")
        if "end_time" in fields:
            end_time = fields["end_time"].isoformat()
            written.append(f"end_time=datetime.fromisoformat({end_time!a})")
        if "enabled" in fields:
            written.append("enabled=False")
        return ", ".join(written)

    def _cron(
        self, stored: tuple[Any, ...], now: datetime, zones: _Zones
    ) -> tuple[str, str | None]:
        """
        The cron expression a stored schedule needs to fire when this
        crontab fired, and how it will differ from beat all the same, if it
        will; or _Refused when no expression does.

        Written from the values Celery fires on, not from the stored
        strings. The two parsers disagree on strings both accept: a field
        that covers its whole range, "1-31" or "*/1", restricts nothing to
        Celery and counts as restricted here, where two restricted day
        fields mean either rather than both.
        """
        *texts, zone = stored
        zone_key = zones.matching(zone)
        fields = []
        for text, field in zip(texts, _beat_cron.FIELDS, strict=True):
            if not isinstance(text, str):
                raise _Refused(FIELD_NOT_TEXT.format(column=field.column))
            try:
                fields.append(_beat_cron.celery_values(text, field))
            except _beat_cron.NotCelery:
                raise _Refused(
                    FIELD_NOT_CELERY.format(column=field.column, text=text)
                ) from None
        # A day of the month that leaves out no date of the months the row
        # runs in decides nothing, so it does not count as narrowing.
        fields = list(_beat_cron.without_redundant_days(tuple(fields)))
        if _beat_cron.narrows_both_day_fields(tuple(fields)):
            raise _Refused(BOTH_DAY_FIELDS)
        cron = _beat_cron.shortest(tuple(fields), CRON_LENGTH)
        # The expression is checked by the parser that will run it, not by
        # the code that wrote it: the same sets, and at most one day field
        # it counts as restricted. A crontab no date satisfies, the 31st
        # of February, is refused by that parser outright. Checked whatever
        # its length, so one too long to store is still known to be right
        # when the reason gives its length.
        try:
            parsed = CronExpression(cron)
        except ValueError as exc:
            raise _Refused(CRON_UNUSABLE.format(cron=cron, problem=str(exc))) from None
        read_back = (
            parsed.minutes,
            parsed.hours,
            parsed.days_of_month,
            parsed.months,
            parsed.days_of_week,
        )
        if [frozenset(values) for values in read_back] != fields or (
            parsed._dom_restricted and parsed._dow_restricted
        ):
            raise _Refused(CRON_READS_DIFFERENTLY.format(cron=cron))
        if len(cron) > CRON_LENGTH:
            raise _Refused(
                CRON_NOT_WITHIN_LIMIT.format(limit=CRON_LENGTH, length=len(cron))
            )
        # Where the clocks change, and where beat's own schedule loading
        # held a run back, the two engines run some runs at other instants.
        # Each is by design here, and none is a reason to refuse the row:
        # it is translated, and the reader is told how and from which day.
        instant = now if timezone.is_aware(now) else now.replace(tzinfo=UTC)
        horizon = _beat_timing.years_on(instant, HORIZON_YEARS)
        parted = _beat_timing.partings(
            parsed, zoneinfo.ZoneInfo(zone_key), instant, horizon, aware=settings.USE_TZ
        )
        notices = []
        if parted.repeated is not None:
            repeated = REPEATED_RUN if settings.USE_TZ else REPEATED_ONCE
            notices.append(repeated.format(date=parted.repeated.isoformat()))
        if parted.skipped is not None:
            notices.append(SKIPPED_RUN.format(date=parted.skipped.isoformat()))
        # beat's hour filter reads the crontab's timezone column whatever
        # DJANGO_CELERY_BEAT_TZ_AWARE says. A table with no such column was
        # not loaded by the version the filter is known in.
        if zones.measured and _Zones.own(zone):
            late = _beat_timing.loaded_late(
                texts[1],
                zone,
                "UTC" if settings.USE_TZ else settings.TIME_ZONE,
                zone_key,
                tuple(fields),
                instant,
                horizon,
            )
            if late is not None:
                notices.append(LOADED_LATE.format(date=late.isoformat()))
        return cron, " ".join(notices) or None

    @staticmethod
    def _every_seconds(every: Any, period: Any) -> int:
        """
        A beat interval in whole seconds, or _Refused when it is not one.

        By beat's own arithmetic. beat hands the row to timedelta as
        timedelta(**{period: every}) and runs on what comes back, so the
        same is built here and the interval is what timedelta made of it.
        For a whole number that is exact. A fraction, which only SQLite
        keeps in the column, is rounded to the microsecond: a tenth of a
        day ran every 8640 seconds under beat, and is imported as that.
        """
        if not isinstance(period, str) or period not in PERIODS:
            raise _Refused(UNKNOWN_PERIOD.format(period=period))
        # SQLite keeps whatever it is given in an integer column, text
        # included. Reject booleans even though Python treats bool as a
        # subclass of int.
        if isinstance(every, bool) or not isinstance(every, int | float):
            raise _Refused(EVERY_NOT_A_NUMBER.format(every=every))
        if isinstance(every, float) and not math.isfinite(every):
            raise _Refused(NON_FINITE_INTERVAL)
        # Past 999,999,999 days timedelta raises, in beat when it builds
        # the row's schedule and in the worker here when it builds the
        # stored one. SQLite's column would hold the number all the same.
        # That far below zero it raises too, and is short, not long.
        try:
            length = timedelta(**{period: every})
        except OverflowError:
            reason = INTERVAL_TOO_LONG if every > 0 else BELOW_ONE_SECOND
            raise _Refused(reason.format(every=every, period=period)) from None
        if length < _ONE_SECOND:
            raise _Refused(BELOW_ONE_SECOND.format(every=every, period=period))
        # Off the timedelta's own whole numbers. Its total_seconds() is a
        # float, which past a few centuries no longer holds a microsecond
        # and would pass an interval that has one as whole.
        if length.microseconds:
            raise _Refused(NOT_WHOLE_SECONDS.format(every=every, period=period))
        return length.days * 86_400 + length.seconds

    @staticmethod
    def _decode(raw: Any, column: str) -> Any:
        """
        A row's args or kwargs as beat decodes them, or _Refused.

        beat's ModelEntry runs `loads(model.args or '[]')` and
        `loads(model.kwargs or '{}')` with kombu's loads, and catches a
        ValueError by disabling the row. So the empty values are beat's
        defaults; text and bytes alike are UTF-8 JSON; and an object that is
        exactly {"__type__": ..., "__value__": ...} is a Celery type marker,
        which kombu turns into the type registered under that name (a
        datetime, a Decimal, bytes) or refuses. The decoding here is kombu's
        own, so a marker is read as beat read it rather than as the plain
        dictionary its JSON spells.

        A marker that decodes is listed: what beat passed was not JSON, and
        a stored schedule's arguments are. One kombu cannot decode is listed
        as a row beat disabled. Anything else beat's own load fails on is a
        row beat could not load at all.
        """
        kombu = _kombu_json()
        source = raw if raw else _EMPTY_ARGUMENTS[column]
        decoded: list[Any] = []

        def hook(obj: dict[str, Any]) -> Any:
            if obj.keys() != {"__type__", "__value__"}:
                return obj
            if kombu is None:
                raise _Marker(obj["__type__"], None)
            try:
                value = kombu.object_hook(obj)
            except Exception as exc:
                raise _Marker(obj["__type__"], exc) from exc
            decoded.append(value)
            return value

        try:
            if kombu is None:
                value = _loads_without_kombu(source, object_hook=hook)
            elif kombu is _OLD_KOMBU:
                # Before type markers, kombu read one as the plain object it
                # spells, and so does this.
                value = _old_kombu_loads(source)
            else:
                value = kombu.loads(source, object_hook=hook)
        except _Marker as marker:
            kind = _excerpt(marker.kind)
            if marker.error is None:
                raise _Refused(
                    ARGUMENT_MARKER_UNCHECKED.format(column=column, kind=kind)
                ) from None
            if not isinstance(marker.error, ValueError):
                raise _Refused(
                    ARGUMENT_MARKER_BREAKS_LOAD.format(column=column, kind=kind)
                ) from None
            if marker.error.args[:1] == ("Unsupported type",):
                raise _Refused(
                    ARGUMENT_TYPE_UNKNOWN.format(column=column, kind=kind)
                ) from None
            raise _Refused(
                ARGUMENT_MARKER_INVALID.format(column=column, kind=kind)
            ) from None
        except RecursionError:
            raise _Refused(TOO_DEEP_TO_DECODE.format(column=column)) from None
        except ValueError:
            # Not JSON, not UTF-8, or an integer longer than Python converts
            # from text: beat disables the row on each of these.
            raise _Refused(INVALID_JSON.format(column=column)) from None
        except TypeError:
            # Neither text nor bytes, which kombu hands to json.loads as it
            # is. beat's load raises, which is not the ValueError it catches.
            raise _Refused(
                ARGUMENT_NOT_LOADABLE.format(column=column, kind=type(raw).__name__)
            ) from None
        # A marker kombu decoded into something JSON holds, a type a project
        # registered that way, is passed by beat as that, and is carried.
        for result in decoded:
            unsupported = _not_json(result)
            if unsupported is not None:
                raise _Refused(
                    ARGUMENT_TYPE_UNSUPPORTED.format(column=column, kind=unsupported)
                )
        return value

    def _arguments(self, row: dict[str, Any]) -> dict[str, Any]:
        """The keyword arguments a row's call carries, or _Refused."""
        decoded = {}
        for column in ("args", "kwargs"):
            decoded[column] = self._decode(row.get(column), column)
            if _non_finite(decoded[column]):
                raise _Refused(NON_FINITE_ARGUMENT.format(column=column))
        # A stored schedule passes keyword arguments only. Asked of what beat
        # decoded, not of how it was stored: bytes holding [] are no
        # positional arguments, as the text [] is none.
        if decoded["args"]:
            raise _Refused(POSITIONAL)
        kwargs = decoded["kwargs"]
        return kwargs if isinstance(kwargs, dict) else {}

    def _end(
        self, row: dict[str, Any], now: datetime, zones: _Zones
    ) -> datetime | None:
        """
        The end_time a row's call carries, or _Refused when its start or
        expiry cannot mean to a stored schedule what it meant to beat.

        No start is ever carried. One still ahead is refused, because beat
        runs the task the moment its start arrives and a stored schedule
        waits for its next tick; one already past adds nothing, because a
        stored schedule starts when it is created.
        """
        start, expires = row["_start_time"], row["_expires"]
        if start is None and expires is None:
            return None
        # With USE_TZ off, and beat's own setting off with it (_settings has
        # refused the row otherwise), beat reads the clock as naive UTC and
        # compares the stored bound with that; a stored schedule reads the
        # same bound as local time. Only where local time is UTC are those
        # one instant.
        if not settings.USE_TZ and expires is not None and not zones.project_is_utc():
            raise _Refused(NAIVE_EXPIRY_NOT_UTC.format(project=settings.TIME_ZONE))
        # Ahead as beat read it, before the start is let go as past. now is
        # in beat's own form: an instant with USE_TZ, and without it the
        # naive UTC wall clock beat compares a naive start with, whatever
        # TIME_ZONE or this process's zone is.
        if start is not None and start > now:
            raise _Refused(START_AHEAD)
        if expires is None:
            return None
        # Celery counts a task as expired from the instant of its expiry.
        # Asked before the window is worked out, by a comparison that
        # cannot overflow: an expiry in year 1 has expired, and the step
        # back from it is not a date.
        if expires <= now:
            raise _Refused(EXPIRED)
        # The window the call would carry, not the one beat stored. The end
        # is a microsecond before the expiry, and the schedule starts when
        # it is created, after now. A stored schedule refuses an end that
        # is not after its start. A live expiry whose end has no date
        # stops the import, as any bound that cannot be carried over does.
        try:
            end_time = self._end_time(expires)
        except OverflowError as exc:
            raise _BoundRefused(row["name"], "expires") from exc
        if end_time is None or end_time <= now:
            raise _Refused(TOO_SHORT)
        return end_time

    def _stoppages(self, rows: list[dict[str, Any]], now: datetime) -> list[str]:
        """
        Comment lines saying that beat ran nothing from this table, and why,
        where something in it stopped beat 2.9.0 for every row. The row
        that did is listed below for itself as well. These lines say what it
        did to the others, which are printed, so that applying them is known
        to start work beat was not doing.

        Each is what beat's own loading does: a crontab whose timezone is
        empty or does not load fails every load, whichever row uses it;
        a row whose schedule beat cannot build raises as the schedule is
        loaded; a start time or an expiry beat cannot compare with its clock
        raises on every tick while its row is loaded, under USE_TZ on with
        DJANGO_CELERY_BEAT_TZ_AWARE off for any bound, and with both off for
        one stored with a UTC offset; and with both off a start still ahead
        raises the same way, until it passes. Only an enabled row beat
        builds is ever loaded.
        """
        lines = []
        bad = [
            key
            for key, zone in self._crontab_zones.items()
            if not _Zones.own(zone) or not _loadable(zone)
        ]
        if bad:
            ids = _first_named(sorted(bad, key=repr))
            lines.append(SOURCE_STOPPED_ZONE.format(ids=ids))
        # Without USE_TZ beat takes the server's current hour in TIME_ZONE.
        if not settings.USE_TZ and not _loadable(settings.TIME_ZONE):
            lines.append(SOURCE_STOPPED_PROJECT_ZONE.format(project=settings.TIME_ZONE))
        enabled = [row for row in rows if row["enabled"]]
        unbuilt = [row for row in enabled if self._builds(row) is None]
        if unbuilt:
            lines.append(SOURCE_STOPPED_BUILD.format(names=_names(unbuilt)))
        if _beat_is_tz_aware():
            return lines
        stopping, waiting = [], []
        for row in enabled:
            if not self._builds(row):
                continue
            bounds = [
                bound
                for bound in (row["_start_time"], row["_expires"])
                if bound is not None
            ]
            if settings.USE_TZ:
                if bounds:
                    stopping.append(row)
            elif any(timezone.is_aware(bound) for bound in bounds):
                stopping.append(row)
            elif row["_start_time"] is not None and row["_start_time"] > now:
                waiting.append(row)
        if stopping:
            lines.append(SOURCE_STOPPED_ROWS.format(names=_names(stopping)))
        elif waiting:
            until = max(row["_start_time"] for row in waiting)
            lines.append(
                SOURCE_STOPPED_UNTIL.format(
                    until=until.isoformat(sep=" "), names=_names(waiting)
                )
            )
        return lines

    @staticmethod
    def _builds(row: dict[str, Any]) -> bool | None:
        """
        What beat 2.9.0 makes of an enabled row as it loads its schedule:
        True if it builds the row's schedule; False if it leaves the row
        out, for a ValueError, which it catches, or a schedule row that is
        gone; None if building it raises anything else, which stops the
        load for every row.
        """
        # beat takes the interval where a row has both.
        if row["interval_id"] is not None:
            if row["_interval"] is None:
                return False
            every, period = row["_interval"]
            try:
                timedelta(**{period: every})
            except ValueError:
                return False
            except (TypeError, OverflowError):
                return None
            return True
        if row["crontab_id"] is not None:
            if row["_cron"] is None:
                return False
            # In the order Celery's crontab reads them: the hour, the
            # minute, the day of the week, of the month, the month.
            for index in (1, 0, 4, 2, 3):
                text, field = row["_cron"][index], _beat_cron.FIELDS[index]
                if text is None:
                    return None
                if isinstance(text, bytes):
                    # A BLOB, which only SQLite keeps in a text column.
                    # Celery reads it as the set of its byte values, and
                    # refuses one out of the field's range as it does a
                    # number: b"5" as a minute is 53, as an hour a refusal.
                    if all(field.low <= value <= field.high for value in text):
                        continue
                    return False
                if not isinstance(text, str):
                    return False
                try:
                    _beat_cron.celery_values(text, field)
                except _beat_cron.NotCeleryParse:
                    return None
                except _beat_cron.NotCelery:
                    return False
            return True
        # Solar and clocked rows: their tables are not read here, and beat
        # loads a solar row always and a clocked one as it falls due.
        return True


def _names(rows: list[dict[str, Any]]) -> str:
    """The names of some rows, as the literals a comment line holds."""
    return _first_named([row["name"] for row in rows])


#: The most crontabs or rows one diagnostic line names. Each is quoted as
#: _excerpt quotes it, so the line has one bound whatever the size of the
#: table.
_NAMED_AT_MOST = 10


def _first_named(values: list[Any], total: int | None = None) -> str:
    """
    Stored values as the literals a diagnostic line holds: the first
    _NAMED_AT_MOST of them, in the order given, and LIST_CUT for the rest.
    `total` is how many there are in all, where `values` holds only the
    first of them. Each row is still listed for itself.
    """
    named = ", ".join(_excerpt(value) for value in values[:_NAMED_AT_MOST])
    more = (len(values) if total is None else total) - _NAMED_AT_MOST
    if more > 0:
        return named + LIST_CUT.format(more=more)
    return named


class _Zones:
    """
    The zone each crontab ran in under beat, and whether a stored schedule,
    which runs in TIME_ZONE, runs in the same one.

    The same means the same data: both keys loaded, and the two TZif files
    zoneinfo reads for them equal byte for byte. Equal offsets today, or on
    any dates sampled, say nothing about the next change in either zone's
    rules, so anything short of that is a different zone. That refuses a
    pair whose files differ only in history neither will meet again; it
    never passes a pair that differs.
    """

    def __init__(self, beat_timezone: str | None, *, measured: bool = False) -> None:
        self._beat_timezone = beat_timezone
        #: Is the installed beat one whose schedule loading this command's
        #: analysis describes?
        self.measured = measured
        self._problems: dict[tuple[str, str], str | None] = {}
        self._project_is_utc: bool | None = None

    def matching(self, row_zone: Any) -> str:
        """The zone a crontab ran in, or _Refused unless it is TIME_ZONE's."""
        # An empty timezone column, or one that names no zone, stops beat
        # 2.9.0 for the whole table whatever DJANGO_CELERY_BEAT_TZ_AWARE
        # says: it loads every crontab's zone to filter its schedule by
        # hour. So the row is listed even where beat ran its crontab in
        # another zone, and the option cannot stand in for it.
        if row_zone is not _NO_ZONE_COLUMN:
            if not self.own(row_zone):
                raise _Refused(ZONE_EMPTY)
            if not _beat_is_tz_aware() and not _loadable(row_zone):
                raise _Refused(ZONE_COLUMN_UNLOADABLE.format(zone=row_zone))
        # With TZ_AWARE off beat builds a plain crontab and never looks at
        # the row's zone: the crontab runs in the Celery app's timezone.
        # That zone is in Celery's configuration, which this command cannot
        # read, and a setting named CELERY_TIMEZONE is not evidence of it:
        # the name depends on the app's namespace. So it is taken from the
        # operator or not at all.
        if not _beat_is_tz_aware():
            if self._beat_timezone is None:
                raise _Refused(ZONE_IGNORED)
            zone, source = self._beat_timezone, ZONE_FROM_OPTION
        # A table from before beat gave a crontab a zone of its own. The
        # beat that ran it built plain crontabs too.
        elif row_zone is _NO_ZONE_COLUMN:
            if self._beat_timezone is None:
                raise _Refused(ZONE_MISSING)
            zone, source = self._beat_timezone, ZONE_FROM_OPTION
        else:
            zone, source = str(row_zone), ZONE_FROM_ROW
        if (zone, source) not in self._problems:
            self._problems[zone, source] = self._problem(zone, source)
        problem = self._problems[zone, source]
        if problem is not None:
            raise _Refused(problem)
        return zone

    @staticmethod
    def own(row_zone: Any) -> bool:
        """Has a crontab a zone of its own: a column for one, and a value in it?"""
        return (
            row_zone is not _NO_ZONE_COLUMN and row_zone is not None and row_zone != ""
        )

    @staticmethod
    def _problem(zone: str, source: str) -> str | None:
        project = settings.TIME_ZONE
        # Loaded first, the equal keys included: two copies of a name
        # zoneinfo cannot load are not a zone anything runs in.
        if not _loadable(zone):
            return ZONE_UNLOADABLE.format(zone=zone, source=source)
        if not _loadable(project):
            return PROJECT_ZONE_UNLOADABLE.format(project=project)
        if zone == project:
            return None
        ours, theirs = _zone_file(project), _zone_file(zone)
        if ours is None or theirs is None:
            return ZONE_DATA_MISSING.format(zone=zone, source=source, project=project)
        if ours != theirs:
            return ZONE_DIFFERS.format(zone=zone, source=source, project=project)
        return None

    def project_is_utc(self) -> bool:
        """
        Is TIME_ZONE the same data as UTC, so local time is UTC? _Refused
        where TIME_ZONE does not load, or where its file or UTC's cannot be
        found: the reasons that follow from a no say its data differs from
        UTC's, and of data nobody compared that cannot be said.
        """
        if self._project_is_utc is None:
            project = settings.TIME_ZONE
            if not _loadable(project):
                raise _Refused(PROJECT_ZONE_UNLOADABLE.format(project=project))
            if project == "UTC":
                self._project_is_utc = True
            else:
                ours, utc = _zone_file(project), _zone_file("UTC")
                if ours is None or utc is None:
                    raise _Refused(
                        PROJECT_ZONE_COMPARISON_UNAVAILABLE.format(project=project)
                    )
                self._project_is_utc = ours == utc
        return self._project_is_utc


class _Destination:
    """
    The database create_schedules will write the printed rows to, resolved
    once for the import, and what it refuses that a schedule's validation
    does not.

    The validation the importer asks for (`_export_preflight`) is the one
    create_schedules runs, and it is the same on every database. What a
    particular database will not store, or stores as something else, is
    found here, so a row that cannot be applied there is listed rather than
    printed: PostgreSQL's NUL and surrogate refusals and its numbers written
    with an exponent, MySQL's JSON depth, surrogates and integers past 64
    bits, and on MySQL two names its collation calls equal.

    It holds for the database as it is while this command runs. A name
    taken later, a permission or a schema changed, a database gone: none of
    that can be found now, and the footer says so.
    """

    #: The deepest MySQL's JSON column nests arrays and objects, counted as
    #: MySQL counts them: each array or object is a level, a scalar none.
    MYSQL_JSON_DEPTH = 100
    #: The integers MySQL's JSON type keeps as integers. Outside them it
    #: stores a double, which is not the number given.
    MYSQL_JSON_INTEGERS = (-(2**63), 2**64 - 1)
    #: How many names one IN query asks about.
    CHUNK = 500

    def __init__(self) -> None:
        self.alias = schedule_db_alias()
        self.connection = connections[self.alias]
        self.vendor = self.connection.vendor
        self.database = _DATABASE_NAMES.get(self.vendor, ascii(self.vendor))
        self.quoted_alias = ascii(self.alias)
        #: Whether names already stored there were checked; set by clashes().
        self.names_checked = True
        self._collation: tuple[str, str] | None = None

    def interval_limit(self) -> tuple[int, str]:
        """
        The most seconds create_schedules accepts in every_seconds when the
        schedules go here, and what sets it: "column", this database's own
        column; "validation", the default database's range, which Django's
        field validation applies whatever database a row is for; "ticks",
        the longest interval a worker can count ticks for. The lowest
        wins, and of two the same, the earlier in that order.
        """
        field = OxSchedule._meta.get_field("every_seconds")
        limits = []
        _, column = self.connection.ops.integer_field_range(field.get_internal_type())
        if column is not None:
            limits.append((int(column), "column"))
        validated = [
            int(validator.limit_value)
            for validator in field.validators
            if isinstance(validator, MaxValueValidator)
        ]
        if validated:
            limits.append((min(validated), "validation"))
        limits.append((MAX_INTERVAL // timedelta(seconds=1), "ticks"))
        return min(limits, key=lambda limit: limit[0])

    def interval_reason(self, seconds: int) -> str | None:
        """
        The reason an interval of `seconds` is listed, naming the one limit
        it is past, or None where it is within that limit and something else
        refused it.
        """
        limit, rule = self.interval_limit()
        if seconds <= limit:
            return None
        if rule == "column":
            return INTERVAL_PAST_COLUMN.format(
                seconds=seconds,
                limit=limit,
                alias=self.quoted_alias,
                database=self.database,
            )
        if rule == "validation":
            vendor = connections[DEFAULT_DB_ALIAS].vendor
            return INTERVAL_PAST_VALIDATION.format(
                seconds=seconds,
                limit=limit,
                database=_DATABASE_NAMES.get(vendor, ascii(vendor)),
            )
        return INTERVAL_PAST_TICKS.format(seconds=seconds, limit=limit)

    def refusal(self, fields: dict[str, Any]) -> str | None:
        """Why this database would refuse or alter a row that validates."""
        if self.vendor == "postgresql":
            for column in ("name", "task_key"):
                if "\x00" in _plain(fields[column]):
                    return self._refuses(column, WHY_NUL)
        arguments = fields.get("arguments")
        if arguments:
            why = self._json_refusal(arguments)
            if why is not None:
                return self._refuses("arguments", why)
        return None

    def _refuses(self, column: str, why: str) -> str:
        return DESTINATION_REFUSES.format(
            alias=self.quoted_alias, database=self.database, column=column, why=why
        )

    def _json_refusal(self, value: Any) -> str | None:
        """What this database's JSON column does not keep of a decoded value."""
        if self.vendor not in ("postgresql", "mysql"):
            return None
        low, high = self.MYSQL_JSON_INTEGERS
        pending = [(value, 0)]
        while pending:
            item, depth = pending.pop()
            if isinstance(item, dict | list):
                depth += 1
                if self.vendor == "mysql" and depth > self.MYSQL_JSON_DEPTH:
                    return WHY_TOO_DEEP.format(limit=self.MYSQL_JSON_DEPTH)
                if isinstance(item, dict):
                    pending.extend((key, depth) for key in item)
                    pending.extend((child, depth) for child in item.values())
                else:
                    pending.extend((child, depth) for child in item)
            elif isinstance(item, str):
                if self.vendor == "postgresql" and "\x00" in item:
                    return WHY_NUL
                if _has_surrogate(item):
                    return WHY_SURROGATE.format(database=self.database)
            elif isinstance(item, bool):
                continue
            elif isinstance(item, int):
                if self.vendor == "mysql" and not low <= item <= high:
                    return WHY_BIG_INTEGER.format(number=_excerpt(item))
            elif isinstance(item, float):
                # json.dumps writes a float from 1e16 up with an exponent,
                # and PostgreSQL's jsonb keeps the number and writes it
                # back in full, which JSON then reads as an integer.
                if self.vendor == "postgresql" and "e+" in repr(item):
                    return WHY_EXPONENT.format(number=repr(item))
        return None

    def clashes(self, names: list[Any]) -> dict[int, str]:
        """
        The printed rows create_schedules would refuse for their name, by
        position in `names`: a name already stored there, or two names the
        name column calls equal. Every member of a clash is listed, none
        chosen to keep it.

        Names already stored are asked of the table as create_schedules
        asks, under the column's own comparison, where the table exists.
        """
        try:
            exists = (
                OxSchedule._meta.db_table in self.connection.introspection.table_names()
            )
            existing = self._existing(names) if exists else []
            groups = self._equal_names([*names, *existing])
        except DatabaseError as exc:
            raise _DestinationUnreadable from exc
        self.names_checked = exists
        clashes: dict[int, str] = {}
        printed = len(names)
        for group in groups:
            members = [index for index in group if index < printed]
            stored = [existing[index - printed] for index in group if index >= printed]
            # A reason names the first of the others and counts the rest, so
            # the first members are all any reason needs, however many rows
            # share the name.
            leading = members[: _NAMED_AT_MOST + 1]
            for index in members:
                if stored:
                    clashes[index] = NAME_TAKEN.format(
                        alias=self.quoted_alias, existing=_excerpt(stored[0])
                    )
                else:
                    others = _first_named(
                        [names[other] for other in leading if other != index],
                        total=len(members) - 1,
                    )
                    clashes[index] = NAME_CLASH.format(
                        others=others,
                        collation=self._collation_label(),
                        alias=self.quoted_alias,
                    )
        return clashes

    def _existing(self, names: list[Any]) -> list[str]:
        """The stored names equal to any of `names` as the column compares."""
        found: list[str] = []
        plain = [_plain(name) for name in names]
        for start in range(0, len(plain), self.CHUNK):
            chunk = plain[start : start + self.CHUNK]
            found.extend(
                OxSchedule.objects.using(self.alias)
                .filter(name__in=chunk)
                .values_list("name", flat=True)
            )
        return found

    def _equal_names(self, names: list[Any]) -> list[list[int]]:
        """
        Positions of the names that are one name to the name column, in
        groups of two or more. PostgreSQL's and SQLite's compare text
        exactly; MySQL's by the column's collation, which the database is
        asked to apply, since no reimplementation of its rules would be
        the rules.
        """
        if self.vendor != "mysql":
            positions: dict[str, list[int]] = {}
            for index, name in enumerate(names):
                positions.setdefault(_plain(name), []).append(index)
            return [group for group in positions.values() if len(group) > 1]
        charset, collation = self._mysql_collation()
        # The character set and collation are interpolated, held to an
        # identifier's characters first (_mysql_collation); the names are
        # a parameter.
        #
        # Each group's positions as a JSON array, which holds as many as
        # the batch has. GROUP_CONCAT can truncate its result at
        # group_concat_max_len, 1,024 bytes by default.
        sql = (
            "SELECT JSON_ARRAYAGG(t.i) FROM JSON_TABLE("  # noqa: S608
            "%s, '$[*]' COLUMNS (i INT PATH '$[0]', n VARCHAR(1024) PATH '$[1]')) AS t "
            f"GROUP BY CONVERT(t.n USING {charset}) COLLATE {collation} "
            "HAVING COUNT(*) > 1"
        )
        payload = json.dumps(
            [[index, _plain(name)] for index, name in enumerate(names)]
        )
        with self.connection.cursor() as cursor:
            cursor.execute(sql, [payload])
            # In the batch's order: JSON_ARRAYAGG takes no ORDER BY.
            return [sorted(json.loads(found)) for (found,) in cursor.fetchall()]

    def _mysql_collation(self) -> tuple[str, str]:
        """
        The character set and collation of the name column, or of the
        database where the table is not there yet: the one Django's
        migration will give it.
        """
        if self._collation is not None:
            return self._collation
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT CHARACTER_SET_NAME, COLLATION_NAME "
                "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = %s AND COLUMN_NAME = %s",
                [OxSchedule._meta.db_table, "name"],
            )
            row = cursor.fetchone()
            if row is None or not row[1]:
                cursor.execute(
                    "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
                    "FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE()"
                )
                row = cursor.fetchone()
        charset, collation = (str(value) for value in row)
        # Interpolated into the query, so held to what an identifier is.
        if not all(_IDENTIFIER.fullmatch(value) for value in (charset, collation)):
            raise _DestinationUnreadable(charset, collation)
        self._collation = (charset, collation)
        return self._collation

    def _collation_label(self) -> str:
        if self.vendor != "mysql":
            return "exact"
        return ascii(self._mysql_collation()[1])


#: How a reason names a database vendor.
_DATABASE_NAMES = {"postgresql": "PostgreSQL", "mysql": "MySQL", "sqlite": "SQLite"}
#: What a MySQL character set or collation name is made of.
_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")


def _has_surrogate(text: str) -> bool:
    """Does the text hold a code point UTF-8 cannot encode: a lone surrogate?"""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


#: The longest quotation of one stored value a diagnostic line carries. The
#: longest literal a name or task a schedule can store needs is 1,282
#: characters (128 characters, each written \\U and eight digits), so no
#: value a stored schedule could hold is ever cut.
_EXCERPT_LIMIT = 1300


def _excerpt(value: Any) -> str:
    """
    A stored value as a diagnostic quotes it: its ASCII literal, which is
    one printable line whatever the value holds, and no longer than
    _EXCERPT_LIMIT. A longer text or bytes value is cut to the longest
    prefix whose literal fits, so what is quoted still reads as a literal,
    and EXCERPT_CUT says how long the whole was.
    """
    value = _plain(value)
    text = ascii(value)
    if len(text) <= _EXCERPT_LIMIT:
        return text
    if isinstance(value, str | bytes):
        # Each item takes at least one character to write, so the prefix
        # is never longer than the limit; the rest is a binary search.
        low, high = 0, min(len(value), _EXCERPT_LIMIT)
        while low < high:
            middle = (low + high + 1) // 2
            if len(ascii(value[:middle])) <= _EXCERPT_LIMIT:
                low = middle
            else:
                high = middle - 1
        unit = "characters" if isinstance(value, str) else "bytes"
        return ascii(value[:low]) + EXCERPT_CUT.format(length=len(value), unit=unit)
    return text[:_EXCERPT_LIMIT] + EXCERPT_CUT.format(
        length=len(text), unit="characters"
    )


class _SourceText(str):
    """
    Text read from beat's tables: the same text in every way but one.
    Quoted into a diagnostic through !a or repr(), it comes out as
    _excerpt makes it, bounded. Wrapped once, as the rows are read
    (handle()), so every reason that quotes a stored value with !a is held
    to the one formatter, wherever in the command that reason is built.
    str() keeps the wrapper, so a value passed through str() is still held
    to it; _plain() is the way back to the text itself, for printed code.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return _excerpt(str.__str__(self))

    def __str__(self) -> str:
        return self


class _SourceBytes(bytes):
    """Bytes read from beat's tables, quoted as _SourceText is."""

    __slots__ = ()

    def __repr__(self) -> str:
        return _excerpt(bytes(self))


class _Excerpt:
    """Any value, quoted through !a as _excerpt quotes it."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        return _excerpt(self.value)


def _source(value: Any) -> Any:
    """A stored value, wrapped so that a diagnostic quoting it is bounded."""
    if isinstance(value, str) and not isinstance(value, _SourceText):
        return _SourceText(value)
    if isinstance(value, bytes) and not isinstance(value, _SourceBytes):
        return _SourceBytes(value)
    return value


def _plain(value: Any) -> Any:
    """A value as it was stored, without the wrapper _source gave it."""
    if isinstance(value, _SourceText):
        return str.__str__(value)
    if isinstance(value, _SourceBytes):
        return bytes(value)
    return value


def _beat_is_tz_aware() -> bool:
    """DJANGO_CELERY_BEAT_TZ_AWARE, read from Django's settings as beat reads it."""
    return bool(getattr(settings, "DJANGO_CELERY_BEAT_TZ_AWARE", True))


def _beat_version() -> str | None:
    """
    The version of django-celery-beat installed beside this command, if
    any. Read from its metadata and not by importing it: this package does
    not depend on it.
    """
    try:
        return importlib.metadata.version("django-celery-beat")
    except importlib.metadata.PackageNotFoundError:
        return None


def _loadable(key: Any) -> bool:
    """Can zoneinfo load a zone under this key?"""
    if not isinstance(key, str):
        return False
    # Every exception, not a chosen list: a missing key, a key that is no
    # relative path, a directory, a file that is not TZif data and a file
    # cut short each raise something different, and all of them mean no.
    try:
        zoneinfo.ZoneInfo(key)
    except Exception:
        return False
    return True


def _zone_file(key: str) -> bytes | None:
    """
    The TZif data zoneinfo reads for a key it can load, found the way
    zoneinfo finds it: each directory on TZPATH in turn, then the tzdata
    package. None when it is in neither.
    """
    try:
        for root in zoneinfo.TZPATH:
            candidate = Path(root, key)
            if candidate.is_file():
                return candidate.read_bytes()
        *folders, name = key.split("/")
        packaged = resources.files(".".join(["tzdata", "zoneinfo", *folders]))
        resource = packaged.joinpath(name)
        return resource.read_bytes() if resource.is_file() else None
    except (ImportError, OSError, ValueError):
        # No tzdata package or no such folder in it, a file that cannot be
        # read, a name no file can have.
        return None


def _kombu_json() -> Any:
    """
    kombu's JSON module, the one beat decodes arguments with in this
    environment. _OLD_KOMBU for a kombu from before Celery type markers
    (5.3), None where kombu is not installed.
    """
    # kombu is beat's dependency, not this package's, and ships no types.
    try:
        from kombu.utils import json as kombu_json  # type: ignore[import-untyped]
    except ImportError:
        return None
    if not hasattr(kombu_json, "object_hook"):
        return _OLD_KOMBU
    return kombu_json


def _old_kombu_loads(source: Any) -> Any:
    """loads of a kombu from before type markers, which takes no object_hook."""
    from kombu.utils import json as kombu_json

    return kombu_json.loads(source)


def _loads_without_kombu(source: Any, object_hook: Any) -> Any:
    """kombu's loads where kombu is missing: its handling of bytes, then JSON."""
    if isinstance(source, memoryview):
        source = source.tobytes().decode("utf-8")
    elif isinstance(source, bytes | bytearray):
        source = source.decode("utf-8")
    return json.loads(source, object_hook=object_hook)


def _not_json(value: Any) -> str | None:
    """
    The type of the first value in a decoded structure that JSON does not
    hold, or None when JSON holds all of it. Walked with an explicit stack,
    as _non_finite is.
    """
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    return type(key).__name__
                pending.append(child)
        elif isinstance(item, list):
            pending.extend(item)
        elif not isinstance(item, _JSON_SCALARS):
            return type(item).__name__
    return None


def _non_finite(value: Any) -> bool:
    """
    Does decoded JSON hold NaN or an infinity anywhere? json.loads accepts
    both, and neither has a Python literal to be printed as.
    """
    # Walk with an explicit stack: decoded JSON can be deep enough for
    # a recursive non-finite check to hit Python's recursion limit.
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, float) and not math.isfinite(item):
            return True
        if isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return False
