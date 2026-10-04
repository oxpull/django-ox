"""The django-celery-beat import command, which must never write."""

import ast
import code
import json
import struct
import sys
import tokenize
import zoneinfo
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta, tzinfo
from io import StringIO
from unittest import mock

import pytest
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import (
    DatabaseError,
    DataError,
    OperationalError,
    connection,
    connections,
)
from django.db.backends.utils import CursorWrapper
from django.test import override_settings
from django.utils import timezone

from django_ox.cron import CronExpression
from django_ox.models import OxSchedule, OxScheduleTick

from . import tasks

#: Every test here runs with USE_TZ on, whatever the settings module has,
#: unless it turns it off itself. With USE_TZ off and beat's own
#: DJANGO_CELERY_BEAT_TZ_AWARE left alone, beat did not keep to any schedule
#: and nothing is translated, so on the legs whose settings have it off
#: there would be nothing to look at. The rules for USE_TZ off have tests of
#: their own, which set it, so both kinds run on every leg.
pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("use_tz"),
]


#: What the command is held to saying, word for word: each of its lines that
#: a test pins, as the template the command fills. Written out here and not
#: imported from the command, so that a test which names one fails when the
#: command says anything else.
BEAT_TIMEZONE_HELP = (
    "Confirm the timezone the Celery app ran beat in, for example Europe/Berlin. This "
    "applies to every crontab when DJANGO_CELERY_BEAT_TZ_AWARE is False, and to "
    "crontab tables with no timezone column. It does not replace an empty timezone "
    "when DJANGO_CELERY_BEAT_TZ_AWARE is True. The command cannot verify your choice."
)
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
HEADER = (
    "# Translated from django-celery-beat for django-ox.\n"
    "# Run this command in beat's Python environment with beat's Django settings.\n"
    "# It uses that environment's timezone data, USE_TZ and\n"
    "# DJANGO_CELERY_BEAT_TZ_AWARE. With --beat-timezone, you confirm\n"
    "# the timezone the Celery app ran beat in.\n"
    "# The command cannot verify these. Check them before applying this output."
)
BEAT_TIMEZONE_UNUSED = (
    "# --beat-timezone was not needed: every crontab row has its own timezone and "
    "DJANGO_CELERY_BEAT_TZ_AWARE is on."
)
SCHEDULE_SOURCE_NOTE = (
    '# Add "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource" to the same '
    "OPTIONS; without it no stored schedule ever runs."
)
NOTHING_TRANSLATED = (
    "# No periodic tasks could be translated. The reasons follow.\n"
    "# There are no schedules to apply."
)
HORIZON = (
    "# Clock changes were checked for ten years from this import in\n"
    "# {project!a}.\n"
    "# Within those ten years, a printed crontab with no clock-change notice\n"
    "# runs through skipped and repeated clock times at the times beat ran it.\n"
    "# Later years were not checked."
)
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
INTERVAL_DIFFERS = (
    "a stored schedule counts this interval from a fixed instant. Celery counts it "
    "from the last run. Its run times may differ."
)
BACKEND_CLEANUP = (
    "celery.backend_cleanup is added by beat to clean Celery's result backend. It is "
    "not a project task to import. Use ox_prune to remove django-ox's finished rows."
)
NOT_TEXT = "its {column} is not text. Store a text value, then import again."
TWO_SCHEDULES = (
    "it has both a crontab and an interval. beat uses the interval, and a stored "
    "schedule has one trigger. Keep only the intended trigger, then import again."
)
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
ZONE_UNLOADABLE = (
    "its schedule runs in {zone!a} ({source}), which this Python cannot load. Check "
    "the timezone value and this environment's timezone data."
)
#: Pinned the same way.
BEAT_VERSION_UNMEASURED = (
    "# django-celery-beat {version} is installed here. Its schedule loading was "
    "checked for 2.9.0 only, so whether it ran crontabs late or stopped on a bad row "
    "was not checked."
)
BEAT_NOT_INSTALLED = (
    "# django-celery-beat is not installed here, so its schedule loading was not "
    "checked: whether it ran crontabs late or stopped on a bad row is not known."
)
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
LIST_CUT = " and {more} more"
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
ZONE_COLUMN_UNLOADABLE = (
    "its crontab's timezone is {zone!a}, which this Python cannot load. beat 2.9.0 "
    "reads it when loading its schedule, even with DJANGO_CELERY_BEAT_TZ_AWARE off, "
    "and raises on every load. Fix the crontab's timezone, then import again."
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
INVALID_JSON = (
    "its {column} contains invalid JSON. beat disables a row whose arguments it cannot "
    "decode, so it did not run this row."
)
EVERY_NOT_A_NUMBER = (
    "its interval's every value is {every!a}, which is not a number. Fix the value, "
    "then import again."
)
NOT_WHOLE_SECONDS = (
    "an interval of {every!a} {period!a} is not a whole number of seconds. A stored "
    "schedule counts in whole seconds."
)
INTERVAL_TOO_LONG = (
    "an interval of {every!a} {period!a} exceeds timedelta's limit of 999,999,999 "
    "days, so beat cannot build it. Fix the interval, then import again."
)
TOO_DEEP_TO_DECODE = (
    "{column} is nested too deeply to decode. Reduce the nesting, then import again."
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
CALL_UNREADABLE = (
    "its arguments could not be turned into a Python call that can be compiled. Check "
    "their nesting depth."
)


#: The periodictask columns django-celery-beat added after its first table:
#: expires is in 0001, one_off and start_time arrived in 0007.
LATER_COLUMNS = ("one_off", "start_time", "expires")


def make_beat_tables(columns=LATER_COLUMNS, *, db="default", crontab_zone=True):
    """
    A minimal stand-in for the tables django-celery-beat creates.

    columns picks which of the later periodictask columns exist, so an older
    table is built in its own shape, and crontab_zone whether a crontab has
    the timezone column beat gave it in 2018. The test tables are created
    directly rather than reduced with DROP COLUMN, which SQLite added in
    3.35.0 and Django enables from 3.35.5.
    """
    datetime_type = connections[db].data_types["DateTimeField"]
    # What Django creates for beat's TextField: on MySQL the type that holds
    # more than 64 KB.
    text_type = connections[db].data_types["TextField"]
    types = {"one_off": "boolean", "start_time": datetime_type}
    later = "".join(f", {name} {types.get(name, datetime_type)}" for name in columns)
    zone = ", timezone varchar(63)" if crontab_zone else ""
    with connections[db].cursor() as cursor:
        cursor.execute(
            "CREATE TABLE django_celery_beat_crontabschedule ("
            "id integer primary key, minute varchar(64), hour varchar(64), "
            "day_of_month varchar(64), month_of_year varchar(64), "
            f"day_of_week varchar(64){zone})"
        )
        cursor.execute(
            "CREATE TABLE django_celery_beat_intervalschedule ("
            "id integer primary key, every integer, period varchar(24))"
        )
        cursor.execute(
            "CREATE TABLE django_celery_beat_periodictask ("
            "id integer primary key, name varchar(200), task varchar(200), "
            f"args {text_type}, kwargs {text_type}, queue varchar(200), "
            f"enabled boolean, crontab_id integer, interval_id integer{later})"
        )
    zoned = {} if crontab_zone else {"timezone": NO_COLUMN}
    insert_crontab(1, db=db, **zoned)
    insert_interval(1, 90, "minutes", db=db)
    one_off = {"one_off": False} if "one_off" in columns else {}
    insert_task(1, "nightly", "reports.tasks.daily", db=db, crontab_id=1, **one_off)
    insert_task(
        2, "poller", "mail.tasks.poll", db=db, queue="mail", interval_id=1, **one_off
    )
    insert_task(3, "orphan", "x.y.z", db=db, **one_off)


#: For insert_crontab: leave the timezone out, on a table that has no such column.
NO_COLUMN = object()


def insert_crontab(pk, *, db="default", **fields):
    """
    Add one crontab. A field not given is "*", but for the fixture's own
    02:00 and the project's zone, so a test names only what it is about.
    """
    values = {
        "minute": "0",
        "hour": "2",
        "day_of_month": "*",
        "month_of_year": "*",
        "day_of_week": "*",
        "timezone": settings.TIME_ZONE,
        **fields,
    }
    if values["timezone"] is NO_COLUMN:
        del values["timezone"]
    names = ", ".join(["id", *values])
    marks = ", ".join(["%s"] * (len(values) + 1))
    with connections[db].cursor() as cursor:
        cursor.execute(
            f"INSERT INTO django_celery_beat_crontabschedule ({names}) "  # noqa: S608
            f"VALUES ({marks})",
            [pk, *values.values()],
        )


def set_crontab(pk, **fields):
    """Change the fields given on one crontab."""
    assignments = ", ".join(f"{name} = %s" for name in fields)
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_crontabschedule SET {assignments} "  # noqa: S608
            "WHERE id = %s",
            [*fields.values(), pk],
        )


def insert_interval(pk, every, period, *, db="default"):
    with connections[db].cursor() as cursor:
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (%s, %s, %s)",
            [pk, every, period],
        )


def set_interval(pk, every, period):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_intervalschedule SET every = %s, period = %s "
            "WHERE id = %s",
            [every, period, pk],
        )


def insert_task(pk, name, task, *, db="default", **columns):
    """
    Add one beat row. Columns not given are NULL, or empty for the arguments.

    Parameterised, and booleans passed as booleans: PostgreSQL will not
    accept 1 for a boolean column where SQLite and MySQL both would.
    """
    values = {"args": "[]", "kwargs": "{}", "enabled": True, **columns}
    names = ", ".join(["id", "name", "task", *values])
    marks = ", ".join(["%s"] * (len(values) + 3))
    with connections[db].cursor() as cursor:
        cursor.execute(
            f"INSERT INTO django_celery_beat_periodictask ({names}) "  # noqa: S608
            f"VALUES ({marks})",
            [pk, name, task, *values.values()],
        )


def drop_beat_tables(db="default"):
    with connections[db].cursor() as cursor:
        for table in (
            "django_celery_beat_periodictask",
            "django_celery_beat_crontabschedule",
            "django_celery_beat_intervalschedule",
        ):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture(autouse=True)
def beat_installed(monkeypatch):
    """
    django-celery-beat 2.9.0 beside the command, as in beat's own Python
    environment, which is where the command is run. The test environments
    do not install it; a test of another version, or of none, says so.
    """
    from django_ox.management.commands import ox_import_beat_schedules as command

    monkeypatch.setattr(command, "_beat_version", lambda: "2.9.0", raising=False)


@pytest.fixture
def beat_tables():
    # Created inside the try: a setup that fails halfway still drops what
    # it made, and dropping skips a table that was never created.
    try:
        make_beat_tables()
        yield
    finally:
        drop_beat_tables()


def run(**options):
    out = StringIO()
    call_command("ox_import_beat_schedules", stdout=out, **options)
    return out.getvalue()


SECTION_2 = "# 2. Create the schedules."
NOT_TRANSLATED = "# Not translated, and why:"
DIFFERENCES = "# Translated, with a difference from beat:"


def section_1(output):
    """The settings fragment, from its first key to section 2."""
    return output[output.index('"SCHEDULABLE_TASKS": {') : output.index(SECTION_2)]


def section_2(output):
    """What is pasted as code: section 2's heading to the end of the output."""
    return output[output.index(SECTION_2) :]


def listed_under(heading, output):
    """The rows named under one heading of the output: what is said, by name."""
    lines = output.splitlines()
    if heading not in lines:
        return {}
    listed = {}
    for line in lines[lines.index(heading) + 1 :]:
        if not line.startswith("#   "):
            break
        rest = line.removeprefix("#   ")
        # The name is a literal, read as Python reads one, so a name that
        # holds ": " cannot be mistaken for the end of itself.
        token = next(tokenize.generate_tokens(StringIO(rest).readline))
        name = ast.literal_eval(token.string)
        listed[name] = rest[token.end[1] :].removeprefix(": ")
    return listed


def refusals(output):
    """Every row listed as not translated: the reason given, by the row's name."""
    return listed_under(NOT_TRANSLATED, output)


def differences(output):
    """Every row translated and named as differing from beat: how, by its name."""
    return listed_under(DIFFERENCES, output)


def run_as_module(section):
    """Section 2 as a data migration or a script runs it: compiled whole."""
    namespace = {}
    exec(compile(section, "<section 2>", "exec"), namespace)  # noqa: S102
    return namespace


class _Shell(code.InteractiveConsole):
    """A manage.py shell that records what went wrong instead of printing it."""

    def __init__(self, namespace):
        super().__init__(locals=namespace)
        self.errors = []

    def showtraceback(self):
        self.errors.append(sys.exc_info()[1])

    def showsyntaxerror(self, *args, **kwargs):
        self.errors.append(sys.exc_info()[1])


def paste_into_shell(section):
    """Section 2 as someone pastes it into a shell: one line at a time."""
    namespace = {}
    shell = _Shell(namespace)
    # splitlines, not split("\n"): it breaks wherever a terminal or an editor
    # may end a line, at CR and U+2028 among others.
    for line in section.splitlines():
        shell.push(line)
    shell.push("")
    assert not shell.errors, shell.errors
    return namespace


def literal_after(text, prefix):
    """The Python literal that starts right after prefix in text, evaluated."""
    rest = text[text.index(prefix) + len(prefix) :]
    token = next(tokenize.generate_tokens(StringIO(rest).readline))
    return ast.literal_eval(token.string)


@contextmanager
def recording():
    """create_schedules as section 2 imports it, recording its rows instead."""
    rows = []
    with mock.patch("django_ox.stored.create_schedules", rows.extend):
        yield rows


@pytest.fixture
def recorded():
    with recording() as rows:
        yield rows


def printed_rows(output):
    """Section 2 run with its writes recorded: each printed row's fields by name."""
    if SECTION_2 not in output:
        return {}
    with recording() as rows:
        run_as_module(section_2(output))
    return {row["name"]: row for row in rows}


def printed_calls(output):
    """The lines of section 2 that each hold one schedule, as the dict() they are."""
    return [
        line.strip().removesuffix(",")
        for line in output.splitlines()
        if line.startswith("    dict(")
    ]


@pytest.fixture
def use_tz(settings):
    """USE_TZ on, whatever the settings module has. See pytestmark."""
    settings.USE_TZ = True


def tzif(*types, changes=()):
    """
    A timezone file built here, so that which zones hold the same data is
    this file's decision and not the tz database's. Each type is seconds
    east of UTC and an abbreviation; each change is an instant, in seconds
    since 1970, and the index of the type that applies from it.
    """
    names = b"".join(name.encode() + b"\0" for _, name in types)
    data = b"TZif" + b"\0" * 16
    data += struct.pack(">6l", 0, 0, 0, len(changes), len(types), len(names))
    data += b"".join(struct.pack(">l", at) for at, _ in changes)
    data += bytes(index for _, index in changes)
    position = 0
    for seconds, name in types:
        data += struct.pack(">lBB", seconds, 0, position)
        position += len(name) + 1
    return data + names


#: Two hours east of UTC, all year and for ever.
PLUS_TWO = tzif((7200, "OXA"))
#: The same until 1 March 2030, then three hours east: its offsets match
#: PLUS_TWO's on any date before that, and its data never does.
PLUS_TWO_FOR_NOW = tzif((7200, "OXA"), (10800, "OXB"), changes=[(1898553600, 1)])
ZERO = tzif((0, "OXU"))


@pytest.fixture
def zone_files(tmp_path):
    """
    A TZPATH holding only what the test puts on it. Returns the function
    that puts a file there, under a key.

    zoneinfo falls back to the tzdata package for a key that is not on the
    path, so a test that means a key to be unknown uses one no package has.
    """

    def place(key, data):
        path = tmp_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    zoneinfo.reset_tzpath(to=[tmp_path])
    zoneinfo.ZoneInfo.clear_cache()
    try:
        yield place
    finally:
        zoneinfo.reset_tzpath()
        zoneinfo.ZoneInfo.clear_cache()


@pytest.fixture
def schedulable(monkeypatch):
    """The fixture's two translatable task paths, registered as schedulable."""
    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    for key in ("reports.tasks.daily", "mail.tasks.poll"):
        register(ScheduleKind(key=key, task=tasks.add))


def instant(text):
    """A datetime written as naive text, as the command reads it back."""
    value = datetime.fromisoformat(text)
    if settings.USE_TZ:
        return timezone.make_aware(value, connection.timezone)
    return value


@pytest.fixture
def clock(monkeypatch):
    """
    Pin timezone.now(), which the command and create_schedule both read.

    Returns a setter taking the time as naive text, and the list of the
    instants each reading of the clock returned.
    """
    readings = []

    def pin(text):
        at = instant(text)

        def now():
            readings.append(at)
            return at

        monkeypatch.setattr(timezone, "now", now)
        return at

    pin.readings = readings
    return pin


def test_it_writes_nothing(beat_tables):
    # The claim the command's own docstring makes. A migration is a decision
    # about production timing, so it prints and stops.
    run()
    assert OxSchedule.objects.count() == 0


def test_a_table_with_no_rows_says_so_and_prints_nothing_else(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask")
    assert run() == "No periodic tasks found.\n"


def test_rows_whose_schedule_tables_are_not_there_are_listed(beat_tables):
    # The periodic tasks alone, as a partial restore leaves them.
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE django_celery_beat_crontabschedule")
        cursor.execute("DROP TABLE django_celery_beat_intervalschedule")
    output = run()
    assert refusals(output) == {
        "nightly": "its schedule row is missing",
        "poller": "its schedule row is missing",
        "orphan": "solar and clocked schedules have no equivalent",
    }
    assert printed_rows(output) == {}


@pytest.mark.parametrize("column", ["name", "task"])
def test_a_row_whose_name_or_task_is_not_text_is_listed(beat_tables, column):
    # beat's own columns cannot hold NULL. A table restored or written by
    # hand can, and a task that is not text has no place in the settings
    # fragment beside ones that are.
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_periodictask SET {column} = NULL "  # noqa: S608
            "WHERE id = 2"
        )
    output = run()
    listed = refusals(output)
    name = None if column == "name" else "poller"
    assert listed.get(name) == NOT_TEXT.format(column=column)
    assert sorted(printed_rows(output)) == ["nightly"]
    assert "None" not in section_1(output)


def test_it_prints_the_allow_list_and_the_calls(beat_tables):
    output = run()
    assert "'reports.tasks.daily': 'reports.tasks.daily'" in output
    assert "cron='0 2 * * *'" in output
    assert "every_seconds=5400" in output


def test_the_generated_calls_actually_run(beat_tables, schedulable):
    """
    Execute the output rather than matching strings in it.

    The previous version of this test asserted the presence of
    `queue_name="mail"`, which named a field the model does not have, so it
    pinned an output that raised TypeError the moment anyone pasted it. A
    printed migration is only worth printing if it runs.
    """
    output = run()
    calls = printed_calls(output)
    assert calls, "the command printed no calls to check"

    # As pasted: on section 2's own imports, in a namespace of its own.
    run_as_module(section_2(output))

    assert OxSchedule.objects.count() == len(calls)


def test_it_opens_by_saying_what_it_took_on_trust(beat_tables):
    # What is printed is right only for the environment and settings beat
    # ran with, and nothing in a table can show that this is them. So the
    # first thing a reader meets says so, before any line they might paste.
    assert run().startswith(
        HEADER + "\n\n# 1. Expose these tasks. A row can only name a key you list.\n"
    )


def test_the_settings_fragment_names_the_schedule_source(beat_tables):
    # Without SCHEDULE_SOURCE a stored schedule saves and never runs, with
    # no error anywhere, so the fragment that is pasted into OPTIONS says so.
    # One comment line, inside the fragment, which still evaluates.
    fragment = section_1(run())
    lines = [line for line in fragment.splitlines() if "SCHEDULE_SOURCE" in line]
    assert lines == [SCHEDULE_SOURCE_NOTE]
    namespace = {}
    exec("TASKS = {" + fragment + "}", namespace)  # noqa: S102
    assert namespace["TASKS"] == {
        "SCHEDULABLE_TASKS": {
            "mail.tasks.poll": "mail.tasks.poll",
            "reports.tasks.daily": "reports.tasks.daily",
        }
    }


def test_only_the_tasks_of_printed_rows_are_exposed(beat_tables):
    """
    A key in SCHEDULABLE_TASKS is a task anyone holding the admin can
    schedule. A row that was not translated creates no schedule, so its
    task has no business in the fragment: here a one-off row's, a row whose
    crontab Celery refuses, and the fixture's row with no schedule at all.
    """
    insert_crontab(2, day_of_week="7")
    insert_task(4, "once", "billing.tasks.charge_once", interval_id=1, one_off=True)
    insert_task(5, "broken", "billing.tasks.broken", crontab_id=2)
    output = run()
    namespace = {}
    exec("TASKS = {" + section_1(output) + "}", namespace)  # noqa: S102
    assert namespace["TASKS"] == {
        "SCHEDULABLE_TASKS": {
            "mail.tasks.poll": "mail.tasks.poll",
            "reports.tasks.daily": "reports.tasks.daily",
        }
    }
    assert "billing.tasks" not in section_1(output)
    assert "x.y.z" not in section_1(output)
    assert set(refusals(output)) == {"orphan", "once", "broken"}


def test_celerys_own_cleanup_task_is_listed_not_translated(beat_tables):
    """
    beat adds a celery.backend_cleanup row to the table by itself. It is
    Celery's housekeeping, not a task of the project's. That one name and
    no other: a project's own task under celery. is still its own.
    """
    insert_crontab(2, hour="4")
    insert_task(4, "celery.backend_cleanup", "celery.backend_cleanup", crontab_id=2)
    insert_task(5, "ours", "celery.backend_cleanup_reports", crontab_id=2)
    insert_task(6, "ours-too", "celery.chord_unlock", interval_id=1)
    output = run()
    assert "celery.backend_cleanup" in refusals(output)
    assert refusals(output)["celery.backend_cleanup"] == BACKEND_CLEANUP
    rows = printed_rows(output)
    assert rows["ours"]["task_key"] == "celery.backend_cleanup_reports"
    assert rows["ours-too"]["task_key"] == "celery.chord_unlock"
    assert "'celery.backend_cleanup'" not in section_1(output)


def test_when_no_row_translates_nothing_pasteable_is_printed(beat_tables):
    """
    A settings fragment and an import with no schedule behind them are
    still something to paste, and pasting them exposes tasks for nothing.
    With no row translated, every line printed is a comment or blank.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET one_off = %s", [True]
        )
    output = run()
    assert set(refusals(output)) == {"nightly", "poller", "orphan"}
    assert all(line.startswith("#") or not line for line in output.splitlines())
    assert "SCHEDULABLE_TASKS" not in output
    assert SECTION_2 not in output
    assert "create_schedules" not in output
    assert output.startswith(
        f"{HEADER}\n\n{NOTHING_TRANSLATED}\n\n{NOT_TRANSLATED}\n#   'nightly': "
    )


def test_a_row_with_positional_arguments_is_not_translated(beat_tables):
    # A stored schedule takes keyword arguments only, so a beat row carrying
    # positional args cannot be expressed and must not be printed as if it can.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET args = %s WHERE id = 1",
            ['["emea"]'],
        )
    output = run()
    assert (
        "#   'nightly': it passes positional arguments, and a stored schedule "
        "takes keyword arguments only; rewrite the task signature or the row"
        in output.splitlines()
    )
    assert "nightly" not in printed_rows(output)


def test_keyword_arguments_are_carried_over(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET kwargs = %s WHERE id = 1",
            ['{"region": "emea"}'],
        )
    assert "arguments={'region': 'emea'}" in run()


def test_it_explains_what_it_could_not_translate(beat_tables):
    # Each reason on its own row's line. These two are the fallbacks every
    # other check runs before, so a check that matched too much would
    # shadow them.
    insert_task(4, "dangling", "x.y.z", crontab_id=99)
    lines = run().splitlines()
    assert "# Not translated, and why:" in lines
    assert "#   'orphan': solar and clocked schedules have no equivalent" in lines
    assert "#   'dangling': its schedule row is missing" in lines


def test_a_row_with_both_a_crontab_and_an_interval_is_listed(beat_tables):
    """
    beat's own validation refuses a row with two schedules, but only on
    save(), so an update() can leave one. beat then runs it on the interval,
    and this command used to print the crontab.
    """
    insert_task(4, "both", "x.y.z", crontab_id=1, interval_id=1)
    output = run()
    assert refusals(output).get("both") == TWO_SCHEDULES
    assert "both" not in printed_rows(output)


def test_it_warns_that_interval_timing_differs(beat_tables):
    # The difference most likely to surprise someone migrating.
    output = run()
    assert "fixed instant" in output


def test_it_ends_with_the_whole_note_on_applying(beat_tables):
    # Keep the complete application note under regression coverage.
    assert run().endswith(f"\n\n{FOOTER}\n")


def test_a_missing_table_is_an_error_not_an_empty_run(beat_tables):
    drop_beat_tables()
    with pytest.raises(CommandError, match="No django_celery_beat_periodictask"):
        run()
    make_beat_tables()  # so the fixture's teardown is symmetric


def zone_differs(zone, source=ZONE_FROM_ROW):
    return ZONE_DIFFERS.format(zone=zone, source=source, project=settings.TIME_ZONE)


def test_a_schedule_in_another_timezone_is_not_translated(beat_tables):
    # A stored schedule has no zone of its own, so a beat schedule carrying
    # one would run at a different time. The command names the difference
    # rather than emitting a line that quietly means something else.
    set_crontab(1, timezone="Asia/Tokyo")
    output = run()
    assert refusals(output).get("nightly") == zone_differs("Asia/Tokyo")
    assert "nightly" not in printed_rows(output)


def test_an_equivalent_zone_under_another_name_is_still_translated(beat_tables):
    # US/Eastern and America/New_York are one zone. Comparing the strings
    # would divert a correctly-aligned schedule into the untranslated list.
    # The one test here that leans on the tz database's own links.
    set_crontab(1, timezone="US/Eastern")
    with override_settings(TIME_ZONE="America/New_York"):
        output = run()
    assert "US/Eastern" not in output
    assert printed_rows(output)["nightly"]["cron"] == "0 2 * * *"


def test_two_names_for_the_same_zone_data_are_one_zone(beat_tables, zone_files):
    # The same file under two keys, which is all a link is.
    zone_files(settings.TIME_ZONE, PLUS_TWO)
    zone_files("Ox/Copy", PLUS_TWO)
    set_crontab(1, timezone="Ox/Copy")
    output = run()
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == "0 2 * * *"


def test_a_zone_is_found_in_the_tzdata_package_when_it_is_not_on_the_path(
    beat_tables, zone_files
):
    """
    zoneinfo looks on TZPATH and then in the tzdata package, and so does the
    comparison. The row's zone here is on no path, so its data is the
    package's; TIME_ZONE is given a copy of that file, then a different one.
    """
    from importlib import resources

    packaged = resources.files("tzdata.zoneinfo.Etc").joinpath("GMT+5").read_bytes()
    set_crontab(1, timezone="Etc/GMT+5")
    zone_files(settings.TIME_ZONE, packaged)
    output = run()
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == "0 2 * * *"

    zone_files(settings.TIME_ZONE, PLUS_TWO)
    assert refusals(run())["nightly"] == zone_differs("Etc/GMT+5")


def test_zones_that_agree_today_and_differ_later_are_not_one_zone(
    beat_tables, zone_files
):
    """
    The two zones here have the same offset on every date before March
    2030, which is the whole of what comparing offsets on a few dates can
    see. Their rules differ, so one day the row would fire an hour out. The
    command once called such a pair the same, as it did Helsinki and Cairo.
    """
    zone_files(settings.TIME_ZONE, PLUS_TWO)
    zone_files("Ox/ForNow", PLUS_TWO_FOR_NOW)
    for at in (datetime(2026, 1, 1), datetime(2026, 7, 1), datetime(2029, 12, 31)):
        assert at.replace(tzinfo=zoneinfo.ZoneInfo("Ox/ForNow")).utcoffset() == (
            at.replace(tzinfo=zoneinfo.ZoneInfo(settings.TIME_ZONE)).utcoffset()
        )
    set_crontab(1, timezone="Ox/ForNow")
    output = run()
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == zone_differs("Ox/ForNow")
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize(
    "data",
    [None, b"not a timezone", PLUS_TWO[:30]],
    ids=["no-such-key", "not-tzif", "cut-short"],
)
def test_a_zone_python_cannot_load_is_listed(beat_tables, zone_files, data):
    zone_files(settings.TIME_ZONE, PLUS_TWO)
    if data is not None:
        zone_files("Ox/Broken", data)
    set_crontab(1, timezone="Ox/Broken")
    output = run()
    assert refusals(output).get("nightly") == ZONE_UNLOADABLE.format(
        zone="Ox/Broken", source=ZONE_FROM_ROW
    )
    assert "nightly" not in printed_rows(output)


def test_the_same_name_is_not_the_same_zone_until_it_loads(
    beat_tables, zone_files, settings
):
    """
    A zone nothing can load is not a zone a schedule runs in, however
    exactly the row and TIME_ZONE agree on its name. The command once
    compared the two strings when loading failed, and printed the row.
    """
    # With USE_TZ on, so that the database session is not told the name.
    settings.TIME_ZONE = "Ox/Nowhere"
    set_crontab(1, timezone="Ox/Nowhere")
    output = run()
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == ZONE_UNLOADABLE.format(
        zone="Ox/Nowhere", source=ZONE_FROM_ROW
    )


@pytest.mark.parametrize("project", ["Ox/Nowhere", None], ids=["unknown", "none"])
def test_a_time_zone_setting_python_cannot_load_lists_every_crontab(
    beat_tables, zone_files, settings, project
):
    # Nothing can be shown to run in a zone that does not load, so no
    # crontab is matched to it. An interval has no zone to match.
    zone_files("Ox/Fixed", PLUS_TWO)
    set_crontab(1, timezone="Ox/Fixed")
    settings.TIME_ZONE = project
    output = run()
    assert refusals(output).get("nightly") == PROJECT_ZONE_UNLOADABLE.format(
        project=project
    )
    assert sorted(printed_rows(output)) == ["poller"]


def test_a_zone_whose_file_has_gone_is_listed(beat_tables, zone_files):
    """
    zoneinfo keeps a zone it has loaded, so a key can load after its file
    has left the path. With no file to compare there is nothing to show the
    two zones are one, and the row is listed.
    """
    zone_files(settings.TIME_ZONE, PLUS_TWO)
    gone = zone_files("Ox/Gone", PLUS_TWO)
    zoneinfo.ZoneInfo("Ox/Gone")
    gone.unlink()
    set_crontab(1, timezone="Ox/Gone")
    output = run()
    assert refusals(output).get("nightly") == ZONE_DATA_MISSING.format(
        zone="Ox/Gone", source=ZONE_FROM_ROW, project=settings.TIME_ZONE
    )
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize("stored", [None, ""], ids=["null", "empty"])
def test_a_crontab_whose_zone_is_empty_is_listed_whatever_the_operator_says(
    beat_tables, settings, stored
):
    """
    The command once read an empty zone as TIME_ZONE. beat 2.9.0 does not
    run it at all: loading its schedule reads every crontab's zone, and an
    empty one raises there, on every load, so beat ran nothing from the
    table. No option can name a zone for that, and a setting called
    CELERY_TIMEZONE is not consulted either. The row is listed, and the
    output says first that beat ran none of the others.
    """
    settings.CELERY_TIMEZONE = settings.TIME_ZONE
    set_crontab(1, timezone=stored)
    for options in ({}, {"beat_timezone": settings.TIME_ZONE}):
        output = run(**options)
        assert "nightly" in refusals(output)
        assert refusals(output)["nightly"] == ZONE_EMPTY
        assert "nightly" not in printed_rows(output)
        assert SOURCE_STOPPED_ZONE.format(ids="1") in output.splitlines()
        assert sorted(printed_rows(output)) == ["poller"]


def test_a_crontab_table_older_than_the_zone_column_needs_the_option(settings):
    """
    django-celery-beat has carried a per-schedule zone since 2018. A table
    from before it ran every crontab in the Celery app's timezone, which is
    in Celery's configuration and not in the table. A setting called
    CELERY_TIMEZONE is not evidence of it: the name depends on the app's
    namespace. So the rows wait for the operator to say.
    """
    settings.CELERY_TIMEZONE = settings.TIME_ZONE
    try:
        make_beat_tables(crontab_zone=False)
        without = run()
        assert refusals(without).get("nightly") == ZONE_MISSING
        assert "nightly" not in printed_rows(without)
        given = run(beat_timezone=settings.TIME_ZONE)
        elsewhere = run(beat_timezone="Asia/Tokyo")
    finally:
        drop_beat_tables()
    assert printed_rows(given)["nightly"]["cron"] == "0 2 * * *"
    assert refusals(elsewhere)["nightly"] == zone_differs(
        "Asia/Tokyo", "from --beat-timezone"
    )


@pytest.fixture(params=["use-tz-on-in-utc", "use-tz-off"])
def tz_aware_off(request, settings):
    """
    beat's DJANGO_CELERY_BEAT_TZ_AWARE off, in each of the two
    configurations a row is translated from with it off: USE_TZ on where
    local time is UTC, and USE_TZ off, anywhere. Returns TIME_ZONE.
    """
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    if request.param == "use-tz-off":
        settings.USE_TZ = False
        settings.TIME_ZONE = "Asia/Kolkata"
    else:
        settings.TIME_ZONE = "UTC"
    return settings.TIME_ZONE


def test_with_tz_aware_off_the_rows_own_zone_is_not_what_beat_used(
    beat_tables, tz_aware_off
):
    """
    With DJANGO_CELERY_BEAT_TZ_AWARE off beat builds a plain crontab and
    never looks at the row's zone. So a row whose zone matches TIME_ZONE
    proves nothing, and a row in Tokyo is fine if the app ran in TIME_ZONE.
    """
    insert_crontab(2, hour="3", timezone="Asia/Tokyo")
    insert_task(4, "tokyo", "reports.tasks.daily", crontab_id=2)

    output = run()
    assert refusals(output).get("nightly") == ZONE_IGNORED
    assert refusals(output).get("tokyo") == ZONE_IGNORED
    assert sorted(printed_rows(output)) == ["poller"]

    rows = printed_rows(run(beat_timezone=tz_aware_off))
    assert rows["nightly"]["cron"] == "0 2 * * *"
    assert rows["tokyo"]["cron"] == "0 3 * * *"

    output = run(beat_timezone="Asia/Tokyo")
    for name in ("nightly", "tokyo"):
        assert refusals(output)[name] == zone_differs(
            "Asia/Tokyo", "from --beat-timezone"
        )


@pytest.mark.parametrize(
    "stored", [None, "", "Mars/Phobos"], ids=["null", "empty", "none-such"]
)
def test_with_tz_aware_off_a_broken_zone_is_still_listed(
    beat_tables, tz_aware_off, stored
):
    """
    With DJANGO_CELERY_BEAT_TZ_AWARE off beat runs a crontab in the Celery
    app's timezone and its due check never reads the row's own. But beat
    2.9.0 reads every crontab's zone as it loads its schedule, to filter it
    by hour, and an empty one, or one that does not load, raises there on
    every load: nothing in the table ran. So the row is listed with the
    option as without it, and the output says so of the rest.
    """
    set_crontab(1, timezone=stored)
    reason = ZONE_COLUMN_UNLOADABLE.format(zone=stored) if stored else ZONE_EMPTY
    for options in ({}, {"beat_timezone": tz_aware_off}):
        output = run(**options)
        assert refusals(output).get("nightly") == reason
        assert "nightly" not in printed_rows(output)
        assert SOURCE_STOPPED_ZONE.format(ids="1") in output.splitlines()


def test_a_broken_zone_no_row_uses_still_stopped_beat(beat_tables):
    # Every crontab's zone is read, one that no task uses included.
    insert_crontab(7, timezone="")
    output = run()
    assert output.splitlines()[6] == SOURCE_STOPPED_ZONE.format(ids="7")
    assert sorted(printed_rows(output)) == ["nightly", "poller"]
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET timezone = %s WHERE id = 7",
            [settings.TIME_ZONE],
        )
    assert "ran nothing" not in run()


@pytest.mark.parametrize("value", [0, None, ""], ids=["zero", "none", "empty"])
def test_tz_aware_is_read_the_way_beat_reads_it(beat_tables, settings, value):
    # beat asks whether the setting is true, not whether it is False.
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = value
    assert refusals(run()).get("nightly") == ZONE_IGNORED


def crontab_not_kept(project):
    return CRONTAB_NOT_KEPT.format(project=project)


def interval_not_supported(project):
    return INTERVAL_NOT_SUPPORTED.format(project=project)


BOUNDS = pytest.mark.parametrize(
    ("start", "expires"),
    [
        ("2020-01-01 00:00:00", None),
        ("2099-01-01 00:00:00", None),
        (None, "2099-12-31 10:00:00"),
        (None, "2020-01-01 00:00:00"),
        ("2020-01-01 00:00:00", "2099-12-31 10:00:00"),
    ],
    ids=["past-start", "future-start", "expiry", "past-expiry", "both"],
)


@pytest.mark.parametrize("zone", ["UTC", "Asia/Kolkata"])
@BOUNDS
def test_with_use_tz_and_without_beat_tz_aware_a_row_with_a_bound_is_listed(
    beat_tables, settings, zone, start, expires
):
    """
    beat reads the clock as naive UTC under this pair of settings and the
    stored bound is aware, so comparing them raised TypeError on every tick,
    before the bound and after it, in UTC as anywhere. A crontab and an
    interval alike: the comparison comes before the schedule is asked.
    """
    settings.TIME_ZONE = zone
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone=zone)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id IN (1, 2)",
            [start, expires],
        )
    output = run()
    assert refusals(output).get("nightly") == AWARE_BOUND_BEAT_NAIVE
    assert refusals(output).get("poller") == AWARE_BOUND_BEAT_NAIVE
    assert printed_rows(output) == {}


@pytest.mark.parametrize(
    "zone", ["Asia/Kolkata", "America/New_York", "Europe/London", "Etc/GMT"]
)
def test_with_use_tz_and_without_beat_tz_aware_no_row_is_translated_outside_utc(
    beat_tables, settings, zone
):
    """
    Under this pair of settings beat writes each run as naive UTC and Django
    stores a naive value as local time, so beat reads every run back off by
    the UTC offset. In Kolkata a daily crontab fired 67 extra times a day;
    in New York one due every ten minutes fired 12 times in 288. Only where
    local time is UTC did beat keep to a crontab, so anywhere else there are
    no times to carry over. Where local time is UTC is decided by the zone's
    data, so London and GMT are listed too, and the reason states the rule
    without claiming that beat was seen to misfire in either. An interval
    was not run in this configuration. It is listed on the same boundary,
    and its reason says so.
    """
    settings.TIME_ZONE = zone
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone=zone)
    output = run()
    assert refusals(output).get("nightly") == crontab_not_kept(zone)
    assert refusals(output).get("poller") == interval_not_supported(zone)
    assert "beat was not run with one" in refusals(output)["poller"]
    assert printed_rows(output) == {}
    assert "SCHEDULABLE_TASKS" not in output


def test_with_use_tz_and_without_beat_tz_aware_rows_are_translated_in_utc(
    beat_tables, settings, zone_files
):
    """
    Where local time is UTC the shift is nothing and beat kept to the
    crontab. Decided by the zone's data, as everywhere here: TIME_ZONE is
    given UTC's own file under another name, then one that differs.
    """
    zone_files("UTC", ZERO)
    zone_files("Ox/Utc", ZERO)
    settings.TIME_ZONE = "Ox/Utc"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    output = run()
    assert refusals(output).get("nightly") == ZONE_IGNORED
    assert sorted(printed_rows(output)) == ["poller"]
    output = run(beat_timezone="Ox/Utc")
    assert sorted(printed_rows(output)) == ["nightly", "poller"]

    zone_files("Ox/Utc", tzif((0, "OXR")))
    output = run(beat_timezone="Ox/Utc")
    assert refusals(output).get("nightly") == crontab_not_kept("Ox/Utc")
    assert refusals(output).get("poller") == interval_not_supported("Ox/Utc")


def test_local_time_is_not_utc_where_neither_zone_file_can_be_found(
    beat_tables, settings, zone_files, monkeypatch
):
    """
    Local time is UTC where TIME_ZONE's file and UTC's are the same bytes.
    Here neither can be found: TIME_ZONE's has left the path since zoneinfo
    loaded it, UTC's was never on it, and there is no tzdata package to
    fall back on. Two absences are not a match, so the rows are listed, and
    for that: no data was compared, so none is said to differ.
    """
    from importlib import resources

    def no_package(name):
        raise ModuleNotFoundError(name)

    gone = zone_files("Ox/Local", ZERO)
    zoneinfo.ZoneInfo("Ox/Local")
    gone.unlink()
    monkeypatch.setattr(resources, "files", no_package)
    settings.TIME_ZONE = "Ox/Local"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    output = run(beat_timezone="Ox/Local")
    assert set(refusals(output)) == {"nightly", "poller", "orphan"}
    unavailable = PROJECT_ZONE_COMPARISON_UNAVAILABLE.format(project="Ox/Local")
    assert refusals(output)["nightly"] == unavailable
    assert refusals(output)["poller"] == unavailable
    assert printed_rows(output) == {}


@pytest.mark.parametrize("use_tz", [True, False], ids=["use-tz-on", "use-tz-off"])
def test_a_comparison_with_utc_that_cannot_be_made_is_said_so(
    beat_tables, settings, zone_files, monkeypatch, use_tz
):
    """
    TIME_ZONE's file is on the path, under a name PostgreSQL also knows
    (with USE_TZ off it is told TIME_ZONE); UTC's is on neither the path nor
    a tzdata package. The zone loads, and nothing compared its data with
    UTC's: each row that comparison decides is listed for that, with USE_TZ
    on (a crontab and an interval) and off (an expiry).
    """
    from importlib import resources

    def no_package(name):
        raise ModuleNotFoundError(name)

    zone_files("Europe/Helsinki", PLUS_TWO)
    monkeypatch.setattr(resources, "files", no_package)
    unavailable = PROJECT_ZONE_COMPARISON_UNAVAILABLE.format(project="Europe/Helsinki")
    if use_tz:
        settings.USE_TZ = True
        settings.TIME_ZONE = "Europe/Helsinki"
        settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
        output = run(beat_timezone="Europe/Helsinki")
        assert refusals(output)["nightly"] == unavailable
        assert refusals(output)["poller"] == unavailable
    else:
        naive_project(settings, "Europe/Helsinki", beat_tz_aware=False)
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
                ["2099-12-31 10:00:00"],
            )
        output = run(beat_timezone="Europe/Helsinki")
        assert refusals(output)["nightly"] == unavailable
        assert sorted(printed_rows(output)) == ["poller"]


def test_with_use_tz_and_without_beat_tz_aware_a_time_zone_that_does_not_load_is_said(
    beat_tables, settings
):
    """
    Whether TIME_ZONE's data is UTC's cannot be asked of a zone that does
    not load. The rows are listed all the same, and for that: the reason
    they are listed for elsewhere says the data differs from UTC's, which
    nothing here showed.
    """
    settings.TIME_ZONE = "Ox/Nowhere"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    output = run(beat_timezone="UTC")
    unloadable = PROJECT_ZONE_UNLOADABLE.format(project="Ox/Nowhere")
    assert refusals(output).get("nightly") == unloadable
    assert refusals(output).get("poller") == unloadable
    assert printed_rows(output) == {}


#: What is said of a crontab with USE_TZ off and beat's own setting on, by
#: the database beat ran on: what was seen there, what was read of it, or
#: that nothing was.
BEAT_DID_NOT_RUN_ON = {
    "sqlite": BEAT_STOPPED,
    "mysql": BEAT_SAVE_REFUSED.format(database="MySQL"),
    "postgresql": BEAT_NOT_ESTABLISHED.format(database="PostgreSQL"),
}


@pytest.mark.parametrize("beat_tz_aware", [True, None], ids=["on", "unset"])
@pytest.mark.parametrize("zone", ["UTC", "Asia/Kolkata"])
def test_without_use_tz_and_with_beat_tz_aware_no_row_is_translated(
    beat_tables, settings, zone, beat_tz_aware
):
    """
    With USE_TZ off and beat's own setting on, which is where it stands
    when nobody set it, beat sends a row once and then stops: the time it
    saves for the run is aware, and Django's SQLite backend refuses that
    with USE_TZ off. So it kept to no crontab, in UTC as anywhere, and no
    row is translated. That was seen on SQLite. Django's MySQL backend
    carries the same refusal and was not run; PostgreSQL's takes the value,
    and what beat then did there is not known. Each sentence says which of
    those it is, and an interval's says that none was run.
    """
    settings.USE_TZ = False
    settings.TIME_ZONE = zone
    if beat_tz_aware is not None:
        settings.DJANGO_CELERY_BEAT_TZ_AWARE = beat_tz_aware
    assert getattr(settings, "DJANGO_CELERY_BEAT_TZ_AWARE", "unset") in (True, "unset")
    set_crontab(1, timezone=zone)
    output = run()
    assert refusals(output) == {
        "nightly": BEAT_DID_NOT_RUN_ON[connection.vendor],
        "poller": INTERVAL_NOT_RUN,
        "orphan": "solar and clocked schedules have no equivalent",
    }
    assert printed_rows(output) == {}
    assert all(line.startswith("#") or not line for line in output.splitlines())


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
def test_the_sentence_is_for_the_database_beat_ran_on(settings):
    # --database names it. Here that is the SQLite alias on every leg,
    # whatever the database the schedules would be written to.
    settings.USE_TZ = False
    try:
        make_beat_tables(db="alt")
        output = run(database="alt")
    finally:
        drop_beat_tables("alt")
    assert refusals(output).get("nightly") == BEAT_DID_NOT_RUN_ON["sqlite"]


@pytest.mark.parametrize(
    ("vendor", "said"),
    [
        ("oracle", BEAT_SAVE_REFUSED.format(database="Oracle")),
        ("firebird", BEAT_NOT_ESTABLISHED.format(database="'firebird'")),
    ],
    ids=["oracle", "another"],
)
def test_the_sentence_for_a_database_no_leg_here_runs_on(settings, vendor, said):
    """
    Oracle's backend carries the refusal SQLite's does, which was read and
    not run, and its sentence says that much. Any other database gets the
    sentence that claims nothing. Neither can be reached through the
    command here, so the rule is asked directly with a connection that says
    which it is.
    """
    from types import SimpleNamespace

    from django_ox.management.commands.ox_import_beat_schedules import (
        Command,
        _Refused,
        _Zones,
    )

    settings.USE_TZ = False
    row = {"_start_time": None, "_expires": None}
    with pytest.raises(_Refused) as refused:
        Command._settings(row, "cron", _Zones(None), SimpleNamespace(vendor=vendor))
    assert refused.value.reason == said


def test_a_beat_timezone_python_cannot_load_is_refused_before_anything_is_read(
    monkeypatch,
):
    """
    A mistyped zone would otherwise come back as a refusal on every row it
    applies to. It is a mistake in the command line, and it is reported as
    one, before the database is asked anything.
    """

    def read(*args, **kwargs):
        raise AssertionError("the database was read")

    monkeypatch.setattr(connection.introspection, "table_names", read)
    error, printed = import_fails(beat_timezone="Europe/Berlim")
    assert str(error) == BEAT_TIMEZONE_UNLOADABLE.format(zone="Europe/Berlim")
    assert printed == ""


def test_the_beat_timezone_is_an_option_of_the_real_command():
    # As typed, through the parser, rather than as a keyword handed past it.
    # On a table from before crontabs had a zone, which is what needs it.
    out = StringIO()
    try:
        make_beat_tables(crontab_zone=False)
        call_command(
            "ox_import_beat_schedules",
            "--beat-timezone",
            settings.TIME_ZONE,
            stdout=out,
        )
        with pytest.raises(CommandError, match="--beat-timezone 'Nowhere/At_All'"):
            call_command("ox_import_beat_schedules", "--beat-timezone=Nowhere/At_All")
    finally:
        drop_beat_tables()
    assert printed_rows(out.getvalue())["nightly"]["cron"] == "0 2 * * *"


def test_the_help_says_what_the_beat_timezone_is_and_is_not_for():
    # As --help prints it, which wraps the sentence to the terminal's width.
    from django_ox.management.commands.ox_import_beat_schedules import Command

    parser = Command().create_parser("manage.py", "ox_import_beat_schedules")
    assert BEAT_TIMEZONE_HELP in " ".join(parser.format_help().split())


def typed(*arguments):
    """The command as it is typed, its options through the parser."""
    out = StringIO()
    call_command("ox_import_beat_schedules", *arguments, stdout=out)
    return out.getvalue()


def test_a_beat_timezone_no_row_needed_is_said_not_to_have_been_needed(beat_tables):
    """
    The option names the zone of crontabs beat ran in the Celery app's
    zone. Here every crontab has a zone of its own and beat's setting is
    where it stands when nobody set it, on, so beat used each row's zone
    and the option changes nothing. Taken without a word, a zone that did
    nothing would be believed to have been applied, so one comment line
    under the opening note says so, and
    nothing else in the output differs.
    """
    without = typed().splitlines()
    given = typed("--beat-timezone", "Asia/Tokyo").splitlines()
    assert BEAT_TIMEZONE_UNUSED not in without
    assert given[6:8] == [BEAT_TIMEZONE_UNUSED, ""]
    assert [*given[:6], *given[7:]] == without
    assert sorted(printed_rows("\n".join(given))) == ["nightly", "poller"]


def test_a_beat_timezone_is_not_called_unneeded_where_no_crontab_row_was_read(
    beat_tables,
):
    # "Every crontab row has its own timezone" says nothing of a table with
    # none, so the line is not printed there.
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id = 1")
    output = typed("--beat-timezone", "Asia/Tokyo")
    assert BEAT_TIMEZONE_UNUSED not in output
    assert sorted(printed_rows(output)) == ["poller"]


def test_a_beat_timezone_a_table_without_zones_needed_is_not_called_unneeded():
    # A table from before a crontab had a zone: the option is the zone the
    # crontab is read in.
    try:
        make_beat_tables(crontab_zone=False)
        output = typed("--beat-timezone", settings.TIME_ZONE)
    finally:
        drop_beat_tables()
    assert printed_rows(output)["nightly"]["cron"] == "0 2 * * *"
    assert BEAT_TIMEZONE_UNUSED not in output


def test_a_beat_timezone_needed_with_tz_aware_off_is_not_called_unneeded(
    beat_tables, tz_aware_off
):
    # With beat's own setting off the option is the zone every crontab is
    # read in, whatever zone the row has.
    output = typed("--beat-timezone", tz_aware_off)
    assert printed_rows(output)["nightly"]["cron"] == "0 2 * * *"
    assert BEAT_TIMEZONE_UNUSED not in output


@pytest.mark.parametrize("stored", [None, ""], ids=["null", "empty"])
@pytest.mark.parametrize("one_off", [False, True], ids=["listed-for-it", "one-off"])
def test_a_beat_timezone_is_not_called_unneeded_beside_a_crontab_with_no_zone(
    beat_tables, stored, one_off
):
    """
    The line gives its reason: every crontab row has a zone of its own.
    One whose zone is empty has not. The option cannot stand in for that
    zone either, which the row's own reason says, but the line would be
    false, so it is not printed. Whether the row got as far as having its
    zone looked at makes no difference: a one-off row is listed before.
    """
    set_crontab(1, timezone=stored)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET one_off = %s WHERE id = 1",
            [one_off],
        )
    output = typed("--beat-timezone", settings.TIME_ZONE)
    assert "nightly" in refusals(output)
    assert BEAT_TIMEZONE_UNUSED not in output


def test_a_beat_timezone_is_not_called_unneeded_on_a_table_without_zones():
    # Nor where no crontab has a zone column to have one in, and the only
    # crontab row was listed before its zone was asked for.
    try:
        make_beat_tables(crontab_zone=False)
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET one_off = %s WHERE id = 1",
                [True],
            )
        output = typed("--beat-timezone", settings.TIME_ZONE)
    finally:
        drop_beat_tables()
    assert refusals(output)["nightly"] == (
        "one-off tasks have no equivalent on a stored schedule"
    )
    assert BEAT_TIMEZONE_UNUSED not in output


def test_a_beat_timezone_is_not_called_unneeded_with_tz_aware_off(
    beat_tables, settings
):
    """
    With beat's own setting off and no crontab among the rows, no row took
    its zone from the option either. The line says the setting is on, so it
    is not printed here: nothing is said of the option at all.
    """
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id = 1")
    output = typed("--beat-timezone", "UTC")
    assert sorted(printed_rows(output)) == ["poller"]
    assert BEAT_TIMEZONE_UNUSED not in output


@pytest.mark.parametrize(
    ("stored", "cron"),
    [
        # A range that ends below its start wraps, to Celery.
        ({"hour": "22-2"}, "0 0-2,22-23 * * *"),
        ({"day_of_week": "fri-mon"}, "0 2 * * 0-1,5-6"),
        ({"month_of_year": "11-2"}, "0 2 * 1-2,11-12 *"),
        # A name is its first three letters, however long it is written.
        ({"day_of_week": "monday"}, "0 2 * * 1"),
        ({"day_of_week": "Sunday,WEDNESDAY"}, "0 2 * * 0,3"),
        ({"month_of_year": "january-march"}, "0 2 * 1-3 *"),
        # In any field: jan is 1 as a minute, and as a day of the week mar
        # is 3, Wednesday.
        ({"minute": "jan"}, "1 2 * * *"),
        ({"day_of_week": "mar"}, "0 2 * * 3"),
        # A number is whatever int() reads.
        ({"minute": " 15"}, "15 2 * * *"),
        ({"minute": "+5,07,1_0"}, "5,7,10 2 * * *"),
        # What follows a range or a step Celery recognised is ignored.
        ({"hour": "1-5-7"}, "0 1-5 * * *"),
        ({"minute": "*/20 or so"}, "*/20 2 * * *"),
        # A step counts along the range, so a wrapped one is stepped in the
        # order it was walked.
        ({"hour": "22-2/2"}, "0 0,2,22 * * *"),
        ({"day_of_week": "mon-fri/2"}, "0 2 * * 1-5/2"),
        # A field that covers its whole range restricts nothing, however it
        # is spelled, and prints as the bare star django-ox reads that way.
        ({"day_of_month": "1-31", "day_of_week": "1"}, "0 2 * * 1"),
        ({"day_of_month": "1-7", "day_of_week": "*/1"}, "0 2 1-7 * *"),
        ({"day_of_month": "1-7", "day_of_week": "0-6"}, "0 2 1-7 * *"),
        ({"day_of_month": "1-7", "day_of_week": "sun-sat"}, "0 2 1-7 * *"),
        ({"hour": "0-23", "minute": "*/1"}, "* * * * *"),
        # A step over the whole field stays a step.
        ({"minute": "*/2"}, "*/2 2 * * *"),
        ({"minute": "0,15,30,45", "hour": "*/6"}, "*/15 */6 * * *"),
        ({"day_of_month": "*/15"}, "0 2 */15 * *"),
        ({"day_of_week": "*/2"}, "0 2 * * */2"),
        # A step that starts off the field's first value is a stepped range,
        # however it was stored, and ends on the last value it reaches.
        ({"minute": "1-59/2"}, "1-59/2 2 * * *"),
        ({"day_of_month": "2-31/2"}, "0 2 2-30/2 * *"),
        ({"minute": "5,20,35,50"}, "5-50/15 2 * * *"),
        (
            {"minute": "1-59/2", "hour": "1-23/2", "day_of_month": "1-31/2"},
            "1-59/2 1-23/2 */2 * *",
        ),
        # Two values are a pair, whatever step would also say them.
        ({"day_of_week": "0,6"}, "0 2 * * 0,6"),
        ({"day_of_month": "1,31"}, "0 2 1,31 * *"),
        ({"hour": "*/12"}, "0 0,12 * * *"),
        ({"month_of_year": "*/6"}, "0 2 * 1,7 *"),
        # Stretches side by side, each written as what it is. The second of
        # a pair may begin the next one.
        ({"minute": "0-20/10,21-23,40"}, "0-20/10,21-23,40 2 * * *"),
        ({"hour": "0,8,9,10"}, "0 0,8-10 * * *"),
        ({"minute": "0,1,3,5,7"}, "0-1,3-7/2 2 * * *"),
    ],
    ids=[
        "wrap-hours",
        "wrap-weekdays",
        "wrap-months",
        "full-name",
        "full-names",
        "full-name-range",
        "month-as-minute",
        "month-as-weekday",
        "space",
        "plus-zero-underscore",
        "junk-after-range",
        "junk-after-step",
        "wrapped-step",
        "stepped-names",
        "whole-days-of-month",
        "whole-week-as-step",
        "whole-week-as-range",
        "whole-week-as-names",
        "whole-day",
        "step",
        "list-that-is-a-step",
        "day-of-month-step",
        "weekday-step",
        "odd-minutes",
        "even-days",
        "list-that-is-a-stepped-range",
        "three-stepped-ranges",
        "weekend",
        "first-and-last-day",
        "twice-a-day",
        "twice-a-year",
        "step-run-and-value",
        "pair-then-run",
        "run-then-step",
    ],
)
def test_a_crontab_is_written_from_what_celery_fires_on(
    beat_tables, schedulable, stored, cron
):
    """
    Not from the stored strings. Celery's grammar is not cron's: the command
    once printed "0 22-2 * * *" and "0 9 * * monday" as they stood, and each
    was refused when the output was pasted, after earlier rows had been
    created. What is printed now is created.
    """
    set_crontab(1, **stored)
    output = run()
    assert f"cron={cron!a}" in output
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == cron
    run_as_module(section_2(output))
    assert OxSchedule.objects.get(name="nightly").cron == cron


@pytest.mark.parametrize(
    ("column", "stored"),
    [
        # Sunday is 0 to Celery, and 7 is out of range.
        ("day_of_week", "7"),
        ("day_of_week", "5-7"),
        # A bare value with a step is not Celery's syntax.
        ("minute", "5/15"),
        ("minute", ""),
        ("minute", "0,,30"),
        ("minute", "*/0"),
        ("minute", "*/"),
        ("minute", "60"),
        ("minute", "-0"),
        ("hour", "24"),
        ("hour", "noon"),
        ("day_of_month", "0"),
        ("day_of_month", "32"),
        ("month_of_year", "13"),
        # As a month, sun is 0.
        ("month_of_year", "sun"),
        # As a day of the week, dec is 12.
        ("day_of_week", "dec"),
        ("minute", "* "),
    ],
)
def test_a_crontab_celery_itself_refuses_is_listed(beat_tables, column, stored):
    """
    django-celery-beat validates a crontab in its admin and nowhere else,
    so a table can hold one Celery refuses to build. beat never ran it.
    Several of these django-ox would accept: 7 as Sunday, 5/15, a trailing
    space gone. Printing them would start a schedule that never ran.
    """
    set_crontab(1, **{column: stored})
    output = run()
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == FIELD_NOT_CELERY.format(
        column=column, text=stored
    )
    assert "nightly" not in printed_rows(output)


def test_a_crontab_field_that_is_not_text_is_listed(beat_tables):
    set_crontab(1, hour=None)
    output = run()
    assert refusals(output).get("nightly") == FIELD_NOT_TEXT.format(column="hour")
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize(
    ("day_of_month", "day_of_week"),
    [("1-7", "mon"), ("1-7", "1"), ("*/2", "mon"), ("15", "fri"), ("1-30", "0-5")],
)
def test_a_crontab_that_narrows_both_day_fields_is_listed(
    beat_tables, day_of_month, day_of_week
):
    """
    "The first Monday of the month" to Celery, which needs the day to be in
    both fields. django-ox runs on a day in either, so the same expression
    here is every Monday and each of the first seven days: 124 runs a year
    where beat made 12. No five-field expression says both, so the row is
    listed rather than printed as something it is not.
    """
    set_crontab(1, day_of_month=day_of_month, day_of_week=day_of_week)
    output = run()
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == BOTH_DAY_FIELDS
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize(
    ("day_of_month", "month_of_year", "day_of_week", "cron"),
    [
        # Mondays in February: every date of February is in 1-30.
        ("1-30", "2", "1", "0 2 * 2 1"),
        ("1-29", "2", "mon", "0 2 * 2 1"),
        # Weekdays of the months with thirty days.
        ("1-30", "4,6,9,11", "1-5", "0 2 * 4,6,9,11 1-5"),
        # A leap year's February runs on the 29th, and 1-29 holds it.
        ("1-29", "feb", "0", "0 2 * 2 0"),
        # Months of thirty and of twenty-nine days together.
        ("1-30", "2,4", "sat,sun", "0 2 * 2,4 0,6"),
        # Days past the end of every month it runs in, and none missing.
        ("*/1", "2", "1", "0 2 * 2 1"),
        ("1-15,16-31", "jan", "1", "0 2 * 1 1"),
    ],
    ids=[
        "february-1-30",
        "february-1-29",
        "thirty-day-months",
        "leap-day-included",
        "mixed-short-months",
        "every-day-stepped",
        "two-ranges",
    ],
)
def test_a_day_of_month_that_leaves_out_no_date_of_its_months_narrows_nothing(
    beat_tables, day_of_month, month_of_year, day_of_week, cron
):
    """
    Celery runs on a date in all three of its day and month sets. 1-30 in
    February leaves out no date February has, so it decides nothing and
    the weekday field alone narrows the days: Mondays in February. A
    day-of-month set that is whole on the calendar does not count as
    narrowing, even where it is not whole as numbers. It is
    written with "*" for the day of the month, which django-ox reads the
    same way Celery reads the row.
    """
    set_crontab(
        1,
        day_of_month=day_of_month,
        month_of_year=month_of_year,
        day_of_week=day_of_week,
    )
    output = run()
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == cron


@pytest.mark.parametrize(
    ("day_of_month", "month_of_year", "day_of_week"),
    [
        # The 29th of a leap February is missing.
        ("1-28", "2", "1"),
        # The 30th of April is missing.
        ("1-29", "4", "1"),
        # January has a 31st, April does not: one month is enough.
        ("1-30", "1,4", "1"),
        # The 1st is missing.
        ("2-31", "2", "1"),
        ("1-30", "*", "1"),
        ("*/2", "2", "1"),
    ],
    ids=[
        "leap-day-missing",
        "thirtieth-missing",
        "one-month-of-31",
        "first-missing",
        "every-month",
        "every-other-day",
    ],
)
def test_a_day_of_month_that_leaves_out_a_date_of_its_months_still_narrows(
    beat_tables, day_of_month, month_of_year, day_of_week
):
    set_crontab(
        1,
        day_of_month=day_of_month,
        month_of_year=month_of_year,
        day_of_week=day_of_week,
    )
    output = run()
    assert refusals(output).get("nightly") == BOTH_DAY_FIELDS
    assert "nightly" not in printed_rows(output)


def test_a_redundant_day_of_month_is_written_as_it_was_when_the_weekday_is_whole(
    beat_tables,
):
    """
    Only where the weekday field narrows too does the redundant field
    become "*": with every weekday, 1-30 in April reads the same in both
    and is printed as the values it is.
    """
    set_crontab(1, day_of_month="1-30", month_of_year="4")
    output = run()
    assert printed_rows(output)["nightly"]["cron"] == "0 2 1-30 4 *"


def test_a_crontab_no_date_satisfies_is_listed(beat_tables):
    # Celery builds a crontab for the 31st of February and never runs it.
    # django-ox refuses the expression, and says why.
    set_crontab(1, day_of_month="31", month_of_year="2")
    output = run()
    assert refusals(output).get("nightly") == (
        "its crontab would be written '0 2 31 2 *', which django-ox refuses: "
        "\"Cron expression '0 2 31 2 *' can never match: no listed month has any "
        'of the listed days."'
    )


def test_an_expression_django_ox_reads_differently_is_listed(beat_tables, monkeypatch):
    """
    What is printed is parsed again by the parser that will run it, and has
    to come back as the sets Celery fired on. Nothing known makes the two
    differ, which is the reason to check: here the writer is made to drop
    the hour, as a defect in it one day might.
    """
    from django_ox.management import _beat_cron

    monkeypatch.setattr(_beat_cron, "expression", lambda fields: "0 * * * *")
    output = run()
    assert refusals(output).get("nightly") == CRON_READS_DIFFERENTLY.format(
        cron="0 * * * *"
    )
    assert "nightly" not in printed_rows(output)


def repeated_run(date):
    return REPEATED_RUN.format(date=date)


def horizon(project):
    return HORIZON.format(project=project) + "\n"


#: The clock changes of 2027 that both engines were run through, as zones
#: built here so that no tz database decides them: the time each goes back
#: at, and the offsets either side of it, in seconds.
#: Berlin: 03:00 back to 02:00 on 31 October, after going forward in March.
BERLIN_2027 = tzif(
    (3600, "OXS"), (7200, "OXD"), changes=[(1806195600, 1), (1824944400, 0)]
)
#: New York: 02:00 back to 01:00 on 7 November.
NEW_YORK_2027 = tzif((-14400, "OXD"), (-18000, "OXS"), changes=[(1825567200, 1)])
#: Havana: 01:00 back to 00:00 on 7 November.
HAVANA_2027 = tzif((-14400, "OXD"), (-18000, "OXS"), changes=[(1825563600, 1)])


@pytest.mark.parametrize(
    ("zone", "stored", "date"),
    [
        (BERLIN_2027, {"minute": "30", "hour": "2"}, "2027-10-31"),
        (BERLIN_2027, {"minute": "0", "hour": "*"}, "2027-10-31"),
        (BERLIN_2027, {"minute": "*/15", "hour": "*"}, "2027-10-31"),
        (BERLIN_2027, {"minute": "0", "hour": "2"}, "2027-10-31"),
        (BERLIN_2027, {"minute": "30", "hour": "2,3"}, "2027-10-31"),
        # Held to the night of the change by a day field, and still on it.
        (
            BERLIN_2027,
            {"minute": "30", "hour": "2", "day_of_week": "sun"},
            "2027-10-31",
        ),
        (
            BERLIN_2027,
            {"minute": "30", "hour": "2", "day_of_month": "31"},
            "2027-10-31",
        ),
        # The instant the repeated hour ends happens once, and so does
        # anything before it starts.
        (BERLIN_2027, {"minute": "0", "hour": "3"}, None),
        (BERLIN_2027, {"minute": "30", "hour": "1"}, None),
        # Kept off the night of the change by a day or a month field.
        (BERLIN_2027, {"minute": "30", "hour": "2", "day_of_week": "mon-sat"}, None),
        (BERLIN_2027, {"minute": "30", "hour": "2", "day_of_month": "1-30"}, None),
        (BERLIN_2027, {"minute": "30", "hour": "2", "month_of_year": "1-9"}, None),
        (NEW_YORK_2027, {"minute": "30", "hour": "1"}, "2027-11-07"),
        (NEW_YORK_2027, {"minute": "0", "hour": "*"}, "2027-11-07"),
        (NEW_YORK_2027, {"minute": "*/15", "hour": "*"}, "2027-11-07"),
        (NEW_YORK_2027, {"minute": "0", "hour": "1"}, "2027-11-07"),
        (NEW_YORK_2027, {"minute": "0", "hour": "2"}, None),
        (NEW_YORK_2027, {"minute": "30", "hour": "0"}, None),
        (HAVANA_2027, {"minute": "0", "hour": "0"}, "2027-11-07"),
        (HAVANA_2027, {"minute": "30", "hour": "0"}, "2027-11-07"),
        (HAVANA_2027, {"minute": "0", "hour": "1"}, None),
    ],
    ids=[
        "berlin-30-2",
        "berlin-hourly",
        "berlin-quarter-hourly",
        "berlin-0-2",
        "berlin-30-2,3",
        "berlin-30-2-sunday",
        "berlin-30-2-the-31st",
        "berlin-0-3",
        "berlin-30-1",
        "berlin-30-2-not-sunday",
        "berlin-30-2-not-the-31st",
        "berlin-30-2-not-october",
        "new-york-30-1",
        "new-york-hourly",
        "new-york-quarter-hourly",
        "new-york-0-1",
        "new-york-0-2",
        "new-york-30-0",
        "havana-0-0",
        "havana-30-0",
        "havana-0-1",
    ],
)
def test_a_crontab_with_a_run_in_a_repeated_hour_is_translated_and_named(
    beat_tables, settings, zone_files, clock, schedulable, zone, stored, date
):
    """
    In the hour a clock goes back through twice, a stored schedule fires a
    matching run on both passes and beat fired it on the first only. Both
    engines were run through these nights, and every row here is one that
    was: the ones that differed there are named here with the date, the
    ones that agreed are not. Such a row is still translated. It is one run
    a year for most, and by design here; the reader is told which row and
    which night, and decides.
    """
    zone_files("Ox/Project", zone)
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    set_crontab(1, timezone="Ox/Project", **stored)
    output = run()
    assert differences(output).get("nightly") == (date and repeated_run(date))
    assert "nightly" not in refusals(output)
    assert "nightly" in printed_rows(output)
    run_as_module(section_2(output))
    assert OxSchedule.objects.filter(name="nightly").exists()


@pytest.mark.parametrize(
    ("zone", "stored", "date"),
    [
        ("Europe/Berlin", {"minute": "30", "hour": "2"}, "2010-10-31"),
        ("Europe/Berlin", {"minute": "0", "hour": "3"}, None),
        # The 30th is the night of the change the year after.
        (
            "Europe/Berlin",
            {"minute": "30", "hour": "2", "day_of_month": "30"},
            "2011-10-30",
        ),
        ("America/New_York", {"minute": "30", "hour": "1"}, "2010-11-07"),
        ("America/New_York", {"minute": "0", "hour": "2"}, None),
        ("America/Havana", {"minute": "0", "hour": "0"}, "2010-10-31"),
        # Half an hour, not an hour: back from 02:00 to 01:30.
        ("Australia/Lord_Howe", {"minute": "45", "hour": "1"}, "2010-04-04"),
        ("Australia/Lord_Howe", {"minute": "15", "hour": "1"}, None),
        # No clock change at all.
        ("UTC", {"minute": "*", "hour": "*"}, None),
        ("Asia/Kolkata", {"minute": "*", "hour": "*"}, None),
    ],
    ids=[
        "berlin-30-2",
        "berlin-0-3",
        "berlin-the-30th",
        "new-york-30-1",
        "new-york-0-2",
        "havana-0-0",
        "lord-howe-45-1",
        "lord-howe-15-1",
        "utc",
        "kolkata",
    ],
)
def test_the_clock_changes_are_the_zones_own(
    beat_tables, settings, clock, zone, stored, date
):
    """
    Through the tz database this time, with the import's clock at the start
    of 2010, so that the ten years looked at are history and no later
    change of rules can move them. A zone that never puts its clock back
    names no row, however often the row runs.
    """
    settings.TIME_ZONE = zone
    clock("2010-01-01 00:00:00")
    set_crontab(1, timezone=zone, **stored)
    output = run()
    assert differences(output).get("nightly") == (date and repeated_run(date))
    assert "nightly" in printed_rows(output)


def test_without_use_tz_a_repeated_hour_runs_once_under_both(
    beat_tables, settings, monkeypatch
):
    """
    Without USE_TZ a stored schedule reads its run times on the wall clock,
    and on the second pass of a repeated hour the wall clock shows times it
    has run already: it runs each once, on the first pass, as beat does.
    One created during the second pass is the exception: beat ran the
    night's run times on the first, and it runs those the wall clock still
    has to show. The clock reading is this process's wall clock, New York's
    here.
    """
    naive_project(settings, "America/New_York", beat_tz_aware=False)
    set_crontab(1, minute="30", hour="1")
    for now, said in [
        (datetime(2010, 11, 6, 23, 0), None),
        # 01:10 and 01:40 on the second pass.
        (datetime(2010, 11, 7, 1, 10, fold=1), REPEATED_ONCE.format(date="2010-11-07")),
        (datetime(2010, 11, 7, 1, 40, fold=1), None),
    ]:
        monkeypatch.setattr(timezone, "now", lambda now=now: now)
        output = run(beat_timezone="America/New_York")
        assert differences(output).get("nightly") == said, now


@pytest.mark.parametrize(
    ("minute", "hour", "now", "named"),
    [
        ("30", "2", datetime(2010, 3, 13, 12, 0), True),
        ("45", "2", datetime(2010, 3, 13, 12, 0), True),
        ("0", "2", datetime(2010, 3, 13, 12, 0), False),
        ("0", "*", datetime(2010, 3, 13, 12, 0), False),
        ("*/15", "*", datetime(2010, 3, 13, 12, 0), False),
        ("30", "1", datetime(2010, 3, 13, 12, 0), False),
        ("0", "3", datetime(2010, 3, 13, 12, 0), False),
        # Half an hour before the clocks jump, its start at 01:30 by the
        # earlier offset: 02:00 runs when they land, under both.
        ("0", "2", datetime(2010, 3, 14, 1, 30), False),
        # At the instant they land, on 03:00, a run time of its own.
        ("0", "*", datetime(2010, 3, 14, 3, 0), False),
    ],
    ids=[
        "30-2",
        "45-2",
        "0-2",
        "hourly",
        "quarter-hourly",
        "30-1",
        "0-3",
        "0-2-just-before",
        "hourly-at-the-landing",
    ],
)
def test_without_use_tz_a_run_the_clocks_skip_comes_early_and_is_named(
    beat_tables, settings, monkeypatch, minute, hour, now, named
):
    """
    Without USE_TZ a stored schedule compares wall clock times, so where
    the clocks jump from 02:00 to 03:00 it runs 02:30 as soon as they land,
    at 03:00. beat ran it at 03:30, by the earlier offset. The command names
    this difference. A run time on the hour the clocks leave lands at 03:00
    under both, whenever before it the schedule was created.
    """
    naive_project(settings, "America/New_York", beat_tz_aware=False)
    set_crontab(1, minute=minute, hour=hour)
    monkeypatch.setattr(timezone, "now", lambda: now)
    output = run(beat_timezone="America/New_York")
    expected = SKIPPED_RUN.format(date="2010-03-14") if named else None
    assert differences(output).get("nightly") == expected


@pytest.mark.parametrize(
    ("hours", "date"),
    [(-1, "2037-01-01"), (1, None)],
    ids=["inside-ten-years", "past-ten-years"],
)
def test_clock_changes_are_looked_for_ten_years_ahead_and_no_further(
    beat_tables, settings, zone_files, clock, hours, date
):
    """
    A zone whose one change is an hour inside the ten years, then one whose
    change is an hour past them. The second names no row, which is why the
    output says how far it looked: no notice is not a promise about 2038.
    """
    change = 2114380800 + hours * 3600
    zone_files("Ox/Project", tzif((7200, "OXD"), (3600, "OXS"), changes=[(change, 1)]))
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    set_crontab(1, timezone="Ox/Project", minute="*", hour="*")
    output = run()
    assert differences(output).get("nightly") == (date and repeated_run(date))
    assert horizon("Ox/Project") in output


@pytest.mark.parametrize(
    ("now", "named"),
    [
        # 02:10 on the first pass, the instant the clocks go back, 02:10 on
        # the second pass, and the second pass's 02:30 itself: tonight's run
        # on the second pass is still to come.
        ("2027-10-31 00:10:00", True),
        ("2027-10-31 01:00:00", True),
        ("2027-10-31 01:10:00", True),
        ("2027-10-31 01:30:00", True),
        # A second past it, and past both passes. The zone changes no later.
        ("2027-10-31 01:30:01", False),
        ("2027-10-31 02:00:00", False),
    ],
    ids=[
        "first-pass",
        "at-the-change",
        "second-pass",
        "at-the-run",
        "past-it",
        "after",
    ],
)
def test_a_repeated_hour_the_import_is_made_in_is_named(
    beat_tables, settings, zone_files, clock, now, named
):
    """
    A stored schedule created during a repeated hour still runs a run time
    on its second pass, and beat ran it on the first. The clock's text
    is UTC here.
    """
    zone_files("Ox/Project", BERLIN_2027)
    settings.TIME_ZONE = "Ox/Project"
    clock(now)
    set_crontab(1, timezone="Ox/Project", minute="30", hour="2")
    output = run()
    expected = repeated_run("2027-10-31") if named else None
    assert differences(output).get("nightly") == expected


@pytest.mark.parametrize(
    ("now", "date"),
    [
        # Ten years on from a 29 February is a year that has none.
        ("2024-02-29 12:00:00", "2027-10-31"),
        # Ten years on from here is past the last year a date can hold.
        ("9995-06-01 00:00:00", None),
        ("9999-12-31 23:59:00", None),
    ],
    ids=["leap-day", "near-the-last-year", "the-last-minute"],
)
def test_the_ten_years_end_where_the_calendar_lets_them(
    beat_tables, settings, zone_files, clock, now, date
):
    zone_files("Ox/Project", BERLIN_2027)
    settings.TIME_ZONE = "Ox/Project"
    clock(now)
    set_crontab(1, timezone="Ox/Project", minute="30", hour="2")
    output = run()
    assert differences(output).get("nightly") == (date and repeated_run(date))
    assert "nightly" in printed_rows(output)


#: The clocks go forward alone, an hour, as Berlin's do on 28 March 2027.
SPRING_2027 = tzif((3600, "OXS"), (7200, "OXD"), changes=[(1806195600, 1)])
#: Two hours at once on the same night, from UTC, as Antarctica/Troll's do.
TWO_HOURS_2027 = tzif((0, "OXU"), (7200, "OXT"), changes=[(1806195600, 1)])
#: Half an hour, 02:00 to 02:30 on 3 October 2027, as Lord Howe's do.
HALF_HOUR_2027 = tzif((37800, "OXA"), (39600, "OXB"), changes=[(1822491000, 1)])


@pytest.mark.parametrize(
    ("zone", "minute", "hour", "date"),
    [
        # Two or more run times in the hour the clocks skip and none where
        # they land: beat runs the first, at 03:00 by the earlier offset,
        # and counts on from there; a stored schedule runs the last.
        (SPRING_2027, "0,30", "2", "2027-03-28"),
        (SPRING_2027, "*/20", "2", "2027-03-28"),
        (SPRING_2027, "15,45", "2", "2027-03-28"),
        (SPRING_2027, "*", "2", "2027-03-28"),
        # Where the first skipped one lands on a run time the clocks reach,
        # both run that one and count on alike.
        (SPRING_2027, "*/20", "*", None),
        (SPRING_2027, "15,45", "*", None),
        # One skipped run time, and none the clocks reach before it lands.
        (SPRING_2027, "30", "2", None),
        # Two hours at once: beat runs 01:00 at 03:00, a stored schedule
        # runs 02:00 at 04:00.
        (TWO_HOURS_2027, "0", "1-2", "2027-03-28"),
        # One skipped run time that lands past one the clocks reach: beat
        # runs 02:30 at 04:30 and passes over 03:30; a stored schedule runs
        # 03:30.
        (TWO_HOURS_2027, "30", "2-3", "2027-03-28"),
        (TWO_HOURS_2027, "0", "*", None),
        # Half an hour: beat runs 02:00 at 02:30, a run a stored schedule
        # never makes.
        (HALF_HOUR_2027, "*/20", "*", "2027-10-03"),
        (HALF_HOUR_2027, "15", "2", None),
    ],
    ids=[
        "hour-0-30",
        "hour-every-20",
        "hour-15-45",
        "hour-every-minute",
        "every-20",
        "every-15-45",
        "hour-30",
        "two-hours-0-1-2",
        "two-hours-30-2-3",
        "two-hours-hourly",
        "half-hour-every-20",
        "half-hour-15-2",
    ],
)
def test_runs_the_clocks_skip_are_named_where_the_two_engines_part(
    beat_tables, settings, zone_files, clock, zone, minute, hour, date
):
    """
    An hour the clocks skip needs no notice for a row with one run time in
    it. With two the engines can part: beat runs the first skipped run time
    and counts on from it, and a stored schedule runs the latest one at or
    before the wall clock. Measured with both on
    Berlin's, New York's, Havana's, Troll's and Lord Howe's clocks; these
    zones are built here so that no tz database decides them.
    """
    zone_files("Ox/Project", zone)
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    set_crontab(1, timezone="Ox/Project", minute=minute, hour=hour)
    output = run()
    assert differences(output).get("nightly") == (
        date and SKIPPED_RUN.format(date=date)
    )
    assert "nightly" in printed_rows(output)


def test_a_clock_put_forward_names_no_row(beat_tables, settings, zone_files, clock):
    # An hour the clocks skip, with one run time in it: both engines fire it
    # once, at the same instant, so there is nothing to say.
    zone_files(
        "Ox/Project", tzif((3600, "OXS"), (7200, "OXD"), changes=[(1806195600, 1)])
    )
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    set_crontab(1, timezone="Ox/Project", minute="30", hour="2")
    output = run()
    assert "nightly" not in differences(output)
    assert "nightly" in printed_rows(output)


def test_a_repeated_stretch_that_crosses_midnight_is_looked_at_minute_by_minute(
    beat_tables, settings, zone_files, clock
):
    """
    A clock put back from 00:30 to 23:30 the evening before, by a zone built
    for it: the stretch it goes through twice is half in one day and half
    in the next. Each row is named with the date its own run falls on, and
    a row held to either date by a day field is only in that half.
    """
    # 2027-06-01 00:30 at +02:00 is 22:30 UTC the day before.
    zone_files(
        "Ox/Project", tzif((7200, "OXD"), (3600, "OXS"), changes=[(1811802600, 1)])
    )
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    insert_crontab(2, timezone="Ox/Project", minute="45", hour="23")
    insert_crontab(3, timezone="Ox/Project", minute="15", hour="0")
    insert_crontab(4, timezone="Ox/Project", minute="45", hour="23", day_of_month="1")
    insert_crontab(5, timezone="Ox/Project", minute="30", hour="0")
    for pk, name in ((2, "late"), (3, "early"), (4, "wrong-day"), (5, "at-the-end")):
        insert_task(10 + pk, name, "reports.tasks.daily", crontab_id=pk)
    named = differences(run())
    assert named.get("late") == repeated_run("2027-05-31")
    assert named.get("early") == repeated_run("2027-06-01")
    assert "wrong-day" not in named
    assert "at-the-end" not in named


@pytest.mark.parametrize(
    ("stored", "named"),
    [
        ({"minute": "0", "hour": "1"}, False),
        ({"minute": "1", "hour": "1"}, True),
        ({"minute": "0", "hour": "2"}, True),
        ({"minute": "1", "hour": "2"}, False),
    ],
    ids=["before-it", "first-minute", "last-minute", "after-it"],
)
def test_a_clock_put_back_mid_minute_repeats_only_the_runs_inside_the_stretch(
    beat_tables, settings, zone_files, clock, stored, named
):
    """
    A clock put back an hour at thirty seconds past 02:00, by a zone built
    for it, goes through 01:00:30 to 02:00:30 twice. A run is on the
    minute: the one at 01:00 came before the stretch and happens once, and
    the one at 02:00 is inside it.
    """
    zone_files(
        "Ox/Project", tzif((7200, "OXD"), (3600, "OXS"), changes=[(1811808030, 1)])
    )
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-01-01 00:00:00")
    set_crontab(1, timezone="Ox/Project", **stored)
    assert ("nightly" in differences(run())) == named


def test_an_interval_is_named_with_how_it_differs(beat_tables):
    """
    Every interval is: it keeps its length and loses its phase. That was a
    sentence in the note at the end, said of every import whether it held
    an interval or not. It is said of each interval now, by name, where the
    other differences are, and the note no longer says it.
    """
    insert_task(4, "second", "mail.tasks.poll", interval_id=1)
    output = run()
    assert differences(output) == {
        "poller": INTERVAL_DIFFERS,
        "second": INTERVAL_DIFFERS,
    }
    assert "fixed instant" not in output[output.index("# Read before applying.") :]


def test_the_ten_years_are_stated_whenever_a_crontab_is_printed(beat_tables):
    """
    With a row named and with none: a crontab that is not named is clear
    for the years that were looked at, and the reader is told how many that
    was. With no crontab printed there is nothing to say it of.
    """
    output = run()
    assert differences(output) == {"poller": INTERVAL_DIFFERS}
    assert horizon(settings.TIME_ZONE) in output

    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id = 2")
    output = run()
    assert DIFFERENCES not in output
    assert horizon(settings.TIME_ZONE) in output

    insert_task(2, "poller", "mail.tasks.poll", interval_id=1)
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id = 1")
    output = run()
    assert differences(output) == {"poller": INTERVAL_DIFFERS}
    assert HORIZON.splitlines()[0] not in output


@pytest.mark.parametrize(
    ("every", "period", "seconds"),
    [
        (2_000_000, "microseconds", 2),
        (1_000_000, "microseconds", 1),
        (1, "seconds", 1),
        (90, "minutes", 5400),
        (36, "hours", 129_600),
        (7, "days", 604_800),
    ],
)
def test_an_interval_is_counted_exactly(beat_tables, every, period, seconds):
    # Two million microseconds are two seconds. The command once counted a
    # microsecond as nothing and listed every such row as below one second.
    set_interval(1, every, period)
    output = run()
    assert "poller" not in refusals(output)
    assert printed_rows(output)["poller"]["every_seconds"] == seconds


def below_one_second(every, period):
    return (
        f"an interval of {every!a} {period!a} is below one second, which the "
        "dispatch loop cannot honour"
    )


def not_whole_seconds(every, period):
    return NOT_WHOLE_SECONDS.format(every=every, period=period)


def unknown_period(period):
    return UNKNOWN_PERIOD.format(period=period)


@pytest.mark.parametrize(
    ("every", "period", "reason"),
    [
        (1_500_000, "microseconds", not_whole_seconds(1_500_000, "microseconds")),
        (999_999, "microseconds", below_one_second(999_999, "microseconds")),
        (0, "seconds", below_one_second(0, "seconds")),
        (-5, "minutes", below_one_second(-5, "minutes")),
        # Further below zero than a timedelta goes, and still short, not long.
        (-2_000_000_000, "days", below_one_second(-2_000_000_000, "days")),
        # A keyword timedelta does not take: beat raised on it.
        (2, "fortnights", unknown_period("fortnights")),
        (2, "Days", unknown_period("Days")),
        (2, "", unknown_period("")),
        (None, "seconds", EVERY_NOT_A_NUMBER.format(every=None)),
    ],
    ids=[
        "one-and-a-half-seconds",
        "under-a-second",
        "zero",
        "negative",
        "far-below-zero",
        "not-a-timedelta-keyword",
        "capitalised",
        "empty-period",
        "null-every",
    ],
)
def test_an_interval_no_stored_schedule_can_keep_is_listed_for_what_it_is(
    beat_tables, every, period, reason
):
    """
    Each for its own reason. The command once had one, "below one second",
    for an interval in microseconds of any length and for a period it did
    not know, neither of which it was true of.
    """
    set_interval(1, every, period)
    output = run()
    assert refusals(output).get("poller") == reason
    assert sorted(printed_rows(output)) == ["nightly"]


@pytest.mark.parametrize(
    ("every", "period", "reason"),
    [
        ("soon", "seconds", EVERY_NOT_A_NUMBER.format(every="soon")),
        (1.5, "seconds", not_whole_seconds(1.5, "seconds")),
        (0.25, "seconds", below_one_second(0.25, "seconds")),
        # Each has microseconds left over, and neither shows them when it
        # is asked for its length in seconds: that is a float, which this
        # far out no longer holds one. Two hundred thousand days and a
        # microsecond, and a tenth of a day short of the longest there is.
        (
            17_280_000_000_000_001,
            "microseconds",
            not_whole_seconds(17_280_000_000_000_001, "microseconds"),
        ),
        (999_999_999.9, "days", not_whole_seconds(999_999_999.9, "days")),
    ],
    ids=["text", "fraction", "small-fraction", "one-microsecond-over", "long-float"],
)
def test_what_only_sqlite_keeps_in_every_is_listed(beat_tables, every, period, reason):
    """
    SQLite keeps what it is given in an integer column: text, fractions,
    and whole numbers longer than the column is on any other database. Text
    there once ended the command in a TypeError traceback.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite stores text, a fraction or 64 bits in that column")
    if not isinstance(every, str):
        length = timedelta(**{period: every})
        assert length.microseconds or length < timedelta(seconds=1)
    set_interval(1, every, period)
    output = run()
    assert refusals(output)["poller"] == reason
    assert sorted(printed_rows(output)) == ["nightly"]


@pytest.mark.parametrize(
    ("every", "period", "seconds"),
    [
        # The number stored for a tenth is not a tenth: counted exactly it
        # is a fraction of a microsecond over 8640 seconds.
        (0.1, "days", 8640),
        (0.5, "hours", 1800),
        (2_000_000.3, "microseconds", 2),
        # Under a second as written, and a second once rounded.
        (0.9999996, "seconds", 1),
    ],
    ids=["tenth-of-a-day", "half-an-hour", "a-third-of-a-microsecond", "rounds-up"],
)
def test_a_fraction_is_counted_the_way_beat_counted_it(
    beat_tables, every, period, seconds
):
    """
    beat hands the row to timedelta, which rounds what it is given to the
    microsecond, and runs on the result. So that is the interval there was,
    and the one imported: a tenth of a day ran every 8640 seconds.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite stores a fraction in an integer column")
    assert timedelta(**{period: every}) == timedelta(seconds=seconds)
    set_interval(1, every, period)
    output = run()
    assert "poller" not in refusals(output)
    # Printed as the whole number it is, too: 8640 and not 8640.0.
    assert f"every_seconds={seconds})" in output
    assert printed_rows(output)["poller"]["every_seconds"] == seconds


@pytest.mark.parametrize(
    ("every", "period", "seconds"),
    [
        (1, "weeks", 604_800),
        (2, "weeks", 1_209_600),
        (2_000, "milliseconds", 2),
        (90_000, "milliseconds", 90),
    ],
    ids=["a-week", "a-fortnight", "two-seconds-in-ms", "ninety-seconds-in-ms"],
)
def test_weeks_and_milliseconds_are_counted_as_beat_counted_them(
    beat_tables, schedulable, every, period, seconds
):
    """
    beat builds an interval as timedelta(**{period: every}), and timedelta
    takes weeks and milliseconds as well as the periods beat's admin offers.
    Its model does not hold a row to those choices, so a row saved through
    the model with either ran, at the length timedelta made of it.
    """
    assert timedelta(**{period: every}) == timedelta(seconds=seconds)
    set_interval(1, every, period)
    output = run()
    assert "poller" not in refusals(output)
    assert printed_rows(output)["poller"]["every_seconds"] == seconds
    run_as_module(section_2(output))
    assert OxSchedule.objects.get(name="poller").every_seconds == seconds


@pytest.mark.parametrize(
    ("every", "period", "reason"),
    [
        (1_500, "milliseconds", not_whole_seconds(1_500, "milliseconds")),
        (999, "milliseconds", below_one_second(999, "milliseconds")),
        (0, "weeks", below_one_second(0, "weeks")),
        (-1, "weeks", below_one_second(-1, "weeks")),
    ],
    ids=["fraction-in-ms", "under-a-second-in-ms", "no-weeks", "negative-weeks"],
)
def test_weeks_and_milliseconds_keep_the_whole_second_rule(
    beat_tables, every, period, reason
):
    set_interval(1, every, period)
    output = run()
    assert refusals(output).get("poller") == reason
    assert sorted(printed_rows(output)) == ["nightly"]


def test_weeks_and_milliseconds_meet_the_ceilings_other_periods_meet(beat_tables):
    """
    The longest interval a stored schedule can derive ticks for is
    62,135,596,800 seconds, 102,737 weeks and some days. In weeks the
    longest under it imports where the destination's column holds it, and
    a week more is listed; in milliseconds, which only SQLite's column for
    beat's every is wide enough to hold, the ceiling itself imports and a
    second more is listed.
    """
    limit, _, _ = COLUMN[connection.vendor]
    set_interval(1, 102_737, "weeks")
    output = run()
    if connection.vendor == "sqlite":
        assert printed_rows(output)["poller"]["every_seconds"] == 62_135_337_600
    else:
        assert refusals(output)["poller"] == over_the_limit(limit, 62_135_337_600)
    set_interval(1, 102_738, "weeks")
    reason = refusals(run())["poller"]
    if connection.vendor == "sqlite":
        assert reason == past_the_ticks(62_135_942_400)
    else:
        assert reason == over_the_limit(limit, 62_135_942_400)
    if connection.vendor != "sqlite":
        return
    set_interval(1, 62_135_596_800_000, "milliseconds")
    output = run()
    assert printed_rows(output)["poller"]["every_seconds"] == 62_135_596_800
    set_interval(1, 62_135_596_801_000, "milliseconds")
    assert refusals(run())["poller"] == past_the_ticks(62_135_596_801)


def past_the_ticks(seconds):
    return INTERVAL_PAST_TICKS.format(seconds=seconds, limit=62_135_596_800)


def would_be_refused(*problems):
    return "create_schedules would refuse it: " + "; ".join(problems)


def too_long(field, length):
    message = f"Ensure this value has at most 128 characters (it has {length})."
    return f"{field}: {message!a}"


def test_a_name_or_task_too_long_for_a_stored_schedule_is_listed(
    beat_tables, schedulable
):
    """
    beat allows a name and a task two hundred characters, and a stored
    schedule's columns hold 128. Printed, such a row would make
    create_schedules refuse the whole batch. It is listed instead, with
    what the paste would have said, and its task is not exposed.
    """
    insert_task(4, "n" * 129, "reports.tasks.daily", interval_id=1)
    insert_task(5, "long-task", "t" * 129, interval_id=1)
    insert_task(6, "n" * 128, "mail.tasks.poll", interval_id=1)
    insert_task(7, "", "reports.tasks.daily", interval_id=1)
    output = run()
    listed = refusals(output)
    assert "n" * 129 in listed
    assert listed["n" * 129] == would_be_refused(too_long("name", 129))
    assert listed["long-task"] == would_be_refused(too_long("task_key", 129))
    assert listed[""] == would_be_refused("name: 'This field cannot be blank.'")
    assert "t" * 129 not in section_1(output)
    run_as_module(section_2(output))
    assert sorted(OxSchedule.objects.values_list("name", flat=True)) == sorted(
        ["nightly", "poller", "n" * 128]
    )


def apart(low, high):
    """
    Values two and three apart by turns, as one crontab field: no two are
    neighbours and no three are next to each other in step, so written a
    stretch at a time there is nothing to write them as but themselves.
    They are every fifth value and every fifth from the second all the
    same, which is how a cover writes them.
    """
    values, value = [], low
    while value <= high:
        values.append(value)
        value += 2 if len(values) % 2 else 3
    return ",".join(map(str, values))


def in_steps(*steps):
    """The values of "first-last/step" progressions taken together."""
    return frozenset(
        value for first, last, step in steps for value in range(first, last + 1, step)
    )


@pytest.mark.parametrize(
    ("stored", "cron", "values"),
    [
        # Every third minute and every third from the first: 36 characters
        # as beat stores them, 175 written a stretch at a time.
        (
            ["*/3,1-59/3", "*/3,1-23/3", "*", "*/3,2-12/3", "*"],
            "*/3,1/3 */3,1/3 * */3,2/3 *",
            [
                in_steps((0, 59, 3), (1, 59, 3)),
                in_steps((0, 23, 3), (1, 23, 3)),
                in_steps((1, 31, 1)),
                in_steps((1, 12, 3), (2, 12, 3)),
                in_steps((0, 6, 1)),
            ],
        ),
        (
            ["*/5,2-59/5", "*/5,2-23/5", "1-31/5,3-31/5", "1-12/5,3-12/5", "*"],
            "*/5,2/5 */5,2/5 */5,3/5 */5,3,8 *",
            [
                in_steps((0, 59, 5), (2, 59, 5)),
                in_steps((0, 23, 5), (2, 23, 5)),
                in_steps((1, 31, 5), (3, 31, 5)),
                in_steps((1, 12, 5), (3, 12, 5)),
                in_steps((0, 6, 1)),
            ],
        ),
        # The values one by one, as a beat row can store them.
        (
            [apart(0, 52), apart(0, 23), apart(1, 31), apart(1, 12), "*"],
            "0-50/5,2-52/5 */5,2/5 */5,3/5 */5,3,8 *",
            [
                in_steps((0, 50, 5), (2, 52, 5)),
                in_steps((0, 20, 5), (2, 22, 5)),
                in_steps((1, 31, 5), (3, 28, 5)),
                in_steps((1, 11, 5), (3, 8, 5)),
                in_steps((0, 6, 1)),
            ],
        ),
    ],
    ids=["thirds", "fifths", "two-and-three-apart"],
)
def test_a_crontab_too_long_written_a_stretch_at_a_time_is_written_in_steps(
    beat_tables, schedulable, stored, cron, values
):
    """
    Written a stretch at a time, as neighbours and pairs, these are 175,
    141 and 135 characters, longer than a stored schedule holds. Written as
    the progressions they are, they fit, and django-ox reads them as the
    same values; v1.7.0 printed the
    first two as stored. Where the form written a stretch at a time fits,
    it is the one printed, so this reaches only rows that would otherwise
    be listed.
    """
    fields = ("minute", "hour", "day_of_month", "month_of_year", "day_of_week")
    set_crontab(1, **dict(zip(fields, stored, strict=True)))
    output = run()
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == cron
    assert len(cron) <= 128
    parsed = CronExpression(cron)
    assert [
        frozenset(parsed.minutes),
        frozenset(parsed.hours),
        frozenset(parsed.days_of_month),
        frozenset(parsed.months),
        frozenset(parsed.days_of_week),
    ] == values
    run_as_module(section_2(output))
    assert OxSchedule.objects.get(name="nightly").cron == cron


#: A crontab no expression django-ox reads writes in 128 characters. Each
#: field is within beat's own column and Celery reads it; the minute holds
#: 34 values in no pattern that saves much. Over every spelling django-ox's
#: parser reads as part of a field (a value; "a-b" and "a-b/s"; "*/s" and
#: "a/s" where the step reaches the top), the four fields need at least 58,
#: 24, 31 and 12 characters, and "*" for the day of the week: 130 with the
#: four spaces.
NO_EXPRESSION_FITS = {
    "minute": "0-1,3,5-8,10-11,13-16,24-26,28,30-33,35-38,40-44,50-54/2,55-59/2",
    "hour": "0-1,5-7,9-11,13-14,16-17,19-20",
    "day_of_month": "1-2,4-8/2,9,11-13,16-19,24-28/2,29,31",
    "month_of_year": "1-4,6-8,10-11",
}


def test_a_crontab_no_expression_found_for_fits_is_listed(beat_tables):
    """
    The shortest the command finds for it is 134 characters, for a column
    of 128. The reason says what was found and does not claim more: the
    search is greedy, and here nothing shorter than 130 exists at all.
    """
    set_crontab(1, **NO_EXPRESSION_FITS)
    output = run()
    assert refusals(output).get("nightly") == CRON_NOT_WITHIN_LIMIT.format(
        limit=128, length=134
    )
    assert sorted(printed_rows(output)) == ["poller"]


def test_the_expression_written_for_a_crontab_is_the_same_every_time(beat_tables):
    # The cover is built from sets, whose order of iteration is the
    # interpreter's business: the same row prints the same text, run after
    # run, and the same reason when it does not fit.
    set_crontab(1, minute="*/3,1-59/3", hour="*/3,1-23/3", month_of_year="*/3,2-12/3")
    first = run()
    assert printed_rows(first)["nightly"]["cron"] == "*/3,1/3 */3,1/3 * */3,2/3 *"
    assert run() == first
    set_crontab(1, **NO_EXPRESSION_FITS)
    first = run()
    assert "nightly" in refusals(first)
    assert run() == first


def test_stepped_ranges_are_not_too_long_for_a_stored_schedule(beat_tables):
    """
    Every other minute, hour and day, off the beat that "*/2" would be.
    Written out value by value those are 160 characters, past the 128 a
    stored schedule holds, where beat stores them in 20. They are written
    as the stepped ranges they are.
    """
    set_crontab(1, minute="1-59/2", hour="1-23/2", day_of_month="2-31/2")
    values = [range(1, 60, 2), range(1, 24, 2), range(2, 32, 2)]
    assert len(" ".join([*(",".join(map(str, field)) for field in values), "* *"])) > (
        128
    )
    output = run()
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["cron"] == "1-59/2 1-23/2 2-30/2 * *"
    parsed = CronExpression("1-59/2 1-23/2 2-30/2 * *")
    assert [list(parsed.minutes), list(parsed.hours), list(parsed.days_of_month)] == [
        list(field) for field in values
    ]


#: By database: the most seconds its column for every_seconds holds, an
#: interval in days that is past it, and whether thirty thousand days fit.
#: The same field is a signed 32-bit column on PostgreSQL and an unsigned
#: one on MySQL. SQLite's holds 64 bits, so no interval is too long for the
#: column itself.
COLUMN = {
    "postgresql": (2_147_483_647, 30_000, False),
    "mysql": (4_294_967_295, 50_000, True),
    "sqlite": (9_223_372_036_854_775_807, None, True),
}


INTERVAL_PAST_COLUMN = (
    "its interval is {seconds} seconds. The every_seconds column of database {alias} "
    "({database}) holds at most {limit}."
)
INTERVAL_PAST_TICKS = (
    "its interval is {seconds} seconds. A stored schedule's interval can be at most "
    "{limit} seconds, the longest a worker can count ticks for."
)


def over_the_limit(limit, seconds):
    database = {"postgresql": "PostgreSQL", "mysql": "MySQL"}[connection.vendor]
    return INTERVAL_PAST_COLUMN.format(
        seconds=seconds, alias="'default'", database=database, limit=limit
    )


def test_an_interval_the_destination_column_cannot_hold_is_listed(beat_tables):
    """
    Thirty thousand days of seconds fit the column SQLite and MySQL give
    every_seconds and not the one PostgreSQL gives it. The command once
    printed the row wherever it ran, and on PostgreSQL the paste stopped at
    it. The limit is asked of the database the schedules are written to.
    """
    limit, days, holds_thirty_thousand = COLUMN[connection.vendor]
    set_interval(1, 30_000, "days")
    output = run()
    if holds_thirty_thousand:
        assert printed_rows(output)["poller"]["every_seconds"] == 2_592_000_000
    else:
        assert "poller" in refusals(output)
        assert refusals(output)["poller"] == over_the_limit(limit, 30_000 * 86_400)
    if days is None:
        # A hundred thousand days are past both of the other columns. They
        # fit this one.
        set_interval(1, 100_000, "days")
        rows = printed_rows(run())
        assert rows["poller"]["every_seconds"] == 8_640_000_000
        return
    set_interval(1, days, "days")
    output = run()
    assert "poller" in refusals(output)
    assert refusals(output)["poller"] == over_the_limit(limit, days * 86_400)
    assert sorted(printed_rows(output)) == ["nightly"]


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
def test_the_limits_are_those_of_the_database_the_schedules_go_to():
    # --database names where the beat tables are, here the SQLite alias on
    # every leg. The schedules are written to the default database, and it
    # is that one's column the interval has to fit.
    limit, days, _ = COLUMN[connection.vendor]
    if days is None:
        pytest.skip("no interval is too long for SQLite's column")
    try:
        make_beat_tables(db="alt")
        with connections["alt"].cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_intervalschedule SET every = %s, "
                "period = %s",
                [days, "days"],
            )
        output = run(database="alt")
    finally:
        drop_beat_tables("alt")
    assert refusals(output)["poller"] == over_the_limit(limit, days * 86_400)


def too_long_for_beat(every, period):
    return INTERVAL_TOO_LONG.format(every=every, period=period)


@pytest.mark.parametrize(
    ("every", "period", "listed"),
    [
        (999_999_999, "days", False),
        (1_000_000_000, "days", True),
        (2_000_000_000, "hours", False),
        (2_000_000_000, "days", True),
    ],
    ids=["the-longest", "a-day-more", "long-in-hours", "far-past"],
)
def test_an_interval_longer_than_beat_could_build_is_listed(
    beat_tables, every, period, listed
):
    """
    beat hands an interval to timedelta, which holds 999,999,999 days and
    raises on more, so a longer one never ran. The worker here builds a
    stored interval the same way and would skip the row for ever, on a
    database whose column is wide enough to hold the number.

    One that beat could build is still listed where no stored schedule can
    have it: for the destination's column on PostgreSQL and MySQL, and on
    SQLite for being longer than the worker can count ticks of. In the
    words create_schedules would use, which are not pinned here.
    """
    with pytest.raises(OverflowError) if listed else nullcontext():
        timedelta(**{period: every})
    set_interval(1, every, period)
    output = run()
    reason = refusals(output).get("poller")
    if listed:
        assert reason == too_long_for_beat(every, period)
    else:
        assert reason is not None
        seconds = timedelta(**{period: every}) // timedelta(seconds=1)
        assert reason.startswith(f"its interval is {seconds} seconds. ")
    assert sorted(printed_rows(output)) == ["nightly"]


def test_an_interval_past_every_column_is_listed(beat_tables):
    # Finite in the table, and as a float only SQLite can hold it there.
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite stores a float in an integer column")
    set_interval(1, 1e308, "days")
    output = run()
    assert refusals(output)["poller"] == too_long_for_beat(1e308, "days")


def test_section_2_is_one_call(beat_tables):
    """
    One create_schedules call holding every row, so that applying the
    output is one decision by the database and not one per schedule. The
    import it needs and no other.
    """
    output = run()
    imports, call = ast.parse(section_2(output)).body
    assert ast.unparse(imports) == "from django_ox.stored import create_schedules"
    assert isinstance(call.value, ast.Call)
    assert call.value.func.id == "create_schedules"
    (rows,) = call.value.args
    assert [ast.unparse(row.func) for row in rows.elts] == ["dict", "dict"]
    assert not any(row.args for row in rows.elts)
    assert [ast.literal_eval(row.keywords[0].value) for row in rows.elts] == [
        "nightly",
        "poller",
    ]


def test_datetime_is_imported_only_when_a_row_names_it(beat_tables):
    assert "import datetime" not in run()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-12-31 10:00:00"],
        )
    lines = section_2(run()).splitlines()
    assert lines[1:3] == [
        "from datetime import datetime",
        "from django_ox.stored import create_schedules",
    ]


def test_applying_the_output_creates_every_schedule_or_none(beat_tables, monkeypatch):
    """
    The command once printed a call per schedule, and a paste that failed
    at the fifth had created four. Here one printed task is not registered
    when the output is applied: nothing is created, as a module or pasted
    into a shell, and the error says which row and why. Registered, the
    same output creates both.
    """
    from django.core.exceptions import ValidationError

    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="reports.tasks.daily", task=tasks.add))
    output = run()

    with pytest.raises(ValidationError) as caught:
        run_as_module(section_2(output))
    assert OxSchedule.objects.count() == 0
    assert caught.value.messages == [
        (
            "rows[1] (name 'poller'), task_key: 'mail.tasks.poll' is not a "
            "schedulable task. Registered keys: reports.tasks.daily."
        )
    ]

    shell = _Shell({})
    for line in section_2(output).splitlines():
        shell.push(line)
    shell.push("")
    assert [type(error) for error in shell.errors] == [ValidationError]
    assert OxSchedule.objects.count() == 0

    register(ScheduleKind(key="mail.tasks.poll", task=tasks.add))
    paste_into_shell(section_2(output))
    assert sorted(OxSchedule.objects.values_list("name", flat=True)) == [
        "nightly",
        "poller",
    ]


@pytest.mark.parametrize(
    "constant", ["FOOTER", "SCHEDULE_SOURCE_NOTE", "STORED_IMPORT"]
)
def test_output_python_cannot_read_is_never_printed(beat_tables, monkeypatch, constant):
    """
    Everything that will be pasted is compiled before a line of it is
    printed. Each row has been compiled by then, so nothing known reaches
    this; here a piece of the output itself is broken, as a defect in the
    command one day might break it. It stops, and prints nothing.
    """
    from django_ox.management.commands import ox_import_beat_schedules as command

    monkeypatch.setattr(command, constant, "not ( python")
    error, printed = import_fails()
    assert str(error) == OUTPUT_UNREADABLE.format(alias="default")
    assert printed == ""


@pytest.mark.parametrize(
    "said",
    ["it has no schedule\nimport os", "it runs in Z\u00fcrich"],
    ids=["line-break", "not-ascii"],
)
@pytest.mark.parametrize("constant", ["NO_EQUIVALENT_KIND", "INTERVAL_DIFFERS"])
def test_a_line_about_a_row_that_is_not_one_ascii_line_is_never_printed(
    beat_tables, monkeypatch, constant, said
):
    """
    A reason, and a difference, is printed on a comment line. One with a
    line break in it would end the comment and paste what follows as code,
    and the result would compile. Each the command has is one line, because
    the stored values in it are literals; one written later is held to the
    same, by the output being refused whole. Here the reason the fixture's
    row with no schedule is listed for is broken, then the difference its
    interval is named with.
    """
    from django_ox.management.commands import ox_import_beat_schedules as command

    monkeypatch.setattr(command, constant, said)
    error, printed = import_fails()
    assert str(error) == OUTPUT_UNREADABLE.format(alias="default")
    assert printed == ""


def test_a_name_holding_a_quote_still_emits_runnable_code(beat_tables):
    # The output is meant to be pasted, so a name that breaks the literal
    # is a line that does not parse.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET name = %s WHERE id = 1",
            ['say "hello"\\backslash'],
        )
    calls = printed_calls(run())
    assert calls, "the command printed no calls to check"
    for line in calls:
        compile(line, "<generated>", "eval")


def test_a_database_it_cannot_reach_is_a_sentence(monkeypatch):
    """
    This command prints code for a person to read and paste. A driver
    traceback in the middle of that is nothing they can act on, and its
    three siblings report the same failure in one line.
    """

    def refuse(*args, **kwargs):
        raise OperationalError("could not connect to server")

    monkeypatch.setattr(connections["default"].introspection, "table_names", refuse)
    with pytest.raises(CommandError) as caught:
        call_command("ox_import_beat_schedules")
    assert "Database unreachable: could not connect to server" in str(caught.value)


def bound_refused(column, name="nightly"):
    return BOUND_REFUSED.format(alias="default", column=column, name=name)


#: Stored dates some driver cannot hand back as a date, by database.
UNREADABLE = {
    "postgresql": ["infinity", "-infinity"],
    "sqlite": ["2099-13-45 00:00:00"],
    "mysql": ["0000-00-00 00:00:00"],
}


def import_fails(**options):
    """Run the command expecting it to stop: its one line, and what it printed."""
    out = StringIO()
    with pytest.raises(CommandError) as caught:
        call_command("ox_import_beat_schedules", stdout=out, **options)
    return caught.value, out.getvalue()


def decoded_by_driver(column):
    """What the driver hands back for row 1's column, or what it raises."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {column} FROM django_celery_beat_periodictask "  # noqa: S608
                "WHERE id = 1"
            )
            return cursor.fetchone()[0]
    except (DatabaseError, ValueError, OverflowError) as exc:
        return exc


@pytest.mark.parametrize("column", ["start_time", "expires"])
def test_a_date_the_driver_cannot_read_stops_the_import_in_one_line(
    beat_tables, column
):
    """
    PostgreSQL's 'infinity' and '-infinity', an impossible date kept as
    text on SQLite, and MySQL's zero date. What reaches the command depends
    on the driver: psycopg raises while the rows are read, where no single
    row can be set aside; PyMySQL hands back text that is no date; and
    mysqlclient hands back None for a value that is not NULL. Each stops
    the import with one line, before printing anything, and the line says
    which it was: a value that would not convert, or a bound on a named row
    that cannot be carried over. A driver that decodes the value as a date,
    as psycopg2 does with infinity, has raised no read error, and that
    value is not checked here.
    """
    checked = []
    for stored in UNREADABLE[connection.vendor]:
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
                "WHERE id = 1",
                [stored],
            )
        decoded = decoded_by_driver(column)
        if isinstance(decoded, datetime):
            continue
        error, printed = import_fails()
        if isinstance(decoded, Exception):
            assert str(error) == STORED_VALUE_ERROR.format(alias="default")
        else:
            assert str(error) == bound_refused(column)
        assert printed == ""
        checked.append(stored)
    if not checked:
        pytest.skip(
            f"this driver reads each of {UNREADABLE[connection.vendor]} as a date"
        )


@pytest.mark.parametrize("where", ["a table's columns", "a query"])
def test_a_database_that_refuses_the_read_is_an_access_failure(
    beat_tables, monkeypatch, where
):
    """
    The introspection or a statement fails: the database is the problem,
    and no stored value is. One line, no driver detail, nothing printed.
    """
    if where == "a query":
        execute = CursorWrapper.execute

        def refuse(cursor, sql, params=None):
            if sql.startswith("SELECT name, task"):
                raise OperationalError("a driver detail")
            return execute(cursor, sql, params)

        monkeypatch.setattr(CursorWrapper, "execute", refuse)
    else:

        def refuse(*args, **kwargs):
            raise OperationalError("a driver detail")

        monkeypatch.setattr(connection.introspection, "get_table_description", refuse)
    failure, printed = import_fails()
    assert str(failure) == ACCESS_ERROR.format(alias="default")
    assert type(failure.__cause__) is OperationalError
    assert printed == ""


@pytest.mark.parametrize(
    "error", [DataError, OperationalError, ValueError, OverflowError]
)
def test_a_value_that_fails_while_the_rows_are_read_is_a_stored_value_failure(
    beat_tables, monkeypatch, error
):
    """
    The statement ran, and reading its rows failed: a stored value the
    driver could not hand over, whatever class it raises for that. SQLite
    reports text it cannot decode as an OperationalError, the class of a
    lost connection, so the class cannot be what decides. The line does not
    send the reader to check access they have.
    """

    def fail(cursor):
        raise error("a driver detail")

    monkeypatch.setattr(CursorWrapper, "__iter__", fail)
    failure, printed = import_fails()
    assert str(failure) == STORED_VALUE_ERROR.format(alias="default")
    assert type(failure.__cause__) is error
    assert printed == ""


@pytest.mark.parametrize("error", [DataError, ValueError, OverflowError])
def test_a_conversion_failure_while_a_query_runs_is_a_stored_value_failure(
    beat_tables, monkeypatch, error
):
    # MySQL's drivers convert as the query runs, not as its rows are read,
    # and a value they cannot convert is still a stored value.
    execute = CursorWrapper.execute

    def fail(cursor, sql, params=None):
        if sql.startswith("SELECT name, task"):
            raise error("a driver detail")
        return execute(cursor, sql, params)

    monkeypatch.setattr(CursorWrapper, "execute", fail)
    failure, printed = import_fails()
    assert str(failure) == STORED_VALUE_ERROR.format(alias="default")
    assert type(failure.__cause__) is error
    assert printed == ""


@pytest.mark.parametrize("error", [ValueError, OverflowError])
def test_a_bound_that_cannot_be_carried_over_stops_the_import_and_names_its_row(
    beat_tables, monkeypatch, error
):
    """
    Found after the fetch, a row at a time, so the line can say which row
    and which bound. It once blamed database access, and named neither.
    """
    from django_ox.management.commands.ox_import_beat_schedules import Command

    parse = Command._parse_datetime

    def fail(value, connection):
        if value is not None:
            raise error("a driver detail")
        return parse(value, connection)

    monkeypatch.setattr(Command, "_parse_datetime", staticmethod(fail))
    # With no bound anywhere there is nothing to carry, and nothing stops.
    run()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 2",
            ["2099-12-31 10:00:00"],
        )
    failure, printed = import_fails()
    assert str(failure) == bound_refused("expires", "poller")
    assert type(failure.__cause__) is error
    assert printed == ""


@pytest.mark.parametrize(
    ("stored", "zone"),
    [
        ("0001-01-01 00:00:00", "UTC"),
        # Read in a zone an hour ahead of UTC, which puts its instant before
        # the first one a datetime holds in UTC.
        ("0001-01-01 00:30:00", "Etc/GMT-1"),
        ("2020-01-01 00:00:00", "UTC"),
    ],
    ids=["year-one", "year-one-before-utc-holds-it", "expired"],
)
def test_an_expiry_long_past_is_listed_as_expired(beat_tables, stored, zone):
    """
    Celery counts a task as expired from its expiry, and one in year 1 is
    long past. The command once worked out the end of its window first, a
    microsecond earlier, which is before the first instant a datetime
    holds, and stopped the whole import over a row that had only expired.
    It asks first now, by a comparison that cannot overflow.
    """
    if zone != "UTC" and connection.vendor == "postgresql":
        pytest.skip("PostgreSQL would store that instant as a year BC")
    with connection_time_zone("default", zone):
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
                [stored],
            )
        output = run()
    assert refusals(output)["nightly"] == "it has expired"
    assert sorted(printed_rows(output)) == ["poller"]


def test_a_live_expiry_whose_end_has_no_date_still_stops_the_import(beat_tables):
    # The last microsecond a datetime holds, an hour behind UTC: an instant
    # in year 10000 in UTC, not yet reached, and a window that cannot be
    # written down. Dropping it would run the schedule past it.
    if connection.vendor == "postgresql":
        pytest.skip("PostgreSQL hands back a year past 9999 as an error of its own")
    with connection_time_zone("default", "Etc/GMT+1"):
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
                ["9999-12-31 23:59:59.999999"],
            )
        error, printed = import_fails()
    assert str(error) == bound_refused("expires")
    assert type(error.__cause__) is OverflowError
    assert printed == ""


@pytest.mark.parametrize("column", ["start_time", "expires"])
def test_a_stored_date_read_as_none_stops_the_import(beat_tables, column):
    """
    Django's SQLite converter returns None for date text it cannot parse.
    Read as no bound, a dropped start would run the schedule early and a
    dropped expiry would run it forever, so the import stops instead.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite keeps date text its converter cannot parse")
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
            "WHERE id = 1",
            ["31/12/2099 10:00"],
        )
    assert decoded_by_driver(column) is None
    error, printed = import_fails()
    assert str(error) == bound_refused(column)
    assert printed == ""


def test_a_driver_that_reads_a_stored_date_as_none_stops_the_import(
    beat_tables, monkeypatch
):
    # The same on every backend: a driver that decodes a stored date as None,
    # as mysqlclient does with a MySQL zero date. A NULL bound still imports.
    from django_ox.management.commands.ox_import_beat_schedules import Command

    monkeypatch.setattr(Command, "_parse_datetime", staticmethod(lambda *args: None))
    run()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-12-31 10:00:00"],
        )
    error, printed = import_fails()
    assert str(error) == bound_refused("expires")
    assert printed == ""


def test_one_off_task_is_not_translated(beat_tables):
    # A stored schedule recurs, so a task meant to run once would run again.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET one_off = %s WHERE id = 1",
            [True],
        )
    output = run()
    assert (
        "#   'nightly': one-off tasks have no equivalent on a stored schedule"
        in output.splitlines()
    )
    assert "nightly" not in printed_rows(output)


def naive_expiry_not_utc(project):
    return NAIVE_EXPIRY_NOT_UTC.format(project=project)


def test_expired_task_is_not_translated(beat_tables):
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2020-01-01 00:00:00"],
        )
    output = run()
    assert "#   'nightly': it has expired" in output.splitlines()
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize(
    "start",
    ["2099-01-01 10:00:00", "2099-01-01 02:00:00"],
    ids=["between-ticks", "on-a-tick"],
)
def test_a_start_time_still_ahead_is_listed_not_carried(beat_tables, clock, start):
    """
    beat runs a task the moment its start arrives, off the schedule's own
    ticks, and a stored schedule waits for the next tick. So a start that
    has not come yet cannot be carried over, not even one that is itself a
    tick of the row's cron, and the call never names start_time.
    """
    clock("2098-12-31 00:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            [start],
        )
    output = run()
    assert "start_time=" not in output
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == START_AHEAD
    assert sorted(printed_rows(output)) == ["poller"]


def test_a_start_ahead_is_the_reason_given_whatever_the_expiry(beat_tables):
    # The start is checked before the expiry, so a window that could never
    # have held a run is listed for the start that puts it out of reach.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            ["2099-01-02 00:00:00", "2099-01-01 00:00:00"],
        )
    output = run()
    assert refusals(output).get("nightly") == START_AHEAD
    assert "nightly" not in printed_rows(output)


def test_a_future_expiry_is_preserved(beat_tables, schedulable):
    future_expiry = "2099-12-31 10:00:00"
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            [future_expiry],
        )

    # On section 2's own printed imports: a call that names datetime is
    # only runnable if the import above it is the right one.
    run_as_module(section_2(run()))

    schedule = OxSchedule.objects.get(name="nightly")
    # A microsecond short: beat does not run a tick at its expiry, and a
    # stored schedule runs one at its end_time.
    assert schedule.end_time == instant(future_expiry) - timedelta(microseconds=1)


def test_past_start_is_omitted(beat_tables):
    # create_schedule starts a schedule when it is created, which is later
    # than any start already past, so a past start adds nothing.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            ["2020-01-01 00:00:00"],
        )
    assert "start_time" not in printed_rows(run())["nightly"]


@pytest.mark.parametrize(
    "columns", [(), ("expires",)], ids=["none-of-them", "before-0007"]
)
def test_an_older_table_without_the_later_columns_still_imports(columns):
    try:
        make_beat_tables(columns)
        if columns:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE django_celery_beat_periodictask SET expires = %s "
                    "WHERE id = 1",
                    ["2099-12-31 10:00:00"],
                )
        calls = printed_rows(run())
    finally:
        drop_beat_tables()
    assert calls["nightly"]["cron"] == "0 2 * * *"
    assert calls["poller"]["every_seconds"] == 5400
    assert ("end_time" in calls["nightly"]) == bool(columns)


def test_disabled_task_remains_disabled(beat_tables):
    calls = printed_rows(run())
    assert "enabled" not in calls["nightly"]
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET enabled = %s WHERE id = 1",
            [False],
        )
    calls = printed_rows(run())
    assert calls["nightly"]["enabled"] is False
    assert "enabled" not in calls["poller"]


def test_no_stored_value_can_leave_its_literal(beat_tables, recorded):
    """
    Every stored value reaches the output as data, whatever it holds.

    The output is pasted into settings and into a shell holding production
    credentials. A line break, quote or backslash in a name, a task path, a
    cron field, a zone, a period or an argument must not end its literal or
    its comment line. Each payload sets a marker if it ever runs as code.
    """
    task = 'tasks.a"\n(OX89_TASK := "ran")\n#\\'
    minute = '0" if (OX89_CRON := "ran") else "'
    zone = "UTC\nOX89_ZONE = 'ran'\n#"
    period = "x\u2028OX89_PERIOD = 'ran'\u2028#"
    arguments = {"note": "a\rOX89_ARG = 'ran'\r#", "city": "Z\u00fcrich"}
    names = {
        "cron": "cr\u00f6n\nOX89_NAME = 'ran'\n#",
        "field": "field\nOX89_FIELD_NAME = 'ran'\n#",
        "interval": "interval' + (OX89_QUOTE := 'ran') + '\\",
        "one-off": "one-off\u2028OX89_ONE_OFF = 'ran'\u2028#",
        "expired": "expired\rOX89_EXPIRED = 'ran'\r#",
        "order": "order\nOX89_ORDER = 'ran'\n#",
        "orphan": 'orphan" + (OX89_ORPHAN := "ran") + "\n#',
        "zone": "zone\nOX89_ZONE_NAME = 'ran'\n#",
        "period": "period\rOX89_PERIOD_NAME = 'ran'\r#",
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask")
    insert_crontab(2, minute=minute)
    insert_crontab(3, timezone=zone)
    insert_interval(2, 5, period)
    insert_task(1, names["cron"], task, crontab_id=1, kwargs=json.dumps(arguments))
    insert_task(2, names["interval"], task, interval_id=1)
    insert_task(3, names["one-off"], task, crontab_id=1, one_off=True)
    insert_task(4, names["expired"], task, interval_id=1, expires="2020-01-01 00:00:00")
    insert_task(
        5,
        names["order"],
        task,
        interval_id=1,
        start_time="2099-01-02 00:00:00",
        expires="2099-01-01 00:00:00",
    )
    insert_task(6, names["orphan"], task)
    insert_task(7, names["zone"], task, crontab_id=3)
    insert_task(8, names["period"], task, interval_id=2)
    insert_task(9, names["field"], task, crontab_id=2)

    output = run()
    # One physical line for every line the command wrote, whatever a
    # terminal or an editor counts as a line break.
    assert output.isascii()
    assert "\r" not in output

    fragment = {}
    exec("TASKS = {" + section_1(output) + "}", fragment)  # noqa: S102
    assert fragment["TASKS"] == {"SCHEDULABLE_TASKS": {task: task}}
    assert not [key for key in fragment if key.startswith("OX89")]

    for apply in (run_as_module, paste_into_shell):
        recorded.clear()
        namespace = apply(section_2(output))
        assert not [key for key in namespace if key.startswith("OX89")], apply
        assert sorted(recorded, key=lambda call: call["name"]) == [
            {
                "name": names["cron"],
                "task_key": task,
                "trigger": "cron",
                "cron": "0 2 * * *",
                "arguments": arguments,
            },
            {
                "name": names["interval"],
                "task_key": task,
                "trigger": "interval",
                "every_seconds": 5400,
            },
        ]

    assert set(refusals(output)) == {
        names[key]
        for key in ("one-off", "expired", "order", "orphan", "zone", "period", "field")
    }
    assert set(differences(output)) == {names["interval"]}
    assert literal_after(output, "its schedule runs in ") == zone
    assert literal_after(output, "its interval period is ") == period
    assert literal_after(output, "its crontab's minute is ") == minute


def test_one_clock_reading_decides_every_row(beat_tables, monkeypatch):
    """
    Whether a row is translated, why it is not, and the bounds its call
    carries all come from one instant, so none can contradict another. The
    clock here jumps a year at every reading, so a decision taken on any
    reading but the first would show: a start that is ahead would be past,
    and an expiry still to come would be behind the start the check a paste
    makes is given.
    """
    first = instant("2099-01-01 00:00:00")
    readings = []

    def now():
        readings.append(first + timedelta(days=365 * len(readings)))
        return readings[-1]

    monkeypatch.setattr(timezone, "now", now)
    insert_task(4, "expired", "x.y.z", interval_id=1, expires="2098-01-01 00:00:00")
    insert_task(5, "ahead", "x.y.z", interval_id=1, start_time="2099-06-01 00:00:00")
    insert_task(6, "ends", "x.y.z", interval_id=1, expires="2099-06-01 00:00:00")
    insert_task(7, "started", "x.y.z", interval_id=1, start_time="2098-06-01 00:00:00")
    output = run()
    assert readings[0] == first
    listed = refusals(output)
    assert listed["expired"] == "it has expired"
    assert listed["ahead"] == START_AHEAD
    rows = printed_rows(output)
    assert rows["ends"]["end_time"] == instant("2099-06-01 00:00:00") - timedelta(
        microseconds=1
    )
    assert "start_time" not in rows["started"]


@pytest.mark.parametrize(
    ("start", "expires", "reason"),
    [
        # Beat counts a task as expired from the instant of its expiry.
        (None, "2099-01-01 00:00:00", "it has expired"),
        # A start still ahead is the reason, whatever the expiry leaves of
        # the window after it.
        ("2099-01-02 00:00:00", "2099-01-02 00:00:00", START_AHEAD),
        ("2099-01-02 00:00:00", "2099-01-01 23:59:59", START_AHEAD),
        ("2099-01-02 00:00:00", "2099-01-02 00:00:00.000001", START_AHEAD),
        ("2099-01-02 00:00:00", "2099-01-02 00:00:00.000002", START_AHEAD),
        # The schedule starts when it is created, which is no earlier than
        # the import, and its end is a microsecond before the expiry.
        (
            None,
            "2099-01-01 00:00:00.000001",
            "its expiry is one microsecond away, too short for a stored schedule",
        ),
        # A start at the import instant is not ahead of it, so it is let go
        # as if there were none.
        (
            "2099-01-01 00:00:00",
            "2099-01-01 00:00:00.000001",
            "its expiry is one microsecond away, too short for a stored schedule",
        ),
    ],
    ids=[
        "expiry-now",
        "expiry-at-start",
        "expiry-before-start",
        "1us-after-start",
        "2us-after-start",
        "1us-no-start",
        "1us-start-now",
    ],
)
def test_a_window_no_stored_schedule_can_hold_is_listed(
    beat_tables, clock, start, expires, reason
):
    clock("2099-01-01 00:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    output = run()
    assert f"#   'nightly': {reason}" in output.splitlines()
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize(
    ("start", "expires", "end_time"),
    [
        (None, "2099-01-01 00:00:00.000002", "2099-01-01 00:00:00.000001"),
        # A start at the import instant is not ahead of it, so it is left
        # out and the schedule starts when it is created.
        ("2099-01-01 00:00:00", "2099-01-03 00:00:00", "2099-01-02 23:59:59.999999"),
        ("2098-06-01 00:00:00", "2099-01-03 00:00:00", "2099-01-02 23:59:59.999999"),
    ],
    ids=["2us-no-start", "start-now", "start-past"],
)
def test_the_shortest_window_a_stored_schedule_holds_is_applied(
    beat_tables, schedulable, clock, start, expires, end_time
):
    now = clock("2099-01-01 00:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    output = run()
    assert "start_time=" not in output
    run_as_module(section_2(output))
    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.start_time == now
    assert schedule.end_time == instant(end_time)


class _ClockChange(tzinfo):
    """
    -05:00, then -04:00 from 2099-03-08 07:00 UTC, when local clocks skip
    from 02:00 to 03:00. Built here rather than read from tz data, whose
    rules for a future year can still change.
    """

    change = datetime(2099, 3, 8, 7)

    def utcoffset(self, dt):
        wall = dt.replace(tzinfo=None, fold=0)
        if wall < datetime(2099, 3, 8, 2):
            return timedelta(hours=-5)
        if wall >= datetime(2099, 3, 8, 3):
            return timedelta(hours=-4)
        # A wall time the change skips, read with the offset from before it
        # unless fold says otherwise, as zoneinfo reads one.
        return timedelta(hours=-4 if dt.fold else -5)

    def dst(self, dt):
        return self.utcoffset(dt) + timedelta(hours=5)

    def tzname(self, dt):
        return None

    def fromutc(self, dt):
        utc = dt.replace(tzinfo=None)
        offset = timedelta(hours=-5 if utc < self.change else -4)
        return (utc + offset).replace(tzinfo=self)


def test_an_expiry_steps_back_on_its_instant_across_a_clock_change():
    # A microsecond before 03:00 on the day clocks skip an hour is 01:59:59
    # and a fraction on the wall. Stepping back on the wall clock gives
    # 02:59:59, a time that never happens, read an hour after the expiry.
    from django_ox.management.commands.ox_import_beat_schedules import Command

    end = Command._end_time(datetime(2099, 3, 8, 3, tzinfo=_ClockChange()))
    assert end.astimezone(UTC) == datetime(2099, 3, 8, 6, 59, 59, 999999, tzinfo=UTC)
    assert end.replace(tzinfo=None) == datetime(2099, 3, 8, 1, 59, 59, 999999)


def test_a_tick_at_the_expiry_does_not_fire(beat_tables, schedulable, clock, settings):
    """
    Beat does not run a tick that falls exactly on its expiry, so the
    imported schedule must not either, although it runs the tick before.
    """
    from django_ox.worker import Worker

    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id <> 1")
        # Every minute, so a tick falls exactly on an expiry on the minute.
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET minute = %s, hour = %s",
            ["*", "*"],
        )
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-01-01 02:00:00"],
        )
    clock("2099-01-01 01:00:00")
    run_as_module(section_2(run()))
    key = f"db:{OxSchedule.objects.get(name='nightly').pk}"

    clock("2099-01-01 01:59:00")
    assert Worker(backoff_initial=0).dispatch_schedules() == 1
    expiry = clock("2099-01-01 02:00:00")
    assert Worker(backoff_initial=0).dispatch_schedules() == 0
    assert list(
        OxScheduleTick.objects.filter(schedule_name=key).values_list(
            "scheduled_for", flat=True
        )
    ) == [expiry - timedelta(minutes=1)]


def naive_project(settings, zone, *, beat_tz_aware):
    """USE_TZ off in the given zone, with beat's own setting on or off."""
    settings.USE_TZ = False
    settings.TIME_ZONE = zone
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = beat_tz_aware
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET timezone = %s", [zone]
        )


@pytest.mark.parametrize("zone", ["UTC", "America/New_York"])
@BOUNDS
def test_without_use_tz_and_with_beat_tz_aware_a_row_with_a_bound_is_listed(
    beat_tables, settings, zone, start, expires
):
    """
    beat reads the clock as an aware time under its default setting, the
    stored bound is naive, and comparing the two raises TypeError. A row
    with a start or an expiry never got past that under beat, in UTC as
    anywhere else, so there is no behaviour to carry over. Even a start
    long past is not let go here: this is asked before that. The row
    without a bound is listed too, for what stopped beat keeping to it.
    """
    naive_project(settings, zone, beat_tz_aware=True)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    output = run()
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == NAIVE_BOUND_BEAT_AWARE
    assert refusals(output)["poller"] == INTERVAL_NOT_RUN
    assert printed_rows(output) == {}


@pytest.mark.parametrize(
    "zone", ["America/New_York", "Asia/Kolkata", "Europe/London", "Etc/GMT"]
)
def test_with_both_off_an_expiry_is_listed_unless_local_time_is_utc(
    beat_tables, settings, zone
):
    """
    With both off beat compares a stored naive bound with UTC wall time: in
    Kolkata an expiry of 10:00 stopped the task at 15:30 local. A stored
    schedule reads the same value as local time and would stop it at 10:00.
    Only where local time is UTC are those one instant. London and GMT are
    listed too: equal to UTC today is not the same data.
    """
    naive_project(settings, zone, beat_tz_aware=False)
    insert_task(4, "ends-too", "x.y.z", interval_id=1, expires="2099-06-30 10:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-12-31 10:00:00"],
        )
    output = run(beat_timezone=zone)
    assert "nightly" in refusals(output)
    assert refusals(output)["nightly"] == naive_expiry_not_utc(zone)
    assert refusals(output)["ends-too"] == naive_expiry_not_utc(zone)
    assert sorted(printed_rows(output)) == ["poller"]


def test_without_use_tz_a_naive_expiry_is_printed_as_it_was_where_local_time_is_utc(
    beat_tables, settings, schedulable
):
    # The one configuration that carries a naive bound over: no offset to
    # convert from, and an end a microsecond before it on the same clock.
    naive_project(settings, "UTC", beat_tz_aware=False)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            ["2020-01-01 10:00:00", "2099-12-31 10:00:00"],
        )
    output = run(beat_timezone="UTC")
    assert "start_time" not in output
    assert "end_time=datetime.fromisoformat('2099-12-31T09:59:59.999999')" in output
    fields = printed_rows(output)["nightly"]
    assert fields["end_time"] == datetime(2099, 12, 31, 9, 59, 59, 999999)
    run_as_module(section_2(output))
    schedule = OxSchedule.objects.get(name="nightly")
    assert schedule.end_time == datetime(2099, 12, 31, 9, 59, 59, 999999)


def test_local_time_is_utc_when_the_zone_data_is_utcs_whatever_the_key(
    beat_tables, settings, zone_files
):
    """
    Decided by the two files, as zone sameness is everywhere else here.
    Reykjavik is given UTC's own file, then one that differs from it.
    """
    zone_files("UTC", ZERO)
    zone_files("Atlantic/Reykjavik", ZERO)
    naive_project(settings, "Atlantic/Reykjavik", beat_tz_aware=False)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2099-12-31 10:00:00"],
        )
    output = run(beat_timezone="Atlantic/Reykjavik")
    assert "nightly" not in refusals(output)
    assert printed_rows(output)["nightly"]["end_time"] == datetime(
        2099, 12, 31, 9, 59, 59, 999999
    )

    zone_files("Atlantic/Reykjavik", tzif((0, "OXR")))
    output = run(beat_timezone="Atlantic/Reykjavik")
    assert refusals(output)["nightly"] == naive_expiry_not_utc("Atlantic/Reykjavik")


@pytest.mark.parametrize(
    ("zone", "now", "start", "ahead"),
    [
        # 15:30 in Kolkata is 10:00 UTC. A start of 12:00 looks past on the
        # local clock and beat, comparing it with UTC, had not reached it.
        ("Asia/Kolkata", "2024-01-15 15:30:00", "2024-01-15 12:00:00", True),
        ("Asia/Kolkata", "2024-01-15 15:30:00", "2024-01-15 09:59:59", False),
        # 10:00 in New York is 15:00 UTC. A start of 12:00 looks ahead on
        # the local clock, and beat was already past it.
        ("America/New_York", "2024-01-15 10:00:00", "2024-01-15 12:00:00", False),
        ("America/New_York", "2024-01-15 10:00:00", "2024-01-15 15:00:01", True),
    ],
    ids=["past-here-ahead-in-utc", "past-in-both", "ahead-here-past-in-utc", "ahead"],
)
def test_without_use_tz_a_start_is_ahead_when_beat_would_not_have_reached_it(
    beat_tables, settings, clock, zone, now, start, ahead
):
    naive_project(settings, zone, beat_tz_aware=False)
    clock(now)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 1",
            [start],
        )
    output = run(beat_timezone=zone)
    assert "start_time=" not in output
    if ahead:
        assert refusals(output)["nightly"] == START_AHEAD
    else:
        assert "nightly" not in refusals(output)
        assert "start_time" not in printed_rows(output)["nightly"]


def test_without_use_tz_a_start_is_placed_when_time_zone_does_not_load(
    beat_tables, settings, zone_files
):
    """
    A naive start is ahead or past by the clock beat compared it with, naive
    UTC, and the import's reading of the clock is an instant whatever
    TIME_ZONE is, so a start is placed even where TIME_ZONE does not load.
    An expiry still needs TIME_ZONE, to be shown to be UTC's, and beat 2.9.0
    under these settings needs it to load its schedule at all, which is
    said.
    """
    if connection.vendor == "postgresql":
        pytest.skip("with USE_TZ off PostgreSQL is told TIME_ZONE and refuses this one")
    settings.USE_TZ = False
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    settings.TIME_ZONE = "Ox/Nowhere"
    insert_task(4, "ends", "x.y.z", interval_id=1, expires="2099-12-31 10:00:00")
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 2",
            ["2020-01-01 00:00:00"],
        )
    output = run(beat_timezone="UTC")
    unloadable = PROJECT_ZONE_UNLOADABLE.format(project="Ox/Nowhere")
    assert "poller" not in refusals(output)
    assert "start_time" not in printed_rows(output)["poller"]
    # Nor can a zone that does not load be shown to be UTC, or to differ
    # from it, which is what an expiry is listed for elsewhere. The row
    # with an expiry is listed for the zone, and so is the crontab.
    assert refusals(output).get("ends") == unloadable
    assert refusals(output).get("nightly") == unloadable
    assert SOURCE_STOPPED_PROJECT_ZONE.format(project="Ox/Nowhere") in output


def naive_new_york(settings):
    """
    USE_TZ off in a zone with clock changes, with the worker reading stored
    schedules. The dates are past ones, whose clock changes tz data will not
    move, with the clock pinned before them.
    """
    settings.USE_TZ = False
    settings.TIME_ZONE = "America/New_York"
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM django_celery_beat_periodictask WHERE id <> 1")
        cursor.execute(
            "UPDATE django_celery_beat_crontabschedule SET minute = %s, hour = %s, "
            "timezone = %s",
            ["*", "*", "America/New_York"],
        )


def minutely_until(end):
    """The schedule the row would have become, written as its call was."""
    from django_ox.stored import create_schedule

    return create_schedule(
        name="nightly",
        task_key="reports.tasks.daily",
        trigger="cron",
        cron="* * * * *",
        end_time=datetime.fromisoformat(end),
    )


def test_without_use_tz_an_expiry_after_a_skipped_hour_is_listed(
    beat_tables, schedulable, clock, settings
):
    """
    The row is listed: beat could not compare a naive expiry with its
    clock, so there is no run of it to match. The second half keeps what
    the scheduler does with such an end. At 03:00 on
    the day New York skips from 02:00, a microsecond before the expiry is
    01:59:59 and a fraction. On the wall clock it would be 02:59:59, which
    never happens, and PostgreSQL stores that an hour later, so the schedule
    would fire every minute of the hour after its expiry.
    """
    from django_ox.worker import Worker

    naive_new_york(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            ["2024-03-10 03:00:00"],
        )
    clock("2024-03-10 00:00:00")
    output = run()
    assert refusals(output) == {"nightly": NAIVE_BOUND_BEAT_AWARE}
    assert printed_rows(output) == {}

    schedule = minutely_until("2024-03-10T01:59:59.999999")
    assert schedule.end_time == datetime(2024, 3, 10, 1, 59, 59, 999999)
    fired = {}
    for wall in ("01:59", "03:00", "03:01", "03:59"):
        clock(f"2024-03-10 {wall}:00")
        fired[wall] = Worker(backoff_initial=0).dispatch_schedules()
    assert fired == {"01:59": 1, "03:00": 0, "03:01": 0, "03:59": 0}


@pytest.mark.parametrize(
    ("expires", "end", "fired"),
    [
        # 03:00 happens once, and so does the microsecond before it.
        ("03:00", "02:59:59.999999", {"02:59": 1, "03:00": 0}),
        # 02:00 happens once. The instant one microsecond before it is in
        # the second pass through the repeated hour. The naive stored end
        # is a wall-clock cutoff; these dispatch checks do not distinguish
        # the two passes.
        ("02:00", "01:59:59.999999", {"01:30": 1, "01:59": 1, "02:00": 0}),
    ],
    ids=["after-the-repeat", "end-of-the-repeat"],
)
def test_without_use_tz_an_expiry_after_a_repeated_hour_is_listed(
    beat_tables, schedulable, clock, settings, expires, end, fired
):
    """
    On the day New York repeats 01:00 to 02:00. Listed, like every naive
    expiry under beat's default setting; the dispatch half is the scheduler
    with such an end.
    """
    from django_ox.worker import Worker

    naive_new_york(settings)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
            [f"2024-11-03 {expires}:00"],
        )
    clock("2024-11-03 00:00:00")
    output = run()
    assert refusals(output) == {"nightly": NAIVE_BOUND_BEAT_AWARE}
    assert printed_rows(output) == {}

    schedule = minutely_until(f"2024-11-03T{end}")
    assert schedule.end_time == datetime.fromisoformat(f"2024-11-03 {end}")
    dispatched = {}
    for wall in fired:
        clock(f"2024-11-03 {wall}:00")
        dispatched[wall] = Worker(backoff_initial=0).dispatch_schedules()
    assert dispatched == fired


@pytest.mark.parametrize("beat_tz_aware", [True, False], ids=["aware", "not-aware"])
@pytest.mark.parametrize(
    ("start", "expires"),
    [
        (None, "2024-11-03 01:00:00"),
        (None, "2024-11-03 01:30:00"),
        (None, "2024-03-10 02:30:00"),
        ("2024-03-10 02:30:00", "2024-03-10 03:00:00"),
    ],
    ids=["repeated-hour-start", "repeated-hour", "skipped-hour", "start-skipped"],
)
def test_without_use_tz_a_bound_in_an_hour_the_clocks_change_is_listed(
    beat_tables, clock, settings, beat_tz_aware, start, expires
):
    """
    A naive local time in an hour the clocks repeat names two instants, and
    one in an hour they skip names none. The command once worked out an end
    for the row from such a time, and stopped the whole import when it
    could not. A row with a naive bound is now listed before any end is
    worked out, whichever way beat's setting stands, and the import goes on.
    """
    naive_new_york(settings)
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = beat_tz_aware
    insert_task(4, "later", "x.y.z", interval_id=1)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 1",
            [start, expires],
        )
    clock("2024-03-01 00:00:00")
    if beat_tz_aware:
        output = run()
        assert refusals(output) == {
            "nightly": NAIVE_BOUND_BEAT_AWARE,
            "later": INTERVAL_NOT_RUN,
        }
        assert printed_rows(output) == {}
    else:
        output = run(beat_timezone="America/New_York")
        assert refusals(output) == {"nightly": naive_expiry_not_utc("America/New_York")}
        assert sorted(printed_rows(output)) == ["later"]


@contextmanager
def connection_time_zone(alias, name):
    """Point one connection at another zone, as DATABASES TIME_ZONE would."""
    wrapper = connections[alias]
    original = wrapper.settings_dict["TIME_ZONE"]

    def reset():
        # Read, then delete: the zone is a cached property of the wrapper,
        # and ensure_timezone sets it on the open connection, as Django's
        # own suite does it.
        for attr in ("timezone", "timezone_name"):
            getattr(wrapper, attr)
            delattr(wrapper, attr)
        wrapper.ensure_timezone()

    wrapper.settings_dict["TIME_ZONE"] = name
    reset()
    try:
        yield
    finally:
        wrapper.settings_dict["TIME_ZONE"] = original
        reset()


@pytest.mark.skipif(not settings.USE_TZ, reason="a naive time has no zone")
def test_a_stored_time_is_read_in_the_connection_time_zone(beat_tables, schedulable):
    """
    SQLite and MySQL keep a datetime without its zone, in the connection's
    zone, and PostgreSQL returns one in it. Read in UTC instead, the
    printed times would be five hours off here. A fixed offset: no clock
    change, and nothing that depends on future tz data.
    """
    with connection_time_zone("default", "Etc/GMT+5"):
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET expires = %s WHERE id = 1",
                ["2099-12-31 10:00:00"],
            )
        output = run()
        # Applied in the same zone, which the stored row is read back in.
        run_as_module(section_2(output))
        schedule = OxSchedule.objects.get(name="nightly")
    call = next(line for line in output.splitlines() if "'nightly'" in line)
    assert "end_time=datetime.fromisoformat('2099-12-31T09:59:59.999999-05:00')" in call
    assert schedule.end_time == datetime(2099, 12, 31, 14, 59, 59, 999999, tzinfo=UTC)


@pytest.mark.skipif(not settings.USE_TZ, reason="a naive time has no zone")
@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
def test_the_zone_comes_from_the_database_it_reads():
    # --database names the alias holding the beat tables, and its zone is
    # the one their naive values were written in, whatever default's is.
    try:
        make_beat_tables(db="alt")
        with connection_time_zone("alt", "Etc/GMT+5"):
            with connections["alt"].cursor() as cursor:
                cursor.execute(
                    "UPDATE django_celery_beat_periodictask SET expires = %s "
                    "WHERE id = 1",
                    ["2099-01-01 10:00:00"],
                )
            out = StringIO()
            call_command("ox_import_beat_schedules", database="alt", stdout=out)
    finally:
        drop_beat_tables("alt")
    end = printed_rows(out.getvalue())["nightly"]["end_time"]
    assert end == datetime(2099, 1, 1, 14, 59, 59, 999999, tzinfo=UTC)
    assert end.utcoffset() == timedelta(hours=-5)


@pytest.mark.parametrize("row", [1, 2], ids=["crontab", "interval"])
@pytest.mark.parametrize(
    ("start", "expires"),
    [
        ("2020-01-01 10:00:00+03:00", None),
        ("2099-01-01 10:00:00+03:00", None),
        (None, "2099-01-01 10:00:00+03:00"),
        ("2020-01-01 10:00:00", "2099-01-01 10:00:00+00:00"),
    ],
    ids=["past-start", "future-start", "expiry", "expiry-in-utc-beside-a-naive-start"],
)
def test_with_both_off_a_bound_stored_with_an_offset_is_listed(
    beat_tables, settings, row, start, expires
):
    """
    Only SQLite can hand back an offset with USE_TZ off, from text a writer
    other than Django left. With both settings off beat reads the clock as
    naive UTC, and its check of the row raises TypeError on comparing the
    two: it did with real beat for a crontab and for an interval, a start
    and an expiry, past and ahead. Nothing ran, so the row is listed. The
    command once read the bound as the local time of the same instant and
    printed the row with that. An offset of zero in UTC is listed too: it
    is the comparison that fails, not the arithmetic.

    Asked before the row's zone is, so without --beat-timezone, which
    every crontab needs with beat's setting off. The interval beside a
    listed crontab is printed as it was, and the crontab beside a listed
    interval waits for the option.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite returns an offset without USE_TZ")
    naive_project(settings, "UTC", beat_tz_aware=False)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = %s",
            [start, expires, row],
        )
    output = run()
    assert "end_time=" not in output
    name = "nightly" if row == 1 else "poller"
    if row == 1:
        assert refusals(output).get("nightly") == OFFSET_BOUND
        assert sorted(printed_rows(output)) == ["poller"]
    else:
        assert refusals(output).get("poller") == OFFSET_BOUND
        assert refusals(output).get("nightly") == ZONE_IGNORED
        assert printed_rows(output) == {}
    # And it raised for every row beat had, not only for itself.
    assert SOURCE_STOPPED_ROWS.format(names=ascii(name)) in output.splitlines()


def test_without_use_tz_and_with_beat_tz_aware_an_offset_is_not_the_reason(
    beat_tables, settings
):
    """
    Under beat's default setting it reads the clock as an aware time, and a
    bound stored with an offset is one it can compare: with real beat the
    check of such a row did not raise. So the bound is not the reason the
    row is listed. What that configuration did to every row is. A row with
    a naive bound beside it still has one beat could not compare.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite returns an offset without USE_TZ")
    naive_project(settings, "UTC", beat_tz_aware=True)
    insert_task(4, "mixed", "x.y.z", interval_id=1)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET expires = %s",
            ["2099-01-01 10:00:00+03:00"],
        )
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s WHERE id = 4",
            ["2020-01-01 10:00:00"],
        )
    output = run()
    assert refusals(output) == {
        "nightly": BEAT_DID_NOT_RUN_ON["sqlite"],
        "poller": INTERVAL_NOT_RUN,
        "orphan": "solar and clocked schedules have no equivalent",
        "mixed": NAIVE_BOUND_BEAT_AWARE,
    }
    assert printed_rows(output) == {}


@pytest.mark.skipif(settings.USE_TZ, reason="the naive settings modules")
def test_nothing_is_translated_under_the_naive_settings_modules_as_they_stand(
    beat_tables, settings
):
    """
    As tests/settings_naive.py and tests/settings_postgres_naive.py have
    them: USE_TZ off, and beat's own setting never mentioned. Every test
    here is given USE_TZ on; this one puts it back, and changes nothing
    else. beat did not keep to a schedule under those settings, so no row
    comes out, each for its own reason.
    """
    settings.USE_TZ = False
    assert not hasattr(settings, "DJANGO_CELERY_BEAT_TZ_AWARE")
    insert_task(4, "ends", "reports.tasks.daily", crontab_id=1)
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET start_time = %s, "
            "expires = %s WHERE id = 4",
            ["2020-01-01 10:00:00", "2099-12-31 10:00:00"],
        )
    output = run()
    assert refusals(output) == {
        "nightly": BEAT_DID_NOT_RUN_ON[connection.vendor],
        "poller": INTERVAL_NOT_RUN,
        "orphan": "solar and clocked schedules have no equivalent",
        "ends": NAIVE_BOUND_BEAT_AWARE,
    }
    assert printed_rows(output) == {}


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        ("args", "['emea']", INVALID_JSON.format(column="args")),
        ("kwargs", "{'region': 'emea'}", INVALID_JSON.format(column="kwargs")),
        # More digits than Python converts from a string by default.
        ("args", "[" + "7" * 5000 + "]", INVALID_JSON.format(column="args")),
        (
            "kwargs",
            '{"region": "emea", "n": ' + "7" * 5000 + "}",
            INVALID_JSON.format(column="kwargs"),
        ),
    ],
    ids=["args-python-repr", "kwargs-python-repr", "args-long-int", "kwargs-long-int"],
)
def test_arguments_that_are_not_json_are_listed_not_dropped(
    beat_tables, column, stored, reason
):
    # Read as none, they would be translated into a call without them.
    with connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE django_celery_beat_periodictask SET {column} = %s "  # noqa: S608
            "WHERE id = 1",
            [stored],
        )
    output = run()
    assert f"#   'nightly': {reason}" in output.splitlines()
    assert "nightly" not in printed_rows(output)


@pytest.mark.parametrize("stored", [None, ""], ids=["null", "empty"])
def test_arguments_stored_as_none_are_still_translated(beat_tables, stored):
    # Beat reads NULL and an empty string as no arguments, and so does this.
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET args = %s, kwargs = %s "
            "WHERE id = 1",
            [stored, stored],
        )
    calls = printed_rows(run())
    assert "arguments" not in calls["nightly"]


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        ("kwargs", '{"a": NaN}', "kwargs contains a non-finite number"),
        (
            "kwargs",
            '{"a": [1, {"b": -Infinity}]}',
            "kwargs contains a non-finite number",
        ),
        # Finite in the text, infinite once decoded.
        ("kwargs", '{"a": 1e999}', "kwargs contains a non-finite number"),
        ("args", "[Infinity]", "args contains a non-finite number"),
    ],
    ids=["kwargs-nan", "kwargs-nested", "kwargs-overflow", "args-infinity"],
)
def test_a_non_finite_argument_is_listed_and_later_rows_still_apply(
    beat_tables, recorded, column, stored, reason
):
    """
    Bare nan and inf raise NameError when evaluated. In a module this
    prevents later calls from running; in an interactive shell the
    failing call is not applied. List the unsupported row and check
    that supported rows apply in both modes.
    """
    insert_task(4, "bad", "x.y.z", interval_id=1, **{column: stored})
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert f"#   'bad': {reason}" in output.splitlines()
    for apply in (run_as_module, paste_into_shell):
        recorded.clear()
        apply(section_2(output))
        assert sorted(call["name"] for call in recorded) == [
            "later",
            "nightly",
            "poller",
        ]


DEEP = 900
POSITIONAL = (
    "it passes positional arguments, and a stored schedule takes "
    "keyword arguments only; rewrite the task signature or the row"
)


@pytest.mark.parametrize(
    ("column", "stored", "reason"),
    [
        (
            "args",
            "[" * 600 + "0" + "]" * 600,
            POSITIONAL,
        ),
        (
            "args",
            "[" * DEEP + "0" + "]" * DEEP,
            POSITIONAL,
        ),
        (
            "kwargs",
            '{"a": ' + "[" * DEEP + "Infinity" + "]" * DEEP + "}",
            "kwargs contains a non-finite number",
        ),
    ],
    ids=["args-600", "args-900", "kwargs-900-infinity"],
)
def test_deeply_nested_arguments_are_listed_and_later_rows_still_apply(
    beat_tables, column, stored, reason
):
    # json.loads decodes lists nested this deep; looking through them for a
    # non-finite number must not run out of recursion and stop the import.
    insert_task(4, "deep", "x.y.z", interval_id=1, **{column: stored})
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert f"#   'deep': {reason}" in output.splitlines()
    assert sorted(printed_rows(output)) == ["later", "nightly", "poller"]


def test_arguments_nested_too_deeply_to_decode_are_listed(beat_tables):
    """
    A hundred thousand levels. An interpreter that cannot decode that
    raises RecursionError, which once ended the command in a traceback; one
    that can decode it cannot print it. Either way the row is listed and
    the rows after it still import.
    """
    stored = '{"a":' * 100_000 + "1" + "}" * 100_000
    try:
        json.loads(stored)
    except RecursionError:
        reason = TOO_DEEP_TO_DECODE.format(column="kwargs")
    else:
        reason = CALL_UNREADABLE
    insert_task(4, "deep", "x.y.z", interval_id=1, kwargs=stored)
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert "deep" in refusals(output)
    assert refusals(output)["deep"] == reason
    assert sorted(printed_rows(output)) == ["later", "nightly", "poller"]


@pytest.mark.parametrize("depth", [198, 300, 5000])
def test_arguments_the_printed_call_could_not_hold_are_listed(beat_tables, depth):
    """
    Keyword arguments nested a few hundred deep decode and print, and then
    Python refuses the call that holds them: "too many nested parentheses".
    Printed, that call would stop the paste.
    """
    stored = '{"a":' * depth + "1" + "}" * depth
    insert_task(4, "deep", "x.y.z", interval_id=1, kwargs=stored)
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert "deep" in refusals(output)
    assert refusals(output)["deep"] == CALL_UNREADABLE
    for apply in (run_as_module, paste_into_shell):
        with recording() as rows:
            apply(section_2(output))
        assert sorted(row["name"] for row in rows) == ["later", "nightly", "poller"]


def test_arguments_too_deep_to_validate_are_listed(beat_tables, monkeypatch):
    """
    The check a paste makes walks the arguments, and an interpreter runs
    out of recursion in that walk before it does in decoding them. The row
    is listed for what it is, and the rows after it still import.
    """
    from django_ox.management.commands import ox_import_beat_schedules as command

    preflight = command._export_preflight

    def walk(fields):
        if fields["name"] == "deep":
            raise RecursionError("maximum recursion depth exceeded")
        return preflight(fields)

    monkeypatch.setattr(command, "_export_preflight", walk)
    insert_task(4, "deep", "x.y.z", interval_id=1, kwargs='{"a": 1}')
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert refusals(output).get("deep") == CALL_UNREADABLE
    assert sorted(printed_rows(output)) == ["later", "nightly", "poller"]


def test_arguments_nested_as_deep_as_python_compiles_are_carried_over(
    beat_tables, recorded
):
    # Three levels short of the two hundred the compiler takes: the call,
    # its list and dict() are each a level around the arguments.
    depth = 197
    stored = '{"a":' * depth + "1" + "}" * depth
    insert_task(4, "deep", "x.y.z", interval_id=1, kwargs=stored)
    output = run()
    if connection.vendor == "mysql":
        # MySQL's JSON column refuses nesting past 100 levels, so there the
        # row is listed for that rather than printed to fail at the paste.
        assert refusals(output)["deep"].endswith(
            "nested more than 100 levels deep, which MySQL JSON refuses."
        )
        return
    assert "deep" not in refusals(output)
    for apply in (run_as_module, paste_into_shell):
        recorded.clear()
        apply(section_2(output))
        rows = {row["name"]: row for row in recorded}
        assert rows["deep"]["arguments"] == json.loads(stored)


@pytest.mark.parametrize(
    ("every", "period"),
    [(float("inf"), "minutes"), (float("-inf"), "seconds")],
    ids=["infinite", "negative-infinite"],
)
def test_a_non_finite_interval_is_listed(beat_tables, every, period):
    # Only SQLite can hold one, as a REAL in the integer column.
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite stores a float in an integer column")
    insert_interval(2, every, period)
    insert_task(4, "endless", "x.y.z", interval_id=2)
    insert_task(5, "later", "x.y.z", interval_id=1)
    output = run()
    assert "#   'endless': interval contains a non-finite number" in output.splitlines()
    assert sorted(printed_rows(output)) == ["later", "nightly", "poller"]


# -- beat's schedule loading, and what one row does to the others -----------------

#: Three hours west of UTC through 2023, then one: beat 2.9.0 takes the first,
#: as America/Nuuk's was on 1 January 2023, and the crontab runs in the second.
WEST_THREE_THEN_ONE = tzif((-10800, "OXA"), (-3600, "OXB"), changes=[(1704067200, 1)])
#: Eleven hours east through 2023, then eight, as Antarctica/Casey's.
EAST_ELEVEN_THEN_EIGHT = tzif((39600, "OXC"), (28800, "OXD"), changes=[(1704067200, 1)])


@pytest.mark.parametrize(
    ("zone", "hour", "named"),
    [
        (WEST_THREE_THEN_ONE, "9", True),
        (WEST_THREE_THEN_ONE, "09", True),
        # Not a plain number: beat loads it at every reload.
        (WEST_THREE_THEN_ONE, "9,9", False),
        (WEST_THREE_THEN_ONE, "009", False),
        (EAST_ELEVEN_THEN_EIGHT, "9", False),
    ],
    ids=["west", "west-padded", "west-list", "west-three-digits", "east"],
)
def test_a_crontab_beats_hour_filter_left_out_at_its_hour_is_named(
    beat_tables, settings, zone_files, clock, zone, hour, named
):
    """
    beat 2.9.0 reloads its schedule every five minutes, and leaves out a
    crontab whose hour field is a plain number unless that hour, moved by
    its timezone column's offset from the server's as both stood on 1
    January 2023, is within two hours of the server's hour or is 4. West: 09:00 at
    an hour west of UTC is 10:00 UTC, and the filter, at three hours west,
    put the crontab at 12:00. The last load before 10:00 is in hour 9,
    which leaves it out, and beat ran it at the first load that let it in.
    East, at eight hours east, 09:00 is 01:00 UTC, the filter put it at
    22:00, and a load in hour 0 keeps that: on time. Measured with beat
    in America/Nuuk, Antarctica/Troll and Antarctica/Casey.
    """
    zone_files("Ox/Project", zone)
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-06-01 00:03:30")
    set_crontab(1, timezone="Ox/Project", minute="0", hour=hour)
    output = run()
    expected = LOADED_LATE.format(date="2027-06-01") if named else None
    assert differences(output).get("nightly") == expected
    assert printed_rows(output)["nightly"]["cron"] == "0 9 * * *"


@pytest.mark.parametrize(
    ("use_tz", "project", "column"),
    [
        (True, "UTC", "Asia/Kolkata"),
        (False, "UTC", "Asia/Kolkata"),
        (False, "America/New_York", "UTC"),
    ],
    ids=["use-tz-on", "use-tz-off", "use-tz-off-column-utc"],
)
def test_with_tz_aware_off_the_hour_filter_reads_the_column_beat_ignores(
    beat_tables, settings, clock, use_tz, project, column
):
    """
    With DJANGO_CELERY_BEAT_TZ_AWARE off beat runs a crontab in the Celery
    app's timezone and its due check never reads the row's own zone. Its
    hour filter still does: 10:00 in a Kolkata column is put at 05:00, five
    hours from the hour it falls due in, and beat ran the row daily at the
    first load the filter let it in, 03:03:30 UTC, and never at 10:00.
    v1.7.0 listed these rows; this command prints them with a notice.
    The other measured case is the model's default column, 'UTC', in a New
    York project.
    """
    settings.USE_TZ = use_tz
    settings.TIME_ZONE = project
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    clock("2027-06-01 00:03:30")
    set_crontab(1, timezone=column, minute="0", hour="10")
    output = run(beat_timezone=project)
    assert differences(output).get("nightly") == LOADED_LATE.format(date="2027-06-01")
    set_crontab(1, hour="10,10")
    assert "nightly" not in differences(run(beat_timezone=project))


@pytest.mark.parametrize(
    ("project", "column", "cron", "named"),
    [
        # 01:00 with a column fourteen hours east and the server twelve west:
        # 1 - 26 + 24 is -1, which the database's remainder leaves negative,
        # in no window, ever. Python's would make it 23.
        ("Etc/GMT+12", "Pacific/Kiritimati", "0 1 * * *", True),
        # 09:00 in a Kolkata column is put at 4, an hour beat keeps always
        # for its own cleanup task.
        ("UTC", "Asia/Kolkata", "0 9 * * *", False),
        # A column two hours west puts 10:03 at 12. The last load before
        # 10:03 can be up to 305 seconds earlier, in hour 9, whose window
        # stops at 11: late. Before 10:07 it cannot be: on time.
        ("UTC", "Etc/GMT+2", "3 10 * * *", True),
        ("UTC", "Etc/GMT+2", "7 10 * * *", False),
    ],
    ids=[
        "negative-hour",
        "cleanup-hour",
        "load-in-the-hour-before",
        "load-in-the-hour",
    ],
)
def test_the_hour_filter_is_beats_to_the_hour(
    beat_tables, settings, clock, project, column, cron, named
):
    """
    beat 2.9.0's arithmetic as it stands: the hour moved by whole hours cut
    toward zero, kept in the day by the database's remainder, held against
    the hours two either side of the server's and the hour 4, at a load at
    most 305 seconds before the run, its loop's default. Each case is one
    of those rules deciding.
    """
    settings.USE_TZ = False
    settings.TIME_ZONE = project
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    clock("2027-06-01 00:00:00")
    minute, hour = cron.split()[:2]
    set_crontab(1, timezone=column, minute=minute, hour=hour)
    output = run(beat_timezone=project)
    expected = LOADED_LATE.format(date="2027-06-01") if named else None
    assert differences(output).get("nightly") == expected


@pytest.mark.parametrize(
    ("version", "line"),
    [
        ("2.8.1", BEAT_VERSION_UNMEASURED.format(version="'2.8.1'")),
        (None, BEAT_NOT_INSTALLED),
    ],
    ids=["another-version", "none"],
)
def test_beats_loading_is_said_not_to_have_been_checked_for_another_version(
    beat_tables, settings, zone_files, clock, monkeypatch, version, line
):
    """
    What the command knows of beat's schedule loading is what 2.9.0 does.
    With any other version beside it, or none, it says that this was not
    checked, and does not name a row as run late or say what stopped beat.
    """
    from django_ox.management.commands import ox_import_beat_schedules as command

    monkeypatch.setattr(command, "_beat_version", lambda: version)
    zone_files("Ox/Project", WEST_THREE_THEN_ONE)
    settings.TIME_ZONE = "Ox/Project"
    clock("2027-06-01 00:03:30")
    set_crontab(1, timezone="Ox/Project", minute="0", hour="9")
    insert_crontab(7, timezone="")
    output = run()
    assert output.splitlines()[6:8] == [line, ""]
    assert "nightly" not in differences(output)
    assert "ran nothing" not in output


def test_with_use_tz_and_without_beat_tz_aware_one_bounded_row_stopped_beat(
    beat_tables, settings
):
    """
    Under these settings beat compares a start time or an expiry with a
    naive clock and raises, on every tick: with real beat nothing in the
    table ran. The row is listed for itself, as before. The others are
    printed, and the output says first that beat was not running them. A
    disabled row is never loaded, and stops nothing.
    """
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone="UTC")
    insert_task(4, "bounded", "x.y.z", interval_id=1, expires="2099-01-01 00:00:00")
    output = run(beat_timezone="UTC")
    assert refusals(output)["bounded"] == AWARE_BOUND_BEAT_NAIVE
    assert output.splitlines()[6] == SOURCE_STOPPED_ROWS.format(names="'bounded'")
    assert sorted(printed_rows(output)) == ["nightly", "poller"]
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET enabled = %s WHERE id = 4",
            [False],
        )
    output = run(beat_timezone="UTC")
    assert refusals(output)["bounded"] == AWARE_BOUND_BEAT_NAIVE
    assert "ran nothing" not in output


def test_with_both_off_a_start_still_ahead_stopped_beat_until_it_passed(
    beat_tables, settings, clock
):
    """
    With USE_TZ and beat's own setting off, a start still ahead by the UTC
    clock makes beat raise on every tick until it passes: with real beat
    nothing in the table ran before it, and everything after. A start
    already past stops nothing.
    """
    naive_project(settings, "UTC", beat_tz_aware=False)
    clock("2027-06-01 10:00:00")
    for start, line in [
        (
            "2027-06-01 12:33:30",
            SOURCE_STOPPED_UNTIL.format(until="2027-06-01 12:33:30", names="'poller'"),
        ),
        ("2027-06-01 09:00:00", None),
    ]:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET start_time = %s "
                "WHERE id = 2",
                [start],
            )
        output = run(beat_timezone="UTC")
        if line is None:
            assert "ran nothing" not in output
            continue
        assert refusals(output)["poller"] == START_AHEAD
        assert output.splitlines()[6] == line
        assert sorted(printed_rows(output)) == ["nightly"]


@pytest.mark.parametrize(
    ("column", "stored", "stops"),
    [
        ("minute", "0,,30", True),
        ("minute", "-5", True),
        ("hour", "1-5/", True),
        ("minute", "*/", True),
        ("minute", "60", False),
        ("day_of_week", "7", False),
        # Celery reads the hour first: its parse error comes before the
        # minute's out-of-range value, and stops the load.
        ("minute,hour", "60,", True),
    ],
)
def test_a_crontab_beat_cannot_build_stopped_beat_unless_beat_left_it_out(
    beat_tables, column, stored, stops
):
    """
    beat 2.9.0 builds each enabled row's schedule as it loads them, and
    leaves out a row whose crontab Celery refuses with a ValueError. Celery
    refuses some fields with its ParseException instead, which is no
    ValueError: an empty part, a step with nothing after the slash, a minus
    sign. That one is not caught, and stops every load. The row is listed
    either way; where beat ran none of the others the output says so.
    """
    fields = (
        {"minute": "60", "hour": ""} if column == "minute,hour" else {column: stored}
    )
    insert_crontab(2, **fields)
    insert_task(4, "broken", "x.y.z", crontab_id=2)
    output = run()
    assert "broken" in refusals(output)
    stop = SOURCE_STOPPED_BUILD.format(names="'broken'")
    assert (stop in output.splitlines()) == stops
    assert sorted(printed_rows(output)) == ["nightly", "poller"]
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_periodictask SET enabled = %s WHERE id = 4",
            [False],
        )
    assert stop not in run().splitlines()


@pytest.mark.parametrize(
    ("every", "period", "stops"),
    [
        (2, "Days", True),
        (1_000_000_000, "days", True),
        (2, "weeks", False),
    ],
    ids=["unknown-period", "past-a-timedelta", "a-period-beat-builds"],
)
def test_an_interval_beat_cannot_build_stopped_beat(beat_tables, every, period, stops):
    # timedelta(**{period: every}) raises TypeError or OverflowError for these,
    # which beat does not catch either. A period beat builds stops nothing,
    # whether or not this command translates it; weeks it now translates.
    insert_interval(2, every, period)
    insert_task(4, "broken", "x.y.z", interval_id=2)
    output = run()
    if stops:
        assert "broken" in refusals(output)
    else:
        assert "broken" in printed_rows(output)
    stop = SOURCE_STOPPED_BUILD.format(names="'broken'")
    assert (stop in output.splitlines()) == stops


@pytest.mark.parametrize(
    ("variant", "process_zone"),
    [("configure", "America/Chicago"), ("tzset", "Asia/Kolkata")],
)
@pytest.mark.parametrize(
    ("start", "ahead"),
    [
        ("2027-06-01 09:59:59", False),
        ("2027-06-01 10:00:00", False),
        ("2027-06-01 10:00:01", True),
    ],
    ids=["before", "at", "after"],
)
def test_without_use_tz_a_start_is_placed_by_beats_clock_whatever_the_process_zone(
    beat_tables, settings, monkeypatch, variant, process_zone, start, ahead
):
    """
    With USE_TZ and beat's own setting off, beat compares a naive start with
    naive UTC. timezone.now() is then this process's wall clock, which is
    TIME_ZONE's only where Django set the process's zone from it: not under
    settings.configure(), not on Windows. The command once read it as
    TIME_ZONE's, so a start beat had not reached could be let go as past.
    The instant is 10:00 UTC throughout. "configure" changes TIME_ZONE
    behind Django's back, as settings.configure() leaves it; "tzset" moves
    the process's zone off a TIME_ZONE Django set. The command calls
    tzset() in neither; only this test does, to make the process's zone.
    """
    import os
    import time as clock_module

    if not hasattr(clock_module, "tzset"):
        pytest.skip("no tzset() here to give the process a zone")
    if variant == "configure":
        settings.USE_TZ = False
        settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
        monkeypatch.setattr(settings._wrapped, "TIME_ZONE", "UTC")
        set_crontab(1, timezone="UTC")
    else:
        naive_project(settings, "UTC", beat_tz_aware=False)
    was = os.environ.get("TZ")
    os.environ["TZ"] = process_zone
    clock_module.tzset()
    try:
        at = datetime(2027, 6, 1, 10, 0, tzinfo=UTC).timestamp()
        monkeypatch.setattr(timezone, "now", lambda: datetime.fromtimestamp(at))
        assert datetime.fromtimestamp(at).hour != 10
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET start_time = %s "
                "WHERE id = 2",
                [start],
            )
        output = run(beat_timezone="UTC")
    finally:
        if was is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = was
        clock_module.tzset()
    if ahead:
        assert refusals(output)["poller"] == START_AHEAD
    else:
        assert "poller" not in refusals(output)
        assert "start_time" not in printed_rows(output)["poller"]


def test_the_installed_beat_is_read_from_its_metadata(monkeypatch):
    # The real lookup, which the rest of this file replaces: the version
    # installed beside the command, or None where there is none, as in the
    # test environments. Never by importing it.
    import importlib.metadata

    from django_ox.management.commands import ox_import_beat_schedules as command

    monkeypatch.undo()
    try:
        installed = importlib.metadata.version("django-celery-beat")
    except importlib.metadata.PackageNotFoundError:
        installed = None
    assert command._beat_version() == installed
    assert "django_celery_beat" not in sys.modules


def test_a_bounded_row_beat_never_loads_stops_nothing(beat_tables, settings):
    """
    Under USE_TZ on and DJANGO_CELERY_BEAT_TZ_AWARE off a bound stops beat
    only from a row in its schedule. One whose crontab Celery refuses with
    a ValueError, and one whose interval row is gone, beat leaves out as it
    loads, and with real beat the others ran.
    """
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone="UTC")
    insert_crontab(2, minute="60", timezone="UTC")
    insert_task(4, "refused", "x.y.z", crontab_id=2, expires="2099-01-01 00:00:00")
    insert_task(5, "orphaned", "x.y.z", interval_id=9, expires="2099-01-01 00:00:00")
    output = run(beat_timezone="UTC")
    assert set(refusals(output)) >= {"refused", "orphaned"}
    assert "ran nothing" not in output
    assert sorted(printed_rows(output)) == ["nightly", "poller"]


@pytest.mark.parametrize(
    ("column", "stored", "stops"),
    [
        # Celery reads a BLOB as the set of its byte values: b"5" as a
        # minute is 53, and it builds the row, which beat then loads.
        ("minute", b"5", True),
        # Empty, it is no value at all, which Celery's range check passes.
        ("minute", b"", True),
        # b"0" as an hour is 48, out of range: a ValueError, which beat
        # catches, leaving the row out.
        ("hour", b"0", False),
    ],
    ids=["in-range", "empty", "out-of-range"],
)
def test_a_crontab_stored_as_a_blob_stops_beat_as_celery_reads_it(
    beat_tables, settings, column, stored, stops
):
    """
    A crontab field stored as a BLOB, which only SQLite keeps in a text
    column. The row is listed whatever the BLOB holds, here for its bound.
    Whether it stopped beat depends on what Celery made of the BLOB: a row
    beat builds and loads, with a bound under these settings, stopped every
    tick; one Celery refuses with a ValueError, beat left out.
    """
    if connection.vendor != "sqlite":
        pytest.skip("only SQLite keeps a BLOB in a text column")
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone="UTC")
    insert_crontab(2, timezone="UTC", **{column: stored})
    insert_task(4, "blob", "x.y.z", crontab_id=2, expires="2099-01-01 00:00:00")
    output = run(beat_timezone="UTC")
    assert refusals(output)["blob"] == AWARE_BOUND_BEAT_NAIVE
    stop = SOURCE_STOPPED_ROWS.format(names="'blob'")
    assert (stop in output.splitlines()) is stops
    assert ("ran nothing" in output) is stops


def stop_beat(kind, count, settings, clock):
    """
    count enabled rows, named stop-00 on, each of which alone stopped beat
    for the whole table in the way kind says, under settings that make it.
    Returns the line saying so, as a function of the list it holds; what
    that list names each row by, in the order the command reads them: for
    kind "zone" the row's crontab, for the others its name; the reason
    each row is listed for; and the options to run the command with.
    """
    names = [f"stop-{index:02}" for index in range(count)]
    literals = [ascii(name) for name in names]
    if kind == "zone":
        # A crontab whose zone is empty fails every load of beat's schedule.
        for index, name in enumerate(names):
            insert_crontab(10 + index, timezone="")
            insert_task(10 + index, name, "x.y.z", crontab_id=10 + index)
        ids = [str(10 + index) for index in range(count)]
        return (
            (lambda listing: SOURCE_STOPPED_ZONE.format(ids=listing)),
            ids,
            ZONE_EMPTY,
            {},
        )
    if kind == "build":
        # Celery refuses a minus sign with a ParseException, which beat's
        # loading does not catch.
        insert_crontab(2, minute="-5")
        for index, name in enumerate(names):
            insert_task(10 + index, name, "x.y.z", crontab_id=2)
        reason = FIELD_NOT_CELERY.format(column="minute", text="-5")
        return (
            (lambda listing: SOURCE_STOPPED_BUILD.format(names=listing)),
            literals,
            reason,
            {},
        )
    if kind == "rows":
        settings.TIME_ZONE = "UTC"
        settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
        set_crontab(1, timezone="UTC")
        for index, name in enumerate(names):
            insert_task(
                10 + index, name, "x.y.z", interval_id=1, expires="2099-01-01 00:00:00"
            )
        return (
            (lambda listing: SOURCE_STOPPED_ROWS.format(names=listing)),
            literals,
            AWARE_BOUND_BEAT_NAIVE,
            {"beat_timezone": "UTC"},
        )
    naive_project(settings, "UTC", beat_tz_aware=False)
    clock("2027-06-01 10:00:00")
    for index, name in enumerate(names):
        insert_task(
            10 + index, name, "x.y.z", interval_id=1, start_time="2027-06-01 12:33:30"
        )
    return (
        (
            lambda listing: SOURCE_STOPPED_UNTIL.format(
                until="2027-06-01 12:33:30", names=listing
            )
        ),
        literals,
        START_AHEAD,
        {"beat_timezone": "UTC"},
    )


@pytest.mark.parametrize("count", [10, 11, 25])
@pytest.mark.parametrize("kind", ["zone", "build", "rows", "until"])
def test_a_line_saying_beat_ran_nothing_names_ten_of_its_rows_at_most(
    beat_tables, settings, clock, kind, count
):
    """
    Such a line names the first ten rows that stopped beat, in order, and
    says how many more there are. Ten it names in full, with nothing after
    them. Every row is still listed below for itself.
    """
    line, named, reason, options = stop_beat(kind, count, settings, clock)
    output = run(**options)
    listing = {
        10: ", ".join(named),
        11: ", ".join(named[:10]) + LIST_CUT.format(more=1),
        25: ", ".join(named[:10]) + LIST_CUT.format(more=15),
    }[count]
    said = [text for text in output.splitlines() if "ran nothing" in text]
    assert said == [line(listing)]
    listed = refusals(output)
    assert [listed.get(f"stop-{index:02}") for index in range(count)] == [
        reason
    ] * count


def test_a_line_saying_beat_ran_nothing_does_not_grow_with_the_table(
    beat_tables, settings
):
    """
    However many rows stopped beat, the line naming them keeps one bound:
    ten names, each quoted as a bounded excerpt of itself, and a count.
    The names here are as long as the column holds, and on SQLite, which
    holds any length, longer than one excerpt quotes.
    """
    settings.TIME_ZONE = "UTC"
    settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
    set_crontab(1, timezone="UTC")
    width = 2000 if connection.vendor == "sqlite" else 200
    said = {}
    added = 0
    for count in (25, 250):
        while added < count:
            insert_task(
                10 + added,
                f"{added:03}-".ljust(width, "n"),
                "x.y.z",
                interval_id=1,
                expires="2099-01-01 00:00:00",
            )
            added += 1
        output = run(beat_timezone="UTC")
        [said[count]] = [text for text in output.splitlines() if "ran nothing" in text]
        # Each row is listed below for itself all the same.
        assert output.count(f": {AWARE_BOUND_BEAT_NAIVE}") == count
    # Ten times as many rows, and only the count says so.
    assert said[250] == said[25].replace(
        LIST_CUT.format(more=15), LIST_CUT.format(more=240)
    )
    # Ten names, each a literal of at most 1,300 characters and the mark
    # that says it was cut.
    assert len(said[250]) < len(SOURCE_STOPPED_ROWS) + 10 * 1400
