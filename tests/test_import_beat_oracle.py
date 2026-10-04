"""
ox_import_beat_schedules against Celery itself.

The command reads a beat crontab with its own implementation of Celery's
grammar, because it cannot import Celery where it runs. That is safe only
while the two agree. So Celery is imported here plainly, at the top, where
a missing package is an error and not a skip, and what the command makes
of a crontab is held to what Celery's own parser makes of the same strings.

Every input is generated, either exhaustively or from a fixed seed, and no
date below is today's.
"""

import calendar
import itertools
import random
import re
import zoneinfo
from datetime import UTC, date, datetime, timedelta

import pytest
from celery import Celery
from celery.schedules import crontab, crontab_parser
from django.conf import settings
from django.db import connection
from django.utils import timezone

from django_ox.cron import CronExpression
from django_ox.management import _beat_cron
from django_ox.management._beat_cron import (
    FIELDS,
    NotCelery,
    NotCeleryParse,
    celery_values,
    expression,
)
from django_ox.management._beat_timing import _late_days, _late_on, changes

from .test_import_beat import (
    BOTH_DAY_FIELDS,
    CRON_NOT_WITHIN_LIMIT,
    FIELD_NOT_CELERY,
    NO_EXPRESSION_FITS,
    REPEATED_ONCE,
    REPEATED_RUN,
    SKIPPED_RUN,
    apart,
    beat_installed,  # noqa: F401
    differences,
    drop_beat_tables,
    make_beat_tables,
    printed_rows,
    refusals,
    run,
    use_tz,  # noqa: F401
)

# With USE_TZ on, for the reason tests/test_import_beat.py gives: off, with
# beat's own setting left alone, no row is translated at all.
pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("use_tz"),
]

SEED = 20261003

#: Four years of days, with a 29 February in them: every date a crontab's
#: day and month fields can tell apart.
DAYS = [date(2024, 1, 1) + timedelta(days=n) for n in range(1461)]


def celery_reads(text, field):
    """One field as Celery's parser reads it: its values, or None if refused."""
    try:
        return frozenset(
            crontab_parser(field.high - field.low + 1, field.low).parse(text)
        )
    except Exception:
        return None


def importer_reads(text, field):
    try:
        return celery_values(text, field)
    except NotCelery:
        return None


def disagreements(texts):
    """
    Where the importer and Celery read a field differently, and how many
    readings each of them accepted and refused.
    """
    differing, accepted, refused = [], 0, 0
    for text in texts:
        for field in FIELDS:
            theirs = celery_reads(text, field)
            if theirs is None:
                refused += 1
            else:
                accepted += 1
            if importer_reads(text, field) != theirs:
                differing.append((text, field.column))
    return differing, accepted, refused


def test_every_short_string_is_read_as_celery_reads_it():
    """
    All of them: every string of up to five characters over the characters
    the grammar is made of, in each of the five fields. Long enough to hold
    a stepped range, a list, a name, a sign and stray whitespace.
    """
    alphabet = "017*/-, m+\n"
    texts = (
        "".join(characters)
        for length in range(6)
        for characters in itertools.product(alphabet, repeat=length)
    )
    differing, accepted, refused = disagreements(texts)
    assert differing == []
    # A comparison in which Celery refused everything would pass too.
    assert accepted > 10_000
    assert refused > 800_000


NAMES = [
    *("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct"),
    *("nov", "dec", "sun", "mon", "tue", "wed", "thu", "fri", "sat"),
    *("Monday", "MONDAY", "sunday", "Saturday", "january", "December", "sept"),
    *("tues", "thursday", "marzipan", "mo", "xyz", "m0n", "\u00e9t\u00e9", "\u017fat"),
]
SPACES = [" ", "\t", "\n", "\r", "\u00a0", "\u2028", "  "]
STEPS = [
    *("0", "1", "2", "3", "5", "7", "15", "30", "60", "100", "00", "010"),
    *("", "x", "mon", "1_0", "_1", "\u0662", " 2", "+2", "-1", "2 ", "1.5"),
]
JUNK = ["", "", "", "", "", "", " ", "\n", " x", "-3", "/2", "!", "x", ":", "*"]
#: Digits int() reads that are not ASCII's: Arabic-Indic, and fullwidth.
ARABIC_INDIC = {ord(str(n)): chr(0x0660 + n) for n in range(10)}
FULLWIDTH = {ord(str(n)): chr(0xFF10 + n) for n in range(10)}


def a_number(rng, field):
    """A number at, inside or just outside the field's range, dressed up."""
    value = rng.choice(
        [
            field.low - 1,
            field.low,
            field.low + 1,
            field.high - 1,
            field.high,
            field.high + 1,
            rng.randint(field.low, field.high),
            rng.randint(0, 70),
        ]
    )
    text = str(value)
    dress = rng.randrange(14)
    if dress == 0:
        return "0" + text
    if dress == 1:
        return "+" + text
    if dress == 2:
        return rng.choice(SPACES) + text
    if dress == 3:
        return text + rng.choice(SPACES)
    if dress == 4 and len(text) > 1:
        return text[0] + "_" + text[1:]
    if dress == 5:
        return text.translate(ARABIC_INDIC)
    if dress == 6:
        return text.translate(FULLWIDTH)
    return text


def a_value(rng, field):
    return rng.choice(NAMES) if rng.random() < 0.25 else a_number(rng, field)


def a_part(rng, field):
    form = rng.randrange(8)
    if form == 0:
        part = "*"
    elif form == 1:
        part = "*/" + rng.choice(STEPS)
    elif form in (2, 3):
        part = f"{a_value(rng, field)}-{a_value(rng, field)}"
    elif form == 4:
        part = f"{a_value(rng, field)}-{a_value(rng, field)}/{rng.choice(STEPS)}"
    else:
        part = a_value(rng, field)
    return part + rng.choice(JUNK)


def a_field(rng, field):
    parts = [a_part(rng, field) for _ in range(rng.choice([1, 1, 1, 2, 2, 3]))]
    if rng.random() < 0.05:
        parts.insert(rng.randrange(len(parts) + 1), "")
    return ",".join(parts)


def test_generated_fields_are_read_as_celery_reads_them():
    """
    Boundaries, ranges that wrap, names short, long and wrong, signs,
    underscores, digits from other scripts, whitespace of every kind, steps
    of every kind and junk after each of them, in every field.
    """
    rng = random.Random(SEED)
    texts = [a_field(rng, field) for field in FIELDS for _ in range(6_000)]
    differing, accepted, refused = disagreements(texts)
    assert differing == []
    assert accepted > 20_000
    assert refused > 20_000


def a_likely_field(rng, field):
    """A field Celery usually accepts, as people write them."""
    form = rng.randrange(10)
    low, high = field.low, field.high
    first, last = sorted((rng.randint(low, high), rng.randint(low, high)))
    if form < 3:
        return "*"
    if form == 3:
        return f"*/{rng.randint(1, high)}"
    if form == 4:
        return f"{first}-{last}"
    if form == 5:
        # Often one that wraps.
        return f"{rng.randint(low, high)}-{rng.randint(low, high)}"
    if form == 6:
        return f"{first}-{last}/{rng.randint(1, 5)}"
    if form == 7:
        return ",".join(str(rng.randint(low, high)) for _ in range(rng.randint(1, 4)))
    if form == 8:
        return rng.choice(NAMES[:23])
    return a_field(rng, field)


def a_crontab(rng):
    return [a_likely_field(rng, field) for field in FIELDS]


#: Dates the calendar decides: ones that never come, one that comes every
#: fourth year, and the last day there is. Too rare to leave to chance.
CALENDAR_CRONTABS = [
    ["0", "0", "31", "2", "*"],
    ["*/5", "*", "30-31", "feb", "*"],
    ["5", "4", "31", "4,6,9,11", "*"],
    ["0", "0", "31", "apr-jun/2", "*"],
    ["0", "0", "29", "2", "*"],
    ["0", "0", "29-31", "2", "*"],
    ["0", "12", "31", "*", "*"],
    ["0", "12", "30,31", "1-6", "*"],
]

#: Pairs of neighbours, which a range makes no shorter than a list.
PAIRED_HOURS = "0-1,3-4,6-7,9-10,12-13,15-16,18-19,21-22"
PAIRED_MONTHS = "1-2,4-5,7-8,10-11"

#: Past what a stored schedule holds written a stretch at a time, with
#: either day field narrowed, and well inside it written as progressions
#: that may overlap: values two and three apart by turns are every fifth
#: and every fifth from the second, and pairs of neighbours are every third
#: and every third from the next. The last two as beat would store them.
WRITTEN_IN_STEPS = [
    [apart(0, 52), apart(0, 23), apart(1, 31), apart(1, 12), "*"],
    [apart(1, 53), apart(1, 23), apart(2, 31), apart(2, 12), "*"],
    [apart(0, 52), PAIRED_HOURS, "*", PAIRED_MONTHS, "0,2-3,5-6"],
    ["*/3,1-59/3", "*/3,1-23/3", "*", "*/3,2-12/3", "*"],
    ["*/5,2-59/5", "*/5,2-23/5", "1-31/5,3-31/5", "1-12/5,3-12/5", "*"],
]

#: No expression django-ox reads writes it in 128 characters: see
#: NO_EXPRESSION_FITS in tests/test_import_beat.py.
NO_EXPRESSION_FITS_ROW = [*NO_EXPRESSION_FITS.values(), "*"]

#: The last of those with two days fewer: as long as a stored schedule
#: holds, to the character, and so printed.
AT_THE_LIMIT = [apart(0, 52), PAIRED_HOURS, "*", PAIRED_MONTHS, "0,2,5"]

#: Fields in step that "*/n" does not say, each with the expression printed
#: for it: stepped ranges off the field's first value, lists that are one,
#: a wrapped one that is not, pairs, and stretches of each kind side by
#: side. Written out value by value the first three would pass the length a
#: stored schedule holds.
STEPPED_CRONTABS = {
    ("1-59/2", "1-23/2", "1-31/2", "*", "*"): "1-59/2 1-23/2 */2 * *",
    ("1-59/2", "1-23/2", "2-31/2", "*", "*"): "1-59/2 1-23/2 2-30/2 * *",
    ("1-59/2", "1-23/2", "*", "2-12/2", "1-6/2"): "1-59/2 1-23/2 * 2-12/2 1-5/2",
    ("5,20,35,50", "3-21/6", "*", "*", "mon-fri/2"): "5-50/15 3-21/6 * * 1-5/2",
    ("22-58/4", "22-2/2", "*", "nov-feb/2", "*"): "22-58/4 0,2,22 * 1,11 *",
    ("0", "9", "*", "*", "0,6"): "0 9 * * 0,6",
    ("0", "9", "1,31", "*", "*"): "0 9 1,31 * *",
    ("0", "*/12", "*", "*/6", "*"): "0 0,12 * 1,7 *",
    ("0-20/10,21-23,40", "0,8,9,10", "*", "*", "*"): "0-20/10,21-23,40 0,8-10 * * *",
}


def celery_crontab(texts):
    """The crontab Celery builds from five stored strings, or None if refused."""
    minute, hour, day_of_month, month_of_year, day_of_week = texts
    try:
        return crontab(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
        )
    except Exception:
        return None


def celery_fires_on(schedule, day):
    """Celery runs on a day that is in all three of its day and month sets."""
    return (
        day.month in schedule.month_of_year
        and day.day in schedule.day_of_month
        and day.isoweekday() % 7 in schedule.day_of_week
    )


def leaves_out_a_date(schedule):
    """
    Does Celery's day-of-month set leave out a date of a month its month set
    allows, in a leap year or in another? Asked of the calendar module, not
    of the command's own table of month lengths.
    """
    return any(
        day not in schedule.day_of_month
        for year in (2024, 2025)
        for month in schedule.month_of_year
        for day in range(1, calendar.monthrange(year, month)[1] + 1)
    )


def narrows_both(schedule):
    """Celery's day of the month leaves out a date, and its weekday a day."""
    return leaves_out_a_date(schedule) and len(schedule.day_of_week) < 7


def test_generated_crontabs_fire_when_celery_fires_them_or_are_listed():
    """
    Whole crontabs through the real command. Each row is either printed,
    and then the expression printed for it fires on exactly the days,
    hours and minutes Celery fires the row on, over four years; or it is
    listed, for exactly the reason it should be: a field Celery refuses,
    two narrowed day fields, a date that never comes, or no expression
    found that a stored schedule holds.
    """
    rng = random.Random(SEED)
    # Within the width of the stand-in columns, which PostgreSQL and MySQL
    # enforce.
    crontabs = [
        texts
        for texts in (a_crontab(rng) for _ in range(1_500))
        if all(len(text) <= 64 for text in texts)
    ]
    crontabs += CALENDAR_CRONTABS + WRITTEN_IN_STEPS + [NO_EXPRESSION_FITS_ROW]
    crontabs += [list(texts) for texts in STEPPED_CRONTABS]
    crontabs.append(AT_THE_LIMIT)
    try:
        make_beat_tables()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM django_celery_beat_periodictask")
            cursor.executemany(
                "INSERT INTO django_celery_beat_crontabschedule "
                "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
                "timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, *texts, settings.TIME_ZONE]
                    for n, texts in enumerate(crontabs)
                ],
            )
            cursor.executemany(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, enabled, crontab_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, f"row-{n}", "app.tasks.t", "[]", "{}", True, 100 + n]
                    for n in range(len(crontabs))
                ],
            )
        output = run()
    finally:
        drop_beat_tables()
    printed = printed_rows(output)
    listed = refusals(output)

    outcomes = {"printed": 0, "celery": 0, "days": 0, "never": 0, "long": 0}
    in_steps = stepped = 0
    for n, texts in enumerate(crontabs):
        name = f"row-{n}"
        assert (name in printed) != (name in listed), name
        schedule = celery_crontab(texts)
        if schedule is None:
            column = next(
                field.column
                for text, field in zip(texts, FIELDS, strict=True)
                if celery_reads(text, field) is None
            )
            text = texts[[field.column for field in FIELDS].index(column)]
            assert listed.get(name) == FIELD_NOT_CELERY.format(
                column=column, text=text
            ), (name, texts)
            outcomes["celery"] += 1
            continue
        if narrows_both(schedule):
            assert listed.get(name) == BOTH_DAY_FIELDS, (name, texts)
            outcomes["days"] += 1
            continue
        fires = [celery_fires_on(schedule, day) for day in DAYS]
        if not any(fires):
            # The 31st of a month that has 30 days. Celery accepts the
            # crontab and never runs it; django-ox refuses the expression.
            assert listed.get(name, "").startswith("its crontab would be written "), (
                name,
                texts,
            )
            assert "can never match" in listed[name]
            outcomes["never"] += 1
            continue
        sets = _beat_cron.without_redundant_days(
            tuple(
                celery_reads(text, field)
                for text, field in zip(texts, FIELDS, strict=True)
            )
        )
        written = _beat_cron.shortest(sets, 128)
        if len(written) > 128:
            assert listed.get(name) == CRON_NOT_WITHIN_LIMIT.format(
                limit=128, length=len(written)
            ), (name, texts)
            outcomes["long"] += 1
            continue
        assert name in printed, (name, texts, listed.get(name))
        assert len(printed[name]["cron"]) <= 128, (name, printed[name]["cron"])
        in_steps += len(expression(sets)) > 128
        cron = CronExpression(printed[name]["cron"])
        assert set(cron.minutes) == schedule.minute, (name, texts)
        assert set(cron.hours) == schedule.hour, (name, texts)
        assert [
            day.month in cron.months
            and cron._day_matches(datetime(day.year, day.month, day.day))
            for day in DAYS
        ] == fires, (name, texts, printed[name]["cron"])
        outcomes["printed"] += 1
        if tuple(texts) in STEPPED_CRONTABS:
            assert printed[name]["cron"] == STEPPED_CRONTABS[tuple(texts)], texts
            stepped += 1
    # Each of the five outcomes was provoked, the common ones often.
    assert outcomes["printed"] > 300, outcomes
    assert outcomes["celery"] > 100, outcomes
    assert outcomes["days"] > 100, outcomes
    assert outcomes["never"] >= 4, outcomes
    assert outcomes["long"] >= 1, outcomes
    # Printed, each, only because a cover fits where a stretch at a time
    # does not.
    assert in_steps >= len(WRITTEN_IN_STEPS), in_steps
    assert stepped == len(STEPPED_CRONTABS)
    assert len(" ".join(AT_THE_LIMIT)) == 128
    assert printed[f"row-{len(crontabs) - 1}"]["cron"] == " ".join(AT_THE_LIMIT)


def import_crontabs(crontabs):
    """The command's output for these crontabs, one row each, named row-n."""
    try:
        make_beat_tables()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM django_celery_beat_periodictask")
            cursor.executemany(
                "INSERT INTO django_celery_beat_crontabschedule "
                "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
                "timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, *texts, settings.TIME_ZONE]
                    for n, texts in enumerate(crontabs)
                ],
            )
            cursor.executemany(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, enabled, crontab_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, f"row-{n}", "app.tasks.t", "[]", "{}", True, 100 + n]
                    for n in range(len(crontabs))
                ],
            )
        return run()
    finally:
        drop_beat_tables()


def fires_as_celery_fires(cron_text, schedule):
    """Every minute, hour and date of four years, a leap day among them."""
    cron = CronExpression(cron_text)
    return (
        set(cron.minutes) == schedule.minute
        and set(cron.hours) == schedule.hour
        and [
            day.month in cron.months
            and cron._day_matches(datetime(day.year, day.month, day.day))
            for day in DAYS
        ]
        == [celery_fires_on(schedule, day) for day in DAYS]
    )


#: Each with its weekday narrowed. A day of the month that leaves out no
#: date of its months, and controls that leave out one: the 29th of a leap
#: February (a Thursday in 2024, so a wrong translation would fire on it
#: inside the four years compared), the 30th of a thirty-day month, the
#: 31st of one month among shorter ones, the 1st.
CALENDAR_DAYS = [
    ["0", "9", "1-30", "2", "1"],
    ["0", "9", "1-29", "2", "mon"],
    ["0", "9", "1-30", "4,6,9,11", "1-5"],
    ["0", "9", "1-29", "feb", "4"],
    ["0", "9", "1-30", "2,4", "sat,sun"],
    ["0", "9", "1-30", "4", "4"],
    ["0", "9", "1-28", "2", "4"],
    ["0", "9", "1-29", "4", "1"],
    ["0", "9", "1-30", "1,4", "1"],
    ["0", "9", "2-31", "2", "1"],
    ["0", "9", "1-30", "*", "1"],
    ["0", "9", "*/2", "feb", "4"],
]


def test_a_day_of_month_whole_on_the_calendar_fires_as_celery_fires_it():
    """
    A row whose day of the month leaves out no date of its months is
    printed, and fires on exactly Celery's dates; one that leaves out a
    date is listed. Which is which is asked of the calendar module and of
    Celery's sets, not of the command.
    """
    output = import_crontabs(CALENDAR_DAYS)
    printed, listed = printed_rows(output), refusals(output)
    whole = narrowing = 0
    for n, texts in enumerate(CALENDAR_DAYS):
        name = f"row-{n}"
        schedule = celery_crontab(texts)
        if narrows_both(schedule):
            assert listed.get(name) == BOTH_DAY_FIELDS, texts
            narrowing += 1
            continue
        assert name in printed, (texts, listed.get(name))
        assert printed[name]["cron"].split()[2] == "*", printed[name]
        assert fires_as_celery_fires(printed[name]["cron"], schedule), texts
        whole += 1
    assert (whole, narrowing) == (6, 6)


def in_step(field):
    """Every set of values in one field that are one distance apart."""
    for first in range(field.low, field.high + 1):
        for step in range(1, field.high - field.low + 1):
            for last in range(first, field.high + 1, step):
                yield frozenset(range(first, last + 1, step))


def test_a_written_field_is_read_by_both_parsers_as_the_values_it_was_written_from():
    """
    The writer on its own. Every set of values one distance apart, in every
    field, and sets drawn at random among them: what is written for a set
    is read by Celery's parser and by django-ox's as that set and no other.
    The bare star is written for the whole field only, being the one
    spelling django-ox reads as unrestricted. A step is written for three
    values or more, never for a pair, and "*/n" only for every n-th value
    of the whole field. Nothing is written longer than the list of its
    values.
    """
    rng = random.Random(SEED + 3)
    whole = [frozenset(range(field.low, field.high + 1)) for field in FIELDS]
    checked = star_steps = stepped_ranges = 0
    for position, field in enumerate(FIELDS):
        sets = set(in_step(field))
        for _ in range(3_000):
            size = rng.randint(1, len(whole[position]))
            sets.add(frozenset(rng.sample(sorted(whole[position]), size)))
        for values in sorted(sets, key=sorted):
            fields = [*whole]
            fields[position] = values
            cron = expression(tuple(fields))
            written = cron.split()[position]
            assert celery_reads(written, field) == values, (field.column, written)
            parsed = CronExpression(cron)
            read_back = (
                parsed.minutes,
                parsed.hours,
                parsed.days_of_month,
                parsed.months,
                parsed.days_of_week,
            )[position]
            assert frozenset(read_back) == values, (field.column, written)
            assert (written == "*") == (values == whole[position]), written
            assert len(written) <= len(",".join(map(str, sorted(values)))), written
            for part in written.split(","):
                body, slash, step = part.partition("/")
                if not slash:
                    continue
                if body == "*":
                    assert written == part
                    said = range(field.low, field.high + 1, int(step))
                    star_steps += 1
                else:
                    first, last = map(int, body.split("-"))
                    said = range(first, last + 1, int(step))
                    assert said[-1] == last, written
                    stepped_ranges += 1
                assert len(said) >= 3, (field.column, written)
                assert int(step) > 1, (field.column, written)
            checked += 1
    assert checked > 15_000
    # Every step over a whole field that has three values or more, and no
    # other: 28 of them as minutes, 10 as hours, 14 as days of the month, 4
    # as months and 2 as days of the week.
    assert star_steps == 58
    assert stepped_ranges > 5_000


def test_a_field_written_past_the_limit_is_read_by_django_ox_as_its_values():
    """
    The writer for a row longer than a stored schedule holds, field by
    field: every set of values in step and their unions, and sets drawn at
    random at every density. What it writes is read by django-ox's parser
    as that set and no other; it is never longer than the field written a
    stretch at a time; and it is the same text whatever order the set was
    built in. A limit of nought sends every row down that path.

    Celery is not asked: past the limit a field may be written with a step
    to the top of the field, "1/3", which django-ox reads as vixie cron does
    and Celery refuses. Nothing reads a printed expression but django-ox.
    """
    rng = random.Random(SEED + 4)
    whole = [frozenset(range(field.low, field.high + 1)) for field in FIELDS]
    checked = vixie = 0
    for position, field in enumerate(FIELDS):
        steps = sorted(set(in_step(field)), key=sorted)
        sets = set(steps[::7])
        for _ in range(400):
            sets.add(rng.choice(steps) | rng.choice(steps))
            density = rng.random()
            drawn = frozenset(v for v in whole[position] if rng.random() < density)
            if drawn:
                sets.add(drawn)
        for values in sorted(sets, key=sorted):
            fields = [*whole]
            fields[position] = values
            cron = _beat_cron.shortest(tuple(fields), 0)
            written = cron.split()[position]
            parsed = CronExpression(cron)
            read_back = (
                parsed.minutes,
                parsed.hours,
                parsed.days_of_month,
                parsed.months,
                parsed.days_of_week,
            )[position]
            assert frozenset(read_back) == values, (field.column, written)
            assert len(written) <= len(expression(tuple(fields)).split()[position])
            shuffled = list(values)
            rng.shuffle(shuffled)
            again = list(fields)
            again[position] = frozenset(shuffled)
            assert _beat_cron.shortest(tuple(again), 0) == cron
            vixie += any(
                part.partition("/")[0].isdigit() and "/" in part
                for part in written.split(",")
            )
            checked += 1
    assert checked > 3_000
    # The spelling Celery lacks was reached, so the parse above covered it.
    assert vixie > 100


def test_a_printed_expression_fires_minute_for_minute_as_celery_does():
    """
    The same comparison at the grain a schedule runs at: every minute of
    days chosen to sit either side of a month's end, a leap day and a
    week's end, through the method the dispatcher itself asks.
    """
    rng = random.Random(SEED + 1)
    crontabs = []
    while len(crontabs) < 60:
        texts = a_crontab(rng)
        schedule = celery_crontab(texts)
        if schedule is None or (
            len(schedule.day_of_month) < 31 and len(schedule.day_of_week) < 7
        ):
            continue
        if (
            any(len(text) > 64 for text in texts)
            or len(
                expression(
                    tuple(
                        celery_reads(text, field)
                        for text, field in zip(texts, FIELDS, strict=True)
                    )
                )
            )
            > 128
        ):
            continue
        if any(celery_fires_on(schedule, day) for day in DAYS):
            crontabs.append((texts, schedule))
    try:
        make_beat_tables()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM django_celery_beat_periodictask")
            cursor.executemany(
                "INSERT INTO django_celery_beat_crontabschedule "
                "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
                "timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, *texts, settings.TIME_ZONE]
                    for n, (texts, _) in enumerate(crontabs)
                ],
            )
            cursor.executemany(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, enabled, crontab_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, f"row-{n}", "app.tasks.t", "[]", "{}", True, 100 + n]
                    for n in range(len(crontabs))
                ],
            )
        output = run()
    finally:
        drop_beat_tables()
    printed = printed_rows(output)
    assert sorted(printed) == sorted(f"row-{n}" for n in range(len(crontabs)))
    days = [
        date(2024, 2, 28),
        date(2024, 2, 29),
        date(2024, 3, 1),
        date(2025, 2, 28),
        date(2025, 12, 31),
        date(2026, 1, 1),
        date(2026, 5, 30),
        date(2026, 5, 31),
        date(2027, 8, 15),
    ]
    compared = fired = 0
    for n, (texts, schedule) in enumerate(crontabs):
        cron = CronExpression(printed[f"row-{n}"]["cron"])
        for day in days:
            on_this_day = celery_fires_on(schedule, day)
            for minute_of_day in range(1440):
                hour, minute = divmod(minute_of_day, 60)
                celery_fires = (
                    on_this_day and hour in schedule.hour and minute in schedule.minute
                )
                moment = datetime(day.year, day.month, day.day, hour, minute)
                assert cron.matches(moment) == celery_fires, (texts, moment)
                compared += 1
                fired += celery_fires
    assert compared == 60 * 9 * 1440
    assert fired > 1_000


def repeated_minutes(zone, start, end):
    """
    Every minute of wall-clock time a zone shows twice between two instants,
    in the order it first shows it.

    Found without reasoning about offsets at all: the instants are walked
    an hour at a time, and wherever the offset is not what it was an hour
    earlier, every instant of the six hours around that hour is turned into
    the wall-clock minute it shows. A minute two instants show is one the
    clock went through twice.
    """
    repeated = []
    hour = timedelta(hours=1)
    moment = start
    before = moment.astimezone(zone).utcoffset()
    while moment < end:
        moment += hour
        offset = moment.astimezone(zone).utcoffset()
        if offset == before:
            continue
        before = offset
        shown = {}
        instant = moment - 3 * hour
        while instant < moment + 3 * hour:
            wall = instant.astimezone(zone).replace(tzinfo=None, fold=0)
            if wall.second == 0:
                shown.setdefault(wall, []).append(instant)
            instant += timedelta(seconds=1 if wall.second else 60)
        repeated.extend(
            wall
            for wall, instants in shown.items()
            if len(instants) > 1 and start < instants[1] and instants[0] <= end
        )
    return repeated


@pytest.mark.parametrize(
    "zone",
    [
        "Europe/Berlin",
        "America/New_York",
        "America/Havana",
        # Half an hour, not an hour.
        "Australia/Lord_Howe",
        # Back and forward again around Ramadan as well as in spring and autumn.
        "Africa/Casablanca",
        # Two hours at once.
        "Antarctica/Troll",
        # Changes on the half hour of UTC, not on the hour.
        "Asia/Tehran",
        "Australia/Adelaide",
        # No clock change at all.
        "Asia/Kolkata",
    ],
)
def test_generated_crontabs_are_named_when_a_run_is_in_a_minute_shown_twice(
    settings, monkeypatch, zone
):
    """
    The naming of a row is held to the clock itself. For each zone, the
    minutes its wall clock shows twice in the ten years from the start of
    2010 are found by walking the instants and looking, and a crontab is
    named exactly when Celery's own reading of its five fields matches one
    of them, with the date of the first. Past years, so that no later change
    of a zone's rules moves what is looked at.
    """
    info = zoneinfo.ZoneInfo(zone)
    start = datetime(2010, 1, 1, tzinfo=UTC)
    twice = repeated_minutes(info, start, start.replace(year=2020))
    rng = random.Random(SEED + 2)
    # On the hour and the half hour of every hour a clock is put back in or
    # to, which is where a stretch shown twice begins and ends, and then
    # whatever the generator makes.
    crontabs = [
        ([minute, hour, "*", "*", "*"], celery_crontab([minute, hour, "*", "*", "*"]))
        for hour in ("0", "1", "2", "3", "23", "*")
        for minute in ("0", "30", "59")
    ]
    while len(crontabs) < 170:
        texts = a_crontab(rng)
        schedule = celery_crontab(texts)
        if schedule is None or (
            len(schedule.day_of_month) < 31 and len(schedule.day_of_week) < 7
        ):
            continue
        written = expression(
            tuple(
                celery_reads(text, field)
                for text, field in zip(texts, FIELDS, strict=True)
            )
        )
        if any(len(text) > 64 for text in texts) or len(written) > 128:
            continue
        if any(celery_fires_on(schedule, day) for day in DAYS):
            crontabs.append((texts, schedule))
    settings.TIME_ZONE = zone
    monkeypatch.setattr(timezone, "now", lambda: start)
    try:
        make_beat_tables()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM django_celery_beat_periodictask")
            cursor.executemany(
                "INSERT INTO django_celery_beat_crontabschedule "
                "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
                "timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [[100 + n, *texts, zone] for n, (texts, _) in enumerate(crontabs)],
            )
            cursor.executemany(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, enabled, crontab_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, f"row-{n}", "app.tasks.t", "[]", "{}", True, 100 + n]
                    for n in range(len(crontabs))
                ],
            )
        output = run()
    finally:
        drop_beat_tables()
    assert not refusals(output)
    # The repeated-time sentence alone: a row can be named for a gap, or
    # for beat's schedule loading, as well, and other tests hold those.
    named = {
        name: found[1]
        for name, notice in differences(output).items()
        if (found := sentence(REPEATED_RUN).search(notice))
    }
    expected = {}
    for n, (_, schedule) in enumerate(crontabs):
        for wall in twice:
            if (
                wall.minute in schedule.minute
                and wall.hour in schedule.hour
                and celery_fires_on(schedule, wall)
            ):
                expected[f"row-{n}"] = wall.date().isoformat()
                break
    assert named == expected
    # The zones that change their clocks name rows, and leave rows unnamed.
    if zone == "Asia/Kolkata":
        assert not twice
        assert not named
    else:
        assert twice
        assert 10 < len(named) < len(crontabs)


def sentence(template):
    """A notice's template as a pattern that finds it and captures its date."""
    return re.compile(
        re.escape(template).replace(re.escape("{date}"), r"(\d{4}-\d{2}-\d{2})")
    )


def beat_runs(texts, zone, start, end):
    """
    The instants beat runs a crontab at, after start and up to end, by
    Celery's own crontab: from each run, the time it says remains to the
    next, counted in UTC. It starts from a day before, where beat in steady
    running has run whatever fell due.
    """
    minute, hour, day_of_month, month_of_year, day_of_week = texts
    app = Celery("oracle", set_as_current=False)
    app.conf.timezone = zone.key
    clock = [start]
    schedule = crontab(
        minute=minute,
        hour=hour,
        day_of_month=day_of_month,
        month_of_year=month_of_year,
        day_of_week=day_of_week,
        nowfun=lambda: clock[0].astimezone(zone),
        app=app,
    )
    last = start - timedelta(days=1)
    runs = []
    for _ in range(100_000):
        clock[0] = last
        due = last + max(schedule.remaining_estimate(last.astimezone(zone)), MINIMUM)
        if due > end:
            return runs
        if due > start:
            runs.append(due)
        last = due
    raise AssertionError("beat never got past the window")


MINIMUM = timedelta(microseconds=1)


def stored_runs(cron, zone, start, end, *, aware):
    """
    The instants a stored schedule runs a crontab at, after start and up to
    end, by the dispatcher's own steps (Worker.dispatch_schedules): the
    latest run time at or before the wall clock, made an instant as
    timezone.make_aware makes it with USE_TZ and compared as a wall clock
    time without; run if it has come and is later than the last one run.
    Looked at only where that can change: at each run time's instant under
    either offset, and at each change of offset. Created long before.
    """
    parsed = CronExpression(cron)

    def due(at):
        wall = at.astimezone(zone).replace(tzinfo=None)
        tick = parsed.previous(wall)
        if aware:
            return tick.replace(tzinfo=zone).astimezone(UTC), at
        return tick, wall

    moments = {start}
    wall = start.astimezone(zone).replace(tzinfo=None) - timedelta(hours=3)
    stop = end.astimezone(zone).replace(tzinfo=None) + timedelta(hours=3)
    tick = parsed.following(wall)
    while tick <= stop:
        for fold in (0, 1):
            at = tick.replace(tzinfo=zone, fold=fold).astimezone(UTC)
            if start <= at <= end:
                moments.add(at)
        tick = parsed.following(tick)
    moments.update(change.instant for change in changes(zone, start, end))
    last, _ = due(start)
    runs = []
    for at in sorted(moments):
        tick, now = due(at)
        if tick <= now and tick > last:
            runs.append(at)
            last = tick
    return runs


def parting(texts, cron, zone, change, *, aware):
    """The wall clock date the two engines first part on around one change."""
    start = change.instant - timedelta(hours=4)
    end = change.instant + timedelta(hours=4)
    ours = beat_runs(texts, zone, start, end)
    theirs = stored_runs(cron, zone, start, end, aware=aware)
    for one, other in itertools.zip_longest(ours, theirs):
        if one != other:
            first = min(run for run in (one, other) if run is not None)
            return first.astimezone(zone).date().isoformat()
    return None


#: Run times at the edges of every change below, and the shapes that part.
EDGE_CRONTABS = [
    [minute, hour, "*", "*", "*"]
    for hour in ("0", "1", "2", "3", "1,2", "1-3", "2,3", "*")
    for minute in ("0", "15", "30", "45", "59", "0,30", "*/20", "15,45")
]


@pytest.mark.parametrize("aware", [True, False], ids=["use-tz-on", "use-tz-off"])
@pytest.mark.parametrize(
    "zone",
    [
        "Europe/Berlin",
        # Half an hour, two hours at once, and a change at midnight.
        "Australia/Lord_Howe",
        "Antarctica/Troll",
        "America/Havana",
    ],
)
def test_generated_crontabs_are_named_where_the_two_engines_part_over_a_change(
    settings, monkeypatch, zone, aware
):
    """
    Notices held to both engines' own steps: beat's by Celery's crontab, a
    stored schedule's by the dispatcher's. For every change of a zone's
    clocks in the ten years from 2010, the two are run around it side by
    side, and a crontab is named for a gap, and for a repeated hour, exactly
    where they part, with the date of the first. With USE_TZ off the stored
    schedule reads wall clock times, and beat's own setting is off too, as
    the import of those settings needs.
    """
    info = zoneinfo.ZoneInfo(zone)
    start = datetime(2010, 1, 1, tzinfo=UTC)
    horizon = start.replace(year=2020)
    rng = random.Random(SEED + 3)
    crontabs = list(EDGE_CRONTABS)
    while len(crontabs) < len(EDGE_CRONTABS) + 40:
        texts = a_crontab(rng)
        schedule = celery_crontab(texts)
        if schedule is None or (
            len(schedule.day_of_month) < 31 and len(schedule.day_of_week) < 7
        ):
            continue
        written = expression(
            tuple(
                celery_reads(text, field)
                for text, field in zip(texts, FIELDS, strict=True)
            )
        )
        if any(len(text) > 64 for text in texts) or len(written) > 128:
            continue
        if any(celery_fires_on(schedule, day) for day in DAYS):
            crontabs.append(texts)
    settings.USE_TZ = aware
    settings.TIME_ZONE = zone
    options = {}
    if aware:
        monkeypatch.setattr(timezone, "now", lambda: start)
    else:
        settings.DJANGO_CELERY_BEAT_TZ_AWARE = False
        options["beat_timezone"] = zone
        # This process's wall clock at that instant, as timezone.now() is.
        monkeypatch.setattr(
            timezone, "now", lambda: datetime.fromtimestamp(start.timestamp())
        )
    try:
        make_beat_tables()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM django_celery_beat_periodictask")
            cursor.executemany(
                "INSERT INTO django_celery_beat_crontabschedule "
                "(id, minute, hour, day_of_month, month_of_year, day_of_week, "
                "timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [[100 + n, *texts, zone] for n, texts in enumerate(crontabs)],
            )
            cursor.executemany(
                "INSERT INTO django_celery_beat_periodictask "
                "(id, name, task, args, kwargs, enabled, crontab_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    [100 + n, f"row-{n}", "app.tasks.t", "[]", "{}", True, 100 + n]
                    for n in range(len(crontabs))
                ],
            )
        output = run(**options)
    finally:
        drop_beat_tables()
    printed = printed_rows(output)
    assert sorted(printed) == sorted(f"row-{n}" for n in range(len(crontabs)))
    repeated = sentence(REPEATED_RUN if aware else REPEATED_ONCE)
    said = {
        name: (
            (found[1] if (found := repeated.search(notice)) else None),
            (found[1] if (found := sentence(SKIPPED_RUN).search(notice)) else None),
        )
        for name, notice in differences(output).items()
    }
    seen = {"repeated": 0, "skipped": 0}
    for n, texts in enumerate(crontabs):
        name = f"row-{n}"
        cron = printed[name]["cron"]
        first = {"repeated": None, "skipped": None}
        for change in changes(info, start, horizon):
            kind = "repeated" if change.after < change.before else "skipped"
            if first[kind] is None:
                first[kind] = parting(texts, cron, info, change, aware=aware)
        expected = (first["repeated"], first["skipped"])
        assert said.get(name, (None, None)) == expected, (name, texts, cron)
        seen = {kind: seen[kind] + (first[kind] is not None) for kind in seen}
    # Both kinds of parting were provoked where the settings allow them: a
    # repeated hour parts the engines with USE_TZ on, and not off.
    assert seen["skipped"] >= 3, seen
    if aware:
        assert seen["repeated"] >= 3, seen
    else:
        assert seen["repeated"] == 0, seen


def test_celerys_parse_errors_are_told_from_its_value_errors():
    """
    beat 2.9.0 leaves out a row whose crontab Celery refuses with a
    ValueError, and stops on one it refuses with its ParseException, so
    which of the two the importer's reading raises is held to Celery's,
    over every short string the grammar is made of.
    """
    alphabet = "017*/-, m"
    differing = []
    counted = {"parse": 0, "value": 0}
    for length in range(1, 6):
        for characters in itertools.product(alphabet, repeat=length):
            text = "".join(characters)
            for field in FIELDS:
                try:
                    crontab_parser(field.high - field.low + 1, field.low).parse(text)
                    theirs = None
                except crontab_parser.ParseException:
                    theirs = "parse"
                except ValueError:
                    theirs = "value"
                try:
                    celery_values(text, field)
                    ours = None
                except NotCeleryParse:
                    ours = "parse"
                except NotCelery:
                    ours = "value"
                if ours != theirs:
                    differing.append((text, field.column, ours, theirs))
                if theirs:
                    counted[theirs] += 1
    assert differing == []
    assert counted["parse"] > 10_000
    assert counted["value"] > 10_000


def test_the_hour_filters_shortcut_answers_each_day_as_a_walk_would():
    """
    Which days beat 2.9.0's loading leaves a run out on is worked out one
    day at a time only near a change of either zone's offset; between them
    each day is taken to answer as the first after a change does. Held here
    to the plain walk over every day, in zones whose offsets change twice a
    year, once, or not, for ten years from now. Both sides read the same
    zone data, so a later change of the rules cannot part them.
    """
    first = date(2026, 10, 4).toordinal()
    last = date(2036, 10, 4).toordinal()
    rng = random.Random(SEED + 4)
    zones = [
        "America/Nuuk",
        "Antarctica/Troll",
        "Antarctica/Casey",
        "America/New_York",
        "Europe/Berlin",
        "Asia/Kolkata",
        "Australia/Lord_Howe",
        "Asia/Tehran",
        "UTC",
    ]
    # Two that are late in their summers only, as measured, then any.
    cases = [("Antarctica/Troll", "UTC", 9, 9, 0), ("America/Nuuk", "UTC", 12, 9, 0)]
    for _ in range(40):
        due, server = rng.choice(zones), rng.choice(["UTC", rng.choice(zones)])
        hour, minute = rng.randrange(24), rng.choice([0, 1, 4, 5, 30, 59])
        cases.append((due, server, rng.randrange(-2, 24), hour, minute))
    late = mixed = 0
    for due, server, mark, hour, minute in cases:
        quick = _late_days(due, server, mark, hour, minute, first, last)
        walked = bytes(
            _late_on(
                date.fromordinal(first + index),
                hour,
                minute,
                mark,
                zoneinfo.ZoneInfo(due),
                zoneinfo.ZoneInfo(server),
            )
            for index in range(last - first + 1)
        )
        assert quick == walked, (due, server, mark, hour, minute)
        late += any(quick)
        mixed += any(quick) and not all(quick)
    # The comparison saw days that are late, and zones where only some are.
    assert late > 10
    assert mixed >= 2
