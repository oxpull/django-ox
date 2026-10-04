"""
A django-celery-beat crontab, read the way Celery reads it.

beat stores five strings and hands them to Celery's ``crontab``, whose
grammar is neither cron's nor this package's. A range whose end is below
its start wraps (``22-2`` as hours is 22, 23, 0, 1, 2). The name of a
month or of a weekday is matched on its first three letters, in any field,
so ``monday`` is 1 and so is ``jan`` as a minute. A number is whatever
``int()`` accepts, spaces around it included. Whatever follows a range or
a step Celery has recognised is ignored. django-celery-beat validates the
strings in its admin and nowhere else, so a table can also hold ones
Celery itself refuses.

``ox_import_beat_schedules`` cannot import Celery, so the grammar is
implemented here and the test suite holds it to Celery's own parser. Each
field becomes the set of values Celery fires on, and the expression
printed for django-ox is written from those sets, never from the stored
text: two parsers that both accept a string need not mean the same thing
by it.
"""

from __future__ import annotations

import re
from typing import NamedTuple


class Field(NamedTuple):
    """One crontab field: beat's column, and the range Celery expands it over."""

    column: str
    low: int
    high: int


DAY_OF_MONTH = Field("day_of_month", 1, 31)
#: Stops at 6: Celery has Sunday as 0 and refuses 7.
DAY_OF_WEEK = Field("day_of_week", 0, 6)

#: In the order a cron expression lists them.
FIELDS = (
    Field("minute", 0, 59),
    Field("hour", 0, 23),
    DAY_OF_MONTH,
    Field("month_of_year", 1, 12),
    DAY_OF_WEEK,
)

_MONTHS = {
    name: number
    for number, name in enumerate(
        (
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ),
        start=1,
    )
}
_WEEKDAYS = {
    name: number
    for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}

# Matched at the start of a part and no further, as Celery matches them, so
# "1-5-7" is 1 to 5 and "*/5 or so" is every fifth. \w is any letter, digit
# or underscore in any script, which is also what int() reads digits in.
_RANGE = re.compile(r"(\w+)-(\w+)(/(\w+)?)?")
_STAR_STEP = re.compile(r"\*/(\w+)?")


class NotCelery(ValueError):
    """A crontab field Celery refuses, so beat never ran the row it is on."""


class NotCeleryParse(NotCelery):
    """
    A field Celery refuses with its ParseException, which is no ValueError:
    an empty part, a step with nothing after the slash, a number with a
    minus sign. beat 2.9.0 leaves out a row whose schedule raises a
    ValueError as it loads, and a row whose schedule raises this stops the
    load for every row.
    """


def celery_values(text: str, field: Field) -> frozenset[int]:
    """The values Celery fires on for one stored field, or NotCelery."""
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise NotCeleryParse
        values.update(_part(part, field.low, field.high))
    return frozenset(values)


def _part(part: str, low: int, high: int) -> list[int]:
    ranged = _RANGE.match(part)
    if ranged:
        # Celery looks for the step before it reads the range's ends.
        if ranged[3] and not ranged[4]:
            raise NotCeleryParse
        first = _number(ranged[1], low, high)
        last = _number(ranged[2], low, high)
        if last < first:
            span = [*range(first, high + 1), *range(low, last + 1)]
        else:
            span = list(range(first, last + 1))
        return _every(span, ranged[4]) if ranged[3] else span
    stepped = _STAR_STEP.match(part)
    if stepped:
        return _every(list(range(low, high + 1)), stepped[1])
    # A lone star, and a star before a final line break: Celery's pattern
    # for it ends in "$", which matches there too.
    if part in ("*", "*\n"):
        return list(range(low, high + 1))
    return [_number(part, low, high)]


def _every(span: list[int], step: str | None) -> list[int]:
    """
    Every step-th value, counted along the span rather than by value: a
    wrapped range is stepped in the order it was walked, so 22-2/2 as hours
    is 22, 0 and 2.
    """
    if step is None:
        raise NotCeleryParse
    try:
        count = int(step)
    except ValueError:
        raise NotCelery from None
    if count == 0:
        raise NotCelery
    return span[::count]


def _number(text: str, low: int, high: int) -> int:
    # Refused on the sign itself, before int() is asked: "-0" is a number
    # in range and Celery still refuses it. A plus sign passes.
    if text.startswith("-"):
        raise NotCeleryParse
    try:
        value = int(text)
    except ValueError:
        # A month's name or a weekday's, whichever field this is: "mar" as
        # a day of the week is 3, Wednesday, and "mon" as a month is 1,
        # January. No name is both.
        name = text[:3].lower()
        named = _MONTHS.get(name, _WEEKDAYS.get(name))
        if named is None:
            raise NotCelery from None
        value = named
    if not low <= value <= high:
        raise NotCelery
    return value


#: The most days each month has in any year. February's is 29: a leap
#: year's February runs on its 29th, so a day-of-month set without it
#: still leaves a date out.
LONGEST_MONTHS = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


def without_redundant_days(
    fields: tuple[frozenset[int], ...],
) -> tuple[frozenset[int], ...]:
    """
    The five sets. Where the day of the week narrows and the day-of-month
    set leaves out no date of the months the row runs in, that set is made
    whole.

    Celery fires on a date in all three of its day and month sets, so the
    day of the month narrows only by a date the month field allows: 1-30
    in February is every day February has, and Mondays in February are
    then the weekday field's doing alone. Made whole, the field is written
    "*" and django-ox reads the row as Celery does. One date missing, the
    29th of a leap February or the 31st of a month that has one, and it
    narrows. Where every weekday is allowed the field is left as it was:
    it reads the same either way, and is printed as the values it is.
    """
    minutes, hours, days, months, weekdays = fields
    if _narrows(weekdays, DAY_OF_WEEK) and all(
        days.issuperset(range(1, LONGEST_MONTHS[month] + 1)) for month in months
    ):
        days = frozenset(range(DAY_OF_MONTH.low, DAY_OF_MONTH.high + 1))
    return minutes, hours, days, months, weekdays


def narrows_both_day_fields(fields: tuple[frozenset[int], ...]) -> bool:
    """
    Do day of month and day of week each leave out a day?

    Celery fires on a day that is in both sets. django-ox reads two
    restricted day fields as either, which is cron's own rule, so a
    crontab that narrows both has no five-field expression here that
    means the same.
    """
    days, weekdays = fields[2], fields[4]
    return _narrows(days, DAY_OF_MONTH) and _narrows(weekdays, DAY_OF_WEEK)


def _narrows(values: frozenset[int], field: Field) -> bool:
    return len(values) < field.high - field.low + 1


def expression(fields: tuple[frozenset[int], ...]) -> str:
    """The five sets as one cron expression in django-ox's syntax."""
    return " ".join(
        _written(values, field) for values, field in zip(fields, FIELDS, strict=True)
    )


#: The fewest values written as a step. Two values are a pair whatever
#: lies between them, and read as one: a weekend is "0,6" and not "*/6".
_IN_STEP = 3


def _written(values: frozenset[int], field: Field) -> str:
    """
    One field, as short as it stays readable.

    A full set is a bare star, which is the only spelling django-ox treats
    as "unrestricted" in the two day fields. Three or more values that are
    every n-th of the whole field are that step, "*/15". Anything else is
    written from its lowest value up, a stretch at a time: neighbours as a
    range, "1-5"; three or more values the same distance apart as a
    stepped range, "1-59/2"; and a value with neither as itself. So a
    stored "*/2" or "1-59/2" does not come back as thirty numbers and run
    past the length a stored cron expression may have.
    """
    ordered = sorted(values)
    whole = list(range(field.low, field.high + 1))
    if ordered == whole:
        return "*"
    if len(ordered) >= _IN_STEP:
        step = ordered[1] - ordered[0]
        if ordered == whole[::step]:
            return f"*/{step}"
    parts: list[str] = []
    start = 0
    while start < len(ordered):
        end, step = _stretch(ordered, start)
        first, last = ordered[start], ordered[end]
        if step == 1 and end > start:
            parts.append(f"{first}-{last}")
        elif end - start + 1 >= _IN_STEP:
            parts.append(f"{first}-{last}/{step}")
        else:
            # Alone, or one of a pair. The other of the pair is looked at
            # afresh: it may begin a stretch of its own.
            parts.append(str(first))
            end = start
        start = end + 1
    return ",".join(parts)


def _stretch(ordered: list[int], start: int) -> tuple[int, int]:
    """
    How far the values go on from one of them at its distance to the next:
    the index of the last that does, and the distance.
    """
    if start + 1 == len(ordered):
        return start, 1
    step = ordered[start + 1] - ordered[start]
    end = start + 1
    while end + 1 < len(ordered) and ordered[end + 1] - ordered[end] == step:
        end += 1
    return end, step


def shortest(fields: tuple[frozenset[int], ...], limit: int) -> str:
    """
    The five sets as one expression, no longer than `limit` if this module
    finds one that short.

    `expression`'s own whenever it fits, so a row that was printed before
    is printed the same. Past the limit each field is also covered by
    progressions that may overlap, and the shorter of the two is taken: a
    stretch at a time, every fifth minute together with every fifth from
    the second is "0,2,5,7,10,...,52", and covered it is "0-50/5,2-52/5".
    The cover is greedy, so what comes back longer than the limit shows
    that nothing shorter was found, not that nothing shorter exists.
    """
    written = expression(fields)
    if len(written) <= limit:
        return written
    return " ".join(
        min(_written(values, field), _covered(values, field), key=len)
        for values, field in zip(fields, FIELDS, strict=True)
    )


#: The values written with one digit, as a mask over a field's values.
_ONE_DIGIT = (1 << 10) - 1


def _covered(values: frozenset[int], field: Field) -> str:
    """
    One field as progressions that may overlap, and whatever none of them
    takes written value by value: the shorter of two greedy covers.

    Every progression of two values or more that the set holds is a
    candidate, taken from its first value as far as the set goes. One
    cover takes, round by round, the candidate that saves the most
    characters over writing out the values it would newly cover, until
    none saves any. The other takes the candidate, a lone value included,
    that costs the fewest characters for each value it newly covers, until
    every value is covered. Neither is always the shorter, and neither is
    always the shortest there is.

    The same set always comes out the same: candidates are built in order,
    a tie keeps the earlier, and the parts are written in order of their
    first value. Bounded by the field: at most sixty values times
    fifty-nine distances make the candidates, and each round covers a value
    more or ends the cover.
    """
    ordered = sorted(values)
    if ordered == list(range(field.low, field.high + 1)):
        return "*"
    held = 0
    for value in ordered:
        held |= 1 << value
    candidates: list[tuple[int, str]] = []
    for first in ordered:
        for step in range(1, field.high - field.low + 1):
            if first - step >= field.low and (held >> (first - step)) & 1:
                continue  # Taken from further down already.
            count = 1
            while (
                first + step * count <= field.high
                and (held >> (first + step * count)) & 1
            ):
                count += 1
            if count >= 2:
                mask = sum(1 << (first + step * k) for k in range(count))
                candidates.append((mask, _progression(first, step, count, field)))
    return min(
        _by_saving(held, candidates, ordered),
        _by_rate(held, candidates, ordered),
        key=len,
    )


#: A cover's parts: the first value of each, which orders them, and its text.
_Parts = list[tuple[int, str]]


def _by_saving(held: int, candidates: list[tuple[int, str]], ordered: list[int]) -> str:
    """The progressions that save the most characters, then lone values."""
    left = held
    parts: _Parts = []
    while True:
        chosen, saved = None, 0
        for mask, text in candidates:
            saving = _listed(mask & left) - (len(text) + 1)
            if saving > saved:
                chosen, saved = (mask, text), saving
        if chosen is None:
            break
        mask, text = chosen
        parts.append(((mask & -mask).bit_length() - 1, text))
        left &= ~mask
    parts.extend((value, str(value)) for value in ordered if (left >> value) & 1)
    return ",".join(text for _, text in sorted(parts))


def _by_rate(held: int, candidates: list[tuple[int, str]], ordered: list[int]) -> str:
    """The part costing fewest characters a newly covered value, each round."""
    choices = [*candidates, *((1 << value, str(value)) for value in ordered)]
    left = held
    parts: _Parts = []
    while left:
        chosen, cost, covers = choices[0], 0, 0
        for mask, text in choices:
            new = (mask & left).bit_count()
            # Fewer characters a value, compared without division: and of
            # two the same, the one that covers more.
            if new and (
                covers == 0
                or (len(text) + 1) * covers < cost * new
                or ((len(text) + 1) * covers == cost * new and new > covers)
            ):
                chosen, cost, covers = (mask, text), len(text) + 1, new
        mask, text = chosen
        parts.append(((mask & -mask).bit_length() - 1, text))
        left &= ~mask
    return ",".join(text for _, text in sorted(parts))


def _listed(mask: int) -> int:
    """The length of the values in a mask written out, each with its comma."""
    one_digit = (mask & _ONE_DIGIT).bit_count()
    return 2 * one_digit + 3 * (mask.bit_count() - one_digit)


def _progression(first: int, step: int, count: int, field: Field) -> str:
    """
    A progression the field holds, in the shortest spelling django-ox reads
    as exactly it: a range for neighbours, a stepped range, and where it
    runs to the top of the field the star's step from the bottom or a
    value's step to the top, "5/15", which django-ox reads as vixie cron
    does. Celery has no such spelling. Nothing reads a printed expression
    but django-ox, and the command parses it again before printing it.
    """
    last = first + step * (count - 1)
    spellings = [f"{first}-{last}/{step}"]
    if step == 1:
        spellings.append(f"{first}-{last}")
    # django-ox reads a weekday field up to 7, which it takes for Sunday, so
    # a step to the top there runs on to 7 and has to stop short of it.
    top = 7 if field is DAY_OF_WEEK else field.high
    if last + step > top:
        spellings.append(f"{first}/{step}")
        if first == field.low:
            spellings.append(f"*/{step}")
    # The shortest, and of those the last listed: "*/5" before "0/5".
    return min(reversed(spellings), key=len)
