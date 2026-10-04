"""
Where beat ran a crontab at other times than a stored schedule will.

``_beat_cron`` makes a stored schedule fire on the values beat's crontab
fires on. The two engines still part where they turn those values into
instants, and where beat 2.9.0 loads its schedule. This module works out
where, from what each engine's own code does with the values; nothing here
runs either of them. The suite holds it to Celery's own crontab and to the
django-ox dispatcher's own steps, and the importer's notices to both
engines run side by side.

Around a change of a zone's UTC offset:

* Where the clocks go forward, the run times they skip never happen.
  beat runs the first of them at the instant the zone's earlier offset
  gives it, which falls after the change, and counts on from there, so it
  passes over every run time up to that instant. A stored schedule with
  USE_TZ runs the latest run time at or before the wall clock once its
  instant comes: the last skipped one, mapped the same way, unless a run
  time the clocks do reach comes first. Without USE_TZ it compares wall
  clock times, and runs the latest skipped one the moment the clocks land.
* Where they go back, the run times they repeat happen twice. beat runs
  them on the first pass only. A stored schedule with USE_TZ runs them on
  both passes. Without USE_TZ it runs each once, as beat does, unless it
  was created during the second pass, after beat had run them.

Where beat 2.9.0 loads its schedule: it reloads it every 300 seconds, and
leaves out a crontab whose hour field is a plain number unless that hour,
moved by the offset between the server's zone and the crontab's timezone
column as both stood on 2023-01-01, is within two hours of the server's
current hour, or is 4. A crontab left out at its run time runs at the
first later load that lets it in.
"""

from __future__ import annotations

import functools
import itertools
import zoneinfo
from datetime import UTC, date, datetime, time, timedelta
from typing import NamedTuple, cast

from ..cron import CronExpression

#: The versions of django-celery-beat whose schedule loading this module
#: describes: run against the importer, and their source read. Of any other
#: version nothing is said.
MEASURED_BEAT_VERSIONS = frozenset({"2.9.0"})

#: The hour fields beat 2.9.0 casts to a number and filters on. Any other
#: spelling, a range, a list, " 9", it loads at every reload.
NUMERIC_HOURS = frozenset(
    [str(hour) for hour in range(24)] + [f"{hour:02d}" for hour in range(10)]
)

#: The longest a run can come after beat's last load of its schedule: a
#: reload falls due 300 seconds after the last, and beat looks at least
#: every 5 seconds, its loop's default.
_LOAD_GAP = timedelta(seconds=305)

#: The moment beat 2.9.0 takes both zones' offsets at, whatever the date.
_FIXED = datetime(2023, 1, 1, 12)

#: How long before the import a change of offset can still part the two
#: engines: the largest change any zone makes is a few hours, and so is
#: the longest a skipped or repeated run waits.
_SETTLE = timedelta(days=1)

#: How far either side of a day a change of offset can move a run on it.
_MARGIN = timedelta(days=2)

_MINUTE = timedelta(minutes=1)
_MICROSECOND = timedelta(microseconds=1)


class Change(NamedTuple):
    """One change of a zone's UTC offset: when, and the offsets either side."""

    instant: datetime
    before: timedelta
    after: timedelta


class Partings(NamedTuple):
    """
    The first wall clock date within the horizon on which a crontab runs
    differently under the two engines where the clocks repeat time, and
    where they skip it, or None for either.
    """

    repeated: date | None
    skipped: date | None


def years_on(moment: datetime, years: int) -> datetime:
    """The same moment that many years later, or as late as a datetime goes."""
    year = moment.year + years
    if year >= datetime.max.year:
        return datetime.max.replace(year=datetime.max.year - 1, tzinfo=moment.tzinfo)
    try:
        return moment.replace(year=year)
    except ValueError:
        # The 29th of February, in a year that has none.
        return moment.replace(year=year, day=28)


@functools.lru_cache(maxsize=256)
def offset_changes(zone: zoneinfo.ZoneInfo, year: int) -> tuple[Change, ...]:
    """
    Every instant in one year of UTC at which a zone's offset changes, with
    the offset before it and the offset from it on.

    zoneinfo says what a zone's offset is at an instant and not when it
    changes, so the year is walked an hour at a time and each change is
    then narrowed to its second. A change undone within the hour it was
    made in would be missed; zones change their clocks months apart. Kept
    by zone and year because the walk is the same whoever asks: a zone's
    data does not change under a ZoneInfo.
    """
    hour = timedelta(hours=1)
    found = []
    moment = datetime(year, 1, 1, tzinfo=UTC)
    end = datetime(year + 1, 1, 1, tzinfo=UTC)
    before = offset(zone, moment)
    while moment < end:
        after = offset(zone, moment + hour)
        if after != before:
            early, late = 0, 3600
            while late - early > 1:
                middle = (early + late) // 2
                if offset(zone, moment + timedelta(seconds=middle)) == before:
                    early = middle
                else:
                    late = middle
            found.append(Change(moment + timedelta(seconds=late), before, after))
        moment, before = moment + hour, after
    return tuple(found)


def offset(zone: zoneinfo.ZoneInfo, instant: datetime) -> timedelta:
    """A zone's UTC offset at an instant."""
    return cast("timedelta", instant.astimezone(zone).utcoffset())


def changes(zone: zoneinfo.ZoneInfo, start: datetime, end: datetime) -> list[Change]:
    """Every change of a zone's offset after start and not after end, in order."""
    # The last year a datetime holds cannot be walked to its end, and the
    # ten years looked at stop short of it (years_on).
    last = min(end.year, datetime.max.year - 1)
    return [
        change
        for year in range(start.year, last + 1)
        for change in offset_changes(zone, year)
        if start < change.instant <= end
    ]


def partings(
    cron: CronExpression,
    zone: zoneinfo.ZoneInfo,
    now: datetime,
    horizon: datetime,
    *,
    aware: bool,
) -> Partings:
    """
    Where a crontab, run in a zone, runs at other instants under the two
    engines around that zone's changes of offset, from the import's clock
    reading to the horizon.

    now is that reading, an instant, and a change counts while any of the
    runs it moves is still to come: the second pass of a repeated hour the
    import is made in, and the runs a gap the clocks have just jumped still
    owes. aware is USE_TZ, which decides how a stored schedule reads its
    run times.
    """
    repeated = skipped = None
    for change in changes(zone, now - _SETTLE, horizon):
        if change.after < change.before:
            if repeated is None:
                repeated = _repeated(cron, change, now, aware=aware)
        elif skipped is None:
            skipped = _skipped(cron, change, now, aware=aware)
        if repeated is not None and skipped is not None:
            break
    return Partings(repeated, skipped)


def _repeated(
    cron: CronExpression, change: Change, now: datetime, *, aware: bool
) -> date | None:
    """
    The date of the first run time the clocks repeat that a stored schedule
    runs and beat does not, after now; None if there is none.

    The repeated stretch runs from the time the clocks go back to, up to
    and not including the time they go back from: a run at its end happens
    once.
    """
    span = change.before - change.after
    back = _wall(change.instant, change.after)
    twice = _ticks(cron, back, back + span)
    if aware:
        # Each runs again on the second pass, at its instant under the later
        # offset, whether the schedule was created before the first pass or
        # during it.
        again = [tick for tick in twice if _instant(tick, change.after) >= now]
    elif change.instant <= now < change.instant + span:
        # Created during the second pass: beat ran these on the first, and
        # the wall clock reaches them again only now.
        again = [tick for tick in twice if tick >= _wall(now, change.after)]
    else:
        # Each wall clock time runs once, on its first pass, as under beat.
        again = []
    return again[0].date() if again else None


def _skipped(
    cron: CronExpression, change: Change, now: datetime, *, aware: bool
) -> date | None:
    """
    The wall clock date of the first run, after now, at which the two
    engines part over run times the clocks skip; None if they do not.
    """
    span = change.after - change.before
    left = _wall(change.instant, change.before)
    landed = left + span
    skipped = _ticks(cron, left, landed)
    if not skipped:
        return None
    first, last = skipped[0] + span, skipped[-1] + span
    # The run times the clocks do reach, as far as the later of the two
    # engines' runs for the skipped ones. Beyond that each engine runs
    # every run time, and they agree.
    reached = _ticks(cron, landed, last + _MINUTE)
    beat = [first, *(tick for tick in reached if tick > first)]
    if aware:
        # The latest run time at or before the wall clock, run once its
        # instant comes: the last skipped one, unless a run time the clocks
        # reach comes before that instant does.
        stored = reached or [last]
        stored_runs = [
            run
            for run in (_instant(tick, change.after) for tick in stored)
            if run >= now
        ]
    else:
        # The wall clock lands past every skipped run time at once, and the
        # run at landing is the latest run time at or before it: one the
        # clocks land on, or else the last skipped one. A schedule does not
        # run a run time before its start, read on the wall clock too.
        landing = reached[0] if reached[:1] == [landed] else skipped[-1]
        landings = [
            (landed, landing),
            *((tick, tick) for tick in reached if tick > landed),
        ]
        start = _wall(now, change.after if now >= change.instant else change.before)
        stored_runs = [
            _instant(at, change.after) for at, tick in landings if tick >= start
        ]
    beat_runs = [run for run in (_instant(t, change.after) for t in beat) if run >= now]
    if beat_runs == stored_runs:
        return None
    for ours, theirs in itertools.zip_longest(beat_runs, stored_runs):
        if ours != theirs:
            first_apart = min(run for run in (ours, theirs) if run is not None)
            return _wall(first_apart, change.after).date()
    return None  # pragma: no cover - the lists differ, so a pair does


def annotation(hour: int, column: zoneinfo.ZoneInfo, server: zoneinfo.ZoneInfo) -> int:
    """
    The hour beat 2.9.0's hour filter gives a crontab: its hour, moved by
    the offset between the server's zone and the crontab's timezone column
    as both stood on 2023-01-01, in whole hours cut toward zero, and kept
    within the day the way the database's remainder keeps it, which
    leaves a negative hour negative.
    """
    gap = cast("timedelta", _FIXED.replace(tzinfo=server).utcoffset()) - cast(
        "timedelta", _FIXED.replace(tzinfo=column).utcoffset()
    )
    total = hour + int(gap.total_seconds() / 3600) + 24
    return total - 24 * int(total / 24)


def loaded_late(
    hour_text: str,
    column: str,
    server: str,
    due: str,
    fields: tuple[frozenset[int], ...],
    now: datetime,
    horizon: datetime,
) -> date | None:
    """
    The first wall clock date, from now to the horizon, on which beat
    2.9.0's schedule loading can leave a crontab out at its run time, so
    that beat ran it late; None if there is none.

    hour_text is the crontab's hour field as stored, column its timezone
    column, server the zone beat takes the current hour in, and due the
    zone beat ran the crontab in; fields are the values it fires on.
    """
    # Ten years on from the last years a datetime holds is no later than
    # now, and there is nothing to look at.
    if hour_text not in NUMERIC_HOURS or horizon <= now:
        return None
    minutes, _, days, months, weekdays = fields
    hour = int(hour_text)
    mark = annotation(hour, zoneinfo.ZoneInfo(column), zoneinfo.ZoneInfo(server))
    zone = zoneinfo.ZoneInfo(due)
    try:
        first = now.astimezone(zone).date().toordinal()
        last = horizon.astimezone(zone).date().toordinal()
    except OverflowError:
        return None
    each = [
        (minute, _late_days(due, server, mark, hour, minute, first, last))
        for minute in sorted(minutes)
    ]
    for index in range(last - first + 1):
        day = date.fromordinal(first + index)
        if (
            day.month not in months
            or day.day not in days
            # Celery's numbering, as the sets are: Sunday is 0.
            or day.isoweekday() % 7 not in weekdays
        ):
            continue
        for minute, late in each:
            if late[index] and now <= _run(day, hour, minute, zone) <= horizon:
                return day
    return None


@functools.lru_cache(maxsize=1024)
def _late_days(
    due: str, server: str, mark: int, hour: int, minute: int, first: int, last: int
) -> bytes:
    """
    For each day from ordinal first to last, whether beat's last load before
    a run at hour:minute in the due zone can have left that run's crontab
    out. The same for every crontab with that hour, minute and mark, so
    kept.

    A day's answer turns on the two zones' offsets, which change a few
    times a year. Days within two of a change are each worked out; between
    changes every day answers as the first one does.
    """
    zone, local = zoneinfo.ZoneInfo(due), zoneinfo.ZoneInfo(server)
    size = last - first + 1
    near = bytearray(size)
    try:
        start = datetime.combine(date.fromordinal(first), time(), UTC) - _MARGIN
        end = datetime.combine(date.fromordinal(last), time(), UTC) + _MARGIN
    except OverflowError:
        # The first or last years a datetime holds: every day on its own.
        near = bytearray(b"\x01" * size)
    else:
        for each in (zone, local):
            for change in changes(each, start, end):
                centre = change.instant.date().toordinal() - first
                for index in range(max(centre - 2, 0), min(centre + 3, size)):
                    near[index] = 1
    late = bytearray(size)
    index = 0
    while index < size:
        late[index] = _late_on(
            date.fromordinal(first + index), hour, minute, mark, zone, local
        )
        if near[index]:
            index += 1
            continue
        stretch = near.find(1, index + 1)
        if stretch == -1:
            stretch = size
        late[index:stretch] = late[index : index + 1] * (stretch - index)
        index = stretch
    return bytes(late)


def _late_on(
    day: date,
    hour: int,
    minute: int,
    mark: int,
    zone: zoneinfo.ZoneInfo,
    local: zoneinfo.ZoneInfo,
) -> bool:
    """Can beat's last load before this one run have left its crontab out?"""
    try:
        run = _run(day, hour, minute, zone)
        # The hours the server's clock shows over the time the last load can
        # have been at: the hour the run is in or, near its start, the one
        # before.
        shown = {
            (run - _LOAD_GAP).astimezone(local).hour,
            (run - _MICROSECOND).astimezone(local).hour,
        }
    except OverflowError:
        # A run on the last day a datetime holds, past it in UTC.
        return False
    return not all(_admitted(mark, hour_shown) for hour_shown in shown)


def _admitted(mark: int, hour: int) -> bool:
    """Does a load in this hour of the server's clock keep a crontab with this mark?"""
    # beat's own list: the hours two either side of the current one, and 4,
    # its cleanup task's.
    return mark == 4 or any((hour + step) % 24 == mark for step in range(-2, 3))


def _run(day: date, hour: int, minute: int, zone: zoneinfo.ZoneInfo) -> datetime:
    """The instant beat gives a run time: on a skipped one, the earlier offset's."""
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=zone).astimezone(
        UTC
    )


def _ticks(cron: CronExpression, low: datetime, high: datetime) -> list[datetime]:
    """Every run time from low up to and not including high, both wall clock times."""
    tick = low.replace(second=0, microsecond=0)
    if tick < low:
        tick += _MINUTE
    found: list[datetime] = []
    try:
        if not cron.matches(tick):
            tick = cron.following(tick)
        while tick < high:
            found.append(tick)
            tick = cron.following(tick)
    except ValueError:
        # following() looks some years ahead and no further; nothing that
        # far off is inside a few hours of a change.
        pass
    return found


def _wall(instant: datetime, shift: timedelta) -> datetime:
    """An instant as the wall clock time an offset gives it."""
    return (instant + shift).replace(tzinfo=None)


def _instant(wall: datetime, shift: timedelta) -> datetime:
    """A wall clock time as the instant an offset gives it."""
    return (wall - shift).replace(tzinfo=UTC)
