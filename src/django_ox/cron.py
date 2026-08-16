"""
A five-field cron expression parser and tick calculator. Stdlib only.

Supports classic vixie-cron syntax: ``*``, lists, ranges, steps (``*/5``,
``1-10/2``, ``5/15``), month and weekday names (``JAN``-``DEC``,
``SUN``-``SAT``), 0 and 7 both meaning Sunday, and the ``@hourly`` /
``@daily`` / ``@midnight`` / ``@weekly`` / ``@monthly`` / ``@yearly`` /
``@annually`` macros. When both day-of-month and day-of-week are
restricted, a day matches if either field matches. A stepped star such as
``*/2`` counts as restricted here, the Python-ecosystem (croniter-style)
interpretation; vixie cron flags any field starting with ``*`` and would
require both fields to match in that case.

Datetimes in and out are naive wall-clock times; callers pick the
timezone (the worker uses the project's current timezone).
"""

from datetime import datetime, timedelta

_MONTH_NAMES = {
    name: number + 1
    for number, name in enumerate(
        [
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
        ]
    )
}
_WEEKDAY_NAMES = {
    name: number
    for number, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])
}

_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

# The longest gap between ticks of a valid expression is just under eight
# years ("0 0 29 2 *" waits from 2096 to 2104 over the missing leap year);
# search further so hitting the bound always indicates a bug, never a rare
# but valid expression.
_SEARCH_DAYS = 366 * 9

# Maximum day each month can have; February counts its leap day, since an
# expression only needs one matchable year to be valid.
_MAX_DAYS = {
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


def _parse_value(
    text: str, low: int, high: int, names: dict[str, int], label: str
) -> int:
    value = names.get(text.lower())
    if value is None:
        try:
            value = int(text)
        except ValueError:
            raise ValueError(f"Invalid {label} value {text!r}.") from None
    if not low <= value <= high:
        raise ValueError(
            f"{label.capitalize()} value {text!r} is outside {low}-{high}."
        )
    return value


def _parse_field(
    field: str, low: int, high: int, names: dict[str, int], label: str
) -> tuple[tuple[int, ...], bool]:
    """Return (sorted allowed values, whether the field restricts anything)."""
    if field == "*":
        return tuple(range(low, high + 1)), False
    values: set[int] = set()
    for part in field.split(","):
        body, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) == 0:
                raise ValueError(
                    f"Invalid step {step_text!r} in {label} field {field!r}."
                )
            step = int(step_text)
            if step > high - low + 1:
                raise ValueError(
                    f"Step {step_text!r} in {label} field {field!r} exceeds "
                    f"the field's range ({low}-{high})."
                )
        if body == "*":
            first, last = low, high
        elif "-" in body:
            first_text, last_text = body.split("-", 1)
            first = _parse_value(first_text, low, high, names, label)
            last = _parse_value(last_text, low, high, names, label)
            if first > last:
                raise ValueError(
                    f"Descending range {body!r} in {label} field {field!r}."
                )
        else:
            first = _parse_value(body, low, high, names, label)
            # Vixie treats a bare value with a step ("5/15") as value-to-max.
            last = high if step_text else first
        values.update(range(first, last + 1, step))
    return tuple(sorted(values)), True


class CronExpression:
    """
    A parsed cron expression that can walk its ticks in either direction.

    Never-matching expressions ("0 0 30 2 *") are rejected at parse time so
    misconfiguration fails at startup, not silently at dispatch time.
    """

    def __init__(self, expression: str) -> None:
        self._expression = expression.strip()
        fields = _MACROS.get(self._expression.lower(), self._expression).split()
        if len(fields) != 5:
            raise ValueError(
                f"Cron expression {expression!r} must have 5 fields "
                "(minute hour day-of-month month day-of-week)."
            )
        self.minutes, _ = _parse_field(fields[0], 0, 59, {}, "minute")
        self.hours, _ = _parse_field(fields[1], 0, 23, {}, "hour")
        self.days_of_month, self._dom_restricted = _parse_field(
            fields[2], 1, 31, {}, "day-of-month"
        )
        self.months, _ = _parse_field(fields[3], 1, 12, _MONTH_NAMES, "month")
        # Parsed over 0-7, then 7 folds onto 0: both mean Sunday.
        days_of_week, self._dow_restricted = _parse_field(
            fields[4], 0, 7, _WEEKDAY_NAMES, "day-of-week"
        )
        self.days_of_week = tuple(
            sorted({0 if day == 7 else day for day in days_of_week})
        )
        # With day-of-week restricted the OR rule makes any date reachable,
        # so only a pure day-of-month restriction can be unsatisfiable.
        if (
            self._dom_restricted
            and not self._dow_restricted
            and not any(
                day <= _MAX_DAYS[month]
                for month in self.months
                for day in self.days_of_month
            )
        ):
            raise ValueError(
                f"Cron expression {expression!r} can never match: no listed "
                "month has any of the listed days."
            )

    def __str__(self) -> str:
        return self._expression

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._expression!r})"

    def _day_matches(self, dt: datetime) -> bool:
        dom_ok = dt.day in self.days_of_month
        # datetime.weekday() has Monday=0; cron has Sunday=0.
        dow_ok = (dt.weekday() + 1) % 7 in self.days_of_week
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    def matches(self, dt: datetime) -> bool:
        """Whether the given minute is a tick (seconds are ignored)."""
        return (
            dt.minute in self.minutes
            and dt.hour in self.hours
            and dt.month in self.months
            and self._day_matches(dt)
        )

    @staticmethod
    def _floor_minute(dt: datetime) -> datetime:
        if dt.tzinfo is not None:
            raise ValueError(
                "CronExpression works on naive datetimes; convert to wall-clock "
                "time first."
            )
        return dt.replace(second=0, microsecond=0)

    def following(self, dt: datetime) -> datetime:
        """Return the earliest tick strictly after dt."""
        cur = self._floor_minute(dt) + timedelta(minutes=1)
        bound = cur + timedelta(days=_SEARCH_DAYS)
        # Each branch advances cur to the earliest instant that could fix
        # its mismatch, so the walk is monotonic and skips whole months,
        # days, and hours instead of stepping minute by minute.
        while cur < bound:
            if cur.month not in self.months:
                if cur.month == 12:
                    cur = datetime(cur.year + 1, 1, 1)
                else:
                    cur = datetime(cur.year, cur.month + 1, 1)
            elif not self._day_matches(cur):
                cur = datetime(cur.year, cur.month, cur.day) + timedelta(days=1)
            elif cur.hour not in self.hours:
                later = [hour for hour in self.hours if hour > cur.hour]
                if later:
                    cur = cur.replace(hour=later[0], minute=0)
                else:
                    cur = datetime(cur.year, cur.month, cur.day) + timedelta(days=1)
            elif cur.minute not in self.minutes:
                later = [minute for minute in self.minutes if minute > cur.minute]
                if later:
                    cur = cur.replace(minute=later[0])
                else:
                    cur = cur.replace(minute=0) + timedelta(hours=1)
            else:
                return cur
        raise ValueError(
            f"No tick of {self._expression!r} within {_SEARCH_DAYS} days after {dt}."
        )

    def previous(self, dt: datetime) -> datetime:
        """Return the latest tick at or before dt (seconds are ignored)."""
        cur = self._floor_minute(dt)
        bound = cur - timedelta(days=_SEARCH_DAYS)
        # Mirror image of following(): every correction jumps to the latest
        # earlier instant that could match.
        while cur > bound:
            if cur.month not in self.months:
                # Last minute of the previous month.
                cur = datetime(cur.year, cur.month, 1) - timedelta(minutes=1)
            elif not self._day_matches(cur):
                # Last minute of the previous day.
                cur = datetime(cur.year, cur.month, cur.day) - timedelta(minutes=1)
            elif cur.hour not in self.hours:
                earlier = [hour for hour in self.hours if hour < cur.hour]
                if earlier:
                    cur = cur.replace(hour=earlier[-1], minute=59)
                else:
                    cur = datetime(cur.year, cur.month, cur.day) - timedelta(minutes=1)
            elif cur.minute not in self.minutes:
                earlier = [minute for minute in self.minutes if minute < cur.minute]
                if earlier:
                    cur = cur.replace(minute=earlier[-1])
                else:
                    cur = cur.replace(minute=0) - timedelta(minutes=1)
            else:
                return cur
        raise ValueError(
            f"No tick of {self._expression!r} within {_SEARCH_DAYS} days before {dt}."
        )
