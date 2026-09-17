"""Duration arguments for the management commands: ``7d``, ``24h``, ``90m``, ``45s``."""

from __future__ import annotations

import argparse
import math
import re
from datetime import timedelta

from django.core.management.base import CommandError

__all__ = ["DURATION_UNITS", "parse_duration", "parse_seconds"]

DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

_DURATION = re.compile(r"(\d+)([smhd]?)")
_FORMS = "use forms like 7d, 24h, 90m, 45s, or a plain number of seconds"


def _whole_seconds(text: str) -> int | None:
    match = _DURATION.fullmatch(text)
    if match is None:
        return None
    number, unit = match.groups()
    return int(number) * DURATION_UNITS[unit or "s"]


def parse_duration(value: str) -> timedelta:
    """Parse '7d' / '24h' / '90m' / '45s' or a plain number of seconds."""
    seconds = _whole_seconds(value.strip())
    if seconds is None:
        raise CommandError(f"Invalid duration {value!r}; {_FORMS}.")
    return timedelta(seconds=seconds)


def parse_seconds(value: str) -> float:
    """
    Seconds from the forms `parse_duration` takes, or from any number
    `float()` takes, fractions included.

    Meant for argparse ``type=``: bad input raises `ArgumentTypeError`, which
    the command reports as a usage error rather than a traceback.
    """
    text = value.strip()
    try:
        seconds = _whole_seconds(text)
        parsed = float(text) if seconds is None else float(seconds)
    except (ValueError, OverflowError):
        raise argparse.ArgumentTypeError(
            f"invalid duration {value!r}; {_FORMS}"
        ) from None
    # float() takes nan, inf and anything that rounds to inf. Every
    # comparison with nan is false, and nothing is ever over inf. A
    # threshold given either way could never be exceeded, so the check it
    # sets could not fail. A check that cannot fail is worse than no check.
    if not math.isfinite(parsed):
        raise argparse.ArgumentTypeError(f"invalid duration {value!r}; {_FORMS}")
    return parsed
