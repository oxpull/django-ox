"""
Resolving rate limits from a backend's OPTIONS.

A limit is a name, a capacity of N admissions per period P, and the tasks
or queues it covers:

    "RATE_LIMITS": {
        "stripe": {
            "limit": 100,
            "per": 60,
            "tasks": ["myapp.billing.charge", "myapp.billing.refund"],
        },
        "sms": {"limit": 500, "per": 60, "queues": ["sms"]},
    }

Both tasks listed under "stripe" draw on one budget of 100 per minute.

Settings are the only declaration site, and that is the whole of the
API. A decorator marking the task itself would put the limit in a task
module, and a worker imports no task modules: django.tasks has no
autodiscovery, and `ox_worker` imports the backend rather than the code
it runs. A limit declared that way would be missing from the registry
the worker builds at startup and enforced only when something else
happened to import that module first, which is worse than having no
decorator at all. OPTIONS is read by every process, so a limit declared
here is visible to the worker and to `manage.py check` alike, which is
what lets the check catch a limit that no worker would apply.

A limit is named once across the whole installation. Its window row is
keyed on the name alone, so two backends configuring the same name share
one window and count each other's admissions. Uniqueness is different and
is scoped per alias, because its key hashes the alias in; a limit's does
not. Give a limit that should apply to one backend a name only that
backend uses.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.core.exceptions import ImproperlyConfigured

CONFIG_KEYS = frozenset({"limit", "per", "tasks", "queues"})

# The width of OxpullRateWindow.limit_name, which is the primary key of
# the window table. models.py takes the column's length from here, so a
# name this module accepts is a name the row can hold on every database.
MAX_LIMIT_NAME_LENGTH = 128

# Seconds between refresh SELECTs, when nothing a worker did has made its
# view stale. run() re-enters the claim on every task completion, not on
# the poll interval, so without a floor a worker with fast in-flight work
# would refresh at completion rate.
RATE_REFRESH_INTERVAL = 0.25


@dataclass(frozen=True)
class RateLimit:
    """One resolved limit: its capacity, and what it covers."""

    name: str
    limit: int
    per: int
    task_paths: frozenset[str]
    queues: frozenset[str]

    def covers(self, task_path: str, queue_name: str) -> bool:
        return task_path in self.task_paths or queue_name in self.queues


def rate_limits_from_options(
    options: dict[str, Any],
    backend_alias: str,
    backend_queues: Iterable[str] = (),
) -> dict[str, RateLimit]:
    """
    RATE_LIMITS from a backend's OPTIONS, as name -> RateLimit ordered by
    name.

    Raises ImproperlyConfigured on anything malformed, so a bad deploy
    fails at the system check and at worker start rather than silently
    never throttling; the same idiom as unique_tasks_from_options.
    """
    raw = options.get("RATE_LIMITS", {})
    if not isinstance(raw, dict) or not all(isinstance(n, str) for n in raw):
        raise ImproperlyConfigured("RATE_LIMITS must be a dict keyed by limit name.")
    accepted = frozenset(backend_queues)
    limits: dict[str, RateLimit] = {}
    for name in sorted(raw):
        config = raw[name]
        if not isinstance(config, dict):
            raise ImproperlyConfigured(
                f"RATE_LIMITS[{name!r}] must be a dict with the keys "
                f"{sorted(CONFIG_KEYS)}."
            )
        unknown = sorted(set(config) - CONFIG_KEYS)
        if unknown:
            raise ImproperlyConfigured(
                f"RATE_LIMITS[{name!r}] has unknown key(s) {unknown}; "
                f"the keys are {sorted(CONFIG_KEYS)}."
            )
        _check_name(name)
        limit, per = _capacity(name, config.get("limit"), config.get("per"))
        paths = frozenset(_string_list(name, config, "tasks"))
        queues = frozenset(_string_list(name, config, "queues"))
        if accepted and not queues <= accepted:
            raise ImproperlyConfigured(
                f"RATE_LIMITS[{name!r}] names queue(s) "
                f"{sorted(queues - accepted)} that backend "
                f"{backend_alias!r} does not accept."
            )
        if not paths and not queues:
            raise ImproperlyConfigured(
                f"Rate limit {name!r} covers no tasks and no queues. List "
                "the task paths under 'tasks', or the queue names under "
                "'queues'."
            )
        limits[name] = RateLimit(name, limit, per, paths, queues)
    _refuse_names_a_collation_would_fold(limits)
    return limits


def _refuse_names_a_collation_would_fold(limits: dict[str, RateLimit]) -> None:
    """
    Refuse two limit names that differ only by case.

    A limit's name is the primary key of its window row, and a primary key
    decides identity with the column's collation rather than with Python's
    ``==``. MySQL's usual ``utf8mb4_0900_ai_ci`` folds case, so ``Reports``
    and ``reports`` are two limits to this parser and one row to the
    database: they share a window, count each other's admissions, and
    throttle traffic they were never meant to see. Nothing raises, and the
    two limits are simply wrong from the first admission onwards.

    Refused rather than warned about, and refused on every database. The
    configuration is broken on MySQL and confusing everywhere, and a name is
    free to change before anything is deployed.
    """
    folded: dict[str, str] = {}
    for name in sorted(limits):
        key = name.casefold()
        first = folded.get(key)
        if first is not None:
            raise ImproperlyConfigured(
                f"RATE_LIMITS has {first!r} and {name!r}, which differ only "
                "by case. A limit's name is the primary key of its window "
                "row, and on a case-insensitive collation the two share one "
                "window and throttle each other. Rename one."
            )
        folded[key] = name


def refresh_interval_from_options(options: dict[str, Any]) -> float:
    """
    RATE_REFRESH_INTERVAL from a backend's OPTIONS, in seconds.

    Validated here, beside the limits, so that a bad value is reported by
    `manage.py check` rather than raised by the first worker to start on
    a deploy that passed its checks.
    """
    raw = options.get("RATE_REFRESH_INTERVAL", RATE_REFRESH_INTERVAL)
    try:
        # An int too large for a float raises here, and would have raised
        # the same OverflowError from math.isfinite; the message below is
        # the one promised.
        seconds = float(raw) if not isinstance(raw, bool) else math.nan
    except (TypeError, ValueError, OverflowError):
        seconds = math.nan
    if not isinstance(raw, int | float) or not math.isfinite(seconds) or seconds < 0:
        raise ImproperlyConfigured(
            f"RATE_REFRESH_INTERVAL must be a finite number of seconds, zero "
            f"or more; got {raw!r}."
        )
    return seconds


def _check_name(name: str) -> None:
    if not name:
        raise ImproperlyConfigured("A rate limit's name cannot be empty.")
    if len(name) > MAX_LIMIT_NAME_LENGTH:
        raise ImproperlyConfigured(
            f"Rate limit name {name!r} is {len(name)} characters, and the "
            f"maximum is {MAX_LIMIT_NAME_LENGTH}. The name is the primary "
            "key of the window table: PostgreSQL and MySQL refuse a longer "
            "one at the first admission, and SQLite stores it and disagrees "
            "with them."
        )


def _capacity(name: str, limit: object, per: object) -> tuple[int, int]:
    if limit is None or per is None:
        raise ImproperlyConfigured(f"Rate limit {name!r} needs limit and per together.")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ImproperlyConfigured(
            f"Rate limit {name!r}: limit must be a whole number of "
            "admissions, zero or more."
        )
    seconds = round(per.total_seconds()) if isinstance(per, timedelta) else per
    if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds < 1:
        raise ImproperlyConfigured(
            f"Rate limit {name!r}: per must be a whole number of seconds, or "
            "a timedelta of at least one second. Sub-second periods are not "
            "supported."
        )
    return limit, seconds


def _string_list(name: str, config: dict[str, Any], key: str) -> list[str]:
    raw = config.get(key, [])
    if not (isinstance(raw, list | tuple) and all(isinstance(v, str) for v in raw)):
        raise ImproperlyConfigured(
            f"RATE_LIMITS[{name!r}][{key!r}] must be a list of strings."
        )
    return list(raw)
