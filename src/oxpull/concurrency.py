"""
Resolving concurrency limits from a backend's OPTIONS.

A limit is a name, a ceiling on how many covered tasks may be RUNNING at
once, and the tasks or queues it covers:

    "CONCURRENCY_LIMITS": {
        "reports": {"max": 2, "tasks": ["myapp.reports.build_monthly"]},
        "sms": {"max": 10, "queues": ["sms"]},
    }

A limit counts RUNNING task rows, not executing bodies. A row stops
counting when its outcome is written, when the reaper takes it back, or
when the stuck-thread watchdog hands it over, whether or not a thread is
still inside the body. A dead worker's rows count until the reaper
reclaims them.

The name is a settings key and nothing else. No row is keyed on it, so
the spelling is the identity on every database and no collation can make
two limits one. That is why this parser has no rule against names that
differ only by case, and no length cap, both of which rate_limits.py
needs because its name is a primary key.

Settings are the only declaration site, for the reason rate_limits.py
gives: a worker imports no task modules.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.db.models import Q

from .rate_limits import _string_list

OPTION = "CONCURRENCY_LIMITS"
CONFIG_KEYS = frozenset({"max", "tasks", "queues"})


@dataclass(frozen=True)
class ConcurrencyLimit:
    """One resolved limit: its ceiling, and what it covers."""

    name: str
    max: int
    task_paths: frozenset[str]
    queues: frozenset[str]

    def covers(self, task_path: str, queue_name: str) -> bool:
        return task_path in self.task_paths or queue_name in self.queues

    def covered_q(self) -> Q:
        """
        The rows this limit covers, as the worker counts them.

        The claim excludes a blocked limit's task_paths and queues, the same
        two fields this reads, so a row the claim can take is a row the count
        sees once it is RUNNING. An empty side compiles to the other alone.
        """
        return Q(task_path__in=sorted(self.task_paths)) | Q(
            queue_name__in=sorted(self.queues)
        )


def concurrency_limits_from_options(
    options: dict[str, Any],
    backend_alias: str,
    backend_queues: Iterable[str] = (),
) -> dict[str, ConcurrencyLimit]:
    """
    CONCURRENCY_LIMITS from a backend's OPTIONS, as name -> ConcurrencyLimit
    ordered by name.

    Raises ImproperlyConfigured on anything malformed, so a bad deploy fails
    at the system check and at worker start rather than never limiting.
    """
    raw = options.get(OPTION, {})
    if not isinstance(raw, dict) or not all(isinstance(n, str) for n in raw):
        raise ImproperlyConfigured(f"{OPTION} must be a dict keyed by limit name.")
    accepted = frozenset(backend_queues)
    limits: dict[str, ConcurrencyLimit] = {}
    for name in sorted(raw):
        config = raw[name]
        if not isinstance(config, dict):
            raise ImproperlyConfigured(
                f"{OPTION}[{name!r}] must be a dict with the keys "
                f"{sorted(CONFIG_KEYS)}."
            )
        unknown = sorted(set(config) - CONFIG_KEYS)
        if unknown:
            raise ImproperlyConfigured(
                f"{OPTION}[{name!r}] has unknown key(s) {unknown}; "
                f"the keys are {sorted(CONFIG_KEYS)}."
            )
        if not name:
            raise ImproperlyConfigured("A concurrency limit's name cannot be empty.")
        ceiling = config.get("max")
        if not isinstance(ceiling, int) or isinstance(ceiling, bool) or ceiling < 0:
            raise ImproperlyConfigured(
                f"Concurrency limit {name!r}: max must be a whole number of "
                "running tasks, zero or more."
            )
        paths = frozenset(_string_list(name, config, "tasks", OPTION))
        queues = frozenset(_string_list(name, config, "queues", OPTION))
        if accepted and not queues <= accepted:
            raise ImproperlyConfigured(
                f"{OPTION}[{name!r}] names queue(s) "
                f"{sorted(queues - accepted)} that backend "
                f"{backend_alias!r} does not accept."
            )
        if not paths and not queues:
            raise ImproperlyConfigured(
                f"Concurrency limit {name!r} covers no tasks and no queues. "
                "List the task paths under 'tasks', or the queue names under "
                "'queues'."
            )
        limits[name] = ConcurrencyLimit(name, ceiling, paths, queues)
    return limits
