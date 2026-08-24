"""
Declarative recurring schedules.

Schedules are configuration, not data: they live under the SCHEDULES key of
a backend's OPTIONS, versioned and deployed with the code that defines the
tasks they enqueue. The database holds only the dispatch log
(OxScheduleTick), which is what makes each tick fire exactly once across
any number of workers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.tasks.base import Task
from django.tasks.exceptions import InvalidTask
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from .cron import CronExpression

SCHEDULE_NAME_MAX_LENGTH = 128

_SCHEDULE_KEYS = {"task", "cron", "args", "kwargs", "queue_name", "priority"}


@dataclass(frozen=True)
class Schedule:
    """A named cron schedule that enqueues one task instance per tick."""

    name: str
    task: Task[..., Any]
    cron: CronExpression
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def schedule_name_collisions(backend_alias: str) -> list[tuple[str, str]]:
    """
    (schedule name, other backend alias) pairs for every schedule name the
    given backend shares with another configured TASKS backend.

    Tick rows are keyed on (schedule name, tick time) with no backend
    column, so two backends sharing a name would suppress each other's
    ticks; the collision must be rejected at config time.
    """

    def names(alias: str) -> set[str]:
        # The annotation states the documented shape, a dict of backend
        # configurations, without repeating what the settings plugin
        # already infers from the literal in the active settings module.
        tasks: dict[str, dict[str, Any]] = settings.TASKS
        raw = tasks[alias].get("OPTIONS", {}).get("SCHEDULES", {})
        return set(raw) if isinstance(raw, dict) else set()

    own = names(backend_alias)
    return [
        (name, alias)
        for alias in settings.TASKS
        if alias != backend_alias
        for name in sorted(own & names(alias))
    ]


def schedules_from_options(
    options: dict[str, Any], backend_alias: str
) -> list[Schedule]:
    """
    Build Schedule objects from a backend's OPTIONS["SCHEDULES"] mapping.

    Raises ImproperlyConfigured on the first invalid entry so a bad deploy
    fails at worker startup (and in system checks), not at dispatch time.
    """
    raw = options.get("SCHEDULES", {})
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(
            "SCHEDULES must be a mapping of schedule name to configuration."
        )
    schedules = []
    for name, config in raw.items():
        if not isinstance(name, str) or not name:
            raise ImproperlyConfigured(
                f"Schedule names must be non-empty strings, got {name!r}."
            )
        if len(name) > SCHEDULE_NAME_MAX_LENGTH:
            raise ImproperlyConfigured(
                f"Schedule name {name!r} exceeds {SCHEDULE_NAME_MAX_LENGTH} characters."
            )
        prefix = f"Schedule {name!r}"
        if not isinstance(config, dict):
            raise ImproperlyConfigured(f"{prefix} must be a mapping.")
        unknown = set(config) - _SCHEDULE_KEYS
        if unknown:
            raise ImproperlyConfigured(
                f"{prefix} has unknown key(s): {', '.join(sorted(unknown))}."
            )
        for required in ("task", "cron"):
            if required not in config:
                raise ImproperlyConfigured(f"{prefix} is missing {required!r}.")

        try:
            obj = import_string(config["task"])
        except ImportError as exc:
            raise ImproperlyConfigured(
                f"{prefix}: cannot import task {config['task']!r} ({exc})."
            ) from exc
        if not isinstance(obj, Task):
            raise ImproperlyConfigured(
                f"{prefix}: {config['task']!r} is not a django.tasks Task."
            )

        try:
            cron = CronExpression(config["cron"])
        except ValueError as exc:
            raise ImproperlyConfigured(f"{prefix}: {exc}") from exc

        args = config.get("args", [])
        kwargs = config.get("kwargs", {})
        if not isinstance(args, (list, tuple)):
            raise ImproperlyConfigured(f"{prefix}: 'args' must be a list.")
        if not isinstance(kwargs, dict):
            raise ImproperlyConfigured(f"{prefix}: 'kwargs' must be a dict.")
        try:
            normalize_json(list(args))
            normalize_json(kwargs)
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"{prefix}: arguments are not JSON-serializable ({exc})."
            ) from exc

        # The schedule is defined under this backend's OPTIONS, so its
        # enqueues go to this backend whatever alias the task was declared
        # with. Task.using() re-runs validate_task, catching a bad
        # queue_name or priority at load time.
        overrides = {
            key: config[key] for key in ("queue_name", "priority") if key in config
        }
        if obj.backend != backend_alias:
            overrides["backend"] = backend_alias
        try:
            task = obj.using(**overrides) if overrides else obj
        except InvalidTask as exc:
            raise ImproperlyConfigured(f"{prefix}: {exc}") from exc

        schedules.append(
            Schedule(
                name=name, task=task, cron=cron, args=tuple(args), kwargs=dict(kwargs)
            )
        )
    return schedules
