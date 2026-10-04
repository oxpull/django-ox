"""
Declarative recurring schedules.

Schedules are configuration by default: they live under the SCHEDULES key
of a backend's OPTIONS, versioned and deployed with the code that defines
the tasks they enqueue. The database holds only the dispatch log
(OxScheduleTick), which is what enqueues each due tick exactly once across
any number of workers.

Where they come from is pluggable. A backend names a ScheduleSource in
OPTIONS["SCHEDULE_SOURCE"], and the worker asks it for the active
schedules on every dispatch pass. The default source reads SCHEDULES and
returns the same list forever, so settings-declared schedules behave
exactly as they always have and cost one list lookup per pass. A source
that reads somewhere changing, a database table for instance, owns its own
freshness: the worker asks, the source decides whether anything moved.

The dispatch log is indifferent to where a schedule came from. It is keyed
on a name and an instant, so tick coordination is unchanged whatever the
source.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol, cast, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError
from django.utils.module_loading import import_string

from .compat import InvalidTask, Task, normalize_json
from .cron import CronExpression

SCHEDULE_NAME_MAX_LENGTH = 128

#: Reserved prefix for the dispatch keys of schedules stored as rows. A
#: settings-declared schedule may not use it, or its ticks would share the
#: unique constraint with a stored schedule's.
STORED_KEY_PREFIX = "db:"

_SCHEDULE_KEYS = {
    "task",
    "cron",
    "every",
    "phase",
    "args",
    "kwargs",
    "queue_name",
    "priority",
}

# Below the dispatch interval a schedule cannot be honoured: the loop looks
# about once a second, and only the latest due tick fires, so faster ticks
# would be silently coalesced away rather than run.
MIN_INTERVAL = timedelta(seconds=1)


# The instant interval ticks are counted from. Any fixed instant would do;
# using a fixed one at all is the point, because it means two workers, and
# the same worker before and after a restart, derive identical ticks with
# nothing stored.
_INTERVAL_EPOCH = datetime(1970, 1, 1)

# The longest interval whose ticks can be derived: the time from year 1 to
# the epoch, which is 719,162 days or 62,135,596,800 seconds, exactly
# 1,969 years. While the clock has not reached epoch + phase, a schedule's
# latest tick is the one an interval before that, and
# IntervalTrigger.previous() reaches it by stepping a whole interval back
# from the epoch and then adding the phase. A datetime cannot say an
# instant before year 1, so a second more than this and that step raises
# OverflowError, in the dispatch pass, on every pass until the clock gets
# there. From the epoch on, no clock reading takes the step twice while the
# phase is under the interval.
MAX_INTERVAL = _INTERVAL_EPOCH - datetime.min

# Both strings in this block: what a SCHEDULES entry is told
# when its interval is refused for its length. `manage.py check` reports
# each as django_ox.E002, and constructing a worker raises it.

#: An `every` longer than MAX_INTERVAL. `limit` is that, in seconds.
_EVERY_TOO_LONG = (
    "{prefix}: 'every' exceeds {limit} seconds, the maximum supported interval."
)
#: A number of seconds, for `every` or `phase`, that no timedelta holds.
_NOT_A_TIMEDELTA = (
    "{prefix}: {key!r} is outside the range of seconds a timedelta can hold."
)


@runtime_checkable
class Trigger(Protocol):
    """
    When a schedule is due.

    One method. It answers from the definition and the clock alone, never
    from history, which is what lets every worker derive the same ticks
    with no leader and no stored cursor.

    May return None, meaning the schedule has no tick at or before dt. No
    trigger in this package answers None today; the dispatch loop handles
    it so a one-shot trigger can be added without touching the loop.
    """

    def previous(self, dt: datetime) -> datetime | None:
        """The latest tick at or before dt."""
        ...  # pragma: no cover - protocol


@dataclass(frozen=True)
class IntervalTrigger:
    """
    Every `every`, counted from a fixed epoch: epoch + n * every + phase.

    Anchored to the epoch rather than to the last run, and that is the
    load-bearing choice. A last-run-relative interval needs every worker
    to agree on when the last run was, which a design with no leader
    cannot provide, and it makes the cadence drift on every restart,
    pause, resume and edit. Anchoring to a fixed instant makes the tick
    sequence a pure function of the definition, so none of those events
    can move it.

    `phase` shifts the whole sequence. Hourly with a zero phase fires on
    the hour; a phase of ten minutes fires at ten past.

    Ticks are derived from wall-clock time, exactly as cron ticks are, so
    the daylight-saving behaviour documented for cron applies here too.
    """

    every: timedelta
    phase: timedelta = timedelta(0)

    def previous(self, dt: datetime) -> datetime:
        elapsed = dt - _INTERVAL_EPOCH - self.phase
        tick = _INTERVAL_EPOCH + (elapsed // self.every) * self.every + self.phase
        # Carry the caller's fold. Where a time zone repeats an hour, the
        # worker passes a wall-clock time whose fold says which pass of it
        # this is, and datetime arithmetic drops that: adding a timedelta
        # always yields fold=0. Without it both passes derive the same
        # instant, the second is suppressed as already recorded, and an
        # interval schedule fires nothing for the whole repeated hour.
        return tick.replace(fold=dt.fold)

    def __str__(self) -> str:
        if self.phase:
            return f"every {self.every} (phase {self.phase})"
        return f"every {self.every}"


@dataclass(frozen=True)
class Schedule:
    """A named schedule that enqueues one task instance per tick."""

    name: str
    task: Task[..., Any]
    trigger: Trigger
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    # Everything below is how a schedule that came from a row differs from
    # one that came from settings. A settings schedule leaves them all at
    # their defaults and dispatches exactly as it always has.

    #: A tick before this does not fire. None means no boundary.
    start_time: datetime | None = None
    #: A tick after this does not fire. None means no end.
    end_time: datetime | None = None
    #: A tick later than this by more than the deadline is dropped rather
    #: than run late. None means fire however late, which is the behaviour
    #: settings-declared schedules have always had.
    starting_deadline: timedelta | None = None
    #: Whether the first sight of this schedule records an anchor instead
    #: of firing. True for a settings schedule, where first observation is
    #: the earliest boundary available: there is no creation event to ask.
    #: False for a row, which carries a start_time written when it was
    #: created, so the first tick at or after that boundary really does
    #: fire even if no worker was running when it passed.
    anchors: bool = True
    #: The identity the tick log coordinates on, when it differs from the
    #: name. A settings-declared schedule leaves this empty and coordinates
    #: on its name, which is fixed at deploy time. A stored schedule cannot:
    #: its name is a label a person edits, and two workers holding different
    #: labels for one row would write two tick rows for the same instant and
    #: both fire.
    dispatch_key: str = ""
    #: Called inside the dispatch transaction, before anything is committed:
    #: on SQLite taking the lock is itself a no-op write, and a refusal rolls
    #: it back with the rest. Returns this schedule as it stands right now,
    #: under its own lock, or None if it is gone or disabled.
    #:
    #: A snapshot is a fast filter and nothing more. Whether a tick commits
    #: is decided from what this returns, because every column the decision
    #: rests on can change between the read and the dispatch: the timing,
    #: the boundary, the deadline, the task, the arguments. Re-checking a
    #: chosen few of them would leave every column not on the list a way
    #: through.
    #:
    #: A source with nothing to refresh leaves this None and is dispatched
    #: from its snapshot, which for settings is the same thing.
    refresh: Callable[[], Schedule | None] | None = None

    @property
    def key(self) -> str:
        """What the tick log is keyed on. The name unless something says otherwise."""
        return self.dispatch_key or self.name

    @property
    def cron(self) -> CronExpression | None:
        """
        This schedule's cron expression, or None if it is not cron-based.

        Kept because the field was named `cron` before triggers existed.
        Dispatch reads `trigger`.
        """
        return self.trigger if isinstance(self.trigger, CronExpression) else None


def zone_repeats_an_hour() -> bool:
    """
    Does the project's time zone put the clock back at any point in a year?

    Walked a day at a time across a year and a bit, looking for any step
    backwards. Two samples at midwinter and midsummer are not enough:
    Morocco is permanently UTC+1 and drops to UTC+0 for Ramadan only, so
    both samples read the same and Africa/Casablanca and Africa/El_Aaiun
    answer no while genuinely repeating an hour every year. The window also
    moves about eleven days a year, so no fixed pair of dates catches it.

    A year and a bit from today rather than a fixed year, because a zone
    that starts or stops observing a transition should change this answer
    when it happens rather than when someone edits a constant.
    """
    try:
        zone = ZoneInfo(settings.TIME_ZONE)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    start = datetime.now(tz=zone).replace(
        hour=12, minute=0, second=0, microsecond=0, tzinfo=None
    )
    previous = None
    for _day in range(400):
        offset = start.replace(tzinfo=zone).utcoffset()
        if previous is not None and offset is not None and offset < previous:
            return True
        previous = offset
        start += timedelta(days=1)
    return False


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


def schedule_names_folding_together(backend_alias: str) -> list[tuple[str, str]]:
    """
    Pairs of configured schedule names that differ only by case.

    The tick log's unique constraint decides identity with the column's
    collation, not with Python's `==`. MySQL's usual `utf8mb4_0900_ai_ci`
    is case- and accent-insensitive, so `Report` and `report` are two
    schedules to the settings parser and one key to the constraint: their
    anchors collide, then every tick after that, and one of them never
    runs again with nothing raised anywhere. PostgreSQL and SQLite as
    ordinarily configured do not fold them.

    Every configured backend is read, because the tick log has no backend
    column and one name space covers all of them.
    """
    seen: dict[str, str] = {}
    clashes: list[tuple[str, str]] = []
    tasks: dict[str, dict[str, Any]] = settings.TASKS
    for alias in sorted(tasks):
        raw = tasks[alias].get("OPTIONS", {}).get("SCHEDULES", {})
        for name in sorted(raw) if isinstance(raw, dict) else []:
            folded = name.casefold()
            if folded in seen and seen[folded] != name:
                clashes.append((seen[folded], name))
            else:
                seen.setdefault(folded, name)
    return clashes


def lock_contention(exc: DatabaseError) -> bool:
    """
    Was this raised because the database gave up waiting for a lock?

    A lock held by another worker's transaction is what the dispatch path
    expects to meet: a row lock on a stored schedule, or the unique tick
    row another dispatcher has inserted and not yet committed. Ordinarily
    the wait ends when that transaction does. Past the engine's patience
    it ends in an error instead, and that error is contention, not a fault
    in the schedule and not the database going away: MySQL reports a
    lock-wait timeout (1205) or a deadlock it resolved against this
    transaction (1213), SQLite reports the database locked once its busy
    timeout runs out, and PostgreSQL reports lock_not_available (55P03)
    when a lock_timeout is set or deadlock_detected (40P01).

    Read off the driver's own error, which Django keeps as the cause:
    MySQL drivers put the numeric code first in the arguments, psycopg
    carries the SQLSTATE, and SQLite has only the message.
    """
    if exc.args and exc.args[0] in (1205, 1213):
        return True
    cause = exc.__cause__
    sqlstate = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
    if sqlstate in ("55P03", "40P01"):
        return True
    return "database is locked" in str(exc)


def _as_interval(value: Any, prefix: str, key: str) -> timedelta:
    """
    A timedelta from a timedelta or a number of seconds.

    Numbers are accepted because a schedule read from a database row
    carries seconds, not a timedelta, and both sources build Schedule
    objects through this same function.
    """
    if isinstance(value, timedelta):
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ImproperlyConfigured(
            f"{prefix}: {key!r} must be a timedelta or a number of seconds, "
            f"not {type(value).__name__}."
        )
    if value != value or value in (float("inf"), float("-inf")):
        raise ImproperlyConfigured(f"{prefix}: {key!r} must be a finite number.")
    try:
        return timedelta(seconds=value)
    except OverflowError:
        # Refused like any other bad entry. Left to escape, it is a
        # traceback out of the check that exists to report the entry.
        raise ImproperlyConfigured(
            _NOT_A_TIMEDELTA.format(prefix=prefix, key=key)
        ) from None


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
        if name.startswith(STORED_KEY_PREFIX):
            raise ImproperlyConfigured(
                f"Schedule name {name!r} starts with {STORED_KEY_PREFIX!r}, "
                "which is reserved for schedules stored in the database. Its "
                "ticks would share the dispatch log with one of those."
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
        if "task" not in config:
            raise ImproperlyConfigured(f"{prefix} is missing 'task'.")
        if ("cron" in config) == ("every" in config):
            raise ImproperlyConfigured(
                f"{prefix} needs exactly one of 'cron' or 'every'."
            )
        if "phase" in config and "every" not in config:
            raise ImproperlyConfigured(f"{prefix}: 'phase' only applies to 'every'.")

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

        trigger: Trigger
        if "cron" in config:
            try:
                trigger = CronExpression(config["cron"])
            except ValueError as exc:
                raise ImproperlyConfigured(f"{prefix}: {exc}") from exc
        else:
            every = _as_interval(config["every"], prefix, "every")
            if every < MIN_INTERVAL:
                raise ImproperlyConfigured(
                    f"{prefix}: 'every' is {every}, below the {MIN_INTERVAL} the "
                    "dispatch loop can honour; ticks that close together would be "
                    "coalesced rather than run."
                )
            if every > MAX_INTERVAL:
                raise ImproperlyConfigured(
                    _EVERY_TOO_LONG.format(
                        prefix=prefix, limit=MAX_INTERVAL // timedelta(seconds=1)
                    )
                )
            phase = (
                _as_interval(config["phase"], prefix, "phase")
                if "phase" in config
                else timedelta(0)
            )
            if phase < timedelta(0) or phase >= every:
                raise ImproperlyConfigured(
                    f"{prefix}: 'phase' must be at least zero and less than 'every'."
                )
            trigger = IntervalTrigger(every=every, phase=phase)

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
                name=name,
                task=task,
                trigger=trigger,
                args=tuple(args),
                kwargs=dict(kwargs),
            )
        )
    return schedules


# -- where schedules come from -----------------------------------------


@runtime_checkable
class ScheduleSource(Protocol):
    """
    Where a worker's active schedules come from.

    One method, called once per dispatch pass. Returning a new list is
    allowed; returning the same one is cheaper and is what the default
    source does. Whatever a source returns is what the dispatch loop acts
    on for that pass, so a source that reads changing state should return
    an immutable snapshot rather than something a concurrent writer can
    tear.

    Freshness belongs to the source, not to the worker. A source backed by
    something that changes decides for itself how often to look, and the
    worker neither knows nor cares.
    """

    def schedules(self) -> list[Schedule]:
        """The schedules that are active now."""
        ...  # pragma: no cover - protocol


class SettingsScheduleSource:
    """
    The default source: the schedules under a backend's OPTIONS["SCHEDULES"].

    Built once and returned unchanged forever, because settings do not
    change in a running process. Reloading them would be theatre, and it
    would turn a list lookup into work on the dispatch path.

    Invalid entries raise at construction, which is what keeps a bad
    deploy failing at worker startup and in system checks rather than at
    dispatch time.
    """

    def __init__(self, options: dict[str, Any], backend_alias: str) -> None:
        self._schedules = schedules_from_options(options, backend_alias)

    def schedules(self) -> list[Schedule]:
        return self._schedules


def schedule_source_from_options(
    options: dict[str, Any], backend_alias: str
) -> ScheduleSource:
    """
    Build the ScheduleSource a backend's OPTIONS names, or the default.

    SCHEDULE_SOURCE is a dotted path to a class taking (options,
    backend_alias), the same shape and the same resolution idiom as
    WORKER_CLASS. Resolved from settings for the same reason: the
    supervisor starts every child as ox_worker, so a source chosen any
    other way would be the configured one in the parent and the default
    one in every child above it.

    Raises ImproperlyConfigured on anything wrong, so a bad source is a
    start-up error and a system check finding rather than a worker that
    silently runs no schedules.
    """
    path = options.get("SCHEDULE_SOURCE")
    if not path:
        return SettingsScheduleSource(options, backend_alias)
    if not isinstance(path, str):
        raise ImproperlyConfigured(
            f"SCHEDULE_SOURCE on backend {backend_alias!r} must be a dotted "
            f"path string, not {type(path).__name__}."
        )
    try:
        cls = import_string(path)
    except ImportError as exc:
        raise ImproperlyConfigured(
            f"SCHEDULE_SOURCE {path!r} on backend {backend_alias!r} cannot be "
            f"imported ({exc})."
        ) from exc
    if not isinstance(cls, type):
        raise ImproperlyConfigured(
            f"SCHEDULE_SOURCE {path!r} on backend {backend_alias!r} is not a class."
        )
    source = cls(options, backend_alias)
    # Checked on the instance rather than with issubclass, so a source is
    # free to be any class with the method instead of inheriting a base.
    if not callable(getattr(source, "schedules", None)):
        raise ImproperlyConfigured(
            f"SCHEDULE_SOURCE {path!r} on backend {backend_alias!r} has no "
            "schedules() method, so the worker cannot ask it for anything."
        )
    return cast("ScheduleSource", source)
