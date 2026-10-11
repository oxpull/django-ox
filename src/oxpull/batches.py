"""
Batches: a group of tasks, and callbacks that fire once every member's row
is settled or gone.

    from oxpull import batches

    batch = batches.create("nightly-invoices", on_complete=summarize_run)
    batch.add(send_invoice, 42)
    batch.add(send_invoice, 43)
    batch.seal()

    batch.progress()  # BatchProgress(total=2, pending=1, succeeded=1, failed=0)

The load-bearing decision: batch state is derived by querying member task
rows, never accumulated from task_finished signals. The django-ox reaper
reclaims a dead worker's task by writing FAILED through a plain queryset
update that emits no signal at all, and the attempts-exhausted path can
skip its signal when the task module no longer imports. A batch that
counted signals would therefore hang forever in exactly the scenario
batches exist to survive. Every count here is a query against the task
rows, so it cannot miss a death however it happened.

Completion is detected by the reconcile() task, which django-ox's schedule
dispatcher runs once a minute. Completion detection is asynchronous: the
callback is enqueued when reconciliation observes that all members have
finished. Worker availability and queue delays can extend that interval.

A sealed batch is not frozen in time, only frozen in membership.
django_ox.actions.retry puts a FAILED or LOST member back on the queue,
and the reconciler treats that as it treats any pending member: the
batch stays SEALED until the retry settles. Retrying a member of a batch
that has already completed does not reopen it, and its callbacks have
already fired.

    TASKS = {
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "OPTIONS": {
                "SCHEDULES": {
                    "oxpull-batch-reconcile": {
                        "task": "oxpull.batches.reconcile",
                        "cron": "* * * * *",
                    },
                },
            },
        }
    }

The reconciler Task is built on attribute access, not at import: a Task
is validated against its backend the moment it is constructed, so a
module-level @task would make importing this module fail in any project
whose QUEUES omits "default", and would build the default task backend
as a side effect of the import. `oxpull.batches.reconcile` is therefore
resolved lazily, on the default task backend's "default" queue when it
has one and on its first queue otherwise; the schedule's own `queue_name`
overrides that. The task runs on whichever backend's SCHEDULES name it:
django-ox rebinds a scheduled task to that backend at load, and refuses
a queue it does not accept with ImproperlyConfigured naming the schedule.
"""

from __future__ import annotations

# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

import json
import logging
import random
import time
import uuid
import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError, OperationalError, connections, transaction
from django.db.models import Count, QuerySet
from django.tasks import task
from django.tasks.base import Task, TaskResult
from django.utils import timezone
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from django_ox import remaining
from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask

from .exceptions import BatchMemberExists, BatchSealed
from .models import OxpullBatch, OxpullBatchMember
from .routing import oxpull_db_alias
from .unique import is_registered

logger = logging.getLogger(__name__)

# Bound on the reissue of a transaction this package owns after a lost
# InnoDB deadlock, and the longest first pause between two attempts after
# any lost conflict, in seconds. A pass ends that way only when another
# transaction closed a lock cycle with it, so reaching the bound takes that
# many lost cycles inside one call. Mirrors MAX_UNIQUE_ATTEMPTS.
MAX_ADD_ATTEMPTS = 5
ADD_RETRY_JITTER = 0.05

# The bound after a serialization failure is higher. A deadlock has one
# victim, but a serialization failure reaches every transaction that waited
# on the writer that committed, so the same losers meet again: eight add()s
# held behind one at PostgreSQL REPEATABLE READ needed a fifth attempt twice
# in 60 races. The pause doubles with each attempt, up to 16 times the first.
MAX_SERIALIZATION_ATTEMPTS = 10
SERIALIZATION_PAUSE_DOUBLINGS = 4

if TYPE_CHECKING:
    # Built by __getattr__ at the bottom of this module; declared here so
    # the public name is visible to type checkers and linters.
    reconcile: Task[[], int]

__all__ = ["Batch", "BatchProgress", "create", "get", "reconcile"]

# A member is unfinished while its task row is in one of these. WAITING is
# a workflow node held back until its upstreams succeed. No batch member is
# ever written WAITING, and wherever the status appears it has not run.
PENDING_STATUSES = (
    OxTask.Status.READY,
    OxTask.Status.RUNNING,
    OxTask.Status.WAITING,
)

# A member is finished when its task row has one of these statuses or has
# been deleted by ox_prune. Use this set rather than PENDING_STATUSES so
# an unrecognised status holds the batch open instead of assigning an
# outcome. The tests assert that the two sets cover every OxTask.Status
# in this checkout, so adding a status requires updating that decision.
SETTLED_STATUSES = (
    OxTask.Status.SUCCESSFUL,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    OxTask.Status.DISCARDED,
)

# A member settled in one of these has no outcome that can be called
# clean, and is not a FAILED row either. LOST says the attempt was never
# observed. DISCARDED says an operator closed the task without running
# it, which is a decision on record but not work that was done. Both are
# held out of the counted set, so both land in the unattributed
# shortfall and both withhold on_success; neither is counted in
# failed_count, which stays a count of members recorded as FAILED.
UNATTRIBUTED_STATUSES = (OxTask.Status.LOST, OxTask.Status.DISCARDED)

NAME_MAX_LENGTH = 128

# The path a schedule names to run the reconciler; the task row stores the
# same string, see the bottom of this module.
RECONCILE_TASK_PATH = "oxpull.batches.reconcile"

# The schedule name every hint gives the reconciler.
RECONCILE_SCHEDULE_NAME = "oxpull-batch-reconcile"

RECONCILE_SCHEDULE_HINT = (
    "Add to a django-ox backend's OPTIONS: "
    '"SCHEDULES": {"oxpull-batch-reconcile": '
    '{"task": "oxpull.batches.reconcile", "cron": "* * * * *"}}.'
)

# The same requirement, for a project whose schedules are rows rather than
# settings entries. Telling that project to add a SCHEDULES block reads as
# advice to undo the move it just made, so it is told what it is actually
# missing: the reconciler is not exposed, so no row is allowed to name it.
RECONCILE_SCHEDULABLE_HINT = (
    "Expose the reconciler to the schedule rows: add to a django-ox "
    'backend\'s OPTIONS "SCHEDULABLE_TASKS": {"oxpull-batch-reconcile": '
    '"oxpull.batches.reconcile"}, then add a schedule for that key.'
)


def task_backend_classes() -> dict[str, type[Any]]:
    """Each settings.TASKS alias with the class its BACKEND names, where it imports."""
    classes: dict[str, type[Any]] = {}
    for alias, params in settings.TASKS.items():
        if not isinstance(params, dict):
            continue
        try:
            backend_class = import_string(str(params.get("BACKEND", "")))
        except ImportError:
            continue
        if isinstance(backend_class, type):
            classes[alias] = backend_class
    return classes


def _schedulable_task_path(config: Any) -> Any:
    """The task one SCHEDULABLE_TASKS entry names; a bare path is the shorthand."""
    if isinstance(config, str):
        return config
    return config.get("task") if isinstance(config, dict) else None


def _ox_backend_options() -> dict[str, dict[str, Any]]:
    """Each settings.TASKS alias backed by django-ox, with its OPTIONS."""
    from django_ox.backend import OxBackend

    found: dict[str, dict[str, Any]] = {}
    for alias, backend_class in task_backend_classes().items():
        if not issubclass(backend_class, OxBackend):
            continue
        params = settings.TASKS[alias]
        options = params.get("OPTIONS", {})
        if isinstance(options, dict):
            found[alias] = options
    return found


def _schedules_reconciler(options: dict[str, Any], task_path: str) -> bool:
    """Whether OPTIONS["SCHEDULES"] declares the reconciler at `task_path`."""
    raw = options.get("SCHEDULES", {})
    if not isinstance(raw, dict):
        return False
    return any(
        isinstance(config, dict) and config.get("task") == task_path
        for config in raw.values()
    )


def _engine_reads_schedule_rows() -> bool:
    """
    Whether django-ox's schedule dispatcher can load schedules from rows.

    Batch reconciliation requires row-backed schedule dispatch. Merely
    accepting SCHEDULE_SOURCE and SCHEDULABLE_TASKS settings does not
    establish that capability: a dispatcher that ignores them leaves a
    sealed batch without its reconciler. Check the loader function that
    implements SCHEDULE_SOURCE rather than inferring support from settings
    or a version number. Both apps ship in one distribution, so this
    capability is fixed within a release.
    """
    from django_ox import schedules

    return callable(getattr(schedules, "schedule_source_from_options", None))


def _reads_schedule_rows(options: dict[str, Any]) -> bool:
    """
    Whether OPTIONS["SCHEDULE_SOURCE"] names a source, so schedules can be rows.

    Only that the option names something. django-ox reports a source it
    cannot build as django_ox.E006 and the worker refuses to start on it,
    so validating the path a second time here would report one fault
    twice and disagree with django-ox about what a source may be:
    a source is any class with the method, not only the one we ship.
    """
    source = options.get("SCHEDULE_SOURCE")
    return isinstance(source, str) and bool(source.strip())


def _exposed_reconcilers(
    options: dict[str, Any], task_path: str
) -> list[Task[..., Any]]:
    """
    The Tasks OPTIONS["SCHEDULABLE_TASKS"] exposes that run the reconciler.

    Each entry is imported and matched on the Task's module_path, which is
    the path its task row carries, rather than on the configured string:
    the entry a row names may be the reconciler rebound with using() under
    a project's own dotted path, and that is the one a row-reading backend
    can run when the public one is on a queue it does not accept. An entry
    that does not import is django_ox.E007's to report.
    """
    raw = options.get("SCHEDULABLE_TASKS", {})
    if not isinstance(raw, dict):
        return []
    found = []
    for config in raw.values():
        path = _schedulable_task_path(config)
        if not isinstance(path, str):
            continue
        try:
            obj = import_string(path)
        except Exception:  # noqa: S112 - django_ox.E007 reports it
            continue
        if isinstance(obj, Task) and obj.module_path == task_path:
            found.append(obj)
    return found


def _binds(task_obj: Task[..., Any], alias: str, queue_name: str | None = None) -> bool:
    """
    Whether a schedule on backend `alias` can build `task_obj`.

    The same rebind django-ox makes: a schedule row, or a SCHEDULES entry,
    runs its task on the backend that dispatches it, through Task.using(),
    which validates the queue against that backend. A row that fails it is
    skipped with a warning at every build, and nothing on the row can fix
    it. Every exception counts as a failure, as it does where django-ox
    builds the row.
    """
    try:
        if queue_name is not None:
            task_obj.using(backend=alias, queue_name=queue_name)
        elif task_obj.backend != alias:
            task_obj.using(backend=alias)
    except Exception:  # django-ox skips a row on any exception
        return False
    return True


@dataclass(frozen=True)
class ReconcilerSchedule:
    """
    What settings say about the schedule of one reconciler.

    aliases are the django-ox backends that run it. row_readers are the
    django-ox backends that read schedule rows, on an engine that reads
    them, and exposed are the Tasks SCHEDULABLE_TASKS lets a row name for
    it, whether or not any of those backends can run them.
    """

    aliases: tuple[str, ...]
    row_readers: tuple[str, ...]
    exposed: tuple[Task[..., Any], ...]

    @property
    def unbound(self) -> bool:
        """
        Exposed to rows that are read, and still run by nothing.

        No backend that reads the rows accepts the queue the reconciler
        is built on, so django-ox skips every row that names it, and no
        SCHEDULES entry runs it either.
        """
        return not self.aliases and bool(self.row_readers) and bool(self.exposed)


def reconcile_schedule(task_path: str = RECONCILE_TASK_PATH) -> ReconcilerSchedule:
    """
    Where the reconciler at `task_path` is scheduled; see reconcile_schedule_aliases.

    Batches and workflows both ask here, each for its own reconciler.
    """
    options_by_alias = _ox_backend_options()
    by_settings = {
        alias
        for alias, options in options_by_alias.items()
        if _schedules_reconciler(options, task_path)
    }
    row_readers: list[str] = []
    exposed: list[Task[..., Any]] = []
    by_rows: set[str] = set()
    if _engine_reads_schedule_rows():
        row_readers = sorted(
            alias
            for alias, options in options_by_alias.items()
            if _reads_schedule_rows(options)
        )
    if row_readers:
        exposed = [
            task_obj
            for options in options_by_alias.values()
            for task_obj in _exposed_reconcilers(options, task_path)
        ]
        by_rows = {
            alias
            for alias in row_readers
            if any(_binds(task_obj, alias) for task_obj in exposed)
        }
    return ReconcilerSchedule(
        aliases=tuple(sorted(by_settings | by_rows)),
        row_readers=tuple(row_readers),
        exposed=tuple(exposed),
    )


def reconcile_schedule_aliases(task_path: str = RECONCILE_TASK_PATH) -> list[str]:
    """
    The django-ox backend aliases whose OPTIONS schedule the reconciler.

    Read from settings alone, with no database, so create(), seal() and
    the system check answer one question from one place. A schedule on
    any django-ox backend counts: schedules_from_options rebinds the task
    to the backend it is declared on, so that backend's worker runs it,
    and the reconciler resolves the oxpull tables through the router
    rather than through the alias it was enqueued on. A backend that is
    not django-ox dispatches no schedule, so an entry there counts for
    nothing.

    Two channels declare a schedule and both count. An OPTIONS["SCHEDULES"]
    entry naming the reconciler is the settings one. From django-ox 1.2.0 a
    backend can instead name OPTIONS["SCHEDULE_SOURCE"] and keep its
    schedules as admin-editable rows, which settings cannot see; for that
    one the question settings can answer is whether a row is allowed to
    name the reconciler at all, which is OPTIONS["SCHEDULABLE_TASKS"], and
    whether the backend reading the rows can run what it exposes. The
    registry is process-wide and a row is not bound to an alias, so the
    backend that reads the rows and the backend whose OPTIONS expose the
    reconciler need not be the same one. The backend that reads the rows
    rebinds the task to itself, though, and skips a row whose task is on a
    queue it does not accept, so a reader counts only when an exposed Task
    binds to it.

    That second channel counts only where the engine implements it. Both
    keys are inert on an engine that does not read them, and accepting
    them there would seal batches nothing can complete on exactly the
    projects that wrote the keys in good faith, so the installed
    django-ox is asked first.

    The honest cost of counting a source is a project that reads rows and
    exposes the reconciler but never writes the row: it passes here and
    its batches sit SEALED. Reading the schedule table instead would make
    the system check -- which runs without a database and before migrate
    -- answer differently from create() and seal(), and that disagreement
    is what this function exists to prevent. Refusing the configuration
    outright is worse than the gap: it fails a project that has the
    reconciler running, on the line that creates a batch.
    """
    return list(reconcile_schedule(task_path).aliases)


def queue_accepted_by(backend: Any, params: Any) -> str:
    """
    The queue a reconciler takes on `backend`: "default" when it accepts
    that, else the first queue its settings declare.
    """
    from django.tasks import DEFAULT_TASK_QUEUE_NAME

    queues = backend.queues
    if not queues or DEFAULT_TASK_QUEUE_NAME in queues:
        return DEFAULT_TASK_QUEUE_NAME
    # The declared order is the operator's own; sorted() is only a
    # fallback for a backend built outside settings.TASKS.
    configured = params.get("QUEUES") if isinstance(params, dict) else None
    return str((configured or sorted(queues))[0])


def schedule_entry_hint(
    task_path: str, schedule_name: str, candidates: list[str]
) -> str:
    """
    A SCHEDULES entry for the reconciler, and the backend to add it to.

    The first candidate that runs the reconciler as it is built is named
    with the plain entry. Failing that, the first one that runs it on a
    queue of its own is named with that queue_name in the entry. Failing
    both, no backend is named. Pasting the entry where it points passes
    django-ox's schedule checks, which a backend the reconciler cannot bind
    to does not: a queue it refuses is django_ox.E002, and the same
    schedule name on two backends is django_ox.E003.
    """
    from django.tasks import task_backends

    entry: dict[str, str] = {"task": task_path, "cron": "* * * * *"}
    try:
        reconciler = import_string(task_path)
    except Exception:
        reconciler = None
    target: tuple[str, str | None] | None = None
    if isinstance(reconciler, Task):
        target = next(
            ((alias, None) for alias in candidates if _binds(reconciler, alias)), None
        )
    if isinstance(reconciler, Task) and target is None:
        for alias in candidates:
            try:
                queue = queue_accepted_by(task_backends[alias], settings.TASKS[alias])
            except Exception:  # noqa: S112 - the backend's own checks report it
                continue
            if _binds(reconciler, alias, queue_name=queue):
                target = (alias, queue)
                break
    if target is None:
        where = "a django-ox backend's OPTIONS['SCHEDULES']"
    else:
        where = f"TASKS[{target[0]!r}]['OPTIONS']['SCHEDULES']"
        if target[1] is not None:
            entry["queue_name"] = target[1]
    return f"Add to {where}: {json.dumps({schedule_name: entry})}"


def unbound_reconciler_message(
    task_path: str, schedule: ReconcilerSchedule, consequence: str
) -> str:
    """Why an exposed reconciler runs nowhere; see ReconcilerSchedule.unbound."""
    queues = ", ".join(
        sorted({repr(task_obj.queue_name) for task_obj in schedule.exposed})
    )
    readers = ", ".join(repr(alias) for alias in schedule.row_readers)
    return (
        f"SCHEDULABLE_TASKS exposes {task_path} to schedule rows, but it is "
        f"built on queue {queues}, and no backend that reads the rows accepts "
        f"it ({readers}). django-ox skips every row that names it, and no "
        f"SCHEDULES entry names it either, so {consequence}."
    )


def unbound_reconciler_hint(
    task_path: str, schedule_name: str, schedule: ReconcilerSchedule
) -> str:
    """What to do about an exposed reconciler that runs nowhere."""
    queues = ", ".join(
        sorted({repr(task_obj.queue_name) for task_obj in schedule.exposed})
    )
    entry = schedule_entry_hint(task_path, schedule_name, list(schedule.row_readers))
    return (
        f"{entry}. Or read the schedule rows on a django-ox backend whose "
        f"QUEUES include {queues}."
    )


def reconcile_schedule_hint() -> str:
    """
    What to do about a missing reconciler schedule, given what is set already.

    A project whose backends read their schedules from rows has already
    done what the settings hint asks for, in the other channel; naming it
    again reads as advice to undo that. It is told the step it is
    actually missing instead.

    On an engine that does not read rows at all, SCHEDULE_SOURCE is not a
    move this hint should protect: nothing is reading the rows, exposing
    the reconciler would change nothing, and the settings block is the
    only remedy that starts one.
    """
    if _engine_reads_schedule_rows() and any(
        _reads_schedule_rows(options) for options in _ox_backend_options().values()
    ):
        return RECONCILE_SCHEDULABLE_HINT
    return RECONCILE_SCHEDULE_HINT


def _require_reconcile_schedule() -> None:
    """
    Refuse to create or seal a batch that nothing would ever complete.

    Completion happens only in reconcile(), and only a django-ox schedule
    runs it. A project that copies the uniqueness and rate-limit settings
    and misses the SCHEDULES block used to get batches that sat SEALED
    forever, with no error and no log line: the system check can be
    silenced and settings can change under a batch that already exists,
    so the refusal lives here as well, on the line that configures the
    batch and again on the line that seals it. A reconciler exposed to
    schedule rows that no backend reading them can run is refused with
    what it lacks, as oxpull.E008 reports it.
    """
    schedule = reconcile_schedule(RECONCILE_TASK_PATH)
    if schedule.aliases:
        return
    if schedule.unbound:
        raise ImproperlyConfigured(
            "Batches need a schedule for oxpull.batches.reconcile that a worker "
            "can run. "
            + unbound_reconciler_message(
                RECONCILE_TASK_PATH, schedule, "a sealed batch would never complete"
            )
            + " "
            + unbound_reconciler_hint(
                RECONCILE_TASK_PATH, RECONCILE_SCHEDULE_NAME, schedule
            )
            + " The system check reports the same condition as oxpull.E008."
        )
    raise ImproperlyConfigured(
        "Batches need a schedule for oxpull.batches.reconcile, and no "
        "django-ox backend in settings.TASKS declares one, so a sealed batch "
        f"would never complete. {reconcile_schedule_hint()} The system check "
        "reports the same condition as oxpull.W002."
    )


@dataclass(frozen=True)
class BatchProgress:
    """
    Live counts for one batch, computed on demand from member task rows.

    total is the sealed snapshot once the batch is sealed, and the live
    member count while it is open. pending, succeeded and failed come
    from the task rows that still say so, and they need not add up to
    total. The difference is unattributed: members whose task row
    ox_prune deleted, members the reaper marked LOST because their
    attempt was never observed, and members an operator closed with
    django_ox.actions.discard. All three count as finished, since none of
    them can still be pending, but none is attributable to success or
    failure.
    """

    total: int
    pending: int
    succeeded: int
    failed: int

    @property
    def unattributed(self) -> int:
        """
        Members that finished without an outcome that reads as clean.

        Three things land here. A member whose task row ox_prune deleted
        joins to nothing. A member the reaper marked LOST holds a row
        that says only that its attempt went unobserved. A member
        django_ox.actions.discard closed holds a row saying an operator
        cancelled it before it ran. None of the three is a success, and
        none is a recorded failure, which is why on_success stays
        withheld while this is above zero.
        """
        return max(self.total - (self.pending + self.succeeded + self.failed), 0)

    @property
    def pruned(self) -> int:
        """
        Deprecated alias of unattributed, to be removed in oxpull 2.0.

        The name was accurate while ox_prune was the only way to lose an
        outcome. The reaper's LOST rows and the operator's DISCARDED rows
        now land in the same bucket, so it reports more than it says.
        Reading it still returns the same number, and emits a
        DeprecationWarning.
        """
        # The warning lives in the getter rather than as a field or a
        # __getattr__ hook, so the dataclass's fields, equality, repr,
        # asdict and pickling are untouched. Attribute access reaches the
        # getter without a Python frame between, so stacklevel=2 is the
        # line that read .pruned.
        warnings.warn(
            "BatchProgress.pruned is deprecated and will be removed in oxpull 2.0. "
            "Use BatchProgress.unattributed, which returns the same number.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.unattributed

    @property
    def finished(self) -> int:
        return self.total - self.pending


def _resolve_callback(
    callback: Task[..., Any] | str | None,
    args: list[Any] | tuple[Any, ...] | None,
    kwargs: dict[str, Any] | None,
    label: str = "Batch",
) -> tuple[str, list[Any], dict[str, Any]]:
    """
    Normalise a callback into (dotted path, args, kwargs) for storage.

    Everything about the callback is validated here rather than at
    completion: a mistake should fail on the line that configures the
    batch, not silently three hours later inside the transaction that
    completes it. That covers the path (it must import to a Task on a
    django-ox backend), the arguments (list and dict, JSON-serialisable
    by the same normalize_json the task row uses) and the shape of the
    Task itself, because only the path is stored and per-call using()
    overrides would be dropped in silence.
    """
    if callback is None:
        return "", [], {}
    if isinstance(callback, str):
        try:
            resolved = import_string(callback)
        except ImportError as exc:
            raise ImproperlyConfigured(
                f"{label} callback {callback!r} does not import ({exc})."
            ) from exc
    else:
        resolved = callback
    if not isinstance(resolved, Task):
        raise ImproperlyConfigured(
            f"{label} callback {callback!r} is not a django.tasks Task; "
            "pass the module-level name created by the @task decorator."
        )
    _require_ox_backend(resolved, role="callbacks", label=label)
    _reject_using_overrides(resolved, label=label)
    _reject_unique(resolved, label=label)
    if args is not None and not isinstance(args, list | tuple):
        raise ImproperlyConfigured(
            f"{label} callback {resolved.module_path!r} args must be a list "
            f"or tuple, not {type(args).__name__}."
        )
    if kwargs is not None and not isinstance(kwargs, dict):
        raise ImproperlyConfigured(
            f"{label} callback {resolved.module_path!r} kwargs must be a "
            f"dict, not {type(kwargs).__name__}."
        )
    try:
        stored_args = cast("list[Any]", normalize_json(list(args or [])))
        stored_kwargs = cast("dict[str, Any]", normalize_json(dict(kwargs or {})))
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(
            f"{label} callback {resolved.module_path!r} arguments are not "
            f"JSON-serializable ({exc})."
        ) from exc
    return resolved.module_path, stored_args, stored_kwargs


def _member_task_counts(batch_pk: uuid.UUID, db_alias: str) -> dict[str, int]:
    """
    Member task rows grouped by status.

    This is the join the whole design rests on, expressed through the ORM
    rather than as SQL text: table and column names come from the
    django_ox model's _meta through the query compiler, so nothing here
    hardcodes a django-ox table name that a future release could rename.
    The join cannot cross database aliases, which is why both sides are
    pinned to the one alias oxpull_db_alias() resolves.

    A membership row whose task row is gone joins to nothing and so
    appears in no bucket: pruned reads as finished, which is what keeps
    long-lived batches completing after ox_prune has been through.
    """
    members = (
        OxpullBatchMember.objects.using(db_alias)
        .filter(batch_id=batch_pk)
        .values("task_id")
    )
    rows = (
        OxTask.objects.using(db_alias)
        .filter(id__in=members)
        .values("status")
        .annotate(count=Count("id"))
        .order_by()
    )
    return {row["status"]: row["count"] for row in rows}


def _pending_count(counts: dict[str, int]) -> int:
    """
    Members whose task row is present and not settled.

    Counted as everything outside SETTLED_STATUSES rather than everything
    inside PENDING_STATUSES: the two agree on every status this package
    knows, and differ on one it does not, which must read as pending. A
    row that is absent altogether is in no bucket and reads as finished.
    Workflow completion and progress read this same function.
    """
    return sum(
        count for status, count in counts.items() if status not in SETTLED_STATUSES
    )


DEADLOCK = "deadlock"
SERIALIZATION_FAILURE = "serialization_failure"


def _lost_conflict(
    exc: OperationalError, db_alias: str, *, deadlocks: bool
) -> str | None:
    """
    DEADLOCK or SERIALIZATION_FAILURE when `exc` is one, else None.

    A deadlock is InnoDB's 1213 or PostgreSQL's 40P01. Both kinds roll back
    the whole transaction on the server and leave nothing behind, and both
    engines' manuals say to run the transaction again.
    """
    from .backend import is_deadlock, is_serialization_failure

    if deadlocks and is_deadlock(exc, db_alias):
        return DEADLOCK
    if is_serialization_failure(exc, db_alias):
        return SERIALIZATION_FAILURE
    return None


def _reissue[T](
    once: Callable[[], T],
    db_alias: str,
    *,
    deadlocks: bool,
    retrying: Callable[[str, int, int], None],
) -> T:
    """
    Run `once`, and run it again when the transaction it opens is lost to
    a conflict: a PostgreSQL serialization failure, and a deadlock (InnoDB's
    1213 or PostgreSQL's 40P01) where `deadlocks` is set.

    Whether this call owns the transaction is read before `once` opens its
    atomic block: inside it the block is always active. The call owns it
    when the connection is in autocommit and no atomic block is open. Then
    the server rolled the whole of it back, so the body can run again from
    the top. Inside a caller's atomic block, or a transaction the caller
    opened by turning autocommit off, the server discarded the caller's work
    too, and only the caller can redo that, so the error propagates. A
    savepoint retry would be unsound either way: the server rolled back the
    transaction, not the statement.

    `retrying` is told which conflict, which attempt and the bound, before
    the pause. The pause is jittered so that the losers of one conflict do
    not run again in lockstep. A deadlock has one victim, so a short pause
    and MAX_ADD_ATTEMPTS do. A serialization failure reaches every
    transaction that waited on the writer that committed, so its pause
    doubles with each attempt and its bound is MAX_SERIALIZATION_ATTEMPTS.
    """
    connection = connections[db_alias]
    owns_transaction = connection.get_autocommit() and not connection.in_atomic_block
    for attempt in range(max(MAX_ADD_ATTEMPTS, MAX_SERIALIZATION_ATTEMPTS)):
        try:
            return once()
        except OperationalError as exc:
            conflict = (
                _lost_conflict(exc, db_alias, deadlocks=deadlocks)
                if owns_transaction
                else None
            )
            bound = (
                MAX_SERIALIZATION_ATTEMPTS
                if conflict == SERIALIZATION_FAILURE
                else MAX_ADD_ATTEMPTS
            )
            if conflict is None or attempt + 1 >= bound:
                raise
            retrying(conflict, attempt + 1, bound)
            pause = ADD_RETRY_JITTER
            if conflict == SERIALIZATION_FAILURE:
                pause *= 2 ** min(attempt, SERIALIZATION_PAUSE_DOUBLINGS)
            time.sleep(random.uniform(0, pause))  # noqa: S311
    raise AssertionError("unreachable: the loop returns or raises")


def _reissue_add[T](
    once: Callable[[], T],
    db_alias: str,
    *,
    label: str,
    owner_pk: uuid.UUID,
    task_path: str,
    log: logging.Logger,
) -> T:
    """
    Run one add() attempt, and reissue it after a lost deadlock or
    PostgreSQL serialization failure, when add() owns its transaction.

    Batch.add and Workflow.add both go through here. Every add() to one
    row claims it with an UPDATE, so at PostgreSQL REPEATABLE READ two
    overlapping add()s are a serialization failure for the one that waited.
    """
    noun = label.lower()

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        what = "a deadlock" if conflict == DEADLOCK else "a serialization failure"
        event = "deadlock" if conflict == DEADLOCK else "serialization"
        log.warning(
            f"{label} %s: add() of %r lost {what} on attempt %d of %d; "
            "reissuing its transaction",
            owner_pk,
            task_path,
            attempt,
            bound,
            extra={
                "event": f"{noun}_add_{event}_retry",
                f"{noun}_id": str(owner_pk),
                "task_path": task_path,
                "attempt": attempt,
            },
        )

    return _reissue(once, db_alias, deadlocks=True, retrying=retrying)


def _claim_open(row: QuerySet[Any], open_status: str) -> str | None:
    """
    Take the lock that serialises add() against seal(), and return the
    status it sees: OPEN, another status, or None when the row is gone.

    `row` is the batch or workflow row, filtered by primary key. A no-op
    UPDATE under the OPEN predicate takes the row lock seal() takes. A
    rowcount of one settles it. Zero does not: it is the row no longer
    OPEN, or it is a MySQL connection counting changed rows rather than
    matched ones, which a no-op UPDATE never changes. Django asks for
    CLIENT_FOUND_ROWS on every MySQL connection, and a project that sets
    OPTIONS["client_flag"] replaces it. So the status is read under the
    row lock: where the UPDATE matched, the lock is already this
    transaction's; where it did not, this read takes it now. Either way
    the answer is current, not the caller's snapshot, which under
    REPEATABLE READ can still say OPEN after a seal() committed.
    """
    if row.filter(status=open_status).update(status=open_status):
        return open_status
    return cast(
        "str | None", row.select_for_update().values_list("status", flat=True).first()
    )


def _count_current(rows: QuerySet[Any], db_alias: str, *, ambient: bool) -> int:
    """
    Count `rows` as of now, for seal(); Batch._count_members says why.

    A plain COUNT when seal() owns its transaction. A locking read, which
    is a current read, only inside a caller's transaction, whose snapshot
    can predate a row committed since, and only where row locks exist.
    """
    if not ambient or not connections[db_alias].features.has_select_for_update:
        return rows.count()
    return len(rows.select_for_update().values_list("pk", flat=True))


class Batch:
    """
    A handle on one OxpullBatch row. Build one with create() or get().

    The handle holds no derived state: status and progress are read from
    the database on each call, because the reconciler that completes a
    batch runs in another process.
    """

    def __init__(self, row: OxpullBatch) -> None:
        self._row = row

    def __repr__(self) -> str:
        return f"<Batch {self._row.name!r} {self._row.pk}>"

    @property
    def id(self) -> uuid.UUID:
        return self._row.pk

    @property
    def name(self) -> str:
        return self._row.name

    @property
    def status(self) -> str:
        """The status as stored now, not as it was when this handle was built."""
        db_alias = oxpull_db_alias()
        return str(
            OxpullBatch.objects.using(db_alias)
            .values_list("status", flat=True)
            .get(pk=self._row.pk)
        )

    def add[**P, R](
        self, task: Task[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> TaskResult[P, R]:
        """
        Enqueue a task and record its membership, in one transaction.

        The task row and the membership row commit or roll back together,
        so a batch can never contain a member that was never enqueued, nor
        enqueue work the batch does not know it is waiting for.

        Raises BatchSealed if the batch is no longer OPEN.
        """
        db_alias = oxpull_db_alias()
        _require_ox_backend(task, role="members")
        return _reissue_add(
            lambda: self._add_once(db_alias, task, *args, **kwargs),
            db_alias,
            label="Batch",
            owner_pk=self._row.pk,
            task_path=task.module_path,
            log=logger,
        )

    def _add_once[**P, R](
        self, db_alias: str, task: Task[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> TaskResult[P, R]:
        """One attempt of add(): the transaction that add() may reissue."""
        with transaction.atomic(using=db_alias):
            # Enqueue first, claim second. A unique member's enqueue
            # takes the OxpullUniqueLock row for its key, so doing it
            # under the batch row lock would make add() hold batch ->
            # unique lock while an ordinary caller that enqueues a unique
            # task and then calls add() holds unique lock -> batch: a
            # deadlock cycle, observed on PostgreSQL and reported as
            # ER_LOCK_DEADLOCK on MySQL. This order is the one callers
            # can also follow, and it keeps the batch row locked for two
            # statements rather than for a whole enqueue.
            result = task.enqueue(*args, **kwargs)
            # A no-op UPDATE under the OPEN predicate, taken before the
            # membership INSERT, is what serialises add() against seal().
            # It takes the same row lock seal() takes, on every supported
            # database, so either seal() waits for this member to commit
            # and counts it, or this add() finds the batch already SEALED
            # and the whole transaction, enqueue included, rolls back. A
            # plain SELECT would not do: under MVCC it can read OPEN from
            # a snapshot while seal() holds an uncommitted UPDATE, and
            # the member would land after the count that total_count
            # froze.
            status = _claim_open(
                OxpullBatch.objects.using(db_alias).filter(pk=self._row.pk),
                OxpullBatch.Status.OPEN,
            )
            if status != OxpullBatch.Status.OPEN:
                raise self._not_open_error(status)
            try:
                # Its own savepoint: a duplicate membership must not
                # poison a caller's surrounding transaction, the same
                # confinement the unique enqueue path uses.
                with transaction.atomic(using=db_alias):
                    OxpullBatchMember.objects.using(db_alias).create(
                        batch_id=self._row.pk,
                        task_id=result.id,
                        created_at=timezone.now(),
                    )
            except IntegrityError as exc:
                raise BatchMemberExists(
                    f"Task {result.id} is already a member of batch "
                    f"{self._row.pk}; a deduplicated unique enqueue returned "
                    "a task this batch already waits on."
                ) from exc
        return result

    def seal(self) -> None:
        """
        Freeze membership and snapshot total_count. Completion is possible
        only after this; a batch that is never sealed never completes.

        Raises BatchSealed if the batch was already sealed or completed.
        There is no un-seal. Raises ImproperlyConfigured if no django-ox
        backend schedules the reconciler, since the batch would then stay
        SEALED forever; the status is checked first, so a batch that is
        already sealed says so whatever the settings.
        """
        db_alias = oxpull_db_alias()
        # Read before the block opens; inside it the answer is always yes.
        ambient = connections[db_alias].in_atomic_block
        with transaction.atomic(using=db_alias):
            # The status write comes first and locks the row, so any
            # add() already in flight commits before the count below runs
            # and any later add() sees SEALED. Counting first would let a
            # member land after the snapshot and make total_count a lie.
            sealed = (
                OxpullBatch.objects.using(db_alias)
                .filter(pk=self._row.pk, status=OxpullBatch.Status.OPEN)
                .update(status=OxpullBatch.Status.SEALED, sealed_at=timezone.now())
            )
            if not sealed:
                raise self._not_open_error(self._stored_status(db_alias))
            # After the status, so that a batch already SEALED or COMPLETE
            # reports that rather than the settings. Inside the block, so
            # that a refusal here rolls the status write back and the
            # batch stays OPEN.
            _require_reconcile_schedule()
            OxpullBatch.objects.using(db_alias).filter(pk=self._row.pk).update(
                total_count=self._count_members(db_alias, ambient=ambient)
            )

    def _count_members(self, db_alias: str, *, ambient: bool) -> int:
        """
        The member count as of now, not as of an older snapshot.

        When seal() owns its transaction the status write above is the
        transaction's first statement. It waits on every add() still
        holding the batch row, and the count that follows is the
        transaction's first consistent read, so even under MySQL's
        REPEATABLE READ its read view begins after those add()s
        committed; a later add() finds the row SEALED. PostgreSQL at
        REPEATABLE READ takes its snapshot at that first statement, and
        a status write that waited on an add() then finds a row changed
        since the snapshot and raises a serialization failure rather
        than proceeding, so nothing is counted stale there either: the
        batch stays OPEN and the caller retries seal(). Django's
        PostgreSQL default is READ COMMITTED, where the UPDATE re-reads
        the row and the COUNT takes a fresh snapshot. A plain COUNT is
        current wherever seal() proceeds, and it is one aggregate rather
        than a row lock per member pulled into Python, which at a million
        members on MySQL was the difference between 28 seconds and a
        count.

        Inside a caller's transaction the snapshot can be older than the
        lock: under REPEATABLE READ the read view was fixed by the
        caller's first consistent read, and a member whose add()
        committed after that is invisible to a plain COUNT even though
        the status write waited on nothing. A locking read is a current
        read on every database that has one, and it also holds the
        members it counted for the rest of the caller's transaction. That
        is the cost of sealing inside a transaction, and only there.
        The locking read blocks no INSERT of a new member; the freeze
        comes from the status flip above, which add()'s UPDATE ... WHERE
        status = OPEN no longer matches.
        SQLite has no row locks and needs none: writers are serialised.
        """
        return _count_current(
            OxpullBatchMember.objects.using(db_alias).filter(batch_id=self._row.pk),
            db_alias,
            ambient=ambient,
        )

    def progress(self) -> BatchProgress:
        """Counts for this batch, queried from the member task rows now."""
        db_alias = oxpull_db_alias()
        row = OxpullBatch.objects.using(db_alias).get(pk=self._row.pk)
        if row.total_count is not None:
            total = row.total_count
            counts = _member_task_counts(row.pk, db_alias)
        else:
            # An open batch has no snapshot yet, so the live membership
            # count is the only honest denominator, and it is a separate
            # statement from the status counts. Read it first and let the
            # counted members raise it: an add() committing between the
            # two then makes this reading lag the batch by a member,
            # which is harmless. The other order inflates the
            # denominator instead, and total - pending reports members
            # that neither finished nor were pruned.
            members = (
                OxpullBatchMember.objects.using(db_alias)
                .filter(batch_id=row.pk)
                .count()
            )
            counts = _member_task_counts(row.pk, db_alias)
            total = max(members, sum(counts.values()))
        return BatchProgress(
            total=total,
            pending=_pending_count(counts),
            succeeded=counts.get(OxTask.Status.SUCCESSFUL, 0),
            failed=counts.get(OxTask.Status.FAILED, 0),
        )

    def _stored_status(self, db_alias: str) -> str | None:
        return (
            OxpullBatch.objects.using(db_alias)
            .values_list("status", flat=True)
            .filter(pk=self._row.pk)
            .first()
        )

    def _not_open_error(self, status: str | None) -> BatchSealed:
        if status is None:
            raise OxpullBatch.DoesNotExist(f"Batch {self._row.pk} no longer exists.")
        return BatchSealed(f"Batch {self._row.pk} is {status}; membership is frozen.")


def _require_ox_backend(
    task: Task[..., Any], *, role: str, label: str = "Batch"
) -> None:
    """
    Members and callbacks alike are only meaningful as OxTask rows.

    A member enqueued through, say, the immediate backend writes no row
    for the completion join to find, and the batch would read it as
    already finished. A callback on that backend is worse: the immediate
    backend runs the function inline, inside the transaction that
    completes the batch, and returns an id that is not a UUID, so
    recording it raises and rolls the completion back with the side
    effect already done. The next tick then repeats it, forever. Fail on
    the add() or the create() instead.
    """
    from django_ox.backend import OxBackend

    backend = task.get_backend()
    if not isinstance(backend, OxBackend):
        raise ImproperlyConfigured(
            f"{label} {role} must be enqueued on a django-ox backend; task "
            f"{task.module_path!r} uses backend {task.backend!r} "
            f"({type(backend).__name__}), whose tasks have no OxTask row."
        )


def _reject_using_overrides(callback: Task[..., Any], label: str = "Batch") -> None:
    """
    Refuse a callback carrying per-call using() overrides.

    Only the dotted path is stored, so the reconciler re-imports the
    module-level Task and enqueues that. A queue, priority or run_after
    set with using() on the way in would be dropped in silence, and the
    callback would land on a queue nobody is draining. Reject it on the
    create() line and say so.
    """
    declared = import_string(callback.module_path)
    if declared == callback:
        return
    changed = (
        sorted(
            field
            for field in ("queue_name", "priority", "run_after", "backend")
            if getattr(declared, field) != getattr(callback, field)
        )
        if isinstance(declared, Task)
        else []
    )
    if not changed:
        raise ImproperlyConfigured(
            f"{label} callback {callback.module_path!r} does not import "
            "back to itself; callbacks are stored as a dotted path, so the "
            "module-level name must be this Task."
        )
    raise ImproperlyConfigured(
        f"{label} callback {callback.module_path!r} carries using() "
        f"override(s) {', '.join(changed)}, which cannot be stored: "
        "only the dotted path is kept, so the callback would be enqueued "
        "with the module-level defaults. Declare the overrides on the "
        "@task decorator instead."
    )


def _reject_unique(callback: Task[..., Any], label: str = "Batch") -> None:
    """
    Refuse a unique task as a callback.

    A unique task coalesces: while one with the same arguments is
    pending, enqueueing it again returns the pending task instead of
    writing a second row. Two batches completing with the same unique
    callback and the same arguments would therefore share one callback
    task, and the batch that coalesced would record an id whose run
    belongs to the other batch. Its on_complete would never fire, and
    nothing would say so. Refused on the create() line, and again at
    completion, because a later deploy can make the path unique through
    UNIQUE_TASKS or the decorator after the batch row was written.
    """
    from .backend import OxpullBackend

    path = callback.module_path
    backend = callback.get_backend()
    configured = (
        isinstance(backend, OxpullBackend) and path in backend.unique_task_paths
    )
    # "Batch" keeps the batch wording exactly; any other label pluralises plainly.
    plural = "batches" if label == "Batch" else f"{label.lower()}s"
    if is_registered(path) or configured:
        raise ImproperlyConfigured(
            f"{label} callback {path!r} is a unique task. Two {plural} "
            "completing with the same unique callback and arguments would "
            "coalesce onto one callback task, and one "
            f"{label.lower()}'s callback would never run. Use a task that is "
            "not unique."
        )


def create(
    name: str,
    *,
    on_complete: Task[..., Any] | str | None = None,
    on_complete_args: list[Any] | tuple[Any, ...] | None = None,
    on_complete_kwargs: dict[str, Any] | None = None,
    on_success: Task[..., Any] | str | None = None,
    on_success_args: list[Any] | tuple[Any, ...] | None = None,
    on_success_kwargs: dict[str, Any] | None = None,
) -> Batch:
    """
    Create an OPEN batch.

    on_complete fires once every member's row is settled (see
    SETTLED_STATUSES) or gone, whatever the outcome; any other status
    holds the batch open, and a member recorded LOST may still be
    executing, so this is not a barrier against a running member. on_success fires in
    addition, and only when every member's outcome is known and none of
    them FAILED. Each is a Task or its
    dotted path, with optional arguments; both are enqueued as ordinary
    tasks, retried and failed like any other, and the batch does not
    supervise them beyond storing their ids.

    Raises ImproperlyConfigured if no django-ox backend schedules the
    reconciler: a batch nothing completes should fail on the line that
    creates it.
    """
    _require_reconcile_schedule()
    if len(name) > NAME_MAX_LENGTH:
        # SQLite would store it, MySQL and PostgreSQL would not; reject
        # it identically everywhere.
        raise ValueError(f"Batch name exceeds {NAME_MAX_LENGTH} characters.")
    complete_path, complete_args, complete_kwargs = _resolve_callback(
        on_complete, on_complete_args, on_complete_kwargs
    )
    success_path, success_args, success_kwargs = _resolve_callback(
        on_success, on_success_args, on_success_kwargs
    )
    db_alias = oxpull_db_alias()
    row = OxpullBatch.objects.using(db_alias).create(
        name=name,
        status=OxpullBatch.Status.OPEN,
        on_complete_path=complete_path,
        on_complete_args=complete_args,
        on_complete_kwargs=complete_kwargs,
        on_success_path=success_path,
        on_success_args=success_args,
        on_success_kwargs=success_kwargs,
        created_at=timezone.now(),
    )
    return Batch(row)


def get(batch_id: uuid.UUID | str) -> Batch:
    """The batch with this id. Raises OxpullBatch.DoesNotExist if there is none."""
    db_alias = oxpull_db_alias()
    return Batch(OxpullBatch.objects.using(db_alias).get(pk=batch_id))


# A reconciler tick stops before its next item when less time is left
# before its task timeout than this many times the slowest item it has
# taken so far.
SWEEP_STOP_FACTOR = 2.0


def _timed_out(exc: BaseException) -> bool:
    """
    Whether this exception is a TaskTimeout, or was raised while one was
    unwinding.

    It follows the chain django-ox's worker follows to record an attempt
    as timed out. A timeout delivered inside a database driver rarely
    arrives bare: PyMySQL turns it into OperationalError 2013, and
    Django's atomic() exit can raise over it, each with the TaskTimeout
    as cause or context.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, TaskTimeout):
            return True
        seen.add(id(current))
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            current = None
        else:
            current = current.__context__
    return False


def _sweep(
    pks: list[uuid.UUID], kind: str, plural: str, log: logging.Logger
) -> Iterator[uuid.UUID]:
    """
    The sealed rows one reconciler tick visits: all of them, in a new
    random order, stopping early when the task's time is nearly up.

    The order changes every tick so that a row which always takes the rest
    of a tick cannot keep the same rows behind it waiting tick after tick.
    It is a shuffle and not a rotation: under a rotation, the row just
    after a slow one would be visited only on ticks that start with it.

    The stop reads django-ox's remaining(), so it applies only under a
    TASK_TIMEOUT or TASK_TIMEOUTS entry. Before each row, it stops when
    less time is left than SWEEP_STOP_FACTOR times the slowest row this
    tick has taken, and logs how many rows were not visited. The task then
    returns normally. A row that overruns anyway gets the TaskTimeout,
    which the caller must let out (_timed_out).
    """
    order = list(pks)
    random.shuffle(order)
    slowest = 0.0
    for index, pk in enumerate(order):
        left = remaining()
        if left is not None and left < SWEEP_STOP_FACTOR * slowest:
            log.warning(
                "Stopped reconciling %s %.3fs before the task timeout, with %d "
                "of %d not visited; a later tick visits them. If every tick "
                "stops, give the reconciler's queue a longer TASK_TIMEOUTS entry.",
                plural,
                left,
                len(order) - index,
                len(order),
                extra={
                    "event": f"{kind}_reconcile_stopped",
                    "visited_count": index,
                    "unvisited_count": len(order) - index,
                    "remaining_seconds": left,
                },
            )
            return
        started = time.monotonic()
        yield pk
        slowest = max(slowest, time.monotonic() - started)


def _reconcile() -> int:
    """
    Complete every SEALED batch whose members have all finished, and
    return how many were completed by this pass.

    Run it through django-ox's schedule dispatcher once a minute. Completion
    and callback enqueue share a transaction, so a crash leaves either an
    incomplete batch or a completed batch with its callbacks enqueued.
    A later pass can revisit an incomplete batch.

    One batch cannot starve the others. A batch whose callback no longer
    imports is logged and skipped, and stays SEALED until the deploy that
    broke it is fixed. Under a task timeout, a tick visits sealed batches
    in a new order, stops between batches when the next would probably
    not finish in time, and lets a TaskTimeout out when one lands anyway,
    so django-ox records that attempt as timed out.
    """
    db_alias = oxpull_db_alias()
    sealed = (
        OxpullBatch.objects.using(db_alias)
        .filter(status=OxpullBatch.Status.SEALED)
        .order_by()
        .values_list("pk", flat=True)
    )
    completed = 0
    for batch_pk in _sweep(list(sealed), "batch", "batches", logger):
        try:
            if _complete_if_finished(batch_pk, db_alias):
                completed += 1
        except Exception as exc:
            # The tick ran out of time; this batch did not fail.
            if _timed_out(exc):
                raise
            logger.exception(
                "Batch %s could not be reconciled",
                batch_pk,
                extra={"event": "batch_reconcile_failed", "batch_id": str(batch_pk)},
            )
    return completed


class _MemberPending(Exception):
    """
    Raised inside the completing transaction to roll it back.

    Not an error anyone sees: it is how a member that became pending
    again between the cheap filter and the compare-and-set undoes the
    CAS. Django rolls an atomic block back on any exception, which is
    what makes this the shortest way out of a write already issued.
    `transaction.set_rollback(True)` would also do it, at the cost of
    every caller between here and the block having to check a flag and
    return early; raising says the same thing in one line and cannot be
    forgotten halfway up.
    """


def _complete_if_finished(batch_pk: uuid.UUID, db_alias: str) -> bool:
    """
    Complete one sealed batch if nothing is pending. True if this call is
    the one that completed it.

    The first count is read outside the transaction, as a cheap filter
    that keeps a sweep from opening a write transaction per SEALED batch.
    It is not the authority. "Nothing is pending" used to be monotone and
    is not any more: django_ox.actions.retry and retry_many move a FAILED
    or LOST row back to READY, so a count read before the CAS can be
    stale by the time the CAS lands. The counts are therefore read again
    inside the transaction, after the CAS has taken the batch row's write
    lock, and a member that became pending again rolls the whole thing
    back. Re-reading after the CAS rather than before it is also what
    keeps a sweep from deadlocking on SQLite's read-to-write lock
    upgrade: by then this transaction is already a writer.

    Everything after the CAS rides in the same transaction, so a crash
    between completing the batch and enqueueing its callbacks rolls both
    back and the next tick redoes them.

    At PostgreSQL REPEATABLE READ a reconciler whose CAS waited on another
    one completing the same batch gets a serialization failure when that
    one commits, not a rowcount of 0. The transaction is reissued, and the
    CAS then matches no SEALED row.
    """
    if _pending_count(_member_task_counts(batch_pk, db_alias)):
        return False

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        logger.info(
            "Batch %s: completion lost a serialization failure on attempt %d "
            "of %d; reissuing its transaction",
            batch_pk,
            attempt,
            bound,
            extra={
                "event": "batch_complete_serialization_retry",
                "batch_id": str(batch_pk),
                "attempt": attempt,
            },
        )

    outcome = _reissue(
        lambda: _complete_once(batch_pk, db_alias),
        db_alias,
        deadlocks=False,
        retrying=retrying,
    )
    if outcome is None:
        return False
    failed, unattributed = outcome
    logger.info(
        "Batch %s complete (%d failed, %d unattributed)",
        batch_pk,
        failed,
        unattributed,
        extra={
            "event": "batch_completed",
            "batch_id": str(batch_pk),
            "failed_count": failed,
            "unattributed_count": unattributed,
        },
    )
    return True


def _complete_once(batch_pk: uuid.UUID, db_alias: str) -> tuple[int, int] | None:
    """
    The completing transaction: (failed, unattributed) when this call
    completed the batch, None when it did not.
    """
    try:
        with transaction.atomic(using=db_alias):
            # The callbacks are enqueued once because of this rowcount:
            # racing reconcilers all attempt the same UPDATE, one matches
            # a SEALED row, and the losers' transactions change nothing.
            # The same class of mechanism as the schedule-tick unique
            # constraint, and for the same reason: never check-then-act.
            # Enqueued once is the whole promise; the callback then runs
            # like any other task, which is at least once.
            # failed_count is not written here: it is written below,
            # once the authoritative counts have been read under this
            # lock, so a rolled-back completion never leaves one behind.
            won = (
                OxpullBatch.objects.using(db_alias)
                .filter(pk=batch_pk, status=OxpullBatch.Status.SEALED)
                .update(
                    status=OxpullBatch.Status.COMPLETE,
                    completed_at=timezone.now(),
                )
            )
            if not won:
                return None
            counts = _member_task_counts(batch_pk, db_alias)
            if _pending_count(counts):
                # An operator retried a member between the filter above
                # and this lock. Undo the completion; the batch stays
                # SEALED and the next tick reconsiders it.
                raise _MemberPending
            failed = counts.get(OxTask.Status.FAILED, 0)
            # A LOST row is settled, but it records only that the attempt
            # was never observed, and the straggler still holding that
            # lease may yet resolve it either way. A DISCARDED row is
            # settled too, and says an operator cancelled the work rather
            # than that it was done. Both stand exactly where a member
            # whose row ox_prune deleted stands, so both are held out of
            # the counted set here and fall into the unattributed
            # shortfall below.
            counted = sum(counts.values()) - sum(
                counts.get(status, 0) for status in UNATTRIBUTED_STATUSES
            )
            row = OxpullBatch.objects.using(db_alias).get(pk=batch_pk)
            # Members whose row ox_prune deleted, plus members the reaper
            # marked LOST, plus members an operator discarded. They are
            # finished, but none of them is work that was done, and
            # failed == 0 is therefore not evidence that the run was
            # clean. total_count was frozen at seal and never moves, so
            # the shortfall against the counted rows is exactly that set.
            callbacks: dict[str, Any] = {"failed_count": failed}
            total = row.total_count
            if total is None:
                total = _sealed_without_a_total(
                    OxpullBatchMember.objects.using(db_alias).filter(batch_id=batch_pk)
                )
                callbacks["total_count"] = total
            unattributed = max(total - counted, 0)
            if row.on_complete_path:
                callbacks["callback_task_id"] = _enqueue_callback(
                    row.on_complete_path, row.on_complete_args, row.on_complete_kwargs
                )
            # An outcome nobody recorded is not a success:
            # on_success is the operator's "the run was clean" signal, so
            # it fires only when every member's outcome is on record and
            # none of them failed. A batch with no members has nothing
            # unaccounted for and nothing failed, so it still completes
            # vacuously successful.
            if failed == 0 and unattributed == 0 and row.on_success_path:
                callbacks["success_task_id"] = _enqueue_callback(
                    row.on_success_path, row.on_success_args, row.on_success_kwargs
                )
            OxpullBatch.objects.using(db_alias).filter(pk=batch_pk).update(**callbacks)
    except _MemberPending:
        return None
    return failed, unattributed


def _sealed_without_a_total(rows: QuerySet[Any]) -> int:
    """
    The total for a SEALED row that seal() did not write: its live count.

    seal() writes the status and total_count in one transaction, so this
    is a row sealed some other way, such as an UPDATE issued by hand to
    finish an abandoned build. Reading the missing total as zero would
    hide every LOST, DISCARDED or pruned row and fire on_success for a
    run that was not clean. The live count is the total seal() would have
    stored: add() claims only an OPEN row, so nothing joins a sealed one,
    and prune removes rows only once the owner is COMPLETE. progress()
    reads the same count for a row with no total.
    """
    return rows.count()


def _enqueue_callback(
    path: str, args: list[Any], kwargs: dict[str, Any], label: str = "Batch"
) -> str:
    """
    Enqueue one stored callback and return its task id.

    create() has already checked all of this, but the row outlives the
    process that wrote it and a later deploy can move the path onto
    another backend. Both checks therefore run again here, before the
    enqueue: on a backend that executes at enqueue time the side effect
    would otherwise fire and only then fail to record, rolling the
    completion back and repeating every tick.
    """
    callback = import_string(path)
    if not isinstance(callback, Task):
        raise ImproperlyConfigured(
            f"{label} callback {path!r} is not a django.tasks Task."
        )
    _require_ox_backend(callback, role="callbacks", label=label)
    _reject_unique(callback, label=label)
    return str(callback.enqueue(*args, **kwargs).id)


# The task row stores f"{func.__module__}.{func.__qualname__}", and both
# the schedule and the worker resolve the reconciler by that path, so the
# function carries its public name however it is spelled above.
_reconcile.__name__ = _reconcile.__qualname__ = "reconcile"


def _build_reconcile_task() -> Task[[], int]:
    """The batch reconciler, on a queue the default backend accepts."""
    return cast("Task[[], int]", _build_task_on_a_queue(_reconcile))


def _build_task_on_a_queue(func: Any) -> Task[..., Any]:
    """
    Wrap a function as a Task, on the queue the backend will accept.

    django.tasks validates a Task against its backend the moment the
    Task is constructed, queue name included. A module-level `@task`
    here would therefore make `import oxpull.batches` raise InvalidTask
    outright in any project whose QUEUES omits "default" -- from a
    view, from a tasks module, from anywhere -- with a message naming
    neither this package nor this module. Building it on access instead
    keeps the import side-effect-free, and picking a queue the backend
    already accepts keeps the reconciler schedulable without editing
    QUEUES for it. The schedule's own `queue_name` overrides this.
    """
    from django.tasks import (
        DEFAULT_TASK_BACKEND_ALIAS,
        DEFAULT_TASK_QUEUE_NAME,
        task_backends,
    )

    queue_name = queue_accepted_by(
        task_backends[DEFAULT_TASK_BACKEND_ALIAS],
        settings.TASKS.get(DEFAULT_TASK_BACKEND_ALIAS, {}),
    )
    if queue_name == DEFAULT_TASK_QUEUE_NAME:
        return task(func)
    return task(queue_name=queue_name)(func)


def __getattr__(name: str) -> Any:
    """Resolve the module's lazily built public names."""
    if name == "reconcile":
        return _build_reconcile_task()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
