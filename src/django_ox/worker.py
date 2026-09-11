import asyncio
import copy
import ctypes
import json
import logging
import os
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Coroutine
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from inspect import iscoroutinefunction
from threading import Barrier, BrokenBarrierError, Condition, Event, Lock, Thread
from traceback import format_exception
from typing import Any, cast

from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    Error,
    IntegrityError,
    close_old_connections,
    connections,
    router,
    transaction,
)
from django.db.models import Max, Q, QuerySet
from django.db.models.expressions import Combinable
from django.db.models.functions import Now
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

from django_ox.compat import (
    DEFAULT_TASK_BACKEND_ALIAS,
    Task,
    TaskContext,
    TaskResult,
    normalize_json,
    task_backends,
    task_finished,
    task_started,
)

from .backend import OxBackend
from .exceptions import TaskAbandoned, TaskTimeout
from .models import OxScheduleTick, OxTask
from .schedules import schedule_name_collisions, schedules_from_options
from .timeouts import (
    RECYCLE_EXIT_CODE,
    _deadline,
    task_timeouts_from_options,
)

logger = logging.getLogger("django_ox")

# Candidates fetched per claim pass on the optimistic (non SKIP LOCKED) path;
# bounds retries when racing other workers for the head of the queue.
CLAIM_BATCH_SIZE = 5

# The watchdog thread exits after this long with nothing to watch, and is
# started again by the next attempt that has a timeout.
WATCHDOG_IDLE = 1.0

# How often the drain re-checks, while recycling, whether the tasks still in
# flight are all stuck threads it must not wait for.
RECYCLE_DRAIN_POLL = 0.25

# The longest the watchdog sleeps in one go. A condition wait takes at most
# the platform's limit (about 49 days on Windows, and the end of time_t
# elsewhere); a deadline years out is waited for in steps of this.
WATCHDOG_MAX_WAIT = 3600.0


def _load_async_exc_injector() -> Callable[[int], None] | None:
    """
    The C API call that raises an exception inside another thread, or None
    where the interpreter has no such call (PyPy).

    PyThreadState_SetAsyncExc takes an exception class, not an instance,
    and the target thread raises it at its next bytecode boundary. It
    cannot reach a thread that is inside a C call, which is what the grace
    backstop is for. ctypes.pythonapi holds the GIL across the call, which
    the call requires.
    """
    try:
        set_async_exc = ctypes.pythonapi.PyThreadState_SetAsyncExc
    except (AttributeError, OSError):
        return None

    def inject(thread_id: int) -> None:
        modified = set_async_exc(
            ctypes.c_ulong(thread_id), ctypes.py_object(TaskTimeout)
        )
        if modified > 1:
            # More than one thread state matched, which the C API says must
            # be undone; it cannot happen for a live thread's own ident.
            set_async_exc(ctypes.c_ulong(thread_id), ctypes.c_void_p(None))

    return inject


_inject_async_exc = _load_async_exc_injector()

# sys.monitoring has six tool slots (PEP 669); get_tool() rejects any other id.
_MONITORING_TOOL_IDS = range(6)


def _active_tracer() -> str | None:
    """
    How the calling thread is being watched, or None if it is not.

    Coverage measurement and debuggers install a callback that the
    interpreter runs between one bytecode and the next, and those callbacks
    hold locks of their own. An exception raised inside the thread can land
    in one of them, so a thread being watched is not one this worker raises
    an exception inside.

    Two mechanisms, both of which count, and the answer names the mechanism
    rather than the tool. The trace hook is per thread, and ``sys.gettrace``
    is what that thread was started with (``threading`` installs its default
    hook as the new thread's own) or has set since; it does not say who
    installed it. ``sys.monitoring`` is a separate, process-wide registry
    that carries the tool's own name, and coverage measurement uses it in
    preference to the trace hook where the interpreter supports it, so a
    thread with no trace hook can still be watched through a registered tool
    id.

    A registered tool id with no events enabled is watching nothing:
    reserving an id runs no callback and takes no lock, so this asks about
    the events rather than the reservation. A tool that enables events on
    individual code objects and none globally is the shape this does not
    see; nothing in the standard library enumerates those without the code
    object to ask about.

    ``sys.setprofile`` is deliberately not consulted. A profile hook runs on
    call and return rather than between one bytecode and the next, sampling
    profilers used in production install one, and degrading every production
    worker's timeouts for it would cost more than it protects.
    """
    if sys.gettrace() is not None:
        return "sys.settrace"
    for tool_id in _MONITORING_TOOL_IDS:
        name = sys.monitoring.get_tool(tool_id)
        if name is not None and sys.monitoring.get_events(tool_id):
            return f"sys.monitoring ({name})"
    return None


def _flush_pending_async_exc() -> None:
    """
    Give a pending injected exception a place to land.

    An injection is delivered at the target thread's next eval-breaker
    check, which every call into a C function is. Calling this right after
    an attempt is deregistered makes the exception surface here, inside the
    caller's handler, rather than in whatever bookkeeping came next.
    """
    time.monotonic()


def _timeout_in(exc: BaseException) -> TaskTimeout | None:
    """
    The TaskTimeout this exception is, or the one it was raised while
    handling, following the chain Python's traceback would print. A cleanup
    that fails while the task unwinds from its timeout (Django's own
    atomic() exit, for one, when the delivery landed inside the driver)
    replaces the TaskTimeout with its own exception; the attempt still
    timed out. ``raise ... from None`` breaks the chain, and with it this
    classification, on purpose.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, TaskTimeout):
            return current
        seen.add(id(current))
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            current = None
        else:
            current = current.__context__
    return None


@dataclass(slots=True)
class _Watch:
    """One attempt under a timeout, as the watchdog sees it."""

    ident: int
    db_task: OxTask
    #: The attempt this watch was armed for, as the executing thread
    #: registered it. Taken at arm time because the watchdog moves the epoch
    #: on its own copy of db_task when it gives up, and the drain has to
    #: recognise the attempt the pool thread is still inside.
    attempt: tuple[Any, int]
    timeout: float
    started: float
    deadline: float
    deadline_at: datetime
    injectable: bool
    # How this thread was being watched when the attempt was armed, if it
    # was. The watchdog leaves a watched thread alone, so the grace backstop
    # is the whole enforcement for it.
    tracer: str | None = None
    fired: bool = False
    grace_at: float = 0.0


# The states an execution may write its outcome onto. RUNNING is the ordinary
# one. LOST is a row the reaper gave up on without seeing how it ended, and
# the execution that lost it is the only party who could ever say, so it keeps
# the right to write there. Every other state either belongs to somebody else
# or has already been answered.
WRITABLE_STATUSES = (OxTask.Status.RUNNING, OxTask.Status.LOST)

# Single-statement claim for PostgreSQL: the SKIP LOCKED subselect, the claim
# UPDATE, and the per-attempt bookkeeping are one round trip, atomic in
# autocommit. The equivalent multi-statement path costs 5 round trips.
#
# The claim's own timestamps come from {lease_clock}, which is
# STATEMENT_TIMESTAMP() when USE_TZ is on and a parameter carrying the worker's
# clock when it is off. That is not a style choice: _lease_now() makes the same
# switch, and renew_leases and the reaper's cutoff both go through it. Hard
# coding the server's clock here while the renewal used the worker's put two
# clocks on one column, so on a deployment where the database and the worker
# are different hosts, a worker whose clock ran behind the server renewed to a
# timestamp the reaper already read as expired and lost its task on the first
# renewal.
# run_after is the exception and is still compared against the worker's clock,
# because that is the clock the retry that wrote it used; see _ready_queryset.
# lease_epoch advances here too: the increment and the claim are one statement,
# so no execution can share an epoch with another.
POSTGRES_CLAIM_SQL = """
UPDATE "{table}" SET
    "status" = %(running)s,
    "locked_by" = %(worker_id)s,
    "locked_at" = {lease_clock},
    "lease_epoch" = "lease_epoch" + 1,
    "attempts" = "attempts" + 1,
    "started_at" = COALESCE("started_at", {lease_clock}),
    "last_attempted_at" = {lease_clock},
    "worker_ids" = "worker_ids" || %(worker_id_json)s::jsonb
WHERE "id" = (
    SELECT "id" FROM "{table}"
    WHERE "status" = %(ready)s
        AND ("run_after" IS NULL OR "run_after" <= %(now)s)
        {queue_clause}{extra_clause}
    ORDER BY "priority" DESC, "enqueued_at"
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
"""


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _lease_now() -> Combinable | datetime:
    """The clock that stamps lease timestamps.

    Database-side time is the right clock for a lease, because on PostgreSQL
    and MySQL it is one clock for every worker and for the reaper even when
    they run on different hosts. It only agrees with what the columns already
    hold when USE_TZ is on, and that is the whole of this function.

    Not on SQLite. Now() compiles there to STRFTIME(..., 'NOW'), which SQLite
    evaluates in the process that ran the statement, so there is no server
    clock to share and every worker stamps on its own whatever USE_TZ says.
    That is a reason to run SQLite on one host rather than a reason to stamp
    it differently: a shared file over a network filesystem has worse problems
    than clock drift.

    Django's Now() compiles to STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') on SQLite,
    and SQLite's 'now' is always UTC, so under USE_TZ=False it writes UTC into
    columns every other writer fills with naive local time. ox_prune and
    ox_health compare those columns against timezone.now(), which does follow
    USE_TZ, so the two clocks end up a whole UTC offset apart: a row that
    finished a second ago reads hours old, and --older-than deletes it. MySQL
    takes its session time_zone, which Django only pins when USE_TZ is on.
    PostgreSQL agrees either way, because Django sets the session timezone from
    TIME_ZONE.

    So: the database's clock where the two agree, the worker's clock where they
    do not. The second case is what every timestamp here used before the lease
    moved to database time, so it carries no behaviour that has not already
    shipped.

    Only the lease is stamped this way: locked_at, the started_at and
    last_attempted_at written beside it in the claim, and the cutoff the reaper
    judges locked_at against. Two columns sit outside it on purpose. run_after
    is one, and _ready_queryset says why. finished_at is the other: nothing
    fences on it, the reaper never reads it, and it is written by the statement
    that gives the lease up rather than by one that holds it. Every comparison
    against it here -- ox_prune's cutoff, the throughput window in stats -- is
    against timezone.now(), and django_ox.actions already stamps it from there,
    so process time is the clock it is read on. Stamping it database-side also
    cost a round trip per task, because a value the database computes has to be
    read back before the in-memory row can be trusted, and _write_outcome is on
    the path every task takes.
    """
    return Now() if settings.USE_TZ else timezone.now()


class Worker:
    """
    Claims READY tasks and executes them, at least once.

    Claiming uses a single UPDATE ... SKIP LOCKED ... RETURNING statement on
    PostgreSQL, SELECT ... FOR UPDATE SKIP LOCKED where another database
    supports it, and otherwise an optimistic compare-and-set UPDATE keyed on
    (status=READY, attempts, lease_epoch), which is atomic on every backend
    including SQLite. Every path folds the per-attempt bookkeeping (attempts,
    started_at, last_attempted_at, worker_ids) into the claim UPDATE itself,
    so a worker that dies mid-task has already consumed the attempt and
    attempts always equals len(worker_ids).

    The lease has three parts, and they only work together:

    - Every claim increments lease_epoch in the claim UPDATE, so the number
      names one execution rather than one task.
    - Every finish write carries that number in its WHERE clause, so a
      worker that was reaped off a row writes nothing instead of writing
      over whoever holds it now. It is fenced by arithmetic, not by timing,
      so no pause is long enough to defeat it.
    - While a worker is executing, it refreshes locked_at on the rows it
      holds, one statement per interval however many are in flight, so the
      reaper only reclaims work from workers that actually went quiet.

    The lease's own timestamps come from the database (Now(), or
    STATEMENT_TIMESTAMP() in the raw claim) rather than from each process,
    so a lease written on one host and judged on another is judged against
    one clock. Under USE_TZ=False they come from the worker instead, because
    the database's clock and the columns disagree there; _lease_now says why.
    run_after is the documented exception under any setting; _ready_queryset
    says why.

    When the backend's OPTIONS define SCHEDULES, every worker also
    dispatches recurring ticks alongside its polling; the unique constraint
    on OxScheduleTick makes that safe with any number of workers.
    """

    def __init__(
        self,
        *,
        backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS,
        queues: list[str] | None = None,
        concurrency: int = 1,
        poll_interval: float = 1.0,
        lock_timeout: float | None = None,
        reap_interval: float | None = None,
        renew_interval: float | None = None,
        schedule_interval: float | None = None,
        backoff_initial: float | None = None,
        backoff_max: float | None = None,
        worker_index: int | None = None,
        parent_pid: int | None = None,
        task_timeout: float | None = None,
        task_timeout_grace: float | None = None,
    ) -> None:
        backend = task_backends[backend_alias]
        if not isinstance(backend, OxBackend):
            raise ImproperlyConfigured(
                f"Backend {backend_alias!r} is {type(backend).__qualname__}, "
                "not a OxBackend."
            )
        self.backend = backend
        options = backend.options
        # Empty means the backend accepts any queue name; the worker then
        # processes all queues rather than filtering.
        self.queues: list[str] = list(queues) if queues else sorted(backend.queues)
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        # Under a supervisor, the pid to watch: a worker whose supervisor has
        # gone (it was SIGKILLed, or died on a signal it could not forward)
        # is reparented, and drains rather than run on as an orphan.
        self.parent_pid = parent_pid
        self.lock_timeout: float = (
            lock_timeout
            if lock_timeout is not None
            else float(options.get("LOCK_TIMEOUT", 300.0))
        )
        self.reap_interval: float = (
            reap_interval
            if reap_interval is not None
            else min(30.0, max(self.lock_timeout / 2, 1.0))
        )
        # A third of the timeout leaves room for two consecutive renewals to
        # be missed (a slow query, a blip, one skipped scheduling slot)
        # before the reaper is entitled to conclude anything.
        self.renew_interval: float = (
            renew_interval
            if renew_interval is not None
            else max(self.lock_timeout / 3, 0.1)
        )
        # Cron granularity is a minute; checking around once a second keeps
        # dispatch latency low for one cheap aggregate query per pass.
        self.schedule_interval: float = (
            schedule_interval
            if schedule_interval is not None
            else max(1.0, min(self.poll_interval, 30.0))
        )
        self.schedules = schedules_from_options(options, backend_alias)
        collisions = schedule_name_collisions(backend_alias)
        if collisions:
            name, other_alias = collisions[0]
            raise ImproperlyConfigured(
                f"Schedule name {name!r} is defined on both the "
                f"{backend_alias!r} and {other_alias!r} backends; schedule "
                "names must be unique across backends."
            )
        self.backoff_initial: float = (
            backoff_initial
            if backoff_initial is not None
            else float(options.get("BACKOFF_INITIAL", 5.0))
        )
        self.backoff_max: float = (
            backoff_max
            if backoff_max is not None
            else float(options.get("BACKOFF_MAX", 600.0))
        )
        # TASK_TIMEOUT, the per-queue TASK_TIMEOUTS and TASK_TIMEOUT_GRACE,
        # validated the same way the system check validates them. The
        # keywords override the default and the grace; per-queue values
        # still come from the options.
        if task_timeout is not None:
            options = {**options, "TASK_TIMEOUT": task_timeout}
        if task_timeout_grace is not None:
            options = {**options, "TASK_TIMEOUT_GRACE": task_timeout_grace}
        self.timeouts = task_timeouts_from_options(options, backend.queues)
        # The index is the slot number under `ox_worker --processes N`, so a
        # log line or a worker_ids entry names the slot as well as the pid.
        # It rides on the id rather than replacing the random part; a
        # restarted slot is a new worker and must not inherit the old lease.
        suffix = "" if worker_index is None else f"-{worker_index}"
        self.worker_id = (
            f"{socket.gethostname()[:40]}-{os.getpid()}-{get_random_string(8)}{suffix}"
        )
        self._stop = Event()
        self._db_alias = router.db_for_write(OxTask)
        # (pk, lease_epoch) of every execution running right now, added and
        # removed by execute(). Renewal reads it rather than renewing
        # everything stamped with this worker_id, so a row whose execution
        # is gone stops being renewed and the reaper can still recover it.
        self._in_flight: set[tuple[Any, int]] = set()
        self._in_flight_lock = Lock()
        # Thread ident -> the attempt running on that thread under a
        # timeout. The lock is the injection lock: the watchdog injects only
        # while it holds the lock and the entry is present, and the runner
        # removes the entry under the same lock, so an injection can never
        # be aimed at a thread that has already left the task.
        #
        # Pool threads take the raw lock, never the Condition. An injected
        # exception lands at the next bytecode, and Condition.__enter__ is
        # Python: an exception delivered inside it, after the acquire and
        # before the with statement has installed its exit, leaves the lock
        # held forever. The C lock closes most of that gap, but from Python
        # 3.14 the with statement itself acquires one instruction before
        # its cleanup is installed, so the injectable path in _disarm takes
        # the lock through an explicit try/finally built around a single
        # indivisible acquire instead.
        self._watches: dict[int, _Watch] = {}
        self._watch_lock = Lock()
        self._watch_cv = Condition(self._watch_lock)
        self._watchdog: Thread | None = None
        # Pool threads the backstop gave up on, as ident -> the attempt it was
        # running when we gave up. The drain does not wait for those; they die
        # with the process. The attempt is part of the key because a pool
        # thread is reused: its ident stays alive and idle after a stuck task
        # finally returns, and again under the next task, so an ident alone
        # cannot say whether the thread is still inside the work we abandoned.
        self._stuck: dict[int, tuple[Any, int]] = {}
        # What each pool thread is executing right now, ident -> attempt.
        # Present only for the duration of an attempt.
        self._running_on: dict[int, tuple[Any, int]] = {}
        self._recycling = False
        # Set once the worker has said that a tracing tool is holding
        # timeouts to the backstop, so it is said once and not per attempt.
        self._backstop_only_notice = False
        # Said once per worker, not once per claim.
        self._claim_filter_notice = False
        self._backstop_only_lock = Lock()
        if self.timeouts.enabled and _inject_async_exc is None:
            self._backstop_only_notice = True
            logger.warning(
                "Worker %s cannot raise TaskTimeout inside a running task on "
                "this interpreter, so the %gs grace backstop is the whole "
                "enforcement. A task that returns before the backstop fires "
                "is recorded as whatever it did, however long it ran; one "
                "still running when it fires is recorded as failed and "
                "recycles this worker with exit code %d",
                self.worker_id,
                self.timeouts.grace,
                RECYCLE_EXIT_CODE,
                extra={
                    "event": "timeouts_backstop_only",
                    "worker_id": self.worker_id,
                    "reason": "interpreter",
                    "grace_s": self.timeouts.grace,
                },
            )

    # -- logging -----------------------------------------------------------

    def _log_extra(
        self, event: str, db_task: OxTask, **extra: object
    ) -> dict[str, object]:
        """
        Stable extra keys for JSON log handlers. Every task lifecycle
        record carries at least these; the keys are documented on the
        Monitoring page and must not change casually.
        """
        return {
            "event": event,
            "task_id": str(db_task.id),
            "task_path": db_task.task_path,
            "queue": db_task.queue_name,
            "attempt": db_task.attempts,
            "worker_id": self.worker_id,
            **extra,
        }

    # -- claiming ----------------------------------------------------------

    def claim_filter_q(self) -> Q | None:
        """
        An extra condition on what this worker may claim, or None.

        Applied to the candidate queryset on the two claim paths that build
        one. claim_filter_sql() is the same condition for the path that does
        not. A subclass that implements one and not the other narrows two of
        the three supported databases and silently does nothing on the third.
        """
        return None

    def claim_filter_sql(self) -> tuple[str, dict[str, Any]]:
        """
        claim_filter_q() as a WHERE fragment and its parameters, for the
        PostgreSQL claim, which builds its own SQL.

        The fragment is appended to the candidate select's conditions as a
        bare ``AND ...`` conjunct against the task table, so it carries its
        own leading separator. That placement is the contract; the statement
        it lands in is not. Returning ("", {}) leaves the emitted SQL
        unchanged.
        """
        return "", {}

    def _postgresql_honours_the_claim_filter(self) -> bool:
        """
        May the single-statement PostgreSQL claim be used?

        Only when this worker's claim filter reaches it. The fast path builds
        its own SQL, so it reads `claim_filter_sql()` and knows nothing about
        `claim_filter_q()`. A subclass that overrides the queryset hook alone
        therefore narrowed SQLite and MySQL and claimed the very rows it meant
        to exclude on PostgreSQL, with nothing raised and nothing logged: the
        one shape this project treats as the most serious kind of defect, a
        mechanism that holds on two databases and silently does nothing on the
        third.

        Falling back to the `SELECT ... FOR UPDATE SKIP LOCKED` path rather
        than refusing to start. Both hooks are a published stability surface,
        so a subclass that works today on two databases has to keep working;
        it gives up one statement per claim on PostgreSQL and gets the
        exclusion it asked for. A subclass that implements both keeps the fast
        path.
        """
        if self.claim_filter_q() is None:
            return True
        fragment, _ = self.claim_filter_sql()
        if fragment:
            return True
        if not self._claim_filter_notice:
            self._claim_filter_notice = True
            logger.warning(
                "%s overrides claim_filter_q() but not claim_filter_sql(), so "
                "the single-statement PostgreSQL claim cannot apply it. Using "
                "the SELECT ... FOR UPDATE SKIP LOCKED path instead, which "
                "costs one extra statement per claim. Implement "
                "claim_filter_sql() to keep the faster path.",
                type(self).__name__,
                extra={
                    "event": "claim_filter_sql_missing",
                    "worker_id": self.worker_id,
                    "worker_class": type(self).__name__,
                },
            )
        return False

    def _ready_queryset(self) -> QuerySet[OxTask]:
        # run_after stays on process time, on both sides of this comparison
        # and in the retry that writes it. Database-side time is the right
        # clock for the lease, but not here: on SQLite, Now() renders
        # milliseconds and Now() + interval renders microseconds, and text
        # comparison then reads "12:00:00.604000" as later than
        # "12:00:00.604", so a retry written with no backoff is not eligible
        # for up to a millisecond (measured: 86 of 200). Skew in run_after
        # only makes a retry early or late; mixing the two clocks across a
        # comparison would be worse than either clock consistently.
        queryset = (
            OxTask.objects.using(self._db_alias)
            .filter(status=OxTask.Status.READY)
            .filter(Q(run_after__isnull=True) | Q(run_after__lte=timezone.now()))
        )
        if self.queues:
            queryset = queryset.filter(queue_name__in=self.queues)
        extra = self.claim_filter_q()
        if extra is not None:
            queryset = queryset.filter(extra)
        return queryset.order_by("-priority", "enqueued_at")

    def _claim_fields(self, candidate: OxTask) -> dict[str, Any]:
        # lease_epoch is a literal rather than F("lease_epoch") + 1 so the
        # claimed value is known here without reading it back; the callers
        # both compare-and-set on the old value, which is what makes the
        # literal exact.
        return {
            "status": OxTask.Status.RUNNING,
            "locked_by": self.worker_id,
            "locked_at": _lease_now(),
            "lease_epoch": candidate.lease_epoch + 1,
            "attempts": candidate.attempts + 1,
            "started_at": candidate.started_at or _lease_now(),
            "last_attempted_at": _lease_now(),
            "worker_ids": [*candidate.worker_ids, self.worker_id],
        }

    def _claim_one_postgresql(self, run_after_cutoff: datetime) -> OxTask | None:
        # run_after_cutoff is the worker's own clock, and it is the only
        # value in this statement that is: everything the claim writes is
        # STATEMENT_TIMESTAMP(). See _ready_queryset for why run_after is
        # the exception.
        queue_clause = 'AND "queue_name" = ANY(%(queues)s)' if self.queues else ""
        extra_clause, extra_params = self.claim_filter_sql()
        # One clock stamps the lease, whichever it is. _lease_now() decides
        # which; this statement has to agree with it or the renewal and the
        # reaper are judging a column a different clock wrote.
        lease_clock = "STATEMENT_TIMESTAMP()" if settings.USE_TZ else "%(lease_now)s"
        sql = POSTGRES_CLAIM_SQL.format(
            table=OxTask._meta.db_table,
            queue_clause=queue_clause,
            extra_clause=extra_clause,
            lease_clock=lease_clock,
        )
        # db_manager, not objects.raw: a RawQuerySet routes through
        # db_for_read, and unlike select_for_update() it does not mark itself
        # for write, so Django has no way to know this SQL is an UPDATE. With a
        # read replica in the router the claim either raises on a read-only
        # standby or lands off-primary, and neither is a claim.
        rows = OxTask.objects.db_manager(self._db_alias).raw(
            sql,
            {
                "running": OxTask.Status.RUNNING,
                "ready": OxTask.Status.READY,
                "worker_id": self.worker_id,
                "worker_id_json": json.dumps([self.worker_id]),
                "now": run_after_cutoff,
                "lease_now": _lease_now() if not settings.USE_TZ else None,
                "queues": self.queues,
                **extra_params,
            },
        )
        return next(iter(rows), None)

    def claim_one(self) -> OxTask | None:
        """Atomically claim the next runnable task, or return None."""
        db_task = self._claim_one()
        if db_task is not None:
            logger.debug(
                "Claimed task id=%s path=%s (attempt %d/%d)",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                extra=self._log_extra("task_claimed", db_task),
            )
        return db_task

    def _claim_one(self) -> OxTask | None:
        connection = connections[self._db_alias]
        skip_locked = connection.features.has_select_for_update_skip_locked
        if (
            connection.vendor == "postgresql"
            and skip_locked
            and self._postgresql_honours_the_claim_filter()
        ):
            return self._claim_one_postgresql(timezone.now())
        if skip_locked:
            with transaction.atomic(using=self._db_alias):
                candidate = (
                    self._ready_queryset().select_for_update(skip_locked=True).first()
                )
                if candidate is None:
                    return None
                OxTask.objects.using(self._db_alias).filter(pk=candidate.pk).update(
                    **self._claim_fields(candidate)
                )
                # The claim wrote database-side timestamps, which cannot be
                # mirrored onto the instance from here; re-read the row so
                # the instance and the row agree. The PostgreSQL path gets
                # this for free from RETURNING *.
                candidate.refresh_from_db()
                return candidate
        # Optimistic compare-and-set. attempts and lease_epoch double as
        # version counters: if another worker claimed (and possibly
        # requeued) the row between the fetch and this UPDATE, both have
        # moved and the literal bookkeeping values below cannot stomp its
        # writes.
        for candidate in self._ready_queryset()[:CLAIM_BATCH_SIZE]:
            granted_epoch = candidate.lease_epoch + 1
            claimed = (
                OxTask.objects.using(self._db_alias)
                .filter(
                    pk=candidate.pk,
                    status=OxTask.Status.READY,
                    attempts=candidate.attempts,
                    lease_epoch=candidate.lease_epoch,
                )
                .update(**self._claim_fields(candidate))
            )
            if claimed:
                held = self._reload_claimed(candidate.pk, granted_epoch)
                if held is None:
                    continue
                return held
        return None

    def _reload_claimed(self, pk: uuid.UUID, granted_epoch: int) -> OxTask | None:
        """
        Re-read a freshly claimed row, pinned to the epoch it was granted.

        The claiming UPDATE autocommits, so this is a second statement with a
        gap in front of it, and the row can move inside that gap: pause for
        longer than LOCK_TIMEOUT and the reaper requeues the row and another
        worker claims it. Reading the row back unconditionally would load that
        worker's lease onto this instance, and every later fence check would
        compare against an epoch this execution was never granted, which is
        what lets a straggler's outcome overwrite the real holder's.

        Pinning the read to the granted epoch turns that into an honest miss.
        None means the lease was lost inside the gap, so there is nothing here
        to execute. The PostgreSQL path never has the gap, because RETURNING *
        is the same statement, and the SKIP LOCKED path holds a row lock
        across it.
        """
        return (
            OxTask.objects.using(self._db_alias)
            .filter(pk=pk, lease_epoch=granted_epoch)
            .first()
        )

    # -- lease renewal -----------------------------------------------------

    def renew_leases(self) -> int:
        """
        Refresh the lock timestamp on this worker's in-flight rows.

        One UPDATE whatever the concurrency, and it can only ever touch
        rows this worker still holds: the pk list is the executions running
        right now, and locked_by and status are checked in the same
        statement, so a row the reaper already took away or another worker
        already claimed is not renewed by us.

        This is the whole of the worker's side of the lease. It is why the
        reaper reclaims work from workers that stopped rather than from
        tasks that are merely slow, and it is deliberately blind to what
        the task itself is doing: a wedged task on a live worker keeps its
        lease, and recovering that is an operator's job, not the reaper's.
        """
        with self._in_flight_lock:
            pks = {pk for pk, _ in self._in_flight}
        if not pks:
            return 0
        return (
            OxTask.objects.using(self._db_alias)
            .filter(
                pk__in=pks,
                status=OxTask.Status.RUNNING,
                locked_by=self.worker_id,
            )
            .update(locked_at=_lease_now())
        )

    def _renewal_loop(self, stop: Event) -> None:
        """Renew until stopped. Runs on its own thread, and its own connection."""
        try:
            while not stop.wait(self.renew_interval):
                try:
                    self.renew_leases()
                except Exception:
                    # A missed renewal is survivable by design: the interval
                    # is a third of the timeout. Drop the connection so the
                    # next tick reconnects, and keep going, because giving
                    # up here would silently expire every live lease.
                    #
                    # Every exception, not a chosen class. This caught
                    # DatabaseError, and `django.db.InterfaceError` does not
                    # inherit from it - it sits beside it under
                    # `django.db.Error` - so the one failure most likely to
                    # arrive here, a connection dropped underneath the
                    # thread, escaped and killed it. Nothing restarts this
                    # thread and nothing checks it is alive, so every
                    # in-flight lease then aged out and the reaper handed
                    # every running task to another worker. The consequence
                    # of guessing wrong is severe and silent, which is
                    # exactly when a guess should not be made.
                    logger.warning(
                        "Lease renewal failed for worker %s; retrying in %.1fs",
                        self.worker_id,
                        self.renew_interval,
                        exc_info=True,
                        extra={
                            "event": "lease_renew_failed",
                            "worker_id": self.worker_id,
                        },
                    )
                    connections.close_all()
        finally:
            connections.close_all()

    # -- execution ---------------------------------------------------------

    def _write_outcome(
        self,
        db_task: OxTask,
        *,
        status: OxTask.Status,
        duration_ms: int,
        **fields: Any,
    ) -> bool:
        """
        Write this attempt's outcome, but only while it still holds the lease.

        The fence is the lease epoch the claim handed out. It identifies
        this execution rather than this task, so a worker that stalled long
        enough for the reaper to requeue the row matches nothing and its
        UPDATE touches zero rows instead of overwriting whatever happened
        next. Handovers are the only thing that moves the number, and every
        handover that puts the row back on the queue moves it.

        One deliberately does not: the reaper's exhausted branch writes LOST
        and leaves the epoch alone, and says why. LOST is an open question
        rather than a settled outcome, and the holder of that epoch is the
        only party who could still answer it, so it keeps the right to write
        over the top. Nothing can re-claim such a row, because the claim
        filters on READY, so no second execution can share the epoch.

        WRITABLE_STATUSES is a second lock on the same door. The epoch alone
        is sufficient given that every handover bumps it, and this condition
        costs one enum comparison to stop depending on that being true
        forever: whatever the epoch says, an outcome may only be written
        onto a row that is still running, or onto one the reaper marked LOST.

        LOST is in the set on purpose. It means nobody saw the outcome, and
        the holder of that epoch is the only party who could ever supply
        one, so this write must still land there. It cannot land on anybody
        else's row, because no other execution ever holds that number.

        Every field must be a value, never a database-side expression. The
        mirror below is what makes the log line and the TaskResult describe
        the row, and it can only copy what the caller already holds; an
        expression would have to be read back afterwards, which is a second
        round trip on the path every task takes. _lease_now says which
        columns are worth that and which are not.

        Returns True when the write landed, having mirrored the stored
        values onto the in-memory instance so the log record and the
        TaskResult that follow describe what is actually in the row.
        Returns False when the lease was gone, having logged it; the caller
        must then abandon the rest of its branch, and in particular must not
        send task_finished for an outcome it does not own.
        """
        updated = (
            OxTask.objects.using(self._db_alias)
            .filter(
                pk=db_task.pk,
                lease_epoch=db_task.lease_epoch,
                status__in=WRITABLE_STATUSES,
            )
            .update(status=status, **fields)
        )
        if not updated:
            logger.warning(
                "Task id=%s path=%s lost its lease on attempt %d/%d; dropping "
                "the %s write, this row belongs to another worker now",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                status,
                extra=self._log_extra(
                    "task_lease_lost",
                    db_task,
                    duration_ms=duration_ms,
                    dropped_status=str(status),
                ),
            )
            return False
        db_task.status = status
        for name, value in fields.items():
            setattr(db_task, name, value)
        return True

    def execute(self, db_task: OxTask, *, inline: bool = False) -> None:
        """
        Run a claimed (RUNNING, locked) task to a terminal or retry state.

        `inline` says this is running on the caller's own thread rather than
        on the pool, which decides what happens to an exception that was
        aimed at the process rather than at the task. See _run_attempt.

        Per-attempt bookkeeping (started_at, last_attempted_at, worker_ids)
        was already written by the claim UPDATE. The (pk, lease_epoch) pair
        joins the renewal set for the duration, so this execution's lease is
        kept alive while it runs and stops being kept alive the moment it
        is not.
        """
        held = (db_task.pk, db_task.lease_epoch)
        ident = threading.get_ident()
        with self._in_flight_lock:
            self._in_flight.add(held)
            self._running_on[ident] = held
        try:
            self._run_attempt(db_task, inline=inline)
        finally:
            with self._in_flight_lock:
                self._in_flight.discard(held)
                if self._running_on.get(ident) == held:
                    del self._running_on[ident]

    def _run_attempt(self, db_task: OxTask, *, inline: bool = False) -> None:
        from .results import task_from_db, task_result_from_db

        started = time.monotonic()
        try:
            task = task_from_db(db_task)
            task_result = task_result_from_db(db_task, task=task)
            # send_robust: these are an observability surface, and a
            # receiver's exception is not the task's fault. task_started fires
            # before the function is reached and inside the attempt's own
            # except clause, so a raising receiver spent an attempt and left
            # the task retried without ever running it.
            task_started.send_robust(sender=type(self.backend), task_result=task_result)
            logger.debug(
                "Starting task id=%s path=%s (attempt %d/%d)",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                extra=self._log_extra("task_started", db_task),
            )
            timeout = self.timeouts.for_queue(db_task.queue_name)
            if timeout is None:
                # No timeout on this queue: the call is the one the worker
                # made before timeouts existed, frame for frame, so the
                # stored traceback of an ordinary failure is unchanged.
                if task.takes_context:
                    raw_return_value = task.call(
                        TaskContext(task_result=task_result),
                        *db_task.args,
                        **db_task.kwargs,
                    )
                else:
                    raw_return_value = task.call(*db_task.args, **db_task.kwargs)
            else:
                raw_return_value = self._call_task(task, db_task, task_result, timeout)
            return_value = normalize_json(raw_return_value)
        except TaskTimeout as exc:
            duration_ms = _elapsed_ms(started)
            logger.warning(
                "Task id=%s path=%s ran past its %ss timeout on attempt %d/%d; "
                "recording the attempt as failed",
                db_task.id,
                db_task.task_path,
                "?" if exc.timeout is None else f"{exc.timeout:g}",
                db_task.attempts,
                db_task.max_attempts,
                extra=self._log_extra(
                    "task_timed_out",
                    db_task,
                    duration_ms=duration_ms,
                    timeout_s=exc.timeout,
                ),
            )
            self._discard_connections()
            self._handle_failure(db_task, exc, duration_ms)
        except (KeyboardInterrupt, SystemExit) as exc:
            # Aimed at the process, not at this task. On the pool it is
            # recorded as a failed attempt and kept there deliberately: one
            # task calling sys.exit() must not be able to stop a fleet, and
            # raising out of a pool thread would end only that thread anyway.
            #
            # Inline is the opposite. run_once() is called from somebody
            # else's process, and swallowing their Ctrl-C into a task failure
            # takes an interrupt they aimed at their own program and files it
            # against the work.
            if inline:
                raise
            self._handle_failure(db_task, exc, _elapsed_ms(started))
        except BaseException as exc:
            self._handle_failure(db_task, exc, _elapsed_ms(started))
        else:
            duration_ms = _elapsed_ms(started)
            if not self._write_outcome(
                db_task,
                status=OxTask.Status.SUCCESSFUL,
                duration_ms=duration_ms,
                return_value=return_value,
                # The errors this execution started with, which is what the
                # row holds unless the reaper wrote its lost-lease note in
                # the meantime. That note says the outcome was never
                # observed, and this write is the observation, so it goes.
                # Leaving it would hand every error reporter reading
                # result.errors an exception nobody raised, on a task that
                # succeeded. _handle_failure rebuilds errors from the same
                # list, so both resolutions leave the same kind of record.
                errors=db_task.errors,
                # Process time, not the lease clock; _lease_now says why.
                finished_at=timezone.now(),
                locked_by=None,
                locked_at=None,
            ):
                return
            logger.info(
                "Task id=%s path=%s succeeded in %dms",
                db_task.id,
                db_task.task_path,
                duration_ms,
                extra=self._log_extra(
                    "task_succeeded", db_task, duration_ms=duration_ms
                ),
            )
            task_finished.send_robust(
                sender=type(self.backend),
                task_result=task_result_from_db(db_task, task=task),
            )

    # -- timeouts ----------------------------------------------------------

    def _call_task(
        self,
        task: "Task[..., Any]",
        db_task: OxTask,
        task_result: "TaskResult[..., Any]",
        timeout: float,
    ) -> Any:
        """
        Call the task function under a timeout and return what it returned.

        The attempt is registered with the watchdog for the duration of the
        call, and the attempt's deadline is published for deadline() and
        remaining(). A queue with no timeout never comes here: _run_attempt
        calls the task directly, as it did before timeouts existed.

        A sync task is interrupted by TaskTimeout raised on this thread, at
        the next bytecode after the deadline. The exception can therefore
        surface anywhere between registration and deregistration, including
        inside the deregistration itself. One that lands in the task is the
        timeout. One that lands in the deregistration arrived after the
        task had already returned or raised, so it is absorbed there, the
        deregistration is finished, and the task's own outcome stands; the
        flush in _disarm is what makes that delivery happen there rather
        than in the bookkeeping that follows. Past the deregistration
        nothing is pending, because the watchdog injects only while the
        entry is present and the entry is gone. An async task is cancelled
        inside its event loop instead.

        A thread that a coverage tool or debugger is watching is left
        alone. The attempt is registered for the grace backstop and nothing
        is raised inside it, so a task that returns first is recorded as
        whatever it did, however long it ran, and one still running when the
        backstop fires is recorded as failed and recycles the worker.
        """

        def invoke() -> Any:
            if task.takes_context:
                return task.call(
                    TaskContext(task_result=task_result),
                    *db_task.args,
                    **db_task.kwargs,
                )
            return task.call(*db_task.args, **db_task.kwargs)

        if iscoroutinefunction(task.func):
            return self._call_async(task, db_task, task_result, timeout)

        ident = threading.get_ident()
        token = _deadline.set(None)
        try:
            try:
                # Registered inside the try, so that a delivery landing
                # between the registration and the call (the watchdog may
                # inject the moment _arm releases the lock) unwinds through
                # the deregistration below like any other.
                watch = self._arm(ident, db_task, timeout, injectable=True)
                _deadline.set(watch.deadline_at)
                return invoke()
            finally:
                # The absorbing try lives in this frame on purpose: the
                # first place a pending delivery can land after invoke()
                # returns is the entry of _disarm, which is inside it.
                try:
                    self._disarm(ident)
                except TaskTimeout:
                    self._disarm(ident)
        except BaseException as exc:
            timed_out = _timeout_in(exc)
            if timed_out is None:
                raise
            self._describe_timeout(timed_out, timeout)
            if timed_out is exc:
                raise
            # Something raised while the task unwound from its timeout, and
            # that is what reached here. The attempt timed out; record it
            # as one, with the whole chain in the traceback.
            raise TaskTimeout(
                self._timeout_message(timeout, unwound=type(exc).__qualname__),
                timeout=timeout,
            ) from exc
        finally:
            _deadline.reset(token)

    def _call_async(
        self,
        task: "Task[..., Any]",
        db_task: OxTask,
        task_result: "TaskResult[..., Any]",
        timeout: float,
    ) -> Any:
        """
        Run a coroutine task under asyncio's own timeout.

        The coroutine is cancelled at the deadline, sees CancelledError at
        the await it was on like any cancelled coroutine, and the
        cancellation becomes a TaskTimeout out here, with the cancellation
        as its cause so the record shows where the task was. Nothing is
        injected: asgiref runs the loop on a thread of its own, and the
        loop already has a cooperative way to stop. The watchdog still
        registers the attempt, for the grace backstop only.
        """

        async def run() -> Any:
            if task.takes_context:
                awaitable = task.func(
                    TaskContext(task_result=task_result),
                    *db_task.args,
                    **db_task.kwargs,
                )
            else:
                awaitable = task.func(*db_task.args, **db_task.kwargs)
            coroutine = cast("Coroutine[Any, Any, Any]", awaitable)
            try:
                async with asyncio.timeout(timeout) as scope:
                    return await coroutine
            except TimeoutError as exc:
                if not scope.expired():
                    raise
                raise TaskTimeout(
                    self._timeout_message(
                        timeout, delivered="its coroutine was cancelled"
                    ),
                    timeout=timeout,
                ) from (exc.__cause__ or exc)

        ident = threading.get_ident()
        watch = self._arm(ident, db_task, timeout, injectable=False)
        token = _deadline.set(watch.deadline_at)
        try:
            return async_to_sync(run)()
        finally:
            self._disarm(ident)
            _deadline.reset(token)

    def _timeout_message(
        self,
        timeout: float,
        *,
        delivered: str = "TaskTimeout was raised inside it",
        unwound: str | None = None,
    ) -> str:
        then = (
            " and this attempt is recorded as failed"
            if unwound is None
            else f", {unwound} was raised while it unwound, and this attempt is "
            "recorded as failed"
        )
        return (
            f"Task ran past the {timeout:g}s timeout on worker "
            f"{self.worker_id!r}; {delivered}{then}."
        )

    def _describe_timeout(self, exc: TaskTimeout, timeout: float) -> None:
        """
        Fill in an injected TaskTimeout. It was raised by class, so it
        carries no message and no timeout; one the task raised itself
        keeps whatever it said.
        """
        if exc.timeout is None:
            exc.timeout = timeout
        if not any(exc.args):
            exc.args = (self._timeout_message(timeout),)

    def _discard_connections(self) -> None:
        """
        Drop every database connection this thread holds, whatever state
        it is in, so that the outcome write opens a fresh one.

        A TaskTimeout is delivered at the next line of Python, and a
        driver that runs a statement through Python (psycopg does) can
        take it after the query was sent and before its result was read;
        the connection then has a command in flight and refuses the next
        one. A delivery inside atomic()'s entry or exit leaves Django's
        wrapper inside a transaction no exit will ever close, and that
        state survives close(): Django keeps a connection closed inside a
        transaction so that the next query fails loudly. Reset the
        transaction state first, so close() forgets the object. The lease
        epoch is unchanged, so the outcome write is the same write on
        either connection.
        """
        for conn in connections.all(initialized_only=True):
            conn.in_atomic_block = False
            conn.savepoint_ids = []
            conn.atomic_blocks = []
            conn.needs_rollback = False
            conn.closed_in_transaction = False
            # close() drops its reference to the driver connection even
            # when closing it raises; a connection broken this way may.
            with suppress(Error):
                conn.close()

    def _note_backstop_only(self, tracer: str) -> None:
        """
        Say once, not once per attempt, that a tracing tool is holding this
        worker's timeouts to the grace backstop.
        """
        with self._backstop_only_lock:
            if self._backstop_only_notice:
                return
            self._backstop_only_notice = True
        logger.warning(
            "Worker %s runs its tasks under %s, so TaskTimeout is not raised "
            "inside a running sync task (an async task is still cancelled at "
            "its deadline) and the %gs grace backstop is the whole "
            "enforcement. A task that returns before the backstop fires is "
            "recorded as whatever it did, however long it ran; one still "
            "running when it fires is recorded as failed and recycles this "
            "worker with exit code %d",
            self.worker_id,
            tracer,
            self.timeouts.grace,
            RECYCLE_EXIT_CODE,
            extra={
                "event": "timeouts_backstop_only",
                "worker_id": self.worker_id,
                "reason": "tracing_tool",
                "tracer": tracer,
                "grace_s": self.timeouts.grace,
            },
        )

    def _arm(
        self, ident: int, db_task: OxTask, timeout: float, *, injectable: bool
    ) -> _Watch:
        # Asked here, on the thread the exception would be raised inside,
        # and asked for every attempt: the trace hook is per thread, so the
        # watchdog cannot answer this from its own, and a tool can be
        # installed long after the worker started. A tool that starts
        # watching this thread after the attempt is armed is not seen until
        # the next attempt; there is no way to read another thread's hook,
        # so the watchdog cannot re-ask at the deadline.
        tracer = (
            _active_tracer() if injectable and _inject_async_exc is not None else None
        )
        if tracer is not None:
            self._note_backstop_only(tracer)
        now = time.monotonic()
        watch = _Watch(
            ident=ident,
            # The watchdog writes the stuck record onto its own copy, so the
            # epoch it moves is never mirrored onto the instance the stuck
            # thread still holds; that thread's own write stays fenced.
            db_task=copy.copy(db_task),
            attempt=(db_task.pk, db_task.lease_epoch),
            timeout=timeout,
            started=now,
            deadline=now + timeout,
            deadline_at=timezone.now() + timedelta(seconds=timeout),
            injectable=injectable and tracer is None,
            tracer=tracer,
        )
        with self._watch_lock:
            self._watches[ident] = watch
            if self._watchdog is None or not self._watchdog.is_alive():
                self._watchdog = Thread(
                    target=self._watchdog_loop, name="ox-watchdog", daemon=True
                )
                self._watchdog.start()
            self._watch_cv.notify_all()
        return watch

    def _disarm(self, ident: int) -> None:
        # This runs while an injection can still be pending, and on Python
        # 3.14 a with statement acquires the lock one instruction before
        # the block's cleanup covers it, so a delivery attributed to the
        # acquiring call would leave the lock held with nobody to release
        # it. list(map(...)) makes the acquire and its record one
        # indivisible instruction: `held` is non-empty exactly when this
        # thread took the lock, whatever instruction the delivery hits,
        # and the finally is installed before any of it runs.
        held: list[bool] = []
        try:
            held.extend(map(self._watch_lock.acquire, (True,)))
            if self._watches.pop(ident, None) is not None:
                # The watchdog may be sleeping out this attempt's grace;
                # wake it so it recomputes from what is left.
                self._watch_cv.notify_all()
        finally:
            if held:
                self._watch_lock.release()
        _flush_pending_async_exc()

    def _watchdog_loop(self) -> None:
        """
        Fire deadlines and grace backstops. One thread for every attempt
        this worker has under a timeout; it exits when idle and is started
        again by the next attempt that needs it.
        """
        try:
            while True:
                with self._watch_lock:
                    if not self._watches:
                        self._watch_cv.wait(WATCHDOG_IDLE)
                        if not self._watches:
                            self._watchdog = None
                            return
                    due = min(
                        watch.grace_at if watch.fired else watch.deadline
                        for watch in self._watches.values()
                    )
                    remaining = due - time.monotonic()
                    if remaining > 0:
                        self._watch_cv.wait(min(remaining, WATCHDOG_MAX_WAIT))
                    stuck = self._fire_due()
                for watch in stuck:
                    try:
                        self._handle_stuck(watch)
                    except Exception:
                        # This thread is the whole of the timeout backstop and
                        # nothing restarts it mid-attempt: an exception here
                        # used to kill it, leaving every attempt already armed
                        # with no deadline and no grace, and the worker with no
                        # record of why. _fire_due has already taken this watch
                        # out of the table, so the loop carries on with the
                        # others rather than retrying a watch whose grace has
                        # passed.
                        logger.exception(
                            "Worker %s could not record a stuck attempt; the "
                            "timeout backstop continues for the others",
                            self.worker_id,
                            extra={
                                "event": "watchdog_error",
                                "worker_id": self.worker_id,
                            },
                        )
        finally:
            connections.close_all()

    def _fire_due(self) -> list[_Watch]:
        """
        Under the watch lock: inject into every attempt past its deadline,
        and take out of the table every attempt past its grace. Returns the
        latter; the caller records them without holding the lock.
        """
        now = time.monotonic()
        stuck: list[_Watch] = []
        for ident, watch in list(self._watches.items()):
            if not watch.fired and now >= watch.deadline:
                watch.fired = True
                watch.grace_at = now + self.timeouts.grace
                if watch.injectable and _inject_async_exc is not None:
                    _inject_async_exc(ident)
            elif watch.fired and now >= watch.grace_at:
                del self._watches[ident]
                stuck.append(watch)
        return stuck

    def _handle_stuck(self, watch: _Watch) -> None:
        """
        The backstop. The thread did not stop within the grace. The usual
        reason is a call that never returns to Python, where TaskTimeout
        cannot land (a socket with no timeout, a sleep, a lock), but a task
        that caught the exception and kept running looks the same from
        here, and the record says only what was seen. Record the attempt
        as failed and move the lease epoch so nothing the thread writes
        later lands, stop renewing its lease, and recycle this worker so
        the thread dies with the process. Runs on the watchdog thread, on
        its own connection.
        """
        db_task = watch.db_task
        duration_ms = _elapsed_ms(watch.started)
        with self._in_flight_lock:
            self._in_flight.discard((db_task.pk, db_task.lease_epoch))
        logger.error(
            "Task id=%s path=%s did not stop %gs after its %gs timeout on "
            "attempt %d/%d; recording the attempt as failed and recycling "
            "worker %s",
            db_task.id,
            db_task.task_path,
            self.timeouts.grace,
            watch.timeout,
            db_task.attempts,
            db_task.max_attempts,
            self.worker_id,
            extra=self._log_extra(
                "task_stuck",
                db_task,
                duration_ms=duration_ms,
                timeout_s=watch.timeout,
                grace_s=self.timeouts.grace,
            ),
        )
        if watch.tracer is not None:
            delivered = (
                f"nothing was raised inside it, because this worker's threads "
                f"are watched by {watch.tracer}"
            )
            usually = ""
        elif not watch.injectable:
            delivered = "its coroutine was cancelled at the deadline"
            usually = (
                " A thread that does not stop is usually in a call that never "
                "returns to Python (a socket with no timeout, time.sleep(), a "
                "lock, a long database statement), or the task caught the "
                "cancellation and kept running."
            )
        elif _inject_async_exc is None:
            delivered = (
                "nothing was raised inside it, because this interpreter "
                "cannot raise TaskTimeout inside a running task"
            )
            usually = ""
        else:
            delivered = "TaskTimeout was raised inside it at the deadline"
            usually = (
                " A thread that does not stop is usually in a call that never "
                "returns to Python (a socket with no timeout, time.sleep(), a "
                "lock, a long database statement), or the task caught "
                "TaskTimeout and kept running."
            )
        exc = TaskTimeout(
            f"Task ran past the {watch.timeout:g}s timeout on worker "
            f"{self.worker_id!r}: {delivered}, and the thread did not stop "
            f"within the {self.timeouts.grace:g}s grace.{usually} This attempt "
            f"is recorded as failed, the worker is recycling so that the thread "
            f"dies with the process, and the outcome the thread eventually "
            f"reports is refused by the lease.",
            timeout=watch.timeout,
        )
        try:
            self._handle_failure(db_task, exc, duration_ms, release=True)
        except Error:
            # Recording the attempt is worth attempting and not worth the
            # fleet. The thread is wedged whether or not the database is
            # reachable, and the recycle below is what stops this worker
            # keeping a dead pool slot and waiting for that thread forever.
            # Letting this propagate would skip it.
            logger.warning(
                "Worker %s could not record the stuck attempt for task id=%s; "
                "recycling anyway",
                self.worker_id,
                db_task.id,
                exc_info=True,
                extra=self._log_extra("task_stuck_unrecorded", db_task),
            )
        # Whether the write landed and whether the thread is stuck are
        # different questions, and this used to answer the second with the
        # first. A lost write can mean the thread's own outcome landed ahead
        # of it, in which case the thread did come back; it can equally mean
        # a reaper requeued the row underneath us, which sets READY and moves
        # the epoch, so the write matches nothing while the thread runs on.
        # Reading that as "not stuck" left the worker un-recycled, the pool
        # slot gone for the life of the process, the thread absent from the
        # stuck set so the drain waited on it without bound, and the row
        # available for anyone to claim and run alongside it.
        #
        # Ask the thread instead. It is still inside this attempt or it is
        # not, and the answer does not depend on any database write.
        with self._in_flight_lock:
            still_running = self._running_on.get(watch.ident) == watch.attempt
        if still_running:
            self._stuck[watch.ident] = watch.attempt
            self._recycle(db_task)

    def _recycle(self, db_task: OxTask) -> None:
        if self._recycling:
            return
        self._recycling = True
        logger.warning(
            "Worker %s recycling: no new claims, draining the other in-flight "
            "tasks, then exiting with code %d",
            self.worker_id,
            RECYCLE_EXIT_CODE,
            extra={
                "event": "worker_recycling",
                "worker_id": self.worker_id,
                "task_id": str(db_task.id),
                "exit_code": RECYCLE_EXIT_CODE,
            },
        )
        self._stop.set()

    @property
    def recycling(self) -> bool:
        """
        True once the backstop has given up on a thread. run() then drains
        and returns, and the process must exit with RECYCLE_EXIT_CODE via
        os._exit, because the stuck thread is not a daemon and would block a
        normal exit at interpreter shutdown, which is the exact wait the
        recycle exists to end.

        `manage.py ox_worker` does that, and the supervisor reads the code and
        replaces the child. Anyone embedding Worker.run() has to do it too:
        returning from run() is not the end of it, and a caller who simply
        falls out of main() hangs on the thread that was abandoned.
        """
        return self._recycling

    def _stuck_alive(self) -> int:
        """
        How many pool threads are still inside an attempt we gave up on.

        Counted by the attempt, not by the thread being alive. A pool thread
        is reused, so its ident is still alive and idle after a stuck task
        finally returns, and alive again under the next task. Counting idents
        therefore over-counts, and the drain would stop waiting for healthy
        work and let the process exit out from under it.
        """
        with self._in_flight_lock:
            return sum(
                1
                for ident, attempt in self._stuck.items()
                if self._running_on.get(ident) == attempt
            )

    def _handle_failure(
        self,
        db_task: OxTask,
        exc: BaseException,
        duration_ms: int,
        *,
        release: bool = False,
    ) -> bool:
        """
        Record a failed attempt: a retry with backoff, or FAILED when the
        attempts are spent. Returns True when the write landed.

        release=True is the stuck-thread case. The execution is being taken
        off the row while its thread is still running, so the write also
        moves the lease epoch, the same way the reaper moves it when it
        takes a row off a worker that went quiet. Whatever that thread
        writes later carries the old number and matches nothing.
        """
        from .results import task_result_from_db

        handover: dict[str, Any] = (
            {"lease_epoch": db_task.lease_epoch + 1} if release else {}
        )
        exception_type = type(exc)
        errors = [
            *db_task.errors,
            {
                "exception_class_path": (
                    f"{exception_type.__module__}.{exception_type.__qualname__}"
                ),
                "traceback": "".join(format_exception(exc)),
            },
        ]

        if db_task.attempts >= db_task.max_attempts:
            if not self._write_outcome(
                db_task,
                status=OxTask.Status.FAILED,
                duration_ms=duration_ms,
                errors=errors,
                # Process time, not the lease clock; _lease_now says why.
                finished_at=timezone.now(),
                locked_by=None,
                locked_at=None,
                **handover,
            ):
                return False
            try:
                task_result = task_result_from_db(db_task)
            except ImportError:
                # Task module no longer importable; the row still records
                # the failure, but no result object can be built to signal.
                task_result = None
            if task_result is not None:
                task_finished.send_robust(
                    sender=type(self.backend), task_result=task_result
                )
            logger.error(
                "Task id=%s path=%s failed after %d/%d attempts (%s)",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                exception_type.__qualname__,
                extra=self._log_extra(
                    "task_failed",
                    db_task,
                    duration_ms=duration_ms,
                    exception=exception_type.__qualname__,
                ),
            )
        else:
            # The exponent is capped before the multiplication, not after.
            # `attempts` is a PositiveSmallIntegerField, so it can reach
            # 32767, and 2 ** 32766 raised OverflowError converting to float
            # while computing a value the min() was about to throw away. It
            # raised inside _handle_failure, on the failure path, so the row
            # stayed RUNNING until the reaper took it: a deployment with a
            # large MAX_ATTEMPTS turned every failure into a lost lease.
            #
            # Capping at 64 doublings is far past any backoff_max anyone
            # configures and keeps the arithmetic in range.
            doublings = min(max(db_task.attempts - 1, 0), 64)
            delay = min(self.backoff_initial * (2**doublings), self.backoff_max)
            if not self._write_outcome(
                db_task,
                status=OxTask.Status.READY,
                duration_ms=duration_ms,
                errors=errors,
                run_after=timezone.now() + timedelta(seconds=delay),
                locked_by=None,
                locked_at=None,
                **handover,
            ):
                return False
            logger.warning(
                "Task id=%s path=%s attempt %d/%d failed (%s); retrying in %.1fs",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                exception_type.__qualname__,
                delay,
                extra=self._log_extra(
                    "task_retrying",
                    db_task,
                    duration_ms=duration_ms,
                    exception=exception_type.__qualname__,
                ),
            )
        return True

    # -- reaping -----------------------------------------------------------

    def reap(self) -> int:
        """
        Take tasks back from workers that stopped renewing their lease.

        A RUNNING row whose locked_at is older than lock_timeout has a
        holder that has gone quiet. That is the only thing the reaper knows,
        and it is all it is allowed to act on. It cannot tell a dead worker
        from a starved one, so it never writes a verdict on the work:

        - **Attempts remaining.** Put the row back to READY and bump the
          lease epoch. The bump is what makes this safe to guess at: if the
          old worker was only slow, its finish write now matches nothing,
          and the row it was going to overwrite is somebody else's.
        - **Attempts exhausted.** Mark the row LOST, which says the lease
          was lost and nothing about the outcome. It records no cause,
          because it witnessed none. The epoch is deliberately *not* bumped
          here: LOST is an open question, and the holder of that epoch is
          the only party who could ever answer it, so it keeps the right to
          write the real outcome over the top.

        Returns the number of rows reclaimed. Sends no signal either way: a
        supervisor that has observed nothing has nothing to announce, and
        announcing a guess would be a second task_finished for a task that
        may yet report its own.
        """
        cutoff = _lease_now() - timedelta(seconds=self.lock_timeout)
        reclaimed = 0
        stuck = OxTask.objects.using(self._db_alias).filter(
            status=OxTask.Status.RUNNING, locked_at__lt=cutoff
        )
        for db_task in stuck:
            exhausted = db_task.attempts >= db_task.max_attempts
            updates: dict[str, Any] = {
                "locked_by": None,
                "locked_at": None,
            }
            if exhausted:
                updates.update(
                    status=OxTask.Status.LOST,
                    # Process time, not the lease clock; _lease_now says why.
                    finished_at=timezone.now(),
                    errors=[
                        *db_task.errors,
                        {
                            "exception_class_path": (
                                f"{TaskAbandoned.__module__}."
                                f"{TaskAbandoned.__qualname__}"
                            ),
                            "traceback": (
                                f"Worker {db_task.locked_by!r} stopped renewing "
                                f"its lease on this task; the claim aged past "
                                f"{self.lock_timeout}s with no attempts "
                                f"remaining. What the attempt did was never "
                                f"observed: it may have succeeded, it may have "
                                f"failed, it may not have got that far. This "
                                f"record is the lost lease, not a cause."
                            ),
                        },
                    ],
                )
            else:
                updates.update(
                    status=OxTask.Status.READY,
                    lease_epoch=db_task.lease_epoch + 1,
                )
            # Two different races, so two predicates. The epoch catches a
            # handover: a worker that finished, or another reaper that got
            # here first, leaves a different epoch and this row is not
            # overwritten. It cannot catch a renewal, because renewing does
            # not change the epoch - `renew_leases` writes `locked_at` and
            # nothing else. Without the expiry predicate here, a worker that
            # was briefly late renewing and then renewed still loses its
            # task: the row was selected while it looked expired, and by the
            # time this UPDATE runs it is held and live. The reclaim would
            # hand a running task to a second worker.
            #
            # Re-checking against the same cutoff the selection used asks the
            # only question that matters: does this row STILL look abandoned.
            # It compares an ordering, not a round-tripped timestamp for
            # equality, which is what an earlier compare got wrong.
            changed = (
                OxTask.objects.using(self._db_alias)
                .filter(
                    pk=db_task.pk,
                    status=OxTask.Status.RUNNING,
                    lease_epoch=db_task.lease_epoch,
                    locked_at__lt=cutoff,
                )
                .update(**updates)
            )
            if changed:
                reclaimed += 1
                logger.warning(
                    "Reclaimed stuck task id=%s path=%s (attempt %d/%d) -> %s",
                    db_task.id,
                    db_task.task_path,
                    db_task.attempts,
                    db_task.max_attempts,
                    updates["status"],
                    extra=self._log_extra(
                        "task_reclaimed", db_task, status=str(updates["status"])
                    ),
                )
        return reclaimed

    # -- scheduling --------------------------------------------------------

    def _latest_ticks(self) -> dict[str, datetime]:
        """Latest recorded tick per schedule name, for this worker's schedules."""
        return {
            row["schedule_name"]: row["latest"]
            for row in OxScheduleTick.objects.filter(
                schedule_name__in=[schedule.name for schedule in self.schedules]
            )
            .values("schedule_name")
            .annotate(latest=Max("scheduled_for"))
        }

    def dispatch_schedules(self) -> int:
        """
        Enqueue one task for each schedule whose latest tick has passed and
        was not yet dispatched. Returns the number of tasks enqueued.

        Safe to run from every worker: the unique constraint on
        (schedule_name, scheduled_for) lets exactly one INSERT per tick
        commit, and a loser's transaction rolls back tick row and task row
        together. If workers were down across one or more ticks, only the
        latest missed tick fires. A schedule with no rows yet is anchored
        at its current tick without firing, so it first fires at the next
        tick after deployment rather than for a time before it existed.
        """
        if not self.schedules:
            return 0
        now = timezone.now()
        # Cron fields describe wall-clock time in the project's timezone.
        local_now = (
            timezone.localtime(now).replace(tzinfo=None) if settings.USE_TZ else now
        )
        latest = self._latest_ticks()
        dispatched = 0
        for schedule in self.schedules:
            tick = schedule.cron.previous(local_now)
            scheduled_for = timezone.make_aware(tick) if settings.USE_TZ else tick
            last = latest.get(schedule.name)
            if last is not None and scheduled_for <= last:
                if last <= now:
                    continue
                # The newest tick in the log is in the future, which a
                # clock-skewed worker's write can leave behind. That must not
                # suppress ticks which are due now, so the comparison against
                # the newest one cannot decide this. Ask about this instant
                # instead: if it has already been recorded, it has run.
                #
                # Without that, every pass enqueued, the unique constraint
                # refused the tick row, and the whole transaction rolled back
                # - except that enqueue() saves and fires task_enqueued before
                # the outer block unwinds, so receivers saw an enqueue per
                # pass, about once a second, for a task that never existed.
                #
                # One extra query, and only while a future tick is the newest
                # one. In ordinary operation the comparison above answers.
                if (
                    OxScheduleTick.objects.using(self._db_alias)
                    .filter(schedule_name=schedule.name, scheduled_for=scheduled_for)
                    .exists()
                ):
                    continue
            try:
                with transaction.atomic(using=self._db_alias):
                    result = None
                    # Whether this is the first sighting is decided here,
                    # from the log, rather than from the snapshot taken
                    # before the loop. Another worker can commit this
                    # schedule's anchor in between, and then this pass is
                    # not the first sighting at all: anchoring again writes
                    # a second no-task row, this time over a tick that had a
                    # boundary and should have fired. The constraint then
                    # suppresses that instant for good, so the schedule
                    # silently skips a run.
                    #
                    # One extra query, and only while a schedule has no
                    # ticks at all. Once it has one, `last` is set and this
                    # never runs again.
                    first_sighting = last is None and not (
                        OxScheduleTick.objects.using(self._db_alias)
                        .filter(schedule_name=schedule.name)
                        .exists()
                    )
                    if not first_sighting:
                        result = schedule.task.enqueue(
                            *schedule.args, **schedule.kwargs
                        )
                    OxScheduleTick.objects.create(
                        schedule_name=schedule.name,
                        scheduled_for=scheduled_for,
                        task_id=result.id if result is not None else None,
                        created_at=now,
                    )
            except IntegrityError:
                # Another worker inserted this tick between our read and
                # INSERT; its transaction won and ours rolled back whole.
                continue
            if result is not None:
                dispatched += 1
                logger.info(
                    "Dispatched schedule %s tick %s (task id=%s)",
                    schedule.name,
                    scheduled_for.isoformat(),
                    result.id,
                    extra={
                        "event": "schedule_dispatched",
                        "schedule": schedule.name,
                        "task_id": str(result.id),
                        "worker_id": self.worker_id,
                    },
                )
        return dispatched

    # -- lifecycle ---------------------------------------------------------

    def run_once(self) -> bool:
        """
        Claim and execute a single task inline. Returns True if one ran.

        Renewed for the duration, the same as a task on the pool. `execute()`
        puts the attempt in the renewal set, but only `run()` starts the
        thread that services it, so a task run this way used to hold a lease
        nothing refreshed: anything outliving LOCK_TIMEOUT was reaped
        mid-flight and handed to a real worker while this call was still
        inside the function. There is no underscore on this method and its
        callers read it as public, so refusing long work here would be
        publishing the trap rather than closing it.
        """
        db_task = self.claim_one()
        if db_task is None:
            return False
        stop = Event()
        renewer = Thread(
            target=self._renewal_loop,
            args=(stop,),
            name="ox-renew-inline",
            daemon=True,
        )
        renewer.start()
        try:
            self.execute(db_task, inline=True)
        finally:
            stop.set()
            renewer.join(timeout=self.renew_interval + 5)
        return True

    def request_stop(self) -> None:
        """Stop claiming new tasks; in-flight tasks drain before run() exits."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def _close_connections_in_thread(self, barrier: Barrier) -> None:
        # The barrier makes every pool thread take exactly one of these
        # tasks; without it one idle thread could consume several and leave
        # another thread's connection open. It cannot deadlock (one close
        # task is submitted per pool slot, and blocked submissions spawn
        # threads up to max_workers); the timeout is pure defense, and a
        # broken barrier still falls through to the close.
        with suppress(BrokenBarrierError):
            barrier.wait(timeout=10)
        connections.close_all()

    def _execute_in_thread(self, db_task: OxTask) -> None:
        # The instance was claimed on the main thread's connection; it is a
        # plain in-memory object here, and its saves use this thread's own
        # connection.
        close_old_connections()
        try:
            self.execute(db_task)
        except Exception:
            logger.exception(
                "Unhandled error executing task id=%s",
                db_task.pk,
                extra=self._log_extra("worker_error", db_task),
            )
        finally:
            close_old_connections()

    def run(self) -> None:
        """Poll for tasks until request_stop(), then drain in-flight tasks."""
        logger.info(
            "Worker %s starting: queues=%s concurrency=%d poll=%.1fs schedules=%d",
            self.worker_id,
            self.queues or "(all)",
            self.concurrency,
            self.poll_interval,
            len(self.schedules),
            extra={
                "event": "worker_started",
                "worker_id": self.worker_id,
                "queues": self.queues,
                "concurrency": self.concurrency,
            },
        )
        in_flight: set[Future[None]] = set()
        last_reap = 0.0
        last_dispatch = 0.0
        executor = ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix="ox"
        )
        # Renewal is a separate thread rather than a step in the poll loop
        # because it has to outlive the loop: a drain can take as long as
        # the slowest in-flight task, and a lease that expires while its
        # task is finishing cleanly is the exact false reclaim this exists
        # to prevent. It is stopped after the drain, not before it.
        renew_stop = Event()
        renewer = Thread(
            target=self._renewal_loop,
            args=(renew_stop,),
            name="ox-renew",
            daemon=True,
        )
        renewer.start()
        try:
            while not self._stop.is_set():
                if self.parent_pid is not None and os.getppid() != self.parent_pid:
                    logger.warning(
                        "Worker %s lost its supervisor (pid %d); draining",
                        self.worker_id,
                        self.parent_pid,
                        extra={
                            "event": "worker_orphaned",
                            "worker_id": self.worker_id,
                            "parent_pid": self.parent_pid,
                        },
                    )
                    self.request_stop()
                    break
                try:
                    if time.monotonic() - last_reap >= self.reap_interval:
                        self.reap()
                        last_reap = time.monotonic()
                    if (
                        self.schedules
                        and time.monotonic() - last_dispatch >= self.schedule_interval
                    ):
                        self.dispatch_schedules()
                        last_dispatch = time.monotonic()
                    in_flight = {f for f in in_flight if not f.done()}
                    claimed_any = False
                    while len(in_flight) < self.concurrency and not self._stop.is_set():
                        db_task = self.claim_one()
                        if db_task is None:
                            break
                        claimed_any = True
                        in_flight.add(executor.submit(self._execute_in_thread, db_task))
                except Error:
                    # django.db.Error rather than DatabaseError, for the reason
                    # the renewal loop learned: InterfaceError sits beside
                    # DatabaseError under Error, and a connection dropped
                    # underneath the worker is the likeliest failure here.
                    #
                    # The envelope this package publishes says the database may
                    # go away and come back, and every other loop here already
                    # honours that: the renewal thread survives it and so does
                    # an attempt. This one did not, so one reconnect error
                    # unwound out of run(). The supervisor then replaced the
                    # child, the replacement failed on its own first reap, and
                    # five deaths in a minute stop the supervisor for good - so
                    # a blip the worker was built to ride out took the whole
                    # fleet down and left it down.
                    #
                    # Reap, dispatch and claim are all retried on the next pass
                    # by construction: nothing here holds state that a missed
                    # pass loses. Dropping the connection is what makes the
                    # next pass reconnect rather than reuse a broken one.
                    logger.warning(
                        "Worker %s could not reach the database this pass; "
                        "retrying in %.1fs",
                        self.worker_id,
                        self.poll_interval,
                        exc_info=True,
                        extra={
                            "event": "worker_poll_failed",
                            "worker_id": self.worker_id,
                        },
                    )
                    # close_old_connections rather than close_all: it drops
                    # exactly the connections Django knows are unusable or past
                    # their age, which is what makes the next pass reconnect,
                    # and it is what the attempt path already does. close_all
                    # would also tear down a connection the caller owns.
                    close_old_connections()
                    self._stop.wait(self.poll_interval)
                    continue
                if not claimed_any:
                    if in_flight:
                        # A slot may free up long before the poll interval
                        # elapses; wake as soon as any in-flight task settles
                        # so throughput is not bounded by poll_interval.
                        wait(
                            in_flight,
                            timeout=self.poll_interval,
                            return_when=FIRST_COMPLETED,
                        )
                    else:
                        self._stop.wait(self.poll_interval)
        finally:
            pending = sum(1 for f in in_flight if not f.done())
            if pending:
                logger.info(
                    "Worker %s draining %d in-flight task(s)",
                    self.worker_id,
                    pending,
                    extra={
                        "event": "worker_draining",
                        "worker_id": self.worker_id,
                        "pending": pending,
                    },
                )
            self._drain(in_flight)
            renew_stop.set()
            renewer.join(timeout=self.renew_interval + 5)
            if self._recycling:
                # A stuck thread never takes a close task, so the barrier
                # below would only time out; the process is about to exit
                # and the connections go with it.
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                # Pool threads hold thread-local DB connections that survive
                # the drain (close_old_connections() only closes expired
                # ones); close each deterministically before the pool exits.
                barrier = Barrier(self.concurrency)
                for _ in range(self.concurrency):
                    executor.submit(self._close_connections_in_thread, barrier)
                executor.shutdown(wait=True)
            connections.close_all()
            logger.info(
                "Worker %s stopped",
                self.worker_id,
                extra={"event": "worker_stopped", "worker_id": self.worker_id},
            )

    def _drain(self, in_flight: set[Future[None]]) -> None:
        """
        Wait for the in-flight tasks, except the stuck ones while recycling:
        a thread the backstop gave up on may never finish, and the point of
        the recycle is to stop waiting for it.
        """
        while True:
            pending = {future for future in in_flight if not future.done()}
            if not pending:
                return
            # Re-read the flag every pass. The backstop can fire part way
            # through an ordinary drain, and a flag read once on the way in
            # would never see it: the drain would wait unbounded on the thread
            # the recycle exists to abandon, and the supervisor would never
            # get its replacement.
            if self._recycling and len(pending) <= self._stuck_alive():
                return
            wait(pending, timeout=RECYCLE_DRAIN_POLL, return_when=FIRST_COMPLETED)


def worker_class(backend_alias: str = DEFAULT_TASK_BACKEND_ALIAS) -> type[Worker]:
    """
    The Worker class for a backend, from its OPTIONS["WORKER_CLASS"].

    Resolved from settings rather than chosen by the management command,
    because the supervisor starts every child as `ox_worker`. A worker
    selected any other way would be the configured worker at --processes 1
    and the default one in every child above it.
    """
    backend = task_backends[backend_alias]
    path = backend.options.get("WORKER_CLASS")
    if not path:
        return Worker
    cls = import_string(path)
    if not (isinstance(cls, type) and issubclass(cls, Worker)):
        raise ImproperlyConfigured(
            f"WORKER_CLASS {path!r} on backend {backend_alias!r} is not a "
            "django_ox.worker.Worker subclass."
        )
    return cls
