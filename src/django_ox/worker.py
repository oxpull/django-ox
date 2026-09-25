import asyncio
import copy
import ctypes
import functools
import json
import logging
import math
import os
import selectors
import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Coroutine, Generator, Iterator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from inspect import iscoroutinefunction
from threading import Barrier, BrokenBarrierError, Condition, Event, Lock, Thread
from traceback import format_exception
from typing import Any, cast

from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    close_old_connections,
    connections,
    router,
    transaction,
)
from django.db.models import (
    DateTimeField,
    ExpressionWrapper,
    F,
    JSONField,
    Max,
    Q,
    QuerySet,
)
from django.db.models.expressions import Combinable, CombinedExpression
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
from .schedules import (
    Schedule,
    lock_contention,
    schedule_name_collisions,
    schedule_source_from_options,
)
from .timeouts import (
    RECYCLE_EXIT_CODE,
    _deadline,
    _deadline_monotonic,
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

# On a pooled PostgreSQL database, the longest lease renewal and the watchdog
# wait to open a connection of their own. Renewal also waits no longer than
# its interval, and a shorter connect_timeout in OPTIONS shortens both.
OWN_CONNECTION_DEADLINE = 5.0

# How long they wait for a connection from Django's pool instead, when their
# own cannot be had in time. A connection the pool has spare is handed over
# at once; waiting any longer would queue them behind the task threads, which
# is the starvation their own connection is there to avoid.
POOL_FALLBACK_WAIT = 0.1

# psycopg_pool refuses a checkout whose timeout is zero or less before it
# looks for an idle connection, so no checkout asks for less than this.
POOL_SHORTEST_WAIT = 0.001

# libpq reads a connect_timeout below this as this, and psycopg does too
# from 3.2.
LIBPQ_MIN_CONNECT_TIMEOUT = 2

# While lease renewal can get no connection at all, it says so at most this
# often, with the number of renewals missed since it last did.
MISSED_RENEWAL_REPORT_INTERVAL = 30.0


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


class _NotAdmitted(Exception):
    """
    Raised inside the dispatch transaction to roll it back.

    Not an error anyone sees: it is how a schedule that changed between
    being read and being acted on, or a tick from before the schedule's
    anchor, undoes a tick row that has already been written. Django rolls
    an atomic block back on any exception, which is the shortest way out
    of a write already made.
    """


def _latch_instant() -> datetime:
    """
    The fixed instant a first-sighting latch row is written at.

    Before any tick a schedule can be due at, so the latch never collides
    with this pass's own tick row and never falls inside the bounded tick
    read, and inside every supported database's datetime range. Not the
    epoch: an interval trigger counts from the epoch, so an interval
    longer than the time since it puts a schedule's first tick exactly
    there, and a latch at the same instant lost the unique index to the
    tick row on every pass. A cron trigger looks back nine years at most,
    and an interval tick is at or after the epoch in the project's zone.
    Naive or aware to match USE_TZ, as every other tick is.
    """
    instant = datetime(1900, 1, 1)
    return instant.replace(tzinfo=UTC) if settings.USE_TZ else instant


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

# Exhausted rows retired per reap pass. See Worker.reap.
REAP_BATCH_DEFAULT = 100

# The most of one traceback kept on the row, in BYTES of UTF-8. A traceback's
# length is set by the failure, not by us: a deep recursion, a chained
# exception, or a library that prints locals can produce megabytes, and
# `errors` holds one per failed attempt. Both ends are worth keeping, where the
# call came from and what actually raised, so an oversized traceback keeps its
# head and its tail with a marker between them saying what was dropped.
#
# Bytes rather than characters because the column is sized in bytes and an
# operator reads this number to size it. A character limit lets one emoji in an
# exception message store four times the stated cap.
MAX_STORED_TRACEBACK = 16384
_TRACEBACK_HEAD = 4096


def _utf8_prefix(text: str, limit: int) -> str:
    """The longest prefix of `text` that encodes within `limit` bytes."""
    encoded = text.encode()[:limit]
    # A cut can land inside a multi-byte sequence; drop the partial character.
    return encoded.decode(errors="ignore")


def _utf8_suffix(text: str, limit: int) -> str:
    """The longest suffix of `text` that encodes within `limit` bytes."""
    if limit <= 0:
        return ""
    encoded = text.encode()[-limit:]
    return encoded.decode(errors="ignore")


def _stored_traceback(exc: BaseException) -> str:
    """
    The exception's traceback, bounded in bytes, with any elision made visible.

    The marker counts towards the bound, so the whole returned string encodes
    within MAX_STORED_TRACEBACK. That is what the documentation promises and
    what an operator sizing the column needs it to mean.
    """
    text = "".join(format_exception(exc))
    size = len(text.encode())
    if size <= MAX_STORED_TRACEBACK:
        return text
    dropped = size - MAX_STORED_TRACEBACK
    marker = (
        f"\n... {dropped} bytes of this traceback were not stored "
        f"(limit {MAX_STORED_TRACEBACK}) ...\n"
    )
    budget = MAX_STORED_TRACEBACK - len(marker.encode())
    if budget <= 0:  # pragma: no cover - only if the limit is set absurdly low
        return _utf8_prefix(text, MAX_STORED_TRACEBACK)
    head = min(_TRACEBACK_HEAD, budget)
    return _utf8_prefix(text, head) + marker + _utf8_suffix(text, budget - head)


# Single-statement claim for PostgreSQL: the SKIP LOCKED subselect, the claim
# UPDATE, and the per-attempt bookkeeping are one round trip, atomic in
# autocommit. The equivalent multi-statement path costs 5 round trips.
#
# The claim's own timestamps come from {lease_clock}, which is
# STATEMENT_TIMESTAMP() when USE_TZ is on and a parameter carrying the worker's
# clock when it is off. That is not a style choice: _lease_now() makes the same
# switch, and renew_leases and the reaper's cutoff both go through it. One
# column, one clock. Stamping this from the server while the renewal stamps
# from the worker would put two on it, and a worker whose clock ran behind the
# server's would renew to a timestamp the reaper reads as already expired.
# run_after is the exception and is still compared against the worker's clock,
# because that is the clock the retry that wrote it used; see _ready_queryset.
# lease_epoch advances here too: the increment and the claim are one statement,
# so no execution can share an epoch with another.
POSTGRES_CLAIM_SQL = """
UPDATE "{table}" SET
    "status" = %(running)s,
    "locked_by" = %(worker_id)s,
    "locked_at" = {lease_clock},
    "lease_expires_at" = {lease_clock} + %(lease_ttl)s,
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
    do not.

    Only the lease is stamped this way: locked_at, the started_at and
    last_attempted_at written beside it in the claim, and the cutoff the reaper
    judges locked_at against. Two columns sit outside it on purpose. run_after
    is one, and _ready_queryset says why. finished_at is the other: nothing
    fences on it, the reaper never reads it, and it is written by the statement
    that gives the lease up rather than by one that holds it. Every comparison
    against it here (ox_prune's cutoff, the throughput window in stats) is
    against timezone.now(), and django_ox.actions already stamps it from there,
    so process time is the clock it is read on. A database-computed value would
    need a read-back, and _write_outcome is on the path every task takes.
    """
    return Now() if settings.USE_TZ else timezone.now()


def _lease_expiry(seconds: float) -> Any:
    """
    When a lease taken now stops being valid, on the lease clock.

    The same clock as _lease_now(), because the reaper compares the two and a
    row whose expiry came from one clock and whose cutoff came from another is
    exactly the disagreement storing it is meant to remove.
    """
    # Built on _lease_now() rather than repeating its choice, so anything that
    # moves the lease clock moves the expiry with it. Stamping this from the
    # process while locked_at came from the database, or the reverse, would put
    # two clocks on one lease, which is the disagreement the stored expiry
    # exists to remove rather than relocate.
    now = _lease_now()
    if settings.USE_TZ:
        return ExpressionWrapper(
            now + timedelta(seconds=seconds), output_field=DateTimeField()
        )
    return now + timedelta(seconds=seconds)


def _pool_options(alias: str) -> Mapping[str, Any] | None:
    """
    The psycopg_pool arguments Django opens `alias`'s connection pool with,
    or None when it opens none.

    Django pools on any true OPTIONS["pool"]: True for psycopg_pool's
    defaults, a non-empty mapping for the arguments themselves, and any
    other true value it refuses when it connects. _outside_the_pool and the
    startup warning both read the pool through this, so they cannot
    disagree about whether there is one. Each checks for PostgreSQL itself.
    """
    pool = connections.settings.get(alias, {}).get("OPTIONS", {}).get("pool")
    if pool is True:
        return {}
    if isinstance(pool, Mapping) and pool:
        return pool
    return None


def _connection_pool(conn: Any) -> Any:
    """
    The open psycopg_pool pool Django checks `conn`'s alias out of, or None
    when the alias is not pooled or its pool is not open. A pool that
    nothing has connected through yet, or that was closed, holds no
    connection to test, and checking it would ask a pool with no workers
    to grow.
    """
    if conn.vendor != "postgresql" or _pool_options(conn.alias) is None:
        return None
    pool = getattr(conn, "pool", None)
    if pool is None or pool.closed:
        return None
    return pool


def _sweep_pool(conn: Any) -> None:
    """
    Have Django's PostgreSQL pool for `conn`'s alias test every connection
    it holds idle, now, and discard each that fails; nothing when the alias
    is not pooled.

    A connection lost to a restart or a failover is rarely the only one:
    the server ended every session, and the pool's idle connections are as
    dead as the one that just failed. Closing that one hands it back for
    the pool to discard, but the next statement on this thread checks out
    one of the others. Without CONN_HEALTH_CHECKS the pool hands it out
    unchecked, and it fails at its first statement too; with them, the
    checkout tests it, discards it and tries the next, waiting longer each
    time, and enough dead ones use up the checkout's timeout. psycopg_pool's
    check() takes every idle connection out, tests each once, puts the
    live ones back and asks for a replacement of each dead one.

    It costs one round trip per idle connection, and a dead one can take
    longer to fail than a live one takes to answer, so it runs only on a
    path that has already failed, never on an ordinary one. It does not
    make the next checkout fresh: a replacement may still be connecting,
    and the checkout waits for it; the database may still be down; and a
    connection the sweep found alive can drop the moment after.

    Best-effort: it runs while the caller handles an error, and whatever it
    raises is dropped, so it can neither replace that error nor stop the
    recovery that follows.
    """
    with suppress(Exception):
        pool = _connection_pool(conn)
        if pool is not None:
            pool.check()


def _close_lost_connection(conn: Any) -> None:
    """
    Close `conn`, which a lost connection to its database has left unusable,
    then sweep its pool, _sweep_pool, when the alias is pooled.

    Only for a connection outside any atomic block, which each caller
    checks first: inside one, the transaction belongs to whoever opened the
    block, not to the worker. close() drops its reference to the driver
    connection even when closing it raises, and on a dead one it may.
    """
    with suppress(Error):
        conn.close()
    _sweep_pool(conn)


def _reason(exc: BaseException) -> str:
    """An exception as one line of a log message."""
    return " ".join(str(exc).split()) or type(exc).__name__


def _positive_seconds(value: Any) -> float | None:
    """A connect_timeout from OPTIONS as positive seconds, or None for none."""
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


# The deadline _connection_by_deadline's connections open by, for the thread
# that is opening one. Set by _Driver.connect() before every connect.
_connect_deadline = threading.local()


def _connect_by(deadline: float, gen: Generator[Any, Any, Any]) -> Any:
    """
    Run psycopg's connection generator `gen` to the end, waiting on its
    socket here, and give up at `deadline`, on time.monotonic().

    psycopg bounds a connection with connect_timeout, and neither way it
    applies it is a deadline. Before 3.2 it is how long each step of the
    handshake may wait, so a server that answers every step slowly takes a
    multiple of it. From 3.2 it bounds each attempt, and there is one
    attempt per host and per address a host name resolves to, so two
    stalled hosts take twice as long. Waiting here, against one deadline
    for every step of every attempt, is what makes it one. The generator is
    closed however this ends, which finishes any connection it had started,
    so nothing it opened outlives the deadline. Resolving a host name
    blocks in the resolver and is not covered.
    """
    from psycopg.errors import ConnectionTimeout

    try:
        with closing(gen), selectors.DefaultSelector() as selector:
            if time.monotonic() >= deadline:
                raise ConnectionTimeout("connection timeout expired")
            fileno, events = next(gen)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectionTimeout("connection timeout expired")
                selector.register(fileno, events)
                ready = selector.select(remaining)
                selector.unregister(fileno)
                if ready:
                    fileno, events = gen.send(ready[0][1])
    except StopIteration as done:
        return done.value


@functools.cache
def _connection_by_deadline() -> type[Any]:
    """
    psycopg's Connection, opening by the calling thread's _connect_deadline.

    psycopg's connect() waits on the generator _connect_gen returns, one per
    attempt, on every version this package supports. This one does the
    waiting itself, through _connect_by, and hands psycopg's own wait a
    generator that has already finished.
    """
    import psycopg

    class ConnectionByDeadline(psycopg.Connection[Any]):
        @classmethod
        def _connect_gen(cls, conninfo: str = "", **kwargs: Any) -> Any:
            conn = _connect_by(
                _connect_deadline.at, super()._connect_gen(conninfo, **kwargs)
            )
            yield from ()
            return conn

    return ConnectionByDeadline


class _Driver:
    """
    psycopg as one wrapper sees it: connect() opens by its owner's deadline,
    or `budget` seconds from the call when the owner has none nearer.
    Django reads the exception classes and the rest of the driver through
    the same attribute, so everything else is psycopg's own.
    """

    def __init__(self, owner: "_OwnConnection") -> None:
        import psycopg

        self._psycopg = psycopg
        self._owner = owner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._psycopg, name)

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        owner = self._owner
        deadline = min(owner.deadline, time.monotonic() + owner.budget)
        if time.monotonic() >= deadline:
            # Refused here, because _connect_by is reached only after psycopg
            # has resolved the host names, and resolving one can block.
            raise self._psycopg.errors.ConnectionTimeout("connection timeout expired")
        _connect_deadline.at = deadline
        return _connection_by_deadline().connect(*args, **kwargs)


class _Checkout:
    """
    Django's pool for `alias` as one wrapper sees it: a checkout waits at
    most `wait` seconds, where Django's own waits the pool's timeout. With
    `once`, only the first checkout reaches the pool, and a reconnect after
    that connection was given back fails at once.
    """

    def __init__(self, pool: Any, wait: float, *, once: bool = False) -> None:
        self._pool = pool
        self._wait = wait
        self._once = once
        self._taken = False

    def open(self) -> None:
        self._pool.open()

    def getconn(self) -> Any:
        if self._once and self._taken:
            import psycopg

            raise psycopg.OperationalError(
                "no reconnect: the one connection this block could take from "
                "the pool was given back"
            )
        self._taken = True
        return self._pool.getconn(timeout=self._wait)


class _OwnConnection:
    """
    A connection of the calling thread's own to a pooled PostgreSQL alias,
    outside Django's pool, opened by a deadline; and, for when it cannot
    be, a connection from the pool for the length of one block.

    `wrapper` is a new wrapper for the alias, built from its settings with
    "pool" taken out of OPTIONS, so the rest of the project's connection
    settings still apply. The settings are copied rather than edited,
    because every other wrapper for the alias reads the same dictionary.

    The one setting changed is connect_timeout, and only in the copy. The
    deadline is `budget` seconds, or a shorter positive connect_timeout
    from OPTIONS; a longer one does not lengthen it. _Driver holds the
    connection to that deadline. connect_timeout becomes the deadline
    rounded up to whole seconds, and no less than libpq's minimum, so that
    psycopg bounds each attempt by itself as well.
    """

    def __init__(self, wrapper: Any, budget: float) -> None:
        options = {
            name: value
            for name, value in wrapper.settings_dict["OPTIONS"].items()
            if name != "pool"
        }
        configured = _positive_seconds(options.get("connect_timeout"))
        self.budget = budget if configured is None else min(budget, configured)
        self.deadline = math.inf
        options["connect_timeout"] = max(
            LIBPQ_MIN_CONNECT_TIMEOUT, math.ceil(self.budget)
        )
        wrapper.settings_dict = {**wrapper.settings_dict, "OPTIONS": options}
        wrapper.Database = _Driver(self)
        self.alias: str = wrapper.alias
        self.wrapper = wrapper

    @property
    def is_open(self) -> bool:
        return self.wrapper.connection is not None

    def connect_by(self, deadline: float) -> None:
        """
        From now until the next call, any connection this opens gives up at
        `deadline`, whether open() asks for it or Django reconnects by
        itself. Once `deadline` has passed, every one fails at once.
        """
        self.deadline = deadline

    def open(self, deadline: float) -> None:
        """
        Open the connection unless it is open, giving up at `deadline`,
        which then holds for any reconnect until the next call.
        """
        self.connect_by(deadline)
        if self.is_open:
            return
        try:
            self.wrapper.ensure_connection()
        except BaseException:
            # One that failed while Django was setting it up is not one to
            # reuse on the next attempt.
            self.close()
            raise

    def close(self) -> None:
        """Close the connection if it is open, quietly."""
        with suppress(Error):
            self.wrapper.close()

    @contextmanager
    def borrowed(self, wait: float, *, reconnect: bool = True) -> Iterator[None]:
        """
        Run the block on a connection from the alias's pool, waiting at
        most `wait` for it, and give it back as the block ends, whether it
        returns or raises. Raises from the checkout when there is none.
        Without `reconnect`, the block gets that one connection and no
        other: once Django has given it back, as it can after the
        connection stops working, a query that would check out another
        fails at once.

        Django's own wrapper checks out with the pool's timeout, 30 s
        unless configured, which is the wait to be avoided. The block's
        wrapper is a new one whose `pool` bounds the checkout: Django looks
        its pool up in _connection_pools, and an instance attribute of that
        name is found first. Everything else is Django's, from the checkout
        and the setup of a pooled connection to giving it back on close.
        The connection is never kept past the block, and is never a task
        thread's.
        """
        before = connections[self.alias]
        wrapper: Any = connections.create_connection(self.alias)
        checkout = _Checkout(wrapper.pool, wait, once=not reconnect)
        wrapper._connection_pools = {self.alias: checkout}
        connections[self.alias] = wrapper
        try:
            wrapper.ensure_connection()
            yield
        finally:
            connections[self.alias] = before
            wrapper.close()


@contextmanager
def _outside_the_pool(
    alias: str, budget: float = OWN_CONNECTION_DEADLINE
) -> Iterator[_OwnConnection | None]:
    """
    Give the calling thread a connection to `alias` of its own for the
    block, outside Django's PostgreSQL connection pool when that is on, and
    yield it, or yield None when there is no such pool.

    The pool is one per process and every thread draws from it. A task
    holds its thread's connection from its first query to the end of the
    attempt, so once the task threads and the poll loop have taken the
    whole pool, the renewal thread waits out the pool timeout on every
    tick: the leases it keeps expire under running work, the reaper
    requeues the rows, and bodies that already ran run again. The
    watchdog's stuck-attempt write waits the same way and holds the
    recycle back. Neither may queue behind the work it protects.

    _OwnConnection says how the connection is built and opened; `budget`
    is its deadline. The pool itself is never touched. Whatever the thread
    had for the alias before is put back afterwards, however the block
    ends.

    Anything other than PostgreSQL with a pool _pool_options recognises
    runs the block on the thread's ordinary connection.
    """
    pooled = _pool_options(alias) is not None
    wrapper = connections.create_connection(alias) if pooled else None
    if wrapper is None or wrapper.vendor != "postgresql":
        yield None
        return
    own = _OwnConnection(wrapper, budget)
    before = [c for c in connections.all(initialized_only=True) if c.alias == alias]
    connections[alias] = own.wrapper
    try:
        yield own
    finally:
        if before:
            connections[alias] = before[0]
        else:
            del connections[alias]
        own.wrapper.close()


def _every(interval: float, stop: Event, tick: Callable[[], object]) -> None:
    """
    Call `tick` every `interval` seconds until `stop` is set, timed from the
    start of one call to the start of the next. The first is due `interval`
    after this is called.

    Timed from the end instead, every tick would push the next one out by
    its own length. A lease renewal that spends its whole connection
    deadline, which below a 15 s lease is the interval itself, would put
    the next attempt an interval after that, past a lease three intervals
    long. A tick that runs past its interval is followed by the next one at
    once, timed from its own start, so ticks never overlap and none is made
    up for later. The wait between them is stop.wait, so setting `stop`
    ends it at once.
    """
    started = time.monotonic()
    while not stop.wait(max(0.0, started + interval - time.monotonic())):
        started = time.monotonic()
        tick()


class _RenewalReport:
    """
    What lease renewal on a pooled PostgreSQL database logs about where its
    connection came from: each change, once, rather than every renewal.

    Renewal starts out on a connection of its own. The first renewal that
    is not on it, whether the pool served it or nothing did, is a warning
    naming why its own could not be had and what became of the pool.
    Further renewals through the pool are debug lines. Renewals that got no
    connection at all are a warning when they start and then a summary at
    most every MISSED_RENEWAL_REPORT_INTERVAL seconds, with the number
    missed since the last line. The first renewal back on its own
    connection is an info line with the totals. `clock` is time.monotonic
    outside tests.
    """

    def __init__(
        self, worker_id: str, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.worker_id = worker_id
        self.clock = clock
        self.path = "own"
        self.through_pool = 0
        self.missed_total = 0
        self.unreported = 0
        self.reported_at = -math.inf

    def own(self) -> None:
        """A renewal on the thread's own connection."""
        if self.path != "own":
            logger.info(
                "Worker %s renews its leases on its own connection again, "
                "after %d renewals through the connection pool and %d missed",
                self.worker_id,
                self.through_pool,
                self.missed_total,
                extra={
                    "event": "lease_renew_recovered",
                    "worker_id": self.worker_id,
                    "fallback_renewals": self.through_pool,
                    "missed_renewals": self.missed_total,
                },
            )
        self.path = "own"
        self.through_pool = self.missed_total = self.unreported = 0

    def pool(self, why: str) -> None:
        """A renewal through the pool, because of `why`."""
        self.through_pool += 1
        if self.path == "own":
            logger.warning(
                "Worker %s could not get a connection of its own for lease "
                "renewal (%s), and renewed through the connection pool "
                "instead. It tries its own again at every renewal",
                self.worker_id,
                why,
                extra={
                    "event": "lease_renew_degraded",
                    "worker_id": self.worker_id,
                    "error": why,
                    "fallback": "succeeded",
                },
            )
        else:
            logger.debug(
                "Worker %s renewed its leases through the connection pool (%s)",
                self.worker_id,
                why,
                extra={
                    "event": "lease_renew_fallback",
                    "worker_id": self.worker_id,
                    "error": why,
                },
            )
        self.path = "pool"

    def missed(self, why: str, pool_why: str) -> None:
        """No renewal: not on its own connection (`why`) nor the pool's."""
        self.missed_total += 1
        self.unreported += 1
        now = self.clock()
        was, self.path = self.path, "none"
        if was == "own":
            logger.warning(
                "Worker %s could not get a connection of its own for lease "
                "renewal (%s), nor one from the connection pool (%s), so this "
                "renewal is missed. It tries both again at every renewal",
                self.worker_id,
                why,
                pool_why,
                extra={
                    "event": "lease_renew_degraded",
                    "worker_id": self.worker_id,
                    "error": why,
                    "fallback": "failed",
                    "fallback_error": pool_why,
                },
            )
        elif now - self.reported_at >= MISSED_RENEWAL_REPORT_INTERVAL:
            logger.warning(
                "Worker %s missed %d lease renewal(s) since it last said so: "
                "no connection of its own (%s), and none from the connection "
                "pool (%s)",
                self.worker_id,
                self.unreported,
                why,
                pool_why,
                extra={
                    "event": "lease_renew_missed",
                    "worker_id": self.worker_id,
                    "missed": self.unreported,
                    "error": why,
                    "fallback_error": pool_why,
                },
            )
        else:
            return
        self.reported_at = now
        self.unreported = 0


class Worker:
    """
    Claims READY tasks and executes them, at least once.

    One worker holds the lease on a task at a time, and the lease number
    fences the row rather than the execution: two workers cannot write the
    same row, and two threads can still be inside the same task body, which
    the production page sets out in full. Task bodies are idempotent, the same
    as under any at-least-once queue.

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
    - While a worker is executing, it attempts to refresh locked_at every
      LOCK_TIMEOUT / 3 seconds, one statement covering every in-flight row.
      The lease holds only while that statement reaches the database on
      time; a running worker can still lose its lease.

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
        reap_batch: int | None = None,
        recycle_drain_budget: float | None = None,
        renew_interval: float | None = None,
        schedule_interval: float | None = None,
        backoff_initial: float | None = None,
        backoff_max: float | None = None,
        worker_index: int | None = None,
        parent_pid: int | None = None,
        task_timeout: float | None = None,
        task_timeout_grace: float | None = None,
        db_alias: str | None = None,
        batch: bool = False,
        max_tasks: int | None = None,
    ) -> None:
        backend = task_backends[backend_alias]
        if not isinstance(backend, OxBackend):
            raise ImproperlyConfigured(
                f"Backend {backend_alias!r} is {type(backend).__qualname__}, "
                "not an OxBackend."
            )
        self.backend = backend
        options = backend.options
        # Empty means the backend accepts any queue name; the worker then
        # processes all queues rather than filtering.
        self.queues: list[str] = list(queues) if queues else sorted(backend.queues)
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        # Both end run() through request_stop() and the normal drain, so a
        # job runner gets the same shutdown as a signal. The count is of
        # claims, not outcomes: a failed attempt and a retry's repeat claim
        # each used a slot of work.
        self.batch = batch
        self.max_tasks = max_tasks
        self._claimed = 0
        # Set when the compare-and-set claim read candidates but returned none,
        # because every candidate lost its CAS or failed ownership read-back.
        # That None does not establish an empty queue, so --batch polls again.
        # run() clears it before each claim_one(), so an override that returns
        # None without calling the base claim never inherits a stale one.
        self._claim_contended = False
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
        # Rows one reap pass may touch, on either branch. The requeue
        # branch names what it requeues in its record and the exhausted
        # branch writes a record specific to each row, so both take rows
        # in bounded batches and leave the remainder to the next pass.
        self.reap_batch: int = (
            reap_batch if reap_batch is not None else REAP_BATCH_DEFAULT
        )
        if self.reap_batch < 1:
            # A zero slices to nothing, so the reaper would retire no
            # exhausted row ever and leave them RUNNING for good, silently.
            raise ImproperlyConfigured(
                f"reap_batch must be at least 1, got {self.reap_batch!r}."
            )
        # How long a recycling worker waits on its healthy in-flight tasks
        # before leaving them to the reaper. See _drain for why this is the
        # lease and not a number of its own.
        self.recycle_drain_budget: float = (
            recycle_drain_budget
            if recycle_drain_budget is not None
            else self.lock_timeout
        )
        # Renewal ticks are scheduled start-to-start. Unless the 0.1 s
        # floor applies, three default intervals equal the lease.
        # One missed tick leaves another before expiry. After two misses,
        # the next tick is at the lease boundary, with no renewal margin.
        # An overrun starts the next tick immediately, without catch-up.
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
        self._schedule_source = schedule_source_from_options(options, backend_alias)
        # Kept as an attribute because it is public and has been since 0.1.0.
        # It is the source's answer at construction, which for the default
        # settings source is its answer forever. Dispatch asks the source
        # rather than reading this, so a source that changes is not stale
        # here in any way that matters.
        self.schedules = self._schedule_source.schedules()
        #: The last dropped tick reported for each schedule. A dropped tick
        #: writes no row, so nothing else stops it being recomputed and
        #: re-reported on every pass until its next tick comes due, which
        #: for a daily schedule is a warning a second for a day on the one
        #: event the documentation says to alert on. Dispatch only ever
        #: considers a schedule's latest tick, so one entry per schedule is
        #: the whole of what has to be remembered.
        self._dropped_reported: dict[str, datetime] = {}
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
        # Resolved once, here, and every statement this worker runs goes to
        # it. ox_worker --database sets it; otherwise it is the alias the
        # router sends OxTask writes to.
        self._db_alias = (
            db_alias if db_alias is not None else router.db_for_write(OxTask)
        )
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
        not. A subclass that overrides this hook alone keeps its condition on
        every database: on PostgreSQL the worker takes the
        ``SELECT ... FOR UPDATE SKIP LOCKED`` path, which applies it, and logs
        ``claim_filter_sql_missing`` once.
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
        would narrow SQLite and MySQL and claim the very rows it meant to
        exclude on PostgreSQL, with nothing raised and nothing logged: a
        condition that holds on two databases and does nothing on the third.

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
        # for up to a millisecond. Skew in run_after
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
            "lease_expires_at": _lease_expiry(self.lock_timeout),
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
                # psycopg adapts a timedelta to an interval, so this works
                # against either lease clock without branching the SQL.
                "lease_ttl": timedelta(seconds=self.lock_timeout),
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
                #
                # On the alias the claim was written to, and the alias is
                # what makes this a read of the row it just claimed.
                # Unqualified, it follows db_for_read: under a router that
                # splits reads from writes the worker is handed the row as
                # it stood before the claim, runs the task holding a lease
                # the row no longer has, and its own finish write is fenced
                # out. This branch is every claim on MySQL and MariaDB.
                candidate.refresh_from_db(using=self._db_alias)
                return candidate
        # Optimistic compare-and-set. attempts and lease_epoch double as
        # version counters: if another worker claimed (and possibly
        # requeued) the row between the fetch and this UPDATE, both have
        # moved and the literal bookkeeping values below cannot stomp its
        # writes.
        read_any = False
        for candidate in self._ready_queryset()[:CLAIM_BATCH_SIZE]:
            read_any = True
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
        # Every candidate went to another claimer, but the read only ever
        # looks at CLAIM_BATCH_SIZE rows and more may be due behind them. The
        # SKIP LOCKED paths never come back empty while an unlocked row is
        # due, so only this one has to say so.
        if read_any:
            self._claim_contended = True
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

        Pinning the read to the granted epoch turns that into a miss.
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
        already claimed is not renewed here.

        This is the whole of the worker's side of the lease. Renewal is
        deliberately blind to what the task itself is doing: even a wedged
        task keeps its lease while renewal reaches the database on time.
        With Django's PostgreSQL pool, renewal needs a connection; 1.4.0
        opens one outside the pool with a bounded deadline and a short
        pooled fallback. A live worker can still lose its lease if renewal
        cannot reach the database in time. Sizing LOCK_TIMEOUT alone does
        not prevent a reclaim. Recovering a wedged task whose lease keeps
        renewing is an operator's job, not the reaper's.
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
            .update(
                locked_at=_lease_now(),
                lease_expires_at=_lease_expiry(self.lock_timeout),
            )
        )

    def _renewal_loop(self, stop: Event) -> None:
        """
        Renew every renew_interval until stopped, timed from the start of one
        renewal to the start of the next, as _every says, with or without a
        pool and whether a renewal succeeds, fails or has nothing to renew.
        Runs on its own thread, and on a pooled PostgreSQL database on its
        own connection, as _renew_on says.
        """
        # Cap private connection establishment at the renewal interval.
        # Pool fallback can add up to 0.1 s. DNS can exceed the deadline.
        # Setup queries and renewal statements are outside this budget.
        # An overrun starts the next tick immediately.
        budget = min(OWN_CONNECTION_DEADLINE, self.renew_interval)
        with _outside_the_pool(self._db_alias, budget) as own:
            report = _RenewalReport(self.worker_id)
            safe_until = time.monotonic() + self.lock_timeout

            def tick() -> None:
                nonlocal safe_until
                if own is None:
                    self._renew_or_warn()
                else:
                    safe_until = self._renew_on(own, report, safe_until)

            try:
                _every(self.renew_interval, stop, tick)
            finally:
                connections.close_all()

    def _renew_or_warn(self) -> bool:
        """
        Renew on the thread's connection for the worker's alias. On failure,
        say so, drop the connection and return False.
        """
        try:
            self.renew_leases()
        except Exception:
            # A missed renewal is survivable by design: the interval is a
            # third of the timeout. Drop the connection so the next tick
            # reconnects, and keep going, because giving up here would
            # silently expire every live lease.
            #
            # Every exception, not a chosen class. Nothing restarts this
            # thread and nothing checks it is alive, and a renewal loop that
            # stops lets every in-flight lease expire.
            # `django.db.InterfaceError` sits beside `DatabaseError` under
            # `django.db.Error`, so a dropped connection, the likeliest
            # failure here, has to be caught too. The consequence of
            # guessing wrong is severe and silent, which is exactly when a
            # guess should not be made.
            logger.warning(
                "Lease renewal failed for worker %s; next retry due within %.1fs",
                self.worker_id,
                self.renew_interval,
                exc_info=True,
                extra={
                    "event": "lease_renew_failed",
                    "worker_id": self.worker_id,
                },
            )
            connections.close_all()
            return False
        return True

    def _renew_on(
        self, own: _OwnConnection, report: _RenewalReport, safe_until: float
    ) -> float:
        """
        One renewal on a pooled PostgreSQL database. Returns the new
        `safe_until`: the earliest a lease this worker holds can expire, on
        time.monotonic(). A lease renewed or taken no earlier than the last
        renewal that succeeded, or than the last tick with nothing to
        renew, lasts lock_timeout from then.

        With nothing in flight, renew_leases() is called without opening
        the thread's own connection first, which for the stock method opens
        nothing. Otherwise the renewal runs on the thread's own connection
        when it is open or can be opened by own.budget from the start of the
        tick, and failing that on a connection from Django's pool, waited
        for at most POOL_FALLBACK_WAIT and given back straight after: the
        connection renewal used before it had one of its own, so a pool with
        one to spare is no worse off than it was. Both happen: the server
        can have no slot left for a connection of the worker's own, and new
        connections can stall while those already open still flow. With
        neither, the renewal is missed and the next tick tries again. A
        tick takes no longer than the deadline and the wait together, so
        ticks never overlap and nothing waits behind them.

        With less of `safe_until` left than an attempt at its own
        connection and the wait for the pool would take, the tick asks the
        pool first, waiting no longer than is left, and tries its own
        connection only when the pool has none. It still tries its own:
        `safe_until` moves only when a renewal succeeds, so once one is
        missed a pool with nothing to spare would otherwise be all any tick
        tried again. A failed renewal statement is not a missing
        connection: it is reported as _renew_or_warn says, and its
        connection is dropped.
        """
        started = time.monotonic()
        with self._in_flight_lock:
            idle = not self._in_flight
        if idle:
            # Nothing to renew, so nothing is opened first. renew_leases() is
            # still called once, as it is without a pool: the stock one
            # returns without a query, and a subclass that overrides it is
            # called every tick. A connection it opens keeps to this tick's
            # deadline. A lease taken from here on lasts lock_timeout from a
            # later instant than this.
            own.connect_by(started + own.budget)
            self._renew_or_warn()
            return started + self.lock_timeout
        renewed = started + self.lock_timeout
        if not own.is_open:
            left = safe_until - started
            hurry = left < own.budget + POOL_FALLBACK_WAIT
            pool_why = ""
            if hurry:
                why = f"not tried first, {max(left, 0.0):.1f}s left on the leases"
                pool_why = self._renew_borrowing(own, safe_until)
                if not pool_why:
                    report.pool(why)
                    return renewed
            try:
                own.open(started + own.budget)
            except Exception as exc:
                why = _reason(exc)
                if not hurry:
                    pool_why = self._renew_borrowing(own, safe_until)
                    if not pool_why:
                        report.pool(why)
                        return renewed
                report.missed(why, pool_why)
                return safe_until
        if not self._renew_or_warn():
            return safe_until
        report.own()
        return renewed

    def _renew_borrowing(self, own: _OwnConnection, safe_until: float) -> str:
        """
        Renew on a connection from the pool, waited for at most
        POOL_FALLBACK_WAIT and not past `safe_until`. Returns "" when the
        renewal went through, or why it did not.
        """
        wait = min(POOL_FALLBACK_WAIT, safe_until - time.monotonic())
        try:
            with own.borrowed(max(wait, POOL_SHORTEST_WAIT)):
                if self._renew_or_warn():
                    return ""
        except Exception as exc:
            return _reason(exc)
        return "the renewal statement failed"

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

    def _write_outcome_reconnecting(
        self,
        db_task: OxTask,
        *,
        status: OxTask.Status,
        duration_ms: int,
        **fields: Any,
    ) -> bool:
        """
        _write_outcome for an attempt's own record, written once more on
        another connection when the first write finds this thread's
        connection to the worker's database gone. Returns what _write_outcome
        returns, or False, having logged it, when the second try fails as
        well.

        The drop before the write, _discard_unusable_connections, catches a
        connection one of the task's statements failed on. It cannot see
        one that died while the task was not using it: the server ended
        the session while the task worked on after its last query, or
        restarted between two tasks while this thread kept a persistent
        connection and the next task made no query. The outcome write is
        then the first statement on the dead connection, and it raised,
        left the row RUNNING with its lease no longer renewed, and the
        reaper ran a finished task again, or requeued a failure with
        neither its error nor its backoff. A probe before every write would
        cost a statement on every outcome and still race with the drop, so
        the write is its own probe, and only its failure costs anything.

        Only a lost connection earns the second try: an OperationalError or
        InterfaceError, after which _outcome_connection_lost holds. A lock
        or statement timeout, a serialization failure or SQLite's "database
        is locked" leave a connection that still answers, which a reconnect
        repairs nothing about, and they are raised as before; so is
        anything inside a caller's transaction, which a close would end.
        The task body never runs again, only the write, with the values
        the caller computed once, run_after and the errors list included.

        Between the two tries the dead connection is closed and, with
        Django's PostgreSQL pool, the pool's idle connections are swept,
        _close_lost_connection. After a restart they are all as dead as
        this one, and the second try would otherwise check one of them out
        and fail the same way. The sweep does not promise the second try a
        fresh connection: a replacement may still be connecting, the
        database may still be down, and a connection the sweep found alive
        can drop the moment after. The second try then fails as the first
        did. There is no third, and no wait before the second.

        A write can commit and still raise, when the connection goes
        between the commit and its reply. Writing it again records nothing
        twice: every field is a value, not an increment, and the landed
        write took the row out of WRITABLE_STATUSES, so the fence matches
        nothing. The fence cannot say why it matched nothing, though, and
        _write_outcome would log a lease loss, and the caller skip
        task_finished, for an outcome that is on the row. So the second
        try first asks whether the row already holds this write,
        _outcome_already_written, and one that does is taken as written.
        A first write the server is still running when the second try
        asks, because the client gave up on a session the server has not
        yet ended, is not seen: the second write waits on its row lock,
        matches nothing once it commits, and the attempt logs a lease loss
        for an outcome that is on the row, once.

        A second failure is logged once, as task_outcome_unrecorded, and
        not raised. The outcome is then unconfirmed, not necessarily
        absent: the first write may have landed, or another recovery path,
        the watchdog's stuck-attempt record or a reaper, may already have
        fenced this attempt. A row that still awaits recovery is the
        reaper's once its lease expires, as before. The line says as much,
        which "Unhandled error executing task" did not. Nothing waits
        between the two tries, so an outage of any length that spans both
        ends here. Whatever the second try raises that is not a database
        error is raised.
        """
        try:
            return self._write_outcome(
                db_task, status=status, duration_ms=duration_ms, **fields
            )
        except (InterfaceError, OperationalError) as exc:
            if not self._outcome_connection_lost():
                raise
            lost = f"{type(exc).__qualname__}: {_reason(exc)}"
        conn = connections[self._db_alias]
        # Outside any atomic block, so the close forgets the dead connection
        # and the next statement checks out or opens another. With Django's
        # pool that other one would be one the pool held idle, which a
        # restart left as dead as this one, so the pool is swept first; even
        # so, the next one is not certain to be alive.
        _close_lost_connection(conn)
        try:
            written = self._outcome_already_written(db_task, status, fields)
            landed = written or self._write_outcome(
                db_task, status=status, duration_ms=duration_ms, **fields
            )
        except Error as exc:
            with suppress(Error):
                conn.close()
            logger.error(
                "Task id=%s path=%s lost its connection to database %r writing "
                "the %s outcome of attempt %d/%d (%s), and a new connection "
                "failed too (%s: %s). The outcome is unconfirmed, not "
                "necessarily absent: the first write may have landed, or "
                "another recovery path may already have fenced this attempt. "
                "If the row still awaits recovery, the reaper handles it once "
                "its lease expires",
                db_task.id,
                db_task.task_path,
                self._db_alias,
                status,
                db_task.attempts,
                db_task.max_attempts,
                lost,
                type(exc).__qualname__,
                _reason(exc),
                exc_info=True,
                extra=self._log_extra(
                    "task_outcome_unrecorded",
                    db_task,
                    duration_ms=duration_ms,
                    dropped_status=str(status),
                ),
            )
            return False
        if written:
            db_task.status = status
            for name, value in fields.items():
                setattr(db_task, name, value)
        if landed:
            logger.warning(
                "Task id=%s path=%s lost its connection to database %r writing "
                "the %s outcome of attempt %d/%d (%s); %s",
                db_task.id,
                db_task.task_path,
                self._db_alias,
                status,
                db_task.attempts,
                db_task.max_attempts,
                lost,
                (
                    "a new connection found it already written"
                    if written
                    else "wrote it on a new connection"
                ),
                extra=self._log_extra(
                    "task_outcome_reconnected",
                    db_task,
                    duration_ms=duration_ms,
                    outcome=str(status),
                    already_written=written,
                ),
            )
        return landed

    def _outcome_connection_lost(self) -> bool:
        """
        Whether this thread's connection to the worker's database is gone,
        and the worker's to replace, after an outcome write raised.

        Inside an atomic block it belongs to whoever opened the block, an
        inline run_once() in a caller's transaction, or an override of
        _write_outcome that wraps it in one, and closing it would end their
        transaction; the error is theirs. A connection a caller took out of
        autocommit is theirs for the same reason. Otherwise it is gone when
        there is none open, because the connect itself failed or the driver
        dropped it, or when is_usable() fails: one probe, only here.
        """
        conn = connections[self._db_alias]
        if conn.in_atomic_block:
            return False
        if conn.connection is None:
            return True
        return bool(conn.autocommit) and not conn.is_usable()

    def _outcome_already_written(
        self, db_task: OxTask, status: OxTask.Status, fields: dict[str, Any]
    ) -> bool:
        """
        Whether the row already holds this outcome write: its status, at
        the epoch the write leaves, with every column it set that compares
        in SQL. The JSON columns are left out, because equality on them
        differs by database, and the rest already pin the write down:
        finished_at or run_after is this process's clock to the
        microsecond, and nothing else writes it at this epoch.
        """
        match = {
            name: value
            for name, value in fields.items()
            if not isinstance(OxTask._meta.get_field(name), JSONField)
        }
        return (
            OxTask.objects.using(self._db_alias)
            .filter(
                pk=db_task.pk,
                status=status,
                **{"lease_epoch": db_task.lease_epoch, **match},
            )
            .exists()
        )

    def execute(self, db_task: OxTask, *, inline: bool = False) -> None:
        """
        Run a claimed (RUNNING, locked) task to a terminal or retry state.

        `inline` says this is running on the caller's own thread rather than
        on the pool, which decides what happens to an exception that was
        aimed at the process rather than at the task. See _run_attempt.

        Per-attempt bookkeeping (started_at, last_attempted_at, worker_ids)
        was already written by the claim UPDATE. The (pk, lease_epoch) pair
        joins the renewal set for the duration, so renewal is attempted
        while this execution runs. The lease holds only while renewal
        reaches the database on time. The pair leaves the set when this
        execution ends.
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
            # before the function is reached, so a receiver that raises must
            # not be charged to the task as an attempt.
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
                # made directly, frame for frame, so the
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
            # A task that caught a database error which ended its connection
            # still succeeded, and this write is the only record of it. On the
            # dead connection it would raise, leave the row RUNNING with its
            # lease no longer renewed, and the reaper would run the task
            # again. Only connections an error left unusable are dropped, so
            # an ordinary success costs no statement here. A connection that
            # died unnoticed is found by the write itself, which then goes
            # once more on another; _write_outcome_reconnecting says when.
            self._discard_unusable_connections()
            if not self._write_outcome_reconnecting(
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
                lease_expires_at=None,
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
        calls the task directly.

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
        monotonic_token = _deadline_monotonic.set(None)
        try:
            try:
                # Registered inside the try, so that a delivery landing
                # between the registration and the call (the watchdog may
                # inject the moment _arm releases the lock) unwinds through
                # the deregistration below like any other.
                watch = self._arm(ident, db_task, timeout, injectable=True)
                _deadline.set(watch.deadline_at)
                _deadline_monotonic.set(watch.deadline)
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
            _deadline_monotonic.reset(monotonic_token)

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
        monotonic_token = _deadline_monotonic.set(watch.deadline)
        try:
            return async_to_sync(run)()
        finally:
            self._disarm(ident)
            _deadline.reset(token)
            _deadline_monotonic.reset(monotonic_token)

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

    def _discard_unusable_connections(self) -> None:
        """
        Close this thread's connections that an error has left unusable, so
        the outcome write that follows checks out or opens another instead
        of failing on the dead one.

        A task can end its own connection and still reach an outcome: a
        statement the server terminated, a failover, a network drop it
        raised through or caught. Django notices only at the end of a
        request, in close_old_connections(), and a worker's attempt is not
        a request, so without this the write runs on the dead connection,
        raises, and leaves the row RUNNING with its lease no longer renewed.

        This is Django's own test from close_old_connections(), narrowed to
        what is certain: a connection with an error since its last commit
        that fails is_usable(). Checking only flagged connections keeps the
        ordinary outcome free of any extra statement, and a flagged one that
        still answers is kept. The price is that a connection that died with
        no statement failing on it, while the task was not using it, is not
        flagged, and the write is the first to find it dead; that write goes
        once more on another connection, _write_outcome_reconnecting.

        Closing a pooled connection hands it back for Django's pool to
        discard, and the write then checks out one the pool held idle.
        After a restart those are as dead as the one closed, so each close
        here also sweeps its alias's pool, _close_lost_connection. That
        does not make the write's connection certain to be alive: the
        database may still be down, and a connection can drop after the
        sweep found it alive. A connection that is kept, or that has no
        pool, costs no sweep.

        One inside an atomic block is left alone: it belongs to whoever
        opened the block, and closing it would end their transaction; an
        inline run_once() inside a caller's transaction is theirs. Every
        alias this thread has opened is checked, not only the worker's: the
        task_finished receivers after the write run on this thread too. Runs
        on the attempt's own thread only, because Django's connections are
        per thread and another thread's are not this one's to close.
        """
        for conn in connections.all(initialized_only=True):
            if conn.connection is None or conn.in_atomic_block:
                continue
            if conn.errors_occurred and not conn.is_usable():
                _close_lost_connection(conn)

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
        # it. extend(map(...)) makes the acquire and its record one
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
        with _outside_the_pool(self._db_alias) as own:
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
                    if stuck:
                        self._record_stuck(own, stuck)
            finally:
                connections.close_all()

    def _record_stuck(self, own: _OwnConnection | None, stuck: list[_Watch]) -> None:
        """
        Record the attempts in `stuck` and recycle as _handle_stuck says,
        as one batch on one connection, which _watchdog_connection acquires
        for the first record and every other record in the batch reuses.

        An attempt whose grace passes while the batch is acquiring its
        connection or recording joins the batch. Attempts that go stuck
        together, their graces milliseconds apart, would otherwise fall
        into batches of their own, and each would wait out an acquisition
        of its own before its recycle.
        """
        # This thread is the whole of the timeout backstop and nothing
        # restarts it mid-attempt, so a failure on one watch must not end it
        # for the others, nor a failure to give the batch's connection back
        # end it at all. _fire_due has already taken these watches out of
        # the table, so the loop carries on rather than retrying a watch
        # whose grace has passed.
        try:
            with self._watchdog_connection(own):
                while stuck:
                    for watch in stuck:
                        try:
                            self._handle_stuck(watch)
                        except Exception:
                            logger.exception(
                                "Worker %s could not record a stuck attempt; "
                                "the timeout backstop continues for the others",
                                self.worker_id,
                                extra={
                                    "event": "watchdog_error",
                                    "worker_id": self.worker_id,
                                },
                            )
                    with self._watch_lock:
                        stuck = self._fire_due()
        except Exception:
            # Every record in the batch has run by now: only giving its
            # connection back failed.
            logger.exception(
                "Worker %s could not give back the connection its timeout "
                "watchdog recorded stuck attempts on; the timeout backstop "
                "continues",
                self.worker_id,
                extra={"event": "watchdog_error", "worker_id": self.worker_id},
            )

    @contextmanager
    def _watchdog_connection(self, own: _OwnConnection | None) -> Iterator[None]:
        """
        Put one connection under a batch of stuck-attempt records: the
        watchdog's own, opened by its deadline as renewal's is, or else one
        from Django's pool, waited for at most POOL_FALLBACK_WAIT. Either is
        acquired once, and no record in the batch connects again: when
        there is neither, or the one there is stops working part way, every
        record after that fails at once, and each recycle still happens.
        So the recycle is delayed by at most one bounded acquisition
        sequence, whatever the batch's size, except that resolving a host
        name is not bounded by the deadline. The connection is closed, or
        given back to the pool, when the batch ends, however it ends; the
        next batch acquires one again. Without a pool the batch runs on the
        thread's ordinary connection, as Django opens it.
        """
        if own is None:
            yield
            return
        with ExitStack() as scope:
            scope.callback(own.close)
            try:
                own.open(time.monotonic() + own.budget)
            except Exception as exc:
                try:
                    scope.enter_context(
                        own.borrowed(POOL_FALLBACK_WAIT, reconnect=False)
                    )
                except Exception as pool_exc:
                    logger.warning(
                        "Worker %s's timeout watchdog has no connection to "
                        "record a stuck attempt on: not its own (%s), and "
                        "none from the connection pool (%s)",
                        self.worker_id,
                        _reason(exc),
                        _reason(pool_exc),
                        extra={
                            "event": "watchdog_connection_unavailable",
                            "worker_id": self.worker_id,
                            "error": _reason(exc),
                            "fallback_error": _reason(pool_exc),
                        },
                    )
            own.connect_by(-math.inf)
            yield

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
        except Exception:
            # Every exception, not a chosen class. The thread is wedged
            # whether or not this write succeeds, and the recycle below is
            # what stops the worker keeping a dead pool slot and waiting for
            # that thread forever; anything that escapes here skips it.
            #
            # `django.db.Error` is not a wide enough net. Django raises
            # ValueError from adapt_datetimefield_value on a naive value, and
            # a driver can raise UnicodeEncodeError on a traceback character
            # the column's charset cannot hold. Neither is a database error
            # by Django's taxonomy, and both reach this path.
            logger.warning(
                "Worker %s could not record the stuck attempt for task id=%s; "
                "recycling anyway",
                self.worker_id,
                db_task.id,
                exc_info=True,
                extra=self._log_extra("task_stuck_unrecorded", db_task),
            )
        # Whether the write landed and whether the thread is stuck are
        # different questions. A lost write can mean the thread's own
        # outcome landed first, so it did come back; it can equally mean a
        # reaper requeued the row underneath us while the thread runs on.
        # Only the thread knows whether it is still inside this attempt, so
        # ask it; the answer does not depend on any database write.
        #
        # Written under the lock `_stuck_alive` iterates it under. The drain
        # is what holds the process open while a stuck attempt is still
        # running, so its iteration must never see this dict change size.
        with self._in_flight_lock:
            still_running = self._running_on.get(watch.ident) == watch.attempt
            if still_running:
                self._stuck[watch.ident] = watch.attempt
        if still_running:
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

        if not release:
            # A task can end its own connection and then fail, often because
            # of it: a statement the server terminated, a network drop it
            # raised through. The write below runs on this thread's
            # connection for the worker's alias, and on the dead one it
            # raises: the row stays RUNNING with its lease no longer renewed,
            # the error is never recorded, and the reaper later requeues the
            # row with no backoff, or marks it LOST on its last attempt. The
            # stuck-thread record, release=True, runs on the watchdog's own
            # connection while the task's thread may still be using its
            # own, which is not the watchdog's to close.
            self._discard_unusable_connections()
        # The attempt's own record goes once more on another connection
        # when the write finds this thread's connection gone; the stuck-thread
        # record is the watchdog's, on its own connection, and keeps the
        # single write it always had.
        write = self._write_outcome if release else self._write_outcome_reconnecting

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
                "traceback": _stored_traceback(exc),
            },
        ]

        if db_task.attempts >= db_task.max_attempts:
            if not write(
                db_task,
                status=OxTask.Status.FAILED,
                duration_ms=duration_ms,
                errors=errors,
                # Process time, not the lease clock; _lease_now says why.
                finished_at=timezone.now(),
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
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
            # `attempts` is a PositiveSmallIntegerField and can reach 32767,
            # and 2 ** 32766 overflows on the way to a float the min() would
            # have discarded. This runs on the failure path, where raising
            # would leave the row RUNNING for the reaper.
            #
            # Capping at 64 doublings is far past any backoff_max anyone
            # configures and keeps the arithmetic in range.
            doublings = min(max(db_task.attempts - 1, 0), 64)
            delay = min(self.backoff_initial * (2**doublings), self.backoff_max)
            if not write(
                db_task,
                status=OxTask.Status.READY,
                duration_ms=duration_ms,
                errors=errors,
                run_after=timezone.now() + timedelta(seconds=delay),
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
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
        # The row's own expiry decides, and this worker's timeout only decides
        # for a row that has none. Deriving the deadline here instead means
        # every reaper answers with its own configuration, so a rolling deploy
        # that changes LOCK_TIMEOUT puts two answers in one fleet and the
        # shorter one reclaims live work from a worker renewing correctly on
        # the longer.
        #
        # NULL is a lease taken before the column existed. Those keep the old
        # comparison until their first renewal fills the column in, which is
        # what lets a fleet upgrade one worker at a time with nothing to run.
        # An expiry older than the row's own locked_at was written by an
        # earlier holder and left in place by a worker that does not know the
        # column; it says nothing about the current lease, so the row is
        # judged on locked_at like one with no expiry at all.
        stuck = OxTask.objects.using(self._db_alias).filter(
            self._abandoned_lease_q(cutoff),
            status=OxTask.Status.RUNNING,
        )
        return self._requeue_abandoned(stuck) + self._abandon_exhausted(stuck, cutoff)

    @staticmethod
    def _abandoned_lease_q(cutoff: Any) -> Q:
        """
        The lease looks abandoned: its stored expiry has passed, or it has no
        usable expiry and its lock is older than the cutoff.
        """
        # Shaped so the planner can seek ox_reaper_expiry_idx on the expiry
        # range: the expiry has passed, and either it belongs to this lease
        # (not older than the lock) or the lock itself is past the cutoff.
        return Q(lease_expires_at__lt=_lease_now()) & (
            Q(lease_expires_at__gte=F("locked_at")) | Q(locked_at__lt=cutoff)
        ) | Q(lease_expires_at__isnull=True, locked_at__lt=cutoff)

    def _requeue_abandoned(self, stuck: QuerySet[OxTask]) -> int:
        """
        Put abandoned rows that still have attempts back on the queue.

        One UPDATE per pass, whatever the number of rows, and it carries the
        full lease predicate rather than trusting the read that chose them: a
        lease renewed in between no longer matches, so the row stays with the
        worker that renewed it.

        The cost is the reason. Every worker reaps on its own interval, so a
        fleet that has just lost half its members runs this on every survivor
        at once, against a database still recovering. A statement per row,
        times the workers, is a second outage on top of the first.

        Two bounds, and both of them matter:

        - The pass takes at most `reap_batch` rows, so memory and log volume
          are bounded whatever the size of the stuck set. The remainder goes
          on the next pass.
        - The UPDATE is restricted to the ids that were read. That is what
          makes the record exact. `cutoff` is a database expression evaluated
          per statement, so without the restriction the UPDATE's cutoff is
          later than the SELECT's and rows can *enter* the set as well as
          leave it. One entering and one leaving keeps the counts equal, and
          the pass then names a task it did not reclaim, with its live holder
          in `held_by`, in the one record an operator reads to explain a task
          that ran twice.

        Pinned to those ids the set can only shrink, so equal counts really do
        mean the manifest describes the write. When it shrank, the pass
        reports the count and names nobody.
        """
        requeue = stuck.filter(attempts__lt=F("max_attempts"))
        manifest = list(
            requeue.order_by("id").values_list(
                "id", "task_path", "queue_name", "attempts", "locked_by"
            )[: self.reap_batch]
        )
        if not manifest:
            return 0
        requeued = requeue.filter(pk__in=[row[0] for row in manifest]).update(
            status=OxTask.Status.READY,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
            lease_epoch=F("lease_epoch") + 1,
        )
        if not requeued:
            return 0
        if requeued == len(manifest):
            for task_id, task_path, queue_name, attempts, held_by in manifest:
                logger.warning(
                    "Reclaimed stuck task id=%s path=%s (attempt %d) -> %s",
                    task_id,
                    task_path,
                    attempts,
                    OxTask.Status.READY,
                    extra={
                        "event": "task_reclaimed",
                        "task_id": str(task_id),
                        "task_path": task_path,
                        "queue": queue_name,
                        "attempt": attempts,
                        "worker_id": self.worker_id,
                        # The reaper's own id answers "who noticed". The
                        # question an operator is actually asking of this
                        # record is "who stopped", and only the row knows.
                        "held_by": held_by,
                        "status": str(OxTask.Status.READY),
                    },
                )
        else:
            logger.warning(
                "Reclaimed %d stuck task(s) -> %s; leases were renewed during "
                "the pass, so this record names none of them",
                requeued,
                OxTask.Status.READY,
                extra={
                    "event": "task_reclaimed",
                    "worker_id": self.worker_id,
                    "status": str(OxTask.Status.READY),
                    "count": requeued,
                },
            )
        return requeued

    def _abandon_exhausted(
        self, stuck: QuerySet[OxTask], cutoff: datetime | CombinedExpression
    ) -> int:
        """
        Mark abandoned rows whose attempts are spent LOST.

        This branch writes something specific to each row, the holder that
        went quiet appended to that row's own error list, so it cannot
        collapse into a single statement the way the requeue does. It is
        bounded instead: `reap_batch` rows per pass, the rest on the next
        one. Exhausting every attempt is the exception, and a bounded walk
        keeps a reaper's cost independent of how many workers died at once.

        Reading first means the write has to re-ask what the read assumed.
        Two predicates, two races: the epoch catches a handover, and the
        expiry catches a renewal, which the epoch cannot see because
        renewing writes locked_at and lease_expires_at and nothing else.
        """
        lost = 0
        candidates = (
            stuck.filter(attempts__gte=F("max_attempts"))
            .only(
                "id",
                "task_path",
                "queue_name",
                "attempts",
                "max_attempts",
                "locked_by",
                "errors",
                "lease_epoch",
            )
            .order_by("id")[: self.reap_batch]
        )
        for db_task in candidates:
            changed = (
                OxTask.objects.using(self._db_alias)
                .filter(
                    self._abandoned_lease_q(cutoff),
                    pk=db_task.pk,
                    status=OxTask.Status.RUNNING,
                    lease_epoch=db_task.lease_epoch,
                )
                .update(
                    status=OxTask.Status.LOST,
                    locked_by=None,
                    locked_at=None,
                    lease_expires_at=None,
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
                                f"its lease on this task; the lease expired "
                                f"with no attempts remaining. What the "
                                f"attempt did was never "
                                f"observed: it may have succeeded, it may have "
                                f"failed, it may not have got that far. This "
                                f"record is the lost lease, not a cause."
                            ),
                        },
                    ],
                )
            )
            if changed:
                lost += 1
                logger.warning(
                    "Reclaimed stuck task id=%s path=%s (attempt %d/%d) -> %s",
                    db_task.id,
                    db_task.task_path,
                    db_task.attempts,
                    db_task.max_attempts,
                    OxTask.Status.LOST,
                    extra=self._log_extra(
                        "task_reclaimed",
                        db_task,
                        status=str(OxTask.Status.LOST),
                        held_by=db_task.locked_by,
                    ),
                )
        return lost

    # -- scheduling --------------------------------------------------------

    def _latest_ticks(
        self, schedules: list[Schedule], since: datetime
    ) -> dict[str, datetime]:
        """
        Latest recorded tick per dispatch key, for the pass's schedules,
        counting only ticks at or after `since`.

        The bound is what keeps this cheap. Asked for the newest tick per
        schedule over all of history, no database can seek to it: PostgreSQL
        reads the whole tick table and hash-aggregates it, MySQL scans the
        unique index end to end. That is a cost proportional to everything
        ever dispatched, paid on every pass by every worker, and `ox_prune`
        is the only thing holding it down.

        `since` is the oldest tick any of the pass's schedules is currently
        asking about, so the answer this method exists to give is complete
        within it, and (schedule_name, scheduled_for) turns into a range the
        index can seek. A schedule whose newest tick predates the bound is
        absent from the result, which reads as "nothing recorded for the
        tick in question", which is exactly what the caller does with it, and
        the caller distinguishes that from a schedule with no ticks at all by
        asking.

        One parameter per key, plus the bound. SQLite before 3.32.0 refuses
        a statement carrying more than 999, and Django splits an IN list
        only where the backend declares a maximum, which is Oracle. So the
        keys are read in slices of what the connection allows, and a
        backend that declares no limit answers None and reads them in one.
        """
        keys = [schedule.key for schedule in schedules]
        limit = connections[self._db_alias].features.max_query_params
        step = max(limit - 1, 1) if limit else max(len(keys), 1)
        latest: dict[str, datetime] = {}
        for start in range(0, len(keys), step):
            latest.update(
                {
                    row["schedule_name"]: row["latest"]
                    for row in OxScheduleTick.objects.using(self._db_alias)
                    .filter(
                        schedule_name__in=keys[start : start + step],
                        scheduled_for__gte=since,
                    )
                    .values("schedule_name")
                    .annotate(latest=Max("scheduled_for"))
                }
            )
        return latest

    def _anchor_boundary(self, key: str, own_pk: int, now: datetime) -> datetime | None:
        """
        The earliest tick recorded for `key` other than this pass's own row,
        or None when there is none, which makes this pass the first sighting.

        Called inside the dispatch transaction, after this pass's tick row
        is in. That row is excluded because it is visible in here, and
        counting it would make every schedule look like it already had
        history.

        A plain read cannot answer "none". Two workers whose first passes
        fall either side of a minute boundary, or whose clocks differ by
        one, hold different candidate ticks: their INSERTs take different
        keys, so the constraint serialises neither, and neither's read sees
        the other's uncommitted row. Both anchor, and the later instant is
        claimed with no task and never fires. The constraint does serialise
        workers that write the same key, so a pass that reads no history
        writes one: a latch row at a fixed instant per schedule, inserted
        and deleted again inside this transaction. The second worker's
        latch INSERT waits for the first's transaction to end, as a unique
        index does with an uncommitted duplicate. Once that commits, its
        latch is gone, this INSERT goes through, and the read repeated
        after it finds the anchor. The repeat is a locking read where the
        database has one, so it is a current read rather than this
        transaction's snapshot, and it skips locked rows where it can: a
        row another worker has inserted and not committed is locked by
        that worker, who may be waiting on this latch, and a locking read
        that waited on it would deadlock. Skipping it is right in any
        case, since an uncommitted row is not history. Every committed
        row is unlocked, because the only thing that locks tick rows is
        this read, and the latch admits one of it at a time.

        Deleted rather than released with a savepoint. What the other
        worker waits on is the transaction that owns the tuple; rolling a
        savepoint back ends that wait before the anchor is committed, and
        a delete does not.

        The latch never commits, so nothing ever reads it: not the bounded
        tick read, not ox_prune, not the admin. SQLite has one writer, so
        the tick INSERT already serialises first sightings there, and the
        latch is two statements that change nothing.
        """
        others = (
            OxScheduleTick.objects.using(self._db_alias)
            .filter(schedule_name=key)
            .exclude(pk=own_pk)
            .order_by("scheduled_for")
            .values_list("scheduled_for", flat=True)
        )
        earliest = others.first()
        if earliest is not None:
            return earliest
        latch = OxScheduleTick.objects.using(self._db_alias).create(
            schedule_name=key,
            scheduled_for=_latch_instant(),
            task_id=None,
            created_at=now,
        )
        others = others.exclude(pk=latch.pk)
        features = connections[self._db_alias].features
        if features.has_select_for_update:
            others = others.select_for_update(
                skip_locked=features.has_select_for_update_skip_locked
            )
        earliest = others.first()
        OxScheduleTick.objects.using(self._db_alias).filter(pk=latch.pk).delete()
        return earliest

    def _log_tick_dropped(
        self, schedule: Schedule, scheduled_for: datetime, now: datetime
    ) -> None:
        """
        Record a tick the starting deadline dropped.

        A dropped tick is a decision, not an absence, so it is logged with
        its lateness and can be alerted on. Once per tick per worker: the
        same tick stays droppable until the next one comes due, and an
        alertable event that repeats once a second is an alert nobody can
        act on.
        """
        if self._dropped_reported.get(schedule.key) == scheduled_for:
            return
        self._dropped_reported[schedule.key] = scheduled_for
        late = (now - scheduled_for).total_seconds()
        logger.warning(
            "Dropped schedule %s tick %s: %.0fs late, past its starting deadline",
            schedule.name,
            scheduled_for.isoformat(),
            late,
            extra={
                "event": "schedule_tick_dropped",
                "schedule": schedule.name,
                "scheduled_for": scheduled_for.isoformat(),
                "late_seconds": late,
                "worker_id": self.worker_id,
            },
        )

    def _still_due(
        self,
        current: Schedule,
        scheduled_for: datetime,
        local_now: datetime,
        now: datetime,
    ) -> bool:
        """
        Does this schedule, as it stands now, still want this exact tick?

        Recomputed rather than re-checked column by column. A retimed
        schedule answers a different instant, a moved boundary excludes it,
        a tightened deadline drops it, and a schedule whose timing was
        changed by a route that touches no model code is caught the same
        way as one changed through the admin, because the answer comes from
        the columns rather than from anything a write path remembered to
        update.

        `local_now` is the snapshot the tick was derived from, so the tick
        recomputed here is the one the caller holds. `now` is the clock as
        it stands when the question is asked, which under the row's lock is
        later than the snapshot by however long the lock took; the deadline
        is judged against that.
        """
        tick = current.trigger.previous(local_now)
        if tick is None:
            return False
        due = timezone.make_aware(tick) if settings.USE_TZ else tick
        if due != scheduled_for:
            return False
        if current.start_time is not None and scheduled_for < current.start_time:
            return False
        if current.end_time is not None and scheduled_for > current.end_time:
            return False
        return not (
            current.starting_deadline is not None
            and now - scheduled_for > current.starting_deadline
        )

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
        schedules = self._schedule_source.schedules()
        if not schedules:
            return 0
        now = timezone.now()
        # Cron fields describe wall-clock time in the project's timezone.
        local_now = (
            timezone.localtime(now).replace(tzinfo=None) if settings.USE_TZ else now
        )
        # Every schedule's due tick first, so the tick log is read once and
        # only as far back as the oldest of them.
        due: list[tuple[Schedule, datetime]] = []
        for schedule in schedules:
            tick = schedule.trigger.previous(local_now)
            if tick is None:
                # A one-shot trigger whose instant has not arrived. Nothing
                # is due and nothing is recorded, so it stays a candidate.
                continue
            scheduled_for = timezone.make_aware(tick) if settings.USE_TZ else tick
            if scheduled_for > now:
                # A tick is the latest instant at or before now, so one in
                # the future is not due and must not be enqueued. It can
                # arise where a local time does not exist: on the day a
                # zone springs forward, a wall clock of 02:30 in a
                # one-hour-past-the-hour sequence is a time that never
                # happens, and attaching the zone to it resolves to 03:30,
                # which has not arrived. Enqueueing there would run the
                # task early and stamp it with an instant that suppresses
                # the real 03:30 tick when it comes.
                continue
            due.append((schedule, scheduled_for))
        if not due:
            return 0
        latest = self._latest_ticks(
            schedules, min(scheduled_for for _, scheduled_for in due)
        )
        dispatched = 0
        for schedule, scheduled_for in due:
            if schedule.start_time is not None and scheduled_for < schedule.start_time:
                # Before the schedule existed, or before it was retimed.
                continue
            if schedule.end_time is not None and scheduled_for > schedule.end_time:
                continue
            last = latest.get(schedule.key)
            if last is not None and scheduled_for <= last:
                if last <= now:
                    continue
                # The newest tick in the log is in the future, which a
                # clock-skewed worker's write can leave behind. That must not
                # suppress ticks which are due now, so the comparison against
                # the newest one cannot decide this. Ask about this instant
                # instead: if it has already been recorded, it has run.
                #
                # One extra query, and only while a future tick is the newest
                # one. In ordinary operation the comparison above answers.
                if (
                    OxScheduleTick.objects.using(self._db_alias)
                    .filter(schedule_name=schedule.key, scheduled_for=scheduled_for)
                    .exists()
                ):
                    continue
            # After the suppression, not before. A tick that already fired
            # is not a tick that was dropped, and checking the deadline
            # first would report one as dropped on every pass until its
            # next tick came due, which for a daily schedule is a warning
            # a second for a day.
            if schedule.starting_deadline is not None and (
                now - scheduled_for > schedule.starting_deadline
            ):
                self._log_tick_dropped(schedule, scheduled_for, now)
                continue
            result = None
            # Set once this pass's tick row is in. An IntegrityError arriving
            # with it clear is the INSERT losing the constraint; with it set,
            # a later statement's.
            claimed = False
            # Set by the first transaction.on_commit callback of the block,
            # registered before anything a task_enqueued receiver can add.
            # Django runs the callbacks in order once the commit is through,
            # and does not guard them, so an exception arriving with this
            # set was raised by a receiver's callback after the transaction
            # committed, whatever its class. One arriving with it clear came
            # before the commit or from the commit itself, and the handlers
            # below read its class. A flag set at the end of the body could
            # not tell those apart: a commit that fails has run the whole
            # body too.
            committed = False

            def mark_committed() -> None:
                nonlocal committed
                committed = True

            try:
                try:
                    with transaction.atomic(using=self._db_alias):
                        transaction.on_commit(mark_committed, using=self._db_alias)
                        # The definition as it stands now, under its own lock.
                        # Everything above this line was a filter over a snapshot
                        # that may be minutes old; everything below decides from
                        # the row itself.
                        #
                        # The lock is taken before the enqueue rather than after.
                        # The enqueue itself is an INSERT that takes no lock on
                        # this row, so for it there is no order to invert, and
                        # deciding first means a refused tick commits nothing:
                        # on PostgreSQL and MySQL it writes nothing at all, and
                        # on SQLite the lock is itself a no-op UPDATE that the
                        # rollback takes back. A task_enqueued receiver is
                        # another matter:
                        # it runs inside this transaction, under this lock, and
                        # can take locks of its own. The documented rule for
                        # receivers is that they take none a transaction that
                        # writes schedules may hold, because that pair deadlocks.
                        current = (
                            schedule.refresh()
                            if schedule.refresh is not None
                            else schedule
                        )
                        # The clock again, now that the lock is held. Waiting
                        # for it takes as long as whoever holds it: another
                        # dispatcher's enqueue and its receivers, an admin save,
                        # up to the lock-wait timeout on MySQL. The starting
                        # deadline is a promise about how late a run may begin,
                        # and a clock read before the wait cannot keep it. The
                        # tick itself is still the snapshot's: which instant is
                        # due comes from the timing, not from when the lock was
                        # granted.
                        admitted_at = timezone.now()
                        if current is None or not self._still_due(
                            current, scheduled_for, local_now, admitted_at
                        ):
                            raise _NotAdmitted
                        # The tick row goes in first, before anything is
                        # enqueued. Every worker derives the same tick times, so
                        # on every tick all of them reach this line and exactly
                        # one INSERT survives the unique constraint. Claiming the
                        # instant before doing the work means the losers do no
                        # work: they raise here and enqueue nothing.
                        #
                        # enqueue() saves and fires task_enqueued before any outer
                        # rollback could unwind it, which is why the constraint
                        # has to decide before the enqueue and not after.
                        #
                        # Through the alias this transaction was opened on. The
                        # INSERT and the enqueue have to commit or roll back
                        # together: that is what makes the constraint the
                        # coordination mechanism. Left to its own routing this
                        # row could land outside the transaction, and a tick
                        # recorded without its task is a tick nothing will run
                        # again. django_ox.E008 checks that the two models
                        # resolve to one database.
                        tick_row = OxScheduleTick.objects.using(self._db_alias).create(
                            schedule_name=current.key,
                            scheduled_for=scheduled_for,
                            task_id=None,
                            created_at=now,
                        )
                        claimed = True
                        # A schedule that anchors records its first sighting
                        # without firing, because first observation is the only
                        # boundary it has. One carrying its own start_time was
                        # given a boundary when it was created, so its first due
                        # tick fires even if no worker saw the schedule appear.
                        #
                        # Whether this is the first sighting is decided here,
                        # from the log, rather than from the snapshot taken
                        # before the loop. Another worker can commit this
                        # schedule's anchor in between, and then this pass is
                        # not the first sighting at all: anchoring again writes
                        # a second no-task row, this time over a tick that had a
                        # boundary and should have fired. The constraint then
                        # holds that instant for good, and the run it was for
                        # never happens. _anchor_boundary says how two workers
                        # holding different ticks are kept from both anchoring.
                        #
                        # Asked only while the bounded read found nothing for
                        # this schedule. One whose newest tick is at or after
                        # the bound has `last` set and skips it; one whose
                        # newest tick predates the bound pays the one indexed
                        # read on every dispatch, and a schedule alone in its
                        # pass, whose bound is its own due tick, is always in
                        # that position. It is not "only until it has a tick".
                        first_sighting = False
                        if last is None and current.anchors:
                            anchor = self._anchor_boundary(
                                current.key, tick_row.pk, now
                            )
                            if anchor is None:
                                first_sighting = True
                            elif scheduled_for < anchor:
                                # A tick from before the schedule's anchor: this
                                # worker's clock, or its pass, is behind the one
                                # that saw the schedule first. The anchor is the
                                # boundary, and nothing before it fires.
                                raise _NotAdmitted
                        if not first_sighting:
                            result = current.task.enqueue(
                                *current.args, **current.kwargs
                            )
                            tick_row.task_id = result.id
                            tick_row.save(using=self._db_alias, update_fields=["task"])
                except Exception:
                    if not committed:
                        raise
                    # The transaction is committed and a
                    # transaction.on_commit callback, registered by a
                    # task_enqueued receiver, raised. Read by its class it
                    # would count as a lost race, contention, or a database
                    # fault that ends the pass, and a task that exists would
                    # go uncounted. The task exists and the tick is
                    # recorded, so the dispatch stands and the callback's
                    # failure is its own event.
                    logger.warning(
                        "Schedule %s dispatched, then a post-commit callback failed",
                        schedule.name,
                        exc_info=True,
                        extra={
                            "event": "schedule_dispatch_callback_failed",
                            "schedule": schedule.name,
                            "task_id": str(result.id) if result is not None else None,
                            "worker_id": self.worker_id,
                        },
                    )
            except _NotAdmitted:
                # The schedule changed between the read and now (disabled,
                # retimed, or gone), or the tick predates its anchor. The
                # rollback took the tick row with it, and the tick stays
                # unclaimed so a worker with a current view can still act
                # on it.
                continue
            except IntegrityError:
                # Two things raise this inside the block, told apart by how
                # far the block had got. Before the tick row is in, it is the
                # INSERT itself: another worker claimed this tick first, its
                # INSERT won, and ours rolled back before it enqueued
                # anything. Silent, and the ordinary case on every tick with
                # more than one worker. Once the row is in, the failure is a
                # later statement's, the enqueue's or the latch's, and
                # reading it as a lost race would retry it silently for as
                # long as it kept failing. Asking the log whether the tick
                # row exists cannot tell them apart: a winner committing
                # between the rollback and that read made a real failure
                # look like a lost race.
                if claimed:
                    logger.exception(
                        "Schedule %s could not be dispatched this pass",
                        schedule.name,
                        extra={
                            "event": "schedule_dispatch_error",
                            "schedule": schedule.name,
                            "worker_id": self.worker_id,
                        },
                    )
                continue
            except OperationalError as exc:
                # The database gave up waiting for a lock: another worker
                # held this tick's unique row, or the schedule's row, for
                # longer than the engine's patience. That is a lost race
                # with a slow winner, not a broken schedule, so it is one
                # warning without a traceback, and the tick fires on a later
                # pass if it is still unclaimed. The stored source treats a
                # timeout on its own row lock the same way.
                if not lock_contention(exc):
                    raise
                logger.warning(
                    "Could not claim schedule %s this pass, the database gave up "
                    "waiting for a lock: %s",
                    schedule.name,
                    exc,
                    extra={
                        "event": "schedule_lock_unavailable",
                        "schedule": schedule.name,
                        "worker_id": self.worker_id,
                    },
                )
                continue
            except DatabaseError:
                # The database, not the schedule: a connection gone away, a
                # server refusing a statement, a lock the engine gave up
                # waiting for. Read as one bad row, it was logged against
                # whichever schedule was in hand, with a traceback, once per
                # schedule, while the pass returned as if it had succeeded
                # and the handler in run() written for exactly this never
                # ran. It goes there instead.
                raise
            except Exception:
                # Anything else at all. A schedule read from a row is
                # input from a person, and the guarantee that one bad
                # row cannot stop the others has to hold for the
                # exception nobody predicted as much as for the ones
                # that were.
                logger.exception(
                    "Schedule %s could not be dispatched this pass",
                    schedule.name,
                    extra={
                        "event": "schedule_dispatch_error",
                        "schedule": schedule.name,
                        "worker_id": self.worker_id,
                    },
                )
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

    def _warn_if_the_connection_pool_is_short(self) -> None:
        """
        Say so at startup when Django's PostgreSQL connection pool for this
        worker's database cannot give every task thread and the poll loop a
        connection at the same time.

        Each task thread holds a connection for the whole of its attempt and
        the poll loop holds one for the life of the process; the renewal and
        watchdog threads connect outside the pool and are not counted. Below
        that, task queries and outcome writes wait for a connection, time
        out and are retried. A warning rather than a refusal, because such
        configurations ran before and still run. It counts this alias alone,
        not other aliases, connections tasks open themselves or the server's
        own limit.
        """
        pool = _pool_options(self._db_alias)
        if pool is None or connections[self._db_alias].vendor != "postgresql":
            return
        # psycopg_pool reads a missing max_size as min_size, and min_size
        # defaults to 4.
        max_size = pool.get("max_size")
        if max_size is None:
            max_size = pool.get("min_size", 4)
        if not isinstance(max_size, int) or isinstance(max_size, bool) or max_size < 1:
            # Only a whole number of connections is compared. psycopg_pool
            # itself refuses None, a string or a size below 1 when Django
            # opens the pool, and this warning must never be what stops the
            # worker.
            return
        needed = self.concurrency + 1
        if max_size >= needed:
            return
        if self.timeouts.enabled:
            unpooled = 2
            outside = (
                "Lease renewal and the timeout watchdog normally use private "
                "connections outside the pool; budget 2 additional connections"
            )
        else:
            unpooled = 1
            outside = (
                "Lease renewal normally uses a private connection outside the pool; "
                "budget 1 additional connection"
            )
        logger.warning(
            "Worker %s: Django's PostgreSQL connection pool for database %r "
            "has a connection limit of %d. Allow at least %d pooled connections "
            "at concurrency %d: one per task thread and one for the poll loop. "
            "Task queries and outcome writes can time out waiting for a connection. "
            "Tasks can be retried and repeat side effects. "
            "Set max_size in OPTIONS['pool'] to at least %d for task-thread "
            "and poll-loop capacity. This does not reserve fallback capacity "
            "or prove that the server has enough slots. Budget all processes, "
            "aliases, private connections and other clients. Account for reserved "
            "slots and role limits. Pool fallback adds resilience, not capacity. "
            "%s per worker process.",
            self.worker_id,
            self._db_alias,
            max_size,
            needed,
            self.concurrency,
            needed,
            outside,
            extra={
                "event": "connection_pool_too_small",
                "worker_id": self.worker_id,
                "database": self._db_alias,
                "concurrency": self.concurrency,
                "max_size": max_size,
                "recommended_max_size": needed,
                "unpooled_connections": unpooled,
            },
        )

    def run_once(self) -> bool:
        """
        Claim and execute a single task inline. Returns True if one ran.

        Renewal is attempted for the duration, as for a task on the worker's
        thread pool: a renewal thread is started for this call and stopped
        before it returns. A task that outlives LOCK_TIMEOUT keeps its lease
        only while renewal reaches the database on time. With Django's
        PostgreSQL pool, renewal needs a connection; 1.4.0 opens one outside
        the pool with a bounded deadline and a short pooled fallback.
        Sizing LOCK_TIMEOUT alone does not prevent a reclaim.
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

    def _limit_reached(self) -> bool:
        return self.max_tasks is not None and self._claimed >= self.max_tasks

    def _complete(self, event: str, message: str) -> None:
        # A stop already under way came from outside, and it is that stop
        # the log should record rather than a completion that did not decide
        # anything.
        if self._stop.is_set():
            return
        logger.info(
            message,
            self.worker_id,
            self._claimed,
            extra={
                "event": event,
                "worker_id": self.worker_id,
                "claimed": self._claimed,
            },
        )
        self.request_stop()

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
        self._warn_if_the_connection_pool_is_short()
        in_flight: set[Future[None]] = set()
        last_reap = 0.0
        last_dispatch = 0.0
        # Set by a failed schedule dispatch and cleared only by one that
        # succeeds, not per pass. Dispatch runs once per schedule_interval,
        # at least a second by default, so under a shorter poll the passes
        # after a failure do not dispatch at all, and --batch must not end
        # on one of them while a due tick may never have been enqueued.
        dispatch_owed = False
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
                    # Not gated on a schedule list read at start-up: a source
                    # whose answer changes would never be asked again after
                    # starting empty. dispatch_schedules() asks the source and
                    # returns immediately when it has nothing, which for the
                    # default settings source is one list check.
                    if time.monotonic() - last_dispatch >= self.schedule_interval:
                        try:
                            self.dispatch_schedules()
                        except DatabaseError:
                            # Any statement in the pass can raise this: the
                            # bounded tick read, a row lock, the tick INSERT,
                            # the enqueue. The cause is the database rather
                            # than a schedule, so the loop lets it out
                            # instead of logging it against whichever
                            # schedule was in hand. A pass lost this way is
                            # recoverable at the next one, and the claim
                            # below still runs this pass. A connection that
                            # is no longer usable is dropped first, so the
                            # claim reconnects rather than failing on it too
                            # and costing the whole poll pass.
                            logger.warning(
                                "Schedule dispatch failed; retrying next pass",
                                exc_info=True,
                                extra={
                                    "event": "schedule_dispatch_failed",
                                    "worker_id": self.worker_id,
                                },
                            )
                            close_old_connections()
                            dispatch_owed = True
                        else:
                            dispatch_owed = False
                        last_dispatch = time.monotonic()
                    in_flight = {f for f in in_flight if not f.done()}
                    # Read before claiming, not after: a task still running
                    # when the claim finds nothing can enqueue work and
                    # finish before a later look, which would then see an
                    # idle worker and an empty queue that is not empty.
                    idle = not in_flight
                    claimed_any = False
                    found_nothing = False
                    while (
                        len(in_flight) < self.concurrency
                        and not self._stop.is_set()
                        and not self._limit_reached()
                    ):
                        self._claim_contended = False
                        db_task = self.claim_one()
                        if db_task is None:
                            # A claim that lost every race found a busy
                            # queue rather than an empty one, so --batch
                            # polls again instead of ending on it.
                            found_nothing = not self._claim_contended
                            break
                        claimed_any = True
                        self._claimed += 1
                        in_flight.add(executor.submit(self._execute_in_thread, db_task))
                except Error:
                    # django.db.Error rather than DatabaseError: InterfaceError
                    # sits beside DatabaseError under Error, and a connection
                    # dropped underneath the worker is the likeliest failure
                    # here.
                    #
                    # The envelope this package publishes says the database may
                    # go away and come back, and every loop here honours it:
                    # the renewal thread rides it out, an attempt rides it
                    # out, and so does this one. Letting the error out of
                    # run() would hand the supervisor a death per blip, and
                    # five in a minute stop it for good.
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
                    # With Django's pool, a restart leaves every connection it
                    # holds idle as dead as the one this pass failed on, and
                    # without CONN_HEALTH_CHECKS it hands them out unchecked:
                    # one to each pass, so the loop claimed and dispatched
                    # nothing for a poll interval per dead connection. With
                    # them, a checkout tests each in turn, waiting longer
                    # after each, and could time out before it found a live
                    # one. One sweep on the failed pass discards them all,
                    # though the next pass can still fail: the database may
                    # still be down, or a connection drop after the sweep.
                    # It replays nothing: reap, dispatch and claim wait for
                    # the next pass as before. A claim that raised may still
                    # have committed, and its row waits out its lease as it
                    # always did.
                    _sweep_pool(connections[self._db_alias])
                    self._stop.wait(self.poll_interval)
                    continue
                if self._limit_reached():
                    self._complete(
                        "worker_max_tasks_reached",
                        "Worker %s reached its task limit after %d claim(s); stopping",
                    )
                    continue
                if (
                    self.batch
                    and idle
                    and found_nothing
                    and not claimed_any
                    and not dispatch_owed
                ):
                    self._complete(
                        "worker_batch_empty",
                        "Worker %s found nothing to claim after %d claim(s); stopping",
                    )
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
            self._drain(in_flight, renew_stop)
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

    def _drain(
        self, in_flight: set[Future[None]], renew_stop: Event | None = None
    ) -> None:
        """
        Wait for the in-flight tasks, except the stuck ones while recycling:
        a thread the backstop gave up on may never finish, and the point of
        the recycle is to stop waiting for it.

        While recycling, the wait is also bounded. Abandoning the stuck thread
        is not enough on its own: the worker still waits for its healthy
        siblings, and a sibling on a queue with no timeout has no obligation
        to finish. The bound is what makes the recycle certain, on a process
        that has already decided it cannot be trusted to run work.

        The budget defaults to `lock_timeout`, which is a generous allowance
        rather than a free one, and the arithmetic adds rather than
        overlaps. These leases are still being renewed
        while the drain waits, so no reaper is entitled to the rows during the
        budget: the wait and the lease do not overlap, they add. Worst case
        from the backstop firing to another worker picking the row up is
        `recycle_drain_budget + lock_timeout`, which is ten minutes at the
        defaults.

        Renewal is stopped here, at the moment the budget expires, rather than
        left to the process exit, so the lease clock starts from a point this
        method controls and an embedded caller behaves like the management
        command.

        The cost is real and deliberate: a task cut short this way is recorded
        as a lost lease and retried, and one on its last attempt becomes LOST
        instead of the success it was heading for. That is the price of a
        recycle that is certain.
        """
        give_up_at: float | None = None
        while True:
            pending = {future for future in in_flight if not future.done()}
            if not pending:
                return
            # Re-read the flag every pass: the backstop can fire part way
            # through an ordinary drain.
            if self._recycling:
                healthy = len(pending) - self._stuck_alive()
                if healthy <= 0:
                    return
                now = time.monotonic()
                if give_up_at is None:
                    give_up_at = now + self.recycle_drain_budget
                elif now >= give_up_at:
                    # Stop refreshing the leases before abandoning the rows,
                    # so they begin ageing out now rather than whenever this
                    # process happens to exit.
                    if renew_stop is not None:
                        renew_stop.set()
                    logger.warning(
                        "Worker %s stopped waiting on %d in-flight task(s) "
                        "after %gs of recycling; their leases will expire and "
                        "the reaper will requeue them",
                        self.worker_id,
                        healthy,
                        self.recycle_drain_budget,
                        extra={
                            "event": "worker_drain_abandoned",
                            "worker_id": self.worker_id,
                            "pending": healthy,
                        },
                    )
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
