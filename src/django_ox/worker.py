import json
import logging
import os
import socket
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import suppress
from datetime import datetime, timedelta
from threading import Barrier, BrokenBarrierError, Event, Lock, Thread
from traceback import format_exception
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DatabaseError,
    IntegrityError,
    close_old_connections,
    connections,
    router,
    transaction,
)
from django.db.models import Max, Q, QuerySet
from django.db.models.expressions import Combinable
from django.db.models.functions import Now
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.tasks.base import TaskContext
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json

from .backend import OxBackend
from .exceptions import TaskAbandoned
from .models import OxScheduleTick, OxTask
from .schedules import schedule_name_collisions, schedules_from_options

logger = logging.getLogger("django_ox")

# Candidates fetched per claim pass on the optimistic (non SKIP LOCKED) path;
# bounds retries when racing other workers for the head of the queue.
CLAIM_BATCH_SIZE = 5

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
# The claim's own timestamps are STATEMENT_TIMESTAMP(), which is what Django's
# Now() compiles to on PostgreSQL, so the lock clock and the reaper's clock are
# the same clock even when the worker and the reaper run on different hosts.
# run_after is the exception and is still compared against the worker's clock,
# because that is the clock the retry that wrote it used; see _ready_queryset.
# lease_epoch advances here too: the increment and the claim are one statement,
# so no execution can share an epoch with another.
POSTGRES_CLAIM_SQL = """
UPDATE "{table}" SET
    "status" = %(running)s,
    "locked_by" = %(worker_id)s,
    "locked_at" = STATEMENT_TIMESTAMP(),
    "lease_epoch" = "lease_epoch" + 1,
    "attempts" = "attempts" + 1,
    "started_at" = COALESCE("started_at", STATEMENT_TIMESTAMP()),
    "last_attempted_at" = STATEMENT_TIMESTAMP(),
    "worker_ids" = "worker_ids" || %(worker_id_json)s::jsonb
WHERE "id" = (
    SELECT "id" FROM "{table}"
    WHERE "status" = %(ready)s
        AND ("run_after" IS NULL OR "run_after" <= %(now)s)
        {queue_clause}
    ORDER BY "priority" DESC, "enqueued_at"
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
"""


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _is_expression(value: object) -> bool:
    """True for a value the database computes, such as Now() or Now() + delta."""
    return isinstance(value, Combinable)


def _lease_now() -> Combinable | datetime:
    """The clock that stamps lease timestamps.

    Database-side time is the right clock for a lease, because it is one clock
    for every worker and for the reaper even when they run on different hosts.
    It only agrees with what the columns already hold when USE_TZ is on, and
    that is the whole of this function.

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
        queryset = OxTask.objects.filter(status=OxTask.Status.READY).filter(
            Q(run_after__isnull=True) | Q(run_after__lte=timezone.now())
        )
        if self.queues:
            queryset = queryset.filter(queue_name__in=self.queues)
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
        sql = POSTGRES_CLAIM_SQL.format(
            table=OxTask._meta.db_table, queue_clause=queue_clause
        )
        rows = OxTask.objects.raw(
            sql,
            {
                "running": OxTask.Status.RUNNING,
                "ready": OxTask.Status.READY,
                "worker_id": self.worker_id,
                "worker_id_json": json.dumps([self.worker_id]),
                "now": run_after_cutoff,
                "queues": self.queues,
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
        if connection.vendor == "postgresql" and skip_locked:
            return self._claim_one_postgresql(timezone.now())
        if skip_locked:
            with transaction.atomic(using=self._db_alias):
                candidate = (
                    self._ready_queryset().select_for_update(skip_locked=True).first()
                )
                if candidate is None:
                    return None
                OxTask.objects.filter(pk=candidate.pk).update(
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
            claimed = OxTask.objects.filter(
                pk=candidate.pk,
                status=OxTask.Status.READY,
                attempts=candidate.attempts,
                lease_epoch=candidate.lease_epoch,
            ).update(**self._claim_fields(candidate))
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
        return OxTask.objects.filter(pk=pk, lease_epoch=granted_epoch).first()

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
        return OxTask.objects.filter(
            pk__in=pks,
            status=OxTask.Status.RUNNING,
            locked_by=self.worker_id,
        ).update(locked_at=_lease_now())

    def _renewal_loop(self, stop: Event) -> None:
        """Renew until stopped. Runs on its own thread, and its own connection."""
        try:
            while not stop.wait(self.renew_interval):
                try:
                    self.renew_leases()
                except DatabaseError:
                    # A missed renewal is survivable by design: the interval
                    # is a third of the timeout. Drop the connection so the
                    # next tick reconnects, and keep going, because giving
                    # up here would silently expire every live lease.
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
        handover moves it.

        WRITABLE_STATUSES is a second lock on the same door. The epoch alone
        is sufficient given that every handover bumps it, and this condition
        costs one enum comparison to stop depending on that being true
        forever: whatever the epoch says, an outcome may only be written
        onto a row that is still running, or onto one the reaper marked LOST.

        LOST is in the set on purpose. It means nobody saw the outcome, and
        the holder of that epoch is the only party who could ever supply
        one, so this write must still land there. It cannot land on anybody
        else's row, because no other execution ever holds that number.

        Returns True when the write landed, having mirrored the stored
        values onto the in-memory instance so the log record and the
        TaskResult that follow describe what is actually in the row.
        Returns False when the lease was gone, having logged it; the caller
        must then abandon the rest of its branch, and in particular must not
        send task_finished for an outcome it does not own.
        """
        updated = OxTask.objects.filter(
            pk=db_task.pk,
            lease_epoch=db_task.lease_epoch,
            status__in=WRITABLE_STATUSES,
        ).update(status=status, **fields)
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
        # _lease_now() hands back a database-side expression when the
        # database holds the lease clock, and an expression is not a value,
        # so it cannot be mirrored from here; read back exactly those
        # columns.
        # Everything else is already known.
        deferred = [name for name, value in fields.items() if _is_expression(value)]
        for name, value in fields.items():
            if name not in deferred:
                setattr(db_task, name, value)
        if deferred:
            db_task.refresh_from_db(fields=deferred)
        return True

    def execute(self, db_task: OxTask) -> None:
        """
        Run a claimed (RUNNING, locked) task to a terminal or retry state.

        Per-attempt bookkeeping (started_at, last_attempted_at, worker_ids)
        was already written by the claim UPDATE. The (pk, lease_epoch) pair
        joins the renewal set for the duration, so this execution's lease is
        kept alive while it runs and stops being kept alive the moment it
        is not.
        """
        held = (db_task.pk, db_task.lease_epoch)
        with self._in_flight_lock:
            self._in_flight.add(held)
        try:
            self._run_attempt(db_task)
        finally:
            with self._in_flight_lock:
                self._in_flight.discard(held)

    def _run_attempt(self, db_task: OxTask) -> None:
        from .results import task_from_db, task_result_from_db

        started = time.monotonic()
        try:
            task = task_from_db(db_task)
            task_result = task_result_from_db(db_task, task=task)
            task_started.send(sender=type(self.backend), task_result=task_result)
            logger.debug(
                "Starting task id=%s path=%s (attempt %d/%d)",
                db_task.id,
                db_task.task_path,
                db_task.attempts,
                db_task.max_attempts,
                extra=self._log_extra("task_started", db_task),
            )
            if task.takes_context:
                raw_return_value = task.call(
                    TaskContext(task_result=task_result),
                    *db_task.args,
                    **db_task.kwargs,
                )
            else:
                raw_return_value = task.call(*db_task.args, **db_task.kwargs)
            return_value = normalize_json(raw_return_value)
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
                finished_at=_lease_now(),
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
            task_finished.send(
                sender=type(self.backend),
                task_result=task_result_from_db(db_task, task=task),
            )

    def _handle_failure(
        self, db_task: OxTask, exc: BaseException, duration_ms: int
    ) -> None:
        from .results import task_result_from_db

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
                finished_at=_lease_now(),
                locked_by=None,
                locked_at=None,
            ):
                return
            try:
                task_result = task_result_from_db(db_task)
            except ImportError:
                # Task module no longer importable; the row still records
                # the failure, but no result object can be built to signal.
                task_result = None
            if task_result is not None:
                task_finished.send(sender=type(self.backend), task_result=task_result)
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
            delay = min(
                self.backoff_initial * (2 ** (db_task.attempts - 1)),
                self.backoff_max,
            )
            if not self._write_outcome(
                db_task,
                status=OxTask.Status.READY,
                duration_ms=duration_ms,
                errors=errors,
                run_after=timezone.now() + timedelta(seconds=delay),
                locked_by=None,
                locked_at=None,
            ):
                return
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
        stuck = OxTask.objects.filter(
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
                    finished_at=_lease_now(),
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
            # CAS on the lease epoch so a worker that finished (or another
            # reaper) in the meantime is not overwritten. The epoch replaces
            # the old compare on locked_at, which depended on a timestamp
            # surviving a round trip unchanged.
            changed = OxTask.objects.filter(
                pk=db_task.pk,
                status=OxTask.Status.RUNNING,
                lease_epoch=db_task.lease_epoch,
            ).update(**updates)
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
            # A tick recorded in the future (a clock-skewed worker's write)
            # must not suppress ticks that are due now; the unique
            # constraint still protects that future instant when it comes.
            if last is not None and scheduled_for <= last and last <= now:
                continue
            try:
                with transaction.atomic(using=self._db_alias):
                    result = None
                    if last is not None:
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
        """Claim and execute a single task inline. Returns True if one ran."""
        db_task = self.claim_one()
        if db_task is None:
            return False
        self.execute(db_task)
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
                    "Draining %d in-flight task(s)",
                    pending,
                    extra={
                        "event": "worker_draining",
                        "worker_id": self.worker_id,
                        "pending": pending,
                    },
                )
            wait(in_flight)
            renew_stop.set()
            renewer.join(timeout=self.renew_interval + 5)
            # Pool threads hold thread-local DB connections that survive the
            # drain (close_old_connections() only closes expired ones); close
            # each deterministically before the pool exits.
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
