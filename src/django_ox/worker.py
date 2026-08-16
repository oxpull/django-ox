import json
import logging
import os
import socket
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import suppress
from datetime import datetime, timedelta
from threading import Barrier, BrokenBarrierError, Event
from traceback import format_exception
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    IntegrityError,
    close_old_connections,
    connections,
    router,
    transaction,
)
from django.db.models import Max, Q, QuerySet
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

# Single-statement claim for PostgreSQL: the SKIP LOCKED subselect, the claim
# UPDATE, and the per-attempt bookkeeping are one round trip, atomic in
# autocommit. The equivalent multi-statement path costs 5 round trips.
POSTGRES_CLAIM_SQL = """
UPDATE "{table}" SET
    "status" = %(running)s,
    "locked_by" = %(worker_id)s,
    "locked_at" = %(now)s,
    "attempts" = "attempts" + 1,
    "started_at" = COALESCE("started_at", %(now)s),
    "last_attempted_at" = %(now)s,
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


class Worker:
    """
    Claims READY tasks and executes them, at least once.

    Claiming uses a single UPDATE ... SKIP LOCKED ... RETURNING statement on
    PostgreSQL, SELECT ... FOR UPDATE SKIP LOCKED where another database
    supports it, and otherwise an optimistic compare-and-set UPDATE keyed on
    (status=READY, attempts), which is atomic on every backend including
    SQLite. Every path folds the per-attempt bookkeeping (attempts,
    started_at, last_attempted_at, worker_ids) into the claim UPDATE itself,
    so a worker that dies mid-task has already consumed the attempt and
    attempts always equals len(worker_ids).

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
        schedule_interval: float | None = None,
        backoff_initial: float | None = None,
        backoff_max: float | None = None,
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
        self.worker_id = (
            f"{socket.gethostname()[:40]}-{os.getpid()}-{get_random_string(8)}"
        )
        self._stop = Event()
        self._db_alias = router.db_for_write(OxTask)

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
        queryset = OxTask.objects.filter(status=OxTask.Status.READY).filter(
            Q(run_after__isnull=True) | Q(run_after__lte=timezone.now())
        )
        if self.queues:
            queryset = queryset.filter(queue_name__in=self.queues)
        return queryset.order_by("-priority", "enqueued_at")

    def _claim_fields(self, candidate: OxTask, now: datetime) -> dict[str, Any]:
        return {
            "status": OxTask.Status.RUNNING,
            "locked_by": self.worker_id,
            "locked_at": now,
            "attempts": candidate.attempts + 1,
            "started_at": candidate.started_at or now,
            "last_attempted_at": now,
            "worker_ids": [*candidate.worker_ids, self.worker_id],
        }

    def _claim_one_postgresql(self, now: datetime) -> OxTask | None:
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
                "now": now,
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
        now = timezone.now()
        skip_locked = connection.features.has_select_for_update_skip_locked
        if connection.vendor == "postgresql" and skip_locked:
            return self._claim_one_postgresql(now)
        if skip_locked:
            with transaction.atomic(using=self._db_alias):
                candidate = (
                    self._ready_queryset().select_for_update(skip_locked=True).first()
                )
                if candidate is None:
                    return None
                fields = self._claim_fields(candidate, now)
                OxTask.objects.filter(pk=candidate.pk).update(**fields)
                for name, value in fields.items():
                    setattr(candidate, name, value)
                return candidate
        # Optimistic compare-and-set. attempts doubles as a version counter:
        # if another worker claimed (and possibly requeued) the row between
        # the fetch and this UPDATE, attempts has moved and the literal
        # bookkeeping values below cannot stomp its writes.
        for candidate in self._ready_queryset()[:CLAIM_BATCH_SIZE]:
            fields = self._claim_fields(candidate, now)
            claimed = OxTask.objects.filter(
                pk=candidate.pk,
                status=OxTask.Status.READY,
                attempts=candidate.attempts,
            ).update(**fields)
            if claimed:
                for name, value in fields.items():
                    setattr(candidate, name, value)
                return candidate
        return None

    # -- execution ---------------------------------------------------------

    def execute(self, db_task: OxTask) -> None:
        """
        Run a claimed (RUNNING, locked) task to a terminal or retry state.

        Per-attempt bookkeeping (started_at, last_attempted_at, worker_ids)
        was already written by the claim UPDATE.
        """
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
            db_task.status = OxTask.Status.SUCCESSFUL
            db_task.return_value = return_value
            db_task.finished_at = timezone.now()
            db_task.locked_by = None
            db_task.locked_at = None
            db_task.save(
                update_fields=[
                    "status",
                    "return_value",
                    "finished_at",
                    "locked_by",
                    "locked_at",
                ]
            )
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
        db_task.errors = [
            *db_task.errors,
            {
                "exception_class_path": (
                    f"{exception_type.__module__}.{exception_type.__qualname__}"
                ),
                "traceback": "".join(format_exception(exc)),
            },
        ]
        now = timezone.now()
        db_task.locked_by = None
        db_task.locked_at = None

        if db_task.attempts >= db_task.max_attempts:
            db_task.status = OxTask.Status.FAILED
            db_task.finished_at = now
            db_task.save(
                update_fields=[
                    "status",
                    "errors",
                    "finished_at",
                    "locked_by",
                    "locked_at",
                ]
            )
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
            db_task.status = OxTask.Status.READY
            db_task.run_after = now + timedelta(seconds=delay)
            db_task.save(
                update_fields=[
                    "status",
                    "errors",
                    "run_after",
                    "locked_by",
                    "locked_at",
                ]
            )
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
        Return stuck RUNNING tasks (lock older than lock_timeout) to READY,
        or fail them if no attempts remain. Returns the number reclaimed.
        """
        cutoff = timezone.now() - timedelta(seconds=self.lock_timeout)
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
                    status=OxTask.Status.FAILED,
                    finished_at=timezone.now(),
                    errors=[
                        *db_task.errors,
                        {
                            "exception_class_path": (
                                f"{TaskAbandoned.__module__}."
                                f"{TaskAbandoned.__qualname__}"
                            ),
                            "traceback": (
                                f"Worker {db_task.locked_by!r} lock expired after "
                                f"{self.lock_timeout}s with no attempts remaining."
                            ),
                        },
                    ],
                )
            else:
                updates.update(status=OxTask.Status.READY)
            # CAS on locked_at so a worker that finished (or another reaper)
            # in the meantime is not overwritten.
            changed = OxTask.objects.filter(
                pk=db_task.pk,
                status=OxTask.Status.RUNNING,
                locked_at=db_task.locked_at,
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
