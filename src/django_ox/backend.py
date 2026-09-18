from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from django.apps import apps
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import router, transaction
from django.utils import timezone

from .compat import (
    BaseTaskBackend,
    Task,
    TaskResult,
    TaskResultDoesNotExist,
    normalize_json,
    task_enqueued,
)

if TYPE_CHECKING:
    from datetime import datetime

    from .models import OxTask

# Rows per INSERT in enqueue_many(); see django_ox.bulk for the reasoning.
INSERT_CHUNK_SIZE = 1000


class OxBackend(BaseTaskBackend):
    """
    Database-backed task backend.

    enqueue() is a plain INSERT on the connection OxTask routes to, so it
    participates in a caller's transaction on that same connection: a task
    enqueued inside transaction.atomic() becomes visible to workers only if
    the transaction commits, and is discarded on rollback. This is the
    durability guarantee; transaction.on_commit() is not needed with this
    backend.

    On the default single-database setup that connection is the default one,
    which is the case the guarantee is usually described in. Under a router
    that sends OxTask elsewhere it is that database, and a caller whose own
    rows are written on a different connection gets two transactions rather
    than one.
    """

    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        self.max_attempts: int = int(self.options.get("MAX_ATTEMPTS", 3))

    def _row[**P, R](
        self,
        task: Task[P, R],
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
        enqueued_at: datetime,
    ) -> OxTask:
        """
        Build the unsaved row for one call. The single place that decides
        how a call is stored, shared by enqueue() and enqueue_many() so the
        two write the same payload and the worker reads one format.
        """
        from .models import OxTask

        return OxTask(
            task_path=task.module_path,
            args=normalize_json(args),
            kwargs=normalize_json(kwargs),
            queue_name=task.queue_name,
            priority=task.priority,
            takes_context=task.takes_context,
            backend_name=self.alias,
            run_after=task.run_after,
            max_attempts=self.max_attempts,
            enqueued_at=enqueued_at,
        )

    def enqueue[**P, R](
        self, task: Task[P, R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[P, R]:
        self.validate_task(task)

        db_task = self._row(task, args, kwargs, timezone.now())
        db_task.save(force_insert=True)

        from .results import task_result_from_db

        task_result = cast("TaskResult[P, R]", task_result_from_db(db_task, task=task))
        # send_robust, for the same reason as the worker's lifecycle signals
        # and one of its own: the row is already saved when this fires, so a
        # receiver has no enqueue left to veto. Letting its exception out of
        # enqueue() would report a failure over a task that exists, and a
        # caller who retries on that creates a second one. Receiver exceptions
        # are logged on the django.dispatch logger, not this package's.
        #
        # Inside an outer transaction.atomic() the row is committed with that
        # block rather than before this line; the reasoning holds either way,
        # because the caller's own rollback is what undoes the task.
        task_enqueued.send_robust(type(self), task_result=task_result)
        return task_result

    def enqueue_many[**P, R](
        self,
        task: Task[P, R],
        calls: Iterable[tuple[Sequence[Any], Mapping[str, Any]]],
    ) -> list[TaskResult[P, R]]:
        """
        Insert one row per (args, kwargs) pair with bulk_create, in input
        order, inside one transaction. django_ox.bulk.enqueue_many is the
        public entry point and documents the contract; this is the insert.

        Validation and serialisation happen before the first write, so an
        InvalidTask or a serialiser TypeError leaves the table untouched.
        Primary keys are generated client-side, so the results are built
        from the objects handed to bulk_create without a read back.
        """
        from .models import OxTask
        from .results import task_result_from_db

        self.validate_task(task)
        enqueued_at = timezone.now()
        rows = [self._row(task, args, kwargs, enqueued_at) for args, kwargs in calls]

        # Pinned to the alias the rows are written through, so the block and
        # the INSERTs share one connection. An unpinned atomic() opens on the
        # default connection while bulk_create routes itself, which under a
        # router guards a connection the INSERTs never touch.
        alias = router.db_for_write(OxTask)
        with transaction.atomic(using=alias):
            OxTask.objects.using(alias).bulk_create(rows, batch_size=INSERT_CHUNK_SIZE)

        results = [
            cast("TaskResult[P, R]", task_result_from_db(row, task=task))
            for row in rows
        ]
        for task_result in results:
            task_enqueued.send_robust(type(self), task_result=task_result)
        return results

    def get_result(self, result_id: str) -> TaskResult[..., Any]:
        from .models import OxTask
        from .results import task_result_from_db

        # Read on the alias the row was written to. Under a router that
        # sends reads to a replica, a result asked for just after enqueue
        # would be one the replica has not got yet, and the caller would be
        # told its task does not exist.
        alias = router.db_for_write(OxTask)
        try:
            db_task = OxTask.objects.using(alias).get(id=result_id)
        except (OxTask.DoesNotExist, ValidationError, ValueError) as exc:
            raise TaskResultDoesNotExist(result_id) from exc
        return task_result_from_db(db_task)

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        errors = super().check(**kwargs)
        if not apps.is_installed("django_ox"):
            errors.append(
                checks.Error(
                    "django_ox must be in INSTALLED_APPS to use OxBackend.",
                    hint="Add 'django_ox' to INSTALLED_APPS and run migrations.",
                    id="django_ox.E001",
                )
            )
        from .schedules import (
            schedule_name_collisions,
            schedule_source_from_options,
            schedules_from_options,
        )

        try:
            schedules_from_options(self.options, self.alias)
        except ImproperlyConfigured as exc:
            errors.append(
                checks.Error(
                    str(exc),
                    hint="Fix the SCHEDULES entry of this backend's OPTIONS.",
                    id="django_ox.E002",
                )
            )
        # Only when the project named a source. The default source builds
        # from SCHEDULES and raises the same error E002 has just reported,
        # so running this unconditionally would report every bad schedule
        # twice under two different ids.
        #
        # Built, not merely imported: a source that raises on construction
        # would otherwise start a worker that dispatches nothing and reports
        # nothing.
        try:
            if self.options.get("SCHEDULE_SOURCE"):
                schedule_source_from_options(self.options, self.alias)
        except ImproperlyConfigured as exc:
            errors.append(
                checks.Error(
                    str(exc),
                    hint=(
                        "Fix OPTIONS['SCHEDULE_SOURCE'], or remove it to read "
                        "schedules from OPTIONS['SCHEDULES']."
                    ),
                    id="django_ox.E006",
                )
            )
        # Registered here as well as by the decorator, because a check runs
        # without importing application code and this is the only channel it
        # can see. Registration is idempotent for an identical kind, so a key
        # declared in both channels is not a collision.
        from .registry import kinds_from_options

        try:
            kinds_from_options(self.options, self.alias)
        except ImproperlyConfigured as exc:
            errors.append(
                checks.Error(
                    str(exc),
                    hint="Fix the SCHEDULABLE_TASKS entry of this backend's OPTIONS.",
                    id="django_ox.E007",
                )
            )
        # One database, or the constraint is not a coordination mechanism.
        # A due tick is enqueued once because the task row and the tick row
        # commit or roll back together; two connections cannot do that, and
        # a tick committed without its task is one nothing will run again.
        # The default router sends everything to one database, so this only
        # ever fires on a project that wrote its own.
        from django.db import router

        from .models import OxSchedule, OxScheduleTick, OxTask

        routed = {
            model._meta.object_name: router.db_for_write(model)
            for model in (OxTask, OxScheduleTick, OxSchedule)
        }
        if len(set(routed.values())) > 1:
            errors.append(
                checks.Error(
                    "django-ox models are routed to more than one database: "
                    + ", ".join(
                        f"{name} to {alias!r}" for name, alias in routed.items()
                    )
                    + ".",
                    hint=(
                        "A task row and its schedule tick row must commit in one "
                        "transaction, so they must live on one database. Route "
                        "the django_ox app to a single database."
                    ),
                    id="django_ox.E008",
                )
            )
        # Names that differ only by case are one key to a case-insensitive
        # collation, which is MySQL's default. On MySQL the collision is
        # real and silent, so it is refused; elsewhere it is a portability
        # hazard worth naming, because the same settings deployed against
        # MySQL would starve one of the two schedules.
        from django.db import connections as _connections
        from django.db import router as _router

        from .models import OxScheduleTick as _Tick
        from .schedules import schedule_names_folding_together

        folding = schedule_names_folding_together(self.alias)
        if folding:
            pairs = ", ".join(f"{a!r} and {b!r}" for a, b in folding)
            tick_db = _router.db_for_write(_Tick)
            on_mysql = _connections[tick_db].vendor == "mysql"
            message = (
                f"Schedule names differing only by case: {pairs}. The schedule "
                "tick log decides identity with its column's collation, so on a "
                "case-insensitive one these share a key: their ticks collide and "
                "one schedule stops running."
            )
            hint = "Give each schedule a name that differs by more than case."
            errors.append(
                checks.Error(message, hint=hint, id="django_ox.E009")
                if on_mysql
                else checks.Warning(message, hint=hint, id="django_ox.W002")
            )
        # A tick's identity is (schedule name, tick time), and under
        # USE_TZ=False that time is stored as a naive wall clock. Where the
        # clock goes back, one label covers two instants, so the second is
        # read as a tick already recorded. A cron schedule then fires once
        # rather than twice, which is arguable; an interval loses roughly
        # half its runs for the length of the repeated hour, which is not.
        # Not fixable without changing what the tick log stores, and that
        # table's schema is a published promise.
        if not settings.USE_TZ:
            from .schedules import zone_repeats_an_hour

            if zone_repeats_an_hour():
                errors.append(
                    checks.Warning(
                        f"USE_TZ is off and TIME_ZONE is {settings.TIME_ZONE!r}, "
                        "which puts the clock back once a year. Schedule ticks "
                        "are recorded against the wall clock, so an interval "
                        "schedule loses about half its runs for the length of "
                        "the repeated hour and a cron schedule inside it fires "
                        "once rather than twice.",
                        hint=(
                            "Set USE_TZ = True, or set TIME_ZONE to a zone with "
                            "no daylight-saving transition such as 'UTC'."
                        ),
                        id="django_ox.W001",
                    )
                )
        from .timeouts import lease_timing_problems, task_timeout_problems

        for problem in lease_timing_problems(self.options):
            errors.append(
                checks.Error(
                    problem,
                    hint=(
                        "LOCK_TIMEOUT, BACKOFF_INITIAL and BACKOFF_MAX are each "
                        "a positive, finite number of seconds."
                    ),
                    id="django_ox.E010",
                )
            )

        for problem in task_timeout_problems(self.options, self.queues):
            unknown_queue = "is not in QUEUES" in problem
            errors.append(
                checks.Error(
                    problem,
                    hint=(
                        "Name a queue from this backend's QUEUES, or drop the entry."
                        if unknown_queue
                        else "TASK_TIMEOUT, each TASK_TIMEOUTS value and "
                        "TASK_TIMEOUT_GRACE are a positive, finite number of "
                        "seconds, at most a thousand years; the first two may "
                        "also be None, which means no limit."
                    ),
                    id="django_ox.E005" if unknown_queue else "django_ox.E004",
                )
            )
        for name, other_alias in schedule_name_collisions(self.alias):
            errors.append(
                checks.Error(
                    f"Schedule name {name!r} is also defined on the "
                    f"{other_alias!r} backend; tick rows are keyed by name "
                    "alone, so the schedules would suppress each other.",
                    hint="Rename one schedule; names must be unique across backends.",
                    id="django_ox.E003",
                )
            )
        return errors
