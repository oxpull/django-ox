from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from django.apps import apps
from django.core import checks
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskResult
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.json import normalize_json

if TYPE_CHECKING:
    from datetime import datetime

    from .models import OxTask

# Rows per INSERT in enqueue_many(); see django_ox.bulk for the reasoning.
INSERT_CHUNK_SIZE = 1000


class OxBackend(BaseTaskBackend):
    """
    Database-backed task backend.

    enqueue() is a plain INSERT on the default database connection, so it
    participates in the caller's open transaction: a task enqueued inside
    transaction.atomic() becomes visible to workers only if the transaction
    commits, and is discarded on rollback. This is the durability guarantee;
    transaction.on_commit() is not needed with this backend.
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
        task_enqueued.send(type(self), task_result=task_result)
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

        with transaction.atomic():
            OxTask.objects.bulk_create(rows, batch_size=INSERT_CHUNK_SIZE)

        results = [
            cast("TaskResult[P, R]", task_result_from_db(row, task=task))
            for row in rows
        ]
        for task_result in results:
            task_enqueued.send(type(self), task_result=task_result)
        return results

    def get_result(self, result_id: str) -> TaskResult[..., Any]:
        from .models import OxTask
        from .results import task_result_from_db

        try:
            db_task = OxTask.objects.get(id=result_id)
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
        from .schedules import schedule_name_collisions, schedules_from_options

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
        from .timeouts import task_timeouts_from_options

        try:
            task_timeouts_from_options(self.options)
        except ImproperlyConfigured as exc:
            errors.append(
                checks.Error(
                    str(exc),
                    hint=(
                        "TASK_TIMEOUT is a positive number of seconds or None; "
                        "TASK_TIMEOUTS maps queue names to the same."
                    ),
                    id="django_ox.E004",
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
