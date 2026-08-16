"""
Conversion between OxTask rows and the django.tasks dataclasses.

TaskResult is a frozen dataclass whose _return_value field is init=False;
mirroring ImmediateBackend, it is set via object.__setattr__ after
construction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.tasks.base import Task, TaskError, TaskResult, TaskResultStatus
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from .models import OxTask


def task_from_db(db_task: OxTask) -> Task[..., Any]:
    """
    Rebuild the Task for a stored row.

    task_path points at the module-level name, which after decoration is the
    Task instance itself; unwrap it to the underlying function.

    The resolved object must be a django.tasks Task (something decorated with
    @task). A row whose task_path points at any other importable callable is
    rejected here rather than executed: the worker never calls an arbitrary
    dotted path pulled from the table, only functions the application
    registered as tasks. See SECURITY.md for the trust model.

    Raises ImportError if the path no longer resolves, or resolves to a
    non-Task object; callers decide whether that is a hard error (get_result)
    or a task failure (worker).
    """
    obj = import_string(db_task.task_path)
    if not isinstance(obj, Task):
        raise ImportError(
            f"{db_task.task_path!r} did not resolve to a django.tasks Task; "
            "django-ox only runs functions registered with @task."
        )
    func = obj.func
    return Task(
        priority=db_task.priority,
        func=func,
        backend=db_task.backend_name,
        queue_name=db_task.queue_name,
        run_after=db_task.run_after,
        takes_context=db_task.takes_context,
    )


def task_result_from_db(
    db_task: OxTask, task: Task[..., Any] | None = None
) -> TaskResult[..., Any]:
    if task is None:
        task = task_from_db(db_task)
    result: TaskResult[..., Any] = TaskResult(
        task=task,
        id=str(db_task.id),
        status=TaskResultStatus(db_task.status),
        enqueued_at=db_task.enqueued_at,
        started_at=db_task.started_at,
        last_attempted_at=db_task.last_attempted_at,
        finished_at=db_task.finished_at,
        args=db_task.args,
        kwargs=db_task.kwargs,
        backend=db_task.backend_name,
        errors=[
            TaskError(
                exception_class_path=error["exception_class_path"],
                traceback=error["traceback"],
            )
            for error in db_task.errors
        ],
        worker_ids=list(db_task.worker_ids),
    )
    if db_task.status == db_task.Status.SUCCESSFUL:
        object.__setattr__(result, "_return_value", db_task.return_value)
    return result
