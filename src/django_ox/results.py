"""
Conversion between OxTask rows and the django.tasks dataclasses.

TaskResult is a frozen dataclass whose _return_value field is init=False;
mirroring ImmediateBackend, it is set via object.__setattr__ after
construction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.utils.module_loading import import_string

from .compat import Task, TaskError, TaskResult, TaskResultStatus

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


def public_status(db_status: str) -> TaskResultStatus:
    """
    Translate a stored status into one of django.tasks' four values.

    Four of the six map straight across. LOST has no counterpart: it says
    the worker holding the task stopped reporting and nobody observed the
    outcome. DISCARDED has none either: an operator closed the task without
    running it. Both map to FAILED, and is_finished is true for both.

    LOST maps to FAILED because READY and RUNNING are instructions to come back
    later, and nothing is coming: the attempts are spent, no worker will
    claim the row, and the only process that could still write to it is one
    there is positive reason to think has gone. Mapping to RUNNING would
    also leave TaskResult.is_finished permanently False, so every wait loop
    over such a task spins forever, which is worse to hand somebody than a
    wrong answer.

    The cost, stated rather than hidden: if the process holding the lost
    lease does come back and records a success, a caller reading the row
    twice sees FAILED and then SUCCESSFUL. It is confined to this path, and
    it is what the same sequence produces today, where the reaper writes a
    real FAILED and the returning worker overwrites it. The row keeps the
    distinction the API cannot carry: its status is LOST, not FAILED, and
    the recorded error says the outcome was never observed rather than
    naming a cause.
    """
    from .models import OxTask

    if db_status in (OxTask.Status.LOST, OxTask.Status.DISCARDED):
        return TaskResultStatus.FAILED
    return TaskResultStatus(db_status)


def task_result_from_db(
    db_task: OxTask, task: Task[..., Any] | None = None
) -> TaskResult[..., Any]:
    if task is None:
        task = task_from_db(db_task)
    result: TaskResult[..., Any] = TaskResult(
        task=task,
        id=str(db_task.id),
        status=public_status(db_task.status),
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
