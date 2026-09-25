"""
Conversion between OxTask rows and the django.tasks dataclasses.

TaskResult is a frozen dataclass whose _return_value field is init=False;
mirroring ImmediateBackend, it is set via object.__setattr__ after
construction.

The task a TaskResult carries is the declared one, whichever path built it:
enqueue, enqueue_many, get_result, the worker's signals, and anything layered
on task_result_from_db. Its class and policy fields are what the code
declares, and only the routing comes from the row. The row's stored
max_attempts is not written onto it. That column is the budget the worker and
the reaper decide on, and an admin retry or an older release can leave it at
a value no declaration may take, 0 or above 32767 among them. Carried on the
task, it would read as a declaration: a result's task would stop comparing
equal to the one it was enqueued from, re-enqueueing it would copy the old
row's budget, and a using() or dataclasses.replace() on it would be refused
for a value nobody declared.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from django.utils.module_loading import import_string

from .compat import Task, TaskError, TaskResult, TaskResultStatus

if TYPE_CHECKING:
    from .models import OxTask


def task_from_db(db_task: OxTask) -> Task[..., Any]:
    """
    Rebuild the Task for a stored row.

    task_path points at the module-level name, which after decoration is the
    Task instance itself. The row supplies what the call was enqueued with:
    priority, backend, queue, run_after and takes_context. Everything else is
    the imported object's, class included: a PolicyTask keeps its live
    max_attempts, backoff and timeout, a subclass keeps its own fields, and a
    plain Task, which is how a task declared on another backend and rebound
    onto this one arrives, stays a plain Task. With the routing it was
    declared with, the rebuilt task compares equal to the imported one, as
    it did before per-task policy existed. The stored max_attempts is not
    read here; see the module docstring.

    The resolved object must be a django.tasks Task (something decorated with
    @task). A row whose task_path points at any other importable callable is
    rejected here rather than executed: the worker never calls an arbitrary
    dotted path pulled from the table, only functions the application
    registered as tasks. The trust model is in SECURITY.md at https://github.com/oxpull/django-ox.

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
    enqueued_as: dict[str, Any] = {
        "priority": db_task.priority,
        "backend": db_task.backend_name,
        "queue_name": db_task.queue_name,
        "run_after": db_task.run_after,
        "takes_context": db_task.takes_context,
    }
    # Built through the constructor, so the backend validates the routing
    # fields exactly as the stock rebuild did. The declared policy fields go
    # through the same validation they passed at import.
    return dataclasses.replace(obj, **enqueued_as)


def public_status(db_status: str) -> TaskResultStatus:
    """
    Translate a stored status into one of django.tasks' four values.

    Four of the seven map straight across. LOST has no counterpart: it says
    the worker holding the task stopped reporting and nobody observed the
    outcome. DISCARDED has none either: the task was closed without running.
    Both map to FAILED, and is_finished is true for both.

    WAITING has no counterpart either. The task has not run, and no worker
    will claim it until something outside the worker releases it. It maps to
    READY, so is_finished is false. The mapping loses a distinction: through
    django.tasks, READY means not finished, not that a worker can take the
    task now. The row's own status keeps the difference.

    Any other value raises ValueError, as the cast always has. A reader that
    does not know a status cannot tell whether the task finished, and READY
    would keep a loop polling a task that already has.

    LOST maps to FAILED because READY and RUNNING are instructions to come back
    later, and nothing is coming: the attempts are spent, no worker will
    claim the row, and the only process that could still write to it is one
    there is positive reason to think has gone. Mapping to RUNNING would
    also leave TaskResult.is_finished permanently False, so every wait loop
    over such a task spins forever. FAILED is the answer a caller can act
    on.

    A caller polling a LOST row sees FAILED; if the holder of the lease later
    records a success, the next read sees SUCCESSFUL. The row keeps the
    distinction the API cannot carry: its status is LOST, and the recorded
    error says the outcome was not observed rather than naming a cause.
    """
    from .models import OxTask

    if db_status in (OxTask.Status.LOST, OxTask.Status.DISCARDED):
        return TaskResultStatus.FAILED
    if db_status == OxTask.Status.WAITING:
        return TaskResultStatus.READY
    return TaskResultStatus(db_status)


def task_result_from_db(
    db_task: OxTask, task: Task[..., Any] | None = None
) -> TaskResult[..., Any]:
    """
    The TaskResult for a row. ``task``, when given, is the Task the caller
    already holds, which saves re-importing it. Results preserve the task's
    declared class and policy fields; the row's stored max_attempts is not
    overlaid.
    """
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
