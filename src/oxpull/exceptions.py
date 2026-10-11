# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

from django.tasks.exceptions import TaskException


class UniqueEnqueueRace(TaskException):
    """
    A unique enqueue exhausted its retries while racing other enqueuers
    over the same key. Safe to retry; never a silent double-enqueue.
    """


class BatchSealed(TaskException):
    """
    A batch was sealed (or completed) before this call ran: add() after
    seal, or a second seal. Membership is frozen at seal so total_count
    stays true; there is no un-seal.
    """


class BatchMemberExists(TaskException):
    """
    The task returned by enqueue() is already a member of this batch.

    Only reachable through unique tasks: a deduplicated enqueue returns
    the pending task's id, and the (batch, task_id) constraint refuses to
    count one task twice in one batch.
    """


class WorkflowSealed(TaskException):
    """
    A workflow was sealed (or completed) before this call ran: add()
    after seal, or a second seal. Membership is frozen at seal so
    total_count stays true; there is no un-seal.
    """


class WorkflowNodeExists(TaskException):
    """
    A node with this name, or this task, is already in the workflow.

    The name clash is a caller mistake: dependencies are declared by
    name, so two nodes sharing one would make depends_on ambiguous. The
    task clash is only reachable through unique tasks, where a
    deduplicated enqueue returns a task the workflow already waits on.
    """


class UnknownWorkflowNode(TaskException):
    """
    depends_on named a node this workflow does not have.

    Dependencies may only point at nodes already added, which is also
    why a cycle cannot be built: every edge points backwards in
    insertion order.
    """
