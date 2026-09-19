"""No-op benchmark tasks for django-ox (Django core django.tasks decorator)."""

from django.tasks import task

from benchsite.retry_ledger import record_execution


@task
def noop():
    return None


@task
def item(n):
    """No-op with one integer argument: the bulk enqueue cell's payload."""
    return None


@task
def flaky(logical_id):
    """
    Raises on its first execution and returns on any later one. The retry
    ledger decides which execution this is (see retry_ledger.py), so the
    body asks nothing of the backend's own attempt counter.
    """
    executions = record_execution(logical_id)
    if executions == 1:
        raise RuntimeError(f"flaky {logical_id}: first execution fails on purpose")
    return executions
