"""No-op benchmark tasks for django-tasks-db (Django core django.tasks decorator).

The function bodies are identical to benchsite.tasks_ox, and so is the
decorator: since 0.13.0 django-tasks-db runs on Django core's django.tasks
when it is present (its compat module imports the core framework on Django
6.0+ and the django_tasks backport only through its [compat] extra on 5.2),
and its result rows accept only the core Task class there. Both backends
therefore ride one framework and the comparison is between the backends.
"""

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
