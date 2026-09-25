"""
A task module whose declaration is refused while it is imported.

``max_attempts=0`` raises InvalidTask where ``@task`` passes the field to
PolicyTask, and TypeError on Django 6.0, where it takes no such argument.
Either way it is not ImportError. Nothing imports this module except a worker
rebuilding a row whose task_path names it.
"""

from django_ox.compat import task


@task(max_attempts=0)
def refused():
    raise AssertionError("the declaration is refused, so this never runs")
