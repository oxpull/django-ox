"""
A task module whose declaration its backend refuses while it is imported.

The task names a queue that the test settings' QUEUES leave out, so `@task`
raises InvalidTask, not ImportError, on every supported Django. Nothing
imports this module except a schedule or registry entry that names it.
"""

from django_ox.compat import task


@task(queue_name="not-a-configured-queue")
def refused():
    raise AssertionError("the declaration is refused, so this never runs")
