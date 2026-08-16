"""No-op benchmark task for django-tasks-db (django_tasks decorator).

The function body is identical to benchsite.tasks_ox.noop; only the
decorator differs, because django-tasks-db enqueues through the django_tasks
package while django-ox enqueues through Django core's django.tasks.
"""

from django_tasks import task


@task
def noop():
    return None
