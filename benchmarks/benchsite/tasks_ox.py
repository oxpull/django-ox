"""No-op benchmark task for django-ox (Django core django.tasks decorator)."""

from django.tasks import task


@task
def noop():
    return None
