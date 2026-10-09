"""Tasks for test_connection_count. Each notes what ran, where, to a file."""

import time

from django.contrib.auth.models import User

from django_ox.compat import task

from .dead_connection_tasks import note, session_id


@task
def tick(notes, seconds=0.0, index=None):
    note(notes, "ran", index=index)
    if seconds:
        time.sleep(seconds)
    return "ran"


@task
def count_users(notes):
    note(notes, "ran", users=User.objects.count())
    return "ran"


@task
def report_session(notes, seconds=0.0):
    note(notes, "ran", session=session_id())
    if seconds:
        time.sleep(seconds)
    return "ran"
