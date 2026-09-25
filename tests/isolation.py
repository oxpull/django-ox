"""
What the schedule-isolation tests share: a value every database refuses,
schedules built around it, history that makes a tick due, and a way to lose
a connection for real.
"""

from datetime import timedelta

from django.conf import settings
from django.db import connections
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask

#: Serializable by Python's json module, which writes the bare token
#: Infinity, and refused by every supported database's JSON column:
#: PostgreSQL with a DataError (22P02), MySQL with an OperationalError
#: (3140) and SQLite with an IntegrityError from Django's JSON_VALID check.
#: normalize_json and the E002 check both let a float through, so a
#: schedule carrying it passes startup and `check`.
REFUSED = {"max_age": float("inf")}

#: A stored row's form field holds the JSON as the string a person typed.
REFUSED_TEXT = '{"max_age": Infinity}'


def twelve_hours_ago():
    """
    The project's wall clock twelve hours back, to the minute, naive.

    A schedule that is daily at this time of day has a due tick twelve hours
    old and its next one twelve hours off, so every pass, every worker and
    every process in a test derives the same tick however long the test
    takes. A minutely schedule fires again when a test straddles a minute
    boundary.
    """
    now = timezone.now()
    local = timezone.localtime(now).replace(tzinfo=None) if settings.USE_TZ else now
    return (local - timedelta(hours=12)).replace(second=0, microsecond=0)


def as_stored(local):
    """A naive project-local instant as the tick log stores it."""
    return timezone.make_aware(local) if settings.USE_TZ else local


def daily_cron_at(local):
    return f"{local.minute} {local.hour} * * *"


def cron_schedules(*names, refused=(), cron=None):
    """
    Settings schedules in this order, each enqueueing tests.tasks.labelled
    with its own name, daily at twelve_hours_ago() unless `cron` says
    otherwise. Those in `refused` carry REFUSED as well.
    """
    cron = cron or daily_cron_at(twelve_hours_ago())
    schedules = {}
    for name in names:
        entry = {"task": "tests.tasks.labelled", "cron": cron, "args": [name]}
        if name in refused:
            entry["kwargs"] = dict(REFUSED)
        schedules[name] = entry
    return schedules


def with_history(*keys, using="default"):
    """
    One recorded tick per schedule a day ago, before any tick the schedules
    here are due at, so the due tick fires rather than anchors.
    """
    now = timezone.now()
    for key in keys:
        OxScheduleTick.objects.using(using).create(
            schedule_name=key,
            scheduled_for=now - timedelta(days=1),
            task_id=None,
            created_at=now,
        )


def fired(key):
    """The tick rows of `key` that enqueued a task, with the label each task carries."""
    ticks = OxScheduleTick.objects.filter(schedule_name=key).exclude(task_id=None)
    return [
        (tick.scheduled_for, _label(OxTask.objects.get(id=tick.task_id)))
        for tick in ticks
    ]


def _label(task):
    if task.args:
        return task.args[0]
    return task.kwargs.get("label")


def labels():
    """The label of every task row, sorted."""
    return sorted(_label(task) for task in OxTask.objects.all())


def kill(connection):
    """
    Lose this connection the way a server restart or a network drop loses it.

    PostgreSQL and MySQL end the session from a second connection, on the
    server, so the client finds out at its next statement. SQLite has no
    server, and closing the driver's connection underneath Django is the
    nearest thing: every statement after it fails, the rollback included.
    """
    raw = connection.connection
    if connection.vendor == "sqlite":
        raw.close()
        return
    if connection.vendor == "postgresql":
        info = getattr(raw, "info", None)
        pid = info.backend_pid if info is not None else raw.get_backend_pid()
        # Waits for the backend to be gone, so the next statement cannot
        # arrive while it is still exiting.
        statement = "SELECT pg_terminate_backend(%s, 10000)"
    else:
        pid = raw.thread_id()
        statement = "KILL %s"
    killer = connections.create_connection(connection.alias)
    try:
        with killer.cursor() as cursor:
            cursor.execute(statement, [pid])
    finally:
        killer.close()
