"""
The stored schedules against a database router.

Every django-ox model must live on one database, but that database need not
be the default one. With a single alias configured the two are the same
connection object, so a transaction opened without an alias and one opened
on the routed alias behave identically and the difference is invisible.
These tests configure a second alias so it is not.
"""

from datetime import timedelta

import pytest
from django.db import IntegrityError, connections, router, transaction
from django.utils import timezone

from django_ox.compat import default_task_backend
from django_ox.models import OxSchedule, OxScheduleChange, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    create_schedule,
    create_schedules,
    delete_schedule,
    schedule_db_alias,
    update_schedule,
)

from . import tasks

ALT = "alt"


class _AllToAlt:
    """Every django-ox model on one database that is not the default."""

    def db_for_read(self, model, **hints):
        return ALT if model._meta.app_label == "django_ox" else None

    db_for_write = db_for_read

    def allow_migrate(self, db, app_label, **hints):
        if app_label == "django_ox":
            return db == ALT
        return None


class _SplitTicksOff:
    """The tick log somewhere the task table is not."""

    def db_for_read(self, model, **hints):
        if model._meta.object_name == "OxScheduleTick":
            return ALT
        return "default" if model._meta.app_label == "django_ox" else None

    db_for_write = db_for_read


@pytest.fixture
def _kinds(monkeypatch, settings):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }


def a_minutely(**over):
    fields = {
        "name": "minutely",
        "task_key": "report",
        "trigger": "cron",
        "cron": "* * * * *",
        "start_time": timezone.now() - timedelta(minutes=5),
    }
    fields.update(over)
    return create_schedule(**fields)


@pytest.mark.django_db(databases=["default", ALT])
@pytest.mark.usefixtures("_kinds")
class TestTheWritePathRunsOnTheAliasItWritesThrough:
    """
    A transaction opened with no alias runs on the default connection while
    the row it means to lock is read and written through the routed one. The
    lock then holds nothing, and the row write and the change-marker bump are
    not atomic, which is the lost update `update_schedule` exists to prevent.
    """

    @pytest.fixture(autouse=True)
    def _router(self, settings):
        settings.DATABASE_ROUTERS = [_AllToAlt()]
        assert schedule_db_alias() == ALT, "the router did not take effect"

    def _atomic_aliases(self, monkeypatch):
        """Every alias `django_ox.stored` opens a transaction on."""
        seen = []
        real = transaction.atomic

        def spy(using=None, **kwargs):
            seen.append(using)
            return real(using=using, **kwargs)

        monkeypatch.setattr("django_ox.stored.transaction.atomic", spy)
        return seen

    def test_create_opens_its_transaction_on_the_routed_alias(self, monkeypatch):
        seen = self._atomic_aliases(monkeypatch)
        a_minutely()
        assert ALT in seen, f"opened on {seen!r}, never on {ALT!r}"
        assert None not in seen, "a transaction was opened on the default connection"

    def test_a_batch_opens_its_transaction_on_the_routed_alias(self, monkeypatch):
        # Every row and the marker are one transaction only if they are
        # written on the connection it was opened on. Opened on the default
        # one, a batch that fails part way leaves its first rows behind.
        seen = self._atomic_aliases(monkeypatch)
        created = create_schedules(
            [
                {"name": name, "task_key": "report", "trigger": "cron", "cron": cron}
                for name, cron in (("hourly", "0 * * * *"), ("nightly", "0 2 * * *"))
            ]
        )
        assert ALT in seen, f"opened on {seen!r}, never on {ALT!r}"
        assert None not in seen, "a transaction was opened on the default connection"
        assert [row._state.db for row in created] == [ALT, ALT]
        assert OxSchedule.objects.using(ALT).count() == 2
        assert not OxSchedule.objects.using("default").exists()
        assert OxScheduleChange.objects.using(ALT).filter(id=1).exists()
        assert not OxScheduleChange.objects.using("default").exists()

    def test_a_batch_refused_at_the_insert_leaves_nothing_on_the_routed_alias(self):
        # What the alias is for, shown rather than read off a spy. The last
        # row's name is taken after the batch checked it, so the unique
        # index refuses that row with the first one already written, and
        # the first has to go back with it on the database it was written to.
        landed = []

        def the_last_name_is_taken_first(execute, sql, params, many, context):
            if (
                not landed
                and sql.lstrip().upper().startswith("INSERT")
                and "oxschedule" in sql
                and "oxschedulechange" not in sql
            ):
                landed.append(True)
                now = timezone.now()
                OxSchedule.objects.using(ALT).create(
                    name="nightly",
                    task_key="report",
                    trigger="cron",
                    cron="0 5 * * *",
                    start_time=now,
                    created_at=now,
                    updated_at=now,
                )
            return execute(sql, params, many, context)

        with (
            connections[ALT].execute_wrapper(the_last_name_is_taken_first),
            pytest.raises(IntegrityError),
        ):
            create_schedules(
                [
                    {
                        "name": name,
                        "task_key": "report",
                        "trigger": "cron",
                        "cron": cron,
                    }
                    for name, cron in (
                        ("hourly", "0 * * * *"),
                        ("nightly", "0 2 * * *"),
                    )
                ]
            )
        assert landed, "the competing row was never written"
        assert not OxSchedule.objects.using(ALT).filter(name="hourly").exists(), (
            "the row written before the refused one outlived the batch"
        )
        assert not OxScheduleChange.objects.using(ALT).exists()

    def test_update_opens_its_transaction_on_the_routed_alias(self, monkeypatch):
        # Asserting `connections[ALT].in_atomic_block` here would prove
        # nothing: the test itself runs inside a transaction on both
        # declared aliases, so it reads True however the code behaves.
        # The alias the code asks for is the thing under test.
        row = a_minutely()
        seen = self._atomic_aliases(monkeypatch)
        update_schedule(row, cron="0 3 * * *")
        assert ALT in seen, f"opened on {seen!r}, never on {ALT!r}"
        assert None not in seen, "a transaction was opened on the default connection"
        row.refresh_from_db()
        assert row.cron == "0 3 * * *"

    def test_delete_bumps_the_marker_atomically_with_the_delete(self, monkeypatch):
        row = a_minutely()
        seen = self._atomic_aliases(monkeypatch)
        delete_schedule(row)
        assert ALT in seen
        assert None not in seen
        assert not OxSchedule.objects.filter(pk=row.pk).exists()
        assert OxScheduleChange.objects.using(ALT).filter(id=1).exists()

    def test_a_dispatched_tick_and_its_task_land_on_that_alias(self):
        from django_ox.worker import Worker

        worker = Worker()
        assert worker._db_alias == ALT
        a_minutely()
        assert worker.dispatch_schedules() == 1
        tick = OxScheduleTick.objects.using(ALT).get()
        assert tick.task_id is not None
        assert OxTask.objects.using(ALT).filter(id=tick.task_id).exists()


@pytest.mark.django_db
@pytest.mark.usefixtures("_kinds")
class TestSplittingTheModelsIsRefused:
    """
    The guarantee is that a due tick is enqueued once, and it holds because
    the task row and the tick row commit or roll back together. Two
    connections cannot do that, so the configuration is refused rather than
    half-supported.
    """

    def test_check_reports_the_tick_log_on_another_database(self, settings):
        settings.DATABASE_ROUTERS = [_SplitTicksOff()]
        assert router.db_for_write(OxScheduleTick) != router.db_for_write(OxTask)
        assert "django_ox.E008" in [e.id for e in default_task_backend.check()]

    def test_one_database_passes(self, settings):
        settings.DATABASE_ROUTERS = [_AllToAlt()]
        assert "django_ox.E008" not in [e.id for e in default_task_backend.check()]
