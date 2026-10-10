"""
A stored row that holds a value its field does not hold is left out and
named, never dispatched from what a converter made of it.

SQLite keeps whatever it is given, and Django's SQLite converters read
some of it as something else without a word: a start or an end that is
text and not a date reads as None, and an `enabled` of 2 or 'abc' reads as
False. A schedule built from that has no activation boundary, no end, or
a pause nobody made. The worker's full read of the rows and its read of
one row under its lock at dispatch both decode each value instead, and
leave such a row out with `schedule_row_skipped` naming the field. A row
that really is paused stays quiet, and an end that really is NULL still
means the schedule never ends.

A row whose task_key this deployment does not register is left out with
that said in so many words.
"""

import logging
from datetime import timedelta

import pytest
from django import forms
from django.db import connection
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.stored import create_schedule
from django_ox.worker import Worker

from . import tasks
from .test_stored_read import only_on, set_column

SQLITE_ONLY = "SQLite keeps this, and its converter reads it as something else"

#: (engine, column, SQL literal), each a value the read must refuse.
MISREAD = [
    pytest.param("sqlite", "start_time", "'banana'", id="sqlite-start-text"),
    pytest.param(
        "sqlite", "start_time", "'10000-01-01 00:00:00'", id="sqlite-start-year-10000"
    ),
    pytest.param("sqlite", "end_time", "'banana'", id="sqlite-end-text"),
    pytest.param(
        "sqlite", "end_time", "'10000-01-01 00:00:00'", id="sqlite-end-year-10000"
    ),
    pytest.param("sqlite", "enabled", "2", id="sqlite-enabled-2"),
    pytest.param("sqlite", "enabled", "'abc'", id="sqlite-enabled-text"),
    pytest.param("mysql", "enabled", "2", id="mysql-enabled-2"),
]


class RegionArgs(ArgsForm):
    """A form whose own clean raises KeyError, as a lookup in one might."""

    label = forms.CharField()

    def clean(self):
        raise KeyError("region")


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="labelled", task=tasks.labelled))


@pytest.fixture
def stored(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
                "SCHEDULE_RECONCILE_INTERVAL": 3600,
            },
        }
    }


def a_row(name):
    """Every minute since ten minutes ago, so a tick is due on every pass."""
    return create_schedule(
        name=name,
        task_key="labelled",
        trigger="cron",
        cron="* * * * *",
        arguments={"label": name},
        start_time=timezone.now() - timedelta(minutes=10),
        end_time=timezone.now() + timedelta(days=1),
    )


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def fired():
    return sorted(task.kwargs["label"] for task in OxTask.objects.all())


@pytest.mark.django_db
@pytest.mark.usefixtures("stored")
class TestAValueTheConverterWouldMisread:
    @pytest.mark.parametrize(("vendor", "column", "literal"), MISREAD)
    def test_the_full_read_leaves_the_row_out_and_names_the_field(
        self, caplog, vendor, column, literal
    ):
        only_on(vendor, SQLITE_ONLY)
        a_row("beside-it")
        victim = a_row("victim")
        set_column(victim.pk, column, literal)
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert fired() == ["beside-it"]
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{victim.pk}"
        ).exists()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert (skipped.schedule, skipped.schedule_pk) == ("victim", victim.pk)
        assert skipped.reason.startswith(f"its {column} holds ")
        # It still exists, so its report and its key are kept.
        assert f"db:{victim.pk}" in worker._schedule_source._stored_keys()

    @pytest.mark.parametrize(("vendor", "column", "literal"), MISREAD)
    def test_the_read_under_the_lock_at_dispatch_leaves_it_out_too(
        self, caplog, vendor, column, literal
    ):
        only_on(vendor, SQLITE_ONLY)
        a_row("beside-it")
        victim = a_row("victim")
        worker = Worker(backoff_initial=0)
        # The snapshot holds the row as it was; the value is changed after.
        assert len(worker._schedule_source.schedules()) == 2
        set_column(victim.pk, column, literal)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
        assert fired() == ["beside-it"]
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert skipped.reason.startswith(f"its {column} holds ")
        assert not events(caplog, "schedule_dispatch_error")
        assert [s.key for s in worker._schedule_source._cached] == [
            f"db:{OxSchedule.objects.get(name='beside-it').pk}"
        ]

    def test_an_end_that_is_null_still_never_ends(self, caplog):
        victim = a_row("no-end")
        set_column(victim.pk, "end_time", "NULL")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert Worker(backoff_initial=0).dispatch_schedules() == 1
        assert fired() == ["no-end"]
        assert not events(caplog, "schedule_row_skipped")

    @pytest.mark.parametrize(("literal", "fires"), [("0", False), ("1", True)])
    def test_enabled_0_and_1_are_paused_and_running(self, caplog, literal, fires):
        victim = a_row("victim")
        set_column(
            victim.pk,
            "enabled",
            literal
            if connection.vendor != "postgresql"
            else {"0": "FALSE", "1": "TRUE"}[literal],
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            Worker(backoff_initial=0).dispatch_schedules()
        assert fired() == (["victim"] if fires else [])
        assert not events(caplog, "schedule_row_skipped"), "a pause is not a skip"

    def test_a_paused_row_with_a_value_that_does_not_read_stays_quiet(self, caplog):
        only_on("sqlite", SQLITE_ONLY)
        victim = a_row("victim")
        set_column(victim.pk, "enabled", "0")
        set_column(victim.pk, "start_time", "'banana'")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert Worker(backoff_initial=0).dispatch_schedules() == 0
        assert not events(caplog, "schedule_row_skipped")


@pytest.mark.django_db
@pytest.mark.usefixtures("stored")
class TestATaskKeyThisDeploymentDoesNotRegister:
    def test_the_reason_says_it_is_not_registered(self, caplog):
        a_row("beside-it")
        victim = a_row("victim")
        set_column(victim.pk, "task_key", "'only.in.newer.code'")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert Worker(backoff_initial=0).dispatch_schedules() == 1
        assert fired() == ["beside-it"]
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert skipped.reason == (
            "its task_key 'only.in.newer.code' is not registered as a "
            "schedulable task in this deployment"
        )
        assert skipped.getMessage() == (
            f"Skipping stored schedule victim (pk {victim.pk}): {skipped.reason}"
        )

    def test_a_key_that_cannot_print_is_escaped(self, caplog):
        victim = a_row("victim")
        OxSchedule.objects.filter(pk=victim.pk).update(task_key="bad\x1b[2Jkey")
        with caplog.at_level(logging.INFO, logger="django_ox"):
            Worker(backoff_initial=0).dispatch_schedules()
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert "\x1b" not in skipped.reason
        assert "not registered as a schedulable task" in skipped.reason

    def test_a_key_error_from_the_task_s_form_is_not_called_a_registry_miss(
        self, caplog
    ):
        register(ScheduleKind(key="regional", task=tasks.labelled, form=RegionArgs))
        a_row("beside-it")
        now = timezone.now()
        victim = OxSchedule.objects.create(
            name="victim",
            task_key="regional",
            trigger="cron",
            cron="* * * * *",
            arguments={"label": "victim"},
            start_time=now - timedelta(minutes=10),
            created_at=now,
            updated_at=now,
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert Worker(backoff_initial=0).dispatch_schedules() == 1
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == victim.pk
        assert "not registered" not in skipped.reason
        assert skipped.reason == "'region'"
