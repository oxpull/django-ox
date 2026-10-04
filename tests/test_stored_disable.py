"""
A readable stored schedule that no longer validates can still be disabled.

Disabling is how an operator stops a schedule, and the schedule most in
need of stopping is one that no longer validates: a row stored under 1.7.0
with an interval past the interval ceiling, or one written around the
write functions with a cron that does not parse.

`enabled=False`, alone, is now written without validating the rest of the
row: under the row's lock, with the registry permission checked, the
boundary handled as a pause handles it, and the workers told. Enabling
such a row, or changing anything else on it, is still validated in full,
and the admin's "Enable selected schedules" reports the rows it could not
enable instead of failing on them.

An interval past the ceiling can only be stored on SQLite; PostgreSQL's and
MySQL's columns end below it. A cron that does not parse can be stored on
every engine, so every engine runs the cases that use it.
"""

import logging
from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import connection
from django.urls import reverse
from django.utils import timezone

from django_ox import stored
from django_ox.admin import _COULD_NOT_ENABLE
from django_ox.models import OxSchedule, OxScheduleChange
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    DatabaseScheduleSource,
    boundary_digest,
    create_schedule,
    update_schedule,
)

from . import tasks

CHANGELIST = "admin:django_ox_oxschedule_changelist"

#: Past the interval ceiling, as a 1.7.0 row could hold it.
PAST_THE_CEILING = 10**12


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.record))
    register(
        ScheduleKind(key="guarded", task=tasks.record, permission="auth.view_user")
    )


@pytest.fixture
def operator(client):
    client.force_login(User.objects.create_superuser("root", "root@example.com", "pw"))
    return client


pytestmark = pytest.mark.django_db


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def a_row(name, **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "interval",
        "every_seconds": 60,
        "start_time": timezone.now() - timedelta(minutes=10),
    }
    fields.update(over)
    return create_schedule(**fields)


def stored_as_1_7_0_would(name, **values):
    """
    A row holding values that no longer validate, with its boundary set for
    them, as 1.7.0's create_schedule left a row it accepted then.
    """
    row = a_row(name)
    OxSchedule.objects.filter(pk=row.pk).update(**values)
    row = OxSchedule.objects.get(pk=row.pk)
    OxSchedule.objects.filter(pk=row.pk).update(boundary_for=boundary_digest(row))
    return OxSchedule.objects.get(pk=row.pk)


def past_the_ceiling(name="past-the-ceiling"):
    _, highest = connection.ops.integer_field_range("PositiveIntegerField")
    if highest < PAST_THE_CEILING:
        pytest.skip(
            f"{connection.vendor}'s integer column holds {highest} at most, so no "
            "row can carry an interval past the ceiling; SQLite is where 1.7.0 "
            "stored one"
        )
    return stored_as_1_7_0_would(name, every_seconds=PAST_THE_CEILING)


def bad_cron(name="bad-cron"):
    return stored_as_1_7_0_would(
        name, trigger="cron", cron="banana", every_seconds=None
    )


INVALID = [
    pytest.param(past_the_ceiling, id="an interval past the ceiling"),
    pytest.param(bad_cron, id="a cron that does not parse"),
]


class TestDisablingThroughTheFunction:
    @pytest.mark.parametrize("invalid", INVALID)
    def test_the_row_is_disabled_and_the_workers_told(self, invalid):
        row = invalid()
        with pytest.raises(ValidationError):
            stored.validate_schedule(row)
        marker = OxScheduleChange.objects.get().changed_at
        before = OxSchedule.objects.get(pk=row.pk)
        returned = update_schedule(row, enabled=False)
        after = OxSchedule.objects.get(pk=row.pk)
        assert returned is row and row.enabled is False
        assert after.enabled is False
        assert after.updated_at > before.updated_at
        assert OxScheduleChange.objects.get().changed_at > marker
        # The boundary is over the row as it now is, so no worker heals it.
        assert after.boundary_for == boundary_digest(after)
        # Nothing else moved: a pause is not a retime.
        assert after.start_time == before.start_time
        assert after.boundary_generation == before.boundary_generation
        for field in ("every_seconds", "cron", "trigger", "name"):
            assert getattr(after, field) == getattr(before, field)

    @pytest.mark.parametrize("invalid", INVALID)
    def test_enabling_it_again_is_still_refused(self, invalid):
        row = invalid()
        update_schedule(row, enabled=False)
        with pytest.raises(ValidationError):
            update_schedule(row, enabled=True)
        assert OxSchedule.objects.get(pk=row.pk).enabled is False

    @pytest.mark.parametrize("invalid", INVALID)
    def test_any_other_change_beside_it_is_validated_in_full(self, invalid):
        row = invalid()
        with pytest.raises(ValidationError):
            update_schedule(row, enabled=False, name="renamed")
        after = OxSchedule.objects.get(pk=row.pk)
        assert (after.enabled, after.name) == (True, row.name)

    @pytest.mark.parametrize("false", [0, "False", "0", "f"])
    def test_false_as_the_field_reads_it_is_the_same_request(self, false):
        row = bad_cron()
        update_schedule(row, enabled=false)
        assert OxSchedule.objects.get(pk=row.pk).enabled is False

    @pytest.mark.parametrize("value", ["maybe", "false", None])
    def test_a_value_that_is_not_a_boolean_is_not_a_pause(self, value):
        # Not one the field reads as False, so the request is validated in
        # full, and the row's own problem is said with the field's.
        row = bad_cron()
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, enabled=value)
        assert set(caught.value.message_dict) == {"enabled", "cron"}
        assert OxSchedule.objects.get(pk=row.pk).enabled is True

    def test_the_registry_permission_still_decides(self):
        row = stored_as_1_7_0_would(
            "guarded",
            task_key="guarded",
            trigger="cron",
            cron="banana",
            every_seconds=None,
        )

        class _NoOne:
            def has_perm(self, perm, obj=None):
                return False

        with pytest.raises(PermissionDenied):
            update_schedule(row, user=_NoOne(), enabled=False)
        assert OxSchedule.objects.get(pk=row.pk).enabled is True

    def test_a_stale_boundary_is_moved_as_a_pause_moves_it(self):
        # Retimed around the functions, then disabled: what update_schedule
        # does for a valid row whose boundary is stale.
        row = bad_cron()
        OxSchedule.objects.filter(pk=row.pk).update(cron="*/5 banana")
        before = OxSchedule.objects.get(pk=row.pk)
        update_schedule(row, enabled=False)
        after = OxSchedule.objects.get(pk=row.pk)
        assert after.start_time > before.start_time
        assert after.boundary_generation == before.boundary_generation + 1
        assert after.boundary_for == boundary_digest(after)

    def test_a_value_its_field_cannot_convert_is_left_as_it_is(self):
        if connection.vendor != "sqlite":
            pytest.skip(
                f"{connection.vendor} refuses text in an integer column; SQLite "
                "keeps it"
            )
        row = a_row("text-in-the-interval")
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE django_ox_oxschedule SET every_seconds = 'abc' WHERE id = %s",
                [row.pk],
            )
        before = OxSchedule.objects.get(pk=row.pk)
        update_schedule(row, enabled=False)
        after = OxSchedule.objects.get(pk=row.pk)
        assert after.enabled is False
        # Its digest cannot be taken, so its boundary is left alone.
        assert after.boundary_for == before.boundary_for

    def test_a_valid_row_is_disabled_as_before(self):
        row = a_row("valid")
        update_schedule(row, enabled=False)
        after = OxSchedule.objects.get(pk=row.pk)
        assert after.enabled is False
        assert after.boundary_for == boundary_digest(after)
        update_schedule(row, enabled=True)
        assert OxSchedule.objects.get(pk=row.pk).enabled is True


class TestTheAdminActions:
    @pytest.fixture(autouse=True)
    def _dispatched(self, settings):
        # A backend that dispatches stored schedules, so the page says
        # nothing about them being stored and never run.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"
                },
            }
        }

    def _act(self, client, action, rows):
        return client.post(
            reverse(CHANGELIST),
            {"action": action, "_selected_action": [str(row.pk) for row in rows]},
            follow=True,
        )

    def _said(self, response):
        return [str(message) for message in response.context["messages"]]

    def test_disabling_a_mixed_selection(self, operator):
        rows = [a_row("valid"), bad_cron()]
        if connection.ops.integer_field_range("PositiveIntegerField")[1] >= 10**12:
            rows.append(past_the_ceiling())
        response = self._act(operator, "disable_selected", rows)
        assert response.status_code == 200
        assert self._said(response) == [f"Disabled {len(rows)} schedule(s)."]
        assert not OxSchedule.objects.filter(enabled=True).exists()

    def test_enabling_a_mixed_selection_enables_what_validates(self, operator):
        valid, broken = a_row("valid"), bad_cron()
        for row in (valid, broken):
            update_schedule(row, enabled=False)
        response = self._act(operator, "enable_selected", [valid, broken])
        assert response.status_code == 200
        assert self._said(response) == [
            "Enabled 1 schedule(s).",
            _COULD_NOT_ENABLE.format(count=1),
        ]
        assert OxSchedule.objects.get(pk=valid.pk).enabled is True
        assert OxSchedule.objects.get(pk=broken.pk).enabled is False

    def test_a_row_past_the_ceiling_disabled_in_the_admin(self, operator):
        row = past_the_ceiling()
        response = self._act(operator, "disable_selected", [row])
        assert response.status_code == 200
        assert OxSchedule.objects.get(pk=row.pk).enabled is False


class TestAWorkerLearnsOfIt:
    def test_the_next_read_leaves_the_row_out_without_a_word(self, settings, caplog):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"
                },
            }
        }
        a_row("beside-it")
        row = bad_cron()
        # Long enough that only the marker the disable moves can make the
        # source read the rows again.
        source = DatabaseScheduleSource(
            {"SCHEDULE_RECONCILE_INTERVAL": 3600}, "default"
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert [s.name for s in source.schedules()] == ["beside-it"]
            assert len(events(caplog, "schedule_row_skipped")) == 1
            update_schedule(row, enabled=False)
            caplog.clear()
            assert [s.name for s in source.schedules()] == ["beside-it"]
        assert not events(caplog, "schedule_row_skipped")
        assert f"db:{row.pk}" in source._stored_keys()
        # A paused row's failure report is forgotten with its skip report.
        assert ("row", row.pk) not in source._report._failing
