"""
An integer column holds what the database it lives in can hold.

Django gives an integer field its range from the default connection, and
the stored schedules need not live in the default database. Routed to
another, a field's own cleaning passes an interval the routed column
cannot hold, and the refusal is left to the write: a database error out of
`create_schedule` and `update_schedule`, a server error out of the admin,
and a row the importer printed whose paste then fails.

So `validate_schedule` holds each integer column to the range of the alias
the row is validated against. Every path that validates then gives the same
field error, once: creating one schedule or a batch, updating one, the
admin's add and change forms, and the importer's preflight.

The destination here is made narrower than any real database, so a value
can sit inside the default connection's range and outside the destination's
whichever engine the suite is running on.
"""

import contextlib

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.db import connection, connections
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleChange, validate_against
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    _export_preflight,
    create_schedule,
    create_schedules,
    schedule_db_alias,
    update_schedule,
    validate_schedule,
)

from . import tasks

ALT = "alt"

#: What the destination's integer columns are made to hold.
NARROW = 1000

ADD = "admin:django_ox_oxschedule_add"
CHANGE = "admin:django_ox_oxschedule_change"


class _AllToAlt:
    """Every django-ox model on one database that is not the default."""

    def db_for_read(self, model, **hints):
        return ALT if model._meta.app_label == "django_ox" else None

    db_for_write = db_for_read

    def allow_migrate(self, db, app_label, **hints):
        if app_label == "django_ox":
            return db == ALT
        return None


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))


@pytest.fixture(autouse=True)
def _a_narrow_alt(monkeypatch):
    monkeypatch.setattr(
        connections[ALT].ops, "integer_field_range", lambda internal_type: (0, NARROW)
    )


@pytest.fixture
def routed(settings):
    settings.DATABASE_ROUTERS = [_AllToAlt()]
    assert schedule_db_alias() == ALT, "the router did not take effect"


@pytest.fixture
def operator(client):
    """A superuser, logged in. The users live in the default database."""
    client.force_login(User.objects.create_superuser("root", "root@example.com", "pw"))
    return client


def an_interval(name="every-so-often", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "interval",
        "every_seconds": 60,
    }
    fields.update(over)
    return fields


def a_cron(name="nightly", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return fields


def the_form_posts(**over):
    """Every field the schedule form submits, the way a browser sends them."""
    data = {
        "name": "every-so-often",
        "task_key": "report",
        "trigger": "interval",
        "cron": "",
        "every_seconds": "60",
        "phase_seconds": "0",
        "arguments": "{}",
        "enabled": "on",
        "end_time_0": "",
        "end_time_1": "",
        "starting_deadline_seconds": "",
    }
    data.update({field: str(value) for field, value in over.items()})
    return data


def too_big(limit=NARROW):
    """What Django's own range validator says of `limit + 1`, whatever its wording."""
    with pytest.raises(ValidationError) as caught:
        MaxValueValidator(limit)(limit + 1)
    (message,) = caught.value.messages
    return message


def shown(response):
    """The errors the admin put beside the form's fields."""
    return {
        field: list(messages)
        for field, messages in response.context["adminform"].form.errors.items()
    }


@contextlib.contextmanager
def statements_on(*aliases):
    """Every statement the block runs on any of these connections."""
    with contextlib.ExitStack() as stack:
        captured = [
            stack.enter_context(CaptureQueriesContext(connections[alias]))
            for alias in aliases
        ]
        ran = []
        yield ran
        for context in captured:
            ran.extend(query["sql"] for query in context.captured_queries)


@pytest.mark.django_db(databases=["default", ALT])
@pytest.mark.usefixtures("routed")
class TestEveryPathAsksTheDestination:
    """
    One value the routed column cannot hold, put to each way of writing a
    schedule. Each answers with the field's own error, and each says it once.
    """

    def test_creating_a_schedule(self):
        with pytest.raises(ValidationError) as caught:
            create_schedule(**an_interval(every_seconds=NARROW + 1))
        assert caught.value.message_dict == {"every_seconds": [too_big()]}
        assert not OxSchedule.objects.using(ALT).exists()

    def test_creating_a_batch(self):
        rows = [a_cron("fine"), an_interval("over", every_seconds=NARROW + 1)]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        assert [
            (
                entry.params["index"],
                entry.params["name"],
                entry.params["field"],
                entry.params["message"],
            )
            for entry in caught.value.error_list
        ] == [(1, "over", "every_seconds", too_big())]
        assert not OxSchedule.objects.using(ALT).exists()

    def test_the_importer_s_preflight(self):
        fields = an_interval(every_seconds=NARROW + 1)
        assert _export_preflight(fields) == [("every_seconds", too_big())]

    def test_updating_a_schedule(self):
        row = create_schedule(**an_interval(every_seconds=NARROW))
        marked = OxScheduleChange.objects.using(ALT).get(id=1).changed_at
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, every_seconds=NARROW + 1)
        assert caught.value.message_dict == {"every_seconds": [too_big()]}
        kept = OxSchedule.objects.using(ALT).get(pk=row.pk)
        assert (kept.every_seconds, kept.boundary_generation) == (NARROW, 0)
        assert OxScheduleChange.objects.using(ALT).get(id=1).changed_at == marked

    def test_the_admin_s_add_form(self, operator):
        response = operator.post(reverse(ADD), the_form_posts(every_seconds=NARROW + 1))
        # Redisplayed with the error beside the field, where before the
        # form validated and the save was what failed.
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [too_big()]}
        assert not OxSchedule.objects.using(ALT).exists()

    def test_the_admin_s_change_form(self, operator):
        row = create_schedule(**an_interval(every_seconds=NARROW))
        response = operator.post(
            reverse(CHANGE, args=[row.pk]), the_form_posts(every_seconds=NARROW + 1)
        )
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [too_big()]}
        assert OxSchedule.objects.using(ALT).get(pk=row.pk).every_seconds == NARROW

    @pytest.mark.parametrize(
        ("over", "fields"),
        [
            ({"starting_deadline_seconds": NARROW + 1}, ["starting_deadline_seconds"]),
            (
                {"every_seconds": NARROW + 2, "phase_seconds": NARROW + 1},
                ["every_seconds", "phase_seconds"],
            ),
        ],
    )
    def test_every_integer_column_a_caller_writes_is_held_to_it(self, over, fields):
        row = create_schedule(**an_interval("kept"))
        assert [f for f, _ in _export_preflight(an_interval(**over))] == fields
        with pytest.raises(ValidationError) as created:
            create_schedule(**an_interval(**over))
        assert list(created.value.message_dict) == fields
        with pytest.raises(ValidationError) as updated:
            update_schedule(row, **over)
        assert list(updated.value.message_dict) == fields
        said = updated.value.message_dict.values()
        assert {message for found in said for message in found} == {too_big()}

    def test_a_range_and_a_rule_on_one_field_are_both_said_range_first(self):
        # The order the field's own cleaning gives where it holds the range
        # itself: what the column cannot hold, then what the rules object to.
        with pytest.raises(ValidationError) as caught:
            create_schedule(
                **an_interval(every_seconds=NARROW + 500, phase_seconds=NARROW + 600)
            )
        assert caught.value.message_dict == {
            "every_seconds": [too_big()],
            "phase_seconds": [too_big(), "The phase must be less than the interval."],
        }

    def test_the_largest_value_the_column_holds_goes_through_on_every_path(
        self, operator
    ):
        assert _export_preflight(an_interval(every_seconds=NARROW)) == []
        row = create_schedule(**an_interval("single", every_seconds=NARROW))
        create_schedules([an_interval("batched", every_seconds=NARROW)])
        update_schedule(row, starting_deadline_seconds=NARROW)
        added = operator.post(
            reverse(ADD), the_form_posts(name="typed-in", every_seconds=NARROW)
        )
        assert added.status_code == 302, shown(added)
        assert sorted(
            OxSchedule.objects.using(ALT).values_list("name", "every_seconds")
        ) == [("batched", NARROW), ("single", NARROW), ("typed-in", NARROW)]
        assert not OxSchedule.objects.using("default").exists()

    def test_reading_the_range_runs_no_statement_on_either_database(self):
        with statements_on("default", ALT) as ran:
            _export_preflight(an_interval(every_seconds=NARROW + 1))
            _export_preflight(an_interval(every_seconds=NARROW))
        assert ran == []


@pytest.mark.django_db(databases=["default", ALT])
class TestWhichAliasIsTheDestination:
    """
    The alias the validation was given, and the alias the row is written to
    when it was given none. Never the default connection for want of asking.
    """

    def test_with_no_router_the_default_database_decides_on_every_path(self, operator):
        # The schedules live in the default database, whose range the
        # narrowed alias has nothing to say about. This is every deployment
        # with one database, and nothing changes for it.
        assert schedule_db_alias() == "default"
        fields = an_interval(every_seconds=NARROW + 1)
        assert _export_preflight(fields) == []
        row = create_schedule(**fields)
        create_schedules([an_interval("batched", every_seconds=NARROW + 1)])
        update_schedule(row, every_seconds=NARROW + 2)
        added = operator.post(
            reverse(ADD), the_form_posts(name="typed-in", every_seconds=NARROW + 3)
        )
        assert added.status_code == 302, shown(added)
        changed = operator.post(
            reverse(CHANGE, args=[row.pk]), the_form_posts(every_seconds=NARROW + 4)
        )
        assert changed.status_code == 302, shown(changed)
        assert sorted(OxSchedule.objects.values_list("name", "every_seconds")) == [
            ("batched", NARROW + 1),
            ("every-so-often", NARROW + 4),
            ("typed-in", NARROW + 3),
        ]

    def test_the_alias_a_validation_was_given_decides_before_the_router_is_asked(
        self,
    ):
        # No router, so the write alias is the default one. A validation
        # told to answer to another alias answers to that one's columns,
        # as its name check already does.
        now = timezone.now()
        row = OxSchedule(
            **an_interval(every_seconds=NARROW + 1),
            start_time=now,
            created_at=now,
            updated_at=now,
        )
        row.full_clean()
        with validate_against(ALT), pytest.raises(ValidationError) as caught:
            row.full_clean()
        assert caught.value.message_dict == {"every_seconds": [too_big()]}

    @pytest.mark.usefixtures("routed")
    def test_the_rules_alone_ask_the_alias_the_row_is_written_to(self):
        # Called with nothing said about an alias, which is how the admin's
        # form reaches it: through the model's clean, from Django's frame.
        now = timezone.now()
        row = OxSchedule(**an_interval(every_seconds=NARROW + 1), start_time=now)
        with pytest.raises(ValidationError) as caught:
            validate_schedule(row)
        assert caught.value.message_dict == {"every_seconds": [too_big()]}
        with pytest.raises(ValidationError) as caught:
            row.clean()
        assert caught.value.message_dict == {"every_seconds": [too_big()]}
        # Put to the rules before its fields were cleaned, a number may
        # still be text. It is held to the range as the number it would be
        # stored as.
        row.every_seconds = str(NARROW + 1)
        with pytest.raises(ValidationError) as caught:
            validate_schedule(row)
        assert caught.value.message_dict == {"every_seconds": [too_big()]}


@pytest.mark.django_db(databases=["default", ALT])
class TestARangeTheFieldHoldsItselfIsSaidOnce:
    """
    Where the destination is the default connection, Django's own validators
    already hold its range, and the rules add nothing to what they say.
    """

    @pytest.fixture
    def beyond(self):
        _, highest = connection.ops.integer_field_range("PositiveIntegerField")
        return highest + 1, too_big(highest)

    def test_creating_and_asking(self, beyond):
        value, message = beyond
        fields = an_interval(every_seconds=value)
        assert _export_preflight(fields) == [("every_seconds", message)]
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields)
        assert caught.value.message_dict == {"every_seconds": [message]}

    def test_updating(self, beyond):
        value, message = beyond
        row = create_schedule(**an_interval())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, starting_deadline_seconds=value)
        assert caught.value.message_dict == {"starting_deadline_seconds": [message]}

    def test_the_admin_s_form(self, operator, beyond):
        value, message = beyond
        response = operator.post(reverse(ADD), the_form_posts(every_seconds=value))
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [message]}

    @pytest.mark.usefixtures("routed")
    def test_routed_to_an_alias_with_the_same_range_nothing_is_added(
        self, monkeypatch, beyond
    ):
        # Two databases using the same vendor.
        value, message = beyond
        monkeypatch.setattr(
            connections[ALT].ops,
            "integer_field_range",
            connection.ops.integer_field_range,
        )
        fields = an_interval(every_seconds=value)
        assert _export_preflight(fields) == [("every_seconds", message)]
        within = create_schedule(**an_interval(every_seconds=NARROW + 1))
        assert OxSchedule.objects.using(ALT).get(pk=within.pk).every_seconds == (
            NARROW + 1
        )
