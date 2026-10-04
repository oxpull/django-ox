"""
An interval, a phase and a starting deadline are seconds a timedelta holds.

A worker turns each of the three into a timedelta when it builds a stored
schedule, and a timedelta holds 86,399,999,999,999 whole seconds. One more
and building it raises, the row is skipped, and it is skipped again at
every read after that. SQLite's integer column is wider than a timedelta,
so there a schedule of 10**15 seconds validated, was stored, and never
fired: no refusal when it was written, and a `schedule_row_skipped` line at
every full read of the table for as long as it stayed.

`validate_schedule` refuses the number instead, on every path that writes.
A phase and a deadline are held to what a timedelta holds. An interval is
held to less, the longest the tick arithmetic can count back, which is
tests/test_interval_ceiling.py's subject; here it is the number an
interval of 10**15 seconds is refused against.

PostgreSQL's and MySQL's integer columns are narrower than either limit, so
on those the column's own range has refused such a number already and the
rule has nothing to add. The tests say which of the two answered rather
than skip: `what_is_said_of` is the refusal to expect on the engine the
suite is running on.
"""

import logging
from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.db import connection
from django.urls import reverse
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import (
    DatabaseScheduleSource,
    _export_preflight,
    create_schedule,
    create_schedules,
    update_schedule,
    validate_schedule,
)
from django_ox.worker import Worker

from . import tasks

pytestmark = pytest.mark.django_db

#: What a timedelta holds, in whole seconds. Written out, because the number
#: is the contract and not something to be derived the way the code derives it.
LONGEST = 86_399_999_999_999

#: The most an interval may be, written out for the same reason: the seconds
#: from year 1 to 1970.
LONGEST_INTERVAL = 62_135_596_800

#: A value above both the interval ceiling and the timedelta limit.
REPORTED = 10**15

ADD = "admin:django_ox_oxschedule_add"
CHANGE = "admin:django_ox_oxschedule_change"


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))


@pytest.fixture
def operator(client):
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
        "cron": "* * * * *",
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


def shown(response):
    """The errors the admin put beside the form's fields."""
    return {
        field: list(messages)
        for field, messages in response.context["adminform"].form.errors.items()
    }


def the_column_holds():
    """The most the default database's integer column takes."""
    _, highest = connection.ops.integer_field_range("PositiveIntegerField")
    return highest


def the_column_is_wider_than_a_timedelta():
    """True on SQLite, the one engine where the rule is what answers."""
    return the_column_holds() > LONGEST


def what_is_said_of(value, column="every_seconds"):
    """
    The one refusal of a number of seconds past what `column` may be.

    Where the column takes the number, the rule refuses it, against the
    field's own limit. Where the column is the narrower of the two, Django's
    own range validator has refused it in the field's cleaning, and the rule
    does not say it again.
    """
    limit = LONGEST_INTERVAL if column == "every_seconds" else LONGEST
    assert value > limit
    highest = the_column_holds()
    if value <= highest:
        return stored._TOO_LONG_FOR_A_SCHEDULE % {"limit": limit}
    with pytest.raises(ValidationError) as caught:
        MaxValueValidator(highest)(value)
    (message,) = caught.value.messages
    return message


class TestTheLimit:
    def test_it_is_the_whole_seconds_of_the_longest_timedelta(self):
        assert stored._LONGEST_SECONDS == LONGEST
        assert timedelta(seconds=LONGEST) <= timedelta.max
        with pytest.raises(OverflowError):
            timedelta(seconds=LONGEST + 1)

    def test_the_message_names_it(self):
        said = stored._TOO_LONG_FOR_A_SCHEDULE % {"limit": stored._LONGEST_SECONDS}
        assert said == "Enter a value of 86399999999999 seconds or less."

    def test_what_a_caller_reads_on_the_engine_this_runs_on(self):
        with pytest.raises(ValidationError) as caught:
            create_schedule(**a_cron(starting_deadline_seconds=REPORTED))
        (message,) = caught.value.message_dict["starting_deadline_seconds"]
        if the_column_is_wider_than_a_timedelta():
            assert message == "Enter a value of 86399999999999 seconds or less."
        else:
            # PostgreSQL and MySQL: the column's range, in Django's words.
            assert str(the_column_holds()) in message
            assert "86399999999999" not in message


class TestTheBoundary:
    def test_the_limit_is_taken_and_one_second_more_is_not(self):
        # The deadline, which is the one of the three an accepted row can
        # carry at this limit: an interval stops short of it, and a phase
        # is under its interval.
        column = "starting_deadline_seconds"
        said = what_is_said_of(LONGEST + 1, column)
        at = a_cron(**{column: LONGEST})
        over = a_cron("one-more", **{column: LONGEST + 1})
        if the_column_is_wider_than_a_timedelta():
            row = create_schedule(**at)
            assert getattr(OxSchedule.objects.get(pk=row.pk), column) == LONGEST
            with pytest.raises(ValidationError) as caught:
                create_schedule(**over)
            assert caught.value.message_dict == {column: [said]}
            assert [error.code for error in caught.value.error_dict[column]] == [
                "max_value"
            ]
            assert not OxSchedule.objects.filter(name="one-more").exists()
            return
        # PostgreSQL and MySQL. The column holds less than a timedelta, so
        # the limit itself is already more than it takes: both are refused
        # by the column's own range, once, and neither is stored.
        for refused in (at, over):
            with pytest.raises(ValidationError) as caught:
                create_schedule(**refused)
            assert caught.value.message_dict == {column: [said]}
        assert not OxSchedule.objects.exists()

    def test_a_phase_is_held_to_it_as_well(self):
        # A phase has to be under its interval, and the interval cannot pass
        # its own, lower limit, so no accepted row carries a phase of this
        # limit. What shows the boundary is which fields are refused: with
        # the interval past its limit, a phase at this one is not, and a
        # phase one second past it is.
        too_long = what_is_said_of(LONGEST + 5, "every_seconds")
        said = what_is_said_of(LONGEST + 1, "phase_seconds")
        with pytest.raises(ValidationError) as at:
            create_schedule(
                **an_interval(every_seconds=LONGEST + 5, phase_seconds=LONGEST)
            )
        with pytest.raises(ValidationError) as over:
            create_schedule(
                **an_interval(every_seconds=LONGEST + 5, phase_seconds=LONGEST + 1)
            )
        assert over.value.message_dict == {
            "every_seconds": [too_long],
            "phase_seconds": [said],
        }
        if the_column_is_wider_than_a_timedelta():
            assert at.value.message_dict == {"every_seconds": [too_long]}
        else:
            # PostgreSQL and MySQL: a phase of the limit is past the column.
            assert at.value.message_dict == over.value.message_dict

    def test_the_largest_number_both_hold_is_a_schedule_a_worker_runs(
        self, settings, caplog
    ):
        # The other half of the defect: what is accepted has to be something
        # a worker builds and fires, not something it skips.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"
                },
            }
        }
        most = min(LONGEST, the_column_holds())
        longest_interval = min(LONGEST_INTERVAL, the_column_holds())
        since = timezone.now() - timedelta(minutes=5)
        create_schedule(
            **an_interval("longest-interval", every_seconds=longest_interval)
        )
        create_schedule(
            **a_cron(
                "longest-deadline", starting_deadline_seconds=most, start_time=since
            )
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            fired = Worker(backoff_initial=0).dispatch_schedules()
        assert sorted(schedule.name for schedule in built) == [
            "longest-deadline",
            "longest-interval",
        ]
        assert not [
            record
            for record in caplog.records
            if getattr(record, "event", None) == "schedule_row_skipped"
        ]
        # The minutely one is due. The other's next tick is further off than
        # a datetime reaches, which is what was asked for.
        assert fired == 1
        assert OxTask.objects.count() == OxScheduleTick.objects.count() == 1


class TestTheNumberThatWasAcceptedAndNeverFired:
    """
    `every_seconds=10**15`, through each way of writing a schedule.

    One refusal each, on the field, and nothing stored. Which rule gives it
    depends on the engine and `what_is_said_of` says which.
    """

    def test_creating_a_schedule(self):
        with pytest.raises(ValidationError) as caught:
            create_schedule(**an_interval(every_seconds=REPORTED))
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(REPORTED)]
        }
        assert not OxSchedule.objects.exists()

    def test_creating_a_batch(self):
        rows = [a_cron("fine"), an_interval("too-long", every_seconds=REPORTED)]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        (entry,) = caught.value.error_list
        assert entry.params == {
            "index": 1,
            "name": "too-long",
            "field": "every_seconds",
            "message": what_is_said_of(REPORTED),
        }
        assert entry.code == "max_value"
        assert not OxSchedule.objects.exists()

    def test_the_importer_s_preflight(self):
        assert _export_preflight(an_interval(every_seconds=REPORTED)) == [
            ("every_seconds", what_is_said_of(REPORTED))
        ]

    def test_updating_a_schedule(self):
        row = create_schedule(**an_interval())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, every_seconds=REPORTED)
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(REPORTED)]
        }
        row.refresh_from_db()
        assert (row.every_seconds, row.boundary_generation) == (60, 0)

    def test_the_admin_s_add_form(self, operator):
        response = operator.post(reverse(ADD), the_form_posts(every_seconds=REPORTED))
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [what_is_said_of(REPORTED)]}
        assert not OxSchedule.objects.exists()

    def test_the_admin_s_change_form(self, operator):
        row = create_schedule(**an_interval())
        response = operator.post(
            reverse(CHANGE, args=[row.pk]), the_form_posts(every_seconds=REPORTED)
        )
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [what_is_said_of(REPORTED)]}
        row.refresh_from_db()
        assert row.every_seconds == 60

    @pytest.mark.parametrize(
        ("change", "columns"),
        [
            pytest.param(
                {"starting_deadline_seconds": REPORTED},
                ["starting_deadline_seconds"],
                id="the deadline",
            ),
            pytest.param(
                {"every_seconds": REPORTED + 5, "phase_seconds": REPORTED},
                ["every_seconds", "phase_seconds"],
                id="the phase, under an interval as long",
            ),
        ],
    )
    def test_the_other_two_numbers_on_every_path_that_takes_them(self, change, columns):
        said = {column: [what_is_said_of(REPORTED, column)] for column in columns}
        fields = an_interval("too-long", **change)
        with pytest.raises(ValidationError) as created:
            create_schedule(**fields)
        assert created.value.message_dict == said
        assert _export_preflight(fields) == [
            (column, what_is_said_of(REPORTED, column)) for column in columns
        ]
        with pytest.raises(ValidationError) as batched:
            create_schedules([fields])
        assert [entry.params["field"] for entry in batched.value.error_list] == columns
        row = create_schedule(**an_interval("kept"))
        with pytest.raises(ValidationError) as updated:
            update_schedule(row, **change)
        assert updated.value.message_dict == said
        with pytest.raises(ValidationError) as direct:
            OxSchedule(**fields, start_time=timezone.now()).full_clean(
                exclude=["created_at", "updated_at"]
            )
        assert direct.value.message_dict == said

    def test_a_number_given_as_text_is_the_same_number(self):
        with pytest.raises(ValidationError) as caught:
            create_schedule(**an_interval(every_seconds=str(REPORTED)))
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(REPORTED)]
        }

    def test_a_rule_about_the_same_field_is_still_said_after_it(self):
        # A cron schedule given an interval past the longest: too long, and
        # not wanted at all. Range first, as a column's range is.
        with pytest.raises(ValidationError) as caught:
            create_schedule(**a_cron(every_seconds=REPORTED))
        assert caught.value.message_dict == {
            "every_seconds": [
                what_is_said_of(REPORTED),
                "A cron schedule has no interval.",
            ]
        }


class TestSuchARowWrittenAroundTheRules:
    def test_it_is_skipped_at_every_read_and_never_fires(self, settings, caplog):
        """
        What refusing the number at the write prevents, shown at the read.

        A row put in the table without the write functions is the row
        `create_schedule` used to put there. A worker will not build it,
        skips it, and goes on to the next, so it never fires and the only
        sign is the log line.
        """
        if not the_column_is_wider_than_a_timedelta():
            pytest.skip(
                f"{connection.vendor}'s integer column holds {the_column_holds()} "
                f"at most, so no row can carry {REPORTED} seconds to be read "
                "back; SQLite is the engine this row exists on"
            )
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"
                },
            }
        }
        now = timezone.now()
        since = now - timedelta(minutes=5)
        OxSchedule.objects.create(
            **an_interval("too-long", every_seconds=REPORTED),
            start_time=since,
            created_at=now,
            updated_at=now,
        )
        create_schedule(**a_cron("beside-it", start_time=since))
        # The rules refuse the row as it stands, which is the fix.
        with pytest.raises(ValidationError) as caught:
            validate_schedule(OxSchedule.objects.get(name="too-long"))
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(REPORTED)]
        }
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            fired = Worker(backoff_initial=0).dispatch_schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        skipped = [
            record
            for record in caplog.records
            if getattr(record, "event", None) == "schedule_row_skipped"
        ]
        assert {record.schedule for record in skipped} == {"too-long"}
        assert fired == 1, "the schedule beside it still fires"
