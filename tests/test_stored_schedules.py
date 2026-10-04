"""Schedules that live in a database row."""

from datetime import UTC, datetime, timedelta

import pytest
from django import forms
from django.conf import settings as project
from django.core.exceptions import ValidationError
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule, OxScheduleChange
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.stored import (
    WRITABLE_FIELDS,
    _export_preflight,
    boundary_digest,
    create_schedule,
    create_schedules,
    update_schedule,
    validate_schedule,
)

from . import tasks

pytestmark = pytest.mark.django_db


class _Args(ArgsForm):
    region = forms.CharField()


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(ScheduleKind(key="checked", task=tasks.add, form=_Args))


def a_cron(**over):
    fields = {
        "name": "nightly",
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return create_schedule(**fields)


class TestTheRegistryIsTheBoundary:
    def test_a_registered_key_is_accepted(self):
        assert a_cron().task_key == "report"

    def test_an_unregistered_key_is_refused_by_the_service_function(self):
        with pytest.raises(ValidationError) as caught:
            a_cron(task_key="os.system")
        assert "task_key" in caught.value.message_dict

    def test_an_unregistered_key_is_refused_through_full_clean(self):
        # The path the admin takes, via ModelForm._post_clean.
        row = OxSchedule(
            name="x",
            task_key="os.system",
            trigger="cron",
            cron="0 2 * * *",
            start_time=timezone.now(),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        with pytest.raises(ValidationError) as caught:
            row.full_clean()
        assert "task_key" in caught.value.message_dict

    def test_the_error_names_the_keys_that_would_work(self):
        with pytest.raises(ValidationError) as caught:
            a_cron(task_key="nope")
        assert "checked, report" in str(caught.value.message_dict["task_key"])

    def test_objects_create_bypasses_validation_and_that_is_expected(self):
        # save() does not call full_clean(); Django documents this. So an
        # unvalidated row can exist, and the dispatch path has to expect
        # one rather than trust the table. Asserted so the expectation is
        # recorded rather than assumed.
        row = OxSchedule.objects.create(
            name="raw",
            task_key="os.system",
            trigger="cron",
            cron="0 2 * * *",
            start_time=timezone.now(),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        assert row.pk is not None


class TestTheWriteApiWritesOnlyWhatACallerOwns:
    """
    The activation boundary and the count of writes to it are one
    mechanism. A worker that finds a row stale records both with its
    sighting and compares both under the row's lock, so a boundary moved
    without the count moving is a boundary the worker cannot tell has been
    superseded: it heals on top of it and discards every tick in between.
    Neither is a caller's to set, and the admin already treats them that
    way.
    """

    def test_update_will_not_move_the_boundary_directly(self):
        row = a_cron()
        before = (row.start_time, row.boundary_generation)
        with pytest.raises(TypeError) as caught:
            update_schedule(row, start_time=timezone.now() + timedelta(days=3650))
        assert "start_time" in str(caught.value)
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == before

    def test_update_will_not_reset_the_count_of_boundary_writes(self):
        row = a_cron()
        update_schedule(row, cron="0 3 * * *")
        row.refresh_from_db()
        assert row.boundary_generation == 1
        with pytest.raises(TypeError) as caught:
            update_schedule(row, boundary_generation=0)
        assert "boundary_generation" in str(caught.value)
        row.refresh_from_db()
        assert row.boundary_generation == 1

    def test_create_will_not_start_the_count_anywhere_but_zero(self):
        with pytest.raises(TypeError):
            a_cron(boundary_generation=99)
        assert not OxSchedule.objects.exists(), "a refused call wrote a row"

    def test_create_still_takes_the_boundary_it_documents(self):
        when = timezone.now() - timedelta(days=2)
        row = a_cron(start_time=when)
        assert row.start_time == when
        assert row.boundary_generation == 0

    def test_a_misspelled_field_is_reported_rather_than_dropped(self):
        # setattr on the instance takes any name, so before this an
        # update_schedule(cronn=...) reported success and changed nothing.
        row = a_cron()
        with pytest.raises(TypeError) as caught:
            update_schedule(row, cronn="0 3 * * *")
        assert "cronn" in str(caught.value)
        row.refresh_from_db()
        assert row.cron == "0 2 * * *"

    def test_every_column_is_a_caller_s_or_this_module_s(self):
        # A column added later belongs to neither set until it is put in
        # one, so it cannot quietly become writable, or quietly stop being.
        package_written = {
            "id",
            "start_time",
            "boundary_for",
            "boundary_generation",
            "created_at",
            "updated_at",
        }
        columns = {field.name for field in OxSchedule._meta.concrete_fields}
        assert columns == WRITABLE_FIELDS | package_written

    def test_every_field_the_admin_submits_is_still_accepted(self):
        from django_ox.admin import OxScheduleForm

        assert set(OxScheduleForm.Meta.fields) <= WRITABLE_FIELDS


class TestArgumentValidation:
    def test_arguments_are_validated_against_the_registered_form(self):
        with pytest.raises(ValidationError) as caught:
            a_cron(name="a", task_key="checked", arguments={"wrong": 1})
        assert "arguments" in caught.value.message_dict

    def test_valid_arguments_are_accepted(self):
        row = a_cron(name="b", task_key="checked", arguments={"region": "emea"})
        assert row.arguments == {"region": "emea"}

    def test_arguments_must_be_a_mapping(self):
        with pytest.raises(ValidationError) as caught:
            a_cron(arguments=[1, 2])
        assert "arguments" in caught.value.message_dict


class TestTriggerValidation:
    @pytest.mark.parametrize(
        ("over", "field"),
        [
            ({"cron": "banana"}, "cron"),
            ({"cron": ""}, "cron"),
            ({"trigger": "interval", "cron": "0 2 * * *"}, "cron"),
            (
                {"trigger": "interval", "cron": "", "every_seconds": None},
                "every_seconds",
            ),
            ({"trigger": "interval", "cron": "", "every_seconds": 0}, "every_seconds"),
            (
                {
                    "trigger": "interval",
                    "cron": "",
                    "every_seconds": 60,
                    "phase_seconds": 60,
                },
                "phase_seconds",
            ),
        ],
    )
    def test_bad_triggers_are_refused(self, over, field):
        with pytest.raises(ValidationError) as caught:
            a_cron(**over)
        assert field in caught.value.message_dict

    def test_end_time_must_follow_start_time(self):
        now = timezone.now()
        with pytest.raises(ValidationError) as caught:
            a_cron(start_time=now, end_time=now - timedelta(hours=1))
        assert "end_time" in caught.value.message_dict


def fields_of(name="nightly", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return fields


def an_interval_of(name="every-minute", **over):
    return fields_of(
        name, **{"trigger": "interval", "cron": "", "every_seconds": 60, **over}
    )


#: A value its own column's field refuses, with the column and the code
#: Django gives the refusal.
REFUSED_BY_ITS_FIELD = [
    pytest.param(
        an_interval_of(every_seconds="soon"),
        "every_seconds",
        "invalid",
        id="an interval that is not a number",
    ),
    pytest.param(
        an_interval_of(every_seconds=[1]),
        "every_seconds",
        "invalid",
        id="an interval that is a list",
    ),
    pytest.param(
        an_interval_of(phase_seconds=None), "phase_seconds", "null", id="no phase"
    ),
    pytest.param(
        an_interval_of(phase_seconds="x"),
        "phase_seconds",
        "invalid",
        id="a phase that is not a number",
    ),
    pytest.param(
        fields_of(starting_deadline_seconds="soon"),
        "starting_deadline_seconds",
        "invalid",
        id="a deadline that is not a number",
    ),
    pytest.param(
        fields_of(end_time="soon"),
        "end_time",
        "invalid",
        id="an end that is not a time",
    ),
]


class TestAValueItsOwnFieldRefuses:
    """
    Django runs a model's rules whether or not its fields cleaned, and a
    field that did not clean leaves on the row whatever the caller gave. The
    rules compared it anyway: "soon" with one, nothing with sixty. That
    raised TypeError out of the whole validation, in place of the field's
    own report.

    The field has the first word and the only one. Each of these is refused
    once, by its field, and the rules have nothing to add.
    """

    @pytest.mark.parametrize(("fields", "field", "code"), REFUSED_BY_ITS_FIELD)
    def test_creating_reports_what_the_field_says_and_nothing_more(
        self, fields, field, code
    ):
        # What the fields say by themselves, before any rule has run.
        with pytest.raises(ValidationError) as cleaned:
            OxSchedule(**fields, start_time=timezone.now()).clean_fields(
                exclude=[
                    "boundary_for",
                    "boundary_generation",
                    "created_at",
                    "updated_at",
                ]
            )
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields)
        assert list(caught.value.message_dict) == [field]
        assert caught.value.message_dict == cleaned.value.message_dict
        assert [error.code for error in caught.value.error_dict[field]] == [code]
        assert not OxSchedule.objects.exists()

    @pytest.mark.parametrize(("fields", "field", "code"), REFUSED_BY_ITS_FIELD)
    def test_a_batch_holds_it_against_its_row(self, fields, field, code):
        with pytest.raises(ValidationError) as caught:
            create_schedules([fields_of("before"), {**fields, "name": "the-one"}])
        (entry,) = caught.value.error_list
        assert (
            entry.params["index"],
            entry.params["name"],
            entry.params["field"],
            entry.code,
        ) == (1, "the-one", field, code)
        assert not OxSchedule.objects.exists()

    @pytest.mark.parametrize(("fields", "field", "code"), REFUSED_BY_ITS_FIELD)
    def test_the_importer_s_preflight_lists_it(self, fields, field, code):
        assert [named for named, _ in _export_preflight(fields)] == [field]

    @pytest.mark.parametrize(("fields", "field", "code"), REFUSED_BY_ITS_FIELD)
    def test_the_rules_put_to_it_directly_have_nothing_to_raise(
        self, fields, field, code
    ):
        # The model's own clean(), reached without its fields having been
        # cleaned first. Not a path this package takes, but one Django
        # leaves open.
        row = OxSchedule(**fields, start_time=timezone.now())
        validate_schedule(row)
        row.clean()

    @pytest.mark.parametrize(
        ("change", "field", "code"),
        [
            ({"phase_seconds": None}, "phase_seconds", "null"),
            (
                {"starting_deadline_seconds": "soon"},
                "starting_deadline_seconds",
                "invalid",
            ),
            ({"end_time": "soon"}, "end_time", "invalid"),
        ],
    )
    def test_updating_reports_it_against_the_field(self, change, field, code):
        row = create_schedule(**an_interval_of())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, **change)
        assert list(caught.value.message_dict) == [field]
        assert [error.code for error in caught.value.error_dict[field]] == [code]
        row.refresh_from_db()
        assert (row.phase_seconds, row.starting_deadline_seconds, row.end_time) == (
            0,
            None,
            None,
        )

    def test_a_number_given_as_text_is_still_held_to_the_rules(self):
        # Not every value that is not yet a number is one the field
        # refuses. The rules read it as the field would store it.
        with pytest.raises(ValidationError) as caught:
            create_schedule(**an_interval_of(every_seconds="0"))
        assert list(caught.value.message_dict) == ["every_seconds"]
        with pytest.raises(ValidationError) as caught:
            validate_schedule(
                OxSchedule(
                    **an_interval_of(every_seconds="60", phase_seconds="60"),
                    start_time=timezone.now(),
                )
            )
        assert caught.value.message_dict == {
            "phase_seconds": ["The phase must be less than the interval."]
        }

    def test_a_rule_that_needs_no_number_still_speaks(self):
        # A cron schedule has no interval, whatever the interval is.
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields_of(every_seconds="soon"))
        assert [error.code for error in caught.value.error_dict["every_seconds"]] == [
            "invalid",
            None,
        ]
        assert caught.value.message_dict["every_seconds"][1] == (
            "A cron schedule has no interval."
        )


#: What Django calls empty and no column can hold. Its field cleaning passes
#: over any of them in a column that may be blank.
EMPTY_AND_NOT_NONE = ["", [], (), {}]


def what_the_field_would_say(column, value):
    """The column's own refusal of a value it cannot read."""
    field = OxSchedule._meta.get_field(column)
    return field.error_messages["invalid"] % {"value": value}


class TestAnEmptyValueItsFieldNeverLookedAt:
    """
    Django's field cleaning skips an empty value in a column that may be
    blank and says why: "The developer is responsible for making sure they
    have a valid value." So "" in the interval, the deadline or the end
    reaches the rules exactly as given, and so do [], () and {}. Of what
    Django calls empty, only None is something those columns hold.

    The rules compared these too, and raised TypeError. Passing them over in
    turn would be worse: nothing else looks until the row is written, and
    what is raised then names no field. So the rules make the refusal the
    field never made, in the field's own words.
    """

    @pytest.mark.parametrize("empty", EMPTY_AND_NOT_NONE, ids=repr)
    @pytest.mark.parametrize(
        ("fields", "column"),
        [
            pytest.param(an_interval_of(), "every_seconds", id="the interval"),
            pytest.param(fields_of(), "starting_deadline_seconds", id="the deadline"),
            pytest.param(fields_of(), "end_time", id="the end"),
        ],
    )
    def test_it_is_refused_once_on_every_path_that_creates(self, fields, column, empty):
        fields = {**fields, column: empty}
        refusal = what_the_field_would_say(column, empty)
        # The control: the fields' own cleaning has nothing to say of it.
        OxSchedule(**fields, start_time=timezone.now()).clean_fields(
            exclude=["boundary_for", "boundary_generation", "created_at", "updated_at"]
        )
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields)
        assert caught.value.message_dict == {column: [refusal]}
        assert [error.code for error in caught.value.error_dict[column]] == ["invalid"]
        assert _export_preflight(fields) == [(column, refusal)]
        with pytest.raises(ValidationError) as caught:
            create_schedules([fields_of("before"), {**fields, "name": "the-one"}])
        (entry,) = caught.value.error_list
        assert entry.params == {
            "index": 1,
            "name": "the-one",
            "field": column,
            "message": refusal,
        }
        with pytest.raises(ValidationError) as caught:
            validate_schedule(OxSchedule(**fields, start_time=timezone.now()))
        assert caught.value.message_dict == {column: [refusal]}
        assert not OxSchedule.objects.exists()

    @pytest.mark.parametrize("empty", EMPTY_AND_NOT_NONE, ids=repr)
    @pytest.mark.parametrize("column", ["starting_deadline_seconds", "end_time"])
    def test_updating_refuses_it_the_same_way(self, column, empty):
        row = create_schedule(**an_interval_of())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, **{column: empty})
        assert caught.value.message_dict == {
            column: [what_the_field_would_say(column, empty)]
        }
        row.refresh_from_db()
        assert (row.starting_deadline_seconds, row.end_time) == (None, None)

    @pytest.mark.parametrize("empty", EMPTY_AND_NOT_NONE, ids=repr)
    def test_where_a_rule_refuses_the_field_the_rule_is_what_is_said(self, empty):
        # A cron schedule given an interval, as it always was: the rule's
        # reason is the one a person can act on, and one refusal is enough.
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields_of(every_seconds=empty))
        assert caught.value.message_dict == {
            "every_seconds": ["A cron schedule has no interval."]
        }

    def test_none_is_the_one_empty_value_these_columns_hold(self):
        row = create_schedule(
            **fields_of(
                every_seconds=None, starting_deadline_seconds=None, end_time=None
            )
        )
        assert (row.every_seconds, row.starting_deadline_seconds, row.end_time) == (
            None,
            None,
            None,
        )


def a_time_the_setting_does_not_expect(**shift):
    """Without a zone while USE_TZ is on, and with one while it is off."""
    late = datetime(2099, 1, 1) + timedelta(**shift)
    return late if project.USE_TZ else late.replace(tzinfo=UTC)


def cannot_be_compared():
    """What is said of such a time when the other bound is the usual kind."""
    if project.USE_TZ:
        return stored._TIME_WITHOUT_A_ZONE
    return stored._TIME_WITH_A_ZONE


class TestATimeThatCannotBeComparedWithTheOther:
    """
    Python will not order a time that has a zone against one that has none,
    and no field objects to either by itself: a naive datetime cleans, and
    so does an aware one. So the rule that the end follows the start raised
    TypeError the moment the two differed, which under USE_TZ is any end
    time written as `datetime(2027, 1, 1)` beside an aware start.

    It is a field error now, on the bound the setting does not expect, since
    that is the one the caller wrote and can put right.
    """

    @pytest.fixture(params=[True, False], ids=["USE_TZ on", "USE_TZ off"])
    def use_tz(self, request, settings):
        settings.USE_TZ = request.param
        return request.param

    def test_an_end_of_the_other_kind_is_refused_on_the_end(self, use_tz):
        fields = fields_of(end_time=a_time_the_setting_does_not_expect())
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields)
        assert caught.value.message_dict == {"end_time": [cannot_be_compared()]}
        assert _export_preflight(fields) == [("end_time", cannot_be_compared())]
        assert not OxSchedule.objects.exists()

    def test_a_start_of_the_other_kind_is_refused_on_the_start(self, use_tz):
        # The reverse: the end is the usual kind, and the start is the one
        # the caller wrote without a zone, or with one.
        fields = fields_of(
            start_time=a_time_the_setting_does_not_expect(days=-36500),
            end_time=timezone.now() + timedelta(days=1),
        )
        with pytest.raises(ValidationError) as caught:
            create_schedule(**fields)
        assert caught.value.message_dict == {"start_time": [cannot_be_compared()]}
        assert _export_preflight(fields) == [("start_time", cannot_be_compared())]

    def test_a_batch_holds_it_against_its_row(self, use_tz):
        rows = [
            fields_of("before"),
            fields_of("the-one", end_time=a_time_the_setting_does_not_expect()),
        ]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        (entry,) = caught.value.error_list
        assert entry.params == {
            "index": 1,
            "name": "the-one",
            "field": "end_time",
            "message": cannot_be_compared(),
        }
        assert not OxSchedule.objects.exists()

    def test_the_rules_put_to_it_directly_say_the_same(self, use_tz):
        row = OxSchedule(
            **fields_of(end_time=a_time_the_setting_does_not_expect()),
            start_time=timezone.now(),
        )
        with pytest.raises(ValidationError) as caught:
            validate_schedule(row)
        assert caught.value.message_dict == {"end_time": [cannot_be_compared()]}

    def test_updating_refuses_it_and_leaves_the_row_alone(self):
        row = create_schedule(**fields_of())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, end_time=a_time_the_setting_does_not_expect())
        assert caught.value.message_dict == {"end_time": [cannot_be_compared()]}
        row.refresh_from_db()
        assert row.end_time is None
        assert row.boundary_generation == 0

    def test_two_times_of_one_kind_are_ordered_as_they_always_were(self):
        # Both of the kind the setting does not expect. They can be
        # compared, so the ordinary rule is the one that answers.
        with pytest.raises(ValidationError) as caught:
            create_schedule(
                **fields_of(
                    start_time=a_time_the_setting_does_not_expect(),
                    end_time=a_time_the_setting_does_not_expect(days=-1),
                )
            )
        assert caught.value.message_dict == {
            "end_time": ["The end time must be after the start time."]
        }


class TestTheDigestSurvivesTheRoundTrip:
    """
    The digest has to name what the column holds, not what the caller passed.

    A boundary digest is written by one process and recomputed by another
    from a row it read. If the two disagree the schedule reads as
    permanently stale: dispatch declines its tick and the boundary is moved
    onto the current timing, so the schedule loses the run it was created
    for.
    """

    def test_the_model_s_own_enum_digests_the_same_as_the_column(self):
        row = a_cron(trigger=OxSchedule.Trigger.CRON)
        assert row.boundary_for == boundary_digest(OxSchedule.objects.get(pk=row.pk)), (
            "a schedule created with the enum reads as stale forever"
        )

    def test_an_interval_given_as_a_string_digests_the_same_as_the_column(self):
        row = a_cron(
            name="every-minute", trigger="interval", cron="", every_seconds="60"
        )
        assert row.boundary_for == boundary_digest(OxSchedule.objects.get(pk=row.pk))

    def test_a_no_op_string_edit_is_not_a_retime(self):
        # The shape a JSON body or an env var produces. Reading "60" as a
        # change from 60 moves the boundary past a tick that was already
        # due, and that tick never fires.
        row = a_cron(name="every-minute", trigger="interval", cron="", every_seconds=60)
        before = row.start_time
        update_schedule(row, every_seconds="60")
        row.refresh_from_db()
        assert row.start_time == before, "a no-op edit moved the activation boundary"


class TestTheBoundary:
    def test_creation_sets_the_boundary_to_now_not_to_first_observation(self):
        before = timezone.now()
        row = a_cron()
        assert before <= row.start_time <= timezone.now()

    def test_retiming_moves_the_boundary(self):
        row = a_cron()
        original = row.start_time
        update_schedule(row, cron="0 3 * * *")
        assert row.start_time > original

    def test_editing_what_it_runs_does_not_move_the_boundary(self):
        # A schedule edited more often than its own period would never fire
        # if any edit re-anchored it, so the boundary tracks timing alone.
        row = a_cron(task_key="checked", arguments={"region": "emea"})
        original = row.start_time
        update_schedule(row, arguments={"region": "apac"})
        assert row.start_time == original

    def test_disabling_does_not_move_the_boundary(self):
        row = a_cron()
        original = row.start_time
        update_schedule(row, enabled=False)
        assert row.start_time == original

    def test_re_enabling_moves_the_boundary(self):
        # Otherwise a pause accumulates a backlog that fires at once on
        # resume, which is what an operator pausing something does not want.
        row = a_cron()
        update_schedule(row, enabled=False)
        paused_at = row.start_time
        update_schedule(row, enabled=True)
        assert row.start_time > paused_at


class TestARetimeGoingRoundTheWriteApi:
    def test_a_bulk_update_does_not_fire_retroactively(self):
        # queryset.update() runs no model code, so the boundary stays where
        # it was and the row now describes different ticks. Nothing detects
        # that at write time and nothing needs to: dispatch recomputes the
        # tick from the row inside the transaction that would record it, so
        # the tick a worker planned no longer matches and is not written.
        # Exercised end to end in tests/test_stored_dispatch.py.
        row = a_cron()
        assert row.boundary_for == boundary_digest(row)
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")
        row.refresh_from_db()
        # The digest is what a worker compares, and it is now stale. Asserting
        # that the update landed would only be a test of queryset.update().
        assert row.boundary_for != boundary_digest(row)


class TestTheChangeRow:
    def test_creating_a_schedule_bumps_it(self):
        a_cron()
        assert OxScheduleChange.objects.get(id=1).changed_at is not None

    def test_updating_a_schedule_bumps_it(self):
        row = a_cron()
        first = OxScheduleChange.objects.get(id=1).changed_at
        update_schedule(row, enabled=False)
        assert OxScheduleChange.objects.get(id=1).changed_at > first

    def test_there_is_only_ever_one_row(self):
        a_cron()
        a_cron(name="another")
        assert OxScheduleChange.objects.count() == 1
