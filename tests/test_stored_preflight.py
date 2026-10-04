"""
The importer's question, asked before it prints a row: would a paste refuse it?

`ox_import_beat_schedules` prints `create_schedules` rows for a person to
paste later, into a project where the registry entries printed beside them
have been applied and the table has been migrated. Neither need be true
while the importer runs. So `_export_preflight` is the validation the paste
will run, less the two answers that depend on those, and it reads nothing.

The tests hold it to both halves: it says what `create_schedule` says, row
for row, and it says it without a database.
"""

import itertools
from datetime import timedelta

import pytest
from django import forms
from django.core.exceptions import NON_FIELD_ERRORS, ValidationError
from django.core.validators import MaxValueValidator
from django.db import connection
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.stored import _export_preflight, create_schedule

from . import tasks


class _Args(ArgsForm):
    region = forms.CharField()


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(ScheduleKind(key="checked", task=tasks.add, form=_Args))


def a_row(name="nightly", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return fields


def an_interval(name="every-minute", **over):
    return a_row(
        name, **{"trigger": "interval", "cron": "", "every_seconds": 60, **over}
    )


def refused_by_create_schedule(fields):
    """What `create_schedule` refuses these fields for, as (field, message)."""
    try:
        create_schedule(**fields)
    except ValidationError as exc:
        return [
            ("" if field == NON_FIELD_ERRORS else field, message)
            for field, messages in exc.message_dict.items()
            for message in messages
        ]
    return []


def over_the_limit(limit, value):
    """The message Django's own range validator gives, whatever its wording."""
    with pytest.raises(ValidationError) as caught:
        MaxValueValidator(limit)(value)
    (message,) = caught.value.messages
    return message


class TestItReadsNothing:
    """
    Unmarked on purpose: without `django_db` pytest-django refuses any use
    of a connection, so these pass only if the preflight never opens one.
    That is the importer's situation exactly, since it may run before
    `migrate django_ox` has.
    """

    def test_a_good_row_passes_with_no_database_to_ask(self):
        assert _export_preflight(a_row()) == []
        assert _export_preflight(an_interval()) == []

    def test_a_bad_row_is_reported_with_no_database_to_ask(self):
        assert [field for field, _ in _export_preflight(a_row(cron="banana"))] == [
            "cron"
        ]

    def test_this_class_would_notice_a_query(self):
        # The control. If the block did nothing, the two tests above would
        # pass however many queries the preflight made.
        with pytest.raises(RuntimeError, match="Database access not allowed"):
            OxSchedule.objects.count()


@pytest.mark.django_db
class TestWhatItLeavesOut:
    def test_a_task_key_nothing_registers_is_not_a_finding(self):
        # The importer prints the registry entries beside the rows, so at
        # the time it asks, the key is one nobody has registered yet.
        row = a_row(task_key="proj.tasks.send_report")
        assert _export_preflight(row) == []
        assert [field for field, _ in refused_by_create_schedule(row)] == ["task_key"]

    def test_the_registry_is_the_only_thing_left_out_of_a_row(self):
        row = a_row(task_key="proj.tasks.send_report", cron="banana", arguments=[1])
        assert [field for field, _ in _export_preflight(row)] == ["cron", "arguments"]

    def test_the_registry_is_back_in_force_however_the_asking_ended(self, monkeypatch):
        # The exemption is for the length of one preflight. Left standing,
        # every later write in the process would accept a key nobody
        # registered. Asked through the model's own clean, the admin's
        # path, because that one sets nothing up for itself: it meets
        # whatever the last caller left behind.
        row = a_row(task_key="proj.tasks.send_report")

        def refused_by_the_model_s_own_clean():
            with pytest.raises(ValidationError) as caught:
                OxSchedule(**row, start_time=timezone.now()).full_clean(
                    exclude=["created_at", "updated_at"]
                )
            return list(caught.value.message_dict)

        def gives_way(expression):
            raise RuntimeError("the cron parser gave way")

        assert _export_preflight(row) == []
        assert refused_by_the_model_s_own_clean() == ["task_key"]
        with monkeypatch.context() as broken:
            broken.setattr(stored, "CronExpression", gives_way)
            with pytest.raises(RuntimeError):
                _export_preflight(row)
        assert refused_by_the_model_s_own_clean() == ["task_key"]

    def test_a_key_that_is_registered_still_has_its_arguments_checked(self):
        # Membership is what cannot be known yet. Where the key is already
        # there, what it says about the arguments is what the paste will say.
        row = a_row(task_key="checked", arguments={"wrong": 1})
        assert _export_preflight(row) == refused_by_create_schedule(row)
        assert [field for field, _ in _export_preflight(row)] == ["arguments"]

    def test_a_name_already_taken_is_not_looked_up(self, django_assert_num_queries):
        create_schedule(**a_row("taken"))
        with django_assert_num_queries(0):
            assert _export_preflight(a_row("taken")) == []
        assert [field for field, _ in refused_by_create_schedule(a_row("taken"))] == [
            "name"
        ]

    def test_no_row_good_or_bad_costs_a_query(self, django_assert_num_queries):
        with django_assert_num_queries(0):
            _export_preflight(a_row())
            _export_preflight(an_interval(phase_seconds=60))
            _export_preflight(a_row(name="", task_key="nope", cron="banana"))


@pytest.mark.django_db
class TestLeavingTheCheckConstraintOutLosesNothing:
    """
    Django validates a check constraint by having the database evaluate it,
    which is a query, so the preflight leaves constraints out. That is only
    safe while `validate_schedule` refuses every row the constraint would.
    """

    def test_the_model_carries_the_one_constraint_that_was_reasoned_about(self):
        # A constraint added later is not covered by the argument above
        # until someone has made it again for that constraint.
        assert [constraint.name for constraint in OxSchedule._meta.constraints] == [
            "ox_schedule_one_trigger"
        ]

    def test_every_row_the_constraint_refuses_the_preflight_refuses(self):
        # The constraint is over three columns, so every way of filling
        # them is put to the database's own reading of it.
        (constraint,) = OxSchedule._meta.constraints
        now = timezone.now()
        refused = 0
        for trigger, cron, every_seconds in itertools.product(
            ["cron", "interval"], ["", "0 2 * * *"], [None, 60]
        ):
            fields = a_row(trigger=trigger, cron=cron, every_seconds=every_seconds)
            row = OxSchedule(**fields, start_time=now, created_at=now, updated_at=now)
            try:
                constraint.validate(OxSchedule, row)
            except ValidationError:
                refused += 1
                assert _export_preflight(fields), (
                    f"the constraint refuses {fields} and the preflight let it through"
                )
        assert refused, "the constraint refused nothing, so nothing was compared"


#: Field sets of every kind. Each is put to the preflight and to
#: `create_schedule`, with the task registered and the name free.
FIELD_SETS = [
    pytest.param({}, id="a cron"),
    pytest.param({"cron": "*/5 9-17 * * mon-fri"}, id="a working-hours cron"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60}, id="an interval"
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 1}, id="one second"
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": "60", "phase_seconds": 5},
        id="an interval from strings",
    ),
    pytest.param({"enabled": False, "starting_deadline_seconds": 30}, id="paused"),
    pytest.param(
        {"arguments": {"region": "emea", "depth": [1, {"a": None}]}}, id="args"
    ),
    pytest.param(
        {"task_key": "checked", "arguments": {"region": "emea"}}, id="checked"
    ),
    pytest.param({"name": "x" * 128}, id="the longest name"),
    pytest.param({"name": ""}, id="blank name"),
    pytest.param({"name": None}, id="null name"),
    pytest.param({"name": "x" * 129}, id="long name"),
    pytest.param({"cron": "banana"}, id="bad cron"),
    pytest.param({"cron": "0 22-2 * * *"}, id="a range that wraps"),
    pytest.param({"cron": "0 9 * * monday"}, id="a day spelled out"),
    pytest.param({"cron": ""}, id="empty cron"),
    pytest.param({"cron": None}, id="null cron"),
    pytest.param(
        {"cron": "0 " + ",".join(str(n % 24) for n in range(60)) + " * * *"},
        id="a cron the column cannot hold",
    ),
    pytest.param({"cron": "0 2 * * *", "every_seconds": 5}, id="cron with interval"),
    pytest.param({"trigger": "interval", "every_seconds": 60}, id="interval with cron"),
    pytest.param({"trigger": "interval", "cron": ""}, id="interval without one"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 0}, id="interval of zero"
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": -5},
        id="negative interval",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60, "phase_seconds": 60},
        id="phase not under the interval",
    ),
    pytest.param({"trigger": "solar"}, id="unknown trigger"),
    pytest.param({"trigger": None}, id="no trigger"),
    pytest.param({"starting_deadline_seconds": 0}, id="deadline of zero"),
    pytest.param({"arguments": [1, 2]}, id="arguments a list"),
    pytest.param({"arguments": None}, id="arguments missing"),
    pytest.param({"arguments": {"when": {1, 2}}}, id="arguments not JSON"),
    pytest.param(
        {"task_key": "checked", "arguments": {"wrong": 1}}, id="arguments refused"
    ),
    pytest.param({"enabled": "maybe"}, id="enabled not a boolean"),
    pytest.param({"start_time": None}, id="no start"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": "soon"},
        id="interval not a number",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60, "phase_seconds": None},
        id="no phase",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60, "phase_seconds": "x"},
        id="phase not a number",
    ),
    pytest.param({"starting_deadline_seconds": "soon"}, id="deadline not a number"),
    pytest.param({"end_time": "soon"}, id="end not a time"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": ""},
        id="interval an empty string",
    ),
    pytest.param({"starting_deadline_seconds": ""}, id="deadline an empty string"),
    pytest.param({"end_time": []}, id="end an empty list"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 10**15},
        id="interval no timedelta holds",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 62_135_596_801},
        id="interval one second past the longest",
    ),
    pytest.param(
        {"starting_deadline_seconds": 86_400_000_000_000},
        id="deadline one second past a timedelta",
    ),
    pytest.param(
        {
            "name": "",
            "trigger": "solar",
            "cron": "x" * 200,
            "every_seconds": -1,
            "arguments": [1],
            "enabled": "maybe",
            "starting_deadline_seconds": 0,
        },
        id="several at once",
    ),
]


@pytest.mark.django_db
class TestItSaysWhatCreateScheduleSays:
    """
    The drift test. The preflight is worth something only while it and the
    paste agree, and they agree because they run one validation, not two
    kept in step by hand. A rule added to `create_schedule` alone shows up
    here as a row the importer prints and the paste refuses.
    """

    @pytest.fixture(autouse=True)
    def _one_instant(self, monkeypatch):
        fixed = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: fixed)
        return fixed

    @pytest.mark.parametrize("over", FIELD_SETS)
    def test_a_problem_is_reported_exactly_when_create_schedule_refuses(self, over):
        fields = a_row(**over)
        said = _export_preflight(fields)
        refused = refused_by_create_schedule(fields)
        assert bool(said) == bool(refused)
        # More than the same verdict: the same fields and the same words,
        # so the importer lists a row for the reason the paste would give.
        assert said == refused

    @pytest.mark.parametrize(
        "end", [timedelta(0), timedelta(hours=-1), timedelta(microseconds=1)]
    )
    def test_an_end_is_held_to_the_start_the_row_will_get(self, _one_instant, end):
        # No start is printed, so the start is the moment of the paste. The
        # preflight measures against its own clock, as the single call does.
        fields = a_row(end_time=_one_instant + end)
        assert _export_preflight(fields) == refused_by_create_schedule(fields)
        assert bool(_export_preflight(fields)) == (end <= timedelta(0))

    def test_a_value_the_default_database_cannot_hold_is_reported_once(self):
        # Django's own validators already hold the default connection's
        # range. Where the destination is that connection, the preflight
        # adds nothing to what they say.
        _, highest = connection.ops.integer_field_range("PositiveIntegerField")
        fields = an_interval(every_seconds=highest + 1)
        assert _export_preflight(fields) == [
            ("every_seconds", over_the_limit(highest, highest + 1))
        ]
        assert _export_preflight(fields) == refused_by_create_schedule(fields)

    def test_a_keyword_that_is_not_a_field_is_raised_not_listed(self):
        # The importer's own mistake rather than the row's, so not something
        # to list beside the rows that were refused. The paste would raise
        # the same.
        fields = a_row(colour="red")
        with pytest.raises(TypeError) as asked:
            _export_preflight(fields)
        assert "colour" in str(asked.value)
        with pytest.raises(TypeError):
            create_schedule(**fields)

    def test_what_the_validation_itself_gives_way_on_is_raised_not_listed(
        self, monkeypatch
    ):
        # Only a refusal is a finding about the row. Anything else is not
        # swallowed into the list, where the importer would print it as the
        # reason a row was left out.
        def gives_way(expression):
            raise RuntimeError("the cron parser gave way")

        monkeypatch.setattr(stored, "CronExpression", gives_way)
        with pytest.raises(RuntimeError):
            _export_preflight(a_row())

    def test_a_failure_of_no_one_field_comes_back_under_an_empty_field(
        self, monkeypatch
    ):
        def refuse_the_row(self):
            raise ValidationError("These two do not go together.")

        monkeypatch.setattr(OxSchedule, "clean", refuse_the_row)
        assert _export_preflight(a_row()) == [("", "These two do not go together.")]

    def test_the_fields_given_are_left_as_they_were(self):
        fields = a_row()
        _export_preflight(fields)
        assert fields == a_row()
