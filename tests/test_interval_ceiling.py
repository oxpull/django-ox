"""
The longest interval is the longest the tick arithmetic can count back.

Interval ticks are counted from 1970. Until the clock reaches 1970 plus a
schedule's phase, the schedule's latest tick is one interval before that
instant, and `IntervalTrigger.previous()` reaches it by stepping a whole
interval back from 1970. A datetime ends at year 1, which is 62,135,596,800
seconds before 1970, so an interval one second longer raised OverflowError
there.

The dispatch pass derives every schedule's tick before it comes to any one
schedule's handling, and the run loop expects a database error from it and
nothing else. So the error ended the pass and the worker: every worker
reading that schedule, and again after each restart, with the tasks queued
beside it left where they were.

An interval is held to the limit in three places. `validate_schedule`
refuses it on every path that writes a stored schedule. The stored source
skips and logs a row that is already in the table, however it got there.
And `schedules_from_options` refuses a settings-declared interval, which
`manage.py check` reports and constructing a worker raises.

PostgreSQL's and MySQL's integer columns end below the limit, at about 68
and 136 years, so no stored row there can pass it and the column's own
range answers first. The tests that need such a row stored skip there and
say why; the others say which of the two answered. A settings-declared
interval has no column, and those tests run on every engine.
"""

import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta

import pytest
from django.conf import settings as project
from django.contrib.auth.models import User
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.core.validators import MaxValueValidator
from django.db import connection
from django.urls import reverse
from django.utils import timezone

from django_ox import schedules, stored
from django_ox.compat import default_task_backend
from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.schedules import (
    _INTERVAL_EPOCH,
    MAX_INTERVAL,
    IntervalTrigger,
    schedules_from_options,
)
from django_ox.stored import (
    DatabaseScheduleSource,
    _export_preflight,
    boundary_digest,
    create_schedule,
    create_schedules,
    update_schedule,
    validate_schedule,
)
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for

#: The seconds from year 1 to 1970. Written out, because the number is the
#: contract; `TestTheLimitIsWhatTheArithmeticHolds` holds the code to it.
CEILING = 62_135_596_800

#: What a timedelta holds, the limit a phase and a deadline keep.
LONGEST = 86_399_999_999_999

ADD = "admin:django_ox_oxschedule_add"
CHANGE = "admin:django_ox_oxschedule_change"
CHANGELIST = "admin:django_ox_oxschedule_changelist"

STORED = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
    }
}


def declared(schedules, **options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"SCHEDULES": schedules, **options},
        }
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.record))


@pytest.fixture
def operator(client):
    client.force_login(User.objects.create_superuser("root", "root@example.com", "pw"))
    return client


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def since_the_epoch(clock):
    return (clock - _INTERVAL_EPOCH) // timedelta(seconds=1)


def ahead_of_the_clock():
    """
    A phase the worker's clock has not reached, by two days.

    The shape that raised: the latest tick is then the one an interval
    before 1970 plus the phase. Read off the clock the dispatch pass reads,
    the project's wall clock, so it stays ahead in any zone and any year.
    """
    now = timezone.now()
    wall_clock = timezone.localtime(now).replace(tzinfo=None) if project.USE_TZ else now
    return since_the_epoch(wall_clock) + 2 * 86_400


def an_interval(name="every-so-often", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "interval",
        "every_seconds": 60,
        "arguments": {"label": name},
    }
    fields.update(over)
    return fields


def written_directly(name, every, phase):
    """
    A row put in the table around the write functions.

    What `create_schedule` stored before it refused such a row, and what
    `queryset.update()`, a fixture or a data migration can still store. The
    boundary is set as the write functions set it, so the row is not one
    the source has to heal first.
    """
    now = timezone.now()
    row = OxSchedule(
        **an_interval(name, every_seconds=every, phase_seconds=phase),
        start_time=now - timedelta(minutes=5),
        created_at=now,
        updated_at=now,
    )
    row.boundary_for = boundary_digest(row)
    row.save()
    return row


def the_column_holds():
    """The most the default database's integer column takes."""
    _, highest = connection.ops.integer_field_range("PositiveIntegerField")
    return highest


def needs_a_column_that_holds_it():
    """Skip, visibly, where no row can carry an interval past the limit."""
    if the_column_holds() <= CEILING:
        pytest.skip(
            f"{connection.vendor}'s integer column holds {the_column_holds()} at "
            f"most, under the {CEILING} seconds an interval may be, so no row "
            "past the limit can be stored to be read back; SQLite is the engine "
            "this row exists on"
        )


def what_is_said_of(value):
    """
    The one refusal of an interval past the limit, on this engine.

    Where the column takes the number, the rule refuses it. Where the column
    is the narrower, Django's own range validator has refused it in the
    field's cleaning, and the rule does not say it again.
    """
    assert value > CEILING
    highest = the_column_holds()
    if value <= highest:
        return stored._TOO_LONG_FOR_A_SCHEDULE % {"limit": CEILING}
    with pytest.raises(ValidationError) as caught:
        MaxValueValidator(highest)(value)
    (message,) = caught.value.messages
    return message


def too_long_for_settings(name="too-long"):
    """What a SCHEDULES entry with an interval past the limit is told."""
    return schedules._EVERY_TOO_LONG.format(prefix=f"Schedule {name!r}", limit=CEILING)


def not_a_timedelta(key, name="too-long"):
    """What one is told of a number of seconds no timedelta holds."""
    return schedules._NOT_A_TIMEDELTA.format(prefix=f"Schedule {name!r}", key=key)


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


#: Clock readings a worker can hold, from the first the arithmetic counts
#: from to the last a datetime says.
CLOCKS = [
    pytest.param(_INTERVAL_EPOCH, id="the epoch"),
    pytest.param(_INTERVAL_EPOCH + timedelta(seconds=1), id="a second after it"),
    pytest.param(datetime(2000, 1, 1), id="2000"),
    pytest.param(datetime(2026, 10, 4, 12, 0), id="when this was written"),
    pytest.param(datetime(2026, 10, 4, 12, 0, fold=1), id="in a repeated hour"),
    pytest.param(datetime(2038, 1, 19, 3, 14, 8), id="2038"),
    pytest.param(datetime(2100, 1, 1), id="2100"),
    pytest.param(datetime.max, id="the last instant a datetime holds"),
]


def phases_for(every, clock):
    """The ends of the range, and each side of every place the answer turns."""
    reached = since_the_epoch(clock)
    wanted = {
        0,
        1,
        2,
        reached - 1,
        reached,
        reached + 1,
        2 * 10**9 - 1,
        2 * 10**9,
        2 * 10**9 + 1,
        every // 2,
        every - 2,
        every - 1,
    }
    return sorted(phase for phase in wanted if 0 <= phase < every)


class TestTheLimitIsWhatTheArithmeticHolds:
    def test_it_is_the_time_from_year_one_to_the_epoch(self):
        longest = MAX_INTERVAL
        assert longest == timedelta(days=719_162)
        assert longest // timedelta(seconds=1) == CEILING
        # One interval back from the epoch is the first instant a datetime
        # holds, and a second more is none.
        assert _INTERVAL_EPOCH - longest == datetime.min
        with pytest.raises(OverflowError):
            _INTERVAL_EPOCH - longest - timedelta(seconds=1)
        assert stored._LONGEST_INTERVAL_SECONDS == CEILING
        assert stored._LONGEST_INTERVAL_SECONDS < stored._LONGEST_SECONDS == LONGEST

    @pytest.mark.parametrize("clock", CLOCKS)
    @pytest.mark.parametrize(
        "every",
        [
            pytest.param(CEILING - 1, id="a second under"),
            pytest.param(CEILING, id="the limit"),
        ],
    )
    def test_every_phase_has_a_tick_at_every_clock_reading(self, every, clock):
        interval = timedelta(seconds=every)
        stepped_back = []
        for phase in phases_for(every, clock):
            offset = timedelta(seconds=phase)
            tick = IntervalTrigger(every=interval, phase=offset).previous(clock)
            # The latest tick at or before the clock, and on the sequence.
            assert clock - interval < tick <= clock, phase
            assert (tick - _INTERVAL_EPOCH - offset) % interval == timedelta(0), phase
            assert tick.fold == clock.fold
            assert not stored._ticks_overflow(every, phase), phase
            if tick < _INTERVAL_EPOCH:
                stepped_back.append(phase)
        # The cells that take the step back from 1970 are the ones the
        # limit is about, and the grid has them wherever the clock leaves
        # room for a phase ahead of it.
        ahead = [p for p in phases_for(every, clock) if p > since_the_epoch(clock)]
        assert stepped_back == ahead
        assert ahead or since_the_epoch(clock) + 1 >= every

    @pytest.mark.parametrize("clock", CLOCKS[:-1])
    def test_one_second_more_cannot_be_counted_back(self, clock):
        # What the limit keeps out. If this stops raising, the arithmetic
        # has changed and the limit is no longer what it holds.
        interval = timedelta(seconds=CEILING + 1)
        reached = since_the_epoch(clock)
        ahead = IntervalTrigger(every=interval, phase=timedelta(seconds=reached + 1))
        with pytest.raises(OverflowError):
            ahead.previous(clock)
        # Only while the phase is ahead of the clock. Once the clock has
        # reached it, the step is not taken, which is how such a schedule
        # could be created and run for a while before a later one did not.
        behind = IntervalTrigger(every=interval, phase=timedelta(seconds=reached))
        assert behind.previous(clock) == clock.replace(microsecond=0)


#: Interval and phase, in seconds. The valid shapes each side of the limit,
#: and the ones only a row written around the rules can have: a phase of
#: one interval or of many.
SHAPES = [
    (60, 0),
    (60, 59),
    (60, 3600),
    (60, 3 * 10**9),
    (60, CEILING),
    (60, CEILING + 1),
    (60, 7 * 10**10),
    (7, CEILING),
    (3600, CEILING + 2 * 10**9),
    (CEILING - 1, CEILING - 2),
    (CEILING - 1, CEILING),
    (CEILING, 1),
    (CEILING, CEILING - 1),
    (CEILING, CEILING),
    (CEILING + 1, 1),
    (CEILING + 1, CEILING),
    (2 * CEILING, 5),
    (LONGEST, LONGEST - 1),
]


class TestWhatTheSourceRefusesIsWhatWouldRaise:
    """
    `_ticks_overflow` against the arithmetic it stands in for.

    The furthest back a tick is counted is at the epoch, so that is where
    the trigger is asked. A shape the rule lets through has to answer
    there and at every later reading; one it refuses, with any phase at
    all, has to be one that raises there.
    """

    @pytest.mark.parametrize(("every", "phase"), SHAPES)
    def test_the_rule_and_the_arithmetic_agree(self, every, phase):
        trigger = IntervalTrigger(
            every=timedelta(seconds=every), phase=timedelta(seconds=phase)
        )
        if stored._ticks_overflow(every, phase):
            with pytest.raises(OverflowError):
                trigger.previous(_INTERVAL_EPOCH)
            return
        for clock in (param.values[0] for param in CLOCKS):
            assert trigger.previous(clock) <= clock

    @pytest.mark.parametrize("every", [CEILING + 1, 2 * CEILING, LONGEST])
    def test_an_interval_past_the_limit_is_refused_whatever_its_phase(self, every):
        # With no phase the step back is never taken, and the arithmetic
        # holds. The limit is on the interval all the same: one rule, the
        # one validation gives, and not one that turns on the phase.
        trigger = IntervalTrigger(every=timedelta(seconds=every))
        assert trigger.previous(_INTERVAL_EPOCH) == _INTERVAL_EPOCH
        assert stored._ticks_overflow(every, 0)


@pytest.mark.django_db
class TestEveryPathThatWritesRefusesIt:
    """
    An interval one second past the limit, through each way of writing one.

    One refusal, on the field, and nothing stored. Which rule gives it
    depends on the engine and `what_is_said_of` says which.
    """

    OVER = CEILING + 1

    def test_the_message_names_the_interval_s_own_limit(self):
        said = stored._TOO_LONG_FOR_A_SCHEDULE % {
            "limit": stored._LONGEST_INTERVAL_SECONDS
        }
        assert said == "Enter a value of 62135596800 seconds or less."

    def test_creating_a_schedule(self):
        with pytest.raises(ValidationError) as caught:
            create_schedule(**an_interval(every_seconds=self.OVER))
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(self.OVER)]
        }
        assert [e.code for e in caught.value.error_dict["every_seconds"]] == [
            "max_value"
        ]
        assert not OxSchedule.objects.exists()

    def test_creating_a_batch(self):
        rows = [an_interval("fine"), an_interval("too-long", every_seconds=self.OVER)]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        (entry,) = caught.value.error_list
        assert entry.params == {
            "index": 1,
            "name": "too-long",
            "field": "every_seconds",
            "message": what_is_said_of(self.OVER),
        }
        assert entry.code == "max_value"
        assert not OxSchedule.objects.exists()

    def test_the_importer_s_preflight(self):
        assert _export_preflight(an_interval(every_seconds=self.OVER)) == [
            ("every_seconds", what_is_said_of(self.OVER))
        ]

    def test_updating_a_schedule(self):
        row = create_schedule(**an_interval())
        with pytest.raises(ValidationError) as caught:
            update_schedule(row, every_seconds=self.OVER)
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(self.OVER)]
        }
        row.refresh_from_db()
        assert (row.every_seconds, row.boundary_generation) == (60, 0)

    def test_the_admin_s_add_form(self, operator):
        response = operator.post(reverse(ADD), the_form_posts(every_seconds=self.OVER))
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [what_is_said_of(self.OVER)]}
        assert not OxSchedule.objects.exists()

    def test_the_admin_s_change_form(self, operator):
        row = create_schedule(**an_interval())
        response = operator.post(
            reverse(CHANGE, args=[row.pk]), the_form_posts(every_seconds=self.OVER)
        )
        assert response.status_code == 200
        assert shown(response) == {"every_seconds": [what_is_said_of(self.OVER)]}
        row.refresh_from_db()
        assert row.every_seconds == 60

    def test_the_model_s_own_clean(self):
        row = OxSchedule(
            **an_interval(every_seconds=self.OVER), start_time=timezone.now()
        )
        with pytest.raises(ValidationError) as caught:
            row.full_clean(exclude=["created_at", "updated_at"])
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(self.OVER)]
        }

    def test_each_field_is_refused_against_its_own_limit(self):
        # The deadline keeps what a timedelta holds, so a number between
        # the two limits is too long for an interval and not for a deadline.
        if the_column_holds() <= LONGEST:
            pytest.skip(
                f"{connection.vendor}'s integer column holds {the_column_holds()} "
                "at most, which refuses all of these for its own range; the "
                "column's refusal is in tests/test_stored_limits.py"
            )
        between = an_interval(every_seconds=LONGEST, starting_deadline_seconds=LONGEST)
        with pytest.raises(ValidationError) as caught:
            create_schedule(**between)
        for_an_interval = stored._TOO_LONG_FOR_A_SCHEDULE % {"limit": CEILING}
        for_a_deadline = stored._TOO_LONG_FOR_A_SCHEDULE % {"limit": LONGEST}
        assert for_an_interval != for_a_deadline
        assert caught.value.message_dict == {"every_seconds": [for_an_interval]}
        both = an_interval(
            every_seconds=LONGEST + 1, starting_deadline_seconds=LONGEST + 1
        )
        with pytest.raises(ValidationError) as caught:
            create_schedule(**both)
        assert caught.value.message_dict == {
            "every_seconds": [for_an_interval],
            "starting_deadline_seconds": [for_a_deadline],
        }

    def test_a_phase_is_refused_for_passing_its_interval_and_nothing_else(self):
        # A phase is under its interval, so under this limit too, and has
        # no second refusal to be given: between the limits it is only
        # ever "not less than the interval".
        if the_column_holds() <= CEILING:
            pytest.skip(
                f"{connection.vendor}'s integer column holds {the_column_holds()} "
                "at most, which refuses a phase this size for its own range"
            )
        with pytest.raises(ValidationError) as caught:
            create_schedule(
                **an_interval(every_seconds=CEILING, phase_seconds=CEILING + 5)
            )
        assert caught.value.message_dict == {
            "phase_seconds": ["The phase must be less than the interval."]
        }


@pytest.mark.django_db(transaction=True)
class TestTheLongestIntervalIsOneAWorkerRuns:
    def test_at_the_limit_with_its_phase_ahead_of_the_clock(self, settings, caplog):
        """
        The shape that ended the worker, one second shorter.

        The row is accepted, built and dispatched: its latest tick is an
        interval before 1970 plus the phase, which today is in the first
        century and is after the boundary given here, so it fires once and
        is recorded.
        """
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        row = create_schedule(
            **an_interval(
                "longest",
                every_seconds=CEILING,
                phase_seconds=ahead_of_the_clock(),
                start_time=timezone.make_aware(datetime(1, 1, 3))
                if project.USE_TZ
                else datetime(1, 1, 3),
            )
        )
        assert OxSchedule.objects.get(pk=row.pk).every_seconds == CEILING
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 1
            assert worker.dispatch_schedules() == 0
        tick = OxScheduleTick.objects.get(schedule_name=f"db:{row.pk}")
        # Before the epoch: the tick the step back was taken for.
        assert tick.scheduled_for.year < 1970
        assert tick.task is not None
        assert not events(caplog, "schedule_row_skipped")
        assert not events(caplog, "schedule_dispatch_error")


@pytest.mark.django_db(transaction=True)
class TestARowAlreadyInTheTable:
    """
    An interval past the limit that is stored already.

    Written by `create_schedule` before it refused one, or around the write
    functions at any time. Validation cannot reach it, so the read does:
    the source will not build it, says so, and goes on to the next row.
    """

    def test_the_worker_skips_it_says_so_and_runs_what_is_queued(
        self, settings, caplog, task_state
    ):
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        too_long = written_directly("too-long", CEILING + 1, ahead_of_the_clock())
        beside = create_schedule(
            **an_interval("beside-it", start_time=timezone.now() - timedelta(minutes=5))
        )
        tasks.record.enqueue("queued")
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            try:
                # A bound on a wait, not a measurement: the first pass does
                # all of this, and a worker the row ended never does.
                ran = wait_for(
                    lambda: {"queued", "beside-it"} <= set(task_state.get("order", [])),
                    timeout=20,
                )
                alive = thread.is_alive()
            finally:
                worker.request_stop()
                thread.join(timeout=30)
        assert ran, "the queued task and the schedule beside the row both ran"
        assert alive, "and the worker was still running once they had"
        assert not thread.is_alive()
        skipped = events(caplog, "schedule_row_skipped")
        assert skipped
        assert {(r.schedule, r.schedule_pk, r.reason) for r in skipped} == {
            ("too-long", too_long.pk, stored._TICKS_BEFORE_YEAR_ONE)
        }
        assert all(r.levelno == logging.WARNING for r in skipped)
        fired = OxScheduleTick.objects.filter(schedule_name=f"db:{beside.pk}")
        assert fired.exists()
        assert all(tick.task is not None for tick in fired)
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{too_long.pk}"
        ).exists()
        assert "too-long" not in task_state["order"]
        assert (
            OxTask.objects.filter(
                status=OxTask.Status.SUCCESSFUL, task_path="tests.tasks.record"
            ).count()
            >= 2
        )

    @pytest.mark.parametrize(
        ("every", "phase"),
        [
            pytest.param(CEILING + 1, "ahead", id="an interval past the limit"),
            pytest.param(CEILING + 1, 0, id="the same with no phase"),
            pytest.param(60, 7 * 10**10, id="a phase of many intervals"),
            pytest.param(3600, CEILING + 2 * 10**9, id="another, an hour apart"),
        ],
    )
    def test_a_dispatch_pass_goes_on_past_it(self, settings, caplog, every, phase):
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        if phase == "ahead":
            phase = ahead_of_the_clock()
        refused = written_directly("refused", every, phase)
        create_schedule(
            **an_interval("beside-it", start_time=timezone.now() - timedelta(minutes=5))
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            fired = Worker(backoff_initial=0).dispatch_schedules()
        assert [schedule.name for schedule in built] == ["beside-it"]
        assert fired == 1, "the schedule beside it still fires"
        skipped = events(caplog, "schedule_row_skipped")
        assert {(r.schedule, r.schedule_pk, r.reason) for r in skipped} == {
            ("refused", refused.pk, stored._TICKS_BEFORE_YEAR_ONE)
        }

    @pytest.mark.parametrize(
        ("every", "phase"),
        [
            pytest.param(60, 3600, id="an hour, under an interval of a minute"),
            pytest.param(60, 3 * 10**9, id="95 years"),
            pytest.param(60, CEILING, id="the limit itself"),
        ],
    )
    def test_a_phase_past_its_interval_that_can_be_counted_still_runs(
        self, settings, caplog, every, phase
    ):
        # Not a row validation accepts, and not one this change is about:
        # its tick can be derived, the source built it before, and it
        # builds it now.
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        written_directly("as-before", every, phase)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            built = DatabaseScheduleSource({}, "default").schedules()
            fired = Worker(backoff_initial=0).dispatch_schedules()
        assert [schedule.name for schedule in built] == ["as-before"]
        assert fired == 1
        assert not events(caplog, "schedule_row_skipped")

    def test_one_changed_under_a_worker_s_snapshot_is_refused_at_its_lock(
        self, settings, caplog
    ):
        """
        The second place a row is read: under its lock, at dispatch.

        The worker holds a snapshot in which the row is a minutely schedule
        with a tick due. The row is then rewritten around the write
        functions, which moves nothing the worker watches, so the pass
        plans the tick from the snapshot and meets the row as it stands
        only when it locks it.
        """
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        row = create_schedule(
            **an_interval("changed", start_time=timezone.now() - timedelta(minutes=5))
        )
        worker = Worker(backoff_initial=0)
        assert [s.name for s in worker._schedule_source.schedules()] == ["changed"]
        changed = OxSchedule.objects.get(pk=row.pk)
        changed.every_seconds = CEILING + 1
        changed.phase_seconds = ahead_of_the_clock()
        OxSchedule.objects.filter(pk=row.pk).update(
            every_seconds=changed.every_seconds,
            phase_seconds=changed.phase_seconds,
            boundary_for=boundary_digest(changed),
        )
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        (skipped,) = events(caplog, "schedule_row_skipped")
        assert skipped.schedule_pk == row.pk
        assert str(skipped.exc_info[1]) == stored._TICKS_BEFORE_YEAR_ONE
        assert not events(caplog, "schedule_dispatch_error")
        assert not OxScheduleTick.objects.exists()
        assert not OxTask.objects.exists()

    def test_the_rules_refuse_it_as_it_stands(self):
        needs_a_column_that_holds_it()
        row = written_directly("too-long", CEILING + 1, 0)
        with pytest.raises(ValidationError) as caught:
            validate_schedule(OxSchedule.objects.get(pk=row.pk))
        assert caught.value.message_dict == {
            "every_seconds": [what_is_said_of(CEILING + 1)]
        }

    def test_a_manual_run_of_it_is_skipped_and_said(self, settings, operator):
        # "Run selected schedules once now" builds the row the way a worker
        # does, so it will not build this one either. It never asked the
        # trigger for a tick, so before the limit it enqueued the task.
        needs_a_column_that_holds_it()
        settings.TASKS = STORED
        row = written_directly("too-long", CEILING + 1, ahead_of_the_clock())
        response = operator.post(
            reverse(CHANGELIST),
            {"action": "run_once_now", "_selected_action": [str(row.pk)]},
            follow=True,
        )
        said = [str(message) for message in response.context["messages"]]
        assert "Skipped 1 schedule(s) that cannot run as written." in said
        assert "Enqueued 0 task(s)." in said
        assert not OxTask.objects.exists()


#: An `every` past the limit as a settings file can give it, with a phase
#: and without. A phase ahead of the clock is the one that raised at the
#: first dispatch pass; the entry is refused whether or not it has one.
PAST_THE_LIMIT = [
    pytest.param({"every": CEILING + 1}, id="a number of seconds"),
    pytest.param({"every": CEILING + 1, "phase": 2 * 10**9}, id="with a phase"),
    pytest.param(
        {
            "every": timedelta(seconds=CEILING + 1),
            "phase": timedelta(seconds=2 * 10**9),
        },
        id="timedeltas",
    ),
    pytest.param({"every": float(CEILING) + 0.5}, id="a float"),
    pytest.param({"every": timedelta(days=365 * 3000)}, id="3000 years"),
    pytest.param({"every": timedelta.max}, id="the longest timedelta"),
    pytest.param(
        {"every": timedelta.max, "phase": timedelta.max - timedelta(seconds=1)},
        id="the longest timedelta and a phase beside it",
    ),
]


class TestAnIntervalDeclaredInSettings:
    """
    `OPTIONS["SCHEDULES"]` is not validated by `validate_schedule`, and its
    intervals go through the same arithmetic. Refused where every other bad
    entry is: reported by `manage.py check` as django_ox.E002, and raised
    when a worker is constructed, so a deploy fails at startup and not at
    its first dispatch pass.
    """

    def test_what_the_entry_is_told(self):
        # The wording, here and nowhere else in this file: the schedule's
        # name, the key, and for the interval the limit in seconds.
        assert too_long_for_settings() == (
            "Schedule 'too-long': 'every' exceeds 62135596800 seconds, the "
            "maximum supported interval."
        )
        assert not_a_timedelta("phase") == (
            "Schedule 'too-long': 'phase' is outside the range of seconds a "
            "timedelta can hold."
        )

    @pytest.mark.parametrize("timing", PAST_THE_LIMIT)
    def test_one_past_the_limit_is_refused(self, timing):
        options = {"SCHEDULES": {"too-long": {"task": "tests.tasks.add", **timing}}}
        with pytest.raises(ImproperlyConfigured) as caught:
            schedules_from_options(options, "default")
        assert str(caught.value) == too_long_for_settings()

    @pytest.mark.parametrize("timing", PAST_THE_LIMIT)
    def test_the_check_reports_it(self, settings, timing):
        settings.TASKS = declared({"too-long": {"task": "tests.tasks.add", **timing}})
        (error,) = default_task_backend.check()
        assert error.id == "django_ox.E002"
        assert error.msg == too_long_for_settings()

    @pytest.mark.parametrize("timing", PAST_THE_LIMIT)
    @pytest.mark.parametrize(
        "source",
        [
            pytest.param({}, id="the settings source"),
            pytest.param(STORED["default"]["OPTIONS"], id="the stored source"),
        ],
    )
    def test_a_worker_is_not_constructed_with_it(self, settings, timing, source):
        settings.TASKS = declared(
            {"too-long": {"task": "tests.tasks.add", **timing}}, **source
        )
        refusal = re.escape(too_long_for_settings())
        with pytest.raises(ImproperlyConfigured, match=refusal):
            Worker()

    @pytest.mark.parametrize(
        ("options", "past"),
        [
            pytest.param(
                '{"SCHEDULES": {"too-long": {"task": "tests.tasks.add",'
                ' "every": 62135596801, "phase": 2000000000}}}',
                "the limit",
                id="past the limit",
            ),
            pytest.param(
                '{"SCHEDULES": {"too-long": {"task": "tests.tasks.add",'
                ' "every": 1000000000000000}}}',
                "a timedelta",
                id="past a timedelta",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "command",
        [
            pytest.param(["check"], id="check"),
            pytest.param(["ox_worker", "--batch"], id="ox_worker"),
        ],
    )
    def test_the_commands_themselves_refuse_it(self, options, past, command):
        """
        Through `manage.py`, in a project with nothing else in it. The
        check reports the entry, and the worker command does not start a
        worker: it runs the checks before it builds one.
        """
        env = dict(os.environ)
        env["DJANGO_SETTINGS_MODULE"] = "tests.settings_plain"
        env["OX_TEST_TASKS_OPTIONS"] = options
        # The arguments that are not literals are this test's own.
        completed = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "django", *command],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert completed.returncode == 1, completed.stdout + completed.stderr
        said = (
            too_long_for_settings() if past == "the limit" else not_a_timedelta("every")
        )
        assert f"(django_ox.E002) {said}" in completed.stderr, completed.stderr
        assert "Traceback" not in completed.stderr

    @pytest.mark.parametrize("key", ["every", "phase"])
    @pytest.mark.parametrize("seconds", [10**15, -(10**15), 1e300])
    def test_a_number_no_timedelta_holds_is_refused_and_not_raised(
        self, settings, key, seconds
    ):
        # timedelta(seconds=10**15) raises OverflowError, which came out of
        # the check as a traceback where every other bad entry is reported.
        timing = {"every": 60, key: seconds}
        settings.TASKS = declared({"too-long": {"task": "tests.tasks.add", **timing}})
        (error,) = default_task_backend.check()
        assert error.id == "django_ox.E002"
        assert error.msg == not_a_timedelta(key)
        with pytest.raises(ImproperlyConfigured, match=re.escape(not_a_timedelta(key))):
            Worker()

    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize(
        "every",
        [
            pytest.param(CEILING, id="a number of seconds"),
            pytest.param(float(CEILING), id="a float"),
            pytest.param(MAX_INTERVAL, id="a timedelta"),
        ],
    )
    def test_the_limit_itself_is_one_a_worker_runs(self, settings, caplog, every):
        """
        With its phase ahead of the clock, the shape that raised.

        A settings-declared schedule anchors at its current tick the first
        time a worker sees it and fires nothing, so the pass returns 0. What
        is asserted on every engine is that the pass returned at all and
        that the minutely schedule beside it was anchored in it.

        The anchor of the long one is a tick before 1970, today one in the
        first century. That it is recorded is asserted on SQLite only.
        On PostgreSQL and MySQL, a refusal of a datetime that early would be
        that schedule's own dispatch error, not the pass's.
        """
        phase = ahead_of_the_clock()
        settings.TASKS = declared(
            {
                "longest": {
                    "task": "tests.tasks.add",
                    "every": every,
                    "phase": timedelta(seconds=phase)
                    if isinstance(every, timedelta)
                    else phase,
                },
                "every-minute": {"task": "tests.tasks.add", "every": 60},
            }
        )
        assert default_task_backend.check() == []
        worker = Worker(backoff_initial=0)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        anchored = {
            tick.schedule_name: tick.scheduled_for
            for tick in OxScheduleTick.objects.all()
        }
        assert "every-minute" in anchored
        if connection.vendor != "sqlite":
            return
        assert not events(caplog, "schedule_dispatch_error")
        assert set(anchored) == {"longest", "every-minute"}
        assert anchored["longest"].year < 1970
