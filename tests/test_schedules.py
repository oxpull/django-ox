import sys
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

from django_ox.compat import default_task_backend
from django_ox.models import OxScheduleTick, OxTask
from django_ox.schedules import (
    IntervalTrigger,
    SettingsScheduleSource,
    schedule_source_from_options,
    schedules_from_options,
)
from django_ox.worker import Worker

from .conftest import start_worker_thread, wait_for


def tasks_setting(schedules):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, "SCHEDULES": schedules},
        }
    }


MINUTELY_ADD = {
    "minutely-add": {"task": "tests.tasks.add", "cron": "* * * * *", "args": [1, 2]},
}


def two_backend_setting(other_schedules):
    """The default backend running MINUTELY_ADD plus a second backend."""
    return {
        **tasks_setting(MINUTELY_ADD),
        "other": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": [],
            "OPTIONS": {"SCHEDULES": other_schedules},
        },
    }


@pytest.fixture
def frozen_now(monkeypatch):
    """
    Pin timezone.now() mid-minute so a minute boundary cannot roll over
    between two dispatch calls within one test.
    """
    fixed = timezone.now().replace(second=30, microsecond=0)
    monkeypatch.setattr(timezone, "now", lambda: fixed)
    return fixed


@pytest.fixture
def scheduled_worker(settings, frozen_now):
    settings.TASKS = tasks_setting(MINUTELY_ADD)
    return Worker(backoff_initial=0, poll_interval=0.05)


def backdate_anchor(name, minutes):
    """Move a schedule's latest tick into the past to make a tick due."""
    updated = OxScheduleTick.objects.filter(schedule_name=name).update(
        scheduled_for=(
            OxScheduleTick.objects.get(schedule_name=name).scheduled_for
            - timedelta(minutes=minutes)
        )
    )
    assert updated == 1


class TestSchedulesFromOptions:
    def test_empty_options_yield_no_schedules(self):
        assert schedules_from_options({}, "default") == []

    def test_builds_schedule_with_defaults(self):
        (schedule,) = schedules_from_options({"SCHEDULES": MINUTELY_ADD}, "default")
        assert schedule.name == "minutely-add"
        assert schedule.task.module_path == "tests.tasks.add"
        assert schedule.args == (1, 2)
        assert schedule.kwargs == {}
        assert str(schedule.trigger) == "* * * * *"

    def test_queue_and_priority_overrides(self):
        (schedule,) = schedules_from_options(
            {
                "SCHEDULES": {
                    "emails-digest": {
                        "task": "tests.tasks.send_email",
                        "cron": "0 8 * * *",
                        "queue_name": "emails",
                        "priority": 10,
                    }
                }
            },
            "default",
        )
        assert schedule.task.queue_name == "emails"
        assert schedule.task.priority == 10

    @pytest.mark.parametrize(
        "config,match",
        [
            ({"cron": "* * * * *"}, "missing 'task'"),
            ({"task": "tests.tasks.add"}, "exactly one of 'cron' or 'every'"),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "every": 60},
                "exactly one of 'cron' or 'every'",
            ),
            ({"task": "tests.tasks.add", "every": 0.5}, "below the"),
            ({"task": "tests.tasks.add", "every": "5m"}, "timedelta or a number"),
            ({"task": "tests.tasks.add", "every": True}, "timedelta or a number"),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "phase": 5},
                "only applies to 'every'",
            ),
            (
                {"task": "tests.tasks.add", "every": 60, "phase": 60},
                "less than 'every'",
            ),
            ({"task": "tests.tasks.add", "every": 60, "phase": -1}, "at least zero"),
            ({"task": "tests.tasks.add", "cron": "not cron"}, "5 fields"),
            ({"task": "tests.tasks.add", "cron": "0 0 30 2 *"}, "never match"),
            ({"task": "tests.tasks.missing", "cron": "* * * * *"}, "cannot import"),
            (
                {"task": "tests.tasks.STATE", "cron": "* * * * *"},
                "not a django.tasks Task",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "banana": 5},
                "unknown key",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "args": "1"},
                "'args' must be a list",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "kwargs": [1]},
                "'kwargs' must be a dict",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "args": [object()]},
                "JSON-serializable",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "queue_name": "nope"},
                "nope",
            ),
        ],
    )
    def test_rejects_invalid_schedule(self, config, match):
        options = {"SCHEDULES": {"bad": config}}
        with pytest.raises(ImproperlyConfigured, match=match):
            schedules_from_options(options, "default")

    def test_rejects_invalid_names_and_shapes(self):
        with pytest.raises(ImproperlyConfigured, match="must be a mapping"):
            schedules_from_options({"SCHEDULES": ["oops"]}, "default")
        with pytest.raises(ImproperlyConfigured, match="non-empty strings"):
            schedules_from_options(
                {"SCHEDULES": {"": {"task": "tests.tasks.add", "cron": "* * * * *"}}},
                "default",
            )
        with pytest.raises(ImproperlyConfigured, match="exceeds 128"):
            schedules_from_options(
                {
                    "SCHEDULES": {
                        "x" * 129: {"task": "tests.tasks.add", "cron": "* * * * *"}
                    }
                },
                "default",
            )

    def test_rebinds_task_to_the_configuring_backend(self, settings):
        settings.TASKS = {
            **tasks_setting({}),
            "other": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": [],
                "OPTIONS": {},
            },
        }
        (schedule,) = schedules_from_options({"SCHEDULES": MINUTELY_ADD}, "other")
        assert schedule.task.backend == "other"

    def test_check_reports_invalid_schedules(self, settings):
        settings.TASKS = tasks_setting(
            {"bad": {"task": "tests.tasks.add", "cron": "banana"}}
        )
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E002"]

    def test_worker_init_rejects_invalid_schedules(self, settings):
        settings.TASKS = tasks_setting({"bad": {"task": "tests.tasks.add"}})
        with pytest.raises(ImproperlyConfigured, match="exactly one of"):
            Worker()

    def test_check_reports_cross_backend_name_collision(self, settings):
        # The tick log is keyed by schedule name alone, so a name shared
        # across backends would let the backends starve each other.
        settings.TASKS = two_backend_setting(MINUTELY_ADD)
        errors = default_task_backend.check()
        assert [error.id for error in errors] == ["django_ox.E003"]
        assert "'minutely-add'" in errors[0].msg
        assert "'other'" in errors[0].msg

    def test_check_passes_with_unique_names_across_backends(self, settings):
        settings.TASKS = two_backend_setting(
            {"other-add": {"task": "tests.tasks.add", "cron": "* * * * *"}}
        )
        assert default_task_backend.check() == []

    def test_worker_init_rejects_cross_backend_name_collision(self, settings):
        settings.TASKS = two_backend_setting(MINUTELY_ADD)
        with pytest.raises(ImproperlyConfigured, match="unique across backends"):
            Worker()
        with pytest.raises(ImproperlyConfigured, match="unique across backends"):
            Worker(backend_alias="other")


@pytest.mark.django_db
class TestDispatch:
    def test_worker_without_schedules_dispatches_nothing(self, worker):
        assert worker.schedules == []
        assert worker.dispatch_schedules() == 0

    def test_first_sight_anchors_without_firing(self, scheduled_worker, frozen_now):
        assert scheduled_worker.dispatch_schedules() == 0

        anchor = OxScheduleTick.objects.get()
        assert anchor.schedule_name == "minutely-add"
        assert anchor.task is None
        # The anchor is the current tick: the top of the frozen minute.
        assert anchor.scheduled_for == frozen_now.replace(second=0)
        assert OxTask.objects.count() == 0

    def test_dispatch_is_idempotent_within_a_tick(self, scheduled_worker):
        scheduled_worker.dispatch_schedules()
        assert scheduled_worker.dispatch_schedules() == 0
        assert OxScheduleTick.objects.count() == 1
        assert OxTask.objects.count() == 0

    def test_fires_when_a_new_tick_passes(self, scheduled_worker, frozen_now):
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)

        assert scheduled_worker.dispatch_schedules() == 1

        db_task = OxTask.objects.get()
        assert db_task.task_path == "tests.tasks.add"
        assert db_task.args == [1, 2]
        assert db_task.status == OxTask.Status.READY
        tick = OxScheduleTick.objects.get(task__isnull=False)
        assert tick.task_id == db_task.id
        assert tick.scheduled_for == frozen_now.replace(second=0)

    def test_future_dated_tick_does_not_suppress_due_ticks(
        self, scheduled_worker, frozen_now
    ):
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        # A clock-skewed worker recorded a tick a day ahead of wall clock;
        # the tick due now must still fire.
        OxScheduleTick.objects.create(
            schedule_name="minutely-add",
            scheduled_for=frozen_now.replace(second=0) + timedelta(days=1),
            created_at=frozen_now,
        )

        assert scheduled_worker.dispatch_schedules() == 1

        assert OxTask.objects.count() == 1
        fired = OxScheduleTick.objects.get(task__isnull=False)
        assert fired.scheduled_for == frozen_now.replace(second=0)
        # And it fires only once: the due tick's own row now exists.
        assert scheduled_worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 1

    def test_missed_ticks_fire_once_on_recovery(self, scheduled_worker, frozen_now):
        scheduled_worker.dispatch_schedules()
        # Ten ticks elapse with no worker running.
        backdate_anchor("minutely-add", 10)

        assert scheduled_worker.dispatch_schedules() == 1

        # Only the latest missed tick fired; the nine older ones are skipped.
        assert OxTask.objects.count() == 1
        assert OxScheduleTick.objects.count() == 2
        latest = OxScheduleTick.objects.latest("scheduled_for")
        assert latest.scheduled_for == frozen_now.replace(second=0)

    def test_concurrent_dispatch_fires_exactly_once(
        self, scheduled_worker, settings, monkeypatch
    ):
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        other = Worker(backoff_initial=0)

        # Both workers read the tick log before either fires: the classic
        # two-scheduler race. The unique constraint must arbitrate.
        stale = other._latest_ticks(other.schedules, timezone.now() - timedelta(days=1))
        monkeypatch.setattr(other, "_latest_ticks", lambda schedules, since: stale)

        assert scheduled_worker.dispatch_schedules() == 1
        assert other.dispatch_schedules() == 0

        assert OxTask.objects.count() == 1
        assert OxScheduleTick.objects.count() == 2

    def test_sequential_workers_agree_via_the_tick_log(self, scheduled_worker):
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        assert scheduled_worker.dispatch_schedules() == 1

        other = Worker(backoff_initial=0)
        assert other.dispatch_schedules() == 0
        assert OxTask.objects.count() == 1

    def test_dispatched_task_runs_normally(self, scheduled_worker):
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        scheduled_worker.dispatch_schedules()

        assert scheduled_worker.run_once() is True

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3

    def test_kwargs_and_queue_override_reach_the_task(self, settings, frozen_now):
        settings.TASKS = tasks_setting(
            {
                "echo-payload": {
                    "task": "tests.tasks.echo",
                    "cron": "* * * * *",
                    "kwargs": {"value": {"k": "v"}},
                    "queue_name": "emails",
                    "priority": 7,
                }
            }
        )
        worker = Worker(backoff_initial=0)
        worker.dispatch_schedules()
        backdate_anchor("echo-payload", 1)
        worker.dispatch_schedules()

        db_task = OxTask.objects.get()
        assert db_task.kwargs == {"value": {"k": "v"}}
        assert db_task.queue_name == "emails"
        assert db_task.priority == 7

    def test_dispatch_without_use_tz(self, settings):
        # timezone.now() is naive with USE_TZ off; the cron math and the
        # stored tick stay naive with it.
        settings.USE_TZ = False
        settings.TASKS = tasks_setting(MINUTELY_ADD)
        worker = Worker(backoff_initial=0)
        assert worker.dispatch_schedules() == 0
        anchor = OxScheduleTick.objects.get()
        assert anchor.scheduled_for.tzinfo is None


@pytest.mark.django_db(transaction=True)
class TestScheduleRunLoop:
    def test_scheduled_task_executes_through_the_run_loop(self, settings, task_state):
        settings.TASKS = tasks_setting(
            {
                "recorder": {
                    "task": "tests.tasks.record",
                    "cron": "* * * * *",
                    "args": ["tick"],
                }
            }
        )
        worker = Worker(poll_interval=0.05, schedule_interval=0.05, backoff_initial=0)
        thread = start_worker_thread(worker)
        try:
            # First pass anchors the schedule without firing.
            assert wait_for(
                lambda: OxScheduleTick.objects.filter(schedule_name="recorder").exists()
            )
            backdate_anchor("recorder", 1)
            assert wait_for(
                lambda: OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).exists()
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)
        assert not thread.is_alive()

        assert "tick" in task_state.get("order", [])
        # Every fired tick is unique and linked to the task it enqueued.
        fired = OxScheduleTick.objects.filter(task__isnull=False)
        assert fired.count() >= 1
        scheduled_times = list(
            OxScheduleTick.objects.values_list("scheduled_for", flat=True)
        )
        assert len(scheduled_times) == len(set(scheduled_times))


# -- where schedules come from -----------------------------------------


class _RecordingSource:
    """A source that counts how often the worker asks, and can change answer."""

    def __init__(self, options=None, backend_alias=None):
        self.calls = 0
        self.answer: list = []

    def schedules(self):
        self.calls += 1
        return self.answer


class _RaisingSource:
    def __init__(self, options, backend_alias):
        raise ImproperlyConfigured("this source refuses to be built")


class _SourceWithoutSchedules:
    def __init__(self, options, backend_alias):
        pass


def test_the_default_source_reads_settings_schedules():
    source = schedule_source_from_options(
        {"SCHEDULES": {"s": {"task": "tests.tasks.add", "cron": "* * * * *"}}},
        "default",
    )
    assert isinstance(source, SettingsScheduleSource)
    assert [s.name for s in source.schedules()] == ["s"]


def test_the_default_source_answers_the_same_list_every_time():
    # Settings do not change in a running process, so re-reading them would
    # be work on the dispatch path for no possible change.
    source = schedule_source_from_options(
        {"SCHEDULES": {"s": {"task": "tests.tasks.add", "cron": "* * * * *"}}},
        "default",
    )
    assert source.schedules() is source.schedules()


def test_a_named_source_is_used_instead():
    source = schedule_source_from_options(
        {"SCHEDULE_SOURCE": f"{__name__}._RecordingSource"}, "default"
    )
    assert isinstance(source, _RecordingSource)


def test_a_source_that_cannot_be_imported_is_a_configuration_error():
    with pytest.raises(ImproperlyConfigured, match="cannot be imported"):
        schedule_source_from_options(
            {"SCHEDULE_SOURCE": "tests.nope.NoSuchSource"}, "default"
        )


def test_a_source_that_raises_on_construction_is_not_swallowed():
    with pytest.raises(ImproperlyConfigured, match="refuses to be built"):
        schedule_source_from_options(
            {"SCHEDULE_SOURCE": f"{__name__}._RaisingSource"}, "default"
        )


def test_a_source_without_schedules_is_refused():
    with pytest.raises(ImproperlyConfigured, match=r"has no\s+schedules\(\) method"):
        schedule_source_from_options(
            {"SCHEDULE_SOURCE": f"{__name__}._SourceWithoutSchedules"}, "default"
        )


def test_a_non_string_source_is_refused():
    with pytest.raises(ImproperlyConfigured, match="dotted path string"):
        schedule_source_from_options({"SCHEDULE_SOURCE": object()}, "default")


def test_check_reports_a_broken_schedule_source(settings):
    # Through the real backend check, not the resolver: a worker with a
    # source it cannot build dispatches nothing and says nothing.
    tasks = tasks_setting({})
    tasks["default"]["OPTIONS"]["SCHEDULE_SOURCE"] = f"{__name__}._RaisingSource"
    settings.TASKS = tasks
    errors = default_task_backend.check()
    assert [error.id for error in errors] == ["django_ox.E006"]


def test_a_bad_schedule_is_reported_once_not_twice(settings):
    # The default source is built from SCHEDULES, so checking it as well
    # would report the same broken entry under both E002 and E006.
    settings.TASKS = tasks_setting(
        {"bad": {"task": "tests.tasks.add", "cron": "banana"}}
    )
    assert [e.id for e in default_task_backend.check()] == ["django_ox.E002"]


@pytest.mark.django_db(transaction=True)
class TestDispatchAsksTheSource:
    def _worker(self, settings):
        tasks = tasks_setting({})
        tasks["default"]["OPTIONS"]["SCHEDULE_SOURCE"] = f"{__name__}._RecordingSource"
        settings.TASKS = tasks
        return Worker(backoff_initial=0)

    def test_every_dispatch_pass_asks_the_source(self, settings):
        worker = self._worker(settings)
        source = worker._schedule_source
        before = source.calls
        worker.dispatch_schedules()
        worker.dispatch_schedules()
        assert source.calls == before + 2

    def test_a_source_that_starts_empty_is_asked_again(self, settings):
        # Through worker.run(), not dispatch_schedules(), because the thing
        # under test is the run loop's own gate. Reading the schedule list
        # once at start-up and skipping dispatch while it is empty would
        # leave a source that gains a schedule later never asked again. An
        # install whose first schedule is created after the worker started is
        # the ordinary case for a source backed by rows.
        worker = self._worker(settings)
        worker.schedule_interval = 0.02
        source = worker._schedule_source
        start_worker_thread(worker)
        try:
            assert wait_for(lambda: source.calls > 0), "the loop never dispatched"
            source.answer = schedules_from_options(
                {
                    "SCHEDULES": {
                        "late": {"task": "tests.tasks.add", "cron": "* * * * *"}
                    }
                },
                "default",
            )
            assert wait_for(
                lambda: OxScheduleTick.objects.filter(schedule_name="late").exists()
            ), "a schedule that appeared after start-up was never dispatched"
        finally:
            worker.request_stop()

    def test_an_empty_source_costs_no_query(self, django_assert_num_queries, settings):
        worker = self._worker(settings)
        with django_assert_num_queries(0):
            worker.dispatch_schedules()


# -- interval triggers -------------------------------------------------


class TestIntervalTrigger:
    def test_ticks_land_on_the_epoch_grid(self):
        trigger = IntervalTrigger(every=timedelta(hours=1))
        assert trigger.previous(datetime(2026, 9, 9, 14, 37, 12)) == datetime(
            2026, 9, 9, 14, 0
        )

    def test_phase_shifts_the_whole_sequence(self):
        trigger = IntervalTrigger(every=timedelta(hours=1), phase=timedelta(minutes=10))
        assert trigger.previous(datetime(2026, 9, 9, 14, 37)) == datetime(
            2026, 9, 9, 14, 10
        )
        assert trigger.previous(datetime(2026, 9, 9, 14, 5)) == datetime(
            2026, 9, 9, 13, 10
        )

    def test_an_instant_exactly_on_a_tick_is_that_tick(self):
        trigger = IntervalTrigger(every=timedelta(minutes=5))
        on_the_tick = datetime(2026, 9, 9, 14, 5)
        assert trigger.previous(on_the_tick) == on_the_tick

    def test_a_restart_cannot_move_the_cadence(self):
        # The property epoch-anchoring exists for. A last-run-relative
        # interval would restart its cadence from whenever the process came
        # back; this one cannot, because when the process started is not an
        # input to the tick function at all.
        trigger = IntervalTrigger(every=timedelta(minutes=90))
        before = trigger.previous(datetime(2026, 9, 9, 10, 0))
        after_a_restart_at_an_awkward_moment = trigger.previous(
            datetime(2026, 9, 9, 10, 0)
        )
        assert before == after_a_restart_at_an_awkward_moment
        # And the sequence itself is fixed, not merely repeatable.
        assert trigger.previous(datetime(2026, 9, 9, 10, 1)) == datetime(
            2026, 9, 9, 9, 0
        )

    def test_two_workers_derive_the_same_tick(self):
        # No leader, no stored cursor: coordination rests on every worker
        # computing the same instant from the definition alone.
        a = IntervalTrigger(every=timedelta(minutes=7))
        b = IntervalTrigger(every=timedelta(minutes=7))
        moment = datetime(2026, 9, 9, 14, 37, 41)
        assert a.previous(moment) == b.previous(moment)

    def test_a_pause_of_any_length_does_not_shift_later_ticks(self):
        trigger = IntervalTrigger(every=timedelta(hours=6))
        # Ticks either side of a three-day gap sit on the same grid.
        assert trigger.previous(datetime(2026, 9, 9, 5, 0)) == datetime(
            2026, 9, 9, 0, 0
        )
        assert trigger.previous(datetime(2026, 9, 12, 5, 0)) == datetime(
            2026, 9, 12, 0, 0
        )


@pytest.mark.django_db
class TestIntervalDispatch:
    def test_an_interval_schedule_dispatches(self, settings):
        settings.TASKS = tasks_setting(
            {"every-minute": {"task": "tests.tasks.add", "every": 60}}
        )
        worker = Worker(backoff_initial=0)
        worker.dispatch_schedules()  # anchors
        backdate_anchor("every-minute", 1)
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1


@pytest.mark.django_db
class TestTheSettingsPathIsUnchangedByStoredSchedules:
    """
    The stored-schedule work shares the dispatch loop with this path, so the
    properties it has always had are asserted rather than assumed.
    """

    def test_ticks_are_keyed_on_the_schedule_name(self, scheduled_worker):
        scheduled_worker.dispatch_schedules()
        names = set(OxScheduleTick.objects.values_list("schedule_name", flat=True))
        assert names == {"minutely-add"}
        assert not any(n.startswith("db:") for n in names)

    def test_the_first_sighting_anchors_without_firing(self, scheduled_worker):
        assert scheduled_worker.dispatch_schedules() == 0
        anchor = OxScheduleTick.objects.get()
        assert anchor.task_id is None, "a settings schedule anchors on first sight"
        assert OxTask.objects.count() == 0

    def test_dispatch_takes_no_schedule_row_lock(
        self, scheduled_worker, django_assert_max_num_queries
    ):
        # A settings schedule has no row to lock, and adding one for the
        # stored path must not have added a query here. Seven is what the
        # settings path costs on its own: the bounded tick read, the
        # savepoint pair, the tick INSERT, the first-sighting check (the
        # anchor sits before the bound, so it is asked), the task INSERT and
        # the UPDATE that attaches the task to the tick.
        scheduled_worker.dispatch_schedules()
        backdate_anchor("minutely-add", 1)
        with django_assert_max_num_queries(7):
            scheduled_worker.dispatch_schedules()

    def test_a_settings_schedule_has_no_boundary_or_refresh(self):
        schedules = schedules_from_options(
            {"SCHEDULES": {"s": {"task": "tests.tasks.add", "cron": "* * * * *"}}},
            "default",
        )
        only = schedules[0]
        assert only.start_time is None
        assert only.refresh is None
        assert only.anchors is True
        assert only.key == only.name


@pytest.mark.django_db
class TestACustomSourceIsHeldToTheSameTickCheck:
    """
    Dispatch recomputes the tick from what `refresh()` returns and writes
    only if it still matches. The stored source reaches that check through
    a boundary digest or an activation boundary first, so a source written
    outside this package is the only thing that exercises the recomputation
    on its own.
    """

    def _source_class(self, holder):
        class _Source:
            def __init__(self, options, backend_alias):
                pass

            def schedules(self):
                return [holder["schedule"]]

        return _Source

    def test_a_refresh_answering_a_different_instant_writes_no_tick(
        self, settings, monkeypatch
    ):
        from django_ox.cron import CronExpression
        from django_ox.schedules import Schedule

        from . import tasks

        holder = {}
        module = sys.modules[__name__]
        monkeypatch.setattr(module, "_TickSource", self._source_class(holder), False)

        def refresh():
            # The same schedule, retimed. Its latest tick is no longer the
            # instant the snapshot planned, and nothing else about it moved:
            # no boundary, no digest, no end time.
            #
            # The retiming is half a minute off the grid rather than another
            # cron expression, because no cron expression can do this job.
            # Cron ticks are floored to a whole minute, so any cron retiming
            # answers the minutely snapshot's own instant throughout the
            # minute it fires in: "0 3 * * *" agrees with "* * * * *" for the
            # whole of 03:00, and this test then failed on the wall clock one
            # minute a day. An interval phased 30 seconds past the epoch ticks
            # at :30 of a minute and never at :00, so it disagrees with a
            # minutely cron tick at every instant there is.
            return replace(
                holder["schedule"],
                trigger=IntervalTrigger(
                    every=timedelta(minutes=1), phase=timedelta(seconds=30)
                ),
            )

        holder["schedule"] = Schedule(
            name="minutely",
            task=tasks.add,
            trigger=CronExpression("* * * * *"),
            args=(1, 2),
            kwargs={},
            anchors=False,
            refresh=refresh,
        )
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": f"{__name__}._TickSource",
                    "SCHEDULES": {},
                },
            }
        }
        worker = Worker(backoff_initial=0)
        assert worker.dispatch_schedules() == 0, "a retimed schedule fired anyway"
        assert OxScheduleTick.objects.count() == 0, "a tick was claimed and not run"

    def test_a_refresh_answering_the_same_instant_still_fires(
        self, settings, monkeypatch
    ):
        from django_ox.cron import CronExpression
        from django_ox.schedules import Schedule

        from . import tasks

        holder = {}
        module = sys.modules[__name__]
        monkeypatch.setattr(module, "_TickSource", self._source_class(holder), False)
        holder["schedule"] = Schedule(
            name="minutely",
            task=tasks.add,
            trigger=CronExpression("* * * * *"),
            args=(1, 2),
            kwargs={},
            anchors=False,
            refresh=lambda: holder["schedule"],
        )
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULE_SOURCE": f"{__name__}._TickSource",
                    "SCHEDULES": {},
                },
            }
        }
        worker = Worker(backoff_initial=0)
        assert worker.dispatch_schedules() == 1


@pytest.mark.django_db
class TestNamesThatFoldTogether:
    """
    The tick log's unique constraint decides identity with its column's
    collation, not with Python's `==`. MySQL's default folds case, so two
    schedule names differing only by case are one key there: their anchors
    collide, then every tick, and one schedule stops running with nothing
    raised. It is refused on MySQL and named everywhere else, because the
    same settings deployed against MySQL would starve one of the two.
    """

    def _two(self, settings, first, second):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {
                    "SCHEDULES": {
                        first: {"task": "tests.tasks.add", "cron": "* * * * *"},
                        second: {"task": "tests.tasks.add", "cron": "* * * * *"},
                    }
                },
            }
        }
        return [e.id for e in default_task_backend.check()]

    def test_names_differing_only_by_case_are_reported(self, settings):
        from django.db import connection

        ids = self._two(settings, "Report", "report")
        expected = (
            "django_ox.E009" if connection.vendor == "mysql" else "django_ox.W002"
        )
        assert expected in ids, ids

    def test_names_differing_by_more_than_case_are_not(self, settings):
        ids = self._two(settings, "report-daily", "report-hourly")
        assert "django_ox.E009" not in ids
        assert "django_ox.W002" not in ids


@pytest.mark.django_db
class TestASecondAnchorDoesNotSwallowATick:
    """
    A schedule anchors once, however many workers first see it at once.

    Which pass is the first sighting has to be asked of the log inside the
    transaction. Taken from the snapshot read before the loop, a worker
    whose pass began while the schedule still had no history goes on
    believing it is the first sighting after another worker has committed
    the anchor. It writes a second anchor, at a tick that already had a
    boundary and should have fired, and the constraint then suppresses that
    instant for good.
    """

    def _worker(self, settings):
        settings.TASKS = tasks_setting(MINUTELY_ADD)
        return Worker(backoff_initial=0)

    def test_a_worker_arriving_a_tick_later_fires_rather_than_anchors(
        self, settings, monkeypatch
    ):
        first = self._worker(settings)
        second = Worker(backoff_initial=0)
        t0 = timezone.now().replace(second=0, microsecond=0)

        stale = second._latest_ticks(second.schedules, t0 - timedelta(days=1))
        assert stale == {}, "the snapshot has to predate the anchor"

        monkeypatch.setattr(timezone, "now", lambda: t0)
        assert first.dispatch_schedules() == 0, "the first sighting must not fire"
        monkeypatch.undo()
        assert OxScheduleTick.objects.count() == 1
        assert OxTask.objects.count() == 0

        monkeypatch.setattr(second, "_latest_ticks", lambda schedules, since: stale)
        monkeypatch.setattr(timezone, "now", lambda: t0 + timedelta(minutes=1))
        second.dispatch_schedules()
        monkeypatch.undo()

        rows = list(
            OxScheduleTick.objects.order_by("scheduled_for").values_list(
                "scheduled_for", "task_id"
            )
        )
        assert OxTask.objects.count() == 1, (
            f"the tick after the anchor was swallowed as a second anchor: {rows}"
        )

    def test_the_uncontended_sequence_is_unchanged(self, settings, monkeypatch):
        worker = self._worker(settings)
        t0 = timezone.now().replace(second=0, microsecond=0)
        monkeypatch.setattr(timezone, "now", lambda: t0)
        assert worker.dispatch_schedules() == 0
        monkeypatch.undo()
        monkeypatch.setattr(timezone, "now", lambda: t0 + timedelta(minutes=1))
        assert worker.dispatch_schedules() == 1
        monkeypatch.undo()
        assert OxScheduleTick.objects.count() == 2
        assert OxTask.objects.count() == 1


@pytest.mark.django_db
class TestTheTickReadFitsTheParameterLimit:
    """
    The pass's tick read names every schedule in one IN list, one parameter
    per key plus the bound. SQLite before 3.32.0 refuses more than 999
    parameters in a statement, and Django splits an IN list only where the
    backend declares a maximum, which is Oracle. The keys are read in slices
    of what the connection allows, and where it allows everything, in one.
    """

    def _schedules(self, count):
        return schedules_from_options(
            {
                "SCHEDULES": {
                    f"s{i:04d}": {"task": "tests.tasks.add", "cron": "* * * * *"}
                    for i in range(count)
                }
            },
            "default",
        )

    def _seed(self, schedules, at):
        for schedule in schedules[::7]:
            OxScheduleTick.objects.create(
                schedule_name=schedule.key,
                scheduled_for=at,
                task_id=None,
                created_at=at,
            )
        return {schedule.key: at for schedule in schedules[::7]}

    def test_the_keys_are_read_in_slices_of_the_connections_limit(
        self, settings, monkeypatch
    ):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        settings.TASKS = tasks_setting({})
        schedules = self._schedules(10)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = self._seed(schedules, at)
        # Four parameters per statement: three keys and the bound. On the
        # class, because SQLite's is a property that reads the live limit.
        # Django 6.1 made PostgreSQL's a cached_property, and a value already
        # cached on the instance shadows the class, so the patch has to clear
        # it. Without the pop this test reads PostgreSQL's real 65535, takes
        # the ten keys in one statement, and fails for the wrong reason.
        monkeypatch.setattr(type(connection.features), "max_query_params", 4)
        connection.features.__dict__.pop("max_query_params", None)
        worker = Worker(backoff_initial=0)
        with CaptureQueriesContext(connection) as captured:
            latest = worker._latest_ticks(schedules, at - timedelta(days=1))
        reads = [q for q in captured.captured_queries if "MAX(" in q["sql"].upper()]
        assert len(reads) == 4, "ten keys in slices of three is four reads"
        assert {key: read.at for key, read in latest.items()} == expected

    def test_a_connection_with_no_limit_reads_them_in_one(self, settings):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        settings.TASKS = tasks_setting({})
        schedules = self._schedules(10)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = self._seed(schedules, at)
        limit = connection.features.max_query_params
        assert limit is None or limit > 11, "this test wants an unconstrained read"
        with CaptureQueriesContext(connection) as captured:
            latest = Worker(backoff_initial=0)._latest_ticks(
                schedules, at - timedelta(days=1)
            )
        reads = [q for q in captured.captured_queries if "MAX(" in q["sql"].upper()]
        assert len(reads) == 1
        assert {key: read.at for key, read in latest.items()} == expected

    def test_more_keys_than_sqlite_3_31_allows_are_still_read(self, settings):
        # The real limit, set on the live connection: 999 is what SQLite
        # before 3.32.0 has, and Django 6.0 still supports 3.31.
        import sqlite3

        from django.db import connection

        if connection.vendor != "sqlite":
            pytest.skip("SQLite's parameter limit")
        settings.TASKS = tasks_setting({})
        schedules = self._schedules(1200)
        at = timezone.now().replace(second=0, microsecond=0)
        expected = self._seed(schedules, at)
        connection.ensure_connection()
        raw = connection.connection
        before = raw.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
        raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        try:
            # Django reads the limit off the live connection.
            assert connection.features.max_query_params == 999
            latest = Worker(backoff_initial=0)._latest_ticks(
                schedules, at - timedelta(days=1)
            )
        finally:
            raw.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, before)
        assert {key: read.at for key, read in latest.items()} == expected
