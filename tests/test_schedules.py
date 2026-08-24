from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import default_task_backend
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask
from django_ox.schedules import schedules_from_options
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
        assert str(schedule.cron) == "* * * * *"

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
            ({"task": "tests.tasks.add"}, "missing 'cron'"),
            ({"task": "tests.tasks.add", "cron": "not cron"}, "5 fields"),
            ({"task": "tests.tasks.add", "cron": "0 0 30 2 *"}, "never match"),
            ({"task": "tests.tasks.missing", "cron": "* * * * *"}, "cannot import"),
            (
                {"task": "tests.tasks.STATE", "cron": "* * * * *"},
                "not a django.tasks Task",
            ),
            (
                {"task": "tests.tasks.add", "cron": "* * * * *", "every": 5},
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
        with pytest.raises(ImproperlyConfigured, match="missing 'cron'"):
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
        stale = other._latest_ticks()
        monkeypatch.setattr(other, "_latest_ticks", lambda: stale)

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
