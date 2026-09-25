"""Dispatching schedules that live in the database."""

import logging
from datetime import timedelta

import pytest
from django import forms
from django.db import OperationalError, transaction
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.stored import (
    boundary_digest,
    create_schedule,
    update_schedule,
)
from django_ox.worker import Worker

from . import tasks


class _Args(ArgsForm):
    region = forms.CharField()


def tasks_setting():
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
            },
        }
    }


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(ScheduleKind(key="checked", task=tasks.add, form=_Args))


@pytest.fixture
def frozen_now(monkeypatch):
    """
    Pin timezone.now() mid-minute, for a test whose reads must share one.

    Dispatch derives its tick by flooring the clock to the minute, so a
    test that reads the clock, writes a row or a tick against it and then
    asks a worker what is due only holds while every one of those reads
    lands in the same minute. On the real clock a minute boundary falling
    inside the body moves the due tick to an instant nothing has recorded,
    and the schedule fires where the test says it must not, or fires twice.

    Not autouse: two tests here read the clock to watch a value advance,
    and a pinned clock would stop it.
    """
    fixed = timezone.now().replace(second=30, microsecond=0)
    monkeypatch.setattr(timezone, "now", lambda: fixed)
    return fixed


@pytest.fixture
def worker(settings):
    settings.TASKS = tasks_setting()
    return Worker(backoff_initial=0)


def a_minutely(**over):
    # The boundary is backdated by default so the current minute's tick is
    # at or after it. A schedule created at 14:37:41 has its 14:37:00 tick
    # *before* its boundary and correctly waits for 14:38:00, which is right
    # but makes for a slow test.
    fields = {
        "name": "minutely",
        "task_key": "report",
        "trigger": "cron",
        "cron": "* * * * *",
        "start_time": timezone.now() - timedelta(minutes=5),
    }
    fields.update(over)
    return create_schedule(**fields)


pytestmark = pytest.mark.django_db


class TestABoundaryReplacesTheAnchor:
    def test_a_row_fires_on_its_first_due_tick(self, worker):
        # The whole point of start_time. A settings schedule would record an
        # anchor here and enqueue nothing, so a row created while every
        # worker was down would lose its first run.
        a_minutely()
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1

    def test_a_tick_before_the_boundary_does_not_fire(self, worker):
        a_minutely(start_time=timezone.now() + timedelta(hours=1))
        assert worker.dispatch_schedules() == 0
        assert OxScheduleTick.objects.count() == 0

    def test_a_tick_after_the_end_time_does_not_fire(self, worker):
        row = a_minutely()
        update_schedule(row, end_time=row.start_time + timedelta(seconds=1))
        assert worker.dispatch_schedules() == 0

    @pytest.mark.usefixtures("frozen_now")
    def test_the_same_tick_fires_only_once(self, worker):
        a_minutely()
        assert worker.dispatch_schedules() == 1
        assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 1


class TestTheDisableRace:
    """
    The window no poll interval can close.

    A worker reads a schedule while its row is enabled, the row changes,
    and the worker then reaches dispatch still holding what it read.
    Re-reading more often shrinks the window and never removes it, so the
    check has to happen under the row's own lock inside the dispatch
    transaction.

    Each test pins the worker's view to what it read *before* the change,
    by replacing the source's answer outright. Priming the cache instead
    would be read back through the freshness check and quietly refreshed,
    which makes the test pass for the wrong reason.
    """

    def _hold_stale_view(self, worker, monkeypatch):
        stale = worker._schedule_source.schedules()
        assert stale, "the worker should be holding a schedule"
        monkeypatch.setattr(worker._schedule_source, "schedules", lambda: stale)
        return stale

    def test_a_schedule_disabled_after_it_was_read_does_not_fire(
        self, worker, monkeypatch
    ):
        row = a_minutely()
        self._hold_stale_view(worker, monkeypatch)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)

        assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 0
        assert not OxScheduleTick.objects.exists(), (
            "a refused tick must stay unclaimed so a current worker can act"
        )

    def test_a_schedule_retimed_after_it_was_read_does_not_fire(
        self, worker, monkeypatch
    ):
        # The ticks this worker computed are no longer this schedule's ticks.
        row = a_minutely()
        self._hold_stale_view(worker, monkeypatch)
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")

        assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 0

    def test_a_deleted_schedule_does_not_fire(self, worker, monkeypatch):
        row = a_minutely()
        self._hold_stale_view(worker, monkeypatch)
        OxSchedule.objects.filter(pk=row.pk).delete()

        assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 0


@pytest.fixture
def half_past(monkeypatch):
    """
    A clock pinned well past the hour.

    An hourly tick with a sixty-second deadline is only late once the hour
    is a minute old. On the wall clock these tests would pass for
    fifty-nine minutes in sixty and fail in the first, which says nothing
    about the code.
    """
    from django.utils import timezone as tz

    pinned = tz.now().replace(minute=30, second=0, microsecond=0)
    monkeypatch.setattr(tz, "now", lambda: pinned)
    return pinned


class TestTheStartingDeadline:
    @pytest.mark.usefixtures("half_past")
    def test_a_tick_later_than_the_deadline_is_dropped(self, worker):
        row = create_schedule(
            name="hourly",
            task_key="report",
            trigger="cron",
            cron="0 * * * *",
            start_time=timezone.now() - timedelta(days=2),
            starting_deadline_seconds=60,
        )
        assert row.pk
        assert worker.dispatch_schedules() == 0

    @pytest.mark.usefixtures("half_past")
    def test_without_a_deadline_a_late_tick_still_fires(self, worker):
        create_schedule(
            name="hourly",
            task_key="report",
            trigger="cron",
            cron="0 * * * *",
            start_time=timezone.now() - timedelta(days=2),
        )
        assert worker.dispatch_schedules() == 1


class TestABadRowDoesNotStopTheOthers:
    def test_an_unknown_key_is_skipped_and_the_tick_stays_unclaimed(self, worker):
        # The rolling-deploy case: an older worker meets a key only newer
        # code registers. If it claimed the tick, the worker that could run
        # it would be suppressed by the unique constraint and that tick would
        # silently never fire.
        unknown = OxSchedule.objects.create(
            name="future",
            task_key="only.in.new.code",
            trigger="cron",
            cron="* * * * *",
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1
        # The tick must be left unclaimed, or a worker that knows the key
        # would be suppressed by the unique constraint and it would never
        # fire. Asserted against the row's dispatch key: filtering on the
        # display name matches nothing and would pass however broken this is.
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{unknown.pk}"
        ).exists()

    def test_a_row_whose_arguments_no_longer_validate_is_skipped(self, worker):
        stale = OxSchedule.objects.create(
            name="stale-args",
            task_key="checked",
            trigger="cron",
            cron="* * * * *",
            arguments={"gone": 1},
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1
        assert not OxScheduleTick.objects.filter(
            schedule_name=f"db:{stale.pk}"
        ).exists()

    def test_a_row_with_an_unparseable_cron_is_skipped(self, worker):
        OxSchedule.objects.create(
            name="bad-cron",
            task_key="report",
            trigger="cron",
            cron="banana",
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1


class TestFreshness:
    def test_an_unchanged_marker_costs_one_query(
        self, worker, django_assert_num_queries
    ):
        a_minutely()
        worker._schedule_source.schedules()
        with django_assert_num_queries(1):
            worker._schedule_source.schedules()

    def test_a_change_is_noticed(self, worker):
        source = worker._schedule_source
        assert source.schedules() == []
        a_minutely()
        assert [s.name for s in source.schedules()] == ["minutely"]

    def test_a_disabled_schedule_leaves_the_set(self, worker):
        row = a_minutely()
        source = worker._schedule_source
        assert len(source.schedules()) == 1
        update_schedule(row, enabled=False)
        assert source.schedules() == []


@pytest.mark.usefixtures("frozen_now")
def test_a_schedule_created_mid_minute_waits_for_the_next_tick(worker):
    # Created at 14:37:41 with a minutely cron, the 14:37:00 tick is before
    # the boundary: at that instant the schedule did not exist. It fires at
    # 14:38:00, not immediately.
    create_schedule(
        name="just-now", task_key="report", trigger="cron", cron="* * * * *"
    )
    assert worker.dispatch_schedules() == 0
    assert not OxScheduleTick.objects.exists()


class TestABadRowCannotStopTheWorker:
    def test_a_zero_interval_row_is_skipped_and_the_others_still_fire(self, worker):
        # IntervalTrigger(every=0) raises
        # ZeroDivisionError, which is not a ValueError, so the per-row guard
        # missed it and it reached the dispatch loop.
        zero = OxSchedule.objects.create(
            name="zero",
            task_key="report",
            trigger="interval",
            cron="",
            every_seconds=0,
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1
        assert not OxScheduleTick.objects.filter(schedule_name=f"db:{zero.pk}").exists()

    def test_a_null_interval_row_cannot_be_created_at_all(self):
        # The check constraint refuses it, so the runtime guard never has to see
        # this shape. Zero still gets through, because zero is not null, which
        # is why both defences exist.
        from django.db.utils import IntegrityError

        with pytest.raises(IntegrityError):
            OxSchedule.objects.create(
                name="null-interval",
                task_key="report",
                trigger="interval",
                cron="",
                every_seconds=None,
                start_time=timezone.now() - timedelta(minutes=5),
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )

    def test_non_mapping_arguments_are_skipped(self, worker):
        OxSchedule.objects.create(
            name="listargs",
            task_key="report",
            trigger="cron",
            cron="* * * * *",
            arguments=[1, 2],
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1


class TestDeadlineValidation:
    def test_a_zero_deadline_is_refused(self):
        from django.core.exceptions import ValidationError

        with pytest.raises(ValidationError) as caught:
            a_minutely(name="d", starting_deadline_seconds=0)
        assert "starting_deadline_seconds" in caught.value.message_dict


class TestDeleteTellsTheWorkers:
    def test_deleting_a_schedule_bumps_the_change_row(self, worker):
        from django_ox.models import OxScheduleChange
        from django_ox.stored import delete_schedule

        row = a_minutely()
        before = OxScheduleChange.objects.get(id=1).changed_at
        delete_schedule(row)
        assert OxScheduleChange.objects.get(id=1).changed_at > before
        assert worker._schedule_source.schedules() == []


class TestRenamingCannotSplitTheCoordination:
    """
    A rename must not produce two tasks for one tick.

    Ticks are keyed on the dispatch log's name column; admission is keyed on
    the row. When the label a person edits was also the coordination key, two
    workers holding different labels for one row wrote two rows for the same
    instant, and the unique constraint saw nothing in common between them.
    """

    def _hold(self, worker, monkeypatch):
        held = worker._schedule_source.schedules()
        assert held
        monkeypatch.setattr(worker._schedule_source, "schedules", lambda: held)
        return held

    @pytest.mark.usefixtures("frozen_now")
    def test_a_rename_mid_flight_still_fires_once(self, worker, monkeypatch):
        row = a_minutely(name="old")
        self._hold(worker, monkeypatch)  # worker holds name="old"
        update_schedule(row, name="new")
        other = Worker(backoff_initial=0)  # reads name="new"

        worker.dispatch_schedules()
        other.dispatch_schedules()

        assert OxTask.objects.count() == 1, "one tick produced more than one task"
        assert OxScheduleTick.objects.count() == 1

    def test_the_tick_is_keyed_on_the_row_not_the_label(self, worker):
        row = a_minutely(name="labelled")
        worker.dispatch_schedules()
        assert OxScheduleTick.objects.get().schedule_name == f"db:{row.pk}"

    def test_renaming_preserves_tick_history(self, worker):
        row = a_minutely(name="before")
        worker.dispatch_schedules()
        before = set(OxScheduleTick.objects.values_list("schedule_name", flat=True))
        update_schedule(row, name="after")
        assert (
            set(OxScheduleTick.objects.values_list("schedule_name", flat=True))
            == before
        ), "a rename must not orphan the schedule's own history"

    def test_a_settings_schedule_may_not_use_the_reserved_prefix(self):
        from django.core.exceptions import ImproperlyConfigured

        from django_ox.schedules import schedules_from_options

        with pytest.raises(ImproperlyConfigured, match="reserved"):
            schedules_from_options(
                {
                    "SCHEDULES": {
                        "db:1": {"task": "tests.tasks.add", "cron": "* * * * *"}
                    }
                },
                "default",
            )


class TestTheDecisionComesFromTheRow:
    """
    A snapshot chooses candidates. The row decides.

    Each test here changes something after a worker has read the schedule
    and before it dispatches. None of them fires,
    because admission re-checked two columns and these are not those two.
    """

    def _hold(self, worker, monkeypatch):
        held = worker._schedule_source.schedules()
        assert held
        monkeypatch.setattr(worker._schedule_source, "schedules", lambda: held)
        return held

    @pytest.mark.usefixtures("frozen_now")
    def test_a_resumed_schedule_does_not_fire_a_pre_resume_tick(
        self, worker, monkeypatch
    ):
        # Pause and resume moves the boundary forward. A worker still holding
        # the pre-pause view would otherwise fire a tick from before it, which
        # is the retroactive run the boundary exists to prevent.
        row = a_minutely()
        self._hold(worker, monkeypatch)
        update_schedule(row, enabled=False)
        update_schedule(row, enabled=True)
        assert worker.dispatch_schedules() == 0
        assert not OxScheduleTick.objects.exists()

    def test_a_bulk_retime_does_not_fire_retroactively(self, worker, monkeypatch):
        # queryset.update() runs no model code at all, so nothing at write
        # time notices. The tick is recomputed from the row instead.
        row = a_minutely(
            cron="0 2 * * *", start_time=timezone.now() - timedelta(days=2)
        )
        self._hold(worker, monkeypatch)
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")
        assert worker.dispatch_schedules() == 0

    def test_a_tightened_deadline_drops_the_tick(self, worker, monkeypatch):
        # The clock is frozen well past the tick. Left to the wall clock,
        # the assertion would be that a minutely tick is more than a second old,
        # which is false for the first second of every minute: a one-in-
        # sixty failure that says nothing about the code.
        from django.utils import timezone as tz

        row = a_minutely(
            cron="0 * * * *", start_time=timezone.now() - timedelta(days=1)
        )
        self._hold(worker, monkeypatch)
        update_schedule(row, starting_deadline_seconds=60)
        frozen = tz.now().replace(minute=30, second=0, microsecond=0)
        monkeypatch.setattr(tz, "now", lambda: frozen)
        assert worker.dispatch_schedules() == 0

    def test_a_moved_end_time_stops_the_tick(self, worker, monkeypatch):
        row = a_minutely()
        self._hold(worker, monkeypatch)
        OxSchedule.objects.filter(pk=row.pk).update(
            end_time=row.start_time + timedelta(seconds=1)
        )
        assert worker.dispatch_schedules() == 0

    def test_the_task_that_runs_is_the_one_the_row_names_now(self, worker, monkeypatch):
        # The snapshot's task is not enqueued; the row's is.
        row = a_minutely()
        self._hold(worker, monkeypatch)
        update_schedule(row, task_key="checked", arguments={"region": "emea"})
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1
        assert "emea" in str(OxTask.objects.get().kwargs)

    def test_a_due_tick_costs_a_bounded_number_of_queries(
        self, worker, django_assert_max_num_queries
    ):
        # Deciding from the row adds a statement on the due-tick path.
        # Measured rather than assumed, and a bound rather than a number:
        # PostgreSQL and MySQL take the lock with one locking read, SQLite
        # has no row locks and takes a no-op write and then a read, so the
        # count differs by database and SQLite is the expensive one.
        #
        # This is the due path, which runs at most once a minute per
        # schedule. The polling path is unchanged. One of the nine is the
        # UPDATE that attaches the task to a tick row written before the
        # enqueue, so a worker that loses the tick announces nothing.
        a_minutely()
        worker._schedule_source.schedules()
        with django_assert_max_num_queries(9):
            worker.dispatch_schedules()


class TestTheBoundaryMustMatchTheTiming:
    """
    A write that runs no model code leaves the boundary set for the old
    timing. Dispatch notices, refuses the tick, and moves the boundary so
    the schedule resumes rather than being refused forever.
    """

    def test_a_fresh_worker_does_not_fire_a_raw_retimed_tick(self, worker):
        # Nobody holds a stale snapshot here: the worker reads the row after
        # the change. Comparing two reads cannot catch this; comparing the
        # boundary against the timing can.
        a_minutely(cron="0 2 * * *", start_time=timezone.now() - timedelta(days=2))
        OxSchedule.objects.filter(name="minutely").update(cron="0 3 * * *")
        assert Worker(backoff_initial=0).dispatch_schedules() == 0

    def test_the_boundary_is_moved_onto_the_new_timing(self, worker):
        from django_ox.stored import boundary_digest

        row = a_minutely(
            cron="0 2 * * *", start_time=timezone.now() - timedelta(days=2)
        )
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")
        for _ in range(3):
            worker.dispatch_schedules()
        row.refresh_from_db()
        assert row.boundary_for == boundary_digest(row)

    def test_a_raw_pause_and_resume_with_no_read_between_is_not_detected(self, worker):
        """
        The documented limit, asserted so it cannot drift into a surprise.

        A digest cannot see a round trip. Disabling and re-enabling outside
        the write API with no read in between leaves every column it covers
        exactly as it found them, so the boundary still looks current and a
        tick from before the resume can fire. update_schedule moves the
        boundary at the moment of the change; queryset.update leaves it to
        the next read.
        """
        row = a_minutely()
        worker._schedule_source.schedules()
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        assert worker.dispatch_schedules() == 1

    def test_a_raw_pause_a_read_found_does_not_replay_on_a_raw_resume(
        self, worker, monkeypatch
    ):
        """
        Pause at T+30 with queryset.update, let the T+60 tick pass while
        paused, resume at T+80 the same way, and the pass at T+90 must not
        fire T+60. The pause was seen by a read at T+40, so the boundary
        moved then; the resume is seen at T+90 and moves it again.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])
        row = a_minutely(start_time=base - timedelta(minutes=5))
        worker._schedule_source._reconcile_interval = 0

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(10)
        assert worker.dispatch_schedules() == 1, "T fires"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(40)
        assert worker.dispatch_schedules() == 0, "paused"
        at(80)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        at(90)
        assert worker.dispatch_schedules() == 0, "T+60 came due inside the pause"
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [0], f"replayed a tick from inside the pause: {fired}"
        at(130)
        assert worker.dispatch_schedules() == 1, "the schedule has resumed"

    def test_a_raw_pause_met_at_dispatch_does_not_replay_on_a_raw_resume(
        self, worker, monkeypatch
    ):
        """
        The other way a pause is found: no full read falls between the
        pause and the tick, so the worker meets the disabled row under the
        lock at dispatch. That sighting has to move the boundary too, on the
        next pass, or a raw resume found by the next full read replays the
        tick that came due inside the pause.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])
        row = a_minutely(start_time=base - timedelta(minutes=5))
        source = worker._schedule_source

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(10)
        assert worker.dispatch_schedules() == 1, "T fires"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        # The reconcile interval is on the monotonic clock and has not
        # elapsed, and a bulk update bumps no marker, so the cached copy
        # stands and the pause is met under the lock.
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal, "the pause seen at dispatch was not kept"
        at(75)
        assert worker.dispatch_schedules() == 0
        at(80)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        source._reconcile_interval = 0  # the next call is a full read
        at(90)
        assert worker.dispatch_schedules() == 0, "T+60 came due inside the pause"
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [0], f"replayed a tick from inside the pause: {fired}"
        at(130)
        assert worker.dispatch_schedules() == 1, "the schedule has resumed"

    def test_a_raw_resume_before_the_heal_does_not_cancel_it(self, worker, monkeypatch):
        """
        The pause is met under the lock at T+70 and queued for the next
        pass's heal. The raw resume lands at T+72, before that pass. At
        T+75 the row's digest matches its boundary again, because
        `enabled` is back to the value the boundary was set for, and a
        heal that asked only whether the digest matched would skip. The
        boundary must still move: the row was seen in a state it was not
        set for, and nothing else has moved it since.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])
        row = a_minutely(start_time=base - timedelta(minutes=5))
        source = worker._schedule_source

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(10)
        assert worker.dispatch_schedules() == 1, "T fires"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal, "the pause seen at dispatch was not kept"
        at(72)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        at(75)
        assert worker.dispatch_schedules() == 0, "the heal pass; not a full read"
        # The sighting is dropped when the heal commits, and the transaction
        # this test runs in never does. What the heal wrote is what matters
        # here; the queue emptying is covered against real commits in
        # TestASightingOutlivesARollbackOfTheCallersTransaction.
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=75), (
            "the heal was cancelled by the resume restoring the digest"
        )
        source._reconcile_interval = 0  # the next call is a full read
        at(90)
        assert worker.dispatch_schedules() == 0, "T+60 came due inside the pause"
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [0], f"replayed a tick from inside the pause: {fired}"
        at(130)
        assert worker.dispatch_schedules() == 1, "the schedule has resumed"

    def test_a_boundary_someone_else_moved_is_left_alone(self, worker, monkeypatch):
        """
        The other half of the same check. A second worker healed the row,
        or the write API resumed it, between this worker's sighting and
        its heal: the boundary column and start time differ from the ones
        this worker saw, and their boundary stands rather than being
        moved again to this worker's later clock.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])
        row = a_minutely(start_time=base - timedelta(minutes=5))
        source = worker._schedule_source

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(10)
        assert worker.dispatch_schedules() == 1
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0
        assert row.pk in source._needs_heal
        at(72)
        update_schedule(row, enabled=True)  # moves the boundary to T+72
        at(75)
        worker.dispatch_schedules()
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=72), (
            "a boundary the write API had already moved was moved again"
        )

    def test_a_boundary_one_heal_moved_is_not_moved_again_by_the_next(
        self, worker, monkeypatch
    ):
        """
        Two workers queue the same sighting, and only the first may heal.

        The fence cannot rest on the boundary columns coming back
        different. A heal writes `start_time=now`, and `now` is not
        guaranteed to differ from the start time the sighting recorded:
        two workers' clocks disagree, a clock steps back over an NTP
        correction, or the clock is coarse enough that the boundary write
        and the heal land in the same granule. The first heal then writes
        exactly the pair the sighting saw, and a second heal that asked
        only whether those columns moved would move the boundary again, to
        its own much later clock, discarding every tick in between.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        assert row.start_time == base + timedelta(seconds=5)
        second = Worker(backoff_initial=0)
        at(10)
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(11)
        assert second.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        at(71)
        assert second.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in worker._schedule_source._needs_heal
        assert row.pk in second._schedule_source._needs_heal
        at(72)
        OxSchedule.objects.filter(pk=row.pk).update(cron="* * * * *")
        # This worker's clock reads the instant the boundary was written
        # at, so its heal writes back the very pair the sighting saw.
        at(5)
        assert worker.dispatch_schedules() == 0
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=5)
        # Its write moved no column, so the count is the only record that
        # it happened, and the only thing the next heal can read it from.
        assert row.boundary_generation == 1, "the first heal did not run"
        # T+120 comes due with nobody dispatching, and the other worker's
        # pass is the one that should fire it.
        at(150)
        fired_now = second.dispatch_schedules()
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == (
            base + timedelta(seconds=5),
            1,
        ), "a second heal moved a boundary the first heal had already set"
        assert fired_now == 1
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [120], f"the tick between the two heals was lost: {fired}"

    def test_a_write_api_resume_that_rewrites_the_pair_still_fences_the_heal(
        self, worker, monkeypatch
    ):
        """
        The same fence, with the write API on the other side of it.

        `update_schedule` sets the boundary to its own clock too, so a
        resume whose clock reads the instant the boundary was last written
        at leaves both columns as the sighting saw them. The worker's
        pending heal has to skip anyway: the resume owns the boundary now,
        and a heal on top of it would move activation to the worker's
        later clock and drop the ticks before it.
        """
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        source = worker._schedule_source
        at(10)
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal
        # The write API's clock reads the instant the boundary was written
        # at, so the resume writes the columns back exactly as they stood.
        at(5)
        update_schedule(row, enabled=True)
        row.refresh_from_db()
        assert (row.boundary_for, row.start_time, row.boundary_generation) == (
            boundary_digest(row),
            base + timedelta(seconds=5),
            1,
        ), "the resume did not rewrite the pair the sighting saw, and count it"
        source._reconcile_interval = 0  # the next call is a full read
        at(150)
        fired_now = worker.dispatch_schedules()
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == (
            base + timedelta(seconds=5),
            1,
        ), "a heal moved a boundary the write API had already written"
        assert fired_now == 1
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [120], f"the tick after the resume was lost: {fired}"

    @pytest.mark.usefixtures("frozen_now")
    def test_the_same_pause_through_the_write_api_is_detected(self, worker):
        row = a_minutely()
        worker._schedule_source.schedules()
        update_schedule(row, enabled=False)
        update_schedule(row, enabled=True)
        assert worker.dispatch_schedules() == 0


class TestASightingOutlivesARollbackOfTheCallersTransaction:
    """
    A sighting is the only record that a row was met in a state its
    boundary was not set for, and the row itself keeps no trace of having
    been seen. `dispatch_schedules()` may be called inside a transaction
    the caller owns, and a heal written in one is undone when that
    transaction rolls back. Forgetting the sighting at the moment the
    heal is written would leave nothing to re-heal it: a raw resume puts
    the digest back, so no later read finds the row stale again, and the
    tick that came due inside the pause fires.

    Real commits, so the two outcomes are distinguishable at all: under a
    test transaction that never commits, every heal looks rolled back.
    """

    def _clock(self, monkeypatch):
        from django.utils import timezone as tz

        base = tz.now().replace(second=0, microsecond=0)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        return base, at

    def _fired(self, base):
        return sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )

    @pytest.mark.django_db(transaction=True)
    def test_a_rolled_back_heal_keeps_the_sighting_and_the_pause_holds(
        self, worker, monkeypatch
    ):
        base, at = self._clock(monkeypatch)
        source = worker._schedule_source
        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        at(10)
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal, "the pause seen at dispatch was not kept"
        at(75)
        with pytest.raises(RuntimeError), transaction.atomic():
            worker.dispatch_schedules()
            raise RuntimeError("the caller rolls back")
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == (
            base + timedelta(seconds=5),
            0,
        ), "the rollback did not undo the heal, so the test proves nothing"
        assert row.pk in source._needs_heal, (
            "the sighting was dropped by a heal the caller's rollback undid"
        )
        at(80)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        source._reconcile_interval = 0  # the next call is a full read
        at(90)
        assert worker.dispatch_schedules() == 0
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=90), (
            "nothing re-healed the row after the rollback"
        )
        at(95)
        assert worker.dispatch_schedules() == 0
        assert self._fired(base) == [], (
            "a tick from inside the pause fired after the rollback"
        )

    @pytest.mark.django_db(transaction=True)
    def test_a_committed_heal_still_forgets_the_sighting(self, worker, monkeypatch):
        # The other direction, so "never forget" is not a fix. A heal that
        # commits inside a caller's transaction is as done as one in
        # autocommit, and re-locking the row on every later pass would be
        # a query per schedule per second forever.
        base, at = self._clock(monkeypatch)
        source = worker._schedule_source
        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        at(10)
        # A pass before the pause, so the row is in the snapshot and the
        # pause is met under the lock at dispatch rather than by a read.
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal
        at(75)
        with transaction.atomic():
            worker.dispatch_schedules()
        assert not source._needs_heal, "a committed heal left the sighting queued"
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == (
            base + timedelta(seconds=75),
            1,
        )

    @pytest.mark.django_db(transaction=True)
    def test_an_autocommit_heal_forgets_the_sighting_in_the_same_pass(
        self, worker, monkeypatch
    ):
        # No caller transaction at all: the ordinary worker pass, which must
        # not start carrying a sighting into the next one.
        base, at = self._clock(monkeypatch)
        source = worker._schedule_source
        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        at(10)
        # A pass before the pause, so the row is in the snapshot and the
        # pause is met under the lock at dispatch rather than by a read.
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0
        assert row.pk in source._needs_heal
        at(75)
        assert worker.dispatch_schedules() == 0
        assert not source._needs_heal, "the heal left the sighting queued"
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=75)

    @pytest.mark.django_db(transaction=True)
    def test_a_sighting_nothing_is_left_to_heal_is_forgotten_too(
        self, worker, monkeypatch
    ):
        # The two passes that write nothing: another writer owns the
        # boundary now, and the row is gone. Neither is a reason to carry
        # the sighting into every later pass and take the row's lock again.
        base, at = self._clock(monkeypatch)
        second = Worker(backoff_initial=0)
        at(5)
        row = create_schedule(
            name="minutely", task_key="report", trigger="cron", cron="* * * * *"
        )
        gone = create_schedule(
            name="doomed", task_key="report", trigger="cron", cron="* * * * *"
        )
        at(10)
        assert worker.dispatch_schedules() == 0, "T is before the boundary"
        at(11)
        assert second.dispatch_schedules() == 0, "T is before the boundary"
        at(30)
        OxSchedule.objects.filter(pk__in=[row.pk, gone.pk]).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0
        at(71)
        assert second.dispatch_schedules() == 0
        assert row.pk in second._schedule_source._needs_heal
        assert gone.pk in second._schedule_source._needs_heal
        at(75)
        assert worker.dispatch_schedules() == 0, "this worker's heal wins"
        OxSchedule.objects.filter(pk=gone.pk).delete()
        at(80)
        assert second.dispatch_schedules() == 0
        assert not second._schedule_source._needs_heal, (
            "a sighting with nothing left to heal was carried forward"
        )
        row.refresh_from_db()
        assert (row.start_time, row.boundary_generation) == (
            base + timedelta(seconds=75),
            1,
        ), "the fenced heal moved a boundary that was not its to move"


class TestTheBoundaryIsTimedUnderTheLock:
    """
    Two writes of the boundary took their clock before the row lock:
    update_schedule at its start, and the heal once for every row it had
    queued. Waiting for the lock is time the schedule runs in, and a
    boundary from before the wait sits behind a tick that came due during
    it. Both now read the clock once the lock is held. As with the
    deadline, the wait is stood in for by a lock that moves the clock.
    """

    def _clock(self, monkeypatch):
        from django.utils import timezone as tz

        base = tz.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        clock = {"now": base}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])

        def at(seconds):
            clock["now"] = base + timedelta(seconds=seconds)

        return base, at

    def _lock_that_waits(self, monkeypatch, during_the_wait):
        """The first lock taken runs `during_the_wait` before it is granted."""
        from django_ox import stored

        lock_row = stored._lock_row
        waited = {"done": False}

        def lock_row_after_a_wait(pk, alias):
            if not waited["done"]:
                waited["done"] = True
                during_the_wait(pk)
            return lock_row(pk, alias)

        monkeypatch.setattr(stored, "_lock_row", lock_row_after_a_wait)

    def test_a_retime_is_timed_from_the_lock_not_from_before_the_wait(
        self, worker, monkeypatch
    ):
        """
        The row is hourly. update_schedule reads the clock at H-1s and waits
        for the lock until H+1s. A dispatcher holding the old definition
        derives the H tick, which the new definition also contains, and
        admits it under the lock against the boundary the retime wrote. A
        boundary of H-1s lets it through; the retime committed after H.
        """
        base, at = self._clock(monkeypatch)
        row = a_minutely(cron="0 * * * *", start_time=base - timedelta(hours=2))
        at(-1800)
        worker._schedule_source.schedules()  # the old definition, cached
        self._lock_that_waits(monkeypatch, lambda pk: at(1))
        at(-1)
        update_schedule(row, cron="0,30 * * * *")
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=1), (
            "the boundary was timed from before the lock wait"
        )
        at(5)
        assert worker.dispatch_schedules() == 0, "H is before the retime"
        assert OxScheduleTick.objects.exclude(task_id=None).count() == 0

    def test_a_heal_is_timed_from_its_own_lock(self, worker, monkeypatch):
        """
        A raw pause met at dispatch is healed on the next pass. That heal's
        lock waits from T+75 to T+125, and the row is re-enabled the same
        raw way at T+122, during the wait. The T+120 tick is inside the
        pause. A boundary of T+75 fires it; one of T+125 does not.
        """
        base, at = self._clock(monkeypatch)
        row = a_minutely(start_time=base - timedelta(minutes=5))
        source = worker._schedule_source
        at(10)
        assert worker.dispatch_schedules() == 1, "T fires"
        at(30)
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        at(70)
        assert worker.dispatch_schedules() == 0, "refused under the lock"
        assert row.pk in source._needs_heal

        def resumed_during_the_wait(pk):
            at(122)
            OxSchedule.objects.filter(pk=pk).update(enabled=True)
            at(125)

        self._lock_that_waits(monkeypatch, resumed_during_the_wait)
        at(75)
        worker.dispatch_schedules()
        row.refresh_from_db()
        assert row.start_time == base + timedelta(seconds=125), (
            "the heal was timed from before its lock wait"
        )
        fired = sorted(
            (t - base).total_seconds()
            for t in OxScheduleTick.objects.exclude(task_id=None).values_list(
                "scheduled_for", flat=True
            )
        )
        assert fired == [0], f"replayed a tick from inside the pause: {fired}"
        at(190)
        assert worker.dispatch_schedules() == 1, "the schedule has resumed"


class TestTheDeadlineIsJudgedUnderTheLock:
    """
    The starting deadline is a promise about how late a run may begin.

    A pass samples the clock once before its loop and then waits for the
    row's lock inside the transaction: for another dispatcher's enqueue and
    its receivers, for an admin save, up to the lock-wait timeout on MySQL.
    A tick inside its deadline at that first sample can be past it by the
    time the lock is granted, and judged against the first sample it was
    enqueued late. The clock is read again under the lock.

    The wait is stood in for by a lock that moves the clock: what the test
    pins is the order, that the deadline is judged after the lock and not
    before it, which a real wait would only demonstrate more slowly.
    """

    def _clock_that_advances_inside_the_lock(self, monkeypatch, before, after):
        from django_ox import stored
        from django_ox import worker as worker_module

        moment = {"now": before}
        monkeypatch.setattr(worker_module.timezone, "now", lambda: moment["now"])
        lock_row = stored._lock_row

        def lock_row_after_a_wait(pk, alias):
            row = lock_row(pk, alias)
            moment["now"] = after
            return row

        monkeypatch.setattr(stored, "_lock_row", lock_row_after_a_wait)

    def test_a_tick_past_its_deadline_once_the_lock_is_granted_is_refused(
        self, worker, monkeypatch
    ):
        a_minutely(starting_deadline_seconds=30)
        tick = timezone.now().replace(second=0, microsecond=0)
        self._clock_that_advances_inside_the_lock(
            monkeypatch,
            before=tick + timedelta(seconds=10),
            after=tick + timedelta(seconds=45),
        )
        assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 0
        assert OxScheduleTick.objects.count() == 0, "a refused tick leaves no row"

    def test_a_wait_that_stays_inside_the_deadline_still_fires(
        self, worker, monkeypatch
    ):
        a_minutely(starting_deadline_seconds=30)
        tick = timezone.now().replace(second=0, microsecond=0)
        self._clock_that_advances_inside_the_lock(
            monkeypatch,
            before=tick + timedelta(seconds=10),
            after=tick + timedelta(seconds=20),
        )
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1


class TestTheRowLockDistinguishesContentionFromTheDatabase:
    """
    The stored source skips a schedule whose row lock the database gave up
    waiting for, and only that. Any other failure at the lock goes to the
    dispatch loop, which asks the connection rather than the exception: a
    connection gone away at that statement ends the pass so run() reports
    it and drops the connection, and a statement that failed on a
    connection still standing is that schedule's.
    """

    def _lock_row_raising(self, monkeypatch, exc):
        from django_ox import stored

        def lock_row(pk, alias):
            raise exc

        monkeypatch.setattr(stored, "_lock_row", lock_row)

    def test_a_lock_the_database_gave_up_on_skips_the_schedule(
        self, worker, monkeypatch, caplog
    ):
        import logging

        from django.db import OperationalError

        a_minutely()
        self._lock_row_raising(monkeypatch, OperationalError("database is locked"))
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        warned = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_lock_unavailable"
        ]
        assert len(warned) == 1
        assert warned[0].exc_info is None, "contention is not a traceback"

    @pytest.mark.django_db(transaction=True)
    def test_a_connection_gone_away_at_the_lock_ends_the_pass(
        self, worker, monkeypatch, caplog
    ):
        import logging

        from django.db import Error

        from django_ox import stored

        from .isolation import kill

        a_minutely()
        real_lock_row = stored._lock_row

        def lock_row_on_a_dead_session(pk, alias):
            from django.db import connections

            kill(connections[alias])
            return real_lock_row(pk, alias)

        monkeypatch.setattr(stored, "_lock_row", lock_row_on_a_dead_session)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            pytest.raises(Error),
        ):
            worker.dispatch_schedules()
        assert not any(
            getattr(r, "event", None)
            in ("schedule_lock_unavailable", "schedule_dispatch_error")
            for r in caplog.records
        ), "a dead connection was reported as one schedule's"

    def test_a_failure_at_the_lock_on_a_usable_connection_is_the_schedules(
        self, worker, monkeypatch, caplog
    ):
        import logging

        from django.db import OperationalError

        a_minutely()
        self._lock_row_raising(
            monkeypatch, OperationalError("server closed the connection unexpectedly")
        )
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        reported = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_dispatch_error"
        ]
        assert [r.schedule for r in reported] == ["minutely"]
        assert not OxScheduleTick.objects.exists()


class TestTheWriteApiCannotLoseAnUpdate:
    def test_a_stale_instance_cannot_undo_a_resume(self):
        # Editor A holds a copy, editor B pauses and resumes, then A saves an
        # unrelated field. A's copy carries the old boundary, and writing
        # every field from it would put that boundary back.
        row = a_minutely()
        stale = OxSchedule.objects.get(pk=row.pk)
        update_schedule(row, enabled=False)
        update_schedule(row, enabled=True)
        resumed_boundary = OxSchedule.objects.get(pk=row.pk).start_time

        update_schedule(stale, name="renamed")

        after = OxSchedule.objects.get(pk=row.pk)
        assert after.name == "renamed"
        assert after.start_time == resumed_boundary, (
            "a stale instance wrote its old boundary over the resumed one"
        )


class TestNothingUnserialisableReachesTheEnqueue:
    """
    A task is enqueued as JSON. A form declares what its arguments clean to,
    and only some of those survive that. Checked at the write, at the read,
    and caught at dispatch, because a row can be written around all of it.
    """

    def _register(self, key, field):

        from django_ox.registry import ArgsForm, ScheduleKind, register

        ns = {"value": field}
        form = type("_F", (ArgsForm,), ns)
        register(ScheduleKind(key=key, task=tasks.add, form=form))
        return form

    @pytest.mark.parametrize(
        ("field_name", "raw"),
        [
            ("DateField", "2026-01-01"),
            ("DateTimeField", "2026-01-01 00:00"),
            ("DecimalField", "1.5"),
            ("DurationField", "1:00:00"),
            ("UUIDField", "8c8b0a5e-0a4e-4a6e-9b6a-3f7c1d2e5a90"),
        ],
    )
    def test_a_form_cleaning_to_a_non_json_value_is_refused_at_the_write(
        self, field_name, raw
    ):
        from django import forms
        from django.core.exceptions import ValidationError

        self._register("dated", getattr(forms, field_name)())
        with pytest.raises(ValidationError) as caught:
            a_minutely(name="dated", task_key="dated", arguments={"value": raw})
        assert "arguments" in caught.value.message_dict

    def test_such_a_row_written_around_validation_does_not_stop_the_others(
        self, worker
    ):
        # The failure this class exists to prevent: an error escaping the
        # enqueue stops the healthy schedule beside it from firing.
        from django import forms

        self._register("dated", forms.DateField())
        OxSchedule.objects.create(
            name="dated",
            task_key="dated",
            trigger="cron",
            cron="* * * * *",
            arguments={"value": "2026-01-01"},
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        a_minutely()
        assert worker.dispatch_schedules() == 1
        assert OxTask.objects.count() == 1

    def test_an_unexpected_error_skips_one_schedule_not_the_pass(
        self, worker, monkeypatch
    ):
        # Not a value problem: whatever goes wrong for one schedule, the
        # others in the same pass still run.
        #
        # Injected at _to_schedule rather than into the snapshot, because
        # dispatch rebuilds the schedule from the row inside the transaction
        # and a stub placed on the snapshot is discarded before it is used.
        import dataclasses

        class _Explodes:
            backend = "default"

            def enqueue(self, *args, **kwargs):
                raise RuntimeError("something nobody predicted")

        a_minutely(name="first")
        a_minutely(name="second")

        source = worker._schedule_source
        build = source._to_schedule

        def sabotage(row):
            built = build(row)
            if row.name == "first":
                return dataclasses.replace(built, task=_Explodes())
            return built

        monkeypatch.setattr(source, "_to_schedule", sabotage)

        worker.dispatch_schedules()
        assert OxTask.objects.count() == 1, "the healthy schedule did not run"


class TestARowChangedOutsideTheWriteApiIsFound:
    """
    The change marker is bumped by this package's write functions and by
    nothing else, so a raw write moves nothing a worker watches. Detection
    sits where the rows are read, not inside the dispatch transaction. A row
    only reaches that transaction
    if the cached copy says a tick is due, so a row whose cached copy said
    otherwise was never examined at all.
    """

    def _source(self, worker, interval=0.0001):
        source = worker._schedule_source
        source._reconcile_interval = interval
        return source

    def test_a_passed_end_time_does_not_hide_a_retime(self, worker):
        # The cached copy says the schedule ended, so no tick is ever due
        # and the dispatch transaction is never entered. Reading the rows is
        # the only thing that can notice.
        row = a_minutely(
            cron="0 * * * *",
            start_time=timezone.now() - timedelta(days=2),
            end_time=timezone.now() - timedelta(hours=2),
        )
        source = self._source(worker)
        source.schedules()
        OxSchedule.objects.filter(pk=row.pk).update(
            trigger="interval", cron="", every_seconds=60, end_time=None
        )
        for _ in range(3):
            worker.dispatch_schedules()
        row.refresh_from_db()
        assert row.boundary_for == boundary_digest(row), (
            "the row changed and no worker ever noticed"
        )

    def test_a_row_re_enabled_outside_the_write_api_is_found(self, worker):
        # Disabled rows are not read at all, so this one is neither in the
        # cache to refresh nor in the query that builds one.
        row = a_minutely()
        update_schedule(row, enabled=False)
        source = self._source(worker)
        assert source.schedules() == []
        OxSchedule.objects.filter(pk=row.pk).update(enabled=True)
        assert [s.name for s in source.schedules()] == ["minutely"]

    def test_a_row_created_outside_the_write_api_is_found(self, worker):
        source = self._source(worker)
        assert source.schedules() == []
        OxSchedule.objects.create(
            name="raw",
            task_key="report",
            trigger="cron",
            cron="* * * * *",
            start_time=timezone.now() - timedelta(minutes=5),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        assert [s.name for s in source.schedules()] == ["raw"]

    def test_the_marker_still_short_circuits_between_reconciles(
        self, worker, django_assert_num_queries
    ):
        # The backstop must not turn every pass into a full read. Between
        # reconciles a pass costs exactly the one marker read.
        a_minutely()
        source = worker._schedule_source
        source._reconcile_interval = 3600
        source.schedules()
        with django_assert_num_queries(1):
            source.schedules()

    def test_the_reconcile_reads_the_rows_again(
        self, worker, django_assert_num_queries
    ):
        # And when it is due, it costs the marker read plus the row read.
        a_minutely()
        source = worker._schedule_source
        source._reconcile_interval = 0.0001
        source.schedules()
        with django_assert_num_queries(2):
            source.schedules()

    def test_a_disabled_row_leaves_the_cache_when_dispatch_finds_it_gone(
        self, worker, monkeypatch
    ):
        row = a_minutely()
        source = worker._schedule_source
        source._reconcile_interval = 3600
        held = source.schedules()
        assert len(held) == 1
        OxSchedule.objects.filter(pk=row.pk).update(enabled=False)
        worker.dispatch_schedules()
        assert source._cached == [], "a schedule that cannot fire stayed cached"

    def test_a_retime_is_found_without_waiting_for_the_next_tick(self, worker):
        # A yearly schedule retimed in June. No tick is due for months, so
        # the dispatch transaction is never entered and the check inside it
        # never runs. Reading the rows is the only thing that can notice.
        row = a_minutely(
            cron="0 3 1 1 *",  # 03:00 on 1 January
            start_time=timezone.now() - timedelta(days=2),
        )
        source = self._source(worker)
        source.schedules()
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 4 1 1 *")
        for _ in range(3):
            worker.dispatch_schedules()
        row.refresh_from_db()
        assert row.boundary_for == boundary_digest(row), (
            "a retime went unnoticed because no tick was due to carry it"
        )


class TestAnOrdinaryEditCannotLaunderAStaleBoundary:
    """
    update_schedule rewrites the boundary digest on every call, whatever
    the call changed. That is what keeps the digest honest about the row
    it is stored on -- but on a row whose boundary was already stale, a
    rewrite alone would make a boundary set for an old definition look
    current, and the schedule would then fire an instant that passed
    while its new timing was in the future.

    So a call made on an already-stale row moves the boundary too, even
    when it changes nothing the boundary is for. A rename is the smallest
    such call, and the routes that leave a boundary stale -- a bulk
    update, a fixture, a data migration, objects.create() -- are all ones
    the schedules page tolerates.
    """

    @pytest.mark.usefixtures("frozen_now")
    def test_a_rename_after_a_raw_retime_moves_the_boundary(self, worker):
        was = timezone.now() - timedelta(days=2)
        row = create_schedule(
            name="nightly",
            task_key="report",
            trigger="cron",
            cron="0 2 * * *",
            start_time=was,
        )
        OxSchedule.objects.filter(pk=row.pk).update(cron="0 3 * * *")
        row.refresh_from_db()
        update_schedule(row, name="nightly-renamed")
        # The enqueue first: what this costs is a real run of the task at an
        # instant nothing scheduled while the new timing was in the future,
        # not a column reading.
        assert worker.dispatch_schedules() == 0, (
            "a tick from before the retime fired after an unrelated edit"
        )
        assert OxTask.objects.count() == 0
        row.refresh_from_db()
        assert row.start_time > was, (
            "the stale boundary was rewritten clean rather than moved"
        )
        assert row.boundary_generation == 1, "the boundary write was not counted"
        assert row.boundary_for == boundary_digest(row)

    @pytest.mark.usefixtures("frozen_now")
    def test_the_same_holds_for_a_row_written_with_objects_create(self, worker):
        # No raw retime at all: a row written around the write API carries
        # no digest, so its boundary is stale from the moment it exists.
        was = timezone.now() - timedelta(days=2)
        row = OxSchedule.objects.create(
            name="nightly",
            task_key="report",
            trigger="cron",
            cron="0 3 * * *",
            start_time=was,
            created_at=was,
            updated_at=was,
        )
        update_schedule(row, name="nightly-renamed")
        assert worker.dispatch_schedules() == 0, (
            "a tick from before the row existed to a worker fired"
        )
        assert OxTask.objects.count() == 0
        row.refresh_from_db()
        assert row.start_time == timezone.now(), (
            "the boundary the writer chose was kept rather than moved"
        )
        assert row.boundary_generation == 1
        assert row.boundary_for == boundary_digest(row)


class TestAnUnreadableMarkerIsReportedWithoutATraceback:
    """
    The marker read is one statement on one small table, once a dispatch
    pass, and while it keeps failing it keeps being reported: at the
    default interval that is a record a second per worker. A traceback on
    each of them is byte-identical every time and costs about 3.4 KB a
    record -- 13 MB an hour per worker -- which on a log or error-tracking
    quota is enough to rate-limit the reports that carry information. The
    same reasoning already governs the lock-contention warning next door.
    """

    def test_it_names_the_cause_and_carries_no_traceback(
        self, worker, caplog, monkeypatch
    ):
        a_minutely()
        source = worker._schedule_source
        source.schedules()

        def unreadable(*args, **kwargs):
            raise OperationalError("no such table: django_ox_oxschedulechange")

        monkeypatch.setattr(
            "django_ox.models.OxScheduleChange.objects", _Raising(unreadable)
        )
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            assert [s.name for s in source.schedules()] == ["minutely"], (
                "an unreadable marker must not read as no schedules at all"
            )
        warned = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_source_unavailable"
        ]
        assert len(warned) == 1
        assert warned[0].exc_info is None, (
            "a traceback repeated every pass is the whole of the log volume"
        )
        assert "no such table" in warned[0].getMessage(), (
            "without the traceback the message has to name the cause"
        )


class _Raising:
    """A manager stand-in whose every chained call raises."""

    def __init__(self, raiser):
        self._raiser = raiser

    def using(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def values_list(self, *args, **kwargs):
        return self

    def first(self):
        self._raiser()


class TestADroppedTickIsReportedOnce:
    def test_a_tick_already_recorded_is_not_reported_as_dropped(
        self, worker, caplog, monkeypatch
    ):
        # The deadline is checked after the already-fired suppression: a
        # tick that has run is not a tick that was dropped. Checking first
        # would re-report it on every later pass, which for a daily
        # schedule is a warning a second for a day on an event the docs
        # say can be alerted on.
        #
        # The clock has to move: the tick must be fresh when it fires and
        # stale when it is looked at again, which is the whole shape of
        # the case.
        import logging

        from django.utils import timezone as tz

        a_minutely(
            cron="0 * * * *",
            starting_deadline_seconds=120,
            start_time=tz.now() - timedelta(days=2),
        )
        real_now = tz.now().replace(minute=0, second=1, microsecond=0)
        clock = {"now": real_now}
        monkeypatch.setattr(tz, "now", lambda: clock["now"])

        assert worker.dispatch_schedules() == 1, "it should fire while fresh"

        clock["now"] = real_now + timedelta(minutes=30)  # same tick, now stale
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            for _ in range(5):
                worker.dispatch_schedules()
        dropped = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_tick_dropped"
        ]
        assert dropped == [], (
            f"a tick that already fired was reported dropped {len(dropped)} times"
        )

    @pytest.mark.usefixtures("half_past")
    def test_a_genuinely_late_tick_is_still_reported(self, worker, caplog):
        import logging

        a_minutely(
            cron="0 * * * *",
            start_time=timezone.now() - timedelta(days=2),
            starting_deadline_seconds=60,
        )
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker.dispatch_schedules()
        assert any(
            getattr(r, "event", None) == "schedule_tick_dropped" for r in caplog.records
        )

    @pytest.mark.usefixtures("half_past")
    def test_the_same_dropped_tick_is_reported_once_not_once_a_pass(
        self, worker, caplog
    ):
        # A dropped tick writes no row, so nothing else stops it being
        # recomputed and reported again on every pass until its next tick
        # comes due. The docs tell operators to alert on this event, and an
        # alert that fires once a second for a day cannot be acted on.
        import logging

        a_minutely(
            cron="0 * * * *",
            start_time=timezone.now() - timedelta(days=2),
            starting_deadline_seconds=60,
        )
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            for _ in range(20):
                worker.dispatch_schedules()
        dropped = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_tick_dropped"
        ]
        assert len(dropped) == 1, f"reported {len(dropped)} times over 20 passes"


class TestAnIntegrityErrorFromTheEnqueueIsNotALostRace:
    """
    The enqueue and the tick INSERT share one transaction, so an integrity
    failure raised by the task write surfaces the same way a lost race
    does. What separates them is how far the block had got: a lost race is
    the tick INSERT itself failing, and a failing enqueue comes after this
    pass's own tick row went in. Read as a lost race, a real failure would
    be retried silently for as long as it kept failing.
    """

    def test_a_failing_enqueue_is_reported(self, worker, caplog, monkeypatch):
        import logging

        from django.db import IntegrityError

        a_minutely()

        def boom(*args, **kwargs):
            raise IntegrityError("the task write failed")

        monkeypatch.setattr("django_ox.backend.OxBackend.enqueue", boom, raising=True)
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        assert [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_dispatch_error"
        ], "a failing enqueue was read as another worker winning the race"
        assert not OxScheduleTick.objects.exists(), "a tick was claimed anyway"

    @pytest.mark.django_db(transaction=True)
    def test_a_failing_enqueue_is_reported_even_as_another_worker_claims_the_tick(
        self, worker, caplog, monkeypatch
    ):
        # The log cannot tell the two apart: a winner whose INSERT was
        # waiting on this pass's uncommitted row lands the moment this pass
        # rolls back, so a read of the log after the rollback finds a tick
        # row and says "lost race" about a failure that was this worker's.
        import logging
        import threading
        import time

        from django.db import IntegrityError, connection
        from django.utils import timezone as tz

        row = a_minutely()
        key = f"db:{row.pk}"
        tick = tz.now().replace(second=0, microsecond=0)
        winner_done = threading.Event()

        def another_worker_claims_the_tick():
            try:
                OxScheduleTick.objects.create(
                    schedule_name=key, scheduled_for=tick, created_at=tz.now()
                )
            finally:
                connection.close()
                winner_done.set()

        winner = threading.Thread(target=another_worker_claims_the_tick)

        def boom(*args, **kwargs):
            # The other worker's INSERT waits on this pass's uncommitted row
            # and goes through the moment the failure below rolls it back.
            winner.start()
            time.sleep(0.2)
            raise IntegrityError("the task write failed")

        rollback = connection.rollback

        def rollback_then_let_the_winner_land():
            # The instant the log would be asked about: after this pass's
            # rollback, once the waiting INSERT has gone through.
            rollback()
            winner_done.wait(timeout=10)

        monkeypatch.setattr("django_ox.backend.OxBackend.enqueue", boom, raising=True)
        monkeypatch.setattr(connection, "rollback", rollback_then_let_the_winner_land)
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        winner.join(timeout=30)
        assert not winner.is_alive()
        assert OxScheduleTick.objects.filter(
            schedule_name=key, scheduled_for=tick
        ).exists(), "the other worker's claim should stand"
        assert [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_dispatch_error"
        ], "this worker's failing enqueue was read as the other worker winning"

    @pytest.mark.usefixtures("frozen_now")
    def test_a_genuine_lost_race_stays_silent(self, worker, caplog, monkeypatch):
        import logging

        from django.utils import timezone as tz

        row = a_minutely()
        # The tick another worker already committed.
        tick = OxScheduleTick.objects.create(
            schedule_name=f"db:{row.pk}",
            scheduled_for=tz.now().replace(second=0, microsecond=0),
            created_at=tz.now(),
        )
        assert tick.pk
        # With a current view the pass would skip the tick before the
        # INSERT and never race at all. The race is a stale view: the log
        # read before the loop says nothing is recorded, and the INSERT is
        # what finds out otherwise.
        monkeypatch.setattr(worker, "_latest_ticks", lambda schedules, since: {})
        with caplog.at_level(logging.ERROR, logger="django_ox"):
            assert worker.dispatch_schedules() == 0
        assert OxTask.objects.count() == 0, "the loser enqueued"
        assert not [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_dispatch_error"
        ], "a lost race was reported as a failure"


class TestSettingsSchedulesKeepWorkingBesideTheRows:
    """
    docs/stored-schedules.md: "`SCHEDULES` entries keep working if you use
    both." Naming the database source must add the rows to the settings
    schedules, not replace them, or the switch silently stops every
    schedule a project already had.
    """

    def _both(self, settings):
        config = tasks_setting()
        config["default"]["OPTIONS"]["SCHEDULES"] = {
            "from-settings": {
                "task": "tests.tasks.add",
                "cron": "* * * * *",
                "args": [1, 2],
            }
        }
        settings.TASKS = config
        a_minutely()
        return Worker(backoff_initial=0)

    def test_the_worker_sees_the_settings_schedule_and_the_row(self, settings):
        worker = self._both(settings)
        keys = sorted(s.key for s in worker._schedule_source.schedules())
        assert len(keys) == 2, keys
        assert "from-settings" in keys, "the settings schedule was dropped"
        assert keys[0].startswith("db:"), keys

    def test_one_pass_serves_both(self, settings):
        worker = self._both(settings)
        # The row fires on its first due tick; the settings schedule is
        # anchored on first sight and fires on the next. Both leave a row.
        assert worker.dispatch_schedules() == 1
        names = set(OxScheduleTick.objects.values_list("schedule_name", flat=True))
        assert "from-settings" in names, "the settings schedule was dropped"
        assert any(n.startswith("db:") for n in names)
        assert OxScheduleTick.objects.get(schedule_name="from-settings").task is None
