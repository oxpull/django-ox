"""
Schedules across a daylight-saving transition.

Driven through `dispatch_schedules` with a frozen clock, not through the
trigger alone. The trigger's arithmetic and the dispatch path can disagree
about a repeated hour, and only the dispatch path is what runs.
"""

import datetime as dt
from datetime import timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

import pytest
from django.utils import timezone

from django_ox.models import OxScheduleTick
from django_ox.worker import Worker

pytestmark = pytest.mark.django_db

LONDON_FALL_BACK = "2025-10-26"
LONDON_SPRING_FORWARD = "2025-03-30"


def tasks_setting(schedules):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULES": schedules},
        }
    }


def run_across(
    monkeypatch, settings, schedules, day, start_h, end_h, zone="Europe/London"
):
    """Step one UTC minute at a time and return every instant that fired."""
    settings.TIME_ZONE = zone
    settings.USE_TZ = True
    settings.TASKS = tasks_setting(schedules)
    worker = Worker(backoff_initial=0)
    base = dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=dt.UTC)
    clock = {"now": base + timedelta(hours=start_h)}
    monkeypatch.setattr(timezone, "now", lambda: clock["now"])
    stop = base + timedelta(hours=end_h)
    while clock["now"] < stop:
        worker.dispatch_schedules()
        clock["now"] += timedelta(minutes=1)
    return sorted(
        OxScheduleTick.objects.exclude(task__isnull=True).values_list(
            "scheduled_for", flat=True
        )
    )


def biggest_gap(instants):
    return max(
        (b - a for a, b in pairwise(instants)),
        default=timedelta(0),
    )


class TestTheRepeatedHour:
    @pytest.mark.parametrize("every", [900, 1800, 3600])
    def test_an_interval_keeps_its_cadence_through_the_fall_back(
        self, every, monkeypatch, settings
    ):
        # The repeated hour has two distinct instants for one wall-clock
        # label. Deriving the tick by arithmetic drops the fold, which would
        # give both passes the same instant, suppress the second as already
        # recorded, and fire nothing for the whole hour.
        fired = run_across(
            monkeypatch,
            settings,
            {"iv": {"task": "tests.tasks.add", "every": every}},
            LONDON_FALL_BACK,
            0,
            3,
        )
        assert fired, "nothing fired at all"
        assert biggest_gap(fired) == timedelta(seconds=every), (
            f"cadence broke across the fall-back: gaps up to {biggest_gap(fired)}"
        )

    def test_a_cron_still_fires_twice_on_the_repeated_hour(self, monkeypatch, settings):
        # Documented behaviour, and it must stay: a wall-clock schedule
        # inside the repeated hour genuinely happens twice.
        fired = run_across(
            monkeypatch,
            settings,
            {"c": {"task": "tests.tasks.add", "cron": "30 1 * * *"}},
            LONDON_FALL_BACK,
            0,
            3,
        )
        assert len(fired) == 2, f"expected both passes of 01:30, got {fired}"

    def test_a_cron_keeps_its_cadence_through_the_fall_back(
        self, monkeypatch, settings
    ):
        fired = run_across(
            monkeypatch,
            settings,
            {"c": {"task": "tests.tasks.add", "cron": "*/15 * * * *"}},
            LONDON_FALL_BACK,
            0,
            3,
        )
        # The count as well as the gap: an evenly spaced subset satisfies the
        # gap on its own, so two surviving ticks would pass it.
        assert len(fired) == 11, f"expected every quarter hour, got {fired}"
        assert biggest_gap(fired) == timedelta(minutes=15)


class TestTheMissingHour:
    @pytest.mark.parametrize("every", [900, 3600])
    def test_an_interval_loses_no_tick_across_the_spring_forward(
        self, every, monkeypatch, settings
    ):
        fired = run_across(
            monkeypatch,
            settings,
            {"iv": {"task": "tests.tasks.add", "every": every}},
            LONDON_SPRING_FORWARD,
            0,
            3,
        )
        assert fired
        assert biggest_gap(fired) == timedelta(seconds=every)

    def test_a_cron_loses_no_tick_across_the_spring_forward(
        self, monkeypatch, settings
    ):
        fired = run_across(
            monkeypatch,
            settings,
            {"c": {"task": "tests.tasks.add", "cron": "*/15 * * * *"}},
            LONDON_SPRING_FORWARD,
            0,
            3,
        )
        assert len(fired) == 11, f"expected every quarter hour, got {fired}"
        assert biggest_gap(fired) == timedelta(minutes=15)


class TestALocalTimeThatDoesNotExist:
    """
    On the day a zone springs forward, an hour of wall-clock labels never
    happens. Attaching the zone to one of them resolves to an instant on
    the far side of the gap, which has not arrived.

    A tick is the latest instant at or before now, so one resolving into
    the future is refused. Enqueueing it would run the task early and stamp
    it with the instant of the next real tick, which the dispatch log would
    then read as already recorded.
    """

    def test_an_interval_whose_tick_lands_in_the_gap_does_not_fire_early(
        self, monkeypatch, settings
    ):
        settings.TIME_ZONE = "America/New_York"
        settings.USE_TZ = True
        settings.TASKS = tasks_setting(
            {"iv": {"task": "tests.tasks.add", "every": 3600, "phase": 1800}}
        )
        worker = Worker(backoff_initial=0)
        # 2025-03-09 in New York: 02:00 EST becomes 03:00 EDT, so every
        # wall clock from 02:00 to 02:59 is a time that never happens.
        base = dt.datetime(2025, 3, 9, 5, 0, tzinfo=dt.UTC)
        clock = {"now": base}
        monkeypatch.setattr(timezone, "now", lambda: clock["now"])
        stop = base + timedelta(hours=6)
        early = []
        while clock["now"] < stop:
            worker.dispatch_schedules()
            early += list(
                OxScheduleTick.objects.filter(
                    scheduled_for__gt=clock["now"], task__isnull=False
                )
            )
            clock["now"] += timedelta(minutes=1)
        assert not early, (
            "enqueued a tick before its instant: "
            f"{[(r.schedule_name, r.scheduled_for.isoformat()) for r in early]}"
        )

    def test_the_tick_on_the_far_side_of_the_gap_still_fires(
        self, monkeypatch, settings
    ):
        fired = run_across(
            monkeypatch,
            settings,
            {"iv": {"task": "tests.tasks.add", "every": 3600, "phase": 1800}},
            "2025-03-09",
            5,
            11,
            zone="America/New_York",
        )
        labels = [f.astimezone(ZoneInfo("America/New_York")) for f in fired]
        assert any(stamp.hour == 3 and stamp.minute == 30 for stamp in labels), (
            f"the first tick after the gap never fired: {labels}"
        )


class TestTheWallClockLimitWithoutTimeZoneSupport:
    """
    Under USE_TZ=False a tick's time is stored as a naive wall clock, and
    the repeated hour has one label for two instants. The second is read as
    a tick already recorded.

    Recorded here as a limit rather than half-guarded. Closing it means
    changing what the tick log stores, and that table's schema is a
    published promise. `django_ox.W001` reports the configuration, and the
    schedules page states the effect.
    """

    def test_an_interval_loses_the_repeated_hour_s_second_pass(
        self, monkeypatch, settings
    ):
        settings.TIME_ZONE = "Europe/London"
        settings.USE_TZ = False
        settings.TASKS = tasks_setting(
            {"iv": {"task": "tests.tasks.add", "every": 1800}}
        )
        worker = Worker(backoff_initial=0)
        base = dt.datetime(2025, 10, 26, tzinfo=dt.UTC)
        clock = {"utc": base}
        # USE_TZ=False makes timezone.now() naive local time. Stepping the
        # underlying UTC instant is what makes the repeated hour happen.
        monkeypatch.setattr(
            timezone,
            "now",
            lambda: (
                clock["utc"].astimezone(ZoneInfo("Europe/London")).replace(tzinfo=None)
            ),
        )
        stop = base + timedelta(hours=3)
        while clock["utc"] < stop:
            worker.dispatch_schedules()
            clock["utc"] += timedelta(minutes=1)
        fired = sorted(
            OxScheduleTick.objects.exclude(task__isnull=True).values_list(
                "scheduled_for", flat=True
            )
        )
        # Six half-hourly instants pass in three hours, four distinct wall
        # clock labels cover them, and the first is spent anchoring.
        assert len(fired) == 3, f"expected the documented loss, got {fired}"
        # The gap alone cannot see this, which is why the count is asserted.
        assert biggest_gap(fired) == timedelta(minutes=30)

    def test_the_check_reports_the_configuration(self, settings):
        from django_ox.compat import default_task_backend

        settings.TIME_ZONE = "Europe/London"
        settings.USE_TZ = False
        settings.TASKS = tasks_setting(
            {"iv": {"task": "tests.tasks.add", "every": 1800}}
        )
        assert "django_ox.W001" in [e.id for e in default_task_backend.check()]

    @pytest.mark.parametrize(
        "zone",
        [
            # Half an hour rather than a whole one, and southern hemisphere.
            "Australia/Lord_Howe",
            "Europe/London",
            "America/New_York",
        ],
    )
    def test_every_zone_that_repeats_an_hour_is_reported(self, zone, settings):
        from django_ox.schedules import zone_repeats_an_hour

        settings.TIME_ZONE = zone
        assert zone_repeats_an_hour(), f"{zone} repeats an hour and was missed"

    @pytest.mark.parametrize(
        "zone", ["UTC", "Asia/Tokyo", "Africa/Nairobi", "Asia/Kolkata"]
    )
    def test_a_zone_that_never_puts_the_clock_back_is_not_reported(
        self, zone, settings
    ):
        from django_ox.schedules import zone_repeats_an_hour

        settings.TIME_ZONE = zone
        assert not zone_repeats_an_hour(), f"{zone} was reported and should not be"

    def test_a_zone_without_a_transition_is_not_reported(self, settings):
        from django_ox.compat import default_task_backend

        settings.TIME_ZONE = "UTC"
        settings.USE_TZ = False
        settings.TASKS = tasks_setting(
            {"iv": {"task": "tests.tasks.add", "every": 1800}}
        )
        assert "django_ox.W001" not in [e.id for e in default_task_backend.check()]


# ---------------------------------------------------------------------------
# A cron label inside the missing hour, and on the repeated hour, from both
# sources. The interval case above covers the future-tick guard; this covers
# the cron case it was written for. `CronExpression.previous()` walks naive
# wall-clock minutes with no knowledge of the zone, so on the spring-forward
# day the label it answers can be one that never happens, and `make_aware`
# is what decides where that label lands.

NEW_YORK = "America/New_York"
# 02:00 EST becomes 03:00 EDT at 07:00Z: the wall clocks 02:00-02:59 never happen.
NEW_YORK_SPRING_FORWARD = dt.datetime(2025, 3, 9, tzinfo=dt.UTC)
# 02:00 EDT becomes 01:00 EST at 06:00Z: the wall clocks 01:00-01:59 happen twice.
NEW_YORK_FALL_BACK = dt.datetime(2025, 11, 2, tzinfo=dt.UTC)


def utc(*args):
    return dt.datetime(*args, tzinfo=dt.UTC)


def stored_setting():
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"},
        }
    }


@pytest.fixture(params=["settings", "stored"])
def cron_schedule(request, monkeypatch, settings):
    """
    Install one cron schedule named `c`, from settings or as a stored row
    with its boundary at `start`, and return a worker for it.

    The two sources differ on their first tick: a settings schedule anchors
    at first sighting, a row fires from its boundary. Every test below
    starts its clock a day's tick before the one it is about, so the
    settings anchor is spent on the day before and the assertions read the
    same for both.
    """
    from django_ox.registry import ScheduleKind, register
    from django_ox.stored import create_schedule

    from . import tasks

    def install(cron, start):
        if request.param == "settings":
            settings.TASKS = tasks_setting(
                {"c": {"task": "tests.tasks.add", "cron": cron}}
            )
        else:
            monkeypatch.setattr("django_ox.registry._registry", {})
            monkeypatch.setattr("django_ox.registry._discovered", True)
            register(ScheduleKind(key="report", task=tasks.add))
            settings.TASKS = stored_setting()
            create_schedule(
                name="c",
                task_key="report",
                trigger="cron",
                cron=cron,
                arguments={"a": 1, "b": 2},
                start_time=start,
            )
        return Worker(backoff_initial=0)

    install.source = request.param
    return install


def step(worker, monkeypatch, start, stop, to_local=None):
    """
    Step the clock one UTC minute at a time from start to stop.

    Returns every instant that fired, and every row that was enqueued while
    its own instant was still ahead of the clock. `to_local` turns the
    stepped UTC instant into what timezone.now() returns, for the
    USE_TZ=False leg, where the second reading is not made: a naive wall
    clock that goes back reads its own past as the future.
    """
    clock = {"utc": start}
    monkeypatch.setattr(
        timezone,
        "now",
        lambda: to_local(clock["utc"]) if to_local is not None else clock["utc"],
    )
    early = []
    while clock["utc"] < stop:
        worker.dispatch_schedules()
        if to_local is None:
            early += [
                (r.schedule_name, r.scheduled_for.isoformat())
                for r in OxScheduleTick.objects.filter(
                    scheduled_for__gt=clock["utc"], task__isnull=False
                )
            ]
        clock["utc"] += timedelta(minutes=1)
    fired = sorted(
        OxScheduleTick.objects.exclude(task__isnull=True).values_list(
            "scheduled_for", flat=True
        )
    )
    return fired, early


class TestACronLabelInTheMissingHour:
    @pytest.mark.parametrize(
        ("cron", "fires_at"),
        [
            # The label resolves with fold=0 to the offset before the
            # transition, so 02:30 EST is 07:30Z, which is 03:30 EDT: half
            # an hour after the clock lands on the far side. It shifts
            # rather than raising, which is what the recurring-tasks page
            # promises, and it is held until that instant arrives.
            ("30 2 * * *", utc(2025, 3, 9, 7, 30)),
            # The leading edge: 02:00 EST is 07:00Z, the very instant the
            # clock becomes 03:00 EDT, so it fires on the first pass after
            # the gap.
            ("0 2 * * *", utc(2025, 3, 9, 7, 0)),
            # The trailing edge: 02:59 EST is 07:59Z, 03:59 EDT.
            ("59 2 * * *", utc(2025, 3, 9, 7, 59)),
        ],
    )
    def test_the_tick_is_not_fired_early_and_fires_exactly_once(
        self, cron, fires_at, cron_schedule, monkeypatch, settings
    ):
        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = True
        start = NEW_YORK_SPRING_FORWARD + timedelta(hours=5)  # 00:00 EST
        worker = cron_schedule(cron, start - timedelta(days=1))
        # The day before, so the settings anchor and the row's first tick
        # both land on the 8th and the 9th is the same test for both.
        day_before = start - timedelta(days=1)
        step(worker, monkeypatch, day_before, day_before + timedelta(hours=4))
        before = OxScheduleTick.objects.count()
        fired, early = step(worker, monkeypatch, start, start + timedelta(hours=6))
        assert not early, f"enqueued before its instant: {early}"
        assert [f for f in fired if f >= start] == [fires_at], (
            f"{cron_schedule.source}: expected one tick at {fires_at.isoformat()}, "
            f"got {[f.isoformat() for f in fired if f >= start]}"
        )
        # Exactly one row for the day, fired: nothing anchored and nothing
        # recorded for the label that never happened.
        assert OxScheduleTick.objects.count() == before + 1

    def test_the_following_day_s_tick_is_not_swallowed(
        self, cron_schedule, monkeypatch, settings
    ):
        # The shifted instant is stamped 07:30Z, and the next day's 02:30
        # EDT is 06:30Z: a distinct instant, so the log does not read it as
        # already recorded.
        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = True
        start = NEW_YORK_SPRING_FORWARD + timedelta(hours=5)
        worker = cron_schedule("30 2 * * *", start - timedelta(days=1))
        day_before = start - timedelta(days=1)
        step(worker, monkeypatch, day_before, day_before + timedelta(hours=4))
        step(worker, monkeypatch, start, start + timedelta(hours=6))
        next_day = NEW_YORK_SPRING_FORWARD + timedelta(days=1, hours=6)  # 02:00 EDT
        fired, early = step(
            worker, monkeypatch, next_day, next_day + timedelta(hours=1)
        )
        assert not early
        assert [f for f in fired if f >= start] == [
            utc(2025, 3, 9, 7, 30),
            utc(2025, 3, 10, 6, 30),
        ], fired

    def test_the_guard_is_what_holds_the_tick_back(
        self, cron_schedule, monkeypatch, settings
    ):
        # The mutation check as a test: without the future-tick guard the
        # label would be enqueued the moment the clock reads 03:00 EDT,
        # stamped 07:30Z, half an hour early. Two passes, no stepper, so
        # this pins the pass's own arithmetic.
        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = True
        worker = cron_schedule(
            "30 2 * * *", NEW_YORK_SPRING_FORWARD - timedelta(days=1)
        )
        # The day before, so a settings schedule has spent its anchor.
        monkeypatch.setattr(timezone, "now", lambda: utc(2025, 3, 8, 7, 30))
        worker.dispatch_schedules()
        monkeypatch.setattr(timezone, "now", lambda: utc(2025, 3, 9, 7, 0))
        worker.dispatch_schedules()
        assert not OxScheduleTick.objects.filter(
            task__isnull=False, scheduled_for__gte=utc(2025, 3, 9)
        ).exists()
        monkeypatch.setattr(timezone, "now", lambda: utc(2025, 3, 9, 7, 30))
        worker.dispatch_schedules()
        assert list(
            OxScheduleTick.objects.filter(
                task__isnull=False, scheduled_for__gte=utc(2025, 3, 9)
            ).values_list("scheduled_for", flat=True)
        ) == [utc(2025, 3, 9, 7, 30)]


class TestACronLabelOnTheRepeatedHour:
    @pytest.mark.parametrize(
        ("cron", "fires_at"),
        [
            # What the recurring-tasks page promises: a wall-clock schedule
            # inside the repeated hour fires once per pass, and no more.
            ("30 1 * * *", [utc(2025, 11, 2, 5, 30), utc(2025, 11, 2, 6, 30)]),
            ("0 1 * * *", [utc(2025, 11, 2, 5, 0), utc(2025, 11, 2, 6, 0)]),
            ("59 1 * * *", [utc(2025, 11, 2, 5, 59), utc(2025, 11, 2, 6, 59)]),
            # Outside the repeated hour: once, at 02:00 EST.
            ("0 2 * * *", [utc(2025, 11, 2, 7, 0)]),
        ],
    )
    def test_fires_once_per_pass_and_nothing_else(
        self, cron, fires_at, cron_schedule, monkeypatch, settings
    ):
        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = True
        start = NEW_YORK_FALL_BACK + timedelta(hours=4)  # 00:00 EDT
        worker = cron_schedule(cron, start - timedelta(days=1))
        day_before = start - timedelta(days=1)
        step(worker, monkeypatch, day_before, day_before + timedelta(hours=4))
        fired, early = step(worker, monkeypatch, start, start + timedelta(hours=5))
        assert not early
        assert [f for f in fired if f >= start] == fires_at, (
            f"{cron_schedule.source}: {[f.isoformat() for f in fired if f >= start]}"
        )


class TestACronLabelWithoutTimeZoneSupport:
    """
    USE_TZ=False stores the naive wall clock. The docs promise the loss on
    the repeated hour and that W001 names it; the missing hour is promised
    nothing, so what happens there is recorded as it is.
    """

    @staticmethod
    def naive_new_york(instant):
        return instant.astimezone(ZoneInfo(NEW_YORK)).replace(tzinfo=None)

    @pytest.mark.parametrize("cron", ["30 2 * * *", "0 2 * * *", "59 2 * * *"])
    def test_a_label_in_the_missing_hour_fires_once_when_the_clock_passes_it(
        self, cron, cron_schedule, monkeypatch, settings
    ):
        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = False
        start = NEW_YORK_SPRING_FORWARD + timedelta(hours=5)
        worker = cron_schedule(cron, self.naive_new_york(start - timedelta(days=1)))
        step(
            worker,
            monkeypatch,
            start - timedelta(days=1),
            start - timedelta(hours=20),
            to_local=self.naive_new_york,
        )
        day = dt.datetime(2025, 3, 9)
        # Up to 01:59 EST nothing has fired: the label is still ahead.
        fired, _ = step(
            worker,
            monkeypatch,
            start,
            start + timedelta(hours=2),
            to_local=self.naive_new_york,
        )
        assert not [f for f in fired if f >= day], fired
        # The naive wall clock steps from 01:59 to 03:00, so the label is
        # behind the clock on the first pass after the gap and fires then,
        # once. What the row is stamped with is the engine's business: a
        # wall clock that never happened has no instant, and PostgreSQL
        # normalises it forward when the session zone is the project's,
        # while SQLite and MySQL keep the text. Nothing is promised there
        # beyond firing once, so only the count and the minute are pinned.
        fired, _ = step(
            worker,
            monkeypatch,
            start + timedelta(hours=2),
            start + timedelta(hours=6),
            to_local=self.naive_new_york,
        )
        today = [f for f in fired if f >= day]
        minute = int(cron.split()[0])
        assert len(today) == 1, fired
        assert today[0].minute == minute and today[0].hour in (2, 3), fired

    def test_a_label_in_the_repeated_hour_fires_once_and_the_check_says_so(
        self, cron_schedule, monkeypatch, settings
    ):
        from django_ox.compat import default_task_backend

        settings.TIME_ZONE = NEW_YORK
        settings.USE_TZ = False
        start = NEW_YORK_FALL_BACK + timedelta(hours=4)
        worker = cron_schedule(
            "30 1 * * *", self.naive_new_york(start - timedelta(days=1))
        )
        step(
            worker,
            monkeypatch,
            start - timedelta(days=1),
            start - timedelta(hours=20),
            to_local=self.naive_new_york,
        )
        fired, _ = step(
            worker,
            monkeypatch,
            start,
            start + timedelta(hours=5),
            to_local=self.naive_new_york,
        )
        # The documented loss: one label for two instants, so the second
        # pass reads as already recorded and the schedule fires once.
        assert [f for f in fired if f >= dt.datetime(2025, 11, 2)] == [
            dt.datetime(2025, 11, 2, 1, 30)
        ], fired
        assert "django_ox.W001" in [e.id for e in default_task_backend.check()]
