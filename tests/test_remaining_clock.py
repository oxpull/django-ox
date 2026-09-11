"""
`remaining()` and the watchdog must not disagree about the same instant.

The watchdog fires on `time.monotonic()`. `remaining()` used to answer from the
wall clock, so for as long as an NTP correction lasts the two sat on different
sides of the deadline: a backwards step left a task confident it had seconds in
hand after the timeout had already fired, and a forwards step made a
well-behaved cooperative task give up early.
"""

import time

import pytest
from django.utils import timezone

from django_ox.timeouts import _deadline, _deadline_monotonic, deadline, remaining

pytestmark = pytest.mark.django_db


class _WallClockJumped:
    """`timezone.now` as a clock that has just been stepped."""

    def __init__(self, monkeypatch, by_seconds):
        real = timezone.now
        monkeypatch.setattr(
            timezone, "now", lambda: real() + timezone.timedelta(seconds=by_seconds)
        )


class TestRemainingIsMeasuredOnTheEnforcersClock:
    def test_a_backwards_step_does_not_invent_time(self, monkeypatch):
        # Deadline one second out, on both clocks.
        _deadline.set(timezone.now() + timezone.timedelta(seconds=1))
        _deadline_monotonic.set(time.monotonic() + 1)
        # NTP puts the wall clock back an hour. The watchdog is unaffected.
        _WallClockJumped(monkeypatch, -3600)
        assert remaining() < 2, (
            "the task was told it has an hour left; the watchdog will fire in "
            "one second"
        )

    def test_a_forwards_step_does_not_end_the_task_early(self, monkeypatch):
        _deadline.set(timezone.now() + timezone.timedelta(seconds=60))
        _deadline_monotonic.set(time.monotonic() + 60)
        _WallClockJumped(monkeypatch, 3600)
        assert remaining() > 0, (
            "the task was told its deadline had passed while the watchdog "
            "still had 60 seconds on it"
        )

    def test_deadline_stays_a_wall_clock_answer(self):
        at = timezone.now() + timezone.timedelta(seconds=30)
        _deadline.set(at)
        _deadline_monotonic.set(time.monotonic() + 30)
        assert deadline() == at, "deadline() answers when, and when is wall clock"

    def test_no_limit_still_reads_as_no_limit(self):
        _deadline.set(None)
        _deadline_monotonic.set(None)
        assert remaining() is None
        assert deadline() is None
