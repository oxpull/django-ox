"""
When lease renewal runs: every renew_interval, timed from the start of one
renewal to the start of the next, whichever path a renewal takes.

A renewal can take as long as its connection deadline, and below a 15 s
lease that deadline is the interval itself. Timed from the end of a renewal,
one that spent its deadline would put the next an interval after that, past
a lease three intervals long, however soon the stall behind it ended.

The loop runs here on a clock the test moves. Each renewal moves it by what
that renewal is scripted to cost, and the stop event moves it by whatever
the loop asks to wait instead of waiting, so what is asserted is the
schedule itself, not how fast the machine is.
"""

import importlib.util
import threading
import time
import uuid

import pytest
from django.db import OperationalError

import django_ox.worker as worker_module
from django_ox.worker import Worker

INTERVAL = 2.0
LEASE = 6.0
# What each of three renewals costs on the clock: less than the interval,
# more than it (a stalled connect that spends the whole 2 s deadline, then
# asks the pool), and less again.
COSTS = [0.5, 2.1, 0.3]
# The waits the loop asks for: the first interval; what is left of the
# interval after the first renewal; nothing after the one that overran; and
# what is left of the next interval, during which the stop is set.
WAITS = [2.0, 1.5, 0.0, 1.7]
# When each renewal starts. The one after the overrun starts as the overrun
# ends, and is the new anchor: nothing is made up for the time it lost.
STARTS = [2.0, 4.0, 6.1]

UNPOOLED = "alt"

needs_psycopg = pytest.mark.skipif(
    importlib.util.find_spec("psycopg") is None, reason="needs psycopg"
)


class Clock:
    """
    The time module as django_ox.worker sees it, with monotonic() reading a
    clock the test moves. Everything else is the real module.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def __getattr__(self, name):
        return getattr(time, name)


class Stop:
    """
    The renewal loop's stop event on the clock. wait() notes the timeout it
    is asked for and moves the clock that far instead of waiting. After
    `ticks` renewals it answers that the stop was set during the wait.
    """

    def __init__(self, clock: Clock, ticks: int) -> None:
        self.clock = clock
        self.ticks = ticks
        self.waits: list[float] = []

    def wait(self, timeout=None):
        self.waits.append(timeout)
        if len(self.waits) > self.ticks:
            return True
        self.clock.now += timeout
        return False


@pytest.fixture
def clock(monkeypatch):
    made = Clock()
    monkeypatch.setattr(worker_module, "time", made)
    return made


def spending(clock: Clock, starts: list[float]):
    """A call that notes when it started and moves the clock by the next cost."""
    costs = iter(COSTS)

    def spend():
        starts.append(clock.now)
        clock.now += next(costs)

    return spend


def without_a_pool(clock, starts, monkeypatch, request):
    worker = Worker(db_alias=UNPOOLED, lock_timeout=LEASE, renew_interval=INTERVAL)
    spend = spending(clock, starts)

    def renew():
        spend()
        return 1

    worker.renew_leases = renew
    return worker


def without_a_pool_failing(clock, starts, monkeypatch, request):
    worker = Worker(db_alias=UNPOOLED, lock_timeout=LEASE, renew_interval=INTERVAL)
    spend = spending(clock, starts)

    def renew():
        spend()
        raise OperationalError("server closed the connection unexpectedly")

    worker.renew_leases = renew
    return worker


def pooled_worker(request):
    alias = request.getfixturevalue("idle_pooled_alias")
    return Worker(db_alias=alias, lock_timeout=LEASE, renew_interval=INTERVAL)


def pooled_with_nothing_in_flight(clock, starts, monkeypatch, request):
    """renew_leases() overridden, as a subclass may, and taking the cost."""
    worker = pooled_worker(request)
    spend = spending(clock, starts)

    def renew():
        spend()
        return 0

    worker.renew_leases = renew
    return worker


def pooled_with_nothing_in_flight_failing(clock, starts, monkeypatch, request):
    worker = pooled_worker(request)
    spend = spending(clock, starts)

    def renew():
        spend()
        raise RuntimeError("an override that fails")

    worker.renew_leases = renew
    return worker


def busy_worker(request):
    worker = pooled_worker(request)
    worker._in_flight.add((uuid.uuid4(), 1))
    worker.renew_leases = lambda: 1
    return worker


def pooled_on_its_own_connection(clock, starts, monkeypatch, request):
    """Its own connection opens, taking the cost, and the renewal runs on it."""
    worker = busy_worker(request)
    spend = spending(clock, starts)
    monkeypatch.setattr(
        worker_module._OwnConnection, "open", lambda own, deadline: spend()
    )
    return worker


def pooled_stalled_with_none_to_spare(clock, starts, monkeypatch, request):
    """
    New connections stall until the deadline and the pool has none to
    spare, so every renewal is missed: the reported case.
    """
    worker = busy_worker(request)
    spend = spending(clock, starts)

    def stalled(own, deadline):
        spend()
        raise OperationalError("connection timeout expired")

    def empty(own, wait):
        raise OperationalError("couldn't get a connection after 0.10 sec")

    monkeypatch.setattr(worker_module._OwnConnection, "open", stalled)
    monkeypatch.setattr(worker_module._OwnConnection, "borrowed", empty)
    return worker


PATHS = [
    pytest.param(without_a_pool, id="no-pool"),
    pytest.param(without_a_pool_failing, id="no-pool-failing"),
    pytest.param(
        pooled_with_nothing_in_flight, id="pool-nothing-in-flight", marks=needs_psycopg
    ),
    pytest.param(
        pooled_with_nothing_in_flight_failing,
        id="pool-nothing-in-flight-failing",
        marks=needs_psycopg,
    ),
    pytest.param(
        pooled_on_its_own_connection, id="pool-own-connection", marks=needs_psycopg
    ),
    pytest.param(
        pooled_stalled_with_none_to_spare,
        id="pool-stalled-none-to-spare",
        marks=needs_psycopg,
    ),
]


def run_loop(worker, stop):
    # On a thread of its own, as in the worker: the loop closes every
    # connection of the thread it runs on when it ends.
    thread = threading.Thread(target=worker._renewal_loop, args=(stop,), daemon=True)
    thread.start()
    thread.join(timeout=10)
    assert not thread.is_alive(), "the renewal loop did not stop"


@pytest.mark.parametrize("path", PATHS)
def test_a_renewal_s_own_length_is_not_added_to_the_interval(
    path, clock, monkeypatch, request
):
    starts: list[float] = []
    worker = path(clock, starts, monkeypatch, request)
    stop = Stop(clock, ticks=len(COSTS))
    run_loop(worker, stop)
    assert stop.waits == pytest.approx(WAITS)
    assert starts == pytest.approx(STARTS)


def test_a_tick_that_overruns_several_intervals_is_followed_by_one_not_the_missed_ones(
    clock,
):
    from django_ox.worker import _every

    starts: list[float] = []
    lengths = iter([5.0, 0.1, 0.1])

    def tick():
        starts.append(clock.now)
        clock.now += next(lengths)

    stop = Stop(clock, ticks=3)
    _every(INTERVAL, stop, tick)
    # The 5 s tick spans two more intervals; the loop does not make them up.
    assert starts == pytest.approx([2.0, 7.0, 9.0])
    assert stop.waits == pytest.approx([2.0, 0.0, 1.9, 1.9])


class WaitsOnce(threading.Event):
    """
    A real event whose first wait returns at once, as if the first interval
    had passed, so the loop renews once and then really waits for the rest
    of the next interval. Notes every timeout it is asked for.
    """

    def __init__(self) -> None:
        super().__init__()
        self.asked: list[float] = []
        self.waiting = threading.Event()

    def wait(self, timeout=None):
        self.asked.append(timeout)
        if len(self.asked) == 1:
            return False
        self.waiting.set()
        return super().wait(timeout)


@pytest.mark.parametrize("pooled", [False, True], ids=["no-pool", "pool"])
def test_the_stop_ends_the_wait_after_a_renewal_at_once(pooled, request, monkeypatch):
    alias = UNPOOLED
    if pooled:
        alias = request.getfixturevalue("idle_pooled_alias")
        monkeypatch.setattr(
            worker_module._OwnConnection, "open", lambda own, deadline: None
        )
    worker = Worker(db_alias=alias, lock_timeout=3 * 3600, renew_interval=3600)
    worker._in_flight.add((uuid.uuid4(), 1))
    renewed = []
    worker.renew_leases = lambda: renewed.append(1) or 1
    stop = WaitsOnce()
    thread = threading.Thread(target=worker._renewal_loop, args=(stop,), daemon=True)
    thread.start()
    assert stop.waiting.wait(timeout=10), "the loop never waited after renewing"
    stop.set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the stop did not end the wait"
    assert renewed == [1]
    assert 3590 < stop.asked[1] <= 3600


class Overriding(Worker):
    """A worker class whose renew_leases() does something of its own."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.renewed_at: list[float] = []

    def renew_leases(self) -> int:
        self.renewed_at.append(worker_module.time.monotonic())
        return 0


def test_an_idle_pooled_worker_calls_an_overriding_renew_leases_every_tick(
    clock, idle_pooled_alias
):
    """
    With nothing in flight on a pooled database the stock renew_leases()
    has nothing to do and opens nothing, but a subclass that overrides it
    is called once a tick, as it is without a pool.
    """
    worker = Overriding(
        db_alias=idle_pooled_alias, lock_timeout=LEASE, renew_interval=INTERVAL
    )
    run_loop(worker, Stop(clock, ticks=3))
    assert worker.renewed_at == pytest.approx([2.0, 4.0, 6.0])
