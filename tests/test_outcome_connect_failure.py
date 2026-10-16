"""
The outcome write of a finished attempt when the worker cannot open a
connection for it: a host out of ephemeral ports, a database at its
connection limit. The error is raised at connect time, so no connection was
ever lost and none is open.
"""

import logging
from threading import Event, Thread

import pytest
from django.db import OperationalError, connection, connections
from django.db.backends.base.base import BaseDatabaseWrapper

import django_ox.worker as worker_module
from django_ox.models import OxTask
from django_ox.worker import (
    OUTCOME_CONNECT_RETRY_DELAYS,
    Worker,
    _outcome_connect_delays,
)

from .tasks import echo, fail_always

pytestmark = pytest.mark.django_db(transaction=True)

CANNOT_ASSIGN = (
    "connection is bad: connection to server at 127.0.0.1, port 5432 failed: "
    "Can't assign requested address"
)


class Pauses(list):
    """Stands in for the worker's sleep: records, never waits."""

    stops: list

    def __init__(self):
        super().__init__()
        self.stops = []

    def __call__(self, seconds, stop=None):
        self.append(seconds)
        self.stops.append(stop)


@pytest.fixture
def pauses(monkeypatch):
    seen = Pauses()
    monkeypatch.setattr(worker_module, "_pause", seen, raising=False)
    return seen


@pytest.fixture
def connect_fails(monkeypatch):
    """
    Once the task body has finished, the worker's connection is gone and the
    next `failures` connects raise as psycopg does when no port can be
    assigned (None fails them all). `connects` counts the attempts.
    """
    state = {"failures": 0, "connects": 0, "armed": False}
    real_discard = Worker._discard_unusable_connections
    real_connect = BaseDatabaseWrapper.connect

    def dropped(self):
        real_discard(self)
        connections[self._db_alias].close()
        state["armed"] = True

    def connect(self):
        if state["armed"]:
            state["connects"] += 1
            limit = state["failures"]
            if limit is None or state["connects"] <= limit:
                raise OperationalError(CANNOT_ASSIGN)
        return real_connect(self)

    monkeypatch.setattr(Worker, "_discard_unusable_connections", dropped)
    monkeypatch.setattr(BaseDatabaseWrapper, "connect", connect)
    yield state
    state["armed"] = False
    connection.close()


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
def test_a_connect_refused_twice_then_allowed_records_the_outcome_once(
    caplog, connect_fails, pauses, task
):
    connect_fails["failures"] = 3
    worker = Worker(backoff_initial=900, backoff_max=900, poll_interval=0.05)
    result = task.enqueue(*(("x",) if task is echo else ()))

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    connect_fails["armed"] = False
    stored = OxTask.objects.get(id=result.id)
    if task is echo:
        assert (stored.status, stored.return_value) == (
            OxTask.Status.SUCCESSFUL,
            "x",
        )
    else:
        assert stored.status == OxTask.Status.READY
        assert len(stored.errors) == 1
    assert (stored.attempts, stored.lease_epoch) == (1, 1)
    assert connect_fails["connects"] == 4
    assert pauses == [0.05, 0.1, 0.2]
    assert len(events(caplog, "task_outcome_reconnected")) == 1
    assert events(caplog, "task_outcome_unrecorded") == []
    assert events(caplog, "task_lease_lost") == []


def test_a_connect_that_stays_refused_ends_bounded_and_logs_once(
    caplog, connect_fails, pauses
):
    connect_fails["failures"] = None
    worker = Worker(backoff_initial=0, poll_interval=0.05)
    result = echo.enqueue("x")

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    connect_fails["armed"] = False
    stored = OxTask.objects.get(id=result.id)
    assert stored.status == OxTask.Status.RUNNING
    assert connect_fails["connects"] == len(OUTCOME_CONNECT_RETRY_DELAYS) + 1
    assert pauses == list(OUTCOME_CONNECT_RETRY_DELAYS)
    (line,) = events(caplog, "task_outcome_unrecorded")
    assert line.exc_info is not None
    assert events(caplog, "task_outcome_reconnected") == []


@pytest.mark.parametrize("lock_timeout", [0.0, 0.2, 1.0, 5.0, 30.0, 300.0])
def test_the_waits_never_pass_a_tenth_of_the_lease(lock_timeout):
    delays = _outcome_connect_delays(lock_timeout)
    assert sum(delays) <= lock_timeout / 10 + 1e-9
    assert all(0 < d <= max(OUTCOME_CONNECT_RETRY_DELAYS) for d in delays)
    assert sum(delays) <= sum(OUTCOME_CONNECT_RETRY_DELAYS)


def test_the_default_lease_allows_the_whole_schedule():
    assert _outcome_connect_delays(300.0) == list(OUTCOME_CONNECT_RETRY_DELAYS)
    assert sum(OUTCOME_CONNECT_RETRY_DELAYS) < 4


def test_a_short_lease_shortens_the_waits_the_worker_makes(
    caplog, connect_fails, pauses
):
    connect_fails["failures"] = None
    worker = Worker(backoff_initial=0, poll_interval=0.05, lock_timeout=1.0)
    echo.enqueue("x")

    assert worker.run_once()

    connect_fails["armed"] = False
    assert sum(pauses) <= 0.1 + 1e-9
    assert pauses == [0.05, 0.05]


def test_a_write_that_loses_a_connection_does_not_wait(monkeypatch, pauses):
    """The dead connection is open and fails its probe: tried again at once."""
    real = Worker._write_outcome
    wrapper = type(connections["default"])
    real_usable = wrapper.is_usable
    calls = []

    def dies_once(self, db_task, **fields):
        calls.append(1)
        if len(calls) == 1:
            monkeypatch.setattr(wrapper, "is_usable", lambda self: False)
            connections["default"].ensure_connection()
            raise OperationalError("server closed the connection unexpectedly")
        monkeypatch.setattr(wrapper, "is_usable", real_usable)
        return real(self, db_task, **fields)

    monkeypatch.setattr(Worker, "_write_outcome", dies_once)
    result = echo.enqueue("x")
    worker = Worker(backoff_initial=0, poll_interval=0.05)

    assert worker.run_once()

    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
    assert pauses == []


def test_a_pause_ends_when_the_worker_is_told_to_stop():
    """A shutdown never sits out the schedule: the wait is on the stop event."""
    stop = Event()
    waiter = Thread(target=worker_module._pause, args=(60.0, stop))
    waiter.start()
    stop.set()
    waiter.join(timeout=10)
    assert not waiter.is_alive()


def test_the_write_waits_on_the_workers_own_stop_event(connect_fails, pauses):
    connect_fails["failures"] = 2
    result = echo.enqueue("x")
    worker = Worker(backoff_initial=0, poll_interval=0.05)

    assert worker.run_once()

    connect_fails["armed"] = False
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
    assert pauses
    assert all(stop is worker._stop for stop in pauses.stops)


def test_a_lost_connection_the_driver_does_not_flag_is_not_a_refused_connect(
    monkeypatch, caplog, pauses
):
    """
    A connection that was open when the write began and died under it is not
    a refused connect, whether or not the driver reports it lost: a second try
    that fails too is left to the reaper, with no wait, as before.
    """
    monkeypatch.setattr(worker_module, "_lost", lambda raw: False)
    wrapper = type(connections["default"])
    calls = []

    def fails_twice(self, db_task, **fields):
        calls.append(1)
        if len(calls) == 1:
            monkeypatch.setattr(wrapper, "is_usable", lambda self: False)
            connections["default"].ensure_connection()
        else:
            # The server drops the session under the second write too, and
            # nothing is left open afterwards.
            connections["default"].close()
        raise OperationalError("Lost connection to the server during query")

    monkeypatch.setattr(Worker, "_write_outcome", fails_twice)
    result = echo.enqueue("x")
    worker = Worker(backoff_initial=0, poll_interval=0.05)

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    assert len(calls) == 2
    assert pauses == []
    assert len(events(caplog, "task_outcome_unrecorded")) == 1
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
