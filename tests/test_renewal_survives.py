"""
The renewal thread outlives whatever the database does to it.

Renewal is the whole of the worker's side of the lease. If the thread stops,
`locked_at` freezes on every in-flight row, the reaper hands each of them to
another worker after the lock timeout, and every one of those tasks runs a
second time while the first attempt is still going. Nothing restarts this
thread and nothing checks that it is alive, so the failure is silent and
permanent.

`django.db.InterfaceError` is the case that matters: a connection dropped
underneath the thread is the most likely thing to arrive here, and it does
not inherit from `DatabaseError`. It sits beside it under `django.db.Error`.

Each case runs on the default database and on a pooled PostgreSQL one,
where renewal takes a path of its own, with nothing in flight.
"""

import threading
import time

import pytest
from django.db import DatabaseError, Error, InterfaceError, OperationalError

from django_ox.worker import Worker

from .conftest import IDLE_POOLED_ALIAS

pytestmark = pytest.mark.django_db


def test_interface_error_is_not_a_database_error():
    # If this ever becomes false upstream, the reasoning below changes.
    assert not issubclass(InterfaceError, DatabaseError)
    assert issubclass(InterfaceError, Error)


@pytest.fixture(params=["default", "pool"])
def worker(settings, request):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    alias = None
    if request.param == "pool":
        alias = request.getfixturevalue("idle_pooled_alias")
    return Worker(backoff_initial=0, renew_interval=0.01, db_alias=alias)


@pytest.mark.parametrize(
    "failure",
    [
        InterfaceError("connection already closed"),
        OperationalError("server closed the connection unexpectedly"),
        DatabaseError("deadlock"),
        RuntimeError("something nobody predicted"),
    ],
    ids=["interface", "operational", "database", "unpredicted"],
)
def test_the_thread_survives(worker, monkeypatch, failure):
    calls = []

    def raise_once():
        calls.append(1)
        if len(calls) == 1:
            raise failure
        return 0

    monkeypatch.setattr(worker, "renew_leases", raise_once)
    pooled_ticks = []
    renew_on = worker._renew_on

    def through_the_pool(*args):
        pooled_ticks.append(1)
        return renew_on(*args)

    monkeypatch.setattr(worker, "_renew_on", through_the_pool)
    stop = threading.Event()
    thread = threading.Thread(target=worker._renewal_loop, args=(stop,), daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 3
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert thread.is_alive(), (
            f"the renewal thread died on {type(failure).__name__}; every "
            "in-flight lease would age out and every running task would be "
            "handed to another worker"
        )
        assert len(calls) >= 3, "renewal did not resume after the failure"
    finally:
        stop.set()
        thread.join(timeout=2)
    if worker._db_alias == IDLE_POOLED_ALIAS:
        assert len(pooled_ticks) >= 3
