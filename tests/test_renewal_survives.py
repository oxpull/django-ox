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
"""

import threading
import time

import pytest
from django.db import DatabaseError, Error, InterfaceError, OperationalError

from django_ox.worker import Worker

pytestmark = pytest.mark.django_db


def test_the_exception_that_prompted_this_is_not_a_database_error():
    # If this ever becomes false upstream, the reasoning below changes.
    assert not issubclass(InterfaceError, DatabaseError)
    assert issubclass(InterfaceError, Error)


@pytest.fixture
def worker(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, renew_interval=0.01)


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
