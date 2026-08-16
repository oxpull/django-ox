import time

import pytest

from django_ox.worker import Worker

from . import tasks


def wait_for(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture(autouse=True)
def task_state():
    tasks.STATE.clear()
    yield tasks.STATE
    tasks.STATE.clear()


@pytest.fixture
def worker():
    """A worker with no backoff delay, suitable for inline run_once() tests."""
    return Worker(backoff_initial=0, poll_interval=0.05)
