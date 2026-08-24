import contextlib
import os
import shutil
import signal
import subprocess
import time

import pytest

from django_ox.worker import Worker

from . import tasks


def _descends_from(pid, ancestor):
    seen = set()
    while pid > 1 and pid not in seen:
        seen.add(pid)
        out = subprocess.run(  # noqa: S603
            ["ps", "-o", "ppid=", "-p", str(pid)],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if not out.isdigit():
            return False
        pid = int(out)
        if pid == ancestor:
            return True
    return False


@pytest.fixture(scope="session", autouse=True)
def no_ox_worker_left_behind():
    """
    A worker process a test failed to stop outlives pytest and holds the
    CI job's output pipe open, which reads as a hung job until the runner
    times out. When the session ends, kill any such process and fail.

    On a developer machine only this session's descendants are touched; a
    child orphaned by a killed supervisor escapes that filter, so CI,
    where every ox_worker belongs to the job, sweeps by name.
    """
    yield
    if os.name != "posix" or shutil.which("pgrep") is None:
        return
    found = subprocess.run(
        ["pgrep", "-f", "ox_worker"],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    pids = [int(p) for p in found.stdout.split() if int(p) != os.getpid()]
    if not os.environ.get("CI"):
        pids = [pid for pid in pids if _descends_from(pid, os.getpid())]
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
    assert not pids, f"ox_worker process(es) left running: {pids}"


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
