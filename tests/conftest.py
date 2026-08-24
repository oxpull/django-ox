import contextlib
import os
import shutil
import signal
import subprocess
import sys
import threading
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


_worker_threads: list[tuple[Worker, threading.Thread]] = []


def start_worker_thread(worker):
    """
    Run worker.run() on a daemon thread that cannot outlive its test.

    Tests stop their workers themselves; the registry behind this is the
    backstop for every other exit: a body that raises before its finally,
    an assertion inside the try, a join too short for the machine.
    """
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    _worker_threads.append((worker, thread))
    return thread


@pytest.fixture(autouse=True)
def no_worker_outlives_its_test():
    """
    Stop and join every worker the test started, however the test exited.

    run() stops the renewer and closes the pool on its way out, so joining
    the run thread joins them too; the watchdog idles out on its own. A
    worker that will not stop when asked has wedged, and silence here
    would hand its threads to whichever test runs next.
    """
    yield
    wedged = []
    while _worker_threads:
        worker, thread = _worker_threads.pop()
        worker.request_stop()
        thread.join(timeout=60)
        if thread.is_alive():
            wedged.append(worker.worker_id)
    if wedged:
        pytest.fail(f"worker(s) did not stop when asked: {wedged}", pytrace=False)


_session_exitstatus = 0


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    global _session_exitstatus
    _session_exitstatus = int(exitstatus)


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config):
    """
    A pool thread wedged outside Python (a driver blocked on its socket,
    say) is non-daemon, and the interpreter would wait on it forever
    after the summary prints, stranding a CI job at its time cap. Give
    lingering pool threads a bounded wait, then leave with the verdict
    pytest already reached. Runs at unconfigure so the terminal summary
    is on the record first.
    """

    def lingering():
        return [
            t
            for t in threading.enumerate()
            if not t.daemon
            and t is not threading.main_thread()
            and t.name.startswith("ox")
        ]

    deadline = time.monotonic() + 30
    while lingering() and time.monotonic() < deadline:
        time.sleep(0.2)
    left = lingering()
    if left:
        sys.stderr.write(
            f"ox thread(s) still running at session end: "
            f"{sorted(t.name for t in left)}; exiting without waiting for them\n"
        )
        # os._exit skips buffer flushing, and losing the terminal summary
        # would hide the verdict from the CI log.
        sys.stderr.flush()
        sys.stdout.flush()
        os._exit(_session_exitstatus or 1)


@pytest.fixture(autouse=True)
def task_state():
    tasks.STATE.clear()
    yield tasks.STATE
    tasks.STATE.clear()


@pytest.fixture
def worker():
    """A worker with no backoff delay, suitable for inline run_once() tests."""
    return Worker(backoff_initial=0, poll_interval=0.05)
