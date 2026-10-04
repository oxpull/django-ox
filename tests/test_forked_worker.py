"""
A Worker built before a fork and used in both processes afterwards.

A web server that imports the application and then forks its workers, with
a module-level Worker that views call run_once() on, has one; so does a
launcher that builds a Worker and forks it into several processes. Each
copy used to claim under the same worker id, with bookkeeping of its own,
and a look for a claim that raised in one process took the row the other
was executing for its own: it released it, another claim ran the body a
second time, and the first execution's outcome was fenced out.

Two forks. os.fork() runs the at-fork hooks, as gunicorn's fork does. libc's
fork() called through ctypes, with the interpreter lock held, runs none, as
uWSGI forks its workers unless told to call them. tests/lost_reply.py loses
the reply to the parent's claim.
"""

import collections
import ctypes
import gc
import json
import os
import signal
import threading
import time
import traceback
import uuid
import weakref
from contextlib import suppress

import pytest
from django.db import (
    DatabaseError,
    close_old_connections,
    connection,
    connections,
)

from django_ox import worker as worker_module
from django_ox.models import OxTask
from django_ox.testing import run_tasks
from django_ox.worker import Worker, _pool_options

from . import tasks
from .conftest import start_worker_thread, wait_for
from .lost_reply import Seams

pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork() is POSIX only"),
    # Python 3.12 warns when a process that has threads forks. The tests
    # that hold locks at the fork have one on purpose, and a thread an
    # earlier test left behind would make any of them warn.
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]

LIMIT = 60.0

#: The body the child is inside while the parent claims.
LONG_BODY = 4.0

#: A call in the child on an empty queue takes a moment; one waiting on a
#: lock copied held never returns.
CHILD_CALL_LIMIT = 20.0

#: A reply each database can lose after the claim has committed.
WINDOW = {"postgresql": "statement", "mysql": "commit", "sqlite": "statement"}

#: os.fork() runs the at-fork hooks; libc's fork() runs none, as uWSGI forks
#: by default. A child forked by libc must not start a thread: CPython keeps
#: the other threads' states in it, and a new thread there is undefined
#: behaviour of the interpreter itself (a child running run() crashed on
#: Python 3.14 in CI). Where the child starts threads, "unhooked" forks with
#: os.fork() and every Worker out of the registry, so the interpreter's own
#: hooks run and django-ox's finds nothing: only the pid check can make the
#: Worker the child's own, as after libc's fork.
FORKS = ["os.fork", "libc"]
THREADED_FORKS = ["os.fork", "unhooked"]


@pytest.fixture(autouse=True)
def no_dead_connection_outlives_the_test():
    yield
    for conn in connections.all(initialized_only=True):
        if (
            conn.connection is not None
            and not conn.in_atomic_block
            and not conn.is_usable()
        ):
            conn.close()


@pytest.fixture
def lose_the_reply():
    seams = Seams()
    yield seams.arm
    seams.remove_all()


def _budget(settings, attempts):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"MAX_ATTEMPTS": attempts},
        }
    }


def _ready_to_fork():
    """
    Nothing of the parent's connections for the child to share: each
    process opens its own. Django's pool is per process too; the parent
    builds a new one on its next statement.
    """
    connections.close_all()
    if _pool_options("default") is not None and connection.vendor == "postgresql":
        connection.close_pool()


class Child:
    """
    target(*args) in a child forked `how`; the child exits 0 when it
    returns and 1 when it raises, without running anything of the parent's
    on the way out.
    """

    def __init__(self, how, target, *args):
        _ready_to_fork()
        if how == "libc":
            fork = ctypes.PyDLL(None).fork
            fork.restype = ctypes.c_int
            fork.argtypes = []
            pid = fork()
        elif how == "unhooked":
            registered = list(worker_module._live_workers)
            for worker in registered:
                worker_module._live_workers.discard(worker)
            pid = os.fork()
            if pid:
                for worker in registered:
                    worker_module._live_workers.add(worker)
        else:
            pid = os.fork()
        if pid == 0:
            code = 1
            try:
                target(*args)
                code = 0
            except BaseException:
                traceback.print_exc()
            finally:
                with suppress(BaseException):
                    connections.close_all()
                os._exit(code)
        self.pid = pid
        self.exitcode = None

    def join(self, timeout):
        """The exit code, or None if the child is still running at `timeout`."""
        deadline = time.monotonic() + timeout
        while self.exitcode is None:
            done, status = os.waitpid(self.pid, os.WNOHANG)
            if done:
                self.exitcode = os.waitstatus_to_exitcode(status)
            elif time.monotonic() >= deadline:
                return None
            else:
                time.sleep(0.02)
        return self.exitcode

    def kill(self):
        if self.exitcode is None:
            with suppress(ProcessLookupError):
                os.kill(self.pid, signal.SIGKILL)
            self.join(LIMIT)


def _runs(path):
    counts = collections.Counter()
    if path.exists():
        for line in path.read_text().splitlines():
            counts[line.split()[0]] += 1
    return counts


def _row(result):
    return OxTask.objects.get(id=result.id)


def _drain():
    """Run whatever is left with a Worker of the parent's own."""
    other = Worker(lock_timeout=300.0, backoff_initial=0)
    deadline = time.monotonic() + LIMIT
    while time.monotonic() < deadline:
        try:
            if not other.run_once():
                return
        except DatabaseError:
            close_old_connections()


def _released(caplog):
    return [
        r.task_id
        for r in caplog.records
        if getattr(r, "event", None) == "worker_claim_released"
    ]


# -- a look in one process never takes the other's row -------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", THREADED_FORKS)
@pytest.mark.parametrize("path", ["loop", "pending", "stop"])
def test_forked_worker_does_not_recover_other_process_claim(
    path, how, tmp_path, settings, caplog, lose_the_reply
):
    """
    The child claims the long task L with run_once() and is inside its
    body. The parent, with its copy of the same Worker, then runs its loop,
    which looks for a claim that raised:

    - loop: the loop's claim of S commits and loses its reply, and the next
      pass looks;
    - pending: a look is pending from an earlier claim that raised, and the
      first pass looks before claiming;
    - stop: the loop's claim of S loses its reply and the worker is asked to
      stop, so the look is the one it makes on the way out.

    L's body runs once, its row is never released, refunded or moved on,
    and the child's outcome is the one recorded.
    """
    caplog.set_level("WARNING", logger="django_ox")
    _budget(settings, 3)
    runs = tmp_path / "runs"
    long = tasks.append_line.using(priority=10).enqueue(str(runs), "L", LONG_BODY)
    short = tasks.append_line.enqueue(str(runs), "S")
    worker = Worker(
        lock_timeout=300.0,
        poll_interval=30.0 if path == "stop" else 0.05,
        backoff_initial=0,
    )
    parent_id = worker.worker_id
    child = Child(how, worker.run_once)
    try:
        assert wait_for(lambda: _runs(runs)["L"] == 1, timeout=LIMIT), (
            "the child never started L"
        )
        if path == "pending":
            worker._claim_unconfirmed_at = time.monotonic()
            seam = None
        else:
            also = (lambda other: worker.request_stop()) if path == "stop" else None
            seam = lose_the_reply(WINDOW[connection.vendor], also=also)
        thread = start_worker_thread(worker)
        try:
            if path == "stop":
                thread.join(timeout=LIMIT)
            else:
                assert wait_for(
                    lambda: _row(short).status == OxTask.Status.SUCCESSFUL,
                    timeout=LIMIT,
                )
        finally:
            worker.request_stop()
            thread.join(timeout=LIMIT)
        assert not thread.is_alive()
        if seam is not None:
            assert seam.fired, "the parent's claim never lost its reply"
        during = _row(long)
    finally:
        if child.join(LIMIT) is None:
            child.kill()
    assert child.exitcode == 0

    _drain()
    long_row, short_row = _row(long), _row(short)
    counts = _runs(runs)
    assert counts["L"] == 1, (
        f"L's body ran {counts['L']} times; after the parent's look L was "
        f"{(during.status, during.attempts, during.lease_epoch)}, released "
        f"{_released(caplog)}"
    )
    assert counts["S"] == 1
    assert str(long.id) not in _released(caplog)
    assert (long_row.status, long_row.attempts, long_row.lease_epoch) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    )
    assert long_row.worker_ids != [parent_id]
    assert short_row.status == OxTask.Status.SUCCESSFUL
    assert worker.worker_id == parent_id


# -- what the child starts with ---------------------------------------------------


def _report_state(worker, report):
    """In the child: one call, which a Worker makes its own before claiming."""
    worker.run_once()
    worker._beat()
    heartbeat = worker._heartbeat
    report.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "worker_id": worker.worker_id,
                "sets": [
                    sorted(worker._in_flight),
                    sorted(worker._in_callers_atomic_block),
                    sorted(worker._claimed_in_callers_atomic_block.values()),
                    sorted(worker._handed_off),
                    sorted(worker._unsettled),
                ],
                "pending": worker._claim_unconfirmed_at,
                "claims_in_flight": worker._claims_in_flight,
                "watches": len(worker._watches),
                "stuck": len(worker._stuck),
                "running_on": len(worker._running_on),
                "heartbeat_path": heartbeat and heartbeat.path,
                "heartbeat_owner": heartbeat and heartbeat.owner,
                "dispatch_report_id": worker._dispatch_report.worker_id,
                "stopping": worker.stopping,
            },
            default=str,
        )
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", FORKS)
def test_child_reinitializes_inherited_worker_state(how, tmp_path):
    """
    The child's copy is a worker of its own before its first claim: a new
    id that keeps the slot suffix, nothing in its bookkeeping, no look
    pending and no timeout watches. It keeps the heartbeat file and updates
    it, under its own id. The parent is untouched.
    """
    beat = tmp_path / "heartbeat"
    report = tmp_path / "report.json"
    worker = Worker(lock_timeout=300.0, worker_index=3, heartbeat_file=str(beat))
    parent_id = worker.worker_id
    worker._beat()
    past = time.time() - 3600
    os.utime(beat, (past, past))
    before = beat.stat().st_mtime_ns
    heartbeat = worker._heartbeat
    running, handed_off, unsettled = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    worker._in_flight.add((running, 1))
    worker._in_callers_atomic_block.add((running, 1))
    # A call of the parent's that is handing a claim to execute().
    call = object()
    worker._claimed_in_callers_atomic_block[call] = (1, handed_off, 1)
    worker._handed_off.add((handed_off, 1))
    worker._unsettled.add((unsettled, 1))
    worker._claim_unconfirmed_at = pending = time.monotonic()
    # Claims the parent's threads are making as it forks.
    worker._claims_in_flight = 2
    worker._watches[1] = "a watch"
    worker._stuck[1] = (running, 1)
    worker._running_on[1] = (running, 1)
    worker._claimed = 5

    child = Child(how, _report_state, worker, report)
    if child.join(LIMIT) is None:
        child.kill()
    assert child.exitcode == 0
    state = json.loads(report.read_text())

    assert state["worker_id"] != parent_id
    assert state["worker_id"].endswith("-3")
    assert f"-{state['pid']}-" in state["worker_id"]
    assert state["sets"] == [[], [], [], [], []]
    assert state["pending"] is None
    assert state["claims_in_flight"] == 0
    assert (state["watches"], state["stuck"], state["running_on"]) == (0, 0, 0)
    assert state["heartbeat_path"] == str(beat)
    assert state["heartbeat_owner"] == f"Worker {state['worker_id']}"
    assert state["dispatch_report_id"] == state["worker_id"]
    assert state["stopping"] is False
    assert beat.stat().st_mtime_ns != before, "the child did not update the file"

    assert worker.worker_id == parent_id
    assert worker._in_flight == {(running, 1)}
    assert worker._in_callers_atomic_block == {(running, 1)}
    assert worker._claimed_in_callers_atomic_block == {call: (1, handed_off, 1)}
    assert worker._handed_off == {(handed_off, 1)}
    assert worker._unsettled == {(unsettled, 1)}
    assert worker._claim_unconfirmed_at == pending
    assert worker._claims_in_flight == 2
    assert worker._watches == {1: "a watch"}
    assert worker._claimed == 5
    assert worker._heartbeat is heartbeat
    assert heartbeat.owner == f"Worker {parent_id}"
    assert worker._dispatch_report.worker_id == parent_id


def _run_stopped(worker):
    worker.request_stop()
    return worker.run()


#: The first call the child makes, each of which makes the Worker its own
#: before it touches a lock.
ENTRIES = {
    "run_once": lambda worker: worker.run_once(),
    "run": _run_stopped,
    "claim_one": lambda worker: worker.claim_one(),
    "look": lambda worker: worker._recover_claims(),
}


def _held_locks(worker):
    return {
        "fence": worker._fence,
        "in_flight": worker._in_flight_lock,
        "watch": worker._watch_lock,
        "backstop_only": worker._backstop_only_lock,
    }


def _call_then_try_the_locks(worker, entry, report):
    try:
        outcome = ["returned", repr(ENTRIES[entry](worker))]
    except BaseException as exc:
        outcome = ["raised", repr(exc)]
    acquired = {}
    for name, lock in _held_locks(worker).items():
        acquired[name] = lock.acquire(timeout=1.0)
        if acquired[name]:
            lock.release()
    acquired["watch_cv"] = worker._watch_cv.acquire(timeout=1.0)
    if acquired["watch_cv"]:
        worker._watch_cv.notify_all()
        worker._watch_cv.release()
    report.write_text(
        json.dumps(
            {"outcome": outcome, "acquired": acquired, "worker_id": worker.worker_id}
        )
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    ("how", "entry"),
    [
        (how, entry)
        for how in [*FORKS, "unhooked"]
        for entry in ENTRIES
        if not (how == "libc" and entry == "run")
    ],
)
def test_child_replaces_locks_held_at_fork(how, entry, tmp_path):
    """
    Another thread of the parent holds every one of the Worker's locks at
    the moment the process forks, and a look is pending. A lock is copied
    held, with no thread in the child to release it. Whichever call the
    child makes first, a claim, the loop or a look, it neither waits on the
    parent's thread nor makes the parent's look, and the Worker's locks are
    the child's own afterwards. The parent's locks are still the parent's.
    """
    report = tmp_path / "report.json"
    worker = Worker(lock_timeout=300.0, backoff_initial=0)
    parent_id = worker.worker_id
    worker._claim_unconfirmed_at = time.monotonic()
    locks = _held_locks(worker)
    holding = threading.Event()
    release = threading.Event()

    def hold():
        for lock in locks.values():
            lock.acquire()
        holding.set()
        release.wait(LIMIT)
        for lock in locks.values():
            lock.release()

    holder = threading.Thread(target=hold)
    holder.start()
    child = None
    try:
        assert holding.wait(LIMIT)
        child = Child(how, _call_then_try_the_locks, worker, entry, report)
        returned = child.join(CHILD_CALL_LIMIT)
        for name, lock in locks.items():
            assert not lock.acquire(blocking=False), f"the parent's {name} lock"
    finally:
        release.set()
        holder.join(LIMIT)
        if child is not None and child.join(10) is None:
            child.kill()

    assert returned == 0, f"the child's {entry} was still waiting or failed"
    state = json.loads(report.read_text())
    assert state["outcome"][0] == "returned", state["outcome"]
    assert state["acquired"] == dict.fromkeys([*locks, "watch_cv"], True)
    assert state["worker_id"] != parent_id
    assert worker.worker_id == parent_id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", THREADED_FORKS)
def test_a_forked_child_running_run_updates_the_heartbeat(how, tmp_path):
    """
    A launcher builds a Worker with a heartbeat file and forks a child that
    runs its loop. The child updates that file, and claims under an id of
    its own that keeps the slot suffix, whichever fork made it.
    """
    beat = tmp_path / "heartbeat"
    runs = tmp_path / "runs"
    result = tasks.append_line.enqueue(str(runs), "C")
    worker = Worker(
        lock_timeout=300.0,
        poll_interval=0.05,
        worker_index=2,
        heartbeat_file=str(beat),
        max_tasks=1,
    )
    parent_id = worker.worker_id

    child = Child(how, worker.run)
    if child.join(LIMIT) is None:
        child.kill()

    assert child.exitcode == 0
    assert beat.exists(), "nothing wrote the heartbeat file"
    row = _row(result)
    assert row.status == OxTask.Status.SUCCESSFUL
    (child_id,) = row.worker_ids
    assert child_id != parent_id
    assert child_id.endswith("-2")
    assert f"-{child.pid}-" in child_id
    assert _runs(runs)["C"] == 1
    assert worker.worker_id == parent_id


# -- the registry ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_worker_registry_does_not_retain_workers():
    """
    Every Worker is registered for the fork hook, and none is kept alive by
    it: run_tasks() builds a Worker per call, and a long-lived process that
    calls it would otherwise hold every one it ever built.
    """
    from django_ox import worker as worker_module

    registry = worker_module._live_workers
    worker = Worker(lock_timeout=300.0)
    assert worker in registry
    gone = weakref.ref(worker)
    del worker
    gc.collect()
    assert gone() is None
    assert all(w is not None for w in registry)

    gc.collect()
    before = len(registry)
    for _ in range(5):
        assert run_tasks() == []
    gc.collect()
    assert len(registry) <= before
