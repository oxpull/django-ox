"""
The timeout watchdog's connection when the database is not pooled.

The watchdog records stuck attempts in batches, on its own thread, and that
thread lives as long as any attempt is under a timeout. Without Django's
PostgreSQL pool, a batch runs on the thread's ordinary connection. Each
batch closes it as the batch ends, however it ends, as a pooled batch closes
the connection it opened, so the next batch opens a fresh one rather than
reuse one the server may have ended in between.

That is the whole of it: a connection that breaks during a batch still
fails the records after the break, and each attempt is still recycled.
"""

import copy
import itertools
import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
    connection,
    connections,
)
from django.utils import timezone

from django_ox.exceptions import TaskTimeout
from django_ox.models import OxTask
from django_ox.timeouts import RECYCLE_EXIT_CODE
from django_ox.worker import Worker, _Watch

from .conftest import wait_for
from .tasks import slow

REPO = Path(__file__).resolve().parent.parent
TIMEOUT_PATH = f"{TaskTimeout.__module__}.{TaskTimeout.__qualname__}"

# Long enough that a row the watchdog recorded is still waiting for its retry
# when the test reads it, whatever the machine's speed.
BACKOFF = 900

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        bool(connections.settings[DEFAULT_DB_ALIAS].get("OPTIONS", {}).get("pool")),
        reason="the unpooled watchdog, and this run's default database is pooled",
    ),
]

on_a_server = pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="ends a connection from the server's side",
)
on_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql", reason="ends backends by application name"
)


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


@pytest.fixture
def stuck():
    """
    A worker on the test database and a factory for attempts it has claimed
    whose grace has passed while their thread is still inside them: what
    the watchdog finds due and records as stuck. Nothing runs them.
    """
    worker = Worker(lock_timeout=30, backoff_initial=BACKOFF, backoff_max=BACKOFF)
    idents = itertools.count(1)

    def claim() -> _Watch:
        slow.enqueue(0)
        task = worker.claim_one()
        assert task is not None
        attempt = (task.pk, task.lease_epoch)
        ident = next(idents)
        worker._in_flight.add(attempt)
        worker._running_on[ident] = attempt
        now = time.monotonic()
        return _Watch(
            ident=ident,
            db_task=copy.copy(task),
            attempt=attempt,
            timeout=1.0,
            started=now - 3,
            deadline=now - 2,
            deadline_at=timezone.now(),
            injectable=False,
            fired=True,
            grace_at=now - 1,
        )

    return worker, claim


def forget_connections():
    """Close the calling thread's connections, whatever a test left on them."""
    for conn in connections.all(initialized_only=True):
        conn.__dict__.pop("close", None)
        with suppress(Exception):
            conn.close()


@pytest.fixture
def watchdog_thread():
    """
    One thread that runs whatever is handed to it, in turn, as the watchdog
    thread runs its batches one after another: what one call leaves on the
    thread's connection, the next call finds. Its connections are closed
    afterwards.
    """
    with ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="wd-test-batches"
    ) as pool:

        def run(call, *args):
            return pool.submit(call, *args).result(timeout=60)

        yield run
        pool.submit(forget_connections).result(timeout=60)


def batch(worker, *watches):
    """
    Record `watches` as one watchdog batch on an unpooled database, from
    the calling thread, and say whether the thread still holds an open
    connection afterwards.
    """
    worker._record_stuck(None, list(watches))
    return connections[DEFAULT_DB_ALIAS].connection is not None


def recorded(watch):
    """The attempt was recorded as a timed-out failure, with its backoff."""
    row = OxTask.objects.get(pk=watch.db_task.pk)
    return (
        row.status == OxTask.Status.READY
        and [e["exception_class_path"] for e in row.errors] == [TIMEOUT_PATH]
        and row.lease_epoch == watch.attempt[1] + 1
        and row.run_after is not None
        and row.run_after > timezone.now() + timedelta(seconds=BACKOFF / 2)
    )


BACKEND_ID = {
    "postgresql": "SELECT pg_backend_pid()",
    "mysql": "SELECT CONNECTION_ID()",
}


def backend_id():
    """The server's id for the calling thread's connection."""
    with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        cursor.execute(BACKEND_ID[connection.vendor])
        return cursor.fetchone()[0]


def backend_alive(ident):
    """Whether the server still has the connection `ident`, from the test's own."""
    sql = {
        "postgresql": "SELECT count(*) FROM pg_stat_activity WHERE pid = %s",
        "mysql": "SELECT count(*) FROM information_schema.PROCESSLIST WHERE ID = %s",
    }[connection.vendor]
    with connection.cursor() as cursor:
        cursor.execute(sql, [ident])
        return cursor.fetchone()[0] > 0


def end_backend(ident):
    """End the connection `ident` from the server's side, as a restart does."""
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SELECT pg_terminate_backend(%s)", [ident])
        else:
            # Refused with "Unknown thread id" once it has already gone.
            with suppress(DatabaseError):
                cursor.execute("KILL CONNECTION %s", [ident])


def test_each_batch_closes_its_connection_and_the_next_opens_one(
    stuck, watchdog_thread, monkeypatch
):
    """
    Three batches on one watchdog thread, as a worker records three stuck
    attempts that went stuck apart. Each records on a connection of its
    own and leaves nothing open behind it. Before, the first batch's
    connection was kept for the thread's lifetime and every later batch
    ran on it.
    """
    worker, claim = stuck
    watches = [claim() for _ in range(3)]
    # The driver connections themselves, not their ids: kept alive, no two
    # can share an id.
    used = []
    handle = worker._handle_stuck

    def noting(watch):
        handle(watch)
        used.append((threading.get_ident(), connections[DEFAULT_DB_ALIAS].connection))

    monkeypatch.setattr(worker, "_handle_stuck", noting)
    left_open = [watchdog_thread(batch, worker, watch) for watch in watches]
    assert left_open == [False, False, False]
    assert len({thread for thread, _ in used}) == 1
    assert None not in [conn for _, conn in used]
    assert len({id(conn) for _, conn in used}) == 3
    assert all(map(recorded, watches))
    assert worker._stuck == {w.ident: w.attempt for w in watches}


@on_a_server
def test_the_connection_a_batch_used_is_gone_from_the_server_once_it_ends(
    stuck, watchdog_thread, monkeypatch
):
    """
    No leak on the server: the backend each batch recorded on has ended by
    the time the next batch runs. Before, it lasted as long as the thread.
    """
    worker, claim = stuck
    used = []
    handle = worker._handle_stuck

    def noting(watch):
        handle(watch)
        used.append(backend_id())

    monkeypatch.setattr(worker, "_handle_stuck", noting)
    for _ in range(2):
        watchdog_thread(batch, worker, claim())
        assert wait_for(lambda: not backend_alive(used[-1])), used
    assert used[0] != used[1]


@on_a_server
def test_a_connection_the_server_ended_between_batches_is_not_reused(
    stuck, watchdog_thread, monkeypatch, caplog
):
    """
    A database restart between two batches on one watchdog thread: the
    connection the first batch used is ended from the server's side before
    the second batch runs. The second attempt is recorded, with its
    TaskTimeout and its backoff. Before, the second batch ran on the ended
    connection, its record failed, and the row stayed RUNNING for the reaper,
    which requeues it without the error and without the backoff.
    """
    worker, claim = stuck
    first, second = claim(), claim()
    used = []
    handle = worker._handle_stuck

    def noting(watch):
        handle(watch)
        used.append(backend_id())

    monkeypatch.setattr(worker, "_handle_stuck", noting)
    watchdog_thread(batch, worker, first)
    monkeypatch.setattr(worker, "_handle_stuck", handle)
    end_backend(used[0])
    assert wait_for(lambda: not backend_alive(used[0]))

    watchdog_thread(batch, worker, second)
    assert recorded(first)
    assert recorded(second), OxTask.objects.get(pk=second.db_task.pk).errors
    assert not events(caplog, "task_stuck_unrecorded")
    assert worker._stuck == {w.ident: w.attempt for w in (first, second)}


class Escaped(BaseException):
    """Not an Exception, so nothing in the batch catches it."""


@pytest.mark.parametrize("failure", ["record", "escapes"])
def test_a_batch_that_fails_still_closes_its_connection(
    stuck, watchdog_thread, monkeypatch, caplog, failure
):
    """
    The first batch's record fails after it has queried, or something the
    batch does not catch leaves it altogether. Either way the connection
    is closed as the batch ends, and the next batch records on a fresh one.
    """
    worker, claim = stuck
    first, second = claim(), claim()
    handle_failure = worker._handle_failure
    handle_stuck = worker._handle_stuck

    def queries_then_fails(*args, **kwargs):
        OxTask.objects.count()
        raise RuntimeError("the record failed")

    def queries_then_escapes(watch):
        OxTask.objects.count()
        raise Escaped

    def first_batch():
        try:
            return batch(worker, first)
        except Escaped:
            return connections[DEFAULT_DB_ALIAS].connection is not None

    if failure == "record":
        monkeypatch.setattr(worker, "_handle_failure", queries_then_fails)
    else:
        monkeypatch.setattr(worker, "_handle_stuck", queries_then_escapes)
    assert watchdog_thread(first_batch) is False
    monkeypatch.setattr(worker, "_handle_failure", handle_failure)
    monkeypatch.setattr(worker, "_handle_stuck", handle_stuck)

    assert OxTask.objects.get(pk=first.db_task.pk).status == OxTask.Status.RUNNING
    if failure == "record":
        (unrecorded,) = events(caplog, "task_stuck_unrecorded")
        assert unrecorded.task_id == str(first.db_task.id)
        # The recycle does not depend on the record.
        assert worker._stuck == {first.ident: first.attempt}
    assert watchdog_thread(batch, worker, second) is False
    assert recorded(second)


@pytest.mark.parametrize(
    ("raised", "logged"),
    [(DatabaseError, False), (RuntimeError, True)],
    ids=["database-error", "other"],
)
def test_a_connection_that_will_not_close_does_not_end_the_backstop(
    stuck, watchdog_thread, monkeypatch, caplog, raised, logged
):
    """
    Closing is quiet about a database error, as a pooled batch's own
    connection is; anything else is logged, and the backstop carries on.
    """
    worker, claim = stuck
    watch = claim()
    handle = worker._handle_stuck

    def refuses():
        raise raised("close failed")

    def then_refuse_to_close(watch):
        handle(watch)
        connections[DEFAULT_DB_ALIAS].close = refuses

    monkeypatch.setattr(worker, "_handle_stuck", then_refuse_to_close)
    watchdog_thread(worker._record_stuck, None, [watch])
    assert recorded(watch)
    assert worker._stuck == {watch.ident: watch.attempt}
    errors = events(caplog, "watchdog_error")
    assert len(errors) == int(logged)
    if logged:
        assert "could not give back the connection" in errors[0].getMessage()


def watchdog_project(tmp_path: Path, application_name: str) -> Path:
    """
    A project whose settings are this suite's with the default database
    unpooled and its connections named `application_name`, so the test can
    end the worker's connections and no other. This checkout's src goes
    first on the path, so the worker runs the code under test.
    """
    project = tmp_path / "proj"
    (project / "wdproj").mkdir(parents=True)
    (project / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wdproj.settings')\n"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (project / "wdproj" / "__init__.py").write_text("")
    (project / "wdproj" / "settings.py").write_text(
        "import sys\n"
        f"sys.path[:0] = [{str(REPO / 'src')!r}, {str(REPO)!r}]\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "_db = DATABASES['default']\n"
        "_options = {k: v for k, v in _db.get('OPTIONS', {}).items() if k != 'pool'}\n"
        f"_options['application_name'] = {application_name!r}\n"
        "DATABASES['default'] = {**_db, 'OPTIONS': _options}\n"
    )
    return project


@on_postgresql
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@pytest.mark.parametrize("restart", [False, True], ids=["control", "restart"])
def test_a_restart_between_two_stuck_attempts_leaves_the_second_recorded(
    tmp_path, restart
):
    """
    The real worker, unpooled, --concurrency 2. Two tasks sleep in C, where
    TaskTimeout cannot land: the first on a queue with a 0.5 s timeout, the
    second on one with 3 s, each with a 0.5 s grace. One watchdog thread
    records the first and the worker starts to recycle; the second is still
    running, so the thread stays up and records it about 2.5 s later, in a
    batch of its own. With `restart`, every connection the worker has
    is ended between the two, as a database restart ends them.

    Before, the watchdog kept its first batch's connection, the second
    record failed on it ("could not record the stuck attempt"), and the
    second row stayed RUNNING with no error and no backoff. The control
    has no restart and records both either way.
    """
    name = f"ox-wd-{os.getpid()}-{int(restart)}"
    project = watchdog_project(tmp_path, name)
    first = slow.enqueue(30)
    second = slow.using(queue_name="emails").enqueue(30)
    log_path = tmp_path / "worker.log"
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "wdproj.settings",
        "OX_TEST_DB_NAME": str(connection.settings_dict["NAME"]),
        "OX_TEST_LOG_LEVEL": "INFO",
        "OX_TEST_LOG_FORMAT": "%(threadName)s:%(thread)d %(message)s",
        "OX_TEST_TASKS_OPTIONS": json.dumps(
            {
                "TASK_TIMEOUTS": {"default": 0.5, "emails": 3},
                "TASK_TIMEOUT_GRACE": 0.5,
                "BACKOFF_INITIAL": BACKOFF,
                "BACKOFF_MAX": BACKOFF,
            }
        ),
    }
    with log_path.open("wb") as log:
        proc = subprocess.Popen(
            [
                sys.executable,
                "manage.py",
                "ox_worker",
                "--concurrency",
                "2",
                "--interval",
                "0.05",
            ],
            cwd=project,
            env=env,
            stdout=log,
            stderr=log,
        )
    ended = None
    try:
        assert wait_for(lambda: OxTask.objects.get(pk=first.id).errors, timeout=60), (
            log_path.read_text()
        )
        if restart:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                    "WHERE application_name = %s",
                    [name],
                )
                ended = cursor.fetchone()[0]
            # The name reaches the worker's connections: the poll loop keeps
            # the one it claimed on through the drain, so there is always
            # one to end, whatever the watchdog holds.
            assert ended, log_path.read_text()
        # The second attempt was still unrecorded when the connections ended.
        at_restart = OxTask.objects.get(pk=second.id)
        assert (at_restart.status, at_restart.errors) == (OxTask.Status.RUNNING, [])
        code = proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    text = log_path.read_text()
    assert code == RECYCLE_EXIT_CODE, text
    stuck_lines = [line for line in text.splitlines() if "did not stop" in line]
    assert len(stuck_lines) == 2, text
    # One watchdog thread recorded both, in two batches: the case at issue.
    (thread,) = {line.split(" ", 1)[0] for line in stuck_lines}
    assert thread.startswith("ox-watchdog:"), text
    assert "could not record the stuck attempt" not in text, (ended, text)
    for task in (first, second):
        row = OxTask.objects.get(pk=task.id)
        assert row.status == OxTask.Status.READY, (ended, text)
        assert [e["exception_class_path"] for e in row.errors] == [TIMEOUT_PATH]
        assert row.lease_epoch == 2
        assert row.run_after > timezone.now() + timedelta(seconds=BACKOFF / 2)
