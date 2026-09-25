"""
A task whose database connection the server ended during the attempt, and
the outcome the worker writes after it.

A failover, a restart, an operator's kill or a network drop ends the
connection a task is using. The task sees a database error and may catch it
and return, or let it out and fail. Either way the worker then writes the
attempt's outcome on the same thread, where Django still holds the dead
connection: Django drops one only between requests, and an attempt is not a
request. Written there, the outcome raised and the row stayed RUNNING with
its lease no longer renewed, so a task that had succeeded ran again once the
lease expired, and a failure lost its error and its backoff, or ended LOST.

A connection can also die with nothing on the task's thread noticing: the
server ends the session while the task works on without the database, or
restarts between two tasks while the thread keeps a persistent connection
and the next task makes no query. Nothing flagged it, so the outcome write
is the first statement to find it dead, and that write goes once more on a
new connection: never inside a caller's transaction, never for an error
that leaves the connection answering, fenced as the first one was, and
without writing twice an outcome whose first write committed before the
connection went.

A restart ends every session at once, the ones Django's pool holds idle
included. The pool hands those out unchecked unless CONN_HEALTH_CHECKS is
set, and when it is, tests them one at a time, waiting longer after each,
until its timeout. So closing a pooled connection that died is followed by
a sweep of the pool's idle connections, and a failed pass of the poll loop
sweeps the pool too.

The end-to-end tests run the real ox_worker command in a process of its
own, against the test database, with tasks that have the server end their
own connection (dead_connection_tasks). They need PostgreSQL or MySQL; the
pool tests need PostgreSQL and psycopg_pool, and the test that closes the
database to new connections needs PostgreSQL. The rest run on every
database and hold the drop, the second write and the sweep to what they may
touch: only a connection with an error that no longer answers, never one
inside an atomic block, and nothing at all on an ordinary outcome or a pass
that did not fail.
"""

import copy
import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import (
    IntegrityError,
    InterfaceError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.models import F
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.compat import task_finished
from django_ox.models import OxTask
from django_ox.worker import Worker

from .conftest import start_worker_thread
from .dead_connection_tasks import (
    end_every_other_session,
    ends_its_connection_and_fails,
    ends_its_connection_and_succeeds,
    ends_its_connection_from_async_and_succeeds,
    ends_the_second_connection_and_succeeds,
    gate,
    loses_its_connection_and_its_lease,
    loses_its_connection_unnoticed,
    loses_the_database,
    makes_no_query,
    other_sessions,
    outcome_commits_then_connection_drops,
    pool_report,
    queries_after_a_restart,
    quick,
    ran,
    read_notes,
    restart_every_other_session,
    survives_a_statement_error,
    works_offline_through_a_restart,
)
from .tasks import echo, fail_always

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="runs the worker as a process"),
]

ends_a_connection = pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="has the server end a connection, with pg_terminate_backend or KILL",
)
on_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="Django's connection pool is PostgreSQL only",
)
closes_the_database = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="closes the database to new connections, with ALTER DATABASE on "
    "PostgreSQL; MySQL has no such switch short of a server-wide one",
)
with_psycopg_pool = pytest.mark.skipif(
    importlib.util.find_spec("psycopg_pool") is None,
    reason="needs psycopg_pool: pip install 'psycopg[pool]'",
)

OPERATIONAL_ERROR = "django.db.utils.OperationalError"
VALUE_ERROR = "builtins.ValueError"

#: The endings of the line an outcome written on a second connection logs.
WROTE_IT_AGAIN = "wrote it on a new connection"
FOUND_IT_WRITTEN = "a new connection found it already written"
NOT_RECORDED = "The outcome is unconfirmed"

#: Django's pool with room for a third connection beside the poll loop's and
#: the task thread's: the one the task ends its own session from.
ROOMY_POOL = {"min_size": 1, "max_size": 4, "timeout": 5.0}

#: A backoff far longer than any test, so a retry the attempt wrote is told
#: apart from a requeue by the reaper, which leaves run_after empty.
LONG_BACKOFF = {"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900}

#: The longest a worker here may take to exit by itself. Far past what a
#: loaded machine needs; reaching it means it never would have.
LIMIT = 90.0


def worker_project(
    tmp_path: Path,
    *,
    pool: object = None,
    second: bool = False,
    conn_max_age: int | None = None,
    health_checks: bool = False,
) -> Path:
    """
    A project whose settings are this run's, with the default database
    pooled when `pool` is given, with a second alias to the same database
    when `second` is, with persistent connections when `conn_max_age` is,
    and with CONN_HEALTH_CHECKS when `health_checks` is. This checkout's
    src goes first on the path, so the worker runs the code under test.
    """
    project = tmp_path / "proj"
    (project / "deadconnproj").mkdir(parents=True)
    (project / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'deadconnproj.settings')\n"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (project / "deadconnproj" / "__init__.py").write_text("")
    lines = [
        "import copy, sys",
        f"sys.path[:0] = [{str(REPO / 'src')!r}, {str(REPO)!r}]",
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403",
    ]
    if pool is not None:
        lines += [
            "_db = DATABASES['default']",
            f"_options = {{**_db.get('OPTIONS', {{}}), 'pool': {pool!r}}}",
            "DATABASES['default'] = {**_db, 'CONN_MAX_AGE': 0, 'OPTIONS': _options}",
        ]
    if second:
        lines.append("DATABASES['second'] = copy.deepcopy(DATABASES['default'])")
    if conn_max_age is not None:
        lines.append(f"DATABASES['default']['CONN_MAX_AGE'] = {conn_max_age!r}")
    if health_checks:
        lines.append("DATABASES['default']['CONN_HEALTH_CHECKS'] = True")
    (project / "deadconnproj" / "settings.py").write_text("\n".join(lines) + "\n")
    return project


def start_worker(
    project: Path, tmp_path: Path, *flags: str, options: dict | None = None
) -> tuple[subprocess.Popen, Path]:
    """Start ox_worker in `project`; the process and the file it logs to."""
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "deadconnproj.settings"
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    if options is not None:
        env["OX_TEST_TASKS_OPTIONS"] = json.dumps(options)
    log = tmp_path / "worker.log"
    with log.open("wb") as out:
        proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "manage.py", "ox_worker", "--interval", "0.05", *flags],
            cwd=project,
            env=env,
            stdout=out,
            stderr=out,
        )
    return proc, log


def finish_worker(proc: subprocess.Popen, log: Path) -> str:
    """Wait for a started worker to exit by itself; its output."""
    try:
        code = proc.wait(timeout=LIMIT)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail(f"the worker did not exit by itself:\n{log.read_text()}")
    output = log.read_text()
    assert code == 0, output
    return output


def run_worker(
    project: Path, tmp_path: Path, *flags: str, options: dict | None = None
) -> str:
    """Run ox_worker in `project` until it exits by itself; its output."""
    return finish_worker(*start_worker(project, tmp_path, *flags, options=options))


def error_paths(stored):
    return [error["exception_class_path"] for error in stored.errors]


def events(notes, name):
    return [note for note in read_notes(notes) if note["event"] == name]


def assert_succeeded_once(stored, output):
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    ), output
    assert len(stored.worker_ids) == 1
    assert stored.return_value == "succeeded anyway"
    assert stored.errors == []


# -- end to end ----------------------------------------------------------------


@ends_a_connection
def test_a_task_that_succeeds_after_its_connection_ended_runs_once(tmp_path):
    """
    The success is recorded and nothing runs the task again. The lease is two
    seconds and the only other task is due after five, so a row left RUNNING
    would be reaped and claimed again first: the worker's second claim says
    which it was.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = ends_its_connection_and_succeeds.enqueue(notes)
    later = ran.using(run_after=timezone.now() + timedelta(seconds=5)).enqueue(notes)

    output = run_worker(
        worker_project(tmp_path), tmp_path, "--lock-timeout", "2", "--max-tasks", "2"
    )

    assert_succeeded_once(OxTask.objects.get(id=result.id), output)
    assert [note["event"] for note in read_notes(notes)] == ["ended", "ran"], output
    (ended,) = events(notes, "ended")
    assert ended["flagged"] is True
    assert OxTask.objects.get(id=later.id).status == OxTask.Status.SUCCESSFUL
    assert "Unhandled error" not in output


@ends_a_connection
@pytest.mark.parametrize("last", [False, True], ids=["retry", "last-attempt"])
def test_a_failure_after_the_connection_ended_is_recorded(tmp_path, last):
    """
    The task's own error is recorded by the attempt, at the attempt's epoch:
    a retry after the worker's backoff, or FAILED on the last attempt. A
    requeue by the reaper would move the epoch, record no error and set no
    run_after, and the reaper marks a last attempt LOST.
    """
    notes = str(tmp_path / "notes.jsonl")
    before = timezone.now()
    result = ends_its_connection_and_fails.enqueue(notes)
    if last:
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

    output = run_worker(
        worker_project(tmp_path), tmp_path, "--max-tasks", "1", options=LONG_BACKOFF
    )

    stored = OxTask.objects.get(id=result.id)
    assert (stored.attempts, stored.lease_epoch) == (1, 1), output
    assert error_paths(stored) == [OPERATIONAL_ERROR]
    assert stored.locked_by is None
    if last:
        assert stored.status == OxTask.Status.FAILED
        assert stored.finished_at is not None
        assert "failed after 1/1 attempts (OperationalError)" in output
    else:
        assert stored.status == OxTask.Status.READY
        assert 890 < (stored.run_after - before).total_seconds() < 1000
        assert "retrying in 900.0s" in output
    assert len(events(notes, "ending")) == 1
    assert "Unhandled error" not in output


@on_postgresql
@with_psycopg_pool
def test_with_the_connection_pool_every_outcome_lands_and_none_is_kept(tmp_path):
    """
    Django's pool, at the size the worker asks for at --concurrency 1: one
    connection for the poll loop and one for the task thread. A connection
    the worker kept after its task, or one the pool handed out again after
    it had ended, would leave a later task waiting out the pool's timeout or
    failing on it. Four tasks end their connection, two returning and two
    failing, one of those on its last attempt, and a fifth reads the pool's
    counters.
    """
    notes = str(tmp_path / "notes.jsonl")
    succeeded = [ends_its_connection_and_succeeds.enqueue(notes) for _ in range(2)]
    retried = ends_its_connection_and_fails.enqueue(notes)
    failed = ends_its_connection_and_fails.enqueue(notes)
    OxTask.objects.filter(id=failed.id).update(max_attempts=1)
    report = pool_report.using(priority=-1).enqueue(notes)
    project = worker_project(
        tmp_path, pool={"min_size": 1, "max_size": 2, "timeout": 5.0}
    )

    output = run_worker(project, tmp_path, "--max-tasks", "5", options=LONG_BACKOFF)

    for result in succeeded:
        assert_succeeded_once(OxTask.objects.get(id=result.id), output)
    for result, status in (
        (retried, OxTask.Status.READY),
        (failed, OxTask.Status.FAILED),
    ):
        stored = OxTask.objects.get(id=result.id)
        assert (stored.status, stored.attempts, stored.lease_epoch) == (
            status,
            1,
            1,
        ), output
        assert error_paths(stored) == [OPERATIONAL_ERROR]
    assert OxTask.objects.get(id=report.id).status == OxTask.Status.SUCCESSFUL
    (stats,) = [note["stats"] for note in events(notes, "pool")]
    # Each ended connection went back to the pool, which found it closed and
    # discarded it rather than hand it out again.
    assert stats["returns_bad"] == 4, stats
    # While the report ran, only the poll loop's connection and its own were
    # out, and nothing was waiting for one.
    assert stats["pool_size"] - stats["pool_available"] == 2, stats
    assert stats.get("requests_waiting", 0) == 0, stats
    assert "couldn't get a connection" not in output
    assert "Unhandled error" not in output


@ends_a_connection
@pytest.mark.parametrize(
    "options", [None, {"TASK_TIMEOUT": 60}], ids=["no-timeout", "under-a-timeout"]
)
def test_an_async_task_ends_the_connection_of_the_thread_that_writes(tmp_path, options):
    """
    A coroutine cannot query on its event loop's thread; sync_to_async runs
    the query on the thread that called async_to_sync, which is the worker's
    pool thread, with or without a task timeout. So the connection an async
    task ends is the one the outcome is written from, and it is dropped the
    same way.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = ends_its_connection_from_async_and_succeeds.enqueue(notes)

    output = run_worker(
        worker_project(tmp_path), tmp_path, "--max-tasks", "1", options=options
    )

    assert_succeeded_once(OxTask.objects.get(id=result.id), output)
    (ended,) = events(notes, "ended")
    (loop,) = events(notes, "loop")
    assert ended["thread"].startswith("ox_"), ended
    assert loop["thread"] != ended["thread"]
    assert "Unhandled error" not in output


@ends_a_connection
def test_the_outcome_lands_when_the_ended_connection_is_not_the_default_one(
    tmp_path,
):
    """
    The worker works on a second alias, --database second, and the task
    ends that connection while the default one stays healthy.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = ends_the_second_connection_and_succeeds.enqueue(notes)

    output = run_worker(
        worker_project(tmp_path, second=True),
        tmp_path,
        "--database",
        "second",
        "--max-tasks",
        "1",
    )

    assert_succeeded_once(OxTask.objects.get(id=result.id), output)
    (ended,) = events(notes, "ended")
    assert (ended["alias"], ended["flagged"]) == ("second", True)
    assert "Unhandled error" not in output


# -- a connection that died unnoticed -------------------------------------------


def only_line(output, text):
    """The one line of `output` that holds `text`; fails unless exactly one."""
    lines = [line for line in output.splitlines() if text in line]
    assert len(lines) == 1, output
    return lines[0]


@ends_a_connection
@pytest.mark.parametrize(
    "pool",
    [None, pytest.param(ROOMY_POOL, marks=[on_postgresql, with_psycopg_pool])],
    ids=["unpooled", "pool"],
)
def test_a_success_after_the_connection_died_unnoticed_runs_once(tmp_path, pool):
    """
    The server ends the task's session from another connection while the
    task works on without the database, and the task returns with no
    further query: Django flagged nothing, so the success write is the first
    statement on the dead connection. It goes once more on a new one. The
    lease is two seconds and the only other task is due after five, so a
    row left RUNNING would be reaped and claimed again first: the worker's
    second claim says which it was.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = loses_its_connection_unnoticed.enqueue(notes)
    later = ran.using(run_after=timezone.now() + timedelta(seconds=5)).enqueue(notes)

    output = run_worker(
        worker_project(tmp_path, pool=pool),
        tmp_path,
        "--lock-timeout",
        "2",
        "--max-tasks",
        "2",
    )

    assert_succeeded_once(OxTask.objects.get(id=result.id), output)
    assert [note["event"] for note in read_notes(notes)] == [
        "ended-unnoticed",
        "finished",
        "ran",
    ], output
    (ended,) = events(notes, "ended-unnoticed")
    assert ended["flagged"] is False
    assert [note["status"] for note in events(notes, "finished")] == ["SUCCESSFUL"]
    line = only_line(output, WROTE_IT_AGAIN)
    assert "the SUCCESSFUL outcome of attempt 1/3" in line
    assert "(OperationalError: " in line, line
    assert OxTask.objects.get(id=later.id).status == OxTask.Status.SUCCESSFUL
    assert "Unhandled error" not in output


@ends_a_connection
@pytest.mark.parametrize("last", [False, True], ids=["retry", "last-attempt"])
def test_a_failure_after_the_connection_died_unnoticed_is_recorded(tmp_path, last):
    """
    The same drop, and the task raises: its own error is recorded once, at
    the attempt's epoch, with the worker's backoff or as FAILED, which a
    requeue by the reaper would not do.
    """
    notes = str(tmp_path / "notes.jsonl")
    before = timezone.now()
    result = loses_its_connection_unnoticed.enqueue(notes, fail=True)
    if last:
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

    output = run_worker(
        worker_project(tmp_path), tmp_path, "--max-tasks", "1", options=LONG_BACKOFF
    )

    stored = OxTask.objects.get(id=result.id)
    assert (stored.attempts, stored.lease_epoch) == (1, 1), output
    assert error_paths(stored) == [VALUE_ERROR]
    assert stored.locked_by is None
    if last:
        assert stored.status == OxTask.Status.FAILED
        assert "failed after 1/1 attempts (ValueError)" in output
        assert "the FAILED outcome of attempt 1/1" in only_line(output, WROTE_IT_AGAIN)
    else:
        assert stored.status == OxTask.Status.READY
        assert 890 < (stored.run_after - before).total_seconds() < 1000
        assert output.count("retrying in 900.0s") == 1
        assert "the READY outcome of attempt 1/3" in only_line(output, WROTE_IT_AGAIN)
    assert len(events(notes, "ended-unnoticed")) == 1
    assert "Unhandled error" not in output


def wait_for(condition, what, proc, log):
    deadline = time.monotonic() + LIMIT
    while not condition():
        if proc.poll() is not None:
            pytest.fail(f"the worker exited before {what}:\n{log.read_text()}")
        if time.monotonic() > deadline:
            pytest.fail(f"no {what} after {LIMIT}s:\n{log.read_text()}")
        time.sleep(0.05)


@ends_a_connection
def test_a_task_with_no_query_after_a_restart_is_recorded(tmp_path):
    """
    Persistent connections: the pool thread keeps the connection its first
    task's outcome was written on. The server then ends every session on
    the database but the test's own, as a restart does, and the next task
    makes no query, so its success write is the first statement on the
    dead connection the thread kept.
    """
    notes = str(tmp_path / "notes.jsonl")
    first = ran.enqueue(notes)
    proc, log = start_worker(
        worker_project(tmp_path, conn_max_age=60),
        tmp_path,
        "--concurrency",
        "1",
        "--max-tasks",
        "2",
    )
    try:
        wait_for(
            lambda: OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL,
            "first success",
            proc,
            log,
        )
        ended = end_every_other_session(connection)
        second = makes_no_query.enqueue(notes)
        output = finish_worker(proc, log)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)

    # At least the poll loop's session and the one the pool thread kept.
    assert ended >= 2, output
    assert_succeeded_once(OxTask.objects.get(id=second.id), output)
    assert [note["event"] for note in read_notes(notes)] == [
        "ran",
        "ran-no-query",
        "finished",
    ], output
    assert "the SUCCESSFUL outcome of attempt 1/3" in only_line(output, WROTE_IT_AGAIN)
    assert "Unhandled error" not in output


@closes_the_database
def test_when_the_database_stays_gone_the_row_is_left_for_the_reaper(tmp_path):
    """
    The task's session ends and the database refuses new connections, so
    the second write cannot connect either. One line says the outcome is
    unconfirmed, and the row stays RUNNING under this attempt's lease for
    the reaper, as it did before for every drop; nothing else is written.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = loses_the_database.enqueue(notes)
    name = connection.settings_dict["NAME"]
    try:
        output = run_worker(worker_project(tmp_path), tmp_path, "--max-tasks", "1")
    finally:
        with connection._nodb_cursor() as cursor:
            cursor.execute(f'ALTER DATABASE "{name}" WITH ALLOW_CONNECTIONS true')

    stored = OxTask.objects.get(id=result.id)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.RUNNING,
        1,
        1,
    ), output
    assert stored.locked_by is not None
    assert (stored.return_value, stored.errors) == (None, [])
    line = only_line(output, NOT_RECORDED)
    assert "the SUCCESSFUL outcome of attempt 1/3" in line
    assert (
        "The outcome is unconfirmed, not necessarily absent: the first write "
        "may have landed, or another recovery path may already have fenced "
        "this attempt. If the row still awaits recovery, the reaper handles it "
        "once its lease expires"
    ) in line
    assert "not currently accepting connections" in line, line
    assert WROTE_IT_AGAIN not in output
    assert not events(notes, "finished")
    assert "Unhandled error" not in output


@ends_a_connection
def test_a_second_write_after_another_worker_took_the_row_writes_nothing(tmp_path):
    """
    While the task works on, its lease runs out, a second worker's reaper
    requeues the row and its claim takes it, and the task's session ends.
    The second write is fenced like the first and matches nothing: the row
    stays the second worker's, and the attempt says it lost its lease.
    """
    notes = str(tmp_path / "notes.jsonl")
    result = loses_its_connection_and_its_lease.enqueue(notes)

    output = run_worker(worker_project(tmp_path), tmp_path, "--max-tasks", "1")

    (taken,) = events(notes, "taken-over")
    stored = OxTask.objects.get(id=result.id)
    assert taken["epoch"] > 1
    assert (stored.status, stored.lease_epoch, stored.attempts, stored.locked_by) == (
        OxTask.Status.RUNNING,
        taken["epoch"],
        taken["attempts"],
        taken["worker_id"],
    ), output
    assert stored.return_value is None
    assert "lost its lease on attempt 1/3; dropping the SUCCESSFUL write" in output
    assert WROTE_IT_AGAIN not in output
    assert "succeeded in" not in output
    assert not events(notes, "finished")
    assert "Unhandled error" not in output


@ends_a_connection
@pytest.mark.parametrize(
    ("fail", "last"),
    [(False, False), (True, False), (True, True)],
    ids=["success", "retry", "last-attempt"],
)
def test_an_outcome_that_committed_before_its_connection_dropped_is_not_written_twice(
    tmp_path, fail, last
):
    """
    The write commits, and the connection is ended before the worker hears
    back, so the write raises although the row holds it. The new connection
    finds it there: one error record, one backoff, one task_finished, and no
    lease loss claimed for an outcome that landed.
    """
    notes = str(tmp_path / "notes.jsonl")
    before = timezone.now()
    result = outcome_commits_then_connection_drops.enqueue(notes, fail=fail)
    if last:
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

    output = run_worker(
        worker_project(tmp_path), tmp_path, "--max-tasks", "1", options=LONG_BACKOFF
    )

    (committed,) = events(notes, "committed")
    assert (committed["autocommit"], committed["in_atomic_block"]) == (True, False)
    stored = OxTask.objects.get(id=result.id)
    finished = [note["status"] for note in events(notes, "finished")]
    if not fail:
        assert_succeeded_once(stored, output)
        assert output.count("succeeded in") == 1
        assert finished == ["SUCCESSFUL"]
    else:
        assert (stored.attempts, stored.lease_epoch) == (1, 1), output
        assert error_paths(stored) == [VALUE_ERROR]
        if last:
            assert stored.status == OxTask.Status.FAILED
            assert output.count("failed after 1/1 attempts (ValueError)") == 1
            assert finished == ["FAILED"]
        else:
            assert stored.status == OxTask.Status.READY
            assert 890 < (stored.run_after - before).total_seconds() < 1000
            assert output.count("retrying in 900.0s") == 1
            assert finished == []
    only_line(output, FOUND_IT_WRITTEN)
    assert WROTE_IT_AGAIN not in output
    assert "lost its lease" not in output
    assert "Unhandled error" not in output


# -- a restart under Django's pool ----------------------------------------------


#: Django's pool, full from the start. After a restart every connection it
#: holds idle is dead: more of them than a checkout with CONN_HEALTH_CHECKS
#: gets through before the pool's timeout, since it waits longer after each
#: one it discards, and without health checks each is handed out unchecked.
RESTART_POOL = {"min_size": 10, "max_size": 10, "timeout": 5.0}

with_and_without_health_checks = pytest.mark.parametrize(
    "health_checks", [False, True], ids=["no-health-checks", "health-checks"]
)


def wait_for_a_full_pool(proc, log):
    """
    Wait until the worker holds the pool's every connection, and a moment
    longer for the last ones to finish connecting.
    """
    wait_for(
        lambda: other_sessions(connection) >= RESTART_POOL["max_size"],
        "a full pool",
        proc,
        log,
    )
    time.sleep(0.5)


def through_a_restart(tmp_path, notes, *, health_checks):
    """
    Run a pooled worker for the one task enqueued, which queries and waits
    on its gate. Once it has queried and the pool is full, end every session
    on the database at once, as a restart does, open the gate, and wait for
    the worker to finish. Its output, and how many sessions were ended.
    """
    project = worker_project(tmp_path, pool=RESTART_POOL, health_checks=health_checks)
    proc, log = start_worker(
        project,
        tmp_path,
        "--concurrency",
        "2",
        "--max-tasks",
        "1",
        options=LONG_BACKOFF,
    )
    try:
        wait_for(lambda: events(notes, "ready"), "the task's first query", proc, log)
        wait_for_a_full_pool(proc, log)
        ended = restart_every_other_session(connection)
        gate(notes).touch()
        output = finish_worker(proc, log)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    return output, ended


def assert_recorded_once(stored, output, *, fail, last, before):
    """
    The attempt's own outcome, recorded once at its own epoch: the success,
    or the task's error with one backoff, or FAILED on the last attempt. A
    row left for the reaper would still be RUNNING here, since the worker
    stops after this one task.
    """
    assert (stored.attempts, stored.lease_epoch) == (1, 1), output
    assert stored.locked_by is None, output
    if not fail:
        assert_succeeded_once(stored, output)
        assert output.count("succeeded in") == 1
    elif last:
        assert stored.status == OxTask.Status.FAILED, output
        assert stored.finished_at is not None
        assert output.count("failed after 1/1 attempts") == 1
    else:
        assert stored.status == OxTask.Status.READY, output
        assert 890 < (stored.run_after - before).total_seconds() < 1000
        assert output.count("retrying in 900.0s") == 1
    assert NOT_RECORDED not in output
    assert "couldn't get a connection" not in output
    assert "Unhandled error" not in output


@on_postgresql
@with_psycopg_pool
@with_and_without_health_checks
@pytest.mark.parametrize(
    ("fail", "last"),
    [(False, False), (True, False), (True, True)],
    ids=["success", "retry", "last-attempt"],
)
def test_after_a_restart_an_outcome_found_dead_by_its_write_lands_once(
    tmp_path, health_checks, fail, last
):
    """
    Every session ends while the task works on without the database, and
    it returns, or raises, with no further query. The outcome write is the
    first statement on its dead connection, and the second try, which
    without the sweep checked out one of the pool's dead idle connections,
    lands. The body runs once.
    """
    notes = str(tmp_path / "notes.jsonl")
    before = timezone.now()
    result = works_offline_through_a_restart.enqueue(notes, fail=fail)
    if last:
        OxTask.objects.filter(id=result.id).update(max_attempts=1)

    output, ended = through_a_restart(tmp_path, notes, health_checks=health_checks)

    assert ended >= RESTART_POOL["max_size"], output
    stored = OxTask.objects.get(id=result.id)
    assert_recorded_once(stored, output, fail=fail, last=last, before=before)
    if fail:
        assert error_paths(stored) == [VALUE_ERROR]
    assert [
        note["event"] for note in read_notes(notes) if note["event"] != "finished"
    ] == [
        "ready",
        "resumed",
    ], output
    (resumed,) = events(notes, "resumed")
    assert resumed["flagged"] is False
    outcome = "FAILED" if last else "READY" if fail else "SUCCESSFUL"
    attempt = "1/1" if last else "1/3"
    assert f"the {outcome} outcome of attempt {attempt}" in only_line(
        output, WROTE_IT_AGAIN
    )


@on_postgresql
@with_psycopg_pool
@with_and_without_health_checks
@pytest.mark.parametrize("catch", [True, False], ids=["catches", "raises"])
def test_after_a_restart_an_outcome_the_task_saw_coming_lands_once(
    tmp_path, health_checks, catch
):
    """
    Every session ends while the task waits, and its next query fails on
    the dead connection: it catches the error and returns, or lets it out.
    The drop before the write closes that connection and sweeps the pool,
    so the write, which without the sweep checked out one of the pool's
    dead idle connections, lands the first time, and so does the error
    with its one backoff.
    """
    notes = str(tmp_path / "notes.jsonl")
    before = timezone.now()
    result = queries_after_a_restart.enqueue(notes, catch=catch)

    output, ended = through_a_restart(tmp_path, notes, health_checks=health_checks)

    assert ended >= RESTART_POOL["max_size"], output
    stored = OxTask.objects.get(id=result.id)
    assert_recorded_once(stored, output, fail=not catch, last=False, before=before)
    if not catch:
        assert error_paths(stored) == [OPERATIONAL_ERROR]
    (failed,) = events(notes, "query-failed")
    assert failed["flagged"] is True
    assert len(events(notes, "ready")) == 1, output
    assert WROTE_IT_AGAIN not in output


# -- the poll loop after a restart under Django's pool ---------------------------


@on_postgresql
@with_psycopg_pool
@with_and_without_health_checks
def test_after_a_restart_the_poll_loop_does_not_spend_a_pass_per_dead_connection(
    tmp_path, health_checks
):
    """
    A worker with a pool of ten idles at the default poll interval, and
    every session on the database ends at once. Without a sweep the loop
    got one dead idle connection back per pass, or, with health checks, a
    checkout that timed out working through them, and claimed nothing until
    they were gone. With it, a failed pass discards them all, and the task
    enqueued after the restart runs. No latency is asserted: the sweep and
    the connections it asks for take what they take.
    """
    notes = str(tmp_path / "notes.jsonl")
    first = quick.enqueue(notes)
    project = worker_project(tmp_path, pool=RESTART_POOL, health_checks=health_checks)
    proc, log = start_worker(
        project,
        tmp_path,
        "--interval",
        "1",
        "--concurrency",
        "1",
        "--max-tasks",
        "2",
    )
    try:
        wait_for(
            lambda: OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL,
            "the first task",
            proc,
            log,
        )
        wait_for_a_full_pool(proc, log)
        ended = restart_every_other_session(connection)
        second = quick.enqueue(notes)
        output = finish_worker(proc, log)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)

    assert ended >= RESTART_POOL["max_size"], output
    stored = OxTask.objects.get(id=second.id)
    assert (stored.status, stored.attempts) == (OxTask.Status.SUCCESSFUL, 1), output
    assert len(events(notes, "quick")) == 2, output
    failed_passes = output.count("could not reach the database this pass")
    assert 1 <= failed_passes <= 2, output
    assert "couldn't get a connection" not in output
    assert "Unhandled error" not in output


# -- what the drop may touch -------------------------------------------------


@pytest.fixture
def probes(monkeypatch):
    """
    The aliases whose connection is_usable() was asked about, in order, on
    every wrapper class this run's databases use.
    """
    asked = []
    for cls in {type(connections[alias]) for alias in connections}:
        real = cls.is_usable

        def spy(self, real=real):
            asked.append(self.alias)
            return real(self)

        monkeypatch.setattr(cls, "is_usable", spy)
    return asked


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
def test_an_ordinary_outcome_costs_no_statement(worker, monkeypatch, probes, task):
    """
    With no database error on the attempt, nothing is asked of any
    connection: the outcome takes the statements it took before.
    """
    args = ("x",) if task is echo else ()
    task.enqueue(*args)
    with CaptureQueriesContext(connection) as dropping:
        assert worker.run_once()
    assert probes == []

    monkeypatch.setattr(Worker, "_discard_unusable_connections", lambda self: None)
    task.enqueue(*args)
    with CaptureQueriesContext(connection) as not_dropping:
        assert worker.run_once()
    assert len(dropping.captured_queries) == len(not_dropping.captured_queries)


def test_a_connection_that_still_answers_after_an_error_is_kept(
    worker, probes, task_state
):
    """
    A statement error the task caught flags the connection, and one probe
    finds it still working, so the outcome is written on it rather than on a
    new one.
    """
    result = survives_a_statement_error.enqueue()

    assert worker.run_once()

    stored = OxTask.objects.get(id=result.id)
    assert (stored.status, stored.return_value) == (OxTask.Status.SUCCESSFUL, "kept")
    assert task_state["flagged"] is True
    assert connection.connection is task_state["driver_connection"]
    assert probes == ["default"]


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
@pytest.mark.parametrize("alias", ["default", "alt"])
@pytest.mark.parametrize(
    ("flagged", "usable", "atomic", "closed", "asked"),
    [
        pytest.param(False, False, False, False, False, id="no-error"),
        pytest.param(True, True, False, False, True, id="error-still-answers"),
        pytest.param(True, False, False, True, True, id="error-and-dead"),
        pytest.param(True, False, True, False, False, id="inside-atomic"),
    ],
)
def test_only_a_dead_connection_outside_a_transaction_is_dropped(
    worker, monkeypatch, alias, flagged, usable, atomic, closed, asked
):
    """
    Each alias the thread holds is judged by itself: dropped when an error
    left it unable to answer, kept otherwise, and not even asked about
    inside an atomic block, which belongs to whoever opened it. is_usable()
    is stood in for, so the dead case is the same on every database.
    """
    conn = connections[alias]
    conn.ensure_connection()
    asked_about = []

    def is_usable(self):
        asked_about.append(self.alias)
        return usable

    monkeypatch.setattr(type(conn), "is_usable", is_usable)
    block = transaction.atomic(using=alias) if atomic else nullcontext()
    with block:
        conn.errors_occurred = flagged
        try:
            worker._discard_unusable_connections()
            assert (conn.connection is None) is closed
        finally:
            conn.errors_occurred = False
    assert asked_about == ([alias] if asked else [])


# -- what the second write may touch -------------------------------------------


GONE = "server closed the connection unexpectedly"


@pytest.fixture
def finished_results():
    """Every task_finished TaskResult sent while the test runs."""
    sent = []

    def receiver(sender, task_result, **kwargs):
        sent.append(task_result)

    task_finished.connect(receiver)
    yield sent
    task_finished.disconnect(receiver)


def outcome_events(caplog, *names):
    return [
        (record.levelno, record.event)
        for record in caplog.records
        if getattr(record, "event", None) in names
    ]


def connection_goes(monkeypatch):
    """From now on this run's database connections fail is_usable()."""
    for cls in {type(connections[alias]) for alias in connections}:
        monkeypatch.setattr(cls, "is_usable", lambda self: False)


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("lock wait timeout exceeded"),
        InterfaceError("cursor already closed"),
        IntegrityError("duplicate key"),
        ValueError("not a database error"),
    ],
    ids=["operational", "interface", "integrity", "other"],
)
def test_an_error_that_leaves_the_connection_answering_is_raised_as_before(
    worker, monkeypatch, probes, error
):
    """
    The write raises, but the connection still answers, or the error is not
    one a lost connection raises: nothing is closed or written again, and
    the error goes out as it always did. Only the connection-level classes
    cost the one probe that tells.
    """
    written = []

    def raises(self, db_task, **fields):
        written.append(fields["status"])
        raise error

    monkeypatch.setattr(Worker, "_write_outcome", raises)
    result = echo.enqueue("x")
    connection.ensure_connection()
    driver = connection.connection

    with pytest.raises(type(error)):
        worker.run_once()

    assert written == [OxTask.Status.SUCCESSFUL]
    assert connection.connection is driver
    lost_connection_class = isinstance(error, OperationalError | InterfaceError)
    assert probes == (["default"] if lost_connection_class else [])
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING


def test_inside_a_callers_transaction_the_write_is_not_tried_again(
    worker, monkeypatch, probes
):
    """
    An inline run_once() inside the caller's atomic block: the connection is
    theirs, so it is neither probed nor closed, and the error is theirs.
    """
    written = []

    def raises(self, db_task, **fields):
        written.append(fields["status"])
        raise OperationalError(GONE)

    monkeypatch.setattr(Worker, "_write_outcome", raises)
    echo.enqueue("x")

    with transaction.atomic():
        connection.ensure_connection()
        driver = connection.connection
        with pytest.raises(OperationalError):
            worker.run_once()
        assert connection.connection is driver
        transaction.set_rollback(True)

    assert written == [OxTask.Status.SUCCESSFUL]
    assert probes == []


def test_with_autocommit_turned_off_by_the_caller_the_write_is_not_tried_again(
    worker, monkeypatch, probes
):
    """
    An inline run_once() on a connection the caller took out of autocommit,
    with no atomic block: the open transaction is theirs just the same.
    Inside an atomic block Django turns autocommit off too, so this is the
    check that also covers the test above.
    """
    written = []

    def raises(self, db_task, **fields):
        written.append(fields["status"])
        raise OperationalError(GONE)

    monkeypatch.setattr(Worker, "_write_outcome", raises)
    echo.enqueue("x")

    transaction.set_autocommit(False)
    try:
        driver = connection.connection
        with pytest.raises(OperationalError):
            worker.run_once()
        assert connection.connection is driver
    finally:
        transaction.rollback()
        transaction.set_autocommit(True)

    assert written == [OxTask.Status.SUCCESSFUL]
    assert probes == []


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
def test_when_the_second_write_fails_too_one_line_says_so(
    worker, monkeypatch, caplog, finished_results, task
):
    """
    Both writes raise on a connection that is gone: run_once() returns, the
    row is left as the claim wrote it, and one ERROR line says so, instead
    of the error escaping as "Unhandled error executing task".
    """
    written = []

    def raises(self, db_task, **fields):
        written.append(fields["status"])
        raise OperationalError(GONE)

    monkeypatch.setattr(Worker, "_write_outcome", raises)
    connection_goes(monkeypatch)
    args = ("x",) if task is echo else ()
    result = task.enqueue(*args)

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    status = OxTask.Status.SUCCESSFUL if task is echo else OxTask.Status.READY
    assert written == [status, status]
    assert outcome_events(
        caplog,
        "task_outcome_unrecorded",
        "task_outcome_reconnected",
        "task_lease_lost",
        "task_succeeded",
        "task_retrying",
    ) == [(logging.ERROR, "task_outcome_unrecorded")]
    stored = OxTask.objects.get(id=result.id)
    assert (stored.status, stored.errors) == (OxTask.Status.RUNNING, [])
    assert finished_results == []


@pytest.mark.parametrize(
    ("task", "max_attempts"),
    [(echo, 3), (fail_always, 3), (fail_always, 1)],
    ids=["success", "retry", "last-attempt"],
)
def test_a_write_that_committed_before_the_connection_went_is_taken_as_written(
    monkeypatch, caplog, finished_results, task, max_attempts
):
    """
    The first write commits and then raises on a connection that is gone.
    The new connection finds the row holding it, so nothing is written a
    second time, one error record and one backoff stand, and the attempt
    reports its outcome once rather than a lease it did not lose.
    """
    worker = Worker(backoff_initial=900, backoff_max=900, poll_interval=0.05)
    real = Worker._write_outcome
    written = []

    def commits_then_raises(self, db_task, **fields):
        written.append(fields["status"])
        # The caller never sees the landed write's values on its instance,
        # as when the reply is lost.
        assert real(self, copy.copy(db_task), **fields)
        connection_goes(monkeypatch)
        raise OperationalError(GONE)

    monkeypatch.setattr(Worker, "_write_outcome", commits_then_raises)
    args = ("x",) if task is echo else ()
    result = task.enqueue(*args)
    OxTask.objects.filter(id=result.id).update(max_attempts=max_attempts)
    before = timezone.now()

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    stored = OxTask.objects.get(id=result.id)
    assert written == [stored.status]
    assert (stored.attempts, stored.lease_epoch) == (1, 1)
    reported = outcome_events(
        caplog,
        "task_outcome_unrecorded",
        "task_outcome_reconnected",
        "task_lease_lost",
        "task_succeeded",
        "task_retrying",
        "task_failed",
    )
    (reconnected,) = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "task_outcome_reconnected"
    ]
    assert reconnected.already_written is True
    assert FOUND_IT_WRITTEN in reconnected.getMessage()
    if task is echo:
        assert stored.status == OxTask.Status.SUCCESSFUL
        assert stored.return_value == "x"
        expected = "task_succeeded"
        assert [r.status for r in finished_results] == ["SUCCESSFUL"]
    elif max_attempts == 3:
        assert stored.status == OxTask.Status.READY
        assert len(stored.errors) == 1
        assert 890 < (stored.run_after - before).total_seconds() < 1000
        expected = "task_retrying"
        assert finished_results == []
    else:
        assert stored.status == OxTask.Status.FAILED
        assert len(stored.errors) == 1
        expected = "task_failed"
        assert [r.status for r in finished_results] == ["FAILED"]
    assert [event for _, event in reported] == ["task_outcome_reconnected", expected]


def test_a_second_write_after_the_row_changed_hands_writes_nothing(
    worker, monkeypatch, caplog, finished_results
):
    """
    Between the two writes another worker's claim moved the epoch: the
    second write is fenced like the first, matches nothing, and the attempt
    says it lost its lease.
    """
    real = Worker._write_outcome
    written = []

    def moved_then_raises(self, db_task, **fields):
        written.append(fields["status"])
        if len(written) == 1:
            OxTask.objects.filter(pk=db_task.pk).update(
                lease_epoch=F("lease_epoch") + 2, attempts=2, locked_by="another"
            )
            connection_goes(monkeypatch)
            raise OperationalError(GONE)
        return real(self, db_task, **fields)

    monkeypatch.setattr(Worker, "_write_outcome", moved_then_raises)
    result = echo.enqueue("x")

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    assert written == [OxTask.Status.SUCCESSFUL, OxTask.Status.SUCCESSFUL]
    stored = OxTask.objects.get(id=result.id)
    assert (stored.status, stored.lease_epoch, stored.locked_by) == (
        OxTask.Status.RUNNING,
        3,
        "another",
    )
    assert stored.return_value is None
    assert outcome_events(
        caplog,
        "task_outcome_unrecorded",
        "task_outcome_reconnected",
        "task_lease_lost",
        "task_succeeded",
    ) == [(logging.WARNING, "task_lease_lost")]
    assert finished_results == []


# -- when the pool is swept -------------------------------------------------------


class Sweeps(list):
    """The aliases whose pool was swept, in order; `error`, check() raises it."""

    error: BaseException | None = None


@pytest.fixture
def sweeps(monkeypatch):
    """
    Every alias is taken for pooled, with a stand-in pool whose check()
    records the sweep, so these run the same on every database. What a real
    pool's check() does is the end-to-end tests' business.
    """
    swept = Sweeps()

    class StandIn:
        def __init__(self, alias):
            self.alias = alias

        def check(self):
            swept.append(self.alias)
            if swept.error is not None:
                raise swept.error

    monkeypatch.setattr(
        "django_ox.worker._connection_pool", lambda conn: StandIn(conn.alias)
    )
    return swept


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
def test_an_ordinary_outcome_sweeps_no_pool(worker, sweeps, task):
    args = ("x",) if task is echo else ()
    task.enqueue(*args)

    assert worker.run_once()

    assert sweeps == []


def test_a_connection_that_still_answers_after_an_error_sweeps_no_pool(worker, sweeps):
    result = survives_a_statement_error.enqueue()

    assert worker.run_once()

    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
    assert sweeps == []


@pytest.mark.django_db(transaction=True, databases=["default", "alt"])
@pytest.mark.parametrize("alias", ["default", "alt"])
@pytest.mark.parametrize(
    ("flagged", "usable", "atomic", "swept"),
    [
        pytest.param(False, False, False, False, id="no-error"),
        pytest.param(True, True, False, False, id="error-still-answers"),
        pytest.param(True, False, False, True, id="error-and-dead"),
        pytest.param(True, False, True, False, id="inside-atomic"),
    ],
)
def test_only_closing_a_dead_connection_sweeps_its_pool(
    worker, monkeypatch, sweeps, alias, flagged, usable, atomic, swept
):
    """
    The drop before the write sweeps the pool of the alias it closed, and
    only when it closed one.
    """
    conn = connections[alias]
    conn.ensure_connection()
    monkeypatch.setattr(type(conn), "is_usable", lambda self: usable)
    block = transaction.atomic(using=alias) if atomic else nullcontext()
    with block:
        conn.errors_occurred = flagged
        try:
            worker._discard_unusable_connections()
        finally:
            conn.errors_occurred = False
    assert sweeps == ([alias] if swept else [])


@pytest.mark.parametrize(
    "error",
    [RuntimeError("pool broke"), OperationalError(GONE)],
    ids=["other", "database"],
)
def test_a_sweep_that_raises_after_the_drop_does_not_escape(
    worker, monkeypatch, sweeps, error
):
    conn = connections["default"]
    conn.ensure_connection()
    monkeypatch.setattr(type(conn), "is_usable", lambda self: False)
    sweeps.error = error
    conn.errors_occurred = True
    try:
        worker._discard_unusable_connections()
    finally:
        conn.errors_occurred = False

    assert conn.connection is None
    assert sweeps == ["default"]


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
def test_the_pool_is_swept_after_the_close_and_before_the_second_try(
    worker, monkeypatch, caplog, task
):
    """
    The first write finds the connection gone; the pool is swept once, after
    the close, before the second try reads the row and writes.
    """
    order = []
    real_write = Worker._write_outcome
    real_read = Worker._outcome_already_written
    wrapper = type(connections["default"])
    real_close = wrapper.close

    class StandIn:
        def check(self):
            order.append("sweep")

    def write(self, db_task, **fields):
        order.append("write")
        if order.count("write") == 1:
            connection_goes(monkeypatch)
            raise OperationalError(GONE)
        return real_write(self, db_task, **fields)

    def read(self, *args):
        order.append("read")
        return real_read(self, *args)

    def close(self):
        order.append("close")
        return real_close(self)

    monkeypatch.setattr("django_ox.worker._connection_pool", lambda conn: StandIn())
    monkeypatch.setattr(Worker, "_write_outcome", write)
    monkeypatch.setattr(Worker, "_outcome_already_written", read)
    monkeypatch.setattr(wrapper, "close", close)
    args = ("x",) if task is echo else ()
    result = task.enqueue(*args)

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    assert order[:5] == ["write", "close", "sweep", "read", "write"], order
    assert order.count("sweep") == 1
    status = OxTask.Status.SUCCESSFUL if task is echo else OxTask.Status.READY
    assert OxTask.objects.get(id=result.id).status == status
    assert outcome_events(caplog, "task_outcome_reconnected") == [
        (logging.WARNING, "task_outcome_reconnected")
    ]


@pytest.mark.parametrize("task", [echo, fail_always], ids=["success", "failure"])
@pytest.mark.parametrize(
    "error",
    [RuntimeError("pool broke"), OperationalError(GONE)],
    ids=["other", "database"],
)
def test_a_sweep_that_raises_neither_escapes_nor_adds_a_write(
    worker, monkeypatch, caplog, sweeps, finished_results, task, error
):
    """
    The sweep raises between the two tries: the second try still goes, once,
    and nothing about the outcome changes; there is no third.
    """
    real = Worker._write_outcome
    written = []

    def raises_once(self, db_task, **fields):
        written.append(fields["status"])
        if len(written) == 1:
            connection_goes(monkeypatch)
            raise OperationalError(GONE)
        return real(self, db_task, **fields)

    monkeypatch.setattr(Worker, "_write_outcome", raises_once)
    sweeps.error = error
    args = ("x",) if task is echo else ()
    result = task.enqueue(*args)

    with caplog.at_level(logging.INFO, logger="django_ox"):
        assert worker.run_once()

    status = OxTask.Status.SUCCESSFUL if task is echo else OxTask.Status.READY
    assert written == [status, status]
    assert sweeps == ["default"]
    stored = OxTask.objects.get(id=result.id)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (status, 1, 1)
    assert len(stored.errors) == (0 if task is echo else 1)
    assert [
        event
        for _, event in outcome_events(
            caplog,
            "task_outcome_unrecorded",
            "task_outcome_reconnected",
            "task_lease_lost",
            "task_succeeded",
            "task_retrying",
        )
    ] == [
        "task_outcome_reconnected",
        "task_succeeded" if task is echo else "task_retrying",
    ]


# -- when the poll loop sweeps -----------------------------------------------------


@pytest.fixture
def batch_worker(settings):
    """A worker that stops on the first pass that finds nothing to claim."""
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {},
        }
    }
    return Worker(backoff_initial=0, poll_interval=0.02, reap_interval=0.0, batch=True)


def run_the_batch(worker, caplog):
    with caplog.at_level(logging.INFO, logger="django_ox"):
        thread = start_worker_thread(worker)
        thread.join(timeout=30)
    assert not thread.is_alive(), "the batch worker never finished"


def claims_once_broken(worker, monkeypatch, order, error=OperationalError):
    """The worker's first claim raises `error`; every claim is noted."""
    real = worker.claim_one

    def claim_one():
        order.append("claim")
        if order.count("claim") == 1:
            raise error(GONE)
        return real()

    monkeypatch.setattr(worker, "claim_one", claim_one)


def poll_events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def test_a_failed_poll_pass_sweeps_the_worker_pool_and_replays_nothing(
    batch_worker, monkeypatch, caplog
):
    """
    A pass the database interrupted sweeps the worker alias's pool once,
    after its own cleanup, and then waits out the poll interval as before:
    the claim is not tried again until the next pass.
    """
    order = []

    class StandIn:
        def __init__(self, alias):
            self.alias = alias

        def check(self):
            order.append(f"sweep:{self.alias}")

    real_wait = batch_worker._stop.wait

    def wait(timeout=None):
        order.append("wait")
        return real_wait(timeout)

    monkeypatch.setattr(
        "django_ox.worker._connection_pool", lambda conn: StandIn(conn.alias)
    )
    monkeypatch.setattr(batch_worker._stop, "wait", wait)
    claims_once_broken(batch_worker, monkeypatch, order)
    result = echo.enqueue("x")

    run_the_batch(batch_worker, caplog)

    assert order[:4] == ["claim", "sweep:default", "wait", "claim"], order
    assert order.count("sweep:default") == 1
    assert len(poll_events(caplog, "worker_poll_failed")) == 1
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL


def test_a_healthy_poll_pass_sweeps_no_pool(batch_worker, sweeps, caplog):
    result = echo.enqueue("x")

    run_the_batch(batch_worker, caplog)

    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
    assert poll_events(caplog, "worker_batch_empty")
    assert sweeps == []


@pytest.mark.parametrize(
    "error",
    [RuntimeError("pool broke"), OperationalError(GONE)],
    ids=["other", "database"],
)
def test_a_sweep_that_raises_does_not_stop_the_poll_loop(
    batch_worker, monkeypatch, caplog, sweeps, error
):
    sweeps.error = error
    order = []
    claims_once_broken(batch_worker, monkeypatch, order)
    result = echo.enqueue("x")

    run_the_batch(batch_worker, caplog)

    assert sweeps == ["default"]
    assert len(poll_events(caplog, "worker_poll_failed")) == 1
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
    assert poll_events(caplog, "worker_batch_empty")
