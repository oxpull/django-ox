"""
How many database connections `ox_worker` opens, and the hint it logs when
every task attempt opens one.

At the default CONN_MAX_AGE of 0 Django closes a thread's connections after
each use, and a task attempt is one use: the execution thread opens a new
connection for every attempt, and the task's queries and its outcome write
ride that one. The poll loop's connection is kept for the life of the process.
With CONN_MAX_AGE set, or with Django's PostgreSQL pool, the execution thread
keeps its connection. Nothing here changes either behaviour; these tests
state them and fail if a change moves them.

Every test runs the real `ox_worker --batch` command in a process of its own
against tasks queued beforehand. The driver's connect call is wrapped in that
process (tests/connection_counter.py), so the count is the connections the
worker opened, by thread, and not the ones the test process opens to watch the
rows. PostgreSQL's own session counter is read as a second witness.
"""

import contextlib
import copy
import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection, connections

from django_ox.models import OxTask
from django_ox.worker import Worker

from . import connection_counter
from .connection_count_tasks import count_users, report_session, tick
from .dead_connection_tasks import end_session, read_notes

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="runs the worker as a process"),
]

on_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql", reason="counts PostgreSQL sessions"
)
ends_a_connection = pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="has the server end a connection, with pg_terminate_backend or KILL",
)
with_psycopg_pool = pytest.mark.skipif(
    connection.vendor != "postgresql"
    or importlib.util.find_spec("psycopg_pool") is None,
    reason="Django's pool is PostgreSQL only and needs psycopg_pool",
)

#: Small enough for CI, large enough that one connection per attempt cannot be
#: mistaken for a fixed cost.
TASKS = 30

POOL = {"min_size": 1, "max_size": 6}
LIMIT = 90.0
HINT = "worker_connection_hint"
HINT_TEXT = (
    "Worker %s: database %r opens a new execution connection per "
    "attempt with CONN_MAX_AGE = 0 on PostgreSQL or MySQL. "
    "To retain connections, set CONN_MAX_AGE above 0 and "
    "CONN_HEALTH_CHECKS = True in worker settings. "
    "Tasks must restore session state they change; a server-ended "
    "idle session can cost a querying task an extra attempt. "
    "PostgreSQL's Django pool is another option and requires "
    "CONN_MAX_AGE = 0."
)


def project(tmp_path: Path, mode: str, name: str = "w") -> tuple[Path, Path]:
    """
    A project whose settings are this run's with the default database in
    `mode`: "default" (CONN_MAX_AGE 0, no pool), "persistent" (CONN_MAX_AGE
    600) or "pool" (Django's PostgreSQL pool). Returns it with the file its
    worker records every connect call in.
    """
    root = tmp_path / name
    (root / "countproj").mkdir(parents=True)
    (root / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'countproj.settings')\n"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (root / "countproj" / "__init__.py").write_text("")
    counts = tmp_path / f"{name}.connects"
    lines = [
        "import sys",
        f"sys.path[:0] = [{str(REPO / 'src')!r}, {str(REPO)!r}]",
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403",
        "from tests.connection_counter import install",
        f"install({str(counts)!r}, {connection.vendor!r})",
        "_db = DATABASES['default']",
        "_opts = _db.get('OPTIONS', {})",
        "_db['OPTIONS'] = {k: v for k, v in _opts.items() if k != 'pool'}",
        f"_db['CONN_MAX_AGE'] = {600 if mode == 'persistent' else 0}",
    ]
    if mode == "pool":
        lines.append(f"_db['OPTIONS']['pool'] = {POOL!r}")
    (root / "countproj" / "settings.py").write_text("\n".join(lines) + "\n")
    return root, counts


def start(
    root: Path,
    tmp_path: Path,
    *flags: str,
    name: str = "w",
    options: dict | None = None,
):
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "countproj.settings"
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    env["OX_TEST_LOG_FORMAT"] = "%(event)s %(message)s"
    if options is not None:
        env["OX_TEST_TASKS_OPTIONS"] = json.dumps(options)
    log = tmp_path / f"{name}.log"
    with log.open("wb") as out:
        proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "manage.py", "ox_worker", "--interval", "0.05", *flags],
            cwd=root,
            env=env,
            stdout=out,
            stderr=out,
        )
    return proc, log


def finish(proc: subprocess.Popen, log: Path) -> str:
    try:
        code = proc.wait(timeout=LIMIT)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        pytest.fail(f"the worker did not exit by itself:\n{log.read_text()}")
    output = log.read_text()
    assert code == 0, output
    return output


def run_batch(tmp_path: Path, mode: str, *flags: str) -> tuple[Path, str]:
    """Run `ox_worker --batch` in `mode`; the connect log and the output."""
    root, counts = project(tmp_path, mode)
    proc, log = start(root, tmp_path, "--batch", "--concurrency", "1", *flags)
    return counts, finish(proc, log)


def hints(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.startswith(f"{HINT} ")]


def sessions_opened() -> int | None:
    """PostgreSQL's count of sessions it has started on the test database."""
    if connection.vendor != "postgresql":
        return None
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_stat_force_next_flush()")
        cursor.execute(
            "SELECT sessions FROM pg_stat_database WHERE datname = current_database()"
        )
        return int(cursor.fetchone()[0])


#: Django's MySQL backend reads the server's version once per thread's
#: wrapper, and when that is the thread's first use it does so on a connection
#: of its own, opened and closed before the ordinary one. It is a cost of the
#: first attempt on a thread, not of every attempt, so a count on MySQL may
#: exceed the expected one by up to the number of threads that ran tasks.
EXTRA_ON_FIRST_USE = 1 if connection.vendor == "mysql" else 0


def assert_opened(found, expected, output):
    """`found` connections where `expected` are, give or take the one above."""
    assert expected <= found <= expected + EXTRA_ON_FIRST_USE, (
        f"{found} connections, expected {expected}\n{output}"
    )


def all_succeeded_once():
    rows = list(OxTask.objects.all())
    assert Counter(row.status for row in rows) == {OxTask.Status.SUCCESSFUL: len(rows)}
    assert {row.attempts for row in rows} == {1}


# -- the counts -----------------------------------------------------------------


def execution_threads(counts: Path) -> Counter[str]:
    """Connects by thread, with every execution thread under one name."""
    return Counter(
        "execution" if name.startswith("ox_") else name
        for _, name in connection_counter.read(counts)
    )


class TestConnectionsPerAttempt:
    def test_zero_tasks_cost_one_connection(self, tmp_path):
        """The fixed cost of starting a worker, which every count below adds to."""
        counts, output = run_batch(tmp_path, "default")
        assert execution_threads(counts) == {"MainThread": 1}, output

    @pytest.mark.skipif(
        connection.vendor == "sqlite",
        reason="SQLite opens a file, not a server connection, and logs no hint",
    )
    def test_the_default_opens_one_connection_per_attempt(self, tmp_path):
        before = sessions_opened()
        for i in range(TASKS):
            tick.enqueue(str(tmp_path / "n.jsonl"), index=i)
        counts, output = run_batch(tmp_path, "default")
        all_succeeded_once()
        found = execution_threads(counts)
        # The poll loop keeps the connection it started with; each attempt's
        # thread opens its own and closes it when the attempt ends.
        assert found["MainThread"] == 1, output
        assert_opened(found["execution"], TASKS, output)
        assert_opened(sum(found.values()), TASKS + 1, output)
        if before is not None:
            assert sessions_opened() - before == TASKS + 1

    @pytest.mark.skipif(
        connection.vendor != "sqlite", reason="the SQLite arm of the same count"
    )
    def test_sqlite_default_also_opens_one_per_attempt(self, tmp_path):
        for i in range(TASKS):
            tick.enqueue(str(tmp_path / "n.jsonl"), index=i)
        counts, output = run_batch(tmp_path, "default")
        all_succeeded_once()
        assert execution_threads(counts) == {
            "MainThread": 1,
            "execution": TASKS,
        }, output
        assert not hints(output)

    @pytest.mark.parametrize("mode", ["persistent", "pool"])
    def test_a_kept_connection_is_opened_once(self, tmp_path, mode):
        if mode == "pool" and (
            connection.vendor != "postgresql"
            or importlib.util.find_spec("psycopg_pool") is None
        ):
            pytest.skip("Django's pool is PostgreSQL only and needs psycopg_pool")
        before = sessions_opened()
        for i in range(TASKS):
            tick.enqueue(str(tmp_path / "n.jsonl"), index=i)
        counts, output = run_batch(tmp_path, mode)
        all_succeeded_once()
        found = execution_threads(counts)
        if mode == "persistent":
            assert found["MainThread"] == 1, output
            assert_opened(found["execution"], 1, output)
        else:
            # The pool opens its connections on threads of its own.
            assert found["execution"] == 0, output
        assert_opened(sum(found.values()), 2, output)
        assert not hints(output)
        if before is not None:
            assert sessions_opened() - before == 2

    def test_task_queries_add_no_connections(self, tmp_path):
        """The ORM work and the outcome write share the attempt's connection."""
        for _ in range(TASKS):
            count_users.enqueue(str(tmp_path / "n.jsonl"))
        counts, output = run_batch(tmp_path, "default")
        all_succeeded_once()
        found = execution_threads(counts)
        assert_opened(sum(found.values()), TASKS + 1, output)

    def test_closing_and_reopening_is_counted_each_time(self, tmp_path):
        """
        The negative control: a process that closes its connection and opens
        another five times is seen to, so a count that stays low is not a
        counter that cannot see.
        """
        root, counts = project(tmp_path, "persistent")
        before = sessions_opened()
        env = dict(os.environ)
        env["DJANGO_SETTINGS_MODULE"] = "countproj.settings"
        env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
        script = (
            "import django; django.setup()\n"
            "from django.db import connection\n"
            "for _ in range(5):\n"
            "    connection.close(); connection.ensure_connection()\n"
        )
        subprocess.run(  # noqa: S603
            [sys.executable, "-c", script], cwd=root, env=env, check=True, timeout=60
        )
        assert execution_threads(counts) == {"MainThread": 5}
        if before is not None:
            assert sessions_opened() - before == 5


# -- the hint -------------------------------------------------------------------


@pytest.mark.skipif(
    connection.vendor == "sqlite", reason="SQLite opens a file and gets no hint"
)
class TestTheHintOnTheRealCommand:
    def test_the_default_logs_it_once(self, tmp_path):
        tick.enqueue(str(tmp_path / "n.jsonl"))
        _, output = run_batch(tmp_path, "default")
        assert len(hints(output)) == 1, output

    @pytest.mark.parametrize("mode", ["persistent", "pool"])
    def test_a_kept_connection_logs_none(self, tmp_path, mode):
        if mode == "pool" and (
            connection.vendor != "postgresql"
            or importlib.util.find_spec("psycopg_pool") is None
        ):
            pytest.skip("Django's pool is PostgreSQL only and needs psycopg_pool")
        tick.enqueue(str(tmp_path / "n.jsonl"))
        _, output = run_batch(tmp_path, mode)
        assert not hints(output), output

    def test_it_does_not_stop_a_worker_with_nothing_to_do(self, tmp_path):
        _, output = run_batch(tmp_path, "default")
        assert len(hints(output)) == 1, output
        assert "worker_stopped" in output


ALIAS = "hinted"


@pytest.fixture
def add_alias():
    def add(**overrides):
        base = copy.deepcopy(connections.settings["default"])
        connections.settings[ALIAS] = {**base, **overrides}

    yield add
    # The wrapper is built once per thread and kept, so a later test would
    # get the first one's engine. Without the engine's driver installed no
    # wrapper was built, and the delete raises AttributeError.
    with contextlib.suppress(KeyError, AttributeError):
        del connections[ALIAS]
    connections.settings.pop(ALIAS, None)


POSTGRESQL = "django.db.backends.postgresql"
MYSQL = "django.db.backends.mysql"
SQLITE = "django.db.backends.sqlite3"


def needs_driver(vendor):
    """Skip unless Django could build a wrapper for `vendor`."""
    if vendor == "postgresql":
        pytest.importorskip("psycopg")
        return
    if importlib.util.find_spec("MySQLdb") is None:
        pymysql = pytest.importorskip("pymysql")
        pymysql.install_as_MySQLdb()


def hint_records(caplog):
    return [r for r in caplog.records if getattr(r, "event", None) == HINT]


class TestWhenTheHintIsGiven:
    """
    The rule, on aliases that are never connected to: it reads settings and the
    engine's name. An alias is a PostgreSQL or MySQL one when the driver is
    importable.
    """

    def hint(self, caplog, **alias):
        worker = Worker(db_alias=ALIAS)
        with caplog.at_level(logging.INFO, logger="django_ox"):
            worker._hint_if_every_task_reconnects()
        return hint_records(caplog)

    @pytest.mark.parametrize(
        ("engine", "vendor"), [(POSTGRESQL, "postgresql"), (MYSQL, "mysql")]
    )
    def test_closing_after_use_without_a_pool_gets_it(
        self, add_alias, caplog, engine, vendor
    ):
        needs_driver(vendor)
        add_alias(ENGINE=engine, CONN_MAX_AGE=0, OPTIONS={}, TEST={})
        (record,) = self.hint(caplog)
        assert record.levelno == logging.INFO
        assert record.database == ALIAS
        assert record.vendor == vendor
        assert record.worker_id
        assert record.getMessage() == HINT_TEXT % (record.worker_id, ALIAS)

    def test_a_missing_setting_is_the_default_and_gets_it(self, add_alias, caplog):
        pytest.importorskip("psycopg")
        add_alias(ENGINE=POSTGRESQL, OPTIONS={}, TEST={})
        connections.settings[ALIAS].pop("CONN_MAX_AGE", None)
        assert len(self.hint(caplog)) == 1

    @pytest.mark.parametrize("age", [1, 600, None])
    def test_keeping_connections_open_gets_none(self, add_alias, caplog, age):
        pytest.importorskip("psycopg")
        add_alias(ENGINE=POSTGRESQL, CONN_MAX_AGE=age, OPTIONS={}, TEST={})
        assert not self.hint(caplog)

    @pytest.mark.parametrize("pool", [True, {"max_size": 3}])
    def test_a_pool_gets_none(self, add_alias, caplog, pool):
        pytest.importorskip("psycopg")
        add_alias(ENGINE=POSTGRESQL, CONN_MAX_AGE=0, OPTIONS={"pool": pool}, TEST={})
        assert not self.hint(caplog)

    @pytest.mark.parametrize("pool", [None, False, {}])
    def test_an_option_that_is_not_a_pool_does_not_count(self, add_alias, caplog, pool):
        pytest.importorskip("psycopg")
        add_alias(ENGINE=POSTGRESQL, CONN_MAX_AGE=0, OPTIONS={"pool": pool}, TEST={})
        assert len(self.hint(caplog)) == 1

    def test_mysql_has_no_pool_to_set(self, add_alias, caplog):
        needs_driver("mysql")
        add_alias(ENGINE=MYSQL, CONN_MAX_AGE=0, OPTIONS={"pool": True}, TEST={})
        assert len(self.hint(caplog)) == 1

    def test_sqlite_gets_none(self, add_alias, caplog):
        add_alias(ENGINE=SQLITE, CONN_MAX_AGE=0, OPTIONS={}, TEST={})
        assert not self.hint(caplog)

    def test_an_unsupported_vendor_gets_none(self, add_alias, caplog, monkeypatch):
        add_alias(ENGINE=SQLITE, CONN_MAX_AGE=0, OPTIONS={}, TEST={})
        monkeypatch.setattr(connections[ALIAS], "vendor", "oracle")
        assert not self.hint(caplog)

    def test_it_opens_no_connection(self, add_alias, caplog):
        pytest.importorskip("psycopg")
        add_alias(
            ENGINE=POSTGRESQL,
            CONN_MAX_AGE=0,
            OPTIONS={},
            HOST="127.0.0.1",
            PORT="1",
            TEST={},
        )
        assert len(self.hint(caplog)) == 1
        assert connections[ALIAS].connection is None


# -- failure and shared queues --------------------------------------------------


def wait_until(predicate, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    pytest.fail("the condition never held")


@ends_a_connection
@pytest.mark.parametrize("queries", [False, True], ids=["no-query", "query"])
def test_a_persistent_worker_recovers_when_its_idle_session_is_ended(tmp_path, queries):
    """
    The connection a persistent worker keeps between tasks is ended by the
    server while it is idle, as a failover or an idle timeout does. Nothing
    flagged it, so the next task finds it dead. The worker opens a new one,
    and every task still completes once its attempts allow.
    """
    notes = str(tmp_path / "notes.jsonl")
    root, counts = project(tmp_path, "persistent")
    # Claims, not tasks: the retry of the one attempt that fails is a ninth.
    claims = "9" if queries else "8"
    proc, log = start(
        root,
        tmp_path,
        "--max-tasks",
        claims,
        "--concurrency",
        "1",
        options={"BACKOFF_INITIAL": 0.05, "BACKOFF_MAX": 0.05},
    )
    try:
        first = [report_session.enqueue(notes) for _ in range(3)]
        wait_until(
            lambda: all(
                OxTask.objects.get(id=r.id).status == OxTask.Status.SUCCESSFUL
                for r in first
            )
        )
        sessions = {n["session"] for n in read_notes(notes)}
        assert len(sessions) == 1, "a persistent worker used more than one session"
        (session,) = sessions
        end_session(connection, session)
        after = [(report_session if queries else tick).enqueue(notes) for _ in range(5)]
        output = finish(proc, log)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    stored = [OxTask.objects.get(id=r.id) for r in first + after]
    assert {s.status for s in stored} == {OxTask.Status.SUCCESSFUL}, output
    assert "Unhandled error" not in output
    extra_attempts = sum(s.attempts - 1 for s in stored)
    # With a query, the first task to touch the dead session fails it and the
    # retry runs on a new one. Without, the outcome write finds it and goes
    # again on a new connection, and no attempt is spent.
    assert extra_attempts == (1 if queries else 0), output
    found = execution_threads(counts)
    assert found["MainThread"] == 1
    # The session that was kept, and the one that replaced it.
    assert_opened(found["execution"], 2, output)
    assert all(s.lease_epoch == s.attempts for s in stored)


@ends_a_connection
def test_each_default_attempt_has_a_session_of_its_own(tmp_path):
    """
    Control for the above: at the default nothing is kept between tasks, so
    there is no idle session for the server to end and no state to leak from
    one task to the next.
    """
    notes = str(tmp_path / "notes.jsonl")
    for _ in range(6):
        report_session.enqueue(notes)
    counts, output = run_batch(tmp_path, "default")
    all_succeeded_once()
    assert len({n["session"] for n in read_notes(notes)}) == 6, output
    assert_opened(sum(execution_threads(counts).values()), 7, output)


def test_workers_with_and_without_persistence_share_a_queue(tmp_path):
    """
    One worker keeps its connection and one reconnects for every attempt,
    both on one queue. Every task completes, each attempt runs once, and the
    lease fencing is as for any two workers: one claim, one epoch.
    """
    notes = str(tmp_path / "notes.jsonl")
    total = 40
    for i in range(total):
        tick.enqueue(notes, 0.05, index=i)
    kept, kept_counts = project(tmp_path, "persistent", "kept")
    plain, plain_counts = project(tmp_path, "default", "plain")
    flags = ("--batch", "--concurrency", "1")
    kept_proc, kept_log = start(kept, tmp_path, *flags, name="kept")
    plain_proc, plain_log = start(plain, tmp_path, *flags, name="plain")
    kept_out = finish(kept_proc, kept_log)
    plain_out = finish(plain_proc, plain_log)

    rows = list(OxTask.objects.all())
    assert len(rows) == total
    assert {r.status for r in rows} == {OxTask.Status.SUCCESSFUL}, kept_out + plain_out
    assert {(r.attempts, r.lease_epoch, len(r.worker_ids)) for r in rows} == {(1, 1, 1)}
    ran = read_notes(notes)
    assert sorted(n["index"] for n in ran) == list(range(total))
    by_pid = Counter(n["pid"] for n in ran)
    assert set(by_pid) == {kept_proc.pid, plain_proc.pid}, "a worker never ran a task"
    # The one that reconnects opens a connection per attempt plus its poll
    # loop's; the one that keeps its connection opens two whatever it ran.
    plain_connects = sum(connection_counter.by_thread(plain_counts).values())
    assert_opened(plain_connects, by_pid[plain_proc.pid] + 1, plain_out)
    kept_connects = sum(connection_counter.by_thread(kept_counts).values())
    assert_opened(kept_connects, 2, kept_out)
