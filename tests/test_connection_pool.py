"""
The worker against Django's PostgreSQL connection pool (DATABASES OPTIONS
"pool"), which every thread of a worker process draws from.

The end-to-end tests pool the worker subprocess only. The test process keeps
its own unpooled connection to watch the rows, so they need psycopg_pool
installed and nothing more. Each fills the pool by construction, one
connection for the poll loop and one per task thread, with tasks that take
their thread's connection and keep it until the test lets go. What they show
follows from the order of events, not from how fast the machine is.
"""

import copy
import importlib.metadata
import importlib.util
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from types import MappingProxyType

import pytest
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, OperationalError, connection, connections
from django.db.models.functions import Now
from django.utils import timezone

import django_ox.worker as worker_module
from django_ox.models import OxTask
from django_ox.timeouts import RECYCLE_EXIT_CODE
from django_ox.worker import Worker, _outside_the_pool, _RenewalReport, _Watch

from .conftest import start_worker_thread, wait_for
from .tasks import query_and_hold, query_then_sleep, slow

REPO = Path(__file__).resolve().parent.parent
POSTGRESQL = "django.db.backends.postgresql"
SQLITE = "django.db.backends.sqlite3"
ALIAS = "pooled"

on_postgresql = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="Django's connection pool is PostgreSQL only",
)
with_psycopg_pool = pytest.mark.skipif(
    importlib.util.find_spec("psycopg_pool") is None,
    reason="needs psycopg_pool: pip install 'psycopg[pool]'",
)

# Building a PostgreSQL wrapper imports the driver; nothing here connects.
needs_psycopg = pytest.mark.skipif(
    importlib.util.find_spec("psycopg") is None, reason="needs psycopg"
)

# From 3.2 psycopg resolves host names itself, through socket.getaddrinfo,
# where a test can answer for one. Before, libpq resolves them in C.
PSYCOPG = (
    tuple(map(int, importlib.metadata.version("psycopg").split(".")[:2]))
    if importlib.util.find_spec("psycopg") is not None
    else (0, 0)
)
resolves_in_python = pytest.mark.skipif(
    PSYCOPG < (3, 2),
    reason="needs psycopg 3.2 or later, which resolves host names in Python",
)

# A control for the unpooled case has nothing to show when the run's own
# database is pooled: the worker it starts is then pooled as well.
without_a_pool = pytest.mark.skipif(
    bool(connections.settings[DEFAULT_DB_ALIAS].get("OPTIONS", {}).get("pool")),
    reason="a no-pool control, and this run's default database is pooled",
)


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def lines(path: Path) -> list[str]:
    return path.read_text().split() if path.exists() else []


def text(path: Path) -> str:
    return path.read_text() if path.exists() else ""


@pytest.fixture
def add_alias():
    """
    Add a database alias to the loaded settings, as a copy of the default
    one with the given keys replaced. Afterwards closes and forgets this
    thread's wrapper for it and any pool built for it, and takes the alias
    out of the settings.

    It takes the alias out itself, rather than through monkeypatch, which
    an autouse fixture sets up first and so tears down last. A test case
    that pytest-django tears down while the alias is still configured
    makes a wrapper for it on this thread, and that wrapper would be the
    one the next test got for the alias. Requested after unguarded_db, as
    here, it is torn down before that test case.
    """

    def add(**overrides):
        conf = {**copy.deepcopy(connections.settings["default"]), **overrides}
        connections.settings[ALIAS] = conf
        return conf

    yield add
    if ALIAS not in connections.settings:
        return
    for wrapper in connections.all(initialized_only=True):
        if wrapper.alias == ALIAS:
            wrapper.close()
            del connections[ALIAS]
    pools = getattr(connections.create_connection(ALIAS), "_connection_pools", {})
    if ALIAS in pools:
        pools.pop(ALIAS).close()
    del connections.settings[ALIAS]


@pytest.fixture
def unguarded_db(transactional_db, django_db_blocker):
    """
    The test database as a transactional test has it, set up for the run and
    flushed afterwards, with its test case's guard lifted. The guard refuses
    every thread's connection to an alias the test case was not given at
    setup, which an alias added by the test cannot be. It works by patching
    ensure_connection, and unblock() puts the real method back until the
    test ends.

    Asking for transactional_db rather than for the database setup alone is
    what gets the database created when no other test in the run needs it,
    as when this test is selected by itself.
    """
    with django_db_blocker.unblock():
        yield


def pooled_project(
    tmp_path: Path, *, pool: object, database: Mapping[str, object] | None = None
) -> Path:
    """
    A project whose settings are this suite's with the default database
    pooled, and with the keys in `database` replaced. This checkout's src
    goes first on the path, so the worker runs the code under test even
    where an install elsewhere would win from the project's directory.
    """
    project = tmp_path / "proj"
    (project / "pooledproj").mkdir(parents=True)
    (project / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pooledproj.settings')\n"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (project / "pooledproj" / "__init__.py").write_text("")
    (project / "pooledproj" / "settings.py").write_text(
        "import sys\n"
        f"sys.path[:0] = [{str(REPO / 'src')!r}, {str(REPO)!r}]\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "_db = DATABASES['default']\n"
        f"_options = {{**_db.get('OPTIONS', {{}}), 'pool': {pool!r}}}\n"
        "DATABASES['default'] = {**_db, 'CONN_MAX_AGE': 0, 'OPTIONS': _options, "
        f"**{dict(database or {})!r}}}\n"
    )
    return project


def refused_port() -> int:
    """A local port nothing listens on, so a connection to it is refused."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Proxy:
    """
    A TCP proxy on a local port in front of `upstream`. In "forward" mode a
    new connection is passed through; in "hold" mode it is accepted and
    never answered, while connections already passed through keep flowing,
    which is how a stall of new connections looks from the client. With no
    upstream every connection is held. `forwarded` counts the connections
    passed through and `held` keeps the ones held, so a test can see each
    was closed by the client; `ended` counts the connections passed through
    that the client has since closed, and cut() breaks every one of them.
    """

    def __init__(self, upstream: tuple[str, int] | None = None) -> None:
        self.upstream = upstream
        self.mode = "forward" if upstream else "hold"
        self.forwarded = 0
        self.ended = 0
        self.held: list[socket.socket] = []
        self._sockets: list[socket.socket] = []
        self.server = socket.create_server(("127.0.0.1", 0))
        self.port = self.server.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                client, _ = self.server.accept()
            except OSError:
                return
            if self.mode == "hold" or self.upstream is None:
                self.held.append(client)
                continue
            try:
                upstream = socket.create_connection(self.upstream)
            except OSError:
                client.close()
                continue
            self._sockets += [client, upstream]
            self.forwarded += 1
            for source, sink in ((client, upstream), (upstream, client)):
                threading.Thread(
                    target=self._pump,
                    args=(source, sink),
                    kwargs={"from_client": source is client},
                    daemon=True,
                ).start()

    def _pump(
        self, source: socket.socket, sink: socket.socket, *, from_client: bool
    ) -> None:
        with suppress(OSError):
            while data := source.recv(65536):
                sink.sendall(data)
            if from_client:
                self.ended += 1
        with suppress(OSError):
            sink.shutdown(socket.SHUT_WR)

    def cut(self) -> None:
        """Break every connection passed through, at both ends."""
        for sock in self._sockets:
            with suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

    def closed_by_the_client(self) -> int:
        """How many held connections the client has since closed."""
        closed = 0
        for sock in self.held:
            sock.settimeout(0)
            try:
                while sock.recv(65536):
                    pass
            except BlockingIOError:
                continue
            except ConnectionResetError:
                pass
            closed += 1
        return closed

    def close(self) -> None:
        self.server.close()
        for sock in self.held + self._sockets:
            sock.close()


@pytest.fixture
def proxy():
    """A Proxy in front of the test database; closed afterwards."""
    settings_dict = connection.settings_dict
    made = Proxy((settings_dict["HOST"] or "127.0.0.1", int(settings_dict["PORT"])))
    yield made
    made.close()


@pytest.fixture
def blackhole():
    """A local port that accepts connections and never answers them."""
    made = Proxy()
    yield made
    made.close()


def start_worker(
    project: Path, log_path: Path, *flags: str, tasks_options: dict | None = None
) -> subprocess.Popen[bytes]:
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "pooledproj.settings"
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    if tasks_options is not None:
        env["OX_TEST_TASKS_OPTIONS"] = json.dumps(tasks_options)
    with log_path.open("wb") as log:
        return subprocess.Popen(  # noqa: S603
            [sys.executable, "manage.py", "ox_worker", "--interval", "0.05", *flags],
            cwd=project,
            env=env,
            stdout=log,
            stderr=log,
        )


def stop(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


TERMINAL = [OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED, OxTask.Status.LOST]


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_renewal_keeps_the_leases_while_the_task_threads_hold_the_pool(tmp_path):
    """
    Two tasks on --concurrency 2 and a pool of 3: the poll loop holds one
    connection and each task thread one, so the pool is empty while both
    run, and they keep their connections past the lease.

    The renewal thread has to reach the database while the pool is empty.
    Drawing from it, the renewal waits out the pool timeout on every tick,
    the leases expire, the reaper requeues the rows under the running bodies
    and the bodies run again.
    """
    bodies = tmp_path / "bodies.log"
    release = tmp_path / "release"
    ids = [query_and_hold.enqueue(str(bodies), str(release)).id for _ in range(2)]
    project = pooled_project(
        tmp_path, pool={"min_size": 1, "max_size": 3, "timeout": 2.0}
    )
    log = tmp_path / "worker.log"
    proc = start_worker(project, log, "--concurrency", "2", "--lock-timeout", "4")
    try:
        # Both task threads have their connections: the pool is empty.
        assert wait_for(lambda: lines(bodies).count("HELD") == 2, timeout=60), text(log)
        expiry = dict(
            OxTask.objects.filter(id__in=ids).values_list("id", "lease_expires_at")
        )

        def outlived_or_reclaimed():
            rows = list(OxTask.objects.filter(id__in=ids).annotate(db_now=Now()))
            if any(
                row.attempts > 1 or row.status != OxTask.Status.RUNNING for row in rows
            ):
                return True
            return all(
                row.db_now > expiry[row.id] and row.lease_expires_at > expiry[row.id]
                for row in rows
            )

        # Only a renewal lets a lease outlive the expiry it had when the pool
        # emptied; without one the reaper takes the row back instead.
        assert wait_for(outlived_or_reclaimed, timeout=60, interval=0.1), text(log)
        release.touch()
        assert wait_for(
            lambda: (
                not OxTask.objects.filter(id__in=ids)
                .exclude(status__in=TERMINAL)
                .exists()
            ),
            timeout=60,
            interval=0.1,
        ), text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    rows = {
        str(row.id): (row.status, row.attempts)
        for row in OxTask.objects.filter(id__in=ids)
    }
    assert rows == dict.fromkeys(ids, (OxTask.Status.SUCCESSFUL, 1)), worker_log
    assert lines(bodies).count("START") == 2, worker_log
    assert "Lease renewal failed" not in worker_log
    assert "lost its lease" not in worker_log
    assert "couldn't get a connection" not in worker_log
    # max_size 3 is concurrency + 1: enough, so nothing to warn about.
    assert "connection pool for database" not in worker_log
    assert proc.returncode == 0, worker_log


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_an_undersized_pool_can_time_out_a_task_but_not_the_lease_renewal(tmp_path):
    """
    A pool of 2 on --concurrency 2, one short of the size the worker warns
    for. The poll loop holds one connection and the first task takes the
    other and keeps it, so the second task's query waits for the pool, times
    out and is retried. Below that size, retries like that are allowed.

    The first task's lease has to be renewed all the same. Drawing from the
    pool, the renewal queues behind the second task and times out with it,
    the lease expires under the running body and the reaper takes the row
    back.
    """
    first_log = tmp_path / "first.log"
    second_log = tmp_path / "second.log"
    release = tmp_path / "release"
    project = pooled_project(
        tmp_path, pool={"min_size": 1, "max_size": 2, "timeout": 2.0}
    )
    log = tmp_path / "worker.log"
    proc = start_worker(project, log, "--concurrency", "2", "--lock-timeout", "4")
    try:
        first = query_and_hold.enqueue(str(first_log), str(release))
        # The poll loop and the first task have the whole pool.
        assert wait_for(lambda: "HELD" in lines(first_log), timeout=60), text(log)
        query_and_hold.enqueue(str(second_log), str(release))
        assert wait_for(lambda: "START" in lines(second_log), timeout=60), text(log)
        expiry = OxTask.objects.get(id=first.id).lease_expires_at

        def renewed_while_a_task_timed_out_or_reclaimed():
            row = OxTask.objects.annotate(db_now=Now()).get(id=first.id)
            if row.attempts > 1 or row.status != OxTask.Status.RUNNING:
                return True
            return (
                "couldn't get a connection" in text(log)
                and row.db_now > expiry
                and row.lease_expires_at > expiry
            )

        # Only a renewal lets the first lease outlive the expiry it had when
        # the second task began waiting for the pool.
        assert wait_for(
            renewed_while_a_task_timed_out_or_reclaimed, timeout=60, interval=0.1
        ), text(log)
        held = OxTask.objects.get(id=first.id)
        assert (held.status, held.attempts) == (OxTask.Status.RUNNING, 1), text(log)
        release.touch()
        assert wait_for(
            lambda: OxTask.objects.get(id=first.id).status in TERMINAL,
            timeout=60,
            interval=0.1,
        ), text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    done = OxTask.objects.get(id=first.id)
    assert (done.status, done.attempts) == (OxTask.Status.SUCCESSFUL, 1), worker_log
    assert lines(first_log).count("START") == 1, worker_log
    assert "Lease renewal failed" not in worker_log
    assert "lost its lease" not in worker_log
    # The second task's thread waited for the pool and timed out.
    assert "couldn't get a connection" in worker_log
    assert worker_log.count("connection pool for database") == 1, worker_log
    assert "has a connection limit of 2." in worker_log


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_the_watchdog_records_a_stuck_attempt_while_the_task_threads_hold_the_pool(
    tmp_path,
):
    """
    A task on a queue with no timeout takes a connection and keeps it, then a
    task on a queue with one takes the last connection and sleeps through its
    timeout and grace. With the poll loop's connection that empties a pool
    of 3, and nothing gives one back until the test releases the first task.

    The stuck attempt has to be recorded, and the worker start recycling,
    while the pool is still empty. Drawing from it, the write waits out the
    pool timeout and fails, and the recycle waits with it.
    """
    holder_log = tmp_path / "holder.log"
    stuck_log = tmp_path / "stuck.log"
    release = tmp_path / "release"
    project = pooled_project(
        tmp_path, pool={"min_size": 1, "max_size": 3, "timeout": 10.0}
    )
    log = tmp_path / "worker.log"
    proc = start_worker(
        project,
        log,
        "--concurrency",
        "2",
        tasks_options={"TASK_TIMEOUTS": {"default": 0.5}, "TASK_TIMEOUT_GRACE": 0.5},
    )
    try:
        holder = query_and_hold.using(queue_name="emails").enqueue(
            str(holder_log), str(release)
        )
        assert wait_for(lambda: "HELD" in lines(holder_log), timeout=60), text(log)
        stuck = query_then_sleep.enqueue(str(stuck_log), 60)
        assert wait_for(
            lambda: "recycling: no new claims" in text(log) or proc.poll() is not None,
            timeout=60,
            interval=0.05,
        ), text(log)
        # The holder still has its connection, so the pool had none to give.
        assert "END" not in lines(holder_log)
        recorded = OxTask.objects.get(id=stuck.id)
        release.touch()
        assert proc.wait(timeout=60) == RECYCLE_EXIT_CODE, text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    assert "did not stop" in worker_log
    assert "could not record the stuck attempt" not in worker_log
    assert "couldn't get a connection" not in worker_log
    assert recorded.attempts == 1
    assert len(recorded.errors) == 1, worker_log
    assert "did not stop" in recorded.errors[-1]["traceback"]
    assert OxTask.objects.get(id=holder.id).status == OxTask.Status.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_two_attempts_stuck_while_new_connections_stall_cost_one_deadline(
    tmp_path, proxy
):
    """
    One worker at --concurrency 2 with a pool of 3, all opened through a
    proxy, and two tasks waiting before it starts that each take a
    connection and then sleep through their timeout and grace. Once both
    have queried, the proxy holds every new connection. The watchdog's own
    connection cannot open, the pool has none to spare, and the deadline is
    1 s through connect_timeout.

    The watchdog tries for a connection once, for both stuck attempts, and
    recycles for both. Opening one for each attempt, it waited a deadline
    for each, and the exit waited with it.
    """
    bodies = tmp_path / "bodies.log"
    ids = [query_then_sleep.enqueue(str(bodies), 60).id for _ in range(2)]
    pool = {"min_size": 3, "max_size": 3}
    project = pooled_project(
        tmp_path,
        pool=pool,
        database={
            "HOST": "127.0.0.1",
            "PORT": str(proxy.port),
            "OPTIONS": {"pool": pool, "connect_timeout": 1},
        },
    )
    log = tmp_path / "worker.log"
    proc = start_worker(
        project,
        log,
        "--concurrency",
        "2",
        tasks_options={"TASK_TIMEOUTS": {"default": 0.5}, "TASK_TIMEOUT_GRACE": 0.5},
    )
    try:
        assert wait_for(lambda: lines(bodies).count("HELD") == 2, timeout=60), text(log)
        proxy.mode = "hold"
        assert proc.wait(timeout=60) == RECYCLE_EXIT_CODE, text(log)
    finally:
        stop(proc)

    worker_log = text(log)
    assert worker_log.count("did not stop") == 2, worker_log
    assert worker_log.count("timeout watchdog has no connection") == 1, worker_log
    assert worker_log.count("could not record the stuck attempt") == 2, worker_log
    assert len(proxy.held) == 1, worker_log
    assert proxy.closed_by_the_client() == 1
    rows = OxTask.objects.filter(id__in=ids)
    assert all(row.status == OxTask.Status.RUNNING for row in rows)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_a_worker_whose_pool_is_too_small_says_so_once_at_startup(tmp_path):
    """
    The real command, with the pool at Django's default of 4 and concurrency
    4: one short of a connection per task thread and one for the poll loop.
    """
    project = pooled_project(tmp_path, pool=True)
    log = tmp_path / "worker.log"
    proc = start_worker(project, log, "--concurrency", "4")
    try:
        assert wait_for(
            lambda: "connection pool for database" in text(log), timeout=60
        ), text(log)
    finally:
        stop(proc)
    worker_log = text(log)
    assert worker_log.count("connection pool for database") == 1, worker_log
    assert "has a connection limit of 4." in worker_log
    assert "at least 5" in worker_log
    assert proc.returncode == 0, worker_log


def renewed_or_reclaimed(ids):
    """
    A wait_for predicate: true once every lease in `ids` has outlived the
    expiry it has now, which only a renewal allows, or once any row was
    taken back instead.
    """
    expiry = dict(
        OxTask.objects.filter(id__in=ids).values_list("id", "lease_expires_at")
    )

    def predicate():
        rows = list(OxTask.objects.filter(id__in=ids).annotate(db_now=Now()))
        if any(row.attempts > 1 or row.status != OxTask.Status.RUNNING for row in rows):
            return True
        return all(
            row.db_now > expiry[row.id] and row.lease_expires_at > expiry[row.id]
            for row in rows
        )

    return predicate


def all_running_once(ids):
    return all(
        (row.status, row.attempts) == (OxTask.Status.RUNNING, 1)
        for row in OxTask.objects.filter(id__in=ids)
    )


def none_left_running(ids):
    return lambda: (
        not OxTask.objects.filter(id__in=ids).exclude(status__in=TERMINAL).exists()
    )


@pytest.fixture
def capped_role():
    """
    A login role for the worker with the rights it needs on the test
    database, and a function that sets how many connections the server
    lets it hold at once. The test process connects as a superuser, whose
    connections PostgreSQL does not count against a limit, so only the
    worker's do. Dropped afterwards.
    """
    role = f"ox_capped_{os.getpid()}_{threading.get_ident() % 10000}"
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE ROLE "{role}" LOGIN PASSWORD %s', ["ox"])
        cursor.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{role}"')
        cursor.execute(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{role}"')

    def cap(limit):
        with connection.cursor() as cursor:
            cursor.execute(f'ALTER ROLE "{role}" CONNECTION LIMIT {int(limit)}')

    def held():
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename = %s", [role]
            )
            return cursor.fetchone()[0]

    yield role, cap, held
    with connection.cursor() as cursor:
        cursor.execute(f'ALTER ROLE "{role}" NOLOGIN')
        cursor.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = %s",
            [role],
        )
    wait_for(lambda: held() == 0, timeout=10)
    with connection.cursor() as cursor:
        cursor.execute(f'DROP OWNED BY "{role}"')
        cursor.execute(f'DROP ROLE "{role}"')


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_renewal_borrows_from_the_pool_when_the_server_has_no_slot_left(
    tmp_path, capped_role
):
    """
    Two worker processes at --concurrency 2, each with Django's default pool
    of 4, and a server that lets the worker hold exactly 8 connections. The
    pools take all 8 as they open, so neither renewal thread can open one
    of its own. Each pool has one to spare: the poll loop and two task
    threads hold 3.

    Renewal has to go through that spare connection for as long as the
    server has no room, and go back to one of its own once it has. Asking
    the server for a new connection and nothing else, it fails on every
    tick, the leases expire under the running bodies and they run again.
    """
    role, cap, held = capped_role
    cap(8)
    bodies = tmp_path / "bodies.log"
    release = tmp_path / "release"
    project = pooled_project(
        tmp_path, pool=True, database={"USER": role, "PASSWORD": "ox"}
    )
    log = tmp_path / "worker.log"
    proc = start_worker(
        project, log, "--processes", "2", "--concurrency", "2", "--lock-timeout", "6"
    )
    try:
        # Both pools are full, and so is the server.
        assert wait_for(lambda: held() == 8, timeout=60), text(log)
        ids = [query_and_hold.enqueue(str(bodies), str(release)).id for _ in range(4)]
        assert wait_for(lambda: lines(bodies).count("HELD") == 4, timeout=60), text(log)
        assert wait_for(renewed_or_reclaimed(ids), timeout=60, interval=0.1), text(log)
        assert all_running_once(ids), text(log)
        cap(10)
        assert wait_for(
            lambda: text(log).count("on its own connection again") == 2, timeout=60
        ), text(log)
        release.touch()
        assert wait_for(none_left_running(ids), timeout=60, interval=0.1), text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    rows = {
        str(row.id): (row.status, row.attempts)
        for row in OxTask.objects.filter(id__in=ids)
    }
    assert rows == dict.fromkeys(map(str, ids), (OxTask.Status.SUCCESSFUL, 1))
    assert lines(bodies).count("START") == 4, worker_log
    assert "lost its lease" not in worker_log
    assert "Lease renewal failed" not in worker_log
    # Said once by each process, not once a tick.
    assert worker_log.count("could not get a connection of its own") == 2, worker_log
    assert "too many connections for role" in worker_log
    assert "renewed through the connection pool instead" in worker_log
    assert proc.returncode == 0, worker_log


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_renewal_borrows_from_the_pool_while_new_connections_stall(tmp_path, proxy):
    """
    One worker at --concurrency 2 with a pool of 4, all opened through a
    proxy that then holds every new connection unanswered, while the ones
    already open keep flowing. The poll loop and two task threads hold 3.

    Renewal has to give up on a connection of its own by its deadline, two
    seconds at this lease, and renew through the pool's spare one; and go
    back to its own once connections open again. Waiting out psycopg's
    connect_timeout instead, 130 s by default, the leases expire under the
    running bodies and they run again. Whatever it gave up on it closed.
    """
    bodies = tmp_path / "bodies.log"
    release = tmp_path / "release"
    project = pooled_project(
        tmp_path,
        pool={"min_size": 4, "max_size": 4},
        database={"HOST": "127.0.0.1", "PORT": str(proxy.port)},
    )
    log = tmp_path / "worker.log"
    proc = start_worker(project, log, "--concurrency", "2", "--lock-timeout", "6")
    try:
        assert wait_for(lambda: proxy.forwarded >= 4, timeout=60), text(log)
        proxy.mode = "hold"
        ids = [query_and_hold.enqueue(str(bodies), str(release)).id for _ in range(2)]
        assert wait_for(lambda: lines(bodies).count("HELD") == 2, timeout=60), text(log)
        assert wait_for(renewed_or_reclaimed(ids), timeout=60, interval=0.1), text(log)
        assert all_running_once(ids), text(log)
        assert proxy.held, "renewal never asked for a connection of its own"
        proxy.mode = "forward"
        assert wait_for(
            lambda: "on its own connection again" in text(log), timeout=60
        ), text(log)
        release.touch()
        assert wait_for(none_left_running(ids), timeout=60, interval=0.1), text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    rows = {
        str(row.id): (row.status, row.attempts)
        for row in OxTask.objects.filter(id__in=ids)
    }
    assert rows == dict.fromkeys(map(str, ids), (OxTask.Status.SUCCESSFUL, 1))
    assert lines(bodies).count("START") == 2, worker_log
    assert "lost its lease" not in worker_log
    assert "Lease renewal failed" not in worker_log
    assert worker_log.count("could not get a connection of its own") == 1, worker_log
    assert "connection timeout expired" in worker_log
    assert proxy.closed_by_the_client() == len(proxy.held)
    assert proc.returncode == 0, worker_log


@pytest.fixture
def lapses(transactional_db):
    """
    A trigger on the task table that notes every write to a running row
    made after its lease expired, and a function listing them as (task id,
    seconds expired). A renewal that came too late is one, and so is the
    reaper taking the row back. A lapsed lease is one any reaper was
    entitled to take, whether or not one ran in time to, so this sees a
    lapse however short. Dropped afterwards.
    """
    table = connection.ops.quote_name(OxTask._meta.db_table)
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE lease_lapses (task_id uuid, expired_for float)")
        cursor.execute(
            "CREATE FUNCTION note_lease_lapse() RETURNS trigger LANGUAGE plpgsql AS $$"
            " BEGIN"
            "  IF OLD.status = 'RUNNING' AND OLD.lease_expires_at < now() THEN"
            "   INSERT INTO lease_lapses VALUES"
            "    (OLD.id, extract(epoch FROM now() - OLD.lease_expires_at));"
            "  END IF;"
            "  RETURN NEW;"
            " END $$"
        )
        cursor.execute(
            f"CREATE TRIGGER note_lease_lapse BEFORE UPDATE ON {table}"
            " FOR EACH ROW EXECUTE FUNCTION note_lease_lapse()"
        )

    def noted():
        with connection.cursor() as cursor:
            cursor.execute("SELECT task_id::text, expired_for FROM lease_lapses")
            return cursor.fetchall()

    yield noted
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TRIGGER note_lease_lapse ON {table}")
        cursor.execute("DROP FUNCTION note_lease_lapse()")
        cursor.execute("DROP TABLE lease_lapses")


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="stops the worker with SIGTERM")
@on_postgresql
@with_psycopg_pool
def test_a_stall_that_ends_before_the_lease_does_costs_no_lease(
    tmp_path, proxy, lapses
):
    """
    One worker at --concurrency 2 with a 6 s lease and a pool of 3, all
    opened through a proxy, and two tasks waiting before it starts, so they
    are claimed as the renewal thread starts and the first renewal with
    work to renew comes one 2 s interval later. The poll loop and the two
    task threads hold the pool. Once both bodies have queried, the proxy
    holds every new connection unanswered for 3.5 s.

    That first renewal asks for a connection of its own during the stall
    and gets none by its 2 s deadline, and the pool has none to spare, so it
    is missed. The stall ends 2.5 s before the leases do. The next renewal
    is due an interval after the missed one started, which has passed by
    then, so it runs at once, finds connections opening again and renews.
    Timed an interval after the missed one ended instead, it comes just
    after the leases expired.
    """
    bodies = tmp_path / "bodies.log"
    release = tmp_path / "release"
    ids = [query_and_hold.enqueue(str(bodies), str(release)).id for _ in range(2)]
    project = pooled_project(
        tmp_path,
        pool={"min_size": 3, "max_size": 3},
        database={"HOST": "127.0.0.1", "PORT": str(proxy.port)},
    )
    log = tmp_path / "worker.log"
    proc = start_worker(project, log, "--concurrency", "2", "--lock-timeout", "6")
    try:
        assert wait_for(lambda: lines(bodies).count("HELD") == 2, timeout=60), text(log)
        proxy.mode = "hold"
        outlived = renewed_or_reclaimed(ids)
        time.sleep(3.5)
        proxy.mode = "forward"
        assert proxy.held, "renewal never asked for a connection during the stall"
        assert wait_for(outlived, timeout=60, interval=0.1), text(log)
        assert wait_for(
            lambda: "on its own connection again" in text(log), timeout=60
        ), text(log)
        release.touch()
        assert wait_for(none_left_running(ids), timeout=60, interval=0.1), text(log)
    finally:
        release.touch()
        stop(proc)

    worker_log = text(log)
    assert lapses() == [], worker_log
    rows = {
        str(row.id): (row.status, row.attempts)
        for row in OxTask.objects.filter(id__in=ids)
    }
    assert rows == dict.fromkeys(map(str, ids), (OxTask.Status.SUCCESSFUL, 1))
    assert lines(bodies).count("START") == 2, worker_log
    assert "lost its lease" not in worker_log
    assert "Reclaimed" not in worker_log
    # The one renewal the stall cost, said once.
    assert worker_log.count("could not get a connection of its own") == 1, worker_log
    assert "so this renewal is missed" in worker_log
    assert len(proxy.held) == 1
    assert proxy.closed_by_the_client() == 1
    assert proc.returncode == 0, worker_log


class TestTheScopeOutsideThePool:
    """_outside_the_pool, on an alias no test connects through."""

    @needs_psycopg
    def test_the_thread_gets_a_wrapper_of_its_own_without_the_pool(self, add_alias):
        options = {"pool": {"max_size": 7}, "connect_timeout": 3}
        conf = add_alias(ENGINE=POSTGRESQL, OPTIONS=options)
        unchanged = copy.deepcopy(conf)
        before = connections[ALIAS]
        with _outside_the_pool(ALIAS):
            own = connections[ALIAS]
            assert own is not before
            assert own.settings_dict["OPTIONS"] == {"connect_timeout": 3}
            assert {**own.settings_dict, "OPTIONS": options} == unchanged
            assert own.pool is None
            assert before.settings_dict["OPTIONS"]["pool"] == {"max_size": 7}
        assert connections[ALIAS] is before
        assert connections.settings[ALIAS] is conf
        assert conf == unchanged

    @needs_psycopg
    def test_a_thread_that_had_no_wrapper_is_left_without_one(self, add_alias):
        add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": True})

        def initialized():
            return [
                c for c in connections.all(initialized_only=True) if c.alias == ALIAS
            ]

        assert not initialized()
        with _outside_the_pool(ALIAS):
            (own,) = initialized()
        assert not initialized()
        assert own.connection is None

    @needs_psycopg
    def test_the_old_wrapper_is_put_back_when_the_block_raises(self, add_alias):
        add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": True})
        before = connections[ALIAS]
        with pytest.raises(RuntimeError, match="inside"), _outside_the_pool(ALIAS):
            assert connections[ALIAS] is not before
            raise RuntimeError("inside")
        assert connections[ALIAS] is before

    @pytest.mark.parametrize(
        ("engine", "options"),
        [
            (POSTGRESQL, {}),
            (POSTGRESQL, {"pool": {}}),
            (POSTGRESQL, {"pool": False}),
            (POSTGRESQL, {"pool": "not-a-mapping"}),
            (POSTGRESQL, {"pool": 1}),
            (POSTGRESQL, {"pool": ["max_size"]}),
            (SQLITE, {"pool": True}),
        ],
        ids=[
            "no-pool",
            "empty-pool",
            "pool-false",
            "pool-string",
            "pool-number",
            "pool-list",
            "not-postgresql",
        ],
    )
    @needs_psycopg
    def test_without_a_postgresql_pool_the_thread_keeps_its_wrapper(
        self, add_alias, engine, options
    ):
        conf = add_alias(ENGINE=engine, OPTIONS=options)
        before = connections[ALIAS]
        with _outside_the_pool(ALIAS):
            assert connections[ALIAS] is before
        assert connections[ALIAS] is before
        assert before.settings_dict is conf


POOL_VALUES = [
    True,
    {"max_size": 2},
    MappingProxyType({"max_size": 2}),
    {},
    False,
    None,
    0,
    1,
    "not-a-mapping",
    ["max_size"],
]


@pytest.mark.parametrize("pool", POOL_VALUES, ids=repr)
@needs_psycopg
def test_the_scope_leaves_the_pool_on_exactly_the_values_the_warning_reads(
    add_alias, caplog, pool
):
    """
    Django pools on True and on a non-empty mapping of psycopg_pool's
    arguments, and refuses any other true value when it connects. Renewal
    leaves the pool, and the warning checks its size, on the same values:
    never one without the other. Concurrency 50 is past every size here.
    """
    add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": pool})
    before = connections[ALIAS]
    with _outside_the_pool(ALIAS):
        left = connections[ALIAS] is not before
    worker = Worker(concurrency=50, db_alias=ALIAS)
    with caplog.at_level(logging.WARNING, logger="django_ox"):
        worker._warn_if_the_connection_pool_is_short()
    warned = bool(events(caplog, "connection_pool_too_small"))
    assert left is warned
    assert left is (pool is True or (isinstance(pool, Mapping) and bool(pool)))


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
def test_the_scope_closes_its_connection_and_leaves_the_shared_pool_alone(add_alias):
    options = {**connections.settings["default"]["OPTIONS"]}
    add_alias(OPTIONS={**options, "pool": {"min_size": 1, "max_size": 2}})
    shared = connections[ALIAS]
    shared.ensure_connection()
    pool = shared.pool
    requests = pool.get_stats()["requests_num"]
    seen = {}

    def on_another_thread():
        with _outside_the_pool(ALIAS):
            own = connections[ALIAS]
            with own.cursor() as cursor:
                cursor.execute("SELECT 1")
            seen["open"] = own.connection is not None
            seen["own"] = own
        seen["after"] = [c.alias for c in connections.all(initialized_only=True)]

    thread = threading.Thread(target=on_another_thread)
    thread.start()
    thread.join(timeout=30)

    assert seen["open"]
    assert seen["own"].connection is None
    assert ALIAS not in seen["after"]
    assert shared.pool is pool
    assert not pool.closed
    assert pool.max_size == 2
    assert pool.get_stats()["requests_num"] == requests
    with shared.cursor() as cursor:
        cursor.execute("SELECT 1")


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
def test_the_poll_loop_keeps_its_connection_and_hooks_while_renewal_leaves_the_pool(
    add_alias,
):
    """
    Only the renewal and watchdog threads leave the pool. The poll loop stays
    on the wrapper it started with, so a hook installed on that wrapper (an
    execute_wrapper, as tracing and test tools install) still sees the claim.
    """
    options = {**connections.settings["default"]["OPTIONS"]}
    add_alias(OPTIONS={**options, "pool": {"min_size": 1, "max_size": 4}})
    worker = Worker(db_alias=ALIAS, poll_interval=0.05, renew_interval=0.05)
    renewals = []
    renew_leases = worker.renew_leases

    def renew():
        renewals.append(connections[ALIAS].settings_dict["OPTIONS"].get("pool"))
        return renew_leases()

    worker.renew_leases = renew
    hooked = []

    def hook(execute, sql, params, many, context):
        hooked.append(sql)
        return execute(sql, params, many, context)

    def run():
        with connections[ALIAS].execute_wrapper(hook):
            worker.run()

    result = slow.enqueue(0.5)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert wait_for(
            lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL,
            timeout=30,
        )
    finally:
        worker.request_stop()
        thread.join(timeout=30)
    assert not thread.is_alive()
    assert any("django_ox_oxtask" in sql for sql in hooked)
    assert renewals
    assert all(pool is None for pool in renewals)


@pytest.mark.django_db(transaction=True)
@without_a_pool
def test_without_a_pool_every_thread_keeps_the_connection_it_had(caplog):
    worker = Worker(concurrency=8, poll_interval=0.05, renew_interval=0.05)
    alias = worker._db_alias
    renewals = []
    renew_leases = worker.renew_leases

    def renew():
        renewals.append(connections[alias].settings_dict is connections.settings[alias])
        return renew_leases()

    worker.renew_leases = renew
    result = slow.enqueue(0.5)
    with caplog.at_level(logging.INFO, logger="django_ox"):
        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
                ),
                timeout=30,
            )
        finally:
            worker.request_stop()
            thread.join(timeout=30)
    assert renewals
    assert all(renewals)
    assert events(caplog, "worker_started")
    assert not events(caplog, "connection_pool_too_small")


class TestTheStartupWarning:
    @pytest.mark.parametrize(
        ("engine", "pool", "concurrency", "warns"),
        [
            (POSTGRESQL, True, 3, False),
            (POSTGRESQL, True, 4, True),
            (POSTGRESQL, {"max_size": 6}, 5, False),
            (POSTGRESQL, {"max_size": 6}, 6, True),
            (POSTGRESQL, {"min_size": 3}, 2, False),
            (POSTGRESQL, {"min_size": 3}, 3, True),
            (POSTGRESQL, {"min_size": 3, "max_size": None}, 3, True),
            (POSTGRESQL, {"timeout": 5.0}, 4, True),
            (POSTGRESQL, None, 50, False),
            (POSTGRESQL, {}, 50, False),
            (POSTGRESQL, "not-a-mapping", 50, False),
            (SQLITE, True, 50, False),
        ],
        ids=[
            "true-at-bound",
            "true-over",
            "max-size-at-bound",
            "max-size-over",
            "min-size-only-at-bound",
            "min-size-only-over",
            "max-size-none",
            "neither-size",
            "pool-absent",
            "pool-empty",
            "pool-invalid",
            "not-postgresql",
        ],
    )
    @needs_psycopg
    def test_it_warns_below_one_connection_per_task_thread_and_one_for_the_poll_loop(
        self, add_alias, caplog, engine, pool, concurrency, warns
    ):
        add_alias(ENGINE=engine, OPTIONS={} if pool is None else {"pool": pool})
        worker = Worker(concurrency=concurrency, db_alias=ALIAS)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._warn_if_the_connection_pool_is_short()
        assert bool(events(caplog, "connection_pool_too_small")) is warns

    @pytest.mark.parametrize(
        "pool",
        [
            {"min_size": None},
            {"max_size": "10"},
            {"min_size": "3"},
            {"max_size": 2.5},
            {"max_size": 0},
            {"max_size": -1},
        ],
        ids=repr,
    )
    @needs_psycopg
    def test_a_size_that_is_not_a_count_of_connections_is_passed_over_quietly(
        self, add_alias, caplog, pool
    ):
        # Every one of these would warn at concurrency 50 if it were read as
        # a size. psycopg_pool refuses most of them when Django opens the
        # pool; the warning is not the place to fail.
        add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": pool})
        worker = Worker(concurrency=50, db_alias=ALIAS)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._warn_if_the_connection_pool_is_short()
        assert not events(caplog, "connection_pool_too_small")

    @pytest.mark.parametrize(("max_size", "warns"), [(1, True), (2, False)])
    @needs_psycopg
    def test_a_task_timeout_does_not_raise_the_bound(
        self, add_alias, caplog, max_size, warns
    ):
        # The watchdog connects outside the pool, so it needs no slot in it.
        add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": {"max_size": max_size}})
        worker = Worker(concurrency=1, db_alias=ALIAS, task_timeout=5)
        assert worker.timeouts.enabled
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._warn_if_the_connection_pool_is_short()
        assert bool(events(caplog, "connection_pool_too_small")) is warns

    # 2 with no timeout in the options too: a task can declare its own, and
    # its attempt starts the watchdog, which connects outside the pool.
    @pytest.mark.parametrize(("task_timeout", "unpooled"), [(None, 2), (5, 2)])
    @needs_psycopg
    def test_it_names_the_database_the_sizes_and_what_can_still_fail(
        self, add_alias, caplog, task_timeout, unpooled
    ):
        add_alias(ENGINE=POSTGRESQL, OPTIONS={"pool": True})
        worker = Worker(concurrency=8, db_alias=ALIAS, task_timeout=task_timeout)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            worker._warn_if_the_connection_pool_is_short()
        (record,) = events(caplog, "connection_pool_too_small")
        assert record.levelno == logging.WARNING
        assert record.worker_id == worker.worker_id
        assert record.database == ALIAS
        assert record.concurrency == 8
        assert record.max_size == 4
        assert record.recommended_max_size == 9
        assert record.unpooled_connections == unpooled
        message = record.getMessage()
        assert f"{ALIAS!r}" in message
        assert "has a connection limit of 4." in message
        assert "at least 9" in message
        assert "outcome writes" in message
        assert "retried" in message
        assert "timeout watchdog" in message
        # A worst case, and said to be one: a task may declare a timeout.
        assert "a task can declare its own, so this assumes the worst case" in (message)
        assert message.endswith(
            "Budget 2 additional connections per worker process."
        ), message


def deadline_alias(add_alias, *, budget_options=None, **overrides):
    """
    A pooled PostgreSQL alias with `overrides`, for tests that only ever
    reach the port they give it. It names a database of its own, because
    this run's may be SQLite's, whose NAME PostgreSQL's settings refuse.
    """
    options = {"pool": True, **(budget_options or {})}
    return add_alias(
        ENGINE=POSTGRESQL,
        NAME="oxtest",
        USER="ox",
        PASSWORD="ox",
        OPTIONS=options,
        **overrides,
    )


def timed(call):
    started = time.monotonic()
    try:
        call()
    except Exception as exc:
        return time.monotonic() - started, exc
    return time.monotonic() - started, None


@pytest.fixture
def resolver(monkeypatch):
    """
    socket.getaddrinfo answering for one made-up host name with 127.0.0.1,
    counting the lookups for it. While `hold` is true a lookup for it waits
    until `release` is set, as a resolver that stalls does; the test's end
    sets it. Every other name goes to the real resolver.
    """

    class Resolver:
        name = "stalling-resolver.invalid"
        lookups = 0
        hold = False
        entered = threading.Event()
        release = threading.Event()

    made = Resolver()
    real = socket.getaddrinfo

    def getaddrinfo(host, *args, **kwargs):
        if host != made.name:
            return real(host, *args, **kwargs)
        made.lookups += 1
        made.entered.set()
        if made.hold:
            made.release.wait(60)
        return real("127.0.0.1", *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    yield made
    made.release.set()


@pytest.fixture
def connecting(django_db_blocker):
    """
    Connecting allowed, to ports that are not a database: pytest-django
    refuses every connection outside a database test, and these need none.
    """
    with django_db_blocker.unblock():
        yield


@pytest.mark.usefixtures("connecting")
class TestOpeningItsOwnConnectionByADeadline:
    """
    _OwnConnection.open against a port that accepts and never answers, and
    one that refuses. psycopg alone would wait connect_timeout for every
    step before 3.2, for every host from 3.2, and 130 s by default.
    """

    @pytest.mark.parametrize(
        ("budget", "configured", "deadline", "guard"),
        [
            (0.5, None, 0.5, 2),
            (0.5, 30, 0.5, 2),
            (3.0, 1, 1.0, 2),
            (1.5, "0", 1.5, 2),
            (0.8, "not-a-number", 0.8, 2),
        ],
        ids=["none", "larger", "smaller", "zero", "invalid"],
    )
    @needs_psycopg
    def test_it_gives_up_at_the_deadline_whatever_connect_timeout_says(
        self, add_alias, blackhole, budget, configured, deadline, guard
    ):
        extra = {} if configured is None else {"connect_timeout": configured}
        deadline_alias(
            add_alias, budget_options=extra, HOST="127.0.0.1", PORT=str(blackhole.port)
        )
        with _outside_the_pool(ALIAS, budget) as own:
            assert own.budget == deadline
            # libpq's own bound, whole seconds and at least 2: the 2 s floor
            # does not hold the connection past a shorter deadline.
            assert own.wrapper.settings_dict["OPTIONS"]["connect_timeout"] == guard
            elapsed, exc = timed(lambda: own.open(time.monotonic() + own.budget))
        assert isinstance(exc, OperationalError), exc
        assert "timeout expired" in str(exc)
        assert deadline - 0.05 <= elapsed < deadline + 0.5, elapsed
        assert not own.is_open

    @needs_psycopg
    def test_one_deadline_covers_every_host(self, add_alias, blackhole):
        second = Proxy()
        try:
            deadline_alias(
                add_alias,
                HOST="127.0.0.1,127.0.0.1,127.0.0.1",
                PORT=f"{blackhole.port},{second.port},{blackhole.port}",
            )
            with _outside_the_pool(ALIAS, 1.0) as own:
                elapsed, exc = timed(lambda: own.open(time.monotonic() + 1.0))
        finally:
            second.close()
        assert isinstance(exc, OperationalError), exc
        # psycopg from 3.2 gives each of the three hosts connect_timeout,
        # at least 2 s: six seconds without the deadline.
        assert 0.95 <= elapsed < 1.5, elapsed

    @needs_psycopg
    def test_a_refused_connection_fails_at_once(self, add_alias):
        deadline_alias(add_alias, HOST="127.0.0.1", PORT=str(refused_port()))
        with _outside_the_pool(ALIAS, 5.0) as own:
            elapsed, exc = timed(lambda: own.open(time.monotonic() + 5.0))
        assert isinstance(exc, OperationalError), exc
        assert elapsed < 1.0, elapsed

    @needs_psycopg
    def test_what_it_gave_up_on_is_closed_and_no_thread_is_left(
        self, add_alias, blackhole
    ):
        deadline_alias(add_alias, HOST="127.0.0.1", PORT=str(blackhole.port))
        threads = threading.active_count()
        with _outside_the_pool(ALIAS, 0.1) as own:
            for _ in range(10):
                with pytest.raises(OperationalError):
                    own.open(time.monotonic() + 0.1)
        assert len(blackhole.held) == 10
        assert wait_for(lambda: blackhole.closed_by_the_client() == 10)
        assert threading.active_count() == threads

    @needs_psycopg
    def test_a_connection_django_opens_by_itself_keeps_to_the_budget(
        self, add_alias, blackhole
    ):
        deadline_alias(add_alias, HOST="127.0.0.1", PORT=str(blackhole.port))
        with _outside_the_pool(ALIAS, 0.5) as own:
            elapsed, exc = timed(connections[ALIAS].ensure_connection)
        assert own is not None
        assert isinstance(exc, OperationalError), exc
        assert 0.45 <= elapsed < 1.0, elapsed

    @needs_psycopg
    def test_a_deadline_that_passed_holds_until_the_next_open(
        self, add_alias, blackhole
    ):
        deadline_alias(add_alias, HOST="127.0.0.1", PORT=str(blackhole.port))
        with _outside_the_pool(ALIAS, 0.3) as own:
            with pytest.raises(OperationalError):
                own.open(time.monotonic() + 0.3)
            elapsed, exc = timed(connections[ALIAS].ensure_connection)
        assert isinstance(exc, OperationalError), exc
        assert elapsed < 0.1, elapsed

    @on_postgresql
    @needs_psycopg
    def test_a_stalled_first_host_spends_the_deadline_and_a_healthy_one_is_not_tried(
        self, add_alias, blackhole, proxy
    ):
        """
        A limit, not a guarantee. One deadline covers every host in turn,
        so a first host that stalls spends all of it, and a healthy host
        after it is never tried; psycopg alone from 3.2 gives each host
        connect_timeout and would reach the second. Only a spare connection
        in the pool covers this.
        """
        add_alias(
            OPTIONS={"pool": True},
            HOST="127.0.0.1,127.0.0.1",
            PORT=f"{blackhole.port},{proxy.port}",
        )
        threads = threading.active_count()
        with _outside_the_pool(ALIAS, 0.5) as own:
            elapsed, exc = timed(lambda: own.open(time.monotonic() + 0.5))
            assert not own.is_open
        assert isinstance(exc, OperationalError), exc
        assert "timeout expired" in str(exc)
        assert 0.45 <= elapsed < 1.0, elapsed
        assert proxy.forwarded == 0
        assert len(blackhole.held) == 1
        assert wait_for(lambda: blackhole.closed_by_the_client() == 1)
        assert threading.active_count() == threads
        # The second host is healthy: on its own it opens.
        connections.settings[ALIAS].update(HOST="127.0.0.1", PORT=str(proxy.port))
        with _outside_the_pool(ALIAS, 0.5) as own:
            own.open(time.monotonic() + 0.5)
            assert own.is_open
        assert proxy.forwarded == 1

    @needs_psycopg
    @resolves_in_python
    def test_a_host_name_slow_to_resolve_holds_the_connection_past_its_deadline(
        self, add_alias, blackhole, resolver
    ):
        """
        A limit, not a guarantee: resolving a host name blocks, and the
        deadline does not cover it, on every supported psycopg. From 3.2
        psycopg resolves in Python before any attempt; before, libpq
        resolves in C inside the first step, where no test can hold it.
        The resolver here holds the name until the test lets it go. Once
        it answers, the deadline has passed and no connection is started.
        """
        deadline_alias(add_alias, HOST=resolver.name, PORT=str(blackhole.port))
        resolver.hold = True
        outcome = {}

        def connect():
            with _outside_the_pool(ALIAS, 0.2) as own:
                outcome["result"] = timed(lambda: own.open(time.monotonic() + 0.2))

        thread = threading.Thread(target=connect, daemon=True)
        thread.start()
        assert resolver.entered.wait(timeout=10)
        # Still resolving well past the deadline: it cannot have returned,
        # because the resolver has not answered.
        thread.join(timeout=0.2 + 0.5)
        assert thread.is_alive()
        resolver.release.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
        elapsed, exc = outcome["result"]
        assert isinstance(exc, OperationalError), exc
        assert "timeout expired" in str(exc)
        assert elapsed >= 0.2 + 0.5, elapsed
        assert resolver.lookups == 1
        assert not blackhole.held

    @needs_psycopg
    @resolves_in_python
    def test_a_deadline_that_passed_refuses_before_resolving_the_host_name(
        self, add_alias, blackhole, resolver
    ):
        """
        What keeps a watchdog batch from connecting again once its one
        attempt failed: the reconnect Django makes by itself does not even
        look the host name up, which the deadline would not bound.
        """
        deadline_alias(add_alias, HOST=resolver.name, PORT=str(blackhole.port))
        with _outside_the_pool(ALIAS, 0.3) as own:
            with pytest.raises(OperationalError):
                own.open(time.monotonic() + 0.3)
            assert resolver.lookups == 1
            elapsed, exc = timed(connections[ALIAS].ensure_connection)
        assert isinstance(exc, OperationalError), exc
        assert "timeout expired" in str(exc)
        assert elapsed < 0.1, elapsed
        assert resolver.lookups == 1
        assert len(blackhole.held) == 1


@pytest.fixture
def claimed(add_alias):
    """
    A worker on a pooled alias of the test database, a pool of at most 2,
    and a task it has claimed and is running, as renewal sees it.
    """
    options = {**connections.settings["default"]["OPTIONS"]}
    add_alias(OPTIONS={**options, "pool": {"min_size": 1, "max_size": 2}})
    worker = Worker(db_alias=ALIAS, lock_timeout=30)
    slow.enqueue(0)
    task = worker.claim_one()
    assert task is not None
    worker._in_flight.add((task.pk, task.lease_epoch))
    pool = connections[ALIAS].pool
    # The claim's connection goes back, so the pool starts with none out.
    connections[ALIAS].close()
    # The fixture claimed a task before the pool's first connection was ready,
    # causing the pool to grow in the background. That connection could arrive
    # between the test's two reads of pool_available. Wait until all pool
    # connections are available before comparing counts. This is a precondition,
    # not a retry of the comparison.
    assert wait_for(
        lambda: (stats := pool.get_stats())["pool_available"] == stats["pool_size"]
    ), pool.get_stats()
    return worker, task, pool


def lease(task):
    return OxTask.objects.get(pk=task.pk).lease_expires_at


@pytest.fixture
def emptied():
    """Take every connection a pool has to give; give them back afterwards."""
    taken = []

    def empty(pool):
        taken.extend((pool, pool.getconn()) for _ in range(pool.max_size))

    yield empty
    for pool, conn in taken:
        pool.putconn(conn)


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
class TestARenewalTick:
    """Worker._renew_on, one tick at a time, against the test database."""

    def test_its_own_connection_renews_while_the_pool_is_empty(
        self, claimed, emptied, caplog
    ):
        worker, task, pool = claimed
        emptied(pool)
        requests = pool.get_stats()["requests_num"]
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 2.0) as own,
        ):
            elapsed, exc = timed(
                lambda: worker._renew_on(
                    own, _RenewalReport(worker.worker_id), time.monotonic() + 30
                )
            )
            assert own.is_open
        assert exc is None
        assert lease(task) > before
        assert elapsed < 1.0, elapsed
        assert pool.get_stats()["requests_num"] == requests
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_a_refused_connection_of_its_own_borrows_one_and_gives_it_back(
        self, claimed, caplog
    ):
        worker, task, pool = claimed
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 2.0) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            available = pool.get_stats()["pool_available"]
            started = time.monotonic()
            safe_until = worker._renew_on(
                own, _RenewalReport(worker.worker_id), started + 30
            )
            assert connections[ALIAS] is own.wrapper
            assert not own.is_open
            assert pool.get_stats()["pool_available"] == available
        assert lease(task) > before
        assert safe_until >= started + worker.lock_timeout
        (record,) = events(caplog, "lease_renew_degraded")
        assert record.levelno == logging.WARNING
        assert record.fallback == "succeeded"
        assert "connection" in record.error
        assert not events(caplog, "lease_renew_failed")

    def test_a_stalled_connection_of_its_own_gives_up_by_the_deadline(
        self, claimed, blackhole, caplog
    ):
        worker, task, _ = claimed
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 1.0) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(blackhole.port)
            elapsed, exc = timed(
                lambda: worker._renew_on(
                    own, _RenewalReport(worker.worker_id), time.monotonic() + 30
                )
            )
        assert exc is None
        assert 0.95 <= elapsed < 1.5, elapsed
        assert lease(task) > before
        (record,) = events(caplog, "lease_renew_degraded")
        assert "timeout expired" in record.error
        assert wait_for(lambda: blackhole.closed_by_the_client() == 1)

    def test_with_neither_the_renewal_is_missed_within_the_bound(
        self, claimed, emptied, caplog
    ):
        worker, task, pool = claimed
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 2.0) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            emptied(pool)
            elapsed, exc = timed(
                lambda: worker._renew_on(own, _RenewalReport(worker.worker_id), 1e9)
            )
            assert connections[ALIAS] is own.wrapper
        assert exc is None
        assert elapsed < 0.1 + 0.4, elapsed
        assert lease(task) == before
        (record,) = events(caplog, "lease_renew_degraded")
        assert record.fallback == "failed"
        assert "couldn't get a connection after 0.10 sec" in record.fallback_error

    def test_with_the_leases_about_to_expire_it_goes_to_the_pool_at_once(
        self, claimed, blackhole, caplog
    ):
        worker, task, _ = claimed
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 1.0) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(blackhole.port)
            elapsed, exc = timed(
                lambda: worker._renew_on(
                    own, _RenewalReport(worker.worker_id), time.monotonic() + 0.5
                )
            )
        assert exc is None
        assert elapsed < 0.5, elapsed
        assert not blackhole.held
        assert lease(task) > before
        (record,) = events(caplog, "lease_renew_degraded")
        assert record.error.startswith("not tried")

    def test_after_a_missed_renewal_a_tick_with_little_left_still_tries_its_own(
        self, claimed, emptied, blackhole, caplog
    ):
        """
        A stall of new connections with nothing to spare in the pool misses
        a renewal, and ends. The next tick, with less of the leases left
        than its own deadline, asks the pool first and then opens its own.
        Asking the pool alone, it would never try its own again while any
        work was in flight, because nothing moves `safe_until` but a
        renewal that succeeds.
        """
        worker, task, pool = claimed
        emptied(pool)
        report = _RenewalReport(worker.worker_id)
        before = lease(task)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 0.5) as own,
        ):
            port = own.wrapper.settings_dict["PORT"]
            own.wrapper.settings_dict["PORT"] = str(blackhole.port)
            first = time.monotonic() + 0.55
            safe_until = worker._renew_on(own, report, first)
            assert safe_until == first
            assert lease(task) == before
            own.wrapper.settings_dict["PORT"] = port
            started = time.monotonic()
            assert safe_until - started < own.budget
            safe_until = worker._renew_on(own, report, safe_until)
            assert own.is_open
        assert lease(task) > before
        assert safe_until >= started + worker.lock_timeout
        assert [r.event for r in caplog.records if hasattr(r, "event")] == [
            "lease_renew_degraded",
            "lease_renew_recovered",
        ]

    def test_a_tick_with_nothing_to_renew_opens_nothing(self, claimed, monkeypatch):
        """
        renew_leases() is still called, and the stock one returns without a
        query, so nothing is opened and nothing is asked of the pool.
        """
        worker, _, pool = claimed
        worker._in_flight.clear()
        renewed = []
        stock = worker.renew_leases
        monkeypatch.setattr(worker, "renew_leases", lambda: renewed.append(stock()))
        requests = pool.get_stats()["requests_num"]
        with _outside_the_pool(ALIAS, 2.0) as own:
            started = time.monotonic()
            safe_until = worker._renew_on(own, _RenewalReport(worker.worker_id), 0)
            assert not own.is_open
        assert renewed == [0]
        assert safe_until >= started + worker.lock_timeout
        assert pool.get_stats()["requests_num"] == requests

    def test_an_override_that_queries_with_nothing_in_flight_gets_a_connection(
        self, claimed, blackhole, caplog
    ):
        """
        A subclass's renew_leases() that queries on a tick with nothing in
        flight connects by that tick's deadline, even after an earlier
        tick's deadline passed without a connection.
        """
        worker, _, _ = claimed

        def renew_leases():
            with connections[ALIAS].cursor() as cursor:
                cursor.execute("SELECT 1")
            return 0

        worker.renew_leases = renew_leases
        report = _RenewalReport(worker.worker_id)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS, 0.3) as own,
        ):
            port = own.wrapper.settings_dict["PORT"]
            own.wrapper.settings_dict["PORT"] = str(blackhole.port)
            worker._renew_on(own, report, time.monotonic() + 30)
            assert not own.is_open
            own.wrapper.settings_dict["PORT"] = port
            worker._in_flight.clear()
            caplog.clear()
            worker._renew_on(own, report, time.monotonic() + 30)
            assert own.is_open
        assert not events(caplog, "lease_renew_failed")

    def test_the_tick_after_its_connection_is_killed_reconnects(self, claimed, caplog):
        """A database restart, as the renewal connection sees one."""
        worker, task, _ = claimed
        report = _RenewalReport(worker.worker_id)
        with (
            caplog.at_level(logging.DEBUG, logger="django_ox"),
            _outside_the_pool(ALIAS, 2.0) as own,
        ):
            worker._renew_on(own, report, time.monotonic() + 30)
            killed = own.wrapper.connection.info.backend_pid
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_terminate_backend(%s)", [killed])

            def gone():
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM pg_stat_activity WHERE pid = %s", [killed]
                    )
                    return cursor.fetchone()[0] == 0

            assert wait_for(gone)
            before = lease(task)
            worker._renew_on(own, report, time.monotonic() + 30)
            assert not own.is_open
            assert lease(task) == before
            worker._renew_on(own, report, time.monotonic() + 30)
            assert own.is_open
            assert own.wrapper.connection.info.backend_pid != killed
        assert lease(task) > before
        assert len(events(caplog, "lease_renew_failed")) == 1
        assert not events(caplog, "lease_renew_degraded")


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
class TestTheBorrowedConnection:
    def test_it_goes_back_to_the_pool_when_the_block_returns(self, claimed):
        _, _, pool = claimed
        with _outside_the_pool(ALIAS) as own:
            with own.borrowed(0.1):
                borrowed = connections[ALIAS]
                assert borrowed is not own.wrapper
                with borrowed.cursor() as cursor:
                    cursor.execute("SELECT 1")
                out = pool.get_stats()["pool_available"]
            assert connections[ALIAS] is own.wrapper
            assert borrowed.connection is None
        assert pool.get_stats()["pool_available"] == out + 1

    def test_it_goes_back_to_the_pool_when_the_block_raises(self, claimed):
        _, _, pool = claimed
        with _outside_the_pool(ALIAS) as own:
            with pytest.raises(RuntimeError, match="inside"), own.borrowed(0.1):
                with connections[ALIAS].cursor() as cursor:
                    cursor.execute("SELECT 1")
                out = pool.get_stats()["pool_available"]
                raise RuntimeError("inside")
            assert connections[ALIAS] is own.wrapper
        assert pool.get_stats()["pool_available"] == out + 1

    def test_an_empty_pool_is_waited_for_no_longer_than_asked(self, claimed, emptied):
        _, _, pool = claimed
        emptied(pool)
        with _outside_the_pool(ALIAS) as own:
            started = time.monotonic()
            with (
                pytest.raises(OperationalError, match="couldn't get a connection"),
                own.borrowed(0.1),
            ):
                pytest.fail("the block ran without a connection")
            elapsed = time.monotonic() - started
            assert connections[ALIAS] is own.wrapper
        assert elapsed < 0.5, elapsed


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
class TestTheWatchdogConnection:
    def test_it_borrows_when_it_cannot_open_its_own(self, claimed):
        worker, _, pool = claimed
        with _outside_the_pool(ALIAS) as own:
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            with worker._watchdog_connection(own):
                assert connections[ALIAS] is not own.wrapper
                with connections[ALIAS].cursor() as cursor:
                    cursor.execute("SELECT 1")
                out = pool.get_stats()["pool_available"]
            assert connections[ALIAS] is own.wrapper
        assert pool.get_stats()["pool_available"] == out + 1

    def test_with_neither_it_says_so_and_the_record_fails_at_once(
        self, claimed, emptied, caplog
    ):
        worker, _, pool = claimed
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            emptied(pool)
            with worker._watchdog_connection(own):
                assert connections[ALIAS] is own.wrapper
                elapsed, exc = timed(connections[ALIAS].ensure_connection)
        assert isinstance(exc, OperationalError), exc
        assert elapsed < 0.1, elapsed
        (record,) = events(caplog, "watchdog_connection_unavailable")
        assert "couldn't get a connection" in record.fallback_error


@pytest.fixture
def stuck_batch(add_alias):
    """
    A worker on a pooled alias of the test database with a pool of at most
    2, three tasks it has claimed, and a watch for each, past its timeout
    and its grace, on a thread still inside the attempt: three stuck
    attempts for the watchdog to record in one batch. The claims'
    connection goes back, so the pool starts with none out.
    """
    options = {**connections.settings["default"]["OPTIONS"]}
    add_alias(OPTIONS={**options, "pool": {"min_size": 1, "max_size": 2}})
    worker = Worker(db_alias=ALIAS, lock_timeout=30)
    for _ in range(3):
        slow.enqueue(0)
    now = time.monotonic()
    watches = []
    for ident in range(1, 4):
        task = worker.claim_one()
        assert task is not None
        attempt = (task.pk, task.lease_epoch)
        worker._in_flight.add(attempt)
        worker._running_on[ident] = attempt
        watches.append(
            _Watch(
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
        )
    pool = connections[ALIAS].pool
    connections[ALIAS].close()
    return worker, watches, pool


def recorded(watch):
    row = OxTask.objects.get(pk=watch.db_task.pk)
    return any("did not stop" in error["traceback"] for error in row.errors)


@on_postgresql
@with_psycopg_pool
@pytest.mark.usefixtures("unguarded_db")
class TestAWatchdogBatch:
    """
    Several stuck attempts recorded together: one acquisition for the
    batch, and every attempt recycled whatever became of it.
    """

    def test_new_connections_stalling_cost_the_batch_one_deadline_not_one_each(
        self, stuck_batch, emptied, blackhole, caplog, monkeypatch
    ):
        """
        The watchdog thread itself, with every watch due. Its own
        connection stalls, the pool has none to spare, and the deadline is
        1 s through connect_timeout. Opening one per stuck attempt, the
        batch took 1.1 s for each of the three.
        """
        worker, watches, pool = stuck_batch
        emptied(pool)
        conf = connections.settings[ALIAS]
        conf["PORT"] = str(blackhole.port)
        conf["OPTIONS"] = {**conf["OPTIONS"], "connect_timeout": 1}
        monkeypatch.setattr(worker_module, "WATCHDOG_IDLE", 0.05)
        worker._watches.update((watch.ident, watch) for watch in watches)
        thread = threading.Thread(target=worker._watchdog_loop, daemon=True)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            started = time.monotonic()
            thread.start()
            thread.join(timeout=30)
            elapsed = time.monotonic() - started
        assert not thread.is_alive()
        assert 0.95 <= elapsed < 1.1 + 1.2, elapsed
        assert len(blackhole.held) == 1
        assert wait_for(lambda: blackhole.closed_by_the_client() == 1)
        assert len(events(caplog, "watchdog_connection_unavailable")) == 1
        # Every attempt is recycled although none could be recorded.
        assert len(events(caplog, "task_stuck_unrecorded")) == 3
        assert worker._stuck == {w.ident: w.attempt for w in watches}
        assert worker.recycling
        assert not events(caplog, "watchdog_error")

    def test_an_attempt_that_goes_stuck_while_the_batch_waits_joins_it(
        self, stuck_batch, emptied, blackhole, caplog, monkeypatch
    ):
        """
        The first attempt's grace has passed and the second's passes 0.3 s
        later, while the watchdog is still waiting for a connection to
        record the first on. The second joins that batch, so both cost one
        deadline. In a batch of its own, it waited out another.
        """
        worker, watches, pool = stuck_batch
        emptied(pool)
        conf = connections.settings[ALIAS]
        conf["PORT"] = str(blackhole.port)
        conf["OPTIONS"] = {**conf["OPTIONS"], "connect_timeout": 1}
        monkeypatch.setattr(worker_module, "WATCHDOG_IDLE", 0.05)
        first, second = watches[:2]
        second.grace_at = time.monotonic() + 0.3
        worker._watches.update({first.ident: first, second.ident: second})
        thread = threading.Thread(target=worker._watchdog_loop, daemon=True)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            started = time.monotonic()
            thread.start()
            thread.join(timeout=30)
            elapsed = time.monotonic() - started
        assert not thread.is_alive()
        assert len(blackhole.held) == 1
        assert len(events(caplog, "watchdog_connection_unavailable")) == 1
        assert worker._stuck == {w.ident: w.attempt for w in (first, second)}
        assert 0.95 <= elapsed < 1.1 + 0.9, elapsed

    def test_its_own_connection_opens_once_for_every_record_and_closes_after(
        self, stuck_batch, emptied, proxy
    ):
        worker, watches, pool = stuck_batch
        emptied(pool)
        with _outside_the_pool(ALIAS) as own:
            own.wrapper.settings_dict["PORT"] = str(proxy.port)
            worker._record_stuck(own, watches)
            assert not own.is_open
        assert proxy.forwarded == 1
        assert wait_for(lambda: proxy.ended == 1)
        assert all(map(recorded, watches))
        assert worker._stuck == {w.ident: w.attempt for w in watches}

    @pytest.mark.parametrize("given_back", [False, True], ids=["kept", "closed"])
    def test_a_connection_that_stops_working_part_way_is_not_replaced(
        self, stuck_batch, emptied, proxy, caplog, monkeypatch, given_back
    ):
        """
        The first record lands; then the connection breaks, and Django
        either keeps the broken connection or closes it. The records after
        it fail at once rather than connect again, and each attempt is
        still recycled.
        """
        worker, watches, pool = stuck_batch
        emptied(pool)
        handle = worker._handle_stuck

        def then_break(watch):
            handle(watch)
            proxy.cut()
            if given_back:
                connections[ALIAS].close()

        monkeypatch.setattr(worker, "_handle_stuck", then_break)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(proxy.port)
            elapsed, exc = timed(lambda: worker._record_stuck(own, watches))
            assert not own.is_open
        assert exc is None
        assert elapsed < 1.0, elapsed
        assert proxy.forwarded == 1
        assert [recorded(w) for w in watches] == [True, False, False]
        assert len(events(caplog, "task_stuck_unrecorded")) == 2
        assert worker._stuck == {w.ident: w.attempt for w in watches}

    def test_a_borrowed_connection_is_checked_out_once_and_given_back(
        self, stuck_batch, caplog
    ):
        worker, watches, pool = stuck_batch
        available = pool.get_stats()["pool_available"]
        requests = pool.get_stats()["requests_num"]
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            worker._record_stuck(own, watches)
            assert connections[ALIAS] is own.wrapper
        assert all(map(recorded, watches))
        assert pool.get_stats()["requests_num"] == requests + 1
        assert pool.get_stats()["pool_available"] == available
        assert not events(caplog, "watchdog_connection_unavailable")

    def test_a_connection_that_cannot_be_given_back_does_not_end_the_backstop(
        self, stuck_batch, caplog, monkeypatch
    ):
        worker, watches, _ = stuck_batch

        def refuses(own):
            raise RuntimeError("close failed")

        monkeypatch.setattr(worker_module._OwnConnection, "close", refuses)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS) as own,
        ):
            worker._record_stuck(own, watches)
        assert all(map(recorded, watches))
        assert worker._stuck == {w.ident: w.attempt for w in watches}
        (record,) = events(caplog, "watchdog_error")
        assert "could not give back the connection" in record.getMessage()

    @pytest.mark.parametrize("given_back", [False, True], ids=["kept", "returned"])
    def test_a_borrowed_connection_that_stops_working_is_not_replaced(
        self, stuck_batch, caplog, monkeypatch, given_back
    ):
        worker, watches, pool = stuck_batch
        requests = pool.get_stats()["requests_num"]
        handle = worker._handle_stuck

        def then_break(watch):
            handle(watch)
            backend = connections[ALIAS].connection.info.backend_pid
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_terminate_backend(%s)", [backend])
            if given_back:
                connections[ALIAS].close()

        monkeypatch.setattr(worker, "_handle_stuck", then_break)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            _outside_the_pool(ALIAS) as own,
        ):
            own.wrapper.settings_dict["PORT"] = str(refused_port())
            worker._record_stuck(own, watches)
            assert connections[ALIAS] is own.wrapper
        assert [recorded(w) for w in watches] == [True, False, False]
        assert pool.get_stats()["requests_num"] == requests + 1
        assert len(events(caplog, "task_stuck_unrecorded")) == 2
        assert worker._stuck == {w.ident: w.attempt for w in watches}


class TestTheRenewalReport:
    """_RenewalReport on a clock the test moves."""

    def test_each_change_is_said_once_and_missed_renewals_every_30_s(self, caplog):
        now = [0.0]
        report = _RenewalReport("w", clock=lambda: now[0])

        def said(step):
            caplog.clear()
            step()
            return [(r.levelname, r.event) for r in caplog.records]

        with caplog.at_level(logging.DEBUG, logger="django_ox"):
            assert said(report.own) == []
            assert said(lambda: report.pool("refused")) == [
                ("WARNING", "lease_renew_degraded")
            ]
            for _ in range(3):
                assert said(lambda: report.pool("refused")) == [
                    ("DEBUG", "lease_renew_fallback")
                ]
            assert said(lambda: report.missed("refused", "empty")) == [
                ("WARNING", "lease_renew_missed")
            ]
            assert caplog.records[0].missed == 1
            for second in (10.0, 20.0, 29.9):
                now[0] = second
                assert said(lambda: report.missed("refused", "empty")) == []
            now[0] = 30.0
            assert said(lambda: report.missed("refused", "empty")) == [
                ("WARNING", "lease_renew_missed")
            ]
            assert caplog.records[0].missed == 4
            now[0] = 31.0
            assert said(lambda: report.pool("refused")) == [
                ("DEBUG", "lease_renew_fallback")
            ]
            now[0] = 35.0
            assert said(lambda: report.missed("refused", "empty")) == []
            now[0] = 60.0
            assert said(lambda: report.missed("refused", "empty")) == [
                ("WARNING", "lease_renew_missed")
            ]
            assert caplog.records[0].missed == 2
            assert said(report.own) == [("INFO", "lease_renew_recovered")]
            assert caplog.records[0].fallback_renewals == 5
            assert caplog.records[0].missed_renewals == 7
            assert said(report.own) == []
            # Straight from its own connection to none at all is the one
            # warning, however recently the last summary was.
            now[0] = 61.0
            assert said(lambda: report.missed("refused", "empty")) == [
                ("WARNING", "lease_renew_degraded")
            ]
            assert caplog.records[0].fallback == "failed"
            assert caplog.records[0].fallback_error == "empty"
