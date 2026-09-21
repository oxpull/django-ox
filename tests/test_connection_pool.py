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
import importlib.util
import json
import logging
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection, connections
from django.db.models.functions import Now

from django_ox.models import OxTask
from django_ox.timeouts import RECYCLE_EXIT_CODE
from django_ox.worker import Worker, _outside_the_pool

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


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def lines(path: Path) -> list[str]:
    return path.read_text().split() if path.exists() else []


def text(path: Path) -> str:
    return path.read_text() if path.exists() else ""


@pytest.fixture
def add_alias(monkeypatch):
    """
    Add a database alias to the loaded settings, as a copy of the default
    one with the given keys replaced. Closes and forgets this thread's
    wrapper for it and any pool built for it afterwards.
    """
    added = []

    def add(**overrides):
        conf = {**copy.deepcopy(connections.settings["default"]), **overrides}
        monkeypatch.setitem(connections.settings, ALIAS, conf)
        added.append(ALIAS)
        return conf

    yield add
    for alias in added:
        for wrapper in connections.all(initialized_only=True):
            if wrapper.alias == alias:
                wrapper.close()
                del connections[alias]
        pools = getattr(connections.create_connection(alias), "_connection_pools", {})
        if alias in pools:
            pools.pop(alias).close()


@pytest.fixture
def unguarded_db(django_db_setup, django_db_blocker):
    """
    The test database without pytest-django's test case around it. That test
    case refuses every thread's connection to an alias it was not given at
    setup, which an alias added by the test cannot be. Rows commit as they
    are written, so the tasks table is emptied afterwards.
    """
    with django_db_blocker.unblock():
        yield
        OxTask.objects.all().delete()


def pooled_project(tmp_path: Path, *, pool: object) -> Path:
    """
    A project whose settings are this suite's with the default database
    pooled. This checkout's src goes first on the path, so the worker runs
    the code under test even where an install elsewhere would win from the
    project's directory.
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
        "DATABASES['default'] = {**_db, 'CONN_MAX_AGE': 0, 'OPTIONS': _options}\n"
    )
    return project


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
    assert proc.returncode == 0, worker_log


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
            (SQLITE, {"pool": True}),
        ],
        ids=["no-pool", "empty-pool", "pool-false", "not-postgresql"],
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
