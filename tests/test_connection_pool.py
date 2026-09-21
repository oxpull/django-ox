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
from django.db import DEFAULT_DB_ALIAS, connection, connections
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
    assert "holds at most 2 connections" in worker_log


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
    assert "holds at most 4 connections" in worker_log
    assert "at least 5" in worker_log
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

    @pytest.mark.parametrize(("task_timeout", "unpooled"), [(None, 1), (5, 2)])
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
        assert "at most 4 connections" in message
        assert "at least 9" in message
        assert "outcome writes" in message
        assert "retried" in message
        assert ("timeout watchdog" in message) is (task_timeout is not None)
        assert message.endswith(" per worker process."), message
