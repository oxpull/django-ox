import contextlib
import sqlite3

import pytest
from django.core.checks.registry import registry as check_registry
from django.db import DEFAULT_DB_ALIAS, connections
from django.test import TestCase

from django_ox.worker import Worker
from oxpull.routing import oxpull_db_alias

from . import isolation, tasks

# The marker that keeps a worker's commit callbacks waiting for a commit that
# never comes, in a test that runs inside one transaction.
COMMIT_CALLBACKS_WAIT = "commit_callbacks_wait"

# Literal statements, not a formatted table name: these are the only two
# statistics tables current SQLite writes.
CLEAR_SQLITE_STATISTICS = {
    "sqlite_stat1": "DELETE FROM sqlite_stat1",
    "sqlite_stat4": "DELETE FROM sqlite_stat4",
}

# What asks a server for its release, by Django's name for the vendor. MariaDB
# answers the MySQL one with its own name in the string.
SERVER_VERSION = {
    "postgresql": "SHOW server_version",
    "mysql": "SELECT VERSION()",
}


def engine_versions() -> str:
    """
    The database engines this run plans with, on one line.

    The SQLite library comes with the operating system or the Python build,
    and a PostgreSQL or MySQL server comes from an image tag that moves.
    None of them is a package the project pins, and the plans the suite
    asserts differ between their releases, so every run says which ones it
    ran on.

    SQLite is loaded into every interpreter, so it is always named. The
    server is asked only where the default database is on one. It is asked
    through the driver, on a connection of its own that is closed again:
    pytest-django blocks Django's connection outside a test, and the header
    is printed before any test runs. The connection names no database, which
    is how Django connects to create the test database: the database in the
    settings need not exist before the run or after it. A server that does
    not answer is said so here, and the tests that need it say the rest.
    """
    versions = f"sqlite: {sqlite3.sqlite_version}"
    connection = connections[DEFAULT_DB_ALIAS]
    query = SERVER_VERSION.get(connection.vendor)
    if query is None:
        return versions
    server_wide = type(connection)({**connection.settings_dict, "NAME": None})
    try:
        raw = server_wide.get_new_connection(server_wide.get_connection_params())
        try:
            with contextlib.closing(raw.cursor()) as cursor:
                cursor.execute(query)
                (server,) = cursor.fetchone()
        finally:
            raw.close()
    except connection.Database.Error as exc:
        server = f"no answer ({type(exc).__name__})"
    return f"{versions}, {connection.vendor}: {server}"


def pytest_report_header(config):
    return engine_versions()


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    # -q prints no header, and CI runs with -q.
    if config.get_verbosity() < 0:
        terminalreporter.write_line(engine_versions())


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "hazard: a test in a file named for a hazard; slow, and run on the "
        "paths and schedules CI selects for it",
    )
    config.addinivalue_line(
        "markers",
        f"{COMMIT_CALLBACKS_WAIT}: a test inside one transaction keeps a worker's "
        "commit callbacks waiting, as a caller's open transaction does",
    )


def pytest_collection_modifyitems(config, items):
    """
    Mark every test in a file with "hazard" in its name as a hazard test.

    CI selects on the marker (-m hazard, -m "not hazard") instead of on file
    names, so the split does not depend on how a command line spells a glob.
    The files are named for the hazard they pin, which is why the name is the
    rule: a new hazard file is marked without anyone remembering to.
    """
    for item in items:
        if "hazard" in item.path.name:
            item.add_marker(pytest.mark.hazard)


@pytest.fixture(autouse=True)
def commit_callbacks_after_a_worker_pass(request, monkeypatch):
    """
    Run a worker's commit callbacks when its pass returns, in a test that runs
    inside one transaction.

    A test marked django_db without transaction=True runs inside a transaction
    that is rolled back at its end, so nothing in it ever commits. A worker's
    pass there is inside a caller's transaction, where the release of a node's
    dependents waits for the commit (transaction.on_commit). An ox_worker runs
    in autocommit and commits that pass itself, so these tests run the
    callbacks the pass registered when it returns, standing in for that commit.

    A test that is about the wait itself carries the commit_callbacks_wait
    marker, and nothing here touches it. A transactional test commits for real
    and is left alone too.
    """
    marker = request.node.get_closest_marker("django_db")
    if marker is None or request.node.get_closest_marker(COMMIT_CALLBACKS_WAIT):
        return
    if marker.kwargs.get("transaction") or "transactional_db" in request.fixturenames:
        return
    real = Worker.execute

    def execute(self, db_task, *, inline=False):
        with TestCase.captureOnCommitCallbacks(using=oxpull_db_alias(), execute=True):
            return real(self, db_task, inline=inline)

    monkeypatch.setattr(Worker, "execute", execute)


@pytest.fixture(autouse=True)
def task_state():
    tasks.STATE.clear()
    yield
    tasks.STATE.clear()


@pytest.fixture(autouse=True)
def sqlite_planner_statistics(request, django_db_blocker):
    """
    Drop the SQLite planner statistics a test leaves behind.

    Some tests run ANALYZE. On SQLite the statistics go into sqlite_stat1
    (and sqlite_stat4, on a build with STAT4), which the flush between tests
    does not touch, so every later test in the process plans with them.
    Statistics taken while a wide fan-in filled the edge table make the
    workflow dependency check scan that whole table once per candidate row:
    a test that takes 7 s on its own took four minutes after one that ran
    ANALYZE.

    After each test that used the database, empty those tables and run
    ANALYZE sqlite_schema, which makes the connection reload the now empty
    statistics. A test that runs ANALYZE in its body still plans with what
    it gathered, because this runs after the test. PostgreSQL and MySQL are
    left alone: the server keeps their statistics current, and the tests
    that ANALYZE there do it on purpose before they measure.
    """
    yield
    connection = connections[DEFAULT_DB_ALIAS]
    if connection.vendor != "sqlite":
        return
    uses_db = request.node.get_closest_marker("django_db") is not None or not {
        "db",
        "transactional_db",
    }.isdisjoint(request.fixturenames)
    if not uses_db:
        return
    with django_db_blocker.unblock(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name IN ('sqlite_stat1', 'sqlite_stat4')"
        )
        present = [name for (name,) in cursor.fetchall()]
        for table in present:
            cursor.execute(CLEAR_SQLITE_STATISTICS[table])
        if present:
            cursor.execute("ANALYZE sqlite_schema")


@pytest.fixture(autouse=True)
def pooled_sessions_are_not_passed_on():
    """
    Leave no connection in Django's pool at an isolation level a test set.

    A test that sets a level which outlives the transaction says so through
    isolation.outlives_the_transaction(): the session's own level on the
    server, or the driver's isolation_level that a changed OPTIONS puts on
    the connection. Unpooled, the close() that ends such a test ends both.
    Pooled, close() hands the connection back to Django's pool as it is,
    and a checkout in a later test runs at that level. So after such a test
    every pool is renewed.
    """
    isolation.CHANGED.clear()
    yield
    if not isolation.CHANGED:
        return
    isolation.CHANGED.clear()
    isolation.renew_pools()


@pytest.fixture(autouse=True)
def check_registry_is_left_as_it_was_found():
    """
    Leave django.core.checks holding what it held before the test.

    A test that adds an app to INSTALLED_APPS runs that app's
    AppConfig.ready(), which registers the app's checks for the rest of the
    process. Nothing takes them off again when INSTALLED_APPS goes back, so
    a later `manage.py check` runs a check belonging to an app that is no
    longer installed, and it raises. The admin tests install
    django.contrib.auth and django.contrib.admin on purpose, and the
    rate-limit name tests run the real command on purpose. Both are right,
    so the registry is what has to be put back.
    """
    registered = set(check_registry.registered_checks)
    deployment = set(check_registry.deployment_checks)
    yield
    check_registry.registered_checks.clear()
    check_registry.registered_checks.update(registered)
    check_registry.deployment_checks.clear()
    check_registry.deployment_checks.update(deployment)


@pytest.fixture
def worker():
    """A worker with no backoff delay, suitable for inline run_once() tests."""
    return Worker(backoff_initial=0, poll_interval=0.05)
