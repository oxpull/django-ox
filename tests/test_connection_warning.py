"""
The startup warning for a PostgreSQL worker that opens a connection per task.

These tests run the real `ox_worker --batch` command through
`call_command`, so the warning has to come from the code path the command
reaches and not from a call the test makes itself. They need no PostgreSQL
server: the suite's database is SQLite, and the test presents it to the worker
as PostgreSQL by name only. The worker's own database work is vendor-neutral
on an empty queue, and the warning reads settings and opens no connection.
tests/test_connection_count.py runs the same rule against a real server.
"""

import logging

import pytest
from django.core.management import call_command
from django.db import connections

from django_ox.worker import Worker

from .connection_count_tasks import tick

EVENT = "worker_connection_hint"

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def as_postgresql(monkeypatch):
    """
    The settings of a PostgreSQL alias with no pool, for the worker's startup
    check alone: the test edits the dict this returns. The check is the real
    method, reached by the real `Worker.run()`. It is shown the dict and the
    name PostgreSQL only for its own call, because the claim queries of an
    empty queue and SQLite's connect cannot run under either.
    """
    shape = {"ENGINE": "django.db.backends.postgresql", "OPTIONS": {}}
    real = Worker._hint_if_every_task_reconnects

    def check(self):
        conn = connections[self._db_alias]
        with monkeypatch.context() as shown:
            shown.setattr(conn, "vendor", "postgresql")
            shown.setattr(conn, "settings_dict", shape)
            shown.setitem(connections.settings, self._db_alias, shape)
            real(self)

    monkeypatch.setattr(Worker, "_hint_if_every_task_reconnects", check)
    return shape


def run_command(caplog, *flags):
    with (
        caplog.at_level(logging.INFO, logger="django_ox"),
        pytest.raises(SystemExit) as stopped,
    ):
        call_command("ox_worker", "--batch", "--interval", "0.05", *flags)
    assert stopped.value.code == 0
    return [r for r in caplog.records if getattr(r, "event", None) == EVENT]


def test_the_defaults_warn_once_with_the_setting_and_the_fix(as_postgresql, caplog):
    as_postgresql["CONN_MAX_AGE"] = 0
    (record,) = run_command(caplog, "--concurrency", "1")
    assert record.levelno == logging.WARNING
    assert record.database == "default"
    assert record.vendor == "postgresql"
    message = record.getMessage()
    assert "CONN_MAX_AGE" in message
    assert "CONN_MAX_AGE = 600" in message
    assert "OPTIONS['pool']" in message


def test_a_missing_setting_is_the_default_and_warns(as_postgresql, caplog):
    as_postgresql.pop("CONN_MAX_AGE", None)
    assert len(run_command(caplog, "--concurrency", "1")) == 1


@pytest.mark.parametrize("age", [600, 1, None])
def test_a_kept_connection_is_silent(as_postgresql, caplog, age):
    as_postgresql["CONN_MAX_AGE"] = age
    assert not run_command(caplog, "--concurrency", "1")


@pytest.mark.parametrize("pool", [True, {"min_size": 0, "max_size": 16, "timeout": 30}])
def test_a_pool_is_silent(as_postgresql, caplog, pool):
    as_postgresql["CONN_MAX_AGE"] = 0
    as_postgresql["OPTIONS"]["pool"] = pool
    assert not run_command(caplog, "--concurrency", "1")


def test_several_threads_and_many_tasks_warn_once(as_postgresql, caplog, tmp_path):
    as_postgresql["CONN_MAX_AGE"] = 0
    for i in range(12):
        tick.enqueue(str(tmp_path / f"{i}.jsonl"))
    assert len(run_command(caplog, "--concurrency", "4")) == 1


def test_sqlite_is_silent(caplog):
    if connections["default"].vendor != "sqlite":
        pytest.skip("the default alias is not SQLite in this run")
    assert not run_command(caplog, "--concurrency", "1")
