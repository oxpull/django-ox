"""
Isolation levels, and what the dedup path does with them, for the tests
that run it inside a caller's transaction at a level other than Django's
default.

Django's PostgreSQL and MySQL backends connect at READ COMMITTED unless
OPTIONS["isolation_level"] names another level. A project can set one
there, a session can set its own, and a transaction can set its own as its
first statement.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from django.db import OperationalError, connections, transaction
from django.db.backends.signals import connection_created

from oxpull.backend import OxpullBackend

#: Set when the running test changed an isolation level that outlives a
#: transaction. The conftest renews Django's connection pools after it.
CHANGED: list[str] = []


def outlives_the_transaction(level: str) -> None:
    """
    Record that the running test set `level` on a session, or in OPTIONS,
    where a later checkout from Django's pool would still find it.

    Unpooled, the close() that ends such a test ends the session and the
    driver connection with it. Pooled, close() hands the connection back
    as it is: SET SESSION CHARACTERISTICS stays on the server session, and
    the isolation_level Django put on the driver connection for a changed
    OPTIONS stays on the connection, which Django does not reset when
    OPTIONS name no level. The conftest renews the pools after the test.
    """
    CHANGED.append(level)


def renew_pools() -> None:
    """
    Close Django's connection pools, so every later checkout is a session
    opened from the settings in force then, by a pool Django builds anew.

    Closing, not draining: a drained pool replaces its connections in the
    background, and one it was already opening lands in the pool after the
    drain with the session it started with. A closed pool closes those as
    they arrive, and each checked-out connection when it comes back.
    """
    for alias in connections:
        connection = connections[alias]
        if getattr(connection, "pool", None) is not None:
            connection.close_pool()


@contextmanager
def callers_transaction(how: str) -> Iterator[None]:
    """
    A transaction the caller opens and ends, around the block. "atomic" is an
    atomic block. "autocommit-off" turns autocommit off on this thread's
    connection, commits when the block ends, and turns it back on.
    """
    if how == "atomic":
        with transaction.atomic():
            yield
        return
    assert how == "autocommit-off", how
    transaction.set_autocommit(False)
    try:
        yield
        transaction.commit()
    finally:
        transaction.rollback()
        transaction.set_autocommit(True)


def pg_sqlstate(exc: BaseException) -> str | None:
    """The SQLSTATE behind a Django OperationalError, from psycopg 3 or psycopg2."""
    cause = exc.__cause__
    return getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)


def result_fields(result):
    """What a caller can read off a TaskResult, as one comparable tuple."""
    return (
        result.id,
        result.status,
        result.enqueued_at,
        result.started_at,
        result.last_attempted_at,
        result.finished_at,
        result.args,
        result.kwargs,
        result.backend,
        result.worker_ids,
        [(error.exception_class_path, error.traceback) for error in result.errors],
        result.task.module_path,
        result.task.queue_name,
        result.task.priority,
        result.task.run_after,
    )


def set_session_isolation(level: str) -> None:
    """
    This thread's connection runs the transactions it opens next at
    `level`. The caller closes the connection afterwards, so the next use
    reopens it at the configured level.
    """
    connection = connections["default"]
    if connection.vendor == "postgresql":
        statement = (
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}"
        )
    elif connection.vendor == "mysql":
        statement = f"SET SESSION TRANSACTION ISOLATION LEVEL {level}"
    else:
        raise AssertionError(f"{connection.vendor} has no session isolation level")
    outlives_the_transaction(level)
    with connection.cursor() as cursor:
        cursor.execute(statement)


def set_transaction_isolation(level: str) -> None:
    """
    The open transaction runs at `level`. PostgreSQL accepts this only as the
    transaction's first statement, and it lasts for that transaction alone.
    """
    with connections["default"].cursor() as cursor:
        cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")


def isolation_for_new_connections(monkeypatch: Any, level: str) -> None:
    """
    Every connection opened from here on runs at `level`, as it does for a
    project that sets OPTIONS["isolation_level"]. A connection already open
    keeps its level.
    """
    connection = connections["default"]
    if connection.vendor == "postgresql":
        from django.db.backends.postgresql.psycopg_any import IsolationLevel

        value: Any = IsolationLevel[level.replace(" ", "_")]
    else:
        value = level.lower()
    outlives_the_transaction(level)
    monkeypatch.setitem(connection.settings_dict["OPTIONS"], "isolation_level", value)


def session_isolation() -> str:
    """The level this thread's connection runs at, spelled as SQL spells it."""
    connection = connections["default"]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SHOW transaction_isolation")
        else:
            cursor.execute("SELECT @@transaction_isolation")
        (level,) = cursor.fetchone()
    return str(level).upper().replace("-", " ")


@contextmanager
def opened_connections() -> Iterator[list[Any]]:
    """
    Every connection created through the handler while the block runs, as
    the wrappers themselves. The handler is shared by every thread: enter
    this where the threads that run meanwhile already have their own
    connections, or those are recorded too.
    """
    opened: list[Any] = []
    create_connection = connections.create_connection

    def create(alias: str) -> Any:
        wrapper = create_connection(alias)
        opened.append(wrapper)
        return wrapper

    connections.create_connection = create
    try:
        yield opened
    finally:
        connections.create_connection = create_connection


@contextmanager
def how_new_connections_run() -> Iterator[list[dict[str, Any]]]:
    """
    For every PostgreSQL connection that connects while the block runs, the
    level a statement in autocommit runs at, whether autocommit is on, and
    the OPTIONS it was built from. Read on the driver's own cursor, so
    Django records nothing.
    """
    seen: list[dict[str, Any]] = []

    def record(sender: Any, connection: Any, **kwargs: Any) -> None:
        if connection.vendor != "postgresql":
            return
        with connection.connection.cursor() as cursor:
            cursor.execute("SHOW transaction_isolation")
            (level,) = cursor.fetchone()
        seen.append(
            {
                "level": str(level).upper(),
                "autocommit": connection.get_autocommit(),
                "options": dict(connection.settings_dict.get("OPTIONS", {})),
            }
        )

    connection_created.connect(record)
    try:
        yield seen
    finally:
        connection_created.disconnect(record)


# The level, lock_timeout and statement_timeout in force on a PostgreSQL
# session, as current_setting() spells them.
SESSION_SETTINGS = (
    "SELECT current_setting('transaction_isolation'), "
    "current_setting('lock_timeout'), current_setting('statement_timeout')"
)


def settings_of(wrapper: Any) -> tuple[str, ...]:
    """SESSION_SETTINGS on the wrapper's driver connection, which Django won't log."""
    with wrapper.connection.cursor() as cursor:
        cursor.execute(SESSION_SETTINGS)
        return tuple(str(value) for value in cursor.fetchone())


def new_session_settings() -> tuple[str, ...]:
    """SESSION_SETTINGS on a new connection built from the current OPTIONS."""
    probe = connections.create_connection("default")
    # Outside Django's pool, as the enqueue's own read is: a pooled checkout
    # is a session the pool opened earlier, from the OPTIONS it was made with.
    options = probe.settings_dict.get("OPTIONS", {})
    probe.settings_dict = {
        **probe.settings_dict,
        "OPTIONS": {name: value for name, value in options.items() if name != "pool"},
    }
    try:
        probe.ensure_connection()
        return settings_of(probe)
    finally:
        probe.close()


@contextmanager
def fresh_read_transactions() -> Iterator[list[dict[str, Any]]]:
    """
    For every PostgreSQL connection created through the handler while the
    block runs: the first word of each statement it ran, SESSION_SETTINGS as
    they stood when it first ran a SELECT, and as they stood after a COMMIT.
    """
    seen: list[dict[str, Any]] = []
    create_connection = connections.create_connection

    def create(alias: str) -> Any:
        wrapper = create_connection(alias)
        if wrapper.vendor != "postgresql":
            return wrapper
        record: dict[str, Any] = {"statements": [], "at_read": None, "after": None}
        seen.append(record)

        # Django calls an execute wrapper with these five, positionally.
        def recording(
            execute: Any, sql: str, params: Any, many: Any, context: Any
        ) -> Any:
            word = sql.split()[0].upper()
            if word == "SELECT" and record["at_read"] is None:
                record["at_read"] = settings_of(wrapper)
            result = execute(sql, params, many, context)
            record["statements"].append(word)
            if word == "COMMIT":
                record["after"] = settings_of(wrapper)
            return result

        wrapper.execute_wrappers.append(recording)
        return wrapper

    connections.create_connection = create
    try:
        yield seen
    finally:
        connections.create_connection = create_connection


def record_holder_checks(monkeypatch: Any) -> list[str | None]:
    """
    What each check of a holder row answered, in order: "granted", or the
    SQLSTATE it was refused with. A test that expects a refusal asserts it
    was met, so a run that never provoked one cannot pass.
    """
    answers: list[str | None] = []
    lock = getattr(OxpullBackend, "_lock_holder_nowait", None)
    if lock is None:
        # A version with no holder check records nothing.
        return answers

    def recording(task_pk: Any, db_alias: str) -> None:
        try:
            lock(task_pk, db_alias)
        except OperationalError as exc:
            answers.append(pg_sqlstate(exc))
            raise
        answers.append("granted")

    monkeypatch.setattr(OxpullBackend, "_lock_holder_nowait", staticmethod(recording))
    return answers
