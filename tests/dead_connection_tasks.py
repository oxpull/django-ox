"""
Tasks that end their own database connection, for test_dead_connection.

The server ends it: pg_terminate_backend on PostgreSQL, KILL on MySQL. That
is what a failover, a restart or an operator's kill looks like from the
worker. Asked for on the task's own connection, the task sees the error and
Django flags the connection; asked for from another connection while the
task works on without the database, nothing on the task's thread sees it,
and the outcome write is the first statement to find it dead. Each body
appends one JSON line per event to the file its `notes` argument names,
since it runs in a worker process of its own; the tests count executions
from those lines, not from the row.
"""

import json
import os
import threading
import time
from datetime import timedelta
from pathlib import Path

from asgiref.sync import sync_to_async
from django.db import DatabaseError, connections
from django.utils import timezone

from django_ox.compat import task, task_finished
from django_ox.models import OxTask

from .tasks import STATE


def note(notes, event, **fields):
    record = {
        "event": event,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        **fields,
    }
    with Path(notes).open("a") as out:
        out.write(json.dumps(record) + "\n")


def read_notes(notes):
    path = Path(notes)
    if not path.exists():
        return []
    with path.open() as lines:
        return [json.loads(line) for line in lines if line.strip()]


def end_connection(alias="default"):
    """
    Have the server end this thread's connection for `alias`, and raise the
    error the statement that asked for it gets. Returns nothing.
    """
    conn = connections[alias]
    with conn.cursor() as cursor:
        if conn.vendor == "postgresql":
            cursor.execute("SELECT pg_terminate_backend(pg_backend_pid())")
        else:
            cursor.execute("SELECT CONNECTION_ID()")
            (own,) = cursor.fetchone()
            cursor.execute(f"KILL {int(own)}")


def end_connection_and_catch(notes, alias="default"):
    try:
        end_connection(alias)
    except DatabaseError as exc:
        note(
            notes,
            "ended",
            alias=alias,
            caught=type(exc).__name__,
            flagged=connections[alias].errors_occurred,
        )
        return
    note(notes, "not-ended", alias=alias)


@task
def ends_its_connection_and_succeeds(notes, alias="default"):
    end_connection_and_catch(notes, alias)
    return "succeeded anyway"


@task
def ends_its_connection_and_fails(notes):
    note(notes, "ending")
    end_connection()


@task
def ends_the_second_connection_and_succeeds(notes):
    # The default connection is used and left working: only the second one,
    # which the worker writes the outcome to, is ended.
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT 1")
    end_connection_and_catch(notes, "second")
    return "succeeded anyway"


@task
async def ends_its_connection_from_async_and_succeeds(notes):
    # Django refuses a query on the event loop's thread; a coroutine reaches
    # the database through sync_to_async, which runs the call on the thread
    # that called async_to_sync, the worker's pool thread, unless told not
    # to. That is the thread the outcome is written from.
    await sync_to_async(end_connection_and_catch)(notes)
    note(notes, "loop")
    return "succeeded anyway"


@task
def ran(notes):
    note(notes, "ran")
    return "ran"


@task
def pool_report(notes):
    # The thread's own connection is taken first, so the counters describe
    # the pool while a task holds one, as every task above did.
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT 1")
    stats = connections["default"].pool.get_stats()
    note(notes, "pool", stats=stats)
    return "reported"


@task
def survives_a_statement_error():
    """
    Run a statement that fails without harming the connection, catch it,
    and keep the driver connection, so the test can see whether the worker
    kept it.
    """
    conn = connections["default"]
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ox_no_such_table")
    except DatabaseError:
        pass
    STATE["flagged"] = conn.errors_occurred
    STATE["driver_connection"] = conn.connection
    return "kept"


# -- a connection that died unnoticed ----------------------------------------


def session_id(alias="default"):
    """This thread's session on the server for `alias`, opening it if needed."""
    conn = connections[alias]
    with conn.cursor() as cursor:
        if conn.vendor == "postgresql":
            cursor.execute("SELECT pg_backend_pid()")
        else:
            cursor.execute("SELECT CONNECTION_ID()")
        (own,) = cursor.fetchone()
    return int(own)


def from_another_connection(work, alias="default"):
    """
    Call work(connection) on a thread of its own, and so on a connection of
    its own, the way an operator's session or another worker reaches the
    database; wait for it, and raise here whatever it raised.
    """
    failed = []

    def run():
        try:
            work(connections[alias])
        except BaseException as exc:
            failed.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, name="dead-connection-other")
    thread.start()
    thread.join()
    if failed:
        raise failed[0]


def end_session(conn, session):
    """
    On `conn`, have the server end session `session`, and return once it
    has gone, so the session that owned it cannot win a race with it.
    """
    with conn.cursor() as cursor:
        if conn.vendor == "postgresql":
            # With a timeout, pg_terminate_backend waits for the backend to
            # exit, and says whether it did.
            cursor.execute("SELECT pg_terminate_backend(%s, 10000)", [session])
            (gone,) = cursor.fetchone()
            if not gone:
                raise AssertionError(f"backend {session} did not exit")
            return
        cursor.execute(f"KILL {int(session)}")
        deadline = time.monotonic() + 10
        while True:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.processlist WHERE id = %s",
                [session],
            )
            if not cursor.fetchone()[0]:
                return
            if time.monotonic() > deadline:
                raise AssertionError(f"connection {session} did not end")
            time.sleep(0.05)


def end_every_other_session(conn):
    """
    On `conn`, end every other session on its database, as a restart of
    the server does; returns how many.
    """
    name = conn.settings_dict["NAME"]
    with conn.cursor() as cursor:
        if conn.vendor == "postgresql":
            cursor.execute(
                "SELECT pid FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                [name],
            )
        else:
            cursor.execute(
                "SELECT id FROM information_schema.processlist "
                "WHERE db = %s AND id <> CONNECTION_ID()",
                [name],
            )
        sessions = [row[0] for row in cursor.fetchall()]
    for session in sessions:
        end_session(conn, session)
    return len(sessions)


#: Tasks whose task_finished signals the tests count, each noted as a line.
REPORTS_FINISHED = frozenset(
    {
        "loses_its_connection_unnoticed",
        "makes_no_query",
        "outcome_commits_then_connection_drops",
        "loses_its_connection_and_its_lease",
        "works_offline_through_a_restart",
        "queries_after_a_restart",
        "quick",
    }
)


def _note_finished(sender, task_result, **kwargs):
    if task_result.task.func.__name__ in REPORTS_FINISHED and task_result.args:
        note(task_result.args[0], "finished", status=str(task_result.status))


task_finished.connect(_note_finished, dispatch_uid="dead_connection_tasks.finished")


@task
def loses_its_connection_unnoticed(notes, *, fail=False):
    """
    Query, then have the server end this session from another connection
    while the body works on without the database, and return, or raise,
    with no further query: nothing on this thread sees the error, so
    Django has flagged nothing.
    """
    own = session_id()
    from_another_connection(lambda conn: end_session(conn, own))
    note(
        notes,
        "ended-unnoticed",
        session=own,
        flagged=connections["default"].errors_occurred,
    )
    if fail:
        raise ValueError("failed after its connection ended")
    return "succeeded anyway"


@task
def makes_no_query(notes):
    note(notes, "ran-no-query")
    return "succeeded anyway"


@task
def loses_the_database(notes):
    """
    Query, then from another connection close this database to new
    connections and end this session: the outcome write finds its
    connection dead and cannot open another. PostgreSQL only; the test
    opens the database again.
    """
    own = session_id()
    name = connections["default"].settings_dict["NAME"]

    def close_the_database(conn):
        # The session is ended first, from this database, which refuses new
        # sessions afterwards. PostgreSQL refuses the ALTER from a session
        # on the database itself, so it goes through Django's session on
        # the maintenance database.
        end_session(conn, own)
        with conn._nodb_cursor() as cursor:
            cursor.execute(f'ALTER DATABASE "{name}" WITH ALLOW_CONNECTIONS false')

    from_another_connection(close_the_database)
    note(notes, "database-closed", session=own)
    return "succeeded anyway"


@task(takes_context=True)
def loses_its_connection_and_its_lease(context, notes):
    """
    Query, then from another connection: let this attempt's lease run out,
    have a second worker's reaper requeue the row and its claim take it,
    and end this session. By the time the outcome is written, the row
    belongs to that worker, at a later epoch.
    """
    from django_ox.worker import Worker

    own = session_id()
    pk = context.task_result.id

    def take_over(conn):
        past = timezone.now() - timedelta(hours=1)
        OxTask.objects.filter(pk=pk).update(locked_at=past, lease_expires_at=past)
        other = Worker(poll_interval=0.05)
        other.reap()
        claimed = other.claim_one()
        if claimed is None or str(claimed.pk) != str(pk):
            raise AssertionError(f"the second worker claimed {claimed!r}")
        note(
            notes,
            "taken-over",
            worker_id=other.worker_id,
            epoch=claimed.lease_epoch,
            attempts=claimed.attempts,
        )
        end_session(conn, own)

    from_another_connection(take_over)
    return "succeeded anyway"


@task
def outcome_commits_then_connection_drops(notes, *, fail=False):
    """
    Have this thread's outcome write commit and then find its connection
    ended before the caller hears back: an execute wrapper on the
    connection lets the worker's UPDATE of the row run, has the server end
    this session from another connection, and runs one more statement on
    it, whose error is what the write raises. Fires once.
    """
    conn = connections["default"]
    own = session_id()
    table = OxTask._meta.db_table

    def commit_then_drop(execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        if sql.lstrip().upper().startswith("UPDATE") and table in sql:
            conn.execute_wrappers.remove(commit_then_drop)
            note(
                notes,
                "committed",
                autocommit=conn.get_autocommit(),
                in_atomic_block=conn.in_atomic_block,
            )
            from_another_connection(lambda other: end_session(other, own))
            execute("SELECT 1", None, many, context)
        return result

    conn.execute_wrappers.append(commit_then_drop)
    note(notes, "armed", session=own)
    if fail:
        raise ValueError("failed; its outcome write will commit, then drop")
    return "succeeded anyway"


# -- a restart under Django's pool --------------------------------------------

#: How long a task waits for the test to open its gate.
GATE_LIMIT = 60.0


def gate(notes):
    """The file whose existence lets a task waiting on `notes`'s gate go on."""
    return Path(notes).parent / "gate"


def wait_for_the_gate(notes):
    deadline = time.monotonic() + GATE_LIMIT
    while not gate(notes).exists():
        if time.monotonic() > deadline:
            raise AssertionError("the test never opened the gate")
        time.sleep(0.02)


def other_sessions(conn):
    """How many sessions other than `conn`'s are on its database."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            [conn.settings_dict["NAME"]],
        )
        return int(cursor.fetchone()[0])


def restart_every_other_session(conn):
    """
    On `conn`, PostgreSQL only: end every other session on its database in
    one statement, as a restart of the server ends them all at once rather
    than one after another, and return once they have all gone; returns
    how many there were. Every connection Django's pool holds idle is one
    of them.
    """
    name = conn.settings_dict["NAME"]
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            [name],
        )
        sessions = [row[0] for row in cursor.fetchall()]
        cursor.execute(
            "SELECT pg_terminate_backend(pid) FROM unnest(%s::integer[]) AS pid",
            [sessions],
        )
        deadline = time.monotonic() + 10
        while True:
            cursor.execute(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE pid = ANY(%s)",
                [sessions],
            )
            if not cursor.fetchone()[0]:
                return len(sessions)
            if time.monotonic() > deadline:
                raise AssertionError(f"sessions {sessions} did not all end")
            time.sleep(0.02)


@task
def quick(notes):
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT 1")
    note(notes, "quick")
    return "quick"


@task
def works_offline_through_a_restart(notes, *, fail=False):
    """
    Query, say so, and wait for the test to open the gate, which it does
    after ending every session; then return, or raise, with no further
    query. Nothing on this thread sees its connection end.
    """
    note(notes, "ready", session=session_id())
    wait_for_the_gate(notes)
    note(notes, "resumed", flagged=connections["default"].errors_occurred)
    if fail:
        raise ValueError("failed after a restart")
    return "succeeded anyway"


@task
def queries_after_a_restart(notes, *, catch=False):
    """
    Query, say so, and wait for the gate as above; then query once more on
    the connection the restart ended, and catch its error and return, or
    let it out. The failed statement flags the connection.
    """
    note(notes, "ready", session=session_id())
    wait_for_the_gate(notes)
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 1")
    except DatabaseError as exc:
        note(
            notes,
            "query-failed",
            error=type(exc).__name__,
            flagged=connections["default"].errors_occurred,
        )
        if not catch:
            raise
        return "succeeded anyway"
    note(notes, "query-answered")
    return "succeeded anyway"
