"""
Threads racing the same unique key. transaction=True so each thread's
connection sees committed state, mirroring real worker processes.
"""

import io
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.db import OperationalError, connections, transaction
from django.db.backends.signals import connection_created
from django.db.models import F
from django.tasks.signals import task_enqueued
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.results import task_result_from_db
from oxpull import backend as backend_module
from oxpull.backend import OxpullBackend
from oxpull.exceptions import UniqueEnqueueRace
from oxpull.models import OxpullUniqueLock

from . import tasks
from .isolation import (
    fresh_read_transactions,
    how_new_connections_run,
    isolation_for_new_connections,
    new_session_settings,
    opened_connections,
    pg_sqlstate,
    record_holder_checks,
    result_fields,
    session_isolation,
    set_session_isolation,
    set_transaction_isolation,
)
from .racing import THREADS, race, race_all


@pytest.mark.django_db(transaction=True)
def test_racing_enqueues_have_exactly_one_winner():
    results, errors = race(lambda: tasks.unique_echo.enqueue("raced").id)

    assert errors == []
    assert len(results) == THREADS
    assert len(set(results)) == 1
    assert OxTask.objects.count() == 1
    lock = OxpullUniqueLock.objects.get()
    assert str(lock.task_id) == results[0]


@pytest.mark.django_db(transaction=True)
def test_racing_enqueues_over_a_stale_lock_release_it_once(worker):
    # Set up a finished holder whose lock is stale, then race the steal
    # path: exactly one thread may release-and-reenqueue.
    first = tasks.unique_echo.enqueue("raced")
    assert worker.run_once() is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL

    results, errors = race(lambda: tasks.unique_echo.enqueue("raced").id)

    assert errors == []
    assert len(set(results)) == 1
    assert results[0] != first.id
    # The finished task plus exactly one new one; one live lock.
    assert OxTask.objects.count() == 2
    lock = OxpullUniqueLock.objects.get()
    assert str(lock.task_id) == results[0]


def shorten_lock_wait(monkeypatch):
    """
    One second, so the loser below gives up in a second rather than in
    the database's default: SQLite's busy timeout is the test settings'
    20 s, MySQL's innodb_lock_wait_timeout is 50 s, and PostgreSQL waits
    forever unless a project sets lock_timeout. SQLite's is a connection
    parameter, applied through the dict every new connection is built
    from; the other two are set on the loser's own session.
    """
    connection = connections["default"]
    if connection.vendor == "sqlite":
        monkeypatch.setitem(connection.settings_dict["OPTIONS"], "timeout", 1)

    def on_this_session() -> None:
        session = connections["default"]
        if session.vendor == "mysql":
            with session.cursor() as cursor:
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
        elif session.vendor == "postgresql":
            with session.cursor() as cursor:
                cursor.execute("SET lock_timeout = '1s'")

    return on_this_session


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("ambient", [False, True], ids=["own", "ambient"])
def test_a_holder_that_outlives_the_lock_wait_raises_the_named_race(
    monkeypatch, ambient
):
    """
    The winner's transaction can outlive the database's lock wait: the
    enqueue joins the caller's transaction, so a slow request holds the
    key for as long as it runs. SQLite then raises busy after its
    timeout, MySQL 1205 after innodb_lock_wait_timeout, PostgreSQL
    lock_not_available if a lock_timeout is set. The loser used to get
    that raw OperationalError where the API promises a TaskResult. It
    now gets UniqueEnqueueRace, chained to the original, which already
    means "nothing was written, safe to retry". Inside a caller's
    transaction the same exception says to retry the whole transaction,
    and the caller's transaction is still usable after it, on a server
    that rolls back the statement and not the transaction (MySQL's
    default; the test below covers the other setting).
    """
    on_this_session = shorten_lock_wait(monkeypatch)

    def loser():
        on_this_session()
        caught = None
        if ambient:
            with transaction.atomic():
                try:
                    tasks.unique_echo.enqueue("held")
                except UniqueEnqueueRace as exc:
                    caught = exc
                # The enqueue rolled back to its savepoint; the caller's
                # transaction is still open and still usable.
                assert OxpullUniqueLock.objects.count() == 0
        else:
            try:
                tasks.unique_echo.enqueue("held")
            except UniqueEnqueueRace as exc:
                caught = exc
        return caught

    with transaction.atomic():
        holder = tasks.unique_echo.enqueue("held")
        results, errors = race_all([loser])

    assert errors == []
    (caught,) = results
    assert isinstance(caught, UniqueEnqueueRace)
    assert isinstance(caught.__cause__, OperationalError)
    assert "lock wait" in str(caught)
    assert ("whole transaction" in str(caught)) is ambient
    # Nothing was written for the loser, and the key still deduplicates.
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == holder.id
    assert tasks.unique_echo.enqueue("held").id == holder.id


def touch_elsewhere(task_id, status=None, other=None):
    """
    Commit a write on the task row from a connection of the test's own, or on
    `other`, an autocommit connection the caller opened and closes. With no
    status the write sets a column to its own value, which leaves the task as
    it was. PostgreSQL still writes a new version of the row, as it does for a
    lease renewal.
    """
    own = other is None
    if own:
        other = connections.create_connection("default")
    table = other.ops.quote_name(OxTask._meta.db_table)
    pk = OxTask._meta.pk.get_db_prep_value(uuid.UUID(str(task_id)), other)
    if status is None:
        sql, params = f"UPDATE {table} SET priority = priority WHERE id = %s", [pk]  # noqa: S608
    else:
        sql, params = f"UPDATE {table} SET status = %s WHERE id = %s", [status, pk]  # noqa: S608
    try:
        with other.cursor() as cursor:
            cursor.execute(sql, params)
            assert cursor.rowcount == 1
    finally:
        if own:
            other.close()


def other_sessions():
    """PostgreSQL sessions on the test database other than this thread's own."""
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
        (count,) = cursor.fetchone()
    return count


def warmed_pool():
    """
    When the alias is pooled, give this thread's connection back to Django's
    pool and wait until the pool holds its minimum of idle connections. The
    pool opens its connections in the background, so a session count taken
    before then can miss one that is still connecting and see it afterwards.
    A pool that is not ready within the timeout fails the test.
    """
    connection = connections["default"]
    pool = getattr(connection, "pool", None)
    if pool is None:
        return
    connection.close()
    pool.open()  # as Django does before every checkout; nothing once open
    pool.wait(timeout=30)


def settle_sessions(expected):
    """
    The session count once it reaches `expected`, or what it is after two
    seconds. A closed session leaves pg_stat_activity a moment after the
    client closes it.
    """
    deadline = time.monotonic() + 2
    count = other_sessions()
    while count != expected and time.monotonic() < deadline:
        time.sleep(0.05)
        count = other_sessions()
    return count


def locked_elsewhere(task_id):
    """
    Whether another connection is refused an exclusive lock on the task row
    at once. A lock of any strength that another transaction keeps on the
    row refuses it.
    """
    probe = connections.create_connection("default")
    table = probe.ops.quote_name(OxTask._meta.db_table)
    try:
        with probe.cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {table} WHERE id = %s FOR UPDATE NOWAIT",  # noqa: S608
                [OxTask._meta.pk.get_db_prep_value(uuid.UUID(str(task_id)), probe)],
            )
            cursor.fetchall()
    except OperationalError:
        return True
    finally:
        probe.close()
    return False


def blocking_locks_held_here():
    """
    The locks this thread's open transaction holds on the task table that
    could make another session wait for it.

    PostgreSQL records a locking read as a RowShareLock on the table, next to
    the AccessShareLock every read takes. Two modes are left out, because
    neither can hold another session off the task table:

    - AccessShareLock, which every read takes and which conflicts only with
      the AccessExclusiveLock a DDL statement asks for. Nothing under test
      here runs DDL.
    - SIReadLock, the predicate lock SERIALIZABLE records for a read. It
      conflicts with no lock mode at all: nothing waits on it, and no claim
      skips a row because of it. It exists so that SSI can spot a
      serialization anomaly and abort a transaction, never to make one wait.
      Its granularity follows the plan, tuple for an index scan and relation
      for a sequential scan, so a plain read of one row records a
      relation-wide one as soon as the planner has statistics that make the
      sequential scan cheaper. The read that fixes this transaction's
      snapshot, above, is enough to record it.

    Every other mode PostgreSQL can record on a table conflicts with
    something, so leaving the filter negative keeps an unexpected one
    visible. InnoDB lists each record lock, and each of those can block.
    """
    connection = connections["default"]
    table = OxTask._meta.db_table
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                "SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() "
                "AND locktype = 'relation' AND relation = %s::regclass "
                "AND mode <> 'AccessShareLock' AND mode <> 'SIReadLock'",
                [table],
            )
        else:
            cursor.execute(
                "SELECT LOCK_MODE FROM performance_schema.data_locks "
                "WHERE OBJECT_NAME = %s AND LOCK_TYPE = 'RECORD' "
                "AND THREAD_ID = PS_CURRENT_THREAD_ID()",
                [table],
            )
        return sorted(row[0] for row in cursor.fetchall())


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "write", ["renew", "finish", "requeue", "lose", "claim", "prune"]
)
@pytest.mark.parametrize("path", ["unchanged", "changed"])
@pytest.mark.parametrize(
    "level",
    [None, "REPEATABLE READ", "SERIALIZABLE"],
    ids=["read-committed", "repeatable-read", "serializable"],
)
def test_a_duplicate_inside_a_callers_transaction_locks_no_live_holder(
    monkeypatch, worker, level, path, write
):
    """
    Deduplicating against a READY or RUNNING holder keeps no row lock.

    A duplicate enqueue inside a caller's transaction (ATOMIC_REQUESTS,
    any atomic() block) used to read the holder with SELECT ... FOR
    UPDATE, and a row lock taken inside a savepoint lives until the
    outer transaction commits. For as long as the request ran, the
    holder's own worker could not renew its lease or write its outcome,
    and the reaper could not take the row. A live holder has nothing to
    release, so its row is read without a lock.

    On PostgreSQL at REPEATABLE READ and SERIALIZABLE the enqueue then asks
    for a share lock on the row without waiting, in a savepoint it rolls
    back at once. On the path "changed", a write committed on the holder's
    row after the request's snapshot began and left the task pending. The
    request is refused (40001), reads the holder again on a connection of
    its own, and closes that connection before returning. Either way no
    lock is left when the enqueue returns. With the request still open, the
    worker renews the lease, writes the outcome, is requeued or recorded
    LOST by the reaper, claims a READY holder, or ox_prune deletes the
    finished row. Each runs with a one-second lock wait, and the request
    stays open until it has run, so a statement that waited on the request
    would fail with the lock wait's error. A share lock kept to the request's
    commit would make the claim skip the holder and every other statement
    wait. At SERIALIZABLE every read also records a predicate lock, which
    makes nothing wait and is not counted; the helper says why. MySQL at
    REPEATABLE READ opens a connection on both paths, as in 1.2.0. SQLite
    has no row locks, and InnoDB's SERIALIZABLE is pinned by the test below.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite has no row locks for a transaction to hold")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE locks the holder, pinned below")
    if level is None and path == "changed":
        pytest.skip("READ COMMITTED reads the current row and runs no check")
    on_this_session = shorten_lock_wait(monkeypatch)
    holder = tasks.unique_echo.enqueue("held")
    db_task = None
    if write != "claim":
        db_task = worker.claim_one()
        assert db_task is not None and str(db_task.id) == holder.id
    if write in ("requeue", "lose"):
        expired = timezone.now() - timedelta(hours=1)
        updates = {"locked_at": expired, "lease_expires_at": expired}
        if write == "lose":
            updates["attempts"] = db_task.max_attempts
        OxTask.objects.filter(pk=db_task.pk).update(**updates)
    if level is not None:
        # Connections opened from here on, the request's among them.
        isolation_for_new_connections(monkeypatch, level)
    checks = record_holder_checks(monkeypatch)

    snapshot_taken = threading.Event()
    changed = threading.Event()
    deduplicated = threading.Event()
    request_done = threading.Event()
    outcome = {}

    def web_request():
        try:
            with transaction.atomic():
                outcome["level"] = session_isolation()
                # The first read fixes the snapshot.
                OxTask.objects.get(id=holder.id)
                snapshot_taken.set()
                assert changed.wait(timeout=10)
                with opened_connections() as opened:
                    outcome["result"] = tasks.unique_echo.enqueue("held")
                outcome["opened"] = [wrapper.alias for wrapper in opened]
                outcome["closed"] = all(
                    wrapper.connection is None for wrapper in opened
                )
                outcome["blocking_locks"] = blocking_locks_held_here()
                deduplicated.set()
                assert request_done.wait(timeout=30)
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
            snapshot_taken.set()
            deduplicated.set()
        finally:
            connections.close_all()

    request = threading.Thread(target=web_request)
    request.start()
    try:
        assert snapshot_taken.wait(timeout=10)
        if path == "changed":
            # A write that leaves the holder pending: its lease renewal where
            # the write under test allows one, otherwise a column set to its
            # own value.
            if write in ("renew", "finish", "prune"):
                with worker._in_flight_lock:
                    worker._in_flight.add((db_task.pk, db_task.lease_epoch))
                assert worker.renew_leases() == 1
            else:
                rows = OxTask.objects.filter(pk=holder.id)
                assert rows.update(priority=F("priority")) == 1
        changed.set()
        assert deduplicated.wait(timeout=10)
        assert "error" not in outcome, outcome["error"]
        assert outcome["level"] == (level or "READ COMMITTED")
        assert outcome["result"].id == holder.id
        expected = task_result_from_db(OxTask.objects.get(pk=holder.id))
        assert result_fields(outcome["result"]) == result_fields(expected)
        refused = vendor == "postgresql" and path == "changed"
        checked = vendor == "postgresql" and level is not None
        assert checks == ((["40001"] if refused else ["granted"]) if checked else [])
        reread = refused or (vendor == "mysql" and level == "REPEATABLE READ")
        assert outcome["opened"] == (["default"] if reread else [])
        assert outcome["closed"]
        assert outcome["blocking_locks"] == [], (
            "the request kept a lock on the task table"
        )
        assert not locked_elsewhere(holder.id), "the request kept a lock on the row"
        on_this_session()
        if write == "renew":
            with worker._in_flight_lock:
                worker._in_flight.add((db_task.pk, db_task.lease_epoch))
            assert worker.renew_leases() == 1
        elif write == "finish":
            worker.execute(db_task)
            assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.SUCCESSFUL
        elif write == "requeue":
            assert worker.reap() == 1
            assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.READY
        elif write == "lose":
            assert worker.reap() == 1
            assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.LOST
        elif write == "claim":
            claimed = worker.claim_one()
            assert claimed is not None, "the claim skipped the holder"
            assert str(claimed.id) == holder.id
        else:
            worker.execute(db_task)
            call_command("ox_prune", older_than="0s", stdout=io.StringIO())
            assert not OxTask.objects.filter(pk=db_task.pk).exists()
    finally:
        changed.set()
        request_done.set()
        request.join(timeout=30)
        assert not request.is_alive()
    assert "error" not in outcome, outcome["error"]


@pytest.mark.django_db(transaction=True)
def test_a_duplicate_at_mysql_serializable_holds_off_the_lease_renewal(
    monkeypatch, worker
):
    """
    A known limit, pinned so it cannot change unnoticed.

    At MySQL's SERIALIZABLE, InnoDB turns every plain read inside a
    transaction into a locking read. A duplicate enqueued there reads the
    holder's row and keeps a shared lock on it until the caller's
    transaction ends. The holder's lease renewal waits on that lock and
    fails with 1205 once innodb_lock_wait_timeout passes, one second here.
    After the caller commits, the renewal lands. 1.2.0 behaves the same.
    """
    if connections["default"].vendor != "mysql":
        pytest.skip("only InnoDB's SERIALIZABLE makes a plain read a locking read")
    on_this_session = shorten_lock_wait(monkeypatch)
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    isolation_for_new_connections(monkeypatch, "SERIALIZABLE")

    deduplicated = threading.Event()
    request_done = threading.Event()
    outcome = {}

    def web_request():
        try:
            with transaction.atomic():
                outcome["level"] = session_isolation()
                outcome["result"] = tasks.unique_echo.enqueue("held")
                deduplicated.set()
                assert request_done.wait(timeout=30)
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
            deduplicated.set()
        finally:
            connections.close_all()

    request = threading.Thread(target=web_request)
    request.start()
    try:
        assert deduplicated.wait(timeout=10)
        assert "error" not in outcome, outcome["error"]
        assert outcome["level"] == "SERIALIZABLE"
        assert outcome["result"].id == holder.id
        assert locked_elsewhere(holder.id)
        on_this_session()
        with worker._in_flight_lock:
            worker._in_flight.add((db_task.pk, db_task.lease_epoch))
        with pytest.raises(OperationalError) as excinfo:
            worker.renew_leases()
        assert excinfo.value.args[0] == 1205
    finally:
        request_done.set()
        request.join(timeout=30)
        assert not request.is_alive()
    assert "error" not in outcome, outcome["error"]
    assert worker.renew_leases() == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("order", ["snapshot-first", "change-first"])
@pytest.mark.parametrize("other", ["outcome", "for-update", "delete"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_holder_another_transaction_is_changing_is_deduplicated_at_once(
    monkeypatch, worker, level, other, order
):
    """
    Another transaction is writing the holder's row, locking it FOR UPDATE
    (as a release path does), or deleting it, and has not committed. The
    row the caller's snapshot holds is still the last committed one. The
    share lock request is refused at once with lock_not_available (55P03),
    and the enqueue deduplicates against that row without waiting, as READ
    COMMITTED would. It must not report the refusal as a lock wait. The
    other transaction's change comes before or after the snapshot, and it
    is rolled back once the enqueue returns. No connection is opened.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    checks = record_holder_checks(monkeypatch)

    writer = connections.create_connection("default")
    writer.set_autocommit(False)
    table = writer.ops.quote_name(OxTask._meta.db_table)
    pk = OxTask._meta.pk.get_db_prep_value(db_task.pk, writer)
    statement, params = {
        "outcome": (
            f"UPDATE {table} SET status = %s, finished_at = now() WHERE id = %s",  # noqa: S608
            [OxTask.Status.SUCCESSFUL, pk],
        ),
        "for-update": (f"SELECT 1 FROM {table} WHERE id = %s FOR UPDATE", [pk]),  # noqa: S608
        "delete": (f"DELETE FROM {table} WHERE id = %s", [pk]),  # noqa: S608
    }[other]

    def change():
        with writer.cursor() as cursor:
            cursor.execute(statement, params)

    isolation_for_new_connections(monkeypatch, level)
    taken = threading.Event()
    go = threading.Event()
    outcome = {}

    def request():
        try:
            with transaction.atomic():
                outcome["level"] = session_isolation()
                assert OxTask.objects.get(id=holder.id).status == OxTask.Status.RUNNING
                taken.set()
                assert go.wait(timeout=10)
                with opened_connections() as opened:
                    outcome["result"] = tasks.unique_echo.enqueue("held")
                outcome["opened"] = [wrapper.alias for wrapper in opened]
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
        finally:
            taken.set()
            connections.close_all()

    try:
        if order == "change-first":
            change()
        thread = threading.Thread(target=request)
        thread.start()
        assert taken.wait(timeout=10)
        if order == "snapshot-first":
            change()
        go.set()
        # The other transaction is still open, so an enqueue that waited for
        # it would still be running when the join gives up.
        thread.join(timeout=30)
        assert not thread.is_alive()
    finally:
        writer.rollback()
        writer.close()

    assert "error" not in outcome, outcome["error"]
    assert outcome["level"] == level
    assert outcome["result"].id == holder.id
    assert outcome["result"].status == "RUNNING"
    assert checks == ["55P03"]
    assert outcome["opened"] == []
    assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.RUNNING


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_enqueues_of_two_keys_in_open_transactions_do_not_wait_on_each_other(
    monkeypatch, level
):
    """
    A request that enqueued one unique key and keeps its transaction open
    does not hold up an enqueue of another key. The lock UPDATE that names
    the new task matches the row's key as well as its unset task. The first
    request stays open until the second enqueue is done, and the lock wait
    here is one second, so an enqueue that waited on the first would raise
    UniqueEnqueueRace.
    """
    if connections["default"].vendor == "sqlite":
        pytest.skip("SQLite has one writer at a time")
    on_this_session = shorten_lock_wait(monkeypatch)
    tasks.unique_echo.enqueue("seed")
    connections["default"].close()
    isolation_for_new_connections(monkeypatch, level)
    enqueued = threading.Event()
    release = threading.Event()
    outcome = {}

    def open_request():
        try:
            with transaction.atomic():
                outcome["a"] = tasks.unique_echo.enqueue("a")
                enqueued.set()
                assert release.wait(timeout=30)
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["a_error"] = exc
            enqueued.set()
        finally:
            connections.close_all()

    request = threading.Thread(target=open_request)
    request.start()
    try:
        assert enqueued.wait(timeout=10)
        on_this_session()
        try:
            outcome["b"] = tasks.unique_echo.enqueue("b")
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["b_error"] = exc
    finally:
        release.set()
        request.join(timeout=30)
        connections["default"].close()
    assert not request.is_alive()

    assert "a_error" not in outcome, outcome["a_error"]
    assert "b_error" not in outcome, outcome["b_error"]
    assert OxTask.objects.count() == 3


def session_id():
    """This thread's server session, as the lock views name it."""
    connection = connections["default"]
    query = (
        "SELECT pg_backend_pid()"
        if connection.vendor == "postgresql"
        else "SELECT CONNECTION_ID()"
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        (value,) = cursor.fetchone()
    return value


def waits_on_a_lock(session, timeout=10.0):
    """
    Whether that session is seen waiting for a row lock within the timeout.

    MySQL is asked through performance_schema, which reports each wait as
    it stands. information_schema.innodb_trx serves a cache that InnoDB
    refreshes only after 0.1 s without a read, so polling it faster keeps
    returning what it held when polling began.
    """
    connection = connections["default"]
    if connection.vendor == "postgresql":
        query = (
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE pid = %s AND wait_event_type = 'Lock'"
        )
    else:
        query = (
            "SELECT count(*) FROM performance_schema.data_lock_waits waits "
            "JOIN performance_schema.threads threads "
            "ON threads.THREAD_ID = waits.REQUESTING_THREAD_ID "
            "WHERE threads.PROCESSLIST_ID = %s"
        )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with connection.cursor() as cursor:
            cursor.execute(query, [session])
            (count,) = cursor.fetchone()
        if count:
            return True
        time.sleep(0.02)
    return False


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_a_release_committing_while_the_lock_insert_waits_does_not_fail_the_enqueue(
    monkeypatch, level
):
    """
    An enqueue that owns its transaction queues its task when another
    process's release of the key commits while the lock INSERT waits on it.

    PostgreSQL at REPEATABLE READ or SERIALIZABLE takes the snapshot at the
    lock INSERT. The INSERT waits on the release's DELETE of the old lock
    row and goes ahead once it commits, but the snapshot still shows that
    row. The UPDATE that names the new task on the lock matched the old row
    by key, and PostgreSQL refused it as a concurrent delete with a
    serialization failure (SQLSTATE 40001), which reached the caller raw.
    The UPDATE now also requires the lock's task to be unset. Only the row
    this transaction inserted is, because a committed lock row always
    names its task.

    The enqueue takes one pass and sends task_enqueued once, for the task
    that commits. The release runs on another connection at READ COMMITTED,
    and it commits once the enqueue is seen waiting on it.
    """
    if connections["default"].vendor == "sqlite":
        pytest.skip("SQLite's single writer leaves no lock INSERT waiting on a row")
    first = tasks.unique_echo.enqueue("x")
    OxTask.objects.filter(id=first.id).update(status=OxTask.Status.FAILED)
    lock = OxpullUniqueLock.objects.get()

    releaser = connections.create_connection("default")
    releaser.set_autocommit(False)
    table = releaser.ops.quote_name(OxpullUniqueLock._meta.db_table)
    key = releaser.ops.quote_name(OxpullUniqueLock._meta.pk.column)
    with releaser.cursor() as cursor:
        cursor.execute(f"DELETE FROM {table} WHERE {key} = %s", [lock.pk])  # noqa: S608
        assert cursor.rowcount == 1
    connections["default"].close()
    isolation_for_new_connections(monkeypatch, level)

    started = threading.Event()
    outcome = {}

    def enqueue():
        try:
            outcome["session"] = session_id()
            with transaction.atomic():
                # PostgreSQL applies a level set in OPTIONS as each
                # transaction begins, so the level is asked inside one.
                outcome["level"] = session_isolation()
            started.set()
            outcome["result"] = tasks.unique_echo.enqueue("x")
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
        finally:
            started.set()
            connections.close_all()

    signalled = []

    def record(sender, task_result, **kwargs):
        signalled.append(task_result.id)

    task_enqueued.connect(record)
    thread = threading.Thread(target=enqueue)
    thread.start()
    try:
        assert started.wait(timeout=10)
        assert waits_on_a_lock(outcome.get("session")), "the INSERT never waited"
        releaser.commit()
    finally:
        releaser.close()
        thread.join(timeout=30)
        task_enqueued.disconnect(record)
    assert not thread.is_alive()

    assert "error" not in outcome, outcome["error"]
    assert outcome["level"] == level
    second = outcome["result"]
    assert second.id != first.id
    ready = OxTask.objects.filter(status=OxTask.Status.READY)
    assert [str(task.id) for task in ready] == [second.id]
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id
    assert signalled == [second.id], "a pass was refused and reissued"


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_serialization_failure_in_the_enqueues_own_transaction_is_reissued(
    monkeypatch, worker, level
):
    """
    PostgreSQL at REPEATABLE READ or SERIALIZABLE refuses to lock a row
    that changed after the transaction's snapshot began. It raises a
    serialization failure (SQLSTATE 40001), and the transaction it
    happened in is lost. With no caller's transaction around the enqueue,
    the snapshot is the resolution's own, taken at its read of the lock. An
    operator retry committing between that read and the locking read of a
    FAILED holder raised the failure raw where the API promises a
    TaskResult. The enqueue owned the transaction the failure ended, so
    nothing of it survived, and the pass is now reissued as an InnoDB
    deadlock's is. The next pass reads the retried holder as live.
    """
    from django_ox import actions

    if connections["default"].vendor != "postgresql":
        pytest.skip("only PostgreSQL refuses a locking read behind the snapshot")
    first = tasks.unique_fail_always.enqueue()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED

    retried = {}

    def retry_on_another_connection():
        try:
            retried["ok"] = actions.retry(first.id)
        finally:
            connections.close_all()

    holder = OxpullBackend._holder
    staged = []
    refused = []

    def retry_before_the_locking_read(task_id, db_alias, *, locking, **kwargs):
        if not locking or staged:
            return holder(task_id, db_alias, locking=locking, **kwargs)
        staged.append(task_id)
        thread = threading.Thread(target=retry_on_another_connection)
        thread.start()
        thread.join(timeout=30)
        assert not thread.is_alive()
        try:
            return holder(task_id, db_alias, locking=True, **kwargs)
        except OperationalError as exc:
            refused.append(pg_sqlstate(exc))
            raise

    monkeypatch.setattr(
        OxpullBackend, "_holder", staticmethod(retry_before_the_locking_read)
    )
    set_session_isolation(level)
    try:
        second = tasks.unique_fail_always.enqueue()
    finally:
        connections["default"].close()

    assert retried.get("ok") is True
    assert refused == ["40001"]
    assert second.id == first.id
    ready = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_fail_always", status=OxTask.Status.READY
    )
    assert ready.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("path", ["unchanged", "changed"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_live_thread_that_resolved_a_duplicate_leaves_no_session_behind(
    monkeypatch, level, path
):
    """
    PostgreSQL refuses DROP DATABASE while any session is connected, so a
    test database cannot be dropped at teardown while a thread that is still
    alive keeps a connection open. A thread from a pool resolves a duplicate
    inside a transaction at a snapshot level and closes its own connections,
    as a request thread does. On the path "changed", the holder's row changed
    after the thread's snapshot began, so the enqueue also read the holder on
    a connection of its own. The pool stays alive, and the database must have
    no more sessions than before.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("only PostgreSQL refuses to drop a database with sessions")
    holder = tasks.unique_echo.enqueue("held")
    connections["default"].close()
    isolation_for_new_connections(monkeypatch, level)
    checks = record_holder_checks(monkeypatch)

    def work():
        try:
            with transaction.atomic():
                OxTask.objects.get(id=holder.id)
                if path == "changed":
                    touch_elsewhere(holder.id)
                return tasks.unique_echo.enqueue("held").id
        finally:
            connections.close_all()

    pool = ThreadPoolExecutor(max_workers=1)
    try:
        warmed_pool()
        before = other_sessions()
        assert pool.submit(work).result(timeout=30) == holder.id
        assert settle_sessions(before) <= before, "a live thread left a session open"
        assert checks == (["40001"] if path == "changed" else ["granted"])
    finally:
        pool.shutdown(wait=True)
        connections["default"].close()


@pytest.mark.django_db(transaction=True)
def test_a_retry_landing_between_the_read_and_the_release_keeps_its_lock(
    monkeypatch, worker
):
    """
    SQLite: the release is conditional on the holder in the DELETE itself.

    SQLite has no row locks, and a transaction that reads and then
    writes fails "database is locked" the moment two of them race, so
    the resolver runs statement by statement there. Between its read of
    a FAILED holder and its DELETE of the lock, an operator retry could
    commit READY; the DELETE then removed a live lock and the next
    attempt inserted a second READY row beside the retried one. The
    DELETE now carries the holder's status in its own WHERE clause, and
    a statement is atomic against SQLite's single writer, so a retry
    that landed first leaves the lock where it is.

    PostgreSQL and MySQL take the holder's row lock on this path, so a
    retry waits behind it and lands after the release, as the documented
    operator bypass; the interleaving here cannot be staged there.
    """
    from django.db.models.query import QuerySet

    from django_ox import actions
    from oxpull.models import OxpullUniqueLock as Lock

    if connections["default"].vendor != "sqlite":
        pytest.skip("the row lock makes a retry wait behind the release here")
    first = tasks.unique_fail_always.enqueue()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED

    retried = {}

    def retry_on_another_connection():
        try:
            retried["ok"] = actions.retry(first.id)
        finally:
            connections.close_all()

    original_delete = QuerySet.delete

    def delete_after_a_retry(self, *args, **kwargs):
        if self.model is Lock and "ok" not in retried:
            thread = threading.Thread(target=retry_on_another_connection)
            thread.start()
            thread.join(timeout=30)
            assert retried.get("ok") is True
        return original_delete(self, *args, **kwargs)

    monkeypatch.setattr(QuerySet, "delete", delete_after_a_retry)
    second = tasks.unique_fail_always.enqueue()

    assert retried.get("ok") is True
    assert second.id == first.id
    ready = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_fail_always", status=OxTask.Status.READY
    )
    assert ready.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id


@pytest.mark.django_db(transaction=True)
def test_the_message_says_when_the_server_rolled_the_whole_transaction_back(
    monkeypatch,
):
    """
    MySQL with innodb_rollback_on_timeout=ON rolls the whole transaction
    back on 1205, not the statement. Django's rollback to the enqueue's
    savepoint then fails, the connection is marked for rollback, and
    nothing the caller did in that transaction survives; the message
    used to promise a savepoint rollback and a usable transaction there.
    The server setting is not dynamic, so this stages what the driver
    reports on such a server: the savepoint rollback raising once the
    holder is in place. Against a server with the setting on, the real
    exchange produces the same message.
    """
    from django.db.backends.base.base import BaseDatabaseWrapper

    on_this_session = shorten_lock_wait(monkeypatch)
    armed = {"fail": False}
    real_rollback = BaseDatabaseWrapper.savepoint_rollback

    def rollback_as_if_the_transaction_were_gone(self, sid):
        if armed["fail"]:
            raise OperationalError(1305, f"SAVEPOINT {sid} does not exist")
        return real_rollback(self, sid)

    monkeypatch.setattr(
        BaseDatabaseWrapper,
        "savepoint_rollback",
        rollback_as_if_the_transaction_were_gone,
    )

    def loser():
        on_this_session()
        armed["fail"] = True
        caught = None
        try:
            with transaction.atomic():
                try:
                    tasks.unique_echo.enqueue("held")
                except UniqueEnqueueRace as exc:
                    caught = exc
                    assert connections["default"].needs_rollback
        except OperationalError:
            # The atomic block's own exit rolls the marked transaction
            # back; the driver's error is not what this test is about.
            pass
        return caught

    with transaction.atomic():
        tasks.unique_echo.enqueue("held")
        results, errors = race_all([loser])

    assert errors == []
    (caught,) = results
    assert isinstance(caught, UniqueEnqueueRace)
    assert "rolled that whole transaction back" in str(caught)
    assert "innodb_rollback_on_timeout" in str(caught)
    assert "still usable" not in str(caught)


def test_the_message_names_a_refused_snapshot_upgrade_on_sqlite():
    """
    SQLITE_BUSY_SNAPSHOT is not a lock wait: SQLite in WAL mode refuses
    at once to make a writer of a reader whose snapshot is behind another
    connection's commit. The exception class is the same, the sentence
    is not. The extended code is staged on the cause; the primary code
    (5) is what the classifier keys on either way.
    """
    import sqlite3

    from django.tasks import default_task_backend

    from oxpull.backend import SQLITE_BUSY_SNAPSHOT, _is_lock_wait_timeout

    if connections["default"].vendor != "sqlite":
        pytest.skip("the extended code is SQLite's")

    class Refused(sqlite3.OperationalError):
        # The module sets sqlite_errorcode on the errors it raises; a
        # constructed one carries it as a class attribute instead.
        sqlite_errorcode = SQLITE_BUSY_SNAPSHOT

    class Busy(sqlite3.OperationalError):
        sqlite_errorcode = 5

    backend = default_task_backend
    exc = OperationalError("database is locked")
    exc.__cause__ = Refused("database is locked")
    assert _is_lock_wait_timeout(exc, "default")
    message = backend._lock_wait_message(
        tasks.unique_echo, exc, "default", ambient_atomic=True
    )
    assert "SQLITE_BUSY_SNAPSHOT" in message
    assert "did not wait" in message
    assert "whole transaction" in message
    plain = OperationalError("database is locked")
    plain.__cause__ = Busy("database is locked")
    assert "SQLITE_BUSY_SNAPSHOT" not in backend._lock_wait_message(
        tasks.unique_echo, plain, "default", ambient_atomic=False
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("project", ["plain", "session-defaults"])
@pytest.mark.parametrize("path", ["deduplicated", "race", "error", "refused"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_fresh_read_connection_is_closed_on_every_path(
    monkeypatch, worker, level, path, project
):
    """
    The read on a connection of its own opens one session and closes it
    before the enqueue returns. That holds when the enqueue deduplicates,
    when it raises UniqueEnqueueRace, when the read fails once connected, and
    when the connection is refused.

    The holder's row changes after the request's snapshot began. A lease
    renewal leaves it RUNNING, or the outcome write finishes it. For "error"
    the statement on the new connection fails once it has connected, and for
    "refused" the connection goes to a port nothing listens on. A failed read
    raises UniqueEnqueueRace chained to the failure, with its first line in
    the message. While the request is still open, pg_stat_activity must show
    no session beyond the ones there before the enqueue, and each connection
    the enqueue created must be closed.

    The project's OPTIONS name the caller's level. For "session-defaults"
    they also start every new session at SERIALIZABLE with a 700 ms
    lock_timeout and statement_timeout, as a project's OPTIONS["options"] or
    a role's settings can. The new connection is built without the level or a
    pool, and its read runs in a transaction of its own at READ COMMITTED
    with lock_timeout 100ms and statement_timeout 2s. Those last for that
    transaction alone: after its COMMIT the session is back at its own
    settings.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    connections["default"].close()
    isolation_for_new_connections(monkeypatch, level)
    options = connections["default"].settings_dict["OPTIONS"]
    injected = OperationalError("injected once connected")

    snapshot_taken = threading.Event()
    changed = threading.Event()
    returned = threading.Event()
    request_done = threading.Event()
    outcome = {}

    def failing(create):
        def create_connection(alias):
            wrapper = create(alias)
            if path == "refused":
                wrapper.settings_dict = {**wrapper.settings_dict, "PORT": "1"}
                return wrapper
            connect_and_open = wrapper.cursor

            def cursor(*args, **kwargs):
                connect_and_open(*args, **kwargs)
                raise injected

            wrapper.cursor = cursor
            return wrapper

        return create_connection

    def web_request():
        try:
            with transaction.atomic():
                OxTask.objects.get(id=holder.id)
                snapshot_taken.set()
                assert changed.wait(timeout=10)
                # Set here, after every other connection in the test is open,
                # so only the sessions opened from now on start with them.
                if project == "session-defaults":
                    options["options"] = (
                        "-c default_transaction_isolation=serializable "
                        "-c lock_timeout=700ms -c statement_timeout=700ms"
                    )
                try:
                    outcome["session"] = new_session_settings()
                    with (
                        how_new_connections_run() as runs,
                        opened_connections() as opened,
                        fresh_read_transactions() as reads,
                    ):
                        if path in ("error", "refused"):
                            connections.create_connection = failing(
                                connections.create_connection
                            )
                        try:
                            outcome["result"] = tasks.unique_echo.enqueue("held")
                        except (UniqueEnqueueRace, OperationalError) as exc:
                            outcome["raised"] = exc
                finally:
                    options.pop("options", None)
                outcome["opened"] = [wrapper.alias for wrapper in opened]
                outcome["closed"] = all(
                    wrapper.connection is None for wrapper in opened
                )
                outcome["runs"] = runs
                outcome["reads"] = reads
                outcome["usable"] = OxTask.objects.filter(id=holder.id).count()
                returned.set()
                assert request_done.wait(timeout=30)
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
            snapshot_taken.set()
            returned.set()
        finally:
            connections.close_all()

    warmed_pool()
    request = threading.Thread(target=web_request)
    request.start()
    try:
        assert snapshot_taken.wait(timeout=10)
        if path == "race":
            worker.execute(db_task)
        else:
            with worker._in_flight_lock:
                worker._in_flight.add((db_task.pk, db_task.lease_epoch))
            assert worker.renew_leases() == 1
        with_request = settle_sessions(other_sessions())
        changed.set()
        assert returned.wait(timeout=10)
        assert "error" not in outcome, outcome["error"]
        while_open = settle_sessions(with_request)
    finally:
        changed.set()
        request_done.set()
        request.join(timeout=30)
        assert not request.is_alive()
    # The request's own session ends with its thread's close_all(), unless
    # Django's pool took the connection back, where it stays open and idle.
    ended = 0 if connections["default"].settings_dict["OPTIONS"].get("pool") else 1
    after = settle_sessions(with_request - ended)

    assert "error" not in outcome, outcome["error"]
    assert while_open == with_request, "the enqueue left its connection open"
    assert after == with_request - ended
    assert outcome["opened"] == ["default"]
    assert outcome["closed"]
    assert outcome["usable"] == 1
    session = outcome["session"]
    if project == "session-defaults":
        assert session == ("serializable", "700ms", "700ms")
    (read,) = outcome["reads"]
    if path == "refused":
        assert outcome["runs"] == []
    else:
        assert [(run["level"], run["autocommit"]) for run in outcome["runs"]] == [
            (session[0].upper(), True)
        ]
        assert "isolation_level" not in outcome["runs"][0]["options"]
        assert "pool" not in outcome["runs"][0]["options"]
    if path in ("deduplicated", "race"):
        assert read["statements"] == ["BEGIN", "SELECT", "COMMIT"]
        assert read["at_read"] == ("read committed", "100ms", "2s")
        assert read["after"] == session
    else:
        assert read["statements"] == []
    if path == "deduplicated":
        assert "raised" not in outcome, outcome["raised"]
        assert outcome["result"].id == holder.id
    elif path == "race":
        assert isinstance(outcome.get("raised"), UniqueEnqueueRace)
        assert "A fresh read found the task SUCCESSFUL." in str(outcome["raised"])
    else:
        raised = outcome.get("raised")
        assert isinstance(raised, UniqueEnqueueRace), raised
        if path == "error":
            assert raised.__cause__ is injected
        else:
            assert isinstance(raised.__cause__, OperationalError)
        first_line = str(raised.__cause__).strip().splitlines()[0]
        message = str(raised)
        assert f"A fresh read on a new connection failed ({first_line})." in message
        assert "on database 'default'" in message
        assert f"inside a transaction at {level}," in message
        assert "The enqueue created nothing." in message


def cancel_lock_waits(besides):
    """
    Cancel every session on the test database that waits for a lock, other
    than this thread's and `besides`. Returns the sessions it cancelled.
    """
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "SELECT pid, pg_cancel_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock' "
            "AND pid <> pg_backend_pid() AND pid IS DISTINCT FROM %s",
            [besides],
        )
        return cursor.fetchall()


def a_duplicate_behind_a_table_lock(monkeypatch, worker, level, table, blocker):
    """
    Enqueue a duplicate whose fresh read meets a lock on the task table or the
    lock table that can't be granted while the caller's transaction is open.

    The caller's transaction reads the key's lock and the holder, which holds
    ACCESS SHARE on both tables, and the holder's lease is then renewed, so
    the holder check is refused and the enqueue reads again on a new
    connection. Before the enqueue, another session asks for ACCESS EXCLUSIVE
    on the table and waits on the caller ("queued"), or the caller's own
    transaction takes it ("caller"). After the enqueue the caller checks its
    transaction still answers, commits, and retries at READ COMMITTED.

    A read with no bound would wait for good, so if the enqueue hasn't
    returned after five seconds the waiting read is cancelled and that is
    recorded as "watchdog".
    """
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    checks = record_holder_checks(monkeypatch)
    model = OxTask if table == "task" else OxpullUniqueLock
    quoted = connections["default"].ops.quote_name(model._meta.db_table)
    take_the_table = f"LOCK TABLE {quoted} IN ACCESS EXCLUSIVE MODE"
    snapshot_taken = threading.Event()
    go = threading.Event()
    outcome = {"checks": checks, "holder": holder}
    locker = {}

    def web_request():
        try:
            set_session_isolation(level)
            with transaction.atomic():
                assert str(OxpullUniqueLock.objects.get().task_id) == holder.id
                assert OxTask.objects.get(id=holder.id).status == OxTask.Status.RUNNING
                snapshot_taken.set()
                assert go.wait(timeout=10)
                if blocker == "caller":
                    with connections["default"].cursor() as cursor:
                        cursor.execute(take_the_table)
                with opened_connections() as opened:
                    started = time.monotonic()
                    try:
                        outcome["result"] = tasks.unique_echo.enqueue("held")
                    except (UniqueEnqueueRace, OperationalError) as exc:
                        outcome["raised"] = exc
                    outcome["seconds"] = time.monotonic() - started
                outcome["opened"] = [wrapper.alias for wrapper in opened]
                outcome["closed"] = all(
                    wrapper.connection is None for wrapper in opened
                )
                outcome["usable"] = OxTask.objects.filter(id=holder.id).count()
                outcome["before_commit"] = time.monotonic()
            with transaction.atomic():
                set_transaction_isolation("READ COMMITTED")
                outcome["retried"] = tasks.unique_echo.enqueue("held")
        except BaseException as exc:  # pragma: no cover - reported by the test
            outcome["error"] = exc
        finally:
            snapshot_taken.set()
            connections.close_all()

    def lock_the_table():
        other = connections.create_connection("default")
        try:
            other.set_autocommit(False)
            with other.cursor() as cursor:
                cursor.execute("SET LOCAL lock_timeout = '20s'")
                cursor.execute("SELECT pg_backend_pid()")
                (locker["pid"],) = cursor.fetchone()
                try:
                    cursor.execute(take_the_table)
                    locker["granted_at"] = time.monotonic()
                except OperationalError as exc:
                    locker["error"] = exc
            other.rollback()
        finally:
            other.close()

    request = threading.Thread(target=web_request)
    request.start()
    lock_taker = None
    try:
        assert snapshot_taken.wait(timeout=10)
        assert "error" not in outcome, outcome["error"]
        with worker._in_flight_lock:
            worker._in_flight.add((db_task.pk, db_task.lease_epoch))
        assert worker.renew_leases() == 1
        if blocker == "queued":
            lock_taker = threading.Thread(target=lock_the_table)
            lock_taker.start()
            deadline = time.monotonic() + 10
            while "pid" not in locker and time.monotonic() < deadline:
                time.sleep(0.01)
            assert waits_on_a_lock(locker["pid"]), "the table lock is not waiting"
        go.set()
        request.join(timeout=5)
        if request.is_alive():
            outcome["watchdog"] = cancel_lock_waits(locker.get("pid"))
    finally:
        go.set()
        request.join(timeout=30)
        if lock_taker is not None:
            lock_taker.join(timeout=30)
    assert not request.is_alive()
    outcome["locker"] = locker
    return outcome


def assert_the_read_gave_up(outcome, level, sqlstate, reason):
    """The enqueue raised UniqueEnqueueRace for the read's failure, in time."""
    assert "error" not in outcome, outcome["error"]
    assert "watchdog" not in outcome, (
        f"the read waited until it was cancelled: {outcome['watchdog']}"
    )
    raised = outcome.get("raised")
    assert isinstance(raised, UniqueEnqueueRace), raised
    assert pg_sqlstate(raised.__cause__) == sqlstate
    message = str(raised)
    assert f"A fresh read on a new connection failed ({reason})." in message
    assert "on database 'default'" in message
    assert f"inside a transaction at {level}," in message
    assert "The enqueue created nothing." in message
    assert outcome["seconds"] < 2
    assert outcome["checks"] == ["40001"]
    assert outcome["opened"] == ["default"]
    assert outcome["closed"]
    assert outcome["usable"] == 1
    locker = outcome["locker"]
    if locker:
        assert "error" not in locker, locker["error"]
        assert locker["granted_at"] > outcome["before_commit"]
    holder = outcome["holder"]
    assert outcome["retried"].id == holder.id
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == holder.id
    return message


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("blocker", ["queued", "caller"])
@pytest.mark.parametrize("table", ["task", "lock"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_fresh_read_stops_waiting_for_a_table_lock_held_off_by_the_caller(
    monkeypatch, worker, level, table, blocker
):
    """
    The read on a connection of its own can meet a lock cycle PostgreSQL
    can't see, and it stops waiting instead of waiting for good.

    A request for ACCESS EXCLUSIVE on the task table or the lock table, such
    as a migration's ALTER TABLE, waits on the caller's transaction, or that
    transaction holds the lock itself. The fresh read queues behind it, and
    the caller waits on the read outside the database, so no deadlock is
    detected and nothing times out by default. The read stops after its
    100ms lock timeout. The enqueue raises UniqueEnqueueRace chained to
    lock_not_available (55P03), says the read waits at most 100ms for a lock,
    and closes the connection. The caller's transaction still answers and
    commits, the queued request gets its lock only then, and the retry at
    READ COMMITTED returns the holder.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    outcome = a_duplicate_behind_a_table_lock(
        monkeypatch, worker, level, table, blocker
    )

    message = assert_the_read_gave_up(
        outcome, level, "55P03", "canceling statement due to lock timeout"
    )
    assert "That read waits at most 100ms for a lock." in message
    assert bool(outcome["locker"]) is (blocker == "queued")


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_fresh_read_stops_at_its_statement_timeout(monkeypatch, worker, level):
    """
    The statement timeout bounds the read too. With the read's lock timeout
    set above it, a read queued behind a table lock is ended by the statement
    timeout instead (57014). The enqueue raises UniqueEnqueueRace chained to
    that, without the sentence about lock waits.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    monkeypatch.setattr(backend_module, "FRESH_READ_LOCK_TIMEOUT", "10s", raising=False)
    monkeypatch.setattr(
        backend_module, "FRESH_READ_STATEMENT_TIMEOUT", "200ms", raising=False
    )
    outcome = a_duplicate_behind_a_table_lock(
        monkeypatch, worker, level, "task", "queued"
    )

    message = assert_the_read_gave_up(
        outcome, level, "57014", "canceling statement due to statement timeout"
    )
    assert "waits at most" not in message


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("released", ["replaced", "pruned"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_lock_released_behind_a_snapshot_never_leaves_two_pending_tasks(
    monkeypatch, worker, level, released, request
):
    """
    A release that races a replaced lock, run through the enqueue itself.

    The first task ran and finished, so its lock is stale. The caller's
    snapshot reads the key's lock and the finished task. Then, at READ
    COMMITTED, another enqueue of the key releases that lock and queues a
    second task ("replaced"), or oxpull_prune clears the stale lock
    ("pruned"). Both commit. The caller then enqueues the key inside its
    transaction. Its snapshot still shows the old lock naming the finished
    task, so the enqueue takes the release path. Whatever that path does, it
    must never leave two pending tasks for the key, and after the caller
    commits, the key's lock must name the one pending task. On PostgreSQL
    the release is refused (40001) when the lock was replaced, and the
    enqueue raises UniqueEnqueueRace. MySQL cannot see the new lock and
    raises after five attempts. When the lock was only pruned, the enqueue
    queues the task. What the caller got is added to the test's report.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and the release would wait")
    first = tasks.unique_echo.enqueue("key")
    assert worker.run_once() is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL
    checks = record_holder_checks(monkeypatch)

    snapshot_taken = threading.Event()
    lock_released = threading.Event()
    outcome = {}

    def caller():
        try:
            set_session_isolation(level)
            with transaction.atomic():
                assert str(OxpullUniqueLock.objects.get().task_id) == first.id
                assert (
                    OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL
                )
                snapshot_taken.set()
                assert lock_released.wait(timeout=10)
                try:
                    outcome["result"] = tasks.unique_echo.enqueue("key")
                except UniqueEnqueueRace as exc:
                    outcome["race"] = exc
                except OperationalError as exc:
                    outcome["raw"] = exc
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
        finally:
            snapshot_taken.set()
            connections.close_all()

    thread = threading.Thread(target=caller)
    thread.start()
    assert snapshot_taken.wait(timeout=10)
    if released == "replaced":
        second = tasks.unique_echo.enqueue("key")
        assert second.id != first.id
    else:
        call_command("oxpull_prune", stdout=io.StringIO())
        assert OxpullUniqueLock.objects.count() == 0
    lock_released.set()
    thread.join(timeout=30)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome["error"]

    pending = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_echo",
        status__in=(OxTask.Status.READY, OxTask.Status.RUNNING),
    )
    got = next(key for key in ("result", "race", "raw") if key in outcome)
    if got == "result":
        detail = outcome["result"].id
    else:
        cause = outcome[got].__cause__ if got == "race" else outcome[got]
        detail = f"{outcome[got]} | cause: {cause!r}"
    request.node.add_report_section(
        "call",
        "released behind a snapshot",
        f"{vendor} {level} {released}: {got} [{detail}] checks={checks} "
        f"pending={pending.count()}",
    )
    assert "raw" not in outcome, outcome["raw"]
    if released == "pruned":
        assert got == "result"
        assert outcome["result"].id != first.id
    elif vendor == "postgresql":
        assert got == "race"
        assert "PostgreSQL refused a row" in str(outcome["race"])
        assert pg_sqlstate(outcome["race"].__cause__) == "40001"
    else:
        assert got == "race"
        assert "after 5 attempts" in str(outcome["race"])
    assert pending.count() == 1, [(str(t.id), t.status) for t in pending]
    assert str(OxpullUniqueLock.objects.get().task_id) == str(pending.get().id)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("writer", ["outcome", "delete", "release"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_writer_committing_after_the_refusal_is_the_read_committed_window(
    monkeypatch, worker, level, writer
):
    """
    lock_not_available (55P03) is contention compatible with deduplicating,
    not a fresh read.

    The holder is RUNNING in the caller's snapshot. Another transaction holds
    the row: it wrote SUCCESSFUL ("outcome"), deleted the row ("delete"), or
    locked it FOR UPDATE to release the key ("release"). The check is refused
    with 55P03. That transaction commits after the refusal and before the
    enqueue returns, and for "release" it writes SUCCESSFUL and deletes the
    key's lock first. The enqueue returns the RUNNING row it read, as a READ
    COMMITTED read made just before that commit would have. It opens no
    connection and does not wait. After the caller commits, the next enqueue
    of the key queues a new task, and exactly one task is pending.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    lock_key = OxpullUniqueLock.objects.get().pk
    checks = record_holder_checks(monkeypatch)

    other = connections.create_connection("default")
    other.set_autocommit(False)
    task_table = other.ops.quote_name(OxTask._meta.db_table)
    lock_table = other.ops.quote_name(OxpullUniqueLock._meta.db_table)
    key_column = other.ops.quote_name(OxpullUniqueLock._meta.pk.column)
    pk = OxTask._meta.pk.get_db_prep_value(db_task.pk, other)

    def run(sql, params):
        with other.cursor() as cursor:
            cursor.execute(sql, params)
            assert cursor.rowcount == 1

    finish = (
        f"UPDATE {task_table} SET status = %s WHERE id = %s",  # noqa: S608
        [OxTask.Status.SUCCESSFUL, pk],
    )
    begin = {
        "outcome": finish,
        "delete": (f"DELETE FROM {task_table} WHERE id = %s", [pk]),  # noqa: S608
        "release": (f"SELECT 1 FROM {task_table} WHERE id = %s FOR UPDATE", [pk]),  # noqa: S608
    }[writer]
    check_holder_row = OxpullBackend._check_holder_row
    committed = []

    def check_then_commit(cls, task_pk, db_alias):
        answer = check_holder_row(task_pk, db_alias)
        if writer == "release":
            run(*finish)
            run(f"DELETE FROM {lock_table} WHERE {key_column} = %s", [lock_key])  # noqa: S608
        other.commit()
        committed.append(answer)
        return answer

    monkeypatch.setattr(
        OxpullBackend, "_check_holder_row", classmethod(check_then_commit)
    )
    set_session_isolation(level)
    try:
        with transaction.atomic():
            assert session_isolation() == level
            assert OxTask.objects.get(id=holder.id).status == OxTask.Status.RUNNING
            run(*begin)
            # The writer commits only once the check has answered, so a check
            # that waited for the writer would wait for good. The statement
            # timeout ends such a wait with query_canceled (57014), which the
            # enqueue raises as it is.
            with connections["default"].cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '30s'")
            with opened_connections() as opened:
                result = tasks.unique_echo.enqueue("held")
    finally:
        other.rollback()
        other.close()
        connections["default"].close()

    assert checks == ["55P03"]
    assert committed == [None], "the writer did not commit before the return"
    assert result.id == holder.id
    assert result.status == "RUNNING"
    assert opened == []
    after = tasks.unique_echo.enqueue("held")
    assert after.id != holder.id
    pending = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_echo",
        status__in=(OxTask.Status.READY, OxTask.Status.RUNNING),
    )
    assert [str(task.id) for task in pending] == [after.id]
    assert str(OxpullUniqueLock.objects.get().task_id) == after.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("path", ["unchanged", "changed", "finished"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_holder_check_inside_nested_atomic_blocks(monkeypatch, worker, level, path):
    """
    The check's savepoint sits inside Django's own savepoints. The enqueue
    runs two atomic blocks deep inside the caller's transaction, after the
    holder's row stayed as it was, changed while the task stayed RUNNING, or
    changed as the task finished. Each block exits as it would around any
    call: normally after a deduplicated enqueue, and through the exception
    after UniqueEnqueueRace. The caller's transaction then still answers a
    query, writes a task, and commits.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    checks = record_holder_checks(monkeypatch)
    set_session_isolation(level)
    outcome = {}
    try:
        with transaction.atomic():
            assert OxTask.objects.get(id=holder.id).status == OxTask.Status.RUNNING
            if path == "changed":
                touch_elsewhere(holder.id)
            elif path == "finished":
                touch_elsewhere(holder.id, OxTask.Status.SUCCESSFUL)
            try:
                with transaction.atomic(), transaction.atomic():
                    outcome["result"] = tasks.unique_echo.enqueue("held")
            except UniqueEnqueueRace as exc:
                outcome["race"] = exc
            outcome["count"] = OxTask.objects.filter(id=holder.id).count()
            outcome["plain"] = tasks.plain_echo.enqueue("after").id
    finally:
        connections["default"].close()

    assert OxTask.objects.filter(id=outcome["plain"]).exists()
    assert outcome["count"] == 1
    if path == "finished":
        assert isinstance(outcome.get("race"), UniqueEnqueueRace)
        assert checks == ["40001"]
    else:
        assert "race" not in outcome, outcome["race"]
        assert outcome["result"].id == holder.id
        assert checks == (["40001"] if path == "changed" else ["granted"])


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("earlier", ["for-update", "update"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_lock_the_caller_already_holds_on_the_holder_outlives_the_check(
    monkeypatch, level, earlier
):
    """
    Rolling back the check's savepoint releases only what the check took.

    The caller's transaction locked the holder's row FOR UPDATE, or updated
    it, before it enqueued a duplicate. The check's share lock is granted on
    a row the transaction already holds, and the enqueue deduplicates without
    opening a connection. The caller's own row lock is still there after the
    savepoint is rolled back. Another connection is refused the row until the
    caller commits, and gets it right after.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    checks = record_holder_checks(monkeypatch)
    set_session_isolation(level)
    try:
        with transaction.atomic():
            if earlier == "for-update":
                OxTask.objects.select_for_update().get(id=holder.id)
            else:
                assert OxTask.objects.filter(id=holder.id).update(priority=5) == 1
            assert locked_elsewhere(holder.id), "the caller holds no lock to keep"
            with opened_connections() as opened:
                result = tasks.unique_echo.enqueue("held")
            held_after = locked_elsewhere(holder.id)
    finally:
        connections["default"].close()

    assert result.id == holder.id
    assert checks == ["granted"]
    assert opened == []
    assert held_after, "the savepoint rollback released the caller's own lock"
    assert not locked_elsewhere(holder.id)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("change", ["finished", "renewed"])
@pytest.mark.parametrize("how", ["set_autocommit", "settings"])
@pytest.mark.parametrize("level", ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"])
def test_a_connection_with_autocommit_off_is_a_callers_transaction(
    monkeypatch, level, how, change
):
    """
    With autocommit turned off every statement joins a transaction that only
    the caller ends, as inside an atomic block, though no atomic block is open.
    A project turns it off with transaction.set_autocommit(False), or with
    AUTOCOMMIT False in the database settings.

    The caller reads the holder, which fixes the snapshot at REPEATABLE READ
    and SERIALIZABLE. Another session then finishes the task, or writes its
    row and leaves it READY, as a lease renewal does. The caller enqueues the
    key. On PostgreSQL at those levels the holder check runs: a finished task
    raises UniqueEnqueueRace, and a task still pending is returned from the
    read on a new connection. On MySQL at REPEATABLE READ the committed status
    is read on a connection of its own, so a finished task's lock is released
    and a new task queued. At READ COMMITTED the read is current. Each time the
    caller's transaction still answers and commits.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and the write would wait")
    holder = tasks.unique_echo.enqueue("held")
    checks = record_holder_checks(monkeypatch)
    connection = connections["default"]
    connection.close()
    if level != "READ COMMITTED":
        isolation_for_new_connections(monkeypatch, level)
    # Connected before AUTOCOMMIT changes, so its write commits at once.
    other = connections.create_connection("default")
    other.ensure_connection()
    settings_dict = connection.settings_dict
    outcome = {}
    try:
        if how == "settings":
            settings_dict["AUTOCOMMIT"] = False
            connection.ensure_connection()
        else:
            transaction.set_autocommit(False)
        outcome["autocommit"] = transaction.get_autocommit()
        outcome["atomic"] = connection.in_atomic_block
        outcome["level"] = session_isolation()
        assert OxTask.objects.get(id=holder.id).status == OxTask.Status.READY
        finished = OxTask.Status.SUCCESSFUL if change == "finished" else None
        touch_elsewhere(holder.id, finished, other=other)
        with opened_connections() as opened:
            try:
                outcome["result"] = tasks.unique_echo.enqueue("held")
            except UniqueEnqueueRace as exc:
                outcome["race"] = exc
        outcome["usable"] = OxTask.objects.filter(id=holder.id).count()
        transaction.commit()
    finally:
        if connection.connection is not None and not connection.get_autocommit():
            connection.rollback()
            connection.set_autocommit(True)
        settings_dict["AUTOCOMMIT"] = True
        other.close()
        connection.close()

    assert outcome["autocommit"] is False
    assert outcome["atomic"] is False
    assert outcome["level"] == level
    snapshot = level != "READ COMMITTED"
    checked = vendor == "postgresql" and snapshot
    ready = [str(task.id) for task in OxTask.objects.filter(status=OxTask.Status.READY)]
    lock = str(OxpullUniqueLock.objects.get().task_id)
    if change == "finished" and checked:
        race = outcome.get("race")
        assert isinstance(race, UniqueEnqueueRace), (
            "deduplicated against a task that had settled"
        )
        assert pg_sqlstate(race.__cause__) == "40001"
        assert "A fresh read found the task SUCCESSFUL." in str(race)
        assert f"inside a transaction at {level}," in str(race)
        assert ready == []
        assert lock == holder.id
    elif change == "finished":
        assert "race" not in outcome, outcome["race"]
        second = outcome["result"].id
        assert second != holder.id, "deduplicated against a task that had settled"
        assert ready == [second]
        assert lock == second
    else:
        assert "race" not in outcome, outcome["race"]
        assert outcome["result"].id == holder.id
        if checked:
            committed = task_result_from_db(OxTask.objects.get(id=holder.id))
            assert result_fields(outcome["result"]) == result_fields(committed)
        assert ready == [holder.id]
        assert lock == holder.id
    assert checks == (["40001"] if checked else [])
    assert [wrapper.alias for wrapper in opened] == (["default"] if snapshot else [])
    assert all(wrapper.connection is None for wrapper in opened)
    assert outcome["usable"] == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_pending_holder_whose_lock_is_gone_is_not_deduplicated_against(
    monkeypatch, level
):
    """
    The read on a connection of its own deduplicates only while the key's lock
    is there.

    The caller's snapshot sees the task READY and holding the key. The task's
    row then changes while it stays READY, as a lease renewal changes it. The
    caller enqueues the key. The lock insert meets the key's lock, and the
    holder check is refused (40001). As the read's new connection opens,
    another session deletes the key's lock and commits, as an operator
    clearing the key by hand would. The read finds the task pending and no
    lock. The key is free, and a READ COMMITTED enqueue would now queue a new
    task, so the enqueue raises UniqueEnqueueRace saying it found the lock
    gone, and creates nothing. The retry at READ COMMITTED queues a new task,
    which takes the key.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    lock_key = OxpullUniqueLock.objects.get().pk
    checks = record_holder_checks(monkeypatch)
    other = connections.create_connection("default")
    other.ensure_connection()
    lock_table = other.ops.quote_name(OxpullUniqueLock._meta.db_table)
    key_column = other.ops.quote_name(OxpullUniqueLock._meta.pk.column)
    deleted = []

    def delete_the_lock(sender, connection, **kwargs):
        if deleted or connection is other:
            return
        with other.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {lock_table} WHERE {key_column} = %s",  # noqa: S608
                [lock_key],
            )
            deleted.append(cursor.rowcount)

    set_session_isolation(level)
    outcome = {}
    try:
        with transaction.atomic():
            assert OxTask.objects.get(id=holder.id).status == OxTask.Status.READY
            touch_elsewhere(holder.id)
            connection_created.connect(delete_the_lock)
            try:
                with opened_connections() as opened:
                    try:
                        outcome["result"] = tasks.unique_echo.enqueue("held")
                    except UniqueEnqueueRace as exc:
                        outcome["race"] = exc
            finally:
                connection_created.disconnect(delete_the_lock)
            outcome["usable"] = OxTask.objects.filter(id=holder.id).count()
        with transaction.atomic():
            set_transaction_isolation("READ COMMITTED")
            outcome["retried"] = tasks.unique_echo.enqueue("held")
    finally:
        other.close()
        connections["default"].close()

    assert deleted == [1], "the key's lock was not deleted before the read"
    race = outcome.get("race")
    assert isinstance(race, UniqueEnqueueRace), (
        "deduplicated against a task whose lock was gone"
    )
    assert pg_sqlstate(race.__cause__) == "40001"
    message = str(race)
    assert "A fresh read found the key's lock gone." in message
    assert f"inside a transaction at {level}," in message
    assert "The enqueue created nothing." in message
    assert checks == ["40001"]
    assert [wrapper.alias for wrapper in opened] == ["default"]
    assert all(wrapper.connection is None for wrapper in opened)
    assert outcome["usable"] == 1
    retried = outcome["retried"].id
    assert retried != holder.id
    assert str(OxpullUniqueLock.objects.get().task_id) == retried
    ready = OxTask.objects.filter(status=OxTask.Status.READY)
    assert sorted(str(task.id) for task in ready) == sorted([holder.id, retried])


@contextmanager
def a_server_that_never_answers(hold):
    """
    A port on this host that accepts connections and never answers them, so a
    client waits for the server's first reply. The connections it accepted are
    closed after `hold` seconds, or when the block ends, so a client with no
    connect timeout fails then instead of waiting for good. Yields the port and
    the list of accepted connections.
    """
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(0.05)
    port = listener.getsockname()[1]
    accepted = []
    done = threading.Event()

    def serve():
        deadline = time.monotonic() + hold
        while not done.is_set() and time.monotonic() < deadline:
            try:
                accepted.append(listener.accept()[0])
            except TimeoutError:
                continue
        for client in accepted:
            client.close()
        listener.close()

    server = threading.Thread(target=serve)
    server.start()
    try:
        yield port, accepted
    finally:
        done.set()
        server.join(timeout=10)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_fresh_read_stops_waiting_to_connect(monkeypatch, level):
    """
    Connecting to the server is bounded too. The read's connection goes to a
    server that accepts it and never answers. The project sets no connect
    timeout, so the read gives libpq a connect_timeout of 2 seconds, and the
    enqueue raises UniqueEnqueueRace chained to the timeout. The server holds
    the connection for 8 seconds, so a read with no bound would fail only
    then. The caller's transaction still answers, the connection is closed,
    and nothing was created.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    checks = record_holder_checks(monkeypatch)
    set_session_isolation(level)
    outcome = {}
    try:
        with a_server_that_never_answers(hold=8) as (port, accepted):
            with transaction.atomic():
                assert OxTask.objects.get(id=holder.id).status == OxTask.Status.READY
                touch_elsewhere(holder.id)
                with opened_connections() as opened:
                    create = connections.create_connection

                    def silent(alias):
                        wrapper = create(alias)
                        wrapper.settings_dict = {
                            **wrapper.settings_dict,
                            "HOST": "127.0.0.1",
                            "PORT": str(port),
                        }
                        return wrapper

                    connections.create_connection = silent
                    started = time.monotonic()
                    try:
                        outcome["result"] = tasks.unique_echo.enqueue("held")
                    except (UniqueEnqueueRace, OperationalError) as exc:
                        outcome["raised"] = exc
                    outcome["seconds"] = time.monotonic() - started
                outcome["usable"] = OxTask.objects.filter(id=holder.id).count()
            outcome["accepted"] = len(accepted)
    finally:
        connections["default"].close()

    raised = outcome.get("raised")
    assert isinstance(raised, UniqueEnqueueRace), raised
    assert isinstance(raised.__cause__, OperationalError)
    message = str(raised)
    assert "A fresh read on a new connection failed (" in message
    assert "timeout expired" in message
    assert f"inside a transaction at {level}," in message
    assert "The enqueue created nothing." in message
    assert outcome["accepted"] >= 1, "the read never reached the silent server"
    assert outcome["seconds"] < 5, f"the read waited {outcome['seconds']:.1f} s"
    assert checks == ["40001"]
    assert [wrapper.alias for wrapper in opened] == ["default"]
    assert all(wrapper.connection is None for wrapper in opened)
    assert outcome["usable"] == 1
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == holder.id


@pytest.mark.django_db
@pytest.mark.parametrize(
    "project", ["none", "options", "environment", "service", "service-environment"]
)
def test_the_fresh_read_keeps_a_connect_timeout_the_project_set(monkeypatch, project):
    """
    The read sets its connect timeout only where the project sets none. A
    timeout in OPTIONS["connect_timeout"] or in PGCONNECT_TIMEOUT, or a
    connection service, named in OPTIONS["service"] or PGSERVICE, whose entry
    can set one, is left for libpq to apply. A value the read passed would
    override each of them. Checked on the parameters the driver is handed.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the read on a connection of its own is PostgreSQL's")
    options = connections["default"].settings_dict["OPTIONS"]
    for name in ("PGCONNECT_TIMEOUT", "PGSERVICE"):
        monkeypatch.delenv(name, raising=False)
    if project == "options":
        monkeypatch.setitem(options, "connect_timeout", 9)
    elif project == "environment":
        monkeypatch.setenv("PGCONNECT_TIMEOUT", "9")
    elif project == "service":
        monkeypatch.setitem(options, "service", "oxpull-test")
    elif project == "service-environment":
        monkeypatch.setenv("PGSERVICE", "oxpull-test")
    handed = []
    create = connections.create_connection

    def recording(alias):
        wrapper = create(alias)

        def get_new_connection(conn_params):
            handed.append(dict(conn_params))
            raise OperationalError("stopped before connecting")

        wrapper.get_new_connection = get_new_connection
        return wrapper

    monkeypatch.setattr(connections, "create_connection", recording)
    with pytest.raises(OperationalError, match="stopped before connecting"):
        OxpullBackend._read_holder_fresh(uuid.uuid4(), "key", "default")

    (params,) = handed
    assert params.get("connect_timeout") == {"none": 2, "options": 9}.get(project)
    assert ("service" in params) is (project == "service")
