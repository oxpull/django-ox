"""
django_ox._contention: which errors are a deadlock or a serialization failure,
and when a transaction may run again after one.

tests/contention.py raises errors shaped the way Django raises the drivers'.
The tests against a real deadlock and a real serialization failure pin that
shape, so a test that simulates one stands for the real thing.
"""

import threading
import uuid
from datetime import timedelta

import pytest
from django.db import (
    DatabaseError,
    IntegrityError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.utils import timezone

from django_ox import _contention
from django_ox.models import OxTask

from .contention import CONTENTION, PsycopgError, simulated


@pytest.mark.parametrize("kind", CONTENTION)
def test_the_simulated_errors_are_contention(kind):
    assert _contention.is_contention(simulated(kind))


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("database is locked"),
        OperationalError(
            1205, "Lock wait timeout exceeded; try restarting transaction"
        ),
        IntegrityError(1062, "Duplicate entry"),
        OperationalError("1213"),
        DatabaseError(),
    ],
    ids=["sqlite-locked", "mysql-lock-wait-timeout", "duplicate", "text", "bare"],
)
def test_other_errors_are_not(error):
    assert not _contention.is_contention(error)


def test_a_postgresql_error_that_is_not_contention_is_not():
    error = OperationalError("canceling statement due to statement timeout")
    error.__cause__ = PsycopgError("57014", "canceling statement")
    assert not _contention.is_contention(error)


class Psycopg2Error(Exception):
    """A psycopg2 error: the SQLSTATE is the driver exception's pgcode."""

    def __init__(self, pgcode, message):
        super().__init__(message)
        self.pgcode = pgcode


@pytest.mark.parametrize("code", ["40P01", "40001"])
def test_a_psycopg2_deadlock_or_serialization_failure_is_contention(code):
    error = OperationalError("deadlock detected")
    error.__cause__ = Psycopg2Error(code, "deadlock detected")
    assert _contention.is_contention(error)
    other = OperationalError("canceling statement")
    other.__cause__ = Psycopg2Error("57014", "canceling statement")
    assert not _contention.is_contention(other)


@pytest.mark.django_db(transaction=True)
def test_only_a_connection_with_no_transaction_open_gets_the_retries():
    assert _contention.attempts("default") == _contention.ATTEMPTS == 3
    with transaction.atomic():
        assert _contention.attempts("default") == 1


@pytest.mark.django_db(transaction=True)
def test_a_connection_with_autocommit_off_gets_no_retries():
    """
    With autocommit off and no atomic block, the caller's code opened the
    transaction and commits it, so it owns what an error rolls back.
    """
    transaction.set_autocommit(False)
    try:
        assert _contention.attempts("default") == 1
    finally:
        transaction.rollback()
        transaction.set_autocommit(True)


def test_the_pause_is_jittered_and_grows_with_each_attempt(monkeypatch):
    slept = []
    monkeypatch.setattr(_contention.time, "sleep", slept.append)
    for _ in range(20):
        _contention.pause(1)
    for _ in range(20):
        _contention.pause(2)
    first, second = slept[:20], slept[20:]
    # Two sessions that just deadlocked pause for different times.
    assert len(set(first)) > 1
    assert len(set(second)) > 1
    assert all(0.025 <= s <= 0.075 for s in first), first
    assert all(0.05 <= s <= 0.15 for s in second), second


def test_run_pauses_between_attempts_and_raises_the_last_error(monkeypatch):
    pauses = []
    monkeypatch.setattr(_contention, "pause", pauses.append)
    monkeypatch.setattr(_contention, "attempts", lambda using: 3)
    calls = []

    def body():
        calls.append(True)
        raise simulated("mysql-deadlock")

    with pytest.raises(OperationalError):
        _contention.run("default", body)
    assert (len(calls), pauses) == (3, [1, 2])

    calls.clear()
    pauses.clear()

    def succeeds_second():
        calls.append(True)
        if len(calls) == 1:
            raise simulated("postgresql-deadlock")
        return "done"

    assert _contention.run("default", succeeds_second) == "done"
    assert (len(calls), pauses) == (2, [1])


def old_failed(pk):
    now = timezone.now()
    return OxTask.objects.create(
        id=pk,
        task_path="tests.tasks.add",
        backend_name="default",
        enqueued_at=now - timedelta(days=2),
        status=OxTask.Status.FAILED,
        finished_at=now - timedelta(days=1),
    )


def needs_row_locks():
    if not connection.features.has_select_for_update:
        pytest.skip("SQLite has no row locks to deadlock over")


@pytest.mark.django_db(transaction=True)
def test_a_real_deadlock_is_contention_and_has_the_simulated_shape():
    """
    Two sessions lock two rows in opposite orders. The database aborts one of
    them, and the error it raises is recognised, and carries its code where the
    simulated ones put it.
    """
    needs_row_locks()
    first, second = old_failed(uuid.uuid4()).pk, old_failed(uuid.uuid4()).pk
    both_hold_one = threading.Barrier(2, timeout=10)
    errors = []

    def lock(one, other):
        try:
            with transaction.atomic():
                list(
                    OxTask.objects.select_for_update().filter(pk=one).values_list("pk")
                )
                both_hold_one.wait()
                list(
                    OxTask.objects.select_for_update()
                    .filter(pk=other)
                    .values_list("pk")
                )
        except DatabaseError as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    threads = [
        threading.Thread(target=lock, args=(first, second)),
        threading.Thread(target=lock, args=(second, first)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
        assert not thread.is_alive()

    assert len(errors) == 1, errors
    (error,) = errors
    assert _contention.is_contention(error)
    if connection.vendor == "postgresql":
        assert error.__cause__.sqlstate == "40P01"
    else:
        assert error.args[0] == 1213


@pytest.mark.django_db(transaction=True)
def test_a_real_serialization_failure_is_contention_and_has_the_simulated_shape():
    """
    At REPEATABLE READ on PostgreSQL, a transaction that updates a row another
    transaction changed after its snapshot is refused.
    """
    if connection.vendor != "postgresql":
        pytest.skip("Only PostgreSQL raises a serialization failure here")
    pk = old_failed(uuid.uuid4()).pk
    errors = []
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        assert OxTask.objects.filter(pk=pk).count() == 1

        def change():
            try:
                OxTask.objects.filter(pk=pk).update(status=OxTask.Status.READY)
            finally:
                connections.close_all()

        thread = threading.Thread(target=change)
        thread.start()
        thread.join(30)
        try:
            with transaction.atomic():
                OxTask.objects.filter(pk=pk).update(status=OxTask.Status.DISCARDED)
        except DatabaseError as exc:
            errors.append(exc)

    assert len(errors) == 1, errors
    assert _contention.is_contention(errors[0])
    assert errors[0].__cause__.sqlstate == "40001"


@pytest.mark.django_db(transaction=True)
def test_a_real_duplicate_key_is_not_contention():
    pk = old_failed(uuid.uuid4()).pk
    with pytest.raises(IntegrityError) as info:
        old_failed(pk)
    assert not _contention.is_contention(info.value)
