"""
Deadlocks and serialization failures, and running a transaction again after
one. Not public API.

When a database finds a deadlock, it aborts one of the transactions in it. At
REPEATABLE READ or SERIALIZABLE it can also refuse a write it can't place in
order. Either way that transaction is rolled back, so it can run again from
its first statement. Part of it can't. MySQL rolls back the whole transaction
on a deadlock, savepoints included, so there is nothing left to resume.

That is why a retry here happens only where django-ox opened the transaction
itself. Inside a caller's transaction the error goes to the caller, who owns
everything that transaction did before it.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

from django.db import DatabaseError, connections

# Attempts in all, the first one included.
ATTEMPTS = 3

# PostgreSQL's deadlock_detected and serialization_failure.
_SQLSTATES = frozenset({"40P01", "40001"})
# MySQL's and MariaDB's ER_LOCK_DEADLOCK.
_MYSQL_ERRORS = frozenset({1213})


def is_contention(exc: BaseException) -> bool:
    """
    True for a deadlock or a serialization failure the database raised.

    Django raises its own exception class with the driver's exception as the
    cause, and copies the driver's arguments onto it. psycopg puts the SQLSTATE
    on the cause as sqlstate and psycopg2 as pgcode. The MySQL drivers put the
    error number first in the arguments.
    """
    for error in (exc, exc.__cause__):
        if error is None:
            continue
        state = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
        if state in _SQLSTATES:
            return True
        args = getattr(error, "args", ())
        if args and type(args[0]) is int and args[0] in _MYSQL_ERRORS:
            return True
    return False


def attempts(using: str) -> int:
    """
    ATTEMPTS when this connection has no transaction open, so the code about to
    open one owns it. 1 inside a caller's transaction.
    """
    connection = connections[using]
    if connection.get_autocommit() and not connection.in_atomic_block:
        return ATTEMPTS
    return 1


def pause(attempt: int) -> None:
    # Jittered, so two sessions that just deadlocked don't meet again in step.
    time.sleep(random.uniform(0.5, 1.5) * 0.05 * attempt)  # noqa: S311 - not a secret


def run[T](using: str, body: Callable[[], T]) -> T:
    """
    Call body, which opens its own transaction on `using`. After a deadlock or
    a serialization failure call it again, up to attempts(using) times in all.
    Any other error, and the last contention error, is raised.
    """
    tries = attempts(using)
    attempt = 1
    while True:
        try:
            return body()
        except DatabaseError as exc:
            if attempt >= tries or not is_contention(exc):
                raise
        pause(attempt)
        attempt += 1
