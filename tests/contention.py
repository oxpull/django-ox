"""
Simulated contention errors, for tests that can't make a database deadlock on
cue. tests/test_contention.py checks their shape against real ones.
"""

import re
import uuid
from contextlib import contextmanager

from django.db import DEFAULT_DB_ALIAS, OperationalError, connections


class PsycopgError(Exception):
    """A psycopg error: the SQLSTATE is an attribute of the driver's exception."""

    def __init__(self, sqlstate, message):
        super().__init__(message)
        self.sqlstate = sqlstate


class MySQLError(Exception):
    """A MySQL driver error: the error number comes first in the arguments."""


def simulated(kind):
    """
    The OperationalError Django raises for one kind of contention. Django
    builds its own exception from the driver's arguments and chains the
    driver's exception as the cause.
    """
    if kind == "postgresql-deadlock":
        cause = PsycopgError("40P01", "deadlock detected")
    elif kind == "postgresql-serialization":
        cause = PsycopgError(
            "40001", "could not serialize access due to concurrent update"
        )
    else:
        cause = MySQLError(
            1213, "Deadlock found when trying to get lock; try restarting transaction"
        )
    error = OperationalError(*cause.args)
    error.__cause__ = cause
    return error


CONTENTION = ["postgresql-deadlock", "postgresql-serialization", "mysql-deadlock"]

UUID_TEXT = re.compile(
    r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}"
)


def normalized(sql):
    return sql.replace('"', "").replace("`", "").upper().lstrip()


@contextmanager
def failing(prefix, make_error, fail_on, using=DEFAULT_DB_ALIAS):
    """
    Raise make_error() in place of this thread's statements on the database
    `using` that start with prefix (compared without quotes, in upper case)
    whenever fail_on(n) is true for n, that statement's 1-based count. Yields
    the list of (sql, ids) for every such statement, sent or failed.
    """
    seen = []

    def wrapper(execute, sql, params, many, context):
        if normalized(sql).startswith(prefix):
            ids = []
            for value in params or ():
                match = UUID_TEXT.fullmatch(str(value))
                if match and uuid.UUID(str(value)) not in ids:
                    ids.append(uuid.UUID(str(value)))
            seen.append((sql, ids))
            if fail_on(len(seen)):
                raise make_error()
        return execute(sql, params, many, context)

    with connections[using].execute_wrapper(wrapper):
        yield seen
