"""
Side table for the exception-retry behaviour row.

Each execution of the `flaky` task inserts one row keyed by the task's
logical id (its integer argument) and reads back how many executions that
id now has. The first execution raises; any later one returns. The ledger,
not a backend's attempt counter, decides which execution this is, so the
task body is the same code under both backends and asks nothing of either.

The rows are written on the worker's own connection in autocommit mode, and
both workers call the task body outside any transaction of their own, so
the row survives the exception the first execution raises. The table is
created by bench.py with plain SQL in each arm's database; nothing here
touches either package's migrations.
"""

import os

from django.db import connection

LEDGER_TABLE = "bench_retry_ledger"


def record_execution(logical_id: int) -> int:
    """Insert this execution; return the id's execution count, this one included."""
    with connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {LEDGER_TABLE} (logical_id, pid) VALUES (%s, %s)",
            [logical_id, os.getpid()],
        )
        cursor.execute(
            f"SELECT count(*) FROM {LEDGER_TABLE} WHERE logical_id = %s",
            [logical_id],
        )
        return cursor.fetchone()[0]
