"""
Count the connections a process opens to its database, for
test_connection_count.

Django's connect hooks fire for the connections it makes and say nothing about
the ones a pool or a driver makes on its own, so this wraps the driver call
itself. Every call appends one line, the process id and the name of the thread
that made it, to a file; a line is written with one os.write so that threads
and processes do not interleave. It is installed by the settings module of a
worker the test starts, before Django opens anything, and is never imported by
the package.
"""

import os
import threading
from collections import Counter
from pathlib import Path


def install(path: str, vendor: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)

    def note() -> None:
        line = f"{os.getpid()} {threading.current_thread().name}\n"
        os.write(fd, line.encode())

    if vendor == "postgresql":
        import psycopg

        connect = psycopg.Connection.connect.__func__  # type: ignore[attr-defined]
        plain = psycopg.connect

        def counted_class_connect(cls, *args, **kwargs):  # type: ignore[no-untyped-def]
            note()
            return connect(cls, *args, **kwargs)

        def counted_connect(*args, **kwargs):  # type: ignore[no-untyped-def]
            note()
            return plain(*args, **kwargs)

        psycopg.Connection.connect = classmethod(counted_class_connect)  # type: ignore[method-assign,assignment]
        psycopg.connect = counted_connect  # type: ignore[assignment]
    elif vendor == "mysql":
        import pymysql

        pymysql.install_as_MySQLdb()
        original = pymysql.connect

        def counted(*args, **kwargs):  # type: ignore[no-untyped-def]
            note()
            return original(*args, **kwargs)

        pymysql.connect = counted  # type: ignore[assignment]
        pymysql.Connect = counted  # type: ignore[assignment]
    else:
        # Django calls sqlite3.dbapi2.connect, not sqlite3.connect: wrapping
        # the second counts nothing.
        from sqlite3 import dbapi2

        original_sqlite = dbapi2.connect

        def counted_sqlite(*args, **kwargs):  # type: ignore[no-untyped-def]
            note()
            return original_sqlite(*args, **kwargs)

        dbapi2.connect = counted_sqlite  # type: ignore[assignment]


def read(path: Path | str) -> list[tuple[int, str]]:
    """The (process id, thread name) of every connect call, in order."""
    found = Path(path)
    if not found.exists():
        return []
    rows = []
    for line in found.read_text().splitlines():
        pid, _, thread = line.partition(" ")
        rows.append((int(pid), thread))
    return rows


def by_thread(path: Path | str) -> Counter[str]:
    """Connect calls per thread name."""
    return Counter(thread for _, thread in read(path))
