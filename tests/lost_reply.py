"""
Losing the reply to a statement the database has committed.

The server really ends the worker's session (KILL on MySQL,
pg_terminate_backend on PostgreSQL) from another connection, at a chosen
point of a chosen statement, once, and the error the worker gets is the one
its driver raises on the dead connection. No hand-written UPDATE touches the
row. The points:

- "before": the session ends before the statement commits; the server rolls
  it back. MySQL's claim ends in its COMMIT, everything else before the
  statement runs.
- "commit" (MySQL's claim): the COMMIT of the claim's transaction lands, then
  the session ends and commit() raises.
- "autocommit" (MySQL's claim): the COMMIT is acknowledged, and the session
  ends before Django restores autocommit, so leaving the atomic block raises.
- "statement": the statement autocommits, then the session ends before its
  reply is read. On SQLite, which has no reply to lose, the statement lands
  and the one after it fails with "database is locked" because another
  connection holds the write lock; that needs the default rollback journal.

The statements: "claim" is any claim UPDATE, the only statement besides a
release that writes worker_ids; "release" is the pinned UPDATE that puts a
claim that raised back on the queue, which writes worker_ids and sets the
status to READY, where every claim sets it to RUNNING.
"""

import sqlite3
from weakref import WeakSet

import pytest
from django.db import connection
from django.db.backends.signals import connection_created

from django_ox.models import OxTask

from .dead_connection_tasks import end_session, from_another_connection

TABLE = OxTask._meta.db_table

#: Where each vendor can lose the reply to each statement, and whether the
#: statement has committed by then.
WINDOWS = {
    "claim": {
        "before": ({"mysql", "postgresql"}, False),
        "commit": ({"mysql"}, True),
        "autocommit": ({"mysql"}, True),
        "statement": ({"postgresql", "sqlite"}, True),
    },
    "release": {
        "before": ({"mysql", "postgresql"}, False),
        "statement": ({"mysql", "postgresql"}, True),
    },
}

#: The windows in which a claim has committed when its reply is lost.
COMMITTED_CLAIM_WINDOWS = [w for w, (_, landed) in WINDOWS["claim"].items() if landed]


def _writes_history(sql):
    text = sql.lstrip().upper()
    return text.startswith("UPDATE") and TABLE.upper() in text and "WORKER_IDS" in text


def is_the_release(sql, params):
    # The ORM's UPDATE puts the status first, as the release names it first.
    # PostgreSQL's claim is raw SQL with named parameters.
    return (
        _writes_history(sql)
        and isinstance(params, (list, tuple))
        and bool(params)
        and params[0] == OxTask.Status.READY
    )


def is_the_claim(sql, params):
    return _writes_history(sql) and not is_the_release(sql, params)


MATCHERS = {"claim": is_the_claim, "release": is_the_release}


def session_of(conn):
    with conn.cursor() as cursor:
        if conn.vendor == "postgresql":
            cursor.execute("SELECT pg_backend_pid()")
        else:
            cursor.execute("SELECT CONNECTION_ID()")
        (own,) = cursor.fetchone()
    return int(own)


def rows_as_seen(conn):
    return sorted(
        OxTask.objects.using(conn.alias).values_list(
            "status", "locked_by", "attempts", "lease_epoch"
        ),
        key=repr,
    )


class LoseTheReply:
    """
    Installed on every connection opened while the test runs; fires once,
    on the `nth` matching `statement`, at `window`. Records whether it fired
    and what another connection saw of the rows at the moment the session
    ended. `also`, when given, is called with that other connection after
    the session has ended, for a test that ends more than one.
    """

    def __init__(self, window, *, statement="claim", nth=1, also=None):
        self.window = window
        self.statement = statement
        self.nth = nth
        self.also = also
        self.fired = False
        self.seen = None
        self.count = 0
        self._matches = MATCHERS[statement]
        self._session = None
        self._pending = None
        self._release = None
        self._patched = WeakSet()

    @property
    def landed(self):
        return WINDOWS[self.statement][self.window][1]

    # -- installing ----------------------------------------------------------

    def install(self, sender, connection, **kwargs):
        if connection in self._patched:
            return
        self._patched.add(connection)
        connection.execute_wrappers.append(self._execute)
        real_commit = connection._commit
        real_set_autocommit = connection._set_autocommit
        connection._commit = lambda: self._commit(connection, real_commit)
        connection._set_autocommit = lambda on: self._set_autocommit(
            connection, real_set_autocommit, on
        )

    def uninstall(self):
        for conn in list(self._patched):
            if self._execute in conn.execute_wrappers:
                conn.execute_wrappers.remove(self._execute)
            conn.__dict__.pop("_commit", None)
            conn.__dict__.pop("_set_autocommit", None)
        self._release_lock()

    # -- losing the reply ----------------------------------------------------

    def _end_the_session(self, conn):
        """
        End `conn`'s session from another connection, and look at the rows
        from there once it has gone.
        """
        own = self._session

        def work(other):
            end_session(other, own)
            self.seen = rows_as_seen(other)
            if self.also is not None:
                self.also(other)

        from_another_connection(work, conn.alias)
        self.fired = True

    def _execute(self, execute, sql, params, many, context):
        conn = context["connection"]
        if self._release is not None:
            # The statement after SQLite's: the one the lock taken below
            # refuses.
            try:
                return execute(sql, params, many, context)
            finally:
                self._release_lock()
        if self.fired or self._pending is not None or not self._matches(sql, params):
            return execute(sql, params, many, context)
        self.count += 1
        if self.count < self.nth:
            return execute(sql, params, many, context)
        if conn.vendor != "sqlite":
            self._session = session_of(conn)
        if self.window == "before" and not conn.in_atomic_block:
            self._end_the_session(conn)
            return execute(sql, params, many, context)
        result = execute(sql, params, many, context)
        if conn.in_atomic_block:
            # MySQL's claim is a transaction; the reply to lose comes later.
            self._pending = conn
            return result
        if conn.vendor == "sqlite":
            self._lock_out(conn)
            return result
        # The statement committed on its own. Nothing of its reply reaches
        # the caller; the next read on the socket fails.
        self._end_the_session(conn)
        return execute("SELECT 1", None, many, context)

    def _commit(self, conn, real_commit):
        if self._pending is not conn:
            return real_commit()
        if self.window == "before":
            self._pending = None
            self._end_the_session(conn)
            return real_commit()
        real_commit()
        if self.window == "commit":
            self._pending = None
            self._end_the_session(conn)
            # The COMMIT landed and its reply was read, but the caller is
            # about to be told otherwise: the driver's own error, from the
            # next read on the socket the server closed.
            with conn.wrap_database_errors:
                conn.connection.cursor().execute("SELECT 1")
        # "autocommit": the reply lost is the one to SET autocommit, below.
        return None

    def _set_autocommit(self, conn, real_set_autocommit, on):
        if on and self._pending is conn and self.window == "autocommit":
            self._pending = None
            self._end_the_session(conn)
        return real_set_autocommit(on)

    # -- SQLite ----------------------------------------------------------------

    def _lock_out(self, conn):
        other = sqlite3.connect(
            conn.settings_dict["NAME"], timeout=10, isolation_level=None
        )
        other.execute("BEGIN EXCLUSIVE")
        self.seen = sorted(
            other.execute(
                f"SELECT status, locked_by, attempts, lease_epoch FROM {TABLE}"  # noqa: S608
            ).fetchall(),
            key=repr,
        )
        # Refused at once rather than after the configured busy timeout: the
        # wait decides when the next statement fails, not whether.
        raw = conn.connection
        (busy,) = raw.execute("PRAGMA busy_timeout").fetchone()
        raw.execute("PRAGMA busy_timeout = 0")

        def release():
            other.execute("ROLLBACK")
            other.close()
            raw.execute(f"PRAGMA busy_timeout = {int(busy)}")

        self._release = release
        if self.also is not None:
            # No other connection ended anything here.
            self.also(None)
        self.fired = True

    def _release_lock(self):
        release, self._release = self._release, None
        if release is not None:
            release()


class Seams:
    """
    The seams one test installs. A test module's fixture yields arm() and
    calls remove_all() afterwards.
    """

    def __init__(self):
        self.installed = []

    def arm(self, window, *, statement="claim", nth=1, on=None, also=None):
        """
        Install a LoseTheReply for the rest of the test, or skip the test when
        `window` is not one the database under test has for `statement`.
        """
        vendors, _ = WINDOWS[statement][window]
        if connection.vendor not in vendors:
            pytest.skip(
                f"{window!r} is a window for the {statement} on "
                f"{' and '.join(sorted(vendors))}"
            )
        seam = LoseTheReply(window, statement=statement, nth=nth, also=also)
        connection_created.connect(seam.install, weak=False)
        self.installed.append(seam)
        if on is not None:
            # Already open, so connection_created will not see it.
            seam.install(sender=None, connection=on)
        return seam

    def remove_all(self):
        for seam in self.installed:
            connection_created.disconnect(seam.install)
            seam.uninstall()
