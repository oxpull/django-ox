"""
Optional unique tasks, batches, rate limiting and workflows for django-ox.

Use OxpullBackend on each task alias that needs these features, in every
enqueuing and claiming process. Rate limits and workflows also require
oxpull.worker.OxpullWorker through OPTIONS["WORKER_CLASS"]. Workflows require
OPTIONS["WORKFLOWS"] = True and a reconciler schedule; batches require their
reconciler schedule.

Uniqueness deduplicates pending task rows. It does not prevent two executions
of the same key from overlapping. Tasks sent outside this backend and workers
that do not apply its admission gate bypass oxpull's unique-task lock rows and
rate limits, respectively.

oxpull creates only its own tables. It does not change any django-ox table,
column, index, constraint or migration.

Workflows do write task rows. They insert blocked nodes as WAITING tasks.
Cancellation moves a READY or WAITING task to DISCARDED by a compare-and-set
on the task row's status and lease epoch. Release moves a WAITING task to READY
and updates run_after; run_after is never NULL. Revival of a DISCARDED node
is not supported. The workflow generation field is reserved and remains 0;
it is not a workflow lease epoch.

A workflow is created at runtime, not declared as a definition in settings.
WORKFLOWS accepts only True or False. Signal receivers must account for the
OxpullBackend sender on tasks enqueued through this backend.

Use ox_prune for task rows and oxpull_prune for oxpull records.
"""

from __future__ import annotations

# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

import os
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, cast

from django.apps import apps
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DatabaseError,
    IntegrityError,
    OperationalError,
    connections,
    transaction,
)
from django.db.models import CharField, Exists, OuterRef, Subquery
from django.db.models.expressions import RawSQL
from django.tasks.base import DEFAULT_TASK_QUEUE_NAME, Task, TaskResult
from django.tasks.exceptions import InvalidTask
from django.utils import timezone
from django.utils.module_loading import import_string

from django_ox.backend import OxBackend

from .exceptions import UniqueEnqueueRace
from .rate_limits import (
    RateLimit,
    rate_limits_from_options,
    refresh_interval_from_options,
)
from .routing import oxpull_db_alias
from .unique import is_registered, unique_key, unique_tasks_from_options

# Bound on the duplicate-path retry loop. A pass either returns the live
# holder's result, deletes one stale lock generation, or ends in a
# transaction the server rolled back and is reissued: an InnoDB deadlock
# victim on MySQL, a serialization failure on PostgreSQL. Reaching the
# bound takes that many finished generations or lost transactions during
# a single enqueue call.
MAX_UNIQUE_ATTEMPTS = 5

# MySQL's ER_LOCK_DEADLOCK. The insert race here can genuinely deadlock
# under InnoDB: a losing INSERT's duplicate check leaves a shared lock
# on the conflicting index record, and two losers re-inserting after a
# stale lock's DELETE each wait on the other's shared lock. InnoDB
# resolves the cycle by rolling back one victim's entire transaction,
# and its manual's instruction for 1213 is to reissue that transaction.
ER_LOCK_DEADLOCK = 1213

# The three ways a database stops waiting for the holder of a key. The
# lock INSERT on the loser's side blocks on the winner's uncommitted row,
# and the enqueue joins the caller's transaction, so a slow request holds
# the key for as long as it runs. SQLite gives up after the connection's
# busy timeout (OPTIONS["timeout"], 5 s unless set) with SQLITE_BUSY;
# every extended busy code shares that primary code, SQLITE_BUSY_SNAPSHOT
# among them, which a WAL-mode read-to-write upgrade raises at once
# rather than after the timeout. MySQL gives up after
# innodb_lock_wait_timeout (50 s unless set) with ER_LOCK_WAIT_TIMEOUT,
# rolling back the statement and not the transaction. PostgreSQL waits
# for as long as the holder runs unless the project sets lock_timeout,
# which ends the wait with lock_not_available. A statement_timeout ends
# it too, as query_canceled (57014); that one is not specific to this
# contention and propagates raw.
SQLITE_BUSY = 5
# The extended code for the WAL-mode case: a transaction that read before
# it wrote asks to become a writer after another connection committed,
# and SQLite refuses at once rather than waiting out the busy timeout.
SQLITE_BUSY_SNAPSHOT = SQLITE_BUSY | (2 << 8)
ER_LOCK_WAIT_TIMEOUT = 1205
PG_LOCK_NOT_AVAILABLE = "55P03"
# PostgreSQL's serialization_failure. At REPEATABLE READ and SERIALIZABLE
# a transaction may not lock, update or delete a row that another
# transaction changed after its snapshot began, and the statement fails
# with this code instead. MySQL reports a deadlock under the same SQLSTATE,
# and that case keeps its own path.
PG_SERIALIZATION_FAILURE = "40001"
# The levels at which PostgreSQL answers a plain read inside a transaction
# from the transaction's snapshot, spelled as current_setting() spells them.
PG_SNAPSHOT_LEVELS = frozenset({"repeatable read", "serializable"})
# The name under which the holder read carries the transaction's level.
ISOLATION_LEVEL_ANNOTATION = "oxpull_isolation_level"
# The name under which the fresh read of a holder carries the task that the
# key's lock names.
LOCK_HOLDER_ANNOTATION = "oxpull_lock_task_id"
# How long that fresh read waits for a lock, and how long its statement may
# run. Any lock wait there is one that can't end while the caller's
# transaction is open (see _read_holder_fresh), so the first is short. The
# second is a backstop for a statement that reads one row by primary key.
FRESH_READ_LOCK_TIMEOUT = "100ms"
FRESH_READ_STATEMENT_TIMEOUT = "2s"
# How long that read's connection waits to connect, in seconds, where the
# project sets no connect timeout of its own. libpq applies it to each address
# the host name resolves to, and treats anything lower as 2. It doesn't cover
# the host name lookup, or a wait in a connection pooler's queue.
FRESH_READ_CONNECT_TIMEOUT = 2
# What a UniqueEnqueueRace raised inside a snapshot transaction tells the
# caller to do.
RETRY_OUTSIDE_THE_SNAPSHOT = (
    "The enqueue created nothing. Retry it in a new, short transaction at "
    "READ COMMITTED, or outside a transaction. A retry inside this transaction "
    "reads the same snapshot."
)


def _pg_sqlstate(exc: BaseException) -> str | None:
    """
    The SQLSTATE of the driver error Django wrapped. psycopg 3 calls it
    sqlstate and psycopg2 calls it pgcode.
    """
    cause = exc.__cause__
    code = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
    return code if isinstance(code, str) else None


def _savepoint(db_alias: str) -> Any:
    """
    Open a savepoint on the alias's connection and return its id. Django 6.1
    names that call transaction.savepoint_create() and deprecates
    transaction.savepoint(), which Django 7.0 removes. Django 6.0 has only the
    old name.
    """
    create = getattr(transaction, "savepoint_create", None)
    if create is None:
        return transaction.savepoint(using=db_alias)
    return create(using=db_alias)


def _in_callers_transaction(db_alias: str) -> bool:
    """
    Whether the enqueue runs inside a transaction that the caller opened and
    only the caller ends. That is an atomic block, or a connection with
    autocommit turned off, by transaction.set_autocommit(False) or by AUTOCOMMIT
    False in the database settings. Either way every statement the enqueue runs
    joins that transaction, reads its snapshot at a snapshot level, and is
    lost with the caller's work when the server rolls the transaction back.
    """
    connection = connections[db_alias]
    return bool(connection.in_atomic_block) or not connection.get_autocommit()


def _sets_its_own_connect_timeout(options: Mapping[str, Any]) -> bool:
    """
    Whether libpq takes a connect timeout from the project rather than from
    oxpull. A project sets one in OPTIONS["connect_timeout"] or in the
    PGCONNECT_TIMEOUT environment variable, or names a connection service,
    whose entry can set one. A value passed with the connection would override
    all three.
    """
    return (
        "connect_timeout" in options
        or "service" in options
        or "PGCONNECT_TIMEOUT" in os.environ
        or "PGSERVICE" in os.environ
    )


# PostgreSQL's serialization_failure. At REPEATABLE READ or SERIALIZABLE,
# a transaction that writes a row another transaction changed after its
# snapshot was taken gets this instead of the newer row, and waiting on
# that writer's lock first does not help: the error arrives when the
# writer commits. The whole transaction is rolled back, and PostgreSQL's
# manual says to retry it from the start.
PG_SERIALIZATION_FAILURE = "40001"

# PostgreSQL's deadlock_detected. The deadlock detector rolls back one
# transaction of the cycle whole, and the manual says to run it again.
PG_DEADLOCK_DETECTED = "40P01"


def is_innodb_deadlock(exc: OperationalError, db_alias: str) -> bool:
    if connections[db_alias].vendor != "mysql":
        return False
    return bool(exc.args and exc.args[0] == ER_LOCK_DEADLOCK)


def is_deadlock(exc: OperationalError, db_alias: str) -> bool:
    """Whether InnoDB (1213) or PostgreSQL (40P01) rolled this transaction back."""
    if is_innodb_deadlock(exc, db_alias):
        return True
    if connections[db_alias].vendor != "postgresql":
        return False
    return getattr(exc.__cause__, "sqlstate", None) == PG_DEADLOCK_DETECTED


def is_serialization_failure(exc: OperationalError, db_alias: str) -> bool:
    """Whether PostgreSQL rolled this transaction back as a serialization failure."""
    if connections[db_alias].vendor != "postgresql":
        return False
    return getattr(exc.__cause__, "sqlstate", None) == PG_SERIALIZATION_FAILURE


def _is_lock_wait_timeout(exc: OperationalError, db_alias: str) -> bool:
    """Whether the database gave up waiting for another transaction's lock."""
    vendor = connections[db_alias].vendor
    cause = exc.__cause__
    if vendor == "sqlite":
        code = getattr(cause, "sqlite_errorcode", None)
        return isinstance(code, int) and code & 0xFF == SQLITE_BUSY
    if vendor == "mysql":
        return bool(exc.args and exc.args[0] == ER_LOCK_WAIT_TIMEOUT)
    if vendor == "postgresql":
        return _pg_sqlstate(exc) == PG_LOCK_NOT_AVAILABLE
    return False


def _is_serialization_failure(exc: OperationalError, db_alias: str) -> bool:
    """Whether PostgreSQL refused a row that changed after the snapshot began."""
    if connections[db_alias].vendor != "postgresql":
        return False
    return _pg_sqlstate(exc) == PG_SERIALIZATION_FAILURE


def _unbound_reconciler_error(
    task_path: str, schedule_name: str, schedule: Any, consequence: str
) -> checks.CheckMessage:
    """oxpull.E008: a reconciler exposed to schedule rows that nothing runs."""
    from .batches import unbound_reconciler_hint, unbound_reconciler_message

    return checks.Error(
        unbound_reconciler_message(task_path, schedule, consequence),
        hint=unbound_reconciler_hint(task_path, schedule_name, schedule),
        id="oxpull.E008",
    )


class OxpullBackend(OxBackend):
    """
    OxBackend plus unique tasks.

    A task is unique when marked with @oxpull.unique.unique or listed in
    this backend's OPTIONS["UNIQUE_TASKS"]. Enqueueing a unique task whose
    key is already held by a READY or RUNNING task returns that task's
    TaskResult instead of inserting a second row; task_enqueued is not
    sent for the suppressed duplicate. Non-unique tasks take the parent
    code path untouched. Uniqueness is scoped per backend alias.

    enqueue_many() is the one parent method a unique task cannot use, and
    it says so rather than writing duplicates: its bulk INSERT does not
    reach the deduplicating path at all.

    Correctness rests on the same mechanism django-ox proves in-tree for
    schedule ticks: a unique constraint plus an IntegrityError loser path,
    never check-then-insert. The lock row and the task row are written in
    one transaction, so they commit or roll back together, preserving the
    parent backend's durability guarantee: a transaction.atomic() opened
    on the database these tables live on covers the enqueue too, and a
    rollback discards task and lock with every other row written there.

    The oxpull tables must live on the same database as the django_ox
    tables; the lock INSERT and the task INSERT must share a transaction.
    __init__ enforces it against the active router, and every read on the
    dedup path is pinned to that database, so a read-replica router
    cannot serve a stale liveness answer.
    """

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        # A split router silently breaks lock/task atomicity; fail at
        # startup, not at the first lost duplicate.
        self._db_alias: str = oxpull_db_alias()
        # Shape errors raise here, loudly, at startup. check() validates
        # the deeper property (that each path imports to a Task).
        self.unique_task_paths: frozenset[str] = unique_tasks_from_options(self.options)

    def _is_unique(self, task: Task[..., Any]) -> bool:
        path = task.module_path
        return is_registered(path) or path in self.unique_task_paths

    def enqueue[**P, R](
        self, task: Task[P, R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[P, R]:
        if not self._is_unique(task):
            return super().enqueue(task, args, kwargs)
        return self._enqueue_unique(task, args, kwargs)

    def enqueue_many[**P, R](
        self,
        task: Task[P, R],
        calls: Iterable[tuple[Sequence[Any], Mapping[str, Any]]],
    ) -> list[TaskResult[P, R]]:
        """
        The parent's bulk insert, refused for a unique task.

        django-ox writes an enqueue_many() call as one bulk INSERT and
        never goes through enqueue(), so no uniqueness lock is taken.
        The rows land beside a lock that knows nothing about them, every
        later enqueue() of that key keeps returning the original task as
        though deduplication were working, and the duplicates run. A
        guarantee that fails silently is worse than a call that refuses,
        so this refuses and names the task.

        Deduplicating in bulk is separate work, not a smaller version of
        this: each key needs its lock row and its task row inserted
        together, with the losers resolved against whichever holder is
        live, and that is not one conditional INSERT on all three
        supported databases. Non-unique tasks take the parent path
        untouched, bulk INSERT included.
        """
        if self._is_unique(task):
            raise InvalidTask(
                f"Task {task.module_path!r} is unique, and enqueue_many() "
                "cannot hold its uniqueness lock: the rows go in as one bulk "
                "INSERT that never reaches the deduplicating enqueue path, so "
                "the duplicates would be written and run. Call task.enqueue() "
                "once per call, or stop marking the task unique."
            )
        return super().enqueue_many(task, calls)

    def _enqueue_unique[**P, R](
        self, task: Task[P, R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[P, R]:
        from .models import OxpullUniqueLock

        # Deliberate double-validation: on the duplicate path
        # super().enqueue() never runs, so this is the only validate_task
        # call the request gets; removing it would make InvalidTask
        # depend on queue state.
        self.validate_task(task)
        key = unique_key(self.alias, task.module_path, task.queue_name, args, kwargs)
        db_alias = self._db_alias

        # A lost deadlock rolls back the victim's entire transaction,
        # not just the statement. When this call owns its transactions,
        # a victimised attempt left nothing behind and is reissued in
        # the loop below. Inside a caller's transaction the server has
        # discarded the caller's work too, and only the caller can
        # decide to redo that, so the error propagates instead. A
        # connection with autocommit turned off is inside one, as an
        # atomic block is.
        ambient_atomic = _in_callers_transaction(db_alias)

        last_conflict: DatabaseError | None = None
        for _ in range(MAX_UNIQUE_ATTEMPTS):
            conflict: IntegrityError | None = None
            about_the_lock = False
            try:
                with transaction.atomic(using=db_alias):
                    try:
                        # The lock INSERT gets its own savepoint so that
                        # only its conflict is caught below; an
                        # IntegrityError out of super().enqueue()
                        # propagates raw instead of being misread as a
                        # duplicate.
                        with transaction.atomic(using=db_alias):
                            lock = OxpullUniqueLock.objects.using(db_alias).create(
                                key=key,
                                task_path=task.module_path,
                                queue_name=task.queue_name,
                                created_at=timezone.now(),
                            )
                    except IntegrityError as exc:
                        # The primary key on key: another enqueue holds
                        # it. A conflicting INSERT blocks until the
                        # holding transaction resolves, within the
                        # database's lock wait: PostgreSQL waits for as
                        # long as the holder runs unless lock_timeout is
                        # set, SQLite for OPTIONS["timeout"] (5 s by
                        # default), MySQL for innodb_lock_wait_timeout
                        # (50 s by default). Inside that wait the
                        # winner has committed by the time IntegrityError
                        # surfaces. Past it the database raises instead,
                        # and the OperationalError handler below names
                        # that condition.
                        conflict = exc
                    else:
                        # Lock first, task second: a losing duplicate
                        # rolls back before super().enqueue() runs, so
                        # no task row is written and no task_enqueued
                        # signal fires for a suppressed duplicate.
                        result = super().enqueue(task, args, kwargs)
                        about_the_lock = True
                        # task_id=None keeps this UPDATE to the row just
                        # inserted. At PostgreSQL's REPEATABLE READ and
                        # SERIALIZABLE the INSERT can wait on another
                        # process's release of the key and go ahead once
                        # it commits, while the snapshot, taken before the
                        # wait, still shows the released row. Matched by
                        # key alone, that row fails the UPDATE with a
                        # serialization failure (40001). A committed lock
                        # row always names its task. SERIALIZABLE can
                        # still refuse the UPDATE for reading the released
                        # row, and the handler below treats that refusal
                        # as it treats one met while resolving.
                        OxpullUniqueLock.objects.using(db_alias).filter(
                            pk=lock.pk, task_id=None
                        ).update(task_id=result.id)
                        return result
                last_conflict = conflict

                about_the_lock = True
                existing = self._existing_result(
                    task, key, db_alias, ambient=ambient_atomic
                )
            except OperationalError as exc:
                if _is_lock_wait_timeout(exc, db_alias):
                    # The holder outlived the wait. Nothing of this
                    # enqueue survived: the transaction rolled back when
                    # this call owns it, the savepoint when it does not,
                    # so the caller's transaction is still usable. No
                    # retry here: another wait inside the caller's
                    # request is what a caller should choose, not have
                    # chosen for it, and UniqueEnqueueRace already means
                    # safe to retry.
                    raise UniqueEnqueueRace(
                        self._lock_wait_message(
                            task, exc, db_alias, ambient_atomic=ambient_atomic
                        )
                    ) from exc
                if about_the_lock and _is_serialization_failure(exc, db_alias):
                    # Only a refusal met after the task is written, or
                    # while resolving the duplicate, is about the key's
                    # lock or its holder: the locking read of a settled
                    # holder, the release DELETE, or the lock UPDATE. A
                    # pending holder's changed row never gets here, because
                    # _resolve_conflict reads it again instead.
                    # PostgreSQL's SERIALIZABLE can also fail the INSERTs
                    # above when it finds a dependency on another
                    # transaction, and that failure propagates as it would
                    # from any enqueue.
                    if ambient_atomic:
                        # The snapshot is the caller's, and an enqueue
                        # retried inside that transaction reads it again,
                        # so only the caller can retry usefully.
                        raise UniqueEnqueueRace(
                            self._serialization_message(task, db_alias)
                        ) from exc
                    # The transaction the failure rolled back was this
                    # call's own, as with a deadlock victim below, and
                    # the next pass takes a new snapshot.
                    last_conflict = exc
                    continue
                if ambient_atomic or not is_innodb_deadlock(exc, db_alias):
                    raise
                last_conflict = exc
                continue
            if existing is not None:
                return existing
        raise UniqueEnqueueRace(
            self._race_message(task, db_alias, ambient=ambient_atomic)
        ) from last_conflict

    def _lock_wait_message(
        self,
        task: Task[..., Any],
        exc: OperationalError,
        db_alias: str,
        *,
        ambient_atomic: bool,
    ) -> str:
        """
        Say what happened, per engine, and what is left to retry.

        Three things vary. SQLite in WAL mode refuses a reader's upgrade
        to writer at once (SQLITE_BUSY_SNAPSHOT) rather than after the
        busy timeout, so that case did not wait for anything. MySQL with
        innodb_rollback_on_timeout=ON rolls the whole transaction back on
        1205, not the statement: Django's savepoint rollback then fails
        and marks the connection for rollback, which is how it is known
        here, and nothing the caller did in that transaction survives.
        Everywhere else inside a caller's transaction the enqueue rolled
        back to its savepoint and the transaction is still usable.
        """
        connection = connections[db_alias]
        snapshot = (
            connection.vendor == "sqlite"
            and getattr(exc.__cause__, "sqlite_errorcode", None) == SQLITE_BUSY_SNAPSHOT
        )
        if snapshot:
            message = (
                f"Could not enqueue unique task {task.module_path!r}: this "
                "transaction had read before it wrote, another connection "
                "committed a write in between, and SQLite in WAL mode refuses "
                "to make a writer of a reader whose snapshot is behind "
                "(SQLITE_BUSY_SNAPSHOT); it did not wait. Nothing was written."
            )
        else:
            message = (
                f"Could not enqueue unique task {task.module_path!r}: another "
                "enqueue of the same key held it for longer than the database's "
                "lock wait, and the database stopped waiting for that transaction "
                "to resolve. Nothing was written; the enqueue is safe to retry "
                "once the holder has committed or rolled back."
            )
        if not ambient_atomic:
            return message
        if connection.needs_rollback:
            return message + (
                " This enqueue ran inside an open transaction, and the server "
                "rolled that whole transaction back (MySQL with "
                "innodb_rollback_on_timeout=ON): nothing done in it survives, "
                "the atomic block rolls back on exit, and the whole transaction "
                "is the thing to retry."
            )
        message += (
            " This enqueue ran inside an open transaction. The enqueue rolled "
            "back to its savepoint and the transaction is still usable, but "
            "retry the whole transaction rather than the enqueue alone."
        )
        level = self._snapshot_level(db_alias)
        if level is None:
            return message
        return message + (
            f" At {level} the transaction reads from a snapshot, and a retry "
            "inside it cannot see a winner that committed after that snapshot "
            "began."
        )

    @staticmethod
    def _snapshot_level(db_alias: str) -> str | None:
        """
        The engine and level to name when this session's plain reads answer
        from a snapshot, or None. A winner that committed after the snapshot
        began is invisible to the lock read, so a retry inside the same
        transaction ends the same way. Asked only on the way to an error.
        """
        connection = connections[db_alias]
        if connection.vendor == "mysql":
            query = "SELECT @@transaction_isolation"
            names = {"REPEATABLE READ": "MySQL's REPEATABLE READ"}
        elif connection.vendor == "postgresql":
            query = "SHOW transaction_isolation"
            names = {
                "REPEATABLE READ": "PostgreSQL's REPEATABLE READ",
                "SERIALIZABLE": "PostgreSQL's SERIALIZABLE",
            }
        else:
            return None
        with connection.cursor() as cursor:
            cursor.execute(query)
            (level,) = cursor.fetchone()
        return names.get(str(level).upper().replace("-", " "))

    @staticmethod
    def _serialization_message(task: Task[..., Any], db_alias: str) -> str:
        """
        PostgreSQL refused a row the enqueue locked, updated or deleted,
        because it changed after the caller's snapshot began. The level is
        asked of the transaction, and only on the way to this error.
        """
        connection = connections[db_alias]
        at = ""
        if connection.vendor == "postgresql" and not connection.needs_rollback:
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_isolation")
                (level,) = cursor.fetchone()
            at = f" at {str(level).upper()}"
        return (
            f"Could not enqueue unique task {task.module_path!r} on database "
            f"{db_alias!r}. The enqueue ran inside a transaction{at}, and "
            "PostgreSQL refused a row that changed after that transaction's "
            "snapshot began (SQLSTATE 40001). The row was the task that held "
            f"the key, or the key's lock. {RETRY_OUTSIDE_THE_SNAPSHOT}"
        )

    def _race_message(
        self, task: Task[..., Any], db_alias: str, *, ambient: bool
    ) -> str:
        message = (
            f"Could not enqueue or deduplicate unique task "
            f"{task.module_path!r} after {MAX_UNIQUE_ATTEMPTS} attempts."
        )
        if not ambient:
            return message
        # The lock row is read on the caller's connection. Where that read
        # answers from a snapshot, a winner that committed after the
        # snapshot began is invisible to it, and every pass ends the same.
        level = self._snapshot_level(db_alias)
        if level is None:
            return message
        return message + (
            f" The enqueue ran inside a transaction at {level}, which reads "
            "from a snapshot. The likely cause is a winner that committed "
            "after that snapshot began, which the transaction cannot see. "
            "Enqueue outside the transaction, or retry the whole transaction."
        )

    def _existing_result[**P, R](
        self, task: Task[P, R], key: str, db_alias: str, *, ambient: bool
    ) -> TaskResult[P, R] | None:
        """
        Resolve a lock conflict: return the live holder's result, or
        release the stale lock and return None so the caller retries.
        `ambient` says whether the caller's transaction is open around
        this call, which is where a plain read can answer from a
        snapshot older than the conflict. Inside one on PostgreSQL at
        REPEATABLE READ or SERIALIZABLE, a pending holder whose row changed
        after the snapshot began is read again on a connection of its own,
        and the enqueue deduplicates only when that read finds it still
        pending and still named by the key's lock.

        Staleness is derived purely from the task row. The reaper writes
        LOST with no signal (django_ox worker.reap), the ImportError
        terminal path can skip its signal, and ox_prune deletes rows
        outright. All of them land here as "no longer READY or RUNNING,
        or gone", which is the whole point of deriving instead of
        listening.

        The lock row is read first, its task second, and the release at
        the end is pinned to the (key, task_id) generation inspected, so
        a release only ever removes a generation whose task this call
        saw terminal or absent. The previous shape, a semijoin from the
        task side with a lock re-read when it came back empty, could
        re-read a winner that committed after the empty semijoin and
        release its live lock; a CI postgres run caught it returning two
        winners for one key.

        Every read is pinned to the write database: a router that sends
        reads to a lagged replica must not be able to make a live lock
        look stale.
        """
        if not connections[db_alias].features.has_select_for_update:
            # SQLite: no row locks, so there is nothing for a transaction
            # to hold between the read and the DELETE, and no atomic
            # block is opened here. When this call owns its transactions
            # each statement then runs on its own, which matters because
            # a deferred transaction that reads and then writes fails
            # with "database is locked" the moment two of them race.
            # Inside a caller's atomic() the statements run in that
            # transaction regardless: skipping the block ends nothing,
            # and the read-to-write upgrade it makes is the one
            # SQLITE_BUSY_SNAPSHOT refuses (see _lock_wait_message). In
            # both cases the DELETE at the end carries the holder's
            # status in its own WHERE clause, which is what makes the
            # release atomic against a retry: a statement runs whole
            # against SQLite's single writer.
            return self._resolve_conflict(task, key, db_alias, ambient=ambient)
        with transaction.atomic(using=db_alias):
            return self._resolve_conflict(task, key, db_alias, ambient=ambient)

    def _resolve_conflict[**P, R](
        self, task: Task[P, R], key: str, db_alias: str, *, ambient: bool
    ) -> TaskResult[P, R] | None:
        """
        The body of _existing_result, inside one transaction where the
        database has row locks.

        The read that decides a release and the DELETE share a
        transaction so that the row lock the read takes is still held
        when the DELETE runs. This runs after the enqueue transaction
        has ended, so without the atomic each statement would commit on
        its own and the locking read would be refused.
        """
        from django_ox.models import OxTask
        from django_ox.results import task_result_from_db

        from .models import OxpullUniqueLock

        # The statuses in which a holder keeps its key. The read that
        # decides a release and the DELETE that performs it test the
        # same set.
        pending = (OxTask.Status.READY, OxTask.Status.RUNNING)
        lock = OxpullUniqueLock.objects.using(db_alias).filter(key=key).first()
        if lock is None:
            # Released by a concurrent enqueue, or rolled back with the
            # transaction that inserted it. Nothing to release; retry.
            return None
        # A live holder is read without a lock. There is nothing to
        # release for it, and a row lock taken here inside a caller's
        # transaction (ATOMIC_REQUESTS, any atomic() block) would live
        # until that transaction commits: for as long as the request ran,
        # the holder's worker could not renew its lease or write its
        # outcome and the reaper could not take the row. PostgreSQL waits
        # for the request unless lock_timeout is set; MySQL gives up with
        # 1205 after innodb_lock_wait_timeout.
        #
        # Inside a caller's transaction on PostgreSQL the same statement
        # reads the transaction's isolation level, so asking costs no
        # statement. It is asked of the transaction, not the settings or
        # the driver, because a session or the transaction itself can set
        # its own. At REPEATABLE READ and SERIALIZABLE the holder read
        # answers from the snapshot, and a holder that finished after the
        # snapshot began still reads as READY or RUNNING. Returning it
        # would hand back a finished task's result and queue nothing.
        # _check_holder_row asks PostgreSQL whether the row changed after
        # the snapshot began, and keeps no lock. Only when it did are the
        # holder and the key's lock read again, once, on a connection of
        # their own. At READ COMMITTED the read is current, and when this
        # call owns its transaction the snapshot began at the lock read a
        # moment ago, so neither runs the check.
        level_sql = self._isolation_level_sql(db_alias) if ambient else None
        holder = self._holder(
            lock.task_id, db_alias, locking=False, level_sql=level_sql
        )
        live = holder is not None and holder.status in pending
        if live and level_sql is not None:
            level = str(getattr(holder, ISOLATION_LEVEL_ANNOTATION)).lower()
            if level in PG_SNAPSHOT_LEVELS:
                refusal = self._check_holder_row(holder.pk, db_alias)
                if refusal is not None:
                    return self._result_from_a_fresh_read(
                        task, key, lock.task_id, db_alias, level=level, refusal=refusal
                    )
        if live and ambient and self._reads_a_snapshot(db_alias):
            # Inside a caller's transaction on MySQL REPEATABLE READ that
            # plain read answers from the transaction's snapshot, and a
            # holder that finished since the snapshot began still reads
            # as RUNNING: the enqueue would hand back a finished task's
            # result and the new work would never be queued. InnoDB has
            # no current read that takes no lock, and a lock here is the
            # one ruled out above, so the committed status is read on a
            # connection of its own, which sees every commit and holds
            # nothing. Only a holder the snapshot calls live pays for
            # that connection; a holder it calls settled is re-read FOR
            # UPDATE below, a current read on this connection. At READ
            # COMMITTED, Django's default for the backend, the plain
            # read is current and nothing is opened.
            live = self._committed_status(lock.task_id, db_alias) in pending
        if live:
            # Pending window holds. Retries also land here: a failed
            # attempt with attempts remaining goes back to READY, so a
            # retrying task keeps its key; retrying is not duplication.
            # The result is built from the row alone: the caller's Task
            # object may carry different using() overrides, and the row
            # is the truth about the task that actually pends.
            return cast("TaskResult[P, R]", task_result_from_db(holder))
        if holder is not None and connections[db_alias].features.has_select_for_update:
            # The release path, and only here, reads the holder with a
            # locking read: the DELETE below is a current write and the
            # status it acts on must be current too. A plain SELECT under
            # REPEATABLE READ answers from the transaction's snapshot: a
            # caller that had read the holder as FAILED, while an operator
            # retry committed READY in between, would read FAILED again
            # here, delete the live lock and insert a second READY row
            # beside the retried one. FOR UPDATE reads the committed row
            # and waits for a retry still in flight. PostgreSQL at
            # REPEATABLE READ or SERIALIZABLE refuses instead when the row
            # changed after the snapshot began (SQLSTATE 40001), and
            # _enqueue_unique reports that or runs a new pass. The row lock is
            # released with the enclosing atomic: at once when this call
            # owns it, at the caller's commit when it does not, and until
            # then a retry of that settled row, a straggler's outcome
            # write onto a LOST row and ox_prune of the row wait behind
            # it. A retry that lands after the release reopens the key
            # beside the new holder, the documented operator bypass.
            holder = self._holder(lock.task_id, db_alias, locking=True)
            if holder is not None and holder.status in pending:
                # A retry committed between the two reads, or, inside a
                # MySQL REPEATABLE READ transaction, since its snapshot.
                # The row is locked now and stays so until the enclosing
                # atomic ends; this is the one case a live holder's row
                # is held, and it is bounded to that race.
                return cast("TaskResult[P, R]", task_result_from_db(holder))
        # Stale or dangling: the task finished, was reaped to LOST, or
        # its row was pruned; a null task_id never survives a committed
        # enqueue. Release exactly the generation inspected; key is the
        # primary key, so (key, task_id) pins the generation and the
        # delete is a no-op if a concurrent enqueue already replaced the
        # lock. The pin covers a replaced generation only: a
        # django_ox.actions.retry moves a FAILED or LOST holder back to
        # READY without touching this lock row, so its (key, task_id) is
        # unchanged. The DELETE therefore carries the holder's status in
        # its own WHERE clause as well, and deletes nothing when the
        # holder is READY or RUNNING by then: on SQLite, where no row
        # lock spans the read and the DELETE, that is the whole of the
        # guard, and the statement is atomic against its single writer.
        OxpullUniqueLock.objects.using(db_alias).filter(
            key=key, task_id=lock.task_id
        ).exclude(
            Exists(
                OxTask.objects.using(db_alias).filter(
                    id=OuterRef("task_id"), status__in=pending
                )
            )
        ).delete()
        return None

    @staticmethod
    def _holder(
        task_id: Any, db_alias: str, *, locking: bool, level_sql: str | None = None
    ) -> Any:
        """
        The holder's row, or None. With level_sql the row also carries the
        transaction's isolation level as ISOLATION_LEVEL_ANNOTATION, read in
        the same statement.
        """
        from django_ox.models import OxTask

        if task_id is None:
            return None
        rows = OxTask.objects.using(db_alias).filter(id=task_id)
        if locking:
            rows = rows.select_for_update()
        if level_sql is not None:
            # A fixed expression from _isolation_level_sql, never input.
            level = RawSQL(level_sql, (), output_field=CharField())  # noqa: S611
            return rows.annotate(**{ISOLATION_LEVEL_ANNOTATION: level}).first()
        return rows.first()

    @staticmethod
    def _isolation_level_sql(db_alias: str) -> str | None:
        """
        The expression the holder read uses to report the transaction's
        isolation level, or None where the read does not need it. Only
        PostgreSQL does. MySQL asks its session when a holder reads as live,
        as it did in 1.2.0, and SQLite has one level.
        """
        if connections[db_alias].vendor != "postgresql":
            return None
        return "current_setting('transaction_isolation')"

    @classmethod
    def _check_holder_row(cls, task_pk: Any, db_alias: str) -> OperationalError | None:
        """
        Whether the holder's row changed after this transaction's snapshot
        began. Returns PostgreSQL's serialization failure when it did. Returns
        None when the row the snapshot holds was still the last committed one
        at the check, including while another transaction is changing it.

        The check asks for a FOR SHARE lock on the row without waiting, inside a
        savepoint, and rolls the savepoint back at once, granted or not.
        That ends the lock before anything else runs, so the holder's lease
        renewal, its outcome write, the reaper, a claim and ox_prune never
        wait on this transaction. Released instead of rolled back, the lock
        would last until the caller commits and hold them all off. A lock
        this transaction took before the savepoint is kept. The request has
        three answers:

        - Granted: no newer version of the row exists. A row this
          transaction wrote or locked itself is granted too.
        - lock_not_available (55P03): another transaction is writing or
          locking the row and has not committed. This is contention that is
          compatible with deduplicating, not a fresh read. The snapshot's
          row was the last committed one when the check ran, and the other
          transaction may commit a finish a moment later. A read at READ
          COMMITTED leaves the same window. It is caught here, or
          _enqueue_unique would take it for a lock wait.
        - serialization_failure (40001): a change or a delete committed
          after the snapshot began. The holder may have finished, or it may
          only have renewed its lease, been claimed or been requeued. This
          transaction cannot read the new row to tell, so the failure is
          returned for a read on another connection to decide.

        Any other error propagates. A share lock, not a key share lock. A
        key share lock conflicts only with a delete or a change to a key
        column, so it is granted after the worker's outcome write and the
        reaper's LOST write, on exactly the stale row this check exists to
        catch.
        """
        sid = _savepoint(db_alias)
        try:
            try:
                cls._lock_holder_nowait(task_pk, db_alias)
            finally:
                transaction.savepoint_rollback(sid, using=db_alias)
        except OperationalError as exc:
            code = _pg_sqlstate(exc)
            if code == PG_LOCK_NOT_AVAILABLE:
                return None
            if code == PG_SERIALIZATION_FAILURE:
                return exc
            raise
        return None

    @staticmethod
    def _lock_holder_nowait(task_pk: Any, db_alias: str) -> None:
        """
        SELECT ... FOR SHARE NOWAIT on the holder's row, on the caller's
        connection. The ORM's select_for_update() cannot ask for a share
        lock. Table and column names come from the model and are quoted by
        the backend.
        """
        from django_ox.models import OxTask

        connection = connections[db_alias]
        meta = OxTask._meta
        id_field = meta.get_field("id")
        # Concrete fields always carry a column; the stubs say Optional.
        id_column = cast("str", id_field.column)
        quote = connection.ops.quote_name
        sql = (
            f"SELECT 1 FROM {quote(meta.db_table)} "  # noqa: S608 - model names
            f"WHERE {quote(id_column)} = %s FOR SHARE NOWAIT"
        )
        with connection.cursor() as cursor:
            cursor.execute(sql, [id_field.get_db_prep_value(task_pk, connection)])

    def _result_from_a_fresh_read[**P, R](
        self,
        task: Task[P, R],
        key: str,
        task_id: Any,
        db_alias: str,
        *,
        level: str,
        refusal: OperationalError,
    ) -> TaskResult[P, R]:
        """
        The pending holder's row changed after the caller's snapshot began.
        Decide on the committed rows instead, read once on a connection of
        their own.

        When the holder is still READY or RUNNING and the key's lock still
        names it, a lease renewal, a claim or a reaper requeue changed the
        row. The enqueue deduplicates and returns the result built from that
        read. The caller's own transaction keeps reading its snapshot, so a
        read of the task inside it can still show the older row.

        Otherwise the holder finished, its row is gone, or the key's lock is
        gone or names another task. This transaction cannot release the lock,
        because the row it would lock changed too, and it cannot see a newer
        holder. UniqueEnqueueRace, chained to PostgreSQL's refusal, names the
        database and the level and says where to retry. Nothing was written.

        When the read itself fails, the enqueue can't decide either, and it
        raises UniqueEnqueueRace chained to that failure: a refused
        connection, the read's lock or statement timeout, or any other
        database error. The caller's transaction is untouched by it, and a
        retry at READ COMMITTED needs no such read.
        """
        from django_ox.models import OxTask
        from django_ox.results import task_result_from_db

        opening = (
            f"Could not enqueue unique task {task.module_path!r} on database "
            f"{db_alias!r}. The enqueue ran inside a transaction at "
            f"{level.upper()}, and the task that held the key changed after that "
            "transaction's snapshot began."
        )
        try:
            holder, lock_task_id = self._read_holder_fresh(task_id, key, db_alias)
        except DatabaseError as exc:
            raise UniqueEnqueueRace(
                f"{opening} {self._fresh_read_failure(exc)} "
                f"{RETRY_OUTSIDE_THE_SNAPSHOT}"
            ) from exc
        if holder is None:
            found = "the task's row gone"
        elif holder.status not in (OxTask.Status.READY, OxTask.Status.RUNNING):
            found = f"the task {holder.status}"
        elif lock_task_id is None:
            found = "the key's lock gone"
        elif lock_task_id != holder.pk:
            found = "the key held by another task"
        else:
            return cast("TaskResult[P, R]", task_result_from_db(holder))
        raise UniqueEnqueueRace(
            f"{opening} A fresh read found {found}. This transaction can't see "
            f"that change. {RETRY_OUTSIDE_THE_SNAPSHOT}"
        ) from refusal

    @staticmethod
    def _fresh_read_failure(exc: DatabaseError) -> str:
        """
        What a failed fresh read tells the caller. The database's words come
        from the driver's primary message, without the statement text psycopg
        adds after it, or else from the error's first line.
        """
        diag = getattr(exc.__cause__, "diag", None)
        detail = getattr(diag, "message_primary", None)
        if not isinstance(detail, str) or not detail:
            lines = str(exc).strip().splitlines()
            detail = lines[0] if lines else type(exc).__name__
        message = f"A fresh read on a new connection failed ({detail})."
        if _pg_sqlstate(exc) == PG_LOCK_NOT_AVAILABLE:
            message += (
                f" That read waits at most {FRESH_READ_LOCK_TIMEOUT} for a lock. "
                "A request for the task or lock table, such as ALTER TABLE, can "
                "be waiting for this transaction to end, and the read queues "
                "behind it."
            )
        return message

    @staticmethod
    def _read_holder_fresh(task_id: Any, key: str, db_alias: str) -> tuple[Any, Any]:
        """
        The holder's committed row, or None when it is gone, and the task id
        the key's lock names, or None when the lock is gone. One read.

        The read runs on a connection created for it and closed before
        returning, whatever happens. The connection is never registered with
        the connection handler, never kept for a later call and never taken
        from OPTIONS["pool"], where a request waiting on its own pool could
        stall. The read runs in a transaction of its own at READ COMMITTED,
        whatever OPTIONS["isolation_level"] or the server's default says, so
        it sees every commit made before it started. It takes no row lock.
        The ORM builds the statement and Django's converters read the row, so
        the result matches a read on the caller's connection.

        The transaction also bounds the wait. The caller's transaction holds
        ACCESS SHARE on the task table and the lock table, from its own reads
        of them. A request for ACCESS EXCLUSIVE on either (ALTER TABLE, LOCK
        TABLE, TRUNCATE, VACUUM FULL) then waits on the caller, and this read
        queues behind that request. The caller waits on this read outside the
        database, so PostgreSQL sees no deadlock, and by default nothing ends
        any of the three waits. Every worker statement on that table queues
        behind the request too. While the caller holds its lock, no such
        request can be granted, so a lock wait here ends only if that request
        gives up. SET LOCAL ends the wait after FRESH_READ_LOCK_TIMEOUT and
        the statement after FRESH_READ_STATEMENT_TIMEOUT, for this transaction
        alone. A session-level SET would stay on a server session that a
        pooler hands to another client.

        Connecting is bounded too. Where the project sets no connect timeout,
        the connection waits FRESH_READ_CONNECT_TIMEOUT seconds for each
        address. Nothing here bounds the host name lookup, or a wait in a
        connection pooler's queue before the statement reaches PostgreSQL.
        """
        from django_ox.models import OxTask

        from .models import OxpullUniqueLock

        fields = [field.attname for field in OxTask._meta.concrete_fields]
        named = OxpullUniqueLock.objects.filter(key=key).values("task_id")[:1]
        query = (
            OxTask.objects.filter(id=task_id)
            .annotate(**{LOCK_HOLDER_ANNOTATION: Subquery(named)})
            .values_list(*fields, LOCK_HOLDER_ANNOTATION)
            .query
        )
        fresh = connections.create_connection(db_alias)
        try:
            # Only PostgreSQL reads a holder this way. The stubbed tests that
            # drive this read on the other databases skip its bounds.
            bounded = fresh.vendor == "postgresql"
            options = {
                name: value
                for name, value in fresh.settings_dict.get("OPTIONS", {}).items()
                if name not in ("pool", "isolation_level")
            }
            if bounded and not _sets_its_own_connect_timeout(options):
                options["connect_timeout"] = FRESH_READ_CONNECT_TIMEOUT
            fresh.settings_dict = {
                **fresh.settings_dict,
                "AUTOCOMMIT": True,
                "OPTIONS": options,
            }
            with fresh.cursor() as cursor:
                if bounded:
                    # Fixed text. With no parameters both drivers send the
                    # three statements in one round trip.
                    cursor.execute(
                        "BEGIN ISOLATION LEVEL READ COMMITTED; "
                        f"SET LOCAL lock_timeout = '{FRESH_READ_LOCK_TIMEOUT}'; "
                        "SET LOCAL statement_timeout = "
                        f"'{FRESH_READ_STATEMENT_TIMEOUT}'"
                    )
                compiler = query.get_compiler(connection=fresh)
                rows = list(compiler.results_iter(tuple_expected=True))
                if bounded:
                    cursor.execute("COMMIT")
        finally:
            fresh.close()
        if not rows:
            return None, None
        *values, lock_task_id = rows[0]
        return OxTask.from_db(db_alias, fields, values), lock_task_id

    @staticmethod
    def _reads_a_snapshot(db_alias: str) -> bool:
        """
        Whether a plain SELECT on this connection, inside its open
        transaction, answers from a read view older than the statement.

        MySQL at REPEATABLE READ, InnoDB's server default, fixes the read
        view at the transaction's first consistent read; at READ
        COMMITTED, which Django's MySQL backend sets on every connection
        unless OPTIONS["isolation_level"] says otherwise, each statement
        takes a fresh one. The session is asked rather than the setting
        read, because the setting may be None for the server's default
        and a session can change its own level.
        """
        connection = connections[db_alias]
        if connection.vendor != "mysql":
            return False
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@transaction_isolation")
            (level,) = cursor.fetchone()
        return str(level).upper().replace("_", "-") == "REPEATABLE-READ"

    @staticmethod
    def _committed_status(task_id: Any, db_alias: str) -> str | None:
        """
        The holder's committed status, read on a connection of its own.

        A connection opened here is outside the caller's transaction, so
        its one statement sees the latest commit, and a consistent read
        takes no lock. Table and column names come from the model and
        are quoted by the backend; the ORM is not used because it routes
        every query through the connection handler, and this connection
        is deliberately not registered there. It is closed before
        returning. None means the row is gone.
        """
        from django_ox.models import OxTask

        meta = OxTask._meta
        id_field = meta.get_field("id")
        # Concrete fields always carry a column; the stubs say Optional.
        id_column = cast("str", id_field.column)
        status_column = cast("str", meta.get_field("status").column)
        fresh = connections.create_connection(db_alias)
        try:
            quote = fresh.ops.quote_name
            sql = (
                f"SELECT {quote(status_column)} "  # noqa: S608 - model names
                f"FROM {quote(meta.db_table)} "
                f"WHERE {quote(id_column)} = %s"
            )
            with fresh.cursor() as cursor:
                cursor.execute(sql, [id_field.get_db_prep_value(task_id, fresh)])
                row = cursor.fetchone()
        finally:
            fresh.close()
        return None if row is None else str(row[0])

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        if self.alias not in settings.TASKS:
            # The parent's schedule checks index settings.TASKS by alias
            # and would KeyError on a backend constructed outside TASKS;
            # fail with a real message instead.
            return [
                checks.Error(
                    f"Backend alias {self.alias!r} is not present in settings.TASKS.",
                    hint="check() requires a configured alias; construct "
                    "the backend through the TASKS setting.",
                    id="oxpull.E003",
                )
            ]
        errors = super().check(**kwargs)
        if not apps.is_installed("oxpull"):
            errors.append(
                checks.Error(
                    "oxpull must be in INSTALLED_APPS to use OxpullBackend.",
                    hint="Add 'oxpull' to INSTALLED_APPS and run migrations.",
                    id="oxpull.E001",
                )
            )
        errors.extend(self._check_unique_task_paths())
        errors.extend(self._check_router())
        errors.extend(self._check_unique_schedules())
        errors.extend(self._check_rate_limits())
        errors.extend(self._check_limit_names_across_backends())
        errors.extend(self._check_reconcile_schedule())
        errors.extend(self._check_workflows())
        return errors

    @staticmethod
    def _first_oxpull_alias() -> str | None:
        """
        The first OxpullBackend alias in settings.TASKS, in sort order.

        A check about the whole installation is emitted from this alias
        alone, so it is reported once rather than once per alias, and one
        id silences it.
        """
        from .batches import task_backend_classes

        return min(
            (
                alias
                for alias, backend_class in task_backend_classes().items()
                if issubclass(backend_class, OxpullBackend)
            ),
            default=None,
        )

    def _check_reconcile_schedule(self) -> list[checks.CheckMessage]:
        """
        Warn, once, when no django-ox backend schedules the reconciler.

        Batches complete only through oxpull.batches.reconcile on a
        schedule, and nothing else says so before the first batch sits
        SEALED forever. The check reads the same settings create() and
        seal() refuse on, and it is emitted from the first OxpullBackend
        alias in sort order rather than from every alias, so one id
        silences it in a project that uses uniqueness or limits and
        never a batch.

        A Warning rather than an Error, unlike E006: check() runs with no
        database, so whether batches exist is unknowable here, and an
        Error would stop migrate and runserver for every installation
        that never creates a batch. The silent failure an Error would
        guard against is closed by the refusal in create() and seal().

        One layout is an Error, oxpull.E008, in place of the warning: the
        reconciler exposed to schedule rows, and no backend that reads
        them able to run it, so django-ox skips every row that names it
        and nothing else schedules it. Exposing the reconciler says the
        project means to run batches, and django-ox reports the same fault
        in its settings channel, a SCHEDULES entry on a queue its backend
        refuses, as the Error django_ox.E002. A row is accepted by the
        admin and create_schedule and then skipped with a log line, so
        this is the first place that can refuse it.
        """
        from .batches import (
            RECONCILE_SCHEDULE_NAME,
            RECONCILE_TASK_PATH,
            reconcile_schedule,
            reconcile_schedule_hint,
        )

        if self._first_oxpull_alias() != self.alias:
            return []
        schedule = reconcile_schedule(RECONCILE_TASK_PATH)
        if schedule.aliases:
            return []
        if schedule.unbound:
            return [
                _unbound_reconciler_error(
                    RECONCILE_TASK_PATH,
                    RECONCILE_SCHEDULE_NAME,
                    schedule,
                    "no batch will ever complete",
                )
            ]
        return [
            checks.Warning(
                "No django-ox backend in settings.TASKS schedules "
                "oxpull.batches.reconcile. Batches complete only through "
                "that schedule; create() and seal() refuse until one exists.",
                hint=reconcile_schedule_hint(),
                id="oxpull.W002",
            )
        ]

    def _check_workflows(self) -> list[checks.CheckMessage]:
        """
        Workflows declared without a worker that gates on dependencies.

        The same silent failure rate limits have, with a worse outcome:
        the claim would carry no dependency conjunct, so every node of
        every workflow runs the moment it is enqueued, in enqueue order,
        and the queue looks healthy while the DAG is not a DAG. An Error
        for that reason.

        WORKFLOWS is a declared option precisely so this check can exist.
        A workflow is created at run time and no check could see one.
        """
        from django_ox.worker import worker_class

        from .worker import OxpullWorker
        from .workflows import workflows_enabled_from_options

        try:
            enabled = workflows_enabled_from_options(self.options)
        except ImproperlyConfigured as exc:
            return [
                checks.Error(
                    str(exc),
                    hint="OPTIONS['WORKFLOWS'] switches the dependency "
                    "gate in the worker's claim; it is validated here so "
                    "a bad value cannot reach a running worker.",
                    id="oxpull.E007",
                ),
            ]
        if not enabled:
            return []
        # Whatever this backend's worker is: the warning is emitted from
        # one backend only, and an Error here must not swallow it.
        reconciler = self._check_workflow_reconciler()
        try:
            runs = worker_class(self.alias)
        except ImproperlyConfigured as exc:
            return [checks.Error(str(exc), id="oxpull.E007"), *reconciler]
        if not issubclass(runs, OxpullWorker):
            return [
                checks.Error(
                    f"Backend {self.alias!r} declares OPTIONS['WORKFLOWS'], "
                    f"but its worker is {runs.__module__}.{runs.__qualname__}, "
                    "which does not gate a claim on its dependencies. Every "
                    "workflow node would run as soon as it was enqueued, "
                    "silently.",
                    hint="Set OPTIONS['WORKER_CLASS'] to 'oxpull.worker.OxpullWorker'.",
                    id="oxpull.E007",
                ),
                *reconciler,
            ]
        return reconciler

    def _check_workflow_reconciler(self) -> list[checks.CheckMessage]:
        """
        Workflows declared with nothing scheduled to reconcile them.

        The claim cannot cancel the downstream cone of a node that
        settled without succeeding, and it cannot complete a sealed
        workflow. Both are the reconciler's, and a workflow backend
        without it fails silently in the worst way: every check passes,
        the queue looks healthy, unreachable
        nodes stay READY in every worker's candidate set forever, and no
        callback ever fires. Nothing else in the package notices.

        The reconciler is global: it sweeps every SEALED workflow whatever
        backend enqueued it, so scheduling it once on any django-ox backend
        is a correct install. The count is the one batches use, for the
        workflow reconciler's path, so create(), seal() and this check
        answer from one place: both schedule channels count, a SCHEDULES
        entry on a backend that is not django-ox does not, and a backend
        reading schedule rows counts only when it can run the reconciler.

        The condition is global too, so it is reported once, from the first
        backend in sort order that declares WORKFLOWS, as oxpull.W002 is. A
        warning per workflow backend, each hint pasted into its own
        backend, would define one schedule name on several backends, which
        django_ox.E003 refuses. The hint names a backend the reconciler
        binds to, so the paste passes django_ox.E002 as well.

        A Warning and not an Error, matching W001 and for a practical
        reason: `manage.py check` is run by every management command,
        `ox_worker` included, and an Error would refuse to start the very
        worker an operator is adding the schedule for. The refusal in
        workflows.create() and seal() closes the silent failure. The one
        Error is oxpull.E008, for the reason _check_reconcile_schedule
        gives.
        """
        from .batches import reconcile_schedule
        from .workflows import (
            RECONCILE_SCHEDULE_NAME,
            RECONCILE_TASK_PATH,
            declaring_aliases,
            reconcile_schedule_hint,
        )

        declaring = declaring_aliases()
        if not declaring or declaring[0] != self.alias:
            return []
        schedule = reconcile_schedule(RECONCILE_TASK_PATH)
        if schedule.aliases:
            return []
        if schedule.unbound:
            return [
                _unbound_reconciler_error(
                    RECONCILE_TASK_PATH,
                    RECONCILE_SCHEDULE_NAME,
                    schedule,
                    "no workflow will ever complete, and nothing will cancel "
                    "the downstream cone of a node that fails",
                )
            ]
        subject = (
            f"Backend {declaring[0]!r} declares"
            if len(declaring) == 1
            else "Backends "
            + ", ".join(repr(alias) for alias in declaring)
            + " declare"
        )
        return [
            checks.Warning(
                f"{subject} OPTIONS['WORKFLOWS'], but no django-ox backend "
                f"schedules {RECONCILE_TASK_PATH!r}. Nothing will cancel the "
                "downstream cone of a node that fails, and no workflow will "
                "ever complete or fire its callbacks. The reconciler is also "
                "what releases a waiting node whose release a worker missed, "
                "so without it that node waits forever. workflows.create() and "
                "seal() refuse until one exists.",
                hint=reconcile_schedule_hint(schedule),
                id="oxpull.W003",
            )
        ]

    def _check_rate_limits(self) -> list[checks.CheckMessage]:
        """
        Validate RATE_LIMITS, then insist on a worker that applies it.

        Limits configured without WORKER_CLASS is the feature's only
        silent failure: everything starts, nothing errors, and no limit
        is ever applied. It is an Error rather than a Warning for that
        reason. The check can see it because RATE_LIMITS is the only
        place a limit is declared, and settings are readable here.
        """
        from django_ox.worker import worker_class

        from .worker import OxpullWorker

        try:
            limits = rate_limits_from_options(self.options, self.alias, self.queues)
            refresh_interval_from_options(self.options)
        except ImproperlyConfigured as exc:
            return [
                checks.Error(
                    str(exc),
                    hint="Fix the OPTIONS entry the message names. "
                    "RATE_LIMITS and RATE_REFRESH_INTERVAL are validated "
                    "here so a bad value cannot reach a running worker.",
                    id="oxpull.E005",
                )
            ]
        if not limits:
            return []
        errors = self._check_limited_task_paths(limits)
        if errors:
            return errors
        try:
            runs = worker_class(self.alias)
        except ImproperlyConfigured as exc:
            return [checks.Error(str(exc), id="oxpull.E006")]
        if not issubclass(runs, OxpullWorker):
            return [
                checks.Error(
                    f"Backend {self.alias!r} configures rate limit(s) "
                    f"{sorted(limits)}, but its worker is "
                    f"{runs.__module__}.{runs.__qualname__}, which does not "
                    "apply them. Every limit would be ignored, silently.",
                    hint="Set OPTIONS['WORKER_CLASS'] to 'oxpull.worker.OxpullWorker'.",
                    id="oxpull.E006",
                )
            ]
        return []

    def _check_limit_names_across_backends(self) -> list[checks.CheckMessage]:
        """
        One limit name configured differently on two OxpullBackend aliases.

        A limit's window row is keyed on its name alone, so every alias
        that configures a name counts on one shared row. Different periods
        cannot both hold on it. A worker on the shorter period rolls the
        window as soon as its own period allows, and the count starts again
        from zero. A worker on the longer period that reads the fresh window
        admits against that count, so its backend can admit more than its
        limit in its own period. A worker that has exhausted its limit
        waits out its own window first, which slows the overrun and does
        not stop it. No restart or deploy order fixes that, so the limit is
        not enforced as configured. It is still a Warning, oxpull.W005, and
        not an Error. django-ox's ox_worker runs every system check before
        it starts, so an Error here would stop the workers of every backend
        at upgrade, not only the two that disagree, and task processing with
        them. That is the breaking upgrade oxpull.W002 is a Warning to avoid.
        Different capacities over one period make a shared budget in which
        each alias stops at its own mark in the shared count. That can be
        meant, so it is a Warning too, under an id of its own, oxpull.W004.

        Two spellings of a name that differ only by case or accents are a
        Warning of their own, oxpull.W006, whatever each alias configures
        under them. MySQL's default collation makes one row of them, so they
        share one window as one name does. PostgreSQL and SQLite, and a
        binary MySQL collation such as utf8mb4_bin, make two, and there the
        spellings already work as two separate limits. utf8mb4_0900_as_cs is
        not named with them, because it makes one row of é precomposed and e
        followed by a combining acute. Two spellings on one backend are
        reported too, because the parser refuses only a case difference
        there. On a collation that folds them, that backend's workers refuse
        them at their first refresh, and the message says so. Settings do
        not show the column's collation, so they are reported on every
        database. An Error would stop every backend's workers at upgrade for
        oxpull.W005's reason, and it would do so on installations where the
        two limits work as their author meant. The message says what MySQL,
        PostgreSQL and SQLite each do, not only the database check runs
        against. check can run against a different database from the one the
        workers use, SQLite in CI and MySQL in production, and a message
        fitted to the check's database would then describe the wrong one.
        _spelling_key decides which spellings are one. What else a collation
        folds only the database can say, and a worker refuses it only within
        its own limits.

        Limits are compared as rate_limits_from_options resolves them, so
        a per of 60 and one of timedelta(minutes=1) are one period, as
        they are to the worker. An alias whose RATE_LIMITS do not parse is
        left out, and its own oxpull.E005 reports why. An alias whose limits
        parse is compared, even when another of its checks fails.

        Settings only, like every check here. It never reads the database.
        Emitted from the first OxpullBackend alias in sort order, as
        oxpull.W002 is, so each name is reported once.
        """
        from .batches import task_backend_classes

        if self._first_oxpull_alias() != self.alias:
            return []
        configured: dict[str, list[tuple[str, RateLimit]]] = {}
        for alias, backend_class in sorted(task_backend_classes().items()):
            if not issubclass(backend_class, OxpullBackend):
                continue
            params = settings.TASKS[alias]
            queues = params.get("QUEUES", [DEFAULT_TASK_QUEUE_NAME])
            try:
                limits = rate_limits_from_options(
                    params.get("OPTIONS", {}), alias, queues
                )
            except ImproperlyConfigured:
                continue
            for name, limit in limits.items():
                configured.setdefault(name, []).append((alias, limit))
        spellings: dict[str, list[str]] = {}
        for name in sorted(configured):
            spellings.setdefault(_spelling_key(name), []).append(name)
        messages: list[checks.CheckMessage] = []
        for names in spellings.values():
            if len(names) > 1:
                messages.append(_one_name_spelled_differently(names, configured))
            for name in names:
                messages.extend(_one_name_disagreeing(name, configured[name]))
        return messages

    def _check_limited_task_paths(
        self, limits: dict[str, RateLimit]
    ) -> list[checks.CheckMessage]:
        """
        Every path under a limit's `tasks` must import to a Task.

        A limit matches on the task path stored in the row, so a typo
        covers nothing and throttles nothing, and the queue looks healthy
        while the third party being protected is not. Same reasoning as
        the UNIQUE_TASKS check, and the same failure it prevents.
        """
        errors: list[checks.CheckMessage] = []
        for limit in limits.values():
            for path in sorted(limit.task_paths):
                try:
                    obj = import_string(path)
                except ImportError:
                    obj = None
                if not isinstance(obj, Task):
                    errors.append(
                        checks.Error(
                            f"RATE_LIMITS[{limit.name!r}]['tasks'] entry "
                            f"{path!r} is not a django.tasks Task. The limit "
                            "would cover nothing under that name.",
                            hint="Use the dotted path of a @task-decorated "
                            "function, as it is stored on the task row.",
                            id="oxpull.E005",
                        )
                    )
        return errors

    def _check_unique_task_paths(self) -> list[checks.CheckMessage]:
        """
        Shape errors raise in __init__; this validates the deeper
        property, that every configured path imports to a django.tasks
        Task. A typo here would otherwise just silently never dedup.
        """
        errors: list[checks.CheckMessage] = []
        for path in sorted(self.unique_task_paths):
            try:
                obj = import_string(path)
            except ImportError:
                errors.append(
                    checks.Error(
                        f"UNIQUE_TASKS entry {path!r} does not import.",
                        hint="Use the dotted path of a @task-decorated function.",
                        id="oxpull.E002",
                    )
                )
                continue
            if not isinstance(obj, Task):
                errors.append(
                    checks.Error(
                        f"UNIQUE_TASKS entry {path!r} is not a django.tasks Task.",
                        hint="Point it at the module-level name created "
                        "by the @task decorator.",
                        id="oxpull.E002",
                    )
                )
        return errors

    def _check_router(self) -> list[checks.CheckMessage]:
        try:
            oxpull_db_alias()
        except ImproperlyConfigured as exc:
            return [
                checks.Error(
                    str(exc),
                    hint="Route django_ox and oxpull models to the same database.",
                    id="oxpull.E004",
                )
            ]
        return []

    def _check_unique_schedules(self) -> list[checks.CheckMessage]:
        """
        Warn when a schedule enqueues a unique task: schedules already
        dedup once per tick, and a unique scheduled task coalesces with
        any pending manual enqueue of the same key, so the tick row may
        record against the manual task.
        """
        raw = self.options.get("SCHEDULES", {})
        if not isinstance(raw, dict):
            return []  # shape reported by the parent's schedule checks
        warnings: list[checks.CheckMessage] = []
        for name, config in raw.items():
            path = config.get("task") if isinstance(config, dict) else None
            if isinstance(path, str) and (
                is_registered(path) or path in self.unique_task_paths
            ):
                warnings.append(
                    checks.Warning(
                        f"Schedule {name!r} enqueues unique task {path!r}; "
                        "unique scheduled tasks coalesce with pending "
                        "manual enqueues, and ticks may record against "
                        "the manual task.",
                        hint="Schedules already dedup once per tick; "
                        "@unique on a scheduled task is usually redundant.",
                        id="oxpull.W001",
                    )
                )
        return warnings


def _one_name_disagreeing(
    name: str, entries: list[tuple[str, RateLimit]]
) -> list[checks.CheckMessage]:
    """oxpull.W005 or oxpull.W004 for one name, if its aliases disagree."""
    if len({(limit.limit, limit.per) for _, limit in entries}) < 2:
        return []
    described = _in_words(
        f"{alias!r} (limit={limit.limit}, per={limit.per})" for alias, limit in entries
    )
    if len({limit.per for _, limit in entries}) > 1:
        return [
            checks.Warning(
                f"Rate limit {name!r} has a different period on backends "
                f"{described}, and it is not enforced as configured. A limit's "
                "window is keyed on its name alone, so these backends roll one "
                "shared window, each at its own period. Each roll restarts the "
                "count, so a backend on a longer period can admit more than its "
                "limit in that period.",
                hint="Use one period for this name on every backend, or give "
                "each backend's limit a name of its own.",
                id="oxpull.W005",
            )
        ]
    return [
        checks.Warning(
            f"Rate limit {name!r} has a different limit on backends "
            f"{described}. A limit's window is keyed on its name alone, so "
            "they share one window and count each other's admissions. Each "
            "backend stops when the shared count reaches its own limit, so "
            "together they admit up to the largest one.",
            hint="If a shared budget is what you want, use the same limit for "
            "this name on every backend. Otherwise give each backend's limit a "
            "name of its own.",
            id="oxpull.W004",
        )
    ]


def _one_name_spelled_differently(
    names: list[str], configured: dict[str, list[tuple[str, RateLimit]]]
) -> checks.CheckMessage:
    """
    oxpull.W006: spellings of one limit name that differ only by case or
    accents.

    The spellings can be on one backend. The parser refuses two that differ
    only by case there, but not two that differ by accents. A backend that
    configures two spellings does not count on the shared window. On a
    collation that folds them, its workers refuse them at their first
    refresh, so the message says that for it.
    """
    shown = _shown_names(names)
    by_alias: dict[str, list[str]] = {}
    for name in names:
        for alias, _ in configured[name]:
            by_alias.setdefault(alias, []).append(name)
    if len(by_alias) == 1:
        alias, spellings = next(iter(by_alias.items()))
        subject = (
            f"Rate limit names {_in_words(shown[name] for name in spellings)} "
            f"on backend {alias!r}"
        )
    else:
        described = _in_words(
            f"{alias!r} ({_in_words(shown[name] for name in spellings)})"
            for alias, spellings in sorted(by_alias.items())
        )
        subject = f"Rate limit names on backends {described}"
    doubled = [alias for alias, found in sorted(by_alias.items()) if len(found) > 1]
    single = [alias for alias, found in sorted(by_alias.items()) if len(found) == 1]
    on_mysql = (
        "On MySQL's default collation, which ignores case and accents, these "
        "spellings are one name"
    )
    if not doubled:
        on_mysql += (
            ", so the backends share one window and count each other's admissions."
        )
        hint = (
            "Use one spelling of this name on every backend. If each backend "
            "needs a limit of its own, put the backend's alias in each name."
        )
    else:
        noun = "backend" if len(doubled) == 1 else "backends"
        on_mysql += (
            f", so the workers for {noun} {_in_words(map(repr, doubled))} "
            "refuse them before claiming any task."
        )
        if len(single) > 1:
            on_mysql += (
                f" Backends {_in_words(map(repr, single))} count each other's "
                "admissions."
            )
        hint = (
            "Keep one of these limits, or give each one a name that differs by "
            "more than case or accents."
        )
    return checks.Warning(
        f"{subject} differ only by case or accents. A limit's window is keyed "
        "on its name, and the database's collation decides which spellings "
        f"match. {on_mysql} On PostgreSQL and SQLite, and on a binary MySQL "
        "collation such as utf8mb4_bin, they are two separate limits.",
        hint=hint,
        id="oxpull.W006",
    )


def _shown_names(names: list[str]) -> dict[str, str]:
    """
    Each name as oxpull.W006 prints it.

    Two names that NFKC normalization makes one can print alike, such as é
    precomposed and e followed by a combining acute. Those are printed with
    each non-ASCII character as its code point, so the reader can tell
    which one to change.
    """
    forms = [unicodedata.normalize("NFKC", name) for name in names]
    return {
        name: _code_points(name) if forms.count(form) > 1 else repr(name)
        for name, form in zip(names, forms, strict=True)
    }


def _code_points(name: str) -> str:
    """repr(name), with each non-ASCII character escaped as its code point."""
    return "".join(
        char
        if char.isascii()
        else f"\\U{ord(char):08x}"
        if ord(char) > 0xFFFF
        else f"\\u{ord(char):04x}"
        for char in repr(name)
    )


# Combining marks that Unicode 3.2 already had and that MySQL's default
# collation weighs as accents only. U+0363 to U+036F are small letters
# written above another, and it weighs those as letters.
_ACCENTS = frozenset(
    chr(code)
    for code in range(0x300, 0x363)
    if unicodedata.ucd_3_2_0.category(chr(code)) == "Mn"
)


def _spelling_key(name: str) -> str:
    """
    One key for the spellings of a limit name that differ only by case or
    accents, for oxpull.W006.

    Two names with one key are meant to be one name to MySQL's default
    collation, utf8mb4_0900_ai_ci, and to utf8mb4_unicode_ci. On MySQL 8.4,
    pieces of names were compared with their own keys under both
    collations, and every one matched. The pieces were each assigned code
    point except controls, surrogates and private use, alone and
    decomposed. They were also 100 base characters, most of them Latin,
    Greek and Cyrillic letters, each followed by each mark from U+0300 to
    U+036F. The last were ten vowels, each followed by 300 pairs of marks
    from U+0300 to U+0362. Whole names were not compared. A longer name
    matches only if MySQL weighs its characters one by one, which these
    checks assume. The key is narrower than either collation, so it misses
    some pairs they fold.

    Only characters Unicode 3.2 already had are folded. MySQL weighs later
    ones, such as Georgian Mtavruli, apart from their lower case. Case is
    folded with str.casefold(). Accents are dropped only from Latin and
    Greek letters, because MySQL keeps й apart from и. A character is
    decomposed only when that drops an accent, because MySQL keeps some
    precomposed Tamil letters apart from their decomposition. Letters with
    no decomposition that MySQL folds, such as ø into o, are left to the
    worker, which asks the database.
    """
    key = ""
    for char in name:
        if char in _ACCENTS:
            if not (key and _takes_accents(key[-1])):
                key += char
        elif unicodedata.ucd_3_2_0.category(char) == "Cn":
            key += char
        else:
            if _takes_accents(unicodedata.normalize("NFD", char)[0]):
                folded = _unaccented(_unaccented(char).casefold())
            else:
                folded = char.casefold()
            if any(unicodedata.ucd_3_2_0.category(c) == "Cn" for c in folded):
                folded = char
            key += folded
    return key


def _takes_accents(char: str) -> bool:
    """A Latin or Greek letter that Unicode 3.2 already had."""
    return unicodedata.ucd_3_2_0.category(char).startswith("L") and (
        unicodedata.name(char, "").startswith(("LATIN ", "GREEK "))
    )


def _unaccented(text: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFD", text) if char not in _ACCENTS
    )


def _in_words(items: Iterable[str]) -> str:
    """Items as a sentence lists them: "a", "a and b", "a, b and c"."""
    listed = list(items)
    if len(listed) < 2:
        return "".join(listed)
    return f"{', '.join(listed[:-1])} and {listed[-1]}"
