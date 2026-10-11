"""
The worker that applies rate limits.

    TASKS = {
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "OPTIONS": {
                "WORKER_CLASS": "oxpull.worker.OxpullWorker",
                "RATE_LIMITS": {
                    "stripe": {
                        "limit": 100,
                        "per": 60,
                        "tasks": ["myapp.billing.charge"],
                    },
                },
            },
        }
    }

ox_worker runs this class when WORKER_CLASS selects it. The supervisor
behind --processes N starts each child through ox_worker, so select the
worker in settings rather than only in the parent process. Otherwise,
children can use the base django_ox.worker.Worker and bypass rate-limit
admission.

The budget is consulted before the claim, never after it. django-ox folds
`attempts = attempts + 1` and `lease_epoch = lease_epoch + 1` into the
claim UPDATE itself, on all three claim paths, so a design that claimed a
task and then discovered it had no budget would have spent an attempt on
a task that never ran, and the lease epoch is a fencing token that cannot
be decremented. A throttled task is therefore never claimed at all: it
stays READY, holds no lease, and is indistinguishable from any other
queued task.

The pre-claim view is necessarily stale, and that is what bounds the
guarantee rather than a shortcut in it:

For a limit of N admissions per period P, with W ox_worker processes
claiming from the queues that carry the limit's tasks, the number of
task attempts started in any one window is at most N + W - 1 + U. U is
the number of admissions whose count did not land: one for each count
that raised (_count_failed) and one for each count the window rolled
out from under MAX_RATE_ATTEMPTS times (the tail of _count), each
logged as rate_limit_uncounted. Add one for each worker process that
dies between claiming a limited task and recording it. With a single
worker process, no such death and every count landed, the limiter is
exact: at most N per window. A window is one generation of the shared
counter row, opened by a roll and lasting P, not every interval of
length P. Windows are contiguous, so an arbitrary interval of length P
that spans a boundary can carry up to 2(N + W - 1 + U). A count that
cannot be written is logged and the attempt runs; three admissions in a
row that the counter did not record, whether the increment raised or
matched no row, close the limit until a write lands.

The proof rests on refreshing the view after every admission, before the
next limited claim, which is why _record() marks the view dirty and why a
limited claim costs a SELECT as well as an UPDATE.
"""

from __future__ import annotations

# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

import inspect
import json
import logging
import random
import time
import uuid
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import (
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    connections,
    transaction,
)
from django.db.models import F, Q, QuerySet
from django.db.models.functions import Now

from django_ox.models import OxTask
from django_ox.worker import POSTGRES_CLAIM_SQL, Worker, _lease_now

from .batches import ADD_RETRY_JITTER
from .models import OxpullRateWindow, OxpullWorkflowNode
from .rate_limits import rate_limits_from_options, refresh_interval_from_options
from .routing import oxpull_db_alias
from .workflows import (
    NODE_FLAG,
    lost_lock_conflict,
    node_flag,
    node_flag_for_a_locking_read,
    node_flag_sql,
    record_node_outcome,
    release_downstream,
    runnable_filter_q,
    runnable_filter_sql,
    session_timeouts,
    transaction_timeouts,
    workflows_enabled_from_options,
    workflows_on_database,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .rate_limits import RateLimit

logger = logging.getLogger("oxpull")

# Bound on the increment's retry loop. A pass only fails when the window
# rolled between the refresh and the increment, so reaching the bound
# takes that many rolls inside one claim. Mirrors MAX_UNIQUE_ATTEMPTS.
MAX_RATE_ATTEMPTS = 5

# Tries of one node outcome's transaction that loses a lock conflict, and the
# seconds each try waits for a lock. Holding the task row for that long stalls
# this worker's renewal of its other leases, so it stays far below a third of
# the default LOCK_TIMEOUT.
OUTCOME_ATTEMPTS = 5
OUTCOME_LOCK_TIMEOUT = 5.0

# Seconds between looks for the workflow node table, by a worker that records
# only because another backend on its database declares WORKFLOWS, while the
# table is not there yet.
NODE_TABLE_RECHECK = 30.0

# The outcomes a node records from the worker. A retry write puts the row back
# to READY and records nothing.
RECORDED_BY_THE_WORKER = (OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED)

# Unrecorded admissions in a row that close a limit: a count that raised,
# or one that matched no row after every attempt. One is a lost count
# and the task runs: the claim is already committed, and losing a
# count is better than losing the task. A run of them is a counter that
# is not being written at all, and admitting against a counter that never
# advances is not rate limiting, it is a worker at full speed with a
# warning in the log.
MAX_COUNT_FAILURES = 3


@dataclass
class _Seen:
    """One limit's window as this worker last read it."""

    window_id: int
    used: int
    # time.monotonic() at which the window ends, derived from the database
    # clock read in the same statement as the row. Local only in its zero
    # point, so a worker whose clock disagrees with the database's still
    # measures the right number of seconds.
    ends_at: float


def _class_hook(worker: Worker, name: str) -> Any:
    """
    The hook `name` as the worker's class defines it, bound to the worker.

    Looked up past the instance, where OxpullWorker binds its own answer
    under the same name, and bound the way attribute access binds it, so
    a hook written as a static method or a class method is called as one.
    """
    raw = inspect.getattr_static(type(worker), name)
    bind = getattr(raw, "__get__", None)
    return raw if bind is None else bind(worker, type(worker))


class OxpullWorker(Worker):
    """
    django-ox's Worker, plus rate limits.

    An install with no limits configured pays nothing: the claim filter
    adds nothing to what the class's hooks return, so the emitted SQL is
    what django-ox emits on its own, and claim_one() delegates without a
    statement of its own.

    A subclass narrows what it claims through django-ox's claim filter
    hooks, claim_filter_q() and claim_filter_sql(). Those hooks carry only
    the subclass's own condition. The worker adds its rate limits to
    whatever they return, so a subclass that never calls super() still
    claims inside every limit.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._limits: dict[str, RateLimit] = rate_limits_from_options(
            self.backend.options, self.backend.alias, self.backend.queues
        )
        self._rate_alias = oxpull_db_alias()
        self._refresh_interval = refresh_interval_from_options(self.backend.options)
        self._workflows = workflows_enabled_from_options(self.backend.options)
        # Whether this worker records node outcomes and releases what they
        # held: wherever a backend on its database declares WORKFLOWS, not
        # only when its own backend does, because a claim is scoped to queues
        # and not to backends.
        self._records = self._workflows or workflows_on_database()
        # A worker whose own backend declares WORKFLOWS needs the workflow
        # tables for its claim in any case. Any other looks for them first.
        self._node_table_seen = self._workflows
        self._node_table_looked_at: float | None = None
        self._seen: dict[str, _Seen] = {}
        self._blocked_paths: frozenset[str] = frozenset()
        self._blocked_queues: frozenset[str] = frozenset()
        # Limits whose counter could not be written MAX_COUNT_FAILURES
        # times running, and the run length that got them there.
        self._closed: set[str] = set()
        self._count_failures: dict[str, int] = {}
        # Stored spelling -> configured name, for a limit whose window row
        # was written under a spelling the column's collation treats as
        # the same name. Learned by _adopt, once; empty everywhere else.
        self._folded: dict[str, str] = {}
        # The view is stale until the first read, and again after every
        # admission this worker makes. That rule is what bounds the
        # overshoot at one per worker rather than at N per worker.
        self._view_stale = True
        self._refresh_after = 0.0
        # Column names come from the model rather than from SQL text here,
        # so a django-ox migration that renamed one would be a failure to
        # import a field, not a fragment that no longer matches.
        self._path_column = OxTask._meta.get_field("task_path").column
        self._queue_column = OxTask._meta.get_field("queue_name").column
        # django-ox reads the claim filter by calling claim_filter_q() and
        # claim_filter_sql() on the worker. Bound here, on the instance,
        # those names answer with what the class's hooks return and what
        # oxpull adds, the rate limits and the workflow gate. Left to the
        # class, they would be a subclass's to override, and one that
        # replaced a hook without calling super() would drop every limit
        # and the gate from its claims.
        self._class_claim_filter_q: Callable[[], Q | None] = _class_hook(
            self, "claim_filter_q"
        )
        self._class_claim_filter_sql: Callable[[], tuple[str, dict[str, Any]]] = (
            _class_hook(self, "claim_filter_sql")
        )
        self.claim_filter_q = self._q_with_oxpull_conditions  # type: ignore[method-assign]
        self.claim_filter_sql = self._sql_with_oxpull_conditions  # type: ignore[method-assign]

    # -- start-up ----------------------------------------------------------

    def run(self) -> None:
        """
        django-ox's poll loop, after checking the workflow schema.

        A worker on a backend that declares WORKFLOWS refuses to start on a
        MySQL database whose workflow task-id columns are on another collation
        than OxTask.id, because every claim it made would fail.
        """
        if self._workflows:
            self._check_workflow_schema()
        super().run()

    def _check_workflow_schema(self) -> None:
        from .checks import collation_mismatches

        try:
            mismatches = collation_mismatches(self._rate_alias)
        except DatabaseError:
            logger.warning(
                "Could not check the workflow schema before starting",
                exc_info=True,
                extra={
                    "event": "workflow_schema_check_failed",
                    "worker_id": self.worker_id,
                },
            )
            return
        if mismatches:
            raise ImproperlyConfigured(
                " ".join(
                    f"{mismatch.message()} {mismatch.hint()}" for mismatch in mismatches
                )
                + " The worker did not start. `manage.py check --database "
                f"{self._rate_alias}` reports the same as oxpull.E009."
            )

    # -- the claim pass ----------------------------------------------------

    def claim_one(self) -> OxTask | None:
        """
        Refresh the budget, claim, then count the admission.

        The refresh and the increment run outside the claim, because the
        SKIP LOCKED path claims inside a transaction that is holding row
        locks on the task table and neither statement belongs in there.

        The claim is the line between fail-closed and fail-open. A
        database error before it propagates: nothing is spent, and the
        poll loop skips the pass. A database error after it, anywhere in
        the accounting, is logged and the claimed task is returned: the
        attempt is spent and the row is RUNNING, and a task claimed but
        never handed over runs nowhere until the reaper finds it.
        """
        # Decided before the claim, which on the SKIP LOCKED path opens a
        # transaction that should not carry a look at the schema.
        self._recording()
        if not self._limits:
            return super().claim_one()
        self._refresh_view()
        db_task = super().claim_one()
        if db_task is not None:
            self._record(db_task)
        return db_task

    def _q_with_oxpull_conditions(self) -> Q | None:
        """
        claim_filter_q() as django-ox reads it from this worker. The class's
        condition and oxpull's own, ANDed.
        """
        condition = self._class_claim_filter_q()
        oxpull = self._oxpull_condition_q()
        if condition is None:
            return oxpull
        if oxpull is None:
            return condition
        return condition & oxpull

    def _sql_with_oxpull_conditions(self) -> tuple[str, dict[str, Any]]:
        """
        claim_filter_sql() as django-ox reads it from this worker. The
        class's fragment, followed by oxpull's own.

        Empty, rate limits and workflow gate and all, when the class has a
        condition in claim_filter_q() and no fragment for it. django-ox takes
        the single-statement PostgreSQL claim for any non-empty fragment, and
        that statement applies the fragment alone, so a fragment of ours there
        would drop the class's condition. An empty one sends django-ox to the
        path that applies claim_filter_q(), which carries both. A class that
        wrote both hooks, or neither, keeps the single statement.
        """
        fragment, params = self._class_claim_filter_sql()
        if not fragment and self._class_claim_filter_q() is not None:
            return "", {}
        ours, our_params = self._oxpull_condition_sql()
        return fragment + ours, {**params, **our_params}

    def _oxpull_condition_q(self) -> Q | None:
        """
        Rate limits and workflow dependencies, conjoined, or None when
        neither applies.

        Two independent reasons a READY row may not be claimed, and both
        have to hold at once: a throttled task is not claimable however
        satisfied its dependencies are, and a workflow node whose parents
        have not succeeded is not claimable however much budget is left.

        Each term is present only where its feature is configured, so a
        backend with neither returns None and the emitted SQL is what
        django-ox emits on its own. The workflow term is switched by
        OPTIONS['WORKFLOWS'] rather than by looking for workflow rows,
        because a workflow is created at run time: a cached "there are no
        workflows" would be a window in which a node runs out of order,
        and a query per claim pass to close that window would cost more
        than the conjunct it was avoiding. Where it is on, it costs one
        probe of oxpull_wf_edge_down_idx, the edge table's index over
        (downstream_task_id, satisfied), per candidate row, which finds
        nothing for a task that is not in a workflow.
        """
        if not (self._workflows or self._blocked_paths or self._blocked_queues):
            return None
        condition = runnable_filter_q() if self._workflows else Q()
        if self._blocked_paths:
            condition &= ~Q(task_path__in=sorted(self._blocked_paths))
        if self._blocked_queues:
            condition &= ~Q(queue_name__in=sorted(self._blocked_queues))
        return condition

    def _oxpull_condition_sql(self) -> tuple[str, dict[str, Any]]:
        """
        The same two conditions for the PostgreSQL claim, which builds
        its own SQL.

        This and claim_filter_q must stay equivalent: PostgreSQL is the
        only database that reads this one, so a divergence would be a
        rule that applies on two databases and not the third, and
        nothing would report it. The suite exercises the ordering
        guarantee through a real claim, so whichever path the database
        under test uses is the one proved.

        Each rate-limit array is one bound parameter however many names
        it holds, so a limit covering hundreds of task paths cannot
        approach the statement's parameter ceiling. Both columns are NOT
        NULL, so `<> ALL` has no three-valued-logic trap.
        """
        conjuncts: list[str] = []
        params: dict[str, Any] = {}
        if self._workflows:
            workflow_clause, workflow_params = runnable_filter_sql()
            conjuncts.append(workflow_clause)
            params.update(workflow_params)
        if self._blocked_paths:
            conjuncts.append(f' AND "{self._path_column}" <> ALL(%(ox_blocked_paths)s)')
            params["ox_blocked_paths"] = sorted(self._blocked_paths)
        if self._blocked_queues:
            conjuncts.append(
                f' AND "{self._queue_column}" <> ALL(%(ox_blocked_queues)s)'
            )
            params["ox_blocked_queues"] = sorted(self._blocked_queues)
        return "".join(conjuncts), params

    # -- the node flag the claim carries ----------------------------------

    def _ready_queryset(self) -> QuerySet[OxTask]:
        """
        django-ox's candidates, each flagged with whether it is a workflow node.

        Only where the claim is the SKIP LOCKED path, which returns the
        candidate it read. The flag is a plain subquery in the select list, so
        the locking read takes no lock on a node row. On MySQL it is NULL for a
        row committed after the claim's read view opened, and the outcome looks
        the node up (node_flag_for_a_locking_read).
        """
        queryset = super()._ready_queryset()
        connection = connections[self._db_alias]
        if self._recording() and connection.features.has_select_for_update_skip_locked:
            queryset = queryset.annotate(
                **{NODE_FLAG: node_flag_for_a_locking_read(connection.vendor)}
            )
        return queryset

    def _claim_one_postgresql(self, run_after_cutoff: datetime) -> OxTask | None:
        """
        django-ox's single-statement PostgreSQL claim with the workflow node
        flag added to its RETURNING list.

        This copies the base worker's method and adds the node flag after
        RETURNING *. Tests compare the task row, returned object, lease
        fence and recorded outcome with the base worker's behaviour so a
        change to the original claim must also be reflected here.
        """
        if not self._recording():
            return super()._claim_one_postgresql(run_after_cutoff)
        queue_clause = 'AND "queue_name" = ANY(%(queues)s)' if self.queues else ""
        extra_clause, extra_params = self.claim_filter_sql()
        lease_clock = "STATEMENT_TIMESTAMP()" if settings.USE_TZ else "%(lease_now)s"
        sql = _with_node_flag(
            POSTGRES_CLAIM_SQL.format(
                table=OxTask._meta.db_table,
                queue_clause=queue_clause,
                extra_clause=extra_clause,
                lease_clock=lease_clock,
            )
        )
        rows = OxTask.objects.db_manager(self._db_alias).raw(
            sql,
            {
                "running": OxTask.Status.RUNNING,
                "ready": OxTask.Status.READY,
                "worker_id": self.worker_id,
                "worker_id_json": json.dumps([self.worker_id]),
                "now": run_after_cutoff,
                "lease_now": _lease_now() if not settings.USE_TZ else None,
                "lease_ttl": timedelta(seconds=self.lock_timeout),
                "queues": self.queues,
                **extra_params,
            },
        )
        return next(iter(rows), None)

    def _reload_claimed(self, pk: uuid.UUID, granted_epoch: int) -> OxTask | None:
        """
        django-ox's re-read of a row the compare-and-set claim took, pinned to
        the epoch it was granted, with the node flag.
        """
        if not self._recording():
            return super()._reload_claimed(pk, granted_epoch)
        return (
            OxTask.objects.using(self._db_alias)
            .filter(pk=pk, lease_epoch=granted_epoch)
            .annotate(**{NODE_FLAG: node_flag()})
            .first()
        )

    # -- recording a node's outcome ----------------------------------------

    def _write_outcome(
        self,
        db_task: OxTask,
        *,
        status: OxTask.Status,
        duration_ms: int,
        **fields: Any,
    ) -> bool:
        """
        django-ox's outcome write, and for a workflow node, the node's record
        and the marks on the edges out of it, in the same transaction.

        This is the hook and not a task_finished receiver for the reason
        recorded all over batches.py: the reaper settles a dead worker's task
        through a queryset update that emits no signal. It is also the only
        place in django-ox that writes SUCCESSFUL.

        Only SUCCESSFUL and FAILED take this path, and only for a row the claim
        flagged as a node or could not flag. A retry write, and a write for a
        row flagged as no node, is django-ox's write alone. For a row claimed
        elsewhere, which carries no flag, and for a row MySQL's claim read from
        outside its read view, whose flag is None, the node lookup inside the
        transaction decides.

        The transaction writes the task row first, through django-ox and its
        lease fence. Then it locks the node row and records the outcome at the
        epoch the write leaves on the row. On SUCCESSFUL it then locks the
        edges out of the task in key order and marks them satisfied. The
        outcome, the record and the marks commit together or not at all. The
        nodes those edges point at are released after the commit, never inside
        it. A release inside would lock their task rows while this one is held.

        When the call owns its transaction, meaning autocommit was on and no
        atomic block was open when it was called, each try waits at most
        OUTCOME_LOCK_TIMEOUT seconds for a lock. Whether it owns the transaction
        is decided before the first try, never from what an error leaves. A try
        that loses a lock conflict puts the row instance back as it was and runs
        the whole transaction again, fence included. It tries up to
        OUTCOME_ATTEMPTS times, and not once less than a renewal interval of the
        lease is left. Putting the instance back is what keeps a stuck-thread
        handover on the epoch it started from. When the tries run out, or
        another database error arrives, the write fails closed. It is logged as
        workflow_outcome_unrecorded and returns False, so no task_finished is
        sent, and the reaper takes the row back as it does after a lost lease.
        Django's InterfaceError, which is not a DatabaseError, fails closed the
        same way.

        One error is raised instead: the one a lost connection leaves,
        _outcome_connection_gone. The instance is put back first. django-ox's
        _write_outcome_reconnecting then closes the connection and calls this
        method once more on another, after asking whether the row already holds
        this write. The task row, the node's record and the marks commit
        together, so a row that holds it is a transaction that committed whole
        before the connection went. That path runs no release of its own: the
        edges are marked, and the nodes they held stay WAITING until the
        reconciler's backstop releases them. A second try that fails as well is
        django-ox's task_outcome_unrecorded, and the row is the reaper's. The
        stuck-thread handover calls this method directly, with no second try:
        the error reaches the watchdog, which logs task_stuck_unrecorded, and
        the row is the reaper's there too.

        Inside a caller's transaction nothing is bounded or tried again, and
        errors from the outcome and the record propagate. The release still
        runs after the commit, the caller's, through transaction.on_commit on
        the database the record was written to. A caller that rolls back
        releases nothing, and a release error after the commit is logged and
        cannot reach the caller's transaction. Running a workflow node inside a
        caller's transaction is not supported. It holds every lock above until
        the caller commits.
        """
        flag = getattr(db_task, NODE_FLAG, None)
        if not (
            self._recording()
            and status in RECORDED_BY_THE_WORKER
            and (flag is None or flag)
        ):
            return super()._write_outcome(
                db_task, status=status, duration_ms=duration_ms, **fields
            )
        epoch = int(fields.get("lease_epoch", db_task.lease_epoch))
        alias = self._rate_alias
        connection = connections[alias]
        owned = connection.get_autocommit() and not connection.in_atomic_block
        if not owned:
            with transaction.atomic(using=alias):
                if not super()._write_outcome(
                    db_task, status=status, duration_ms=duration_ms, **fields
                ):
                    return False
                downstream = record_node_outcome(db_task.pk, status, epoch, alias)
                if downstream:
                    held = downstream
                    transaction.on_commit(
                        lambda: self._release_after_outcome(db_task, held),
                        using=alias,
                        robust=True,
                    )
            return True

        saved = {
            "status": db_task.status,
            **{name: getattr(db_task, name) for name in fields},
        }
        # The driver connection the tries start on, which get_autocommit()
        # opened if none was. _outcome_connection_gone compares against it.
        driver = connection.connection
        started = time.monotonic()
        attempt = 0
        downstream = None
        try:
            with session_timeouts(alias, lock_wait=OUTCOME_LOCK_TIMEOUT):
                while True:
                    attempt += 1
                    try:
                        with transaction.atomic(using=alias):
                            transaction_timeouts(alias, lock_wait=OUTCOME_LOCK_TIMEOUT)
                            if not super()._write_outcome(
                                db_task,
                                status=status,
                                duration_ms=duration_ms,
                                **fields,
                            ):
                                return False
                            downstream = record_node_outcome(
                                db_task.pk, status, epoch, alias
                            )
                        break
                    except BaseException as exc:
                        _put_back(db_task, saved)
                        if not (
                            lost_lock_conflict(exc, alias)
                            and attempt < OUTCOME_ATTEMPTS
                            and self._lease_allows_a_retry(started)
                        ):
                            raise
                        logger.info(
                            "Task id=%s: recording its workflow outcome lost a lock "
                            "conflict on try %d of %d. Trying again",
                            db_task.id,
                            attempt,
                            OUTCOME_ATTEMPTS,
                            extra=self._log_extra(
                                "workflow_outcome_retry", db_task, try_number=attempt
                            ),
                        )
                        time.sleep(random.uniform(0, ADD_RETRY_JITTER))  # noqa: S311
        except Error as exc:
            _put_back(db_task, saved)
            if self._outcome_connection_gone(exc, alias, driver):
                raise
            logger.error(
                "Could not record the %s outcome of workflow node task id=%s after "
                "%d tries. The task row is unchanged, and the reaper will take it "
                "back",
                status,
                db_task.id,
                attempt,
                exc_info=True,
                extra=self._log_extra(
                    "workflow_outcome_unrecorded",
                    db_task,
                    dropped_status=str(status),
                    tries=attempt,
                ),
            )
            return False
        if downstream is not None and status == OxTask.Status.SUCCESSFUL:
            self._release_after_outcome(db_task, downstream)
        return True

    def _recording(self) -> bool:
        """
        Whether this worker flags claimed nodes and records their outcomes now.

        Only where a backend on the database declares WORKFLOWS. A worker whose
        own backend does not also waits for the workflow node table, because
        before 0004_workflows is applied no row can be a node and the flag has
        no table to read. It looks for the table at most every
        NODE_TABLE_RECHECK seconds until it is there, and then stops looking.
        """
        if not self._records or self._node_table_seen:
            return self._records
        now = time.monotonic()
        looked_at = self._node_table_looked_at
        if looked_at is not None and now - looked_at < NODE_TABLE_RECHECK:
            return False
        self._node_table_looked_at = now
        tables = connections[self._rate_alias].introspection.table_names()
        self._node_table_seen = OxpullWorkflowNode._meta.db_table in tables
        return self._node_table_seen

    def _outcome_connection_gone(self, exc: Error, alias: str, driver: Any) -> bool:
        """
        Whether a node outcome's own transaction failed because its connection
        was lost, so that the error goes to django-ox's
        _write_outcome_reconnecting, which closes the connection and writes
        once more on another.

        Only an InterfaceError or an OperationalError, only on the database
        django-ox's check and close act on, and only when that check,
        _outcome_connection_lost, says the connection is gone. A lock timeout,
        a deadlock whose tries ran out and every other error leave a connection
        that answers, and the write fails closed as before.

        Django drops a connection whose rollback fails, which is what a
        transaction on a lost connection ends in, and then opens another to
        turn autocommit back on. That one never saw the transaction, and it
        answers, so the check would call the lost connection alive. It is
        closed first, when the driver connection is no longer the one the
        tries started on, and the check then sees what happened: no connection
        open. Nothing is lost by that: it holds no transaction, and the next
        statement opens one. A connection that could not be reopened is not
        open either.
        """
        if not isinstance(exc, (InterfaceError, OperationalError)):
            return False
        if alias != self._db_alias:
            return False
        connection = connections[alias]
        if connection.connection is not None and connection.connection is not driver:
            with suppress(Error):
                connection.close()
        return self._outcome_connection_lost()

    def _lease_allows_a_retry(self, started: float) -> bool:
        """
        Whether another try still leaves the lease more than a renewal interval.

        The lease was renewed at most one interval before the first try, and a
        try that holds the task row stalls its renewal.
        """
        spent = time.monotonic() - started
        return spent < self.lock_timeout - 2 * self.renew_interval

    def _release_after_outcome(
        self, db_task: OxTask, downstream: list[uuid.UUID]
    ) -> None:
        """
        Release the nodes a recorded success was holding, after the commit that
        recorded it.

        A failure is logged and swallowed. It costs liveness, not correctness:
        the edges are marked, and the nodes stay WAITING for the reconciler.
        Raising here would lose the outcome of a task that ran.

        When commit callbacks run inside a transaction that is still open, as
        django.test's captureOnCommitCallbacks runs them, the release takes a
        savepoint, so its error does not end that transaction.
        """
        alias = self._rate_alias
        inside = connections[alias].in_atomic_block
        try:
            with transaction.atomic(using=alias) if inside else nullcontext():
                released = release_downstream(downstream, alias)
        except Error:
            logger.warning(
                "Could not release the dependents of task id=%s. The reconciler "
                "will release them",
                db_task.id,
                exc_info=True,
                extra={
                    "event": "workflow_release_failed",
                    "task_id": str(db_task.id),
                    "worker_id": self.worker_id,
                },
            )
            return
        if released:
            logger.debug(
                "Task id=%s released %d workflow node(s)",
                db_task.id,
                released,
                extra={
                    "event": "workflow_nodes_released",
                    "task_id": str(db_task.id),
                    "released_count": released,
                    "worker_id": self.worker_id,
                },
            )

    # -- the budget --------------------------------------------------------

    def _refresh_view(self) -> None:
        if not self._view_stale and time.monotonic() < self._refresh_after:
            return
        self._read_windows()

    def _read_windows(self) -> None:
        """
        One SELECT over every configured limit, then a roll attempt for
        each window the database's own clock says has expired.
        """
        started = time.monotonic()
        if self._closed:
            self._reopen_what_counts_again()
        rows = self._select()
        missing = [name for name in self._limits if name not in rows]
        if missing:
            for name in missing:
                self._seed(name)
            rows = self._select()
            for name in missing:
                if name not in rows:
                    rows[name] = self._adopt(name)
        seen: dict[str, _Seen] = {}
        blocked: set[str] = set()
        for name, config in self._limits.items():
            row = rows[name]
            db_now = row.db_now
            window_id, used = row.window_id, row.used
            ends_at = row.window_started_at + timedelta(seconds=config.per)
            if ends_at <= db_now and self._roll(name, window_id, config.per):
                # This worker opened the window, so the whole capacity is
                # free. The roll takes no token: it runs before any claim,
                # which is what makes it free to lose.
                window_id, used = window_id + 1, 0
                ends_at = db_now + timedelta(seconds=config.per)
            seen[name] = _Seen(
                window_id=window_id,
                used=used,
                ends_at=started + max(0.0, (ends_at - db_now).total_seconds()),
            )
            if used >= config.limit or name in self._closed:
                blocked.add(name)
        self._seen = seen
        self._blocked_paths = frozenset(
            path for name in blocked for path in self._limits[name].task_paths
        )
        self._blocked_queues = frozenset(
            queue for name in blocked for queue in self._limits[name].queues
        )
        self._view_stale = False
        self._refresh_after = started + self._refresh_interval
        if blocked and len(blocked) == len(self._limits) and not self._closed:
            # Nothing this worker can claim is admissible until the
            # earliest of these windows ends, and a window's end is known
            # exactly. That is the payoff for a window over a bucket,
            # whose exhaustion has no knowable expiry. A limit that is
            # closed is the exception: it reopens when the database takes
            # a write again, which no window's end predicts, so the
            # ordinary cooldown paces the probe instead.
            self._refresh_after = max(
                self._refresh_after, min(seen[name].ends_at for name in blocked)
            )

    def _select(self) -> dict[str, Any]:
        # Now() rides along in the same statement so the window's age is
        # measured entirely on the database's clock. Comparing a stored
        # window against timezone.now() would be wrong under USE_TZ=False,
        # where SQLite writes UTC into a column every other writer fills
        # with naive local time.
        #
        # Keyed by the configured name, which is the stored spelling except
        # for a row _adopt has attributed to a limit spelled differently.
        rows = (
            OxpullRateWindow.objects.using(self._rate_alias)
            .filter(limit_name__in=self._limits)
            .annotate(db_now=Now())
        )
        return {self._folded.get(row.limit_name, row.limit_name): row for row in rows}

    def _seed(self, name: str) -> None:
        """
        Open the first window for a limit no worker has used yet.

        The loser of the race takes the winner's row, the same pattern as
        the uniqueness lock and OxScheduleTick. The savepoint keeps a
        caller's transaction usable after the conflict.
        """
        try:
            with transaction.atomic(using=self._rate_alias):
                OxpullRateWindow.objects.using(self._rate_alias).create(
                    limit_name=name, window_started_at=Now()
                )
        except IntegrityError:
            pass

    def _adopt(self, name: str) -> Any:
        """
        Read the row the database holds for a limit under another spelling.

        _seed found a row in its way and _select did not return one under
        this name, so the column's collation treats some other spelling
        as this name: a limit deployed as "Stripe" and renamed to "stripe"
        on MySQL. Only the database knows what its collation folds, so ask
        it with the same predicate every write to the row uses. From here
        on _select reads that row as this limit, and _roll and _count
        already land on it by the same collation, so the count continues
        across the rename rather than the worker dying on it.
        """
        row = (
            OxpullRateWindow.objects.using(self._rate_alias)
            .filter(limit_name=name)
            .annotate(db_now=Now())
            .first()
        )
        if row is None:
            raise ImproperlyConfigured(
                f"Rate limit {name!r}: the window table refused a row for it "
                "and holds none that matches it."
            )
        # The row another configured limit already owns is not this
        # limit's to adopt. The parser refuses two names that differ only
        # by case, which every case-insensitive collation folds; what
        # else a collation folds (MySQL's utf8mb4_0900_ai_ci makes one
        # name of `Café` and `cafe`) only the database can say, and this
        # is where it says it. Two limits on one row would count each
        # other's admissions and throttle traffic neither was configured
        # for, so the worker refuses here, before its first claim. A
        # single limit renamed to a spelling the collation folds is the
        # case above, and it stays a rename.
        other = self._folded.get(row.limit_name)
        if other is None and row.limit_name in self._limits:
            other = row.limit_name
        if other is not None and other != name:
            raise ImproperlyConfigured(
                f"RATE_LIMITS has {other!r} and {name!r}, and the database "
                f"holds one window row ({row.limit_name!r}) for both: the "
                "column's collation treats the two names as one. Two limits "
                "on one row would share a window and throttle each other. "
                "Rename one."
            )
        self._folded[row.limit_name] = name
        logger.info(
            "Rate limit %r: its window row is stored as %r; the column's "
            "collation treats the two as one name, so that row is the limit's",
            name,
            row.limit_name,
            extra={
                "event": "rate_limit_row_adopted",
                "limit": name,
                "stored_as": row.limit_name,
                "worker_id": self.worker_id,
            },
        )
        return row

    def _roll(self, name: str, window_id: int, period: int) -> bool:
        """
        Open the next window, if this worker is the one that gets to.

        window_id is the compare-and-set: the winner's write moves it, so
        every loser's predicate stops matching and their rowcount is 0.
        The period comparison is evaluated on the database clock on both
        sides, so a worker whose clock is wrong cannot roll a live window.
        """
        return bool(
            OxpullRateWindow.objects.using(self._rate_alias)
            .filter(
                limit_name=name,
                window_id=window_id,
                window_started_at__lte=Now() - timedelta(seconds=period),
            )
            .update(window_id=window_id + 1, used=0, window_started_at=Now())
        )

    def _reopen_what_counts_again(self) -> None:
        """
        Probe each closed limit, and reopen the ones that answer.

        A closed limit admits nothing, so no claim will run another count
        and no count will succeed on its own: the only way to learn that
        the counter can be written again is to write one. The probe is
        the statement that failed with no increment in it, so a limit
        that reopens has proved the exact write it is about to need.
        """
        for name in sorted(self._closed):
            try:
                OxpullRateWindow.objects.using(self._rate_alias).filter(
                    limit_name=name
                ).update(used=F("used"))
            except DatabaseError:
                continue
            self._closed.discard(name)
            self._count_failures.pop(name, None)
            logger.warning(
                "Rate limit %r: its counter takes writes again; admitting again",
                name,
                extra={
                    "event": "rate_limit_reopened",
                    "limit": name,
                    "worker_id": self.worker_id,
                },
            )

    def _record(self, db_task: OxTask) -> None:
        counted = False
        for name, config in self._limits.items():
            if config.covers(db_task.task_path, db_task.queue_name):
                self._count(name, db_task)
                counted = True
        if counted:
            # Refresh before the next limited claim. Drop this and a
            # worker that saw an empty window could admit the whole
            # capacity on its own before looking again, and the bound
            # would degrade from N + W - 1 to N x W.
            self._view_stale = True

    def _count(self, name: str, db_task: OxTask) -> None:
        """
        Count one admission. Unconditional, and never a gate.

        The task is already claimed by the time this runs, so a
        compare-and-set that could refuse would have burned the attempt
        this whole design exists to protect. Two workers taking the last
        token both get it; what stops either taking a second is that both
        read the post-increment count at their next refresh.

        A statement that raises is the one case that does gate, at the
        next refresh rather than here: see _count_failed.
        """
        for _ in range(MAX_RATE_ATTEMPTS):
            seen = self._seen.get(name)
            if seen is None:
                return
            try:
                counted = (
                    OxpullRateWindow.objects.using(self._rate_alias)
                    .filter(limit_name=name, window_id=seen.window_id)
                    .update(used=F("used") + 1)
                )
            except DatabaseError:
                self._count_failed(name, db_task)
                return
            if counted:
                # The admission is on the counter, so any earlier run of
                # unrecorded admissions is over. Only a matched row says
                # so: a statement that ran and matched nothing has
                # recorded no more than one that raised.
                self._count_failures.pop(name, None)
                return
            # The window rolled between the refresh and here, so the
            # increment landed on nothing. Re-read and count against the
            # window that is open now. The re-read is accounting too:
            # the claim is committed, so a database that goes away here
            # costs the count, not the task. Left uncaught, the error
            # would escape claim_one() with the task claimed and
            # unreturned, and the row would sit RUNNING until the reaper
            # took it back, one attempt poorer. ImproperlyConfigured is
            # the re-read finding the limit's row gone and the seed
            # refused (_adopt); before the claim that propagates, here it
            # is the same loss of a count and is logged the same way.
            try:
                self._read_windows()
            except (DatabaseError, ImproperlyConfigured):
                self._count_failed(name, db_task)
                return
        logger.warning(
            "Rate limit %r: window rolled under %d attempts to count task "
            "id=%s; it runs uncounted",
            name,
            MAX_RATE_ATTEMPTS,
            db_task.id,
            extra={
                "event": "rate_limit_uncounted",
                "limit": name,
                "task_id": str(db_task.id),
                "worker_id": self.worker_id,
            },
        )
        # Rolled out is an admission that was not recorded, the same as
        # one whose increment raised, and it counts toward the run.
        self._unrecorded(name)

    def _count_failed(self, name: str, db_task: OxTask) -> None:
        """
        Record one failed count, and close the limit on a run of them.

        A single failure is fail-open on purpose: the claim is committed
        and the attempt is spent, so refusing to run the task would lose
        work to save a counter. A run of MAX_COUNT_FAILURES is a
        different thing. The counter is not advancing, every worker's
        next refresh reads a used that never moves, and the limiter
        admits at full speed for as long as the database stays that way.
        Closing the limit fails the other way: throttled work waits,
        which is what a limit is for.
        """
        logger.warning(
            "Rate limit %r: could not count task id=%s; it runs uncounted",
            name,
            db_task.id,
            exc_info=True,
            extra={
                "event": "rate_limit_uncounted",
                "limit": name,
                "task_id": str(db_task.id),
                "worker_id": self.worker_id,
            },
        )
        self._unrecorded(name)

    def _unrecorded(self, name: str) -> None:
        """
        One more admission under this limit went unrecorded; close the
        limit when that makes MAX_COUNT_FAILURES in a row. The run ends
        only when an increment lands (_count), so it is a run of
        admissions the counter does not know about, whichever way each
        one was lost.
        """
        failures = self._count_failures.get(name, 0) + 1
        self._count_failures[name] = failures
        if failures < MAX_COUNT_FAILURES or name in self._closed:
            return
        # Once, on the transition. Every later claim is excluded before
        # it reaches the count, so there is nothing per-claim to log.
        self._closed.add(name)
        logger.warning(
            "Rate limit %r: %d counts failed in a row; admitting nothing "
            "under it until its counter takes a write again",
            name,
            failures,
            extra={
                "event": "rate_limit_closed",
                "limit": name,
                "failures": failures,
                "worker_id": self.worker_id,
            },
        )


def _put_back(db_task: OxTask, saved: dict[str, Any]) -> None:
    """Restore the instance fields a failed outcome try mirrored onto it."""
    for name, value in saved.items():
        setattr(db_task, name, value)


def _with_node_flag(sql: str) -> str:
    """The formatted PostgreSQL claim, with the node flag after RETURNING *."""
    head, returning, tail = sql.rpartition("RETURNING *")
    if not returning or tail.strip():
        raise ImproperlyConfigured(
            "django-ox's PostgreSQL claim must end in RETURNING * so "
            "OxpullWorker can append the workflow node flag. Update "
            "OxpullWorker's claim adapter to match the base worker's SQL."
        )
    return f"{head}{returning}{node_flag_sql()}{tail}"
