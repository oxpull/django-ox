"""
The drain behind django_ox.testing.run_tasks(). Not public API.

It claims with the configured worker class's own claim_one() and runs each
task through its own execute(inline=True), on the caller's thread and
database connections, so a TestCase's uncommitted rows are both what gets
claimed and what the task sees. Nothing else of the worker runs: no poll
loop, no pool, no lease renewal, no watchdog, no reaper, no schedule
dispatch, and no connection cleanup of its own.

A subclass of the configured class, made for each call, changes two things.
The timeout call runs the body directly, so no timeout is enforced; see
_DrainWorker. And the body seam, Worker._task_body, gives the body a
savepoint on every connection the caller holds inside an atomic block and
runs the commit callbacks the attempt registered there, each in a savepoint
of its own, standing in for the commit a worker's autocommit connection
makes; see _Body and _Attempt.

That stand-in is commit emulation, not a commit. Other connections see
nothing, a callback runs when its attempt's body returns rather than at the
moment its own transaction would have committed, and callbacks on different
databases run one database after another.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Literal, cast

from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError, Error, connections, transaction
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.transaction import Atomic, TransactionManagementError

from .backend import OxBackend
from .compat import Task, TaskContext, TaskResult, task_backends
from .models import OxTask
from .results import task_result_from_db
from .worker import Worker, _executing, worker_class

logger = logging.getLogger("django_ox")

#: The most attempts a call makes when it is given no max_tasks.
SAFETY_LIMIT = 1000

# True for the length of a run_tasks() call, outside the attempts as well as
# inside them, so a commit callback or a signal receiver that the drain runs
# between attempts cannot start a second drain. _executing covers the
# attempts themselves, whoever runs them.
_draining: ContextVar[bool] = ContextVar("django_ox_draining", default=False)

# One entry of a connection's run_on_commit: the savepoint ids it was
# registered under, the callback, and whether it is robust.
_Hook = tuple[set[str], Callable[[], object], bool]


def _hooks(conn: BaseDatabaseWrapper) -> list[_Hook]:
    # django-stubs types the entries as pairs; Django has stored the robust
    # flag as a third item since 4.2.
    return cast("list[_Hook]", conn.run_on_commit)


def _broken(conn: BaseDatabaseWrapper) -> bool:
    return conn.in_atomic_block and (conn.needs_rollback or conn.closed_in_transaction)


def _savepoint(conn: BaseDatabaseWrapper) -> Atomic:
    """
    An atomic block for `conn`, which is inside one already, so entering it
    opens a savepoint.

    Django refuses a durable block directly inside one that is not a
    TestCase's own. The savepoint takes the flag of the block it sits in, so
    a durable block inside it is refused exactly where it would be without
    it.
    """
    block = transaction.atomic(using=conn.alias)
    parent = conn.atomic_blocks[-1] if conn.atomic_blocks else None
    cast("Any", block)._from_testcase = getattr(parent, "_from_testcase", False)
    return block


def _name(func: Callable[[], object]) -> object:
    return getattr(func, "__qualname__", func)


class _Attempt:
    """
    One attempt's share of the connections, from the claim to its results.

    `connections` are the ones the caller held inside an atomic block when
    the attempt began; autocommit connections keep Django's own behaviour,
    which runs a callback at once. The callbacks pending on each of them
    then are the caller's, and they stay the first entries of its
    run_on_commit while the attempt runs: on_commit() appends, and a
    savepoint rolled back inside the attempt removes only the callbacks
    registered under it, keeping the others in order. So the attempt's own
    callbacks are the entries after them, and the caller's are never run,
    moved or dropped here.

    Before running or removing any callback, the first entries are checked
    against the callback functions recorded when the attempt began, one by
    one and in order. If something other than on_commit() and a savepoint
    rollback changed them, the attempt's callbacks can no longer be told
    from the caller's: nothing is run or removed there, and RuntimeError
    says so.
    """

    def __init__(self, worker: Worker, db_task: OxTask) -> None:
        self.worker = worker
        self.db_task = db_task
        self.connections = [
            conn
            for conn in connections.all(initialized_only=True)
            if conn.in_atomic_block
        ]
        self._callers = {
            conn.alias: [func for _sids, func, _robust in _hooks(conn)]
            for conn in self.connections
        }
        #: What the attempt was recorded as failing with, when the seam saw it.
        self.failure: BaseException | None = None
        #: An error setting up the seam, raised to the caller after execute().
        self.setup_error: BaseException | None = None
        #: The RuntimeError that found the caller's callbacks changed, raised
        #: to the caller; nothing is run or removed after it.
        self.moved: RuntimeError | None = None

    def _live(self) -> list[BaseDatabaseWrapper]:
        """
        The connections whose transaction can go on. A broken one runs
        nothing, and the rollback it is waiting for drops what the attempt
        registered there.
        """
        return [conn for conn in self.connections if not _broken(conn)]

    def _check(self, conns: list[BaseDatabaseWrapper]) -> None:
        """Raise RuntimeError unless the caller's callbacks are where they were."""
        if self.moved is not None:
            raise self.moved
        for conn in conns:
            callers = self._callers[conn.alias]
            hooks = _hooks(conn)
            if len(hooks) >= len(callers) and all(
                hook[1] is func for hook, func in zip(hooks, callers, strict=False)
            ):
                continue
            self.moved = RuntimeError(
                f"The transaction.on_commit() callbacks pending on database "
                f"{conn.alias!r} before task {self.db_task.task_path} ran are "
                f"no longer the first {len(callers)} there, in the order they "
                "were registered, so run_tasks() cannot tell the task's "
                "callbacks from the caller's and neither runs nor removes any "
                "of them. Something other than on_commit() and a savepoint "
                "rollback changed the connection's run_on_commit list while "
                "the task ran."
            )
            raise self.moved

    def _take(self, conn: BaseDatabaseWrapper) -> list[_Hook]:
        """Remove and return this attempt's pending callbacks on `conn`."""
        hooks = _hooks(conn)
        count = len(self._callers[conn.alias])
        mine = hooks[count:]
        del hooks[count:]
        return mine

    def discard(self) -> None:
        """Drop whatever this attempt registered and nothing ran."""
        if self.moved is not None:
            return
        live = self._live()
        self._check(live)
        for conn in live:
            self._take(conn)

    def commit(self) -> tuple[Callable[[], object], Exception] | None:
        """
        Run the callbacks this attempt has registered and still holds, as a
        commit would: in registration order, a database at a time, each
        taken off run_on_commit before it runs, and the ones they register
        after them. A callback on a savepoint that was rolled back is no
        longer there to run.

        A robust callback's exception is logged and the rest go on. A
        non-robust one is returned with its exception, and every callback
        still waiting is dropped, as Django's own commit drops them. A
        connection whose transaction is broken runs nothing; the attempt's
        outcome write will say so.
        """
        while True:
            live = self._live()
            self._check(live)
            pending = [hook for conn in live for hook in self._take(conn)]
            if not pending:
                return None
            for _sids, func, robust in pending:
                error = self._call(func)
                if error is None:
                    continue
                if not robust:
                    self.discard()
                    return func, error
                self.report_callback_error(func, error, robust=True)

    def _call(self, func: Callable[[], object]) -> Exception | None:
        """
        Run one callback in a savepoint of its own on every connection whose
        transaction can go on, and return the exception it raised, or None.

        On a worker the callback runs after a commit, in autocommit, where a
        failed statement cannot break anything else. Here the savepoints are
        rolled back before its exception is returned, so the error is
        handled, logged or recorded outside them and the caller's
        transaction goes on, whatever the error was. What the callback wrote
        goes with them. A callback that returns with a transaction marked
        for rollback, by a database error it caught or by set_rollback(True),
        is rolled back there too, and a TransactionManagementError saying so
        is returned in place of success. KeyboardInterrupt and SystemExit
        roll the savepoints back and go on to the caller.
        """
        live = self._live()
        marked: BaseDatabaseWrapper | None = None
        try:
            with ExitStack() as stack:
                for conn in live:
                    stack.enter_context(_savepoint(conn))
                func()
                marked = next(
                    (
                        conn
                        for conn in live
                        if conn.needs_rollback and not conn.closed_in_transaction
                    ),
                    None,
                )
        except Exception as exc:
            return exc
        if marked is None:
            return None
        return TransactionManagementError(
            f"A commit callback of task {self.db_task.task_path}, "
            f"{_name(func)}, left the transaction on database {marked.alias!r} "
            "marked for rollback, by a database error it caught or by "
            "set_rollback(True), so run_tasks() rolled back what it wrote "
            "there. A worker runs the callback in autocommit and would have "
            "kept those writes; a statement whose error the callback catches "
            "belongs in its own transaction.atomic() block."
        )

    def report_callback_error(
        self, func: Callable[[], object], exc: Exception, *, robust: bool
    ) -> None:
        why = (
            "a robust callback's error does not fail the attempt"
            if robust
            else "the attempt had already failed, and its own error is the one recorded"
        )
        logger.error(
            "Error calling %s in on_commit() (%s), a commit callback task id=%s "
            "path=%s registered; run_tasks() ran it in place of a commit, and %s",
            _name(func),
            exc,
            self.db_task.id,
            self.db_task.task_path,
            why,
            exc_info=exc,
            extra=self.worker._log_extra("run_tasks_callback_failed", self.db_task),
        )


class _Body:
    """
    The body seam for one attempt: a savepoint on each of the attempt's
    connections, and its commit callbacks when the body is done.

    What the body wrote is kept whenever its connection can go on, whether
    it returned or raised, because a worker's autocommit connection keeps
    it: a retry then sees what the failed attempt left. When a database
    error has marked the transaction for rollback, or releasing the
    savepoint fails (PostgreSQL refuses every statement after an error in a
    transaction, raw SQL included), Django rolls back to the savepoint and
    the attempt's writes on that database are gone; the failure is then
    recorded outside the savepoint, where the write can land. A body that
    returned but left its transaction unable to go on is recorded as a
    failure, never as a success whose writes were thrown away: with the
    error the release met, or with a TransactionManagementError that says
    the transaction was marked for rollback. A rollback that itself fails
    leaves the transaction marked for rollback, as Django leaves it, and
    the outcome write raises.

    KeyboardInterrupt and SystemExit reach the caller of run_tasks(), as
    they do from run_once(), and nothing is recorded. Raised by the body,
    they roll the savepoints back and no callback runs. Raised by one of
    the attempt's commit callbacks, they come after the savepoints were
    released: the body's writes are kept, only that callback's own
    savepoint is rolled back, and the callbacks still waiting are dropped.
    """

    def __init__(self, attempt: _Attempt) -> None:
        self.attempt = attempt
        self.blocks: list[tuple[BaseDatabaseWrapper, Atomic]] = []

    def __enter__(self) -> None:
        try:
            for conn in self.attempt.connections:
                block = _savepoint(conn)
                block.__enter__()
                self.blocks.append((conn, block))
        except BaseException as exc:
            # The worker records whatever this raises as the attempt's
            # failure, being inside its attempt; run_tasks() raises it to
            # its caller after that.
            self.attempt.setup_error = exc
            for _conn, entered in reversed(self.blocks):
                entered.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            for _conn, block in reversed(self.blocks):
                block.__exit__(exc_type, exc, tb)
            return False
        release_error, lost = self._release()
        failure = exc if exc is not None else release_error
        if not lost and not any(_broken(conn) for conn in self.attempt.connections):
            # A RuntimeError from here, saying the caller's callbacks were
            # changed, is recorded as the attempt's failure by the worker and
            # raised by run_tasks() after that.
            failed = self.attempt.commit()
            if failed is not None:
                func, callback_error = failed
                if failure is None:
                    failure = callback_error
                else:
                    self.attempt.report_callback_error(
                        func, callback_error, robust=False
                    )
        self.attempt.failure = failure
        if failure is None or failure is exc:
            # The body's own exception, if any, goes on as it was raised.
            return False
        raise failure

    def _release(self) -> tuple[Error | None, bool]:
        """
        Leave every savepoint as a body that returned would, which releases
        it unless the transaction needs a rollback. Returns the first error
        this met, and whether an error that was not a DatabaseError says the
        connection itself is gone.

        The error is what a release raised, after Django rolled back to that
        savepoint, or, where the transaction was already marked for rollback
        and Django rolled back without a word, a TransactionManagementError
        saying so.
        """
        first: Error | None = None
        lost = False
        for conn, block in reversed(self.blocks):
            marked = conn.needs_rollback and not conn.closed_in_transaction
            try:
                block.__exit__(None, None, None)
            except Error as error:
                if first is None:
                    first = error
                if not isinstance(error, DatabaseError):
                    lost = True
                continue
            if marked and first is None:
                first = TransactionManagementError(
                    f"Task {self.attempt.db_task.task_path} left the transaction "
                    f"on database {conn.alias!r} marked for rollback, by a "
                    "database error it caught or by set_rollback(True), so "
                    "run_tasks() rolled back what it wrote there. A worker runs "
                    "the task in autocommit and would have kept those writes; "
                    "a statement whose error the task catches belongs in its "
                    "own transaction.atomic() block."
                )
        return first, lost


class _DrainWorker(Worker):
    """
    What run_tasks() changes in the configured worker class, which it
    subclasses for each call with this first in the method order: the
    timeout call and the body seam. Claims, execution, retries, backoff,
    signals and outcome writes are the configured class's own.
    """

    #: The attempt being run, set by drain() around each execute().
    _attempt: _Attempt | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        #: Task paths the timeout warning has been logged for in this call.
        self._inert_timeouts: set[str] = set()

    def _task_body(self, db_task: OxTask) -> AbstractContextManager[None]:
        attempt = self._attempt
        if attempt is None or attempt.db_task is not db_task:
            return super()._task_body(db_task)
        return _Body(attempt)

    def _call_task(
        self,
        task: Task[..., Any],
        db_task: OxTask,
        task_result: TaskResult[..., Any],
        timeout: float,
    ) -> Any:
        """
        The body, called the way an attempt with no timeout calls it. No
        watchdog is armed and no deadline is published, so deadline() and
        remaining() answer None inside it, and it runs as long as it runs.
        """
        self._report_inert_timeout(db_task, timeout)
        if task.takes_context:
            return task.call(
                TaskContext(task_result=task_result),
                *db_task.args,
                **db_task.kwargs,
            )
        return task.call(*db_task.args, **db_task.kwargs)

    def _report_inert_timeout(self, db_task: OxTask, timeout: float) -> None:
        """Log the timeout warning, once per task path in a run_tasks() call."""
        if db_task.task_path in self._inert_timeouts:
            return
        self._inert_timeouts.add(db_task.task_path)
        logger.warning(
            "Task %s runs under a %gs timeout, which run_tasks() does not "
            "enforce: the task runs to the end on the caller's thread, and "
            "deadline() and remaining() return None inside it. Test the "
            "timeout against a real worker.",
            db_task.task_path,
            timeout,
            extra={
                "event": "run_tasks_timeout_inert",
                "task_path": db_task.task_path,
                "backend": self.backend.alias,
                "timeout_s": timeout,
            },
        )


def _refuse_nesting() -> None:
    running = _executing.get()
    if running is not None:
        raise RuntimeError(
            f"run_tasks() was called inside a running task ({running!r}). It "
            "drains the queue for test code; a task, or a signal receiver or "
            "commit callback it triggers, enqueues work instead, and the "
            "drain or worker running it claims that work next."
        )
    if _draining.get():
        raise RuntimeError(
            "run_tasks() was called while another run_tasks() call was "
            "draining, from a commit callback or signal receiver that call "
            "ran. Tasks enqueued there run in the drain already under way."
        )


def _refuse_broken_transactions() -> None:
    for conn in connections.all(initialized_only=True):
        if not _broken(conn):
            continue
        why = (
            "its connection was closed inside the transaction"
            if conn.closed_in_transaction
            else "an earlier error marked it for rollback"
        )
        raise TransactionManagementError(
            f"run_tasks() cannot run while the transaction on database "
            f"{conn.alias!r} is broken: {why}. No query can run on it until "
            "the atomic block that holds it ends."
        )


def _drain_worker(backend: str, queues: list[str] | None) -> Worker:
    oxbackend = task_backends[backend]
    if not isinstance(oxbackend, OxBackend):
        raise ImproperlyConfigured(
            f"run_tasks() drains an OxBackend, and backend {backend!r} is "
            f"{type(oxbackend).__qualname__}."
        )
    configured = worker_class(backend)
    # Named for the configured class, so a log line that names the worker's
    # class names the one in the settings.
    cls = type(
        configured.__name__,
        (_DrainWorker, configured),
        {"__qualname__": configured.__qualname__, "__module__": __name__},
    )
    return cast("Worker", cls(backend_alias=backend, queues=queues))


def drain(
    *,
    backend: str,
    queues: list[str] | None,
    max_tasks: int | None,
    raise_failures: bool,
) -> list[TaskResult[..., Any]]:
    """run_tasks(), once its arguments are known to be valid."""
    _refuse_nesting()
    worker = _drain_worker(backend, queues)
    limit = SAFETY_LIMIT if max_tasks is None else max_tasks
    results: list[TaskResult[..., Any]] = []
    draining = _draining.set(True)
    try:
        while len(results) < limit:
            _refuse_broken_transactions()
            db_task = worker.claim_one()
            if db_task is None:
                return results
            results.append(_run_one(worker, db_task, raise_failures=raise_failures))
    finally:
        _draining.reset(draining)
    if max_tasks is None:
        # A conservative look, not a claim: the claim can still refuse a row
        # this finds, one gated by a workflow or held by a rate limit.
        candidate = worker._ready_queryset().values_list("task_path", flat=True)
        remaining = candidate.first()
        if remaining is not None:
            raise RuntimeError(
                f"run_tasks() reached its safety limit of {SAFETY_LIMIT} "
                f"attempts; due READY work remains (for example: "
                f"'{remaining}'). Remaining work may be gated or rate-limited. "
                "Use max_tasks=N for bounded stepping."
            )
    return results


def _run_one(
    worker: Worker, db_task: OxTask, *, raise_failures: bool
) -> TaskResult[..., Any]:
    drain_worker = cast("_DrainWorker", worker)
    attempt = _Attempt(worker, db_task)
    drain_worker._attempt = attempt
    try:
        worker.execute(db_task, inline=True)
        if attempt.setup_error is not None:
            raise attempt.setup_error
        if attempt.moved is not None:
            raise attempt.moved
        # The outcome write's own callbacks, which a worker's commit of that
        # write would run: Pro releases a workflow node's dependents here.
        failed = attempt.commit()
        if failed is not None:
            raise failed[1]
        result = task_result_from_db(db_task)
    finally:
        drain_worker._attempt = None
        attempt.discard()
    if raise_failures and attempt.failure is not None:
        raise attempt.failure
    return result
