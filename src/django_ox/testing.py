"""
Test helpers: backends that accept the per-task policy fields, and
run_tasks(), which runs enqueued tasks inside a test.

Django's ImmediateBackend and DummyBackend build the stock Task, so on Django
6.1 and on the 5.2 backport a module that declares ``@task(max_attempts=5)``
fails to import under test settings that point TASKS at either of them:
``TypeError: Task.__init__() got an unexpected keyword argument``. These
subclasses build django_ox.tasks.PolicyTask instead and hold its fields to
the same validation OxBackend does, so the module imports, and an invalid
value fails the same way it would in production.

They are import-compatible test doubles, not retry or timeout simulators.
ImmediateBackend runs each enqueued task once, on the caller's thread, and
DummyBackend never runs it; neither retries, calls a backoff or enforces a
timeout. The first time each task that declares any of the three fields is
enqueued, the backend logs one WARNING saying so. Test the policy itself
against a real worker.

Use them in test settings::

    TASKS = {"default": {"BACKEND": "django_ox.testing.ImmediateBackend"}}

run_tasks() is for test settings that keep OxBackend. It runs what the test
enqueued the way a worker runs it, on the test's own thread and connection,
so it works inside a TestCase's transaction; its docstring says what it
does and does not reproduce.
"""

from __future__ import annotations

import logging
from threading import Lock
from typing import Any

from .compat import DEFAULT_TASK_BACKEND_ALIAS, Task, TaskResult
from .compat import DummyBackend as _StockDummyBackend
from .compat import ImmediateBackend as _StockImmediateBackend
from .tasks import PolicyTask, task_policy, validate_policy

__all__ = ["DummyBackend", "ImmediateBackend", "run_tasks"]

logger = logging.getLogger("django_ox")


class _PolicyTolerant:
    """
    What the two test backends share: PolicyTask, its validation, and one
    warning per policy-bearing task that the policy is not enforced here.
    """

    task_class: type[Task[..., Any]] = PolicyTask
    alias: str
    #: What this backend does with a task instead, for the warning.
    runs = ""

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)  # type: ignore[call-arg]
        self._warned: set[str] = set()
        self._warned_lock = Lock()

    def validate_task(self, task: Task[..., Any]) -> None:
        super().validate_task(task)  # type: ignore[misc]
        validate_policy(task)

    def _warn_inert(self, task: Task[..., Any]) -> None:
        declared = {
            name: value
            for name, value in zip(
                ("max_attempts", "backoff", "timeout"), task_policy(task), strict=True
            )
            if value is not None
        }
        if not declared:
            return
        with self._warned_lock:
            if task.module_path in self._warned:
                return
            self._warned.add(task.module_path)
        logger.warning(
            "Task %s declares %s, which the %r backend (%s) does not enforce: "
            "it %s, with no retry, no backoff and no timeout. Test that policy "
            "against a real worker.",
            task.module_path,
            ", ".join(sorted(declared)),
            self.alias,
            type(self).__qualname__,
            self.runs,
            extra={
                "event": "task_policy_inert",
                "task_path": task.module_path,
                "backend": self.alias,
                "declared": sorted(declared),
            },
        )


class ImmediateBackend(_PolicyTolerant, _StockImmediateBackend):
    """Django's ImmediateBackend, building PolicyTask; runs each task once."""

    runs = "runs each task once, on the caller's thread"

    def enqueue(self, task: Task[..., Any], args: Any, kwargs: Any) -> Any:
        self._warn_inert(task)
        return super().enqueue(task, args, kwargs)


class DummyBackend(_PolicyTolerant, _StockDummyBackend):
    """Django's DummyBackend, building PolicyTask; never runs a task."""

    runs = "stores each task without running it"

    def enqueue(self, task: Task[..., Any], args: Any, kwargs: Any) -> Any:
        self._warn_inert(task)
        return super().enqueue(task, args, kwargs)


def run_tasks(
    *,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    queues: list[str] | None = None,
    max_tasks: int | None = None,
    raise_failures: bool = False,
) -> list[TaskResult[..., Any]]:
    """
    Run the tasks waiting on an OxBackend as its worker would, on the
    caller's thread and database connections, and return one TaskResult per
    attempt, in the order the attempts ran.

    The backend's worker class, WORKER_CLASS or Worker, claims each task
    with its own claim_one() and runs it with its own execute(inline=True).
    So a task runs only when a worker would claim it: READY, due by
    timezone.now(), on the worker's queues, and passed by any claim filter
    or rate limit. A row whose run_after is later, a retry whose backoff has
    not elapsed, and a WAITING, RUNNING or finished row are left as they
    are. Tasks enqueued while the call runs are run by it too. Each result
    is its attempt's recorded state, so a task retried within the call
    appears once per attempt.

    `queues` narrows the claim as it narrows a worker's; None, the default,
    claims what the backend's worker claims. `max_tasks` is the most
    attempts to make; the call then returns, whatever is left. With None it
    goes on until a claim finds nothing, up to 1000 attempts, and raises
    RuntimeError if those ran and the worker's candidate query still finds
    a due READY row. That query does not claim, so a row the claim would
    refuse, one a workflow gates or a rate limit holds, counts. With
    `raise_failures` True, an attempt whose task body failed, retried or
    not, has its exception raised once the failure is recorded and its
    callbacks have run, and the call stops there.

    Inside an atomic block, which every TestCase is, each task body runs in
    a savepoint on every database the caller holds in one. A body that
    raises keeps what it wrote there, as a worker's autocommit connection
    would, unless a database error left the transaction unable to go on:
    then its writes on that database are rolled back, and the failure is
    recorded after them. A body that returns in that state is recorded as
    failed, not as a success. The transaction.on_commit() callbacks the
    attempt registered there run when the body is done, in place of a
    worker's commit, and those the outcome registered run after it. Each
    runs in a savepoint of its own, rolled back if it raises, so what it
    wrote goes and an error it meets, a database error included, leaves the
    caller's transaction usable; one that returns with its transaction
    marked for rollback is rolled back too and counts as failing. A failing
    callback from the body fails an attempt that had succeeded; one from
    the outcome is raised from here, the outcome kept. The caller's own
    pending callbacks are left alone. Outside an atomic block none of this
    applies, which is the worker's own behaviour.

    Timeouts are not enforced: a task runs to the end, deadline() and
    remaining() return None inside it, and each task path under a timeout is
    logged as run_tasks_timeout_inert once per call. Nothing bounds how long
    a task or a callback runs.

    Raises RuntimeError when called inside a running task, or from a
    callback or signal receiver a run_tasks() call runs, and when the
    caller's pending commit callbacks were changed during an attempt by
    something other than on_commit() and a savepoint rollback, so the
    attempt's own can no longer be told from them;
    TransactionManagementError when a transaction on one of the caller's
    connections is already broken; ImproperlyConfigured for a backend that
    is not an OxBackend; TypeError or ValueError for an invalid argument. A
    KeyboardInterrupt or SystemExit reaches the caller with nothing
    recorded: raised by a task body, with the attempt's writes rolled back;
    raised by a commit callback the body registered, with the body's writes
    already kept, since its savepoint was released before the callbacks
    ran. An attempt whose task no longer imports is recorded as a worker
    records it, and the ImportError is then raised, since no TaskResult can
    be built for it.
    """
    if not isinstance(backend, str):
        raise TypeError(f"run_tasks() backend must be a TASKS alias, got {backend!r}.")
    if queues is not None:
        if isinstance(queues, (str, bytes)):
            raise TypeError(
                f"run_tasks() queues must be a list of queue names, not a single "
                f"string ({queues!r})."
            )
        try:
            names = list(queues)
        except TypeError:
            raise TypeError(
                f"run_tasks() queues must be a list of queue names, got {queues!r}."
            ) from None
        if not all(isinstance(name, str) for name in names):
            raise TypeError(
                f"run_tasks() queues must be a list of queue names, got {queues!r}."
            )
        queues = names
    if max_tasks is not None:
        if isinstance(max_tasks, bool) or not isinstance(max_tasks, int):
            raise TypeError(
                f"run_tasks() max_tasks must be a whole number or None, got "
                f"{max_tasks!r}."
            )
        if max_tasks < 0:
            raise ValueError(
                f"run_tasks() max_tasks must be 0 or more, got {max_tasks!r}."
            )
    if not isinstance(raise_failures, bool):
        raise TypeError(
            f"run_tasks() raise_failures must be True or False, got {raise_failures!r}."
        )
    # Imported here, not at the top: this module is named in TASKS settings,
    # and the worker imports the models.
    from ._run_tasks import drain

    return drain(
        backend=backend,
        queues=queues,
        max_tasks=max_tasks,
        raise_failures=raise_failures,
    )
