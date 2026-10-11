"""
At PostgreSQL REPEATABLE READ, a no-op write to the workflow
row becomes a serialization failure that nothing retries.

add(), seal() and both reconciler transactions each open with an UPDATE
of the workflow row. On PostgreSQL even a no-op UPDATE writes a new row
version. A REPEATABLE READ transaction whose snapshot predates another
writer's commit, and which waited on that writer's row lock, gets
SerializationFailure (SQLSTATE 40001) when the writer commits. READ
COMMITTED would re-read the row instead. Nothing in workflows.py reissued
on 40001, so the caller got a raw django.db.OperationalError.

Now add() (Workflow.add and Batch.add), Workflow.seal() and both
reconcilers' completing transactions run again on a serialization failure
when they own the transaction, and every test here passes. Batch.seal()
still raises for its caller to retry, as 1.2.0 documents and
test_batches.py pins.

Each test asserts the behaviour the code's own comments promise.
Concurrent add()s all land. seal() waits out an add() in flight and
counts it. A reconciler that loses the completion compare-and-set reads a
rowcount of 0. Each test is parametrized over how a project can set the
isolation level:

- READ COMMITTED, Django's default. This is the control.
- REPEATABLE READ through DATABASES OPTIONS["isolation_level"].
- REPEATABLE READ as the server default. On PostgreSQL that is
  default_transaction_isolation. On MySQL it is OPTIONS
  ["isolation_level"] = None, which leaves InnoDB's own default in place.

MySQL runs the same tests as a vendor control, because InnoDB answers a
waited-on UPDATE with a current read. SQLite has no isolation level to
set, so it skips.

Three of the four tests hold the window open deterministically. A monitor
thread sees the contending sessions waiting on a lock before the holder
commits, and every test asserts that it did, so a green control is a
control that actually contended.
"""

import contextlib
import logging
import threading
import time

import pytest
from django.db import connections, transaction
from django.tasks.signals import task_enqueued

from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.exceptions import WorkflowSealed
from oxpull.models import OxpullBatch, OxpullBatchMember, OxpullWorkflow

from . import isolation, tasks
from .racing import THREADS, race_all
from .test_workflows import drain, flow_worker, reconcile, stored

LEVELS = [
    "read-committed",
    "repeatable-read-options",
    "repeatable-read-server-default",
]


def require_a_database_with_isolation_levels():
    if connections["default"].vendor not in ("postgresql", "mysql"):
        pytest.skip("SQLite has no isolation level to configure")


def expected_level(level):
    return "read committed" if level == "read-committed" else "repeatable read"


@contextlib.contextmanager
def new_connections_at(level):
    """
    Every connection opened inside the block runs at `level`, set the way
    a project sets it.

    Django reads settings_dict["OPTIONS"] when it connects, and each
    thread's connection shares that dict, so a thread started inside the
    block connects with the level. The test's own connection is already
    open at READ COMMITTED and keeps it. Each test also asserts the level
    a new transaction on every thread actually ran at, so a setting that
    never reached the connection cannot pass as a control.
    """
    connection = connections["default"]
    options = connection.settings_dict["OPTIONS"]
    missing = object()
    before = options.get("isolation_level", missing)
    altered_database = None
    isolation.outlives_the_transaction(level)
    if connection.vendor == "postgresql":
        from psycopg import IsolationLevel

        if level == "read-committed":
            options["isolation_level"] = IsolationLevel.READ_COMMITTED
        elif level == "repeatable-read-options":
            options["isolation_level"] = IsolationLevel.REPEATABLE_READ
        else:
            options.pop("isolation_level", None)
            altered_database = connection.ops.quote_name(
                connection.settings_dict["NAME"]
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER DATABASE {altered_database} "
                    "SET default_transaction_isolation = 'repeatable read'"
                )
            # Only a session opened after the ALTER reads the new default,
            # and a pooled checkout may be a session opened before it.
            isolation.renew_pools()
    else:
        options["isolation_level"] = {
            "read-committed": "read committed",
            "repeatable-read-options": "repeatable read",
            "repeatable-read-server-default": None,
        }[level]
    try:
        yield
    finally:
        if altered_database is not None:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER DATABASE {altered_database} "
                    "RESET default_transaction_isolation"
                )
        if before is missing:
            options.pop("isolation_level", None)
        else:
            options["isolation_level"] = before


def level_of_a_new_transaction():
    """The isolation level a transaction on this thread's connection gets."""
    connection = connections["default"]
    with transaction.atomic(), connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SHOW transaction_isolation")
        else:
            cursor.execute("SELECT @@SESSION.transaction_isolation")
        return str(cursor.fetchone()[0]).lower().replace("-", " ")


def lock_waiters():
    """Sessions on this test database waiting on a lock, right now."""
    connection = connections["default"]
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND datname = current_database()"
            )
        else:
            cursor.execute(
                "SELECT COUNT(DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID) "
                "FROM performance_schema.data_lock_waits w "
                "JOIN performance_schema.data_locks l "
                "ON l.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID "
                "WHERE l.OBJECT_SCHEMA = DATABASE()"
            )
        return int(cursor.fetchone()[0])


def wait_for_lock_waiters(count, timeout=15.0):
    """
    Poll until `count` sessions wait on a lock, and return how many did.

    It runs in autocommit on its own thread, so every poll is a new
    statement and PostgreSQL's activity snapshot is fresh each time.
    """
    deadline = time.monotonic() + timeout
    seen = 0
    while time.monotonic() < deadline:
        seen = lock_waiters()
        if seen >= count:
            return seen
        time.sleep(0.02)
    return seen


def describe(errors):
    """One line per error: the Django type, the driver's cause, its code."""
    lines = []
    for exc in errors:
        cause = exc.__cause__
        code = getattr(cause, "sqlstate", None) or (
            cause.args[0] if cause is not None and cause.args else None
        )
        first = str(exc).splitlines()[0] if str(exc) else ""
        lines.append(
            f"{type(exc).__name__} <- {type(cause).__name__} [{code}]: {first}"
        )
    return lines


# -- add() -------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_concurrent_adds_to_one_open_workflow_all_land(level):
    """
    The plain race: eight add()s on one OPEN workflow, released together.

    No add() here runs inside a caller's transaction, so each owns its
    transaction and nothing stops it from being reissued. The nodes do not
    conflict. They are eight different names under one root, and the only
    thing they contend for is the workflow row's no-op UPDATE.
    """
    require_a_database_with_isolation_levels()
    flow = workflows.create("rr")
    flow.add("root", tasks.step, "root")
    levels = []

    def add(index):
        def call():
            try:
                flow.add(
                    f"n{index}",
                    tasks.slow_step,
                    f"n{index}",
                    0.0,
                    depends_on="root",
                )
            finally:
                levels.append(level_of_a_new_transaction())
            return f"n{index}"

        return call

    with new_connections_at(level):
        _results, errors = race_all([add(index) for index in range(THREADS)])

    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert len(flow.node_ids()) == THREADS + 1
    assert OxTask.objects.count() == THREADS + 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_adds_that_waited_on_an_add_in_flight_all_land_once_it_commits(level):
    """
    The same property, with the window held open.

    One add() runs inside a caller's transaction and holds the workflow
    row: a request under ATOMIC_REQUESTS that adds a node and then does
    more work. Seven more add()s start while it holds the row. The holder
    commits only once all seven are seen waiting on a lock, so the READ
    COMMITTED control proves they waited and re-read, and did not simply
    run after it.
    """
    require_a_database_with_isolation_levels()
    flow = workflows.create("rr-held")
    flow.add("root", tasks.step, "root")
    held = threading.Event()
    waited = threading.Event()
    observed = {}
    levels = []
    others = THREADS - 1

    def holder():
        with transaction.atomic():
            flow.add("held", tasks.step, "held", depends_on="root")
            held.set()
            assert waited.wait(timeout=20)
        levels.append(level_of_a_new_transaction())
        return "held"

    def add(index):
        def call():
            assert held.wait(timeout=10)
            try:
                flow.add(f"n{index}", tasks.step, f"n{index}", depends_on="root")
            finally:
                levels.append(level_of_a_new_transaction())
            return f"n{index}"

        return call

    def monitor():
        assert held.wait(timeout=10)
        observed["waiters"] = wait_for_lock_waiters(others)
        waited.set()
        return "monitor"

    with new_connections_at(level):
        _results, errors = race_all(
            [holder, *[add(index) for index in range(others)], monitor]
        )

    assert observed["waiters"] == others
    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert len(flow.node_ids()) == THREADS + 1
    assert OxTask.objects.count() == THREADS + 1


# -- seal() ------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_seal_waits_out_an_add_in_flight_and_counts_it(level):
    """
    seal()'s comment: "any add() already in flight commits before the
    count below". The add() here commits while the workflow is OPEN, so
    the one correct outcome is a SEALED workflow that counts two nodes.
    WorkflowSealed would be wrong too, because nothing sealed it first.
    """
    require_a_database_with_isolation_levels()
    flow = workflows.create("rr-seal")
    flow.add("root", tasks.step, "root")
    held = threading.Event()
    waited = threading.Event()
    observed = {}
    levels = []

    def holder():
        with transaction.atomic():
            flow.add("held", tasks.step, "held", depends_on="root")
            held.set()
            assert waited.wait(timeout=20)
        return "held"

    def seal():
        assert held.wait(timeout=10)
        try:
            flow.seal()
        except WorkflowSealed:
            return "refused"
        finally:
            levels.append(level_of_a_new_transaction())
        return "sealed"

    def monitor():
        assert held.wait(timeout=10)
        observed["waiters"] = wait_for_lock_waiters(1)
        waited.set()
        return "monitor"

    with new_connections_at(level):
        results, errors = race_all([holder, seal, monitor])

    assert observed["waiters"] == 1
    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert sorted(results) == ["held", "monitor", "sealed"]
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.SEALED
    assert row.total_count == 2


# -- the reconciler ----------------------------------------------------


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_reconciler_that_loses_the_completion_race_reads_zero_rows(level):
    """
    _complete_if_finished's comment: "Racing reconcilers all attempt the
    same UPDATE, one matches a SEALED row, and the losers' transactions
    change nothing."

    The first reconciler wins the compare-and-set and is held inside its
    completing transaction while it enqueues the callback. A task_enqueued
    receiver holds it there until the second reconciler is seen waiting
    on the workflow row. The loser should return 0 and log nothing. What
    it must not do is log workflow_reconcile_failed at ERROR with a
    traceback, which is what an operator alerts on.
    """
    require_a_database_with_isolation_levels()
    flow = workflows.create(
        "rr-reconcile",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    flow.add("only", tasks.step, "only")
    flow.seal()
    assert drain(flow_worker()) == 1
    observed, levels, results, errors, failures = race_the_completion(
        level, reconcile, "oxpull.workflows", "workflow_reconcile_failed"
    )
    assert observed["waiters"] == 1
    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert failures == []
    assert dict(results) == {"winner": 1, "loser": 0, "monitor": None}
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 1
    )


def race_the_completion(level, reconcile_once, logger_name, failed_event):
    """
    Two reconcilers on one finished row: the first is held inside its
    completing transaction, after its compare-and-set, until the second is
    seen waiting on the row. Returns what the monitor saw, the levels each
    thread ran at, the results, the errors, and every `failed_event` record.
    """
    cas_held = threading.Event()
    waited = threading.Event()
    observed = {}
    levels = []
    guard = threading.Lock()

    def hold_the_completion(sender, task_result, **kwargs):
        if task_result.task.module_path != "tests_oxpull.tasks.record_callback":
            return
        with guard:
            if cas_held.is_set():
                return
            cas_held.set()
        waited.wait(timeout=20)

    def winner():
        try:
            return ("winner", reconcile_once())
        finally:
            levels.append(level_of_a_new_transaction())

    def loser():
        assert cas_held.wait(timeout=10)
        try:
            return ("loser", reconcile_once())
        finally:
            levels.append(level_of_a_new_transaction())

    def monitor():
        assert cas_held.wait(timeout=10)
        observed["waiters"] = wait_for_lock_waiters(1)
        waited.set()
        return ("monitor", None)

    handler = _Records()
    reconciler_logger = logging.getLogger(logger_name)
    reconciler_logger.addHandler(handler)
    task_enqueued.connect(
        hold_the_completion, weak=False, dispatch_uid="hold_the_completion"
    )
    try:
        with new_connections_at(level):
            results, errors = race_all([winner, loser, monitor])
    finally:
        task_enqueued.disconnect(dispatch_uid="hold_the_completion")
        reconciler_logger.removeHandler(handler)

    failures = [
        describe([record.exc_info[1]]) if record.exc_info else record.getMessage()
        for record in handler.records
        if getattr(record, "event", None) == failed_event
    ]
    return observed, levels, results, errors, failures


# -- batches, which share add() and the shape of the completion ----------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_concurrent_adds_to_one_open_batch_all_land(level):
    """
    Batch.add goes through the same reissue as Workflow.add. 1.2.0 landed
    one of eight at PostgreSQL REPEATABLE READ.
    """
    require_a_database_with_isolation_levels()
    batch = batches.create("rr-batch")
    levels = []

    def add(index):
        def call():
            try:
                batch.add(tasks.plain_echo, f"m{index}")
            finally:
                levels.append(level_of_a_new_transaction())
            return index

        return call

    with new_connections_at(level):
        _results, errors = race_all([add(index) for index in range(THREADS)])

    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert OxpullBatchMember.objects.filter(batch_id=batch.id).count() == THREADS
    assert OxTask.objects.count() == THREADS


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_batch_reconciler_that_loses_the_completion_race_reads_zero_rows(
    level, worker
):
    """The batch reconciler's compare-and-set, raced the same way."""
    require_a_database_with_isolation_levels()
    batch = batches.create(
        "rr-batch-reconcile",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    batch.add(tasks.plain_echo, "only")
    batch.seal()
    assert worker.run_once() is True
    observed, levels, results, errors, failures = race_the_completion(
        level, batches._reconcile, "oxpull.batches", "batch_reconcile_failed"
    )
    assert observed["waiters"] == 1
    assert set(levels) == {expected_level(level)}
    assert describe(errors) == []
    assert failures == []
    assert dict(results) == {"winner": 1, "loser": 0, "monitor": None}
    assert OxpullBatch.objects.get(pk=batch.id).status == OxpullBatch.Status.COMPLETE
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 1
    )
