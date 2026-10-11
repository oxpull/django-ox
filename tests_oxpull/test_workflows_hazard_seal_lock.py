"""
Workflow.seal() reads no node row, in or out of a caller's transaction.

It used to take a row lock on every node, even when it owned its transaction.
oxpull 1.2.0 changed Batch.seal() to count members with a plain COUNT(*) when
seal() owns its transaction, and to keep the locking read only inside a
caller's transaction, where the caller's snapshot can predate a member
(CHANGELOG 1.2.0, batches.py _count_members). Workflow._count_nodes was still
the 1.0.0 shape its docstring called "Batch._count_members, verbatim":
SELECT id ... FOR UPDATE over every node, each primary key pulled into Python,
and every row held until the seal commits.

Now add() increments the workflow row's node_count in the statement that takes
that row's lock, and seal()'s status write copies node_count into total_count.
An UPDATE reads the row's latest committed version and waits on an add() still
holding it, so the copy is current in and out of a caller's transaction.

The first three tests pin what seal() does now: its statements read no
node row when it owns its transaction, another connection can lock every
node just before it commits, and on MySQL REPEATABLE READ it does not
stall an add() to another workflow. Before node_count, the first test
pinned a plain COUNT(*). Locking every node violates these contracts.
SQLite has no row locks, so the second and third tests skip there.

The rest are ported from `oxpull` 1.2.0's batch seal() correctness tests.
They catch a change that goes too far: inside a caller's transaction,
the total must count a node committed after the caller's snapshot,
which a plain COUNT(*) misses on MySQL at REPEATABLE READ.

The first three drive only public paths: workflows.create(), Workflow.add()
and Workflow.seal(). Their one instrument is a hook on the sealing
connection's commit(), which runs a probe after seal()'s last statement and
before its COMMIT. At a handful of nodes the seal's transaction is open for a
few milliseconds; the hook holds that moment still so another connection
can look at it. At a large node count the locking read itself held it open
for seconds.
"""

import logging
import threading
import time
from contextlib import contextmanager

import pytest
from django.db import OperationalError, connections, transaction
from django.test.utils import CaptureQueriesContext

from oxpull import workflows
from oxpull.exceptions import WorkflowSealed
from oxpull.models import OxpullWorkflow, OxpullWorkflowNode

from . import isolation, tasks
from .racing import race_all
from .test_batches import a_session_waits_on_this_transaction, session_isolation
from .test_workflows import stored

pytestmark = pytest.mark.django_db(transaction=True)

# How long an add() to an unrelated workflow may take while another
# workflow's seal() is open before it counts as waiting on that seal. An
# uncontended add() is a few milliseconds; InnoDB's lock wait is 50 s.
STALL_WINDOW = 2.0


def _require_row_locks():
    if not connections["default"].features.has_select_for_update:
        pytest.skip("no row locks on this database, so there is nothing to hold")


def node_reads(captured):
    table = OxpullWorkflowNode._meta.db_table
    return [
        query["sql"] for query in captured.captured_queries if table in query["sql"]
    ]


@contextmanager
def before_commit(connection, action):
    """
    Call action() when the outermost transaction on this connection is
    about to commit: every statement has run and every lock is still held.
    """
    original = connection.commit

    def commit():
        action()
        return original()

    connection.commit = commit
    try:
        yield
    finally:
        del connection.commit


def test_seal_owning_its_transaction_reads_no_node_row():
    """
    The query shape. seal()'s status UPDATE copies node_count into total_count
    and is its only statement on the workflow, so no node row is read, counted
    or locked. This test pinned a plain COUNT(*) here until node_count took its
    place.
    """
    flow = workflows.create("shape")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    connection = connections["default"]
    assert not connection.in_atomic_block

    with CaptureQueriesContext(connection) as captured:
        flow.seal()

    assert node_reads(captured) == []
    (sealing,) = [
        query["sql"]
        for query in captured.captured_queries
        if query["sql"].lstrip().upper().startswith("UPDATE")
        and OxpullWorkflow._meta.db_table in query["sql"]
    ]
    assert "node_count" in sealing, sealing
    assert stored(flow).total_count == 2


def test_seal_owning_its_transaction_leaves_every_node_lockable_until_commit():
    """
    The same property observed from the lock manager instead of the SQL
    text, so a fix that spells the count differently is still held to it.
    Just before seal() commits, a second connection asks for every node row
    with SKIP LOCKED. It should get all of them.
    """
    _require_row_locks()
    width = 20
    flow = workflows.create("locks")
    for index in range(width):
        flow.add(f"n{index}", tasks.step, f"n{index}")
    lockable = []

    def count_lockable_nodes_elsewhere():
        def probe():
            with transaction.atomic():
                return len(
                    OxpullWorkflowNode.objects.filter(workflow_id=flow.id)
                    .select_for_update(skip_locked=True)
                    .values_list("pk", flat=True)
                )

        results, errors = race_all([probe])
        assert errors == []
        lockable.extend(results)

    with before_commit(connections["default"], count_lockable_nodes_elsewhere):
        flow.seal()

    assert lockable == [width], (
        f"with seal() about to commit, another connection could lock "
        f"{lockable} of {width} node rows"
    )
    assert stored(flow).total_count == width


def test_sealing_one_workflow_does_not_stall_an_add_to_another_under_repeatable_read():
    """
    Where the lock reaches past the workflow being sealed. Under InnoDB's
    REPEATABLE READ a locking read over workflow_id = X takes next-key
    locks: the gaps beside X's index records are locked too, and an INSERT
    of another workflow's node into either gap waits for the seal to commit.
    Django's MySQL backend opens connections at READ COMMITTED, where a
    locking read takes no gap locks; a project that sets
    OPTIONS["isolation_level"] to "repeatable read", or to None for the
    server default, runs at this level, which is the level test_batches'
    snapshot tests exercise for the same reason.

    Here the sealing workflow owns the only node records, so the other
    workflow's first node lands in one of those gaps. In a table of many
    workflows it is whichever workflow's nodes sort next to the one being
    sealed, and it waits for as long as that seal's transaction lasts.

    A locking read in seal()'s own transaction can take gap locks.
    Rows deleted by earlier tests can bound those locks until the server
    purges them, making the regression depend on test order. seal() now
    reads no node row when it owns its transaction, so it locks no gap
    and that timing no longer matters.
    """
    if connections["default"].vendor != "mysql":
        pytest.skip(
            "gap locks are InnoDB's; PostgreSQL's row locks cover no gap, and "
            "SQLite has no row locks"
        )
    sealing = workflows.create("sealing")
    building = workflows.create("building")
    sealing.add("a", tasks.step, "a")
    sealing.add("b", tasks.step, "b")
    added = threading.Event()
    outcome = []
    stalled = []

    def add_first_node_to_the_other_workflow():
        try:
            building.add("first", tasks.step, "first")
            outcome.append("added")
        except BaseException as exc:
            outcome.append(exc)
        finally:
            added.set()
            connections.close_all()

    def while_seal_is_open():
        threading.Thread(target=add_first_node_to_the_other_workflow).start()
        stalled.append(not added.wait(timeout=STALL_WINDOW))

    with (
        session_isolation("REPEATABLE READ"),
        before_commit(connections["default"], while_seal_is_open),
    ):
        sealing.seal()

    assert added.wait(timeout=60)
    assert stalled == [False], (
        f"add() to another workflow was still waiting {STALL_WINDOW}s into "
        "seal()'s open transaction"
    )
    assert outcome == ["added"]
    assert stored(sealing).total_count == 2
    assert building.node_ids().keys() == {"first"}


# -- guards: the total stays current where a snapshot could lie ----------


def test_seal_inside_a_callers_transaction_reads_no_node_row_either():
    """
    Inside a caller's transaction too. This test pinned a locking read here,
    because a plain COUNT could miss a node committed after the caller's
    snapshot. The copy of node_count is an UPDATE, which reads the workflow
    row's latest committed version, so it needs no read of the nodes (the next
    test is the case).
    """
    flow = workflows.create("ambient-shape")
    flow.add("a", tasks.step, "a")
    connection = connections["default"]

    with CaptureQueriesContext(connection) as captured, transaction.atomic():
        flow.seal()

    assert node_reads(captured) == []
    assert stored(flow).total_count == 1


def test_ambient_seal_counts_a_node_committed_after_the_callers_snapshot():
    """
    The case a snapshot could get wrong. Under MySQL REPEATABLE READ the
    caller's transaction fixes its read view at its first consistent read,
    and an add() that commits after that is invisible to a plain COUNT,
    although seal()'s UPDATE waited on nothing: the add() had already
    committed. The UPDATE's copy of node_count reads the row's latest
    committed version and counts the node. PostgreSQL's READ COMMITTED finds
    it either way; the test runs there to show the promise is the same.
    """
    if connections["default"].vendor == "sqlite":
        pytest.skip(
            "a reading SQLite transaction holds its shared lock, so the "
            "concurrent add() could not commit under it"
        )
    flow = workflows.create("snapshot")

    def add_and_commit():
        flow.add("late", tasks.step, "late")
        return "added"

    with session_isolation("REPEATABLE READ"), transaction.atomic():
        # The caller's first consistent read; the read view starts here.
        assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 0
        results, errors = race_all([add_and_commit])
        assert (results, errors) == (["added"], [])
        flow.seal()

    assert stored(flow).total_count == 1


def test_seal_owning_its_transaction_counts_the_node_whose_add_it_waited_for():
    """
    Why the total is current when seal() owns its transaction, even under
    MySQL REPEATABLE READ: the status UPDATE is the transaction's first
    statement and waits on an add() still holding the workflow row, and it
    copies node_count from the version that add() committed.
    """
    flow = workflows.create("waited")
    added = threading.Event()

    def add_slowly():
        with transaction.atomic():
            flow.add("slow", tasks.step, "slow")
            added.set()
            # Hold the workflow row across seal()'s arrival.
            time.sleep(1.0)
        return "added"

    def seal_after_the_add_started():
        assert added.wait(timeout=10)
        with session_isolation("REPEATABLE READ"):
            flow.seal()
        return "sealed"

    results, errors = race_all([add_slowly, seal_after_the_add_started])

    assert errors == []
    assert sorted(results) == ["added", "sealed"]
    assert stored(flow).total_count == 1


def test_an_add_arriving_between_seals_statements_waits_and_is_refused(monkeypatch):
    """
    The interleaving a copy without locks might open: an add() committing
    after seal()'s status UPDATE returned and before seal() commits. It
    cannot happen, because add() claims the workflow row the UPDATE holds
    until seal() commits. Proven by starting the add() from inside seal()'s
    transaction, after the UPDATE (in its reconciler-schedule check, the step
    that follows it), watching it block for a second, and reading its refusal
    once seal() has committed.
    """
    flow = workflows.create("between")
    flow.add("before", tasks.step, "before")
    add_finished = threading.Event()
    outcome = []

    def late_add():
        try:
            flow.add("late", tasks.step, "late")
            outcome.append("added")
        except WorkflowSealed:
            outcome.append("refused")
        finally:
            add_finished.set()
            connections.close_all()

    original = workflows._require_reconcile_schedule
    started = []

    def check_after_starting_an_add():
        if not started:
            started.append(True)
            threading.Thread(target=late_add).start()
            assert not add_finished.wait(timeout=1.0), "add() did not wait for seal()"
        return original()

    monkeypatch.setattr(
        workflows, "_require_reconcile_schedule", check_after_starting_an_add
    )
    flow.seal()

    assert add_finished.wait(timeout=10)
    assert outcome == ["refused"]
    assert stored(flow).total_count == 1
    assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 1


def test_seal_under_postgresql_repeatable_read_reissues_rather_than_counting_stale(
    caplog,
):
    """
    PostgreSQL REPEATABLE READ takes the snapshot at seal()'s first
    statement, the status UPDATE. When that UPDATE has waited on an add()
    holding the workflow row, the row it then finds changed after the
    snapshot, and PostgreSQL raises a serialization failure instead of
    copying a stale count. seal() owns its transaction here, so it runs it
    again on a fresh snapshot, which counts every node. Batch.seal() still
    raises for its caller to retry, as test_batches.py pins; this test used
    to pin the same for workflows, until the reissue in
    test_workflows_hazard_postgresql_repeatable_read_concurrent_add.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("PostgreSQL's REPEATABLE READ takes its snapshot at the UPDATE")
    caplog.set_level(logging.INFO, logger="oxpull.workflows")
    flow = workflows.create("rr")
    flow.add("before", tasks.step, "before")
    added = threading.Event()

    def add_slowly():
        with transaction.atomic():
            flow.add("slow", tasks.step, "slow")
            added.set()
            # The workflow row stays locked until seal()'s UPDATE is waiting
            # on it. That UPDATE took the snapshot when it began, so the
            # snapshot is older than this commit however late the sealing
            # thread got going.
            assert a_session_waits_on_this_transaction(), (
                "seal() was never seen waiting on the workflow row add() holds"
            )
        return "added"

    def seal_at_repeatable_read():
        assert added.wait(timeout=10)
        isolation.outlives_the_transaction("REPEATABLE READ")
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                "REPEATABLE READ"
            )
        try:
            flow.seal()
        except OperationalError as exc:
            return type(exc.__cause__).__name__
        return "sealed"

    results, errors = race_all([add_slowly, seal_at_repeatable_read])

    assert errors == []
    assert sorted(results) == ["added", "sealed"]
    # The conflict was provoked, and answered by running seal() again.
    retries = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "workflow_seal_serialization_retry"
    ]
    assert retries, "seal() never met the serialization failure this test is about"
    assert stored(flow).status == OxpullWorkflow.Status.SEALED
    assert stored(flow).total_count == 2
