"""
Workflow.add() on MySQL: a replaced client flag and an InnoDB deadlock.

Each test asserts the behaviour `oxpull` 1.2.0 already gives Batch.add.
All but the guard catch workflow regressions in the cases below:

* add() read its no-op OPEN-claim UPDATE's rowcount as "the workflow is
  OPEN". On MySQL that rowcount counts matched rows only because Django
  asks for CLIENT_FOUND_ROWS, and a project that sets
  OPTIONS["client_flag"] replaces the flag. 1.2.0 fixed exactly this in
  Batch._add_once, and Workflow.add now shares that code; the tests are
  ports of tests_oxpull/test_batches.py's without_found_rows pair.
* add() was one atomic block with no reissue after an InnoDB deadlock
  (1213) when it owns its transaction. 1.2.0's Batch.add reissues up to
  MAX_ADD_ATTEMPTS, and Workflow.add now shares that loop. A port of
  test_batches.py's deadlock_once test, which injects the 1213: no real
  InnoDB deadlock inside add() was reached. The guard beside it pins that
  a caller's own transaction is never reissued.
* Adjacent, same flag, still open: the reconciler's _cancel_one_level
  reads a no-op SEALED-claim UPDATE's rowcount the same way, and batches
  have no such statement.

MySQL only, except the guard: the flag is a MySQL connection flag and 1213
an InnoDB code.
"""

from contextlib import contextmanager

import pytest
from django.db import OperationalError, connections, transaction

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.backend import ER_LOCK_DEADLOCK
from oxpull.exceptions import WorkflowSealed
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .racing import race_all
from .test_batches import session_isolation
from .test_workflows import drain, flow_worker, reconcile, stored

pytestmark = pytest.mark.django_db(transaction=True)

# A value a project might really set, and zero. Neither carries the
# FOUND_ROWS bit (value 2); MULTI_STATEMENTS is bit 16 in both PyMySQL's
# and mysqlclient's constants.
WITHOUT_FOUND_ROWS = [
    pytest.param(0, id="client_flag=0"),
    pytest.param(1 << 16, id="client_flag=MULTI_STATEMENTS"),
]

# Read at collection: every expected failure below exists on MySQL only, and
# the tests skip elsewhere.
ON_MYSQL = connections["default"].vendor == "mysql"


def require_mysql(reason):
    if connections["default"].vendor != "mysql":
        pytest.skip(reason)


@contextmanager
def client_flag(flag):
    """
    tests_oxpull.test_batches.without_found_rows with the flag value chosen, and
    the OPTIONS restored before the block's caller reconnects, so no
    later statement or test inherits a connection without the flag.
    None leaves Django's own flags in place.
    """
    connection = connections["default"]
    require_mysql("CLIENT_FOUND_ROWS is a MySQL connection flag")
    if flag is None:
        yield
        return
    options = connection.settings_dict["OPTIONS"]
    missing = object()
    previous = options.get("client_flag", missing)
    options["client_flag"] = flag
    connection.close()
    try:
        yield
    finally:
        connection.close()
        if previous is missing:
            options.pop("client_flag", None)
        else:
            options["client_flag"] = previous


# -- item 4: the OPEN claim without CLIENT_FOUND_ROWS --------------------


@pytest.mark.parametrize("flag", WITHOUT_FOUND_ROWS)
def test_add_on_an_open_workflow_does_not_depend_on_the_mysql_found_rows_flag(flag):
    """
    Port of test_add_does_not_depend_on_the_mysql_found_rows_flag.
    Without the fix, the first add() on a brand-new OPEN workflow raised
    WorkflowSealed ("... is OPEN; membership is frozen."), so no workflow
    could be built.
    """
    with client_flag(flag):
        flow = workflows.create("no-found-rows")
        root = flow.add("root", tasks.step, "root")
        child = flow.add("child", tasks.step, "child", depends_on="root")
        assert OxpullWorkflowEdge.objects.filter(
            workflow_id=flow.id,
            upstream_task_id=root.id,
            downstream_task_id=child.id,
        ).exists()
        flow.seal()
        with pytest.raises(WorkflowSealed, match="SEALED"):
            flow.add("late", tasks.step, "late")
    assert stored(flow).total_count == 2
    assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 2
    assert OxTask.objects.count() == 2


@pytest.mark.parametrize(
    "flag",
    [pytest.param(None, id="django-default-flags"), *WITHOUT_FOUND_ROWS],
)
def test_add_refused_by_a_committed_seal_names_the_status_it_locked(flag):
    """
    Port of test_add_without_the_flag_reads_the_status_it_locked_not_its
    _snapshot. Inside a caller's REPEATABLE READ transaction whose
    snapshot says OPEN, a concurrent seal() commits; add() must refuse
    and say SEALED. A zero-rowcount refusal alone is insufficient:
    _not_open_error must not read the status with a plain SELECT, which
    answers from the caller's snapshot and can report that the workflow
    "is OPEN; membership is frozen". This also affects Django's own flags
    whenever the session runs at REPEATABLE READ. A fallback that trusts
    that snapshot read could instead admit a node into a sealed workflow.
    """
    with client_flag(flag):
        flow = workflows.create("stale-snapshot")

        def seal_and_commit():
            flow.seal()
            return "sealed"

        with session_isolation("REPEATABLE READ"), transaction.atomic():
            # The caller's snapshot: the workflow is OPEN in it from here.
            assert flow.status == OxpullWorkflow.Status.OPEN
            assert race_all([seal_and_commit]) == (["sealed"], [])
            with pytest.raises(WorkflowSealed, match="SEALED"):
                flow.add("late", tasks.step, "late")

    assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 0
    assert OxTask.objects.count() == 0
    assert stored(flow).total_count == 0


# -- item 5: no reissue after an InnoDB deadlock ------------------------


def node_insert_deadlocks_once(monkeypatch):
    """
    tests_oxpull.test_batches.deadlock_once, on the node INSERT instead of the
    membership INSERT, and for the reason given there: the error carries
    MySQL's own code and text, and add()'s atomic block does the rollback
    InnoDB would already have done.
    """
    real_save = OxpullWorkflowNode.save
    calls = []

    def save_or_deadlock(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OperationalError(
                ER_LOCK_DEADLOCK,
                "Deadlock found when trying to get lock; try restarting transaction",
            )
        return real_save(self, *args, **kwargs)

    monkeypatch.setattr(OxpullWorkflowNode, "save", save_or_deadlock)
    return calls


def test_add_reissues_its_own_transaction_after_a_deadlock(monkeypatch):
    """
    Port of test_add_reissues_its_own_transaction_after_a_deadlock. add()
    owns its transaction, so losing a deadlock leaves nothing behind and
    the whole add() (enqueue, workflow row claim, node, edge) can be
    reissued instead of exposing the raw OperationalError(1213).
    """
    require_mysql("the reissue is InnoDB's 1213")
    flow = workflows.create("victim")
    root = flow.add("root", tasks.step, "root")
    calls = node_insert_deadlocks_once(monkeypatch)

    child = flow.add("child", tasks.step, "child", depends_on="root")

    assert len(calls) == 2
    assert OxTask.objects.count() == 2
    node = OxpullWorkflowNode.objects.get(workflow_id=flow.id, name="child")
    assert str(node.task_id) == child.id
    assert (
        OxpullWorkflowEdge.objects.filter(
            upstream_task_id=root.id, downstream_task_id=child.id
        ).count()
        == 1
    )


def test_add_inside_a_callers_transaction_leaves_the_deadlock_to_the_caller(
    monkeypatch,
):
    """
    Port of the test_batches test of the same name, and the guard on the
    reissue above. Inside a caller's atomic block a deadlock has rolled
    back the caller's work too, and only the caller can redo it, so add()
    propagates the error after one attempt, on every database, and leaves
    no task, node or edge of its own behind. A reissue that ignored who
    owns the transaction passes the test above and fails this one.
    """
    flow = workflows.create("ambient")
    flow.add("root", tasks.step, "root")
    calls = node_insert_deadlocks_once(monkeypatch)

    with pytest.raises(OperationalError), transaction.atomic():
        flow.add("child", tasks.step, "child", depends_on="root")

    assert len(calls) == 1
    assert OxTask.objects.count() == 1
    assert set(flow.node_ids()) == {"root"}
    assert not OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).exists()


# -- adjacent: the reconciler's SEALED claim without CLIENT_FOUND_ROWS ----


@pytest.mark.xfail(
    ON_MYSQL,
    strict=True,
    reason=(
        "On MySQL without CLIENT_FOUND_ROWS, the reconciler never cancels a "
        "failed node's downstream nodes, so the workflow never completes"
    ),
)
@pytest.mark.parametrize("flag", WITHOUT_FOUND_ROWS)
def test_the_reconciler_cancels_a_failed_cone_without_the_found_rows_flag(flag):
    """
    _cancel_one_level opens with a no-op UPDATE under the SEALED
    predicate. Without the found-rows flag, its rowcount is 0 even when
    the predicate matches. Treating that as no match leaves a FAILED
    node's cone uncancelled, the blocked child READY, and the workflow
    incomplete. Build the workflow with Django's own flags to isolate
    cancellation from add()'s handling of the flag. Both configurations
    reach the same rows.
    """
    flow = workflows.create("doomed-no-found-rows")
    flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()

    with client_flag(flag):
        assert drain(flow_worker()) == 3  # root, three attempts
        assert OxTask.objects.get(args=["root"]).status == OxTask.Status.FAILED
        assert reconcile() == 1

    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.DISCARDED
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert row.cancelled_count == 1
