"""
Workflows: the test plan from WORKFLOWS-DESIGN.md.

The load-bearing tests are the ones that prove ordering is enforced by
the claim statement and not by anything a process has to remember. Two of
them are written as a matched pair -- the same diamond drained by the
gating worker and by the `django_ox.worker.Worker` that does not carry the gate --
so the guard is proven rather than asserted. If the gate ever stops
reaching the emitted SQL, test_a_worker_without_the_gate_runs_the_diamond
_out_of_order keeps passing and its partner fails, which is the shape of
a rule that holds on two databases and not the third, and the reason the pair
exists.
"""

import copy
import logging
import subprocess
import sys
import threading
import time
import uuid
from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections, transaction
from django.db.models import QuerySet
from django.test import override_settings
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import unique as unique_module
from oxpull import workflows
from oxpull.exceptions import (
    UnknownWorkflowNode,
    WorkflowNodeExists,
    WorkflowSealed,
)
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode
from oxpull.worker import OxpullWorker

from . import tasks
from .racing import THREADS, race, race_all

pytestmark = pytest.mark.django_db


def flow_worker(**kwargs):
    """A worker on the backend that declares WORKFLOWS."""
    return OxpullWorker(
        backend_alias="flows", backoff_initial=0, poll_interval=0.01, **kwargs
    )


def free_worker(**kwargs):
    """
    The `django_ox.worker.Worker`: no dependency gate in its claim.

    Its claim does not filter on backend_name, so it will happily take
    the "flows" rows the gating worker is holding back. That is not a
    contrivance for the test; it is exactly what an install that forgets
    WORKER_CLASS gets, and it is what oxpull.E007 exists to refuse.
    """
    return Worker(backoff_initial=0, poll_interval=0.01, **kwargs)


def drain(worker, limit=50):
    """
    Run tasks until the worker can claim nothing more.

    An empty claim is not proof that the workflow is finished. A worker's
    release of what it finished is best-effort: a release chunk that loses
    two lock conflicts is logged and left to the reconciler, and workers
    racing on one SQLite database do lose them. Django opens a DEFERRED
    transaction, so the release has to upgrade a read transaction to a write
    one, and SQLite refuses that upgrade with SQLITE_BUSY the instant another
    connection holds the write lock, skipping the busy handler rather than
    risking a deadlock. The timeout in tests_oxpull/settings.py never applies, both
    attempts go at once, and the node the release was holding stays WAITING,
    which no claim admits. So every worker comes back empty with work still
    in the workflow. A caller whose count has to be complete finishes on
    settle(), which is the reconciler that every workflow deployment runs.
    """
    ran = 0
    while ran < limit and worker.run_once():
        ran += 1
    return ran


def settle(flow, limit, worker=None):
    """
    One reconciler pass and the drain after it, if the workflow has a node
    left. Returns how many tasks that drain ran.

    `create()` and `seal()` refuse a workflow whose backend has no schedule
    running the reconciler, so this is the deployment the product describes
    and not a second chance the test gives itself. It is called with every
    worker thread joined, so no lock is held and nothing here can lose a
    conflict: one pass releases what an inline release left, and one drain
    runs it. A run whose inline releases all won reaches this with nothing
    pending and it does nothing.

    One pass is enough. A deferred chunk's ids go to their workflow's skip
    queue, which the backstop retries before anything else, and the drain
    after it is the only writer left, so nothing it releases can lose a
    conflict either.

    It lives here rather than beside any one race because three of them end
    on it now, on two backends, which is why the worker is an argument.
    """
    if not flow.progress().pending:
        return 0
    reconcile()
    return drain(worker if worker is not None else flow_worker(), limit)


def order():
    return list(tasks.STATE.get("order", []))


def deferrals(caplog):
    """
    What the product logged about a release it did not complete: a chunk
    that lost both its attempts, or one that raised.

    A race that leaves work for the reconciler must have said so. These are
    the two events that say it, and asserting one of them is present is what
    stops a silently lost wakeup from passing as a documented deferral.
    """
    return [
        record
        for record in caplog.records
        if getattr(record, "event", "")
        in ("workflow_release_deferred", "workflow_release_failed")
    ]


def reconcile():
    """Run one reconciler pass inline, as a scheduled worker would."""
    return workflows._reconcile()


def stored(flow):
    return OxpullWorkflow.objects.get(pk=flow.id)


def diamond(name="diamond", **create_kwargs):
    """
    extract -> {transform-a, transform-b} -> load, with the priorities
    set so that dependency order is the exact reverse of claim order.

    That inversion is deliberate and load-bearing. A DAG declared in
    topological order runs correctly under a worker with no gate at all,
    simply because the claim orders by priority then enqueued_at -- so a
    test built on one proves nothing about the gate. Here the ungated
    claim reaches `load` first, and only the dependency conjunct can
    stop it. Its partner test drains this same DAG with the
    `django_ox.worker.Worker` and asserts it comes out backwards.
    """
    flow = workflows.create(name, **create_kwargs)
    flow.add("extract", tasks.step.using(priority=0), "extract")
    flow.add(
        "transform-a",
        tasks.step.using(priority=5),
        "transform-a",
        depends_on="extract",
    )
    flow.add(
        "transform-b",
        tasks.step.using(priority=5),
        "transform-b",
        depends_on="extract",
    )
    flow.add(
        "load",
        tasks.step.using(priority=10),
        "load",
        depends_on=["transform-a", "transform-b"],
    )
    flow.seal()
    return flow


# -- shape and configuration -------------------------------------------


def test_create_returns_an_open_workflow():
    flow = workflows.create("nightly")
    assert flow.name == "nightly"
    assert flow.status == OxpullWorkflow.Status.OPEN
    assert stored(flow).total_count is None
    assert stored(flow).failed_count is None


def test_get_returns_the_same_workflow():
    flow = workflows.create("fetch")
    assert workflows.get(flow.id).id == flow.id
    assert workflows.get(str(flow.id)).name == "fetch"


def test_get_raises_for_an_unknown_workflow():
    with pytest.raises(OxpullWorkflow.DoesNotExist):
        workflows.get("6c1b3f52-0000-4000-8000-000000000000")


def test_over_long_names_are_rejected_identically_on_every_database():
    with pytest.raises(ValueError, match="128 characters"):
        workflows.create("x" * 129)
    flow = workflows.create("names")
    with pytest.raises(ValueError, match="128 characters"):
        flow.add("y" * 129, tasks.step, "x")


def test_a_callback_made_unique_after_create_is_refused_at_completion(
    monkeypatch, caplog
):
    """
    As batches refuse it, and for the reason batches._reject_unique gives.
    A callback that was an ordinary task at create() and is unique by the
    time the workflow completes must not be enqueued: the workflow stays
    SEALED and the reconciler says which task and why, every tick, until
    the deploy is fixed.
    """
    flow = workflows.create(
        "later", on_complete=tasks.record_callback, on_complete_args=["c"]
    )
    flow.seal()
    monkeypatch.setattr(
        unique_module,
        "_registry",
        unique_module._registry | {"tests_oxpull.tasks.record_callback"},
    )
    with caplog.at_level(logging.ERROR, logger="oxpull.workflows"):
        assert reconcile() == 0
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.SEALED
    assert row.callback_task_id is None
    assert not OxTask.objects.filter(
        task_path="tests_oxpull.tasks.record_callback"
    ).exists()
    assert "'tests_oxpull.tasks.record_callback'" in caplog.text
    assert "unique" in caplog.text


def test_callback_that_does_not_import_fails_at_create():
    with pytest.raises(ImproperlyConfigured, match="Workflow callback"):
        workflows.create("bad", on_complete="tests_oxpull.tasks.nope")


def test_a_backend_without_the_gate_is_refused_at_add():
    """
    The whole product is ordering, so a backend that would not enforce it
    fails on the line that creates the work, not three hours later with a
    healthy-looking queue.
    """
    flow = workflows.create("ungated")
    with pytest.raises(ImproperlyConfigured, match="WORKFLOWS"):
        flow.add("first", tasks.default_step, "first")
    assert OxTask.objects.count() == 0
    assert OxpullWorkflowNode.objects.count() == 0


def test_a_backend_that_declares_workflows_without_a_gating_worker_is_an_error():
    """oxpull.E007: declared but not enforced is the silent failure."""
    from django.tasks import task_backends

    errors = task_backends["flows_ungated"].check()
    assert [error.id for error in errors] == ["oxpull.E007"]
    assert "would run as soon as it was enqueued" in errors[0].msg


def test_a_non_bool_workflows_option_is_an_error():
    from django.tasks import task_backends
    from django.test import override_settings as _override

    with _override(
        TASKS={
            **__import__("django.conf", fromlist=["settings"]).settings.TASKS,
            "flows_ungated": {
                "BACKEND": "oxpull.backend.OxpullBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"WORKFLOWS": "yes"},
            },
        }
    ):
        errors = task_backends["flows_ungated"].check()
    assert [error.id for error in errors] == ["oxpull.E007"]
    assert "must be True or False" in errors[0].msg


def test_depends_on_must_name_a_node_that_exists():
    flow = workflows.create("unknown-dep")
    with pytest.raises(UnknownWorkflowNode):
        flow.add("second", tasks.step, "second", depends_on="first")
    # The refusal rolls the whole add() back, enqueue included.
    assert OxTask.objects.count() == 0


def test_a_duplicate_node_name_is_refused():
    flow = workflows.create("dupe")
    flow.add("first", tasks.step, "a")
    with pytest.raises(WorkflowNodeExists):
        flow.add("first", tasks.step, "b")
    assert OxpullWorkflowNode.objects.count() == 1
    assert OxTask.objects.count() == 1


def test_a_repeated_dependency_is_one_edge():
    flow = workflows.create("repeat")
    flow.add("first", tasks.step, "a")
    flow.add("second", tasks.step, "b", depends_on=["first", "first"])
    assert OxpullWorkflowEdge.objects.count() == 1


def test_add_after_seal_raises_and_enqueues_nothing():
    flow = workflows.create("sealed")
    flow.add("first", tasks.step, "a")
    flow.seal()
    with pytest.raises(WorkflowSealed):
        flow.add("second", tasks.step, "b")
    assert OxTask.objects.count() == 1
    assert stored(flow).total_count == 1


def test_seal_twice_raises():
    flow = workflows.create("twice")
    flow.seal()
    with pytest.raises(WorkflowSealed):
        flow.seal()


# -- the gate ----------------------------------------------------------


def test_the_diamond_runs_in_dependency_order():
    """
    Fan-out then fan-in, one worker, nothing but the claim statement
    deciding what may run.
    """
    flow = diamond()
    assert drain(flow_worker()) == 4

    ran = order()
    assert ran[0] == "extract"
    assert ran[-1] == "load"
    assert sorted(ran[1:3]) == ["transform-a", "transform-b"]
    assert reconcile() == 1
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


def put_back_to_ready(flow, names=("transform-a", "transform-b", "load")):
    """
    Move waiting nodes to READY by hand, their edges still unsatisfied.

    The shape django-ox's retry or an application's own UPDATE leaves, and
    the case the claim's dependency check exists for: nothing released
    these rows, so only the claim can hold them.
    """
    ids = flow.node_ids()
    moved = OxTask.objects.filter(
        pk__in=[ids[name] for name in names], status=OxTask.Status.WAITING
    ).update(status=OxTask.Status.READY)
    assert moved == len(names)


def test_no_worker_claims_a_waiting_node():
    """
    The diamond as add() leaves it: extract READY, everything below it
    WAITING. The `django_ox.worker.Worker`'s claim carries no dependency check, and
    it still takes only extract, because no claim reads a WAITING row.
    """
    flow = diamond("waiting-diamond")
    statuses = OxTask.objects.filter(pk__in=flow.node_ids().values()).values_list(
        "status", flat=True
    )
    assert sorted(statuses) == [
        OxTask.Status.READY,
        OxTask.Status.WAITING,
        OxTask.Status.WAITING,
        OxTask.Status.WAITING,
    ]
    assert drain(free_worker()) == 1
    assert order() == ["extract"]


def test_a_worker_without_the_gate_runs_the_diamond_out_of_order():
    """
    The proof that the claim's check is what holds a READY row back, and
    not the order in which the rows happen to be enqueued.

    The partner of test_the_gate_holds_ready_rows_whose_edges_are_unsatisfied.
    The same DAG with its blocked rows put back to READY, drained by a
    worker whose claim carries no dependency conjunct. It runs `load`
    before either of its parents, which is what the gated worker must
    never do. If this test ever starts passing *and* its partner starts
    failing, the gate has stopped reaching the emitted SQL.
    """
    put_back_to_ready(diamond("ungated-diamond"))
    assert drain(free_worker()) == 4

    # Straight down the priority order, with every dependency violated:
    # the fan-in first, then the transforms, then the root they all
    # needed. Nothing consulted an edge.
    ran = order()
    assert ran == ["load", "transform-a", "transform-b", "extract"]
    # The real assertion: this worker emits no dependency conjunct at all.
    assert free_worker().claim_filter_q() is None


def test_the_gate_holds_ready_rows_whose_edges_are_unsatisfied():
    """
    The same rows, drained by the worker whose claim carries the check. It
    runs extract first and load last. Each outcome write marks the edges it
    satisfies, which is what lets the next level through.
    """
    put_back_to_ready(diamond("gated-ready-diamond"))
    assert drain(flow_worker()) == 4
    ran = order()
    assert ran[0] == "extract"
    assert ran[-1] == "load"


def test_the_gate_holds_a_child_while_a_parent_is_still_pending():
    flow = workflows.create("held")
    flow.add("parent", tasks.step, "parent")
    flow.add("child", tasks.step, "child", depends_on="parent")
    flow.seal()

    worker = flow_worker()
    # The child waits, so no claim can reach it once the parent is taken
    # out of the running.
    OxTask.objects.filter(task_path="tests_oxpull.tasks.step", args=["parent"]).update(
        status=OxTask.Status.RUNNING, locked_by="someone-else"
    )
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.WAITING
    assert worker.claim_one() is None
    assert flow.progress().blocked == 1

    # A SUCCESSFUL written by hand releases nothing on its own. The
    # reconciler's backstop marks the edge and releases the child.
    OxTask.objects.filter(args=["parent"]).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    assert worker.run_once() is False
    reconcile()
    assert worker.run_once() is True
    assert order() == ["child"]


def test_a_task_outside_a_workflow_is_untouched_by_the_gate():
    tasks.step.enqueue("loose")
    assert drain(flow_worker()) == 1
    assert order() == ["loose"]


def _as_uuid(value):
    """
    One spelling for an id, whatever produced it.

    TaskResult.id is a str, the ORM hands back UUID objects, and a raw
    cursor hands back a UUID on PostgreSQL and 32 undashed hex characters
    on SQLite. Comparing sets across those without normalising would make
    this test pass or fail on the storage format rather than on the
    predicate.
    """
    return uuid.UUID(str(value))


def _rows_the_orm_path_allows():
    return {
        _as_uuid(pk)
        for pk in OxTask.objects.filter(status=OxTask.Status.READY)
        .filter(workflows.runnable_filter_q())
        .values_list("id", flat=True)
    }


def _rows_the_sql_path_allows():
    """
    The raw fragment, executed, against the same rows.

    The fragment is written for the single-statement PostgreSQL claim and
    quotes its identifiers the way PostgreSQL does. SQLite accepts the
    same quoting; MySQL does not without ANSI_QUOTES, and MySQL never
    reads this path in production, so there it is unexecutable by
    construction rather than untested by omission. The caller skips it.
    """
    fragment, params = workflows.runnable_filter_sql()
    table = OxTask._meta.db_table
    sql = (
        f'SELECT "id" FROM "{table}" WHERE "status" = %(ox_ready)s'  # noqa: S608
        f"{fragment}"
    )
    with connections["default"].cursor() as cursor:
        cursor.execute(sql, {"ox_ready": OxTask.Status.READY, **params})
        return {_as_uuid(row[0]) for row in cursor.fetchall()}


def test_the_two_claim_paths_agree_on_the_same_rows():
    """
    claim_filter_q and claim_filter_sql are one predicate with two
    implementations, and PostgreSQL is the only database that reads the
    second. A divergence would be a rule that applies on two databases
    and not the third, which no test run short of the third would catch.

    Asserting the fragment's shape would not establish this. A fragment
    can start with " AND NOT EXISTS (", mention both table names, and
    still select different rows. So the two predicates are run against the
    same table, in every state the gate distinguishes, and their result
    sets are compared.

    Every downstream row is put back to READY first, because the gate
    judges READY rows and add() leaves these WAITING. The states are the
    ones where a plausible mis-write diverges: an upstream that succeeded,
    one still pending, one that failed, one whose row is gone with nothing
    recorded (blocks), one whose row is gone after its edge was marked
    satisfied (runs), an edge marked satisfied over an upstream still
    pending (the mark decides), a node with two upstreams where only one
    is done, and a task in no workflow at all.
    """
    fragment, params = workflows.runnable_filter_sql()
    assert fragment.startswith(" AND NOT EXISTS (")
    assert params == {
        "ox_wf_success": OxTask.Status.SUCCESSFUL,
        "ox_wf_unsatisfied": False,
    }
    assert OxpullWorkflowEdge._meta.db_table in fragment
    assert OxTask._meta.db_table in fragment
    # Both features conjoined, in the order the fragment builds them.
    worker = flow_worker()
    assert worker.claim_filter_sql()[0] == fragment
    assert worker.claim_filter_q() is not None

    loose = tasks.step.enqueue("loose")

    def chain(name, label):
        flow = workflows.create(name)
        flow.add("up", tasks.step, f"u{label}")
        down = flow.add("down", tasks.step, f"d{label}", depends_on="up")
        return flow, flow.node_ids()["up"], down

    _, done_up, after_done = chain("succeeded-upstream", 1)
    OxTask.objects.filter(id=done_up).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    _, _, after_pending = chain("pending-upstream", 2)
    _, failed_up, after_failed = chain("failed-upstream", 3)
    OxTask.objects.filter(id=failed_up).update(
        status=OxTask.Status.FAILED, finished_at=timezone.now()
    )
    _, gone_up, after_gone = chain("pruned-upstream", 4)
    OxTask.objects.filter(id=gone_up).delete()
    _, marked_gone_up, after_marked_gone = chain("pruned-after-success", 6)
    OxpullWorkflowEdge.objects.filter(upstream_task_id=marked_gone_up).update(
        satisfied=True
    )
    OxTask.objects.filter(id=marked_gone_up).delete()
    _, marked_pending_up, after_marked_pending = chain("marked-pending", 7)
    OxpullWorkflowEdge.objects.filter(upstream_task_id=marked_pending_up).update(
        satisfied=True
    )

    half = workflows.create("one-of-two")
    half.add("a", tasks.step, "u5a")
    half.add("b", tasks.step, "u5b")
    after_half = half.add("down", tasks.step, "d5", depends_on=["a", "b"])
    OxTask.objects.filter(id=half.node_ids()["a"]).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )

    OxTask.objects.filter(status=OxTask.Status.WAITING).update(
        status=OxTask.Status.READY
    )

    allowed = _rows_the_orm_path_allows()
    # Pin what the predicate is, not only that the two agree: two
    # identically-wrong implementations would agree with each other.
    assert _as_uuid(loose.id) in allowed
    assert _as_uuid(after_done.id) in allowed
    assert _as_uuid(after_marked_gone.id) in allowed
    assert _as_uuid(after_marked_pending.id) in allowed
    assert _as_uuid(after_gone.id) not in allowed
    assert _as_uuid(after_pending.id) not in allowed
    assert _as_uuid(after_failed.id) not in allowed
    assert _as_uuid(after_half.id) not in allowed

    if connections["default"].vendor == "mysql":
        pytest.skip(
            "the fragment quotes identifiers the PostgreSQL way; MySQL "
            "never executes this path"
        )
    assert _rows_the_sql_path_allows() == allowed


# -- atomicity ---------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_a_failing_edge_insert_leaves_no_orphan_task(monkeypatch):
    """
    The invariant the gate rests on: a worker cannot see a node's task
    row without also seeing every edge that gates it. They commit
    together, so a failure anywhere in add() leaves neither.
    """
    flow = workflows.create("atomic")
    flow.add("parent", tasks.step, "parent")
    before = OxTask.objects.count()

    real = QuerySet.bulk_create

    def boom(self, objs, *args, **kwargs):
        if self.model is OxpullWorkflowEdge:
            raise RuntimeError("edge insert failed")
        return real(self, objs, *args, **kwargs)

    monkeypatch.setattr(QuerySet, "bulk_create", boom)
    with pytest.raises(RuntimeError):
        flow.add("child", tasks.step, "child", depends_on="parent")

    assert OxTask.objects.count() == before
    assert OxpullWorkflowNode.objects.filter(name="child").count() == 0


# -- failure and cancellation ------------------------------------------


def test_a_permanently_failing_upstream_cancels_its_whole_cone():
    """
    Three levels, cancelled in one pass rather than one level per tick.
    """
    flow = workflows.create(
        "doomed",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    flow.add("root", tasks.failing_step, "root")
    flow.add("mid", tasks.step, "mid", depends_on="root")
    flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.seal()

    worker = flow_worker()
    # Three attempts, then FAILED; nothing downstream may run meanwhile.
    for _ in range(3):
        assert worker.run_once() is True
    assert OxTask.objects.get(args=["root"]).status == OxTask.Status.FAILED
    assert worker.run_once() is False
    assert order() == ["root", "root", "root"]

    assert reconcile() == 1
    assert (
        OxTask.objects.filter(status=OxTask.Status.DISCARDED)
        .values_list("args", flat=True)
        .count()
        == 2
    )
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert row.failed_count == 1
    assert row.cancelled_count == 2
    assert row.callback_task_id is not None  # on_complete fires
    assert row.success_task_id is None  # on_success does not
    assert flow.progress().unattributed == 2


def test_a_sibling_branch_still_runs_when_one_branch_fails():
    """
    Cancellation is a cone, not a workflow-wide abort. Celery's chord
    leaves the rest of the header running on a failure; so does this, and
    it also refuses to run the fan-in that depended on the failure.
    """
    flow = workflows.create("half-doomed")
    flow.add("root", tasks.step, "root")
    flow.add("good", tasks.step, "good", depends_on="root")
    flow.add("bad", tasks.failing_step, "bad", depends_on="root")
    flow.add("join", tasks.step, "join", depends_on=["good", "bad"])
    flow.seal()

    assert drain(flow_worker()) == 5  # root, good, bad x3
    assert "good" in order()
    assert "join" not in order()

    assert reconcile() == 1
    assert OxTask.objects.get(args=["join"]).status == OxTask.Status.DISCARDED
    assert stored(flow).cancelled_count == 1


def test_a_running_upstream_does_not_cancel_anything():
    """
    Only a node that has settled without succeeding cancels its cone. A
    node that is merely still running is not a verdict.
    """
    flow = workflows.create("in-flight")
    flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    OxTask.objects.filter(args=["root"]).update(status=OxTask.Status.RUNNING)

    assert reconcile() == 0
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.WAITING


def test_an_operator_discarding_a_node_cancels_its_cone():
    """
    A node a person discarded is an upstream that settled without
    succeeding, so the workflow cancels its cone. The cancelled count is
    the workflow's own: the rows its cancellation changed, here one
    (child). root is the operator's and shows only as unattributed.
    """
    flow = workflows.create("discarded")
    root = flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    discarded = OxTask.objects.filter(
        id__in=flow.node_ids().values(), status=OxTask.Status.DISCARDED
    )

    assert actions.discard(root.id) is True
    assert discarded.count() == 1  # the operator's
    assert reconcile() == 1
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.DISCARDED
    assert discarded.count() == 2  # the workflow cancelled one row, child
    row = stored(flow)
    assert row.failed_count == 0
    # The stored cancelled_count must be that same 1, not 2. Completion
    # stores every DISCARDED node today, so the stored value is asserted in
    # test_operator_discards_are_not_counted_as_nodes_the_workflow_cancelled,
    # an expected failure until completion stores the workflow's own count.
    assert flow.progress().unattributed == 2


# -- crash and resume --------------------------------------------------


def test_a_node_reaped_to_lost_cancels_its_cone_and_completes(worker):
    """
    Crash mid-workflow. The reaper settles a dead worker's task through a
    queryset update that emits no signal at all, so anything counting
    task_finished would hang here. LOST says the attempt was never
    observed, and "we do not know it worked" is not "it worked", so the
    cone is cancelled rather than released.
    """
    flow = workflows.create("crashed")
    root = flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    OxTask.objects.filter(id=root.id).update(
        status=OxTask.Status.RUNNING,
        attempts=3,
        max_attempts=3,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    assert worker.reap() == 1
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.LOST

    assert reconcile() == 1
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.DISCARDED
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert row.failed_count == 0
    assert flow.progress().unattributed == 2


def test_a_node_reaped_with_attempts_remaining_resumes_the_workflow(worker):
    """
    The other half of crash-and-resume: a requeued node keeps the
    workflow open, runs, and releases its child with no reconciler help.
    """
    flow = workflows.create("resumed")
    root = flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    OxTask.objects.filter(id=root.id).update(
        status=OxTask.Status.RUNNING,
        attempts=1,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    assert worker.reap() == 1
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.READY
    assert reconcile() == 0
    assert flow.progress().blocked == 1

    assert drain(flow_worker()) == 2
    assert order() == ["root", "child"]
    assert reconcile() == 1


def test_a_retried_node_holds_the_workflow_open():
    flow = workflows.create("retried")
    root = flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()

    worker = flow_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.FAILED

    assert actions.retry(root.id) is True
    assert reconcile() == 0
    assert OxTask.objects.get(args=["child"]).status == OxTask.Status.WAITING
    assert stored(flow).status == OxpullWorkflow.Status.SEALED


def test_reconciling_a_complete_workflow_is_a_no_op():
    """
    A duplicate completion arriving at a workflow that is already
    complete changes nothing and enqueues no second callback.
    """
    flow = diamond(
        "idempotent",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    drain(flow_worker())
    assert reconcile() == 1
    first = stored(flow).callback_task_id

    assert reconcile() == 0
    assert reconcile() == 0
    assert stored(flow).callback_task_id == first
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 1
    )


def test_an_unsealed_workflow_never_completes():
    flow = workflows.create("never-sealed")
    flow.add("only", tasks.step, "only")
    assert drain(flow_worker()) == 1
    assert reconcile() == 0
    assert flow.status == OxpullWorkflow.Status.OPEN


def test_progress_reports_blocked_separately_from_pending():
    flow = diamond("progress")
    progress = flow.progress()
    assert (progress.total, progress.pending, progress.blocked) == (4, 4, 3)

    assert flow_worker().run_once() is True  # extract
    progress = flow.progress()
    assert (progress.total, progress.succeeded, progress.blocked) == (4, 1, 1)


# -- concurrency -------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_workers_racing_to_finish_a_fan_in_run_it_exactly_once(caplog):
    """
    The classic double-fire, and the reason this design does not have one.
    Eight workers finish the leaves of a fan-in at the same instant, and
    each of them releases the join it was holding back. Exactly one worker
    gets the join, because a claim is a compare-and-set on one row and
    carries the gate besides.

    The release is best-effort (see drain), and this is the shape that meets
    the contention: eight completions against one child. Whichever of them
    frees it, the join runs once and last. So the run ends where a
    deployment's would, on settle(), and the two drains are added up.

    Asserted five ways: the fan-in ran once, it ran last, no worker raised,
    nothing was left waiting, and a run that left the reconciler work said
    so in the log rather than losing the wakeup in silence.
    """
    flow = workflows.create("fan-in")
    flow.add("root", tasks.step, "root")
    for index in range(THREADS):
        flow.add(
            f"leaf-{index}", tasks.slow_step, f"leaf-{index}", 0.05, depends_on="root"
        )
    flow.add(
        "join",
        tasks.step,
        "join",
        depends_on=[f"leaf-{index}" for index in range(THREADS)],
    )
    flow.seal()
    assert flow_worker().run_once() is True  # root

    def work():
        worker = flow_worker()
        # A ceiling under race()'s 30 second join. The loop leaves as soon
        # as no row is READY or RUNNING, so a passing run never waits it out.
        deadline = time.monotonic() + 25
        ran = 0
        try:
            while time.monotonic() < deadline:
                if worker.run_once():
                    ran += 1
                    continue
                if not OxTask.objects.filter(
                    status__in=(OxTask.Status.READY, OxTask.Status.RUNNING)
                ).exists():
                    break
                time.sleep(0.005)
        finally:
            connections.close_all()
        return ran

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        results, errors = race(work)
        settled = settle(flow, THREADS + 1)

    assert errors == []
    assert sum(results) + settled == THREADS + 1  # every leaf, and the join
    assert settled == 0 or deferrals(caplog), (
        f"the reconciler had {settled} task(s) to run and the product logged "
        "no deferred or failed release, so a wakeup went missing in silence"
    )
    ran = order()
    assert ran.count("join") == 1
    assert ran[-1] == "join"
    assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == THREADS + 2
    # The backstop is the backstop, not the mechanism: once the deployment's
    # reconciler has run, it has nothing of its own left to do.
    assert workflows._release_backstop("default") == 0
    assert flow.progress().pending == 0


@pytest.mark.django_db(transaction=True)
def test_racing_reconcilers_fire_the_callbacks_once():
    """
    The compare-and-set rowcount enqueues each callback once, the same
    mechanism batches use and the same reason: never check-then-act. The
    enqueued task then runs like any other, which is at least once.
    """
    flow = diamond(
        "raced",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    drain(flow_worker())

    results, errors = race(reconcile)

    assert errors == []
    assert sorted(results) == [0] * (THREADS - 1) + [1]
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 2
    )


@pytest.mark.django_db(transaction=True)
def test_racing_add_and_seal_never_strands_a_node_outside_the_count():
    flow = workflows.create("sealing")

    def add_or_refuse(index):
        def call():
            try:
                flow.add(f"node-{index}", tasks.step, f"node-{index}")
            except WorkflowSealed:
                return "refused"
            return "added"

        return call

    def seal():
        flow.seal()
        return "sealed"

    results, errors = race_all(
        [seal, *[add_or_refuse(index) for index in range(THREADS - 1)]]
    )

    assert errors == []
    added = results.count("added")
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.SEALED
    assert OxpullWorkflowNode.objects.count() == added
    assert OxTask.objects.count() == added
    assert row.total_count == added


@pytest.mark.django_db(transaction=True)
def test_a_retry_cannot_interleave_with_the_cancellation_that_would_discard_it():
    """
    The one race cancellation could have had, and the lock that closes it.

    An operator retries a FAILED upstream at the moment the reconciler is
    deciding to discard its downstream. Without the SELECT ... FOR UPDATE
    on the doomed upstream rows, the reconciler can read FAILED, the
    retry can succeed, the child can become legitimately runnable, and
    the reconciler's UPDATE can then discard it -- and no compare-and-set
    sees the difference, because the child is READY before and after and
    retry() bumps the parent's lease_epoch, not the child's.

    With the lock, retry()'s plain UPDATE must wait for our transaction,
    so the two orders are the only outcomes: the child is discarded and
    the retry lands afterwards, or the retry lands first and the parent
    is not FAILED when we look. Both are asserted; a torn state -- the
    parent successfully retried *and* the child discarded in the same
    pass -- is not reachable and is what would fail here.
    """
    flow = workflows.create("retry-race")
    root = flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()
    for _ in range(3):
        worker.run_once()
    assert OxTask.objects.get(id=root.id).status == OxTask.Status.FAILED

    started = threading.Event()

    def cancel():
        started.set()
        try:
            return workflows._cancel_unreachable(flow.id, "default")
        finally:
            connections.close_all()

    def retry():
        assert started.wait(timeout=10)
        try:
            return actions.retry(root.id)
        finally:
            connections.close_all()

    _, errors = race_all([cancel, retry])
    assert errors == []

    root_row = OxTask.objects.get(id=root.id)
    child_row = OxTask.objects.get(args=["child"])
    if child_row.status == OxTask.Status.DISCARDED:
        # We cancelled; the retry either lost the row or landed after.
        assert root_row.status in (OxTask.Status.FAILED, OxTask.Status.READY)
    else:
        # The retry went first, so nothing was doomed to cancel.
        assert root_row.status == OxTask.Status.READY
        assert child_row.status == OxTask.Status.WAITING


# -- routing -----------------------------------------------------------


def test_split_router_refuses_workflow_operations():
    class SplitRouter:
        def db_for_write(self, model, **hints):
            if model._meta.app_label == "oxpull":
                return "replica"
            return "default"

    with (
        override_settings(DATABASE_ROUTERS=[SplitRouter()]),
        pytest.raises(ImproperlyConfigured, match="one database"),
    ):
        workflows.create("split")


@pytest.mark.django_db(transaction=True)
def test_a_node_added_inside_a_caller_transaction_is_never_visible_alone():
    """
    The gate's invariant, checked from outside: while add() is in flight,
    another connection sees neither the task row nor its edge; once it
    commits it sees both. There is no instant at which the task row is
    claimable without its gate.
    """
    flow = workflows.create("visibility")
    flow.add("root", tasks.step, "root")
    inside = threading.Event()
    looked = threading.Event()
    observed = []

    def adder():
        try:
            with transaction.atomic():
                flow.add("child", tasks.step, "child", depends_on="root")
                inside.set()
                # The transaction stays open until the observer has read
                # both tables, so its reads land mid-transaction however
                # slowly its thread and connection start.
                assert looked.wait(timeout=10)
        finally:
            connections.close_all()
        return "added"

    def observer():
        try:
            assert inside.wait(timeout=10)
            observed.append(
                (
                    OxTask.objects.filter(args=["child"]).exists(),
                    OxpullWorkflowEdge.objects.count(),
                )
            )
        finally:
            looked.set()
            connections.close_all()
        return "observed"

    results, errors = race_all([adder, observer])
    assert errors == []
    assert sorted(results) == ["added", "observed"]
    # Mid-transaction the other connection saw neither half.
    assert observed == [(False, 0)]
    assert OxTask.objects.filter(args=["child"]).exists()
    assert OxpullWorkflowEdge.objects.count() == 1


# -- the reconciler must be scheduled (oxpull.W003) --------------------


def without_the_workflow_reconciler():
    """
    settings.TASKS with every SCHEDULES entry naming the workflow
    reconciler removed. The suite's settings schedule it, as a project
    must before workflows.create() and seal() accept a workflow.
    """
    from django.conf import settings as django_settings

    rebuilt = copy.deepcopy(django_settings.TASKS)
    for params in rebuilt.values():
        schedules = params.get("OPTIONS", {}).get("SCHEDULES")
        if not isinstance(schedules, dict):
            continue
        for name, config in list(schedules.items()):
            if (
                isinstance(config, dict)
                and config.get("task") == workflows.RECONCILE_TASK_PATH
            ):
                del schedules[name]
    return rebuilt


CHECK_SCRIPT = """
import sys
import django
from django.conf import settings

SCHEDULES = {
    "oxpull-workflow-reconcile": {
        "task": "oxpull.workflows.reconcile",
        "cron": "* * * * *",
    },
}
options = {
    "WORKER_CLASS": "oxpull.worker.OxpullWorker",
    "WORKFLOWS": True,
    # The batch reconciler is scheduled in both runs, so oxpull.W002 stays
    # silent and the workflow reconciler is the only thing that can be missing.
    "SCHEDULES": {
        "oxpull-batch-reconcile": {
            "task": "oxpull.batches.reconcile",
            "cron": "* * * * *",
        },
    },
}
if "--scheduled" in sys.argv:
    options["SCHEDULES"].update(SCHEDULES)

settings.configure(
    SECRET_KEY="check-only",
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    INSTALLED_APPS=["django_ox", "oxpull"],
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    TASKS={
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["default"],
            "OPTIONS": options,
        }
    },
)
django.setup()
from django.core.management import execute_from_command_line

execute_from_command_line(["manage.py", "check"])
"""


def _run_check(*args):
    """`manage.py check` in a fresh interpreter, exactly as an operator runs it."""
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", CHECK_SCRIPT, *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_manage_py_check_reports_an_unscheduled_reconciler():
    """
    oxpull.W003, through the real management command.

    Not through backend.check(): this project has a recorded failure
    where a test proved a command shape the package does not ship, and
    the whole point of this check is that it reaches an operator who
    types `manage.py check` and reads the output. The command is what is
    asserted, in a fresh interpreter, with settings that are correct in
    every other respect -- a gating worker, WORKFLOWS declared, nothing
    else wrong -- so the only thing the warning can be about is the
    missing schedule.
    """
    result = _run_check()
    combined = result.stdout + result.stderr
    assert "oxpull.W003" in combined, combined
    assert "oxpull.workflows.reconcile" in combined, combined
    # A warning, not an error: `manage.py check` and every command that
    # runs the checks -- ox_worker included -- must still start.
    assert result.returncode == 0, combined


def test_manage_py_check_is_silent_once_the_reconciler_is_scheduled():
    result = _run_check("--scheduled")
    combined = result.stdout + result.stderr
    assert "oxpull.W003" not in combined, combined
    assert result.returncode == 0, combined
    assert "no issues" in combined, combined


def test_the_reconciler_schedule_may_live_on_another_backend():
    """
    The reconciler is global -- it sweeps every SEALED workflow whatever
    backend enqueued it -- so one schedule anywhere is a correct install,
    and a warning per workflow backend would be noise an operator learns
    to ignore.
    """
    from django.tasks import task_backends

    with override_settings(TASKS=without_the_workflow_reconciler()):
        ids = [m.id for m in task_backends["flows"].check()]
    assert "oxpull.W003" in ids, ids

    scheduled = {
        **without_the_workflow_reconciler(),
        "cron": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["default"],
            "OPTIONS": {
                "SCHEDULES": {
                    "wf": {
                        "task": workflows.RECONCILE_TASK_PATH,
                        "cron": "* * * * *",
                    },
                },
            },
        },
    }
    with override_settings(TASKS=scheduled):
        ids = [m.id for m in task_backends["flows"].check()]
    assert "oxpull.W003" not in ids, ids


def test_the_warning_names_the_path_the_schedule_must_carry():
    """
    The hint is only useful if the string it tells an operator to paste
    is the string the schedule dispatcher resolves. Pin them together.
    """
    from django.tasks import task_backends

    with override_settings(TASKS=without_the_workflow_reconciler()):
        warning = next(
            m for m in task_backends["flows"].check() if m.id == "oxpull.W003"
        )
    assert workflows.RECONCILE_TASK_PATH in warning.hint
    assert workflows.reconcile.module_path == workflows.RECONCILE_TASK_PATH
