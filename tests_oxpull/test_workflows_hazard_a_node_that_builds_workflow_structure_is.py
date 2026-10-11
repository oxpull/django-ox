"""
A node that builds workflow structure is not re-executable.

Nothing refuses building a workflow from inside a node, and the nodes of
an OPEN workflow already run, so the fan-out-after-discovery shape is
invited: extract runs, finds N items, and adds N transforms that depend
on itself. Workflow.add(), Workflow.seal() and workflows.create() each
commit in their own transaction, whatever lease the calling node holds.

django-ox runs a task at least once. A node body runs again when:

- lease_lost: its lease is taken while the body is still running (a
  stall past LOCK_TIMEOUT, or django_ox.actions.expire_lease, which is
  documented as "It does not stop the task"). The reaper requeues the
  row with the epoch bumped, and the fence refuses execution 1's own
  SUCCESSFUL write, although its body returned normally.
- raised: the body raises after its structure committed, and the attempt
  is retried like any other.

Execution 2 then meets what execution 1 committed. add() raises
WorkflowNodeExists, or WorkflowSealed if execution 1 also sealed, so
every remaining attempt fails and the node ends FAILED. The reconciler
then discards the child execution 1 added, because its only upstream is
FAILED. A node that calls create() starts a second sub-workflow of the
same name instead; both run in full and both fire their callbacks.

Everything runs through the production path. A real OxpullWorker claims
and executes the node, on a thread when its lease is to be lost. The
lease is expired with the public action and taken by a second worker's
real reaper. Every later attempt is claimed and executed by a worker, and
the reconciler runs as the task a SCHEDULES entry enqueues, claimed by a
gating worker. No task row is written by hand.

Each test asserts the re-executable outcome: the node whose body
succeeded is SUCCESSFUL, the structure it built exists once and runs
once, and the workflow completes clean. A fix that instead refuses
building structure from inside a node would fail these at their
preconditions and needs its own tests.
"""

import threading
import time

import pytest
from django.db import connections
from django.tasks import task

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, order, stored

pytestmark = pytest.mark.django_db(transaction=True)

WAIT = 30.0

REEXECUTIONS = ["lease_lost", "raised"]

# label -> the node's controls. Module state, because a worker thread
# resolves the task by its dotted path and must reach the Events the test
# holds.
_CONTROL: dict[str, dict] = {}


@pytest.fixture(autouse=True)
def _control():
    _CONTROL.clear()
    yield
    for ctl in _CONTROL.values():
        ctl["release"].set()
    _CONTROL.clear()


def _arm(label, mode):
    _CONTROL[label] = {
        "mode": mode,
        "calls": 0,
        "bodies_completed": 0,
        "raised": [],
        "started": threading.Event(),
        "release": threading.Event(),
        "released": None,
    }


def _enter(label):
    _CONTROL[label]["calls"] += 1


def _after_structure(label):
    """
    Execution 1 only: outlive the lease, or fail transiently. The
    structure it built is already committed either way.
    """
    ctl = _CONTROL[label]
    if ctl["calls"] == 1:
        if ctl["mode"] == "lease_lost":
            ctl["started"].set()
            ctl["released"] = ctl["release"].wait(timeout=60)
        else:
            raise RuntimeError(
                f"{label}: transient failure after its structure committed"
            )
    ctl["bodies_completed"] += 1
    tasks._record(label)
    return label


def _raising(label, exc):
    _CONTROL[label]["raised"].append(type(exc).__name__)


@task(backend="flows")
def grafting_step(workflow_id, label, seal_in_body):
    """Discovers its work, then adds a child that depends on itself."""
    _enter(label)
    try:
        flow = workflows.get(workflow_id)
        flow.add(f"{label}-child", tasks.step, f"{label}-child", depends_on=label)
        if seal_in_body:
            flow.seal()
    except Exception as exc:
        _raising(label, exc)
        raise
    return _after_structure(label)


@task(backend="flows")
def spawning_step(label):
    """Starts a sub-workflow of its own."""
    _enter(label)
    sub = workflows.create(
        f"sub-of-{label}",
        on_success=tasks.record_callback,
        on_success_args=[f"sub-of-{label}-success"],
    )
    sub.add("leaf", tasks.step, f"{label}-leaf")
    sub.seal()
    return _after_structure(label)


def first_execution(root_id, label, mode):
    """
    Run execution 1 of the node through a real worker, and leave the row
    READY for a second execution the way production does.
    """
    ctl = _CONTROL[label]
    if mode == "raised":
        assert flow_worker().run_once() is True
        row = OxTask.objects.get(id=root_id)
        assert row.status == OxTask.Status.READY, row.status
        assert row.attempts == 1
        assert [e["exception_class_path"] for e in row.errors] == [
            "builtins.RuntimeError"
        ]
        return {"epoch_at_claim": row.lease_epoch, "epoch_after": row.lease_epoch}

    executor = flow_worker()
    errors: list[BaseException] = []

    def run():
        try:
            executor.run_once()
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert ctl["started"].wait(WAIT), "execution 1 never reached its structure"
        row = OxTask.objects.get(id=root_id)
        assert row.status == OxTask.Status.RUNNING
        assert row.attempts == 1 < row.max_attempts
        # A 1.2.0 lease: a stored expiry and a claimed epoch.
        assert row.lease_expires_at is not None
        assert row.lease_epoch >= 1
        claimed = row.lease_epoch
        assert actions.expire_lease(root_id) is True
        assert flow_worker().reap() == 1
        row = OxTask.objects.get(id=root_id)
        assert row.status == OxTask.Status.READY
        assert row.lease_epoch == claimed + 1
        assert thread.is_alive(), "execution 1 must outlive its lease"
    finally:
        ctl["release"].set()
        thread.join(WAIT)
    assert not thread.is_alive()
    assert errors == []
    assert ctl["released"] is True, "execution 1 timed out instead of finishing"
    assert ctl["bodies_completed"] == 1
    row = OxTask.objects.get(id=root_id)
    # Execution 1's body returned normally and its SUCCESSFUL write was
    # refused by the fence: the row is still the requeued one.
    assert row.status == OxTask.Status.READY
    assert row.lease_epoch == claimed + 1
    return {"epoch_at_claim": claimed, "epoch_after": row.lease_epoch}


def reconciler_tick():
    """
    One reconciler tick, run the way a SCHEDULES entry runs it: enqueued
    as the task, then claimed and executed by a gating worker.
    """
    result = workflows.reconcile.enqueue()
    runner = flow_worker()
    deadline = time.monotonic() + WAIT
    while OxTask.objects.get(id=result.id).status in (
        OxTask.Status.READY,
        OxTask.Status.RUNNING,
    ):
        assert time.monotonic() < deadline, "no worker ran the reconciler"
        if not runner.run_once():
            time.sleep(0.01)
    row = OxTask.objects.get(id=result.id)
    assert row.status == OxTask.Status.SUCCESSFUL, row.errors
    return row.return_value


def node_rows(flow):
    ids = flow.node_ids()
    rows = {row.id: row for row in OxTask.objects.filter(id__in=list(ids.values()))}
    return {name: rows[tid] for name, tid in ids.items()}


def describe_node(row):
    return (
        f"{row.status} attempts={row.attempts}/{row.max_attempts} "
        f"epoch={row.lease_epoch} "
        f"errors={[e['exception_class_path'] for e in row.errors]}"
    )


def callbacks_enqueued(label):
    return OxTask.objects.filter(
        task_path="tests_oxpull.tasks.record_callback", args=[label]
    ).count()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A node that adds to its own workflow cannot run again: every later "
        "execution raises on the structure the first one committed, so the "
        "node ends FAILED and the child it added is discarded."
    ),
)
@pytest.mark.parametrize("seal", ["seal_afterwards", "seal_in_body"])
@pytest.mark.parametrize("mode", REEXECUTIONS)
def test_a_node_that_grafts_onto_its_own_workflow_survives_running_again(mode, seal):
    flow = workflows.create(
        f"graft-{mode}-{seal}",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    root = flow.add("root", grafting_step, str(flow.id), "root", seal == "seal_in_body")
    _arm("root", mode)

    epochs = first_execution(root.id, "root", mode)
    # Execution 1's structure is committed before anything runs it again.
    assert "root-child" in flow.node_ids()
    if seal == "seal_in_body":
        assert stored(flow).status == OxpullWorkflow.Status.SEALED

    drain(flow_worker())
    ctl = _CONTROL["root"]
    assert ctl["calls"] >= 2, "the node was never run a second time"
    if seal == "seal_afterwards":
        # Whoever seals once discovery is over; here, after the node ran.
        flow.seal()
    reconciler_tick()
    drain(flow_worker())
    reconciler_tick()

    nodes = node_rows(flow)
    row = stored(flow)
    problems = []
    if nodes["root"].status != OxTask.Status.SUCCESSFUL:
        problems.append(
            f"root is {nodes['root'].status}: execution 1 committed its "
            f"structure, and every later execution raised {ctl['raised']}"
        )
    if nodes["root-child"].status != OxTask.Status.SUCCESSFUL:
        problems.append(
            f"root-child, which execution 1 of root added, is "
            f"{nodes['root-child'].status}"
        )
    if order().count("root-child") != 1:
        problems.append(f"root-child ran {order().count('root-child')} time(s)")
    if (
        row.status != OxpullWorkflow.Status.COMPLETE
        or row.failed_count != 0
        or row.cancelled_count != 0
        or row.success_task_id is None
    ):
        problems.append(
            f"workflow {row.status}, failed_count={row.failed_count}, "
            f"cancelled_count={row.cancelled_count}, on_success "
            f"{'withheld' if row.success_task_id is None else 'enqueued'}"
        )
    assert not problems, "\n".join(
        [
            f"re-executed via {mode}, {seal}",
            *problems,
            f"epochs {epochs}",
            f"nodes: { ({n: describe_node(r) for n, r in nodes.items()}) }",
            (
                f"body entered {ctl['calls']} time(s), returned normally "
                f"{ctl['bodies_completed']}; order={order()}"
            ),
            (
                f"callbacks enqueued: complete={callbacks_enqueued('complete')}, "
                f"success={callbacks_enqueued('success')}"
            ),
        ]
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "A node that starts a sub-workflow starts a second one of the same name "
        "when it runs again, and both run in full and fire their callbacks."
    ),
)
@pytest.mark.parametrize("mode", REEXECUTIONS)
def test_a_node_that_starts_a_sub_workflow_starts_exactly_one(mode):
    parent = workflows.create(f"parent-{mode}")
    root = parent.add("root", spawning_step, "root")
    parent.seal()
    _arm("root", mode)

    epochs = first_execution(root.id, "root", mode)
    # Execution 1 created and sealed its sub-workflow before anything runs
    # the node again.
    subs = OxpullWorkflow.objects.filter(name="sub-of-root")
    assert subs.count() == 1
    assert subs.get().status == OxpullWorkflow.Status.SEALED

    drain(flow_worker())
    ctl = _CONTROL["root"]
    assert ctl["calls"] >= 2, "the node was never run a second time"
    reconciler_tick()
    drain(flow_worker())

    root_row = OxTask.objects.get(id=root.id)
    sub_rows = list(subs.order_by("created_at"))
    leaf_runs = order().count("root-leaf")
    success_callbacks = callbacks_enqueued("sub-of-root-success")
    problems = []
    if len(sub_rows) != 1:
        problems.append(
            f"{len(sub_rows)} workflows named 'sub-of-root', started by one "
            "node that succeeded once"
        )
    if leaf_runs != 1:
        problems.append(f"the sub-workflow's leaf ran {leaf_runs} time(s)")
    if success_callbacks != 1:
        problems.append(
            f"the sub-workflow's on_success was enqueued {success_callbacks} time(s)"
        )
    if root_row.status != OxTask.Status.SUCCESSFUL:
        problems.append(f"root is {root_row.status}")
    assert not problems, "\n".join(
        [
            f"re-executed via {mode}",
            *problems,
            f"epochs {epochs}",
            f"root: {describe_node(root_row)}",
            "sub-workflows: "
            + str(
                [
                    (
                        str(s.id)[:8],
                        s.status,
                        s.total_count,
                        s.success_task_id is not None,
                    )
                    for s in sub_rows
                ]
            ),
            (
                f"body entered {ctl['calls']} time(s), returned normally "
                f"{ctl['bodies_completed']}; order={order()}"
            ),
            (
                "record_callback ran for sub success: "
                f"{tasks.STATE.get('sub-of-root-success', 0)}"
            ),
        ]
    )
