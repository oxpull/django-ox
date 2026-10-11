"""
A node the reconciler cancels must say so through oxpull: why it was
cancelled, which upstream task decided it, and in which generation of its
workflow.

Today _cancel_one_level (workflows.py) moves a doomed cone to DISCARDED
with one queryset UPDATE and records nothing per node. The reconciler logs
a count, and the workflow row stores cancelled_count when it completes.
django-ox maps DISCARDED to TaskResultStatus.FAILED, so the TaskResult a
caller kept from flow.add() reads FAILED with no errors and no attempts,
which is exactly what an operator's discard reads. Nothing tells the two
apart, and nothing says which upstream was to blame.

The record belongs on oxpull's own node row: a cause, the id of the
upstream task that decided the cancellation, and the workflow generation
it happened in (cancel_cause, cancelled_by_task_id and cancelled_generation
on OxpullWorkflowNode, against OxpullWorkflow.generation). The django-ox
row is left as it is. No task_finished is sent and no error entry is
written: django-ox 1.2.0 sends nothing for an operator's discard either,
and the controls at the bottom pin both facts.

What the tests require:

- every node the reconciler cancels carries a cause, the id of a dead
  upstream it depends on, and the workflow's current generation. That
  holds whether the upstream failed on a worker or an operator discarded
  it, and whether the cone is one chain or a fan-out cancelled in one tick.
  Any cause counts as recorded except NULL and the empty string, so a
  cause stored as a choice whose value is 0 is accepted;
- no task_finished is heard for a node the workflow cancelled. A receiver
  is connected for the whole test and must have heard the nodes a worker
  did finish, so its silence about the cancelled ones means something;
- a node the workflow did not cancel carries no cause and no upstream: the
  dead upstream itself, an independent node that succeeded, and a node an
  operator discarded by hand. The last one reads identically through
  django.tasks, so the record keyed by task id is the only thing that
  separates it from a cancelled node;
- the record is stored, not worked out on read. It still stands, unchanged,
  after the failed upstream is retried from the admin and succeeds, and
  after a later reconciler tick. That retry revives nothing;
- progress() counts a node cancelled by an upstream that has since
  succeeded: none while the upstream is FAILED, one once the admin retry
  has made it SUCCESSFUL. The count's name is not fixed; any count
  progress() adds that reads that way is accepted.

A cancelled node that sits further down a chain may name either its dead
parent or the node whose failure started the cancellation; both are
accepted. A LOST upstream is not a case here, because its cone is held
rather than cancelled.

Known limit: every case runs in a workflow's first generation, because
nothing can start a second one until Workflow.retry() exists. So the
generation check rejects a missing generation and one that differs from
the workflow row, but a record that stamps a constant equal to the first
generation passes here. That a later generation is stamped belongs to the
tests that retry a workflow through Workflow.retry().

The module runs with transaction=True, so every enqueue, claim, completion
and reconciler tick commits on its own, anything deferred to commit really
runs, and each record is read as committed.
"""

from typing import NamedTuple

import pytest
from django.core.exceptions import FieldDoesNotExist
from django.tasks import task, task_backends
from django.tasks.base import TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowNode
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import drain, flow_worker, order, stored

pytestmark = pytest.mark.django_db(transaction=True)

COMPLETE = OxpullWorkflow.Status.COMPLETE

# Where a cancellation is recorded: on oxpull's node row, against the
# generation stored on the workflow row.
NODE_RECORD_FIELDS = ("cancel_cause", "cancelled_by_task_id", "cancelled_generation")
WORKFLOW_GENERATION_FIELD = "generation"


def worker_on(alias):
    return OxpullWorker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)


def reconcile_through_a_worker(worker):
    """
    One reconciler tick as production runs it: the task a schedule
    enqueues, claimed and executed by a worker. Top priority, so the
    worker takes it ahead of any READY node.
    """
    result = workflows.reconcile.using(priority=100).enqueue()
    assert worker.run_once() is True
    row = OxTask.objects.get(pk=result.id)
    assert row.status == OxTask.Status.SUCCESSFUL, (row.status, row.errors)


def reconcile_until_complete(flow, worker, ticks=3):
    for _ in range(ticks):
        reconcile_through_a_worker(worker)
        if stored(flow).status == COMPLETE:
            return
    raise AssertionError(f"workflow did not complete: {describe(flow)}")


def status_of(task_id):
    return OxTask.objects.filter(pk=task_id).values_list("status", flat=True).first()


def describe(flow):
    return {name: status_of(pk) for name, pk in flow.node_ids().items()}


# -- reading what oxpull records -----------------------------------------


class NodeRecord(NamedTuple):
    name: str
    task_id: str
    cause: object
    upstream: str | None
    generation: object


def _has_field(model, name):
    try:
        model._meta.get_field(name)
    except FieldDoesNotExist:
        return False
    return True


def _absent(value):
    """NULL and the empty string record nothing. Any other value does, 0 included."""
    return value is None or value == ""


def record_from(name, task_id, cause, upstream, generation):
    return NodeRecord(
        name=name,
        task_id=str(task_id),
        cause=None if _absent(cause) else cause,
        upstream=None if _absent(upstream) else str(upstream),
        generation=generation,
    )


def cancellations(flow):
    """
    What oxpull has committed about cancellation in this workflow: the
    workflow's current generation, and each node's record by node name.
    An empty cause or upstream reads as None.
    """
    missing = [
        f"OxpullWorkflowNode.{name}"
        for name in NODE_RECORD_FIELDS
        if not _has_field(OxpullWorkflowNode, name)
    ]
    if not _has_field(OxpullWorkflow, WORKFLOW_GENERATION_FIELD):
        missing.append(f"OxpullWorkflow.{WORKFLOW_GENERATION_FIELD}")
    assert not missing, (
        "oxpull records nothing about a cancelled node: no cause, no "
        "upstream task id and no generation. Missing: " + ", ".join(missing)
    )
    generation = OxpullWorkflow.objects.values_list(
        WORKFLOW_GENERATION_FIELD, flat=True
    ).get(pk=flow.id)
    rows = OxpullWorkflowNode.objects.filter(workflow_id=flow.id).values_list(
        "name", "task_id", *NODE_RECORD_FIELDS
    )
    records = {row[0]: record_from(*row) for row in rows}
    return generation, records


def assert_cancelled(record, generation, decided_by):
    """
    decided_by maps the task id of each dead upstream allowed to have
    decided this cancellation to its node name.
    """
    assert record.cause is not None, (
        f"{record.name} was cancelled, and no cause is recorded: {record}"
    )
    assert record.upstream in decided_by, (
        f"{record.name} names upstream {record.upstream!r}, not the dead "
        f"upstream that decided it (one of {sorted(decided_by.values())}): {record}"
    )
    assert generation is not None and record.generation == generation, (
        f"{record.name} was cancelled in generation {generation!r} and "
        f"records {record.generation!r}: {record}"
    )


def assert_not_cancelled(record):
    assert record.cause is None and record.upstream is None, (
        f"{record.name} was not cancelled by its workflow, and carries a "
        f"cancellation record: {record}"
    )


# -- task_finished --------------------------------------------------------


@pytest.fixture
def finished_signals():
    """The task ids task_finished is sent for while the test runs."""
    heard: list[str] = []

    def receiver(sender, task_result, **kwargs):
        heard.append(str(task_result.id))

    task_finished.connect(receiver, dispatch_uid="cancellation-record-finished")
    try:
        yield heard
    finally:
        task_finished.disconnect(dispatch_uid="cancellation-record-finished")


def assert_no_finished_signal(heard, silent, live):
    """
    silent and live map node names to task ids. No task_finished may have
    been sent for a silent node. Every live node is one a worker finished,
    so the receiver must have heard it; otherwise it vouches for nothing.
    """
    deaf = sorted(name for name, task_id in live.items() if task_id not in heard)
    if deaf:
        pytest.fail(
            f"setup: the task_finished receiver heard nothing for {deaf}, "
            "which a worker finished, so its silence proves nothing"
        )
    sent = sorted(name for name, task_id in silent.items() if task_id in heard)
    assert not sent, (
        f"task_finished was sent for {sent}, which no worker finished: the "
        "workflow cancelled them, and a cancellation sends no signal"
    )


# -- every cancelled node records why ---------------------------------------


def _kill_root_on_a_worker(flow, ids):
    """Three real attempts on a worker, then FAILED."""
    assert drain(flow_worker()) == 4  # root x3, side
    assert status_of(ids["root"]) == OxTask.Status.FAILED


def _kill_root_by_operator(flow, ids):
    """The django-ox admin's discard, before root ever runs."""
    assert actions.discard(ids["root"]) is True
    assert drain(flow_worker()) == 1  # side
    assert status_of(ids["root"]) == OxTask.Status.DISCARDED


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="A node the reconciler cancels is moved to DISCARDED, and oxpull "
    "records no cause, no upstream task id and no generation for it.",
)
@pytest.mark.parametrize(
    ("root_task", "kill"),
    [
        (tasks.failing_step.using(priority=10), _kill_root_on_a_worker),
        (tasks.step.using(priority=10), _kill_root_by_operator),
    ],
    ids=["root-failed", "root-discarded"],
)
def test_every_node_the_reconciler_cancels_records_its_cause_upstream_and_generation(
    root_task, kill, finished_signals
):
    """
    root -> mid -> leaf, plus an independent node `side` that succeeds.

    mid can only have been decided by root. leaf may name root or mid.
    root and side were not cancelled by the workflow, so they carry no
    record. In the root-discarded case root is an operator's discard,
    which must not read as a workflow cancellation. task_finished is
    heard for side, and for root when a worker failed it, and never for
    mid or leaf.
    """
    flow = workflows.create("cone")
    flow.add("root", root_task, "root")
    flow.add("mid", tasks.step, "mid", depends_on="root")
    flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.add("side", tasks.step, "side")
    flow.seal()
    ids = {name: str(pk) for name, pk in flow.node_ids().items()}

    kill(flow, ids)
    reconcile_until_complete(flow, flow_worker())

    # Preconditions: the cone really was cancelled, and nothing in it ran.
    assert stored(flow).status == COMPLETE, describe(flow)
    assert status_of(ids["mid"]) == OxTask.Status.DISCARDED, describe(flow)
    assert status_of(ids["leaf"]) == OxTask.Status.DISCARDED, describe(flow)
    assert status_of(ids["side"]) == OxTask.Status.SUCCESSFUL, describe(flow)
    assert "mid" not in order() and "leaf" not in order()

    live = {"side": ids["side"]}
    if kill is _kill_root_on_a_worker:
        live["root"] = ids["root"]
    assert_no_finished_signal(
        finished_signals, {"mid": ids["mid"], "leaf": ids["leaf"]}, live
    )

    generation, records = cancellations(flow)
    assert_cancelled(records["mid"], generation, {ids["root"]: "root"})
    assert_cancelled(
        records["leaf"], generation, {ids["root"]: "root", ids["mid"]: "mid"}
    )
    assert_not_cancelled(records["root"])
    assert_not_cancelled(records["side"])


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="When the reconciler cancels a whole fan-out in one tick, none of "
    "the cancelled children records a cause, the upstream that decided it, "
    "or the generation.",
)
@pytest.mark.parametrize(
    ("alias", "failing", "child"),
    [
        ("flows", tasks.failing_step, tasks.step),
        ("flows_deferred", tasks.failing_deferred_step, tasks.deferred_step),
    ],
    ids=["gate", "deferred"],
)
def test_a_whole_fan_out_cancelled_in_one_tick_records_each_child(
    alias, failing, child, finished_signals
):
    """
    One upstream fails; one reconciler tick cancels every child of it.
    task_finished is heard for the failed root and for no child.
    """
    width = 12
    flow = workflows.create("fan-out")
    flow.add("root", failing, "root")
    for index in range(width):
        flow.add(f"child-{index}", child, f"child-{index}", depends_on="root")
    flow.seal()
    ids = {name: str(pk) for name, pk in flow.node_ids().items()}

    worker = worker_on(alias)
    assert drain(worker) == 3  # root x3; every child gated
    assert status_of(ids["root"]) == OxTask.Status.FAILED
    reconcile_through_a_worker(worker)

    children = sorted(name for name in ids if name != "root")
    assert all(status_of(ids[name]) == OxTask.Status.DISCARDED for name in children), (
        describe(flow)
    )
    assert stored(flow).status == COMPLETE, describe(flow)

    assert_no_finished_signal(
        finished_signals,
        {name: ids[name] for name in children},
        {"root": ids["root"]},
    )

    generation, records = cancellations(flow)
    unrecorded = [name for name in children if records[name].cause is None]
    assert not unrecorded, (
        f"{len(unrecorded)} of {width} cancelled children record no cause: "
        f"{[records[name] for name in unrecorded]}"
    )
    for name in children:
        assert_cancelled(records[name], generation, {ids["root"]: "root"})
    assert_not_cancelled(records["root"])


# -- a cancelled node is told apart from an operator's discard ------------


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="A cancelled node and a node an operator discarded by hand read "
    "the same through django.tasks, and oxpull keeps no record by task id "
    "that tells them apart.",
)
def test_a_cancelled_node_is_told_apart_from_an_operators_discard_by_task_id(
    finished_signals,
):
    """
    The handles a caller keeps from flow.add(), read by their task ids.

    Through django.tasks all three read FAILED, finished, no errors, no
    attempts, and they keep reading that way: the record does not go onto
    the django-ox row. No task_finished is heard for any of the three.
    Only oxpull's record separates the two nodes the workflow cancelled
    from the one an operator discarded.
    """
    flow = workflows.create("read")
    root = flow.add("root", tasks.failing_step, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    leaf = flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    by_hand = flow.add("by-hand", tasks.step, "by-hand")
    flow.seal()
    assert actions.discard(by_hand.id) is True
    worker = flow_worker()
    assert drain(worker) == 3  # root x3; mid and leaf gated
    reconcile_through_a_worker(worker)
    assert stored(flow).status == COMPLETE, describe(flow)

    for handle in (mid, leaf, by_hand):
        assert status_of(handle.id) == OxTask.Status.DISCARDED
        handle.refresh()
        via_backend = task_backends["flows"].get_result(str(handle.id))
        for result in (handle, via_backend):
            assert (
                result.status,
                result.is_finished,
                result.errors,
                result.attempts,
            ) == (TaskResultStatus.FAILED, True, [], 0), (handle.id, result)

    assert_no_finished_signal(
        finished_signals,
        {"mid": str(mid.id), "leaf": str(leaf.id), "by-hand": str(by_hand.id)},
        {"root": str(root.id)},
    )

    generation, records = cancellations(flow)
    by_task_id = {record.task_id: record for record in records.values()}
    decided_by_root = {str(root.id): "root"}
    assert_cancelled(by_task_id[str(mid.id)], generation, decided_by_root)
    assert_cancelled(
        by_task_id[str(leaf.id)],
        generation,
        {**decided_by_root, str(mid.id): "mid"},
    )
    assert_not_cancelled(by_task_id[str(by_hand.id)])


# -- the record is stored, not inferred ------------------------------------


@task(backend="flows")
def fails_until_allowed(label):
    """Fails until the test allows it to succeed, as a fixed bug would."""
    tasks._record(label)
    if not tasks.STATE.get(f"{label} may succeed"):
        raise ValueError(f"{label} boom")
    return label


def _root_cancels_mid():
    """root -> mid; root fails for good and a reconciler tick cancels mid."""
    flow = workflows.create("retried")
    root = flow.add("root", fails_until_allowed, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    flow.seal()
    worker = flow_worker()
    assert drain(worker) == 3  # root x3; mid gated
    assert status_of(root.id) == OxTask.Status.FAILED
    reconcile_through_a_worker(worker)
    assert stored(flow).status == COMPLETE, describe(flow)
    assert status_of(mid.id) == OxTask.Status.DISCARDED, describe(flow)
    return flow, root, mid, worker


def _the_admin_retries_root_and_it_succeeds(flow, root, mid, worker):
    tasks.STATE["root may succeed"] = True
    assert actions.retry(root.id) is True
    assert drain(worker) == 1  # root, once more; mid stays DISCARDED
    assert status_of(root.id) == OxTask.Status.SUCCESSFUL, describe(flow)
    reconcile_through_a_worker(worker)
    assert status_of(mid.id) == OxTask.Status.DISCARDED, describe(flow)
    assert "mid" not in order()


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="oxpull keeps no record of why a node was cancelled, so nothing "
    "survives the failed upstream being retried from the admin and succeeding.",
)
def test_the_record_outlives_the_upstream_being_retried_and_succeeding(
    finished_signals,
):
    """
    root fails, the reconciler cancels mid, then an operator retries root
    from the django-ox admin and it succeeds.

    That retry revives nothing: mid stays DISCARDED and never runs, and no
    task_finished is heard for it. The record must still name root, its
    cause and the same generation, so a later reading can say mid was
    cancelled by an upstream that is now SUCCESSFUL. A record worked out
    on read from the upstream's current status would vanish here, and so
    would one that the next reconciler tick clears.
    """
    flow, root, mid, worker = _root_cancels_mid()

    generation, before = cancellations(flow)
    assert_cancelled(before["mid"], generation, {str(root.id): "root"})
    assert_not_cancelled(before["root"])

    _the_admin_retries_root_and_it_succeeds(flow, root, mid, worker)

    assert_no_finished_signal(
        finished_signals, {"mid": str(mid.id)}, {"root": str(root.id)}
    )
    generation_after, after = cancellations(flow)
    assert generation_after == generation, (
        f"an admin retry moved the workflow from generation {generation!r} "
        f"to {generation_after!r}"
    )
    assert after["mid"] == before["mid"], (
        "mid's record changed once root was retried and succeeded: "
        f"before {before['mid']}, after {after['mid']}"
    )


# Counts WorkflowProgress has today. Any other count is one a fix added.
COUNTS_TODAY = frozenset(
    {"total", "pending", "blocked", "succeeded", "failed", "unattributed", "finished"}
)


def added_progress_counts(flow):
    """Every public integer count on progress() that is not one of today's."""
    progress = flow.progress()
    counts = {}
    for name in dir(progress):
        if name.startswith("_") or name in COUNTS_TODAY:
            continue
        value = getattr(progress, name)
        if isinstance(value, int) and not isinstance(value, bool):
            counts[name] = value
    return progress, counts


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="progress() has no count of nodes cancelled by an upstream that has "
    "since succeeded, so a node the admin retry left cancelled does not show "
    "in the workflow's counts.",
)
def test_progress_counts_a_node_cancelled_by_an_upstream_that_has_since_succeeded():
    """
    root fails, the reconciler cancels mid, then the admin retries root and
    it succeeds. Nothing downstream runs, so mid is left cancelled by an
    upstream that is now SUCCESSFUL: work that will not happen unless
    someone acts, and the workflow's counts must say so.

    Some count progress() adds reads 0 while root is FAILED and 1 once root
    has succeeded. Its name is not fixed. A count of every cancelled node
    reads 1 both times and is rejected, and so is a count that forgets the
    cancellation once the upstream succeeds, which reads 0 both times.
    """
    flow, root, mid, worker = _root_cancels_mid()
    progress_before, before = added_progress_counts(flow)

    _the_admin_retries_root_and_it_succeeds(flow, root, mid, worker)

    progress_after, after = added_progress_counts(flow)
    counting = sorted(
        name for name, value in after.items() if value == 1 and before.get(name) == 0
    )
    assert counting, (
        "progress() has no count that goes from 0 to 1 when the upstream of a "
        f"cancelled node succeeds. Added counts before the retry: {before}, "
        f"after: {after}. progress() before: {progress_before}, after: "
        f"{progress_after}"
    )


# -- controls: what holds today, recorded ------------------------------


def _doomed_chain():
    flow = workflows.create("read")
    root = flow.add("root", tasks.failing_step, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    leaf = flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.seal()
    worker = flow_worker()
    assert drain(worker) == 3
    reconcile_through_a_worker(worker)
    assert stored(flow).status == COMPLETE, describe(flow)
    return flow, root, mid, leaf


def test_control_a_worker_written_failure_always_carries_attempts_and_errors():
    """
    The only public tell that exists today. A FAILED written by a worker
    has attempts >= 1 and an error per attempt (django-ox increments
    attempts and appends worker_ids in the claim), so FAILED with
    attempts 0 means "never ran". It does not say why, and an operator's
    discard of a READY task reads exactly the same (next control).
    """
    _flow, root, mid, _leaf = _doomed_chain()
    root.refresh()
    mid.refresh()
    assert (root.status, len(root.errors), root.attempts) == (
        TaskResultStatus.FAILED,
        3,
        3,
    )
    assert (mid.status, mid.is_finished, mid.errors, mid.attempts) == (
        TaskResultStatus.FAILED,
        True,
        [],
        0,
    )


def test_control_django_ox_already_sends_nothing_for_an_operator_discard():
    """
    Pre-existing and documented in django-ox 1.2.0 (docs/monitoring.md,
    actions.py): a discard finishes a result without task_finished. Its
    public reading is identical to a node the reconciler cancelled, which
    is why the tests above ask oxpull for the record rather than
    django.tasks.
    """
    heard = {"enqueued": set(), "finished": set()}

    def on_enqueued(sender, task_result, **kwargs):
        heard["enqueued"].add(str(task_result.id))

    def on_finished(sender, task_result, **kwargs):
        heard["finished"].add(str(task_result.id))

    task_enqueued.connect(on_enqueued, dispatch_uid="discard-enqueued")
    task_finished.connect(on_finished, dispatch_uid="discard-finished")
    try:
        plain = tasks.default_step.enqueue("plain")
        assert actions.discard(plain.id) is True
    finally:
        task_enqueued.disconnect(dispatch_uid="discard-enqueued")
        task_finished.disconnect(dispatch_uid="discard-finished")
    assert str(plain.id) in heard["enqueued"]
    assert str(plain.id) not in heard["finished"]
    plain.refresh()
    assert (plain.status, plain.is_finished, plain.errors, plain.attempts) == (
        TaskResultStatus.FAILED,
        True,
        [],
        0,
    )
