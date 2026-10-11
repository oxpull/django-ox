"""
The workflow reconciler: its backstop, its completion and its cancellation.

The backstop pages through each open workflow in three phases: the edges not
marked satisfied, the nodes whose record may be behind their task row, and the
waiting nodes to release. Its place survives between ticks, it stops at the
high-water marks its pass began with, and it visits workflows round-robin. A
row another transaction holds is queued and passed, and retried on every tick.

Completion records outcomes before it completes, locks the task rows of
FAILED and LOST nodes before it classifies them again, and writes what it
counted under the workflow's generation once.

The cancellation decides only on dead upstream rows it holds locked, or on
missing rows whose record is dead, and it records what it discarded. Its
discard and a release of the same row are each pinned to the epoch their writer
read.

Every write of several node or edge rows locks them in key order first. Those
tests read the statements each path sends.

Shortcuts, each named where it is taken:

- settled_by_hand() writes FAILED onto a task row with one UPDATE. It stands in
  for any writer that records nothing on the node, such as a django-ox worker
  of another version.
- Several tests shrink BACKSTOP_PAGE, VISIT_PAGES or BACKSTOP_BUDGET so a pass
  spans visits with a handful of rows.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import pytest
from django.core.management import call_command
from django.db import connection, connections, transaction
from django.tasks import task
from django.utils import timezone

from django_ox import _waiting, actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import (
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
    OxpullWorkflowRound,
    OxpullWorkflowScan,
)
from oxpull.routing import oxpull_db_alias

from . import isolation, tasks
from .test_workflows import drain, flow_worker, free_worker, order, reconcile, stored
from .test_workflows_callers import DEADLOCK_CODE, conflict

pytestmark = pytest.mark.django_db

READY = OxTask.Status.READY
WAITING = OxTask.Status.WAITING
SUCCESSFUL = OxTask.Status.SUCCESSFUL
FAILED = OxTask.Status.FAILED
DISCARDED = OxTask.Status.DISCARDED
EDGES = OxpullWorkflowScan.Phase.EDGES
NODES = OxpullWorkflowScan.Phase.NODES
RELEASE = OxpullWorkflowScan.Phase.RELEASE

TASK_TABLE = OxTask._meta.db_table.upper()
WORKFLOW_TABLE = OxpullWorkflow._meta.db_table.upper()

needs_row_locks = pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite has no row locks, and its writers serialise",
)


@task(backend="flows")
def flagged_step(label):
    """Fails while its label is flagged, and succeeds once the flag is cleared."""
    tasks._record(label)
    if tasks.STATE.get(f"fail:{label}"):
        raise ValueError(f"{label} fails while flagged")
    return label


# -- helpers -----------------------------------------------------------------


def backstop() -> int:
    return workflows._release_backstop(oxpull_db_alias())


def status(task_id) -> str:
    return OxTask.objects.values_list("status", flat=True).get(pk=task_id)


def record_of(task_id) -> tuple[str | None, int | None]:
    return OxpullWorkflowNode.objects.values_list("outcome", "outcome_epoch").get(
        task_id=task_id
    )


def recorded(results) -> int:
    return OxpullWorkflowNode.objects.filter(
        task_id__in=[result.id for result in results], outcome__isnull=False
    ).count()


def edge_marks(result) -> list[bool]:
    return sorted(
        OxpullWorkflowEdge.objects.filter(downstream_task_id=result.id).values_list(
            "satisfied", flat=True
        )
    )


def scan_phase(flow) -> str | None:
    return (
        OxpullWorkflowScan.objects.filter(workflow_id=flow.id)
        .values_list("phase", flat=True)
        .first()
    )


def skip_queue(flow) -> list[str]:
    row = OxpullWorkflowScan.objects.filter(workflow_id=flow.id).first()
    return [] if row is None else list(row.skipped)


def settled_by_hand(results, to=FAILED) -> None:
    """Shortcut: settle rows the way a writer that records nothing does."""
    OxTask.objects.filter(pk__in=[result.id for result in results]).update(
        status=to, finished_at=timezone.now()
    )


def open_roots(name: str, count: int):
    """An OPEN workflow of `count` nodes with no dependencies."""
    flow = workflows.create(name)
    return flow, [flow.add(f"n{i:02d}", tasks.step, f"n{i:02d}") for i in range(count)]


def on_another_connection(step: Callable[[], Any]) -> Any:
    out: list[Any] = []
    err: list[BaseException] = []

    def run() -> None:
        try:
            out.append(step())
        except BaseException as exc:
            err.append(exc)
        finally:
            connections.close_all()

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(60)
    assert not thread.is_alive(), "the step on another connection never finished"
    if err:
        raise err[0]
    return out[0]


@contextmanager
def holding_node_row(node_pk: int) -> Iterator[None]:
    """Another transaction holds this node row's lock for the block."""
    held = threading.Event()
    done = threading.Event()
    errors: list[BaseException] = []

    def hold() -> None:
        try:
            with transaction.atomic():
                list(
                    OxpullWorkflowNode.objects.select_for_update()
                    .filter(pk=node_pk)
                    .values_list("pk", flat=True)
                )
                held.set()
                done.wait(120)
        except BaseException as exc:
            errors.append(exc)
            held.set()
        finally:
            connections.close_all()

    thread = threading.Thread(target=hold)
    thread.start()
    assert held.wait(30)
    assert not errors, errors
    try:
        yield
    finally:
        done.set()
        thread.join(60)


# -- the backstop --------------------------------------------------------------


def test_the_backstop_records_an_outcome_no_worker_recorded():
    """
    A django-ox worker writes the outcome and nothing else. The backstop
    records it.
    """
    _, (root,) = open_roots("unrecorded", 1)
    assert free_worker().run_once() is True
    row = OxTask.objects.get(pk=root.id)
    assert row.status == SUCCESSFUL
    assert record_of(root.id) == (None, None)

    backstop()
    assert record_of(root.id) == (SUCCESSFUL, row.lease_epoch)
    assert OxpullWorkflowNode.objects.get(task_id=root.id).succeeded is True


def test_an_edge_whose_pruned_upstream_recorded_its_success_is_satisfied_and_released():
    """
    root ran on a django-ox worker, so nothing marked its edge. Its success was
    recorded, and then ox_prune deleted its row. The recorded success is proof
    enough: the backstop marks the edge and releases the child.

    Shortcut: the record is written with record_outcome(), the write the
    backstop's node phase and completion make, so the edge is still unmarked
    when the row goes.
    """
    flow = workflows.create("recorded-then-pruned")
    root = flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    assert free_worker().run_once() is True
    epoch = OxTask.objects.get(pk=root.id).lease_epoch
    node = OxpullWorkflowNode.objects.get(task_id=root.id)
    assert workflows.record_outcome(node.pk, SUCCESSFUL, epoch, oxpull_db_alias())
    call_command("ox_prune", "--older-than", "0", stdout=StringIO())
    assert not OxTask.objects.filter(pk=root.id).exists()
    assert (edge_marks(child), status(child.id)) == ([False], WAITING)

    backstop()
    assert edge_marks(child) == [True]
    assert status(child.id) == READY
    assert flow_worker().run_once() is True
    assert order() == ["root", "child"]


def test_an_edge_whose_upstream_was_pruned_before_anything_recorded_it_holds():
    """The control: a pruned row with no record proves nothing, so the child waits."""
    flow = workflows.create("pruned-unrecorded")
    root = flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    assert free_worker().run_once() is True
    call_command("ox_prune", "--older-than", "0", stdout=StringIO())
    assert not OxTask.objects.filter(pk=root.id).exists()

    for _ in range(3):
        backstop()
    assert record_of(root.id) == (None, None)
    assert (edge_marks(child), status(child.id)) == ([False], WAITING)
    assert flow_worker().claim_one() is None


def test_a_pass_longer_than_a_visit_goes_on_from_where_it_stopped(monkeypatch):
    """
    Five nodes that never succeed, two to a page, one page a visit. Each tick
    records the next page. A visit that began its pass again from the start
    would read the same first page for ever.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_PAGE", 2)
    monkeypatch.setattr(workflows, "VISIT_PAGES", 1, raising=False)
    flow, roots = open_roots("longer-than-a-visit", 5)
    settled_by_hand(roots)

    seen = []
    for _ in range(4):
        backstop()
        seen.append((recorded(roots), scan_phase(flow)))
    assert seen == [(0, NODES), (2, NODES), (4, NODES), (5, RELEASE)]


def test_nodes_added_after_a_pass_began_wait_for_the_next_pass(monkeypatch):
    """
    The pass records its high-water marks when it begins and stops there, so a
    workflow that keeps growing cannot keep one pass going for ever.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_PAGE", 2)
    monkeypatch.setattr(workflows, "VISIT_PAGES", 1, raising=False)
    flow, roots = open_roots("high-water", 3)
    settled_by_hand(roots)
    backstop()  # the edge phase: no edges
    backstop()  # n00 and n01
    late = flow.add("late", tasks.step, "late")
    settled_by_hand([late])

    backstop()  # n02, the last node under the mark
    assert recorded(roots) == 3
    assert record_of(late.id) == (None, None), (
        "a node added after the pass began was read by that pass"
    )

    ticks = 0
    while record_of(late.id) == (None, None) and ticks < 8:
        backstop()
        ticks += 1
    assert record_of(late.id) == (FAILED, 0), f"not recorded after {ticks} more ticks"


def test_edges_added_after_a_pass_began_wait_for_the_next_pass(monkeypatch):
    """
    The same for edges. Three edges from an upstream that succeeded unmarked,
    two to a page. The pass marks two, then an edge added after it began
    waits: the pass's second edge page stops at the mark it began with.

    Shortcut: settled_by_hand() writes SUCCESSFUL, which marks nothing.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_PAGE", 2)
    monkeypatch.setattr(workflows, "VISIT_PAGES", 1, raising=False)
    flow = workflows.create("edge-high-water")
    root = flow.add("root", tasks.step, "root")
    children = [
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="root") for i in range(3)
    ]
    settled_by_hand([root], to=SUCCESSFUL)
    backstop()  # c0 and c1
    assert [edge_marks(child) for child in children] == [[True], [True], [False]]

    other = flow.add("other", tasks.step, "other")
    late = flow.add("late", tasks.step, "late", depends_on="other")
    settled_by_hand([other], to=SUCCESSFUL)
    backstop()  # c2, the last edge under the mark
    assert edge_marks(children[2]) == [True]
    assert edge_marks(late) == [False], (
        "an edge added after the pass began was read by that pass"
    )

    ticks = 0
    while edge_marks(late) == [False] and ticks < 8:
        backstop()
        ticks += 1
    assert edge_marks(late) == [True], f"not marked after {ticks} more ticks"
    assert status(late.id) == READY


def test_a_visit_pages_on_while_its_pages_come_back_full(monkeypatch):
    """
    Five open nodes at two a page is three pages a phase. One visit takes them
    all, within VISIT_PAGES, and the pass ends inside it, so the place it
    stored after its full pages is back at rest.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_PAGE", 2)
    flow, roots = open_roots("busy", 5)
    settled_by_hand(roots)

    backstop()
    assert recorded(roots) == 5
    place = (
        OxpullWorkflowScan.objects.filter(workflow_id=flow.id)
        .values_list("phase", "position", "edge_hwm", "node_hwm")
        .first()
    )
    assert place == (EDGES, 0, None, None)


def test_a_tick_that_spends_its_budget_leaves_the_next_workflow_to_the_next_tick(
    monkeypatch,
):
    """
    With no budget each tick runs its one guaranteed page, so ticks take turns
    over the workflows, and each one gets its visit in every round.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_BUDGET", 0.0, raising=False)
    first, (a,) = open_roots("first", 1)
    second, (b,) = open_roots("second", 1)
    settled_by_hand([a, b])
    in_order = sorted([first.id, second.id])

    positions = []
    for _ in range(4):
        backstop()
        positions.append(
            OxpullWorkflowRound.objects.values_list("workflow_id", flat=True).first()
        )
    assert positions == [in_order[0], in_order[1], in_order[0], in_order[1]]
    assert (record_of(a.id)[0], record_of(b.id)[0]) == (FAILED, FAILED)


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_node_row_another_transaction_holds_is_queued_and_passed(monkeypatch):
    """
    The first node's row is held. The page records the rest, queues the held
    one, and moves past it. Once the lock is gone the queue gets it.
    """
    monkeypatch.setattr(workflows, "BACKSTOP_PAGE", 2)
    flow, roots = open_roots("held-node", 3)
    settled_by_hand(roots)
    held = OxpullWorkflowNode.objects.get(task_id=roots[0].id)

    with holding_node_row(held.pk):
        backstop()
        records = [record_of(root.id)[0] for root in roots]
        queued = skip_queue(flow)
    assert records == [None, FAILED, FAILED]
    assert queued == [str(roots[0].id)]

    backstop()
    assert record_of(roots[0].id) == (FAILED, 0)
    assert skip_queue(flow) == []


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_node_row_held_across_ticks_is_tried_every_tick_and_others_are_served():
    """
    While a node row stays held, every tick tries it again with a lock wait up
    to its bound, and a timed-out wait keeps it queued. Another workflow's node
    is recorded in the same ticks. Once the lock is gone, the node is recorded.
    """
    flow, (a,) = open_roots("held-across-ticks", 1)
    settled_by_hand([a])
    _, (b,) = open_roots("served-meanwhile", 1)
    held = OxpullWorkflowNode.objects.get(task_id=a.id)
    waited = []

    with holding_node_row(held.pk):
        backstop()
        assert skip_queue(flow) == [str(a.id)]
        settled_by_hand([b])
        for _ in range(2):
            started = time.monotonic()
            backstop()
            waited.append(time.monotonic() - started)
            assert skip_queue(flow) == [str(a.id)]
            assert record_of(a.id) == (None, None)
        assert record_of(b.id) == (FAILED, 0)
    assert min(waited) >= workflows.BACKSTOP_LOCK_TIMEOUT * 0.9, waited

    backstop()
    assert record_of(a.id) == (FAILED, 0)
    assert skip_queue(flow) == []


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_an_edge_row_another_transaction_holds_is_queued_and_passed():
    """
    The edge into c0 is held. The page marks the edge into c1 and releases c1
    without waiting, queues c0, and moves past it. Once the lock is gone, the
    queue gets c0.
    """
    flow = workflows.create("held-edge")
    flow.add("root", tasks.step, "root")
    c0 = flow.add("c0", tasks.step, "c0", depends_on="root")
    c1 = flow.add("c1", tasks.step, "c1", depends_on="root")
    assert free_worker().run_once() is True
    held = OxpullWorkflowEdge.objects.get(downstream_task_id=c0.id)
    done = threading.Event()
    locked = threading.Event()

    def hold() -> None:
        try:
            with transaction.atomic():
                list(
                    OxpullWorkflowEdge.objects.select_for_update()
                    .filter(pk=held.pk)
                    .values_list("pk", flat=True)
                )
                locked.set()
                done.wait(120)
        finally:
            locked.set()
            connections.close_all()

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert locked.wait(30)
        backstop()
        during = (edge_marks(c0), status(c0.id), edge_marks(c1), status(c1.id))
        queued = skip_queue(flow)
    finally:
        done.set()
        thread.join(60)
    assert during == ([False], WAITING, [True], READY)
    assert queued == [str(c0.id)]

    backstop()
    assert (edge_marks(c0), status(c0.id), skip_queue(flow)) == ([True], READY, [])


@pytest.mark.django_db(transaction=True)
def test_the_backstop_releases_only_after_its_marks_commit(monkeypatch):
    flow = workflows.create("marks-then-release")
    flow.add("root", tasks.step, "root")
    child = flow.add("child", tasks.step, "child", depends_on="root")
    assert free_worker().run_once() is True
    assert edge_marks(child) == [False]
    seen = []
    real = _waiting.release_many

    def release_many(rows, *, using):
        rows = list(rows)
        seen.append(on_another_connection(lambda: edge_marks(child)))
        return real(rows, using=using)

    monkeypatch.setattr(workflows._waiting, "release_many", release_many)
    backstop()
    assert seen == [[True]]
    assert status(child.id) == READY


# -- completion ------------------------------------------------------------------


def test_completion_records_outcomes_first_and_completes_on_the_next_tick():
    flow = workflows.create("records-first")
    root = flow.add("root", tasks.step, "root")
    flow.seal()
    assert free_worker().run_once() is True
    alias = oxpull_db_alias()

    assert workflows._complete_if_finished(flow.id, alias) is False
    assert record_of(root.id)[0] == SUCCESSFUL
    assert stored(flow).status == OxpullWorkflow.Status.SEALED

    assert workflows._complete_if_finished(flow.id, alias) is True
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


def test_completion_writes_what_it_counted_under_its_generation():
    flow = workflows.create(
        "counted",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    flow.add("root", tasks.step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.add("bad", tasks.failing_step, "bad")
    flow.seal()
    drain(flow_worker())

    assert reconcile() == 1
    row = stored(flow)
    assert list(row.completions) == ["0"]
    entry = row.completions["0"]
    counts = {
        key: entry[key]
        for key in ("total", "succeeded", "failed", "cancelled", "unattributed")
    }
    assert counts == {
        "total": 3,
        "succeeded": 2,
        "failed": 1,
        "cancelled": 0,
        "unattributed": 0,
    }
    assert entry["callback_task_id"] == str(row.callback_task_id)
    assert entry["success_task_id"] is None
    written = datetime.fromisoformat(entry["completed_at"])
    assert abs((written - row.completed_at).total_seconds()) < 1


def test_completion_never_rewrites_a_generations_entry():
    flow = workflows.create("kept")
    flow.add("root", tasks.step, "root")
    flow.seal()
    drain(flow_worker())
    OxpullWorkflow.objects.filter(pk=flow.id).update(completions={"0": {"kept": True}})

    assert reconcile() == 1
    assert stored(flow).completions == {"0": {"kept": True}}


def test_progress_counts_a_pruned_failure_as_completion_does():
    """
    A failure recorded before ox_prune deleted its row is still a failure, in
    progress() as in the completion that follows it.
    """
    flow = workflows.create("pruned-failure-progress")
    bad = flow.add("bad", tasks.failing_step, "bad")
    flow.add("good", tasks.step, "good")
    flow.seal()
    drain(flow_worker())
    OxTask.objects.filter(pk=bad.id).update(
        finished_at=timezone.now() - timedelta(days=8)
    )
    call_command("ox_prune", "--include-failed", stdout=StringIO())
    assert not OxTask.objects.filter(pk=bad.id).exists()

    progress = flow.progress()
    assert (progress.succeeded, progress.failed, progress.unattributed) == (1, 1, 0)
    assert reconcile() == 1
    assert stored(flow).failed_count == progress.failed


def test_a_node_pruned_before_anything_recorded_it_does_not_hold_completion():
    """
    root ran on an OxpullWorker, which recorded it. leaf ran on a django-ox
    worker, which records nothing, and ox_prune deleted both rows before any
    tick. root still counts as succeeded by its record. leaf is finished and
    its outcome unknown: it does not hold the workflow, and on_success is
    withheld.
    """
    flow = workflows.create(
        "unknown-leaf",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    root = flow.add("root", tasks.step, "root")
    leaf = flow.add("leaf", tasks.step.using(priority=-5), "leaf")
    flow.seal()
    assert flow_worker().run_once() is True
    assert free_worker().run_once() is True
    assert order() == ["root", "leaf"]
    call_command("ox_prune", "--older-than", "0", stdout=StringIO())
    assert not OxTask.objects.filter(pk__in=[root.id, leaf.id]).exists()
    assert (record_of(root.id)[0], record_of(leaf.id)) == (SUCCESSFUL, (None, None))

    assert reconcile() == 1
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE
    assert row.callback_task_id is not None and row.success_task_id is None
    entry = row.completions["0"]
    assert (entry["succeeded"], entry["failed"], entry["unattributed"]) == (1, 0, 1)


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_retry_racing_the_completion_waits_for_it(monkeypatch):
    """
    django-ox's retry takes no workflow lock. It moves the FAILED leaf back to
    READY while completion is deciding. The completion holds the leaf's row,
    so the retry lands after it commits, never inside it.
    """
    flow = workflows.create(
        "retry-races-completion",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    leaf = flow.add("leaf", tasks.failing_step, "leaf")
    flow.seal()
    drain(flow_worker())
    assert status(leaf.id) == FAILED
    step: dict[str, Any] = {}
    real = workflows._enqueue_callback

    def enqueue_callback(*args, **kwargs):
        if "thread" not in step:
            thread = threading.Thread(
                target=lambda: step.setdefault(
                    "retried", on_another_connection(lambda: actions.retry(leaf.id))
                )
            )
            step["thread"] = thread
            thread.start()
            thread.join(2.0)
            step["finished_inside"] = not thread.is_alive()
        return real(*args, **kwargs)

    monkeypatch.setattr(workflows, "_enqueue_callback", enqueue_callback)
    completed = reconcile()
    step["thread"].join(60)
    facts = (
        f"completed={completed} retry finished inside the completion="
        f"{step['finished_inside']} retried={step.get('retried')} "
        f"workflow={stored(flow).status} leaf={status(leaf.id)}"
    )
    assert not (step["finished_inside"] and completed == 1), facts
    assert completed == 1, facts
    assert step.get("retried") is True, facts
    assert status(leaf.id) == READY, facts


# -- cancellation ------------------------------------------------------------------


def test_the_cancellation_records_each_node_it_discards():
    flow = workflows.create("recorded-cancellation")
    flow.add("root", tasks.failing_step, "root")
    mid = flow.add("mid", tasks.step, "mid", depends_on="root")
    leaf = flow.add("leaf", tasks.step, "leaf", depends_on="mid")
    flow.seal()
    drain(flow_worker())

    assert reconcile() == 1
    for node in (mid, leaf):
        epoch = OxTask.objects.get(pk=node.id).lease_epoch
        assert (status(node.id), record_of(node.id)) == (DISCARDED, (DISCARDED, epoch))
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


AFTER_THE_FIRST_READ = "after the first read"
AFTER_THE_LOCK = "after the lock"


class CancellationPause:
    """
    Run `step` once on its own thread and connection, inside the reconciler's
    first cancellation transaction, and wait up to `wait` seconds for it.

    The transaction is found by what it sends: its first statement is the
    UPDATE of the workflow row. AFTER_THE_FIRST_READ is after the first SELECT
    that follows it, AFTER_THE_LOCK after the first SELECT ... FOR UPDATE on the
    task table that follows it.
    """

    def __init__(self, point: str, step: Callable[[], Any], wait: float = 3.0):
        self.point = point
        self.step = step
        self.wait = wait
        self.opened = False
        self.fired = False
        self.finished_inside: bool | None = None
        self.result: Any = None
        self.error: BaseException | None = None
        self.thread: threading.Thread | None = None

    def _run(self) -> None:
        try:
            self.result = self.step()
        except BaseException as exc:
            self.error = exc
        finally:
            connections.close_all()

    def __call__(self, execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        text = " ".join(sql.split()).upper()
        if not self.opened:
            self.opened = text.startswith("UPDATE") and WORKFLOW_TABLE in text
            return result
        if self.fired or not text.startswith("SELECT"):
            return result
        if self.point == AFTER_THE_LOCK and not (
            "FOR UPDATE" in text and TASK_TABLE in text
        ):
            return result
        self.fired = True
        self.thread = threading.Thread(target=self._run)
        self.thread.start()
        self.thread.join(self.wait)
        self.finished_inside = not self.thread.is_alive()
        return result

    def join(self) -> None:
        if self.thread is not None:
            self.thread.join(60)
            assert not self.thread.is_alive()


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_an_upstream_retried_before_the_cancellation_locks_it_keeps_its_child():
    """
    x and y both failed for good, each with a child. The operator retries x
    after the cancellation has read its candidates and before it locks them.
    The lock finds x READY, so only y's child is cancelled, and x's child runs
    once x succeeds.
    """
    flow = workflows.create("retried-before-the-lock")
    x = flow.add("x", flagged_step, "x")
    d = flow.add("d", tasks.step, "d", depends_on="x")
    flow.add("y", tasks.failing_step, "y")
    e = flow.add("e", tasks.step, "e", depends_on="y")
    flow.seal()
    tasks.STATE["fail:x"] = True
    drain(flow_worker())
    assert (status(x.id), status(d.id), status(e.id)) == (FAILED, WAITING, WAITING)

    pause = CancellationPause(AFTER_THE_FIRST_READ, lambda: actions.retry(x.id))
    with connection.execute_wrapper(pause):
        reconcile()
    pause.join()
    after = (status(x.id), status(d.id), status(e.id))
    tasks.STATE["fail:x"] = False
    drain(flow_worker())
    reconcile()
    facts = (
        f"retry finished inside the pass={pause.finished_inside} "
        f"returned={pause.result!r} error={pause.error!r}; after the pass "
        f"(x, d, e)={after}; at the end d={status(d.id)} order={order()} "
        f"workflow={stored(flow).status}"
    )
    assert after[1] != DISCARDED, facts
    assert pause.fired and pause.finished_inside and pause.result is True, facts
    assert after[2] == DISCARDED, facts
    assert status(d.id) == SUCCESSFUL, facts


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_an_upstream_dead_before_the_pass_makes_a_retry_wait_for_the_cancellation():
    """
    The interlock: the retry of an upstream the cancellation holds waits for
    it, so the child is cancelled and the retry lands afterwards.
    """
    flow = workflows.create("dead-before-the-pass")
    x = flow.add("x", tasks.failing_step, "x")
    d = flow.add("d", tasks.step, "d", depends_on="x")
    flow.seal()
    drain(flow_worker())
    assert status(x.id) == FAILED

    pause = CancellationPause(AFTER_THE_LOCK, lambda: actions.retry(x.id))
    with connection.execute_wrapper(pause):
        reconcile()
    pause.join()
    facts = (
        f"fired={pause.fired} retry finished inside the pass="
        f"{pause.finished_inside} returned={pause.result!r} error={pause.error!r} "
        f"x={status(x.id)} d={status(d.id)}"
    )
    assert pause.fired and pause.finished_inside is False, facts
    assert pause.error is None and pause.result is True, facts
    assert (status(d.id), status(x.id)) == (DISCARDED, READY), facts


# The orders that meet at the downstream row: the cancellation's discard and a
# release, each pinned to the lease epoch its writer read. Under the release rule
# no oxpull release moves a node with an unmarked edge from a dead upstream, so
# the release below stands in for one that is not oxpull's (a faulty release or
# an application's UPDATE). A claim that lands first is
# test_workflows_hazard_cancel_cas's case.


def doomed_child(name: str):
    flow = workflows.create(name)
    x = flow.add("x", tasks.failing_step, "x")
    d = flow.add("d", tasks.step, "d", depends_on="x")
    flow.seal()
    drain(flow_worker())
    assert (status(x.id), status(d.id)) == (FAILED, WAITING)
    return flow, x, d, OxTask.objects.get(pk=d.id).lease_epoch


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_release_that_lands_before_the_cancellations_discard_is_cancelled_anyway(
    monkeypatch,
):
    """
    The cancellation has read d WAITING at epoch e. A release moves d to READY at
    e before the discard. The discard is pinned to e, and a release leaves the
    epoch alone, so it still matches READY at e and d is cancelled.
    """
    flow, _, d, epoch = doomed_child("release-first")
    real = _waiting.cancel_many
    landed = []

    def cancel_many(rows, *, using):
        rows = list(rows)
        landed.append(
            on_another_connection(
                lambda: _waiting.release(d.id, lease_epoch=epoch, using=using)
            )
        )
        landed.append(on_another_connection(lambda: status(d.id)))
        return real(rows, using=using)

    monkeypatch.setattr(workflows._waiting, "cancel_many", cancel_many)
    workflows._cancel_unreachable(flow.id, oxpull_db_alias())
    assert landed == [True, READY]
    assert (status(d.id), record_of(d.id)) == (DISCARDED, (DISCARDED, epoch))


def test_a_release_pinned_before_the_cancellation_matches_nothing_after_it():
    """
    Cancel first: a release decided while d waited at epoch e lands after d was
    cancelled at e, and moves nothing.
    """
    flow, _, d, epoch = doomed_child("cancel-first")
    workflows._cancel_unreachable(flow.id, oxpull_db_alias())
    assert status(d.id) == DISCARDED

    assert _waiting.release_many([(d.id, epoch)], using=oxpull_db_alias()) == (0, 1)
    assert (status(d.id), record_of(d.id)) == (DISCARDED, (DISCARDED, epoch))


def test_a_release_pinned_before_a_cancellation_and_a_revival_matches_nothing():
    """
    Cancel and revive first: d is WAITING again at epoch e + 1, so the release
    decided at e moves nothing, and the next cancellation of d records its own
    epoch.
    """
    flow, _, d, epoch = doomed_child("cancel-and-revive-first")
    alias = oxpull_db_alias()
    workflows._cancel_unreachable(flow.id, alias)
    revived = _waiting.revive_many([(d.id, epoch)], using=alias)
    assert revived == {uuid.UUID(str(d.id)): _waiting.Revival.REVIVED}

    assert _waiting.release_many([(d.id, epoch)], using=alias) == (0, 1)
    assert OxTask.objects.values_list("status", "lease_epoch").get(pk=d.id) == (
        WAITING,
        epoch + 1,
    )
    workflows._cancel_unreachable(flow.id, alias)
    assert (status(d.id), record_of(d.id)) == (DISCARDED, (DISCARDED, epoch + 1))


def test_every_id_the_cancellation_names_is_a_uuid():
    """
    The pairs handed to django-ox are (UUID, int), whatever the database returns
    for a UUID column.
    """
    flow = workflows.create("pairs")
    flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    drain(flow_worker())
    handed = []
    real = _waiting.cancel_many

    def cancel_many(rows, *, using):
        rows = list(rows)
        handed.extend(rows)
        return real(rows, using=using)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(workflows._waiting, "cancel_many", cancel_many)
        reconcile()
    assert handed
    assert all(isinstance(pk, uuid.UUID) and isinstance(e, int) for pk, e in handed)


# -- lock order ----------------------------------------------------------------------

NODE_TABLE = OxpullWorkflowNode._meta.db_table
EDGE_TABLE = OxpullWorkflowEdge._meta.db_table


@contextmanager
def every_statement() -> Iterator[list[str]]:
    """Every statement this thread's connection runs in the block, in order."""
    log: list[str] = []

    def wrapper(execute, sql, params, many, context):
        log.append(" ".join(sql.split()))
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        yield log


def ordered_by_key(sql: str, table: str) -> bool:
    """
    Whether a SELECT of `table` is ordered by its key. Django orders by position
    when the key is the first column it selects.
    """
    quoted = connection.ops.quote_name
    key = f"{quoted(table)}.{quoted('id')}"
    return f"FROM {quoted(table)}" in sql and (
        f"ORDER BY {key} ASC" in sql
        or (sql.startswith(f"SELECT {key}") and "ORDER BY 1 ASC" in sql)
    )


def ordered_lock_of(sql: str, table: str) -> bool:
    """Whether a statement is a locking read of `table` in key order."""
    return ordered_by_key(sql, table) and " FOR UPDATE" in sql


def multi_row_writes_lock_in_key_order(log: list[str], table: str) -> int:
    """
    Check that every UPDATE of several rows of `table` by primary key comes after
    a locking read of that table in key order, and return how many there were.
    """
    quoted = connection.ops.quote_name
    update = f"UPDATE {quoted(table)} "
    by_keys = f"{quoted(table)}.{quoted('id')} IN ("
    checked = 0
    for at, sql in enumerate(log):
        if not (sql.startswith(update) and by_keys in sql):
            continue
        checked += 1
        assert any(ordered_lock_of(s, table) for s in log[:at]), (
            f"an UPDATE of {table} by keys took no lock in key order first: {sql}"
        )
    return checked


def _chain_run_by_a_free_worker(name: str):
    flow = workflows.create(name)
    root = flow.add("root", tasks.step, "root")
    children = [
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="root") for i in range(3)
    ]
    assert free_worker().run_once() is True
    return flow, root, children


def _lock_order_hook():
    flow = workflows.create("hook")
    flow.add("root", tasks.step, "root")
    for i in range(3):
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="root")
    with every_statement() as log:
        assert flow_worker().run_once() is True
    return log, EDGE_TABLE


def _lock_order_backstop_edges():
    _chain_run_by_a_free_worker("backstop-edges")
    with every_statement() as log:
        backstop()
    return log, EDGE_TABLE


def _lock_order_backstop_nodes():
    _, roots = open_roots("backstop-nodes", 3)
    settled_by_hand(roots)
    with every_statement() as log:
        backstop()
    return log, NODE_TABLE


def _lock_order_repair_nodes():
    flow = workflows.create("repair-nodes")
    roots = [flow.add(f"n{i}", tasks.step, f"n{i}") for i in range(3)]
    flow.seal()
    settled_by_hand(roots)
    with every_statement() as log:
        workflows._complete_if_finished(flow.id, oxpull_db_alias())
    return log, NODE_TABLE


def _lock_order_repair_edges():
    flow, _, _ = _chain_run_by_a_free_worker("repair-edges")
    flow.seal()
    with every_statement() as log:
        workflows._complete_if_finished(flow.id, oxpull_db_alias())
    return log, EDGE_TABLE


def _lock_order_cancellation():
    flow = workflows.create("cancellation")
    flow.add("root", tasks.failing_step, "root")
    for i in range(3):
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="root")
    flow.seal()
    drain(flow_worker())
    with every_statement() as log:
        workflows._cancel_unreachable(flow.id, oxpull_db_alias())
    return log, NODE_TABLE


def _lock_order_add_recheck():
    flow = workflows.create("add-recheck")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b")
    real = workflows._succeeded_among

    def sees_nothing_yet(task_ids, db_alias):
        # Both upstreams succeed after add() read them and before it commits.
        OxTask.objects.filter(pk__in=task_ids).update(status=SUCCESSFUL)
        return real([], db_alias)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(workflows, "_succeeded_among", sees_nothing_yet)
        with every_statement() as log:
            flow.add("child", tasks.step, "child", depends_on=["a", "b"])
    return log, EDGE_TABLE


LOCK_ORDER_PATHS = {
    "the outcome hook's marks": _lock_order_hook,
    "the backstop's edge page": _lock_order_backstop_edges,
    "the backstop's node page": _lock_order_backstop_nodes,
    "completion's record repair": _lock_order_repair_nodes,
    "completion's marks": _lock_order_repair_edges,
    "the cancellation's records": _lock_order_cancellation,
    "add()'s check after commit": _lock_order_add_recheck,
}


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("path", list(LOCK_ORDER_PATHS))
def test_a_multi_row_write_of_node_or_edge_rows_locks_them_in_key_order_first(path):
    """
    The lock order: several node or edge rows are locked by a locking read in
    key order before they are written, because an UPDATE locks rows in the
    order its plan reads them. Each path here writes several rows of one table,
    and each such UPDATE must follow a locking read of that table ordered by
    its key. The paths run in autocommit, as a worker and the reconciler do.
    """
    log, table = LOCK_ORDER_PATHS[path]()
    assert multi_row_writes_lock_in_key_order(log, table) >= 1, (
        f"{path} wrote no rows of {table} by key, so there was nothing to check"
    )


def test_the_outcome_hook_reads_the_edges_out_of_a_node_in_key_order():
    """
    The hook's read of the edges it marks is ordered by key on every database,
    and is the locking read where the database has row locks.
    """
    flow = workflows.create("hook-read")
    flow.add("root", tasks.step, "root")
    for i in range(3):
        flow.add(f"c{i}", tasks.step, f"c{i}", depends_on="root")
    quoted = connection.ops.quote_name
    with every_statement() as log:
        assert flow_worker().run_once() is True
    reads = [
        sql
        for sql in log
        if sql.startswith(f"SELECT {quoted(EDGE_TABLE)}.")
        and f"{quoted(EDGE_TABLE)}.{quoted('upstream_task_id')} = " in sql
    ]
    assert reads, log
    assert all(ordered_by_key(sql, EDGE_TABLE) for sql in reads), reads
    if connection.features.has_select_for_update:
        assert all(sql.endswith("FOR UPDATE") for sql in reads), reads


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_node_that_fails_again_inside_the_completion_holds_it_back():
    """
    Both nodes failed for good. Completion takes the workflow row, and just then
    an operator retries b and a worker claims it, so completion's read of the
    FAILED rows finds only a and locks only a. b's worker then fails it again.
    Under READ COMMITTED the classification sees b FAILED and recorded, as if
    nothing had happened. Nothing holds b, so an operator's next retry would
    land at once and the workflow would complete with b READY. The completion
    must notice a FAILED row it did not lock and leave the workflow SEALED for
    the next tick.
    """
    flow = workflows.create("fails-again-inside")
    a = flow.add("a", tasks.failing_step, "a")
    b = flow.add("b", tasks.failing_step, "b")
    flow.seal()
    drain(flow_worker())
    assert (status(a.id), status(b.id)) == (FAILED, FAILED)
    steps: dict[str, Any] = {}

    def retry_and_claim_b():
        assert actions.retry(b.id) is True
        claimed = flow_worker().claim_one()
        assert claimed is not None and str(claimed.id) == str(b.id)
        return claimed

    class Pauses:
        def __call__(self, execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            text = " ".join(sql.split()).upper()
            if (
                "cas" not in steps
                and text.startswith("UPDATE")
                and WORKFLOW_TABLE in text
            ):
                steps["cas"] = True
                steps["claimed"] = on_another_connection(retry_and_claim_b)
            elif (
                "cas" in steps
                and "failed_again" not in steps
                and text.startswith("SELECT")
                and "FOR UPDATE" in text
                and TASK_TABLE in text
            ):
                steps["failed_again"] = True
                on_another_connection(
                    lambda: flow_worker().execute(steps["claimed"], inline=True)
                )
                steps["b_after"] = on_another_connection(lambda: status(b.id))
            return result

    with connection.execute_wrapper(Pauses()):
        completed = workflows._complete_if_finished(flow.id, oxpull_db_alias())
    facts = (
        f"completed={completed} steps={sorted(steps)} workflow={stored(flow).status}"
    )
    assert steps.get("failed_again") and steps.get("b_after") == FAILED, facts
    assert completed is False, facts
    assert stored(flow).status == OxpullWorkflow.Status.SEALED, facts

    assert reconcile() == 1
    assert stored(flow).failed_count == 2


def test_add_reads_the_upstream_nodes_it_names_in_key_order():
    """
    add() holds the workflow row, then reads the nodes it depends on. The read is
    ordered by key, and is the locking read where the database has row locks.
    """
    flow = workflows.create("add-read")
    for name in ("c", "a", "b"):
        flow.add(name, tasks.step, name)
    quoted = connection.ops.quote_name
    with every_statement() as log:
        flow.add("child", tasks.step, "child", depends_on=["c", "a", "b"])
    reads = [
        sql
        for sql in log
        if sql.startswith(f"SELECT {quoted(NODE_TABLE)}.") and quoted("name_key") in sql
    ]
    assert len(reads) == 1, log
    assert ordered_by_key(reads[0], NODE_TABLE), reads
    if connection.features.has_select_for_update:
        assert reads[0].endswith("FOR UPDATE"), reads


# -- decisions at both isolation levels -----------------------------------------

LEVELS = ["read committed", "repeatable read"]
NODE_TABLE_UPPER = NODE_TABLE.upper()


@contextmanager
def every_connection_at(level: str) -> Iterator[None]:
    """
    DATABASES OPTIONS["isolation_level"] for every connection opened in the
    block, in any thread, as a project that sets it runs. PostgreSQL and MySQL.
    """
    alias = oxpull_db_alias()
    targets = {
        id(options): options
        for options in (
            connections.settings[alias].setdefault("OPTIONS", {}),
            connections[alias].settings_dict.setdefault("OPTIONS", {}),
        )
    }
    missing = object()
    saved = {
        key: options.get("isolation_level", missing) for key, options in targets.items()
    }
    isolation.outlives_the_transaction(level)
    value: Any = level
    if connection.vendor == "postgresql":
        from psycopg import IsolationLevel

        value = (
            IsolationLevel.REPEATABLE_READ
            if level == "repeatable read"
            else IsolationLevel.READ_COMMITTED
        )
    for options in targets.values():
        options["isolation_level"] = value
    connections[alias].close()
    try:
        assert level_in_use() == level
        yield
    finally:
        for key, options in targets.items():
            if saved[key] is missing:
                options.pop("isolation_level", None)
            else:
                options["isolation_level"] = saved[key]
        connections[alias].close()


def serialization_failures_at(level: str) -> bool:
    """Whether a row changed after the snapshot fails a lock at this level."""
    return connection.vendor == "postgresql" and level == "repeatable read"


def events_named(caplog, name: str) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if getattr(record, "event", "") == name
    ]


def level_in_use() -> str:
    with transaction.atomic(), connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute("SHOW transaction_isolation")
        else:
            cursor.execute("SELECT @@SESSION.transaction_isolation")
        return str(cursor.fetchone()[0]).replace("-", " ").lower()


class RetryInside:
    """
    Run django-ox's retry of one task on another connection, once, right after
    the completing transaction sends the statement `after` picks out, and wait
    up to three seconds for it. The transaction is found by its compare-and-set,
    the UPDATE of the workflow row to COMPLETE.
    """

    def __init__(self, task_id: Any, after: Callable[[str], bool]):
        self.task_id = task_id
        self.after = after
        self.cas = False
        self.fired = False
        self.finished_inside: bool | None = None
        self.result: Any = None
        self.thread: threading.Thread | None = None

    def _run(self) -> None:
        try:
            self.result = actions.retry(self.task_id)
        finally:
            connections.close_all()

    def __call__(self, execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        text = " ".join(sql.split()).upper()
        if self.fired:
            return result
        if not self.cas:
            self.cas = (
                text.startswith("UPDATE")
                and WORKFLOW_TABLE in text
                and OxpullWorkflow.Status.COMPLETE in [str(p) for p in params or ()]
            )
            return result
        if self.after(text):
            self.fired = True
            self.thread = threading.Thread(target=self._run)
            self.thread.start()
            self.thread.join(3.0)
            self.finished_inside = not self.thread.is_alive()
        return result

    def join(self) -> None:
        if self.thread is not None:
            self.thread.join(60)
            assert not self.thread.is_alive()


def the_read_of_dead_rows(text: str) -> bool:
    return text.startswith("SELECT") and TASK_TABLE in text and "FOR UPDATE" not in text


def the_classification(text: str) -> bool:
    return text.startswith("SELECT") and NODE_TABLE_UPPER in text and "GROUP BY" in text


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_retry_after_completions_read_of_the_failed_rows_holds_the_workflow(level):
    """
    The completing transaction has read the FAILED leaf at its epoch and not yet
    locked it. An operator's retry commits in that moment, so the leaf is READY.
    The lock then finds the leaf in another status than the read saw, and the
    completion rolls back. At MySQL REPEATABLE READ the classification that
    follows answers from the read view the first read opened, where the leaf is
    still FAILED, so that comparison is the only thing that stops a COMPLETE
    workflow with a READY leaf. At PostgreSQL REPEATABLE READ the lock raises a
    serialization failure, and the transaction runs again and finds the leaf
    pending.
    """
    flow = workflows.create(
        f"retry-after-the-read-{level}",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    leaf = flow.add("leaf", tasks.failing_step, "leaf")
    flow.add("other", tasks.step, "other")
    flow.seal()
    drain(flow_worker())
    assert status(leaf.id) == FAILED

    pause = RetryInside(leaf.id, the_read_of_dead_rows)
    with every_connection_at(level), connection.execute_wrapper(pause):
        completed = workflows._complete_if_finished(flow.id, oxpull_db_alias())
    pause.join()
    facts = (
        f"level={level} completed={completed} retry finished inside="
        f"{pause.finished_inside} returned={pause.result!r} "
        f"workflow={stored(flow).status} leaf={status(leaf.id)}"
    )
    assert pause.fired and pause.finished_inside and pause.result is True, facts
    assert completed is False, facts
    assert (stored(flow).status, status(leaf.id)) == (
        OxpullWorkflow.Status.SEALED,
        READY,
    ), facts
    assert "complete" not in tasks.STATE, facts


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_retry_of_a_lost_leaf_waits_for_the_completion(level):
    """
    The leaf's worker died and the reaper wrote LOST. The completing transaction
    locks the LOST row with the FAILED ones. An operator's retry of it after the
    classification therefore waits for the commit and lands on a COMPLETE
    workflow, never inside the completion.
    """
    flow = workflows.create(
        f"lost-leaf-{level}",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    leaf = flow.add("leaf", tasks.step, "leaf")
    reaper = flow_worker()
    for _ in range(3):
        claimed = flow_worker().claim_one()
        assert claimed is not None and str(claimed.id) == str(leaf.id)
        assert actions.expire_lease(leaf.id) is True
        assert reaper.reap() == 1
    assert status(leaf.id) == OxTask.Status.LOST
    flow.add("other", tasks.step, "other")
    flow.seal()
    drain(flow_worker())
    assert workflows._complete_if_finished(flow.id, oxpull_db_alias()) is False
    assert record_of(leaf.id)[0] == OxTask.Status.LOST

    pause = RetryInside(leaf.id, the_classification)
    with every_connection_at(level), connection.execute_wrapper(pause):
        completed = workflows._complete_if_finished(flow.id, oxpull_db_alias())
    pause.join()
    facts = (
        f"level={level} completed={completed} retry finished inside="
        f"{pause.finished_inside} returned={pause.result!r} "
        f"workflow={stored(flow).status} leaf={status(leaf.id)}"
    )
    assert pause.fired and pause.result is True, facts
    assert pause.finished_inside is False, facts
    assert completed is True, facts
    assert (stored(flow).status, status(leaf.id)) == (
        OxpullWorkflow.Status.COMPLETE,
        READY,
    ), facts


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_an_upstream_retried_before_the_lock_keeps_its_child_at_either_level(
    caplog, level
):
    """
    The retry lands after the cancellation's first read and before its lock, as
    in the test above, at a level a project sets. The cone is taken from the
    upstreams the lock found dead and nothing else. At MySQL REPEATABLE READ a
    fresh read of the edges answers from the read view the first read opened,
    where x is still FAILED, and would cancel x's child. At PostgreSQL
    REPEATABLE READ the lock raises a serialization failure, and the
    cancellation runs its transaction again in the same tick.
    """
    flow = workflows.create(f"retried-before-the-lock-{level}")
    x = flow.add("x", flagged_step, "x")
    d = flow.add("d", tasks.step, "d", depends_on="x")
    flow.add("y", tasks.failing_step, "y")
    e = flow.add("e", tasks.step, "e", depends_on="y")
    flow.seal()
    tasks.STATE["fail:x"] = True
    drain(flow_worker())
    assert (status(x.id), status(d.id), status(e.id)) == (FAILED, WAITING, WAITING)

    pause = CancellationPause(AFTER_THE_FIRST_READ, lambda: actions.retry(x.id))
    with (
        every_connection_at(level),
        caplog.at_level("INFO", logger="oxpull"),
        connection.execute_wrapper(pause),
    ):
        reconcile()
    pause.join()
    failures = events_named(caplog, "workflow_reconcile_failed")
    reissued = events_named(caplog, "workflow_reconcile_conflict_retry")
    after = (status(x.id), status(d.id), status(e.id))
    facts = (
        f"level={level} retry finished inside the pass={pause.finished_inside} "
        f"returned={pause.result!r} error={pause.error!r} (x, d, e)={after} "
        f"failures={failures} reissued={reissued}"
    )
    assert pause.fired and pause.finished_inside and pause.result is True, facts
    assert after == (READY, WAITING, DISCARDED), facts
    assert failures == [], facts
    if serialization_failures_at(level):
        assert len(reissued) == 1, facts


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_record_repair_that_waited_on_another_writer_runs_again(caplog, level):
    """
    Completion repairs a record a writer of another version left behind. Another
    transaction is writing that same record and holds the node row. The repair's
    lock waits for it. At PostgreSQL REPEATABLE READ the lock then raises a
    serialization failure, because the row changed after the repair's snapshot.
    The repair runs its transaction again, finds the record written, and the
    tick goes on. Nothing is logged as a failed workflow, and the next tick
    completes it.
    """
    flow = workflows.create(f"repair-waits-{level}")
    root = flow.add("root", tasks.step, "root")
    flow.seal()
    settled_by_hand([root])
    epoch = OxTask.objects.get(pk=root.id).lease_epoch
    node_pk = OxpullWorkflowNode.objects.get(task_id=root.id).pk
    held = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def write_the_same_record() -> None:
        try:
            with transaction.atomic():
                assert workflows.record_outcome(
                    node_pk, FAILED, epoch, oxpull_db_alias()
                )
                held.set()
                release.wait(60)
        except BaseException as exc:
            errors.append(exc)
            held.set()
        finally:
            connections.close_all()

    def commit_the_writer_while_the_repair_waits(execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        if (
            not release.is_set()
            and text.startswith("SELECT")
            and NODE_TABLE_UPPER in text
            and "FOR UPDATE" in text
            and "SKIP LOCKED" not in text
        ):
            threading.Timer(0.5, release.set).start()
        return execute(sql, params, many, context)

    with every_connection_at(level):
        writer = threading.Thread(target=write_the_same_record)
        writer.start()
        assert held.wait(30) and not errors, errors
        try:
            with (
                caplog.at_level("INFO", logger="oxpull"),
                connection.execute_wrapper(commit_the_writer_while_the_repair_waits),
            ):
                reconcile()
        finally:
            release.set()
            writer.join(60)
    failures = events_named(caplog, "workflow_reconcile_failed")
    reissued = events_named(caplog, "workflow_reconcile_conflict_retry")
    assert not errors, errors
    assert failures == [], failures
    if serialization_failures_at(level):
        assert len(reissued) == 1, reissued
    assert record_of(root.id) == (FAILED, epoch)
    assert reconcile() == 1
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


# -- deadlocks -------------------------------------------------------------------


def lose_a_deadlock_once(monkeypatch, name: str) -> list[int]:
    """
    Make the first call of workflows.<name>, one attempt of an owned
    transaction, lose a deadlock with this database's own code before it sends
    anything: InnoDB's 1213 on MySQL, 40P01 on PostgreSQL. The server would
    have rolled the transaction back.
    """
    real = getattr(workflows, name)
    calls: list[int] = []

    def attempt(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise conflict(DEADLOCK_CODE[connection.vendor])
        return real(*args, **kwargs)

    monkeypatch.setattr(workflows, name, attempt)
    return calls


@needs_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_record_repair_that_loses_a_deadlock_runs_again(monkeypatch, caplog):
    """
    The record repair owns its transaction. One that loses a deadlock runs it
    again, records the outcome no worker recorded, and the next tick completes
    the workflow.
    """
    flow = workflows.create("repair-deadlock")
    root = flow.add("root", tasks.step, "root")
    flow.seal()
    settled_by_hand([root])
    calls = lose_a_deadlock_once(monkeypatch, "_repair_records_once")

    with caplog.at_level("INFO", logger="oxpull"):
        assert workflows._complete_if_finished(flow.id, oxpull_db_alias()) is False

    conflicts = [
        record.conflict
        for record in caplog.records
        if getattr(record, "event", "") == "workflow_reconcile_conflict_retry"
    ]
    assert (len(calls), conflicts) == (2, ["deadlock"])
    assert record_of(root.id)[0] == FAILED
    assert reconcile() == 1
    assert stored(flow).status == OxpullWorkflow.Status.COMPLETE


def failed_upstream_with_a_child_below_it(label: str):
    """
    A sealed workflow whose FAILED x has a WAITING child d whose key sorts below
    x's. Task ids are random, so children are added until one does, in a new
    workflow each time eight children did not. Earlier workflows stay OPEN and
    are never reconciled here.
    """
    for attempt in range(8):
        flow = workflows.create(f"{label}-{attempt}")
        x = flow.add("x", tasks.failing_step, f"{label}-{attempt}-x")
        for i in range(8):
            d = flow.add(f"d{i}", tasks.step, f"{label}-{attempt}-d{i}", depends_on="x")
            if uuid.UUID(str(d.id)) < uuid.UUID(str(x.id)):
                flow.seal()
                drain(flow_worker())
                assert (status(x.id), status(d.id)) == (FAILED, WAITING)
                return flow, x, d
    raise AssertionError("no child sorted below its upstream")


DEADLOCK_WEIGHT_TABLE = "oxpull_test_deadlock_weight"


@needs_row_locks
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", LEVELS)
def test_a_cancellation_that_loses_a_real_deadlock_runs_again(caplog, level):
    """
    The cancellation holds FAILED x's row and next locks its child d, whose key
    is lower. Another transaction took django-ox's ordered locking read of d and
    x together, so it holds d and waits for x. That is a deadlock, and the
    database picks the cancellation as its victim. On MySQL the other transaction
    first writes rows, so InnoDB rolls back the lighter cancellation. On
    PostgreSQL it waits longer before it looks for the cycle, so the
    cancellation finds it first. The cancellation owns its transaction, so it
    runs it again once the other one ends, and d is cancelled and recorded.
    """
    flow, x, d = failed_upstream_with_a_child_below_it(f"deadlock-{level}")
    if connection.vendor == "mysql":
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {DEADLOCK_WEIGHT_TABLE}")
            cursor.execute(f"CREATE TABLE {DEADLOCK_WEIGHT_TABLE} (id INT PRIMARY KEY)")
    other: dict[str, Any] = {}

    class RollBack(Exception):
        pass

    def lock_d_then_x() -> None:
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    if connection.vendor == "postgresql":
                        cursor.execute("SET LOCAL deadlock_timeout = '20s'")
                    else:
                        cursor.executemany(
                            f"INSERT INTO {DEADLOCK_WEIGHT_TABLE} (id) VALUES (%s)",  # noqa: S608
                            [(i,) for i in range(300)],
                        )
                other["locked"] = list(
                    OxTask.objects.filter(pk__in=[x.id, d.id])
                    .order_by("pk")
                    .select_for_update()
                    .values_list("pk", flat=True)
                )
                raise RollBack
        except RollBack:
            pass

    pause = CancellationPause(AFTER_THE_LOCK, lock_d_then_x, wait=1.0)
    try:
        with (
            every_connection_at(level),
            caplog.at_level("INFO", logger="oxpull"),
            connection.execute_wrapper(pause),
        ):
            workflows._cancel_unreachable(flow.id, oxpull_db_alias())
        pause.join()
    finally:
        if connection.vendor == "mysql":
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {DEADLOCK_WEIGHT_TABLE}")
    conflicts = [
        record.conflict
        for record in caplog.records
        if getattr(record, "event", "") == "workflow_reconcile_conflict_retry"
    ]
    facts = (
        f"level={level} fired={pause.fired} inside={pause.finished_inside} "
        f"error={pause.error!r} locked={other.get('locked')} d={status(d.id)} "
        f"conflicts={conflicts}"
    )
    assert pause.fired and pause.finished_inside is False, facts
    assert pause.error is None and len(other["locked"]) == 2, facts
    assert conflicts == ["deadlock"], facts
    assert status(d.id) == DISCARDED, facts
    assert record_of(d.id)[0] == DISCARDED, facts
    assert status(x.id) == FAILED, facts
