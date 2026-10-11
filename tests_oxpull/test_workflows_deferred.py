"""
Releasing waiting workflow nodes, on "flows_deferred".

A node with dependencies is inserted WAITING and moves to READY only once
every edge into it is marked satisfied. That is a blocked -> ready
transition, so most of what is below is about it: who makes it, what happens
when nobody does, and what two workers finishing a fan-in at the same
instant do to each other.

"flows_deferred" still carries the removed OPTIONS["DEFER_BLOCKED_NODES"],
which changes nothing, so every test here also shows the leftover option is
ignored. The helpers are shared with the hazard tests that name this
backend.

The claim still checks the edges. That is the safety story: the release
decides from marks only a writer that saw the upstream succeed can make, and
an extra conjunct in the claim can only remove rows from the candidate set,
so no interleaving here can make a node run early or twice. Every test below
is therefore a liveness test, deliberately.
"""

import contextlib
import logging

import pytest
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .racing import race_all
from .test_workflows import deferrals, settle

pytestmark = pytest.mark.django_db

READY = OxTask.Status.READY
WAITING = OxTask.Status.WAITING


def deferring_worker(**kwargs):
    return OxpullWorker(
        backend_alias="flows_deferred", backoff_initial=0, poll_interval=0.01, **kwargs
    )


def order():
    return list(tasks.STATE.get("order", []))


def run_after(label):
    return OxTask.objects.get(args=[label]).run_after


def status(label):
    return OxTask.objects.get(args=[label]).status


def chain(name="chain"):
    """parent -> child, on the second workflow backend."""
    flow = workflows.create(name)
    flow.add("parent", tasks.deferred_step, "parent")
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    flow.seal()
    return flow


def fan_in(name="fan-in"):
    """{a, b} -> child, the case two workers can finish at the same instant."""
    flow = workflows.create(name)
    flow.add("a", tasks.deferred_step, "a")
    flow.add("b", tasks.deferred_step, "b")
    flow.add("child", tasks.deferred_step, "child", depends_on=["a", "b"])
    flow.seal()
    return flow


# -- the status --------------------------------------------------------


def test_a_blocked_node_waits_and_a_free_node_does_not():
    chain()
    assert (status("parent"), run_after("parent")) == (READY, None)
    assert (status("child"), run_after("child")) == (WAITING, None)


def test_a_waiting_row_is_outside_the_candidate_set_of_every_worker():
    """
    Not merely refused by the gate: absent from the candidate set. The
    ungated `django_ox.worker.Worker` is the sharpest way to say it, because the
    gate is the only thing that ever held it back before.
    """
    chain()
    OxTask.objects.filter(args=["parent"]).update(
        status=OxTask.Status.RUNNING, locked_by="someone-else"
    )
    assert deferring_worker().claim_one() is None
    assert Worker(backoff_initial=0, poll_interval=0.01).claim_one() is None


def test_a_node_whose_parent_already_succeeded_is_released_as_it_is_added():
    """
    add() reads the upstream as SUCCESSFUL, marks the edge and releases the
    node in its own transaction, so nothing is owed a later release.
    """
    flow = workflows.create("late-add")
    flow.add("parent", tasks.deferred_step, "parent")
    assert deferring_worker().run_once() is True
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    flow.seal()
    assert status("child") == READY
    assert deferring_worker().run_once() is True
    assert order() == ["parent", "child"]


def test_a_callers_own_run_after_is_never_overwritten():
    """
    run_after is a public argument and a stronger statement than ours: the
    caller has said when this may run. The node waits with that time on its
    row, and the release keeps it.
    """
    later = timezone.now() + timezone.timedelta(days=1)
    flow = workflows.create("scheduled")
    flow.add("parent", tasks.deferred_step, "parent")
    flow.add(
        "child",
        tasks.deferred_step.using(run_after=later),
        "child",
        depends_on="parent",
    )
    flow.seal()
    assert (status("child"), run_after("child")) == (WAITING, later)
    assert deferring_worker().run_once() is True  # the parent
    assert (status("child"), run_after("child")) == (READY, later)
    assert deferring_worker().run_once() is False


# -- the release -------------------------------------------------------


def test_the_worker_releases_its_dependents_in_the_same_pass():
    """
    No reconciler, no second tick, no waiting: the worker that writes
    SUCCESSFUL releases what that task was holding back, and the very next
    claim takes it.

    This is the inline release on its own, and it is the reason the race
    tests may end on the reconciler without that becoming the only thing
    they prove. One worker on one connection, and no other writer anywhere,
    so no chunk here can lose a lock conflict and the release either happens
    in the pass or the product is broken. The reconciler is never run.
    """
    chain()
    worker = deferring_worker()
    assert worker.run_once() is True
    assert status("child") == READY
    assert worker.run_once() is True
    assert order() == ["parent", "child"]


def test_a_fan_in_child_is_released_only_by_the_last_parent():
    fan_in()
    worker = deferring_worker()
    assert worker.run_once() is True
    assert status("child") == WAITING, "one parent of two must not release the child"
    assert worker.run_once() is True
    assert status("child") == READY
    assert worker.run_once() is True
    assert order()[-1] == "child"


def test_a_failed_parent_releases_nothing():
    flow = workflows.create("doomed")
    flow.add("parent", tasks.failing_deferred_step, "parent")
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    flow.seal()
    worker = deferring_worker()
    for _ in range(10):
        if not worker.run_once():
            break
        # Drop the retry backoff so the attempts are exhausted here rather
        # than in real time.
        OxTask.objects.filter(args=["parent"], status=READY).update(run_after=None)
    assert status("parent") == OxTask.Status.FAILED
    assert status("child") == WAITING
    # And the reconciler cancels it rather than releasing it: a doomed cone
    # is cancellation's business, not the release's.
    workflows._reconcile()
    assert status("child") == OxTask.Status.DISCARDED


def test_a_retrys_backoff_is_not_mistaken_for_a_release():
    """
    The release moves only WAITING rows. A node that failed and is going
    round again is READY with a real backoff time, and a second successful
    write on its upstream must not touch it.
    """
    flow = workflows.create("retrying")
    parent = flow.add("parent", tasks.deferred_step, "parent")
    flow.add("child", tasks.failing_deferred_step, "child", depends_on="parent")
    flow.seal()
    worker = deferring_worker()
    assert worker.run_once() is True  # parent succeeds, child released
    assert status("child") == READY
    assert worker.run_once() is True  # child fails, retries with a backoff
    backoff = run_after("child")
    assert backoff is not None
    OxTask.objects.filter(id=parent.id).update(status=READY)
    assert worker.run_once() is True
    assert run_after("child") == backoff


# -- the backstop ------------------------------------------------------


def test_the_reconciler_releases_what_the_worker_never_saw():
    """
    Worker._write_outcome is the only place django-ox writes SUCCESSFUL, so
    on the normal path the worker's release covers it. This is everything
    else: a status written out of band, a worker that died between the
    outcome write and the release. All of them are bounded by one reconciler
    tick, and that bound is why oxpull.W003 exists.
    """
    chain()
    OxTask.objects.filter(args=["parent"]).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    assert status("child") == WAITING
    assert deferring_worker().run_once() is False
    workflows._reconcile()
    assert status("child") == READY
    assert deferring_worker().run_once() is True
    assert order() == ["child"]


def test_the_backstop_reaches_an_unsealed_workflow():
    """
    The release backstop covers OPEN workflows, unlike the sealed sweep. An
    OPEN workflow's nodes run before it is sealed, so a waiting node in one
    needs the backstop just as much.
    """
    flow = workflows.create("open")
    parent = flow.add("parent", tasks.deferred_step, "parent")
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    OxTask.objects.filter(id=parent.id).update(
        status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
    )
    workflows._reconcile()
    assert status("child") == READY


def test_a_pruned_upstream_that_left_no_record_keeps_its_dependents_waiting():
    """
    A node whose upstream row is gone, with nothing recorded about how that
    upstream finished, is held: it stays WAITING through the reconciler's
    pass. A missing row does not say the upstream succeeded, and this one
    never ran at all, so releasing the node would run work whose input was
    never produced. Holding is stuck work an operator can see; releasing is
    wrongly run work.
    """
    flow = workflows.create("pruned")
    parent = flow.add("parent", tasks.deferred_step, "parent")
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    flow.seal()
    OxTask.objects.filter(id=parent.id).delete()
    assert status("child") == WAITING
    workflows._reconcile()
    assert status("child") == WAITING, (
        f"the reconciler released the child to {status('child')} although its "
        "upstream is gone and nothing recorded that it succeeded"
    )


# -- the transition under contention -----------------------------------


@pytest.mark.django_db(transaction=True)
def test_a_fan_in_released_by_racing_workers_runs_its_child_exactly_once(caplog):
    """
    The transition, hammered.

    Eight workers drain a fan-in whose two parents can be claimed at the
    same instant, so the release runs concurrently on both, against the
    same child. What is asserted is an invariant and not a count: every
    node ran exactly once, in dependency order, and nothing was left
    waiting.

    Correctness never depended on the release: the child's claim is a
    compare-and-set on one row and still carries the gate. Liveness is what
    is under test, and the release is only half of what provides it. It is
    best-effort by design: a chunk that loses two lock conflicts is logged
    and left to the reconciler, and on SQLite it loses them readily, because
    Django opens a DEFERRED transaction and the engine refuses the read-to-
    write upgrade at once rather than waiting out the busy timeout. The
    reconciler is the other half, and create() and seal() refuse a backend
    that runs no schedule for it, so the run ends where a deployment's
    would: the workers drain, then one reconciler pass and the drain after
    it, with every thread joined.

    Two things are asserted about that ending, and together they are
    stronger than asking the backstop to have had nothing to do. The
    workflow is finished, with nothing waiting and nothing for the backstop
    left. And whatever the reconciler had to run, the product said so first:
    a wakeup that went missing without a word fails here. What it must never
    take is a node run twice or out of order, which is asserted over both
    drains. test_the_worker_releases_its_dependents_in_the_same_pass is
    where the inline release is asserted on its own, with no contention.
    """
    tasks.STATE["order"] = []
    flow = fan_in("raced")
    total = 3

    def work():
        worker = deferring_worker()
        ran = 0
        while ran < 10 and worker.run_once():
            ran += 1
        return ran

    with caplog.at_level(logging.WARNING, logger="oxpull"):
        results, errors = race_all([work] * 8)
        settled = settle(flow, total, worker=deferring_worker())

    assert not errors, errors[:3]
    assert settled == 0 or deferrals(caplog), (
        f"the reconciler had {settled} task(s) to run and the product logged "
        "no deferred or failed release, so a wakeup went missing in silence"
    )
    ran = order()
    assert sorted(ran) == ["a", "b", "child"], ran
    assert ran.index("child") == 2, ran
    assert sum(results) + settled == total
    assert OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 3
    assert flow.progress().pending == 0
    # Now, and only now, the backstop is owed nothing.
    assert workflows._release_backstop("default") == 0


@pytest.mark.django_db(transaction=True)
def test_a_release_racing_an_operator_retry_of_the_same_upstream():
    """
    django_ox.actions.retry moves a settled upstream back to READY while a
    release may be marking and releasing at that instant.

    The property under test is not that the release is correctly ordered
    against this out-of-band traffic, but that the write paths do not
    deadlock, do not raise, and cannot make a child claimable before its
    parent has ever succeeded.

    That last one is asserted as a necessary condition rather than a
    timestamp comparison: `succeeded` is set before any SUCCESSFUL write is
    issued, so a child claimed while it is clear is a child that was
    claimable with no successful parent behind it.
    """
    import threading

    tasks.STATE["order"] = []
    flow = chain("contended")
    parent = flow.node_ids()["parent"]
    child = flow.node_ids()["child"]
    succeeded = threading.Event()
    violations = []

    def releaser():
        for _ in range(20):
            succeeded.set()
            OxTask.objects.filter(id=parent).update(
                status=OxTask.Status.SUCCESSFUL, finished_at=timezone.now()
            )
            workflows._satisfy_edges_into([child], "default")
            workflows.release_downstream([child], "default")
        return True

    def retrier():
        for _ in range(20):
            row = OxTask.objects.filter(id=parent).first()
            if row is not None and row.status == OxTask.Status.SUCCESSFUL:
                OxTask.objects.filter(id=parent).update(status=OxTask.Status.FAILED)
                # Whether a retry is legal depends on the status another
                # thread just wrote; the outcome is not the subject.
                with contextlib.suppress(Exception):
                    actions.retry(parent)
        return True

    def claimer():
        worker = deferring_worker()
        for _ in range(20):
            claimed = worker.claim_one()
            if claimed is None:
                continue
            if claimed.id == child and not succeeded.is_set():
                violations.append(claimed.id)
            OxTask.objects.filter(id=claimed.id).update(
                status=OxTask.Status.READY, locked_by=None, locked_at=None
            )
        return True

    _, errors = race_all([releaser, retrier, claimer] * 2)
    assert not errors, errors[:3]
    assert not violations, violations
