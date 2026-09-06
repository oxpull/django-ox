import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import NamedTuple

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError, connections
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.compat import (
    IMMEDIATE_BACKEND_PATH,
    TaskResultStatus,
    default_task_backend,
    task_finished,
)
from django_ox.exceptions import TaskAbandoned
from django_ox.models import OxTask
from django_ox.worker import Worker

from .conftest import start_worker_thread, wait_for
from .tasks import add, fail_always, flaky, record_interval, slow


@contextmanager
def collect_task_finished():
    """Collect every task_finished TaskResult sent inside the block."""
    received = []

    def receiver(sender, task_result, **kwargs):
        received.append(task_result)

    task_finished.connect(receiver)
    try:
        yield received
    finally:
        task_finished.disconnect(receiver)


def reap_away(worker, db_task):
    """Age this claim past lock_timeout and let the reaper take the row."""
    OxTask.objects.filter(pk=db_task.pk).update(
        locked_at=timezone.now() - timedelta(seconds=worker.lock_timeout + 10)
    )
    assert worker.reap() == 1


@pytest.mark.django_db
class TestRetries:
    def test_failure_schedules_retry_with_backoff(self):
        worker = Worker(backoff_initial=30, poll_interval=0.05)
        fail_always.enqueue()

        before = timezone.now()
        assert worker.run_once() is True

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.attempts == 1
        assert len(db_task.errors) == 1
        assert db_task.run_after >= before + timedelta(seconds=30)
        assert db_task.locked_by is None
        assert db_task.locked_at is None

    # A retry's delay is written as an absolute run_after, so the delay a
    # test measures is the rule plus however long the attempt took to get
    # there. SLACK is the room for that. The cap is set below the second
    # attempt's doubling, so the two rules give 15 rather than 20, and the
    # slack is nowhere near wide enough to reach the uncapped value: a cap
    # that stopped working fails this test rather than passing it.
    BACKOFF_INITIAL = 10.0
    BACKOFF_MAX = 15.0
    SLACK = 4.0

    def test_backoff_doubles_per_attempt_up_to_the_cap(self):
        worker = Worker(
            backoff_initial=self.BACKOFF_INITIAL,
            backoff_max=self.BACKOFF_MAX,
            poll_interval=0.05,
        )
        fail_always.enqueue()

        delays = []
        for _ in range(2):
            db_task = OxTask.objects.get()
            OxTask.objects.update(run_after=None)
            before = timezone.now()
            assert worker.run_once() is True
            db_task.refresh_from_db()
            delays.append((db_task.run_after - before).total_seconds())

        # The first attempt waits backoff_initial.
        assert self.BACKOFF_INITIAL <= delays[0] < self.BACKOFF_INITIAL + self.SLACK
        # The second would double to 20; the cap holds it at backoff_max.
        assert self.BACKOFF_MAX <= delays[1] < self.BACKOFF_MAX + self.SLACK
        assert self.BACKOFF_MAX + self.SLACK < self.BACKOFF_INITIAL * 2

    def test_retry_until_success(self, worker):
        result = flaky.enqueue(succeed_on=3)

        assert worker.run_once() is True  # attempt 1 fails
        assert worker.run_once() is True  # attempt 2 fails
        assert worker.run_once() is True  # attempt 3 succeeds

        result.refresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 3
        assert result.attempts == 3
        assert len(result.errors) == 2

    def test_max_attempts_then_failed(self, worker):
        result = fail_always.enqueue()

        for _ in range(3):  # MAX_ATTEMPTS = 3 in test settings
            assert worker.run_once() is True
        assert worker.run_once() is False

        result.refresh()
        assert result.status == TaskResultStatus.FAILED
        assert result.is_finished
        assert result.attempts == 3
        assert len(result.errors) == 3
        assert result.errors[0].exception_class is ValueError
        assert "ValueError: boom" in result.errors[0].traceback
        with pytest.raises(ValueError, match="Task failed"):
            _ = result.return_value

        db_task = OxTask.objects.get()
        assert db_task.finished_at is not None
        assert db_task.locked_by is None


@pytest.mark.django_db
class TestClaiming:
    def test_claim_is_exclusive(self, worker):
        add.enqueue(1, 2)
        other = Worker(backoff_initial=0)

        claimed = worker.claim_one()
        assert claimed is not None
        assert claimed.status == OxTask.Status.RUNNING
        assert claimed.locked_by == worker.worker_id
        assert claimed.attempts == 1

        assert other.claim_one() is None

    def test_claim_writes_all_bookkeeping_atomically(self, worker):
        add.enqueue(1, 2)

        claimed = worker.claim_one()
        db_task = OxTask.objects.get()

        # The claim UPDATE itself wrote every per-attempt field, and the
        # returned instance matches the row. PostgreSQL gets that from
        # RETURNING *; elsewhere the claim reads the row back, because the
        # timestamps it wrote were computed by the database.
        for field in (
            "status",
            "attempts",
            "worker_ids",
            "started_at",
            "last_attempted_at",
            "locked_by",
            "locked_at",
        ):
            assert getattr(claimed, field) == getattr(db_task, field)
        assert db_task.worker_ids == [worker.worker_id]
        assert db_task.attempts == len(db_task.worker_ids)
        assert db_task.started_at is not None
        assert db_task.last_attempted_at is not None

    def test_reclaim_preserves_started_at_and_appends_worker_id(self, worker):
        fail_always.enqueue()
        assert worker.run_once() is True  # attempt 1 fails and requeues
        first = OxTask.objects.get()
        assert first.status == OxTask.Status.READY

        claimed = worker.claim_one()

        assert claimed is not None
        assert claimed.attempts == 2
        assert claimed.started_at == first.started_at
        assert claimed.last_attempted_at > first.last_attempted_at
        assert claimed.worker_ids == [worker.worker_id, worker.worker_id]

    def test_cas_claim_skips_stale_candidate(self, worker, monkeypatch):
        from django.db import connections

        add.enqueue(1, 2)
        stale = list(OxTask.objects.all())

        # Force the compare-and-set path, then simulate another worker
        # claiming and requeueing the row between fetch and UPDATE.
        connection = connections[worker._db_alias]
        monkeypatch.setattr(
            connection.features, "has_select_for_update_skip_locked", False
        )
        monkeypatch.setattr(worker, "_ready_queryset", lambda: stale)
        OxTask.objects.update(attempts=1, worker_ids=["other-worker"])

        assert worker.claim_one() is None
        db_task = OxTask.objects.get()
        assert db_task.attempts == 1
        assert db_task.worker_ids == ["other-worker"]

    def test_worker_requires_ox_backend(self, settings):
        settings.TASKS = {"default": {"BACKEND": IMMEDIATE_BACKEND_PATH}}
        with pytest.raises(ImproperlyConfigured):
            Worker()


@pytest.mark.django_db
class TestReaper:
    def _make_stuck(self, worker, attempts=None):
        add.enqueue(1, 2)
        db_task = worker.claim_one()
        stale = timezone.now() - timedelta(seconds=worker.lock_timeout + 10)
        updates = {"locked_at": stale}
        if attempts is not None:
            updates["attempts"] = attempts
        OxTask.objects.filter(pk=db_task.pk).update(**updates)
        return db_task

    def test_reaper_requeues_stuck_task(self, worker):
        db_task = self._make_stuck(worker)

        assert worker.reap() == 1
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.READY
        assert db_task.locked_by is None
        assert db_task.locked_at is None
        # The consumed attempt stays counted; the task then runs normally.
        assert db_task.attempts == 1
        assert worker.run_once() is True
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.SUCCESSFUL

    def test_reaper_requeue_bumps_the_lease_epoch(self, worker):
        db_task = self._make_stuck(worker)
        epoch_at_claim = db_task.lease_epoch

        assert worker.reap() == 1

        db_task.refresh_from_db()
        assert db_task.lease_epoch == epoch_at_claim + 1

    def test_reaper_records_a_lost_lease_and_not_a_failure(self, worker):
        """
        Attempts exhausted is the one case the reaper cannot requeue out of,
        and it is the case it used to invent a verdict for. It has watched a
        lock go quiet and nothing else, so LOST is the whole of what it may
        write, and it announces nothing.
        """
        db_task = self._make_stuck(worker, attempts=3)

        with collect_task_finished() as finished:
            assert worker.reap() == 1
        assert finished == []

        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.LOST
        assert db_task.return_value is None
        assert db_task.finished_at is not None
        assert db_task.locked_by is None

        (error,) = db_task.errors
        assert error["exception_class_path"] == "django_ox.exceptions.TaskAbandoned"
        assert "stopped renewing its lease" in error["traceback"]
        assert "never observed" in error["traceback"]
        assert "not a cause" in error["traceback"]

    def test_lost_row_keeps_its_epoch_and_is_not_reclaimed_again(self, worker):
        """
        The requeue bumps the epoch to fence the old worker out. LOST must
        not, because the holder of that epoch is the only party who could
        ever say what happened, and this is the row it would say it on.
        """
        db_task = self._make_stuck(worker, attempts=3)
        epoch_at_claim = db_task.lease_epoch

        assert worker.reap() == 1
        db_task.refresh_from_db()
        assert db_task.lease_epoch == epoch_at_claim

        # LOST is settled: no reaper touches it again and no worker claims it.
        assert worker.reap() == 0
        assert worker.claim_one() is None

    def test_reaper_ignores_fresh_locks(self, worker):
        add.enqueue(1, 2)
        worker.claim_one()
        assert worker.reap() == 0


@pytest.mark.django_db
class TestLeaseEpoch:
    """
    lease_epoch names one execution. It moves in the same statement that
    hands the row over and it never moves any other way, which is what lets
    a finish write ask "is this still mine?" with an integer comparison
    instead of a timing argument.
    """

    def test_every_claim_increments_it(self, worker):
        fail_always.enqueue()
        assert OxTask.objects.get().lease_epoch == 0

        first = worker.claim_one()
        assert first.lease_epoch == 1
        assert OxTask.objects.get().lease_epoch == 1

        worker.execute(first)  # fails, requeues for retry
        second = worker.claim_one()
        assert second.lease_epoch == 2
        assert OxTask.objects.get().lease_epoch == 2

    def test_a_retry_write_does_not_move_it(self, worker):
        """
        Only a handover moves the epoch. The retry write is the end of an
        execution, not the start of one, so the next claim is what advances
        it; keeping them separate is what makes the number countable.
        """
        fail_always.enqueue()
        claimed = worker.claim_one()

        worker.execute(claimed)

        assert OxTask.objects.get().lease_epoch == claimed.lease_epoch == 1

    def test_it_survives_a_reap_and_keeps_rising(self, worker):
        add.enqueue(1, 2)
        first = worker.claim_one()
        reap_away(worker, first)  # requeue: +1
        second = worker.claim_one()  # claim: +1

        assert (first.lease_epoch, second.lease_epoch) == (1, 3)


@pytest.mark.django_db
class TestLeaseFencesTerminalWrites:
    """
    Every finish write is conditional on the lease epoch the claim handed
    out. A worker that stalled long enough to be reaped holds a number
    nobody else will ever hold again, so its UPDATE matches zero rows rather
    than overwriting whoever owns the row now, and it must not announce a
    completion it does not own either.
    """

    def test_stale_retry_does_not_unterminal_a_finished_task(self, worker):
        """
        The reported race: worker A is reaped, worker B runs the task to
        SUCCESSFUL, and A then fails with retries left and writes READY over
        the top. A completed task went back on the queue.
        """
        add.enqueue(1, 2)
        stale = worker.claim_one()  # worker A, holding this instance
        assert stale.attempts == 1

        reap_away(worker, stale)

        other = Worker(backoff_initial=0, poll_interval=0.05)
        with collect_task_finished() as finished:
            assert other.run_once() is True
        assert len(finished) == 1
        assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL

        # A wakes up. Its attempt failed, and it has retries remaining.
        with collect_task_finished() as finished:
            worker._handle_failure(stale, RuntimeError("slow, not dead"), 12)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3
        assert db_task.attempts == 2
        assert db_task.errors == []
        assert db_task.run_after is None
        assert db_task.locked_by is None

    def test_stale_success_cannot_overwrite_a_terminal_row(self, worker):
        add.enqueue(1, 2)
        stale = worker.claim_one()

        reap_away(worker, stale)
        other = Worker(backoff_initial=0, poll_interval=0.05)
        claimed = other.claim_one()
        assert claimed is not None
        # Whatever the other worker wrote, it owns the outcome.
        OxTask.objects.filter(pk=claimed.pk, lease_epoch=claimed.lease_epoch).update(
            status=OxTask.Status.FAILED,
            errors=[
                {"exception_class_path": "builtins.ValueError", "traceback": "theirs"}
            ],
            finished_at=timezone.now(),
            locked_by=None,
            locked_at=None,
        )

        # A's attempt now succeeds, far too late.
        with collect_task_finished() as finished:
            worker.execute(stale)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.return_value is None
        assert db_task.attempts == 2
        assert [e["traceback"] for e in db_task.errors] == ["theirs"]

    def test_stale_final_failure_cannot_overwrite_a_terminal_row(self, worker):
        """
        The third write site, reached by hand.

        No sequence in the product gets here today: the epoch only moves on
        a claim or a reaper requeue, a requeue needs attempts remaining, and
        a claim needs the row READY, so a worker on its last attempt cannot
        have the row taken off it except by LOST, which is deliberately
        writable (see TestLostState). The condition is on this write anyway,
        because a site that is fenced by luck rather than by construction is
        the one that breaks the next time the state machine grows.
        """
        fail_always.enqueue()
        OxTask.objects.update(attempts=2)  # the claim below is the last attempt
        stale = worker.claim_one()
        assert stale.attempts == stale.max_attempts == 3

        OxTask.objects.filter(pk=stale.pk).update(
            status=OxTask.Status.SUCCESSFUL,
            return_value="theirs",
            lease_epoch=stale.lease_epoch + 1,
            finished_at=timezone.now(),
            locked_by=None,
            locked_at=None,
        )

        with collect_task_finished() as finished:
            worker.execute(stale)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == "theirs"
        assert db_task.errors == []

    @pytest.mark.parametrize(
        "settled", [OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED, OxTask.Status.READY]
    )
    def test_a_settled_row_refuses_the_write_even_at_a_matching_epoch(
        self, worker, settled
    ):
        """
        The epoch is the fence, and it is sufficient only for as long as
        every handover bumps it. The write also checks that the row is one
        an execution could still be running, so a state machine that grows a
        path which forgets to bump does not silently reopen this bug. This
        is the shape the shipped reproduction writes by hand.
        """
        add.enqueue(1, 2)
        stale = worker.claim_one()
        OxTask.objects.filter(pk=stale.pk).update(
            status=settled, locked_by=None, locked_at=None
        )
        assert OxTask.objects.get().lease_epoch == stale.lease_epoch

        with collect_task_finished() as finished:
            worker.execute(stale)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == settled
        assert db_task.return_value is None

    def test_a_reclaimed_worker_can_still_write_its_new_epoch(self, worker):
        """
        locked_by is not enough on its own: a worker may legitimately
        re-claim a task it was reaped off, and its stale execution must not
        write over its own later one. They share pk, status and locked_by,
        and differ only in the epoch.
        """
        add.enqueue(1, 2)
        first = worker.claim_one()
        reap_away(worker, first)

        second = worker.claim_one()
        assert second.locked_by == first.locked_by == worker.worker_id
        assert (first.lease_epoch, second.lease_epoch) == (1, 3)

        # The stale first execution finishes while the second is running.
        with collect_task_finished() as finished:
            worker._handle_failure(first, RuntimeError("slow, not dead"), 7)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.RUNNING
        assert db_task.attempts == 2
        assert db_task.errors == []
        assert db_task.locked_by == worker.worker_id
        assert db_task.run_after is None

        # The live second execution writes its outcome normally.
        with collect_task_finished() as finished:
            worker.execute(second)
        assert len(finished) == 1
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3

    # -- a held lease still writes, and still signals -----------------------

    def test_live_lease_writes_success_and_signals_once(self, worker):
        result = add.enqueue(1, 2)
        claimed = worker.claim_one()

        with collect_task_finished() as finished:
            worker.execute(claimed)

        assert len(finished) == 1
        assert finished[0].id == result.id
        assert finished[0].status == TaskResultStatus.SUCCESSFUL
        assert finished[0].return_value == 3

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3
        assert db_task.finished_at is not None
        assert db_task.locked_by is None
        assert db_task.locked_at is None
        # The conditional UPDATE does not refresh the instance, so the write
        # mirrors what it stored back onto it; the signalled TaskResult is
        # built from that instance and must match the row.
        assert claimed.status == db_task.status
        assert claimed.return_value == db_task.return_value
        assert claimed.finished_at == db_task.finished_at
        assert claimed.locked_by is None
        assert claimed.locked_at is None

    def test_the_success_write_costs_one_statement(self, worker):
        # A finish write that reads a column back afterwards is a second
        # round trip on the path every task takes. This asserts the
        # outcome write stays a single statement. _write_outcome
        # takes values so the instance can be mirrored from what the caller
        # already holds; nothing it is given may be an expression the
        # database has to compute.
        add.enqueue(1, 2)
        claimed = worker.claim_one()

        with CaptureQueriesContext(connections["default"]) as captured:
            worker.execute(claimed)

        assert len(captured) == 1, (
            "an outcome is one UPDATE, but this wrote "
            f"{[q['sql'] for q in captured.captured_queries]}"
        )

    def test_live_lease_writes_retry_with_backoff_and_does_not_signal(self):
        worker = Worker(backoff_initial=30, poll_interval=0.05)
        fail_always.enqueue()
        claimed = worker.claim_one()

        before = timezone.now()
        with collect_task_finished() as finished:
            worker.execute(claimed)
        assert finished == []

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.run_after >= before + timedelta(seconds=30)
        assert len(db_task.errors) == 1
        assert db_task.finished_at is None
        assert db_task.locked_by is None
        assert claimed.run_after == db_task.run_after
        assert claimed.status == db_task.status
        assert claimed.errors == db_task.errors
        assert claimed.locked_by is None
        assert claimed.locked_at is None

    def test_live_lease_writes_final_failure_and_signals_once(self, worker):
        result = fail_always.enqueue()
        OxTask.objects.update(attempts=2)  # the claim below is the last attempt
        claimed = worker.claim_one()

        with collect_task_finished() as finished:
            worker.execute(claimed)

        assert len(finished) == 1
        assert finished[0].id == result.id
        assert finished[0].status == TaskResultStatus.FAILED

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.FAILED
        assert db_task.finished_at is not None
        assert db_task.locked_by is None
        assert len(db_task.errors) == 1
        assert claimed.status == db_task.status
        assert claimed.errors == db_task.errors
        assert claimed.finished_at == db_task.finished_at


@pytest.mark.django_db
class TestClaimCannotAdoptAnotherLease:
    """
    A claim reads its row back in a second statement, and the row can move
    in between. If it does, the worker must come away with nothing rather
    than with somebody else's lease: an adopted epoch defeats every fence
    downstream, because a fence can only compare against what the instance
    is carrying.

    The gap is narrow, so it is held open deliberately here. What happens
    inside it is not simulated: the reaper and the competing claim are the
    shipped code paths.
    """

    @pytest.fixture(autouse=True)
    def _optimistic_path_only(self):
        if connections["default"].features.has_select_for_update_skip_locked:
            pytest.skip("no gap on this backend: RETURNING or a row lock covers it")

    @staticmethod
    def steal_row_inside_the_gap(monkeypatch, thief):
        """Reap and re-claim the row inside the claiming worker's read gap.

        Returns a dict that carries the thief's claim once the gap is used.
        The gate closes before the nested claim rather than after it: that
        claim re-enters this same patched method, and leaving it open turns
        one theft into a recursion that burns every attempt on the row.
        """
        stolen: dict[str, object] = {}
        used: list[bool] = []
        real_reload = Worker._reload_claimed

        def reload_after_theft(self, pk, granted_epoch):
            if not used:
                used.append(True)
                reap_away(self, OxTask.objects.get(pk=pk))
                stolen["by"] = thief.claim_one()
            return real_reload(self, pk, granted_epoch)

        monkeypatch.setattr(Worker, "_reload_claimed", reload_after_theft)
        return stolen

    def test_a_claim_reaped_mid_read_comes_back_empty(self, worker, monkeypatch):
        add.enqueue(1, 2)
        other = Worker(backoff_initial=0, poll_interval=0.05)
        stolen = self.steal_row_inside_the_gap(monkeypatch, other)

        mine = worker.claim_one()

        assert stolen.get("by") is not None, "the competing worker never got the row"
        assert mine is None, (
            "the claim came back holding a lease it was never granted, so "
            "every fence downstream would compare against that worker's epoch."
        )

    def test_the_worker_that_did_claim_it_keeps_the_lease(self, worker, monkeypatch):
        add.enqueue(1, 2)
        other = Worker(backoff_initial=0, poll_interval=0.05)
        stolen = self.steal_row_inside_the_gap(monkeypatch, other)

        worker.claim_one()

        holder = stolen["by"]
        row = OxTask.objects.get(pk=holder.pk)
        assert row.locked_by == other.worker_id
        assert row.lease_epoch == holder.lease_epoch
        assert row.status == OxTask.Status.RUNNING


@pytest.mark.django_db
class TestLostState:
    """
    LOST is django-ox's own status and has no counterpart in
    django.tasks.TaskResultStatus, which has four values and gets no fifth.
    It reads as FAILED at the public boundary: nothing is going to run the
    task again, so READY and RUNNING would be instructions to wait for
    something that is not coming, and a caller that waits on is_finished
    would wait forever.
    """

    def _lose_the_lease(self, worker):
        """Claim a last attempt and let the reaper mark the row LOST."""
        fail_always.enqueue()
        OxTask.objects.update(attempts=2)
        stale = worker.claim_one()
        assert stale.attempts == stale.max_attempts == 3
        reap_away(worker, stale)
        assert OxTask.objects.get().status == OxTask.Status.LOST
        return stale

    def test_django_still_has_exactly_four_public_statuses(self):
        assert {status.value for status in TaskResultStatus} == {
            "READY",
            "RUNNING",
            "FAILED",
            "SUCCESSFUL",
        }
        assert "LOST" not in {status.value for status in TaskResultStatus}

    def test_lost_reads_as_failed_and_is_finished(self, worker):
        stale = self._lose_the_lease(worker)

        result = default_task_backend.get_result(str(stale.pk))
        assert result.status == TaskResultStatus.FAILED
        assert result.is_finished
        assert result.errors[-1].exception_class is TaskAbandoned
        with pytest.raises(ValueError, match="Task failed"):
            _ = result.return_value

    def test_lost_is_not_pending_for_completion_counting(self, worker):
        """
        The paid batches feature counts unfinished members as
        (READY, RUNNING) against this column. A fifth value is therefore
        settled by construction, which is the property that stops a batch
        with a lost member hanging.
        """
        self._lose_the_lease(worker)

        pending = (OxTask.Status.READY, OxTask.Status.RUNNING)
        assert OxTask.Status.LOST not in pending
        assert OxTask.objects.filter(status__in=pending).count() == 0

    def test_the_lost_epoch_may_still_resolve_it(self, worker):
        """
        The documented cost of mapping LOST to FAILED. The row said FAILED
        while the outcome was unknown; the worker that held the lease comes
        back with the answer and the row changes to SUCCESSFUL. Only that
        one execution can do this, and only while the row is still LOST.
        """
        add.enqueue(1, 2)
        OxTask.objects.update(attempts=2)
        stale = worker.claim_one()
        reap_away(worker, stale)

        result = default_task_backend.get_result(str(stale.pk))
        assert result.status == TaskResultStatus.FAILED

        with collect_task_finished() as finished:
            worker.execute(stale)
        assert len(finished) == 1
        assert finished[0].status == TaskResultStatus.SUCCESSFUL

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3
        # The reaper's note is gone. It said the outcome was never observed,
        # and this write is that observation, so it no longer describes the
        # row. What happened to the lease is still on record: worker_ids
        # names every holder, and the reaper logged task_lease_lost.
        assert db_task.errors == []
        assert len(db_task.worker_ids) == len(set(db_task.worker_ids))

        result.refresh()
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 3

    def test_both_resolutions_leave_the_same_kind_of_record(self, worker):
        """
        Whichever way a lost lease resolves, errors must mean the same thing.

        The reaper's note says the outcome was never observed. Once the
        holder reports, that is no longer true, so the note is superseded
        either way and errors goes back to meaning what attempts raised.
        Keeping it on a success puts an exception that was never raised in
        front of every error reporter reading result.errors; keeping it on a
        failure and not on a success would make the same row mean two
        different things depending on how it ended.

        The lease event itself is not lost: it is a task_lease_lost warning
        in the log, and worker_ids on the row names every worker that held
        the task.
        """
        add.enqueue(1, 2)
        OxTask.objects.update(attempts=2)
        succeeding = worker.claim_one()
        reap_away(worker, succeeding)
        assert any(
            error["exception_class_path"].endswith("TaskAbandoned")
            for error in OxTask.objects.get(pk=succeeding.pk).errors
        ), "the reaper should have recorded the lost lease"
        worker.execute(succeeding)
        after_success = OxTask.objects.get(pk=succeeding.pk)

        fail_always.enqueue()
        OxTask.objects.filter(status=OxTask.Status.READY).update(attempts=2)
        failing = worker.claim_one()
        reap_away(worker, failing)
        worker.execute(failing)
        after_failure = OxTask.objects.get(pk=failing.pk)

        assert after_success.status == OxTask.Status.SUCCESSFUL
        assert after_failure.status == OxTask.Status.FAILED

        def notes(db_task):
            return [
                error["exception_class_path"]
                for error in db_task.errors
                if error["exception_class_path"].endswith("TaskAbandoned")
            ]

        assert notes(after_success) == [], (
            "a task that succeeded carries an exception that was never raised; "
            "anything reading result.errors reports a failure that did not happen"
        )
        assert notes(after_failure) == []
        assert [error["exception_class_path"] for error in after_failure.errors] == [
            "builtins.ValueError"
        ], "the real exception must survive"

    def test_nobody_but_the_lost_epoch_may_resolve_it(self, worker):
        stale = self._lose_the_lease(worker)
        impostor = Worker(backoff_initial=0, poll_interval=0.05)

        # Same task, same worker-shaped write, wrong epoch.
        stale_copy = OxTask.objects.get(pk=stale.pk)
        stale_copy.lease_epoch = stale.lease_epoch + 1
        with collect_task_finished() as finished:
            impostor.execute(stale_copy)
        assert finished == []

        assert OxTask.objects.get().status == OxTask.Status.LOST


@pytest.mark.django_db
class TestLeaseRenewalScope:
    def test_renewal_touches_only_rows_this_worker_is_running(self, worker):
        add.enqueue(1, 2)
        add.enqueue(3, 4)
        mine = worker.claim_one()
        other = Worker(backoff_initial=0, poll_interval=0.05)
        theirs = other.claim_one()

        stale = timezone.now() - timedelta(seconds=worker.lock_timeout + 10)
        OxTask.objects.update(locked_at=stale)

        # Nothing is registered as in flight yet, so there is nothing to renew.
        assert worker.renew_leases() == 0

        worker._in_flight.add((mine.pk, mine.lease_epoch))
        assert worker.renew_leases() == 1

        assert OxTask.objects.get(pk=mine.pk).locked_at > stale
        assert OxTask.objects.get(pk=theirs.pk).locked_at == stale

    def test_renewal_stops_once_the_row_is_no_longer_held(self, worker):
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        worker._in_flight.add((claimed.pk, claimed.lease_epoch))
        assert worker.renew_leases() == 1

        # Reaped off us: the row is READY and belongs to nobody.
        reap_away(worker, claimed)
        assert worker.renew_leases() == 0

    def test_renewal_loop_survives_a_database_error(self, worker, caplog):
        """
        Giving up on the first failed renewal would expire every live lease
        this worker holds, so the loop logs and carries on. Two consecutive
        misses are already inside the timeout by design.
        """
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker.renew_interval = 0.02
        attempts = []

        def flaky_renew():
            attempts.append(1)
            if len(attempts) == 1:
                raise DatabaseError("connection reset")
            return 0

        worker.renew_leases = flaky_renew
        stop = threading.Event()
        thread = threading.Thread(target=worker._renewal_loop, args=(stop,))
        thread.start()
        try:
            assert wait_for(lambda: len(attempts) >= 3)
        finally:
            stop.set()
            thread.join(timeout=5)

        (record,) = [
            r for r in caplog.records if getattr(r, "event", "") == "lease_renew_failed"
        ]
        assert record.worker_id == worker.worker_id
        assert record.levelno == logging.WARNING

    def test_execute_registers_and_releases_the_lease(self, worker):
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        seen = []

        original = worker._write_outcome

        def spy(db_task, **kwargs):
            seen.append(set(worker._in_flight))
            return original(db_task, **kwargs)

        worker._write_outcome = spy
        worker.execute(claimed)

        assert seen == [{(claimed.pk, claimed.lease_epoch)}]
        assert worker._in_flight == set()


class _Hammered(NamedTuple):
    """What one pass of the reaper over a live worker's task saw."""

    reaped: int
    # Distinct locked_at values seen before anything was reclaimed: the one
    # the claim wrote, then one more for every renewal that reached the
    # database. Two samples with the same value mean nothing landed between
    # them, so the length is a count of renewals plus the claim.
    leases: list[datetime]
    statuses: set[str]

    @property
    def renewals(self) -> int:
        return max(len(self.leases) - 1, 0)


@pytest.mark.django_db(transaction=True)
class TestLeaseRenewalUnderTheReaper:
    """
    The point of renewal: the reaper stops reclaiming tasks that are merely
    slow and reclaims only workers that actually stopped reporting.

    The timings below are a ratio rather than a stopwatch. What is under
    test is the shape the worker documents -- a renewal every
    LOCK_TIMEOUT / 3, so two consecutive renewals can be missed before the
    reaper is entitled to conclude anything -- and the ratio, not the
    absolute value, is what these tests assert. The absolute values are
    scaled so that a single renewal round-trip on a networked database, or
    under a coverage tracer, still lands well inside the lease. A tighter
    lease makes the same test measure how fast the machine is instead.
    """

    LOCK_TIMEOUT = 1.5
    # What the worker derives from that timeout on its own. The renewal
    # tests below pass no interval and check this one came out of the
    # worker, so what is exercised is the rule that ships rather than a
    # number the test made up.
    RENEW_INTERVAL = LOCK_TIMEOUT / 3
    # Longer than the lease, so a claim that stopped being renewed is
    # certain to age past the timeout inside the window rather than merely
    # likely to.
    HAMMER_SECONDS = LOCK_TIMEOUT * 2.25
    # The task has to still be running when the window closes: a reaper
    # finds nothing to reclaim on a finished task whatever renewal did, and
    # a test that let the task finish early would pass on an empty window.
    TASK_SECONDS = LOCK_TIMEOUT * 3

    def _hammer_the_reaper(self, reaper, seconds):
        """
        Run the reaper flat out for `seconds`, watching the lease as it goes.

        A count of reclaims on its own cannot tell "renewal held the lease"
        apart from "there was nothing here to reclaim", so every pass also
        reads the row: the status it was in, and the lease timestamp, which
        moves forward once per renewal that reached the database. Sampling
        stops at the first reclaim, because from there the lease has
        changed hands and says nothing further about the worker that held
        it.
        """
        reaped = 0
        leases: list[datetime] = []
        statuses: set[str] = set()
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if reaped == 0:
                status, locked_at = OxTask.objects.values_list(
                    "status", "locked_at"
                ).get()
                statuses.add(status)
                if locked_at is not None and locked_at not in leases:
                    leases.append(locked_at)
            reaped += reaper.reap()
            time.sleep(0.05)
        return _Hammered(reaped, leases, statuses)

    def _assert_renewal_held_the_lease(self, hammered):
        """
        The claim both renewal tests make: nothing was reclaimed, and the
        reason was renewal rather than an idle window. A stopped renewal
        thread fails here even though it reclaims nothing.
        """
        assert hammered.reaped == 0
        assert hammered.statuses == {OxTask.Status.RUNNING}
        assert hammered.renewals >= 3
        assert hammered.leases == sorted(hammered.leases)

    def test_renewal_keeps_a_slow_task_out_of_the_reaper(self):
        worker = Worker(
            lock_timeout=self.LOCK_TIMEOUT, poll_interval=0.05, backoff_initial=0
        )
        assert worker.renew_interval == self.RENEW_INTERVAL
        reaper = Worker(lock_timeout=self.LOCK_TIMEOUT, poll_interval=0.05)
        slow.enqueue(self.TASK_SECONDS)

        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.RUNNING
            )
            self._assert_renewal_held_the_lease(
                self._hammer_the_reaper(reaper, self.HAMMER_SECONDS)
            )
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        db_task = OxTask.objects.get()
        assert db_task.attempts == 1
        assert db_task.return_value == "done"

    def test_without_renewal_the_same_task_is_reclaimed(self):
        """
        The control for the test above. Same timings, renewal pushed out of
        reach, so the only difference is whether the worker is refreshing
        its own lock.
        """
        worker = Worker(
            lock_timeout=self.LOCK_TIMEOUT,
            renew_interval=1000,
            reap_interval=1000,
            poll_interval=0.05,
            backoff_initial=0,
        )
        reaper = Worker(lock_timeout=self.LOCK_TIMEOUT, poll_interval=0.05)
        slow.enqueue(self.TASK_SECONDS)

        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.RUNNING
            )
            hammered = self._hammer_the_reaper(reaper, self.HAMMER_SECONDS)
            assert hammered.reaped >= 1
            # Reclaimed for the stated reason: the task was running the
            # whole time and its lease sat at the value the claim wrote.
            assert hammered.statuses == {OxTask.Status.RUNNING}
            assert hammered.renewals == 0
        finally:
            worker.request_stop()
            thread.join(timeout=5)

    def test_renewal_continues_through_a_graceful_drain(self):
        """
        A drain lasts as long as the slowest in-flight task. A lease that
        expires while its task is finishing cleanly is the exact false
        reclaim renewal exists to prevent, so the renewal thread outlives
        the poll loop.
        """
        worker = Worker(
            lock_timeout=self.LOCK_TIMEOUT, poll_interval=0.05, backoff_initial=0
        )
        assert worker.renew_interval == self.RENEW_INTERVAL
        reaper = Worker(lock_timeout=self.LOCK_TIMEOUT, poll_interval=0.05)
        slow.enqueue(self.TASK_SECONDS)

        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.RUNNING
            )
            worker.request_stop()  # drain begins with the task still running
            self._assert_renewal_held_the_lease(
                self._hammer_the_reaper(reaper, self.HAMMER_SECONDS)
            )
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.attempts == 1


@pytest.mark.django_db(transaction=True)
class TestRunLoop:
    def _run_in_thread(self, worker):
        return start_worker_thread(worker)

    def test_graceful_shutdown_drains_in_flight(self):
        worker = Worker(poll_interval=0.05, backoff_initial=0)
        result = slow.enqueue(0.6)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
            )
            worker.request_stop()
            thread.join(timeout=5)
            assert not thread.is_alive()
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        db_task = OxTask.objects.get(id=result.id)
        assert db_task.status == OxTask.Status.SUCCESSFUL

    def test_shutdown_closes_each_pool_thread_connection(self, monkeypatch):
        closer_threads = []
        original = Worker._close_connections_in_thread

        def spy(self, barrier):
            closer_threads.append(threading.current_thread().name)
            original(self, barrier)

        monkeypatch.setattr(Worker, "_close_connections_in_thread", spy)
        worker = Worker(concurrency=2, poll_interval=0.05, backoff_initial=0)
        add.enqueue(1, 2)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)
        assert not thread.is_alive()

        # One close task per pool slot, each on a distinct pool thread;
        # the barrier is what guarantees the distinctness.
        assert len(closer_threads) == 2
        assert len(set(closer_threads)) == 2
        assert all(name.startswith("ox") for name in closer_threads)

    # How long each of the two tasks below runs. Both are dispatched into a
    # pool of two, so they overlap unless the second dispatch is a whole
    # task behind the first; the length is what buys the room for that, and
    # a machine slow enough to spend this long between two dispatches has
    # not run them concurrently in any sense worth passing.
    CONCURRENT_SECONDS = 1.0

    def test_run_loop_processes_tasks_concurrently(self, task_state):
        worker = Worker(concurrency=2, poll_interval=0.05, backoff_initial=0)
        r1 = record_interval.enqueue(self.CONCURRENT_SECONDS)
        r2 = record_interval.enqueue(self.CONCURRENT_SECONDS)

        thread = self._run_in_thread(worker)
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).count() == 2
                ),
                timeout=self.CONCURRENT_SECONDS * 10,
            )
        finally:
            worker.request_stop()
            thread.join(timeout=5)

        (start_a, end_a), (start_b, end_b) = task_state["intervals"]
        # Both executions must overlap in time; serial execution cannot.
        assert max(start_a, start_b) < min(end_a, end_b)

        for result in (r1, r2):
            result.refresh()
            assert result.return_value == "done"
