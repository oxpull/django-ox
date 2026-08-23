"""
django_ox.actions: retry and discard as compare-and-set moves on one row.
"""

import threading
import uuid

import pytest
from django.db import connections
from django.tasks import TaskResultStatus, default_task_backend
from django.utils import timezone

from django_ox import actions, stats
from django_ox.models import OxTask
from django_ox.worker import Worker

from .tasks import STATE, add, fail_always, flaky
from .test_worker import reap_away


def run_to_failed(worker):
    """Enqueue fail_always and burn every attempt, clearing the backoff."""
    fail_always.enqueue()
    for _ in range(3):
        OxTask.objects.update(run_after=None)
        assert worker.run_once() is True
    db_task = OxTask.objects.get()
    assert db_task.status == OxTask.Status.FAILED
    assert db_task.attempts == db_task.max_attempts == 3
    return db_task


def lose_the_lease(worker):
    """Claim a last attempt and let the reaper mark the row LOST."""
    add.enqueue(1, 2)
    OxTask.objects.update(attempts=2)
    stale = worker.claim_one()
    reap_away(worker, stale)
    assert OxTask.objects.get().status == OxTask.Status.LOST
    return stale


@pytest.mark.django_db
class TestRetry:
    def test_failed_task_runs_again(self, worker):
        failed = run_to_failed(worker)
        epoch = failed.lease_epoch

        assert actions.retry(failed.pk) is True

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == epoch + 1
        assert db_task.attempts == 3
        assert db_task.max_attempts == 4
        assert db_task.run_after is None
        assert db_task.finished_at is None
        assert len(db_task.errors) == 3

        assert worker.run_once() is True
        db_task.refresh_from_db()
        assert db_task.attempts == 4
        assert len(db_task.errors) == 4
        assert db_task.status == OxTask.Status.FAILED

    def test_retry_is_one_more_attempt_and_can_succeed(self, worker):
        flaky.enqueue(succeed_on=4)
        for _ in range(3):
            OxTask.objects.update(run_after=None)
            worker.run_once()
        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.FAILED

        assert actions.retry(str(db_task.pk)) is True
        assert worker.run_once() is True

        result = default_task_backend.get_result(str(db_task.pk))
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.return_value == 4
        assert result.attempts == 4

    def test_retry_refuses_running(self, worker):
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        epoch = claimed.lease_epoch

        assert actions.retry(claimed.pk) is False

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.RUNNING
        assert db_task.lease_epoch == epoch
        assert db_task.locked_by == worker.worker_id

    @pytest.mark.parametrize(
        "status",
        [OxTask.Status.READY, OxTask.Status.SUCCESSFUL, OxTask.Status.DISCARDED],
    )
    def test_retry_refuses_other_states(self, status):
        db_task = OxTask.objects.create(
            task_path="tests.tasks.add",
            backend_name="default",
            status=status,
            enqueued_at=timezone.now(),
        )
        assert actions.retry(db_task.pk) is False
        assert OxTask.objects.get().status == status

    def test_retry_of_unknown_or_malformed_id_is_false(self):
        assert actions.retry(uuid.uuid4()) is False
        assert actions.retry("not-a-uuid") is False

    def test_retry_of_lost_row_fences_the_straggler_out(self, worker):
        """
        A LOST row's last worker may still be alive and still holds the
        epoch the reaper left in place. Retrying bumps it, so that worker's
        outcome write matches nothing, and exactly one execution owns the
        row: the retry's.
        """
        stale = lose_the_lease(worker)

        assert actions.retry(stale.pk) is True
        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == stale.lease_epoch + 1

        # The straggler finishes its old attempt now.
        assert (
            worker._write_outcome(
                stale,
                status=OxTask.Status.SUCCESSFUL,
                duration_ms=1,
                return_value=3,
            )
            is False
        )
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.READY
        assert db_task.return_value is None

        # The reaper has nothing to do with a READY row either.
        assert worker.reap() == 0

        assert worker.run_once() is True
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3
        # The lost-lease note on attempt 3 stays: that attempt's outcome was
        # never observed, and the retry is attempt 4, not a rewrite of 3.
        (note,) = db_task.errors
        assert note["exception_class_path"] == "django_ox.exceptions.TaskAbandoned"

    def test_reaper_then_retry_requeues_once(self, worker):
        """
        The reaper requeues a stale RUNNING row with attempts left. A retry
        arriving after it sees READY and does nothing; one arriving before
        it sees RUNNING and does nothing. The row is requeued once.
        """
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert actions.retry(claimed.pk) is False
        reap_away(worker, claimed)
        assert actions.retry(claimed.pk) is False

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == claimed.lease_epoch + 1

    @pytest.mark.django_db(transaction=True)
    def test_concurrent_retries_requeue_once(self, worker):
        failed = run_to_failed(worker)
        epoch = failed.lease_epoch
        outcomes = []
        start = threading.Barrier(8)

        def attempt():
            try:
                start.wait(timeout=5)
                outcomes.append(actions.retry(failed.pk))
            finally:
                connections.close_all()

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert outcomes.count(True) == 1
        assert outcomes.count(False) == 7
        db_task = OxTask.objects.get()
        assert db_task.lease_epoch == epoch + 1
        assert db_task.max_attempts == 4


@pytest.mark.django_db
class TestDiscard:
    def test_discarded_ready_task_never_runs(self, worker):
        result = add.enqueue(1, 2)

        assert actions.discard(result.id) is True
        assert worker.run_once() is False

        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.DISCARDED
        assert db_task.attempts == 0
        assert db_task.finished_at is not None
        assert STATE == {}

        result.refresh()
        assert result.status == TaskResultStatus.FAILED
        assert result.is_finished
        assert result.errors == []

    def test_discard_failed_and_lost(self, worker):
        failed = run_to_failed(worker)
        assert actions.discard(failed.pk) is True
        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.DISCARDED
        assert len(db_task.errors) == 3

        OxTask.objects.all().delete()
        stale = lose_the_lease(worker)
        assert actions.discard(stale.pk) is True
        assert OxTask.objects.get().status == OxTask.Status.DISCARDED

        # The straggler's outcome is refused by status.
        assert (
            worker._write_outcome(
                stale, status=OxTask.Status.SUCCESSFUL, duration_ms=1, return_value=3
            )
            is False
        )
        assert OxTask.objects.get().status == OxTask.Status.DISCARDED

    def test_discard_refuses_running_and_successful(self, worker):
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        assert actions.discard(claimed.pk) is False
        assert OxTask.objects.get().status == OxTask.Status.RUNNING

        worker.execute(claimed)
        assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
        assert actions.discard(claimed.pk) is False
        assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL

    def test_discard_twice_is_false_the_second_time(self):
        result = add.enqueue(1, 2)
        assert actions.discard(result.id) is True
        assert actions.discard(result.id) is False
        assert actions.retry(result.id) is False

    def test_discard_of_unknown_or_malformed_id_is_false(self):
        assert actions.discard(uuid.uuid4()) is False
        assert actions.discard("") is False

    def test_discard_loses_the_race_to_a_claim(self, worker):
        """
        A discard that read the row READY but reaches the database after a
        worker claimed it matches nothing: the epoch it read has moved.
        """
        result = add.enqueue(1, 2)
        before = OxTask.objects.get()
        claimed = worker.claim_one()
        assert claimed is not None

        assert (
            OxTask.objects.filter(
                pk=before.pk,
                status__in=actions.DISCARDABLE_STATUSES,
                lease_epoch=before.lease_epoch,
            ).update(status=OxTask.Status.DISCARDED)
            == 0
        )
        assert actions.discard(result.id) is False
        assert OxTask.objects.get().status == OxTask.Status.RUNNING


@pytest.mark.django_db
class TestDiscardedIsSettled:
    def test_stats_column_and_prune(self, worker):
        result = add.enqueue(1, 2)
        actions.discard(result.id)
        (row,) = stats.queue_stats()
        assert row.discarded == 1
        assert row.failed == 0
        assert stats.ready_count() == 0
        assert stats.throughput() == 0.0
        assert stats.failure_rate() is None

    def test_django_still_has_exactly_four_public_statuses(self):
        assert "DISCARDED" not in {status.value for status in TaskResultStatus}

    def test_fresh_worker_does_not_claim_discarded(self):
        result = add.enqueue(1, 2)
        actions.discard(result.id)
        assert Worker(backoff_initial=0, poll_interval=0.05).claim_one() is None


def seed(status, count, **fields):
    """bulk_create count rows in one status, without going through enqueue."""
    now = timezone.now()
    rows = [
        OxTask(
            task_path="tests.tasks.add",
            args=[1, 2],
            backend_name="default",
            status=status,
            attempts=fields.pop("attempts", 3 if status != OxTask.Status.READY else 0),
            max_attempts=3,
            enqueued_at=now,
            lease_epoch=fields.pop("lease_epoch", 3),
            **fields,
        )
        for _ in range(count)
    ]
    return OxTask.objects.bulk_create(rows, batch_size=1000)


def snapshot():
    return {
        row["pk"]: row
        for row in OxTask.objects.values(
            "pk", "status", "lease_epoch", "max_attempts", "attempts", "run_after"
        )
    }


@pytest.mark.django_db
class TestMany:
    def mixed(self):
        seed(OxTask.Status.READY, 2)
        seed(OxTask.Status.RUNNING, 2, locked_by="w", locked_at=timezone.now())
        seed(OxTask.Status.FAILED, 2, run_after=timezone.now())
        seed(OxTask.Status.SUCCESSFUL, 2)
        seed(OxTask.Status.LOST, 2)
        seed(OxTask.Status.DISCARDED, 2)
        return list(OxTask.objects.values_list("pk", flat=True))

    @pytest.mark.parametrize(
        ("one", "many", "expected"),
        [
            (actions.retry, actions.retry_many, 4),
            (actions.discard, actions.discard_many, 6),
        ],
    )
    def test_bulk_equals_repeated_single_row(self, one, many, expected):
        ids = self.mixed()
        ids_with_noise = [*ids, uuid.uuid4(), "not-a-uuid", ids[0]]
        before = snapshot()

        changed, skipped = many(ids_with_noise)
        after_many = snapshot()

        # Reset and do the same selection one row at a time.
        for pk, row in before.items():
            OxTask.objects.filter(pk=pk).update(
                **{k: v for k, v in row.items() if k != "pk"}
            )
        results = [one(pk) for pk in ids_with_noise]
        after_one = snapshot()

        assert after_many == after_one
        assert changed == sum(results) == expected
        # The unknown id counts as skipped; the duplicate and the malformed id
        # are not ids at all.
        assert skipped == len(ids) + 1 - expected

    def test_many_accepts_a_queryset(self):
        self.mixed()
        assert actions.retry_many(OxTask.objects.all()) == (4, 8)
        assert actions.discard_many(OxTask.objects.all()) == (6, 6)
        assert actions.retry_many(OxTask.objects.none()) == (0, 0)

    def test_retry_many_moves_what_retry_moves(self):
        self.mixed()
        actions.retry_many(OxTask.objects.all())
        moved = OxTask.objects.filter(status=OxTask.Status.READY, lease_epoch=4)
        assert moved.count() == 4
        assert set(moved.values_list("max_attempts", flat=True)) == {4}
        assert moved.filter(run_after__isnull=False).count() == 0
        assert OxTask.objects.filter(lease_epoch=3).count() == 8

    def test_an_error_mid_way_changes_nothing(self, monkeypatch):
        seed(OxTask.Status.FAILED, 2500)
        ids = list(OxTask.objects.values_list("pk", flat=True))
        calls = []
        original = actions.OxTask.objects.filter

        def filter_then_fail(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("boom")
            return original(*args, **kwargs)

        monkeypatch.setattr(actions.OxTask.objects, "filter", filter_then_fail)
        with pytest.raises(RuntimeError):
            actions.retry_many(ids)
        monkeypatch.undo()
        assert OxTask.objects.filter(status=OxTask.Status.FAILED).count() == 2500
        assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 0

    def test_twenty_thousand_rows_is_one_update_per_chunk(self):
        """
        One SELECT for the ids, one UPDATE per UPDATE_CHUNK_SIZE ids, and the
        transaction's own statements. Never a query per row.
        """
        from django.test.utils import CaptureQueriesContext

        seed(OxTask.Status.FAILED, 14000)
        seed(OxTask.Status.SUCCESSFUL, 6000)
        with CaptureQueriesContext(connections["default"]) as ctx:
            assert actions.retry_many(OxTask.objects.all()) == (14000, 6000)
        updates = [q for q in ctx.captured_queries if q["sql"].startswith("UPDATE")]
        assert len(updates) == 20000 // actions.UPDATE_CHUNK_SIZE
        assert len(ctx) <= len(updates) + 3
        assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 14000

    def test_bulk_retry_fences_the_straggler_out(self, worker):
        stale = lose_the_lease(worker)

        assert actions.retry_many([stale.pk]) == (1, 0)
        db_task = OxTask.objects.get()
        assert db_task.status == OxTask.Status.READY
        assert db_task.lease_epoch == stale.lease_epoch + 1

        assert (
            worker._write_outcome(
                stale,
                status=OxTask.Status.SUCCESSFUL,
                duration_ms=1,
                return_value=3,
            )
            is False
        )
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.READY
        assert db_task.return_value is None

        assert worker.run_once() is True
        db_task.refresh_from_db()
        assert db_task.status == OxTask.Status.SUCCESSFUL
        assert db_task.return_value == 3
