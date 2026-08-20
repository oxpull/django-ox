from datetime import timedelta

import pytest
from django.utils import timezone

from django_ox import stats
from django_ox.models import OxTask

from .tasks import add, fail_always


def make_task(
    status=OxTask.Status.READY,
    *,
    queue="default",
    enqueued_minutes_ago=0.0,
    run_after_minutes=None,
    finished_minutes_ago=None,
    last_attempted_minutes_ago=None,
):
    """run_after_minutes is relative to now: negative is past, positive future."""
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        queue_name=queue,
        status=status,
        enqueued_at=now - timedelta(minutes=enqueued_minutes_ago),
        run_after=(
            now + timedelta(minutes=run_after_minutes)
            if run_after_minutes is not None
            else None
        ),
        finished_at=(
            now - timedelta(minutes=finished_minutes_ago)
            if finished_minutes_ago is not None
            else None
        ),
        last_attempted_at=(
            now - timedelta(minutes=last_attempted_minutes_ago)
            if last_attempted_minutes_ago is not None
            else None
        ),
    )


@pytest.mark.django_db
class TestQueueStats:
    def test_empty_table(self):
        assert stats.queue_stats() == []

    def test_counts_per_queue_and_status(self):
        make_task(OxTask.Status.READY)
        make_task(OxTask.Status.READY, run_after_minutes=60)  # deferred still READY
        make_task(OxTask.Status.RUNNING)
        make_task(OxTask.Status.FAILED)
        make_task(OxTask.Status.SUCCESSFUL, queue="emails")
        make_task(OxTask.Status.SUCCESSFUL, queue="emails")

        make_task(OxTask.Status.LOST)

        assert stats.queue_stats() == [
            stats.QueueStats(
                queue_name="default",
                ready=2,
                running=1,
                failed=1,
                successful=0,
                lost=1,
            ),
            stats.QueueStats(
                queue_name="emails", ready=0, running=0, failed=0, successful=2, lost=0
            ),
        ]


@pytest.mark.django_db
class TestReadyCount:
    def test_counts_only_eligible_ready(self):
        make_task(OxTask.Status.READY)
        make_task(OxTask.Status.READY, run_after_minutes=-5)  # due
        make_task(OxTask.Status.READY, run_after_minutes=60)  # deferred
        make_task(OxTask.Status.RUNNING)

        assert stats.ready_count() == 2

    def test_queue_filter(self):
        make_task(OxTask.Status.READY)
        make_task(OxTask.Status.READY, queue="emails")

        assert stats.ready_count("emails") == 1
        assert stats.ready_count("empty") == 0


@pytest.mark.django_db
class TestOldestReadyAge:
    def test_none_when_nothing_waits(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
        assert stats.oldest_ready_age() is None

    def test_age_from_enqueue(self):
        make_task(enqueued_minutes_ago=10)
        make_task(enqueued_minutes_ago=2)

        age = stats.oldest_ready_age()
        assert timedelta(minutes=9, seconds=50) <= age
        assert age <= timedelta(minutes=10, seconds=10)

    def test_deferred_task_ages_from_run_after(self):
        # Enqueued an hour ago but only became eligible five minutes ago:
        # the backlog age is five minutes, not sixty.
        make_task(enqueued_minutes_ago=60, run_after_minutes=-5)

        age = stats.oldest_ready_age()
        assert timedelta(minutes=4, seconds=50) <= age
        assert age <= timedelta(minutes=5, seconds=10)

    def test_future_deferred_is_not_backlog(self):
        make_task(run_after_minutes=60)
        assert stats.oldest_ready_age() is None


@pytest.mark.django_db
class TestThroughput:
    def test_terminal_states_in_window_per_minute(self):
        for _ in range(3):
            make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
        make_task(OxTask.Status.FAILED, finished_minutes_ago=1)
        make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=10)  # outside
        make_task(OxTask.Status.READY)  # not terminal

        assert stats.throughput(window=timedelta(minutes=5)) == pytest.approx(0.8)

    def test_queue_filter(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
        make_task(OxTask.Status.SUCCESSFUL, queue="emails", finished_minutes_ago=1)

        assert stats.throughput(
            window=timedelta(minutes=2), queue_name="emails"
        ) == pytest.approx(0.5)

    @pytest.mark.parametrize("window", [timedelta(0), timedelta(seconds=-1)])
    def test_rejects_non_positive_window(self, window):
        with pytest.raises(ValueError, match="positive"):
            stats.throughput(window=window)


@pytest.mark.django_db
class TestFailureRate:
    def test_none_when_nothing_finished(self):
        make_task(OxTask.Status.READY)
        assert stats.failure_rate() is None

    def test_fraction_of_terminal_outcomes(self):
        for _ in range(3):
            make_task(OxTask.Status.SUCCESSFUL, finished_minutes_ago=1)
        make_task(OxTask.Status.FAILED, finished_minutes_ago=1)
        make_task(OxTask.Status.FAILED, finished_minutes_ago=10)  # outside window

        assert stats.failure_rate(window=timedelta(minutes=5)) == pytest.approx(0.25)

    def test_queue_filter(self):
        make_task(OxTask.Status.FAILED, finished_minutes_ago=1)
        make_task(OxTask.Status.SUCCESSFUL, queue="emails", finished_minutes_ago=1)

        assert stats.failure_rate(queue_name="emails") == pytest.approx(0.0)


@pytest.mark.django_db
class TestLastClaimAge:
    def test_none_when_nothing_ever_claimed(self):
        make_task(OxTask.Status.READY)
        assert stats.last_claim_age() is None

    def test_uses_most_recent_claim(self):
        make_task(OxTask.Status.SUCCESSFUL, last_attempted_minutes_ago=30)
        make_task(OxTask.Status.SUCCESSFUL, last_attempted_minutes_ago=5)

        age = stats.last_claim_age()
        assert timedelta(minutes=4, seconds=50) <= age
        assert age <= timedelta(minutes=5, seconds=10)

    def test_queue_filter(self):
        make_task(OxTask.Status.SUCCESSFUL, last_attempted_minutes_ago=5)
        assert stats.last_claim_age("emails") is None


@pytest.mark.django_db
class TestAgainstRealWorker:
    def test_metrics_reflect_executed_tasks(self, worker):
        add.enqueue(1, 2)
        fail_always.enqueue()
        for _ in range(4):  # 1 success + 3 failing attempts
            assert worker.run_once() is True

        assert stats.queue_stats() == [
            stats.QueueStats(
                queue_name="default", ready=0, running=0, failed=1, successful=1
            )
        ]
        assert stats.ready_count() == 0
        assert stats.throughput(window=timedelta(minutes=1)) == pytest.approx(2.0)
        assert stats.failure_rate(window=timedelta(minutes=1)) == pytest.approx(0.5)
        assert stats.last_claim_age() < timedelta(minutes=1)
