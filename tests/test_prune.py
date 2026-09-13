import re
import threading
import uuid
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, connections
from django.db.models.signals import pre_delete
from django.utils import timezone

from django_ox import actions
from django_ox.durations import parse_duration
from django_ox.models import OxScheduleTick, OxTask


def make_task(status, *, finished_days_ago=None, queue_name="default"):
    now = timezone.now()
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        queue_name=queue_name,
        enqueued_at=now - timedelta(days=400),
        status=status,
        finished_at=(
            now - timedelta(days=finished_days_ago)
            if finished_days_ago is not None
            else None
        ),
    )


def make_tick(name, *, scheduled_days_ago):
    when = timezone.now() - timedelta(days=scheduled_days_ago)
    return OxScheduleTick.objects.create(
        schedule_name=name, scheduled_for=when, created_at=when
    )


def prune(*args):
    out = StringIO()
    call_command("ox_prune", *args, stdout=out)
    return out.getvalue()


class TestParseDuration:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("7d", timedelta(days=7)),
            ("24h", timedelta(hours=24)),
            ("90m", timedelta(minutes=90)),
            ("45s", timedelta(seconds=45)),
            ("3600", timedelta(seconds=3600)),
            (" 7d ", timedelta(days=7)),
        ],
    )
    def test_accepted_forms(self, value, expected):
        assert parse_duration(value) == expected

    @pytest.mark.parametrize("value", ["banana", "1.5d", "-5", "", "7w", "d7", "7 d"])
    def test_rejects_garbage(self, value):
        with pytest.raises(CommandError):
            parse_duration(value)


@pytest.mark.django_db
class TestPrune:
    def test_prunes_old_successful_only_by_default(self):
        old_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        old_failed = make_task(OxTask.Status.FAILED, finished_days_ago=8)
        young_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=1)

        out = prune()

        assert not OxTask.objects.filter(pk=old_ok.pk).exists()
        assert OxTask.objects.filter(pk=old_failed.pk).exists()
        assert OxTask.objects.filter(pk=young_ok.pk).exists()
        assert "Deleted 1 SUCCESSFUL/DISCARDED task row(s)" in out

    def test_queue_restricts_pruning_to_that_queue(self):
        emails = make_task(
            OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="emails"
        )
        reports = make_task(
            OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="reports"
        )

        out = prune("--queue", "emails")

        assert not OxTask.objects.filter(pk=emails.pk).exists()
        assert OxTask.objects.filter(pk=reports.pk).exists()
        assert "Deleted 1 SUCCESSFUL/DISCARDED (queue emails) task row(s)" in out

    def test_queue_dry_run_counts_only_that_queue(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="emails")
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="reports")
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="reports")

        out = prune("--queue", "reports", "--dry-run")

        assert "Would delete 2 SUCCESSFUL/DISCARDED (queue reports) task row(s)" in out
        assert OxTask.objects.count() == 3

    def test_queue_keeps_batching(self, django_assert_num_queries):
        for _ in range(5):
            make_task(
                OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="emails"
            )
        kept = make_task(
            OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="reports"
        )

        # The same 24 queries as test_deletes_in_batches. The queue is one
        # more condition on each statement, not one more statement.
        with django_assert_num_queries(24):
            out = prune("--queue", "emails", "--batch-size", "2")

        assert "Deleted 5 SUCCESSFUL/DISCARDED (queue emails) task row(s)" in out
        assert list(OxTask.objects.values_list("pk", flat=True)) == [kept.pk]

    def test_queue_still_prunes_every_schedules_old_ticks(self):
        make_tick("a", scheduled_days_ago=30)
        latest_a = make_tick("a", scheduled_days_ago=10)
        make_tick("b", scheduled_days_ago=20)
        latest_b = make_tick("b", scheduled_days_ago=9)

        # No task row is in this queue. The tick log is pruned all the same:
        # a tick's queue cannot be read reliably, so --queue does not narrow it.
        out = prune("--queue", "emails")

        assert set(OxScheduleTick.objects.values_list("pk", flat=True)) == {
            latest_a.pk,
            latest_b.pk,
        }
        assert "Deleted 0 SUCCESSFUL/DISCARDED (queue emails) task row(s)" in out
        assert "Deleted 2 schedule tick row(s)" in out

    def test_include_failed_prunes_failed_and_lost_too(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.FAILED, finished_days_ago=8)
        make_task(OxTask.Status.LOST, finished_days_ago=8)
        young_failed = make_task(OxTask.Status.FAILED, finished_days_ago=1)

        out = prune("--include-failed")

        assert set(OxTask.objects.values_list("pk", flat=True)) == {young_failed.pk}
        assert "Deleted 3 SUCCESSFUL/DISCARDED/FAILED/LOST task row(s)" in out

    def test_lost_rows_survive_without_include_failed(self):
        lost = make_task(OxTask.Status.LOST, finished_days_ago=8)

        out = prune()

        assert set(OxTask.objects.values_list("pk", flat=True)) == {lost.pk}
        assert "Deleted 0 SUCCESSFUL/DISCARDED task row(s)" in out

    def test_ready_and_running_never_pruned(self):
        # finished_at is forced to an ancient date to prove the status
        # filter alone protects non-terminal rows, not just null-ness.
        make_task(OxTask.Status.READY, finished_days_ago=399)
        make_task(OxTask.Status.RUNNING, finished_days_ago=399)

        out = prune("--older-than=1s", "--include-failed")

        assert OxTask.objects.count() == 2
        assert "Deleted 0" in out

    def test_older_than_cutoff_boundary(self):
        newer = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=0)
        OxTask.objects.filter(pk=newer.pk).update(
            finished_at=timezone.now() - timedelta(hours=23)
        )
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=2)

        prune("--older-than=24h")

        assert set(OxTask.objects.values_list("pk", flat=True)) == {newer.pk}

    def test_dry_run_deletes_nothing(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)

        out = prune("--dry-run")

        assert "Would delete 2" in out
        assert OxTask.objects.count() == 2

    def test_a_pruned_task_leaves_its_schedule_tick_with_no_task(self):
        task = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        tick = make_tick("a", scheduled_days_ago=1)
        tick.task = task
        tick.save()

        out = prune()

        assert not OxTask.objects.filter(pk=task.pk).exists()
        tick.refresh_from_db()
        assert tick.task_id is None
        assert "Deleted 1 SUCCESSFUL/DISCARDED task row(s)" in out

    def test_deletes_in_batches(self, django_assert_num_queries):
        for _ in range(5):
            make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)

        # Each task batch (2+2+1 rows over three batches) is a pk SELECT,
        # then a transaction holding the lock on the rows (a SELECT ... FOR
        # UPDATE, or on SQLite a no-op UPDATE) and the deletion collector's
        # row SELECT, tick SET NULL, and DELETE. Inside the test's own
        # transaction that one is a savepoint, which adds its SAVEPOINT and
        # RELEASE. Then the empty terminating SELECT, and the tick pass adds
        # its schedule-name SELECT and empty pk SELECT.
        with django_assert_num_queries(24):
            out = prune("--batch-size=2")

        assert "Deleted 5" in out
        assert OxTask.objects.count() == 0

    def test_prunes_old_ticks_but_keeps_each_schedules_latest(self):
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=20)
        latest_a = make_tick("a", scheduled_days_ago=10)
        # A rarely firing schedule keeps its only tick however old it is:
        # it is the anchor the dispatcher measures missed ticks against.
        latest_b = make_tick("b", scheduled_days_ago=40)

        out = prune()

        assert set(OxScheduleTick.objects.values_list("pk", flat=True)) == {
            latest_a.pk,
            latest_b.pk,
        }
        assert "Deleted 2 schedule tick row(s)" in out

    def test_young_ticks_are_kept(self):
        make_tick("a", scheduled_days_ago=1)
        make_tick("a", scheduled_days_ago=2)

        out = prune()

        assert OxScheduleTick.objects.count() == 2
        assert "Deleted 0 schedule tick row(s)" in out

    def test_dry_run_reports_ticks_without_deleting(self):
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=10)

        out = prune("--dry-run")

        assert "Would delete 1 schedule tick row(s)" in out
        assert OxScheduleTick.objects.count() == 2

    def test_rejects_garbage_duration(self):
        with pytest.raises(CommandError):
            call_command("ox_prune", "--older-than=fortnight")

    def test_rejects_non_positive_batch_size(self):
        with pytest.raises(CommandError):
            call_command("ox_prune", "--batch-size=0")


def make_old_rows(n, status):
    now = timezone.now()
    rows = [
        OxTask(
            task_path="tests.tasks.add",
            backend_name="default",
            enqueued_at=now - timedelta(days=31),
            status=status,
            attempts=3,
            max_attempts=3,
            finished_at=now - timedelta(days=30),
        )
        for _ in range(n)
    ]
    OxTask.objects.bulk_create(rows, batch_size=250)
    return [row.pk for row in rows]


def deleted_task_count(out):
    match = re.search(r"Deleted (\d+) \S+ task row\(s\)", out)
    assert match is not None, out
    return int(match.group(1))


def _sql(sql):
    return sql.replace('"', "").replace("`", "").upper()


class Beside:
    """
    An operator action, run on its own thread and connection while ox_prune
    is part way through deleting a batch.

    start() returns once the action has finished, or once it has waited in
    its first write for GRACE seconds. A write that waits that long is
    waiting on the prune, and cannot go on until the prune commits.
    """

    GRACE = 2.0

    def __init__(self, action):
        self.action = action
        self.result = None
        self.error = None
        self.writing = threading.Event()
        self.thread = threading.Thread(target=self._run)

    def _run(self):
        def note_write(execute, sql, params, many, context):
            if _sql(sql).lstrip().startswith(("UPDATE", "DELETE")):
                self.writing.set()
            return execute(sql, params, many, context)

        try:
            with connection.execute_wrapper(note_write):
                self.result = self.action()
        except BaseException as exc:
            self.error = exc
        finally:
            self.writing.set()
            connections.close_all()

    def start(self):
        self.thread.start()
        assert self.writing.wait(30), "the action never reached its write"
        self.thread.join(self.GRACE)

    def finish(self):
        self.thread.join(60)
        assert not self.thread.is_alive(), "the action outlived the prune"
        if self.error is not None:
            raise self.error
        return self.result


def prune_during_batch_delete(widen, action, *args):
    """
    Run ox_prune and start `action` once, inside the deletion of its first
    batch: after Django has read the rows it will delete and before it has
    written anything for them.

    Two ways to reach that point, so the tests do not rest on one statement
    Django happens to send. "statement" goes before the UPDATE that detaches
    the rows' schedule ticks. "signal" is a pre_delete receiver, which Django
    sends before either write. `action` takes the ids in the batch, read from
    that UPDATE's parameters, or None for the signal.
    """
    started = []

    def begin(ids):
        if not started:
            started.append(Beside(lambda: action(ids)))
            started[0].start()

    if widen == "statement":

        def before_tick_update(execute, sql, params, many, context):
            if _sql(sql).startswith("UPDATE DJANGO_OX_OXSCHEDULETICK"):
                # The parameters are the NULL being set, then the batch's ids.
                begin([uuid.UUID(str(value)) for value in params if value is not None])
            return execute(sql, params, many, context)

        with connection.execute_wrapper(before_tick_update):
            out = prune(*args)
    else:

        def receiver(**kwargs):
            begin(None)

        pre_delete.connect(
            receiver, sender=OxTask, weak=False, dispatch_uid="test_prune_window"
        )
        try:
            out = prune(*args)
        finally:
            pre_delete.disconnect(sender=OxTask, dispatch_uid="test_prune_window")
    assert len(started) == 1, "the prune never reached a batch deletion"
    return out, started[0].finish()


def prune_after_batch_selected(action, *args):
    """
    Run ox_prune and run `action` to completion once, just after the prune
    has read its first batch of ids and before its next statement.
    """
    state = {"selected": False, "result": None, "ran": False}

    def wrapper(execute, sql, params, many, context):
        if state["selected"] and not state["ran"]:
            state["ran"] = True
            beside = Beside(action)
            beside.thread.start()
            state["result"] = beside.finish()
        result = execute(sql, params, many, context)
        text = _sql(sql)
        if text.startswith("SELECT") and "DJANGO_OX_OXTASK" in text and "LIMIT" in text:
            state["selected"] = True
        return result

    with connection.execute_wrapper(wrapper):
        out = prune(*args)
    assert state["ran"], "the prune never read a batch"
    return out, state["result"]


WIDEN = ["statement", "signal"]


@pytest.mark.django_db(transaction=True)
class TestPruneAgainstOperatorActions:
    """
    An operator can retry or discard a row while ox_prune is deleting the
    batch it was selected in. Whatever the action reports has to stand: a
    row it moved is still there afterwards, and a row the prune deleted
    was never moved.
    """

    @pytest.mark.parametrize("widen", WIDEN)
    @pytest.mark.parametrize(
        "status,act,moved_to",
        [
            (OxTask.Status.FAILED, actions.retry, OxTask.Status.READY),
            (OxTask.Status.LOST, actions.retry, OxTask.Status.READY),
            (OxTask.Status.FAILED, actions.discard, OxTask.Status.DISCARDED),
        ],
        ids=["retry-failed", "retry-lost", "discard-failed"],
    )
    def test_an_action_inside_the_delete_is_not_undone(
        self, widen, status, act, moved_to
    ):
        victim, *_ = make_old_rows(5, status)

        out, moved = prune_during_batch_delete(
            widen, lambda _ids: act(victim), "--include-failed"
        )

        row = OxTask.objects.filter(pk=victim).first()
        if moved:
            assert row is not None, (
                f"{act.__name__}() reported that it moved the row, and "
                "ox_prune --include-failed deleted it anyway"
            )
            assert row.status == moved_to
        else:
            assert row is None
        assert deleted_task_count(out) == 5 - OxTask.objects.count()

    def test_a_bulk_retry_inside_the_delete_loses_nothing(self):
        # One full batch at the default size, and part of a second.
        make_old_rows(1500, OxTask.Status.FAILED)

        out, (changed, skipped) = prune_during_batch_delete(
            "statement", actions.retry_many, "--include-failed"
        )

        ready = OxTask.objects.filter(status=OxTask.Status.READY).count()
        assert changed + skipped == 1000
        assert ready == changed, (
            f"retry_many() reported {changed} rows requeued and "
            f"{changed - ready} of them were deleted by ox_prune"
        )
        assert deleted_task_count(out) == 1500 - OxTask.objects.count()

    @pytest.mark.parametrize(
        "act,moved_to",
        [
            (actions.retry, OxTask.Status.READY),
            (actions.discard, OxTask.Status.DISCARDED),
        ],
        ids=["retry", "discard"],
    )
    def test_a_row_that_left_the_selection_before_the_delete_survives(
        self, act, moved_to
    ):
        victim, *_ = make_old_rows(5, OxTask.Status.FAILED)

        out, moved = prune_after_batch_selected(lambda: act(victim), "--include-failed")

        assert moved is True
        row = OxTask.objects.filter(pk=victim).first()
        assert row is not None, (
            f"{act.__name__}() moved the row before its batch was deleted, "
            "and ox_prune --include-failed deleted it anyway"
        )
        assert row.status == moved_to
        assert deleted_task_count(out) == 4
        assert OxTask.objects.count() == 1

    @pytest.mark.parametrize("widen", WIDEN)
    def test_the_default_prune_keeps_rows_retried_inside_the_delete(self, widen):
        make_old_rows(3, OxTask.Status.SUCCESSFUL)
        discarded = make_old_rows(2, OxTask.Status.DISCARDED)
        failed = make_old_rows(2, OxTask.Status.FAILED)

        def act(_ids):
            return [actions.retry(pk) for pk in [*failed, discarded[0]]]

        out, results = prune_during_batch_delete(widen, act)

        assert results == [True, True, False]
        assert set(OxTask.objects.values_list("pk", "status")) == {
            (pk, OxTask.Status.READY) for pk in failed
        }
        assert "Deleted 5 SUCCESSFUL/DISCARDED task row(s)" in out
