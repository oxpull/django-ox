import json
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from io import StringIO

import pytest
from django.conf import settings
from django.core.management import ManagementUtility, call_command
from django.core.management.base import CommandError
from django.db import (
    DatabaseError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.models.signals import pre_delete
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import _waiting, actions
from django_ox.durations import parse_duration
from django_ox.models import OxScheduleTick, OxTask

from .contention import CONTENTION, failing, in_key_order, simulated
from .test_waiting import waiting_for_a_lock


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
            ("0", timedelta(0)),
            (" 7d ", timedelta(days=7)),
        ],
    )
    def test_accepted_forms(self, value, expected):
        assert parse_duration(value) == expected

    @pytest.mark.parametrize("value", ["banana", "1.5d", "-5", "", "7w", "d7", "7 d"])
    def test_rejects_garbage(self, value):
        with pytest.raises(CommandError):
            parse_duration(value)

    @pytest.mark.parametrize(
        "value", ["1000000000d", "99999999999999d", "9" * 5000 + "d"]
    )
    def test_rejects_out_of_range(self, value):
        with pytest.raises(CommandError) as info:
            parse_duration(value)
        assert str(info.value) == f"Invalid duration {value!r}; it is out of range."

    def test_a_duration_that_only_overflows_at_the_cutoff_still_parses(self):
        # ox_prune's own guard catches this one; see TestOutOfRangeDuration.
        assert parse_duration("3000000d") == timedelta(days=3_000_000)


@pytest.mark.django_db
class TestPrune:
    def test_an_unreadable_duration_keeps_the_forms_message(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)

        with pytest.raises(CommandError, match="use forms like 7d"):
            prune("--older-than=soon")

        assert OxTask.objects.count() == 1

    def test_zero_prunes_a_row_finished_just_now(self):
        # The default 7d would keep this row.
        just_finished = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=0)

        prune("--older-than=0")

        assert not OxTask.objects.filter(pk=just_finished.pk).exists()

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

    def test_prune_never_deletes_a_waiting_row(self):
        # The same forced finished_at: a waiting row has not run, whatever
        # its columns say, and the widest prune leaves it.
        waiting = make_task(OxTask.Status.WAITING, finished_days_ago=399)

        out = prune("--older-than=1s", "--include-failed")

        assert set(OxTask.objects.values_list("pk", flat=True)) == {waiting.pk}
        assert "Deleted 0" in out

    def test_a_queue_prune_never_deletes_a_waiting_row(self):
        # Waiting rows in the named queue and in another one, with the same
        # forced finished_at. The finished row beside them shows the prune
        # did delete something.
        waiting = make_task(
            OxTask.Status.WAITING, finished_days_ago=399, queue_name="emails"
        )
        elsewhere = make_task(OxTask.Status.WAITING, finished_days_ago=399)
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=399, queue_name="emails")
        args = ("--queue", "emails", "--older-than=1s", "--include-failed")
        label = "SUCCESSFUL/DISCARDED/FAILED/LOST (queue emails)"

        dry = prune(*args, "--dry-run")
        out = prune(*args)

        assert f"Would delete 1 {label} task row(s)" in dry
        assert f"Deleted 1 {label} task row(s)" in out
        assert set(OxTask.objects.values_list("pk", flat=True)) == {
            waiting.pk,
            elsewhere.pk,
        }

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

    @pytest.mark.parametrize(
        "args", [(), ("--queue", "default")], ids=["every-queue", "queue"]
    )
    def test_the_locking_read_takes_its_rows_in_key_order(self, args):
        # A locking read locks rows in the order it reads them. In key order,
        # it takes the rows it shares with any writer that also locks in key
        # order in the same order, so one waits for the other and they can't
        # deadlock over those rows.
        if not connection.features.has_select_for_update:
            pytest.skip("SQLite has no row locks, and its prune sends no locking read")
        for _ in range(3):
            make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        statements = []

        def record(execute, sql, params, many, context):
            statements.append(_sql(sql).strip())
            return execute(sql, params, many, context)

        with connection.execute_wrapper(record):
            out = prune(*args, "--batch-size=2")

        locking = [sql for sql in statements if "FOR UPDATE" in sql]
        assert len(locking) == 2, statements
        # The pk is the only column the read selects, so ORDER BY 1 is the pk.
        in_key_order = re.compile(
            r"SELECT DJANGO_OX_OXTASK\.ID(?: AS PK)? FROM .*"
            r" ORDER BY (?:1|PK|DJANGO_OX_OXTASK\.ID) ASC FOR UPDATE"
        )
        for sql in locking:
            assert in_key_order.fullmatch(sql), sql
        assert deleted_task_count(out) == 3

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


def make_old_rows(n, status, *, queue_name="default"):
    now = timezone.now()
    rows = [
        OxTask(
            task_path="tests.tasks.add",
            backend_name="default",
            queue_name=queue_name,
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
    match = re.search(r"Deleted (\d+) \S+(?: \(queue \S+\))? task row\(s\)", out)
    assert match is not None, out
    return int(match.group(1))


def _sql(sql):
    return sql.replace('"', "").replace("`", "").upper()


def _writes_or_locks(sql):
    text = _sql(sql).lstrip()
    return text.startswith(("UPDATE", "DELETE")) or "FOR UPDATE" in text


class Beside:
    """
    An operator action, run on its own thread and connection while ox_prune
    is part way through deleting a batch.

    start() returns once the action has finished, or once it has waited in
    its first write or locking read for GRACE seconds. A statement that waits
    that long is waiting on the prune, and cannot go on until the prune
    commits.
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
            if _writes_or_locks(sql):
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


def set_status_only(pk):
    """
    Move a row to READY and leave finished_at as it was. No operator action
    does that. Each one that moves a finished row also writes finished_at.
    This one changes the status alone.
    """
    return OxTask.objects.filter(pk=pk).update(status=OxTask.Status.READY) == 1


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


def prune_while_an_action_is_open(action, *args, hold=5.0):
    """
    Run `action` in a transaction on its own thread, then ox_prune. The
    action's transaction stays open until the prune has finished, or for
    `hold` seconds, whichever comes first.

    Returns the prune's output, the action's result, and whether the prune
    finished while the action's transaction was still open. A prune that
    waits on a row the action wrote can't finish before then.
    """
    acted = threading.Event()
    pruned = threading.Event()
    state = {"result": None, "error": None, "pruned_first": False}

    def act():
        try:
            with transaction.atomic(using="default"):
                state["result"] = action()
                acted.set()
                state["pruned_first"] = pruned.wait(hold)
        except BaseException as exc:
            state["error"] = exc
        finally:
            acted.set()
            connections.close_all()

    thread = threading.Thread(target=act)
    thread.start()
    assert acted.wait(30), "the action never ran"
    try:
        out = prune(*args)
    finally:
        pruned.set()
        thread.join(60)
    assert not thread.is_alive(), "the action outlived the prune"
    if state["error"] is not None:
        raise state["error"]
    return out, state["result"], state["pruned_first"]


def prune_beside_an_open_action(action, *args):
    """
    Run `action` in a transaction on its own thread, then ox_prune. The
    action's transaction stays open until the prune sends its first write or
    locking read, and commits a moment later, while that statement waits on
    the rows the action wrote.
    """
    acted = threading.Event()
    reached = threading.Event()
    state = {"result": None, "error": None, "reached": False}

    def act():
        try:
            with transaction.atomic(using="default"):
                state["result"] = action()
                acted.set()
                reached.wait(30)
                time.sleep(Beside.GRACE / 4)
        except BaseException as exc:
            state["error"] = exc
        finally:
            acted.set()
            connections.close_all()

    def note_reach(execute, sql, params, many, context):
        if _writes_or_locks(sql):
            state["reached"] = True
            reached.set()
        return execute(sql, params, many, context)

    thread = threading.Thread(target=act)
    thread.start()
    assert acted.wait(30), "the action never ran"
    try:
        with connection.execute_wrapper(note_reach):
            out = prune(*args)
    finally:
        reached.set()
        thread.join(60)
    assert not thread.is_alive(), "the action outlived the prune"
    if state["error"] is not None:
        raise state["error"]
    assert state["reached"], "the prune never wrote or locked a row"
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
        "args", [(), ("--queue", "emails")], ids=["every-queue", "queue"]
    )
    @pytest.mark.parametrize(
        "act,moved_to",
        [
            (actions.retry, OxTask.Status.READY),
            (actions.discard, OxTask.Status.DISCARDED),
            (set_status_only, OxTask.Status.READY),
        ],
        ids=["retry", "discard", "status-only"],
    )
    def test_a_row_that_left_the_selection_before_the_delete_survives(
        self, act, moved_to, args
    ):
        # Each move takes the row out of the selection a different way. A
        # retry clears finished_at and sets a status the prune never deletes.
        # A discard sets a status the prune deletes and stamps finished_at
        # with now. A status change alone leaves finished_at old. So the
        # check needs the cutoff for the discard and the status for the
        # status change, with or without --queue.
        victim, *_ = make_old_rows(5, OxTask.Status.FAILED, queue_name="emails")

        out, moved = prune_after_batch_selected(
            lambda: act(victim), *args, "--include-failed"
        )

        assert moved is True
        row = OxTask.objects.filter(pk=victim).first()
        command = " ".join(["ox_prune", *args, "--include-failed"])
        assert row is not None, (
            f"{act.__name__}() moved the row before its batch was deleted, "
            f"and {command} deleted it anyway"
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

    @pytest.mark.parametrize("widen", WIDEN)
    def test_a_queue_prune_keeps_rows_retried_inside_the_delete(self, widen):
        """
        ox_prune --queue checks each batch again, as a prune of every queue
        does. A retry in the named queue that lands while its batch is deleted
        either keeps its row or reports that nothing moved. Rows in another
        queue are neither deleted nor counted, whether they were retried or
        not.
        """
        victim, *_ = make_old_rows(5, OxTask.Status.FAILED, queue_name="emails")
        retried, untouched = make_old_rows(2, OxTask.Status.FAILED)

        def act(_ids):
            return actions.retry(victim), actions.retry(retried)

        out, (moved, moved_elsewhere) = prune_during_batch_delete(
            widen, act, "--queue", "emails", "--include-failed"
        )

        row = OxTask.objects.filter(pk=victim).first()
        if moved:
            assert row is not None, (
                "retry() reported that it moved the row, and "
                "ox_prune --queue emails --include-failed deleted it anyway"
            )
            assert row.status == OxTask.Status.READY
        else:
            assert row is None
        assert moved_elsewhere is True
        assert set(
            OxTask.objects.filter(queue_name="default").values_list("pk", "status")
        ) == {(retried, OxTask.Status.READY), (untouched, OxTask.Status.FAILED)}
        left = OxTask.objects.filter(queue_name="emails").count()
        assert deleted_task_count(out) == 5 - left
        assert "(queue emails)" in out

    @pytest.mark.parametrize(
        "args,pruned_first",
        [(("--queue", "emails"), True), ((), False)],
        ids=["queue", "every-queue"],
    )
    def test_a_queue_prune_does_not_wait_on_another_queues_retry(
        self, args, pruned_first
    ):
        """
        A retry in the default queue has moved its row and not committed. A
        prune of the emails queue neither locks that row nor waits for it. A
        prune of every queue selected the row and has to wait, which shows
        the test can see a wait.
        """
        if not connection.features.has_select_for_update:
            pytest.skip("SQLite lets one writer in at a time, whatever the queue")
        make_old_rows(3, OxTask.Status.FAILED, queue_name="emails")
        (elsewhere,) = make_old_rows(1, OxTask.Status.FAILED)

        out, moved, finished = prune_while_an_action_is_open(
            lambda: actions.retry(elsewhere), *args, "--include-failed"
        )

        assert moved is True
        assert finished is pruned_first
        assert deleted_task_count(out) == 3
        assert set(OxTask.objects.values_list("pk", "status")) == {
            (elsewhere, OxTask.Status.READY)
        }

    @pytest.mark.parametrize("widen", WIDEN)
    def test_a_revive_inside_the_delete_is_not_undone(self, widen):
        """
        A package built on django-ox can move a DISCARDED row back to WAITING.
        A revive that reports the row revived keeps it. One the prune got to
        first reports it not found.
        """
        victim, *_ = make_old_rows(5, OxTask.Status.DISCARDED)

        def revive(_ids):
            try:
                return _waiting.revive_many([(victim, 0)], using="default")
            except OperationalError:
                # SQLite has no row locks. A revive that has read while the
                # prune holds the write lock can't take it, and moves nothing.
                if connection.vendor != "sqlite":
                    raise
                return None

        out, result = prune_during_batch_delete(widen, revive)

        row = OxTask.objects.filter(pk=victim).first()
        if result == {victim: _waiting.Revival.REVIVED}:
            assert row is not None, (
                "revive_many() reported the row revived, and ox_prune deleted it anyway"
            )
            assert (row.status, row.lease_epoch) == (OxTask.Status.WAITING, 1)
        else:
            assert result in (None, {victim: _waiting.Revival.NOT_FOUND})
            assert row is None
        assert deleted_task_count(out) == 5 - OxTask.objects.count()

    def test_a_prune_that_waits_on_an_open_revive_keeps_the_row(self):
        """
        The revive has moved the row back to WAITING and not yet committed
        when the prune reaches the row. The prune waits for it, then finds the
        row no longer prunable and leaves it.
        """
        victim, *_ = make_old_rows(3, OxTask.Status.DISCARDED)

        out, result = prune_beside_an_open_action(
            lambda: _waiting.revive_many([(victim, 0)], using="default")
        )

        assert result == {victim: _waiting.Revival.REVIVED}
        row = OxTask.objects.filter(pk=victim).first()
        assert row is not None, (
            "revive_many() revived the row, and ox_prune deleted it once the "
            "revive committed"
        )
        assert (row.status, row.lease_epoch) == (OxTask.Status.WAITING, 1)
        assert deleted_task_count(out) == 2


def old_failed_rows(keys, *, queue_name="default"):
    """Old FAILED rows with these keys, inserted in the order given."""
    now = timezone.now()
    for pk in keys:
        OxTask.objects.create(
            id=pk,
            task_path="tests.tasks.add",
            backend_name="default",
            queue_name=queue_name,
            enqueued_at=now - timedelta(days=31),
            status=OxTask.Status.FAILED,
            attempts=3,
            max_attempts=3,
            finished_at=now - timedelta(days=30),
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("waits_first", ["action", "prune"])
@pytest.mark.parametrize(
    "act,moved_to",
    [
        (actions.retry_many, OxTask.Status.READY),
        (actions.discard_many, OxTask.Status.DISCARDED),
    ],
    ids=["retry_many", "discard_many"],
)
def test_a_bulk_action_and_a_prune_of_the_same_rows_wait_rather_than_deadlock(
    act, moved_to, waits_first
):
    """
    Two old FAILED rows, the larger key inserted first, so a scan in table
    order meets them in the reverse of key order. A third session holds one of
    them. The first caller waits on it, the second caller starts and waits
    too, and then the holder commits.

    An action whose UPDATE locks in table order deadlocks with the prune in
    both of these on PostgreSQL. When the action waits first, on the larger
    key, the prune takes the smaller key while it queues for the larger, and
    the action wants the smaller one next. When the prune waits first, on the
    smaller key, the action takes the larger one while it queues for the
    smaller, and the prune wants the larger one next. Locking in key order,
    the second caller queues for the smaller key holding nothing.

    Every statement on both sides is watched. A deadlock a retry then hid
    still fails the test.
    """
    if not connection.features.has_select_for_update:
        pytest.skip("SQLite has no row locks to take in any order")
    small, large = in_key_order(uuid.uuid4() for _ in range(2))
    old_failed_rows([large, small])
    held = large if waits_first == "action" else small

    locked = threading.Event()
    release = threading.Event()
    errors = []
    outcome = {}

    def hold():
        try:
            with transaction.atomic():
                list(
                    OxTask.objects.select_for_update().filter(pk=held).values_list("pk")
                )
                locked.set()
                release.wait(30)
        finally:
            locked.set()
            connections.close_all()

    def watched(name, body):
        def note(execute, sql, params, many, context):
            try:
                return execute(sql, params, many, context)
            except DatabaseError as exc:
                errors.append((name, exc))
                raise

        try:
            with connection.execute_wrapper(note):
                outcome[name] = body()
        except Exception as exc:
            outcome[name] = exc
        finally:
            connections.close_all()

    callers = {
        "action": lambda: act([small, large]),
        "prune": lambda: prune("--include-failed"),
    }
    order = [waits_first, "prune" if waits_first == "action" else "action"]
    holder = threading.Thread(target=hold)
    holder.start()
    assert locked.wait(10)
    threads = []
    waited = []
    for count, name in enumerate(order, start=1):
        thread = threading.Thread(target=watched, args=(name, callers[name]))
        thread.start()
        threads.append(thread)
        waited.append(waiting_for_a_lock(count))
    release.set()
    for thread in [holder, *threads]:
        thread.join(60)
        assert not thread.is_alive()

    # The interleaving happened: each caller waited on a lock.
    assert waited == [True, True]
    assert errors == []
    rows = dict(OxTask.objects.values_list("pk", "status"))
    if waits_first == "action":
        assert outcome["action"] == (2, 0)
        assert rows == {small: moved_to, large: moved_to}
        assert deleted_task_count(outcome["prune"]) == 0
    else:
        assert deleted_task_count(outcome["prune"]) == 2
        assert outcome["action"] == (0, 2)
        assert rows == {}


@pytest.mark.django_db(transaction=True)
class TestPruneAfterContention:
    """
    ox_prune commits batch by batch. A batch that loses a deadlock or a
    serialization failure runs again in a new transaction, up to three times,
    and after that the command stops with an error that says a rerun finishes.
    The errors are simulated in place of the batch's DELETE, shaped as Django
    raises the drivers' (tests/test_contention.py pins the shape).
    """

    ARGS = ("--include-failed", "--batch-size=2")
    DELETE = "DELETE FROM DJANGO_OX_OXTASK"

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_batch_that_loses_runs_again_and_checks_its_rows_again(self, kind):
        make_old_rows(5, OxTask.Status.FAILED)
        state = {"retried": None}

        def act(execute, sql, params, many, context):
            # The first statement after the lost DELETE belongs to the second
            # attempt at that batch. Before it runs, an operator retries one of
            # the batch's rows on another connection.
            if len(deletes) == 2 and state["retried"] is None:
                pk = deletes[1][1][0]
                beside = Beside(lambda: actions.retry(pk))
                beside.thread.start()
                assert beside.finish() is True
                state["retried"] = pk
            return execute(sql, params, many, context)

        with (
            connection.execute_wrapper(act),
            failing(self.DELETE, lambda: simulated(kind), lambda n: n == 2) as deletes,
        ):
            out = prune(*self.ARGS)

        # Batches of 2, 1 (the retried row left its batch) and 1.
        assert deleted_task_count(out) == 4
        assert len(deletes) == 4
        assert set(OxTask.objects.values_list("pk", "status")) == {
            (state["retried"], OxTask.Status.READY)
        }

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_batch_that_lost_leaves_the_next_batch_its_own_three_attempts(self, kind):
        """
        The three attempts belong to the batch, not to the run. Five rows in
        batches of two, where the first batch loses once and then commits, and
        the second loses twice before it commits. The second batch's third
        attempt is the one that deletes it, so the command finishes and every
        row is gone. A run that shared one budget across its batches would
        stop here with two rows left.
        """
        make_old_rows(5, OxTask.Status.FAILED)

        with failing(
            self.DELETE, lambda: simulated(kind), lambda n: n in (1, 3, 4)
        ) as deletes:
            out = prune(*self.ARGS)

        # Batch one twice, batch two three times, batch three once.
        assert len(deletes) == 6
        assert deleted_task_count(out) == 5
        assert OxTask.objects.count() == 0

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_batch_that_keeps_losing_stops_the_prune_and_a_rerun_finishes(self, kind):
        make_old_rows(5, OxTask.Status.FAILED)

        with (
            failing(self.DELETE, lambda: simulated(kind), lambda n: n >= 2) as deletes,
            pytest.raises(CommandError) as info,
        ):
            prune(*self.ARGS)

        assert str(info.value) == (
            "Stopped after deleting 2 SUCCESSFUL/DISCARDED/FAILED/LOST task row(s). "
            "The next batch hit a database deadlock or serialization failure 3 "
            "times. The rows already deleted stay deleted. Run ox_prune again to "
            "prune the rest."
        )
        assert isinstance(info.value.__cause__, OperationalError)
        assert len(deletes) == 1 + 3
        assert OxTask.objects.count() == 3

        assert deleted_task_count(prune(*self.ARGS)) == 3
        assert OxTask.objects.count() == 0

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_prune_that_gives_up_exits_non_zero_from_the_command_line(
        self, kind, monkeypatch, capsys
    ):
        """
        Through the command line, as cron runs it, rather than call_command.
        The command exits 1 with the message on stderr, having paused between
        its attempts at the batch.
        """
        make_old_rows(5, OxTask.Status.FAILED)
        pauses = []
        monkeypatch.setattr("django_ox._contention.pause", pauses.append)
        # The system checks read every alias, which this test doesn't open.
        argv = ["manage.py", "ox_prune", *self.ARGS, "--skip-checks"]

        with (
            failing(self.DELETE, lambda: simulated(kind), lambda n: n >= 2),
            pytest.raises(SystemExit) as info,
        ):
            ManagementUtility(argv).execute()

        assert info.value.code == 1
        err = capsys.readouterr().err
        assert "Stopped after deleting 2 " in err
        assert "Run ox_prune again to prune the rest." in err
        assert pauses == [1, 2]
        assert OxTask.objects.count() == 3

    def test_a_batch_whose_commit_fails_counts_once(self, monkeypatch):
        make_old_rows(5, OxTask.Status.FAILED)
        wrapper = connections["default"]
        real_commit = wrapper.commit
        failed = []

        def commit():
            if not failed:
                failed.append(True)
                raise simulated("postgresql-serialization")
            return real_commit()

        monkeypatch.setattr(wrapper, "commit", commit)
        out = prune(*self.ARGS)
        monkeypatch.undo()

        assert failed == [True]
        assert deleted_task_count(out) == 5
        assert OxTask.objects.count() == 0

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_inside_a_callers_transaction_a_lost_batch_is_not_run_again(self, kind):
        make_old_rows(5, OxTask.Status.FAILED)

        with (
            failing(self.DELETE, lambda: simulated(kind), lambda n: n == 1) as deletes,
            pytest.raises(OperationalError),
            transaction.atomic(),
        ):
            prune(*self.ARGS)

        assert len(deletes) == 1
        assert OxTask.objects.count() == 5

    def test_any_other_database_error_stops_the_prune_at_once(self):
        make_old_rows(5, OxTask.Status.FAILED)

        with (
            failing(
                self.DELETE,
                lambda: OperationalError("disk I/O error"),
                lambda n: n == 2,
            ) as deletes,
            pytest.raises(OperationalError, match="disk I/O"),
        ):
            prune(*self.ARGS)

        assert len(deletes) == 2
        assert OxTask.objects.count() == 3


@pytest.mark.django_db(transaction=True)
class TestPruneJsonFormat:
    def test_json_format_prunes_and_outputs_exact_schema(self):
        old_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.FAILED, finished_days_ago=8)
        make_tick("a", scheduled_days_ago=30)
        latest_tick = make_tick("a", scheduled_days_ago=5)

        out = prune("--format", "json")
        data = json.loads(out)
        # One line, so a cron line can pipe it straight into jq or a
        # `while read` loop. json.loads() alone accepts pretty-printing.
        assert out.strip() == json.dumps(data)

        assert data["queue"] is None
        assert data["statuses"] == ["SUCCESSFUL", "DISCARDED"]
        assert data["task_rows"] == 1
        assert data["tick_rows"] == 1
        assert data["dry_run"] is False
        assert "cutoff" in data
        assert datetime.fromisoformat(data["cutoff"])
        assert set(data.keys()) == {
            "queue",
            "cutoff",
            "statuses",
            "task_rows",
            "tick_rows",
            "dry_run",
        }
        assert not OxTask.objects.filter(pk=old_ok.pk).exists()
        assert OxScheduleTick.objects.filter(pk=latest_tick.pk).exists()

    def test_json_format_with_queue(self):
        emails = make_task(
            OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="emails"
        )
        reports = make_task(
            OxTask.Status.SUCCESSFUL, finished_days_ago=8, queue_name="reports"
        )

        out = prune("--format", "json", "--queue", "emails")
        data = json.loads(out)

        assert data["queue"] == "emails"
        assert data["task_rows"] == 1
        assert not OxTask.objects.filter(pk=emails.pk).exists()
        assert OxTask.objects.filter(pk=reports.pk).exists()

    def test_json_format_include_failed(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_task(OxTask.Status.FAILED, finished_days_ago=8)
        make_task(OxTask.Status.LOST, finished_days_ago=8)

        out = prune("--format", "json", "--include-failed")
        data = json.loads(out)

        assert data["statuses"] == ["SUCCESSFUL", "DISCARDED", "FAILED", "LOST"]
        assert data["task_rows"] == 3
        assert OxTask.objects.count() == 0

    def test_json_format_dry_run(self):
        make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=5)

        out = prune("--format", "json", "--dry-run")
        data = json.loads(out)

        assert data["dry_run"] is True
        assert data["task_rows"] == 1
        assert data["tick_rows"] == 1
        assert OxTask.objects.count() == 1
        assert OxScheduleTick.objects.count() == 2

    TICK_DELETE = "DELETE FROM DJANGO_OX_OXSCHEDULETICK"

    def test_text_format_reports_task_rows_when_tick_pruning_fails(self):
        # The task rows are gone whatever happens next. A failure while
        # pruning ticks must not take their count with it.
        make_old_rows(2, OxTask.Status.SUCCESSFUL)
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=5)

        out = StringIO()
        with (
            failing(
                self.TICK_DELETE, lambda: simulated("mysql-deadlock"), lambda n: n == 1
            ),
            pytest.raises(DatabaseError),
        ):
            call_command("ox_prune", stdout=out)

        body = out.getvalue()
        assert "Deleted 2" in body
        assert "task row(s)" in body
        assert "schedule tick row(s)" not in body

    def test_json_format_reports_task_rows_when_tick_pruning_fails(self):
        make_old_rows(2, OxTask.Status.SUCCESSFUL)
        make_tick("a", scheduled_days_ago=30)
        make_tick("a", scheduled_days_ago=5)

        out = StringIO()
        with (
            failing(
                self.TICK_DELETE, lambda: simulated("mysql-deadlock"), lambda n: n == 1
            ),
            pytest.raises(DatabaseError),
        ):
            call_command("ox_prune", "--format", "json", stdout=out)

        data = json.loads(out.getvalue())
        assert data["task_rows"] == 2
        assert data["tick_rows"] == 0
        assert data["dry_run"] is False

    def test_json_format_reports_tick_rows_deleted_before_a_failure(self):
        make_tick("a", scheduled_days_ago=5)
        for days in (40, 39, 38, 37):
            make_tick("a", scheduled_days_ago=days)

        out = StringIO()
        with (
            failing(
                self.TICK_DELETE, lambda: simulated("mysql-deadlock"), lambda n: n == 2
            ),
            pytest.raises(DatabaseError),
        ):
            call_command("ox_prune", "--format", "json", "--batch-size=1", stdout=out)

        data = json.loads(out.getvalue())
        assert data["tick_rows"] == 1

    def test_json_format_cutoff_follows_older_than(self):
        before = timezone.now()
        out = prune("--format", "json", "--older-than", "30d")
        after = timezone.now()

        cutoff = datetime.fromisoformat(json.loads(out)["cutoff"])
        # Aware or naive follows USE_TZ, the way timezone.now() does. The
        # naive settings modules run this too.
        assert (cutoff.utcoffset() is not None) is settings.USE_TZ
        assert before - timedelta(days=30) <= cutoff <= after - timedelta(days=30)

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_json_format_contention_reports_partial_task_rows(self, kind):
        make_old_rows(5, OxTask.Status.FAILED)

        out = StringIO()
        with (
            failing(
                TestPruneAfterContention.DELETE,
                lambda: simulated(kind),
                lambda n: n >= 2,
            ),
            pytest.raises(CommandError),
        ):
            call_command(
                "ox_prune",
                "--format",
                "json",
                "--include-failed",
                "--batch-size=2",
                stdout=out,
            )

        data = json.loads(out.getvalue())
        assert data["task_rows"] == 2
        assert data["tick_rows"] == 0
        assert data["dry_run"] is False
        assert data["statuses"] == ["SUCCESSFUL", "DISCARDED", "FAILED", "LOST"]
        assert OxTask.objects.count() == 3

    @pytest.mark.parametrize("kind", CONTENTION)
    def test_json_format_contention_command_line_exit_non_zero(
        self, kind, monkeypatch, capsys
    ):
        make_old_rows(5, OxTask.Status.FAILED)
        pauses = []
        monkeypatch.setattr("django_ox._contention.pause", pauses.append)
        argv = [
            "manage.py",
            "ox_prune",
            "--format",
            "json",
            "--include-failed",
            "--batch-size=2",
            "--skip-checks",
        ]

        with (
            failing(
                TestPruneAfterContention.DELETE,
                lambda: simulated(kind),
                lambda n: n >= 2,
            ),
            pytest.raises(SystemExit) as info,
        ):
            ManagementUtility(argv).execute()

        assert info.value.code == 1
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["task_rows"] == 2
        assert data["tick_rows"] == 0
        assert data["dry_run"] is False
        assert "Stopped after deleting 2 " in captured.err
        assert pauses == [1, 2]
        assert OxTask.objects.count() == 3


OUT_OF_RANGE = ["3000000d", "1000000000d", "99999999999999d", "9" * 5000 + "d"]


@pytest.mark.django_db(transaction=True)
class TestOutOfRangeDuration:
    """#82: a retention too large to convert or to subtract is an argument error."""

    @pytest.mark.parametrize("dry_run", [False, True])
    @pytest.mark.parametrize("value", OUT_OF_RANGE)
    def test_is_a_command_error_and_deletes_nothing(self, value, dry_run):
        make_old_rows(3, OxTask.Status.SUCCESSFUL)
        args = [f"--older-than={value}", *(["--dry-run"] if dry_run else [])]

        with (
            CaptureQueriesContext(connection) as queries,
            pytest.raises(CommandError) as info,
        ):
            prune(*args)

        assert queries.captured_queries == []
        assert str(info.value) == f"Invalid duration {value!r}; it is out of range."
        assert OxTask.objects.count() == 3

    def test_exits_1_without_a_traceback_from_the_command_line(self, capsys):
        make_old_rows(3, OxTask.Status.SUCCESSFUL)
        argv = ["manage.py", "ox_prune", "--older-than=3000000d", "--skip-checks"]

        with pytest.raises(SystemExit) as info:
            ManagementUtility(argv).execute()

        assert info.value.code == 1
        err = capsys.readouterr().err
        assert "Invalid duration '3000000d'; it is out of range." in err
        assert "Traceback" not in err
        assert OxTask.objects.count() == 3

    def test_traceback_flag_still_raises(self):
        argv = [
            "manage.py",
            "ox_prune",
            "--older-than=3000000d",
            "--skip-checks",
            "--traceback",
        ]
        with pytest.raises(CommandError) as info:
            ManagementUtility(argv).execute()
        assert str(info.value) == "Invalid duration '3000000d'; it is out of range."


def _cutoff_now():
    """Fixed whole-second now; awareness follows settings.USE_TZ."""
    now = datetime(2026, 9, 26, 12, 0, 0)
    if settings.USE_TZ:
        return now.replace(tzinfo=UTC)
    return now


def _match_awareness(value):
    if settings.USE_TZ:
        return value if timezone.is_aware(value) else timezone.make_aware(value, UTC)
    return value.replace(tzinfo=None) if timezone.is_aware(value) else value


def _seconds_before(now, cutoff):
    return str(int((now - cutoff).total_seconds()))


@contextmanager
def connection_time_zone(alias, name):
    """Point one connection at another zone, as DATABASES TIME_ZONE would."""
    wrapper = connections[alias]
    original = wrapper.settings_dict["TIME_ZONE"]

    def reset():
        for attr in ("timezone", "timezone_name"):
            getattr(wrapper, attr)
            delattr(wrapper, attr)
        wrapper.ensure_timezone()

    wrapper.settings_dict["TIME_ZONE"] = name
    reset()
    try:
        yield
    finally:
        wrapper.settings_dict["TIME_ZONE"] = original
        reset()


@pytest.mark.django_db(transaction=True)
class TestFirstDayCutoff:
    """#104: cutoffs on the first day of year one are rejected before bind."""

    @pytest.mark.skipif(not settings.USE_TZ, reason="Requires timezone-aware datetimes")
    def test_year_one_morning_is_rejected_in_new_york(self, monkeypatch):
        now = _cutoff_now()
        monkeypatch.setattr(timezone, "now", lambda: now)
        cutoff = _match_awareness(datetime(1, 1, 1, 1, 0, 0, tzinfo=UTC))
        value = _seconds_before(now, cutoff)
        make_old_rows(3, OxTask.Status.SUCCESSFUL)

        with connection_time_zone("default", "America/New_York"):
            for extra in ([], ["--dry-run"]):
                with pytest.raises(CommandError) as info:
                    prune(f"--older-than={value}", *extra)
                assert str(info.value) == (
                    f"Invalid duration {value!r}; it is out of range."
                )
        assert OxTask.objects.count() == 3

    def test_one_second_below_the_floor_is_rejected(self, monkeypatch):
        now = _cutoff_now()
        monkeypatch.setattr(timezone, "now", lambda: now)
        floor = _match_awareness(datetime.min + timedelta(days=1))
        cutoff = floor - timedelta(seconds=1)
        value = _seconds_before(now, cutoff)
        make_old_rows(3, OxTask.Status.SUCCESSFUL)

        with pytest.raises(CommandError) as info:
            prune(f"--older-than={value}")
        assert str(info.value) == f"Invalid duration {value!r}; it is out of range."
        assert OxTask.objects.count() == 3

    def test_cutoff_exactly_at_the_floor_succeeds(self, monkeypatch):
        now = _cutoff_now()
        monkeypatch.setattr(timezone, "now", lambda: now)
        floor = _match_awareness(datetime.min + timedelta(days=1))
        value = _seconds_before(now, floor)
        make_old_rows(3, OxTask.Status.SUCCESSFUL)

        prune(f"--older-than={value}")

        assert OxTask.objects.count() == 3

    def test_ordinary_seven_day_retention_is_unchanged(self):
        old_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=8)
        young_ok = make_task(OxTask.Status.SUCCESSFUL, finished_days_ago=1)

        prune("--older-than=7d")

        assert not OxTask.objects.filter(pk=old_ok.pk).exists()
        assert OxTask.objects.filter(pk=young_ok.pk).exists()

    @pytest.mark.skipif(not settings.USE_TZ, reason="Requires timezone-aware datetimes")
    def test_year_one_morning_exits_1_without_a_traceback(self, monkeypatch, capsys):
        now = _cutoff_now()
        monkeypatch.setattr(timezone, "now", lambda: now)
        cutoff = datetime(1, 1, 1, 1, 0, 0, tzinfo=UTC)
        value = _seconds_before(now, cutoff)
        make_old_rows(3, OxTask.Status.SUCCESSFUL)
        argv = [
            "manage.py",
            "ox_prune",
            f"--older-than={value}",
            "--skip-checks",
        ]

        with connection_time_zone("default", "America/New_York"):
            with pytest.raises(SystemExit) as info:
                ManagementUtility(argv).execute()

        assert info.value.code == 1
        err = capsys.readouterr().err
        assert f"Invalid duration {value!r}; it is out of range." in err
        assert "Traceback" not in err
        assert OxTask.objects.count() == 3
