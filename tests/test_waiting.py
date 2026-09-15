"""
The WAITING status and django_ox._waiting, the only code that writes it.

django-ox never calls the helpers. A package built on django-ox does, under an
exact version pin, so this file is what keeps them from drifting: a changed
signature or a loosened guard fails here before it reaches anything that
depends on it.
"""

import ast
import inspect
import logging
import re
import threading
import time
import uuid
from datetime import timedelta
from io import StringIO
from pathlib import Path

import pytest
from django.core.management import call_command
from django.db import (
    DatabaseError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.db.migrations.exceptions import IrreversibleError
from django.db.migrations.recorder import MigrationRecorder
from django.db.models import QuerySet
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

import django_ox
from django_ox import _contention, _waiting, actions
from django_ox.backend import OxBackend
from django_ox.compat import (
    IMMEDIATE_BACKEND_PATH,
    InvalidTask,
    TaskResultStatus,
    task_enqueued,
    task_finished,
    task_started,
)
from django_ox.models import OxTask
from django_ox.results import public_status
from django_ox.worker import WRITABLE_STATUSES

from .conftest import wait_for
from .contention import descending, failing, in_key_order, simulated
from .tasks import add
from .test_worker import reap_away
from .test_write_routing import ALT

SRC = Path(django_ox.__file__).resolve().parent
REPO = SRC.parent.parent

# Every helper names its database. The suite's default alias is the one the
# worker fixture claims from.
DB = "default"

WAITING = OxTask.Status.WAITING
READY = OxTask.Status.READY
DISCARDED = OxTask.Status.DISCARDED


def held(task=add, args=(1, 2), kwargs=None, *, using=DB):
    """A row inserted WAITING, the way the helpers insert one."""
    result = _waiting.enqueue(task, list(args), kwargs or {}, using=using)
    return OxTask.objects.using(using).get(pk=result.id)


def a_row(status, *, using=DB, **fields):
    """A row in any status, written directly, for the guard tests."""
    return OxTask.objects.using(using).create(
        task_path="tests.tasks.add",
        backend_name="default",
        status=status,
        enqueued_at=timezone.now(),
        **fields,
    )


def current(row):
    return OxTask.objects.get(pk=row.pk)


# -- the status set -----------------------------------------------------------

PENDING = {READY, OxTask.Status.RUNNING, WAITING}
SETTLED = {
    OxTask.Status.SUCCESSFUL,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    DISCARDED,
}


def test_every_status_is_either_pending_or_settled():
    """
    A closed set: a status added later has to be put on one side on purpose.
    Pending reads as unfinished through django.tasks, settled as finished.
    """
    assert PENDING.isdisjoint(SETTLED)
    assert set(OxTask.Status) == PENDING | SETTLED
    for status in OxTask.Status:
        finished = public_status(status) in (
            TaskResultStatus.SUCCESSFUL,
            TaskResultStatus.FAILED,
        )
        assert finished is (status in SETTLED), status


def test_waiting_is_a_choice_with_its_label():
    assert WAITING == "WAITING"
    assert WAITING.label == "Waiting"
    assert list(OxTask.Status)[-1] == WAITING
    assert len(WAITING) <= OxTask._meta.get_field("status").max_length


# -- enqueue -------------------------------------------------------------------

ENQUEUE_PATHS = {
    "OxBackend.enqueue": lambda task: task.enqueue(1, 2),
    "_waiting.enqueue": lambda task: _waiting.enqueue(task, [1, 2], {}, using=DB),
}


@pytest.mark.django_db
class TestEnqueue:
    def test_enqueue_inserts_waiting_and_never_ready(self):
        with CaptureQueriesContext(connection) as ctx:
            result = _waiting.enqueue(add, [1, 2], {}, using=DB)
        statements = [query["sql"] for query in ctx.captured_queries]
        assert len(statements) == 1, statements
        assert statements[0].lstrip().upper().startswith("INSERT"), statements
        assert "WAITING" in statements[0]
        assert "READY" not in statements[0]
        assert OxTask.objects.get(pk=result.id).status == WAITING
        assert result.status == TaskResultStatus.READY
        assert not result.is_finished

    def test_enqueue_writes_what_oxbackend_enqueue_writes(self):
        task = add.using(
            priority=7,
            queue_name="emails",
            run_after=timezone.now() + timedelta(hours=1),
        )
        ready = task.enqueue(3, b=4)
        waiting = _waiting.enqueue(task, [3], {"b": 4}, using=DB)

        generated = {"id", "status", "enqueued_at"}
        columns = [
            field.attname
            for field in OxTask._meta.concrete_fields
            if field.attname not in generated
        ]
        assert (
            OxTask.objects.filter(pk=waiting.id).values(*columns).get()
            == OxTask.objects.filter(pk=ready.id).values(*columns).get()
        )
        assert OxTask.objects.get(pk=ready.id).status == READY
        assert OxTask.objects.get(pk=waiting.id).status == WAITING
        assert (waiting.task, waiting.args, waiting.kwargs, waiting.backend) == (
            ready.task,
            ready.args,
            ready.kwargs,
            ready.backend,
        )

    @pytest.mark.parametrize("path", list(ENQUEUE_PATHS))
    def test_enqueue_refuses_what_the_backend_refuses(self, path, monkeypatch):
        # Built first: django.tasks also validates a task when it is made.
        task = add.using(priority=3)
        refused = []

        def refuse(backend, refused_task):
            refused.append(refused_task)
            raise InvalidTask("refused by the backend")

        monkeypatch.setattr(OxBackend, "validate_task", refuse)
        with pytest.raises(InvalidTask, match="refused by the backend"):
            ENQUEUE_PATHS[path](task)
        assert refused == [task]
        assert not OxTask.objects.exists()

    def test_enqueue_refuses_a_task_on_another_backend(self, settings):
        settings.TASKS = {
            **settings.TASKS,
            "immediate": {"BACKEND": IMMEDIATE_BACKEND_PATH},
        }
        with pytest.raises(TypeError, match="not an OxBackend"):
            _waiting.enqueue(add.using(backend="immediate"), [1, 2], {}, using=DB)
        assert not OxTask.objects.exists()


# -- release -------------------------------------------------------------------


@pytest.mark.django_db
class TestRelease:
    def test_release_moves_a_waiting_row_to_ready_and_keeps_its_epoch(self):
        row = held()
        assert _waiting.release(row.pk, lease_epoch=0, using=DB) is True
        after = current(row)
        assert after.status == READY
        assert after.lease_epoch == row.lease_epoch
        assert after.run_after is not None
        assert _waiting.release(row.pk, lease_epoch=0, using=DB) is False

    @pytest.mark.parametrize(
        "status", [status for status in OxTask.Status if status != WAITING]
    )
    def test_release_moves_only_waiting_rows(self, status):
        row = a_row(status, lease_epoch=3)
        assert _waiting.release(row.pk, lease_epoch=3, using=DB) is False
        assert _waiting.release_many([(row.pk, 3)], using=DB) == (0, 1)
        after = current(row)
        assert (after.status, after.run_after, after.lease_epoch) == (status, None, 3)

    def test_a_missing_or_malformed_id_moves_nothing(self):
        row = held()
        assert _waiting.release(uuid.uuid4(), lease_epoch=0, using=DB) is False
        assert _waiting.release("not-a-uuid", lease_epoch=0, using=DB) is False
        # Counted as django_ox.actions counts: duplicates once, malformed skipped.
        assert _waiting.release_many(
            [(row.pk, 0), (str(row.pk), 0), ("not-a-uuid", 0), (uuid.uuid4(), 0)],
            using=DB,
        ) == (1, 2)
        assert _waiting.release_many([], using=DB) == (0, 0)

    def test_there_is_no_unpinned_release(self):
        """
        A release that matched on status alone could move a row that was
        cancelled and revived after its caller read it. Every release names
        the epoch its caller read.
        """
        row = held()
        with pytest.raises(TypeError, match="lease_epoch"):
            _waiting.release(row.pk, using=DB)
        with pytest.raises(TypeError):
            _waiting.release_many([row.pk], using=DB)
        assert (current(row).status, current(row).run_after) == (WAITING, None)

    @pytest.mark.parametrize("bulk", [False, True], ids=["release", "release_many"])
    def test_a_release_decided_before_a_cancel_and_revive_changes_nothing(self, bulk):
        """
        The caller reads the row at epoch 7 and decides to release it. Before
        that release lands, the row is cancelled at 7 and revived, which puts
        it back to WAITING at 8 for a decision nobody has made yet. The release
        decided at 7 matches nothing.
        """
        row = held()
        OxTask.objects.filter(pk=row.pk).update(lease_epoch=7)
        decided_at = current(row).lease_epoch
        assert decided_at == 7

        assert _waiting.cancel_many([(row.pk, 7)], using=DB) == 1
        assert _waiting.revive_many([(row.pk, 7)], using=DB) == {
            row.pk: _waiting.Revival.REVIVED
        }
        assert (current(row).status, current(row).lease_epoch) == (WAITING, 8)

        if bulk:
            assert _waiting.release_many([(row.pk, decided_at)], using=DB) == (0, 1)
        else:
            assert _waiting.release(row.pk, lease_epoch=decided_at, using=DB) is False
        after = current(row)
        assert (after.status, after.lease_epoch, after.run_after) == (WAITING, 8, None)

        # A release decided from the revived row still moves it.
        if bulk:
            assert _waiting.release_many([(row.pk, 8)], using=DB) == (1, 0)
        else:
            assert _waiting.release(row.pk, lease_epoch=8, using=DB) is True
        assert current(row).status == READY

    def test_the_epoch_pin_refuses_a_moved_row(self):
        row = held()
        OxTask.objects.filter(pk=row.pk).update(lease_epoch=5)
        assert _waiting.release(row.pk, lease_epoch=4, using=DB) is False
        assert current(row).status == WAITING
        assert _waiting.release(row.pk, lease_epoch=5, using=DB) is True
        assert current(row).status == READY

        other = held()
        OxTask.objects.filter(pk=other.pk).update(lease_epoch=5)
        assert _waiting.cancel_many([(other.pk, 4)], using=DB) == 0
        assert current(other).status == WAITING
        assert _waiting.cancel_many([(other.pk, 5)], using=DB) == 1
        assert _waiting.revive_many([(other.pk, 4)], using=DB) == {
            other.pk: _waiting.Revival.WRONG_STATUS_OR_EPOCH
        }
        assert current(other).status == DISCARDED
        assert _waiting.revive_many([(other.pk, 5)], using=DB) == {
            other.pk: _waiting.Revival.REVIVED
        }
        assert (current(other).status, current(other).lease_epoch) == (WAITING, 6)

    @pytest.mark.parametrize(
        "helper", ["release", "release_many", "cancel_many", "revive_many"]
    )
    def test_an_epoch_ahead_of_the_row_moves_nothing(self, helper):
        """
        The pin matches the epoch exactly. An epoch ahead of the row's matches
        nothing, just as one behind it does. A bulk form is called with that
        row alone, which filters on one epoch, and then beside a row named at
        its own epoch, which pins each row through a CASE.
        """
        status = DISCARDED if helper == "revive_many" else WAITING
        ahead, beside = a_row(status, lease_epoch=3), a_row(status, lease_epoch=3)
        wrong, revived = (
            _waiting.Revival.WRONG_STATUS_OR_EPOCH,
            _waiting.Revival.REVIVED,
        )

        if helper == "release":
            assert _waiting.release(ahead.pk, lease_epoch=4, using=DB) is False
        else:
            call = getattr(_waiting, helper)
            alone = call([(ahead.pk, 4)], using=DB)
            both = call([(ahead.pk, 4), (beside.pk, 3)], using=DB)
            assert (alone, both) == {
                "release_many": ((0, 1), (1, 1)),
                "cancel_many": (0, 1),
                "revive_many": (
                    {ahead.pk: wrong},
                    {ahead.pk: wrong, beside.pk: revived},
                ),
            }[helper]
            assert current(beside).status != status
        after = current(ahead)
        assert (after.status, after.lease_epoch, after.run_after) == (status, 3, None)

    @pytest.mark.parametrize("bulk", [False, True], ids=["release", "release_many"])
    def test_release_keeps_a_later_run_after_and_otherwise_stamps_now(self, bulk):
        later = timezone.now() + timedelta(hours=2)
        earlier = timezone.now() - timedelta(hours=2)
        unset = held()
        future = held(add.using(run_after=later))
        past = held(add.using(run_after=earlier))
        assert current(past).run_after == earlier

        before = timezone.now()
        rows = (unset, future, past)
        if bulk:
            pinned = [(row.pk, 0) for row in rows]
            assert _waiting.release_many(pinned, using=DB) == (3, 0)
        else:
            assert all(
                _waiting.release(row.pk, lease_epoch=0, using=DB) for row in rows
            )
        after = timezone.now()

        assert current(future).run_after == later
        for row in (unset, past):
            assert before <= current(row).run_after <= after


# -- cancel and revive ---------------------------------------------------------


@pytest.mark.django_db
class TestCancelAndRevive:
    @pytest.mark.parametrize("status", list(OxTask.Status))
    def test_cancel_closes_only_ready_and_waiting_rows(self, status):
        row = a_row(status, lease_epoch=3, attempts=1, worker_ids=["w"])
        moved = _waiting.cancel_many([(row.pk, 3)], using=DB)
        after = current(row)
        if status in (READY, WAITING):
            assert moved == 1
            assert after.status == DISCARDED
            assert after.finished_at is not None
            assert (after.lease_epoch, after.attempts, after.worker_ids) == (
                3,
                1,
                ["w"],
            )
            assert (after.locked_by, after.locked_at, after.lease_expires_at) == (
                None,
                None,
                None,
            )
        else:
            assert moved == 0
            assert (after.status, after.lease_epoch) == (status, 3)

    @pytest.mark.parametrize("status", list(OxTask.Status))
    def test_revive_moves_only_discarded_rows(self, status):
        later = timezone.now() + timedelta(hours=1)
        errors = [{"exception_class_path": "builtins.ValueError", "traceback": "t"}]
        row = a_row(
            status,
            lease_epoch=3,
            attempts=2,
            errors=errors,
            run_after=later,
            finished_at=timezone.now(),
        )
        results = _waiting.revive_many([(row.pk, 3)], using=DB)
        after = current(row)
        if status == DISCARDED:
            assert results == {row.pk: _waiting.Revival.REVIVED}
            assert after.status == WAITING
            assert after.lease_epoch == 4
            assert after.finished_at is None
            assert (after.attempts, after.errors, after.run_after) == (2, errors, later)
        else:
            assert results == {row.pk: _waiting.Revival.WRONG_STATUS_OR_EPOCH}
            assert (after.status, after.lease_epoch) == (status, 3)

    def test_revive_reports_a_row_pruned_after_its_cancel_as_not_found(self):
        """
        ox_prune deletes DISCARDED rows, so a cancelled task can be gone before
        anything revives it. Reviving it has nothing to move, and the result
        says the row is not there instead of leaving it out.
        """
        pruned, kept, never_cancelled = held(), held(), held()
        assert _waiting.cancel_many([(pruned.pk, 0)], using=DB) == 1
        out = StringIO()
        call_command("ox_prune", "--older-than=0s", stdout=out)
        assert "Deleted 1 SUCCESSFUL/DISCARDED task row(s)" in out.getvalue()
        assert not OxTask.objects.filter(pk=pruned.pk).exists()

        assert _waiting.cancel_many([(kept.pk, 0)], using=DB) == 1
        results = _waiting.revive_many(
            [(pruned.pk, 0), (kept.pk, 0), (never_cancelled.pk, 0)], using=DB
        )
        assert results == {
            pruned.pk: _waiting.Revival.NOT_FOUND,
            kept.pk: _waiting.Revival.REVIVED,
            never_cancelled.pk: _waiting.Revival.WRONG_STATUS_OR_EPOCH,
        }
        assert list(results) == in_key_order(results)
        assert (current(kept).status, current(kept).lease_epoch) == (WAITING, 1)
        untouched = current(never_cancelled)
        assert (untouched.status, untouched.lease_epoch) == (WAITING, 0)

    def test_each_row_is_pinned_to_its_own_epoch(self):
        rows = [held() for _ in range(4)]
        for row, epoch in zip(rows, (0, 2, 1, 2), strict=True):
            OxTask.objects.filter(pk=row.pk).update(lease_epoch=epoch)
        # The second row is named with an epoch it no longer has.
        claimed = [(rows[0].pk, 0), (rows[1].pk, 1), (rows[2].pk, 1), (rows[3].pk, 2)]
        assert _waiting.cancel_many(claimed, using=DB) == 3
        assert [current(row).status for row in rows] == [
            DISCARDED,
            WAITING,
            DISCARDED,
            DISCARDED,
        ]
        results = _waiting.revive_many([(rows[0].pk, 0), (rows[2].pk, 1)], using=DB)
        assert results == {
            rows[0].pk: _waiting.Revival.REVIVED,
            rows[2].pk: _waiting.Revival.REVIVED,
        }
        assert [current(row).lease_epoch for row in rows] == [1, 2, 2, 2]

    def test_an_id_given_twice_is_decided_by_its_first_epoch(self):
        """
        Each pinned form takes an id once. A second pair for the same id,
        with another epoch, changes nothing about how that row is decided.
        """
        row = held()
        OxTask.objects.filter(pk=row.pk).update(lease_epoch=2)

        # The first epoch is stale and the second is current: nothing moves.
        assert _waiting.release_many([(row.pk, 1), (row.pk, 2)], using=DB) == (0, 1)
        assert _waiting.cancel_many([(row.pk, 1), (str(row.pk), 2)], using=DB) == 0
        assert (current(row).status, current(row).run_after) == (WAITING, None)

        # The first epoch is current and the second is stale: it moves once.
        assert _waiting.cancel_many([(row.pk, 2), (row.pk, 1)], using=DB) == 1
        assert _waiting.revive_many([(row.pk, 1), (row.pk, 2)], using=DB) == {
            row.pk: _waiting.Revival.WRONG_STATUS_OR_EPOCH
        }
        assert _waiting.revive_many([(row.pk, 2), (str(row.pk), 1)], using=DB) == {
            row.pk: _waiting.Revival.REVIVED
        }
        assert (current(row).status, current(row).lease_epoch) == (WAITING, 3)
        assert _waiting.release_many([(row.pk, 3), (row.pk, 2)], using=DB) == (1, 0)
        assert current(row).status == READY

    def test_a_malformed_id_in_a_pinned_form_moves_nothing(self):
        row = held()
        malformed = [("not-a-uuid", 0), (uuid.uuid4(), 0)]
        assert _waiting.cancel_many(malformed, using=DB) == 0
        assert _waiting.cancel_many([], using=DB) == 0
        assert _waiting.revive_many([], using=DB) == {}
        assert current(row).status == WAITING

        # revive_many reports on every row it is given, and a malformed id
        # names no row to report on, so it refuses the call before moving any.
        assert _waiting.cancel_many([(row.pk, 0)], using=DB) == 1
        with pytest.raises(ValueError, match="not-a-uuid"):
            _waiting.revive_many([(row.pk, 0), ("not-a-uuid", 0)], using=DB)
        assert current(row).status == DISCARDED

    def test_revive_bumps_the_epoch_and_fences_a_straggler(self, worker):
        add.enqueue(1, 2)
        OxTask.objects.update(attempts=2)
        stale = worker.claim_one()
        reap_away(worker, stale)
        assert current(stale).status == OxTask.Status.LOST
        assert actions.discard(stale.pk) is True

        results = _waiting.revive_many([(stale.pk, stale.lease_epoch)], using=DB)
        assert results == {stale.pk: _waiting.Revival.REVIVED}
        revived = current(stale)
        assert (revived.status, revived.lease_epoch) == (
            WAITING,
            stale.lease_epoch + 1,
        )

        finish = {"duration_ms": 0, "return_value": 3, "finished_at": timezone.now()}
        status = OxTask.Status.SUCCESSFUL
        assert worker._write_outcome(stale, status=status, **finish) is False
        # A decision made from a read taken before the revival misses too.
        assert _waiting.cancel_many([(stale.pk, stale.lease_epoch)], using=DB) == 0
        assert (
            _waiting.release(stale.pk, lease_epoch=stale.lease_epoch, using=DB) is False
        )

        assert (
            _waiting.release(stale.pk, lease_epoch=revived.lease_epoch, using=DB)
            is True
        )
        claimed = worker.claim_one()
        assert claimed.pk == stale.pk
        assert claimed.lease_epoch == revived.lease_epoch + 1
        assert worker._write_outcome(stale, status=status, **finish) is False
        assert current(stale).status == OxTask.Status.RUNNING


# -- the bulk forms ------------------------------------------------------------

BULK = ["release_many", "cancel_many", "revive_many"]


def run_bulk(helper, rows):
    pinned = [(row.pk, current(row).lease_epoch) for row in rows]
    return getattr(_waiting, helper)(pinned, using=DB)


def rows_for(helper, count):
    rows = [held() for _ in range(count)]
    if helper == "revive_many":
        _waiting.cancel_many([(row.pk, row.lease_epoch) for row in rows], using=DB)
    return rows


def all_moved(helper, rows):
    """What a bulk helper returns when every row it was given moved."""
    if helper == "release_many":
        return (len(rows), 0)
    if helper == "revive_many":
        return {row.pk: _waiting.Revival.REVIVED for row in rows}
    return len(rows)


def update_that_fails_second(monkeypatch):
    """Patch QuerySet.update so its second call raises. Returns the calls."""
    real_update = QuerySet.update
    calls = []

    def update(self, **kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise DatabaseError("the second chunk fails")
        return real_update(self, **kwargs)

    monkeypatch.setattr(QuerySet, "update", update)
    return calls


UUID_TEXT = re.compile(
    r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}"
)


@pytest.mark.django_db
class TestBulk:
    @pytest.mark.parametrize("helper", ["cancel_many", "revive_many"])
    def test_a_cancel_or_revive_that_fails_part_way_moves_nothing(
        self, helper, monkeypatch
    ):
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        rows = rows_for(helper, 5)
        before = [current(row).status for row in rows]

        calls = update_that_fails_second(monkeypatch)
        with pytest.raises(DatabaseError, match="second chunk"):
            run_bulk(helper, rows)
        monkeypatch.undo()

        assert len(calls) == 2
        assert [current(row).status for row in rows] == before

    @pytest.mark.parametrize("helper", BULK)
    def test_bulk_moves_lock_in_primary_key_order(self, helper, monkeypatch):
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        rows = rows_for(helper, 5)
        # Interleaved epochs, so ordering by epoch would break key order.
        for index, row in enumerate(in_key_order(rows, by=lambda r: r.pk)):
            OxTask.objects.filter(pk=row.pk).update(lease_epoch=index % 2)
        given = sorted(rows, key=lambda r: r.pk, reverse=True)
        given = given[1::2] + given[::2]

        with CaptureQueriesContext(connection) as ctx:
            moved = run_bulk(helper, given)
        assert moved == all_moved(helper, rows)

        statements = []
        for query in ctx.captured_queries:
            if not query["sql"].lstrip().upper().startswith("UPDATE"):
                continue
            ids = []
            for text in UUID_TEXT.findall(query["sql"]):
                pk = uuid.UUID(text)
                if pk not in ids:
                    ids.append(pk)
            statements.append(ids)
        assert [len(ids) for ids in statements] == [2, 2, 1], statements
        issued = [pk for ids in statements for pk in ids]
        assert issued == in_key_order(row.pk for row in rows)

    @pytest.mark.parametrize("helper", BULK)
    def test_the_chunks_follow_the_key_order_of_the_database(self, helper, monkeypatch):
        """
        As for the _many actions in tests/test_actions.py, with the database's
        key order swapped for one no database uses.
        """
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        rows = rows_for(helper, 5)
        keys = sorted((row.pk for row in rows), reverse=True)
        monkeypatch.setattr(actions, "_key_order", lambda alias: descending)

        with failing("UPDATE", lambda: None, lambda n: False) as seen:
            moved = run_bulk(helper, sorted(rows, key=lambda row: row.pk))

        assert moved == all_moved(helper, rows)
        assert chunks_of(seen) == [keys[0:2], keys[2:4], keys[4:5]]
        if helper == "revive_many":
            assert list(moved) == keys


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "ambient", [False, True], ids=["autocommit", "in-the-callers-transaction"]
)
def test_release_many_commits_each_chunk_on_its_own(ambient, monkeypatch):
    """
    A large release is not one long transaction. Outside a transaction each
    chunk commits by itself, so a failure in the second leaves the first
    released and the rest WAITING, for a later release to finish. Inside the
    caller's transaction the chunks commit or roll back with it.
    """
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    rows = in_key_order((held() for _ in range(5)), by=lambda row: row.pk)
    pinned = [(row.pk, 0) for row in reversed(rows)]

    calls = update_that_fails_second(monkeypatch)
    with pytest.raises(DatabaseError, match="second chunk"):
        if ambient:
            with transaction.atomic(using=DB):
                _waiting.release_many(pinned, using=DB)
        else:
            _waiting.release_many(pinned, using=DB)
    monkeypatch.undo()

    assert len(calls) == 2
    statuses = [current(row).status for row in rows]
    if ambient:
        assert statuses == [WAITING] * 5
    else:
        assert statuses == [READY, READY, WAITING, WAITING, WAITING]


@pytest.mark.django_db
@pytest.mark.parametrize("helper", ["release_many", "cancel_many"])
def test_each_chunk_is_locked_in_key_order_before_its_update(helper, monkeypatch):
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    keys = in_key_order(row.pk for row in rows_for(helper, 5))
    pinned = [(pk, 0) for pk in reversed(keys)]

    with CaptureQueriesContext(connection) as ctx:
        getattr(_waiting, helper)(pinned, using=DB)

    statements, others = [], []
    for query in ctx.captured_queries:
        sql = query["sql"].strip()
        verb = sql.split(None, 1)[0].upper()
        if verb not in {"SELECT", "UPDATE"}:
            others.append(sql)
            continue
        ids = []
        for text in UUID_TEXT.findall(sql):
            if uuid.UUID(text) not in ids:
                ids.append(uuid.UUID(text))
        statements.append((verb, ids, sql))

    chunks = [keys[0:2], keys[2:4], keys[4:5]]
    if connection.features.has_select_for_update:
        expected = [(verb, chunk) for chunk in chunks for verb in ("SELECT", "UPDATE")]
    else:
        expected = [("UPDATE", chunk) for chunk in chunks]
    assert [(verb, ids) for verb, ids, _ in statements] == expected, statements

    table = connection.ops.quote_name(OxTask._meta.db_table)
    pk = re.escape(f"{table}.{connection.ops.quote_name(OxTask._meta.pk.column)}")
    # The read selects the key alone, so ORDER BY 1 is ORDER BY the key.
    locking_read = re.compile(
        rf"^SELECT {pk}(?: AS \S+)? FROM {re.escape(table)} WHERE .*"  # noqa: S608
        rf" ORDER BY (?:1|{pk}) ASC FOR UPDATE$"
    )
    for verb, _, sql in statements:
        if verb == "SELECT":
            assert locking_read.search(sql), sql
    # cancel_many's own transaction can add a savepoint and its release when
    # it runs inside another one. Nothing more, and nothing per chunk.
    assert len(others) <= 2, others


def waiting_for_a_lock(count=1, timeout=10):
    """
    Wait until `count` other sessions are queued for a lock. True if they
    were. MariaDB serves INNODB_TRX from a cache it refreshes only after
    100 ms without a read, so a poll comes after a pause longer than that,
    and the first read of a round is never the last round's snapshot.
    """
    if getattr(connection, "mysql_is_mariadb", False):
        return wait_for(
            lambda: time.sleep(0.15) or lock_waits() >= count,
            timeout=timeout,
            interval=0,
        )
    return wait_for(lambda: lock_waits() >= count, timeout=timeout)


def lock_waits():
    """Other sessions on this test database that are waiting for a lock."""
    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            # Called inside a transaction, which would otherwise read the same
            # snapshot of pg_stat_activity for as long as it stays open.
            cursor.execute("SELECT pg_stat_clear_snapshot()")
            cursor.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = "
                "current_database() AND wait_event_type = 'Lock' "
                "AND pid <> pg_backend_pid()"
            )
        elif getattr(connection, "mysql_is_mariadb", False):
            # MariaDB has no performance_schema.data_locks.
            cursor.execute(
                "SELECT count(*) FROM information_schema.INNODB_TRX "
                "WHERE trx_state = 'LOCK WAIT'"
            )
        else:
            cursor.execute(
                "SELECT count(*) FROM performance_schema.data_locks "
                "WHERE object_schema = DATABASE() AND lock_status = 'WAITING'"
            )
        return cursor.fetchone()[0]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "second",
    ["release_many", "cancel_many"],
    ids=["release_many-against-cancel_many", "cancel_many-against-cancel_many"],
)
def test_two_bulk_calls_over_shared_rows_do_not_deadlock(second, monkeypatch):
    """
    Each call sorts its ids, so the order they are given in changes nothing.
    What crosses is chunking. cancel_many is given four rows, in two chunks of
    two, and the second call only the middle two, so its one chunk overlaps
    both of cancel_many's. The rows are inserted largest key first, so a scan
    in table order meets them in the reverse of key order.

    cancel_many stops after its first chunk until the second call waits on a
    lock. Locking in table order, the second call holds the third row while it
    waits for the second, then cancel_many's next chunk waits for the third,
    and the database breaks the deadlock by raising in one of them. Locking in
    key order, the second call waits for the second row holding nothing, and
    cancel_many finishes first.
    """
    if not connection.features.has_select_for_update:
        pytest.skip("SQLite has no row locks to take in any order")
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    keys = in_key_order(uuid.uuid4() for _ in range(4))
    for pk in reversed(keys):
        a_row(WAITING, pk=pk, lease_epoch=0)

    first_chunk_held = threading.Event()
    waited = []
    real_chunks = _waiting._chunks

    def chunks(items):
        for index, chunk in enumerate(real_chunks(items)):
            if index == 1 and threading.current_thread().name == "first":
                first_chunk_held.set()
                waited.append(waiting_for_a_lock())
            yield chunk

    monkeypatch.setattr(_waiting, "_chunks", chunks)
    outcome = {}

    def cancel_all():
        return _waiting.cancel_many([(pk, 0) for pk in reversed(keys)], using=DB)

    def the_middle_two():
        first_chunk_held.wait(10)
        return getattr(_waiting, second)([(pk, 0) for pk in keys[1:3]], using=DB)

    def run(body):
        name = threading.current_thread().name
        try:
            outcome[name] = body()
        except Exception as exc:
            outcome[name] = exc
        finally:
            connections.close_all()

    threads = [
        threading.Thread(target=run, args=(cancel_all,), name="first"),
        threading.Thread(target=run, args=(the_middle_two,), name="second"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
        assert not thread.is_alive(), f"the {thread.name} call did not finish"

    raised = {name: exc for name, exc in outcome.items() if isinstance(exc, Exception)}
    assert not raised, raised
    # The interleaving happened: the second call waited on a row cancel_many
    # held. Without that, a pass would say nothing about lock order.
    assert waited == [True]
    assert outcome["first"] == 4
    assert outcome["second"] == ((0, 2) if second == "release_many" else 0)
    assert (
        list(
            OxTask.objects.filter(pk__in=keys)
            .order_by("pk")
            .values_list("status", "lease_epoch")
        )
        == [(DISCARDED, 0)] * 4
    )


# -- deadlocks and serialization failures -------------------------------------


def needs_a_locking_read(statement):
    """
    A contention error is simulated in place of a chunk's UPDATE, or of its
    locking read, where a deadlock with a writer that holds a row outside the
    chunk lands. SQLite has no locking read.
    """
    if statement == "SELECT" and not connections[DB].features.has_select_for_update:
        pytest.skip("SQLite has no locking read to lose on")


def five_rows(helper, monkeypatch):
    """Five rows in chunks of 2, 2 and 1, given in reverse key order."""
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    keys = in_key_order(row.pk for row in rows_for(helper, 5))
    return keys, [(pk, 0) for pk in reversed(keys)]


def chunks_of(seen):
    return [ids for _, ids in seen]


def unmoved(helper):
    """The status five_rows gives a helper's rows."""
    return DISCARDED if helper == "revive_many" else WAITING


def statuses(keys):
    rows = dict(OxTask.objects.filter(pk__in=keys).values_list("pk", "status"))
    return [rows[pk] for pk in keys]


@pytest.mark.django_db(transaction=True)
class TestContendedHelpers:
    """
    Outside a transaction, release_many runs a chunk that lost a deadlock or a
    serialization failure again. cancel_many and revive_many run the whole
    call again. Each stops after three attempts in all. Inside a caller's
    transaction none of them runs again. The errors are simulated in place of
    one statement, shaped as Django raises the drivers' (tests/test_contention.py
    pins the shape against real ones).
    """

    @pytest.mark.parametrize("kind", ["postgresql-deadlock", "mysql-deadlock"])
    @pytest.mark.parametrize("statement", ["UPDATE", "SELECT"])
    def test_release_many_runs_the_chunk_that_lost_again(
        self, statement, kind, monkeypatch
    ):
        needs_a_locking_read(statement)
        keys, pinned = five_rows("release_many", monkeypatch)

        with failing(statement, lambda: simulated(kind), lambda n: n == 2) as seen:
            assert _waiting.release_many(pinned, using=DB) == (5, 0)

        # The second chunk lost and ran again. The first had committed, so it
        # did not.
        assert chunks_of(seen) == [keys[0:2], keys[2:4], keys[2:4], keys[4:5]]
        assert statuses(keys) == [READY] * 5

    @pytest.mark.parametrize("kind", ["postgresql-serialization", "mysql-deadlock"])
    def test_release_many_stops_after_three_attempts_at_one_chunk(
        self, kind, monkeypatch
    ):
        keys, pinned = five_rows("release_many", monkeypatch)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: n >= 2) as seen,
            pytest.raises(OperationalError),
        ):
            _waiting.release_many(pinned, using=DB)

        assert chunks_of(seen) == [keys[0:2]] + [keys[2:4]] * 3
        assert statuses(keys) == [READY, READY, WAITING, WAITING, WAITING]

    @pytest.mark.parametrize("kind", ["postgresql-deadlock", "mysql-deadlock"])
    @pytest.mark.parametrize("statement", ["UPDATE", "SELECT"])
    def test_cancel_many_runs_the_whole_call_again(self, statement, kind, monkeypatch):
        needs_a_locking_read(statement)
        keys, pinned = five_rows("cancel_many", monkeypatch)

        with failing(statement, lambda: simulated(kind), lambda n: n == 2) as seen:
            assert _waiting.cancel_many(pinned, using=DB) == 5

        # The second chunk lost, which rolled back the first chunk's move too,
        # so the call ran again from the first chunk.
        assert chunks_of(seen) == [
            keys[0:2],
            keys[2:4],
            keys[0:2],
            keys[2:4],
            keys[4:5],
        ]
        assert statuses(keys) == [DISCARDED] * 5
        assert set(OxTask.objects.values_list("lease_epoch", flat=True)) == {0}

    @pytest.mark.parametrize("kind", ["postgresql-serialization", "mysql-deadlock"])
    def test_cancel_many_stops_after_three_attempts(self, kind, monkeypatch):
        keys, pinned = five_rows("cancel_many", monkeypatch)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: True) as seen,
            pytest.raises(OperationalError),
        ):
            _waiting.cancel_many(pinned, using=DB)

        assert chunks_of(seen) == [keys[0:2]] * 3
        assert statuses(keys) == [WAITING] * 5

    @pytest.mark.parametrize("kind", ["postgresql-serialization", "mysql-deadlock"])
    @pytest.mark.parametrize("statement", ["UPDATE", "SELECT"])
    def test_revive_many_runs_the_whole_call_again(self, statement, kind, monkeypatch):
        needs_a_locking_read(statement)
        keys, pinned = five_rows("revive_many", monkeypatch)

        with failing(statement, lambda: simulated(kind), lambda n: n == 2) as seen:
            results = _waiting.revive_many(pinned, using=DB)

        assert results == dict.fromkeys(keys, _waiting.Revival.REVIVED)
        assert list(results) == keys
        # The second chunk lost, which rolled back the first chunk's move too,
        # so the call ran again from the first chunk.
        assert chunks_of(seen) == [
            keys[0:2],
            keys[2:4],
            keys[0:2],
            keys[2:4],
            keys[4:5],
        ]
        assert statuses(keys) == [WAITING] * 5
        # One bump each. The first attempt's bump rolled back with it.
        assert set(OxTask.objects.values_list("lease_epoch", flat=True)) == {1}

    def test_revive_many_reports_the_rows_the_last_attempt_found(self, monkeypatch):
        keys, pinned = five_rows("revive_many", monkeypatch)
        real_pause = _contention.pause
        seen = []

        def pause(attempt):
            # Between the attempts, one row is pruned and another moves on.
            # Their statements are not the helper's, so they are not kept.
            kept = len(seen)
            OxTask.objects.filter(pk=keys[0]).delete()
            OxTask.objects.filter(pk=keys[3]).update(lease_epoch=5)
            del seen[kept:]
            real_pause(attempt)

        monkeypatch.setattr(_contention, "pause", pause)
        kind = "postgresql-deadlock"
        with failing("UPDATE", lambda: simulated(kind), lambda n: n == 2) as updates:
            seen = updates
            results = _waiting.revive_many(pinned, using=DB)

        assert chunks_of(seen) == [
            keys[0:2],
            keys[2:4],
            keys[1:2],
            keys[2:3],
            keys[4:5],
        ]
        assert results == {
            keys[0]: _waiting.Revival.NOT_FOUND,
            keys[1]: _waiting.Revival.REVIVED,
            keys[2]: _waiting.Revival.REVIVED,
            keys[3]: _waiting.Revival.WRONG_STATUS_OR_EPOCH,
            keys[4]: _waiting.Revival.REVIVED,
        }
        assert list(results) == keys
        assert statuses(keys[1:]) == [WAITING, WAITING, DISCARDED, WAITING]

    @pytest.mark.parametrize("kind", ["postgresql-deadlock", "mysql-deadlock"])
    def test_revive_many_stops_after_three_attempts(self, kind, monkeypatch):
        keys, pinned = five_rows("revive_many", monkeypatch)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: True) as seen,
            pytest.raises(OperationalError),
        ):
            _waiting.revive_many(pinned, using=DB)

        assert chunks_of(seen) == [keys[0:2]] * 3
        assert statuses(keys) == [DISCARDED] * 5
        assert set(OxTask.objects.values_list("lease_epoch", flat=True)) == {0}

    @pytest.mark.parametrize("helper", ["release_many", "cancel_many", "revive_many"])
    @pytest.mark.parametrize("kind", ["postgresql-deadlock", "mysql-deadlock"])
    def test_inside_a_callers_transaction_nothing_runs_again(
        self, helper, kind, monkeypatch
    ):
        keys, pinned = five_rows(helper, monkeypatch)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: n == 2) as seen,
            pytest.raises(OperationalError),
            transaction.atomic(using=DB),
        ):
            getattr(_waiting, helper)(pinned, using=DB)

        assert chunks_of(seen) == [keys[0:2], keys[2:4]]
        assert statuses(keys) == [unmoved(helper)] * 5

    @pytest.mark.parametrize("helper", ["release_many", "cancel_many", "revive_many"])
    def test_any_other_database_error_is_raised_at_once(self, helper, monkeypatch):
        keys, pinned = five_rows(helper, monkeypatch)

        with (
            failing(
                "UPDATE", lambda: OperationalError("disk I/O error"), lambda n: n == 1
            ) as seen,
            pytest.raises(OperationalError, match="disk I/O"),
        ):
            getattr(_waiting, helper)(pinned, using=DB)

        assert chunks_of(seen) == [keys[0:2]]
        assert statuses(keys) == [unmoved(helper)] * 5


# -- the database a move is written to ----------------------------------------

MOVES = {
    # helper: (status before, the call, what it returns when it moved, after)
    "release": (
        WAITING,
        lambda pk, alias: _waiting.release(pk, lease_epoch=0, using=alias),
        lambda pk: True,
        READY,
    ),
    "release_many": (
        WAITING,
        lambda pk, alias: _waiting.release_many([(pk, 0)], using=alias),
        lambda pk: (1, 0),
        READY,
    ),
    "cancel_many": (
        WAITING,
        lambda pk, alias: _waiting.cancel_many([(pk, 0)], using=alias),
        lambda pk: 1,
        DISCARDED,
    ),
    "revive_many": (
        DISCARDED,
        lambda pk, alias: _waiting.revive_many([(pk, 0)], using=alias),
        lambda pk: {pk: _waiting.Revival.REVIVED},
        WAITING,
    ),
}

WITHOUT_USING = {
    "enqueue": lambda pk: _waiting.enqueue(add, [1, 2], {}),
    "release": lambda pk: _waiting.release(pk, lease_epoch=0),
    "release_many": lambda pk: _waiting.release_many([(pk, 0)]),
    "cancel_many": lambda pk: _waiting.cancel_many([(pk, 0)]),
    "revive_many": lambda pk: _waiting.revive_many([(pk, 0)]),
}


@pytest.mark.django_db(databases=["default", ALT])
class TestTheDatabaseAMoveIsWrittenTo:
    """
    The same row, primary key included, sits on both databases, so a move
    written through the wrong connection changes the wrong copy instead of
    failing to find one.
    """

    @pytest.mark.parametrize("helper", list(MOVES))
    def test_a_move_is_written_to_the_database_it_names(self, helper):
        before, move, moved, after = MOVES[helper]
        pk = uuid.uuid4()
        a_row(before, pk=pk)
        a_row(before, pk=pk, using=ALT)

        assert move(pk, ALT) == moved(pk)
        assert OxTask.objects.using(ALT).get(pk=pk).status == after
        assert OxTask.objects.using(DB).get(pk=pk).status == before

    def test_enqueue_inserts_into_the_database_it_names(self):
        result = _waiting.enqueue(add, [1, 2], {}, using=ALT)
        assert OxTask.objects.using(ALT).get(pk=result.id).status == WAITING
        assert not OxTask.objects.using(DB).exists()

    def test_a_move_through_another_database_changes_nothing_and_says_so(self):
        """
        A task enqueued on one database is not there to move through another.
        Each helper says so in what it returns, and the row stays WAITING on
        the database it was enqueued on.
        """
        pk = uuid.UUID(_waiting.enqueue(add, [1, 2], {}, using=ALT).id)

        assert _waiting.release(pk, lease_epoch=0, using=DB) is False
        assert _waiting.release_many([(pk, 0)], using=DB) == (0, 1)
        assert _waiting.cancel_many([(pk, 0)], using=DB) == 0
        assert _waiting.revive_many([(pk, 0)], using=DB) == {
            pk: _waiting.Revival.NOT_FOUND
        }
        row = OxTask.objects.using(ALT).get(pk=pk)
        assert (row.status, row.lease_epoch, row.run_after) == (WAITING, 0, None)
        assert not OxTask.objects.using(DB).exists()

        assert _waiting.release(pk, lease_epoch=0, using=ALT) is True
        assert OxTask.objects.using(ALT).get(pk=pk).status == READY

    @pytest.mark.parametrize("helper", list(WITHOUT_USING))
    def test_every_helper_needs_its_database_named(self, helper):
        """
        No helper falls back to a router, which could send a release to
        another database than the one its task was enqueued on.
        """
        pk = uuid.uuid4()
        a_row(WAITING, pk=pk)
        with pytest.raises(TypeError, match="using"):
            WITHOUT_USING[helper](pk)
        assert list(OxTask.objects.using(DB).values_list("pk", "status")) == [
            (pk, WAITING)
        ]
        assert not OxTask.objects.using(ALT).exists()


def test_the_helpers_send_no_signal(caplog, db):
    received = []

    def receiver(sender, **kwargs):
        received.append(sender)

    signals = (task_enqueued, task_started, task_finished)
    for signal in signals:
        signal.connect(receiver)
    try:
        with caplog.at_level(logging.DEBUG, logger="django_ox"):
            first, second, third = (held() for _ in range(3))
            assert _waiting.release(first.pk, lease_epoch=0, using=DB)
            assert _waiting.release_many([(second.pk, 0)], using=DB) == (1, 0)
            assert _waiting.cancel_many([(third.pk, 0)], using=DB) == 1
            assert _waiting.revive_many([(third.pk, 0)], using=DB) == {
                third.pk: _waiting.Revival.REVIVED
            }
    finally:
        for signal in signals:
            signal.disconnect(receiver)
    assert received == []
    assert [r for r in caplog.records if r.name.startswith("django_ox")] == []


# -- the claim, the reaper and the outcome fence -------------------------------


def force_claim_path(path, worker, monkeypatch):
    features = connections[worker._db_alias].features
    vendor = connections[worker._db_alias].vendor
    if path == "single-statement":
        if vendor != "postgresql":
            pytest.skip("only PostgreSQL claims in a single statement")
    elif path == "skip-locked":
        if not features.has_select_for_update_skip_locked:
            pytest.skip("this database has no SKIP LOCKED")
        monkeypatch.setattr(
            worker, "_postgresql_honours_the_claim_filter", lambda: False
        )
    else:
        monkeypatch.setattr(features, "has_select_for_update_skip_locked", False)


def claim_shape(statements):
    locking = [sql for sql in statements if "SKIP LOCKED" in sql.upper()]
    if not locking:
        return "compare-and-set"
    if locking[0].lstrip().upper().startswith("UPDATE"):
        return "single-statement"
    return "skip-locked"


@pytest.mark.django_db
class TestWorkersNeverTouchAWaitingRow:
    @pytest.mark.parametrize(
        "path", ["single-statement", "skip-locked", "compare-and-set"]
    )
    def test_a_waiting_row_is_never_claimed(self, path, worker, monkeypatch):
        force_claim_path(path, worker, monkeypatch)
        # Ahead of everything else in claim order.
        waiting = held(add.using(priority=10))

        with CaptureQueriesContext(connections[worker._db_alias]) as ctx:
            assert worker.claim_one() is None
        assert claim_shape([q["sql"] for q in ctx.captured_queries]) == path

        ready = add.enqueue(3, 4)
        claimed = worker.claim_one()
        assert claimed is not None
        assert str(claimed.pk) == ready.id
        assert worker.claim_one() is None

        after = current(waiting)
        assert (after.status, after.attempts, after.lease_epoch) == (WAITING, 0, 0)
        assert after.worker_ids == []

    def test_the_reaper_and_renewal_never_touch_a_waiting_row(self, worker):
        stale = timezone.now() - timedelta(days=1)
        lease = {"locked_by": worker.worker_id, "locked_at": stale}
        spare = held()
        spent = held()
        OxTask.objects.filter(pk=spare.pk).update(lease_expires_at=stale, **lease)
        OxTask.objects.filter(pk=spent.pk).update(
            lease_expires_at=None, attempts=3, max_attempts=3, **lease
        )
        before = list(OxTask.objects.order_by("pk").values())
        with worker._in_flight_lock:
            worker._in_flight.update({(spare.pk, 0), (spent.pk, 0)})

        assert worker.renew_leases() == 0
        assert worker.reap() == 0
        assert list(OxTask.objects.order_by("pk").values()) == before

    def test_a_straggler_cannot_write_onto_a_waiting_row(self, worker):
        assert WAITING not in WRITABLE_STATUSES
        add.enqueue(1, 2)
        claimed = worker.claim_one()
        # The claim's own epoch, which no helper leaves on a waiting row, so the
        # status is the only thing standing between the straggler and the row.
        OxTask.objects.filter(pk=claimed.pk).update(status=WAITING)

        written = worker._write_outcome(
            claimed,
            status=OxTask.Status.SUCCESSFUL,
            duration_ms=0,
            return_value=3,
            finished_at=timezone.now(),
        )
        assert written is False
        assert current(claimed).status == WAITING


# -- what keeps the helpers private and stable ---------------------------------

WRITING_CALLS = {"update", "create", "get_or_create", "update_or_create", "OxTask"}


def names_waiting(node):
    if isinstance(node, ast.Attribute):
        return node.attr == "WAITING"
    return isinstance(node, ast.Constant) and node.value == "WAITING"


def waiting_writes(tree):
    """Line numbers where WAITING is assigned to a status, however it is spelled."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else None
            if isinstance(func, ast.Name):
                name = func.id
            if name in WRITING_CALLS and any(
                keyword.arg == "status" and names_waiting(keyword.value)
                for keyword in node.keywords
            ):
                yield node.lineno
        elif isinstance(node, ast.Assign):
            if names_waiting(node.value) and any(
                isinstance(target, ast.Attribute) and target.attr == "status"
                for target in node.targets
            ):
                yield node.lineno
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "status"
                    and names_waiting(value)
                ):
                    yield node.lineno


def test_only_the_private_helpers_write_waiting():
    found = {}
    for path in sorted(SRC.rglob("*.py")):
        lines = list(waiting_writes(ast.parse(path.read_text(encoding="utf-8"))))
        if lines:
            found[path.relative_to(SRC).as_posix()] = lines
    # Non-empty on purpose: a scan that finds nothing anywhere proves nothing.
    assert set(found) == {"_waiting.py"}, found


EXPECTED_SIGNATURES = {
    "enqueue": (
        "(task: 'Task[P, R]', args: 'Sequence[Any]', kwargs: 'Mapping[str, Any]', "
        "*, using: 'str') -> 'TaskResult[P, R]'"
    ),
    "release": "(task_id: 'TaskId', *, lease_epoch: 'int', using: 'str') -> 'bool'",
    "release_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') -> 'tuple[int, int]'"
    ),
    "cancel_many": "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') -> 'int'",
    "revive_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str') "
        "-> 'dict[uuid.UUID, Revival]'"
    ),
}


def test_the_waiting_helpers_keep_their_signatures():
    actual = {
        name: str(inspect.signature(getattr(_waiting, name)))
        for name in EXPECTED_SIGNATURES
    }
    assert actual == EXPECTED_SIGNATURES
    assert _waiting.TaskId == str | uuid.UUID
    assert {member.name: member.value for member in _waiting.Revival} == {
        "REVIVED": "revived",
        "NOT_FOUND": "not found",
        "WRONG_STATUS_OR_EPOCH": "wrong status or epoch",
    }


def test_the_waiting_helpers_stay_private():
    assert _waiting.__name__ == "django_ox._waiting"
    init = ast.parse((SRC / "__init__.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(init):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert not any("_waiting" in name for name in imported), imported
    assert not any("waiting" in name for name in django_ox.__all__)

    token = re.compile(r"(?<!\w)_waiting(?!\w)")
    public = [REPO / "README.md", REPO / "CHANGELOG.md"]
    public += sorted(
        path
        for path in (REPO / "docs").rglob("*")
        if path.suffix in {".md", ".txt", ".yml", ".html"}
    )
    assert len(public) > 3
    named = [
        path.relative_to(REPO).as_posix()
        for path in public
        if token.search(path.read_text(encoding="utf-8"))
    ]
    assert named == []


# -- the migration -------------------------------------------------------------


# Transactional because SQLite's schema editor refuses to open inside the
# atomic block an ordinary database test runs in, even to collect SQL.
@pytest.mark.django_db(transaction=True)
def test_migration_0008_runs_no_sql():
    for direction in ([], ["--backwards"]):
        out = StringIO()
        call_command("sqlmigrate", "django_ox", "0008", *direction, stdout=out)
        text = out.getvalue()
        statements = {
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith("--")
        }
        assert statements <= {"BEGIN;", "COMMIT;"}, text
        assert "(no-op)" in text, text


def applied_migrations(alias=DB):
    recorder = MigrationRecorder(connections[alias])
    return {name for app, name in recorder.applied_migrations() if app == "django_ox"}


ROLLBACK_STEPS = "Rolling back to 1.2 after workflows have run"


@pytest.mark.django_db(transaction=True)
def test_reversing_0008_refuses_while_waiting_rows_exist():
    row = held()
    try:
        with pytest.raises(IrreversibleError, match="1 task") as refused:
            call_command("migrate", "django_ox", "0007_oxschedule", verbosity=0)
        # A count of 0 at this moment is not a way back on its own, so the
        # refusal sends the reader to the changelog's steps, which must exist.
        message = str(refused.value)
        assert f'"{ROLLBACK_STEPS}"' in message
        assert "https://oxpull.com/django-ox/changelog/" in message
        changelog = (REPO / "CHANGELOG.md").read_text()
        assert f"**{ROLLBACK_STEPS}.**" in changelog
        assert current(row).status == WAITING
        assert "0008_waiting" in applied_migrations()

        OxTask.objects.filter(pk=row.pk).delete()
        call_command("migrate", "django_ox", "0007_oxschedule", verbosity=0)
        assert "0008_waiting" not in applied_migrations()
    finally:
        call_command("migrate", "django_ox", verbosity=0)
    assert "0008_waiting" in applied_migrations()


@pytest.mark.django_db(databases=["default", ALT], transaction=True)
def test_reversing_0008_counts_the_rows_on_the_database_it_migrates():
    """
    Waiting rows on one database neither block nor excuse the way back on
    another: the refusal reads through the connection being migrated.
    """
    back = ("migrate", "django_ox", "0007_oxschedule")
    row = a_row(WAITING, using=ALT)
    try:
        with pytest.raises(IrreversibleError, match="1 task"):
            call_command(*back, database=ALT, verbosity=0)
        assert "0008_waiting" in applied_migrations(ALT)

        OxTask.objects.using(ALT).filter(pk=row.pk).delete()
        a_row(WAITING)
        call_command(*back, database=ALT, verbosity=0)
        assert "0008_waiting" not in applied_migrations(ALT)
        assert "0008_waiting" in applied_migrations()
    finally:
        call_command("migrate", "django_ox", database=ALT, verbosity=0)
    assert "0008_waiting" in applied_migrations(ALT)
