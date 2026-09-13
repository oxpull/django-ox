"""
The WAITING status and django_ox._waiting, the only code that writes it.

django-ox never calls the helpers. A package built on django-ox does, under an
exact version pin, so this file is what keeps them from drifting: a changed
signature or a loosened guard fails here before it reaches anything that
depends on it.
"""

import ast
import logging
import re
import uuid
from datetime import timedelta
from io import StringIO
from pathlib import Path

import pytest
from django.core.management import call_command
from django.db import DatabaseError, connection, connections
from django.db.migrations.exceptions import IrreversibleError
from django.db.migrations.recorder import MigrationRecorder
from django.db.models import QuerySet
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

import django_ox
from django_ox import _waiting, actions
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

from .tasks import add
from .test_worker import reap_away
from .test_write_routing import ALT, _PrimaryAndReplica

SRC = Path(django_ox.__file__).resolve().parent
REPO = SRC.parent.parent

WAITING = OxTask.Status.WAITING


def held(task=add, args=(1, 2), kwargs=None):
    """A row inserted WAITING, the way the helpers insert one."""
    result = _waiting.enqueue(task, list(args), kwargs or {})
    return OxTask.objects.get(pk=result.id)


def a_row(status, *, using="default", **fields):
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

PENDING = {OxTask.Status.READY, OxTask.Status.RUNNING, WAITING}
SETTLED = {
    OxTask.Status.SUCCESSFUL,
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    OxTask.Status.DISCARDED,
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
    "_waiting.enqueue": lambda task: _waiting.enqueue(task, [1, 2], {}),
}


@pytest.mark.django_db
class TestEnqueue:
    def test_enqueue_inserts_waiting_and_never_ready(self):
        with CaptureQueriesContext(connection) as ctx:
            result = _waiting.enqueue(add, [1, 2], {})
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
        waiting = _waiting.enqueue(task, [3], {"b": 4})

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
        assert OxTask.objects.get(pk=ready.id).status == OxTask.Status.READY
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
            _waiting.enqueue(add.using(backend="immediate"), [1, 2], {})
        assert not OxTask.objects.exists()


# -- release -------------------------------------------------------------------


@pytest.mark.django_db
class TestRelease:
    def test_release_moves_a_waiting_row_to_ready_and_keeps_its_epoch(self):
        row = held()
        assert _waiting.release(row.pk) is True
        after = current(row)
        assert after.status == OxTask.Status.READY
        assert after.lease_epoch == row.lease_epoch
        assert after.run_after is not None
        assert _waiting.release(row.pk) is False

    @pytest.mark.parametrize(
        "status", [status for status in OxTask.Status if status != WAITING]
    )
    def test_release_moves_only_waiting_rows(self, status):
        row = a_row(status, lease_epoch=3)
        assert _waiting.release(row.pk) is False
        assert _waiting.release(row.pk, lease_epoch=3) is False
        assert _waiting.release_many([row.pk]) == (0, 1)
        after = current(row)
        assert (after.status, after.run_after, after.lease_epoch) == (status, None, 3)

    def test_a_missing_or_malformed_id_moves_nothing(self):
        row = held()
        assert _waiting.release(uuid.uuid4()) is False
        assert _waiting.release("not-a-uuid") is False
        # Counted as django_ox.actions counts: duplicates once, malformed skipped.
        assert _waiting.release_many(
            [row.pk, str(row.pk), "not-a-uuid", uuid.uuid4()]
        ) == (1, 2)
        assert _waiting.release_many([]) == (0, 0)

    def test_the_epoch_pin_refuses_a_moved_row(self):
        row = held()
        OxTask.objects.filter(pk=row.pk).update(lease_epoch=5)
        assert _waiting.release(row.pk, lease_epoch=4) is False
        assert current(row).status == WAITING
        assert _waiting.release(row.pk, lease_epoch=5) is True
        assert current(row).status == OxTask.Status.READY

        other = held()
        OxTask.objects.filter(pk=other.pk).update(lease_epoch=5)
        assert _waiting.cancel_many([(other.pk, 4)]) == 0
        assert current(other).status == WAITING
        assert _waiting.cancel_many([(other.pk, 5)]) == 1
        assert _waiting.revive_many([(other.pk, 4)]) == 0
        assert current(other).status == OxTask.Status.DISCARDED
        assert _waiting.revive_many([(other.pk, 5)]) == 1
        assert (current(other).status, current(other).lease_epoch) == (WAITING, 6)

    @pytest.mark.parametrize("bulk", [False, True], ids=["release", "release_many"])
    def test_release_keeps_a_later_run_after_and_otherwise_stamps_now(self, bulk):
        later = timezone.now() + timedelta(hours=2)
        earlier = timezone.now() - timedelta(hours=2)
        unset = held()
        future = held(add.using(run_after=later))
        past = held(add.using(run_after=earlier))
        assert current(past).run_after == earlier

        before = timezone.now()
        if bulk:
            assert _waiting.release_many([unset.pk, future.pk, past.pk]) == (3, 0)
        else:
            assert all(_waiting.release(row.pk) for row in (unset, future, past))
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
        moved = _waiting.cancel_many([(row.pk, 3)])
        after = current(row)
        if status in (OxTask.Status.READY, WAITING):
            assert moved == 1
            assert after.status == OxTask.Status.DISCARDED
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
        moved = _waiting.revive_many([(row.pk, 3)])
        after = current(row)
        if status == OxTask.Status.DISCARDED:
            assert moved == 1
            assert after.status == WAITING
            assert after.lease_epoch == 4
            assert after.finished_at is None
            assert (after.attempts, after.errors, after.run_after) == (2, errors, later)
        else:
            assert moved == 0
            assert (after.status, after.lease_epoch) == (status, 3)

    def test_each_row_is_pinned_to_its_own_epoch(self):
        rows = [held() for _ in range(4)]
        for row, epoch in zip(rows, (0, 2, 1, 2), strict=True):
            OxTask.objects.filter(pk=row.pk).update(lease_epoch=epoch)
        # The second row is named with an epoch it no longer has.
        claimed = [(rows[0].pk, 0), (rows[1].pk, 1), (rows[2].pk, 1), (rows[3].pk, 2)]
        assert _waiting.cancel_many(claimed) == 3
        assert [current(row).status for row in rows] == [
            OxTask.Status.DISCARDED,
            WAITING,
            OxTask.Status.DISCARDED,
            OxTask.Status.DISCARDED,
        ]
        assert _waiting.revive_many([(rows[0].pk, 0), (rows[2].pk, 1)]) == 2
        assert [current(row).lease_epoch for row in rows] == [1, 2, 2, 2]

    def test_a_malformed_id_in_a_pinned_form_moves_nothing(self):
        row = held()
        assert _waiting.cancel_many([("not-a-uuid", 0), (uuid.uuid4(), 0)]) == 0
        assert _waiting.revive_many([("not-a-uuid", 0)]) == 0
        assert _waiting.cancel_many([]) == 0
        assert current(row).status == WAITING

    def test_revive_bumps_the_epoch_and_fences_a_straggler(self, worker):
        add.enqueue(1, 2)
        OxTask.objects.update(attempts=2)
        stale = worker.claim_one()
        reap_away(worker, stale)
        assert current(stale).status == OxTask.Status.LOST
        assert actions.discard(stale.pk) is True

        assert _waiting.revive_many([(stale.pk, stale.lease_epoch)]) == 1
        revived = current(stale)
        assert (revived.status, revived.lease_epoch) == (
            WAITING,
            stale.lease_epoch + 1,
        )

        finish = {"duration_ms": 0, "return_value": 3, "finished_at": timezone.now()}
        status = OxTask.Status.SUCCESSFUL
        assert worker._write_outcome(stale, status=status, **finish) is False
        # A decision made from a read taken before the revival misses too.
        assert _waiting.cancel_many([(stale.pk, stale.lease_epoch)]) == 0
        assert _waiting.release(stale.pk, lease_epoch=stale.lease_epoch) is False

        assert _waiting.release(stale.pk, lease_epoch=revived.lease_epoch) is True
        claimed = worker.claim_one()
        assert claimed.pk == stale.pk
        assert claimed.lease_epoch == revived.lease_epoch + 1
        assert worker._write_outcome(stale, status=status, **finish) is False
        assert current(stale).status == OxTask.Status.RUNNING


# -- the bulk forms ------------------------------------------------------------

BULK = ["release_many", "cancel_many", "revive_many"]


def run_bulk(helper, rows):
    if helper == "release_many":
        return _waiting.release_many([row.pk for row in rows])
    pinned = [(row.pk, current(row).lease_epoch) for row in rows]
    return getattr(_waiting, helper)(pinned)


def rows_for(helper, count):
    rows = [held() for _ in range(count)]
    if helper == "revive_many":
        _waiting.cancel_many([(row.pk, row.lease_epoch) for row in rows])
    return rows


UUID_TEXT = re.compile(
    r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}"
)


@pytest.mark.django_db
class TestBulk:
    @pytest.mark.parametrize("helper", BULK)
    def test_a_bulk_move_that_fails_part_way_moves_nothing(self, helper, monkeypatch):
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        rows = rows_for(helper, 5)
        before = [current(row).status for row in rows]

        real_update = QuerySet.update
        calls = []

        def update_that_fails_second(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                raise DatabaseError("the second chunk fails")
            return real_update(self, **kwargs)

        monkeypatch.setattr(QuerySet, "update", update_that_fails_second)
        with pytest.raises(DatabaseError, match="second chunk"):
            run_bulk(helper, rows)
        monkeypatch.setattr(QuerySet, "update", real_update)

        assert len(calls) == 2
        assert [current(row).status for row in rows] == before

    @pytest.mark.parametrize("helper", BULK)
    def test_bulk_moves_lock_in_primary_key_order(self, helper, monkeypatch):
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        rows = rows_for(helper, 5)
        if helper != "release_many":
            # Interleaved epochs, so ordering by epoch would break key order.
            for index, row in enumerate(sorted(rows, key=lambda r: r.pk)):
                OxTask.objects.filter(pk=row.pk).update(lease_epoch=index % 2)
        given = sorted(rows, key=lambda r: r.pk, reverse=True)
        given = given[1::2] + given[::2]

        with CaptureQueriesContext(connection) as ctx:
            moved = run_bulk(helper, given)
        expected_moved = (5, 0) if helper == "release_many" else 5
        assert moved == expected_moved

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
        assert issued == sorted(row.pk for row in rows)


# -- the database a move is written to ----------------------------------------

READY = OxTask.Status.READY
DISCARDED = OxTask.Status.DISCARDED

MOVES = {
    # helper: (status before, the call, what it returns when it moved, status after)
    "release": (WAITING, lambda pk, kw: _waiting.release(pk, **kw), True, READY),
    "release_many": (
        WAITING,
        lambda pk, kw: _waiting.release_many([pk], **kw),
        (1, 0),
        READY,
    ),
    "cancel_many": (
        WAITING,
        lambda pk, kw: _waiting.cancel_many([(pk, 0)], **kw),
        1,
        DISCARDED,
    ),
    "revive_many": (
        DISCARDED,
        lambda pk, kw: _waiting.revive_many([(pk, 0)], **kw),
        1,
        WAITING,
    ),
}


@pytest.mark.django_db(databases=["default", ALT])
class TestTheDatabaseAMoveIsWrittenTo:
    """
    The same row, primary key included, sits on both databases, so a move
    written through the wrong connection changes the wrong copy instead of
    failing to find one.
    """

    @pytest.fixture(params=["using", "router"])
    def to_alt(self, request, settings):
        """The keyword arguments that send a helper's write to ALT."""
        if request.param == "router":
            settings.DATABASE_ROUTERS = [_PrimaryAndReplica()]
            return {}
        return {"using": ALT}

    @pytest.mark.parametrize("helper", list(MOVES))
    def test_a_move_is_written_to_the_database_it_names(self, helper, to_alt):
        before, move, moved, after = MOVES[helper]
        pk = uuid.uuid4()
        a_row(before, pk=pk)
        a_row(before, pk=pk, using=ALT)

        assert move(pk, to_alt) == moved
        assert OxTask.objects.using(ALT).get(pk=pk).status == after
        assert OxTask.objects.using("default").get(pk=pk).status == before

    def test_enqueue_inserts_into_the_database_it_names(self, to_alt):
        result = _waiting.enqueue(add, [1, 2], {}, **to_alt)
        assert OxTask.objects.using(ALT).get(pk=result.id).status == WAITING
        assert not OxTask.objects.using("default").exists()


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
            assert _waiting.release(first.pk)
            assert _waiting.release_many([second.pk]) == (1, 0)
            assert _waiting.cancel_many([(third.pk, 0)]) == 1
            assert _waiting.revive_many([(third.pk, 0)]) == 1
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
        "*, using: 'str | None' = None) -> 'TaskResult[P, R]'"
    ),
    "release": (
        "(task_id: 'TaskId', *, using: 'str | None' = None, "
        "lease_epoch: 'int | None' = None) -> 'bool'"
    ),
    "release_many": (
        "(task_ids: 'Iterable[TaskId]', *, using: 'str | None' = None) "
        "-> 'tuple[int, int]'"
    ),
    "cancel_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str | None' = None) -> 'int'"
    ),
    "revive_many": (
        "(rows: 'Iterable[tuple[TaskId, int]]', *, using: 'str | None' = None) -> 'int'"
    ),
}


def test_the_waiting_helpers_keep_their_signatures():
    import inspect

    actual = {
        name: str(inspect.signature(getattr(_waiting, name)))
        for name in EXPECTED_SIGNATURES
    }
    assert actual == EXPECTED_SIGNATURES
    assert _waiting.TaskId == str | uuid.UUID


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


def applied_migrations(alias="default"):
    recorder = MigrationRecorder(connections[alias])
    return {name for app, name in recorder.applied_migrations() if app == "django_ox"}


@pytest.mark.django_db(transaction=True)
def test_reversing_0008_refuses_while_waiting_rows_exist():
    row = held()
    try:
        with pytest.raises(IrreversibleError, match="1 task"):
            call_command("migrate", "django_ox", "0007_oxschedule", verbosity=0)
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
