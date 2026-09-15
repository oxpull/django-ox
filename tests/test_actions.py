"""
django_ox.actions: retry and discard as compare-and-set moves on one row.
"""

import re
import threading
import uuid
from types import SimpleNamespace

import pytest
from django.db import OperationalError, connection, connections, transaction
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import _waiting, actions, stats
from django_ox.compat import TaskResultStatus, default_task_backend
from django_ox.models import OxTask
from django_ox.worker import Worker

from .contention import (
    CONTENTION,
    UUID_TEXT,
    descending,
    failing,
    in_key_order,
    simulated,
)
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
        # The unknown id and the malformed string count as skipped; the
        # duplicate counts once.
        assert skipped == len(ids) + 2 - expected

    def test_malformed_ids_count_as_skipped(self):
        """(changed, skipped) accounts for every distinct item passed in."""
        seed(OxTask.Status.FAILED, 1)
        pk = OxTask.objects.get().pk
        assert actions.retry_many(["nope", "nope", "also-nope"]) == (0, 2)
        assert actions.retry_many([pk, "junk"]) == (1, 1)
        assert actions.discard_many(["junk"]) == (0, 1)

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
        transaction's own statements. Where there are row locks, one locking
        read per chunk as well. Never a query per row.
        """
        seed(OxTask.Status.FAILED, 14000)
        seed(OxTask.Status.SUCCESSFUL, 6000)
        with CaptureQueriesContext(connections["default"]) as ctx:
            assert actions.retry_many(OxTask.objects.all()) == (14000, 6000)
        updates = [q for q in ctx.captured_queries if q["sql"].startswith("UPDATE")]
        locks = [q for q in ctx.captured_queries if "FOR UPDATE" in q["sql"]]
        assert len(updates) == 20000 // actions.UPDATE_CHUNK_SIZE
        if connection.features.has_select_for_update:
            assert len(locks) == len(updates)
        else:
            assert locks == []
        assert len(ctx) <= len(updates) + len(locks) + 3
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


@pytest.mark.django_db
class TestWaitingRows:
    def test_the_discardable_statuses_gain_waiting_and_nothing_else(self):
        assert actions.DISCARDABLE_STATUSES == (
            OxTask.Status.READY,
            OxTask.Status.FAILED,
            OxTask.Status.LOST,
            OxTask.Status.WAITING,
        )
        assert actions.RETRYABLE_STATUSES == (OxTask.Status.FAILED, OxTask.Status.LOST)

    def test_retry_and_expire_lease_refuse_waiting(self):
        held = _waiting.enqueue(add, [1, 2], {}, using="default")
        before = OxTask.objects.filter(pk=held.id).values().get()

        assert actions.retry(held.id) is False
        assert actions.retry_many([held.id]) == (0, 1)
        assert actions.expire_lease(held.id) is False

        assert OxTask.objects.filter(pk=held.id).values().get() == before

    def test_discard_closes_a_waiting_row(self, worker):
        held = _waiting.enqueue(add, [1, 2], {}, using="default")
        other = _waiting.enqueue(add, [3, 4], {}, using="default")

        assert actions.discard(held.id) is True
        assert actions.discard_many([other.id]) == (1, 0)

        for result in (held, other):
            row = OxTask.objects.get(pk=result.id)
            assert row.status == OxTask.Status.DISCARDED
            assert row.finished_at is not None
            assert row.lease_epoch == 0
            fetched = default_task_backend.get_result(result.id)
            assert fetched.status == TaskResultStatus.FAILED
        assert _waiting.release(held.id, lease_epoch=0, using="default") is False
        assert worker.run_once() is False


def release_at_0(result_id):
    return _waiting.release(result_id, lease_epoch=0, using="default")


@pytest.mark.django_db(transaction=True)
def test_discard_racing_a_release_has_one_outcome():
    """
    A release leaves the row READY at the same epoch, which discard still
    accepts. So in either order, and with both moves at once, the row ends
    DISCARDED and discard reports it; release reports True only when it got
    there first.
    """
    first = _waiting.enqueue(add, [1, 2], {}, using="default")
    assert release_at_0(first.id) is True
    assert actions.discard(first.id) is True

    second = _waiting.enqueue(add, [1, 2], {}, using="default")
    assert actions.discard(second.id) is True
    assert release_at_0(second.id) is False

    for result in (first, second):
        assert OxTask.objects.get(pk=result.id).status == OxTask.Status.DISCARDED

    for _ in range(10):
        result = _waiting.enqueue(add, [1, 2], {}, using="default")
        barrier = threading.Barrier(2)
        outcomes = {}

        def race(name, move, result=result, barrier=barrier, outcomes=outcomes):
            try:
                barrier.wait(timeout=10)
                outcomes[name] = move(result.id)
            finally:
                connections.close_all()

        threads = [
            threading.Thread(target=race, args=("release", release_at_0)),
            threading.Thread(target=race, args=("discard", actions.discard)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert set(outcomes) == {"release", "discard"}, outcomes
        assert outcomes["discard"] is True, outcomes
        assert OxTask.objects.get(pk=result.id).status == OxTask.Status.DISCARDED
    assert Worker(backoff_initial=0).claim_one() is None


MANY = {"retry_many": actions.retry_many, "discard_many": actions.discard_many}
MOVED_TO = {"retry_many": OxTask.Status.READY, "discard_many": OxTask.Status.DISCARDED}


@pytest.mark.django_db
@pytest.mark.parametrize("name", list(MANY))
def test_chunks_go_in_key_order_each_locked_before_its_update(name, monkeypatch):
    """
    The ids are sorted before they are chunked, whatever order they are given
    in. Where there are row locks, each chunk's UPDATE follows a locking read
    of that chunk's ids in key order. The read names the ids and nothing else:
    with a status in it, MySQL can read the rows from a status index and lock
    them in that index's order.
    """
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    keys = in_key_order(row.pk for row in seed(OxTask.Status.FAILED, 5))
    given = [keys[3], keys[0], keys[4], keys[1], keys[2]]

    with CaptureQueriesContext(connection) as ctx:
        assert MANY[name](given) == (5, 0)

    statements = []
    for query in ctx.captured_queries:
        sql = query["sql"].strip()
        verb = sql.split(None, 1)[0].upper()
        if verb not in {"SELECT", "UPDATE"}:
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
    status = re.escape(f"{table}.{connection.ops.quote_name('status')}")
    # The read selects the key alone, so ORDER BY 1 is ORDER BY the key.
    locking_read = re.compile(
        rf"^SELECT {pk}(?: AS \S+)? FROM {re.escape(table)} "  # noqa: S608
        rf"WHERE {pk} IN \([^()]*\) ORDER BY (?:1|{pk}) ASC FOR UPDATE$"
    )
    for verb, _, sql in statements:
        if verb == "SELECT":
            assert locking_read.search(sql), sql
        else:
            assert re.search(rf"{status} IN \(", sql), sql
    assert set(OxTask.objects.values_list("status", flat=True)) == {MOVED_TO[name]}


# The order MariaDB 10.11 and 11.4 read these values back in, from a uuid
# primary key column, with ORDER BY on the key.
MARIADB_UUID_ORDER = [
    "00000000-0000-0000-0000-000000000000",
    "a0000000-0000-0a00-8000-000000000000",
    "c0000000-0000-5fff-ffff-000000000000",
    "ffffffff-ffff-4fff-bfff-000000000001",
    "00000000-0000-4000-8000-000000000002",
    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "017f22e2-79b0-7cc3-98c4-dc0c0c07398f",
    "1ec9414c-232a-6b00-b3c8-9e6bdeced846",
    "12345678-9abc-4def-8123-456789abcdef",
    "b0000000-0000-4000-4000-000000000000",
    "fedcba98-7654-4321-8fed-cba987654321",
    "d0000000-0000-6000-8000-000000000000",
]


@pytest.mark.django_db
def test_the_key_order_is_the_order_the_database_reads_keys_in():
    keys = [row.pk for row in seed(OxTask.Status.FAILED, 300)]
    assert in_key_order(keys) == list(
        OxTask.objects.order_by("pk").values_list("pk", flat=True)
    )


def test_mariadb_key_order_follows_its_uuid_column():
    expected = [uuid.UUID(text) for text in MARIADB_UUID_ORDER]
    assert sorted(expected) != expected
    assert sorted(sorted(expected), key=actions._mariadb_uuid_order) == expected


@pytest.mark.parametrize(
    ("vendor", "native_uuid", "mariadb"),
    [
        ("mysql", True, True),
        ("mysql", False, False),
        ("postgresql", True, False),
        ("sqlite", False, False),
    ],
)
def test_only_a_uuid_column_on_mysql_gets_mariadbs_order(vendor, native_uuid, mariadb):
    alias = "key-order-probe"
    connections[alias] = SimpleNamespace(
        vendor=vendor, features=SimpleNamespace(has_native_uuid_field=native_uuid)
    )
    try:
        key = actions._key_order(alias)
    finally:
        del connections[alias]
    values = [uuid.UUID(text) for text in MARIADB_UUID_ORDER]
    expected = values if mariadb else sorted(values)
    assert sorted(values[1::2] + values[::2], key=key) == expected


@pytest.mark.django_db
@pytest.mark.parametrize("name", list(MANY))
def test_the_chunks_follow_the_key_order_of_the_database(name, monkeypatch):
    """
    The order comes from actions._key_order.
    test_the_key_order_is_the_order_the_database_reads_keys_in pins that to
    the database under test. Here it is swapped for descending order, which
    no database uses, so a sort that ignored it would show on any of them.
    """
    monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
    monkeypatch.setattr(actions, "_key_order", lambda alias: descending)
    keys = sorted((row.pk for row in seed(OxTask.Status.FAILED, 5)), reverse=True)

    with failing("UPDATE", lambda: None, lambda n: False) as updates:
        assert MANY[name](sorted(keys)) == (5, 0)

    assert [ids for _, ids in updates] == [keys[0:2], keys[2:4], keys[4:5]]


def epochs():
    return dict(OxTask.objects.values_list("pk", "lease_epoch"))


@pytest.mark.django_db(transaction=True)
class TestContendedBulkCalls:
    """
    A bulk call that opened its own transaction and loses a deadlock or a
    serialization failure starts again from its first chunk, three attempts in
    all. Anything else, and anything inside a caller's transaction, goes to
    the caller at once. The errors are simulated in place of an UPDATE, shaped
    as Django raises the drivers' (tests/test_contention.py pins the shape
    against real ones).
    """

    @pytest.mark.parametrize("name", list(MANY))
    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_call_that_loses_runs_again_from_its_first_chunk(
        self, kind, name, monkeypatch
    ):
        monkeypatch.setattr(actions, "UPDATE_CHUNK_SIZE", 2)
        keys = in_key_order(row.pk for row in seed(OxTask.Status.FAILED, 5))
        before = epochs()

        with failing("UPDATE", lambda: simulated(kind), lambda n: n == 2) as updates:
            assert MANY[name](list(reversed(keys))) == (5, 0)

        # The second chunk lost, which rolled back the first one's move too.
        # The call then ran again from the first chunk, not from the second.
        assert [ids for _, ids in updates] == [
            keys[0:2],
            keys[2:4],
            keys[0:2],
            keys[2:4],
            keys[4:5],
        ]
        assert set(OxTask.objects.values_list("status", flat=True)) == {MOVED_TO[name]}
        # Moved once each: a retry bumps the epoch by one, a discard not at all.
        bump = 1 if name == "retry_many" else 0
        assert epochs() == {pk: epoch + bump for pk, epoch in before.items()}

    @pytest.mark.parametrize("name", list(MANY))
    @pytest.mark.parametrize("kind", CONTENTION)
    def test_a_call_that_keeps_losing_raises_after_three_attempts(self, kind, name):
        seed(OxTask.Status.FAILED, 3)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: True) as updates,
            pytest.raises(OperationalError),
        ):
            MANY[name](OxTask.objects.all())

        assert len(updates) == 3
        assert set(OxTask.objects.values_list("status", flat=True)) == {
            OxTask.Status.FAILED
        }

    @pytest.mark.parametrize("name", list(MANY))
    def test_any_other_database_error_is_raised_at_once(self, name):
        seed(OxTask.Status.FAILED, 3)

        with (
            failing(
                "UPDATE", lambda: OperationalError("disk I/O error"), lambda n: True
            ) as updates,
            pytest.raises(OperationalError, match="disk I/O"),
        ):
            MANY[name](OxTask.objects.all())

        assert len(updates) == 1

    @pytest.mark.parametrize("name", list(MANY))
    @pytest.mark.parametrize("kind", CONTENTION)
    def test_inside_a_callers_transaction_nothing_runs_again(self, kind, name):
        seed(OxTask.Status.FAILED, 3)

        with (
            failing("UPDATE", lambda: simulated(kind), lambda n: n == 1) as updates,
            pytest.raises(OperationalError),
            transaction.atomic(),
        ):
            MANY[name](OxTask.objects.all())

        assert len(updates) == 1
        assert set(OxTask.objects.values_list("status", flat=True)) == {
            OxTask.Status.FAILED
        }
