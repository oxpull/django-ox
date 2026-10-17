import contextlib
import threading
import uuid
import warnings
from datetime import timedelta

import pytest
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.core.management import call_command
from django.db import OperationalError, connections, transaction
from django.db.models import F
from django.tasks import TaskResultStatus, default_task_backend
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from django.test import override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import actions
from django_ox.backend import OxBackend
from django_ox.bulk import enqueue_many
from django_ox.models import OxTask
from django_ox.results import task_result_from_db
from django_ox.worker import Worker
from oxpull import backend as backend_module
from oxpull.backend import OxpullBackend
from oxpull.exceptions import UniqueEnqueueRace
from oxpull.models import OxpullUniqueLock
from oxpull.unique import is_registered, unique
from oxpull.worker import OxpullWorker

from . import tasks
from .isolation import (
    callers_transaction,
    isolation_for_new_connections,
    opened_connections,
    pg_sqlstate,
    record_holder_checks,
    result_fields,
    session_isolation,
    set_session_isolation,
    set_transaction_isolation,
)

pytestmark = pytest.mark.django_db

LEVELS = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]


# -- marking -----------------------------------------------------------


def test_decorator_registers_module_path():
    assert is_registered("tests_oxpull.tasks.unique_echo")
    assert not is_registered("tests_oxpull.tasks.plain_echo")


def test_decorator_rejects_plain_functions():
    with pytest.raises(TypeError):
        unique(lambda: None)


def test_bad_unique_tasks_shape_raises_at_init():
    with pytest.raises(ImproperlyConfigured):
        OxpullBackend("default", {"OPTIONS": {"UNIQUE_TASKS": "not.a.list"}})


def test_check_flags_unique_task_path_that_does_not_import():
    backend = OxpullBackend(
        "default", {"OPTIONS": {"UNIQUE_TASKS": ["tests_oxpull.tasks.nope"]}}
    )
    assert "oxpull.E002" in [message.id for message in backend.check()]


def test_check_flags_unique_task_path_that_is_not_a_task():
    backend = OxpullBackend(
        "default", {"OPTIONS": {"UNIQUE_TASKS": ["tests_oxpull.tasks.STATE"]}}
    )
    assert "oxpull.E002" in [message.id for message in backend.check()]


def test_check_reports_unconfigured_alias():
    backend = OxpullBackend("ghost", {})
    assert [message.id for message in backend.check()] == ["oxpull.E003"]


def test_split_router_raises_at_init():
    with (
        override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.SplitTaskRouter"]),
        pytest.raises(ImproperlyConfigured),
    ):
        OxpullBackend("default", {})


def test_check_reports_split_router():
    backend = OxpullBackend("default", {})
    with override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.SplitTaskRouter"]):
        assert "oxpull.E004" in [message.id for message in backend.check()]


def test_split_rate_window_router_raises_at_init():
    with (
        override_settings(
            DATABASE_ROUTERS=["tests_oxpull.routers.SplitRateWindowRouter"]
        ),
        pytest.raises(ImproperlyConfigured),
    ):
        OxpullBackend("default", {})


def test_check_reports_a_router_that_splits_the_rate_window():
    # The window is the one oxpull table that is neither written in a
    # task's transaction nor joined to the task table, so it needs its
    # own case here: OxpullWorker reads and writes it on the alias the
    # task tables use, and a router naming another database for it does
    # not change which alias those statements name.
    backend = OxpullBackend("default", {})
    with override_settings(
        DATABASE_ROUTERS=["tests_oxpull.routers.SplitRateWindowRouter"]
    ):
        assert "oxpull.E004" in [message.id for message in backend.check()]


def test_check_warns_when_schedule_enqueues_unique_task():
    backend = OxpullBackend(
        "default",
        {
            "OPTIONS": {
                "SCHEDULES": {
                    "nightly": {
                        "task": "tests_oxpull.tasks.unique_echo",
                        "cron": "0 3 * * *",
                    }
                }
            }
        },
    )
    assert "oxpull.W001" in [message.id for message in backend.check()]


def test_backend_check_passes_on_test_settings():
    assert default_task_backend.check() == []


# -- dedup semantics ---------------------------------------------------


def test_duplicate_returns_existing_result():
    first = tasks.unique_echo.enqueue("x")
    second = tasks.unique_echo.enqueue("x")

    assert second.id == first.id
    assert second.status == TaskResultStatus.READY
    assert OxTask.objects.count() == 1
    assert OxpullUniqueLock.objects.count() == 1
    assert OxpullUniqueLock.objects.get().task_id is not None
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id


def test_duplicate_result_supports_refresh():
    first = tasks.unique_echo.enqueue("x")
    second = tasks.unique_echo.enqueue("x")
    second.refresh()
    assert second.id == first.id
    assert second.status == TaskResultStatus.READY


def test_different_args_are_different_tasks():
    tasks.unique_echo.enqueue("x")
    tasks.unique_echo.enqueue("y")
    assert OxTask.objects.count() == 2
    assert OxpullUniqueLock.objects.count() == 2


def test_kwarg_order_is_deduplicated():
    tasks.unique_add.enqueue(a=1, b=2)
    tasks.unique_add.enqueue(b=2, a=1)
    assert OxTask.objects.count() == 1


def test_positional_and_keyword_spellings_are_distinct_keys():
    # Deliberate: the key hashes args and kwargs as stored, with no
    # signature binding. add(1, 2) and add(1, b=2) are different keys.
    tasks.unique_add.enqueue(1, 2)
    tasks.unique_add.enqueue(1, b=2)
    assert OxTask.objects.count() == 2


def test_plain_tasks_are_untouched():
    tasks.plain_echo.enqueue("x")
    tasks.plain_echo.enqueue("x")
    assert OxTask.objects.count() == 2
    assert OxpullUniqueLock.objects.count() == 0


def test_options_configured_task_is_unique():
    first = tasks.configured_unique.enqueue("x")
    second = tasks.configured_unique.enqueue("x")
    assert second.id == first.id
    assert OxTask.objects.count() == 1


def test_queue_scopes_the_key():
    tasks.unique_echo.enqueue("x")
    tasks.unique_echo.using(queue_name="emails").enqueue("x")
    assert OxTask.objects.count() == 2


def test_using_copies_stay_unique():
    # Task.using() returns a new frozen Task; uniqueness keys off
    # module_path, which the copy shares.
    first = tasks.unique_echo.using(priority=5).enqueue("x")
    second = tasks.unique_echo.enqueue("x")
    assert second.id == first.id
    assert OxTask.objects.count() == 1


def test_duplicate_result_reflects_the_stored_row():
    first = tasks.unique_echo.enqueue("x")
    second = tasks.unique_echo.using(priority=7).enqueue("x")
    assert second.id == first.id
    # Row truth: the pending task was enqueued at priority 0; the
    # duplicate's using() override does not rewrite history.
    assert second.task.priority == 0


def test_int_and_string_dict_keys_deduplicate_like_stored_json():
    # JSONField stringifies int keys at dump time, so these payloads
    # store identically; the key must match them rather than raise over
    # an unsortable mixed-key dict.
    first = tasks.unique_echo.enqueue({1: "x", "y": 2})
    second = tasks.unique_echo.enqueue({"1": "x", "y": 2})
    assert second.id == first.id
    assert OxTask.objects.count() == 1


def test_no_signal_for_suppressed_duplicate():
    received = []

    def receiver(sender, task_result, **kwargs):
        received.append(task_result.id)

    task_enqueued.connect(receiver)
    try:
        first = tasks.unique_echo.enqueue("x")
        tasks.unique_echo.enqueue("x")
    finally:
        task_enqueued.disconnect(receiver)
    assert received == [first.id]


# -- lock release on every terminal path -------------------------------


def test_lock_released_after_success(worker):
    first = tasks.unique_echo.enqueue("x")
    assert worker.run_once() is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL

    second = tasks.unique_echo.enqueue("x")

    assert second.id != first.id
    assert OxTask.objects.count() == 2
    lock = OxpullUniqueLock.objects.get()
    assert str(lock.task_id) == second.id


def test_lock_released_after_terminal_failure(worker):
    first = tasks.unique_fail_always.enqueue()
    for _ in range(3):  # MAX_ATTEMPTS in tests_oxpull/settings.py
        assert worker.run_once() is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED

    second = tasks.unique_fail_always.enqueue()
    assert second.id != first.id
    assert OxTask.objects.count() == 2


def test_retry_is_not_a_duplicate(worker):
    first = tasks.unique_flaky.enqueue(2)
    assert worker.run_once() is True  # attempt 1 fails -> back to READY

    row = OxTask.objects.get(id=first.id)
    assert row.status == OxTask.Status.READY
    assert row.attempts == 1

    # The key is still held while the task waits to retry.
    duplicate = tasks.unique_flaky.enqueue(2)
    assert duplicate.id == first.id
    assert OxTask.objects.count() == 1

    assert worker.run_once() is True  # attempt 2 succeeds
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.SUCCESSFUL


def test_reaper_lost_write_does_not_strand_the_lock(worker):
    """
    The reaper settles the row via a queryset update with no signal at all
    (django_ox worker.reap). Nothing here listens for signals, so the lock
    must still release: staleness is derived from the row.

    It writes LOST rather than FAILED, and the lock releases on either.
    Holding the key for an unobserved attempt would block the name for as
    long as the vanished worker stays vanished, which is unbounded.
    """
    first = tasks.unique_echo.enqueue("x")
    OxTask.objects.filter(id=first.id).update(
        status=OxTask.Status.RUNNING,
        attempts=3,
        max_attempts=3,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    assert worker.reap() == 1
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.LOST

    second = tasks.unique_echo.enqueue("x")
    assert second.id != first.id
    assert OxTask.objects.count() == 2
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id


def test_reaper_requeue_keeps_the_lock(worker):
    """A reclaimed task with attempts remaining goes back to READY; the
    key stays held and a duplicate still resolves to it."""
    first = tasks.unique_echo.enqueue("x")
    OxTask.objects.filter(id=first.id).update(
        status=OxTask.Status.RUNNING,
        attempts=1,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    assert worker.reap() == 1
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.READY

    duplicate = tasks.unique_echo.enqueue("x")
    assert duplicate.id == first.id
    assert OxTask.objects.count() == 1


def _claim_then_lose_the_worker(worker, task, *args, last_attempt):
    """
    Enqueue a unique task, claim it, then age its lease past expiry, which
    is how a worker that died mid-attempt leaves the row.

    With last_attempt the row is set one attempt short of its limit before
    the claim, so the claim spends the final attempt and the in-memory row
    it returns is what that execution holds: the database's attempts and
    the lease epoch the claim handed out.
    """
    result = task.enqueue(*args)
    if last_attempt:
        OxTask.objects.filter(id=result.id).update(attempts=F("max_attempts") - 1)
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == result.id
    assert (db_task.attempts == db_task.max_attempts) is last_attempt
    expired = timezone.now() - timedelta(seconds=9999)
    OxTask.objects.filter(id=result.id).update(
        locked_at=expired, lease_expires_at=expired
    )
    return result, db_task


@pytest.mark.django_db(transaction=True)
def test_a_duplicate_coalesces_onto_an_expired_lease_until_the_reaper_writes_lost(
    worker,
):
    """
    A RUNNING holder whose worker has died still holds its key.

    Liveness is the row's status and nothing else. An expired lease says
    only that renewals stopped, and a dead worker and a live one whose
    renewal is late look the same from here, so the key is not released
    on it. A duplicate enqueued in that gap returns the holder's result
    and writes no row. The reaper settles it: on the last attempt it
    writes LOST, which frees the key. The duplicate that coalesced is
    left with a result that reads FAILED and no row of its own, and the
    next enqueue creates one. `docs/features/unique-tasks.md` names the gap.

    Releasing on an expired lease instead would not close the gap, only
    move it: a late renewal, or the reaper's requeue when attempts are
    left, would then leave two pending rows for one key.
    """
    first, _ = _claim_then_lose_the_worker(
        worker, tasks.unique_echo, "x", last_attempt=True
    )

    duplicate = tasks.unique_echo.enqueue("x")
    assert duplicate.id == first.id
    assert duplicate.status == TaskResultStatus.RUNNING
    assert OxTask.objects.count() == 1

    assert worker.reap() == 1
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.LOST
    assert OxTask.objects.count() == 1
    duplicate.refresh()
    assert duplicate.status == TaskResultStatus.FAILED

    third = tasks.unique_echo.enqueue("x")
    assert third.id != first.id
    assert OxTask.objects.count() == 2
    assert str(OxpullUniqueLock.objects.get().task_id) == third.id


@pytest.mark.django_db(transaction=True)
def test_a_duplicate_coalesces_onto_an_expired_lease_the_reaper_requeues(worker):
    """
    The same gap with attempts left. The duplicate coalesces onto the
    expired RUNNING holder, the reaper puts the row back to READY, which
    is still pending, and the key stays with it, so the coalesced work
    runs as the retry.
    """
    first, _ = _claim_then_lose_the_worker(
        worker, tasks.unique_echo, "x", last_attempt=False
    )

    assert tasks.unique_echo.enqueue("x").id == first.id
    assert OxTask.objects.count() == 1

    assert worker.reap() == 1
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.READY
    assert tasks.unique_echo.enqueue("x").id == first.id
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    ("task", "args", "outcome"),
    [
        (tasks.unique_echo, ["x"], OxTask.Status.SUCCESSFUL),
        (tasks.unique_fail_always, [], OxTask.Status.FAILED),
    ],
    ids=["successful", "failed"],
)
@pytest.mark.parametrize(
    "worker_class", [Worker, OxpullWorker], ids=["django-ox", "oxpull"]
)
def test_a_lost_holders_late_outcome_lands_on_its_own_row_and_keeps_the_new_lock(
    worker_class, task, args, outcome
):
    """
    A LOST holder's execution can outlive its key, and its outcome write
    touches its own row only.

    The reaper writes LOST without moving the lease epoch, so the
    execution that lost the lease keeps the right to write its outcome
    onto that row. The key is free by then: the next enqueue takes it
    with a new row, and a worker claims that row while the first
    execution is still running. Uniqueness deduplicates pending rows and
    does not exclude executions. When the straggler finishes, its write
    lands on the LOST row, the replacement stays RUNNING, and the lock
    still names the replacement, so a further duplicate coalesces onto it.

    Run with both worker classes. A fleet can hold either, and an outcome
    hook on OxpullWorker is where a change would most easily break this.
    """
    worker = worker_class(backoff_initial=0, poll_interval=0.05)
    first, straggler = _claim_then_lose_the_worker(
        worker, task, *args, last_attempt=True
    )
    assert worker.reap() == 1
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.LOST

    second = task.enqueue(*args)
    assert second.id != first.id
    replacement = worker.claim_one()
    assert replacement is not None and str(replacement.id) == second.id

    worker.execute(straggler)

    assert OxTask.objects.get(id=first.id).status == outcome
    assert OxTask.objects.get(id=second.id).status == OxTask.Status.RUNNING
    assert OxpullUniqueLock.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id
    assert task.enqueue(*args).id == second.id
    assert OxTask.objects.count() == 2


def test_pruned_task_row_releases_the_lock():
    first = tasks.unique_echo.enqueue("x")
    # ox_prune deletes finished rows outright; the lock must read a
    # missing row as "finished", not hold the key forever.
    OxTask.objects.filter(id=first.id).delete()

    second = tasks.unique_echo.enqueue("x")
    assert second.id != first.id
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id


# -- transactions ------------------------------------------------------


def test_duplicate_inside_caller_transaction_keeps_it_usable():
    first = tasks.unique_echo.enqueue("x")
    with transaction.atomic():
        second = tasks.unique_echo.enqueue("x")
        # The IntegrityError was confined to the inner savepoint; the
        # caller's transaction is still usable.
        assert OxTask.objects.count() == 1
    assert second.id == first.id


def test_two_enqueues_inside_one_transaction_deduplicate():
    # Same connection, same open transaction: the second INSERT conflicts
    # with the caller's own uncommitted lock row, which surfaces
    # immediately (no cross-transaction wait) and resolves to the first
    # task, still visible on this connection.
    with transaction.atomic():
        first = tasks.unique_echo.enqueue("x")
        second = tasks.unique_echo.enqueue("x")
    assert second.id == first.id
    assert OxTask.objects.count() == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", ["options", "session"])
@pytest.mark.parametrize("level", LEVELS)
def test_two_enqueues_inside_one_transaction_deduplicate_at_every_level(
    monkeypatch, level, how
):
    """
    A transaction that enqueues the same unique task twice gets the first
    task back at every level, whether the project sets the level in
    OPTIONS["isolation_level"] or the session sets its own.

    The holder is the transaction's own row, not yet committed. On
    PostgreSQL at REPEATABLE READ or SERIALIZABLE the enqueue asks for a
    share lock on the holder's row, and a row this transaction wrote is
    granted. No connection is opened on PostgreSQL.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite has one isolation level")
    connections["default"].close()
    if how == "options":
        isolation_for_new_connections(monkeypatch, level)
    else:
        set_session_isolation(level)
    checks = record_holder_checks(monkeypatch)
    outcome = {}
    try:
        with opened_connections() as opened, transaction.atomic():
            outcome["level"] = session_isolation()
            first = tasks.unique_echo.enqueue("x")
            try:
                outcome["second"] = tasks.unique_echo.enqueue("x")
            except UniqueEnqueueRace as exc:
                outcome["race"] = exc
    finally:
        connections["default"].close()

    assert outcome["level"] == level
    assert "race" not in outcome, outcome["race"]
    assert outcome["second"].id == first.id
    assert outcome["second"].status == TaskResultStatus.READY
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id
    if vendor == "postgresql":
        assert opened == []
        assert checks == ([] if level == "READ COMMITTED" else ["granted"])


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", ["options", "session"])
@pytest.mark.parametrize("change", ["discarded", "deleted", "finished"])
@pytest.mark.parametrize("level", LEVELS)
def test_a_holder_this_transaction_settled_is_not_deduplicated_against(
    monkeypatch, level, change, how
):
    """
    A transaction that discards, deletes or finishes a key's holder and
    then enqueues the same key queues a new task, at every level. A
    snapshot shows the transaction's own changes, so the holder reads as
    settled, or as gone, and the release path runs.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite has one isolation level")
    first = tasks.unique_echo.enqueue("x")
    connections["default"].close()
    if how == "options":
        isolation_for_new_connections(monkeypatch, level)
    else:
        set_session_isolation(level)
    outcome = {}
    try:
        with transaction.atomic():
            outcome["level"] = session_isolation()
            if change == "discarded":
                assert actions.discard(first.id)
            elif change == "deleted":
                assert OxTask.objects.filter(id=first.id).delete()[0] == 1
            else:
                assert (
                    OxTask.objects.filter(id=first.id).update(
                        status=OxTask.Status.SUCCESSFUL
                    )
                    == 1
                )
            with opened_connections() as opened:
                try:
                    outcome["second"] = tasks.unique_echo.enqueue("x")
                except UniqueEnqueueRace as exc:
                    outcome["race"] = exc
    finally:
        connections["default"].close()

    assert outcome["level"] == level
    assert "race" not in outcome, outcome["race"]
    second = outcome["second"]
    assert second.id != first.id, "deduplicated against a holder it had settled"
    assert second.status == TaskResultStatus.READY
    ready = OxTask.objects.filter(status=OxTask.Status.READY)
    assert [str(task.id) for task in ready] == [second.id]
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id
    if vendor == "postgresql":
        assert opened == []


def test_rollback_discards_task_and_lock_together():
    with pytest.raises(ValidationError), transaction.atomic():
        tasks.unique_echo.enqueue("x")
        raise ValidationError("abort")

    assert OxTask.objects.count() == 0
    assert OxpullUniqueLock.objects.count() == 0

    # The key is free; the next enqueue wins cleanly.
    result = tasks.unique_echo.enqueue("x")
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == result.id


def test_worker_still_runs_deduplicated_task_to_success(worker):
    first = tasks.unique_add.enqueue(2, 3)
    tasks.unique_add.enqueue(2, 3)
    assert worker.run_once() is True
    first.refresh()
    assert first.status == TaskResultStatus.SUCCESSFUL
    assert first.return_value == 5


# -- diagnostics and routing -------------------------------------------


@pytest.mark.django_db(databases=["default", "replica"])
def test_replica_read_router_cannot_fake_staleness():
    # A router that sends reads to a lagged (here: empty) replica must
    # not make a live lock look stale; every dedup-path read is pinned
    # to the write database.
    first = tasks.unique_echo.enqueue("pinned")
    with override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.ReplicaReadRouter"]):
        second = tasks.unique_echo.enqueue("pinned")
    assert second.id == first.id
    assert OxTask.objects.using("default").count() == 1
    assert OxpullUniqueLock.objects.using("default").count() == 1


# -- django-ox 0.3.x bulk enqueue and operator actions ------------------


def test_enqueue_many_refuses_a_unique_task():
    """
    Repro against django-ox 0.3.1: enqueue_many() writes its rows with
    one bulk INSERT and never reaches enqueue(), so no lock is taken.
    Against 0.2.1 that call did not exist. Against 0.3.1 it wrote three
    READY rows for one unique key beside a single lock that pointed at
    the first, and every later enqueue() of the key kept returning that
    first task, so the guarantee read as working while the duplicates
    ran.

    Refusing is the fix: silently voiding the property the task is
    marked with is worse than a call that says it cannot hold it.
    """
    first = tasks.unique_echo.enqueue("a")

    with pytest.raises(InvalidTask) as excinfo:
        enqueue_many(tasks.unique_echo, [(["a"], {}), (["b"], {})])
    assert "tests_oxpull.tasks.unique_echo" in str(excinfo.value)
    assert "enqueue_many()" in str(excinfo.value)

    assert OxTask.objects.count() == 1
    assert OxpullUniqueLock.objects.count() == 1
    assert tasks.unique_echo.enqueue("a").id == first.id


def test_enqueue_many_refuses_an_options_configured_unique_task():
    """UNIQUE_TASKS is the same marking, so it gets the same refusal."""
    with pytest.raises(InvalidTask):
        enqueue_many(tasks.configured_unique, [([1], {}), ([2], {})])
    assert OxTask.objects.count() == 0


def test_enqueue_many_still_bulk_inserts_a_plain_task():
    """A non-unique task keeps the parent's bulk path, untouched."""
    results = enqueue_many(tasks.plain_echo, [(["a"], {}), (["b"], {}), (["c"], {})])
    assert len({result.id for result in results}) == 3
    assert OxTask.objects.count() == 3
    assert OxpullUniqueLock.objects.count() == 0
    # The promise is the returned list, which enqueue_many documents as being
    # in input order. Every row written by one call shares an enqueued_at, so
    # ordering the rows by it is a three-way tie and the sequence that comes
    # back is the storage engine's: MySQL answers it differently from SQLite
    # and PostgreSQL. Assert the order the product gives, and the contents of
    # the table without an order it never promised.
    assert [result.args for result in results] == [["a"], ["b"], ["c"]]
    assert sorted(row.args for row in OxTask.objects.all()) == [["a"], ["b"], ["c"]]


def test_discarded_task_releases_the_lock():
    """
    django_ox.actions.discard closes a READY task without running it.
    Staleness is derived from the row rather than from a signal, and
    DISCARDED is neither READY nor RUNNING, so the key frees itself with
    no change to the release path.
    """
    first = tasks.unique_echo.enqueue("x")
    assert actions.discard(first.id) is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.DISCARDED

    second = tasks.unique_echo.enqueue("x")
    assert second.id != first.id
    assert OxTask.objects.count() == 2
    lock = OxpullUniqueLock.objects.get()
    assert str(lock.task_id) == second.id


def test_prune_clears_the_lock_of_a_discarded_task():
    first = tasks.unique_echo.enqueue("x")
    assert actions.discard(first.id) is True
    call_command("oxpull_prune")
    assert OxpullUniqueLock.objects.count() == 0


def test_retrying_a_settled_task_can_reopen_a_key_a_duplicate_took(worker):
    """
    A documented consequence, pinned so it cannot change unnoticed.

    The lock is an enqueue-time gate, not a runtime one. Once a unique
    task fails terminally its key is free, and the next enqueue takes it.
    django_ox.actions.retry then puts the original row back on the queue
    without passing the gate, so two READY rows share one key. Nothing in
    oxpull can prevent that: retry writes the task table directly, and
    oxpull does not own that table.

    An operator who retries a task that has already been superseded is
    asking for the old attempt to run, which is what happens.
    """
    first = tasks.unique_fail_always.enqueue()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED

    second = tasks.unique_fail_always.enqueue()
    assert second.id != first.id

    assert actions.retry(first.id) is True
    ready = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_fail_always", status=OxTask.Status.READY
    )
    assert ready.count() == 2
    assert OxpullUniqueLock.objects.count() == 1


# -- a holder read behind a caller's snapshot ---------------------------


def assert_retry_outside_the_snapshot(race, level, found=None):
    """
    PostgreSQL's refusal of a changed row, as the enqueue reports it. The
    message names the database and the level, says nothing was created, and
    says to retry in a new, short transaction at READ COMMITTED. `found` is
    what the read on a connection of its own found, when one ran.
    """
    assert isinstance(race, UniqueEnqueueRace), "no UniqueEnqueueRace was raised"
    assert isinstance(race.__cause__, OperationalError)
    assert pg_sqlstate(race.__cause__) == "40001"
    message = str(race)
    assert "on database 'default'" in message
    assert f"inside a transaction at {level}," in message
    assert "The enqueue created nothing." in message
    assert "Retry it in a new, short transaction at READ COMMITTED" in message
    if found is None:
        assert "A fresh read found" not in message
    else:
        assert f"A fresh read found {found}." in message


def finish_elsewhere(task_id):
    """Commit SUCCESSFUL onto the task row from a connection of the test's own."""
    other = connections.create_connection("default")
    try:
        table = other.ops.quote_name(OxTask._meta.db_table)
        with other.cursor() as cursor:
            cursor.execute(
                f"UPDATE {table} SET status = %s WHERE id = %s",  # noqa: S608
                [
                    OxTask.Status.SUCCESSFUL,
                    OxTask._meta.pk.get_db_prep_value(uuid.UUID(task_id), other),
                ],
            )
            assert cursor.rowcount == 1
    finally:
        other.close()


class BehindASnapshot:
    """
    A caller's transaction on a thread of its own. It reads the holder,
    which fixes the snapshot at REPEATABLE READ and SERIALIZABLE, waits
    while the test changes the holder, and then enqueues the same key in
    that transaction. After a UniqueEnqueueRace it checks the transaction
    still answers a query, commits it, and retries the enqueue in a new
    transaction at READ COMMITTED, as the message says to.
    """

    def __init__(self, level, holder_id, seen, enqueue):
        self.outcome = {}
        self._taken = threading.Event()
        self._changed = threading.Event()
        self._thread = threading.Thread(
            target=self._run, args=(level, holder_id, seen, enqueue)
        )
        self._thread.start()
        assert self._taken.wait(timeout=10)
        assert "error" not in self.outcome, self.outcome["error"]

    def _run(self, level, holder_id, seen, enqueue):
        outcome = self.outcome
        try:
            if level != "READ COMMITTED":
                set_session_isolation(level)
            with transaction.atomic():
                # The first read fixes the snapshot.
                assert OxTask.objects.get(id=holder_id).status == OxTask.Status[seen]
                self._taken.set()
                assert self._changed.wait(timeout=10)
                with opened_connections() as opened:
                    try:
                        outcome["result"] = enqueue()
                    except UniqueEnqueueRace as exc:
                        outcome["race"] = exc
                        outcome["usable"] = OxTask.objects.filter(id=holder_id).count()
                outcome["opened"] = [wrapper.alias for wrapper in opened]
                outcome["closed"] = all(
                    wrapper.connection is None for wrapper in opened
                )
            if "race" in outcome:
                # The retry the message asks for, in a new transaction at
                # READ COMMITTED.
                with transaction.atomic():
                    if connections["default"].vendor == "postgresql":
                        set_transaction_isolation("READ COMMITTED")
                    outcome["retried_level"] = session_isolation()
                    outcome["retried"] = enqueue()
        except BaseException as exc:  # pragma: no cover - reported by resume()
            outcome["error"] = exc
        finally:
            self._taken.set()
            connections.close_all()

    def resume(self):
        """Let the enqueue run, and return what happened."""
        self._changed.set()
        self._thread.join(timeout=30)
        assert not self._thread.is_alive()
        assert "error" not in self.outcome, self.outcome["error"]
        return self.outcome


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_holder_retried_behind_a_snapshot_keeps_its_lock(worker, level):
    """
    The duplicate path reads a settled holder with a locking read, so a
    status the transaction's snapshot no longer agrees with cannot release
    a live lock.

    A caller that read the holder as FAILED, then enqueued the same key
    while an operator retry committed READY in between, used to read FAILED
    again, delete the lock as stale, and insert a second READY row beside
    the retried one. The engines part at the locking read. InnoDB's is a
    current read, so the enqueue sees READY and returns the retried task.
    PostgreSQL refuses to lock a row that changed after the snapshot began
    (SQLSTATE 40001), which used to reach the caller raw. It is now
    UniqueEnqueueRace, chained to that failure, saying to retry in a new,
    short transaction at READ COMMITTED, and that retry returns the retried
    task. A holder the snapshot calls settled is not read again elsewhere.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and there is no snapshot")
    first = tasks.unique_fail_always.enqueue()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED

    caller = BehindASnapshot(
        level, first.id, "FAILED", tasks.unique_fail_always.enqueue
    )
    assert actions.retry(first.id) is True
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.READY
    outcome = caller.resume()

    if vendor == "postgresql":
        assert_retry_outside_the_snapshot(outcome.get("race"), level)
        assert "The row was the task that held the key" in str(outcome["race"])
        assert outcome["retried_level"] == "READ COMMITTED"
        assert outcome["retried"].id == first.id
    else:
        assert "race" not in outcome, outcome["race"]
        assert outcome["result"].id == first.id
    assert outcome["opened"] == []
    ready = OxTask.objects.filter(
        task_path="tests_oxpull.tasks.unique_fail_always", status=OxTask.Status.READY
    )
    assert ready.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("change", ["finished", "lost", "pruned"])
@pytest.mark.parametrize("seen", ["READY", "RUNNING"])
@pytest.mark.parametrize("level", LEVELS)
def test_a_holder_settled_behind_a_snapshot_is_not_deduplicated_against(
    monkeypatch, worker, level, seen, change
):
    """
    A holder that settles after the caller's snapshot began does not keep
    its key. Its worker finishes it, or the reaper records it LOST, or its
    row is pruned after it finished.

    The holder read takes no lock, so inside a caller's transaction at
    REPEATABLE READ, or at PostgreSQL's SERIALIZABLE, it answers from the
    snapshot and still calls the holder READY or RUNNING. On PostgreSQL the
    enqueue returned that settled task's result and queued nothing. The
    enqueue now asks for a share lock on the holder's row without waiting,
    in a savepoint it rolls back at once. PostgreSQL refuses a row that
    changed after the snapshot began. The enqueue then reads the holder and
    the key's lock once on a connection of its own, finds the task settled
    or gone, and raises UniqueEnqueueRace saying to retry in a new, short
    transaction at READ COMMITTED. That retry queues the new task. A key
    share lock would have been granted after the outcome and LOST writes,
    which change no key column.

    MySQL at REPEATABLE READ reads the committed status on a connection of
    its own, as 1.2.0 does. READ COMMITTED reads the current row. InnoDB's
    SERIALIZABLE makes this test's own read a locking read, which would
    hold off the worker; test_concurrency pins that level.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and there is no snapshot")
    first = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one() if seen == "RUNNING" else None
    checks = record_holder_checks(monkeypatch)

    caller = BehindASnapshot(
        level, first.id, seen, lambda: tasks.unique_echo.enqueue("held")
    )
    if db_task is None:
        db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == first.id
    if change == "lost":
        expired = timezone.now() - timedelta(hours=1)
        OxTask.objects.filter(pk=db_task.pk).update(
            attempts=db_task.max_attempts, locked_at=expired, lease_expires_at=expired
        )
        assert worker.reap() == 1
        assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.LOST
    else:
        worker.execute(db_task)
        assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.SUCCESSFUL
        if change == "pruned":
            assert OxTask.objects.filter(pk=db_task.pk).delete()[0] == 1
    outcome = caller.resume()

    snapshot = level != "READ COMMITTED"
    if vendor == "postgresql" and snapshot:
        found = {
            "finished": "the task SUCCESSFUL",
            "lost": "the task LOST",
            "pruned": "the task's row gone",
        }[change]
        assert_retry_outside_the_snapshot(outcome.get("race"), level, found)
        assert outcome["usable"] == 1
        assert outcome["retried_level"] == "READ COMMITTED"
        second = outcome["retried"]
    else:
        assert "race" not in outcome, outcome["race"]
        second = outcome["result"]
    assert second.id != first.id, "deduplicated against a task that had settled"
    ready = OxTask.objects.filter(status=OxTask.Status.READY)
    assert [str(task.id) for task in ready] == [second.id]
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id
    assert checks == (["40001"] if vendor == "postgresql" and snapshot else [])
    assert outcome["opened"] == (["default"] if snapshot else [])
    assert outcome["closed"]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("change", ["renewed", "claimed", "requeued"])
@pytest.mark.parametrize("level", LEVELS)
def test_a_holder_still_pending_but_changed_behind_a_snapshot(
    monkeypatch, worker, level, change
):
    """
    A holder that is still READY or RUNNING, but whose row changed after the
    caller's snapshot began, is deduplicated. Its worker renewed the lease,
    a worker claimed it, or the reaper put it back to READY.

    On PostgreSQL at REPEATABLE READ or SERIALIZABLE the share lock request
    is refused (40001), and the enqueue reads the holder and the key's lock
    once on a connection of its own. The task is still pending and the lock
    still names it, so the enqueue returns the result built from that read,
    not the snapshot's row, and closes the connection. No second task is
    queued. MySQL at REPEATABLE READ reads the committed status elsewhere and
    returns the snapshot's row, as 1.2.0 does. READ COMMITTED reads the
    current row.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and there is no snapshot")
    first = tasks.unique_echo.enqueue("held")
    seen = "READY" if change == "claimed" else "RUNNING"
    db_task = None
    if change != "claimed":
        db_task = worker.claim_one()
        assert db_task is not None and str(db_task.id) == first.id
    if change == "requeued":
        expired = timezone.now() - timedelta(hours=1)
        OxTask.objects.filter(pk=db_task.pk).update(
            locked_at=expired, lease_expires_at=expired
        )

    checks = record_holder_checks(monkeypatch)
    caller = BehindASnapshot(
        level, first.id, seen, lambda: tasks.unique_echo.enqueue("held")
    )
    if change == "renewed":
        with worker._in_flight_lock:
            worker._in_flight.add((db_task.pk, db_task.lease_epoch))
        assert worker.renew_leases() == 1
    elif change == "claimed":
        db_task = worker.claim_one()
        assert db_task is not None and str(db_task.id) == first.id
    else:
        assert worker.reap() == 1
        assert OxTask.objects.get(pk=db_task.pk).status == OxTask.Status.READY
    outcome = caller.resume()

    snapshot = level != "READ COMMITTED"
    assert "race" not in outcome, outcome["race"]
    result = outcome["result"]
    assert result.id == first.id
    if vendor == "mysql" and snapshot:
        # 1.2.0's path: the status is read elsewhere, the result is the
        # snapshot's row.
        assert result.status == seen
    else:
        committed = task_result_from_db(OxTask.objects.get(id=first.id))
        assert result_fields(result) == result_fields(committed)
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == first.id
    assert checks == (["40001"] if vendor == "postgresql" and snapshot else [])
    assert outcome["opened"] == (["default"] if snapshot else [])
    assert outcome["closed"]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_a_pending_holder_the_key_no_longer_names_is_not_deduplicated_against(
    monkeypatch, worker, level
):
    """
    The read on a connection of its own deduplicates only when the key's lock
    still names the holder.

    The caller's snapshot sees the first task READY and holding the key. The
    task then fails for good, another enqueue of the key queues a second task
    and takes the lock, and an operator retry puts the first task back to
    READY beside it, as django_ox.actions.retry may. The caller then enqueues
    the key. Its snapshot still shows the first task holding the key, and
    that row changed, so the enqueue reads again elsewhere. The first task is
    pending there, but the lock names the second task. Returning the first
    task would deduplicate against a task that no longer holds the key, so
    the enqueue raises UniqueEnqueueRace. The retry at READ COMMITTED returns
    the second task.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("MySQL reads the holder's status alone, as 1.2.0 does")
    first = tasks.unique_fail_always.enqueue()
    checks = record_holder_checks(monkeypatch)
    caller = BehindASnapshot(level, first.id, "READY", tasks.unique_fail_always.enqueue)
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=first.id).status == OxTask.Status.FAILED
    second = tasks.unique_fail_always.enqueue()
    assert second.id != first.id
    assert actions.retry(first.id) is True
    outcome = caller.resume()

    assert_retry_outside_the_snapshot(
        outcome.get("race"), level, "the key held by another task"
    )
    assert checks == ["40001"]
    assert outcome["opened"] == ["default"]
    assert outcome["closed"]
    assert outcome["retried"].id == second.id
    assert str(OxpullUniqueLock.objects.get().task_id) == second.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_fresh_read_asks_for_the_lock_of_the_key_being_enqueued(
    monkeypatch, worker, level
):
    """
    The read on a connection of its own reads the lock of the key being
    enqueued, not any lock.

    Two keys are held by two RUNNING tasks, and the other key's lock is the
    older row. The caller's snapshot sees the second task, whose lease is
    then renewed, and the caller enqueues the second key. The fresh read
    must find the second task named by its own key's lock, and the enqueue
    returns it. A read that took another key's lock would find the key held
    by another task and raise UniqueEnqueueRace.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("MySQL reads the holder's status alone, as 1.2.0 does")
    other = tasks.unique_echo.enqueue("other")
    older = timezone.now() - timedelta(hours=1)
    assert (
        OxpullUniqueLock.objects.filter(task_id=other.id).update(created_at=older) == 1
    )
    held = tasks.unique_echo.enqueue("held")
    claimed = {str(task.id): task for task in (worker.claim_one(), worker.claim_one())}
    assert set(claimed) == {other.id, held.id}
    db_task = claimed[held.id]
    checks = record_holder_checks(monkeypatch)
    caller = BehindASnapshot(
        level, held.id, "RUNNING", lambda: tasks.unique_echo.enqueue("held")
    )
    with worker._in_flight_lock:
        worker._in_flight.add((db_task.pk, db_task.lease_epoch))
    assert worker.renew_leases() == 1
    outcome = caller.resume()

    assert "race" not in outcome, outcome["race"]
    committed = task_result_from_db(OxTask.objects.get(id=held.id))
    assert result_fields(outcome["result"]) == result_fields(committed)
    assert checks == ["40001"]
    assert outcome["opened"] == ["default"]
    assert outcome["closed"]
    assert OxTask.objects.count() == 2
    named = OxpullUniqueLock.objects.values_list("task_id", flat=True)
    assert sorted(str(task_id) for task_id in named) == sorted([other.id, held.id])


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", ["options", "session", "transaction"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_holder_check_follows_the_level_however_it_was_set(monkeypatch, level, how):
    """
    A project sets PostgreSQL's level in OPTIONS["isolation_level"], which
    the driver applies as each transaction begins. A session can set its
    own, and a transaction can set its own as its first statement. The
    driver knows only the first. The holder read asks the transaction with
    current_setting('transaction_isolation'), in the same statement, so a
    holder that finished after the snapshot began raises UniqueEnqueueRace
    whichever way the level was set. Asked the driver, the other two would
    read as READ COMMITTED and the enqueue would return the finished task.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    first = tasks.unique_echo.enqueue("x")
    connections["default"].close()
    if how == "options":
        isolation_for_new_connections(monkeypatch, level)
    outcome = {}
    try:
        if how == "session":
            set_session_isolation(level)
        with transaction.atomic():
            if how == "transaction":
                set_transaction_isolation(level)
            outcome["level"] = session_isolation()
            assert OxTask.objects.get(id=first.id).status == OxTask.Status.READY
            finish_elsewhere(first.id)
            with opened_connections() as opened:
                try:
                    outcome["second"] = tasks.unique_echo.enqueue("x")
                except UniqueEnqueueRace as exc:
                    outcome["race"] = exc
            outcome["driver"] = connections["default"].connection.isolation_level
    finally:
        connections["default"].close()

    assert outcome["level"] == level
    assert (outcome["driver"] is None) is (how != "options")
    assert "second" not in outcome, "deduplicated against a task that had settled"
    assert_retry_outside_the_snapshot(outcome.get("race"), level, "the task SUCCESSFUL")
    assert [wrapper.alias for wrapper in opened] == ["default"]
    assert all(wrapper.connection is None for wrapper in opened)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("how", ["atomic", "autocommit-off"])
@pytest.mark.parametrize("level", LEVELS)
def test_a_winner_committed_behind_the_callers_snapshot_is_named_in_the_race(
    level, how
):
    """
    The known corner, and what the enqueue says about it.

    A caller's transaction reads, another enqueue of the same key commits,
    and the caller then enqueues that key. At READ COMMITTED the enqueue
    sees the winner and returns it. At MySQL's REPEATABLE READ, and at
    PostgreSQL's REPEATABLE READ or SERIALIZABLE, the lock row is read from
    the caller's snapshot, which cannot see the winner. The enqueue raises
    UniqueEnqueueRace after five passes rather than queue a second task.
    The message names the snapshot wherever the session reads one, and says
    to retry the whole transaction. The retried transaction returns the
    winner. At MySQL's SERIALIZABLE the caller's read locks, so the winner
    could not commit in between. A connection with autocommit turned off is
    the caller's transaction as surely as an atomic block, and the message
    says so there too.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite's writer waits on the open reader")
    if vendor == "mysql" and level == "SERIALIZABLE":
        pytest.skip("InnoDB SERIALIZABLE reads lock, and the winner would wait")

    snapshot_taken = threading.Event()
    won = threading.Event()
    outcome = {}

    def enqueue_behind_the_snapshot():
        try:
            set_session_isolation(level)
            with callers_transaction(how):
                assert OxpullUniqueLock.objects.count() == 0
                snapshot_taken.set()
                assert won.wait(timeout=10)
                try:
                    outcome["second"] = tasks.unique_echo.enqueue("corner")
                except UniqueEnqueueRace as exc:
                    outcome["race"] = exc
            if "race" in outcome:
                with transaction.atomic():
                    outcome["second"] = tasks.unique_echo.enqueue("corner")
        except BaseException as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
        finally:
            snapshot_taken.set()
            connections.close_all()

    thread = threading.Thread(target=enqueue_behind_the_snapshot)
    thread.start()
    assert snapshot_taken.wait(timeout=10)
    winner = tasks.unique_echo.enqueue("corner")
    won.set()
    thread.join(timeout=30)
    assert not thread.is_alive()

    assert "error" not in outcome, outcome["error"]
    assert outcome["second"].id == winner.id
    if level == "READ COMMITTED":
        assert "race" not in outcome
    else:
        message = str(outcome.get("race"))
        assert "snapshot" in message
        assert "retry the whole transaction" in message
    assert OxTask.objects.count() == 1


# Statements a duplicate of a live holder runs on the caller's connection in
# 1.2.0, as Django's query log records them: BEGIN, COMMIT and savepoints
# included. Keyed by engine, level and whether a caller's transaction is open.
STATEMENTS_IN_1_2_0 = {
    ("postgresql", False): 10,
    ("postgresql", True): 12,
    ("mysql", False): 10,
    ("mysql", True): 13,
    ("sqlite", False): 8,
    ("sqlite", True): 10,
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("ambient", [False, True], ids=["own", "ambient"])
@pytest.mark.parametrize("level", LEVELS)
def test_what_a_duplicate_of_a_live_holder_runs(monkeypatch, worker, level, ambient):
    """
    The statements a duplicate of a live, unchanged holder runs, against
    1.2.0's count.

    On PostgreSQL inside a caller's transaction the holder read also reads
    the transaction's level, in the same statement. At REPEATABLE READ and
    SERIALIZABLE three statements follow: a savepoint, the share lock
    request without waiting, and the rollback to that savepoint, which is
    never released instead. The lock is granted and the holder returned.
    At READ COMMITTED, when the enqueue owns its transactions, on MySQL and
    on SQLite the count is 1.2.0's, and the level is never asked with a
    statement of its own. No connection is opened, except MySQL's at
    REPEATABLE READ inside a caller's transaction, as in 1.2.0.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite" and level != "READ COMMITTED":
        pytest.skip("SQLite has one isolation level")
    holder = tasks.unique_echo.enqueue("held")
    db_task = worker.claim_one()
    assert db_task is not None and str(db_task.id) == holder.id
    connection = connections["default"]
    connection.close()
    if vendor != "sqlite":
        isolation_for_new_connections(monkeypatch, level)
    connection.ensure_connection()
    try:
        with (
            CaptureQueriesContext(connection) as captured,
            opened_connections() as opened,
        ):
            if ambient:
                with transaction.atomic():
                    result = tasks.unique_echo.enqueue("held")
            else:
                result = tasks.unique_echo.enqueue("held")
    finally:
        connection.close()

    assert result.id == holder.id
    assert result.status == TaskResultStatus.RUNNING
    sql = [query["sql"] for query in captured.captured_queries]
    checked = vendor == "postgresql" and ambient and level != "READ COMMITTED"
    mysql_level = vendor == "mysql" and ambient
    expected = STATEMENTS_IN_1_2_0[(vendor, ambient)] + (3 if checked else 0)
    assert len(sql) == expected, sql
    share = [index for index, statement in enumerate(sql) if "FOR SHARE" in statement]
    if checked:
        (index,) = share
        assert sql[index].endswith("FOR SHARE NOWAIT")
        assert "KEY SHARE" not in sql[index]
        assert sql[index - 1].startswith("SAVEPOINT ")
        savepoint = sql[index - 1].split()[1]
        assert sql[index + 1] == f"ROLLBACK TO SAVEPOINT {savepoint}"
        assert not any(
            statement == f"RELEASE SAVEPOINT {savepoint}" for statement in sql
        )
    else:
        assert share == []
    level_reads = [s for s in sql if "current_setting('transaction_isolation')" in s]
    assert len(level_reads) == (1 if vendor == "postgresql" and ambient else 0)
    assert not any("SHOW transaction_isolation" in statement for statement in sql)
    asked = [s for s in sql if "@@transaction_isolation" in s]
    assert len(asked) == (1 if mysql_level else 0)
    assert [wrapper.alias for wrapper in opened] == (
        ["default"] if mysql_level and level == "REPEATABLE READ" else []
    )
    assert OxpullUniqueLock.objects.count() == 1


# -- the enqueue's error paths, checked on every database ----------------


class StubCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql):
        self.connection.executed.append(sql)

    def fetchone(self):
        return (self.connection.reported,)


class StubConnection:
    """A connection that answers the level query with a fixed string."""

    def __init__(self, vendor, reported=None):
        self.vendor = vendor
        self.reported = reported
        self.executed = []
        self.in_atomic_block = False
        self.needs_rollback = False

    def cursor(self):
        return StubCursor(self)


def refused(sqlstate, attribute="sqlstate"):
    """
    An OperationalError as Django wraps a driver's, with that SQLSTATE.
    psycopg 3 carries it as sqlstate, psycopg2 as pgcode.
    """

    class Cause(Exception):
        pass

    cause = Cause()
    setattr(cause, attribute, sqlstate)
    exc = OperationalError(f"stubbed {sqlstate}")
    exc.__cause__ = cause
    return exc


def sqlstate_of(exc):
    return pg_sqlstate(exc)


@pytest.mark.parametrize("attribute", ["sqlstate", "pgcode"])
@pytest.mark.parametrize(
    ("vendor", "sqlstate", "refusal"),
    [
        ("postgresql", "40001", True),
        ("postgresql", "55P03", False),
        ("postgresql", None, False),
        ("mysql", "40001", False),
        ("sqlite", "40001", False),
    ],
)
def test_only_postgresql_reports_a_refused_changed_row(
    monkeypatch, vendor, sqlstate, refusal, attribute
):
    """
    SQLSTATE 40001 is PostgreSQL's serialization_failure, and MySQL reports
    its deadlocks under the same SQLSTATE. A deadlock keeps its own path, so
    only PostgreSQL's code is read as a refused changed row. psycopg 3 and
    psycopg2 carry the code under different names, and both are read, for
    lock_not_available as well.
    """
    monkeypatch.setattr(
        backend_module, "connections", {"default": StubConnection(vendor)}
    )
    exc = refused(sqlstate, attribute)
    assert backend_module._is_serialization_failure(exc, "default") is refusal
    waited = vendor == "postgresql" and sqlstate == "55P03"
    assert backend_module._is_lock_wait_timeout(exc, "default") is waited


@pytest.mark.parametrize("attribute", ["sqlstate", "pgcode"])
@pytest.mark.parametrize(
    ("sqlstate", "answer"),
    [
        (None, "current"),
        ("55P03", "current"),
        ("40001", "changed"),
        ("57014", "raises"),
    ],
    ids=["granted", "lock-not-available", "serialization-failure", "query-canceled"],
)
def test_the_holder_check_rolls_back_its_savepoint_whatever_it_meets(
    monkeypatch, sqlstate, answer, attribute
):
    """
    The check asks for its lock in a savepoint and rolls that savepoint back
    every time, granted or refused, and never releases it: released, the
    share lock would stay with the caller's transaction until it commits.
    Granted, or refused with lock_not_available because another transaction
    is writing or locking the row, the check reports nothing. Refused with
    serialization_failure, which says the row changed after the snapshot
    began, it returns that failure for a fresh read to decide on. Anything
    else propagates. Stubbed so it runs on every database, with the SQLSTATE
    where psycopg 3 puts it and where psycopg2 does. On Django 6.1 the
    savepoint is opened with savepoint_create(), never with the deprecated
    savepoint().
    """
    calls = []
    refusal = None if sqlstate is None else refused(sqlstate, attribute)

    def lock(task_pk, db_alias):
        calls.append(("lock", task_pk, db_alias))
        if refusal is not None:
            raise refusal

    monkeypatch.setattr(OxpullBackend, "_lock_holder_nowait", staticmethod(lock))
    opens = (
        "savepoint_create"
        if hasattr(backend_module.transaction, "savepoint_create")
        else "savepoint"
    )
    monkeypatch.setattr(
        backend_module.transaction,
        opens,
        lambda using: calls.append(("savepoint", using)) or "sid",
    )
    if opens != "savepoint":

        def deprecated(using=None):
            raise AssertionError("opened with the deprecated transaction.savepoint()")

        monkeypatch.setattr(backend_module.transaction, "savepoint", deprecated)
    monkeypatch.setattr(
        backend_module.transaction,
        "savepoint_rollback",
        lambda sid, using: calls.append(("rollback", sid, using)),
    )
    monkeypatch.setattr(
        backend_module.transaction,
        "savepoint_commit",
        lambda sid, using: calls.append(("release", sid, using)),
    )
    if answer == "raises":
        with pytest.raises(OperationalError) as excinfo:
            OxpullBackend._check_holder_row("pk", "default")
        assert excinfo.value is refusal
    elif answer == "changed":
        assert OxpullBackend._check_holder_row("pk", "default") is refusal
    else:
        assert OxpullBackend._check_holder_row("pk", "default") is None
    assert calls == [
        ("savepoint", "default"),
        ("lock", "pk", "default"),
        ("rollback", "sid", "default"),
    ]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("path", ["unchanged", "finished"])
@pytest.mark.parametrize("level", ["REPEATABLE READ", "SERIALIZABLE"])
def test_the_holder_check_raises_no_warning(monkeypatch, level, path):
    """
    A duplicate that runs the holder check raises no warning. Django 6.1
    deprecates transaction.savepoint() with RemovedInDjango70Warning, so a
    project that turns warnings into errors would fail every such duplicate.
    Warnings are errors while the enqueue runs. The holder's row is unchanged,
    so the check is granted, or the task finished, so the check is refused and
    the holder is read again on a new connection.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("the holder check is PostgreSQL's")
    holder = tasks.unique_echo.enqueue("held")
    checks = record_holder_checks(monkeypatch)
    set_session_isolation(level)
    outcome = {}
    try:
        with transaction.atomic():
            assert OxTask.objects.get(id=holder.id).status == OxTask.Status.READY
            if path == "finished":
                finish_elsewhere(holder.id)
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                try:
                    outcome["result"] = tasks.unique_echo.enqueue("held")
                except UniqueEnqueueRace as exc:
                    outcome["race"] = exc
    finally:
        connections["default"].close()

    if path == "unchanged":
        assert "race" not in outcome, outcome["race"]
        assert outcome["result"].id == holder.id
        assert checks == ["granted"]
    else:
        assert_retry_outside_the_snapshot(
            outcome.get("race"), level, "the task SUCCESSFUL"
        )
        assert checks == ["40001"]


FRESH_READ_FINDS = {
    "40001-settled": "the task SUCCESSFUL",
    "40001-gone": "the task's row gone",
    "40001-lock-gone": "the key's lock gone",
    "40001-another-task": "the key held by another task",
}

# A fresh read that fails, and the SQLSTATE it fails with.
FRESH_READ_FAILS = {
    "40001-read-lock-timeout": "55P03",
    "40001-read-statement-timeout": "57014",
    "40001-read-refused": None,
}


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "answer",
    [
        "read committed",
        "granted",
        "55P03",
        "40001-pending",
        *FRESH_READ_FINDS,
        *FRESH_READ_FAILS,
        "57014",
    ],
)
def test_what_a_duplicate_does_with_each_answer_to_the_holder_check(
    monkeypatch, answer
):
    """
    Each answer the holder check can get, through a real enqueue inside a
    caller's transaction on every database. The level the holder read
    reports and the lock request are stubbed. So is the fresh read, where it
    has to find something other than the holder as it stands, or fail.

    At READ COMMITTED the check does not run. Granted, or refused because
    another transaction is changing the row (55P03), the enqueue returns the
    holder and opens no connection. Refused because the row changed after
    the snapshot began (40001), the enqueue reads the holder and the key's
    lock once on a connection of its own. When the holder is pending and the
    lock names it, the enqueue returns the result of that read. When that
    read finds the task settled or gone, or the lock gone or naming another
    task, the enqueue raises UniqueEnqueueRace, chained to the refusal, and
    says what it found. When the read fails, by its lock timeout, its
    statement timeout or a refused connection, the enqueue raises
    UniqueEnqueueRace chained to that failure, with the failure's first line
    in the message. Any other error from the check reaches the caller as it
    was raised. Each time nothing is written, every connection opened is
    closed, and the caller's transaction still answers a query.
    """
    holder = tasks.unique_echo.enqueue("held")
    level = "read committed" if answer == "read committed" else "repeatable read"
    monkeypatch.setattr(
        OxpullBackend,
        "_isolation_level_sql",
        staticmethod(lambda db_alias: f"'{level}'"),
    )
    checked = []
    code = answer.split("-")[0]

    def lock(task_pk, db_alias):
        checked.append(str(task_pk))
        if code in ("55P03", "40001", "57014"):
            raise refused(code)

    monkeypatch.setattr(OxpullBackend, "_lock_holder_nowait", staticmethod(lock))

    failure = None
    if answer in FRESH_READ_FAILS:
        failed_with = FRESH_READ_FAILS[answer]
        failure = (
            refused(failed_with)
            if failed_with
            else OperationalError(
                'connection failed: connection to server at "127.0.0.1", port 1 '
                "failed: Connection refused\n\tIs the server running on that host?"
            )
        )

    def fresh(task_id, key, db_alias):
        if failure is not None:
            raise failure
        row = OxTask.objects.get(id=task_id)
        if answer == "40001-settled":
            row.status = OxTask.Status.SUCCESSFUL
            return row, row.pk
        if answer == "40001-gone":
            return None, None
        if answer == "40001-lock-gone":
            return row, None
        return row, uuid.uuid4()

    if answer in FRESH_READ_FINDS or answer in FRESH_READ_FAILS:
        monkeypatch.setattr(OxpullBackend, "_read_holder_fresh", staticmethod(fresh))
    with opened_connections() as opened, transaction.atomic():
        if answer in FRESH_READ_FINDS:
            with pytest.raises(UniqueEnqueueRace) as excinfo:
                tasks.unique_echo.enqueue("held")
            assert sqlstate_of(excinfo.value.__cause__) == "40001"
            message = str(excinfo.value)
            assert f"A fresh read found {FRESH_READ_FINDS[answer]}." in message
            assert "on database 'default'" in message
            assert "inside a transaction at REPEATABLE READ," in message
        elif answer in FRESH_READ_FAILS:
            with pytest.raises(UniqueEnqueueRace) as excinfo:
                tasks.unique_echo.enqueue("held")
            assert excinfo.value.__cause__ is failure
            message = str(excinfo.value)
            first_line = str(failure).splitlines()[0]
            assert f"A fresh read on a new connection failed ({first_line})." in message
            assert "Is the server running" not in message
            waited = FRESH_READ_FAILS[answer] == "55P03"
            assert ("That read waits at most 100ms for a lock." in message) is waited
            assert "on database 'default'" in message
            assert "inside a transaction at REPEATABLE READ," in message
            assert "The enqueue created nothing." in message
        elif answer == "57014":
            with pytest.raises(OperationalError) as excinfo:
                tasks.unique_echo.enqueue("held")
            assert sqlstate_of(excinfo.value) == "57014"
        else:
            result = tasks.unique_echo.enqueue("held")
            assert result.id == holder.id
            expected = task_result_from_db(OxTask.objects.get(id=holder.id))
            assert result_fields(result) == result_fields(expected)
        assert OxTask.objects.count() == 1
    assert checked == ([] if answer == "read committed" else [holder.id])
    assert [wrapper.alias for wrapper in opened] == (
        ["default"] if answer == "40001-pending" else []
    )
    assert all(wrapper.connection is None for wrapper in opened)
    assert str(OxpullUniqueLock.objects.get().task_id) == holder.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("owner", ["enqueue", "atomic", "autocommit-off"])
def test_a_refused_changed_row_is_reissued_or_raised_by_who_owns_the_transaction(
    monkeypatch, owner
):
    """
    What the enqueue does with PostgreSQL's refusal of a changed row, checked
    on every database by raising that failure in place of the first
    resolution. When the enqueue owns its transactions the failure rolled
    back only its own work, so the pass is reissued and the next one finds
    the holder. Inside a caller's transaction the snapshot is the caller's,
    so the enqueue raises UniqueEnqueueRace, chained to the failure, saying
    to retry the whole transaction, and writes nothing. That transaction is
    an atomic block, or a connection with autocommit turned off.
    """
    holder = tasks.unique_echo.enqueue("held")
    refusal = refused("40001")
    existing_result = OxpullBackend._existing_result
    calls = []

    def refuse_the_first(self, *args, **kwargs):
        calls.append(kwargs["ambient"])
        if len(calls) == 1:
            raise refusal
        return existing_result(self, *args, **kwargs)

    monkeypatch.setattr(OxpullBackend, "_existing_result", refuse_the_first)
    monkeypatch.setattr(
        backend_module,
        "_is_serialization_failure",
        lambda exc, db_alias: sqlstate_of(exc) == "40001",
    )
    if owner == "enqueue":
        assert tasks.unique_echo.enqueue("held").id == holder.id
        assert calls == [False, False]
    else:
        with (
            callers_transaction(owner),
            pytest.raises(UniqueEnqueueRace, match="new, short transaction") as excinfo,
        ):
            tasks.unique_echo.enqueue("held")
        assert excinfo.value.__cause__ is refusal
        assert calls == [True]
    assert OxTask.objects.count() == 1
    assert str(OxpullUniqueLock.objects.get().task_id) == holder.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("owner", ["enqueue", "atomic", "autocommit-off"])
def test_a_deadlock_is_reissued_only_when_the_enqueue_owns_its_transaction(
    monkeypatch, owner
):
    """
    An InnoDB deadlock rolls back the victim's whole transaction. When the
    enqueue owns that transaction nothing else was lost, and the enqueue tries
    again. Inside a caller's transaction the caller's work went with it, so the
    error reaches the caller as raised and nothing is written. A connection
    with autocommit turned off is a caller's transaction, as an atomic block
    is. Checked on every database by raising MySQL's deadlock in place of the
    first lock insert.
    """
    from django.db.models.query import QuerySet

    deadlock = OperationalError(
        1213, "Deadlock found when trying to get lock; try restarting transaction"
    )
    create = QuerySet.create
    calls = []

    def deadlock_the_first_lock_insert(self, **kwargs):
        if self.model is OxpullUniqueLock:
            calls.append(kwargs["key"])
            if len(calls) == 1:
                raise deadlock
        return create(self, **kwargs)

    monkeypatch.setattr(QuerySet, "create", deadlock_the_first_lock_insert)
    monkeypatch.setattr(
        backend_module,
        "is_innodb_deadlock",
        lambda exc, db_alias: bool(exc.args) and exc.args[0] == 1213,
    )
    if owner == "enqueue":
        result = tasks.unique_echo.enqueue("held")
        assert len(calls) == 2
        assert [str(task.id) for task in OxTask.objects.all()] == [result.id]
        assert str(OxpullUniqueLock.objects.get().task_id) == result.id
    else:
        with callers_transaction(owner), pytest.raises(OperationalError) as excinfo:
            tasks.unique_echo.enqueue("held")
        assert excinfo.value is deadlock
        assert len(calls) == 1
        assert OxTask.objects.count() == 0
        assert OxpullUniqueLock.objects.count() == 0


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("ambient", [False, True], ids=["own", "ambient"])
def test_a_refused_lock_update_is_reissued_or_raised_by_who_owns_the_transaction(
    monkeypatch, ambient
):
    """
    PostgreSQL's SERIALIZABLE can refuse the UPDATE that names the new task
    on its lock. The refusal reached the caller raw. When the enqueue owns
    its transaction it now tries again. Inside a caller's transaction it
    raises UniqueEnqueueRace, chained to the refusal, and writes nothing.
    Checked on every database by raising PostgreSQL's failure in place of
    the first UPDATE.
    """
    from django.db.models.query import QuerySet

    refusal = refused("40001")
    update = QuerySet.update
    calls = []

    def refuse_the_first_lock_update(self, **kwargs):
        if self.model is OxpullUniqueLock:
            calls.append(kwargs)
            if len(calls) == 1:
                raise refusal
        return update(self, **kwargs)

    monkeypatch.setattr(QuerySet, "update", refuse_the_first_lock_update)
    monkeypatch.setattr(
        backend_module,
        "_is_serialization_failure",
        lambda exc, db_alias: sqlstate_of(exc) == "40001",
    )
    if ambient:
        with (
            transaction.atomic(),
            pytest.raises(UniqueEnqueueRace, match="new, short transaction") as excinfo,
        ):
            tasks.unique_echo.enqueue("held")
        assert excinfo.value.__cause__ is refusal
        assert len(calls) == 1
        assert OxTask.objects.count() == 0
        assert OxpullUniqueLock.objects.count() == 0
    else:
        result = tasks.unique_echo.enqueue("held")
        assert len(calls) == 2
        assert [str(task.id) for task in OxTask.objects.all()] == [result.id]
        assert str(OxpullUniqueLock.objects.get().task_id) == result.id


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "released", [False, True], ids=["first-pass", "after-a-release"]
)
@pytest.mark.parametrize("ambient", [False, True], ids=["own", "ambient"])
def test_a_serialization_failure_outside_the_resolution_propagates_raw(
    monkeypatch, ambient, released
):
    """
    Only a refusal met after the task is written, or while resolving a
    duplicate, is reissued or reported as UniqueEnqueueRace. PostgreSQL's
    SERIALIZABLE can also fail the task write itself with SQLSTATE 40001.
    That failure says nothing about the holder or the lock, so it reaches
    the caller once, as any other enqueue at that level raises it, and
    nothing is written. The same holds on the pass after one that released
    a settled holder's lock. Checked on every database.
    """
    if released:
        settled = tasks.unique_echo.enqueue("held")
        OxTask.objects.filter(id=settled.id).update(status=OxTask.Status.FAILED)

    refusal = refused("40001")
    calls = []

    def refuse(self, task, args, kwargs):
        calls.append(task.module_path)
        raise refusal

    monkeypatch.setattr(OxBackend, "enqueue", refuse)
    monkeypatch.setattr(
        backend_module,
        "_is_serialization_failure",
        lambda exc, db_alias: sqlstate_of(exc) == "40001",
    )
    with (
        transaction.atomic() if ambient else contextlib.nullcontext(),
        pytest.raises(OperationalError) as excinfo,
    ):
        tasks.unique_echo.enqueue("held")
    assert excinfo.value is refusal
    assert calls == ["tests_oxpull.tasks.unique_echo"]
    assert OxTask.objects.count() == (1 if released else 0)
    assert OxpullUniqueLock.objects.count() == 0


SNAPSHOT_NAMES = [
    ("mysql", "REPEATABLE-READ", "MySQL's REPEATABLE READ"),
    ("postgresql", "repeatable read", "PostgreSQL's REPEATABLE READ"),
    ("postgresql", "serializable", "PostgreSQL's SERIALIZABLE"),
    ("mysql", "READ-COMMITTED", None),
    ("mysql", "SERIALIZABLE", None),
    ("postgresql", "read committed", None),
    ("sqlite", None, None),
]


@pytest.mark.parametrize(("vendor", "reported", "names"), SNAPSHOT_NAMES)
def test_the_race_message_names_a_snapshot_only_where_the_session_reads_one(
    monkeypatch, vendor, reported, names
):
    """
    After five passes the message names the likely cause when the enqueue
    ran inside a transaction whose session reads from a snapshot. That was
    said for MySQL alone, and for a MySQL session at any level. At READ
    COMMITTED and at MySQL's SERIALIZABLE the lock read is current, so it is
    not the cause there. Outside a transaction the level is not asked.
    """
    connection = StubConnection(vendor, reported)
    monkeypatch.setattr(backend_module, "connections", {"default": connection})
    inside = default_task_backend._race_message(
        tasks.unique_echo, "default", ambient=True
    )
    asked = list(connection.executed)
    outside = default_task_backend._race_message(
        tasks.unique_echo, "default", ambient=False
    )

    assert inside.startswith(outside)
    assert "after 5 attempts." in outside
    assert "snapshot" not in outside
    assert connection.executed == asked
    assert len(asked) == (0 if vendor == "sqlite" else 1)
    if names is None:
        assert inside == outside
    else:
        assert names in inside
        assert "snapshot" in inside
        assert "retry the whole transaction" in inside


@pytest.mark.parametrize(("vendor", "reported", "names"), SNAPSHOT_NAMES)
def test_the_lock_wait_message_names_a_snapshot_only_where_the_session_reads_one(
    monkeypatch, vendor, reported, names
):
    """
    Inside a caller's transaction the lock-wait message says to retry the
    whole transaction. It gave MySQL's REPEATABLE READ as the reason on every
    database and at every level. It now names a snapshot only where the
    session reads one, as the race message does. Outside a transaction the
    level is not asked.
    """
    connection = StubConnection(vendor, reported)
    monkeypatch.setattr(backend_module, "connections", {"default": connection})
    exc = OperationalError("stopped waiting")
    exc.__cause__ = Exception("stopped waiting")
    outside = default_task_backend._lock_wait_message(
        tasks.unique_echo, exc, "default", ambient_atomic=False
    )
    assert connection.executed == []
    inside = default_task_backend._lock_wait_message(
        tasks.unique_echo, exc, "default", ambient_atomic=True
    )

    assert inside.startswith(outside)
    assert "whole transaction" not in outside
    assert "whole transaction" in inside
    if names is None:
        assert "snapshot" not in inside
        assert "REPEATABLE READ" not in inside
    else:
        assert names in inside
        assert "snapshot" in inside


def test_oxpull_w001_fires_for_a_unique_task_and_not_for_the_prune_task():
    from django_ox.maintenance import prune  # noqa: F401

    def warnings_for(path):
        backend = OxpullBackend(
            "default",
            {
                "OPTIONS": {
                    "QUEUES": ["default", "maintenance"],
                    "SCHEDULES": {"nightly": {"task": path, "cron": "0 3 * * *"}},
                }
            },
        )
        return [message.id for message in backend.check()]

    assert "oxpull.W001" in warnings_for("tests_oxpull.tasks.unique_echo")
    assert "oxpull.W001" not in warnings_for("django_ox.maintenance.prune")
