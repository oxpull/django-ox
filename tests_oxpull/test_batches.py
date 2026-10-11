"""
Batch creation, membership, sealing, progress and reconciliation.

The load-bearing tests are the ones that prove state is derived, not
counted: a member the reaper settles emits no signal at all and the batch
must still complete, and a member whose task row was pruned or whose
attempt went unobserved must read as finished without being called a
success or a failure.
"""

import copy
import dataclasses
import logging
import pickle
import subprocess
import sys
import threading
import time
import warnings
from contextlib import contextmanager
from datetime import timedelta

import pytest
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.core.management import call_command
from django.db import IntegrityError, OperationalError, connections, transaction
from django.db.models import F
from django.tasks import TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished
from django.test import override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import actions
from django_ox.models import OxTask
from django_ox.results import public_status
from django_ox.schedules import schedules_from_options
from oxpull import batches
from oxpull import unique as unique_module
from oxpull.backend import OxpullBackend
from oxpull.exceptions import BatchMemberExists, BatchSealed
from oxpull.models import OxpullBatch, OxpullBatchMember
from oxpull.routing import oxpull_db_alias

from . import isolation, tasks
from .racing import THREADS, race, race_all

pytestmark = pytest.mark.django_db

RECONCILE_SCHEDULE = {
    "SCHEDULES": {
        "oxpull-batch-reconcile": {
            "task": "oxpull.batches.reconcile",
            "cron": "* * * * *",
        }
    }
}


def reconcile():
    """Run one reconciler pass inline, as a scheduled worker would."""
    return batches.reconcile.call()


def stored(batch):
    return OxpullBatch.objects.get(pk=batch.id)


# -- shape and configuration -------------------------------------------


def test_create_returns_an_open_batch():
    batch = batches.create("nightly-invoices")
    assert batch.name == "nightly-invoices"
    assert batch.status == OxpullBatch.Status.OPEN
    assert stored(batch).total_count is None
    assert stored(batch).failed_count is None


def test_get_returns_the_same_batch():
    batch = batches.create("fetch")
    assert batches.get(batch.id).id == batch.id
    assert batches.get(str(batch.id)).name == "fetch"


def test_get_raises_for_an_unknown_batch():
    with pytest.raises(OxpullBatch.DoesNotExist):
        batches.get("6c1b3f52-0000-4000-8000-000000000000")


def test_over_long_name_is_rejected_identically_on_every_database():
    with pytest.raises(ValueError, match="128 characters"):
        batches.create("x" * 129)


def test_callback_that_does_not_import_fails_at_create():
    with pytest.raises(ImproperlyConfigured):
        batches.create("bad", on_complete="tests_oxpull.tasks.nope")


def test_callback_that_is_not_a_task_fails_at_create():
    with pytest.raises(ImproperlyConfigured):
        batches.create("bad", on_success="tests_oxpull.tasks.STATE")


def test_callback_arguments_are_stored_normalised():
    batch = batches.create(
        "args",
        on_complete=tasks.record_callback,
        on_complete_args=("done",),
    )
    row = stored(batch)
    assert row.on_complete_path == "tests_oxpull.tasks.record_callback"
    assert row.on_complete_args == ["done"]
    assert row.on_complete_kwargs == {}


def test_reconcile_is_schedulable_from_backend_options():
    schedules = schedules_from_options(RECONCILE_SCHEDULE, "default")
    assert [schedule.name for schedule in schedules] == ["oxpull-batch-reconcile"]
    assert schedules[0].task.module_path == "oxpull.batches.reconcile"


# A project whose QUEUES omits "default": the reconciler Task must still
# be constructible, because constructing it is what importing
# oxpull.batches used to do.
QUEUES_WITHOUT_DEFAULT = """
import django
from django.conf import settings

settings.configure(
    INSTALLED_APPS=["django_ox", "oxpull"],
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    TASKS={
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["emails", "reports"],
        }
    },
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
)
django.setup()

from django_ox.schedules import schedules_from_options

import oxpull.batches  # must not raise

def queue_for(config):
    options = {"SCHEDULES": {"oxpull-batch-reconcile": config}}
    return schedules_from_options(options, "default")[0].task.queue_name

print(oxpull.batches.reconcile.queue_name)
print(queue_for({"task": "oxpull.batches.reconcile", "cron": "* * * * *"}))
print(
    queue_for(
        {
            "task": "oxpull.batches.reconcile",
            "cron": "* * * * *",
            "queue_name": "reports",
        }
    )
)
"""


def test_reconcile_is_importable_when_queues_omit_default():
    """
    Repro in a fresh interpreter, because the failure was at import
    time. A module-level @task is validated against its backend the
    moment it is built, so `import oxpull.batches` raised InvalidTask
    ("Queue 'default' is not valid for backend") in any project whose
    QUEUES omits "default" -- naming neither oxpull nor batches, and
    before the schedule's own queue_name could apply.
    """
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", QUEUES_WITHOUT_DEFAULT],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    # The first configured queue by default, and the schedule may route it.
    assert result.stdout.split() == ["emails", "emails", "reports"]


def test_members_must_be_enqueued_on_a_django_ox_backend():
    batch = batches.create("immediate")
    with pytest.raises(ImproperlyConfigured, match="django-ox backend"):
        batch.add(tasks.immediate_echo, "x")
    assert OxpullBatchMember.objects.count() == 0


def test_callbacks_must_be_enqueued_on_a_django_ox_backend():
    # The guard add() applies to members applies to callbacks too, and
    # for a sharper reason: a backend that runs the function at enqueue
    # time would run it inside the completing transaction and hand back
    # an id that is not a UUID.
    with pytest.raises(ImproperlyConfigured, match="django-ox backend"):
        batches.create("immediate-callback", on_complete=tasks.immediate_echo)
    with pytest.raises(ImproperlyConfigured, match="django-ox backend"):
        batches.create(
            "immediate-callback", on_success="tests_oxpull.tasks.immediate_echo"
        )
    assert OxpullBatch.objects.count() == 0


def test_a_stored_callback_on_a_non_ox_backend_never_runs(worker):
    """
    Repro for the reconciler loop. A callback path that only became
    non-django-ox after create() (a later deploy moved the task onto an
    immediate backend) must be refused before it is enqueued: an
    immediate backend executes the function inline and returns a
    non-UUID id, so recording it raises, the completion rolls back, and
    the side effect has already happened. Without the guard in
    _enqueue_callback each tick repeats it and the batch never leaves
    SEALED.
    """
    batch = batches.create(
        "deployed-away",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    batch.seal()
    OxpullBatch.objects.filter(pk=batch.id).update(
        on_complete_path="tests_oxpull.tasks.immediate_record"
    )

    for _ in range(3):
        assert reconcile() == 0

    assert stored(batch).status == OxpullBatch.Status.SEALED
    assert stored(batch).callback_task_id is None
    assert "complete" not in tasks.STATE  # the side effect never fired


def test_callback_using_overrides_are_rejected_at_create():
    # Only the dotted path is stored, so an override passed here would
    # be dropped in silence and the callback would land on a queue the
    # operator is not draining.
    with pytest.raises(ImproperlyConfigured, match="queue_name"):
        batches.create(
            "override", on_complete=tasks.record_callback.using(queue_name="emails")
        )
    assert OxpullBatch.objects.count() == 0


def test_a_unique_callback_is_refused_at_create():
    """
    A unique task coalesces while one with the same arguments is
    pending, so two batches sharing it as on_complete would point at one
    callback task and one of them would never see its callback run.
    Refused where the batch is configured, naming the task.
    """
    with pytest.raises(ImproperlyConfigured, match="unique") as caught:
        batches.create("one", on_complete=tasks.unique_echo, on_complete_args=["same"])
    assert "'tests_oxpull.tasks.unique_echo'" in str(caught.value)
    assert OxpullBatch.objects.count() == 0


def test_a_unique_on_success_callback_is_refused_at_create():
    with pytest.raises(ImproperlyConfigured, match="unique") as caught:
        batches.create("one", on_success=tasks.unique_echo, on_success_args=["same"])
    assert "'tests_oxpull.tasks.unique_echo'" in str(caught.value)


def test_a_callback_made_unique_through_options_is_refused_at_create():
    with pytest.raises(ImproperlyConfigured, match="unique") as caught:
        batches.create("one", on_complete=tasks.configured_unique)
    assert "'tests_oxpull.tasks.configured_unique'" in str(caught.value)


def test_a_callback_made_unique_after_create_is_refused_at_completion(
    monkeypatch, caplog
):
    """
    The row outlives the deploy that wrote it. A callback that was an
    ordinary task at create() and is unique by the time the batch
    completes must not be enqueued: the batch stays SEALED and the
    reconciler says which task and why, every tick, until the deploy is
    fixed.
    """
    batch = batches.create(
        "later", on_complete=tasks.record_callback, on_complete_args=["c"]
    )
    batch.seal()
    monkeypatch.setattr(
        unique_module,
        "_registry",
        unique_module._registry | {"tests_oxpull.tasks.record_callback"},
    )
    with caplog.at_level(logging.ERROR, logger="oxpull.batches"):
        assert reconcile() == 0
    row = stored(batch)
    assert row.status == OxpullBatch.Status.SEALED
    assert row.callback_task_id is None
    assert not OxTask.objects.filter(
        task_path="tests_oxpull.tasks.record_callback"
    ).exists()
    assert "'tests_oxpull.tasks.record_callback'" in caplog.text
    assert "unique" in caplog.text


def test_callback_arguments_must_be_a_list_and_a_dict():
    # list() on a bare string would silently explode it into characters.
    with pytest.raises(ImproperlyConfigured, match="args must be a list"):
        batches.create(
            "strargs",
            on_complete=tasks.record_callback,
            on_complete_args="admin@example.com",
        )
    with pytest.raises(ImproperlyConfigured, match="kwargs must be a dict"):
        batches.create(
            "strkwargs",
            on_success=tasks.record_callback,
            on_success_kwargs=[("label", "x")],
        )
    assert OxpullBatch.objects.count() == 0


# -- happy path --------------------------------------------------------


def test_seal_then_complete_reports_progress_at_each_stage(worker):
    batch = batches.create(
        "nightly",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    batch.add(tasks.plain_echo, 1)
    batch.add(tasks.plain_echo, 2)

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 2, 0)

    batch.seal()
    assert batch.status == OxpullBatch.Status.SEALED
    assert stored(batch).total_count == 2

    assert worker.run_once() is True
    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 1, 1)

    # A batch never completes past a member that has not run.
    assert reconcile() == 0
    assert batch.status == OxpullBatch.Status.SEALED

    assert worker.run_once() is True
    assert reconcile() == 1

    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.failed_count == 0
    assert row.completed_at is not None
    assert OxTask.objects.filter(id=row.callback_task_id).exists()
    assert OxTask.objects.filter(id=row.success_task_id).exists()

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 0, 2)
    assert progress.unattributed == 0


def test_callbacks_are_enqueued_with_their_stored_arguments(worker):
    batch = batches.create(
        "callback-args",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    batch.seal()
    assert reconcile() == 1

    callback = OxTask.objects.get(id=stored(batch).callback_task_id)
    assert callback.task_path == "tests_oxpull.tasks.record_callback"
    assert callback.args == ["complete"]

    # An ordinary task: a worker runs it like any other.
    assert worker.run_once() is True
    assert tasks.STATE["complete"] == 1


def test_zero_member_batch_completes_vacuously_successful():
    batch = batches.create(
        "empty",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    batch.seal()
    assert stored(batch).total_count == 0

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.failed_count == 0
    assert row.callback_task_id is not None
    assert row.success_task_id is not None
    assert batch.progress().total == 0


def test_batch_without_callbacks_still_completes(worker):
    batch = batches.create("no-callbacks")
    batch.add(tasks.plain_echo, "x")
    batch.seal()
    assert worker.run_once() is True

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is None
    assert row.success_task_id is None
    assert OxTask.objects.count() == 1


def test_reconciler_is_idempotent(worker):
    batch = batches.create(
        "idempotent", on_complete=tasks.record_callback, on_complete_args=["complete"]
    )
    batch.add(tasks.plain_echo, "x")
    batch.seal()
    assert worker.run_once() is True

    assert reconcile() == 1
    callback_id = stored(batch).callback_task_id
    assert reconcile() == 0
    assert stored(batch).callback_task_id == callback_id
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 1
    )


def test_reconcile_runs_as_an_ordinary_task(worker):
    batch = batches.create("scheduled")
    batch.add(tasks.plain_echo, "x")
    batch.seal()
    assert worker.run_once() is True  # the member

    result = batches.reconcile.enqueue()
    assert worker.run_once() is True  # the reconciler itself
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 1
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


# -- sealing -----------------------------------------------------------


def test_add_after_seal_raises_and_enqueues_nothing():
    batch = batches.create("sealed")
    batch.seal()

    with pytest.raises(BatchSealed):
        batch.add(tasks.plain_echo, "late")

    assert OxTask.objects.count() == 0
    assert OxpullBatchMember.objects.count() == 0
    assert stored(batch).total_count == 0


def test_add_after_completion_raises():
    batch = batches.create("done")
    batch.seal()
    assert reconcile() == 1

    with pytest.raises(BatchSealed):
        batch.add(tasks.plain_echo, "late")


def test_seal_twice_raises():
    batch = batches.create("twice")
    batch.seal()
    sealed_at = stored(batch).sealed_at

    with pytest.raises(BatchSealed):
        batch.seal()
    assert stored(batch).sealed_at == sealed_at


def test_unsealed_batch_never_completes(worker):
    batch = batches.create("never-sealed")
    batch.add(tasks.plain_echo, "x")
    assert worker.run_once() is True

    assert reconcile() == 0
    assert stored(batch).status == OxpullBatch.Status.OPEN


# -- transactions ------------------------------------------------------


def test_rollback_discards_task_and_membership_together():
    batch = batches.create("rollback")
    with pytest.raises(ValidationError), transaction.atomic():
        batch.add(tasks.plain_echo, "x")
        raise ValidationError("abort")

    assert OxTask.objects.count() == 0
    assert OxpullBatchMember.objects.count() == 0
    batch.seal()
    assert stored(batch).total_count == 0


@pytest.mark.django_db(transaction=True)
def test_a_failed_membership_insert_leaves_no_orphan_task(monkeypatch):
    """
    add()'s own transaction, exercised in real autocommit.

    Under the ordinary django_db fixture every test already runs inside
    a transaction, so a rollback test there passes whether or not add()
    opens one of its own: the fixture supplies the rollback. This one
    runs with transaction=True and forces the membership INSERT to fail,
    so it fails if add()'s atomic block is ever dropped or narrowed --
    the enqueued task would commit on its own and become a task no batch
    waits on and no operator can see.
    """

    def refuse_to_save(*args, **kwargs):
        raise IntegrityError("membership insert refused")

    monkeypatch.setattr(OxpullBatchMember, "save", refuse_to_save)

    batch = batches.create("orphan")
    with pytest.raises(BatchMemberExists):
        batch.add(tasks.plain_echo, "x")
    monkeypatch.undo()

    assert OxTask.objects.count() == 0
    assert OxpullBatchMember.objects.count() == 0


def deadlock_once(monkeypatch):
    """
    Make the first membership INSERT the victim of an InnoDB deadlock.

    A real 1213 needs two transactions to close a lock cycle inside
    add(), and the only cycle the code once had (batch row before unique
    lock) is the one its statement order removed; none has been reached
    by racing. So the error is raised from the membership INSERT with
    MySQL's own code and text. The one difference from the real thing is
    that InnoDB has already rolled the whole transaction back by the
    time 1213 reaches the client; here add()'s atomic block does the
    rollback on the way out, and the end state is the same.
    """
    real_save = OxpullBatchMember.save
    calls = []

    def save_or_deadlock(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OperationalError(
                1213,
                "Deadlock found when trying to get lock; try restarting transaction",
            )
        return real_save(self, *args, **kwargs)

    monkeypatch.setattr(OxpullBatchMember, "save", save_or_deadlock)
    return calls


@pytest.mark.django_db(transaction=True)
def test_add_reissues_its_own_transaction_after_a_deadlock(monkeypatch):
    """
    add() owns its transaction when the caller has not opened one, so a
    deadlock that rolls that transaction back left nothing behind and
    add() can reissue it: enqueue again, claim the batch row again,
    insert the membership again. The enqueue of a unique member runs
    inside add()'s transaction and so cannot reissue on its own.

    On MySQL the retry lands and the batch has one member for one task.
    On the other databases the code is not a deadlock code and the error
    propagates as before, with the rolled-back transaction leaving no
    orphan task and no membership.
    """
    calls = deadlock_once(monkeypatch)
    batch = batches.create("victim")

    if connections["default"].vendor == "mysql":
        result = batch.add(tasks.unique_echo, "k")
        assert len(calls) == 2
        assert OxTask.objects.count() == 1
        member = OxpullBatchMember.objects.get()
        assert str(member.task_id) == result.id
        assert member.batch_id == batch.id
    else:
        with pytest.raises(OperationalError):
            batch.add(tasks.unique_echo, "k")
        assert len(calls) == 1
        assert OxTask.objects.count() == 0
        assert OxpullBatchMember.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test_add_inside_a_callers_transaction_leaves_the_deadlock_to_the_caller(
    monkeypatch,
):
    """
    Inside a caller's atomic block a deadlock has rolled back the
    caller's work too, and only the caller can decide to redo that. add()
    propagates the error after one attempt on every database and the
    caller retries its whole transaction, as documented.
    """
    calls = deadlock_once(monkeypatch)
    batch = batches.create("ambient")

    with pytest.raises(OperationalError), transaction.atomic():
        batch.add(tasks.unique_echo, "k")
    assert len(calls) == 1
    assert OxTask.objects.count() == 0
    assert OxpullBatchMember.objects.count() == 0


class SimulatedDriverError(Exception):
    """The psycopg error a PostgreSQL OperationalError carries as its cause."""


def conflict_once(monkeypatch, code):
    """
    Make the first membership INSERT lose a conflict with this database's own
    error code: an InnoDB code on MySQL, a SQLSTATE on PostgreSQL, set where
    Django's error wrapper leaves it. The server would already have rolled the
    transaction back, and add()'s atomic block does that on the way out.
    """
    real_save = OxpullBatchMember.save
    calls = []

    def save_or_lose(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            if connections["default"].vendor == "mysql":
                raise OperationalError(code, "simulated")
            cause = SimulatedDriverError("simulated")
            cause.sqlstate = code
            error = OperationalError("simulated")
            error.__cause__ = cause
            raise error
        return real_save(self, *args, **kwargs)

    monkeypatch.setattr(OxpullBatchMember, "save", save_or_lose)
    return calls


@pytest.mark.django_db(transaction=True)
def test_add_reissues_its_own_transaction_after_a_postgresql_deadlock(monkeypatch):
    """
    PostgreSQL's deadlock detector (40P01) rolls back the whole transaction of
    the victim, as InnoDB's 1213 does. add() that owns its transaction runs it
    again, and the batch has one member for one task.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("40P01 is PostgreSQL's deadlock code")
    calls = conflict_once(monkeypatch, "40P01")
    batch = batches.create("pg-victim")

    result = batch.add(tasks.unique_echo, "k")

    assert len(calls) == 2
    assert OxTask.objects.count() == 1
    member = OxpullBatchMember.objects.get()
    assert str(member.task_id) == result.id


@pytest.mark.django_db(transaction=True)
def test_add_in_a_transaction_opened_by_turning_autocommit_off_is_not_run_again(
    monkeypatch,
):
    """
    A caller that turned autocommit off opened the transaction, even with no
    atomic block open. A conflict rolled back the caller's work too, so add()
    runs once and the error reaches the caller, as inside an atomic block.
    """
    vendor = connections["default"].vendor
    if vendor == "sqlite":
        pytest.skip("SQLite raises neither conflict")
    code = {"postgresql": "40001", "mysql": 1213}[vendor]
    calls = conflict_once(monkeypatch, code)
    batch = batches.create("autocommit-off")
    connection = connections["default"]

    connection.set_autocommit(False)
    try:
        with pytest.raises(OperationalError):
            batch.add(tasks.unique_echo, "k")
    finally:
        connection.rollback()
        connection.set_autocommit(True)

    assert len(calls) == 1
    assert OxTask.objects.count() == 0
    assert OxpullBatchMember.objects.count() == 0


def test_one_poisoned_batch_does_not_abort_the_sweep(monkeypatch, worker):
    """
    "One poisoned batch cannot starve the sweep" needs two batches to
    mean anything: with one, skipping it and abandoning the pass look
    identical. The first batch's callback explodes; the second must
    still complete in the same pass.
    """
    poisoned = batches.create("poisoned", on_complete=tasks.plain_fail)
    poisoned.seal()
    healthy = batches.create(
        "healthy", on_complete=tasks.record_callback, on_complete_args=["complete"]
    )
    healthy.seal()

    real_enqueue = batches._enqueue_callback

    def explode_for_the_first(path, args, kwargs):
        if path == "tests_oxpull.tasks.plain_fail":
            raise RuntimeError("callback enqueue failed")
        return real_enqueue(path, args, kwargs)

    monkeypatch.setattr(batches, "_enqueue_callback", explode_for_the_first)
    assert reconcile() == 1
    monkeypatch.undo()

    assert stored(poisoned).status == OxpullBatch.Status.SEALED
    assert stored(healthy).status == OxpullBatch.Status.COMPLETE
    assert stored(healthy).callback_task_id is not None


def test_callback_enqueue_is_atomic_with_the_compare_and_set(monkeypatch, worker):
    batch = batches.create(
        "atomic", on_complete=tasks.record_callback, on_complete_args=["complete"]
    )
    batch.add(tasks.plain_echo, "x")
    batch.seal()
    assert worker.run_once() is True

    def explode(*args, **kwargs):
        raise RuntimeError("callback enqueue failed")

    monkeypatch.setattr(batches, "_enqueue_callback", explode)
    # The pass logs and skips the batch; one poisoned batch must not
    # abort the sweep.
    assert reconcile() == 0

    row = stored(batch)
    assert row.status == OxpullBatch.Status.SEALED
    assert row.completed_at is None
    assert row.failed_count is None
    assert OxTask.objects.count() == 1  # the member only; no callback row

    monkeypatch.undo()
    assert reconcile() == 1
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


# -- failure, retry and death ------------------------------------------


def test_batch_waits_for_a_retrying_member(worker):
    batch = batches.create("retrying")
    result = batch.add(tasks.plain_flaky, 2)
    batch.seal()

    assert worker.run_once() is True  # attempt 1 fails, back to READY
    row = OxTask.objects.get(id=result.id)
    assert row.status == OxTask.Status.READY
    assert row.attempts == 1

    assert reconcile() == 0
    assert batch.progress().pending == 1

    assert worker.run_once() is True  # attempt 2 succeeds
    assert reconcile() == 1
    assert stored(batch).failed_count == 0


def test_running_member_holds_the_batch_open():
    batch = batches.create("running")
    result = batch.add(tasks.plain_echo, "x")
    batch.seal()
    OxTask.objects.filter(id=result.id).update(
        status=OxTask.Status.RUNNING, locked_by="worker", locked_at=timezone.now()
    )

    assert reconcile() == 0
    assert batch.progress().pending == 1


def test_failed_member_completes_the_batch_without_on_success(worker):
    batch = batches.create(
        "failing",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    result = batch.add(tasks.plain_fail)
    batch.seal()
    for _ in range(3):  # MAX_ATTEMPTS in tests_oxpull/settings.py
        assert worker.run_once() is True
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.FAILED

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.failed_count == 1
    assert row.callback_task_id is not None
    assert row.success_task_id is None

    progress = batch.progress()
    assert (progress.total, progress.failed, progress.succeeded) == (1, 1, 0)


def test_member_reaped_to_lost_still_completes_the_batch(worker):
    """
    The marquee case. A dead worker's task is reclaimed by the reaper,
    which settles it through a queryset update and emits no signal at all
    (django_ox worker.reap). A batch that counted task_finished would hang
    here forever; this one asks the rows.

    The reaper writes LOST rather than FAILED, having no evidence either
    way about work it never saw finish. django.tasks carries no such value,
    so the public status still reads FAILED and callers see what they
    always saw. What changes for a batch is attribution: an unobserved
    member is no longer counted a failure, so failed_count is 0 and the
    member falls to unattributed instead. on_success stays withheld under
    either reading, which is the property an operator actually relies on.
    """
    batch = batches.create(
        "reaped",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    result = batch.add(tasks.plain_echo, "x")
    batch.seal()
    OxTask.objects.filter(id=result.id).update(
        status=OxTask.Status.RUNNING,
        attempts=3,
        max_attempts=3,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    signals = []

    def receiver(sender, task_result, **kwargs):
        signals.append(task_result.id)

    task_finished.connect(receiver)
    try:
        assert worker.reap() == 1
    finally:
        task_finished.disconnect(receiver)

    assert signals == []  # the reaper is silent; that is the whole point
    lost = OxTask.objects.get(id=result.id)
    assert lost.status == OxTask.Status.LOST
    # Unchanged for anyone reading through django.tasks.
    assert public_status(lost.status) == TaskResultStatus.FAILED

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is not None  # on_complete fires
    assert row.success_task_id is None  # on_success does not
    assert row.failed_count == 0
    assert batch.progress().unattributed == 1


def test_member_reaped_with_attempts_remaining_keeps_the_batch_open(worker):
    batch = batches.create("reclaimed")
    result = batch.add(tasks.plain_echo, "x")
    batch.seal()
    OxTask.objects.filter(id=result.id).update(
        status=OxTask.Status.RUNNING,
        attempts=1,
        locked_by="dead-worker",
        locked_at=timezone.now() - timedelta(seconds=9999),
    )

    assert worker.reap() == 1
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.READY

    assert reconcile() == 0
    assert batch.progress().pending == 1


def test_pruned_member_counts_as_finished(worker):
    batch = batches.create("pruned")
    first = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    batch.seal()

    # ox_prune deletes finished task rows outright; the membership row
    # then joins to nothing and must read as finished, not as lost.
    OxTask.objects.filter(id=first.id).delete()
    assert worker.run_once() is True  # the surviving member

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 0, 1)
    assert progress.unattributed == 1
    # The deprecated alias still reads the same number off real rows.
    with pytest.warns(DeprecationWarning, match="BatchProgress.unattributed"):
        assert progress.pruned == 1

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    # Attribution degrades to "finished": a pruned member cannot be
    # counted as a failure.
    assert row.failed_count == 0


def test_pruned_member_withholds_on_success(worker):
    """
    Repro: `ox_prune --include-failed` deletes a FAILED member out from
    under a live batch, and the surviving rows then read as zero
    failures. on_success is the operator's "the run was clean" signal,
    so it must not fire on a run whose outcome is no longer knowable.
    """
    batch = batches.create(
        "pruned-failure",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    failing = batch.add(tasks.plain_fail)
    batch.add(tasks.plain_echo, "b")
    batch.seal()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=failing.id).status == OxTask.Status.FAILED

    # Age the failed row past the cutoff and run the real prune command.
    OxTask.objects.filter(id=failing.id).update(
        finished_at=timezone.now() - timedelta(hours=2)
    )
    call_command("ox_prune", "--older-than", "1h", "--include-failed")
    assert not OxTask.objects.filter(id=failing.id).exists()

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is not None  # on_complete still fires
    assert row.success_task_id is None  # on_success does not
    assert batch.progress().unattributed == 1


PRUNED_DEPRECATION = (
    "BatchProgress.pruned is deprecated and will be removed in oxpull 2.0. "
    "Use BatchProgress.unattributed, which returns the same number."
)


def test_pruned_warns_names_unattributed_and_points_at_the_caller():
    """
    pruned is the old name for unattributed, and it undersells the
    number: LOST and discarded members land there too. Reading it warns
    once, names the replacement and the release that removes it, and
    blames the line that read it rather than oxpull, so the warning is
    actionable where it is shown.
    """
    progress = batches.BatchProgress(total=5, pending=1, succeeded=1, failed=1)

    with pytest.warns(DeprecationWarning) as caught:
        reading_line = sys._getframe().f_lineno + 1
        value = progress.pruned

    assert value == progress.unattributed == 2
    assert len(caught) == 1
    assert caught[0].category is DeprecationWarning
    assert str(caught[0].message) == PRUNED_DEPRECATION
    assert (caught[0].filename, caught[0].lineno) == (__file__, reading_line)


def test_unattributed_and_the_dataclass_protocol_do_not_warn():
    """
    The warning sits on the one deprecated read. The replacement, the
    other derived property and everything the dataclass does with its
    fields stay silent, so equality, hashing, repr, asdict, replace,
    copying and pickling a progress reading behave as they did.
    """
    progress = batches.BatchProgress(total=5, pending=1, succeeded=1, failed=1)
    same = batches.BatchProgress(total=5, pending=1, succeeded=1, failed=1)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert progress.unattributed == 2
        assert progress.finished == 4
        assert progress == same
        assert hash(progress) == hash(same)
        assert repr(progress) == (
            "BatchProgress(total=5, pending=1, succeeded=1, failed=1)"
        )
        assert dataclasses.asdict(progress) == {
            "total": 5,
            "pending": 1,
            "succeeded": 1,
            "failed": 1,
        }
        assert dataclasses.replace(progress, pending=0) == batches.BatchProgress(
            total=5, pending=0, succeeded=1, failed=1
        )
        assert copy.deepcopy(progress) == progress
        assert pickle.loads(pickle.dumps(progress)) == progress  # noqa: S301


def test_callback_failure_leaves_the_batch_complete(worker):
    batch = batches.create("bad-callback", on_complete=tasks.plain_fail)
    batch.seal()
    assert reconcile() == 1
    callback_id = stored(batch).callback_task_id

    for _ in range(3):
        assert worker.run_once() is True
    assert OxTask.objects.get(id=callback_id).status == OxTask.Status.FAILED
    # Batches do not supervise their callbacks.
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


# -- unique members ----------------------------------------------------


def test_deduplicated_unique_task_can_join_two_batches():
    first = batches.create("one")
    second = batches.create("two")
    a = first.add(tasks.unique_echo, "x")
    b = second.add(tasks.unique_echo, "x")

    assert a.id == b.id
    assert OxTask.objects.count() == 1
    assert OxpullBatchMember.objects.count() == 2


def test_adding_one_deduplicated_task_twice_to_a_batch_raises():
    batch = batches.create("dedup")
    batch.add(tasks.unique_echo, "x")

    with pytest.raises(BatchMemberExists):
        batch.add(tasks.unique_echo, "x")

    assert OxTask.objects.count() == 1
    assert OxpullBatchMember.objects.count() == 1
    # The caller's connection survives the constraint violation.
    batch.seal()
    assert stored(batch).total_count == 1


# -- routing -----------------------------------------------------------


def test_split_router_refuses_batch_operations():
    with (
        override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.SplitTaskRouter"]),
        pytest.raises(ImproperlyConfigured),
    ):
        batches.create("split")


@pytest.mark.django_db(databases=["default", "replica"])
def test_replica_read_router_cannot_complete_a_pending_batch():
    # The completion join must never run against a lagged replica: an
    # empty replica would report nothing pending and complete the batch
    # while its member is still queued. Every read is pinned to the
    # write database.
    batch = batches.create("pinned")
    batch.add(tasks.plain_echo, "x")
    batch.seal()

    with override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.ReplicaReadRouter"]):
        assert reconcile() == 0
        assert batch.progress().pending == 1

    assert stored(batch).status == OxpullBatch.Status.SEALED


@pytest.mark.django_db(transaction=True)
def test_progress_on_an_open_batch_never_invents_a_finished_member(monkeypatch):
    """
    Repro: progress() on an OPEN batch reads the status counts and the
    membership denominator in two statements. With the denominator read
    second, an add() committing between them raises total without
    raising pending, and the derived finished and unattributed report
    members that neither finished nor were pruned.
    """
    batch = batches.create("torn")
    batch.add(tasks.plain_echo, "a")

    real_counts = batches._member_task_counts
    raced = []

    def counts_then_a_concurrent_add(*args, **kwargs):
        result = real_counts(*args, **kwargs)
        if not raced:
            raced.append(True)
            batch.add(tasks.plain_echo, "b")
        return result

    monkeypatch.setattr(batches, "_member_task_counts", counts_then_a_concurrent_add)
    progress = batch.progress()
    monkeypatch.undo()

    assert raced == [True]
    assert progress.unattributed == 0
    assert progress.finished == 0
    assert progress.total == progress.pending
    # The batch itself is unharmed: the settled reading is both members.
    assert batch.progress() == batches.BatchProgress(
        total=2, pending=2, succeeded=0, failed=0
    )


# -- width -------------------------------------------------------------


def statements_during(call):
    """Every statement one call executes, as (sql, bound parameter count)."""
    alias = oxpull_db_alias()
    seen = []

    def spy(execute, sql, params, many, context):
        seen.append((sql, len(params or ())))
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(spy):
        call()
    return seen


def test_no_batch_query_binds_one_parameter_per_member():
    """
    The completion join filters task rows with `id__in=<membership
    queryset>`, and that has to stay a SQL subquery rather than becoming
    one bound parameter per member.

    A version that inlined the ids would fail on exactly the batches
    batches exist for, and fail outright rather than slowly: SQLite
    refuses a statement with more than SQLITE_MAX_VARIABLE_NUMBER
    parameters and PostgreSQL more than 65,535, so a nightly run of
    10,000 invoices would raise instead of completing. Historical width
    measurements found the parameter count stayed at 1 from 1,000 to
    100,000 members on both databases. This test asserts it at two widths:
    the property that matters is that the count does not move with
    membership, not that it happens to be small in one reading.
    """

    def sealed_batch_of(members):
        batch = batches.create(f"width-{members}")
        for index in range(members):
            batch.add(tasks.plain_echo, index)
        batch.seal()
        return batch

    def completion_query(batch):
        statements = statements_during(batch.progress)
        grouped = [(sql, count) for sql, count in statements if "GROUP BY" in sql]
        assert len(grouped) == 1, statements
        return grouped[0], max(count for _, count in statements)

    (narrow_sql, narrow_params), narrow_widest = completion_query(sealed_batch_of(2))
    (wide_sql, wide_params), wide_widest = completion_query(sealed_batch_of(60))

    # The member ids never leave the database.
    assert "IN (SELECT" in " ".join(narrow_sql.split())
    assert "IN (SELECT" in " ".join(wide_sql.split())
    # One parameter, the batch id, at either width -- and no other
    # statement on the path grows either.
    assert narrow_params == wide_params == 1
    assert narrow_widest == wide_widest


# -- racing reconcilers ------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_racing_reconcilers_fire_the_callbacks_once(worker):
    """
    Exactly-once callbacks come from the compare-and-set rowcount, the
    same class of mechanism as the schedule-tick unique constraint. Every
    thread sees a finished batch; only the one whose UPDATE matches a
    SEALED row may enqueue.
    """
    batch = batches.create(
        "raced",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    batch.add(tasks.plain_echo, "x")
    batch.seal()
    assert worker.run_once() is True

    results, errors = race(reconcile)

    assert errors == []
    assert len(results) == THREADS
    assert sorted(results) == [0] * (THREADS - 1) + [1]

    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert (
        OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback").count()
        == 2
    )
    assert {str(row.callback_task_id), str(row.success_task_id)} == {
        str(task_id)
        for task_id in OxTask.objects.filter(
            task_path="tests_oxpull.tasks.record_callback"
        ).values_list("id", flat=True)
    }


@pytest.mark.django_db(transaction=True)
def test_add_does_not_deadlock_a_caller_that_already_holds_a_unique_lock():
    """
    Repro for a lock-order inversion. add() must not hold the batch row
    lock across enqueue(): a unique member's enqueue takes the
    OxpullUniqueLock row for its key, so batch row -> unique lock is the
    library's order, while a caller that enqueues a unique task and then
    calls add() inside one transaction holds unique lock -> batch row.
    That closes a cycle, and PostgreSQL aborts one side with a deadlock
    (MySQL with ER_LOCK_DEADLOCK, which add() reissues only when it owns
    the transaction, and the caller here does). add() enqueues before it
    claims the batch row, so both sides take the two locks in the same
    order.
    """
    batch = batches.create("lock-order")
    holds_the_key = threading.Event()
    member_add_started = threading.Event()

    def enqueue_then_add():
        with transaction.atomic():
            tasks.unique_echo.enqueue("k")  # holds the unique-lock row
            holds_the_key.set()
            assert member_add_started.wait(timeout=10)
            # Let the other thread reach the batch row first; under the
            # inverted order it is holding it and waiting on this
            # transaction's unique lock.
            time.sleep(0.5)
            batch.add(tasks.plain_echo, "x")
        return "caller"

    def add_the_unique_member():
        assert holds_the_key.wait(timeout=10)
        member_add_started.set()
        batch.add(tasks.unique_echo, "k")
        return "member"

    results, errors = race_all([enqueue_then_add, add_the_unique_member])

    assert errors == []
    assert sorted(results) == ["caller", "member"]
    assert OxpullBatchMember.objects.filter(batch_id=batch.id).count() == 2


@pytest.mark.django_db(transaction=True)
def test_racing_add_and_seal_never_strands_a_member_outside_the_count():
    """
    seal() freezes membership, so total_count must equal the members that
    actually committed. add() takes the batch row lock before it enqueues
    anything, so it either commits before seal() counts or finds the
    batch already SEALED; a plain status read would let a member land
    after the snapshot and make total_count a lie.
    """
    batch = batches.create("sealing")

    def add_or_refuse():
        try:
            batch.add(tasks.plain_echo, "x")
        except BatchSealed:
            return "refused"
        return "added"

    def seal():
        batch.seal()
        return "sealed"

    results, errors = race_all([seal, *[add_or_refuse] * (THREADS - 1)])

    assert errors == []
    added = results.count("added")
    row = stored(batch)
    assert row.status == OxpullBatch.Status.SEALED
    assert OxpullBatchMember.objects.count() == added
    assert OxTask.objects.count() == added
    assert row.total_count == added


def test_lost_member_withholds_on_success(worker):
    """
    Repro: the reaper writes LOST when a claim ages out with no attempts
    left, meaning the attempt was never observed. That is an unknown
    outcome rather than a clean one, and the straggler holding the lease
    may still resolve the row to FAILED afterwards. on_success is the
    operator's "the run was clean" signal, so a batch holding an
    unobserved member must not fire it.

    The status is written directly rather than driven through the reaper:
    what is under test is how completion reads a LOST row, not how the
    reaper comes to write one.
    """
    batch = batches.create(
        "lost-member",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    lost = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    batch.seal()
    while worker.run_once():
        pass

    OxTask.objects.filter(id=lost.id).update(status=OxTask.Status.LOST)

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is not None  # on_complete still fires
    assert row.success_task_id is None, (
        "on_success fired on a batch holding a member whose outcome was "
        "never observed; a straggler can still resolve that row to FAILED."
    )
    assert row.failed_count == 0
    assert batch.progress().unattributed == 1


def test_lost_member_counts_as_finished(worker):
    """
    A LOST row is settled, so it must not hold a batch open indefinitely.

    The reaper deliberately leaves lease_epoch alone when it writes LOST,
    so the straggler can still resolve the row. Waiting for one that never
    returns would mean a batch that never completes and fires no callback
    at all, which is worse than completing with the outcome marked
    unknown.
    """
    batch = batches.create("lost-finished")
    lost = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    batch.seal()
    while worker.run_once():
        pass
    OxTask.objects.filter(id=lost.id).update(status=OxTask.Status.LOST)

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 0, 1)
    assert progress.unattributed == 1
    assert reconcile() == 1
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    ("member", "args", "outcome"),
    [
        (tasks.plain_echo, ["x"], OxTask.Status.SUCCESSFUL),
        (tasks.plain_fail, [], OxTask.Status.FAILED),
    ],
    ids=["successful", "failed"],
)
def test_a_straggler_resolving_a_lost_member_leaves_the_completed_batch_alone(
    worker, member, args, outcome
):
    """
    A LOST member completes the batch, and the execution that lost it
    writing its outcome afterwards changes nothing the batch decided.

    Driven through a real claim, the reaper and the straggler's own
    execute(), rather than by writing statuses: the straggler holds the
    lease epoch its claim handed out, which is what lets its write land
    on the LOST row. While the lease has only expired the member is
    RUNNING and holds the batch open. Once the reaper writes LOST the
    member is finished and unattributed, so on_complete is enqueued,
    on_success is withheld and failed_count is 0. The straggler then
    writes SUCCESSFUL or FAILED onto that row. The next tick leaves the
    COMPLETE batch as it was, with no callback enqueued again and
    failed_count unchanged. Only progress(), which reads the rows, shows
    the new outcome.
    """
    batch = batches.create(
        "straggler",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    result = batch.add(member, *args)
    batch.seal()
    OxTask.objects.filter(id=result.id).update(attempts=F("max_attempts") - 1)
    straggler = worker.claim_one()
    assert straggler is not None and str(straggler.id) == result.id
    assert straggler.attempts == straggler.max_attempts
    expired = timezone.now() - timedelta(seconds=9999)
    OxTask.objects.filter(id=result.id).update(
        locked_at=expired, lease_expires_at=expired
    )

    assert reconcile() == 0
    assert batch.progress().pending == 1

    assert worker.reap() == 1
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.LOST
    assert reconcile() == 1
    completed = stored(batch)
    assert completed.status == OxpullBatch.Status.COMPLETE
    assert completed.callback_task_id is not None
    assert completed.success_task_id is None
    assert completed.failed_count == 0
    callbacks = OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback")
    enqueued = [str(completed.callback_task_id)]
    assert [str(pk) for pk in callbacks.values_list("id", flat=True)] == enqueued

    worker.execute(straggler)
    assert OxTask.objects.get(id=result.id).status == outcome

    assert reconcile() == 0
    after = stored(batch)
    assert after.status == OxpullBatch.Status.COMPLETE
    assert after.completed_at == completed.completed_at
    assert after.failed_count == 0
    assert after.callback_task_id == completed.callback_task_id
    assert after.success_task_id is None
    assert [str(pk) for pk in callbacks.values_list("id", flat=True)] == enqueued
    progress = batch.progress()
    assert progress.unattributed == 0
    succeeded = outcome == OxTask.Status.SUCCESSFUL
    assert (progress.succeeded, progress.failed) == (int(succeeded), int(not succeeded))


# -- django-ox 0.3.x operator actions -----------------------------------


def test_discarded_member_withholds_on_success(worker):
    """
    Repro against django-ox 0.3.1: django_ox.actions.discard closes a
    READY, FAILED or LOST task without running it, and 0.2.1 knew nothing
    about that status. A DISCARDED member fell through every bucket, so
    the completing transaction counted it as attributed and enqueued
    on_success: the batch reported a clean run over work an operator had
    just cancelled. progress() already read it the other way, so the two
    paths disagreed about the same batch.

    on_complete still fires. The batch is complete; it was not clean.
    """
    batch = batches.create(
        "discarded-member",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    doomed = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    assert actions.discard(doomed.id) is True
    batch.seal()
    while worker.run_once():
        pass

    assert OxTask.objects.get(id=doomed.id).status == OxTask.Status.DISCARDED
    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is not None  # on_complete still fires
    assert row.success_task_id is None, (
        "on_success fired on a batch holding a member an operator "
        "discarded; the batch is complete, but the work was cancelled "
        "rather than done."
    )
    assert row.failed_count == 0  # discarded is not failed


def test_discarded_member_counts_as_finished_and_unattributed(worker):
    """
    DISCARDED is terminal and never claimed, so it must not hold a batch
    open. It is not a success and not a recorded failure either, which is
    the same standing as a pruned or LOST member, so it lands in the
    unattributed shortfall. progress() and the completing transaction
    have to agree on that number.
    """
    batch = batches.create("discarded-finished")
    doomed = batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    assert actions.discard(doomed.id) is True
    batch.seal()
    while worker.run_once():
        pass

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.succeeded) == (2, 0, 1)
    assert (progress.failed, progress.unattributed) == (0, 1)
    assert reconcile() == 1
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


def test_discard_many_of_every_member_still_completes_the_batch(worker):
    """
    A batch an operator cancelled outright completes with no member
    attributed. on_complete fires, so the operator's own cleanup still
    runs; on_success does not.
    """
    batch = batches.create(
        "cancelled-run",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    batch.add(tasks.plain_echo, "a")
    batch.add(tasks.plain_echo, "b")
    batch.seal()

    changed, skipped = actions.discard_many(OxTask.objects.all())
    assert (changed, skipped) == (2, 0)

    assert reconcile() == 1
    row = stored(batch)
    assert row.status == OxpullBatch.Status.COMPLETE
    assert row.callback_task_id is not None
    assert row.success_task_id is None
    assert batch.progress().unattributed == 2


def test_retried_member_holds_the_batch_open(worker):
    """
    django_ox.actions.retry puts a FAILED member back on the queue, and a
    sealed batch is frozen in membership, not in time. The retry is a
    pending member again, so the batch stays SEALED and no callback
    fires until it settles.
    """
    batch = batches.create(
        "retried-member",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    failing = batch.add(tasks.plain_fail)
    batch.add(tasks.plain_echo, "b")
    batch.seal()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=failing.id).status == OxTask.Status.FAILED

    assert actions.retry(failing.id) is True
    assert OxTask.objects.get(id=failing.id).status == OxTask.Status.READY

    assert batch.progress().pending == 1
    assert reconcile() == 0
    assert stored(batch).status == OxpullBatch.Status.SEALED
    assert stored(batch).callback_task_id is None

    while worker.run_once():
        pass
    assert reconcile() == 1
    assert stored(batch).failed_count == 1


def test_retry_landing_inside_the_completing_transaction_is_not_lost(
    worker, monkeypatch
):
    """
    The narrow window django-ox 0.3.0 opened. The reconciler reads the
    member counts, then opens a transaction and compare-and-sets the
    batch to COMPLETE. Until 0.3.0 that gap was safe, because nothing
    moved a settled row back to READY. actions.retry does exactly that,
    so a retry landing in the gap used to complete the batch and fire
    its callbacks over a member queued to run again.

    The retry is driven from inside the first count, which is the gap
    itself: the counts the reconciler carries into the transaction are
    then stale in precisely the way a real retry makes them stale.
    """
    batch = batches.create(
        "retry-in-the-gap",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
    )
    failing = batch.add(tasks.plain_fail)
    batch.seal()
    while worker.run_once():
        pass
    assert OxTask.objects.get(id=failing.id).status == OxTask.Status.FAILED

    real_counts = batches._member_task_counts
    calls = []

    def counts_then_retry(batch_pk, db_alias):
        result = real_counts(batch_pk, db_alias)
        calls.append(result)
        if len(calls) == 1:
            # The gap: after the filter has read "nothing pending", before
            # the compare-and-set.
            assert actions.retry(failing.id) is True
        return result

    monkeypatch.setattr(batches, "_member_task_counts", counts_then_retry)

    assert batches.reconcile.call() == 0
    row = stored(batch)
    assert row.status == OxpullBatch.Status.SEALED, (
        "the batch completed while a retried member was READY; the "
        "compare-and-set has to be re-checked under its own row lock."
    )
    assert row.completed_at is None
    assert row.failed_count is None
    assert row.callback_task_id is None

    monkeypatch.undo()
    while worker.run_once():
        pass
    assert reconcile() == 1
    assert stored(batch).status == OxpullBatch.Status.COMPLETE


# -- the status contract with django-ox --------------------------------


def test_the_reconciler_knows_every_django_ox_status():
    """
    Finished-ness is recognised positively, from an explicit set of
    settled statuses, so a status this package has never seen holds a
    batch open rather than completing it and attributing an outcome to
    it. That is only safe while the sets here cover everything django-ox
    can write, and this is the test that fails when a django-ox release
    adds a seventh status.
    """
    pending = set(batches.PENDING_STATUSES)
    settled = set(batches.SETTLED_STATUSES)
    assert pending & settled == set()
    assert pending | settled == set(OxTask.Status)
    assert set(batches.UNATTRIBUTED_STATUSES) <= settled


def test_a_status_the_reconciler_does_not_know_holds_the_batch_open():
    """
    A member whose row carries a status this package cannot read must
    not count as finished, and must not be attributed to on_success. It
    stays pending, and the batch stays SEALED, until a release that knows
    the status reads it.
    """
    batch = batches.create(
        "unknown-status",
        on_complete=tasks.record_callback,
        on_complete_args=["complete"],
        on_success=tasks.record_callback,
        on_success_args=["success"],
    )
    member = batch.add(tasks.plain_echo, "a")
    batch.seal()
    assert OxTask.objects.filter(pk=member.id).update(status="ZOMBIE") == 1

    progress = batch.progress()
    assert (progress.total, progress.pending, progress.finished) == (1, 1, 0)
    assert reconcile() == 0
    row = stored(batch)
    assert row.status == OxpullBatch.Status.SEALED
    assert row.callback_task_id is None
    assert row.success_task_id is None


# -- seal: a count, and a lock only where a snapshot could lie ----------


@contextmanager
def session_isolation(level):
    """
    This connection's isolation level, MySQL only, for the block.

    Django's MySQL backend sets READ COMMITTED on every connection it
    opens; a project that sets OPTIONS["isolation_level"] to "repeatable
    read", or to None for the server's default, runs under REPEATABLE
    READ, and that is the level whose snapshot the tests below are about.
    The connection is closed afterwards so the next use reopens it at the
    configured level.
    """
    connection = connections["default"]
    if connection.vendor != "mysql":
        yield
        return
    with connection.cursor() as cursor:
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
    try:
        yield
    finally:
        connection.close()


def a_session_waits_on_this_transaction(timeout=20.0):
    """
    Whether another session is seen waiting on a lock this thread's open
    transaction holds, within the timeout. PostgreSQL only.

    Asked from inside that transaction, so of pg_locks and pg_blocking_pids,
    which read the lock manager as it stands and say who blocks whom.
    PostgreSQL documents pg_stat_activity as collected once per transaction,
    so a poll from here does not rely on it.
    """
    connection = connections["default"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM pg_locks WHERE NOT granted "
                "AND pg_backend_pid() = ANY(pg_blocking_pids(pid))"
            )
            (count,) = cursor.fetchone()
        if count:
            return True
        time.sleep(0.02)
    return False


def member_reads(captured):
    table = OxpullBatchMember._meta.db_table
    return [
        query["sql"] for query in captured.captured_queries if table in query["sql"]
    ]


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("ambient", [False, True], ids=["own", "ambient"])
def test_seal_locks_members_only_inside_a_caller_transaction(ambient):
    """
    When seal() owns its transaction the status UPDATE is its first
    statement and serialises against every in-flight add(), so the count
    that follows is current on every database as a plain COUNT; the row
    lock per member it used to take was paid for nothing, 28 s at a
    million members on MySQL. Inside a caller's transaction the count
    stays a locking read: under MySQL REPEATABLE READ a plain COUNT is
    answered from the read view the caller's earlier statements fixed.
    """
    batch = batches.create("shape")
    batch.add(tasks.plain_echo, "a")
    connection = connections[oxpull_db_alias()]
    with CaptureQueriesContext(connection) as captured:
        if ambient:
            with transaction.atomic():
                batch.seal()
        else:
            batch.seal()
    (count_sql,) = member_reads(captured)
    locking = ambient and connection.features.has_select_for_update
    assert ("FOR UPDATE" in count_sql) is locking
    assert ("COUNT(" in count_sql.upper()) is not locking
    assert stored(batch).total_count == 1


@pytest.mark.django_db(transaction=True)
def test_ambient_seal_counts_a_member_committed_after_the_callers_snapshot():
    """
    The case the locking read exists for. Under MySQL REPEATABLE READ the
    caller's transaction fixes its read view at its first consistent
    read, and an add() that commits after that is invisible to a plain
    COUNT even though seal()'s UPDATE waited on nothing: the add() had
    already committed. Counting with a locking read is a current read
    and finds the member. PostgreSQL's READ COMMITTED would find it
    either way; the test runs there to show the promise is the same.
    """
    if connections["default"].vendor == "sqlite":
        pytest.skip(
            "a reading SQLite transaction holds its shared lock, so the "
            "concurrent add() could not commit under it"
        )
    batch = batches.create("snapshot")

    def add_and_commit():
        batch.add(tasks.plain_echo, "late")
        return "added"

    with session_isolation("REPEATABLE READ"), transaction.atomic():
        # The caller's first consistent read; the read view starts here.
        assert OxpullBatchMember.objects.filter(batch_id=batch.id).count() == 0
        results, errors = race_all([add_and_commit])
        assert (results, errors) == (["added"], [])
        batch.seal()

    assert stored(batch).total_count == 1


@pytest.mark.django_db(transaction=True)
def test_seal_owning_its_transaction_counts_the_member_whose_add_it_waited_for():
    """
    Why the plain COUNT is current when seal() owns its transaction, even
    under MySQL REPEATABLE READ: the status UPDATE is the transaction's
    first statement and waits on an add() still holding the batch row,
    and the COUNT after it is the transaction's first consistent read,
    so its read view begins after that add() committed.
    """
    batch = batches.create("waited")
    added = threading.Event()

    def add_slowly():
        with transaction.atomic():
            batch.add(tasks.plain_echo, "slow")
            added.set()
            # Hold the batch row across seal()'s arrival.
            time.sleep(1.0)
        return "added"

    def seal_after_the_add_started():
        assert added.wait(timeout=10)
        with session_isolation("REPEATABLE READ"):
            batch.seal()
        return "sealed"

    results, errors = race_all([add_slowly, seal_after_the_add_started])

    assert errors == []
    assert sorted(results) == ["added", "sealed"]
    assert stored(batch).total_count == 1


@pytest.mark.django_db(transaction=True)
def test_an_add_arriving_between_seals_statements_waits_and_is_refused(monkeypatch):
    """
    The interleaving a count without locks was feared to open: an add()
    committing after seal()'s status UPDATE returned and before its
    COUNT ran. It cannot happen, because add() claims the batch row that
    the UPDATE holds until seal() commits. Proven by starting the add()
    from inside the count, watching it block for a second, and reading
    its refusal once seal() has committed.
    """
    batch = batches.create("between")
    batch.add(tasks.plain_echo, "before")
    add_finished = threading.Event()
    outcome = []

    def late_add():
        try:
            batch.add(tasks.plain_echo, "late")
            outcome.append("added")
        except BatchSealed:
            outcome.append("refused")
        finally:
            add_finished.set()
            connections.close_all()

    original = batches.Batch._count_members

    def count_after_starting_an_add(self, db_alias, **kwargs):
        threading.Thread(target=late_add).start()
        assert not add_finished.wait(timeout=1.0), "add() did not wait for seal()"
        return original(self, db_alias, **kwargs)

    monkeypatch.setattr(batches.Batch, "_count_members", count_after_starting_an_add)
    batch.seal()

    assert add_finished.wait(timeout=10)
    assert outcome == ["refused"]
    assert stored(batch).total_count == 1
    assert OxpullBatchMember.objects.filter(batch_id=batch.id).count() == 1
    assert OxTask.objects.count() == 1


# -- add(): the OPEN claim without MySQL's CLIENT_FOUND_ROWS -------------


@contextmanager
def without_found_rows(monkeypatch):
    """
    A MySQL connection whose UPDATE rowcount is rows changed, not rows
    matched. Django's backend asks for CLIENT_FOUND_ROWS on every
    connection; a project that sets OPTIONS["client_flag"] replaces it.
    The connection is closed on the way in so the next statement
    reconnects without the flag, and on the way out so the next test
    reconnects with it.
    """
    connection = connections["default"]
    if connection.vendor != "mysql":
        pytest.skip("CLIENT_FOUND_ROWS is a MySQL connection flag")
    monkeypatch.setitem(connection.settings_dict["OPTIONS"], "client_flag", 0)
    connection.close()
    try:
        yield
    finally:
        connection.close()


@pytest.mark.django_db(transaction=True)
def test_add_does_not_depend_on_the_mysql_found_rows_flag(monkeypatch):
    """
    add() serialises against seal() with a no-op UPDATE under the OPEN
    predicate and read its rowcount as "the batch is OPEN". On MySQL
    that rowcount counts matched rows only because of CLIENT_FOUND_ROWS;
    without the flag a no-op UPDATE reports the rows it changed, none,
    and every add() on an OPEN batch raised BatchSealed saying membership
    was frozen. A zero rowcount now sends add() to read the status under
    the row lock instead, and only a status other than OPEN refuses.
    """
    with without_found_rows(monkeypatch):
        batch = batches.create("no-found-rows")
        result = batch.add(tasks.plain_echo, "x")
        assert OxpullBatchMember.objects.filter(
            batch_id=batch.id, task_id=result.id
        ).exists()
        batch.seal()
        with pytest.raises(BatchSealed, match="SEALED"):
            batch.add(tasks.plain_echo, "y")
    assert stored(batch).total_count == 1
    assert OxTask.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_add_without_the_flag_reads_the_status_it_locked_not_its_snapshot(
    monkeypatch,
):
    """
    The fallback read has to be a locking read. Inside a caller's
    transaction under REPEATABLE READ, a plain read of the batch's status
    answers from the caller's snapshot, which can still say OPEN after a
    concurrent seal() committed; the no-op UPDATE saw the current row and
    matched nothing, and a snapshot read would then admit a member into a
    sealed batch. Reading under the lock sees the seal and refuses.
    """
    with without_found_rows(monkeypatch):
        batch = batches.create("stale-snapshot")

        def seal_and_commit():
            batch.seal()
            return "sealed"

        with session_isolation("REPEATABLE READ"), transaction.atomic():
            # The caller's snapshot: the batch is OPEN in it from here on.
            assert batch.status == OxpullBatch.Status.OPEN
            assert race_all([seal_and_commit]) == (["sealed"], [])
            with pytest.raises(BatchSealed, match="SEALED"):
                batch.add(tasks.plain_echo, "late")

    assert OxpullBatchMember.objects.filter(batch_id=batch.id).count() == 0
    assert OxTask.objects.count() == 0
    assert stored(batch).total_count == 0


# -- the reconciler schedule is required, not assumed --------------------


def tasks_without_the_reconciler(**moved_to):
    """
    settings.TASKS with every reconciler schedule removed, and optionally
    the schedule put back on another alias (moved_to={"limited": True}).
    """
    rebuilt = {}
    for alias, params in settings.TASKS.items():
        options = {
            key: value
            for key, value in params.get("OPTIONS", {}).items()
            if key != "SCHEDULES"
        }
        if moved_to.get(alias):
            options.update(RECONCILE_SCHEDULE)
        rebuilt[alias] = {**params, "OPTIONS": options}
    return rebuilt


def check_ids(alias):
    return [
        message.id
        for message in OxpullBackend(alias, settings.TASKS[alias]).check()
        if message.id == "oxpull.W002"
    ]


def test_check_warns_once_when_no_backend_schedules_the_reconciler():
    """
    Batches complete only through oxpull.batches.reconcile on a django-ox
    schedule. Nothing checked that any backend declared one; a project
    that copied the uniqueness and rate-limit settings and missed the
    SCHEDULES block had batches that sat SEALED forever, with no error
    and no log line. The check is emitted once, from the first
    OxpullBackend alias in sort order, so it can be silenced by one id.
    """
    with override_settings(TASKS=tasks_without_the_reconciler()):
        messages = [
            message
            for message in OxpullBackend("default", settings.TASKS["default"]).check()
            if message.id == "oxpull.W002"
        ]
        assert len(messages) == 1
        assert "oxpull.batches.reconcile" in messages[0].msg
        assert "oxpull.batches.reconcile" in messages[0].hint
        # Once, not per alias: "limited" is an OxpullBackend too.
        assert check_ids("limited") == []
    assert check_ids("default") == []


def test_a_reconciler_schedule_on_any_django_ox_backend_satisfies_the_check():
    # The schedule rebinds the task to the backend it is declared on, so
    # any django-ox backend's worker can run the reconciler.
    with override_settings(TASKS=tasks_without_the_reconciler(limited=True)):
        assert check_ids("default") == []
        assert check_ids("limited") == []


# django-ox 1.2.0 lets a project keep its schedules as admin-editable rows
# instead of settings entries. These cover the shape that has no SCHEDULES
# block at all. It counts only on an engine that reads the rows, so the
# ones that assert it counts run with that engine in place.


@pytest.fixture
def engine_reads_schedule_rows(monkeypatch):
    """
    An installed django-ox that turns SCHEDULE_SOURCE into a source a
    worker dispatches from.

    The loader function is put where the probe looks for it, so the probe
    itself runs rather than being replaced by an answer.
    """
    import django_ox.schedules

    monkeypatch.setattr(
        django_ox.schedules,
        "schedule_source_from_options",
        lambda options, alias: None,
        raising=False,
    )
    assert batches._engine_reads_schedule_rows()


def tasks_with_a_stored_source(
    *, expose=True, source="django_ox.stored.DatabaseScheduleSource"
):
    """
    settings.TASKS with no SCHEDULES anywhere, "default" reading its
    schedules from rows, and the reconciler exposed as schedulable so a
    row is allowed to name it.
    """
    rebuilt = tasks_without_the_reconciler()
    options = dict(rebuilt["default"]["OPTIONS"])
    options["SCHEDULE_SOURCE"] = source
    if expose:
        options["SCHEDULABLE_TASKS"] = {
            "oxpull-batch-reconcile": "oxpull.batches.reconcile"
        }
    rebuilt["default"] = {**rebuilt["default"], "OPTIONS": options}
    return rebuilt


def test_a_stored_schedule_source_satisfies_the_reconciler_requirement(
    engine_reads_schedule_rows,
):
    """
    A backend that reads its schedules from rows can schedule the
    reconciler without a SCHEDULES entry, and does: the row dispatches
    and the batch completes. The requirement counted settings entries
    only, so this configuration -- the one django-ox 1.2.0 invites -- was
    refused out of create() and seal() on a path that had worked.
    """
    with override_settings(TASKS=tasks_with_a_stored_source()):
        assert batches.reconcile_schedule_aliases() == ["default"]
        assert check_ids("default") == []
        batch = batches.create("stored-source")
        batch.add(tasks.plain_echo, "x")
        batch.seal()
    assert stored(batch).status == OxpullBatch.Status.SEALED


def test_a_stored_source_that_cannot_run_the_reconciler_is_still_refused(
    engine_reads_schedule_rows,
):
    """
    SCHEDULE_SOURCE alone proves only that rows are read, not that any
    row may name the reconciler. Without the reconciler exposed to the
    schedulable registry no row can, so the refusal stands -- and it
    names the step that is missing rather than the settings block the
    project deliberately left behind.
    """
    with override_settings(TASKS=tasks_with_a_stored_source(expose=False)):
        assert batches.reconcile_schedule_aliases() == []
        with pytest.raises(ImproperlyConfigured) as caught:
            batches.create("orphan")
        assert "SCHEDULABLE_TASKS" in str(caught.value)
        assert "SCHEDULES" not in str(caught.value)
        messages = [
            message
            for message in OxpullBackend("default", settings.TASKS["default"]).check()
            if message.id == "oxpull.W002"
        ]
        assert len(messages) == 1
        assert "SCHEDULABLE_TASKS" in messages[0].hint
        assert "SCHEDULES" not in messages[0].hint


def test_the_settings_block_is_still_the_remedy_when_nothing_reads_rows(
    engine_reads_schedule_rows,
):
    """
    A project with neither channel is the case the refusal was built for,
    and it keeps the hint it has always had.
    """
    with override_settings(TASKS=tasks_without_the_reconciler()):
        with pytest.raises(ImproperlyConfigured) as caught:
            batches.create("orphan")
        assert "SCHEDULES" in str(caught.value)
        assert "SCHEDULABLE_TASKS" not in str(caught.value)


def test_an_unusable_schedule_source_does_not_count(engine_reads_schedule_rows):
    """
    The option has to name something. django-ox reports a source it
    cannot build as django_ox.E006 and the worker refuses to start, so
    the value is not validated twice here; an empty or non-string one
    names nothing at all and must not satisfy the requirement.
    """
    for source in ("", True, None):
        with override_settings(TASKS=tasks_with_a_stored_source(source=source)):
            assert batches.reconcile_schedule_aliases() == []


def test_the_reconciler_exposed_on_one_alias_serves_a_source_on_another(
    engine_reads_schedule_rows,
):
    """
    The schedulable registry is process-wide and rows are not bound to an
    alias, so the backend that reads the rows and the backend whose
    OPTIONS expose the reconciler need not be the same one.
    """
    rebuilt = tasks_without_the_reconciler()
    rebuilt["default"] = {
        **rebuilt["default"],
        "OPTIONS": {
            **rebuilt["default"]["OPTIONS"],
            "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
        },
    }
    rebuilt["limited"] = {
        **rebuilt["limited"],
        "OPTIONS": {
            **rebuilt["limited"]["OPTIONS"],
            "SCHEDULABLE_TASKS": {"oxpull-batch-reconcile": "oxpull.batches.reconcile"},
        },
    }
    with override_settings(TASKS=rebuilt):
        assert batches.reconcile_schedule_aliases() == ["default"]


def test_a_schedulable_entry_in_the_long_form_counts_too(engine_reads_schedule_rows):
    """SCHEDULABLE_TASKS takes a mapping as well as a bare dotted path."""
    rebuilt = tasks_with_a_stored_source()
    rebuilt["default"]["OPTIONS"]["SCHEDULABLE_TASKS"] = {
        "oxpull-batch-reconcile": {
            "task": "oxpull.batches.reconcile",
            "permission": "oxpull.run_reconcile",
        }
    }
    with override_settings(TASKS=rebuilt):
        assert batches.reconcile_schedule_aliases() == ["default"]


def _engine_that_cannot_read_rows(monkeypatch):
    """
    Stand in for a django-ox older than the one this release pins, forced
    in against the pin. The gate asks the installed engine for its loader
    rather than for a version number, so the engine is simulated here too;
    depending on whatever django-ox happens to be installed made these tests
    true under one pin and false under the next.
    """
    import django_ox.schedules

    monkeypatch.delattr(
        django_ox.schedules, "schedule_source_from_options", raising=False
    )


def test_a_row_shaped_schedule_does_not_count_on_an_engine_that_cannot_read_it(
    monkeypatch,
):
    """
    SCHEDULE_SOURCE and SCHEDULABLE_TASKS are keys only an engine that reads
    rows looks at. On one that reads neither, the two entries are dictionary
    keys nothing looks at, no row can dispatch the reconciler, and counting
    them would seal a batch with nothing that ever completes it, which is the
    condition the requirement exists to prevent.
    """
    _engine_that_cannot_read_rows(monkeypatch)
    with override_settings(TASKS=tasks_with_a_stored_source()):
        assert batches.reconcile_schedule_aliases() == []
        with pytest.raises(ImproperlyConfigured):
            batches.create("row-shaped")


def test_the_hint_names_the_settings_block_when_the_engine_cannot_read_rows(
    monkeypatch,
):
    """
    Naming SCHEDULABLE_TASKS here would send the project round a loop:
    nothing reads the rows, so exposing the reconciler to them starts
    nothing. The settings block is the only remedy on this engine.
    """
    _engine_that_cannot_read_rows(monkeypatch)
    with override_settings(TASKS=tasks_with_a_stored_source()):
        with pytest.raises(ImproperlyConfigured) as caught:
            batches.create("row-shaped")
        assert "SCHEDULES" in str(caught.value)
        assert "SCHEDULABLE_TASKS" not in str(caught.value)


def test_the_engine_capability_is_read_from_the_engine_itself():
    """
    Asked of the pinned django-ox's own loader, not of a version number,
    so the pair moving to an engine that reads rows needs no edit here
    and a fork that carries the feature is not refused for its version
    string.
    """
    import django_ox.schedules

    exposes = hasattr(django_ox.schedules, "schedule_source_from_options")
    assert batches._engine_reads_schedule_rows() is exposes


def test_create_and_seal_refuse_when_no_backend_schedules_the_reconciler():
    """
    The check can be silenced and settings can change under a batch that
    already exists, so create() and seal() refuse too, from the same
    settings the check reads and without a database round trip. A batch
    that cannot be sealed cannot sit SEALED forever.
    """
    batch = batches.create("before-the-schedule-went")
    with override_settings(TASKS=tasks_without_the_reconciler()):
        with pytest.raises(ImproperlyConfigured, match=r"oxpull\.batches\.reconcile"):
            batches.create("orphan")
        with pytest.raises(ImproperlyConfigured, match=r"oxpull\.batches\.reconcile"):
            batch.seal()
    assert OxpullBatch.objects.count() == 1
    assert stored(batch).status == OxpullBatch.Status.OPEN
    with override_settings(TASKS=tasks_without_the_reconciler(limited=True)):
        batch.seal()
    assert stored(batch).status == OxpullBatch.Status.SEALED


def test_seal_on_a_sealed_batch_reports_the_seal_before_the_missing_schedule():
    """
    The refusal for a missing reconciler schedule runs on the line that
    seals, and it used to run first: seal() on a batch already SEALED or
    COMPLETE raised ImproperlyConfigured about the schedule rather than
    BatchSealed about the batch. The status is the thing the caller asked
    about, so it is checked first and the settings guard second.
    """
    batch = batches.create("sealed-twice")
    batch.seal()
    with (
        override_settings(TASKS=tasks_without_the_reconciler()),
        pytest.raises(BatchSealed),
    ):
        batch.seal()
    assert reconcile() == 1
    with (
        override_settings(TASKS=tasks_without_the_reconciler()),
        pytest.raises(BatchSealed),
    ):
        batch.seal()


@pytest.mark.django_db(transaction=True)
def test_seal_under_postgresql_repeatable_read_raises_rather_than_counting_stale():
    """
    PostgreSQL REPEATABLE READ is the one level where "the status write
    waits and the count that follows is current" does not describe what
    happens. The transaction's snapshot is taken at its first statement,
    which is that status UPDATE; when the UPDATE has waited on an add()
    holding the batch row, the row it then finds was changed after the
    snapshot, and PostgreSQL raises a serialization failure rather than
    proceeding. Nothing is counted stale: the batch stays OPEN with no
    total, and a fresh seal() counts every member. Django's default is
    READ COMMITTED, where the UPDATE re-reads the row and the COUNT that
    follows takes a fresh snapshot; this test sets the level on the
    sealing session.
    """
    if connections["default"].vendor != "postgresql":
        pytest.skip("PostgreSQL's REPEATABLE READ takes its snapshot at the UPDATE")
    batch = batches.create("rr")
    batch.add(tasks.plain_echo, "before")
    added = threading.Event()

    def add_slowly():
        with transaction.atomic():
            batch.add(tasks.plain_echo, "slow")
            added.set()
            # The batch row stays locked until seal()'s UPDATE is waiting on
            # it. That UPDATE took the snapshot when it began, so the
            # snapshot is older than this commit however late the sealing
            # thread got going.
            assert a_session_waits_on_this_transaction(), (
                "seal() was never seen waiting on the batch row add() holds"
            )
        return "added"

    def seal_at_repeatable_read():
        assert added.wait(timeout=10)
        isolation.outlives_the_transaction("REPEATABLE READ")
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL "
                "REPEATABLE READ"
            )
        try:
            batch.seal()
        except OperationalError as exc:
            return type(exc.__cause__).__name__
        return "sealed"

    results, errors = race_all([add_slowly, seal_at_repeatable_read])

    assert errors == []
    assert sorted(results) == ["SerializationFailure", "added"]
    assert stored(batch).status == OxpullBatch.Status.OPEN
    assert stored(batch).total_count is None
    batch.seal()
    assert stored(batch).total_count == 2


@pytest.mark.django_db(transaction=True)
def test_a_reissued_add_sends_task_enqueued_for_each_attempt(monkeypatch):
    """
    task_enqueued fires inside the enqueue transaction, before commit,
    unchanged from django-ox. When add() owns its transaction and
    reissues it after an InnoDB deadlock, the attempt that lost the
    deadlock had already fired the signal for a row the rollback then
    discarded, and the reissue fires it again for the row that lands:
    one member, two firings, two ids. A receiver that acts on the
    signal directly acts twice, once on a task that does not exist;
    one deferred with transaction.on_commit acts once, on the row that
    committed. `docs/features/batches.md` says so beside the retry.
    """
    if connections["default"].vendor != "mysql":
        pytest.skip("the reissue is InnoDB's 1213")
    fired = []
    committed = []
    task_enqueued.connect(
        lambda sender, task_result, **kwargs: fired.append(task_result.id),
        weak=False,
        dispatch_uid="fired",
    )
    task_enqueued.connect(
        lambda sender, task_result, **kwargs: transaction.on_commit(
            lambda: committed.append(task_result.id)
        ),
        weak=False,
        dispatch_uid="committed",
    )
    try:
        calls = deadlock_once(monkeypatch)
        result = batches.create("victim").add(tasks.plain_echo, "k")
    finally:
        task_enqueued.disconnect(dispatch_uid="fired")
        task_enqueued.disconnect(dispatch_uid="committed")

    assert len(calls) == 2
    assert len(fired) == 2 and len(set(fired)) == 2
    assert committed == [result.id]
    assert OxTask.objects.count() == 1
