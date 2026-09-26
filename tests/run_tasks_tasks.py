"""
Tasks and worker classes for tests/test_run_tasks.py.

Module-level, as django.tasks requires. Tasks report through STATE, which the
`task_state` fixture clears around every test.
"""

import threading

from asgiref.sync import sync_to_async
from django.contrib.auth.models import Group
from django.db import IntegrityError, connection, connections, transaction

import django_ox
from django_ox.compat import task
from django_ox.exceptions import TaskTimeout
from django_ox.testing import run_tasks
from django_ox.worker import Worker

from .policy_tasks import policy_task, retry_now
from .tasks import STATE


def _log(label):
    STATE.setdefault("ran", []).append(label)


def _callback(label, *, fail=False):
    def callback():
        STATE.setdefault("callbacks", []).append(label)
        if fail:
            raise RuntimeError(f"callback {label} failed")

    callback.__qualname__ = f"callback_{label}"
    return callback


@task
def note(label):
    _log(label)
    return label


@task(queue_name="emails")
def note_email(label):
    _log(label)
    return label


@task(priority=-10)
def gated(label):
    _log(label)
    return label


@task
def where_am_i():
    STATE["thread"] = threading.get_ident()
    STATE["driver"] = id(connection.connection)
    STATE["in_atomic_block"] = connection.in_atomic_block
    return "here"


@task
def seen(name):
    return Group.objects.filter(name=name).exists()


@task
def make_group(name):
    return Group.objects.create(name=name).pk


@task
def make_then_raise(name):
    Group.objects.create(name=name)
    exc = ValueError(f"plain failure after writing {name}")
    STATE["raised"] = exc
    raise exc


@task
def make_then_integrity_error(name):
    Group.objects.create(name=name)
    Group.objects.create(name=f"{name}-2")
    Group.objects.create(name=name)


def _raw_duplicate(name):
    table = connection.ops.quote_name(Group._meta.db_table)
    column = connection.ops.quote_name("name")
    with connection.cursor() as cursor:
        cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s)", [name])  # noqa: S608


@task
def make_then_raw_error(name):
    Group.objects.create(name=name)
    try:
        _raw_duplicate(name)
    except IntegrityError:
        # Django marks the transaction for rollback when the ORM fails, and
        # not when raw SQL does.
        STATE["needs_rollback"] = connection.needs_rollback
        raise


@task
def make_then_swallow_raw_error(name):
    Group.objects.create(name=name)
    try:
        _raw_duplicate(name)
    except IntegrityError:
        STATE["swallowed"] = True
    return "returned"


@task
def make_then_swallow_orm_error(name):
    Group.objects.create(name=name)
    try:
        Group.objects.create(name=name)
    except IntegrityError:
        STATE["swallowed"] = True
    return "returned"


@task
def nested_savepoints(name):
    with transaction.atomic():
        Group.objects.create(name=f"{name}-a")
    try:
        with transaction.atomic():
            Group.objects.create(name=f"{name}-b")
            Group.objects.create(name=f"{name}-a")
    except IntegrityError:
        STATE["inner_rolled_back"] = True
    with transaction.atomic():
        Group.objects.create(name=f"{name}-c")
        transaction.set_rollback(True)
    Group.objects.create(name=f"{name}-d")
    return sorted(
        Group.objects.filter(name__startswith=name).values_list("name", flat=True)
    )


@task
def durable_write(name):
    with transaction.atomic(durable=True):
        Group.objects.create(name=name)
    return name


@task
def make_on(alias, name):
    Group.objects.using(alias).create(name=name)
    return name


@task
def make_on_both_then_integrity_error_on_alt(name):
    Group.objects.create(name=name)
    Group.objects.using("alt").create(name=name)
    Group.objects.using("alt").create(name=name)


@task
def enqueue_child(label):
    _log(label)
    note.enqueue(f"{label}-child")
    return label


@task
def enqueue_child_on_commit(label):
    _log(label)
    transaction.on_commit(lambda: note.enqueue(f"{label}-child"))
    return label


@task
def chain(step, last):
    _log(step)
    if step < last:
        chain.enqueue(step + 1, last)
    return step


@task
def loop_forever(step):
    loop_forever.enqueue(step + 1)
    return step


@task
def fail_always():
    raise ValueError("boom")


@task
def flaky(succeed_on):
    calls = STATE.get("flaky_calls", 0) + 1
    STATE["flaky_calls"] = calls
    if calls < succeed_on:
        raise RuntimeError(f"failing on call {calls}")
    return calls


@policy_task(max_attempts=3, backoff=retry_now)
def flaky_retry_now(succeed_on):
    calls = STATE.get("flaky_calls", 0) + 1
    STATE["flaky_calls"] = calls
    if calls < succeed_on:
        raise RuntimeError(f"failing on call {calls}")
    return calls


def _wait_a_minute(exc, task_result):
    return 60


@policy_task(max_attempts=3, backoff=_wait_a_minute)
def fail_then_wait_a_minute():
    raise RuntimeError("fails, and asks for a minute")


@task
def unserializable():
    return object()


@task
def interrupts(name, aimed):
    Group.objects.create(name=name)
    transaction.on_commit(_callback(name))
    raise {"KeyboardInterrupt": KeyboardInterrupt, "SystemExit": SystemExit}[aimed]


@task
def register(label, *, robust=False, fail=False):
    transaction.on_commit(_callback(label, fail=fail), robust=robust)
    return label


@task
def register_on(alias, label):
    transaction.on_commit(_callback(label), using=alias)
    return label


@task
def register_then_raise(label, *, fail=False):
    transaction.on_commit(_callback(label, fail=fail))
    raise ValueError(f"{label} raised after registering")


@task
def register_then_integrity_error(label):
    Group.objects.create(name=label)
    transaction.on_commit(_callback(label))
    Group.objects.create(name=label)


@task
def register_in_rolled_back_block(label):
    try:
        with transaction.atomic():
            transaction.on_commit(_callback(f"{label}-rolled-back"))
            raise ValueError("roll the block back")
    except ValueError:
        pass
    transaction.on_commit(_callback(f"{label}-kept"))
    return label


@task
def register_chain(label):
    def second():
        STATE.setdefault("callbacks", []).append(f"{label}-second")

    def first():
        STATE.setdefault("callbacks", []).append(f"{label}-first")
        transaction.on_commit(second)

    transaction.on_commit(first)
    transaction.on_commit(_callback(f"{label}-third"))
    return label


@task
def register_order(label):
    for n in range(3):
        transaction.on_commit(_callback(f"{label}-{n}"))
    return label


def _record_refusal(exc):
    STATE.setdefault("refused", []).append((type(exc), str(exc)))


def _try_run_tasks():
    try:
        run_tasks()
    except RuntimeError as exc:
        _record_refusal(exc)


@task
def calls_run_tasks():
    _try_run_tasks()
    return "called"


@task
def calls_run_tasks_from_callback():
    transaction.on_commit(_try_run_tasks)
    return "registered"


@task
async def async_calls_run_tasks():
    await sync_to_async(_try_run_tasks)()
    return "called"


@task
async def async_make_group(name):
    STATE["async_saw"] = await Group.objects.filter(name=f"{name}-seed").aexists()
    group = await Group.objects.acreate(name=name)
    return group.pk


@policy_task(timeout=1)
def outlives_its_timeout(seconds):
    STATE["deadline"] = django_ox.deadline()
    STATE["remaining"] = django_ox.remaining()
    threading.Event().wait(seconds)
    return "finished"


@task
def outlives_the_backend_timeout(seconds):
    STATE["deadline"] = django_ox.deadline()
    STATE["remaining"] = django_ox.remaining()
    threading.Event().wait(seconds)
    return "finished"


@task
def make_then_raise_task_timeout(name):
    Group.objects.create(name=name)
    raise TaskTimeout("raised by the task itself")


@task
def closes_its_connection():
    Group.objects.create(name="before-close")
    connection.close()
    return "closed"


@task
def breaks_the_transaction_in_a_callback(name):
    def callback():
        Group.objects.create(name=name)
        Group.objects.create(name=name)

    transaction.on_commit(callback)
    return name


def _roll_back_a_block(name):
    """A block of the task's own that raises and is caught."""
    try:
        with transaction.atomic():
            Group.objects.create(name=f"{name}-rolled-back")
            raise ValueError("roll the block back")
    except ValueError:
        pass


@task
def rolls_back_its_own_block(label):
    _roll_back_a_block(label)
    transaction.on_commit(_callback(label))
    return label


@task
def sets_rollback_in_its_own_block(label):
    with transaction.atomic():
        Group.objects.create(name=f"{label}-rolled-back")
        transaction.set_rollback(True)
    transaction.on_commit(_callback(label))
    return label


@task
def inserts_a_marker_once(label):
    # The guard a task that may run twice keeps: a second run finds the
    # marker and stops.
    try:
        with transaction.atomic():
            Group.objects.create(name=label)
    except IntegrityError:
        transaction.on_commit(_callback(f"{label}-done-already"))
        return "done already"
    return "inserted"


def shared_callback():
    STATE.setdefault("shared", []).append("ran")


@task
def register_the_shared_callback(label):
    transaction.on_commit(shared_callback)
    _roll_back_a_block(label)
    transaction.on_commit(shared_callback)
    return label


@task
def drops_the_callers_first_callback(label):
    transaction.on_commit(_callback(label))
    del connection.run_on_commit[0]
    return label


@task
def replaces_the_callers_first_callback(label):
    transaction.on_commit(_callback(label))
    sids, _func, robust = connection.run_on_commit[0]
    connection.run_on_commit[0] = (sids, _callback(f"{label}-replacement"), robust)
    return label


@task
def writes_then_registers_a_breaking_callback(name, *, robust=False):
    Group.objects.create(name=f"{name}-body")

    def breaks():
        Group.objects.create(name=f"{name}-callback")
        Group.objects.create(name=f"{name}-callback")

    transaction.on_commit(breaks, robust=robust)
    transaction.on_commit(_callback(f"{name}-after"))
    return name


@task
def registers_a_callback_that_swallows_an_error(name, *, robust=False):
    def swallows():
        Group.objects.create(name=f"{name}-callback")
        try:
            Group.objects.create(name=f"{name}-callback")
        except IntegrityError:
            STATE["swallowed"] = True

    transaction.on_commit(swallows, robust=robust)
    return name


@task
def registers_a_callback_that_writes_then_raises(name):
    def writes_then_raises():
        Group.objects.create(name=f"{name}-callback")
        raise ValueError(f"callback {name} raised after writing")

    transaction.on_commit(writes_then_raises, robust=True)
    transaction.on_commit(_callback(f"{name}-after"))
    return name


@task
def registers_a_callback_that_interrupts(name, aimed):
    Group.objects.create(name=f"{name}-body")

    def interrupts_in_a_callback():
        Group.objects.create(name=f"{name}-callback")
        raise {"KeyboardInterrupt": KeyboardInterrupt, "SystemExit": SystemExit}[aimed]

    transaction.on_commit(interrupts_in_a_callback, robust=True)
    transaction.on_commit(_callback(f"{name}-after"))
    return name


@task
def registers_a_durable_callback(name):
    def durable():
        with transaction.atomic(durable=True):
            Group.objects.create(name=name)

    transaction.on_commit(durable)
    return name


class CountingWorker(Worker):
    """Counts its claims in STATE, to show the configured class is used."""

    def claim_one(self):
        STATE["claims"] = STATE.get("claims", 0) + 1
        return super().claim_one()


class GatingWorker(CountingWorker):
    """
    Refuses to claim the `gated` task while _ready_queryset() still lists it,
    the way a claim-time rate limit does.
    """

    def claim_one(self):
        head = self._ready_queryset().first()
        if head is not None and head.task_path == f"{__name__}.gated":
            STATE["claims"] = STATE.get("claims", 0) + 1
            return None
        return super().claim_one()


class OutcomeCallbackWorker(Worker):
    """
    Registers a commit callback from its outcome write, the way Pro releases
    a workflow node's dependents after the write commits.
    """

    def _write_outcome(self, db_task, *, status, duration_ms, **fields):
        written = super()._write_outcome(
            db_task, status=status, duration_ms=duration_ms, **fields
        )
        label = f"outcome-{db_task.task_path.rsplit('.', 1)[-1]}-{status}"
        callback = (
            _try_run_tasks
            if STATE.get("outcome_callback_runs_tasks")
            else _callback(label, fail=STATE.get("outcome_callback_fails", False))
        )
        transaction.on_commit(
            callback,
            using=self._db_alias,
            robust=STATE.get("outcome_callback_robust", False),
        )
        return written


def alias_connections():
    """The initialized connections, for assertions about their state."""
    return {conn.alias: conn for conn in connections.all(initialized_only=True)}
