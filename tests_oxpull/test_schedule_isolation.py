"""
A schedule the database refuses, ahead of oxpull's own schedules, through
real `ox_worker --batch` processes.

oxpull's batch and workflow reconcilers are ordinary django-ox schedule
entries, and nothing else completes a sealed batch or workflow. A schedule
ahead of them whose arguments the database refuses (a non-finite float:
PostgreSQL raises a DataError, MySQL an OperationalError and SQLite an
IntegrityError) is that schedule's failure. It must leave both reconcilers
to run, leave unique tasks deduplicated when two workers dispatch the same
ticks, and leave rate limits and read routing doing what they do without
it.

Every test starts the worker a deployment runs, with oxpull's worker class,
on this session's test database, after proving which oxpull and which
django-ox that process imports. What is asserted is what the database holds
afterwards and how the process ended. Every wait is a deadlock guard with a
bound, never a measurement: a batch that exits does so on its own, and one
that does not has failed. SQLite is the control: it isolated this refusal
before, and PostgreSQL and MySQL did not.
"""

import copy
import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, connection, connections, transaction
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask
from django_ox.stored import create_schedule
from django_ox.worker import Worker
from oxpull import batches, workflows
from oxpull.models import (
    OxpullBatch,
    OxpullRateWindow,
    OxpullUniqueLock,
    OxpullWorkflow,
)
from oxpull.unique import unique_key

from . import tasks
from .subprocesses import django_ox_path
from .test_workflows import drain, flow_worker

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="worker processes"),
]

#: How long a test waits before calling a batch stuck. Every batch here
#: finishes in seconds; the failure these tests exist for was a batch that
#: never finished.
GUARD = 60

BATCH_RECONCILE = "oxpull.batches.reconcile"
WORKFLOW_RECONCILE = workflows.RECONCILE_TASK_PATH
BATCH_KEY = "oxpull-batch-reconcile"
WORKFLOW_KEY = "oxpull-workflow-reconcile"

#: Serializable by Python's json module, which writes the bare token
#: Infinity, and refused by every supported database's JSON column. Nothing
#: django-ox or oxpull checks at start-up refuses it.
REFUSED = {"max_age": float("inf")}

#: The same value as a stored row's form field holds it: the text typed.
REFUSED_TEXT = '{"max_age": Infinity}'

VALUE_KIND = "iso.value"

SUCCESSFUL = OxTask.Status.SUCCESSFUL


def twelve_hours_ago():
    """
    The project's wall clock twelve hours back, to the minute, naive.

    A schedule that runs daily at this time of day has a due tick twelve
    hours old and its next one twelve hours off, so every pass, worker and
    process in a test derives the same tick however long the test takes. A
    minutely schedule would fire again across a minute boundary.
    """
    now = timezone.now()
    local = timezone.localtime(now).replace(tzinfo=None) if settings.USE_TZ else now
    return (local - timedelta(hours=12)).replace(second=0, microsecond=0)


def as_stored(local):
    """A naive project-local instant as the tick log stores it."""
    return timezone.make_aware(local) if settings.USE_TZ else local


def daily_cron_at(local):
    return f"{local.minute} {local.hour} * * *"


def entry(task_path, *args, cron, refused=False):
    """A SCHEDULES entry; a refused one carries REFUSED as its `value`."""
    config = {"task": task_path, "cron": cron}
    if args:
        config["args"] = list(args)
    if refused:
        config["kwargs"] = {"value": REFUSED}
    return config


def with_history(*keys):
    """
    One recorded tick per schedule a day ago, so each schedule's due tick
    fires rather than anchoring a first sighting, which enqueues nothing
    and so would never meet the refusal.
    """
    now = timezone.now()
    for key in keys:
        OxScheduleTick.objects.create(
            schedule_name=key,
            scheduled_for=now - timedelta(days=1),
            task_id=None,
            created_at=now,
        )


def fired(key):
    """(tick instant, task) for every tick of `key` that enqueued a task."""
    return [
        (tick.scheduled_for, OxTask.objects.get(id=tick.task_id))
        for tick in OxScheduleTick.objects.filter(schedule_name=key).exclude(
            task_id=None
        )
    ]


def deployment_tasks(**options_by_alias):
    """
    settings.TASKS as a deployment runs it, with OPTIONS replaced per alias
    (None removes one).

    "default" gets oxpull's worker class, which tests_oxpull/settings.py leaves
    out. "flows_ungated" stops declaring WORKFLOWS: it is the suite's
    deliberate oxpull.E007 fixture, and a system-check Error stops
    ox_worker at start-up. Its alias stays because tests_oxpull.tasks declares a
    task on it.
    """
    config = copy.deepcopy(settings.TASKS)
    config["flows_ungated"]["OPTIONS"]["WORKFLOWS"] = False
    config["default"]["OPTIONS"]["WORKER_CLASS"] = "oxpull.worker.OxpullWorker"
    for alias, options in options_by_alias.items():
        for name, value in options.items():
            if value is None:
                config[alias]["OPTIONS"].pop(name, None)
            else:
                config[alias]["OPTIONS"][name] = value
    return config


def deploy(tmp_path, config, *, router=False):
    """
    The environment of a worker process on this session's test database,
    with `config` as its TASKS.

    TASKS goes through json.loads, which reads the bare token Infinity back
    as the float a settings file would hold. The replica alias is the
    suite's never-written database; on SQLite it is moved into tmp_path so
    that a read routed to it creates nothing in the checkout. `router`
    installs tests_oxpull.routers.ReplicaReadRouter, which sends every django_ox
    and oxpull read that names no database to that replica.
    """
    lines = [
        "import copy, json",
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403",
        f"TASKS = json.loads({json.dumps(config)!r})",
        "DATABASES = copy.deepcopy(DATABASES)  # noqa: F405",
    ]
    if connection.vendor == "sqlite":
        replica = tmp_path / "replica.sqlite3"
        lines.append(f"DATABASES['replica']['NAME'] = {str(replica)!r}")
    if router:
        lines.append("DATABASE_ROUTERS = ['tests_oxpull.routers.ReplicaReadRouter']")
    lines += [
        "LOGGING = {",
        "    'version': 1,",
        "    'disable_existing_loggers': False,",
        "    'handlers': {'console': {'class': 'logging.StreamHandler'}},",
        "    'loggers': {",
        "        'django_ox': {'handlers': ['console'], 'level': 'INFO'},",
        "        'oxpull': {'handlers': ['console'], 'level': 'INFO'},",
        "    },",
        "}",
    ]
    (tmp_path / "isolation_settings.py").write_text("\n".join(lines) + "\n")
    name = connection.settings_dict["NAME"]
    if connection.vendor == "sqlite":
        name = Path(name).resolve()
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "isolation_settings",
        "PYTHONPATH": os.pathsep.join(
            [str(SRC), django_ox_path(), str(tmp_path), str(REPO_ROOT)]
        ),
        "OX_TEST_DB_NAME": str(name),
    }


def run_python(env, *args):
    return subprocess.run(  # noqa: S603
        [sys.executable, *args],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


PAIR_PROBE = """
import django
django.setup()
import django_ox, oxpull
from django_ox.worker import worker_class
print(oxpull.__file__)
print(django_ox.__file__)
cls = worker_class(ALIAS)
print(cls.__module__ + "." + cls.__qualname__)
"""


def assert_the_pair(env, alias):
    """
    The process imports this checkout's oxpull and the django-ox this
    process runs against, and ox_worker would build oxpull's worker for
    `alias`. A failure here is the set-up, not the behaviour under test.
    """
    probe = run_python(env, "-c", PAIR_PROBE.replace("ALIAS", repr(alias)))
    lines = probe.stdout.split()
    if not (
        len(lines) == 3
        and Path(lines[0]).resolve().is_relative_to(SRC.resolve())
        and Path(lines[1]).resolve().is_relative_to(Path(django_ox_path()))
        and lines[2] == "oxpull.worker.OxpullWorker"
    ):
        pytest.fail(f"unexpected worker set-up:\n{probe.stdout}{probe.stderr}")


ROUTER_PROBE = """
import django
django.setup()
from django.db import router
from django_ox.models import OxScheduleTick
from oxpull.models import OxpullBatch
print(router.db_for_read(OxScheduleTick), router.db_for_read(OxpullBatch))
try:
    found = OxScheduleTick.objects.filter(schedule_name=KEY).exists()
except Exception as exc:
    print(type(exc).__name__)
else:
    print(found)
"""


def assert_reads_are_routed_away(env, key):
    """
    The router reaches the worker process: a read that names no database
    goes to the replica, and there it cannot see the tick row `key` this
    test wrote. Every read that has to see it must name its database.
    """
    probe = run_python(env, "-c", ROUTER_PROBE.replace("KEY", repr(key)))
    lines = probe.stdout.splitlines()
    if not (len(lines) == 2 and lines[0] == "replica replica" and lines[1] != "True"):
        pytest.fail(
            f"the router did not reach the process:\n{probe.stdout}{probe.stderr}"
        )


def start(env, log_path, backend="default"):
    log = log_path.open("wb")
    try:
        return subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "django",
                "ox_worker",
                "--batch",
                "--backend",
                backend,
                "--interval",
                "0.05",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=log,
            stderr=log,
        )
    finally:
        log.close()


def finish(process, log_path):
    """The exit code of a batch that ended on its own, or a failed test."""
    try:
        return process.wait(timeout=GUARD)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        pytest.fail(
            f"the batch did not finish within {GUARD}s:\n"
            + log_path.read_text()[-4000:]
        )


def run_batch(env, log_path, backend="default"):
    process = start(env, log_path, backend)
    return finish(process, log_path), log_path.read_text()


def wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def waiting_on_locks():
    """
    Sessions of this test database blocked on a lock right now, asked on a
    connection of its own: PostgreSQL keeps one pg_stat_activity snapshot
    per transaction, so asking inside the barrier's transaction would read
    the same answer forever.
    """
    probe = connections.create_connection(DEFAULT_DB_ALIAS)
    try:
        with probe.cursor() as cursor:
            if probe.vendor == "postgresql":
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            else:
                cursor.execute(
                    "SELECT COUNT(*) FROM performance_schema.data_lock_waits w "
                    "JOIN performance_schema.threads t "
                    "ON t.THREAD_ID = w.REQUESTING_THREAD_ID "
                    "WHERE t.PROCESSLIST_DB = DATABASE()"
                )
            return cursor.fetchone()[0]
    finally:
        probe.close()


def sealed_and_run():
    """
    A sealed workflow and a sealed batch whose every task has run. Only a
    reconciler completes either of them now, and the batch's on_complete
    callback is enqueued by that completion.
    """
    flow = workflows.create("isolation-flow")
    flow.add("only", tasks.step, "only")
    flow.seal()
    assert drain(flow_worker()) == 1
    batch = batches.create(
        "isolation-batch",
        on_complete=tasks.record_callback,
        on_complete_args=["isolation-batch-complete"],
    )
    batch.add(tasks.plain_echo, 1)
    batch.add(tasks.plain_echo, 2)
    batch.seal()
    members = Worker(backend_alias="default", backoff_initial=0, poll_interval=0.01)
    assert drain(members) == 2
    assert set(OxTask.objects.values_list("status", flat=True)) == {SUCCESSFUL}
    assert OxpullWorkflow.objects.get(pk=flow.id).status == OxpullWorkflow.Status.SEALED
    assert OxpullBatch.objects.get(pk=batch.id).status == OxpullBatch.Status.SEALED
    return flow, batch


def assert_reconciled(flow, batch, reconcilers, due):
    """
    Each reconciler schedule fired its due tick once, into one task of its
    own path that ran, and between them they completed the workflow and the
    batch, whose callback ran too.
    """
    for key, path in reconcilers.items():
        ((instant, task),) = fired(key)
        assert instant == as_stored(due), key
        assert (task.task_path, task.status) == (path, SUCCESSFUL), key
    assert OxpullWorkflow.objects.get(pk=flow.id).status == (
        OxpullWorkflow.Status.COMPLETE
    )
    assert OxpullBatch.objects.get(pk=batch.id).status == OxpullBatch.Status.COMPLETE
    (callback,) = OxTask.objects.filter(task_path="tests_oxpull.tasks.record_callback")
    assert callback.args == ["isolation-batch-complete"]
    assert callback.status == SUCCESSFUL


def assert_refused_left_nothing(keys, names, logs):
    """
    Each refused schedule fired nothing and was reported as its own failure
    by some worker, and no pass was abandoned. A refused unique schedule's
    lock row went with its rollback, so no lock names a refused key.
    """
    for key in keys:
        assert fired(key) == [], key
    for name in names:
        assert any(
            f"Schedule {name} could not be dispatched this pass" in log for log in logs
        ), name
    for log in logs:
        assert "Schedule dispatch failed" not in log, log
    refused_lock = unique_key(
        "default", tasks.unique_echo.module_path, "default", [], {"value": REFUSED}
    )
    assert not OxpullUniqueLock.objects.filter(key=refused_lock).exists()


@pytest.mark.parametrize("router", [False, True], ids=["plain", "replica-read-router"])
def test_refused_schedules_ahead_of_both_reconcilers_leave_them_to_complete(
    router, tmp_path
):
    """
    A refused plain schedule and a refused unique one, ahead of both
    reconcilers in SCHEDULES. The second run of a deployment's batch job
    meets the refusal. It has to complete the batch and the workflow, run
    the batch's callback, and exit 0 by itself.

    With the replica read router the same run must come out the same: every
    read the dispatch pass and the reconcilers make names its database.
    """
    due = twelve_hours_ago()
    cron = daily_cron_at(due)
    schedules = {
        "bad-plain": entry("tests_oxpull.tasks.plain_echo", cron=cron, refused=True),
        "bad-unique": entry("tests_oxpull.tasks.unique_echo", cron=cron, refused=True),
        BATCH_KEY: entry(BATCH_RECONCILE, cron=cron),
        WORKFLOW_KEY: entry(WORKFLOW_RECONCILE, cron=cron),
    }
    with_history(*schedules)
    flow, batch = sealed_and_run()
    env = deploy(
        tmp_path, deployment_tasks(default={"SCHEDULES": schedules}), router=router
    )
    assert_the_pair(env, "default")
    if router:
        assert_reads_are_routed_away(env, BATCH_KEY)

    code, log = run_batch(env, tmp_path / "run.log")

    assert code == 0, log
    assert_reconciled(
        flow, batch, {BATCH_KEY: BATCH_RECONCILE, WORKFLOW_KEY: WORKFLOW_RECONCILE}, due
    )
    assert_refused_left_nothing(
        ["bad-plain", "bad-unique"], ["bad-plain", "bad-unique"], [log]
    )
    # Nothing else was enqueued: the batch's two members, the workflow's
    # node, one task per reconciler and the callback.
    assert sorted(OxTask.objects.values_list("task_path", flat=True)) == sorted(
        [
            BATCH_RECONCILE,
            WORKFLOW_RECONCILE,
            "tests_oxpull.tasks.plain_echo",
            "tests_oxpull.tasks.plain_echo",
            "tests_oxpull.tasks.record_callback",
            "tests_oxpull.tasks.step",
        ]
    )
    assert not OxpullUniqueLock.objects.exists()


def test_refused_entries_ahead_of_stored_reconciler_rows(tmp_path, monkeypatch):
    """
    The reconcilers as stored rows, which oxpull counts as scheduling them.
    Settings schedules come before stored rows and rows come in primary-key
    order, so a refused settings entry and a refused row written first are
    both ahead of the two reconciler rows.
    """
    # The registry is process-wide. Start it empty, as the stored-schedule
    # test in test_workflows_hazard_reconcile_schedule.py does, so the
    # settings channel fills it from SCHEDULABLE_TASKS below.
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    due = twelve_hours_ago()
    cron = daily_cron_at(due)
    config = deployment_tasks(
        default={
            "SCHEDULES": {
                "bad-plain": entry(
                    "tests_oxpull.tasks.plain_echo", cron=cron, refused=True
                )
            },
            "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
            "SCHEDULABLE_TASKS": {
                VALUE_KIND: {
                    "task": "tests_oxpull.tasks.plain_echo",
                    "form": "tests_oxpull.forms.ValueArgs",
                },
                BATCH_KEY: BATCH_RECONCILE,
                WORKFLOW_KEY: WORKFLOW_RECONCILE,
            },
        }
    )
    with_history("bad-plain")
    flow, batch = sealed_and_run()
    boundary = timezone.now() - timedelta(days=1)
    with override_settings(TASKS=config):
        bad = create_schedule(
            name="r-bad",
            task_key=VALUE_KIND,
            trigger="cron",
            cron=cron,
            arguments={"value": REFUSED_TEXT},
            start_time=boundary,
        )
        batch_row = create_schedule(
            name="batch-reconcile",
            task_key=BATCH_KEY,
            trigger="cron",
            cron=cron,
            start_time=boundary,
        )
        flow_row = create_schedule(
            name="workflow-reconcile",
            task_key=WORKFLOW_KEY,
            trigger="cron",
            cron=cron,
            start_time=boundary,
        )
    assert bad.pk < batch_row.pk < flow_row.pk
    env = deploy(tmp_path, config)
    assert_the_pair(env, "default")

    code, log = run_batch(env, tmp_path / "run.log")

    assert code == 0, log
    assert_reconciled(
        flow,
        batch,
        {
            f"db:{batch_row.pk}": BATCH_RECONCILE,
            f"db:{flow_row.pk}": WORKFLOW_RECONCILE,
        },
        due,
    )
    assert_refused_left_nothing(
        ["bad-plain", f"db:{bad.pk}"], ["bad-plain", "r-bad"], [log]
    )


def test_two_workers_dispatch_a_unique_task_once_behind_refused_schedules(tmp_path):
    """
    Two batch workers over the same due ticks: two refused schedules, then
    two schedules enqueueing the same unique call, then both reconcilers.

    On PostgreSQL and MySQL both workers are held inside the first unique
    schedule's dispatch by a database barrier: this test holds an
    uncommitted lock row for that call's key until both workers wait, one
    on the key and the other on the first one's tick, then rolls it back.
    Neither can run anything before its pass ends, so the second schedule's
    enqueue finds the first one's task still pending and returns it: one
    task, which both ticks name. SQLite has one writer, so its workers take
    turns without a barrier, and a worker that meets the write lock there
    can end its pass and run the task before the second tick fires. What
    holds there is the unique contract itself: a second task only after the
    first one finished.
    """
    due = twelve_hours_ago()
    cron = daily_cron_at(due)
    schedules = {
        "bad-plain": entry("tests_oxpull.tasks.plain_echo", cron=cron, refused=True),
        "bad-unique": entry("tests_oxpull.tasks.unique_echo", cron=cron, refused=True),
        "u-one": entry("tests_oxpull.tasks.unique_echo", "same", cron=cron),
        "u-two": entry("tests_oxpull.tasks.unique_echo", "same", cron=cron),
        BATCH_KEY: entry(BATCH_RECONCILE, cron=cron),
        WORKFLOW_KEY: entry(WORKFLOW_RECONCILE, cron=cron),
    }
    with_history(*schedules)
    env = deploy(tmp_path, deployment_tasks(default={"SCHEDULES": schedules}))
    assert_the_pair(env, "default")
    key = unique_key("default", tasks.unique_echo.module_path, "default", ["same"], {})

    logs = [tmp_path / "w1.log", tmp_path / "w2.log"]
    processes = []
    barrier = connection.vendor in ("postgresql", "mysql")
    try:
        if barrier:
            with transaction.atomic():
                OxpullUniqueLock.objects.create(
                    key=key,
                    task_path=tasks.unique_echo.module_path,
                    queue_name="default",
                    created_at=timezone.now(),
                )
                processes = [start(env, log) for log in logs]
                held = wait_for(lambda: waiting_on_locks() >= 2, timeout=GUARD)
                transaction.set_rollback(True)
            assert held, "both workers never waited: " + "\n".join(
                log.read_text()[-2000:] for log in logs
            )
        else:
            processes = [start(env, log) for log in logs]
        codes = [
            finish(process, log) for process, log in zip(processes, logs, strict=True)
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
    text = [log.read_text() for log in logs]
    assert codes == [0, 0], text

    ((first_at, first),) = fired("u-one")
    ((second_at, second),) = fired("u-two")
    assert first_at == second_at == as_stored(due)
    uniques = list(OxTask.objects.filter(task_path=tasks.unique_echo.module_path))
    assert all(task.args == ["same"] for task in uniques)
    assert all(task.status == SUCCESSFUL for task in uniques)
    assert {task.id for task in uniques} == {first.id, second.id}
    if barrier:
        assert first.id == second.id, "the second tick enqueued a duplicate"
    elif first.id != second.id:
        assert first.finished_at <= second.enqueued_at, (
            "a second task was enqueued while the first was pending"
        )
    # One lock for the call, naming the latest task; the refused schedule's
    # lock rolled back with it.
    (lock,) = OxpullUniqueLock.objects.all()
    assert (lock.key, lock.task_id) == (key, second.id)
    for reconciler in (BATCH_KEY, WORKFLOW_KEY):
        ((_, task),) = fired(reconciler)
        assert task.status == SUCCESSFUL
    assert_refused_left_nothing(
        ["bad-plain", "bad-unique"], ["bad-plain", "bad-unique"], text
    )
    # Losing the race for a tick is not a schedule's failure.
    for log in text:
        for name in ("u-one", "u-two", BATCH_KEY, WORKFLOW_KEY):
            assert f"Schedule {name} could not be dispatched" not in log, log


def test_a_refused_schedule_ahead_of_rate_limited_ones_leaves_the_limit_deciding(
    tmp_path,
):
    """
    Schedules on the "limited" backend, whose one-per-hour limit covers
    limited_echo: a refused one, then two healthy ones. Both healthy ticks
    enqueue; the limit admits one, holds the other READY without spending
    an attempt, and the batch still ends, because a throttled task is not
    one this worker can claim.
    """
    due = twelve_hours_ago()
    cron = daily_cron_at(due)
    schedules = {
        "l-bad": entry("tests_oxpull.tasks.limited_echo", cron=cron, refused=True),
        "l-one": entry("tests_oxpull.tasks.limited_echo", "one", cron=cron),
        "l-two": entry("tests_oxpull.tasks.limited_echo", "two", cron=cron),
    }
    with_history(*schedules)
    env = deploy(tmp_path, deployment_tasks(limited={"SCHEDULES": schedules}))
    assert_the_pair(env, "limited")

    code, log = run_batch(env, tmp_path / "run.log", backend="limited")

    assert code == 0, log
    ((_, one),) = fired("l-one")
    ((_, two),) = fired("l-two")
    assert sorted([one.status, two.status]) == sorted([SUCCESSFUL, OxTask.Status.READY])
    held = one if one.status == OxTask.Status.READY else two
    assert held.attempts == 0
    assert OxpullRateWindow.objects.get(limit_name="hourly").used == 1
    assert fired("l-bad") == []
    assert "Schedule l-bad could not be dispatched this pass" in log
    assert "Schedule dispatch failed" not in log
