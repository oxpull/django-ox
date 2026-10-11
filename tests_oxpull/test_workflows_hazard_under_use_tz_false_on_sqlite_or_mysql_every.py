"""
Workflows under USE_TZ=False on SQLite and MySQL.

Django's SQLite and MySQL backends refuse to bind an aware datetime to a
DateTimeField when USE_TZ is off, filter values included, with a ValueError
raised in Python before any SQL is sent. PostgreSQL accepts it. Development
builds of workflows bound an aware run_after sentinel on every reconciler
tick, on every release and on every add() of a blocked node, and each of
those raised. A blocked node now waits in django-ox's WAITING status
instead, and its release binds the process clock, which is naive under
USE_TZ=False.

USE_TZ=False is a configuration django-ox supports (django_ox.W001 exists
for it, CI runs a naive leg). `docs/features/rate-limiting.md` documents
rate limits under it. Every test here passes on all three databases.

Every test asserts the behaviour a user is owed. Two layers:

  * In-process, under override_settings(USE_TZ=False), through the public
    API and real OxpullWorker.run_once() claims.
  * A fresh interpreter configured with USE_TZ=False from the start
    (TIME_ZONE="UTC", the zone django_ox.W001's own hint recommends),
    migrated, and driven by the real `ox_worker` management command for
    each backend, with the workflow reconciler on a real django-ox
    schedule. This is the path a user runs, and it rules out
    override_settings as the cause.
"""

import json
import os
import subprocess
import sys
import traceback
from datetime import timedelta
from pathlib import Path

import pytest
from django.db import connections
from django.tasks.signals import task_finished
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, order, stored
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

REPO_ROOT = Path(__file__).resolve().parent.parent


def naive():
    return override_settings(USE_TZ=False)


def _raised(fn):
    """
    Call fn; return (value, None), or (None, the raise path) if it raised.

    The raise path is every frame in oxpull or django_ox plus the frame
    that raised, so a failure names the line that bound the value.
    """
    try:
        return fn(), None
    except Exception as exc:
        frames = traceback.extract_tb(exc.__traceback__)
        kept = [
            frame
            for index, frame in enumerate(frames)
            if "/oxpull/" in frame.filename
            or "/django_ox/" in frame.filename
            or index == len(frames) - 1
        ]
        path = "".join(traceback.format_list(kept))
        return None, f"{path}{type(exc).__name__}: {exc}"


# -- (a) the reconciler ------------------------------------------------


def test_a_scheduled_reconcile_task_completes_a_sealed_workflow():
    """
    The reconciler as a schedule runs it: the oxpull.workflows.reconcile
    task enqueued and executed by a django-ox worker. No backend here
    defers anything; only the "flows" backend is involved.
    """
    with naive():
        flow = workflows.create(
            "naive-chain",
            on_complete=tasks.record_callback,
            on_complete_args=["complete"],
            on_success=tasks.record_callback,
            on_success_args=["success"],
        )
        flow.add("root", tasks.step, "root")
        flow.add("child", tasks.step, "child", depends_on="root")
        flow.seal()
        assert drain(flow_worker()) == 2
        assert order() == ["root", "child"]

        tick = workflows.reconcile.enqueue()
        drain(Worker(backoff_initial=0, poll_interval=0.01), limit=10)

        row = OxTask.objects.get(pk=tick.id)
        assert row.status == OxTask.Status.SUCCESSFUL, (
            f"the reconcile task ended {row.status} after {row.attempts} "
            f"attempt(s); last error: {row.errors[-1:]}"
        )
        workflow = stored(flow)
        assert workflow.status == OxpullWorkflow.Status.COMPLETE
        assert workflow.callback_task_id is not None
        assert workflow.success_task_id is not None


def test_one_reconciler_pass_cancels_a_doomed_cone_and_completes_the_workflow():
    """
    The pass itself, called as the scheduled task calls it, on a workflow
    that needs both of the reconciler's jobs: cancel the child of a FAILED
    root, then complete the workflow.
    """
    with naive():
        flow = workflows.create(
            "naive-doomed",
            on_complete=tasks.record_callback,
            on_complete_args=["complete"],
        )
        flow.add("root", tasks.failing_step, "root")
        child = flow.add("child", tasks.step, "child", depends_on="root")
        flow.seal()
        worker = flow_worker()
        for _ in range(3):
            assert worker.run_once() is True
        assert OxTask.objects.get(args=["root"]).status == OxTask.Status.FAILED

        completed, error = _raised(workflows._reconcile)
        assert error is None, f"_reconcile() raised:\n{error}"
        assert completed == 1
        assert OxTask.objects.get(pk=child.id).status == OxTask.Status.DISCARDED
        workflow = stored(flow)
        assert workflow.status == OxpullWorkflow.Status.COMPLETE
        assert workflow.cancelled_count == 1
        assert workflow.callback_task_id is not None


# -- (b) add() of a node that waits ------------------------------------


def test_add_holds_a_blocked_node_on_the_backend_with_the_leftover_option():
    with naive():
        flow = workflows.create("naive-deferred")
        flow.add("parent", tasks.deferred_step, "parent")
        child, error = _raised(
            lambda: flow.add("child", tasks.deferred_step, "child", depends_on="parent")
        )
        assert error is None, f"add() of a blocked node raised:\n{error}"
        row = OxTask.objects.get(pk=child.id)
        assert (row.status, row.run_after) == (OxTask.Status.WAITING, None), (
            "the child is blocked, so it should wait with no run_after of its own"
        )
        flow.seal()
        assert drain(deferring_worker()) == 2
        assert order() == ["parent", "child"]


# -- (c) the eager release after a SUCCESSFUL write ------------------------


def _finish_upstream_and_report(root_id):
    """
    Run one claim on the deferring worker. Return (run_once's value, its
    raise path or None, the root's stored status, whether task_finished
    was sent for the root).
    """
    finished = []

    def receiver(sender, task_result, **kwargs):
        finished.append(str(task_result.id))

    task_finished.connect(receiver)
    try:
        ran, error = _raised(deferring_worker().run_once)
    finally:
        task_finished.disconnect(receiver)
    status = OxTask.objects.get(pk=root_id).status
    return ran, error, status, str(root_id) in finished


def test_a_deferring_worker_finishing_an_upstream_of_another_backends_node():
    """
    Root on the deferring backend, child on the non-deferring "flows"
    backend, so add() binds no sentinel and succeeds. The root's success
    then reaches release_dependents.
    """
    with naive():
        flow = workflows.create("naive-mixed")
        root = flow.add("root", tasks.deferred_step, "root")
        flow.add("child", tasks.step, "child", depends_on="root")
        flow.seal()

        ran, error, status, signalled = _finish_upstream_and_report(root.id)
        assert error is None, (
            f"run_once() raised after the root was written {status} "
            f"(task_finished sent: {signalled}):\n{error}"
        )
        assert ran is True
        assert status == OxTask.Status.SUCCESSFUL
        assert signalled, "task_finished was not sent for a task that succeeded"
        assert drain(flow_worker()) == 1
        assert order() == ["root", "child"]


def test_a_deferring_worker_finishing_an_upstream_of_a_scheduled_node():
    """
    The same release on one deferring backend: the child carries the
    caller's own run_after, so add() leaves it alone (a caller's run_after
    is never overwritten) and binds nothing. Not a cross-backend corner.
    """
    with naive():
        flow = workflows.create("naive-scheduled")
        root = flow.add("root", tasks.deferred_step, "root")
        flow.add(
            "child",
            tasks.deferred_step.using(run_after=timezone.now() - timedelta(seconds=1)),
            "child",
            depends_on="root",
        )
        flow.seal()

        ran, error, status, signalled = _finish_upstream_and_report(root.id)
        assert error is None, (
            f"run_once() raised after the root was written {status} "
            f"(task_finished sent: {signalled}):\n{error}"
        )
        assert ran is True
        assert status == OxTask.Status.SUCCESSFUL
        assert signalled, "task_finished was not sent for a task that succeeded"
        assert drain(deferring_worker()) == 1
        assert order() == ["root", "child"]


# -- a real USE_TZ=False project, driven by ox_worker ----------------------

DRIVER = r"""
import collections
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
mode, cfg_path = sys.argv[1], sys.argv[2]
cfg = json.loads(Path(cfg_path).read_text())

if "mysql" in cfg["database"]["ENGINE"]:
    try:
        import MySQLdb  # noqa: F401
    except ModuleNotFoundError:
        import pymysql

        pymysql.install_as_MySQLdb()

import django
from django.conf import settings

GATED = {"WORKER_CLASS": "oxpull.worker.OxpullWorker", "WORKFLOWS": True}
settings.configure(
    SECRET_KEY="naive-tz",
    USE_TZ=False,
    TIME_ZONE="UTC",
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    INSTALLED_APPS=["django_ox", "oxpull"],
    DATABASES={"default": cfg["database"]},
    TASKS={
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["default"],
            "OPTIONS": {
                "SCHEDULES": {
                    "oxpull-workflow-reconcile": {
                        "task": "oxpull.workflows.reconcile",
                        "every": 1,
                    },
                    "oxpull-batch-reconcile": {
                        "task": "oxpull.batches.reconcile",
                        "cron": "* * * * *",
                    },
                },
            },
        },
        "flows": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["flowq"],
            "OPTIONS": dict(GATED),
        },
        "flows_deferred": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["deferq"],
            "OPTIONS": dict(GATED, DEFER_BLOCKED_NODES=True),
        },
    },
    LOGGING={
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {"format": "%(levelname)s %(name)s %(message)s"},
        },
        "handlers": {
            "stderr": {"class": "logging.StreamHandler", "formatter": "plain"},
        },
        "loggers": {
            "django_ox": {"handlers": ["stderr"], "level": "INFO"},
            "oxpull": {"handlers": ["stderr"], "level": "INFO"},
        },
    },
)

if mode == "manage":
    django.setup()
    from django.core.management import execute_from_command_line

    execute_from_command_line(["manage.py", *sys.argv[3:]])
    sys.exit(0)

name = cfg["database"]["NAME"]
engine = cfg["database"]["ENGINE"]
django.setup()
from django.db import connection

if "sqlite" not in engine:
    extra = ""
    if "mysql" in engine:
        extra = " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    quote = connection.ops.quote_name
    with connection._nodb_cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {quote(name)}")
        cursor.execute(f"CREATE DATABASE {quote(name)}{extra}")

result = {}
procs = []
try:
    import oxpull
    from django.core import checks
    from django.core.management import call_command
    from django_ox.models import OxTask

    result["oxpull_file"] = oxpull.__file__
    result["use_tz"] = settings.USE_TZ
    result["vendor"] = connection.vendor
    result["check_issues"] = [f"{m.id}: {m.msg}" for m in checks.run_checks()]
    call_command("migrate", verbosity=0)

    import naive_tz_tasks as t
    from oxpull import workflows
    from oxpull.models import OxpullWorkflow

    chain = workflows.create(
        "chain",
        on_complete=t.record, on_complete_args=["complete"],
        on_success=t.record, on_success_args=["success"],
    )
    chain.add("root", t.step, "chain-root")
    chain.add("child", t.step, "chain-child", depends_on="root")
    chain.seal()

    mixed = workflows.create("mixed")
    mixed.add("root", t.deferred_step, "mixed-root")
    mixed.add("child", t.step, "mixed-child", depends_on="root")
    mixed.seal()

    for alias in ("default", "flows", "flows_deferred"):
        log = open(HERE / f"worker-{alias}.log", "w")
        proc = subprocess.Popen(
            [sys.executable, __file__, "manage", cfg_path, "ox_worker",
             "--backend", alias, "--interval", "0.2"],
            stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append((alias, proc, log))

    def statuses():
        return {
            str(pk): status
            for pk, status in OxpullWorkflow.objects.values_list("pk", "status")
        }

    deadline = time.monotonic() + cfg["seconds"]
    started = time.monotonic()
    while time.monotonic() < deadline:
        now = statuses()
        wanted = OxpullWorkflow.Status.COMPLETE
        if all(now.get(str(f.id)) == wanted for f in (chain, mixed)):
            break
        time.sleep(0.25)
    result["waited_seconds"] = round(time.monotonic() - started, 1)
finally:
    for alias, proc, log in procs:
        proc.send_signal(signal.SIGTERM)
    for alias, proc, log in procs:
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        log.close()
        result.setdefault("exit_codes", {})[alias] = proc.returncode

try:
    def wf(flow):
        row = OxpullWorkflow.objects.get(pk=flow.id)
        return {
            "status": row.status,
            "callback_task_id": row.callback_task_id and str(row.callback_task_id),
            "success_task_id": row.success_task_id and str(row.success_task_id),
        }

    result["chain"] = wf(chain)
    result["mixed"] = wf(mixed)
    result["nodes"] = {
        args[0]: status
        for args, status in OxTask.objects.filter(
            queue_name__in=["flowq", "deferq"]
        ).values_list("args", "status")
    }
    ticks = list(
        OxTask.objects.filter(task_path="oxpull.workflows.reconcile")
        .order_by("enqueued_at")
        .values_list("status", "errors")
    )
    result["reconcile_ticks"] = dict(collections.Counter(s for s, _ in ticks))
    errored = [e for _, e in ticks if e]
    result["reconcile_last_error"] = str(errored[-1][-1])[-700:] if errored else None
    logs = {}
    for alias, proc, log in procs:
        text = (HERE / f"worker-{alias}.log").read_text()
        lines = text.splitlines()
        unhandled = [x for x in lines if "Unhandled error executing task" in x]
        logs[alias] = {
            "unhandled_error": len(unhandled),
            "unhandled_lines": unhandled[:2],
            "value_error_lines": [x for x in lines if x.startswith("ValueError")][:3],
            "succeeded_lines": [x for x in lines if " succeeded in " in x][:6],
        }
    result["worker_logs"] = logs

    blocked = workflows.create("blocked-add")
    blocked.add("root", t.deferred_step, "blocked-root")
    try:
        blocked.add("child", t.deferred_step, "blocked-child", depends_on="root")
        result["blocked_add"] = "ok"
    except Exception as exc:
        result["blocked_add"] = f"{type(exc).__name__}: {exc}"
    print("NAIVE-TZ-RESULT " + json.dumps(result), flush=True)
finally:
    if "sqlite" not in engine:
        connection.close()
        with connection._nodb_cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {connection.ops.quote_name(name)}")
"""

DRIVER_TASKS = """
from django.tasks import task


@task(backend="flows", queue_name="flowq")
def step(label):
    return label


@task(backend="flows_deferred", queue_name="deferq")
def deferred_step(label):
    return label


@task(queue_name="default")
def record(label):
    return label
"""


@pytest.fixture(scope="module")
def real_project(django_db_setup, tmp_path_factory):
    here = tmp_path_factory.mktemp("naive_tz_project")
    source = connections["default"].settings_dict
    database = {
        key: source[key]
        for key in ("ENGINE", "USER", "PASSWORD", "HOST", "PORT", "OPTIONS")
        if key in source
    }
    if connections["default"].vendor == "sqlite":
        database["NAME"] = str(here / "naive.sqlite3")
        database["OPTIONS"] = {"timeout": 20}
    else:
        database["NAME"] = f"{source['NAME']}_naive"
    # "seconds" is a ceiling on three ox_worker interpreters starting cold and
    # finishing both workflows. The driver stops waiting the moment both are
    # COMPLETE, so a passing run never waits it out.
    (here / "config.json").write_text(
        json.dumps({"database": database, "seconds": 120})
    )
    (here / "driver.py").write_text(DRIVER)
    (here / "naive_tz_tasks.py").write_text(DRIVER_TASKS)
    env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(REPO_ROOT / "src"), env.get("PYTHONPATH")])
    )
    proc = subprocess.run(  # noqa: S603
        [sys.executable, str(here / "driver.py"), "drive", str(here / "config.json")],
        capture_output=True,
        text=True,
        env=env,
        cwd=here,
        # Past the driver's own worst case, so a failing run still reports:
        # the wait above, then up to a minute for each worker to stop.
        timeout=420,
    )
    marker = "NAIVE-TZ-RESULT "
    found = [line for line in proc.stdout.splitlines() if line.startswith(marker)]
    assert found, (
        f"driver exited {proc.returncode} with no result\n"
        f"stdout:\n{proc.stdout[-4000:]}\nstderr:\n{proc.stderr[-4000:]}"
    )
    return json.loads(found[-1][len(marker) :])


def _assert_supported(result):
    """Preconditions: this is the worktree's oxpull, in a configuration
    that raises no error and no workflow or timezone warning."""
    print(json.dumps(result, indent=2))  # noqa: T201
    assert result["use_tz"] is False
    assert result["oxpull_file"].startswith(str(REPO_ROOT / "src")), result[
        "oxpull_file"
    ]
    blocking = [
        issue
        for issue in result["check_issues"]
        if ".E" in issue.split(":")[0]
        or issue.startswith(("django_ox.W001", "oxpull.W002", "oxpull.W003"))
    ]
    assert not blocking, blocking


def test_ox_worker_completes_sealed_workflows_in_a_use_tz_false_project(real_project):
    _assert_supported(real_project)
    nodes = real_project["nodes"]
    assert nodes.get("chain-root") == nodes.get("chain-child") == "SUCCESSFUL", nodes
    assert real_project["chain"]["status"] == "COMPLETE", (
        f"after {real_project['waited_seconds']}s of ox_worker with the reconciler "
        f"scheduled every second: chain={real_project['chain']} "
        f"mixed={real_project['mixed']} reconcile ticks="
        f"{real_project['reconcile_ticks']} last tick error: "
        f"{real_project['reconcile_last_error']}"
    )
    assert real_project["chain"]["callback_task_id"]
    assert real_project["chain"]["success_task_id"]
    assert real_project["mixed"]["status"] == "COMPLETE", real_project["mixed"]


def test_ox_worker_finishing_a_deferring_upstream_reports_it(real_project):
    _assert_supported(real_project)
    assert real_project["nodes"].get("mixed-root") == "SUCCESSFUL", real_project[
        "nodes"
    ]
    deferq = real_project["worker_logs"]["flows_deferred"]
    assert deferq["unhandled_error"] == 0, deferq
    assert deferq["succeeded_lines"], deferq


def test_add_of_a_blocked_node_in_a_use_tz_false_project(real_project):
    _assert_supported(real_project)
    assert real_project["blocked_add"] == "ok", real_project["blocked_add"]
