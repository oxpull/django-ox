"""
Rolling Workflows back must not strand the nodes that wait.

A workflow node whose upstream has not succeeded is a django-ox row in the
WAITING status. Only this version releases it: the OxpullWorker that writes
its upstream's SUCCESSFUL, and the workflow reconciler. oxpull 1.2.0 and the
`django_ox.worker.Worker` never claim, release or reap a WAITING row.

Two routine ways back off the feature would remove both:

  * `manage.py migrate oxpull 0003_oxpullratewindow`, then running the
    worker that remains: oxpull 1.2.0 or the `django_ox.worker.Worker`. The
    rewind would drop the workflow tables and leave the task rows WAITING.
  * leaving the schema alone and taking WORKFLOWS and the reconcile schedule
    out of settings. oxpull.W003 is emitted only by a backend that declares
    WORKFLOWS, so with none left it cannot warn.

Each hazard test asserts an acceptable outcome: the child runs once its
parent has succeeded, or the rollback refuses before it drops a table, or
(for the settings route) a database check reports the waiting nodes.
Migration 0004 refuses to unapply while any workflow row exists, and
oxpull.W007 counts workflow nodes left WAITING with no backend declaring
WORKFLOWS. The controls show the refusal leaves a working install, and that
a rollback before the first workflow goes through.
"""

from __future__ import annotations

import io
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError, connection

import oxpull
from django_ox import actions
from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from ._historical import extract_released_oxpull
from .subprocesses import django_ox_path
from .test_upgrade import _latest_migration, _migrate
from .test_workflows import drain
from .test_workflows_deferred import chain

REPO_ROOT = Path(__file__).resolve().parent.parent
BEFORE_WORKFLOWS = "0003_oxpullratewindow"
WORKFLOW_TABLES = tuple(
    model._meta.db_table
    for model in (OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode)
)
# How long the remaining worker gets to run the child once the parent has
# succeeded. A released child is claimed on the next poll (0.1 s here).
GRACE_SECONDS = 5.0


def status_of(label):
    return OxTask.objects.filter(args=[label]).values_list("status", flat=True).first()


@pytest.fixture
def restore_leaf():
    """Put oxpull back on its newest migration, even when the test fails."""
    latest = _latest_migration()
    try:
        yield
    finally:
        _migrate(latest)


def roll_back():
    """
    `manage.py migrate oxpull 0003_oxpullratewindow`, the real command.

    Returns what it raised, or None. A rewind that refuses while nodes are
    deferred is one of the acceptable fixes, so a refusal goes back to the
    test. A database error is not a refusal and propagates.
    """
    try:
        call_command(
            "migrate", "oxpull", BEFORE_WORKFLOWS, interactive=False, verbosity=0
        )
    except DatabaseError:
        raise
    except Exception as exc:
        return exc
    return None


def assert_refused_before_dropping_anything(refusal):
    tables = connection.introspection.table_names()
    missing = [table for table in WORKFLOW_TABLES if table not in tables]
    assert not missing, f"the rewind raised {refusal!r} after dropping {missing}"


def assert_rewound():
    tables = connection.introspection.table_names()
    present = [table for table in WORKFLOW_TABLES if table in tables]
    assert not present, f"premise: the rewind should drop {present}"


def evidence(label="child"):
    """What an operator has to go on, read only when an assertion fails."""
    row = OxTask.objects.get(args=[label])
    health = io.StringIO()
    try:
        call_command("ox_health", "--max-backlog", "0", "--max-age", "1", stdout=health)
        health_says = health.getvalue().strip()
    except CommandError as exc:
        health_says = f"CommandError: {exc}"
    parked = OxTask.objects.filter(status=OxTask.Status.WAITING).count()
    return (
        f"{label!r} is {row.status} with run_after="
        f"{row.run_after.isoformat() if row.run_after else None}; "
        f"WAITING rows: {parked}; "
        f"django_ox.actions.retry({label!r}) -> {actions.retry(row.pk)}; "
        f"ox_health --max-backlog 0 --max-age 1 -> {health_says!r}"
    )


# -- rollback by migration, the `django_ox.worker.Worker` in process -------


@pytest.mark.django_db(transaction=True)
def test_after_migrating_back_to_0003_the_free_worker_runs_the_deferred_child(
    restore_leaf,
):
    chain("rollback-free")
    assert status_of("child") == OxTask.Status.WAITING, "premise: child waiting"

    refusal = roll_back()
    if refusal is not None:
        assert_refused_before_dropping_anything(refusal)
        return
    assert_rewound()

    drain(Worker(backend_alias="flows_deferred", backoff_initial=0, poll_interval=0.01))

    assert status_of("parent") == OxTask.Status.SUCCESSFUL, "premise: parent ran"
    assert status_of("child") == OxTask.Status.SUCCESSFUL, evidence()


@pytest.mark.django_db(transaction=True)
def test_control_before_the_first_workflow_the_same_rollback_goes_through(
    restore_leaf,
):
    """
    With no workflow row, nothing can be stranded, so the rewind drops the
    workflow tables and a worker without workflows runs the queue.
    """
    refusal = roll_back()
    assert refusal is None, f"the rewind refused with no workflow present: {refusal!r}"
    assert_rewound()

    tasks.default_step.enqueue("plain")
    drain(Worker(backoff_initial=0, poll_interval=0.01))
    assert status_of("plain") == OxTask.Status.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_control_after_the_refused_rollback_the_workflow_finishes(restore_leaf):
    """
    A refused rewind leaves the schema and the rows as they were, so the
    workflow's own workers finish it.
    """
    chain("rollback-refused-then-run")
    refusal = roll_back()
    assert refusal is not None, "premise: the rewind refused"
    assert_refused_before_dropping_anything(refusal)

    drain(Worker(backend_alias="flows_deferred", backoff_initial=0, poll_interval=0.01))
    assert status_of("parent") == OxTask.Status.SUCCESSFUL
    assert status_of("child") == OxTask.Status.WAITING
    from oxpull.worker import OxpullWorker

    drain(
        OxpullWorker(
            backend_alias="flows_deferred", backoff_initial=0, poll_interval=0.01
        )
    )
    workflows._reconcile()
    drain(
        OxpullWorker(
            backend_alias="flows_deferred", backoff_initial=0, poll_interval=0.01
        )
    )
    assert status_of("child") == OxTask.Status.SUCCESSFUL


# -- real processes ---------------------------------------------------------


@pytest.fixture(scope="module")
def released_oxpull(tmp_path_factory):
    """oxpull 1.2.0's source, read from the vendored archive."""
    root = tmp_path_factory.mktemp("oxpull-released")
    return extract_released_oxpull(root)


def process_env(tmp_path, src):
    """
    A separate process on this session's test database, with the settings
    an operator has after backing out of Workflows: every backend loses
    WORKFLOWS, DEFER_BLOCKED_NODES and the workflow reconciler schedule,
    which tests_oxpull.settings carries and oxpull 1.2.0 cannot import. django_ox
    logs at DEBUG so the worker's claims are in its log.
    """
    (tmp_path / "rollback_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)\n"
        "for _params in TASKS.values():\n"
        "    _params.get('OPTIONS', {}).pop('WORKFLOWS', None)\n"
        "    _params.get('OPTIONS', {}).pop('DEFER_BLOCKED_NODES', None)\n"
        "    _params.get('OPTIONS', {}).get('SCHEDULES', {}).pop(\n"
        "        'oxpull-workflow-reconcile', None\n"
        "    )\n"
        "LOGGING = {\n"
        "    'version': 1,\n"
        "    'disable_existing_loggers': False,\n"
        "    'handlers': {'console': {'class': 'logging.StreamHandler'}},\n"
        "    'loggers': {'django_ox': {'handlers': ['console'], 'level': 'DEBUG'}},\n"
        "}\n"
    )
    name = connection.settings_dict["NAME"]
    if connection.vendor == "sqlite":
        name = Path(name).resolve()
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "rollback_settings",
        # The current source runs against this process's django-ox. A released
        # oxpull runs against the django-ox the interpreter has installed.
        "PYTHONPATH": os.pathsep.join(
            [str(src)]
            + ([django_ox_path()] if src == REPO_ROOT / "src" else [])
            + [str(tmp_path), str(REPO_ROOT)]
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


def assert_imports(env, src, version):
    """Prove which oxpull the process runs before believing anything it does."""
    probe = run_python(
        env, "-c", "import oxpull; print(oxpull.__version__); print(oxpull.__file__)"
    )
    assert probe.returncode == 0, probe.stderr
    lines = probe.stdout.split()
    assert lines[0] == version, lines
    assert lines[1].startswith(str(src)), lines


def run_remaining_worker(env, log_path, backend):
    """
    `ox_worker --backend <backend>` until the child has succeeded, or for
    GRACE_SECONDS after the parent has. Returns (exit code, log text).
    """
    parent_done_at = None
    with log_path.open("w") as log:
        process = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "django",
                "ox_worker",
                "--backend",
                backend,
                "--interval",
                "0.1",
            ],
            env=env,
            cwd=REPO_ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and process.poll() is None:
                if status_of("child") == OxTask.Status.SUCCESSFUL:
                    break
                if parent_done_at is None:
                    if status_of("parent") == OxTask.Status.SUCCESSFUL:
                        parent_done_at = time.monotonic()
                elif time.monotonic() - parent_done_at > GRACE_SECONDS:
                    break
                time.sleep(0.1)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    return process.returncode, log_path.read_text()


@pytest.mark.django_db(transaction=True)
def test_after_migrating_back_to_0003_an_oxpull_1_2_0_worker_runs_the_deferred_child(
    restore_leaf, released_oxpull, tmp_path
):
    env = process_env(tmp_path, released_oxpull)
    assert_imports(env, released_oxpull, "1.2.0")

    chain("rollback-1.2.0")
    assert status_of("child") == OxTask.Status.WAITING, "premise: child waiting"

    refusal = roll_back()
    if refusal is not None:
        assert_refused_before_dropping_anything(refusal)
        return
    assert_rewound()

    check = run_python(env, "-m", "django", "check")
    code, log = run_remaining_worker(env, tmp_path / "ox_worker.log", "flows_deferred")

    assert status_of("parent") == OxTask.Status.SUCCESSFUL, (
        f"premise: the 1.2.0 worker ran the parent (exit {code})\n{log[-3000:]}"
    )
    assert status_of("child") == OxTask.Status.SUCCESSFUL, (
        f"{evidence()}; oxpull 1.2.0 `manage.py check`: "
        f"{(check.stdout + check.stderr).strip()!r}; worker exit {code}; "
        f"log tail:\n{log[-1500:]}"
    )


# -- rollback by settings, the schema left in place -------------------------


@pytest.mark.django_db(transaction=True)
def test_turning_workflows_off_runs_the_deferred_child_or_a_check_reports_it(
    tmp_path,
):
    src = REPO_ROOT / "src"
    env = process_env(tmp_path, src)
    assert_imports(env, src, oxpull.__version__)

    baseline = run_python(env, "-m", "django", "check", "--database", "default")

    # Created while the deployed settings still declare workflows, as in
    # production: the workflow is in flight when the deploy that turns it off
    # lands.
    chain("options-off")
    assert status_of("child") == OxTask.Status.WAITING, "premise: child waiting"

    code, log = run_remaining_worker(env, tmp_path / "ox_worker.log", "flows_deferred")
    assert status_of("parent") == OxTask.Status.SUCCESSFUL, (
        f"premise: the worker ran the parent (exit {code})\n{log[-3000:]}"
    )

    after = run_python(env, "-m", "django", "check", "--database", "default")
    said_before = (baseline.stdout + baseline.stderr).strip()
    said_after = (after.stdout + after.stderr).strip()

    child_ran = status_of("child") == OxTask.Status.SUCCESSFUL
    reported = said_after != said_before
    assert child_ran or reported, (
        f"{evidence()}; `manage.py check --database default` with the child "
        f"parked: {said_after!r} (before the workflow existed: {said_before!r}); "
        f"worker exit {code}; log tail:\n{log[-1500:]}"
    )
