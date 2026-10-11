"""
MySQL: workflow task-id columns on a different collation from
django_ox_oxtask.id make every gated claim raise error 1267.

How a project gets there. `migrate` creates a table on the database's default
collation at that moment, and mysqldump writes an explicit COLLATE= into every
table it restores. Restore django-ox's tables from a 5.7-era database
(utf8mb4_unicode_ci or utf8mb4_general_ci) into a database created on 8.x
(default utf8mb4_0900_ai_ci), and task ids keep the old collation while
migration 0004 creates the edge and node tables on the new one. ALTER DATABASE
... COLLATE produces the same split, and so does a DBA converting django-ox's
tables to the new default after 0004 has run.

The gate compares the edge columns with django_ox_oxtask.id inside the claim.
MySQL refuses to compare two IMPLICIT columns of one character set on
different collations, and it refuses while resolving the statement, before a
row is read. So by reading, every claim on a WORKFLOWS backend fails, whether
or not a workflow exists.

Every test asserts the correct behaviour. The in-process tests admit either
fix direction:
  * the gate works whatever the columns' collations, so the claim returns the
    right task, or
  * a system check run against the database (`manage.py check --database
    default`, which `migrate` also runs) names the collation mismatch.
The ox_worker process test is stricter, and says why at the test.

Migration 0004 now creates the four task-id columns on OxTask.id's
collation, oxpull.E009 reports a later split from `check --database`, and
an OxpullWorker on a WORKFLOWS backend refuses to start on one.

MySQL only. PostgreSQL stores a UUIDField as uuid, which has no collation, and
SQLite never raises on a collation mix.
"""

import os
import re
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from django.conf import settings
from django.core import checks
from django.core.management import call_command
from django.db import DatabaseError, connection

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .subprocesses import django_ox_path
from .test_upgrade import _latest_migration, _migrate
from .test_workflows import drain, flow_worker, order

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

# The migration before Workflows. Rewinding to it drops the workflow tables.
BEFORE_WORKFLOWS = "0003_oxpullratewindow"

# MySQL 8.x's default collation for utf8mb4, and 5.7's.
NEW_DEFAULT = "utf8mb4_0900_ai_ci"
OLD_DEFAULT = "utf8mb4_general_ci"

TASK_TABLE = OxTask._meta.db_table
TASK_ID = OxTask._meta.pk.column
EDGE_TABLE = OxpullWorkflowEdge._meta.db_table
NODE_TABLE = OxpullWorkflowNode._meta.db_table

# The two columns the claim's gate compares with a task id. Neither carries a
# foreign key, so moving their collation leaves the schema otherwise valid.
EDGE_TASK_ID_COLUMNS = [
    (EDGE_TABLE, OxpullWorkflowEdge._meta.get_field("upstream_task_id").column),
    (EDGE_TABLE, OxpullWorkflowEdge._meta.get_field("downstream_task_id").column),
]
# Reported in failure messages: 0004 creates this one on the default as well.
NODE_TASK_ID_COLUMN = (NODE_TABLE, OxpullWorkflowNode._meta.get_field("task_id").column)


@pytest.fixture(autouse=True)
def mysql_only():
    if connection.vendor != "mysql":
        pytest.skip(
            "MySQL only: a uuid column has no collation on PostgreSQL, and "
            "SQLite does not raise on a collation mix"
        )


# -- schema helpers ------------------------------------------------------


def column_info(table, column):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLUMN_TYPE, IS_NULLABLE, COLLATION_NAME"
            " FROM information_schema.COLUMNS"
            " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
            " AND COLUMN_NAME = %s",
            [table, column],
        )
        row = cursor.fetchone()
    assert row is not None, f"no column {table}.{column}"
    return row


def collation_of(table, column):
    return column_info(table, column)[2]


def collations():
    """The task id and every workflow task-id column, for failure messages."""
    return {
        f"{table}.{column}": collation_of(table, column)
        for table, column in [
            (TASK_TABLE, TASK_ID),
            *EDGE_TASK_ID_COLUMNS,
            NODE_TASK_ID_COLUMN,
        ]
    }


def set_collation(table, column, collation):
    column_type, nullable, _ = column_info(table, column)
    charset = collation.split("_", 1)[0]
    null = "NULL" if nullable == "YES" else "NOT NULL"
    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER TABLE `{table}` MODIFY `{column}` {column_type}"
            f" CHARACTER SET {charset} COLLATE {collation} {null}"
        )


@contextmanager
def edge_columns_on(collation):
    """Move both edge task-id columns to `collation`, and back afterwards."""
    original = {(t, c): collation_of(t, c) for t, c in EDGE_TASK_ID_COLUMNS}
    try:
        for table, column in EDGE_TASK_ID_COLUMNS:
            set_collation(table, column, collation)
        # About this setup only: the DDL did what it says.
        assert {collation_of(t, c) for t, c in EDGE_TASK_ID_COLUMNS} == {collation}
        yield
    finally:
        for (table, column), before in original.items():
            set_collation(table, column, before)


def a_collation_other_than_the_task_ids(preferred):
    task_collation = collation_of(TASK_TABLE, TASK_ID)
    if preferred == task_collation:
        pytest.skip(f"task ids are already on {preferred}; nothing to split")
    return preferred


def schema_default():
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA"
            " WHERE SCHEMA_NAME = DATABASE()"
        )
        return cursor.fetchone()[0]


def set_schema_default(collation):
    name = connection.settings_dict["NAME"]
    with connection.cursor() as cursor:
        cursor.execute(f"ALTER DATABASE `{name}` COLLATE {collation}")


def collation_reported():
    """
    What `manage.py check --database default` says about collations, at
    Warning level or above. `migrate` runs the same check with the same
    database before it applies anything.

    oxpull's database-tagged check is called directly, not through
    checks.run_checks(), whose other registered checks depend on which
    modules earlier tests imported. A test that puts django.contrib.auth
    in INSTALLED_APPS registers that app's checks for the rest of the
    process, and they raise once it is out of INSTALLED_APPS again.
    E009 is the only collation message either way.
    """
    from oxpull.checks import check_workflow_schema

    return [
        f"{message.id}: {message.msg}"
        for message in check_workflow_schema(databases=["default"])
        if message.level >= checks.WARNING
        and "collation" in f"{message.msg} {message.hint or ''}".lower()
    ]


def add_root_and_child(name):
    """
    root(priority 0) -> child(priority 10). An ungated claim reaches the child
    first, so only the gate can put root first.
    """
    flow = workflows.create(name)
    flow.add("root", tasks.step.using(priority=0), "root")
    flow.add("child", tasks.step.using(priority=10), "child", depends_on="root")
    flow.seal()
    return flow


def describe(error, stage):
    if error is None:
        return "no error"
    return f"{type(error).__name__} at {stage}: {error}"


# -- the claim -----------------------------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("collation", [NEW_DEFAULT, OLD_DEFAULT])
def test_a_gated_claim_runs_the_workflow_whatever_collation_the_edge_columns_are_on(
    collation,
):
    """Edge columns on another collation, then a real gated drain."""
    collation = a_collation_other_than_the_task_ids(collation)
    error = None
    stage = "create"
    with edge_columns_on(collation):
        try:
            add_root_and_child(f"split-{collation}")
            stage = "claim"
            drain(flow_worker())
        except DatabaseError as exc:
            error = exc
        reported = collation_reported()
        snapshot = collations()

    assert reported or (error is None and order() == ["root", "child"]), (
        f"OxpullWorker(backend_alias='flows') ran {order()} with "
        f"{describe(error, stage)}. Columns: {snapshot}. "
        f"Collation checks from `check --database default`: {reported}"
    )


@pytest.mark.django_db(transaction=True)
def test_a_plain_task_on_a_workflows_backend_runs_despite_the_split():
    """
    No workflow exists at all. The gate still sits in every claim on a
    WORKFLOWS backend, so a plain task there is exposed as well.
    """
    collation = a_collation_other_than_the_task_ids(NEW_DEFAULT)
    error = None
    ran = False
    with edge_columns_on(collation):
        # About this setup only: nothing workflow-shaped is in the database.
        assert not OxpullWorkflowEdge.objects.exists()
        tasks.step.enqueue("plain")
        try:
            ran = flow_worker().run_once()
        except DatabaseError as exc:
            error = exc
        reported = collation_reported()
        snapshot = collations()

    assert reported or (error is None and ran and order() == ["plain"]), (
        f"a plain task on 'flows' with no workflow in the database: ran={ran}, "
        f"order={order()}, {describe(error, 'claim')}. Columns: {snapshot}. "
        f"Collation checks: {reported}"
    )


# -- the production sequence: migrate after the default changed -----------


@pytest.mark.django_db(transaction=True)
def test_migrate_after_the_default_collation_changed_leaves_claims_working():
    """
    Not an ALTER on oxpull's own columns: the real `migrate` creates the
    workflow tables on a database whose default collation is no longer the one
    django-ox's tables carry, the state a mysqldump restore into a database
    created on 8.x leaves behind.
    """
    task_collation = collation_of(TASK_TABLE, TASK_ID)
    new_default = NEW_DEFAULT if task_collation != NEW_DEFAULT else OLD_DEFAULT
    original_default = schema_default()
    latest = _latest_migration()
    error = None
    stage = "migrate"
    _migrate(BEFORE_WORKFLOWS)
    try:
        set_schema_default(new_default)
        # About this setup only: django-ox's table kept its collation, and the
        # database now defaults to another one.
        assert collation_of(TASK_TABLE, TASK_ID) == task_collation
        assert schema_default() == new_default
        # skip_checks: the suite's settings carry flows_ungated, a deliberate
        # oxpull.E007 fixture that would stop `migrate` before it applied
        # anything. The checks are run below, against the migrated schema.
        call_command("migrate", "oxpull", skip_checks=True, verbosity=0)
        snapshot = collations()
        try:
            stage = "create"
            add_root_and_child("restored")
            stage = "claim"
            drain(flow_worker())
        except DatabaseError as exc:
            error = exc
        reported = collation_reported()
    finally:
        # Migration 0004 refuses to unapply while a workflow row exists, so
        # the rows this test made go first, children before their workflow.
        for model in (OxpullWorkflowEdge, OxpullWorkflowNode, OxpullWorkflow):
            model.objects.all().delete()
        _migrate(BEFORE_WORKFLOWS)
        set_schema_default(original_default)
        _migrate(latest)

    assert reported or (error is None and order() == ["root", "child"]), (
        f"after `migrate oxpull` on a database defaulting to {new_default} with "
        f"task ids on {task_collation}: ran {order()} with "
        f"{describe(error, stage)}. Columns: {snapshot}. "
        f"Collation checks: {reported}"
    )


# -- a real ox_worker process --------------------------------------------


def process_env(tmp_path):
    """
    The suite's settings for a separate process on this test database, with
    DEBUG django_ox logs (the "Claimed task" lines). flows_ungated stops
    declaring WORKFLOWS: it is the suite's deliberate oxpull.E007 fixture and
    would stop ox_worker at start-up, which has nothing to do with this hazard.
    """
    (tmp_path / "collation_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)  # noqa: F405\n"
        "TASKS['flows_ungated']['OPTIONS']['WORKFLOWS'] = False\n"
        "LOGGING = {\n"
        "    'version': 1,\n"
        "    'disable_existing_loggers': False,\n"
        "    'handlers': {'console': {'class': 'logging.StreamHandler'}},\n"
        "    'loggers': {'django_ox': {'handlers': ['console'], 'level': 'DEBUG'}},\n"
        "}\n"
    )
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "collation_settings",
        "PYTHONPATH": os.pathsep.join(
            [str(SRC), django_ox_path(), str(tmp_path), str(REPO_ROOT)]
        ),
        "OX_TEST_DB_NAME": str(connection.settings_dict["NAME"]),
    }


def stop(process):
    """SIGTERM, as a deploy does; the worker drains and exits."""
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    return process.returncode


@pytest.mark.django_db(transaction=True)
def test_an_ox_worker_process_runs_its_tasks_or_refuses_to_start(
    tmp_path,
):
    """
    What a user's worker does on the split schema.

    Correct is: it runs the workflow in order and the plain task, or it exits
    non-zero on its own and names the collation. A check that only `migrate`
    or `check --database` prints does not pass this test, deliberately:
    ox_worker runs no database-tagged check, and the first `migrate` runs its
    checks before 0004 has created the table there is to inspect.
    """
    collation = a_collation_other_than_the_task_ids(NEW_DEFAULT)
    env = process_env(tmp_path)
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import django; django.setup(); import oxpull; "
                "from django_ox.worker import worker_class; "
                "cls = worker_class('flows'); print(oxpull.__file__); "
                "print(cls.__module__ + '.' + cls.__qualname__)"
            ),
        ],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    lines = probe.stdout.split()
    assert len(lines) == 2, probe.stdout + probe.stderr
    assert lines[0].startswith(str(SRC)), lines
    assert lines[1] == "oxpull.worker.OxpullWorker", lines

    log_path = tmp_path / "ox_worker.log"
    with edge_columns_on(collation):
        # Evidence only, not asserted: what an operator's own check says about
        # this schema, in a fresh interpreter.
        check = subprocess.run(
            [sys.executable, "-m", "django", "check", "--database", "default"],
            env=env,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        flow = add_root_and_child("process")
        plain = tasks.step.enqueue("plain")
        ids = {**flow.node_ids(), "plain": plain.id}
        label_of = {str(task_id): label for label, task_id in ids.items()}
        with log_path.open("w") as log:
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "django",
                    "ox_worker",
                    "--backend",
                    "flows",
                    "--concurrency",
                    "1",
                    "--interval",
                    "0.05",
                ],
                env=env,
                cwd=REPO_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline and worker.poll() is None:
                    unfinished = OxTask.objects.filter(pk__in=ids.values()).exclude(
                        status=OxTask.Status.SUCCESSFUL
                    )
                    if not unfinished.exists():
                        break
                    time.sleep(0.2)
                alive_at_the_end = worker.poll() is None
            finally:
                code = stop(worker)
        statuses = {
            label: OxTask.objects.get(pk=task_id).status
            for label, task_id in ids.items()
        }
        snapshot = collations()

    text = log_path.read_text()
    claimed = [
        label_of[task_id]
        for task_id in re.findall(r"Claimed task id=([0-9a-f-]{36}) ", text)
        if task_id in label_of
    ]
    poll_failures = text.count("could not reach the database this pass")
    first_mix = next(
        (line for line in text.splitlines() if "Illegal mix of collations" in line),
        None,
    )

    ran_in_order = (
        set(statuses.values()) == {OxTask.Status.SUCCESSFUL}
        and {"root", "child"} <= set(claimed)
        and claimed.index("root") < claimed.index("child")
    )
    refused_to_start = (
        not alive_at_the_end and code != 0 and "collation" in text.lower()
    )
    assert ran_in_order or refused_to_start, (
        f"`ox_worker --backend flows` on columns {snapshot}: statuses {statuses}, "
        f"claimed {claimed}, still running at the deadline: {alive_at_the_end}, "
        f"exit {code} after SIGTERM. {poll_failures} poll passes logged "
        f"'could not reach the database this pass'; first 1267 line: {first_mix!r}. "
        f"`check --database default` in that environment: exit {check.returncode}, "
        f"{(check.stdout + check.stderr).strip()!r}"
    )


# -- the other workflow reads that carry the gate ---------------------------


@pytest.mark.django_db(transaction=True)
def test_progress_and_the_reconciler_work_whatever_collation_the_edge_columns_are_on(
    caplog,
):
    """
    progress() counts blocked nodes with the gate, and the reconciler's
    release backstop reads the edges on every tick. On a sealed root ->
    child with root still READY, correct is blocked == 1, and a reconcile
    pass that completes nothing, raises nothing and logs no failure.
    """
    collation = a_collation_other_than_the_task_ids(NEW_DEFAULT)
    flow = add_root_and_child("reads")
    errors = {}
    progress = completed = None
    with edge_columns_on(collation):
        try:
            progress = flow.progress()
        except DatabaseError as exc:
            errors["progress"] = f"{type(exc).__name__}: {exc}"
        try:
            completed = workflows._reconcile()
        except DatabaseError as exc:
            errors["reconcile"] = f"{type(exc).__name__}: {exc}"
        reported = collation_reported()
        snapshot = collations()
    failures_logged = [
        record.getMessage()
        for record in caplog.records
        if getattr(record, "event", "") == "workflow_reconcile_failed"
    ]

    correct = (
        not errors
        and progress is not None
        and progress.blocked == 1
        and completed == 0
        and not failures_logged
    )
    assert reported or correct, (
        f"progress={progress}, reconcile completed={completed}, errors={errors}, "
        f"reconcile failures logged={failures_logged}. Columns: {snapshot}. "
        f"Collation checks: {reported}"
    )


# -- control -------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("target", ["task-id-collation", "utf8mb4_bin"])
def test_control_the_same_alter_to_a_compatible_collation_keeps_the_gate_working(
    target,
):
    """
    Passes today. The same DDL, to the task ids' own collation and to a binary
    collation MySQL lets win a comparison, leaves the gated drain correct. So
    the failures above are the collation split, not the ALTER or the fixture.
    """
    collation = collation_of(TASK_TABLE, TASK_ID) if target != "utf8mb4_bin" else target
    with edge_columns_on(collation):
        add_root_and_child(f"control-{target}")
        drain(flow_worker())
    assert order() == ["root", "child"]
