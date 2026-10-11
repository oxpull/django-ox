"""
MariaDB, after a server upgrade from 10.6 to 10.7 or later.

Django picks a UUIDField's column type from the server version when a
migration runs: char(32) below MariaDB 10.7, native uuid from 10.7 on. It
never alters an existing column. It also picks how to bind a UUID
parameter from the server version, but it does that on every connection:
hex below 10.7, a hyphenated string from 10.7 on.

A project that migrated django_ox on 10.6 and then upgraded the server
keeps django_ox_oxtask.id as char(32). `migrate oxpull` run after the
upgrade creates the workflow edge and node columns as uuid. The hazard as
written asks what the workflow joins then do across the two types.

These tests only mean something against a database built that way:
mariadb:10.6 on a volume, `migrate django_ox` and `migrate oxpull 0003`,
then mariadb:10.11 on the same volume, `mariadb-upgrade`, then
`migrate oxpull`, with a settings module that points at that server.
Run with --reuse-db, or pytest-django replaces that database with a fresh
one where every column is uuid. Controls where every test should pass: a
fresh 10.11 database and a fresh 10.6 server. The suite's other legs skip
the file, because MySQL has no native uuid type and PostgreSQL and SQLite
use one type on both sides.

Two of these tests hold UUID binding to hex, which is how a pre-10.7
connection wrote its rows (see `hex_binding`). Each one says why.
"""

import json
import re
from contextlib import contextmanager

import pytest
from django.db import connection
from django.tasks import default_task_backend

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import flow_worker, reconcile, stored
from .test_workflows_cost import claim_candidates

pytestmark = pytest.mark.skipif(
    connection.vendor != "mysql",
    reason="MariaDB only: the hazard needs native uuid beside char(32)",
)

BLOCKED = 400


@pytest.fixture(autouse=True)
def mariadb():
    if not connection.mysql_is_mariadb:
        pytest.skip("MariaDB only: MySQL has no native uuid column type")


def column_type(table: str, column: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_type FROM information_schema.columns"
            " WHERE table_schema = DATABASE() AND table_name = %s"
            " AND column_name = %s",
            [table, column],
        )
        return cursor.fetchone()[0]


def join_columns() -> dict[str, str]:
    """Every column the gate, the counts and the release compare."""
    edge = OxpullWorkflowEdge._meta
    node = OxpullWorkflowNode._meta
    return {
        "oxtask.id": column_type(OxTask._meta.db_table, OxTask._meta.pk.column),
        "edge.upstream_task_id": column_type(edge.db_table, "upstream_task_id"),
        "edge.downstream_task_id": column_type(edge.db_table, "downstream_task_id"),
        "node.task_id": column_type(node.db_table, "task_id"),
    }


def premise() -> str:
    return (
        f"server {connection.mysql_version}, "
        f"native uuid binding {connection.features.has_native_uuid_field}, "
        f"columns {join_columns()}"
    )


@contextmanager
def hex_binding():
    """
    Bind UUID parameters as 32 hex digits, as Django does below MariaDB 10.7.

    This is a shortcut. The production sequence that writes rows this way
    is django-ox enqueueing on 10.6 before the upgrade.
    It is also how every write would
    bind if django-ox adopted Django 5.0's documented remedy for existing
    UUIDFields on MariaDB 10.7+ (a char(32) subclass). That remedy is the
    only way to get workflow rows beside char(32) task ids on 10.7+,
    because an unpatched connection cannot insert a task at all.
    """
    features = connection.features
    original = features.has_native_uuid_field
    features.__dict__["has_native_uuid_field"] = False
    try:
        yield
    finally:
        features.__dict__["has_native_uuid_field"] = original


def root_and_child(name: str):
    """
    root -> child, with priorities that put the child first in claim order.
    The only thing that can hold the child back is the gate.
    """
    flow = workflows.create(name)
    root = flow.add("root", tasks.step.using(priority=0), "root")
    child = flow.add("child", tasks.step.using(priority=10), "child", depends_on="root")
    flow.seal()
    return flow, root, child


def assert_child_is_held(flow, root, child) -> None:
    claimed = flow_worker().claim_one()
    assert claimed is not None, premise()
    # TaskResult.id is a str; OxTask.pk is a UUID.
    assert str(claimed.pk) == str(root.id), (
        f"claimed {claimed.pk} ({claimed.args}), expected the root; {premise()}"
    )
    assert flow_worker().claim_one() is None, premise()
    counts = workflows._node_task_counts(flow.id, "default")
    assert counts == {"RUNNING": 1, "READY": 1}, f"{counts}; {premise()}"
    assert reconcile() == 0, premise()
    assert stored(flow).status == OxpullWorkflow.Status.SEALED, premise()


# -- real path: the schema and the public API ------------------------------


@pytest.mark.django_db
def test_the_gate_join_columns_have_one_type():
    """
    The root of the hazard, read from the schema that the real migrate
    commands left behind. Every column the gate compares should have one type.
    """
    columns = join_columns()
    assert len(set(columns.values())) == 1, premise()


@pytest.mark.django_db
def test_a_task_can_be_enqueued_after_the_upgrade():
    """
    Below the workflows code: the `django_ox.backend.OxBackend`'s own enqueue. On the
    upgraded schema Django binds a 36-character id into char(32), and strict
    mode refuses it with error 1406.
    """
    result = tasks.plain_echo.enqueue("after-the-upgrade")
    assert default_task_backend.get_result(result.id).status == "READY", premise()


@pytest.mark.django_db
def test_a_workflow_built_after_the_upgrade_holds_its_child_behind_a_running_root():
    """
    The upgrade scenario, driven through the public API: create, add,
    seal, and a real claim sets the root RUNNING. The child must stay
    unclaimable, the counts must read one RUNNING and one READY, and the
    reconciler must leave the workflow SEALED.
    """
    flow, root, child = root_and_child("after-upgrade")
    assert_child_is_held(flow, root, child)


# -- shortcut: rows written with pre-10.7 binding ----------------------------


@pytest.mark.django_db
def test_a_task_enqueued_before_the_upgrade_is_still_found_by_id():
    """
    Written with the binding the 10.6 server used (hex), read back with the
    binding the upgraded server uses (hyphenated). get_result on a task
    enqueued before a database upgrade should still find it.
    """
    with hex_binding():
        result = tasks.plain_echo.enqueue("before-the-upgrade")
    assert OxTask.objects.count() == 1, premise()
    assert default_task_backend.get_result(result.id).status == "READY", premise()


@pytest.mark.django_db
def test_the_gate_compares_char32_task_ids_with_uuid_edges_by_value():
    """
    The hazard's outcome (b): if MariaDB compared the two columns as strings,
    a hyphenated uuid would never equal bare hex, every upstream would read as
    absent and so satisfied, and the child would be claimed first.

    Binding is held to hex for the whole test, so that the enqueue failure
    above cannot hide the answer. Nothing inside the gate, the counts or the
    reconciler's completion check binds a task id: they compare columns with
    columns, and that comparison is the question.
    """
    with hex_binding():
        flow, root, child = root_and_child("by-value")
        assert_child_is_held(flow, root, child)


def _plan_tables(node, out: list[dict]) -> list[dict]:
    if isinstance(node, dict):
        if "table_name" in node:
            out.append(node)
        for value in node.values():
            _plan_tables(value, out)
    elif isinstance(node, list):
        for value in node:
            _plan_tables(value, out)
    return out


@pytest.mark.django_db(transaction=True)
def test_the_gated_claim_probes_each_upstream_by_primary_key():
    """
    The hazard's outcome (a): the comparison is right, but the inner lookup
    `U.id = edge.upstream_task_id` casts the char(32) side, so it cannot use
    PRIMARY and scans django_ox_oxtask once for every edge the claim checks.

    transaction=True because ANALYZE TABLE commits implicitly on MariaDB.
    The rows are written with hex binding, for the reason `hex_binding` gives.
    """
    with hex_binding():
        flow = workflows.create("cost")
        flow.add("root", tasks.step.using(priority=0), "root")
        for i in range(BLOCKED):
            flow.add(
                f"b{i:04d}",
                tasks.step.using(priority=10),
                f"b{i:04d}",
                depends_on="root",
            )
        flow.seal()
    with connection.cursor() as cursor:
        cursor.execute(
            "ANALYZE TABLE django_ox_oxtask, oxpull_oxpullworkflowedge,"
            " oxpull_oxpullworkflownode"
        )
        cursor.fetchall()

    queryset = claim_candidates(gated=True)
    sql = str(queryset.query)
    # The outer FROM is followed by WHERE; the gate's inner lookup has an alias.
    upstream_alias = re.search(r"FROM `django_ox_oxtask` (?!WHERE\b)(\w+)", sql).group(
        1
    )
    plan = json.loads(queryset.explain(format="json", analyze=True))
    if isinstance(plan, str):
        # Django's MySQL backend hands MariaDB's JSON back quoted as a string.
        plan = json.loads(plan)
    tables = _plan_tables(plan, [])
    upstream = [t for t in tables if t["table_name"] == upstream_alias]
    assert len(upstream) == 1, tables
    probe = upstream[0]
    read = probe.get("r_loops", 0) * probe.get("r_rows", 0)
    summary = {
        t["table_name"]: (
            t.get("access_type"),
            t.get("key"),
            t.get("r_loops"),
            t.get("r_rows"),
        )
        for t in tables
    }
    assert (probe.get("access_type"), probe.get("key")) == ("eq_ref", "PRIMARY"), (
        f"upstream probe {upstream_alias}: {probe.get('access_type')} on "
        f"{probe.get('key')}, ~{read:.0f} rows read over {BLOCKED} blocked nodes; "
        f"plan {summary}; {premise()}"
    )
