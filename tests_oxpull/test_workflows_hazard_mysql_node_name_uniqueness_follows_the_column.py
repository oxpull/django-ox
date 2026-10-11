"""
A node's name must not be identified by the column's collation on MySQL.

A unique index on (workflow, name) used to enforce node-name uniqueness, so
the column's collation decided which names counted as the same. PostgreSQL
and SQLite compare bytes. MySQL's server default (utf8mb4_0900_ai_ci on 8.x)
folds case and accents, and utf8mb4_unicode_ci, which tests_oxpull/settings_mysql.py
creates, also pads trailing spaces. Uniqueness and depends_on now go through
name_key, the SHA-256 digest of the name's UTF-8 bytes, so a name is its
exact spelling on every database.

Every test here asserts the behaviour PostgreSQL and SQLite give, and runs
unskipped on every database, so the MySQL leg is the one that matters.
"""

import pytest
from django.db import connection

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.exceptions import UnknownWorkflowNode, WorkflowNodeExists
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import drain, flow_worker, order, reconcile, stored

pytestmark = pytest.mark.django_db

# Spellings that Python's == and a byte comparison keep apart. The first
# pair differs by case, the second by accents, the third by a trailing
# space, which only a PAD SPACE collation folds.
PAIRS = [
    pytest.param("Extract", "extract", id="case"),
    pytest.param("resume", "résumé", id="accent"),
    pytest.param("load", "load ", id="trailing-space"),
]


def name_collation():
    """What decides node-name identity on this connection, for messages."""
    if connection.vendor != "mysql":
        return f"{connection.vendor}: bytes"
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT c.collation_name, k.pad_attribute"
            " FROM information_schema.columns c"
            " JOIN information_schema.collations k"
            " ON k.collation_name = c.collation_name"
            " WHERE c.table_schema = DATABASE()"
            " AND c.table_name = %s AND c.column_name = 'name'",
            [OxpullWorkflowNode._meta.db_table],
        )
        ((collation, pad),) = cursor.fetchall()
    return f"mysql: {collation} ({pad})"


@pytest.mark.parametrize(("first", "second"), PAIRS)
def test_two_names_python_tells_apart_are_two_nodes(first, second):
    """
    add() of a second name that differs from the first by case, an accent
    or a trailing space admits a second node, as it does on PostgreSQL.

    On MySQL a unique index on the name column made the node INSERT hit
    oxpull_wf_node_name_uniq, and add() turned the IntegrityError into
    WorkflowNodeExists.
    """
    flow = workflows.create("fold")
    a = flow.add(first, tasks.step, "a")
    try:
        b = flow.add(second, tasks.step, "b")
    except WorkflowNodeExists as exc:
        pytest.fail(
            f"[{name_collation()}] add({second!r}) after add({first!r}) "
            f"raised WorkflowNodeExists: {exc} "
            f"(OxTask rows: {OxTask.objects.count()}, "
            f"nodes: {sorted(flow.node_ids())})"
        )
    assert {name: str(task_id) for name, task_id in flow.node_ids().items()} == {
        first: str(a.id),
        second: str(b.id),
    }
    assert OxTask.objects.count() == 2


@pytest.mark.parametrize(("first", "second"), PAIRS)
def test_a_name_add_calls_taken_is_a_name_depends_on_resolves(first, second):
    """
    The unique index and depends_on must agree about what a name names.

    add(second) either admits a node named `second`, in which case
    depends_on=second points at that node, or refuses with
    WorkflowNodeExists ("already has a node named ..."), in which case the
    name is taken and depends_on=second must point at the node that took
    it. Refusing the name as existing and then refusing it again as
    missing is the one answer that is wrong under every design.

    On MySQL that was the answer: the IN filter fetched the row stored as
    `first`, and the exact membership test in _resolve_dependencies
    discarded it and raised UnknownWorkflowNode.
    """
    flow = workflows.create("fold-resolve")
    flow.add(first, tasks.step, first)
    try:
        flow.add(second, tasks.step, second)
    except WorkflowNodeExists as exc:
        refused_as_existing = str(exc)
    else:
        refused_as_existing = None
    try:
        downstream = flow.add("downstream", tasks.step, "d", depends_on=second)
    except UnknownWorkflowNode as exc:
        pytest.fail(
            f"[{name_collation()}] add({second!r}) was refused as existing "
            f"({refused_as_existing}), then depends_on={second!r} was refused "
            f"as missing: {exc}"
        )
    nodes = flow.node_ids()
    edge = OxpullWorkflowEdge.objects.get(downstream_task_id=downstream.id)
    named = second if refused_as_existing is None else first
    assert edge.upstream_task_id == nodes[named]


@pytest.mark.parametrize(("first", "second"), PAIRS)
def test_a_dag_named_from_data_builds_runs_and_completes(first, second):
    """
    A builder that names one node per input (a file, a customer, a key)
    and then seals gets a workflow that runs every node and completes.

    On MySQL the build used to die at the second name. The node already
    added was committed, and because nodes of an OPEN workflow run, a real
    worker ran it. The reconciler sweeps only SEALED workflows, so the
    workflow stayed OPEN, never completed and never fired a callback.
    """
    names = [first, second]
    flow = workflows.create("from-data")
    refused = None
    try:
        for name in names:
            flow.add(name, tasks.step, name)
        flow.seal()
    except WorkflowNodeExists as exc:
        refused = exc
    drain(flow_worker())
    reconcile()
    reconcile()
    ran = sorted(order())
    status = stored(flow).status
    assert (refused, ran, status) == (
        None,
        sorted(names),
        OxpullWorkflow.Status.COMPLETE,
    ), (
        f"[{name_collation()}] build refused: {refused!r}; nodes that ran: "
        f"{ran}; workflow status: {status}; "
        f"node rows: {OxpullWorkflowNode.objects.count()}"
    )
