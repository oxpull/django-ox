"""
The refusals that end every workflow migration. Not a migration itself:
Django's migration loader skips a module whose name starts with an underscore.

Migrating oxpull back unapplies its later migrations first, newest first. Each
workflow migration ends with a refusal as its last operation, so migrating
back meets it before that migration undoes anything. With the refusal in 0004
alone, 0005 would drop the index the release reads by upstream, and then 0004
would refuse, leaving running workers without it.

Each migration's refusal says what unapplying that migration drops, so its
message is true whichever target the command names.
"""

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.exceptions import IrreversibleError
from django.db.migrations.state import StateApps

ROLLBACK_RULE = "Rolling back is supported only before the first workflow is created."


def _workflow_count(apps: StateApps, schema_editor: BaseDatabaseSchemaEditor) -> int:
    workflow_model = apps.get_model("oxpull", "OxpullWorkflow")
    rows = workflow_model._default_manager.using(schema_editor.connection.alias)
    return int(rows.count())


def refuse_while_workflows_exist(
    apps: StateApps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    """
    0004's refusal: do not unapply 0004_workflows while any workflow row exists.

    Unapplying 0004 drops the tables that hold every node, edge and record,
    and a version from before it cannot release a waiting node or finish a
    workflow.

    This detects a mistake. It does not make a rollback safe while workers
    run: a workflow created after this check passes is dropped with the
    tables. Stop every process that can create a workflow before migrating
    back.
    """
    count = _workflow_count(apps, schema_editor)
    if count:
        raise IrreversibleError(
            f"Cannot migrate oxpull back past 0004_workflows. {count} "
            "workflow(s) exist on this database. Migrating back drops the "
            "tables that record their nodes and edges, and a version before "
            f"0004 cannot run or finish them. {ROLLBACK_RULE}"
        )


def refuse_dropping_the_upstream_index(
    apps: StateApps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    """
    0005's refusal: do not unapply 0005_workflow_edge_upstream_idx while any
    workflow row exists.

    Unapplying 0005 drops oxpull_wf_edge_up_idx, which a worker reads when a
    node succeeds, to mark the edges out of it and release what they held.
    Migrating further back meets 0004's refusal. The same caveat holds: this
    detects a mistake, and processes that create workflows must be stopped
    first.
    """
    count = _workflow_count(apps, schema_editor)
    if count:
        raise IrreversibleError(
            "Cannot migrate oxpull back past 0005_workflow_edge_upstream_idx. "
            f"{count} workflow(s) exist on this database. Unapplying it drops "
            "the index a worker reads when a node succeeds, to release the "
            "nodes that wait on it. Migrating back past 0004_workflows would "
            f"also drop the workflow tables. {ROLLBACK_RULE}"
        )
