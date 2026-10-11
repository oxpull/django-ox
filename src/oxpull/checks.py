"""
System checks that read the database.

Django runs them for `manage.py check --database <alias>`, and `migrate` runs
them against the database it is about to migrate. The other oxpull checks run
from each backend's check() and never touch a database.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.models import Field

from django_ox.models import OxTask


@dataclass(frozen=True)
class CollationMismatch:
    """A workflow task-id column on another collation than OxTask.id."""

    table: str
    column: str
    column_type: str
    nullable: bool
    collation: str
    task_table: str
    task_column: str
    task_charset: str
    task_collation: str

    def message(self) -> str:
        return (
            f"On MySQL, {self.table}.{self.column} is on collation "
            f"{self.collation} and {self.task_table}.{self.task_column} is on "
            f"{self.task_collation}. MySQL refuses to compare the two, so every "
            "claim on a backend that declares WORKFLOWS fails."
        )

    def hint(self) -> str:
        null = "NULL" if self.nullable else "NOT NULL"
        return (
            f"Run ALTER TABLE `{self.table}` MODIFY `{self.column}` "
            f"{self.column_type} CHARACTER SET {self.task_charset} COLLATE "
            f"{self.task_collation} {null} to put the column on the task id's "
            "collation."
        )


def collation_mismatches(db_alias: str) -> list[CollationMismatch]:
    """
    The workflow task-id columns whose collation differs from OxTask.id's.

    MySQL only. PostgreSQL stores a UUID as uuid, which has no collation, and
    SQLite never refuses a comparison over one. Migration 0004 puts the
    columns on the task id's collation when it creates them. This finds a
    split made afterwards, such as django-ox's table converted to a new
    default. A table that does not exist yet contributes nothing.
    """
    connection = connections[db_alias]
    if connection.vendor != "mysql":
        return []
    from .models import OxpullWorkflowEdge, OxpullWorkflowNode

    task_table = OxTask._meta.db_table
    # Concrete fields always carry a column; the stubs say Optional.
    task_column = str(cast("Field[Any, Any]", OxTask._meta.get_field("id")).column)
    columns = [
        (model._meta.db_table, str(cast("Field[Any, Any]", field).column))
        for model, field in (
            (model, model._meta.get_field(name))
            for model, name in (
                (OxpullWorkflowEdge, "upstream_task_id"),
                (OxpullWorkflowEdge, "downstream_task_id"),
                (OxpullWorkflowNode, "task_id"),
                (OxpullWorkflowNode, "cancelled_by_task_id"),
            )
        )
    ]
    tables = sorted({task_table, *(table for table, _ in columns)})
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,"
            " CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS"
            " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (%s, %s, %s)",
            tables,
        )
        found = {(row[0], row[1]): row[2:] for row in cursor.fetchall()}
    task = found.get((task_table, task_column))
    if task is None or task[3] is None:
        return []
    mismatches = []
    for table, column in columns:
        row = found.get((table, column))
        if row is None or row[3] is None or row[3] == task[3]:
            continue
        mismatches.append(
            CollationMismatch(
                table=table,
                column=column,
                column_type=str(row[0]),
                nullable=row[1] == "YES",
                collation=str(row[3]),
                task_table=task_table,
                task_column=task_column,
                task_charset=str(task[2]),
                task_collation=str(task[3]),
            )
        )
    return mismatches


def waiting_node_count(db_alias: str) -> int:
    """Workflow nodes whose task row is WAITING, or 0 before the tables exist."""
    from .models import OxpullWorkflowNode

    connection = connections[db_alias]
    with connection.cursor() as cursor:
        tables = connection.introspection.table_names(cursor)
    if OxpullWorkflowNode._meta.db_table not in tables:
        return 0
    nodes = OxpullWorkflowNode.objects.using(db_alias).values("task_id")
    return (
        OxTask.objects.using(db_alias)
        .filter(status=OxTask.Status.WAITING, pk__in=nodes)
        .count()
    )


@checks.register(checks.Tags.database)
def check_workflow_schema(
    app_configs: Any = None, databases: Any = None, **kwargs: Any
) -> list[checks.CheckMessage]:
    """oxpull.E009 and oxpull.W007, for the database oxpull's tables live on."""
    if not databases:
        return []
    from ._django_gate import supported

    if not supported():
        # .workflows imports django.tasks, which Django 5.2 does not have.
        # OxpullConfig.ready() has already refused the app with a message.
        return []
    from .routing import oxpull_db_alias

    try:
        alias = oxpull_db_alias()
    except ImproperlyConfigured:
        # oxpull.E004 reports a router that splits the tables.
        return []
    if alias not in databases:
        return []
    messages: list[checks.CheckMessage] = [
        checks.Error(mismatch.message(), hint=mismatch.hint(), id="oxpull.E009")
        for mismatch in collation_mismatches(alias)
    ]
    from .workflows import declaring_aliases

    if not declaring_aliases():
        waiting = waiting_node_count(alias)
        if waiting:
            messages.append(
                checks.Warning(
                    f"{waiting} workflow node(s) on database {alias!r} are WAITING, "
                    "and no backend in settings.TASKS declares "
                    "OPTIONS['WORKFLOWS']. No worker releases them when their "
                    "upstreams succeed.",
                    hint="Declare OPTIONS['WORKFLOWS'] again on the backend these "
                    "workflows ran on, and schedule oxpull.workflows.reconcile.",
                    id="oxpull.W007",
                )
            )
    return messages
