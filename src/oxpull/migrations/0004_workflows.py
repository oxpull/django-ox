import re
import uuid

import django.db.models.deletion
from django.db import migrations, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.state import StateApps

from oxpull.migrations._workflow_guard import refuse_while_workflows_exist

# The workflow columns that hold a django-ox task id and are compared with
# OxTask.id in SQL, as (model, field).
TASK_ID_COLUMNS = (
    ("OxpullWorkflowEdge", "upstream_task_id"),
    ("OxpullWorkflowEdge", "downstream_task_id"),
    ("OxpullWorkflowNode", "task_id"),
    ("OxpullWorkflowNode", "cancelled_by_task_id"),
)

# A character set or collation name as information_schema spells one.
MYSQL_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def copy_task_id_collation(
    apps: StateApps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    """
    On MySQL, put the task-id columns on the collation of OxTask.id.

    migrate creates a table on the database's default collation at that
    moment. django-ox's table can carry another one, after a dump restored
    into a server with a newer default or an ALTER DATABASE. MySQL refuses to
    compare two columns of one character set on different collations, and
    every gated claim compares these columns with OxTask.id. The tables were
    created just above and are empty, so the ALTER rewrites nothing.

    A column with no collation, such as MariaDB's native uuid type, is left
    alone. oxpull.E009 reports a mismatch that arises later.
    """
    connection = schema_editor.connection
    if connection.vendor != "mysql":
        return
    task_model = apps.get_model("django_ox", "OxTask")
    quote = schema_editor.quote_name
    with connection.cursor() as cursor:
        target = _column(cursor, task_model._meta.db_table, "id", task_model)
        if target is None or target[2] is None:
            return
        charset, collation = target[3], target[2]
        if not (MYSQL_NAME.match(charset) and MYSQL_NAME.match(collation)):
            return
        for model_name, field_name in TASK_ID_COLUMNS:
            model = apps.get_model("oxpull", model_name)
            table = model._meta.db_table
            found = _column(cursor, table, field_name, model)
            if found is None or found[2] is None or found[2] == collation:
                continue
            column_type, nullable = found[0], found[1]
            null = "NULL" if nullable == "YES" else "NOT NULL"
            column = _column_name(model, field_name)
            cursor.execute(
                f"ALTER TABLE {quote(table)} MODIFY {quote(column)} {column_type}"
                f" CHARACTER SET {charset} COLLATE {collation} {null}"
            )


# Distinct downstream ids per edge row, as a fraction, that PostgreSQL's
# planner assumes for the edge table: a node has two edges in, on average.
EDGE_DOWNSTREAM_N_DISTINCT = -0.5


def pin_edge_statistics(
    apps: StateApps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    """
    On PostgreSQL, fix the planner's count of distinct downstream ids on the
    edge table.

    The claim's dependency check probes the edges into each candidate row by
    downstream_task_id. ANALYZE run while one wide fan-in fills the table
    counts one downstream id for thousands of edges, and the planner then
    reads the whole edge table on every claim instead of probing the index.
    A node has a few edges in, so the check is a probe. With n_distinct set,
    ANALYZE keeps this value for the column and still gathers the rest.
    """
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        return
    model = apps.get_model("oxpull", "OxpullWorkflowEdge")
    quote = schema_editor.quote_name
    column = _column_name(model, "downstream_task_id")
    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER TABLE {quote(model._meta.db_table)} ALTER COLUMN {quote(column)}"
            f" SET (n_distinct = {EDGE_DOWNSTREAM_N_DISTINCT})"
        )


def _column(
    cursor: object, table: str, field_name: str, model: type[models.Model]
) -> tuple[str, str, str | None, str] | None:
    """COLUMN_TYPE, IS_NULLABLE, COLLATION_NAME and CHARACTER_SET_NAME."""
    column = _column_name(model, field_name)
    cursor.execute(  # type: ignore[attr-defined]
        "SELECT COLUMN_TYPE, IS_NULLABLE, COLLATION_NAME, CHARACTER_SET_NAME"
        " FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()"
        " AND TABLE_NAME = %s AND COLUMN_NAME = %s",
        [table, column],
    )
    row = cursor.fetchone()  # type: ignore[attr-defined]
    return None if row is None else (str(row[0]), str(row[1]), row[2], str(row[3]))


def _column_name(model: type[models.Model], field_name: str) -> str:
    field = model._meta.get_field(field_name)
    if not isinstance(field, models.Field):
        raise TypeError(f"{model.__name__}.{field_name} is not a concrete field")
    return str(field.column)


class Migration(migrations.Migration):
    dependencies = [
        ("django_ox", "0008_waiting"),
        ("oxpull", "0003_oxpullratewindow"),
    ]

    operations = [
        migrations.CreateModel(
            name="OxpullWorkflowRound",
            fields=[
                (
                    "id",
                    models.PositiveSmallIntegerField(
                        default=1, primary_key=True, serialize=False
                    ),
                ),
                ("workflow_id", models.UUIDField(blank=True, null=True)),
                ("version", models.BigIntegerField(default=0)),
            ],
        ),
        migrations.CreateModel(
            name="OxpullWorkflowScan",
            fields=[
                ("workflow_id", models.UUIDField(primary_key=True, serialize=False)),
                (
                    "phase",
                    models.CharField(
                        choices=[
                            ("EDGES", "Edges"),
                            ("NODES", "Nodes"),
                            ("RELEASE", "Release"),
                        ],
                        default="EDGES",
                        max_length=10,
                    ),
                ),
                ("position", models.BigIntegerField(default=0)),
                ("edge_hwm", models.BigIntegerField(blank=True, null=True)),
                ("node_hwm", models.BigIntegerField(blank=True, null=True)),
                ("skipped", models.JSONField(blank=True, default=list)),
                ("version", models.BigIntegerField(default=0)),
            ],
        ),
        migrations.CreateModel(
            name="OxpullWorkflow",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("name", models.CharField(max_length=128)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("OPEN", "Open"),
                            ("SEALED", "Sealed"),
                            ("COMPLETE", "Complete"),
                        ],
                        default="OPEN",
                        max_length=10,
                    ),
                ),
                ("total_count", models.PositiveIntegerField(blank=True, null=True)),
                (
                    "on_complete_path",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("on_complete_args", models.JSONField(blank=True, default=list)),
                ("on_complete_kwargs", models.JSONField(blank=True, default=dict)),
                (
                    "on_success_path",
                    models.CharField(blank=True, default="", max_length=255),
                ),
                ("on_success_args", models.JSONField(blank=True, default=list)),
                ("on_success_kwargs", models.JSONField(blank=True, default=dict)),
                ("failed_count", models.PositiveIntegerField(blank=True, null=True)),
                ("cancelled_count", models.PositiveIntegerField(blank=True, null=True)),
                ("callback_task_id", models.UUIDField(blank=True, null=True)),
                ("success_task_id", models.UUIDField(blank=True, null=True)),
                ("created_at", models.DateTimeField()),
                ("sealed_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("generation", models.PositiveIntegerField(default=0)),
                ("cancel_requested_at", models.DateTimeField(blank=True, null=True)),
                ("metadata_retired_at", models.DateTimeField(blank=True, null=True)),
                (
                    "key_hash",
                    models.CharField(blank=True, max_length=64, null=True, unique=True),
                ),
                ("completions", models.JSONField(blank=True, default=dict)),
                ("node_count", models.PositiveIntegerField(default=0)),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["status", "created_at"], name="oxpull_wf_sweep_idx"
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="OxpullWorkflowEdge",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("upstream_task_id", models.UUIDField()),
                ("downstream_task_id", models.UUIDField()),
                ("satisfied", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField()),
                (
                    "workflow",
                    models.ForeignKey(
                        db_index=False,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="edges",
                        to="oxpull.oxpullworkflow",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["downstream_task_id", "satisfied"],
                        name="oxpull_wf_edge_down_idx",
                    ),
                    models.Index(
                        fields=["workflow", "satisfied", "id"],
                        name="oxpull_wf_edge_open_idx",
                    ),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("workflow", "upstream_task_id", "downstream_task_id"),
                        name="oxpull_wf_edge_uniq",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="OxpullWorkflowNode",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=128)),
                ("name_key", models.CharField(max_length=64)),
                ("task_id", models.UUIDField()),
                ("created_at", models.DateTimeField()),
                ("outcome", models.CharField(blank=True, max_length=10, null=True)),
                ("outcome_epoch", models.BigIntegerField(blank=True, null=True)),
                ("succeeded", models.BooleanField(default=False)),
                (
                    "cancel_cause",
                    models.CharField(blank=True, max_length=16, null=True),
                ),
                ("cancelled_by_task_id", models.UUIDField(blank=True, null=True)),
                (
                    "cancelled_generation",
                    models.PositiveIntegerField(blank=True, null=True),
                ),
                (
                    "workflow",
                    models.ForeignKey(
                        db_index=False,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="nodes",
                        to="oxpull.oxpullworkflow",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["workflow", "succeeded", "id"],
                        name="oxpull_wf_node_open_idx",
                    )
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("workflow", "name_key"),
                        name="oxpull_wf_node_name_uniq",
                    ),
                    models.UniqueConstraint(
                        fields=("task_id",), name="oxpull_wf_node_task_uniq"
                    ),
                ],
            },
        ),
        migrations.RunPython(
            copy_task_id_collation, reverse_code=migrations.RunPython.noop
        ),
        migrations.RunPython(
            pin_edge_statistics, reverse_code=migrations.RunPython.noop
        ),
        # Last, so migrating back meets it before any table is dropped.
        migrations.RunPython(
            migrations.RunPython.noop, reverse_code=refuse_while_workflows_exist
        ),
    ]
