from django.db import migrations, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.exceptions import IrreversibleError
from django.db.migrations.state import StateApps


def refuse_while_waiting(
    apps: StateApps, schema_editor: BaseDatabaseSchemaEditor
) -> None:
    """
    Migrating back to 0007 means running a version that cannot read a waiting
    task and has nothing that would release one, so it is refused while any
    exists. When none does, the cost is one exists() on a status-led index.
    """
    task_model = apps.get_model("django_ox", "OxTask")
    waiting = task_model._default_manager.using(schema_editor.connection.alias).filter(
        status="WAITING"
    )
    if waiting.exists():
        raise IrreversibleError(
            f"Cannot unapply django_ox.0008_waiting: {waiting.count()} task(s) "
            "are WAITING, and a django-ox version from before this migration "
            "can neither read a waiting task nor release one. Stop creating the "
            "workflows that hold tasks back, let them finish or cancel them "
            "until no task is WAITING, then migrate back."
        )


class Migration(migrations.Migration):
    dependencies = [
        ("django_ox", "0007_oxschedule"),
    ]

    operations = [
        migrations.AlterField(
            model_name="oxtask",
            name="status",
            field=models.CharField(
                choices=[
                    ("READY", "Ready"),
                    ("RUNNING", "Running"),
                    ("FAILED", "Failed"),
                    ("SUCCESSFUL", "Successful"),
                    ("LOST", "Lost"),
                    ("DISCARDED", "Discarded"),
                    ("WAITING", "Waiting"),
                ],
                default="READY",
                max_length=10,
            ),
        ),
        # The AlterField runs no SQL on any engine: choices is not a database
        # attribute, so the schema editor has nothing to change. What the way
        # back has to decide is whether a row still holds the new value.
        migrations.RunPython(
            migrations.RunPython.noop, reverse_code=refuse_while_waiting
        ),
    ]
