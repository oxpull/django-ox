from django.db import migrations, models

from oxpull.migrations._workflow_guard import refuse_dropping_the_upstream_index


class Migration(migrations.Migration):
    """
    The index the release reads.

    OxpullWorkflowEdge already carries an index on downstream_task_id --
    the one the claim's gate probes, asking "does anything gate this
    candidate". The release asks the opposite question, "what did this
    task just unblock", and the unique constraint cannot answer it: its
    leading column is workflow_id, so upstream_task_id is not a usable
    prefix. Without this the release is a sequential scan of the edge
    table on every successful task.
    """

    dependencies = [("oxpull", "0004_workflows")]

    operations = [
        migrations.AddIndex(
            model_name="oxpullworkflowedge",
            index=models.Index(
                fields=["upstream_task_id"], name="oxpull_wf_edge_up_idx"
            ),
        ),
        # Last, so migrating back meets it before the index is dropped.
        migrations.RunPython(
            migrations.RunPython.noop, reverse_code=refuse_dropping_the_upstream_index
        ),
    ]
