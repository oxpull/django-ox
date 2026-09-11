from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_ox", "0004_discarded"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="oxtask",
            name="ox_dequeue_idx",
        ),
        migrations.AddIndex(
            model_name="oxtask",
            index=models.Index(
                fields=["status", "-priority", "enqueued_at"],
                name="ox_dequeue_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="oxtask",
            index=models.Index(
                fields=["status", "queue_name", "-priority", "enqueued_at"],
                name="ox_dequeue_queue_idx",
            ),
        ),
    ]
