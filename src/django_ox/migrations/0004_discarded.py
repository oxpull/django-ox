from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_ox", "0003_lease_epoch"),
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
                ],
                default="READY",
                max_length=10,
            ),
        ),
    ]
