from django.apps import AppConfig


class OxpullConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "oxpull"
    verbose_name = "Oxpull"

    def ready(self) -> None:
        # Before anything that imports django.tasks, so Django 5.2 gets a
        # message that names the fix.
        from ._django_gate import require_django_6

        require_django_6()
        # Registers the checks that read the database.
        from . import checks  # noqa: F401
