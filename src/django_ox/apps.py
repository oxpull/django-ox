from django.apps import AppConfig


class DjangoOxConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_ox"
    verbose_name = "django-ox"

    def ready(self) -> None:
        # The Tasks framework registers its system check, the one that calls
        # every backend's check(), on import. A project whose settings and
        # URLconf import it nowhere would otherwise run `manage.py check`
        # without ever reaching django_ox.E001 to E005. See compat for what
        # the backport needs instead.
        from .compat import import_tasks_framework

        import_tasks_framework()
