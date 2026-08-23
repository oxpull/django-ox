from django.apps import AppConfig


class DjangoOxConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_ox"
    verbose_name = "Ox task queue"

    def ready(self) -> None:
        # Django registers its tasks system check, the one that calls every
        # backend's check(), when django.tasks is first imported. A project
        # whose settings and URLconf import it nowhere would otherwise run
        # `manage.py check` without ever reaching django_ox.E001 to E005.
        import django.tasks  # noqa: F401
