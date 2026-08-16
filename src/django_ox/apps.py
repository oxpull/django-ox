from django.apps import AppConfig


class DjangoOxConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_ox"
    verbose_name = "Ox task queue"
