from .settings import *  # noqa: F403

# The smallest project that installs django_ox: no admin, no URLconf, nothing
# that happens to import django.tasks before the system checks run. The check
# tests start `manage.py check` on these to prove the django-ox checks run
# on their own, not because the admin's autodiscover pulled them in.
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django_ox",
]
MIDDLEWARE: list[str] = []
TEMPLATES: list[dict[str, object]] = []
del ROOT_URLCONF  # noqa: F821
