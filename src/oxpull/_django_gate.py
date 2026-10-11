"""
The Django floor for oxpull, in one place.

django-ox runs on Django 5.2 and later. oxpull is built on Django's Tasks
framework (django.tasks), which Django ships from 6.0, so it needs Django 6.0
or later. The modules that import django.tasks import _requires_django_6
first, and OxpullConfig.ready() calls require_django_6(), so a project on
5.2 gets this message and not a ModuleNotFoundError from inside a management
command.

The functions are here and nothing runs on import, because oxpull.checks has
to ask whether Django is supported without being refused itself. The modules
that must refuse import _requires_django_6 instead.
"""

import django
from django.core.exceptions import ImproperlyConfigured

MINIMUM = (6, 0)


def unsupported_message() -> str:
    return (
        f"oxpull needs Django 6.0 or later, and this is Django {django.get_version()}. "
        "django-ox itself runs on Django 5.2, but oxpull is built on Django's "
        "Tasks framework, which Django ships from 6.0. Upgrade Django to 6.0 or "
        "later, or remove 'oxpull' from INSTALLED_APPS and set the task backend "
        "to 'django_ox.backend.OxBackend'."
    )


def supported() -> bool:
    return django.VERSION[:2] >= MINIMUM


def require_django_6() -> None:
    if not supported():
        raise ImproperlyConfigured(unsupported_message())
