"""Environment helpers for tests that start another Python process."""

from pathlib import Path

import django_ox


def django_ox_path() -> str:
    """
    The directory this process imports django_ox from.

    A child process given a PYTHONPATH of its own otherwise falls back to
    whichever django-ox the interpreter has installed, which need not be the
    one this process runs against. Put this on the child's PYTHONPATH when it
    runs the current oxpull source.
    """
    return str(Path(django_ox.__file__).resolve().parent.parent)
