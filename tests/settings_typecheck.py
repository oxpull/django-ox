from .settings import *  # noqa: F403

# Settings for strict type checking and migration checks with both apps.
# Run these checks on Django 6.0 or later because oxpull is installed here.
# The default test settings omit oxpull so tests/ can also run on Django 5.2.
INSTALLED_APPS = [*INSTALLED_APPS, "oxpull"]  # noqa: F405
