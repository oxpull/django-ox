"""
The oxpull feature suite: unique tasks, batches, workflows and rate limits.

oxpull needs Django 6.0, so this suite cannot run on the Django 5.2 legs and
has its own settings, apart from the base suite's. Run it as

    pytest tests_oxpull --ds=tests_oxpull.settings

pyproject.toml points pytest at tests.settings, which has no oxpull app, and
testpaths is tests alone, so the Django 5.2 legs, which run `pytest tests`,
never collect this directory. Pointing pytest at the repository root loads this
package's conftest and refuses with the message below.
"""

import os

import pytest

# A child process the suite starts names a settings module of its own, so only
# the base suite's settings are refused.
if os.environ.get("DJANGO_SETTINGS_MODULE", "").startswith("tests."):
    raise pytest.UsageError(
        "tests_oxpull runs on its own settings, not the base suite's. Run it as: "
        "pytest tests_oxpull --ds=tests_oxpull.settings"
    )
