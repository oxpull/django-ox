"""
oxpull needs Django 6.0 or later; django-ox does not.

These tests live in tests/, not tests_oxpull/, because they must run on the
Django 5.2 legs, where the oxpull suite is not collected at all. Each case runs
a fresh interpreter, since the gate acts at import time. On Django 6.0 and
later a case pretends to be 5.2 by setting django.VERSION before oxpull is
imported; on a real 5.2 the same case runs against the real thing.
"""

import subprocess
import sys
import textwrap

import django
import pytest

REAL_5_2 = django.VERSION[:2] < (6, 0)
GATED_MODULES = [
    "oxpull.backend",
    "oxpull.worker",
    "oxpull.unique",
    "oxpull.batches",
    "oxpull.workflows",
    "oxpull.exceptions",
    "oxpull.management.commands.oxpull_prune",
]
ENGINE = "django.db.backends.sqlite3"
PRELUDE = """
ENGINE = {engine!r}
import sys
import django
from django.conf import settings

def pretend_5_2():
    if {fake}:
        django.VERSION = (5, 2, 18, "final", 0)

settings.configure(
    INSTALLED_APPS={apps!r},
    DATABASES={{"default": {{"ENGINE": ENGINE, "NAME": ":memory:"}}}},
    USE_TZ=True,
)
"""


def run(body, *, fake, apps=("django_ox", "oxpull")):
    code = PRELUDE.format(engine=ENGINE, fake=fake, apps=list(apps)) + textwrap.dedent(
        body
    )
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


# On Django 5.2 only the real thing means anything; elsewhere only the pretence.
MODES = [pytest.param(True, id="simulated-5.2")]
if REAL_5_2:
    MODES = [pytest.param(False, id="real-5.2")]


@pytest.mark.parametrize("fake", MODES)
def test_ready_refuses_with_a_message_that_names_the_fix(fake):
    result = run(
        """
        from django.apps import apps
        django.setup()
        pretend_5_2()
        apps.get_app_config("oxpull").ready()
        """,
        fake=fake,
    )
    assert result.returncode != 0
    assert "ImproperlyConfigured" in result.stderr
    assert "oxpull needs Django 6.0 or later" in result.stderr
    assert "remove 'oxpull' from INSTALLED_APPS" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


@pytest.mark.parametrize("fake", MODES)
@pytest.mark.parametrize("module", GATED_MODULES)
def test_each_module_that_needs_django_tasks_refuses_on_import(module, fake):
    result = run(
        f"""
        import importlib
        pretend_5_2()
        try:
            importlib.import_module({module!r})
        except Exception as exc:
            print(type(exc).__name__)
            print(exc)
        else:
            print("IMPORTED")
        """,
        fake=fake,
    )
    assert result.returncode == 0, result.stderr
    name, _, message = result.stdout.partition("\n")
    assert name == "ImproperlyConfigured", result.stdout
    assert "Django 6.0" in message


@pytest.mark.parametrize("fake", MODES)
def test_the_database_check_does_not_import_workflows(fake):
    result = run(
        """
        django.setup()
        pretend_5_2()
        from oxpull import checks
        found = checks.check_workflow_schema(databases={"default"})
        print(found, "oxpull.workflows" in sys.modules)
        """,
        fake=fake,
        apps=("django_ox",),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[] False"


@pytest.mark.parametrize("fake", MODES)
def test_the_modules_that_need_no_tasks_framework_still_import(fake):
    result = run(
        """
        pretend_5_2()
        import oxpull, oxpull.apps, oxpull.rate_limits, oxpull.routing
        print(oxpull.__version__ == __import__("django_ox").__version__)
        """,
        fake=fake,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True"


@pytest.mark.skipif(REAL_5_2, reason="needs a Django that supports oxpull")
def test_a_supported_django_sets_up_with_oxpull_installed():
    result = run("django.setup()\nprint('ok')", fake=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
