"""
Declaring policy through ``@task``, as a fresh process imports it.

A task module is decorated once, at import, under whatever TASKS names then,
so each case here is a new interpreter importing a module that declares
``@task(max_attempts=..., timeout=..., backoff=...)``. What it should get
depends on the installed framework, and each environment asserts its own:

- Django 6.1 and the 5.2 backport forward the keyword arguments to the
  backend's task class, so OxBackend and django_ox.testing's backends build a
  PolicyTask, and Django's own Immediate and Dummy backends, whose class is
  the stock Task, refuse them.
- Django 6.0's ``task()`` takes no extra keyword arguments at all, and raises
  its own TypeError whatever the backend.
"""

import json
import os
import subprocess
import sys
import textwrap

import django
import pytest

from django_ox.compat import DUMMY_BACKEND_PATH, HAS_CORE_TASKS, IMMEDIATE_BACKEND_PATH

from .policy_tasks import DECORATOR_TAKES_POLICY

OURS = [
    "django_ox.backend.OxBackend",
    "django_ox.testing.ImmediateBackend",
    "django_ox.testing.DummyBackend",
]
STOCK = [IMMEDIATE_BACKEND_PATH, DUMMY_BACKEND_PATH]

SETTINGS = """
import json, os
SECRET_KEY = "test-only"
USE_TZ = True
INSTALLED_APPS = ["django.contrib.contenttypes", "django_ox"]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
TASKS = json.loads(os.environ["POLICY_TASKS"])
"""

MODULE = """
from django_ox.compat import task


def backoff(exc, task_result):
    return 1


@task(max_attempts=5, timeout=30, backoff=backoff)
def sync_account(account_id):
    return account_id


@task
def bare(value):
    return value
"""

PROBE = """
import json
import django
django.setup()
try:
    import policy_module
except Exception as exc:
    print(json.dumps({"raised": type(exc).__name__, "message": str(exc)}))
else:
    declared = policy_module.sync_account
    print(json.dumps({
        "type": type(declared).__qualname__,
        "fields": [declared.max_attempts, declared.timeout],
        "backoff": declared.backoff is policy_module.backoff,
    }))
"""


@pytest.fixture
def project(tmp_path):
    (tmp_path / "policy_settings.py").write_text(SETTINGS)
    (tmp_path / "policy_module.py").write_text(MODULE)
    return tmp_path


def fresh_import(project, backend, probe=PROBE):
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "policy_settings"
    env["POLICY_TASKS"] = json.dumps({"default": {"BACKEND": backend}})
    env["PYTHONPATH"] = os.pathsep.join(
        [str(project), *filter(None, [env.get("PYTHONPATH")])]
    )
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-c", textwrap.dedent(probe)],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.splitlines()[-1])


def test_the_environment_says_which_case_it_is():
    # The tests below branch on this; pin it to the installed Django.
    assert DECORATOR_TAKES_POLICY is (
        not HAS_CORE_TASKS or django.VERSION[:2] >= (6, 1)
    )


DJANGO_60_REFUSAL = {
    "raised": "TypeError",
    "message": "task() got an unexpected keyword argument 'max_attempts'",
}


@pytest.mark.parametrize("backend", OURS)
def test_our_backends_build_a_policy_task_where_the_decorator_forwards(
    project, backend
):
    outcome = fresh_import(project, backend)
    if DECORATOR_TAKES_POLICY:
        assert outcome == {"type": "PolicyTask", "fields": [5, 30], "backoff": True}
    else:
        assert outcome == DJANGO_60_REFUSAL


@pytest.mark.parametrize("backend", STOCK)
def test_djangos_test_backends_refuse_the_keywords(project, backend):
    outcome = fresh_import(project, backend)
    if DECORATOR_TAKES_POLICY:
        assert outcome["raised"] == "TypeError"
        assert outcome["message"] == (
            "Task.__init__() got an unexpected keyword argument 'max_attempts'"
        )
    else:
        assert outcome == DJANGO_60_REFUSAL


@pytest.mark.parametrize("backend", OURS)
def test_a_bare_decorator_builds_a_policy_task_everywhere(project, backend):
    (project / "policy_module.py").write_text(
        "from django_ox.compat import task\n\n\n"
        "@task\ndef bare(value):\n    return value\n"
    )
    probe = """
    import json
    import django
    django.setup()
    import policy_module
    declared = policy_module.bare
    print(json.dumps([type(declared).__qualname__, declared.max_attempts]))
    """
    assert fresh_import(project, backend, probe) == ["PolicyTask", None]


SWITCHING_PROBE = """
import json, logging
import django
django.setup()
from django.test import override_settings
import policy_module

records = []

class Keep(logging.Handler):
    def emit(self, record):
        records.append(getattr(record, "event", None))

logging.getLogger("django_ox").addHandler(Keep())
outcomes = {}
for label, path in (("ours", "django_ox.testing.ImmediateBackend"),
                    ("stock", STOCK_IMMEDIATE)):
    with override_settings(TASKS={"default": {"BACKEND": path}}):
        result = policy_module.sync_account.enqueue(7)
        outcomes[label] = [str(result.status), result.attempts,
                           type(result.task).__qualname__]
print(json.dumps({"outcomes": outcomes, "events": records}))
"""


@pytest.mark.skipif(
    not DECORATOR_TAKES_POLICY, reason="Django 6.0's @task takes no policy"
)
def test_switching_to_a_test_backend_after_import_runs_the_task_once(project):
    """
    Declared under OxBackend and then run under a test backend, as a suite
    that overrides TASKS does: ours runs the task once and says the policy
    is inert; Django's runs it once and says nothing.
    """
    probe = SWITCHING_PROBE.replace("STOCK_IMMEDIATE", repr(IMMEDIATE_BACKEND_PATH))
    outcome = fresh_import(project, "django_ox.backend.OxBackend", probe)
    assert outcome["outcomes"] == {
        "ours": ["SUCCESSFUL", 1, "PolicyTask"],
        "stock": ["SUCCESSFUL", 1, "PolicyTask"],
    }
    assert outcome["events"] == ["task_policy_inert"]
