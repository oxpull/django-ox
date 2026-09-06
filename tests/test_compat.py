"""
The Tasks framework is imported from one place, and that place resolves.

django-ox runs on Django 5.2 LTS, where the Tasks framework is the
``django-tasks`` backport, and on Django 6.0+, where it is ``django.tasks``.
``django_ox.compat`` is the only module allowed to know which. These tests
assert both halves: that compat resolved to the package the running Django
actually has, and that nothing else reaches around it.
"""

import ast
import pathlib

import django
import pytest
from django.core.checks.registry import registry

import django_ox
from django_ox import compat

# Module paths compat exists to hide. django.utils.json arrived with the Tasks
# framework in Django 6.0 and has no counterpart on 5.2; the backport carries
# the same function as django_tasks.utils.normalize_json.
FORBIDDEN_PREFIXES = ("django.tasks", "django_tasks", "django.utils.json")

SRC = pathlib.Path(django_ox.__file__).parent


def django_ox_modules():
    """Every shipped module except compat itself."""
    return sorted(p for p in SRC.rglob("*.py") if p.name != "compat.py")


@pytest.mark.parametrize("path", django_ox_modules(), ids=lambda p: p.name)
def test_only_compat_imports_the_tasks_framework(path):
    """
    A direct `from django.tasks import ...` anywhere else is an ImportError on
    Django 5.2, and one that only fires for users, since CI on 6.0 would never
    reach it. Catch it in the source instead of at a user's traceback.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            name = node.module
        elif isinstance(node, ast.Import):
            name = node.names[0].name
        else:
            continue
        if name.startswith(FORBIDDEN_PREFIXES):
            offenders.append(f"{name} (line {node.lineno})")
    assert not offenders, (
        f"{path.relative_to(SRC)} imports the Tasks framework directly: "
        f"{offenders}. Import it from django_ox.compat instead."
    )


def test_compat_resolved_to_the_running_djangos_framework():
    """
    HAS_CORE_TASKS is a claim about the installed Django. Prove it against the
    module the names actually came from, so a wrong branch cannot pass quietly.
    """
    expected_core = django.VERSION >= (6, 0)
    assert compat.HAS_CORE_TASKS is expected_core

    root = "django.tasks" if expected_core else "django_tasks"
    assert compat.Task.__module__.startswith(root)
    assert compat.TaskResult.__module__.startswith(root)
    assert compat.TaskContext.__module__.startswith(root)
    assert compat.BaseTaskBackend.__module__.startswith(root)
    assert compat.InvalidTask.__module__.startswith(root)
    assert compat.TaskResultDoesNotExist.__module__.startswith(root)
    assert compat.IMMEDIATE_BACKEND_PATH.startswith(root)
    assert compat.DUMMY_BACKEND_PATH.startswith(root)
    assert compat.normalize_json.__module__.startswith(
        "django.utils.json" if expected_core else "django_tasks"
    )


def test_the_backend_subclasses_the_frameworks_own_base():
    """
    OxBackend must extend the base class the framework will hand tasks to. Two
    copies of BaseTaskBackend in one process would leave every enqueue looking
    for a backend that is not there.
    """
    from django_ox.backend import OxBackend

    assert issubclass(OxBackend, compat.BaseTaskBackend)
    assert compat.task_backends["default"].__class__ is OxBackend


def test_importing_the_framework_registers_its_system_check():
    """
    django_ox.E001 to E005 are only reachable through the framework's own
    check, which core registers on import and the backport registers from its
    AppConfig. django_ox.apps calls compat to cover both.
    """
    compat.import_tasks_framework()
    checks = registry.get_checks(include_deployment_checks=False)
    names = {f"{f.__module__}.{f.__name__}" for f in checks}
    expected = (
        "django.tasks.checks.check_tasks"
        if compat.HAS_CORE_TASKS
        else "django_tasks.checks.check_tasks"
    )
    assert expected in names


def test_normalize_json_behaves_the_same_on_both():
    assert compat.normalize_json((1, "a", {"b": (2,)})) == [1, "a", {"b": [2]}]
    with pytest.raises(TypeError):
        compat.normalize_json(object())


def test_invalid_task_is_what_the_framework_raises():
    """
    Released backports up to 0.12.0 spell it InvalidTaskError. Whatever the
    name, it must be the class validate_task actually raises, or django-ox's
    own `except InvalidTask` never fires.
    """
    from .tasks import add

    backend = compat.task_backends["default"]
    with pytest.raises(compat.InvalidTask):
        backend.validate_task(add.using(queue_name="not-a-configured-queue"))
