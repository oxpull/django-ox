"""
The single import site for Django's Tasks framework.

Django ships the framework as ``django.tasks`` from 6.0. On Django 5.2 LTS the
same framework is available as the ``django-tasks`` backport, importable as
``django_tasks``. The two are the same API behind two module paths, with one
exception name spelled differently on released backports and one JSON helper in
a different place. That difference is reconciled here, and nowhere else: no
other module in ``django_ox`` imports either package directly.

The type checker is pointed at the ``django.tasks`` names, which django-stubs
describes; the runtime picks whichever package is installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = [
    "DEFAULT_TASK_BACKEND_ALIAS",
    "DUMMY_BACKEND_PATH",
    "HAS_CORE_TASKS",
    "IMMEDIATE_BACKEND_PATH",
    "BaseTaskBackend",
    "InvalidTask",
    "Task",
    "TaskContext",
    "TaskError",
    "TaskResult",
    "TaskResultDoesNotExist",
    "TaskResultMismatch",
    "TaskResultStatus",
    "default_task_backend",
    "import_tasks_framework",
    "normalize_json",
    "task",
    "task_backends",
    "task_enqueued",
    "task_finished",
    "task_started",
]

if TYPE_CHECKING:
    from django.tasks import (
        DEFAULT_TASK_BACKEND_ALIAS,
        default_task_backend,
        task,
        task_backends,
    )
    from django.tasks.backends.base import BaseTaskBackend
    from django.tasks.base import (
        Task,
        TaskContext,
        TaskError,
        TaskResult,
        TaskResultStatus,
    )
    from django.tasks.exceptions import (
        InvalidTask,
        TaskResultDoesNotExist,
        TaskResultMismatch,
    )
    from django.tasks.signals import task_enqueued, task_finished, task_started
    from django.utils.json import normalize_json

    HAS_CORE_TASKS = True
    IMMEDIATE_BACKEND_PATH = "django.tasks.backends.immediate.ImmediateBackend"
    DUMMY_BACKEND_PATH = "django.tasks.backends.dummy.DummyBackend"
else:
    import django

    HAS_CORE_TASKS = django.VERSION >= (6, 0)
    """True when the framework comes from Django core rather than the backport."""

    if HAS_CORE_TASKS:
        from django.tasks import (
            DEFAULT_TASK_BACKEND_ALIAS,
            default_task_backend,
            task,
            task_backends,
        )
        from django.tasks.backends.base import BaseTaskBackend
        from django.tasks.base import (
            Task,
            TaskContext,
            TaskError,
            TaskResult,
            TaskResultStatus,
        )
        from django.tasks.exceptions import (
            InvalidTask,
            TaskResultDoesNotExist,
            TaskResultMismatch,
        )
        from django.tasks.signals import task_enqueued, task_finished, task_started
        from django.utils.json import normalize_json

        IMMEDIATE_BACKEND_PATH = "django.tasks.backends.immediate.ImmediateBackend"
        DUMMY_BACKEND_PATH = "django.tasks.backends.dummy.DummyBackend"
    else:
        try:
            from django_tasks import (
                DEFAULT_TASK_BACKEND_ALIAS,
                default_task_backend,
                task,
                task_backends,
            )
            from django_tasks.backends.base import BaseTaskBackend
            from django_tasks.base import (
                Task,
                TaskContext,
                TaskError,
                TaskResult,
                TaskResultStatus,
            )
            from django_tasks.exceptions import (
                TaskResultDoesNotExist,
                TaskResultMismatch,
            )
            from django_tasks.signals import (
                task_enqueued,
                task_finished,
                task_started,
            )
            from django_tasks.utils import normalize_json
        except ImportError as exc:
            raise ImportError(
                f"django-ox is a backend for Django's Tasks framework, which "
                f"Django {django.get_version()} does not ship. Install the "
                f"backport that provides it on Django 5.2: "
                f"pip install 'django-ox[backport]'"
            ) from exc

        try:
            from django_tasks.exceptions import InvalidTask
        except ImportError:
            # Released backports up to 0.12.0 still carry the pre-merge name.
            # Django dropped the Error suffix when the framework landed in
            # core, and the backport's own main branch has followed.
            from django_tasks.exceptions import InvalidTaskError as InvalidTask

        IMMEDIATE_BACKEND_PATH = "django_tasks.backends.immediate.ImmediateBackend"
        DUMMY_BACKEND_PATH = "django_tasks.backends.dummy.DummyBackend"


def import_tasks_framework() -> None:
    """
    Import the Tasks framework so its system check is registered.

    Django registers its tasks system check, the one that calls every backend's
    check(), when django.tasks is first imported. A project whose settings and
    URLconf import it nowhere would otherwise run `manage.py check` without
    ever reaching django_ox.E001 to E005.

    The backport registers the same check, and the receivers that rebuild the
    backend handler when TASKS changes, from its AppConfig.ready() rather than
    at import time. Importing the package is therefore not enough on Django
    5.2: import the two modules its AppConfig imports. Both are idempotent, so
    a project that also lists django_tasks in INSTALLED_APPS is unaffected.
    """
    if HAS_CORE_TASKS:
        import django.tasks  # noqa: F401
    else:
        from django_tasks import checks, signals  # noqa: F401
