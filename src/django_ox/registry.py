"""
The tasks a schedule may name.

A schedule that lives in settings names a task by dotted path, and that is
safe because settings are deployed. A schedule that lives in a database row
is edited by a person, and a dotted path in an editable row means anyone
holding the change permission can run any importable callable with any
arguments.

So a row does not name code. It names a **key** in a registry that code
owns:

    from django_ox.registry import schedulable

    @schedulable("reports.daily", form=DailyReportArgs, permission="reports.run")
    @task
    def daily_report(region: str) -> None:
        ...

The row stores ``"reports.daily"`` and validated arguments. The dotted path
never leaves the deployment. Renaming the module later does not mean editing
production rows, because the key is the identity and the code moves
underneath it.

Registration is discovered lazily, the first time anything asks the registry
a question, by importing every installed app's ``tasks`` module. Lazily
rather than from AppConfig.ready() so that a project not using the registry
sees no import it did not already have: nothing here runs until something
needs an answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django import forms
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.utils.module_loading import autodiscover_modules, import_string

from .compat import InvalidTask, Task

KEY_MAX_LENGTH = 128

_registry: dict[str, ScheduleKind] = {}
_discovered = False


@dataclass(frozen=True)
class ScheduleKind:
    """One thing a schedule row is allowed to run."""

    key: str
    task: Task[..., Any]
    form: type[Any] | None = None
    permission: str | None = None


class ArgsForm(forms.Form):
    """
    Validates the arguments a schedule row carries.

    A plain Django form, so a project already knows the API and the admin
    gets per-field errors for free, with three of Django's form defaults
    closed because they are written for HTML posts and a schedule row is
    not one.

    **An unknown key is an error, not ignored.** `_clean_fields` iterates
    the declared fields, so a typo or an argument left behind by a
    refactor passes validation and then fails inside the task, hours later
    and somewhere else. Here it fails at the row.

    **A text field will not silently swallow a non-string.** A CharField
    given 5 returns "5", so a row meaning an integer reaches the task as a
    string and the failure looks like a bug in the task. If the value is
    meant to be a number the field should say so.

    Not closed, and documented rather than guessed at: an absent
    BooleanField with `required=False` cleans to False, which is HTML
    checkbox semantics. For a schedule row "absent" and "false" are
    usually the same intent, so this is left alone; use a
    NullBooleanField when they differ.
    """

    def __init__(self, data: dict[str, Any] | None = None, **kwargs: Any) -> None:
        super().__init__(data=data if data is not None else {}, **kwargs)

    def clean(self) -> dict[str, Any]:
        # super().clean() is typed as optional; the base returns cleaned_data.
        cleaned: dict[str, Any] = super().clean() or {}
        unknown = sorted(set(self.data) - set(self.fields))
        if unknown:
            raise ValidationError(
                "Unknown argument(s): %(names)s.",
                code="unknown_argument",
                params={"names": ", ".join(unknown)},
            )
        # Read from self.data, the values as given, because by this point a
        # text field has already turned 5 into "5" and cleaned_data cannot
        # report that. Public API only: overriding _clean_fields would
        # reach into a private method the type stubs do not even declare.
        for name, field in self.fields.items():
            if not isinstance(field, forms.CharField):
                continue
            value = self.data.get(name)
            if value is not None and not isinstance(value, str):
                self.add_error(
                    name,
                    ValidationError(
                        "Expected a string, got %(got)s. A text field would "
                        "coerce it silently.",
                        code="not_a_string",
                        params={"got": type(value).__name__},
                    ),
                )
        return cleaned


def register(kind: ScheduleKind) -> None:
    """
    Add a kind to the registry.

    A duplicate key is an error rather than a silent overwrite: two
    registrations under one key would make which task runs depend on
    import order, and the row naming that key would be pointing at
    whichever of them happened to win.
    """
    if not isinstance(kind.key, str) or not kind.key:
        raise ImproperlyConfigured("A schedulable key must be a non-empty string.")
    if len(kind.key) > KEY_MAX_LENGTH:
        raise ImproperlyConfigured(
            f"Schedulable key {kind.key!r} exceeds {KEY_MAX_LENGTH} characters."
        )
    if not isinstance(kind.task, Task):
        raise ImproperlyConfigured(
            f"Schedulable {kind.key!r} is not a django.tasks Task. Apply "
            "@schedulable above @task, not below it."
        )
    existing = _registry.get(kind.key)
    if existing is not None and existing != kind:
        raise ImproperlyConfigured(
            f"Schedulable key {kind.key!r} is already registered for a different task."
        )
    _registry[kind.key] = kind


def schedulable(
    key: str,
    *,
    form: type[Any] | None = None,
    permission: str | None = None,
) -> Any:
    """
    Expose a task under `key`, so a schedule row may name it.

    Goes above @task, because it registers the Task object rather than the
    undecorated function.

    `form` is a django_ox.registry.ArgsForm subclass validating the row's
    arguments. `permission` is a permission string checked in addition to
    the model permissions before a row naming this key may be written.
    """

    def decorate(task_obj: Task[..., Any]) -> Task[..., Any]:
        register(ScheduleKind(key=key, task=task_obj, form=form, permission=permission))
        return task_obj

    return decorate


def _register_from_settings() -> None:
    """
    Apply every backend's SCHEDULABLE_TASKS.

    The system check reads this option as well, but a check runs under
    `manage.py` and nowhere else. A worker started with `--skip-checks`,
    and every WSGI or ASGI process, would otherwise hold a registry that
    only the decorator had filled, and a row naming a key declared in
    settings would be skipped as unknown by the deployment that declared
    it.
    """
    for alias, config in (getattr(settings, "TASKS", None) or {}).items():
        options = (config or {}).get("OPTIONS") or {}
        if "SCHEDULABLE_TASKS" in options:
            kinds_from_options(options, alias)


def _discover() -> None:
    """Fill the registry from both channels before anyone reads it."""
    global _discovered
    if not _discovered:
        # Set first: an app whose tasks module raises would otherwise be
        # re-imported on every lookup, turning one failure into many.
        _discovered = True
        autodiscover_modules("tasks")
    # Every call, not only the first. Registration is idempotent for an
    # identical kind, and an entry that cannot be built has to keep saying
    # so rather than leaving one silent gap for the rest of the process.
    _register_from_settings()


def kinds() -> dict[str, ScheduleKind]:
    """Every registered kind, keyed by key."""
    _discover()
    return dict(_registry)


def get(key: str) -> ScheduleKind:
    """
    The kind registered under `key`.

    Raises KeyError, which callers turn into a validation error on the
    write path and a skipped row on the dispatch path. A key this
    deployment does not know is not necessarily wrong: during a rolling
    deploy it may be one only the newer code registers.
    """
    _discover()
    return _registry[key]


def kinds_from_options(options: dict[str, Any], backend_alias: str) -> None:
    """
    Register the entries under a backend's OPTIONS["SCHEDULABLE_TASKS"].

    The second registration channel, and the only one a system check can
    validate without importing application code at check time. The
    decorator colocates a kind with its task; this puts it where a
    deployment can see and override it.

    Shape:

        "SCHEDULABLE_TASKS": {
            "reports.daily": {
                "task": "reports.tasks.daily",
                "form": "reports.forms.DailyArgs",
                "permission": "reports.run_daily",
            },
        }

    A bare dotted path is accepted as shorthand for a kind with no form
    and no permission.
    """
    raw = options.get("SCHEDULABLE_TASKS", {})
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(
            "SCHEDULABLE_TASKS must be a mapping of key to configuration."
        )
    for key, config in raw.items():
        prefix = f"Schedulable {key!r}"
        if isinstance(config, str):
            config = {"task": config}
        if not isinstance(config, dict):
            raise ImproperlyConfigured(f"{prefix} must be a mapping or a dotted path.")
        unknown = set(config) - {"task", "form", "permission"}
        if unknown:
            raise ImproperlyConfigured(
                f"{prefix} has unknown key(s): {', '.join(sorted(unknown))}."
            )
        if "task" not in config:
            raise ImproperlyConfigured(f"{prefix} is missing 'task'.")
        # InvalidTask as well as ImportError, as the settings schedule loader
        # does: a task module whose `@task` declaration its backend refuses
        # raises it while it is imported.
        try:
            task_obj = import_string(config["task"])
        except (ImportError, InvalidTask) as exc:
            raise ImproperlyConfigured(
                f"{prefix}: cannot import task {config['task']!r} ({exc})."
            ) from exc
        form = None
        if "form" in config:
            try:
                form = import_string(config["form"])
            except ImportError as exc:
                raise ImproperlyConfigured(
                    f"{prefix}: cannot import form {config['form']!r} ({exc})."
                ) from exc
            if not (isinstance(form, type) and issubclass(form, ArgsForm)):
                raise ImproperlyConfigured(
                    f"{prefix}: form {config['form']!r} is not a "
                    "django_ox.registry.ArgsForm subclass."
                )
        permission = config.get("permission")
        if permission is not None and not isinstance(permission, str):
            raise ImproperlyConfigured(f"{prefix}: 'permission' must be a string.")
        register(ScheduleKind(key=key, task=task_obj, form=form, permission=permission))
