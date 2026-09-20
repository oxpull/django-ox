"""
Django admin for the task table.

Loaded by the admin's autodiscover, so it is only imported when
django.contrib.admin is installed; a project without the admin never sees
this module and needs nothing from it. The list shows what a worker would
see; the detail page is read-only, with every attempt's traceback; and two
actions call django_ox.actions.retry_many and discard_many on the selected
rows, one conditional UPDATE per thousand rows in one transaction, and
report counts.

Every page here reads the database its rows are written to, and both
get_queryset methods below say so. A ModelAdmin builds its queryset from the
default manager, which follows db_for_read. Under a router that sends reads
to a replica these pages would answer from a database no worker writes, and
the change form submits what it rendered.
"""

from __future__ import annotations

from copy import copy
from typing import TYPE_CHECKING, Any, cast

from django import forms
from django.conf import settings
from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.db import router, transaction
from django.db.models import (
    CharField,
    DateTimeField,
    OuterRef,
    QuerySet,
    Subquery,
    Value,
)
from django.db.models.functions import Concat
from django.http import HttpRequest
from django.utils import timezone
from django.utils.html import format_html, format_html_join
from django.utils.module_loading import import_string

from . import actions, registry, stored
from .compat import DEFAULT_TASK_BACKEND_ALIAS
from .models import OxSchedule, OxScheduleTick, OxTask
from .schedules import STORED_KEY_PREFIX

if TYPE_CHECKING:
    from datetime import datetime

    _ModelAdmin = admin.ModelAdmin[OxTask]
    _ScheduleAdmin = admin.ModelAdmin[OxSchedule]
    _ScheduleForm = forms.ModelForm[OxSchedule]
else:
    _ModelAdmin = admin.ModelAdmin
    _ScheduleAdmin = admin.ModelAdmin
    _ScheduleForm = forms.ModelForm

ERROR_TEMPLATE = (
    '<p><strong>Attempt {}: {}</strong></p><pre style="white-space: pre-wrap">{}</pre>'
)


@admin.register(OxTask)
class OxTaskAdmin(_ModelAdmin):
    list_display = (
        "id",
        "task_path",
        "queue_name",
        "status",
        "attempts",
        "enqueued_at",
        "finished_at",
    )
    list_filter = ("status", "queue_name")
    search_fields = ("id", "task_path")
    ordering = ("-enqueued_at",)
    date_hierarchy = "enqueued_at"
    actions = ("retry_selected", "discard_selected")
    readonly_fields = (
        "id",
        "task_path",
        "args",
        "kwargs",
        "queue_name",
        "priority",
        "takes_context",
        "backend_name",
        "status",
        "run_after",
        "attempts",
        "max_attempts",
        "return_value",
        "worker_ids",
        "enqueued_at",
        "started_at",
        "last_attempted_at",
        "finished_at",
        "locked_by",
        "locked_at",
        "lease_expires_at",
        "lease_epoch",
        "attempt_errors",
    )
    fieldsets = (
        (None, {"fields": ("id", "task_path", "args", "kwargs", "status")}),
        (
            "Queue",
            {
                "fields": (
                    "queue_name",
                    "priority",
                    "backend_name",
                    "takes_context",
                    "run_after",
                )
            },
        ),
        (
            "Attempts",
            {
                "fields": (
                    "attempts",
                    "max_attempts",
                    "worker_ids",
                    "return_value",
                    "attempt_errors",
                )
            },
        ),
        (
            "Timing",
            {
                "fields": (
                    "enqueued_at",
                    "started_at",
                    "last_attempted_at",
                    "finished_at",
                )
            },
        ),
        (
            "Lease",
            {
                "fields": (
                    "locked_by",
                    "locked_at",
                    "lease_expires_at",
                    "lease_epoch",
                )
            },
        ),
    )

    # Rows are written by workers, by django_ox.actions and by
    # django_ox._waiting. The admin can read them and run the two actions; it
    # cannot add, edit or delete one, because a hand-edited status would
    # bypass the lease and a delete could take a row from under a running
    # worker. ox_prune deletes.
    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(
        self, request: HttpRequest, obj: OxTask | None = None
    ) -> bool:
        return False

    def has_delete_permission(
        self, request: HttpRequest, obj: OxTask | None = None
    ) -> bool:
        return False

    def get_queryset(self, request: HttpRequest) -> QuerySet[OxTask]:
        """
        The alias the task rows are written to, for every page here.

        A changelist, a detail page and the queryset an action is handed all
        start here, so this one line decides the database all of them read.
        Left to `db_for_read`, a router that splits reads sends this page to
        a replica: a task enqueued a moment ago is not in the list, and the
        detail page for one that exists reports it as deleted.
        """
        return super().get_queryset(request).using(router.db_for_write(self.model))

    @admin.display(description="Attempt errors")
    def attempt_errors(self, obj: OxTask) -> str:
        if not obj.errors:
            return "No errors recorded."
        return format_html_join(
            "",
            ERROR_TEMPLATE,
            (
                (index, error["exception_class_path"], error["traceback"])
                for index, error in enumerate(obj.errors, start=1)
            ),
        )

    def _apply(
        self,
        request: HttpRequest,
        queryset: QuerySet[OxTask],
        action: Any,
        verb: str,
    ) -> None:
        done, skipped = action(queryset)
        self.message_user(request, f"{verb} {done} task(s).", messages.SUCCESS)
        if skipped:
            self.message_user(
                request,
                format_html(
                    "Skipped {} task(s) whose status did not allow it.", skipped
                ),
                messages.WARNING,
            )

    @admin.action(description="Retry selected tasks", permissions=("retry_or_discard",))
    def retry_selected(self, request: HttpRequest, queryset: QuerySet[OxTask]) -> None:
        self._apply(request, queryset, actions.retry_many, "Retried")

    @admin.action(
        description="Discard selected tasks", permissions=("retry_or_discard",)
    )
    def discard_selected(
        self, request: HttpRequest, queryset: QuerySet[OxTask]
    ) -> None:
        self._apply(request, queryset, actions.discard_many, "Discarded")

    def has_retry_or_discard_permission(self, request: HttpRequest) -> bool:
        # The model's change permission, without the change form: the
        # actions are the only writes the admin offers.
        opts = self.opts
        return request.user.has_perm(f"{opts.app_label}.change_{opts.model_name}")


class OxScheduleForm(_ScheduleForm):
    """
    The change form for a stored schedule.

    `task_key` is a choice drawn from the registry rather than a text
    input. That is the difference between this admin and every other
    Django scheduling admin: the field cannot express a task the code did
    not expose, so holding the change permission here is not permission to
    run any importable callable.

    A ChoiceField validates membership on the server, so a hand-made POST
    naming an unexposed task is refused here and not merely absent from the
    rendered select. The model checks membership again in its own clean(),
    which is what covers the write paths that never build a form:
    objects.create(), a data migration, a fixture.

    The field is a choice rather than a text input because a text input
    would let anyone holding the change permission name any importable
    callable.

    The choices are every registered key, not the ones this user may
    schedule. Narrowing them per user would make the change form for an
    existing restricted row fail validation on the key the row already
    holds, and it would hide which keys exist rather than saying what is
    needed to use one. The refusal is a field error instead.
    """

    #: The person submitting, when there is one. OxScheduleAdmin.get_form
    #: binds it; a form built any other way has none, and then the check
    #: below is skipped exactly as it is for a write with no `user`.
    user: Any = None

    class Meta:
        model = OxSchedule
        fields = (
            "name",
            "task_key",
            "trigger",
            "cron",
            "every_seconds",
            "phase_seconds",
            "arguments",
            "enabled",
            "end_time",
            "starting_deadline_seconds",
        )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        keys = sorted(registry.kinds())
        self.fields["task_key"] = forms.ChoiceField(
            choices=[(key, key) for key in keys],
            label="Task",
            help_text=(
                "Tasks the code has exposed with @schedulable or "
                "SCHEDULABLE_TASKS. Nothing else can be scheduled."
                if keys
                else "No tasks are exposed yet. django-ox imports each "
                "installed app's `tasks` module, so @schedulable takes "
                "effect there; from any other module, name the task in "
                "OPTIONS['SCHEDULABLE_TASKS']."
            ),
        )

    def _effective_start_time(self, cleaned: dict[str, Any]) -> Any:
        """
        The activation boundary this submission will end up with.

        `start_time` is not a form field. The service layer sets it: to now
        when a schedule is created, retimed or re-enabled, and to whatever
        the row already held otherwise. Django excludes a field the form
        does not carry from its own validation, so without this the pair is
        never compared on the way in.
        """
        if self.instance.pk is None:
            return timezone.now()
        moved = any(
            cleaned.get(field) != getattr(self.instance, field)
            for field in stored.TIMING_FIELDS
        ) or (cleaned.get("enabled") and not self.instance.enabled)
        return timezone.now() if moved else self.instance.start_time

    def _check_the_registry_permission(self, cleaned: dict[str, Any]) -> None:
        """
        Put a registry entry's own permission in front of the person.

        django_ox.stored enforces it on every write path, form or no form,
        and that is where the guarantee lives; nothing unpermitted has ever
        been written. But the refusal arrived out of save_model as a bare
        403: no admin chrome, no link back, no field error, and every value
        the person had typed discarded. This form answers any other
        validation failure with the page and the values intact, so the
        refusal an advertised feature produces was the one it handled
        worst.

        Checked against a copy of the instance, so an object-level
        permission backend sees the row it would see at the write.
        """
        task_key = cleaned.get("task_key")
        if not task_key:
            return
        candidate = copy(self.instance)
        candidate.task_key = task_key
        try:
            stored.check_permission(candidate, self.user)
        except PermissionDenied as exc:
            self.add_error("task_key", str(exc))

    def clean(self) -> dict[str, Any]:
        cleaned = cast("dict[str, Any]", super().clean())
        self._check_the_registry_permission(cleaned)
        end_time = cleaned.get("end_time")
        if end_time is not None and end_time <= self._effective_start_time(cleaned):
            # As a field error rather than an exception out of the save. The
            # same rule runs again in validate_schedule, which is the
            # authority for every write path; this is what puts it in front
            # of the person filling the form in.
            self.add_error(
                "end_time",
                "The end time must be after the start time, which this "
                "submission sets to now.",
            )
        return cleaned


@admin.register(OxSchedule)
class OxScheduleAdmin(_ScheduleAdmin):
    form = OxScheduleForm
    list_display = (
        "name",
        "task_key",
        "timing",
        "enabled",
        "start_time",
        # Here because "Run selected schedules once now" ignores it, so an
        # operator choosing rows has to be able to see which of them have
        # already ended.
        "end_time",
        "last_tick",
    )
    list_filter = ("enabled", "trigger")
    search_fields = ("name", "task_key")
    ordering = ("name",)
    actions = ("enable_selected", "disable_selected", "run_once_now")
    readonly_fields = ("start_time", "created_at", "updated_at")

    #: Said on the pages where a person is about to write a row that
    #: nothing would dispatch. The condition cannot be a system check: a
    #: check runs without importing application code, so it can see
    #: SCHEDULABLE_TASKS in settings but never an @schedulable decorator,
    #: and it would need a database read to know whether any row exists.
    #: Here the registry is populated and the person is standing in front
    #: of the mistake.
    NO_SOURCE_WARNING = (
        "No backend sets OPTIONS['SCHEDULE_SOURCE'] to "
        "django_ox.stored.DatabaseScheduleSource, so schedules saved here "
        "are stored and never dispatched."
    )

    def _warn_if_nothing_dispatches(self, request: HttpRequest) -> None:
        # On the rendered page only. An action POST is handled by
        # changelist_view and then redirects to it, so warning on both
        # would say the same thing twice on the page that follows.
        if request.method == "GET" and self._stored_backend() is None:
            self.message_user(request, self.NO_SOURCE_WARNING, messages.WARNING)

    def changelist_view(
        self, request: HttpRequest, extra_context: dict[str, Any] | None = None
    ) -> Any:
        self._warn_if_nothing_dispatches(request)
        return super().changelist_view(request, extra_context)

    def add_view(
        self,
        request: HttpRequest,
        form_url: str = "",
        extra_context: dict[str, Any] | None = None,
    ) -> Any:
        self._warn_if_nothing_dispatches(request)
        return super().add_view(request, form_url, extra_context)

    def get_queryset(self, request: HttpRequest) -> QuerySet[OxSchedule]:
        """
        The alias the schedules are written to, for every page here.

        The same pin `OxTaskAdmin` makes, and here a write depends on it.
        The change form is built from this queryset and submits every field,
        so a form rendered from a replica writes the replica's values back
        over the primary. `stored.update_schedule` takes the row lock and
        writes only the submitted fields to stop a caller holding a stale
        copy doing exactly that, and a stale form walks straight through it.
        Nothing raises and nothing is logged; the newer values are gone.

        Django's own `ModelAdmin.get_object` reads this queryset too. So does
        the count `delete_selected` takes before it deletes. Unpinned, the
        redirect after an add lands on "doesn't exist" for a row that was
        just written, and the delete action removes nothing and says nothing.

        The `last_tick_at` annotation serves the Last tick column, which
        used to spend one query per row on a lookup the page already had
        the schedules for. The tick key is the prefix plus the schedule's
        primary key, so the subquery rebuilds the key in SQL, correlated on
        the outer row. It is compiled into the query this method returns,
        and django_ox.E008 refuses a router that writes ticks anywhere but
        the schedules' database, so the inlined lookup reads the alias the
        ticks are written to without naming it a second time.
        """
        # Keep the pk numeric. On MySQL, casting it to char gives the key
        # implicit coercibility, which can conflict with schedule_name's
        # collation. The prefix plus a numeric pk produces a coercible
        # key, so the column's collation wins.
        ticks = (
            OxScheduleTick.objects.filter(
                schedule_name=Concat(
                    Value(STORED_KEY_PREFIX),
                    OuterRef("pk"),
                    output_field=CharField(),
                )
            )
            .order_by("-scheduled_for")
            .values("scheduled_for")[:1]
        )
        return (
            super()
            .get_queryset(request)
            .using(stored.schedule_db_alias())
            .annotate(last_tick_at=Subquery(ticks, output_field=DateTimeField()))
        )

    def get_form(
        self,
        request: HttpRequest,
        obj: OxSchedule | None = None,
        change: bool = False,  # noqa: FBT001, FBT002 - ModelAdmin's own signature
        **kwargs: Any,
    ) -> Any:
        """
        Bind the requesting user onto the form class.

        ModelAdmin hands the form class to the change view, which builds it
        with arguments of its own, so there is no other seam to pass a user
        through. A subclass per request rather than a partial, because the
        admin also reads `base_fields` off what this returns.
        """
        form_class = super().get_form(request, obj, change=change, **kwargs)
        return type(form_class.__name__, (form_class,), {"user": request.user})

    @admin.display(description="Timing")
    def timing(self, obj: OxSchedule) -> str:
        if obj.trigger == OxSchedule.Trigger.CRON:
            return obj.cron
        every = f"every {obj.every_seconds}s"
        return f"{every} +{obj.phase_seconds}s" if obj.phase_seconds else every

    @admin.display(description="Last tick")
    def last_tick(self, obj: OxSchedule) -> str:
        # get_queryset() supplies the newest tick, on the schedules'
        # database. The alias reasoning lives there with it.
        tick = cast("datetime | None", getattr(obj, "last_tick_at", None))
        return "never" if tick is None else f"{tick:%Y-%m-%d %H:%M}"

    def save_model(
        self,
        request: HttpRequest,
        obj: OxSchedule,
        form: Any,
        change: bool,  # noqa: FBT001 - ModelAdmin's own signature
    ) -> None:
        """
        Route through the service layer rather than calling save().

        The default implementation saves the instance directly, which would
        leave the activation boundary and the change marker untouched: a
        retimed schedule would keep a boundary set for its old timing, and
        no worker would learn the row had moved.
        """
        if change:
            # The values this request submitted, not every field off the
            # instance. A form is built from a row read at the start of the
            # request, so saving all of it would write back whatever else has
            # changed since, including a boundary someone else just moved.
            submitted = {
                field: form.cleaned_data[field]
                for field in form.fields
                if field in form.cleaned_data
            }
            stored.update_schedule(obj, user=request.user, **submitted)
        else:
            created = stored.create_schedule(
                user=request.user,
                **{
                    field: getattr(obj, field)
                    for field in (
                        "name",
                        "task_key",
                        "trigger",
                        "cron",
                        "every_seconds",
                        "phase_seconds",
                        "arguments",
                        "enabled",
                        "end_time",
                        "starting_deadline_seconds",
                    )
                },
            )
            # The service function builds and saves its own instance, and
            # the admin goes on using the one it holds. Without this its pk
            # stays None: "save and continue editing" redirects to a URL
            # containing None, and the log entry records the row's id as
            # the string "None", so its history is attached to nothing.
            obj.pk = created.pk
            # From the alias the row was written to, which is the one the
            # service function resolved and used. Unqualified this follows
            # db_for_read, and a replica that has not seen the row yet
            # raises DoesNotExist on a save that succeeded.
            obj.refresh_from_db(using=created._state.db)

    def has_change_permission(
        self, request: HttpRequest, obj: OxSchedule | None = None
    ) -> bool:
        """
        The model permission, plus the registry entry's own if it has one.

        Checked here because this is the hook Django gives an object to.
        The per-action hook does not take one, so an action's own check
        cannot see which task the selected rows name.
        """
        if not super().has_change_permission(request, obj):
            return False
        return self._registry_permitted(request, obj)

    def has_delete_permission(
        self, request: HttpRequest, obj: OxSchedule | None = None
    ) -> bool:
        """
        The registry entry's permission gates deletion too.

        Stopping a production schedule by deleting it is the same authority
        the permission exists to gate, so checking it only on change would
        leave the obvious way round.
        """
        if not super().has_delete_permission(request, obj):
            return False
        return self._registry_permitted(request, obj)

    def _registry_permitted(self, request: HttpRequest, obj: OxSchedule | None) -> bool:
        if obj is None:
            return True
        kind = registry.kinds().get(obj.task_key)
        if kind is None or kind.permission is None:
            return True
        return bool(
            request.user.has_perm(kind.permission, obj)
            or request.user.has_perm(kind.permission)
        )

    def delete_model(self, request: HttpRequest, obj: OxSchedule) -> None:
        stored.delete_schedule(obj)

    def delete_queryset(
        self, request: HttpRequest, queryset: QuerySet[OxSchedule]
    ) -> None:
        # One marker bump for the batch rather than one per row: workers only
        # need to learn that something moved.
        alias = stored.schedule_db_alias()
        with transaction.atomic(using=alias):
            queryset.using(alias).delete()
            stored._touch_change_row(alias)

    def _set_enabled(
        self,
        request: HttpRequest,
        queryset: QuerySet[OxSchedule],
        *,
        enabled: bool,
    ) -> None:
        # The rows this action works on come from the alias it writes to,
        # the way delete_queryset takes them. The queryset Django hands an
        # action names no alias, so unqualified it is read through
        # db_for_read: on a replica that is behind, the selection and the
        # count below are of rows as they were, not as they are.
        queryset = queryset.using(stored.schedule_db_alias())
        changed = 0
        for schedule in queryset:
            if not self.has_change_permission(request, schedule):
                continue
            stored.update_schedule(schedule, user=request.user, enabled=enabled)
            changed += 1
        verb = "Enabled" if enabled else "Disabled"
        self.message_user(request, f"{verb} {changed} schedule(s).", messages.SUCCESS)
        skipped = queryset.count() - changed
        if skipped:
            self.message_user(
                request,
                f"Skipped {skipped} schedule(s) you do not have permission to change.",
                messages.WARNING,
            )

    @admin.action(description="Enable selected schedules", permissions=("change",))
    def enable_selected(
        self, request: HttpRequest, queryset: QuerySet[OxSchedule]
    ) -> None:
        self._set_enabled(request, queryset, enabled=True)

    @admin.action(description="Disable selected schedules", permissions=("change",))
    def disable_selected(
        self, request: HttpRequest, queryset: QuerySet[OxSchedule]
    ) -> None:
        self._set_enabled(request, queryset, enabled=False)

    @admin.action(
        description="Run selected schedules once now", permissions=("change",)
    )
    def run_once_now(
        self, request: HttpRequest, queryset: QuerySet[OxSchedule]
    ) -> None:
        """
        Enqueue each selected schedule's task immediately.

        No tick row is written, because this is not a tick: the schedule's
        own ticks are unaffected and the next one still fires. A manual run
        does not consume a scheduled one.

        Built through the same path a dispatched tick takes, so a manual run
        carries the same arguments and goes to the same backend. Enqueueing
        the task directly passed the row's raw values where a tick passes
        the form's cleaned ones, and skipped the backend binding, so the
        two could differ in both what they carried and where they landed.

        A disabled schedule and one past its end time both run: this is the
        one way to run a paused schedule, and a run is not a tick, so
        neither bound applies to it. Both are reported, because the
        changelist shows paused and running rows together, an admin action
        has no confirmation step, and a success count alone leaves an
        operator who mis-selected a paused schedule with nothing to read.
        """
        alias, options, source_class = self._stored_backend() or (
            DEFAULT_TASK_BACKEND_ALIAS,
            {},
            stored.DatabaseScheduleSource,
        )
        source = source_class(options, alias)
        if not callable(getattr(source, "_to_schedule", None)):
            # A source is only required to answer schedules(). One that
            # reaches the rows some other way has nothing for a manual run
            # to build a single row with, so the shipped reading is used
            # for that and the project's own alias is still the one the
            # task is enqueued on.
            source = stored.DatabaseScheduleSource(options, alias)
        now = timezone.now()
        run, skipped, overridden, refused = 0, 0, 0, 0
        # A manual run enqueues from the row's own values rather than
        # re-reading them, so the alias this reads is the alias the run
        # carries: unqualified, a schedule edited a moment ago would run
        # with the arguments a lagging replica still holds.
        queryset = queryset.using(stored.schedule_db_alias())
        for schedule in queryset:
            if not self.has_change_permission(request, schedule):
                # Counted separately from `skipped`, which reports a row
                # that cannot run as written: telling an operator their
                # payroll schedule is broken when they are simply not
                # allowed to run it sends them to fix the wrong thing.
                refused += 1
                continue
            try:
                built = source._to_schedule(schedule)
            except Exception:
                skipped += 1
                continue
            built.task.enqueue(*built.args, **built.kwargs)
            run += 1
            # Counted after the enqueue, so a row that could not be built is
            # reported as skipped rather than as a run that overrode a bound.
            if not schedule.enabled or (
                schedule.end_time is not None and schedule.end_time < now
            ):
                overridden += 1
        self.message_user(request, f"Enqueued {run} task(s).", messages.SUCCESS)
        if skipped:
            self.message_user(
                request,
                f"Skipped {skipped} schedule(s) that cannot run as written.",
                messages.WARNING,
            )
        if refused:
            self.message_user(
                request,
                f"Skipped {refused} schedule(s) you do not have permission to change.",
                messages.WARNING,
            )
        if overridden:
            self.message_user(
                request,
                f"Ran {overridden} schedule(s) that were disabled or past "
                "their end time. A manual run ignores both.",
                messages.WARNING,
            )

    @staticmethod
    def _stored_backend() -> tuple[str, dict[str, Any], type[Any]] | None:
        """
        The backend whose workers dispatch the stored schedules, its
        options, and the class it reads them with, or None when no backend
        names one.

        Read from settings rather than from the instantiated backends, the
        same way the system checks read it: a check must not construct
        every backend in a project to answer a question about a dictionary.

        The test applied here is the loader's own: build the class the
        path names and ask whether it answers `schedules()`. Nothing else
        decides what the worker dispatches from, so nothing else may
        decide what this page says about it. Matched by name, and then by
        `issubclass` against the shipped source, a project whose source
        is written by composition rather than by inheritance -- which the
        loader documents as supported -- was told its schedules are
        stored and never dispatched while its worker was dispatching
        them, and its manual run went to the default alias.

        The cost of taking the loader's answer is that a source which
        answers `schedules()` without reading the rows counts too. That
        is the same class of source the worker would run, and the admin
        has nothing else to read it by; a false warning on a working
        deployment is the worse of the two.

        The class is returned with the options because a subclass that
        changes how a row becomes a schedule has to be the one the manual
        run builds with, or the admin runs something the project did not
        define.
        """
        for alias, config in settings.TASKS.items():
            options = config.get("OPTIONS") if isinstance(config, dict) else None
            if not isinstance(options, dict):
                continue
            path = options.get("SCHEDULE_SOURCE")
            if not isinstance(path, str):
                continue
            try:
                source_class = import_string(path)
            except ImportError:
                # The worker refuses this configuration outright. Here it
                # is one backend of several and the page still has to load,
                # so it is simply not the one.
                continue
            if not isinstance(source_class, type):
                continue
            try:
                source = source_class(options, str(alias))
            except Exception:  # noqa: S112 - the worker reports it, not this page
                # Same reasoning as the failed import: a source that cannot
                # be built is refused at worker startup and by the system
                # check, and the admin still has to render.
                continue
            if callable(getattr(source, "schedules", None)):
                return str(alias), options, source_class
        return None
