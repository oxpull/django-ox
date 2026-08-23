"""
Django admin for the task table.

Loaded by the admin's autodiscover, so it is only imported when
django.contrib.admin is installed; a project without the admin never sees
this module and needs nothing from it. The list shows what a worker would
see; the detail page is read-only, with every attempt's traceback; and two
actions call django_ox.actions on the selected rows and report counts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.html import format_html, format_html_join

from . import actions
from .models import OxTask

if TYPE_CHECKING:
    _ModelAdmin = admin.ModelAdmin[OxTask]
else:
    _ModelAdmin = admin.ModelAdmin

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
        ("Lease", {"fields": ("locked_by", "locked_at", "lease_epoch")}),
    )

    # Rows are written by workers and by django_ox.actions only. The admin
    # can read them and run the two actions; it cannot add, edit or delete
    # one, because a hand-edited status would bypass the lease and a delete
    # could take a row from under a running worker. ox_prune deletes.
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
        done = sum(1 for pk in queryset.values_list("pk", flat=True) if action(pk))
        skipped = queryset.count() - done
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
        self._apply(request, queryset, actions.retry, "Retried")

    @admin.action(
        description="Discard selected tasks", permissions=("retry_or_discard",)
    )
    def discard_selected(
        self, request: HttpRequest, queryset: QuerySet[OxTask]
    ) -> None:
        self._apply(request, queryset, actions.discard, "Discarded")

    def has_retry_or_discard_permission(self, request: HttpRequest) -> bool:
        # The model's change permission, without the change form: the
        # actions are the only writes the admin offers.
        opts = self.opts
        return request.user.has_perm(f"{opts.app_label}.change_{opts.model_name}")
