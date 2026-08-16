import uuid

from django.db import models


class OxTask(models.Model):
    """
    A queued task and its result record.

    Rows double as the durable queue and the result store. Status values
    mirror django.tasks.TaskResultStatus exactly so conversion is a plain
    value cast.
    """

    class Status(models.TextChoices):
        READY = "READY"
        RUNNING = "RUNNING"
        FAILED = "FAILED"
        SUCCESSFUL = "SUCCESSFUL"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    task_path = models.CharField(max_length=255)
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)
    queue_name = models.CharField(max_length=128, default="default")
    priority = models.SmallIntegerField(default=0)
    takes_context = models.BooleanField(default=False)
    backend_name = models.CharField(max_length=128)

    status = models.CharField(
        max_length=10, choices=Status.choices, default=Status.READY
    )
    run_after = models.DateTimeField(null=True, blank=True)

    attempts = models.PositiveSmallIntegerField(default=0)
    max_attempts = models.PositiveSmallIntegerField(default=3)

    return_value = models.JSONField(null=True, blank=True)
    # Each entry: {"exception_class_path": str, "traceback": str}.
    errors = models.JSONField(default=list, blank=True)
    worker_ids = models.JSONField(default=list, blank=True)

    enqueued_at = models.DateTimeField()
    started_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    locked_by = models.CharField(max_length=64, null=True, blank=True)
    locked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["status", "queue_name", "-priority", "run_after"],
                name="ox_dequeue_idx",
            ),
            models.Index(fields=["status", "locked_at"], name="ox_reaper_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.task_path} [{self.status}]"


class OxScheduleTick(models.Model):
    """
    One dispatched tick of a named recurring schedule.

    The unique constraint on (schedule_name, scheduled_for) is the
    multi-worker coordination point: every worker derives the same tick
    times from the cron expression, the first INSERT wins, and the losers'
    transactions (tick row and task row together) roll back. A row with
    task=None is an anchor written when a schedule is first seen; it marks
    the tick before the schedule's first fire and enqueued nothing.
    """

    schedule_name = models.CharField(max_length=128)
    scheduled_for = models.DateTimeField()
    task = models.ForeignKey(
        OxTask,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="schedule_ticks",
    )
    created_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["schedule_name", "scheduled_for"], name="ox_tick_uniq"
            ),
        ]

    def __str__(self) -> str:
        return f"{self.schedule_name} @ {self.scheduled_for:%Y-%m-%d %H:%M}"
