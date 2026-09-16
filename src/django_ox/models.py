import uuid
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from django.db import models, router


class OxTask(models.Model):
    """
    A queued task and its result record.

    Rows double as the durable queue and the result store. Four of the seven
    status values mirror django.tasks.TaskResultStatus, so conversion is a
    plain value cast; LOST, DISCARDED and WAITING are django-ox's own and are
    translated at the boundary by django_ox.results.
    """

    class Status(models.TextChoices):
        READY = "READY"
        RUNNING = "RUNNING"
        FAILED = "FAILED"
        SUCCESSFUL = "SUCCESSFUL"
        # Written only by the reaper, only when a claim aged past
        # LOCK_TIMEOUT with no attempts left. It means the worker holding
        # the task stopped reporting and its outcome was never observed.
        # It is not a verdict on the work: the reaper has no evidence
        # either way, so it must not write FAILED. Settled rather than
        # pending, so completion counting terminates; see results.py for
        # what callers of django.tasks see.
        LOST = "LOST"
        # Written by django_ox.actions.discard on a READY, WAITING, FAILED or
        # LOST row, and by django_ox._waiting, which django-ox itself never
        # calls, when a package built on django-ox cancels a task that has
        # not run. Settled: never claimed and never written by a worker. The
        # row does not run or retry unless django_ox._waiting revives it to
        # WAITING. Its previous attempts keep their records.
        DISCARDED = "DISCARDED"
        # Written only by django_ox._waiting, which django-ox itself never
        # calls. Never claimed, reaped or pruned. Appended after DISCARDED so
        # the existing members keep their order.
        WAITING = "WAITING"

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

    # When this lease stops being valid, written by the worker that took it.
    #
    # Stored on the row so every reaper in a fleet judges a lease by the
    # same deadline, whatever LOCK_TIMEOUT each one was started with; the
    # setting can change in a rolling deploy without two workers disagreeing
    # about which rows are abandoned. NULL means a lease
    # taken before this column existed, and the reaper falls back to comparing
    # locked_at against its own timeout for those; the first renewal after an
    # upgrade fills it in, so a fleet converges lease by lease with no step an
    # operator has to run.
    #
    # It does not give the two ends one clock. _lease_now() already decides
    # which clock stamps the lease, and this is written by the same one; the
    # production page says where that is shared and where it is not.
    lease_expires_at = models.DateTimeField(null=True, blank=True)

    # Fencing token. Every claim, and every reaper requeue, increments it in
    # the same UPDATE that hands the row over, so it identifies one execution
    # rather than merely one task. A worker carries the value it was given
    # and puts it in the WHERE clause of its own finish write, which is why a
    # worker that was reaped off this row cannot write over whoever holds it
    # now: the UPDATE matches zero rows instead of a stale one. Monotonic per
    # row; never reset, never reused.
    lease_epoch = models.BigIntegerField(default=0)

    class Meta:
        indexes = [
            # Two shapes because the claim has two shapes, and one btree
            # cannot serve both. The query is an equality on status, an
            # optional restriction on queue_name, a range on run_after, and
            # `ORDER BY priority DESC, enqueued_at`.
            #
            # `ox_dequeue_idx` ends on the sort columns with nothing variable
            # in front of them, so it delivers rows in claim order for a
            # worker that names several queues or names none at all, which is
            # the default. `ox_dequeue_queue_idx` puts queue_name first and is
            # tighter for a worker pinned to exactly one queue, the shape the
            # production page recommends: it walks only that queue's rows
            # instead of filtering the others out. PostgreSQL picks between
            # them per query.
            #
            # `run_after` is in neither: a range condition behind the sort
            # columns is unusable by a btree, and an index that ends there
            # loses the ordering.
            models.Index(
                fields=["status", "-priority", "enqueued_at"],
                name="ox_dequeue_idx",
            ),
            models.Index(
                fields=["status", "queue_name", "-priority", "enqueued_at"],
                name="ox_dequeue_queue_idx",
            ),
            models.Index(fields=["status", "locked_at"], name="ox_reaper_idx"),
            # The reaper's selection, once a row carries its own expiry.
            models.Index(
                fields=["status", "lease_expires_at"], name="ox_reaper_expiry_idx"
            ),
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


#: The alias the validation running in this thread reads, while one runs.
_validating_on: ContextVar[str | None] = ContextVar(
    "django_ox_validating_on", default=None
)


@contextmanager
def validate_against(alias: str) -> Iterator[None]:
    """
    Validate a schedule against `alias` for the length of this block.

    An operation resolves its alias once and writes every statement to it.
    Validation is the one part it cannot hand the alias to directly:
    `full_clean()` takes no alias, and the uniqueness query Django builds
    underneath it names none, so it follows `db_for_read` to wherever the
    router sends reads. This is how the operation's own alias reaches it.

    A context variable rather than an argument, because the frame in
    between is Django's. It is per thread and per task, so a worker
    validating on one connection cannot move another's.
    """
    token = _validating_on.set(alias)
    try:
        yield
    finally:
        _validating_on.reset(token)


class OxScheduleManager(models.Manager["OxSchedule"]):
    """
    The default manager, which answers a validation's alias while one runs.

    Django's uniqueness check builds its query from
    `_default_manager.filter(...)`, which names no alias. This is the seam
    that lets `validate_against` reach it without reimplementing the check
    itself, and it changes nothing outside a validation: with no alias set
    the queryset is the ordinary one, and an explicit `.using()` or
    `db_manager()` still wins.
    """

    def get_queryset(self) -> models.QuerySet["OxSchedule"]:
        queryset = super().get_queryset()
        alias = _validating_on.get()
        if alias is None or self._db is not None:
            return queryset
        return queryset.using(alias)


class OxSchedule(models.Model):
    """
    A recurring schedule stored as a row, so it can be changed without a
    deploy.

    Settings-declared schedules stay the default and are unaffected by this
    table. What a row adds is the ability to create, retime, pause and
    delete a schedule at runtime, and what it costs is that the row is
    input from a person rather than from a deployment. Two columns carry
    the whole of that cost.

    **task_key names a registry key, never an import path.** Code decides
    what the key resolves to (see django_ox.registry), so holding the
    change permission on this table does not become permission to run any
    importable callable with arguments of your choosing.

    **start_time is the activation boundary**: a tick fires only if it is
    at or after it. It is written when the row is created, by the creator,
    in the creating transaction. That is the difference from the anchor row
    a settings-declared schedule gets, which is written when a worker first
    *observes* the schedule: a row created at 12:00 and first due at 12:05
    would lose that run if every worker were down until 12:06. Both
    mechanisms answer the same question, and they differ because a row has
    a creation event to hang the answer on and a settings entry does not.

    **Retiming moves the boundary.** Change a cron from 02:00 to 03:00 at
    15:00 and the tick function starts answering 03:00 today, an instant
    that already passed and that nothing scheduled while it was in the
    future. update_schedule moves start_time when the timing columns
    change, so it does not fire.

    A write that goes around update_schedule leaves the boundary where it
    was, and `boundary_for` is what catches that: it records the timing the
    boundary was set for, and dispatch compares it against the row's timing
    now. When they differ the boundary is stale, this tick belonged to the
    old definition and is not fired, and the boundary is moved forward so
    the schedule resumes on its new timing.
    """

    class Trigger(models.TextChoices):
        CRON = "cron", "Cron expression"
        INTERVAL = "interval", "Fixed interval"

    name = models.CharField(max_length=128, unique=True)
    task_key = models.CharField(max_length=128)
    trigger = models.CharField(max_length=16, choices=Trigger.choices)
    cron = models.CharField(max_length=128, blank=True, default="")
    every_seconds = models.PositiveIntegerField(null=True, blank=True)
    phase_seconds = models.PositiveIntegerField(default=0)
    arguments = models.JSONField(default=dict, blank=True)
    enabled = models.BooleanField(default=True)
    start_time = models.DateTimeField()
    #: A digest of the timing columns and `enabled` as they were when
    #: start_time was last written. Dispatch and the periodic read recompute
    #: it from the row and compare, so a retime or a pause made by a route
    #: that runs no model code, a bulk update or a fixture, is still noticed
    #: at the next read. django_ox.stored says what that can and cannot see.
    #: Written there, never by hand.
    boundary_for = models.CharField(max_length=64, blank=True, default="")
    #: How many times this package has written the activation boundary.
    #: A worker that found the boundary stale heals it on a later pass and
    #: records this count with the sighting, so a second worker holding
    #: the same sighting can tell that the first one's heal superseded it.
    #: The columns alone cannot say so: a heal writes `start_time=now`,
    #: and `now` need not differ from the start time the sighting saw.
    #: Written by django_ox.stored, never by hand.
    boundary_generation = models.PositiveIntegerField(default=0)
    end_time = models.DateTimeField(null=True, blank=True)
    starting_deadline_seconds = models.PositiveIntegerField(null=True, blank=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    objects = OxScheduleManager()

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=models.Q(trigger="cron", every_seconds__isnull=True)
                | models.Q(trigger="interval", cron="", every_seconds__isnull=False),
                name="ox_schedule_one_trigger",
            ),
        ]

    def __str__(self) -> str:
        return self.name

    def clean(self) -> None:
        """
        Validate through the same rules the service functions use.

        The admin calls this for free, through ModelForm._post_clean.
        save() does not, which is why it is not the only caller.
        """
        from .stored import validate_schedule

        validate_schedule(self)

    def validate_unique(self, exclude: Collection[str] | None = None) -> None:
        """
        Check the name against the database this row is written to.

        Django's uniqueness query names no alias, so under a router that
        sends reads to a replica it asks the replica whether the name is
        taken. A replica that is behind does not hold the name yet, the
        check passes, and the INSERT on the primary raises IntegrityError:
        through the admin that is a server error where the person should
        have been told the name is already in use.

        The unique index stays and the error path stays. Validation cannot
        win a race against an insert that commits between the check and
        the write, wherever it reads; what asking the right database buys
        is that the ordinary duplicate is a field error again.
        """
        alias = _validating_on.get() or router.db_for_write(type(self))
        with validate_against(alias):
            super().validate_unique(exclude=exclude)


class OxScheduleChange(models.Model):
    """
    One row, bumped whenever a stored schedule changes.

    A worker reading schedules from the database needs to know whether to
    re-read them, and asking that question has to be cheaper than the
    answer: this is one indexed read of one row per dispatch pass, against
    a table that holds a single row forever.
    """

    id = models.PositiveSmallIntegerField(primary_key=True, default=1)
    changed_at = models.DateTimeField()

    def __str__(self) -> str:
        return f"schedules changed at {self.changed_at:%Y-%m-%d %H:%M:%S}"
