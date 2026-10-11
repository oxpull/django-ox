import uuid

from django.db import models

from .rate_limits import MAX_LIMIT_NAME_LENGTH


class OxpullUniqueLock(models.Model):
    """
    The uniqueness lock for one pending unique task.

    A row means: a task with this key was enqueued and has not been seen
    to finish. The row is written in the same transaction as the OxTask
    INSERT, so the lock and the task commit or roll back together. key is
    the primary key, so the dedup point is a single unique btree,
    maintained once per winner INSERT and once per release DELETE.

    Lock state is derived from the task row, never from signals: the
    django-ox reaper writes FAILED without emitting task_finished, so any
    release mechanism driven by signals would strand locks exactly when a
    worker dies. Instead, a lock whose task is no longer READY or RUNNING
    (or whose task row was pruned) is stale, and is released by the next
    enqueue of the same key or by the oxpull_prune command.

    task_id is a bare UUID, not a ForeignKey: ox_prune deletes finished
    OxTask rows, and a dangling reference must read as "finished" rather
    than block pruning or cascade the lock away while it is still the
    enqueue-time dedup point.
    """

    # SHA-256 hex digest over (backend_alias, task_path, queue_name,
    # args, kwargs).
    key = models.CharField(max_length=64, primary_key=True)
    task_id = models.UUIDField(null=True, blank=True)

    # Denormalised for operators reading the table; the key alone decides.
    # task_path and queue_name stay readable after the task row is pruned,
    # when task_id already dangles by design.
    task_path = models.CharField(max_length=255)
    queue_name = models.CharField(max_length=128, default="default")

    created_at = models.DateTimeField()

    def __str__(self) -> str:
        return f"{self.task_path} [{self.key[:12]}]"


class OxpullBatch(models.Model):
    """
    A named group of tasks and the callbacks that fire once every member's
    row is settled (SUCCESSFUL, FAILED, LOST or DISCARDED) or gone.

    Status moves OPEN -> SEALED -> COMPLETE and never back. Membership is
    frozen at seal, which is also when total_count is snapshotted; the
    reconciler completes a SEALED batch under a compare-and-set on that
    status, and the CAS rowcount is what gets the callbacks enqueued
    once. They then run like any other task, which is at least once.

    There is no FAILED status. Completion is completion: failed_count
    plus the absent on_success callback is how a bad run reads. Collapsing
    "done" and "how it went" into one enum is the door the signal-counting
    design comes back through.

    Batch state is never accumulated from signals. The django-ox reaper
    writes FAILED through a queryset update and emits nothing, so a batch
    that counted task_finished would hang exactly when a worker dies,
    which is the case batches exist to survive. Every count here is
    derived by querying member task rows.

    Callbacks are stored as a dotted task path plus JSON arguments, not
    as a pickled Task: the row outlives the process that created it, and
    the reconciler that enqueues it is a different worker.
    """

    class Status(models.TextChoices):
        OPEN = "OPEN"
        SEALED = "SEALED"
        COMPLETE = "COMPLETE"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128)
    status = models.CharField(
        max_length=10, choices=Status.choices, default=Status.OPEN
    )

    # Member count frozen at seal time. NULL while OPEN, where the live
    # member count is the only honest answer; after seal it is also the
    # denominator that keeps totals true once ox_prune deletes member
    # task rows.
    total_count = models.PositiveIntegerField(null=True, blank=True)

    on_complete_path = models.CharField(max_length=255, blank=True, default="")
    on_complete_args = models.JSONField(default=list, blank=True)
    on_complete_kwargs = models.JSONField(default=dict, blank=True)

    on_success_path = models.CharField(max_length=255, blank=True, default="")
    on_success_args = models.JSONField(default=list, blank=True)
    on_success_kwargs = models.JSONField(default=dict, blank=True)

    # Written once, in the completing transaction. NULL means "not
    # determined yet" rather than "no failures", which a default of 0
    # would claim about every open batch.
    failed_count = models.PositiveIntegerField(null=True, blank=True)

    # Bare UUIDs of the enqueued callbacks, for the same reason as
    # OxpullBatchMember.task_id: ox_prune deletes finished task rows.
    callback_task_id = models.UUIDField(null=True, blank=True)
    success_task_id = models.UUIDField(null=True, blank=True)

    created_at = models.DateTimeField()
    sealed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            # The reconciler's sweep: every SEALED batch, oldest first.
            models.Index(
                fields=["status", "created_at"], name="oxpull_batch_sweep_idx"
            ),
        ]

    def __str__(self) -> str:
        return f"{self.name} [{self.status}]"


class OxpullBatchMember(models.Model):
    """
    One task's membership of one batch.

    task_id is a bare UUID, not a ForeignKey: ox_prune deletes finished
    OxTask rows, and a foreign key would either block the prune or
    cascade the membership away. A membership row whose task row is gone
    joins to nothing, which is exactly the wanted reading: pruned means
    finished. total_count keeps the denominator honest when that happens.

    The (batch, task_id) constraint refuses to count one task twice in
    one batch, reachable only through a deduplicated unique enqueue, and
    doubles as the index the completion join reads.
    """

    batch = models.ForeignKey(
        OxpullBatch, on_delete=models.CASCADE, related_name="members"
    )
    task_id = models.UUIDField()
    created_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["batch", "task_id"], name="oxpull_batch_member_uniq"
            ),
        ]

    def __str__(self) -> str:
        return f"{self.batch_id} -> {self.task_id}"


class OxpullRateWindow(models.Model):
    """
    The current window of one named rate limit.

    One row per limit name, forever: the table counts admissions, not
    tasks, so it does not grow with traffic and needs no pruner. Every
    statement against it is keyed on limit_name, one name for a write
    and the worker's configured names with IN for the refresh's select,
    with an optional AND window_id = %s; the primary key answers all of
    them on its own. Identity is the column's collation: a name the
    collation folds onto an existing row is that row, which is why the
    worker asks the database which row a renamed limit owns.

    Capacity is not stored. A limit's N and P live in settings, and a
    column holding them would be a second source of truth with no
    answer to which one wins halfway through a rolling deploy.

    There is no foreign key to anything, matching OxpullUniqueLock and
    OxpullBatchMember: there is nothing to reference.
    """

    # The configured name, whose length rate_limits.py validates against
    # this same constant, so a name that reaches a row always fits it.
    limit_name = models.CharField(max_length=MAX_LIMIT_NAME_LENGTH, primary_key=True)

    # Monotone per row, never reset and never reused. It names one window,
    # and it is the compare-and-set target that makes exactly one worker
    # the one that rolls, the role lease_epoch plays on OxTask.
    window_id = models.BigIntegerField(default=0)

    # When this window opened, on the database clock, always. Nothing but
    # this table writes or reads this column, and it is only ever compared
    # against the database's own clock in the same statement, so there is
    # no other writer for it to agree with.
    window_started_at = models.DateTimeField()

    # Admissions counted in the current window. It may exceed the
    # configured capacity by up to one per additional worker process;
    # that overshoot is the guarantee's slack, and a value above the
    # limit here is expected rather than corrupt.
    used = models.PositiveIntegerField(default=0)

    def __str__(self) -> str:
        return f"{self.limit_name} [{self.used} in window {self.window_id}]"


class OxpullWorkflow(models.Model):
    """
    A named DAG of tasks, and the callbacks that fire once every node has
    finished.

    The status columns are OxpullBatch's, name for name: OPEN -> SEALED ->
    COMPLETE, and total_count snapshotted at seal. The reconciler completes
    a SEALED row under a compare-and-set, and that CAS rowcount is what gets
    the callbacks enqueued once. They then run like any other task, which is
    at least once.

    A node whose upstream has not succeeded is a django-ox row in the
    WAITING status, which no worker claims. It moves to READY only once
    every edge into it is marked satisfied. The claim still reads the edges,
    and refuses a READY row behind any edge that is not marked satisfied and
    whose upstream row is absent or not SUCCESSFUL.

    This row is the first lock every workflow operation takes.
    """

    class Status(models.TextChoices):
        OPEN = "OPEN"
        SEALED = "SEALED"
        COMPLETE = "COMPLETE"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128)
    status = models.CharField(
        max_length=10, choices=Status.choices, default=Status.OPEN
    )

    # Node count frozen at seal. NULL while OPEN, where the live node
    # count is the only honest answer; after seal it is the denominator
    # that keeps totals true once ox_prune deletes node task rows.
    total_count = models.PositiveIntegerField(null=True, blank=True)

    on_complete_path = models.CharField(max_length=255, blank=True, default="")
    on_complete_args = models.JSONField(default=list, blank=True)
    on_complete_kwargs = models.JSONField(default=dict, blank=True)

    on_success_path = models.CharField(max_length=255, blank=True, default="")
    on_success_args = models.JSONField(default=list, blank=True)
    on_success_kwargs = models.JSONField(default=dict, blank=True)

    failed_count = models.PositiveIntegerField(null=True, blank=True)
    # Nodes this workflow cancelled because an upstream of theirs settled
    # without succeeding. They are DISCARDED rows and therefore also
    # unattributed, but an operator reading the table wants to know the
    # workflow did it rather than a person.
    cancelled_count = models.PositiveIntegerField(null=True, blank=True)

    callback_task_id = models.UUIDField(null=True, blank=True)
    success_task_id = models.UUIDField(null=True, blank=True)

    created_at = models.DateTimeField()
    sealed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    # Reserved. Nothing in this release writes generation or
    # cancel_requested_at. generation is read as the key completions are
    # stored under, and it is 0 on every row.
    generation = models.PositiveIntegerField(default=0)
    cancel_requested_at = models.DateTimeField(null=True, blank=True)
    # When oxpull_prune began retiring this workflow's node and edge rows.
    # Once set, no node of it is revived.
    metadata_retired_at = models.DateTimeField(null=True, blank=True)
    # Reserved. Nothing in this release writes key_hash, so it is NULL on
    # every row. NULL rather than "" because the unique index has to let
    # any number of workflows carry no value.
    key_hash = models.CharField(max_length=64, null=True, blank=True, unique=True)
    # What completion recorded: counts, callback ids and when, under the
    # generation as key. Written by the compare-and-set that completes the
    # workflow, and never rewritten.
    completions = models.JSONField(default=dict, blank=True)
    # Nodes added, incremented by add() in the statement that takes this
    # row's lock, so it counts exactly the nodes that committed.
    node_count = models.PositiveIntegerField(default=0)

    class Meta:
        indexes = [
            models.Index(fields=["status", "created_at"], name="oxpull_wf_sweep_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.name} [{self.status}]"


class OxpullWorkflowNode(models.Model):
    """
    One task's membership of one workflow, under a name dependencies are
    declared with, and what the workflow recorded about how it finished.

    task_id is a bare UUID for the reason recorded on OxpullBatchMember:
    ox_prune deletes finished OxTask rows, and a foreign key would either
    block the prune or cascade the membership away. It is unique across
    workflows, because a task is one node: add() refuses unique tasks, so no
    enqueue can hand two workflows the same row.

    A name is identified by name_key, the SHA-256 digest of its UTF-8 bytes,
    so two names are one name only when they are spelled the same. The
    name column's collation plays no part, and MySQL's case, accent and
    trailing-space folding cannot refuse a name or resolve the wrong one.

    The foreign key to the workflow is PROTECT: a workflow row cannot be
    deleted while any of its nodes remain.
    """

    workflow = models.ForeignKey(
        OxpullWorkflow,
        on_delete=models.PROTECT,
        related_name="nodes",
        db_index=False,
    )
    name = models.CharField(max_length=128)
    name_key = models.CharField(max_length=64)
    task_id = models.UUIDField()
    created_at = models.DateTimeField()

    # The settled status the workflow recorded for this node's task row, and
    # the lease epoch it was recorded at. NULL until something records one,
    # which lets a record decide for a row that has since been pruned.
    outcome = models.CharField(max_length=10, null=True, blank=True)
    outcome_epoch = models.BigIntegerField(null=True, blank=True)
    # True once the record says SUCCESSFUL. Its own column so the open-row
    # index can lead with it.
    succeeded = models.BooleanField(default=False)

    # Reserved. Nothing in this release writes these three, so they are
    # NULL on every row. A node the workflow discarded is found by its
    # DISCARDED task row and its recorded outcome.
    cancel_cause = models.CharField(max_length=16, null=True, blank=True)
    cancelled_by_task_id = models.UUIDField(null=True, blank=True)
    cancelled_generation = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["workflow", "name_key"], name="oxpull_wf_node_name_uniq"
            ),
            models.UniqueConstraint(
                fields=["task_id"], name="oxpull_wf_node_task_uniq"
            ),
        ]
        indexes = [
            # The nodes of one workflow not yet recorded as succeeded, in key
            # order: a page of them without reading the ones that are done.
            models.Index(
                fields=["workflow", "succeeded", "id"], name="oxpull_wf_node_open_idx"
            ),
        ]

    def __str__(self) -> str:
        return f"{self.workflow_id}:{self.name} -> {self.task_id}"


class OxpullWorkflowEdge(models.Model):
    """
    One dependency: downstream_task_id may not run until upstream_task_id
    has succeeded.

    satisfied is the workflow's record that it saw the upstream succeed.
    SUCCESSFUL is final in django-ox, so a satisfied edge stays satisfied
    after the upstream row is pruned. An edge not marked satisfied blocks
    unless its upstream row is there and SUCCESSFUL. A missing upstream row
    with no record is not taken as success.

    Both sides are bare task UUIDs rather than node references, because
    this table is read inside the worker's claim statement and a join
    through the node table would be a second index probe on the hottest
    query the package has.

    The index on (downstream_task_id, satisfied) is the one the claim uses:
    it asks "does anything unsatisfied point at this candidate", with no
    workflow in hand. A satisfied edge costs the claim one index entry and
    no read of the task table.
    """

    workflow = models.ForeignKey(
        OxpullWorkflow,
        on_delete=models.PROTECT,
        related_name="edges",
        db_index=False,
    )
    upstream_task_id = models.UUIDField()
    downstream_task_id = models.UUIDField()
    satisfied = models.BooleanField(default=False)
    created_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["workflow", "upstream_task_id", "downstream_task_id"],
                name="oxpull_wf_edge_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=["downstream_task_id", "satisfied"],
                name="oxpull_wf_edge_down_idx",
            ),
            # The reverse question: "what did this task unblock". The unique
            # constraint above cannot answer it: its leading column is the
            # workflow, and the outcome write has a task id and no workflow
            # in hand, exactly as the claim does not.
            models.Index(fields=["upstream_task_id"], name="oxpull_wf_edge_up_idx"),
            # The edges of one workflow not yet satisfied, in key order.
            models.Index(
                fields=["workflow", "satisfied", "id"], name="oxpull_wf_edge_open_idx"
            ),
        ]

    def __str__(self) -> str:
        return f"{self.upstream_task_id} -> {self.downstream_task_id}"


class OxpullWorkflowScan(models.Model):
    """
    Where the reconciler's backstop is in one workflow that is not COMPLETE.

    The backstop pages through a workflow's open edges and nodes in key
    order, a page per visit. This row holds its place between visits, so a
    pass survives a crash or a timed-out tick. The high-water marks are the
    largest edge and node ids when the pass began, and the pass stops there,
    so rows added during it wait for the next pass instead of extending it.

    workflow_id is a bare UUID, not a foreign key: this is bookkeeping, and
    nothing about it may stop a workflow row from being dealt with.
    """

    class Phase(models.TextChoices):
        EDGES = "EDGES"
        NODES = "NODES"
        RELEASE = "RELEASE"

    workflow_id = models.UUIDField(primary_key=True)
    phase = models.CharField(max_length=10, choices=Phase.choices, default=Phase.EDGES)
    # The last key visited in this phase.
    position = models.BigIntegerField(default=0)
    edge_hwm = models.BigIntegerField(null=True, blank=True)
    node_hwm = models.BigIntegerField(null=True, blank=True)
    # Keys a visit could not write because another transaction held them,
    # tried again on a later visit.
    skipped = models.JSONField(default=list, blank=True)
    # The compare-and-set target that moves the position.
    version = models.BigIntegerField(default=0)

    def __str__(self) -> str:
        return f"{self.workflow_id} {self.phase}@{self.position}"


class OxpullWorkflowRound(models.Model):
    """
    The backstop's round-robin position across workflows: one row.

    Workflows are visited in id order, wrapping, and this row names the last
    one visited. version is the compare-and-set target, so two reconcilers
    cannot both move it.
    """

    id = models.PositiveSmallIntegerField(primary_key=True, default=1)
    workflow_id = models.UUIDField(null=True, blank=True)
    version = models.BigIntegerField(default=0)

    def __str__(self) -> str:
        return f"round at {self.workflow_id} (version {self.version})"
