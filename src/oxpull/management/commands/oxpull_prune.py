# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import connections
from django.db.models import Q, QuerySet
from django.utils import timezone

from django_ox.durations import parse_duration
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import (
    OxpullUniqueLock,
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
)
from oxpull.routing import oxpull_db_alias


class Command(BaseCommand):
    help = (
        "Delete unique-task locks whose task is finished or pruned, and the "
        "node and edge rows of workflows that completed longer ago than "
        "--older-than and that no retry can move again. Batch members are "
        "kept. Stale locks are also released lazily by the next enqueue of the "
        "same key. This command clears locks for keys that are never enqueued "
        "again."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help=(
                "Rows deleted per DELETE statement, bounding statement "
                "size on large tables (default: %(default)s). A statement "
                "deletes at most 1,000 workflow rows."
            ),
        )
        parser.add_argument(
            "--older-than",
            default="7d",
            help=(
                "How long a workflow must have been COMPLETE before its node "
                "and edge rows are considered. It takes 7d, 24h, 90m, 45s or a "
                "plain number of seconds (default: %(default)s). Locks are not "
                "aged. A lock is either held by a live task or it is stale."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help=(
                "Report how many rows would be deleted without deleting or "
                "stamping anything."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be a positive integer.")
        live = OxTask.objects.filter(
            status__in=[OxTask.Status.READY, OxTask.Status.RUNNING]
        ).values("id")
        # The isnull arm is not redundant: SQL's NOT IN treats a NULL
        # task_id as unknown, so without it a NULL row would be kept
        # forever. A committed lock never carries a NULL task_id (enqueue
        # writes it in the same transaction), so the arm only clears
        # defensive residue.
        prunable = OxpullUniqueLock.objects.filter(
            Q(task_id__isnull=True) | ~Q(task_id__in=live)
        )
        older_than = options["older_than"]
        try:
            cutoff = timezone.now() - parse_duration(older_than)
        except OverflowError:
            # A duration timedelta can hold may still reach back past year 1.
            raise CommandError(
                f"Invalid duration {older_than!r}; it is out of range."
            ) from None
        db_alias = oxpull_db_alias()

        if options["dry_run"]:
            self.stdout.write(f"Would delete {prunable.count()} stale unique lock(s).")
        else:
            deleted = self._delete_in_batches(prunable, options["batch_size"])
            self.stdout.write(f"Deleted {deleted} stale unique lock(s).")

        # Batch members are never deleted. A batch's progress() reads them
        # for as long as the batch row exists, and a pruned member counts as
        # unattributed, as it did in 1.2.0.
        if not self._workflow_tables_exist(db_alias):
            self.stdout.write(
                "Skipped workflows, because their tables do not exist on this "
                "database yet. Run manage.py migrate oxpull."
            )
            return
        retirable = workflows.retirable_workflows(cutoff, db_alias)
        if options["dry_run"]:
            self._dry_run(retirable, db_alias)
            return
        nodes = edges = pending = 0
        for workflow_pk in retirable:
            retired = workflows.retire_workflow(
                workflow_pk,
                db_alias,
                chunk_size=min(options["batch_size"], workflows.ID_CHUNK_SIZE),
            )
            nodes += retired.nodes
            edges += retired.edges
            pending += retired.pending
        self.stdout.write(f"Deleted {nodes} workflow node row(s).")
        self.stdout.write(f"Deleted {edges} workflow edge row(s).")
        if pending:
            self.stdout.write(
                f"Skipped {pending} workflow(s) with a node still pending."
            )
        scans = workflows.clean_scan_rows(
            db_alias,
            chunk_size=min(options["batch_size"], workflows.ID_CHUNK_SIZE),
        )
        self.stdout.write(f"Deleted {scans} workflow scan row(s).")
        self._report_retained()

    def _workflow_tables_exist(self, db_alias: str) -> bool:
        with connections[db_alias].cursor() as cursor:
            tables = connections[db_alias].introspection.table_names(cursor)
        return {
            OxpullWorkflow._meta.db_table,
            OxpullWorkflowNode._meta.db_table,
            OxpullWorkflowEdge._meta.db_table,
        }.issubset(tables)

    def _dry_run(self, retirable: list[Any], db_alias: str) -> None:
        nodes = edges = 0
        for workflow_pk in retirable:
            would = workflows.retirable_rows(workflow_pk, db_alias)
            nodes += would.nodes
            edges += would.edges
        self.stdout.write(f"Would delete {nodes} workflow node row(s).")
        self.stdout.write(f"Would delete {edges} workflow edge row(s).")
        self._report_retained()

    def _report_retained(self) -> None:
        kept = workflows.retained_rows()
        self.stdout.write(
            f"Kept {kept.nodes} workflow node row(s) and {kept.edges} workflow "
            f"edge row(s) of {kept.workflows} retired workflow(s)."
        )

    def _delete_in_batches(self, prunable: QuerySet[Any], batch_size: int) -> int:
        # Neither the key nor a task's staleness is monotone. Between
        # the SELECT and the DELETE a concurrent enqueue can release a
        # stale lock and insert a new live generation under the same pk,
        # and django_ox.actions.retry can move a FAILED or LOST row back
        # to READY, making a lock the SELECT read as stale live again.
        # The DELETE therefore goes through prunable, which re-applies
        # the staleness predicate inside the statement, so each batch is
        # re-verified against current rows: a stolen key survives, and so
        # does the lock on a task that was retried.
        # Bounded batches are the same idiom as ox_prune's deletes,
        # deliberately duplicated.
        deleted = 0
        while True:
            batch = list(prunable.values_list("pk", flat=True)[:batch_size])
            if not batch:
                break
            deleted += prunable.filter(pk__in=batch).delete()[0]
        return deleted
