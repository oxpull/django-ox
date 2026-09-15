from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DatabaseError, connections, router, transaction
from django.db.models import F, QuerySet
from django.utils import timezone

from django_ox import _contention
from django_ox.durations import parse_duration
from django_ox.models import OxScheduleTick, OxTask


class Command(BaseCommand):
    help = "Delete finished task rows older than a cutoff."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--queue",
            default=None,
            help="Restrict pruning to one queue's task rows (default: all queues).",
        )
        parser.add_argument(
            "--older-than",
            default="7d",
            help=(
                "How long a task must have been finished before it is "
                "deleted. Forms: 7d, 24h, 90m, 45s, or a plain number of "
                "seconds (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--include-failed",
            action="store_true",
            help=(
                "Also delete FAILED and LOST rows. Off by default: they "
                "hold the per-attempt tracebacks and only go when asked."
            ),
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help=(
                "Rows deleted per DELETE statement, keeping locks and IN "
                "clauses bounded on large tables (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many rows would be deleted without deleting any.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be a positive integer.")
        cutoff = timezone.now() - parse_duration(options["older_than"])
        # DISCARDED prunes with SUCCESSFUL: the row is already closed, so
        # there is nothing left on it to wait for. WAITING is in neither
        # list, because that task has not run.
        statuses = [OxTask.Status.SUCCESSFUL, OxTask.Status.DISCARDED]
        if options["include_failed"]:
            # LOST goes with FAILED rather than with SUCCESSFUL: it is the
            # same kind of row to keep, holding tracebacks and the note that
            # the lease was lost, and the same kind to discard. Leaving it
            # out of both would make it the one status that never prunes.
            statuses += [OxTask.Status.FAILED, OxTask.Status.LOST]
        prunable = OxTask.objects.filter(status__in=statuses, finished_at__lt=cutoff)
        label = "/".join(statuses)
        queue: str | None = options["queue"]
        if queue is not None:
            # Task rows only. The tick log below is pruned for every schedule
            # whatever --queue names: deleting a task clears its ticks' link
            # to it, and an anchor never had one, so a tick's queue cannot be
            # read reliably.
            prunable = prunable.filter(queue_name=queue)
            label = f"{label} (queue {queue})"

        # Old schedule ticks are dispatch-log bookkeeping, but each
        # schedule's latest tick is the anchor the dispatcher measures
        # missed ticks against. Keep that row whatever its age: deleting it
        # would make the schedule re-anchor and skip a pending tick.
        anchors = [
            OxScheduleTick.objects.filter(schedule_name=name).latest("scheduled_for").pk
            for name in OxScheduleTick.objects.values_list(
                "schedule_name", flat=True
            ).distinct()
        ]
        prunable_ticks = OxScheduleTick.objects.filter(
            scheduled_for__lt=cutoff
        ).exclude(pk__in=anchors)

        if options["dry_run"]:
            self.stdout.write(
                f"Would delete {prunable.count()} {label} task row(s) "
                f"finished before {cutoff.isoformat()}."
            )
            self.stdout.write(
                f"Would delete {prunable_ticks.count()} schedule tick row(s) "
                f"scheduled before {cutoff.isoformat()}."
            )
            return

        deleted = self._delete_tasks_in_batches(prunable, options["batch_size"], label)
        self.stdout.write(
            f"Deleted {deleted} {label} task row(s) "
            f"finished before {cutoff.isoformat()}."
        )
        deleted_ticks = self._delete_in_batches(prunable_ticks, options["batch_size"])
        self.stdout.write(
            f"Deleted {deleted_ticks} schedule tick row(s) "
            f"scheduled before {cutoff.isoformat()}."
        )

    def _delete_tasks_in_batches(
        self, prunable: QuerySet[OxTask], batch_size: int, label: str
    ) -> int:
        alias = router.db_for_write(OxTask)
        # Each batch commits by itself. One that loses a deadlock or a
        # serialization failure runs again in a new transaction, which checks
        # the batch again under the lock and deletes again. Not inside a
        # caller's transaction, where the error has rolled back more than this
        # batch, all of it on MySQL. There the error goes to the caller.
        tries = _contention.attempts(alias)
        deleted = 0
        while True:
            candidates = list(prunable.values_list("pk", flat=True)[:batch_size])
            if not candidates:
                break
            attempt = 1
            while True:
                try:
                    count = self._delete_batch(prunable, candidates, alias)
                except DatabaseError as exc:
                    if tries == 1 or not _contention.is_contention(exc):
                        raise
                    if attempt >= tries:
                        # The batches before this one have committed. The
                        # candidates are the first rows still prunable, so a
                        # rerun starts again from this batch.
                        raise CommandError(
                            f"Stopped after deleting {deleted} {label} task "
                            "row(s). The next batch hit a database deadlock or "
                            f"serialization failure {tries} times. The rows "
                            "already deleted stay deleted. Run ox_prune again "
                            "to prune the rest."
                        ) from exc
                    _contention.pause(attempt)
                    attempt += 1
                    continue
                # Counted once the batch has committed, so a batch that ran
                # twice counts once.
                deleted += count
                break
        return deleted

    def _delete_batch(
        self, prunable: QuerySet[OxTask], candidates: list[Any], alias: str
    ) -> int:
        """Delete what is still prunable of one batch, in one transaction."""
        batch = candidates
        # A row can leave the selection after that read: an operator
        # retries it, or discards it and its finished_at becomes now, or
        # django_ox._waiting revives a DISCARDED row to WAITING.
        # Django's delete reads the rows again but then deletes by
        # primary key alone, so a row that changes between that read and
        # the DELETE is deleted anyway. The batch is therefore checked
        # again under a lock, in the transaction that deletes it, and
        # only the rows still prunable under that lock go.
        #
        # SQLite has no row locks, and Django drops FOR UPDATE there
        # without raising. What serialises SQLite is being the writer,
        # and a transaction that reads first starts as a reader. The
        # no-op UPDATE makes this one the writer before it reads.
        with transaction.atomic(using=alias):
            selected = prunable.filter(pk__in=batch)
            if connections[alias].features.has_select_for_update:
                # In primary-key order. Without an ORDER BY the read locks
                # rows in the order its plan reads them, which on
                # PostgreSQL can be table order. A writer that locks the
                # same rows in key order could then hold a row this read
                # waits for while this read holds one that writer needs,
                # and the database would abort one of the two.
                batch = list(
                    selected.select_for_update()
                    .order_by("pk")
                    .values_list("pk", flat=True)
                )
            else:
                selected.update(finished_at=F("finished_at"))
            # Returned from inside the block, and so reaches the caller only
            # once the block has committed.
            return prunable.filter(pk__in=batch).delete()[0]

    def _delete_in_batches(self, prunable: QuerySet[Any], batch_size: int) -> int:
        deleted = 0
        while True:
            batch = list(prunable.values_list("pk", flat=True)[:batch_size])
            if not batch:
                break
            deleted += prunable.filter(pk__in=batch).delete()[0]
        return deleted
