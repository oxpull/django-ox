import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from django.core.management.base import CommandError, CommandParser
from django.db import DatabaseError, connections, transaction
from django.db.models import F, QuerySet
from django.utils import timezone

from django_ox import _contention
from django_ox.durations import parse_duration
from django_ox.management._database import DatabaseCommand
from django_ox.models import OxScheduleTick, OxTask

# SQLite and MySQL overflow when binding cutoffs on the first day of year
# one after converting into a connection zone west of UTC. Reject that
# whole first day on every engine so the command fails as an argument
# error instead of a driver traceback.
_CUTOFF_FLOOR = datetime.min + timedelta(days=1)


class Command(DatabaseCommand):
    help = "Delete finished task rows older than a cutoff."

    _task_rows = 0
    _tick_rows = 0

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
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
        parser.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help=(
                "Output format. json prints one object with the same figures "
                "on stdout; the exit status is the same either way "
                "(default: %(default)s)."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["batch_size"] < 1:
            raise CommandError("--batch-size must be a positive integer.")
        # Once, before the first statement. Every queryset below is built on
        # this alias, so the rows the command reads are the rows it deletes.
        alias = self.database(options)
        older_than = options["older_than"]
        try:
            cutoff = timezone.now() - parse_duration(older_than)
        except OverflowError:
            # A duration timedelta can hold may still reach back past year 1.
            raise CommandError(
                f"Invalid duration {older_than!r}; it is out of range."
            ) from None
        floor = _CUTOFF_FLOOR
        if timezone.is_aware(cutoff):
            floor = timezone.make_aware(_CUTOFF_FLOOR, UTC)
        if cutoff < floor:
            raise CommandError(
                f"Invalid duration {older_than!r}; it is out of range."
            )
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
        prunable = OxTask.objects.using(alias).filter(
            status__in=statuses, finished_at__lt=cutoff
        )
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
        ticks = OxScheduleTick.objects.using(alias)
        anchors = [
            ticks.filter(schedule_name=name).latest("scheduled_for").pk
            for name in ticks.values_list("schedule_name", flat=True).distinct()
        ]
        prunable_ticks = ticks.filter(scheduled_for__lt=cutoff).exclude(pk__in=anchors)

        as_json = options["format"] == "json"

        if options["dry_run"]:
            task_count = prunable.count()
            tick_count = prunable_ticks.count()
            if as_json:
                self._write_json(
                    queue=queue,
                    cutoff=cutoff,
                    statuses=statuses,
                    task_rows=task_count,
                    tick_rows=tick_count,
                    dry_run=True,
                )
            else:
                self.stdout.write(
                    f"Would delete {task_count} {label} task row(s) "
                    f"finished before {cutoff.isoformat()}."
                )
                self.stdout.write(
                    f"Would delete {tick_count} schedule tick row(s) "
                    f"scheduled before {cutoff.isoformat()}."
                )
            return

        self._task_rows = 0
        self._tick_rows = 0
        try:
            deleted = self._delete_tasks_in_batches(
                prunable, options["batch_size"], label, alias
            )
            # Written before the ticks are touched, the way it was before
            # --format text gained a second deletion behind the same try.
            # A tick failure must not cost the operator the task count.
            if not as_json:
                self.stdout.write(
                    f"Deleted {deleted} {label} task row(s) "
                    f"finished before {cutoff.isoformat()}."
                )
            deleted_ticks = self._delete_in_batches(
                prunable_ticks, options["batch_size"]
            )
        except (CommandError, DatabaseError):
            # DatabaseError too: _delete_in_batches has no retry wrapper, and
            # is_contention() only knows PostgreSQL and MySQL codes, so a
            # lock on SQLite or a failure in the tick phase arrives raw.
            # Either way the committed rows are gone and their count is the
            # one thing the caller cannot recover afterwards.
            if as_json:
                self._write_json(
                    queue=queue,
                    cutoff=cutoff,
                    statuses=statuses,
                    task_rows=self._task_rows,
                    tick_rows=self._tick_rows,
                    dry_run=False,
                )
            raise

        if as_json:
            self._write_json(
                queue=queue,
                cutoff=cutoff,
                statuses=statuses,
                task_rows=deleted,
                tick_rows=deleted_ticks,
                dry_run=False,
            )
            return

        self.stdout.write(
            f"Deleted {deleted_ticks} schedule tick row(s) "
            f"scheduled before {cutoff.isoformat()}."
        )

    def _write_json(
        self,
        *,
        queue: str | None,
        cutoff: datetime,
        statuses: Sequence[str],
        task_rows: int,
        tick_rows: int,
        dry_run: bool,
    ) -> None:
        self.stdout.write(
            json.dumps(
                {
                    "queue": queue,
                    "cutoff": cutoff.isoformat(),
                    "statuses": list(statuses),
                    "task_rows": task_rows,
                    "tick_rows": tick_rows,
                    "dry_run": dry_run,
                }
            )
        )

    def _delete_tasks_in_batches(
        self, prunable: QuerySet[OxTask], batch_size: int, label: str, alias: str
    ) -> int:
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
                self._task_rows = deleted
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
            self._tick_rows = deleted
        return deleted
