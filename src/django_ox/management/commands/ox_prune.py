import re
from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db.models import QuerySet
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask

DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: str) -> timedelta:
    """Parse '7d' / '24h' / '90m' / '45s' or a plain number of seconds."""
    match = re.fullmatch(r"(\d+)([smhd]?)", value.strip())
    if match is None:
        raise CommandError(
            f"Invalid duration {value!r}; use forms like 7d, 24h, 90m, 45s, "
            "or a plain number of seconds."
        )
    number, unit = match.groups()
    return timedelta(seconds=int(number) * DURATION_UNITS[unit or "s"])


class Command(BaseCommand):
    help = "Delete finished task rows older than a cutoff."

    def add_arguments(self, parser: CommandParser) -> None:
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
                "Also delete FAILED rows. Off by default: failed rows hold "
                "the per-attempt tracebacks and only go when asked."
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
        statuses = [OxTask.Status.SUCCESSFUL]
        if options["include_failed"]:
            statuses.append(OxTask.Status.FAILED)
        prunable = OxTask.objects.filter(status__in=statuses, finished_at__lt=cutoff)
        label = "/".join(statuses)

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

        deleted = self._delete_in_batches(prunable, options["batch_size"])
        self.stdout.write(
            f"Deleted {deleted} {label} task row(s) "
            f"finished before {cutoff.isoformat()}."
        )
        deleted_ticks = self._delete_in_batches(prunable_ticks, options["batch_size"])
        self.stdout.write(
            f"Deleted {deleted_ticks} schedule tick row(s) "
            f"scheduled before {cutoff.isoformat()}."
        )

    def _delete_in_batches(self, prunable: QuerySet[Any], batch_size: int) -> int:
        model = prunable.model
        deleted = 0
        while True:
            batch = list(prunable.values_list("pk", flat=True)[:batch_size])
            if not batch:
                break
            # Rows selected here are immutable for pruning purposes
            # (terminal task rows never change status; tick rows never
            # move), so deleting by pk alone cannot remove a row that
            # became live since the SELECT.
            deleted += model._default_manager.filter(pk__in=batch).delete()[0]
        return deleted
