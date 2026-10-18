import json
import uuid
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import router

from django_ox import actions
from django_ox.models import OxTask
from django_ox.tasks import MAX_ATTEMPTS_LIMIT


class Command(BaseCommand):
    help = "Retry one or more failed or lost tasks."

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "task_ids",
            nargs="+",
            metavar="TASK_ID",
            help="One or more task IDs (UUIDs) to retry.",
        )
        parser.add_argument(
            "--queue",
            default=None,
            help="Restrict retry to tasks on this queue.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many tasks are eligible without retrying any.",
        )
        parser.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help="Output format (default: %(default)s).",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        raw_ids: list[str] = options["task_ids"]
        queue: str | None = options["queue"]
        dry_run: bool = options["dry_run"]
        as_json: bool = options["format"] == "json"

        # Parse IDs and count duplicates once
        parsed_ids: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        for item in raw_ids:
            try:
                parsed = uuid.UUID(str(item))
            except (ValueError, TypeError, AttributeError):
                raise CommandError(
                    f"Invalid task ID {item!r}; must be a valid UUID.",
                    returncode=2,
                ) from None
            if parsed not in seen:
                seen.add(parsed)
                parsed_ids.append(parsed)

        alias = router.db_for_write(OxTask)

        # Exclude queue mismatches before delegation
        if queue is not None:
            matching_ids = set(
                OxTask.objects.using(alias)
                .filter(pk__in=parsed_ids, queue_name=queue)
                .values_list("pk", flat=True)
            )
            eligible_pool = [pk for pk in parsed_ids if pk in matching_ids]
            queue_excluded = len(parsed_ids) - len(eligible_pool)
        else:
            eligible_pool = parsed_ids
            queue_excluded = 0

        if dry_run:
            # Preview applies status eligibility and retry attempt ceiling
            eligible_count = (
                OxTask.objects.using(alias)
                .filter(
                    pk__in=eligible_pool,
                    status__in=actions.RETRYABLE_STATUSES,
                    attempts__lt=MAX_ATTEMPTS_LIMIT,
                )
                .count()
            )
            skipped_count = len(parsed_ids) - eligible_count
            if as_json:
                self.stdout.write(
                    json.dumps(
                        {
                            "eligible": eligible_count,
                            "skipped": skipped_count,
                            "dry_run": True,
                        }
                    )
                )
            else:
                self.stdout.write(
                    f"Would retry {eligible_count} task(s); {skipped_count} skipped."
                )
            return

        changed, act_skipped = actions.retry_many(eligible_pool)
        skipped = queue_excluded + act_skipped

        if as_json:
            self.stdout.write(
                json.dumps(
                    {
                        "changed": changed,
                        "skipped": skipped,
                    }
                )
            )
        else:
            self.stdout.write(f"Retried {changed} task(s); {skipped} skipped.")
