from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DatabaseError

from django_ox import stats


def _seconds(value: timedelta | None) -> str:
    return "none" if value is None else f"{value.total_seconds():.0f}s"


class Command(BaseCommand):
    help = (
        "Check queue health. Exits 0 when every enabled check passes, "
        "non-zero with a one-line reason otherwise. With no flags, only "
        "database reachability is checked."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--queue",
            default=None,
            help="Restrict the checks to one queue (default: all queues).",
        )
        parser.add_argument(
            "--max-backlog",
            type=int,
            default=None,
            help=(
                "Fail when more than this many READY tasks are eligible to "
                "run. Tasks deferred to a future run_after do not count "
                "(default: no backlog check)."
            ),
        )
        parser.add_argument(
            "--max-age",
            type=float,
            default=None,
            help=(
                "Fail when the oldest task waiting to run has waited longer "
                "than this many seconds since becoming eligible "
                "(default: no age check)."
            ),
        )
        parser.add_argument(
            "--worker-timeout",
            type=float,
            default=None,
            help=(
                "Fail when no worker has claimed a task within this many "
                "seconds. Claim activity is the only worker trace in the "
                "database, so this check suits queues with steady traffic; "
                "for bursty queues prefer --max-age (default: no worker "
                "check)."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        max_backlog: int | None = options["max_backlog"]
        max_age: float | None = options["max_age"]
        worker_timeout: float | None = options["worker_timeout"]
        if max_backlog is not None and max_backlog < 0:
            raise CommandError("--max-backlog must be zero or a positive integer.")
        if max_age is not None and max_age <= 0:
            raise CommandError("--max-age must be a positive number of seconds.")
        if worker_timeout is not None and worker_timeout <= 0:
            raise CommandError("--worker-timeout must be a positive number of seconds.")

        queue: str | None = options["queue"]
        try:
            backlog = stats.ready_count(queue)
            oldest = stats.oldest_ready_age(queue)
            claim_age = stats.last_claim_age(queue)
        except DatabaseError as exc:
            raise CommandError(f"Database unreachable: {exc}") from exc

        problems: list[str] = []
        if max_backlog is not None and backlog > max_backlog:
            problems.append(f"backlog is {backlog}, over --max-backlog {max_backlog}")
        if (
            max_age is not None
            and oldest is not None
            and oldest.total_seconds() > max_age
        ):
            problems.append(
                f"oldest waiting task is {_seconds(oldest)} old, "
                f"over --max-age {max_age:g}s"
            )
        if worker_timeout is not None:
            if claim_age is None:
                problems.append(
                    f"no task claim recorded (--worker-timeout {worker_timeout:g}s)"
                )
            elif claim_age.total_seconds() > worker_timeout:
                problems.append(
                    f"last task claim was {_seconds(claim_age)} ago, "
                    f"over --worker-timeout {worker_timeout:g}s"
                )
        if problems:
            raise CommandError("; ".join(problems))

        self.stdout.write(
            f"OK: backlog={backlog} oldest_age={_seconds(oldest)} "
            f"last_claim_age={_seconds(claim_age)}"
        )
