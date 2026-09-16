import json
from datetime import timedelta
from typing import Any, NoReturn

from django.core.management.base import CommandError, CommandParser
from django.db import DatabaseError

from django_ox import stats
from django_ox.durations import parse_seconds
from django_ox.management._database import DatabaseCommand


def _seconds(value: timedelta | None) -> str:
    return "none" if value is None else f"{value.total_seconds():.0f}s"


def _total_seconds(value: timedelta | None) -> float | None:
    return None if value is None else value.total_seconds()


class Command(DatabaseCommand):
    help = (
        "Check queue health. Exits 0 when every enabled check passes, "
        "non-zero with a one-line reason otherwise. With no flags, only "
        "database reachability is checked."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "--format",
            choices=["text", "json"],
            default="text",
            help=(
                "Output format. json prints one object with the same figures "
                "on stdout, also when a check fails; the exit status is the "
                "same either way (default: %(default)s)."
            ),
        )
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
            type=parse_seconds,
            default=None,
            help=(
                "Fail when a READY task has been eligible to run for longer "
                "than this. Accepts 7d, 24h, 90m, 45s, or a plain number of "
                "seconds (default: no age check)."
            ),
        )
        parser.add_argument(
            "--worker-timeout",
            type=parse_seconds,
            default=None,
            help=(
                "Fail when no worker has claimed a task within this long. "
                "Accepts 7d, 24h, 90m, 45s, or a plain number of seconds. "
                "Claim activity is the only worker trace in the "
                "database, so this check suits queues with steady traffic; "
                "for bursty queues prefer --max-age (default: no worker "
                "check)."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        max_backlog: int | None = options["max_backlog"]
        max_age: float | None = options["max_age"]
        worker_timeout: float | None = options["worker_timeout"]
        queue: str | None = options["queue"]
        as_json = options["format"] == "json"

        def _invalid(reason: str) -> NoReturn:
            if as_json:
                self._write_json(queue, None, None, None, [reason])
            raise CommandError(reason)

        # Once: the three figures below are three readings of one queue, so
        # they come from one alias rather than one each. Reported the way
        # the other bad arguments are, so --format json still gets an object.
        try:
            alias = self.database(options)
        except CommandError as exc:
            _invalid(str(exc))

        if max_backlog is not None and max_backlog < 0:
            _invalid("--max-backlog must be zero or a positive integer.")
        if max_age is not None and max_age <= 0:
            _invalid("--max-age must be a positive number of seconds.")
        if worker_timeout is not None and worker_timeout <= 0:
            _invalid("--worker-timeout must be a positive number of seconds.")

        try:
            backlog = stats.ready_count(queue, using=alias)
            oldest = stats.oldest_ready_age(queue, using=alias)
            claim_age = stats.last_claim_age(queue, using=alias)
        except DatabaseError as exc:
            reason = f"Database unreachable: {exc}"
            if as_json:
                self._write_json(queue, None, None, None, [reason])
            raise CommandError(reason) from exc

        problems: list[str] = []
        if max_backlog is not None and backlog > max_backlog:
            problems.append(f"backlog is {backlog}, over --max-backlog {max_backlog}")
        if (
            max_age is not None
            and oldest is not None
            and oldest.total_seconds() > max_age
        ):
            problems.append(
                f"oldest ready task is {_seconds(oldest)} old, "
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
        if as_json:
            # A monitoring agent wants the figures most when a check fails, so
            # the object is printed before the non-zero exit, not instead of it.
            self._write_json(queue, backlog, oldest, claim_age, problems)
        if problems:
            raise CommandError("; ".join(problems))
        if as_json:
            return

        self.stdout.write(
            f"OK: backlog={backlog} oldest_age={_seconds(oldest)} "
            f"last_claim_age={_seconds(claim_age)}"
        )

    def _write_json(
        self,
        queue: str | None,
        backlog: int | None,
        oldest: timedelta | None,
        claim_age: timedelta | None,
        problems: list[str],
    ) -> None:
        self.stdout.write(
            json.dumps(
                {
                    "ok": not problems,
                    "queue": queue,
                    "backlog": backlog,
                    "oldest_age_seconds": _total_seconds(oldest),
                    "last_claim_age_seconds": _total_seconds(claim_age),
                    "problems": problems,
                }
            )
        )
