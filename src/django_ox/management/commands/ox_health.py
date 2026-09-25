import json
from datetime import timedelta
from typing import Any, NoReturn

from django.core.management.base import CommandError, CommandParser
from django.db import DatabaseError

from django_ox import heartbeat, stats
from django_ox.durations import parse_seconds
from django_ox.management._database import DatabaseCommand

#: What --max-heartbeat-age is when it is not given, in seconds.
DEFAULT_MAX_HEARTBEAT_AGE = 60.0

# The options that read the database, by dest, with the flag an operator
# typed. None of them means anything to a check that reads files.
_DATABASE_OPTIONS = (
    ("database", "--database"),
    ("queue", "--queue"),
    ("max_backlog", "--max-backlog"),
    ("max_age", "--max-age"),
    ("worker_timeout", "--worker-timeout"),
)

# The options that only qualify --heartbeat-file. Without it they would be
# accepted and ignored, and a probe that looks configured would check the
# database instead.
_HEARTBEAT_OPTIONS = (
    ("max_heartbeat_age", "--max-heartbeat-age"),
    ("processes", "--processes"),
)


def _seconds(value: timedelta | None) -> str:
    return "none" if value is None else f"{value.total_seconds():.0f}s"


def _total_seconds(value: timedelta | None) -> float | None:
    return None if value is None else value.total_seconds()


class Command(DatabaseCommand):
    help = (
        "Check queue health. Exits 0 when every enabled check passes, "
        "non-zero with a one-line reason otherwise. With no flags, only "
        "database reachability is checked. With --heartbeat-file, only the "
        "worker heartbeat files on this machine are checked, and the "
        "database is not touched."
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
        parser.add_argument(
            "--heartbeat-file",
            default=None,
            metavar="PATH",
            help=(
                "Check the heartbeat file that ox_worker --heartbeat-file "
                "PATH keeps, instead of the database: pass when it was "
                "updated within --max-heartbeat-age. Reads file metadata "
                "only, runs no system checks and opens no database "
                "connection, so it cannot be combined with --database, "
                "--queue, --max-backlog, --max-age or --worker-timeout "
                "(default: check the database)."
            ),
        )
        parser.add_argument(
            "--max-heartbeat-age",
            type=parse_seconds,
            default=None,
            metavar="SECONDS",
            help=(
                "With --heartbeat-file, fail when a heartbeat file was last "
                "updated longer ago than this. Accepts 7d, 24h, 90m, 45s, or "
                "a plain number of seconds (default: 60)."
            ),
        )
        parser.add_argument(
            "--processes",
            type=int,
            default=None,
            metavar="N",
            help=(
                "With --heartbeat-file, the --processes the worker runs with. "
                "Above 1, PATH.supervisor and PATH.0 to PATH.(N-1) must all "
                "be fresh (default: 1, which checks PATH itself)."
            ),
        )

    #: Set once an object has been printed, so a failure before handle()
    #: is reported and one inside it is not reported twice.
    _reported = False

    def execute(self, *args: Any, **options: Any) -> Any:
        """
        Choose between the database and the heartbeat-file check, and report
        a failure before `handle()` in the format that was asked for.

        The system checks run first, and scoping them to one alias is what
        opens the connection, so a database that is down ends the command
        there. Without this, `--format json` against one prints nothing on
        stdout, where it is documented to print an object with the figures
        null and the reason in `problems`. A healthcheck reads that object,
        and a healthcheck is what this flag is for.
        """
        try:
            if self._heartbeat_mode(options):
                # Chosen here, before BaseCommand.execute() runs the system
                # checks, because scoping them to an alias is what opens the
                # connection. A liveness probe that needs the database fails
                # in every container at once when the database does, and
                # restarts workers that were riding the outage out. This
                # mode reads file metadata and nothing else, so it has no
                # use for the checks; the database mode keeps them.
                options = {**options, "skip_checks": True}
            return super().execute(*args, **options)
        except CommandError as exc:
            if options.get("format") == "json" and not self._reported:
                if options.get("heartbeat_file") is not None:
                    self._write_heartbeat_json(options, None, [str(exc)])
                else:
                    self._write_json(options.get("queue"), None, None, None, [str(exc)])
            raise

    def _heartbeat_mode(self, options: dict[str, Any]) -> bool:
        """
        Whether this run checks heartbeat files, with the options that must
        not be mixed across the two modes refused before either one starts.
        """
        if options.get("heartbeat_file") is None:
            for dest, flag in _HEARTBEAT_OPTIONS:
                if options.get(dest) is not None:
                    raise CommandError(f"{flag} needs --heartbeat-file.")
            return False
        if not options["heartbeat_file"]:
            raise CommandError("--heartbeat-file needs a path.")
        mixed = [
            flag for dest, flag in _DATABASE_OPTIONS if options.get(dest) is not None
        ]
        if mixed:
            raise CommandError(
                "--heartbeat-file checks files, not the database, so it cannot "
                f"be combined with {', '.join(mixed)}; run those checks as a "
                "separate ox_health."
            )
        max_age = _max_heartbeat_age(options)
        # parse_seconds has already refused nan and inf.
        if max_age <= 0:
            raise CommandError(
                "--max-heartbeat-age must be a positive number of seconds."
            )
        if _processes(options) < 1:
            raise CommandError("--processes must be at least 1.")
        return True

    def handle(self, *args: Any, **options: Any) -> None:
        if options.get("heartbeat_file") is not None:
            self._handle_heartbeat(options)
            return
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

    def _handle_heartbeat(self, options: dict[str, Any]) -> None:
        reports = heartbeat.check(
            options["heartbeat_file"],
            _max_heartbeat_age(options),
            _processes(options),
        )
        problems = [report.problem for report in reports if report.problem]
        as_json = options["format"] == "json"
        if as_json:
            self._write_heartbeat_json(options, reports, problems)
        if problems:
            raise CommandError("; ".join(problems))
        if as_json:
            return
        oldest = max(report.age or 0.0 for report in reports)
        self.stdout.write(
            f"OK: heartbeat_files={len(reports)} oldest_heartbeat_age={oldest:.1f}s "
            f"max_heartbeat_age={_max_heartbeat_age(options):g}s"
        )

    def _write_heartbeat_json(
        self,
        options: dict[str, Any],
        reports: list[heartbeat.HeartbeatReport] | None,
        problems: list[str],
    ) -> None:
        self._reported = True
        self.stdout.write(
            json.dumps(
                {
                    "ok": not problems,
                    "heartbeat_file": options.get("heartbeat_file"),
                    "processes": _processes(options),
                    "max_heartbeat_age_seconds": _max_heartbeat_age(options),
                    # null when the files were never looked at, as the
                    # database figures are null when it was never reached.
                    "files": None
                    if reports is None
                    else [
                        {
                            "path": report.path,
                            "ok": report.ok,
                            "age_seconds": report.age,
                            "problem": report.problem,
                        }
                        for report in reports
                    ],
                    "problems": problems,
                }
            )
        )

    def _write_json(
        self,
        queue: str | None,
        backlog: int | None,
        oldest: timedelta | None,
        claim_age: timedelta | None,
        problems: list[str],
    ) -> None:
        self._reported = True
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


def _max_heartbeat_age(options: dict[str, Any]) -> float:
    value: float | None = options.get("max_heartbeat_age")
    return DEFAULT_MAX_HEARTBEAT_AGE if value is None else value


def _processes(options: dict[str, Any]) -> int:
    value: int | None = options.get("processes")
    return 1 if value is None else value
