import argparse
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from django_ox.supervisor import STOP_SIGNALS, Supervisor
from django_ox.worker import Worker

logger = logging.getLogger("django_ox")


class Command(BaseCommand):
    help = "Run a django-ox worker that executes tasks from the database queue."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Task backend alias from the TASKS setting (default: %(default)s).",
        )
        parser.add_argument(
            "--queues",
            default=None,
            help=(
                "Comma-separated queue names to process. Defaults to all "
                "queues configured for the backend."
            ),
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=1,
            help="Number of tasks to execute concurrently (default: %(default)s).",
        )
        parser.add_argument(
            "--interval",
            type=float,
            default=1.0,
            help="Polling interval in seconds when idle (default: %(default)s).",
        )
        parser.add_argument(
            "--lock-timeout",
            type=float,
            default=None,
            help=(
                "Seconds before a RUNNING task's lock is considered stale and "
                "the task is reclaimed (default: backend OPTIONS LOCK_TIMEOUT, "
                "or 300)."
            ),
        )

        parser.add_argument(
            "--processes",
            type=int,
            default=1,
            help=(
                "Worker processes to run. Above 1, this command supervises "
                "that many copies of itself, each a full worker with its own "
                "thread pool of --concurrency (default: %(default)s)."
            ),
        )
        # Set by the supervisor on each child; names the slot in worker ids.
        parser.add_argument(
            "--worker-index", type=int, default=None, help=argparse.SUPPRESS
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if options["processes"] < 1:
            raise CommandError("--processes must be at least 1.")
        if options["verbosity"] > 0 and not logger.handlers:
            handler = logging.StreamHandler(self.stderr)
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
            logger.addHandler(handler)
            logger.setLevel(
                logging.DEBUG if options["verbosity"] >= 2 else logging.INFO
            )

        if options["processes"] > 1:
            if os.name != "posix":
                raise CommandError(
                    "--processes above 1 needs POSIX signals; run one "
                    "ox_worker per process on this platform."
                )
            supervisor = Supervisor(
                processes=options["processes"],
                worker_args=worker_args(options),
            )
            for signum in STOP_SIGNALS:
                signal.signal(signum, supervisor.handle_signal)
            sys.exit(supervisor.run())

        parent_pid = None
        if options["worker_index"] is not None:
            parent_pid = os.getppid()
            _die_with_parent()

        queues = (
            [q.strip() for q in options["queues"].split(",") if q.strip()]
            if options["queues"]
            else None
        )
        worker = Worker(
            backend_alias=options["backend"],
            queues=queues,
            concurrency=options["concurrency"],
            poll_interval=options["interval"],
            lock_timeout=options["lock_timeout"],
            worker_index=options["worker_index"],
            parent_pid=parent_pid,
        )

        def handle_signal(signum: int, frame: Any) -> None:
            if worker.stopping:
                logger.error(
                    "Worker %s: second signal received; forcing exit.", worker.worker_id
                )
                os._exit(130)
            logger.info(
                "Worker %s received %s; draining in-flight tasks. "
                "Signal again to force exit.",
                worker.worker_id,
                signal.Signals(signum).name,
            )
            worker.request_stop()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        worker.run()
        sys.exit(0)


def _die_with_parent() -> None:
    """
    On Linux, ask the kernel to SIGTERM this process when its parent exits
    (PR_SET_PDEATHSIG). The worker also polls ``os.getppid()``, which covers
    every platform and the window before this call; this is the prompt
    version. Best effort: anything missing or refused is ignored.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        libc.prctl(1, signal.SIGTERM, 0, 0, 0)  # PR_SET_PDEATHSIG
    except (OSError, AttributeError):
        return


def worker_args(options: dict[str, Any]) -> list[str]:
    """The flags a child process needs to be this worker, minus --processes."""
    args = [
        "--backend",
        options["backend"],
        "--concurrency",
        str(options["concurrency"]),
        "--interval",
        str(options["interval"]),
        "--verbosity",
        str(options["verbosity"]),
    ]
    if options["queues"]:
        args += ["--queues", options["queues"]]
    if options["lock_timeout"] is not None:
        args += ["--lock-timeout", str(options["lock_timeout"])]
    # Django consumes these before the command sees them, but leaves them in
    # options. The child has to find the same settings from the same path.
    if options.get("settings"):
        args += ["--settings", options["settings"]]
    if options.get("pythonpath"):
        args += ["--pythonpath", str(Path(options["pythonpath"]).resolve())]
    return args
