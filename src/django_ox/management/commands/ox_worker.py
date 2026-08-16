import logging
import os
import signal
import sys
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

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

    def handle(self, *args: Any, **options: Any) -> None:
        if options["verbosity"] > 0 and not logger.handlers:
            handler = logging.StreamHandler(self.stderr)
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
            logger.addHandler(handler)
            logger.setLevel(
                logging.DEBUG if options["verbosity"] >= 2 else logging.INFO
            )

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
        )

        def handle_signal(signum: int, frame: Any) -> None:
            if worker.stopping:
                logger.error("Second signal received; forcing exit.")
                os._exit(130)
            logger.info(
                "Received %s; draining in-flight tasks. Signal again to force exit.",
                signal.Signals(signum).name,
            )
            worker.request_stop()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        worker.run()
        sys.exit(0)
