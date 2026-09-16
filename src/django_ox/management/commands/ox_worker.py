import argparse
import logging
import os
import queue
import signal
import sys
import threading
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from django.core.management.base import CommandError, CommandParser

from django_ox.compat import DEFAULT_TASK_BACKEND_ALIAS
from django_ox.management._database import DatabaseCommand
from django_ox.supervisor import STOP_SIGNALS, SUPERVISOR_PID_ENV, Supervisor
from django_ox.timeouts import RECYCLE_EXIT_CODE
from django_ox.worker import Worker, worker_class

logger = logging.getLogger("django_ox")


class Command(DatabaseCommand):
    help = "Run a django-ox worker that executes tasks from the database queue."

    # A worker outlives the database it works on. The poll that cannot
    # reach it logs and waits, so a restart costs a pass rather than the
    # process, and that is worth more here than a startup check: an exit
    # would hand a restarting worker straight back to the same unreachable
    # database, and a process manager gives up after a few of those.
    checks_the_database = False

    def add_arguments(self, parser: CommandParser) -> None:
        super().add_arguments(parser)
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
        # Removed at once, so a task that starts an ox_worker of its own does
        # not pass this process's supervisor on to it.
        supervisor_pid = os.environ.pop(SUPERVISOR_PID_ENV, None)
        if options["processes"] < 1:
            raise CommandError("--processes must be at least 1.")
        # Before the supervisor branch, so a bad alias is reported by the
        # parent rather than by every child it starts.
        alias = self.database(options)
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
                worker_args=worker_args(options, alias),
            )
            for signum in STOP_SIGNALS:
                signal.signal(signum, supervisor.handle_signal)
            sys.exit(supervisor.run())

        parent_pid = None
        if options["worker_index"] is not None:
            # A getppid() read here names whoever adopted the child if the
            # supervisor died first, and the orphan would watch that pid
            # forever. The snapshot remains for a child started without the
            # variable, such as one a supervisor still running the previous
            # release restarts after an upgrade.
            if supervisor_pid is not None:
                with suppress(ValueError):
                    parent_pid = int(supervisor_pid)
            if parent_pid is None:
                parent_pid = os.getppid()

        queues = (
            [q.strip() for q in options["queues"].split(",") if q.strip()]
            if options["queues"]
            else None
        )
        worker = worker_class(options["backend"])(
            backend_alias=options["backend"],
            queues=queues,
            concurrency=options["concurrency"],
            poll_interval=options["interval"],
            lock_timeout=options["lock_timeout"],
            worker_index=options["worker_index"],
            parent_pid=parent_pid,
            db_alias=alias,
        )

        retire_signal_thread = install_stop_handlers(worker)
        if parent_pid is not None:
            # After the handlers: a parent-death signal armed before they
            # exist would kill the child instead of draining it. A supervisor
            # that died before the arming sends nothing; run() compares the
            # parent with parent_pid before every poll, its first included.
            _die_with_parent()

        try:
            worker.run()
        finally:
            retire_signal_thread()
        if worker.recycling:
            # A thread the timeout could not stop is still running. A normal
            # exit would wait for it at interpreter shutdown, which is the
            # wait the recycle exists to end; flush what can be flushed and
            # leave without it.
            logging.shutdown()
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(RECYCLE_EXIT_CODE)
        sys.exit(0)


def install_stop_handlers(worker: Worker) -> Callable[[], None]:
    """
    Make SIGTERM and SIGINT drain ``worker``, and a second one exit at once.

    Returns the function that ends the helper thread once ``run()`` returns.
    """
    stop_requests: queue.SimpleQueue[int | None] = queue.SimpleQueue()
    signals_seen = 0

    def handle_signal(signum: int, frame: Any) -> None:
        # Only a count, a SimpleQueue.put and os._exit belong here. Python
        # runs this on the main thread wherever that thread was, including
        # inside Event.wait() with the stop Event's lock held, so
        # request_stop(), logging or anything else that takes a lock can
        # block forever on the lock its own thread holds. SimpleQueue.put is
        # documented as safe to call from a signal handler.
        #
        # Counted here rather than read off worker.stopping: a worker that
        # is recycling is already stopping, and the operator's first signal
        # during that drain should not be the force-exit.
        nonlocal signals_seen
        signals_seen += 1
        if signals_seen > 1:
            # Nothing is written first, not even with os.write: a stderr
            # pipe that nobody is reading would block the exit.
            os._exit(130)
        stop_requests.put(signum)

    def act_on_stop_requests() -> None:
        while (signum := stop_requests.get()) is not None:
            # The stop first: a log handler that raises or blocks must not
            # cost the drain.
            worker.request_stop()
            with suppress(Exception):
                logger.info(
                    "Worker %s received %s; draining in-flight tasks. "
                    "Signal again to force exit.",
                    worker.worker_id,
                    signal.Signals(signum).name,
                )

    # The handlers first: off the main thread signal.signal() raises, and
    # nothing should be left behind when it does. A signal that arrives
    # before the thread starts waits in the queue. A daemon, so a forced
    # exit need not wait for it.
    signums = (signal.SIGTERM, signal.SIGINT)
    previous = {signum: signal.getsignal(signum) for signum in signums}
    for signum in signums:
        signal.signal(signum, handle_signal)
    thread = threading.Thread(
        target=act_on_stop_requests, name="ox-signal", daemon=True
    )
    try:
        thread.start()
    except BaseException:
        # Handlers that fed a queue nobody reads would swallow a stop, and a
        # signal already queued is one nobody else will act on. This is
        # ordinary code, not a handler, so it can stop the worker itself.
        for signum, handler in previous.items():
            if handler is not None:
                signal.signal(signum, handler)
        if not stop_requests.empty():
            worker.request_stop()
        raise

    def retire() -> None:
        stop_requests.put(None)
        thread.join(timeout=1.0)

    return retire


def _die_with_parent() -> None:
    """
    On Linux, ask the kernel to SIGTERM this process when its parent exits
    (PR_SET_PDEATHSIG). The worker also compares ``os.getppid()`` with the
    supervisor's pid, which covers every platform, a supervisor that died
    before this call and a refused call; this is the prompt version.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            logger.debug(
                "PR_SET_PDEATHSIG refused (errno %d); the parent pid check "
                "still applies",
                ctypes.get_errno(),
            )
    except (OSError, AttributeError):
        return


def worker_args(options: dict[str, Any], alias: str) -> list[str]:
    """The flags a child process needs to be this worker, minus --processes."""
    args = [
        "--backend",
        options["backend"],
        # Named rather than left to each child to resolve: one router that
        # answers differently in two processes would split the fleet across
        # two databases without saying so.
        "--database",
        alias,
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
    # Django's global flags change how a command runs before handle() is
    # reached; each child must run under the same ones it was given.
    if options.get("skip_checks"):
        args += ["--skip-checks"]
    if options.get("traceback"):
        args += ["--traceback"]
    if options.get("no_color"):
        args += ["--no-color"]
    if options.get("force_color"):
        args += ["--force-color"]
    # Django consumes these before the command sees them, but leaves them in
    # options. The child has to find the same settings from the same path.
    if options.get("settings"):
        args += ["--settings", options["settings"]]
    if options.get("pythonpath"):
        args += ["--pythonpath", str(Path(options["pythonpath"]).resolve())]
    return args
