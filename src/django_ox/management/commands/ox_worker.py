import argparse
import inspect
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

from django_ox.compat import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django_ox.heartbeat import child_file
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
        parser.add_argument(
            "--batch",
            action="store_true",
            help=(
                "Exit once a poll pass finds nothing to claim and no task is "
                "running, instead of waiting for more work."
            ),
        )
        parser.add_argument(
            "--max-tasks",
            default=None,
            metavar="N",
            help=(
                "Exit after claiming N task attempts, counting failed "
                "attempts and retries."
            ),
        )
        parser.add_argument(
            "--heartbeat-file",
            default=None,
            metavar="PATH",
            help=(
                "Update this file's modification time at the head of every "
                "poll and drain pass, for ox_health --heartbeat-file. With "
                "--processes above 1 the supervisor writes PATH.supervisor "
                "and each worker process PATH.0, PATH.1 and so on. The "
                "directory must exist and be private to this container "
                "(default: no heartbeat file)."
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
        max_tasks = _max_tasks(options["max_tasks"])
        heartbeat_file: str | None = options["heartbeat_file"]
        if heartbeat_file is not None and not heartbeat_file:
            raise CommandError("--heartbeat-file needs a path.")
        if options["processes"] > 1 and (options["batch"] or max_tasks is not None):
            # The supervisor restarts a child that exits unasked, so a
            # planned exit would be undone rather than honoured.
            raise CommandError(
                "--batch and --max-tasks run one worker process; they cannot "
                "be combined with --processes above 1."
            )
        # Before the supervisor branch, so a bad alias is reported by the
        # parent rather than by every child it starts.
        alias = self.database(options)
        backend_alias = options["backend"]
        if backend_alias not in task_backends:
            known = ", ".join(sorted(task_backends)) or "none"
            raise CommandError(
                f"No task backend alias {backend_alias!r} in TASKS. "
                f"Known aliases: {known}."
            )
        if heartbeat_file is not None:
            # Here as well as in each child, so a supervisor refuses once
            # instead of starting children that fail on it one after another.
            _require_heartbeat_keyword(worker_class(backend_alias), backend_alias)
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
                heartbeat_file=heartbeat_file,
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
        # Only when asked for: a WORKER_CLASS with a fixed-signature
        # constructor keeps working on every invocation that does not use
        # these flags (--batch, --max-tasks, --heartbeat-file).
        optional: dict[str, Any] = {}
        if options["batch"]:
            optional["batch"] = True
        if max_tasks is not None:
            optional["max_tasks"] = max_tasks
        if heartbeat_file is not None:
            # The supervisor hands every child the base path; the slot's own
            # file is named here, from the index it was started with, so a
            # restarted slot writes the file its predecessor did.
            optional["heartbeat_file"] = (
                heartbeat_file
                if options["worker_index"] is None
                else child_file(heartbeat_file, options["worker_index"])
            )
        cls = worker_class(options["backend"])
        worker = cls(
            backend_alias=options["backend"],
            queues=queues,
            concurrency=options["concurrency"],
            poll_interval=options["interval"],
            lock_timeout=options["lock_timeout"],
            worker_index=options["worker_index"],
            parent_pid=parent_pid,
            db_alias=alias,
            **optional,
        )
        if heartbeat_file is not None and not getattr(worker, "heartbeat_file", None):
            # Taken, by **kwargs say, and never passed on: the worker would
            # run and write nothing, and the probe would restart it for a
            # file it was never going to keep.
            raise CommandError(
                f"WORKER_CLASS {_class_name(cls)} on backend {backend_alias!r} "
                "accepted heartbeat_file but did not pass it on to "
                "Worker.__init__(), so no heartbeat file would be written. "
                "Pass it on, or run without --heartbeat-file."
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


def _max_tasks(value: str | None) -> int | None:
    # Validated here rather than with argparse's type=int, whose rejection
    # exits 2; an invalid limit is a CommandError like every other bad
    # option to this command.
    if value is None:
        return None
    try:
        limit = int(value)
    except ValueError:
        limit = 0
    if limit < 1:
        raise CommandError(
            f"--max-tasks must be an integer of at least 1, not {value!r}."
        )
    return limit


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


def _class_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _require_heartbeat_keyword(cls: type[Worker], backend_alias: str) -> None:
    """
    Refuse --heartbeat-file for a WORKER_CLASS whose constructor cannot take
    heartbeat_file, in a sentence rather than the TypeError its call would
    raise. A class written before the flag existed keeps working without
    it, which is why ox_worker passes the keyword only when asked to.
    """
    try:
        parameters = inspect.signature(cls).parameters.values()
    except (TypeError, ValueError):  # pragma: no cover - a class has one
        return
    if any(
        parameter.kind is parameter.VAR_KEYWORD
        or (
            parameter.name == "heartbeat_file"
            and parameter.kind
            in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY)
        )
        for parameter in parameters
    ):
        return
    raise CommandError(
        f"WORKER_CLASS {_class_name(cls)} on backend {backend_alias!r} does not "
        "accept the keyword argument heartbeat_file, which --heartbeat-file "
        "passes to it. Accept it and pass it on to Worker.__init__(), or run "
        "without --heartbeat-file."
    )


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
    # A value the operator chose goes in the same token as its flag. As a
    # token of its own, one that starts with a dash (a path such as -hb, a
    # queue named -x) reads as an option in the child, and every child
    # exits with a usage error before its loop starts, over and over.
    if options["queues"]:
        args += [f"--queues={options['queues']}"]
    if options["lock_timeout"] is not None:
        args += [f"--lock-timeout={options['lock_timeout']}"]
    # The base path, and only when set: an absent flag stays absent, so a
    # fixed-signature WORKER_CLASS in every child keeps working. Each child
    # names its own file from its --worker-index, so none of them writes the
    # supervisor's file or a sibling's.
    if options.get("heartbeat_file"):
        args += [f"--heartbeat-file={options['heartbeat_file']}"]
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
