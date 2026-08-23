"""
The process supervisor behind ``ox_worker --processes N``.

Each slot is a child interpreter running ``ox_worker --processes 1`` with the
same flags, so a slot is byte-for-byte the single-process worker: its own
database connections, its own renewer, its own reaper, its own drain. Nothing
is inherited across a fork, because there is no fork. ``multiprocessing`` with
the spawn start method would give a fresh interpreter too, but it re-imports
the parent's ``__main__`` (``manage.py``) to find the target, pickles the
target across, and owns the signal story; a plain subprocess re-invoking the
command is the thing an operator would type by hand, and it is what they see
in ``ps``.

The supervisor never opens a database connection. It starts the children,
forwards SIGTERM and SIGINT to them, restarts a slot that dies while the
supervisor is not stopping, and exits with the children's worst exit code.
"""

import logging
import os
import signal
import subprocess
import sys
import time
from collections import deque
from contextlib import suppress
from typing import Any

from django.conf import settings

logger = logging.getLogger("django_ox")

# A slot that dies is restarted after this many seconds, so a crash loop
# does not spin, and a slot lost to a transient (a database restart) comes
# back before anyone notices.
RESTART_DELAY = 1.0

# More restarts than this inside one minute, across all slots, and the
# supervisor stops everything and exits 1. The process manager above it
# (systemd, the container runtime) then applies its own restart policy with
# its own backoff, which is where a persistent fault belongs. Five restarts
# in a minute is a fault, not a blip; a single bad deploy hits it within
# seconds rather than logging forever.
RESTART_CAP = 5
RESTART_WINDOW = 60.0

# How often the supervisor polls its children. It is also the granularity of
# the restart delay and of the response to a stop request.
POLL_INTERVAL = 0.1


def child_command(worker_args: list[str], index: int) -> list[str]:
    """The argv for one slot. ``worker_args`` is every flag except --processes."""
    return [
        sys.executable,
        "-m",
        "django",
        "ox_worker",
        *worker_args,
        "--processes",
        "1",
        "--worker-index",
        str(index),
    ]


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    # The parent found its settings somehow (manage.py sets the variable,
    # or it was already in the environment). The child runs `-m django`,
    # which only knows the environment.
    module = getattr(settings, "SETTINGS_MODULE", None)
    if module:
        env["DJANGO_SETTINGS_MODULE"] = module
    return env


def _describe_exit(code: int) -> str:
    if code < 0:
        try:
            return f"signal {signal.Signals(-code).name}"
        except ValueError:
            return f"signal {-code}"
    return f"exit code {code}"


class Supervisor:
    """Runs ``processes`` copies of ``ox_worker`` and keeps them running."""

    def __init__(
        self,
        *,
        processes: int,
        worker_args: list[str],
        restart_delay: float = RESTART_DELAY,
        restart_cap: int = RESTART_CAP,
        restart_window: float = RESTART_WINDOW,
    ) -> None:
        if processes < 1:
            raise ValueError("processes must be at least 1")
        self.processes = processes
        self.worker_args = list(worker_args)
        self.restart_delay = restart_delay
        self.restart_cap = restart_cap
        self.restart_window = restart_window
        self._children: dict[int, subprocess.Popen[bytes]] = {}
        self._exit_codes: dict[int, int] = {}
        self._restart_due: dict[int, float] = {}
        self._restarts: deque[float] = deque()
        self._stopping = False
        self._signals_seen = 0

    # -- children ----------------------------------------------------------

    def _start(self, index: int) -> None:
        # Each child gets its own process group so a terminal Ctrl-C reaches
        # the supervisor alone. The supervisor forwards it once; a child in
        # the foreground group would otherwise get the terminal's copy and
        # the forwarded one, and two signals mean force-exit.
        self._children[index] = subprocess.Popen(  # noqa: S603
            child_command(self.worker_args, index),
            env=_child_env(),
            process_group=0,
        )
        self._exit_codes.pop(index, None)
        self._restart_due.pop(index, None)

    def _signal_children(self, signum: int) -> None:
        for proc in self._children.values():
            if proc.poll() is None:
                with suppress(ProcessLookupError):
                    proc.send_signal(signum)

    def _reap_exited(self) -> None:
        """Collect children that have exited; schedule or record each."""
        for index, proc in list(self._children.items()):
            code = proc.poll()
            if code is None:
                continue
            del self._children[index]
            if self._stopping:
                self._exit_codes[index] = code
                continue
            now = time.monotonic()
            self._restarts.append(now)
            while self._restarts and now - self._restarts[0] > self.restart_window:
                self._restarts.popleft()
            if len(self._restarts) > self.restart_cap:
                logger.error(
                    "Worker process %d exited with %s; %d restarts in %.0fs "
                    "is over the cap of %d, stopping",
                    index,
                    _describe_exit(code),
                    len(self._restarts),
                    self.restart_window,
                    self.restart_cap,
                    extra={
                        "event": "supervisor_restart_cap",
                        "worker_index": index,
                        "exit_code": code,
                        "restarts": len(self._restarts),
                    },
                )
                self._exit_codes[index] = code
                self.request_stop()
                self._signal_children(signal.SIGTERM)
                continue
            logger.warning(
                "Worker process %d exited with %s; restarting in %.1fs",
                index,
                _describe_exit(code),
                self.restart_delay,
                extra={
                    "event": "worker_process_restarted",
                    "worker_index": index,
                    "exit_code": code,
                },
            )
            self._restart_due[index] = now + self.restart_delay

    def _start_due(self) -> None:
        now = time.monotonic()
        for index, due in list(self._restart_due.items()):
            if now >= due:
                self._start(index)

    # -- signals -----------------------------------------------------------

    def handle_signal(self, signum: int, frame: Any) -> None:
        self._signals_seen += 1
        name = signal.Signals(signum).name
        if self._signals_seen == 1:
            logger.info(
                "Received %s; stopping %d worker process(es). "
                "Signal again to force exit.",
                name,
                len(self._children),
            )
        else:
            logger.error("Second signal received; forcing worker exit.")
        self.request_stop()
        # Children treat SIGTERM and SIGINT the same way, and their second
        # signal is the force-exit, so the supervisor's count is theirs.
        self._signal_children(signal.SIGTERM)

    def request_stop(self) -> None:
        self._stopping = True
        self._restart_due.clear()

    @property
    def stopping(self) -> bool:
        return self._stopping

    # -- lifecycle ---------------------------------------------------------

    def run(self) -> int:
        """Run until every child has stopped. Returns the exit code to use."""
        logger.info(
            "Supervisor %d starting %d worker process(es)",
            os.getpid(),
            self.processes,
            extra={"event": "supervisor_started", "processes": self.processes},
        )
        for index in range(self.processes):
            self._start(index)
        try:
            while self._children or self._restart_due:
                self._reap_exited()
                self._start_due()
                time.sleep(POLL_INTERVAL)
        finally:
            # Whatever path brought us here (a stop, the cap, an exception),
            # leave no child behind.
            self._stopping = True
            self._signal_children(signal.SIGTERM)
            for index, proc in self._children.items():
                self._exit_codes[index] = proc.wait()
            self._children.clear()
        failures = [
            (index, code)
            for index, code in sorted(self._exit_codes.items())
            if code != 0
        ]
        code = failures[0][1] if failures else 0
        # A child killed by a signal reports a negative code; the shell
        # convention for that is 128 + the signal number.
        if code < 0:
            code = 128 - code
        logger.info(
            "Supervisor %d stopped with exit code %d",
            os.getpid(),
            code,
            extra={"event": "supervisor_stopped", "exit_code": code},
        )
        return code
