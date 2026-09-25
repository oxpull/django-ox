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
forwards SIGTERM, SIGINT and SIGHUP to them, restarts a slot that dies while
the supervisor is not stopping, and exits with the children's worst exit
code, or 1 when a slot tripped the restart cap. A child that the forwarded
SIGTERM killed before it had installed its own handler is not one of those
codes: see _record_exit.

Under ``--heartbeat-file PATH`` it writes ``PATH.supervisor`` at the head of
each pass of its own loop, and removes slot ``i``'s ``PATH.i`` before it
starts that slot and once it sees the slot exit. It never writes a child's
file: django_ox.heartbeat says why.
"""

import logging
import os
import queue
import signal
import stat
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Any

from django.conf import settings

from .heartbeat import HeartbeatFile, child_file, invalidate, supervisor_file
from .timeouts import RECYCLE_EXIT_CODE

logger = logging.getLogger("django_ox")

# A slot that dies is restarted after this many seconds the first time, so a
# slot lost to a transient (a database restart) comes back before anyone
# notices. Each further death inside BACKOFF_RESET seconds of the previous
# start doubles the delay, up to BACKOFF_MAX; a child that lived at least
# BACKOFF_RESET seconds starts the sequence over.
RESTART_DELAY = 1.0
BACKOFF_MAX = 30.0
BACKOFF_RESET = 60.0

# More deaths than this inside one minute, in one slot, and the supervisor
# stops everything and exits 1. The process manager above it (systemd, the
# container runtime) then applies its own restart policy with its own
# backoff, which is where a persistent fault belongs. The count is per slot
# so that every slot dying at once (a database restart) is one restart each,
# not a trip; a sixth death of one slot in a minute is a fault, not a blip.
RESTART_CAP = 5
RESTART_WINDOW = 60.0

# After a second stop signal the children get SIGTERM again and this long
# to act on it, then SIGKILL.
KILL_GRACE = 5.0

# How often the supervisor polls its children. It is also the granularity of
# the restart delay and of the response to a stop request.
POLL_INTERVAL = 0.1

# Built from the signals this platform has, rather than named outright. SIGHUP
# does not exist on Windows and `ox_worker` imports this module
# unconditionally, so a named constant would fail at import, before argparse
# and before the command could explain anything. The limit belongs to
# --processes, and the check that reports it lives in handle().
STOP_SIGNALS = tuple(
    getattr(signal, name)
    for name in ("SIGTERM", "SIGINT", "SIGHUP")
    if hasattr(signal, name)
)

# Same reason. Where there is no SIGKILL, SIGTERM is the strongest signal
# available, so escalation repeats it.
FORCE_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)


# Set in each child's environment to the supervisor's pid, read before the
# child existed, so a child whose supervisor is already gone can tell. An
# environment variable rather than a flag: a child running an older release,
# as after a downgrade in place, ignores it instead of refusing to start.
SUPERVISOR_PID_ENV = "OX_SUPERVISOR_PID"


def child_command(
    worker_args: list[str], index: int, argv0: str | None = None
) -> list[str]:
    """
    The argv for one slot. ``worker_args`` is every flag except --processes.

    The child is started the way the supervisor was: a ``manage.py`` (or any
    other script) is re-run by absolute path, so the child has the same
    ``sys.path[0]`` whatever the current directory; anything else (a
    ``django-admin`` console script, ``python -m django``) becomes ``python
    -m django`` with the settings module in the environment.
    """
    script = Path(sys.argv[0] if argv0 is None else argv0)
    if script.suffix == ".py" and script.name != "__main__.py" and script.is_file():
        entry = [sys.executable, str(script.resolve())]
    else:
        entry = [sys.executable, "-m", "django"]
    return [
        *entry,
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
    # --settings, or it was already in the environment). The child may run
    # `-m django`, which only knows the environment.
    module = getattr(settings, "SETTINGS_MODULE", None)
    if module:
        env["DJANGO_SETTINGS_MODULE"] = module
    env[SUPERVISOR_PID_ENV] = str(os.getpid())
    return env


def _describe_exit(code: int) -> str:
    if code < 0:
        try:
            return f"signal {signal.Signals(-code).name}"
        except ValueError:
            return f"signal {-code}"
    return f"exit code {code}"


def _left_behind(path: str) -> str:
    """
    What a slot file that could not be removed means to the probe, for the
    warning that says so. Only a regular file can pass for fresh; anything
    else at the path fails the check for as long as it is there.
    """
    try:
        mode = os.lstat(path).st_mode
    except OSError:
        return "it cannot be read, so ox_health --heartbeat-file fails that slot"
    if stat.S_ISREG(mode):
        return "it counts as fresh until it ages out"
    return (
        "it is not a regular file, so ox_health --heartbeat-file fails that "
        "slot until it is removed"
    )


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
        backoff_max: float = BACKOFF_MAX,
        backoff_reset: float = BACKOFF_RESET,
        kill_grace: float = KILL_GRACE,
        heartbeat_file: str | None = None,
    ) -> None:
        if processes < 1:
            raise ValueError("processes must be at least 1")
        self.processes = processes
        self.worker_args = list(worker_args)
        self.restart_delay = restart_delay
        self.restart_cap = restart_cap
        self.restart_window = restart_window
        self.backoff_max = backoff_max
        self.backoff_reset = backoff_reset
        self.kill_grace = kill_grace
        self._children: dict[int, subprocess.Popen[bytes]] = {}
        self._started_at: dict[int, float] = {}
        self._exit_codes: dict[int, int] = {}
        self._restart_due: dict[int, float] = {}
        self._deaths: dict[int, deque[float]] = {}
        self._backoff: dict[int, float] = {}
        self._stopping = False
        self._cap_tripped = False
        self._signals_seen = 0
        self._kill_at: float | None = None
        # What handle_signal records and the run loop acts on.
        self._signals: queue.SimpleQueue[int] = queue.SimpleQueue()
        # Guards the stop flag and the restart decision together, so a stop
        # requested from another thread between the two never starts a
        # child that nobody will signal. Re-entrant because _start_due holds
        # it while it calls _start, which takes it too.
        self._lock = threading.RLock()
        # The base path of ox_worker --heartbeat-file. The supervisor writes
        # its own file from its own loop and never a child's: a child that is
        # stopped or wedged must go stale however well the supervisor is.
        self.heartbeat_file = heartbeat_file
        self._heartbeat = (
            HeartbeatFile(
                supervisor_file(heartbeat_file), owner=f"Supervisor {os.getpid()}"
            )
            if heartbeat_file
            else None
        )
        # Slot files whose removal failed, said once each.
        self._invalidation_reported: set[str] = set()

    # -- heartbeat ---------------------------------------------------------

    def _beat(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.touch()

    def _invalidate(self, index: int) -> None:
        """
        Remove slot ``index``'s heartbeat file, before a start and once its
        exit is seen.

        Without it a slot that dies, or a slot left over from the previous
        run of this container, keeps a fresh file for up to the probe's age
        window, and the check passes for a process that is not there. Best
        effort: a regular file that cannot be removed ages out instead, and
        anything else at the path fails the check until it is removed.
        """
        if self.heartbeat_file is None:
            return
        path = child_file(self.heartbeat_file, index)
        error = invalidate(path)
        if error is not None and path not in self._invalidation_reported:
            self._invalidation_reported.add(path)
            logger.warning(
                "Supervisor %d could not remove the heartbeat file %s of worker "
                "process %d (%s); %s",
                os.getpid(),
                path,
                index,
                error,
                _left_behind(path),
                extra={
                    "event": "heartbeat_invalidate_failed",
                    "heartbeat_file": path,
                    "worker_index": index,
                    "error": str(error),
                },
            )

    # -- children ----------------------------------------------------------

    def _start(self, index: int) -> None:
        with self._lock:
            if self._stopping:
                return
            # Each child gets its own process group so a terminal Ctrl-C
            # reaches the supervisor alone. The supervisor forwards it once;
            # a child in the foreground group would otherwise get the
            # terminal's copy and the forwarded one, and two signals mean
            # force-exit.
            self._invalidate(index)
            proc = subprocess.Popen(  # noqa: S603
                child_command(self.worker_args, index),
                env=_child_env(),
                cwd=Path.cwd(),
                process_group=0,
            )
            self._children[index] = proc
            self._started_at[index] = time.monotonic()
            self._exit_codes.pop(index, None)
            self._restart_due.pop(index, None)
        if self._stopping:
            # A stop requested from another thread landed while Popen was
            # running, before this child was in _children to be signalled.
            with suppress(ProcessLookupError):
                proc.send_signal(signal.SIGTERM)

    def _signal_children(self, signum: int) -> None:
        for proc in self._children.values():
            if proc.poll() is None:
                with suppress(ProcessLookupError):
                    proc.send_signal(signum)

    def _record_exit(self, index: int, code: int) -> None:
        """
        Record how one slot ended, as the supervisor's own exit code sees it.

        A child dies *from* SIGTERM only in the window between its exec and
        the moment it installs its handler; from there on the signal drains
        it and it exits 0. So a child that died this way, while the
        supervisor was stopping, was killed by the SIGTERM the supervisor
        itself had just forwarded, before it had opened a database
        connection or claimed a task. Nothing was lost and nothing failed.

        Reporting it upwards would say otherwise. A stop that lands during
        startup is ordinary (a restart, a deploy that rolls twice, a
        health check that never went green), and a unit on
        ``Restart=on-failure`` would read the 143 as a fault and start the
        service again.
        """
        if self._stopping and code == -signal.SIGTERM:
            logger.info(
                "Worker process %d was still starting when it was stopped; "
                "it had claimed no work",
                index,
                extra={
                    "event": "worker_process_stopped_early",
                    "worker_index": index,
                },
            )
            code = 0
        self._exit_codes[index] = code

    def _next_delay(self, index: int, now: float) -> float:
        """The restart delay for this death of ``index``, with backoff."""
        lived = now - self._started_at.get(index, now)
        if lived >= self.backoff_reset:
            self._backoff.pop(index, None)
        delay = self._backoff.get(index, self.restart_delay)
        self._backoff[index] = min(delay * 2, self.backoff_max)
        return delay

    def _reap_exited(self) -> None:
        """Collect children that have exited; schedule or record each."""
        for index, proc in list(self._children.items()):
            code = proc.poll()
            if code is None:
                continue
            del self._children[index]
            self._invalidate(index)
            self._record_exit(index, code)
            if self._stopping:
                continue
            now = time.monotonic()
            if code == RECYCLE_EXIT_CODE:
                # The worker chose to exit, after a task thread its timeout
                # could not stop. That is the recovery working, not a
                # fault: restart at the base delay, and keep it out of the
                # death count and the backoff.
                logger.warning(
                    "Worker process %d recycled itself (exit code %d); "
                    "restarting in %.1fs",
                    index,
                    code,
                    self.restart_delay,
                    extra={
                        "event": "worker_process_recycled",
                        "worker_index": index,
                        "exit_code": code,
                        "delay": self.restart_delay,
                    },
                )
                with self._lock:
                    if not self._stopping:
                        self._restart_due[index] = now + self.restart_delay
                continue
            deaths = self._deaths.setdefault(index, deque())
            deaths.append(now)
            while deaths and now - deaths[0] > self.restart_window:
                deaths.popleft()
            if len(deaths) > self.restart_cap:
                logger.error(
                    "Worker process %d exited with %s; %d deaths of this slot "
                    "in %.0fs is over the cap of %d, stopping",
                    index,
                    _describe_exit(code),
                    len(deaths),
                    self.restart_window,
                    self.restart_cap,
                    extra={
                        "event": "supervisor_restart_cap",
                        "worker_index": index,
                        "exit_code": code,
                        "restarts": len(deaths),
                    },
                )
                self._cap_tripped = True
                self.request_stop()
                self._signal_children(signal.SIGTERM)
                continue
            delay = self._next_delay(index, now)
            logger.warning(
                "Worker process %d exited with %s; restarting in %.1fs",
                index,
                _describe_exit(code),
                delay,
                extra={
                    "event": "worker_process_restarted",
                    "worker_index": index,
                    "exit_code": code,
                    "delay": delay,
                },
            )
            with self._lock:
                if not self._stopping:
                    self._restart_due[index] = now + delay

    def _start_due(self) -> None:
        with self._lock:
            if self._stopping:
                return
            now = time.monotonic()
            for index, due in list(self._restart_due.items()):
                if now >= due:
                    self._start(index)

    def _kill_overdue(self) -> None:
        if self._kill_at is None or time.monotonic() < self._kill_at:
            return
        self._kill_at = None
        alive = [index for index, proc in self._children.items() if proc.poll() is None]
        if alive:
            logger.error(
                "Worker process(es) %s still running %.0fs after the second "
                "signal; sending SIGKILL",
                ", ".join(str(index) for index in alive),
                self.kill_grace,
                extra={"event": "supervisor_killed_workers", "worker_indexes": alive},
            )
        self._signal_children(FORCE_SIGNAL)

    # -- signals -----------------------------------------------------------

    def handle_signal(self, signum: int, frame: Any) -> None:
        # Records the signal and nothing else; the run loop acts on it
        # within POLL_INTERVAL. Python runs a handler on the main thread in
        # the middle of whatever that thread was doing, which may be a log
        # write or a held lock, and a handler that logs or locks there can
        # fail or block. SimpleQueue.put is documented as safe to call from
        # a signal handler.
        self._signals.put(signum)

    def _process_signals(self) -> None:
        """Act on the signals handle_signal recorded, oldest first."""
        while True:
            try:
                signum = self._signals.get_nowait()
            except queue.Empty:
                return
            self._signals_seen += 1
            name = signal.Signals(signum).name
            if self._signals_seen == 1:
                logger.info(
                    "Received %s; stopping %d worker process(es). "
                    "Signal again to force exit.",
                    name,
                    len(self._children),
                )
            elif self._kill_at is None:
                logger.error(
                    "Second signal received; forcing worker exit, SIGKILL in %.0fs.",
                    self.kill_grace,
                )
                self._kill_at = time.monotonic() + self.kill_grace
            else:
                # A third signal: the operator has waited long enough.
                self._kill_at = time.monotonic()
            self.request_stop()
            # Children treat SIGTERM and SIGINT the same way, and their
            # second signal is the force-exit, so the supervisor's count is
            # theirs.
            self._signal_children(signal.SIGTERM)

    def request_stop(self) -> None:
        with self._lock:
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
            # Between starts, so a stop that lands while children are being
            # started ends the starting too, and one that arrived before
            # run() starts nothing.
            self._process_signals()
            if self._stopping:
                break
            self._start(index)
        try:
            while self._children or (self._restart_due and not self._stopping):
                self._beat()
                self._process_signals()
                self._reap_exited()
                self._start_due()
                self._kill_overdue()
                time.sleep(POLL_INTERVAL)
        finally:
            # Whatever path reached this point (a stop, the cap, an
            # exception), leave no child behind.
            self.request_stop()
            self._signal_children(signal.SIGTERM)
            # Polled rather than waited on: the signals only this loop acts
            # on include the second and third, which are what turn a child
            # that will not exit into a SIGKILL.
            while True:
                self._beat()
                self._process_signals()
                self._kill_overdue()
                self._reap_exited()
                if not self._children:
                    break
                time.sleep(POLL_INTERVAL)
        # A recycle that lands during a stop is a worker that finished the
        # job it was asked to do, not a failure to report upwards.
        failures = [
            (index, code)
            for index, code in sorted(self._exit_codes.items())
            if code not in (0, RECYCLE_EXIT_CODE)
        ]
        code = failures[0][1] if failures else 0
        # A child killed by a signal reports a negative code; the shell
        # convention for that is 128 + the signal number.
        if code < 0:
            code = 128 - code
        if self._cap_tripped:
            code = 1
        logger.info(
            "Supervisor %d stopped with exit code %d",
            os.getpid(),
            code,
            extra={"event": "supervisor_stopped", "exit_code": code},
        )
        return code
