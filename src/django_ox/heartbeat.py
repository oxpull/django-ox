"""
Heartbeat files: local evidence that a worker's controlling loop is advancing.

``ox_worker --heartbeat-file PATH`` updates the file's modification time at
the head of every pass of the poll loop and of the drain, and before each
claim a pass makes, on the thread that runs them (the main thread, under
ox_worker), before the pass does any database work. ``ox_health
--heartbeat-file PATH`` reads that time back with ``lstat`` and nothing
else, so the probe needs no database, and a database that is down or hung
changes nothing about it.

A fresh file means exactly this: the expected controlling loops have advanced
within the age window. It does not mean the worker is healthy.

- A pass whose statements fail still starts with the update, so a database
  that refuses connections leaves the file fresh. The worker rides that out
  by itself, and a restart would buy nothing.
- A process that is stopped, a loop wedged in a call that never returns, and
  a statement the database accepts and never answers all stop the updates,
  and the file goes stale. django-ox sets no timeout on the loop's
  statements, so a database that hangs for everyone makes every worker's
  file stale at once. From
  inside the worker that looks the same as its own connection going
  half-open, and nothing here tells them apart.
- A task slot stuck in a task that never returns leaves the loop advancing,
  and the file fresh. TASK_TIMEOUT is what recovers that case. Neither the
  lease renewal thread nor the timeout watchdog writes the file, and nothing
  here reports whether they are alive.

Under ``--processes N`` above 1 the supervisor writes ``PATH.supervisor``
from its own loop and slot ``i`` writes ``PATH.i``. Neither refreshes the
other's file, and the probe requires every one of the ``N + 1`` to be fresh:
it never discovers files by listing the directory, so a slot that is missing
stays in the count and fails it. A slot whose supervisor has died stops
writing its file, draining or not: it no longer speaks for the slot.

The files are local and disposable. There are no contents, no temporary
file and rename, no lock and no fsync; the only thing that carries meaning is
the modification time, and only on the machine that wrote it. A directory
shared between replicas lets a live writer mask a dead one, so the directory
must be private to one container.

A supervised slot whose supervisor has died stops writing its heartbeat
file, draining or not. The worker can finish its in-flight tasks, but
cannot keep a replacement worker's slot fresh.
"""

from __future__ import annotations

import logging
import math
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("django_ox")

__all__ = [
    "HeartbeatFile",
    "HeartbeatReport",
    "check",
    "check_file",
    "child_file",
    "expected_files",
    "invalidate",
    "supervisor_file",
]

# O_NOFOLLOW makes the open fail on a symlink rather than update whatever it
# points at. O_NONBLOCK makes a FIFO without a reader fail the open instead
# of blocking the loop that is meant to prove it is not blocked. Neither
# changes anything for a regular file. Taken where the platform has them.
_OPEN_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_NOCTTY", 0)
    | getattr(os, "O_CLOEXEC", 0)
)

# Owner read and write, before the umask. A new file is the only one this
# mode reaches: an existing file keeps its permissions and its contents.
_MODE = 0o600


def supervisor_file(path: str) -> str:
    """The file the ``--processes`` supervisor writes for base ``path``."""
    return f"{path}.supervisor"


def child_file(path: str, index: int) -> str:
    """The file slot ``index`` writes under a supervisor, for base ``path``."""
    return f"{path}.{index}"


def expected_files(path: str, processes: int = 1) -> list[str]:
    """
    Every file an ``ox_worker --processes`` run writes for base ``path``.

    One process writes ``path`` itself. Above one, the supervisor's file
    comes first, then each slot's in order, and ``path`` is not among them.
    """
    if processes < 1:
        raise ValueError("processes must be at least 1")
    if processes == 1:
        return [path]
    return [supervisor_file(path), *(child_file(path, i) for i in range(processes))]


class _NotARegularFile(OSError):
    pass


def _touch(path: str) -> None:
    try:
        fd = os.open(path, _OPEN_FLAGS, _MODE)
    except PermissionError:
        # A umask that masks owner-write gives the file the first pass
        # creates mode 0400: the open that creates it may write, and every
        # later one may not. The owner may still set a file's times to now,
        # as touch does, so the file keeps moving instead of going stale one
        # age window after a healthy start. Anything else keeps the refusal,
        # so the open's rules for symlinks and special files stand.
        if not _owned_regular_file(path):
            raise
        # Not followed: a symlink swapped in since the lstat gets its own
        # time set, and the probe refuses it.
        os.utime(path, follow_symlinks=False)
        return
    try:
        mode = os.fstat(fd).st_mode
        if not stat.S_ISREG(mode):
            raise _NotARegularFile(f"{path} is not a regular file")
        if os.utime in os.supports_fd:
            os.utime(fd)
        else:  # pragma: no cover - every POSIX platform supports futimens
            os.utime(path)
    finally:
        os.close(fd)


def _owned_regular_file(path: str) -> bool:
    """Whether ``path`` is a regular file, not a link to one, this process owns."""
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:  # pragma: no cover - no owner to compare on Windows
        return False
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISREG(st.st_mode) and st.st_uid == geteuid()


class HeartbeatFile:
    """
    One process's heartbeat file, and the only thing that writes it.

    ``touch()`` never raises. A directory that is missing or read-only, a
    symlink or a FIFO at the path: each is reported once, as a warning, and
    every later call tries again, so a directory created after startup is
    picked up by the next pass. The probe meanwhile reports the file missing
    or stale, which is the right answer for a process whose evidence cannot
    be written.
    """

    def __init__(self, path: str, *, owner: str) -> None:
        self.path = path
        self.owner = owner
        self._warned = False

    def touch(self) -> bool:
        """Update the file's modification time. True when it was updated."""
        try:
            _touch(self.path)
        except Exception as exc:
            # Anything at all, not only OSError: this runs at the head of the
            # loop it reports on, outside the handler for database errors,
            # and a heartbeat must never be the reason that loop ends.
            if not self._warned:
                self._warned = True
                logger.warning(
                    "%s could not update its heartbeat file %s (%s). "
                    "ox_health --heartbeat-file reports it missing or stale "
                    "until it can; every pass tries again, and this is said "
                    "once",
                    self.owner,
                    self.path,
                    exc,
                    extra={
                        "event": "heartbeat_write_failed",
                        "heartbeat_file": self.path,
                        "error": str(exc),
                    },
                )
            return False
        return True


def invalidate(path: str) -> OSError | None:
    """
    Remove ``path`` if it is there, so stale evidence cannot pass for fresh.

    Returns the error when the removal failed for any reason other than the
    file already being gone; the caller decides whether to say so. A regular
    file that could not be removed stays valid until it ages out; nothing
    else at the path is ever valid.
    """
    try:
        Path(path).unlink()
    except FileNotFoundError:
        return None
    except OSError as exc:
        return exc
    return None


@dataclass(frozen=True)
class HeartbeatReport:
    """What the probe found at one expected path."""

    path: str
    #: Seconds since the file was last updated, by the checker's clock.
    #: None when there is no regular file to read a time from. Negative when
    #: the file's time is ahead of the checker's clock.
    age: float | None
    #: Why this file fails the check, or None when it passes.
    problem: str | None

    @property
    def ok(self) -> bool:
        return self.problem is None


def check_file(path: str, max_age: float, now: float | None = None) -> HeartbeatReport:
    """
    Judge one heartbeat file from its metadata alone.

    It passes only as a regular file, not a symlink to one, whose age is at
    least zero and at most ``max_age``. ``now`` is the wall clock, as
    ``time.time()`` gives it, when not passed. The file's time is the
    writer's wall clock, so a writer and a checker on one machine agree, and
    a time ahead of the checker's is reported as clock skew rather than
    counted as fresh.
    """
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return HeartbeatReport(path, None, f"heartbeat file {path} is missing")
    except OSError as exc:
        return HeartbeatReport(
            path, None, f"heartbeat file {path} cannot be read: {exc.strerror or exc}"
        )
    if stat.S_ISLNK(st.st_mode):
        kind = "a symlink"
    elif stat.S_ISDIR(st.st_mode):
        kind = "a directory"
    elif not stat.S_ISREG(st.st_mode):
        kind = "a special file"
    else:
        kind = None
    if kind is not None:
        return HeartbeatReport(
            path, None, f"heartbeat file {path} is {kind}, not a regular file"
        )
    age = (time.time() if now is None else now) - st.st_mtime
    if age < 0:
        return HeartbeatReport(
            path,
            age,
            f"heartbeat file {path} was updated {_ahead(-age)}s in the future; "
            "the clocks of the worker and the check disagree",
        )
    if age > max_age:
        return HeartbeatReport(
            path,
            age,
            f"heartbeat file {path} is {_over(age, max_age)}s old, "
            f"over --max-heartbeat-age {_limit(max_age)}s",
        )
    return HeartbeatReport(path, age, None)


def _over(age: float, max_age: float) -> str:
    """
    ``age``, which is over ``max_age``, to a tenth of a second, and never
    shown at or under it: rounded, 60.04 seconds over a 60 second limit
    would read "60.0s old, over --max-heartbeat-age 60s". Near the limit it
    is rounded up instead, to the first tenth above the limit.
    """
    tenths = round(age * 10)
    if tenths / 10 <= max_age:
        tenths = math.ceil(age * 10)
        # Only where age * 10 lost the difference to rounding.
        while tenths / 10 <= max_age:
            tenths += 1
    return f"{tenths / 10:.1f}"


def _limit(max_age: float) -> str:
    """
    ``max_age`` as ``%g`` shows it, unless that rounds it: 123456.7 would
    read as 123457, above an age shown as 123456.8 that is over it.
    """
    text = f"{max_age:g}"
    return text if float(text) == max_age else repr(max_age)


def _ahead(seconds: float) -> str:
    """How far in the future a file's time is, never shown as zero."""
    return "<0.1" if seconds < 0.05 else f"{seconds:.1f}"


def check(
    path: str, max_age: float, processes: int = 1, now: float | None = None
) -> list[HeartbeatReport]:
    """
    Judge every file an ``ox_worker --processes processes`` run should keep
    fresh, in the order ``expected_files`` gives. By default, each file is
    checked against the current time sampled after reading its metadata. If
    `now` is supplied, that value is used for all files.
    """
    return [check_file(p, max_age, now) for p in expected_files(path, processes)]
