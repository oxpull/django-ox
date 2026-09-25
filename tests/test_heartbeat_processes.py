"""
ox_worker --heartbeat-file and ox_health --heartbeat-file as real processes.

Each test synchronises on what it can observe: a heartbeat file's
modification time moving, a line in the worker's log, a process id. The
timeouts are safety bounds, not the expected timings. A probe that must fail
runs only once the file has been seen to be older than the age it is given,
and nothing that stopped writing it can make it younger again.

Two faults are real rather than simulated, and differ by database:

- PostgreSQL and MySQL: the worker reaches the database through a relay in
  this process. Refusing closes the relay's port and every open flow, so a
  reconnect is refused; freezing stops relaying while every socket stays
  open, so a statement is sent and never answered.
- SQLite has no server to refuse or hang. This process takes an EXCLUSIVE
  lock on the database file instead. Under a short busy timeout every
  statement the worker runs fails at once, and under a long one every
  statement waits, which is a pass that does not come back.
"""

import contextlib
import os
import re
import select
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from django.conf import settings
from django.core.management import CommandError, call_command
from django.db import connection

from django_ox.models import OxTask

from .conftest import wait_for
from .tasks import add, query_and_hold

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="POSIX signals and file modes"),
    pytest.mark.skipif(
        shutil.which("pgrep") is None or shutil.which("ps") is None,
        reason="pgrep and ps are needed to find worker processes",
    ),
]

# Every probe that must pass is given this much, and every worker polls at
# INTERVAL, so a pass is minutes of margin rather than a race with a busy
# machine. Probes that must fail are given STALE_AGE, and run only after the
# file has been seen older than that.
FRESH_AGE = "30"
STALE_AGE = 1.0
INTERVAL = "0.05"

POLL_FAILED = "could not reach the database this pass"


# -- processes ----------------------------------------------------------------


def settings_module(tmp_path: Path) -> str:
    """
    The suite's settings, with two knobs a fault needs: the port the worker
    reaches the database on, and SQLite's busy timeout.
    """
    (tmp_path / "hb_settings.py").write_text(
        "import os\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "_db = DATABASES['default']  # noqa: F405\n"
        "if os.environ.get('OX_TEST_DB_PORT'):\n"
        "    _db['HOST'] = '127.0.0.1'\n"
        "    _db['PORT'] = os.environ['OX_TEST_DB_PORT']\n"
        "if os.environ.get('OX_TEST_SQLITE_TIMEOUT'):\n"
        "    _db['OPTIONS'] = {\n"
        "        **_db.get('OPTIONS', {}),\n"
        "        'timeout': float(os.environ['OX_TEST_SQLITE_TIMEOUT']),\n"
        "    }\n"
    )
    return "hb_settings"


def environment(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = settings_module(tmp_path)
    # The repository too, for a process started anywhere but in it.
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in [str(tmp_path), str(REPO), env.get("PYTHONPATH", "")] if p
    )
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    env.update(extra)
    return env


class Running:
    """One ox_worker process tree, its log, and a way to always end it."""

    def __init__(
        self, tmp_path: Path, *flags: str, cwd: Path = REPO, **env: str
    ) -> None:
        self.log_path = tmp_path / "worker.log"
        self._log = self.log_path.open("wb")
        self.proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "django", "ox_worker", *flags],
            cwd=cwd,
            env=environment(tmp_path, **env),
            stdout=self._log,
            stderr=self._log,
        )

    @property
    def pid(self) -> int:
        return self.proc.pid

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")

    def children(self) -> dict[int, int]:
        """Slot index -> pid, read off each child's own argv."""
        found = subprocess.run(  # noqa: S603
            ["pgrep", "-P", str(self.pid)],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
        slots = {}
        for pid in map(int, found):
            match = re.search(r"--worker-index (\d+)", argv(pid))
            if match:
                slots[int(match.group(1))] = pid
        return slots

    def stop(self) -> int:
        """Resume anything stopped, SIGTERM, and SIGKILL what will not go."""
        pids = [self.pid, *self.children().values()]
        for pid in pids:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGCONT)
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            for pid in pids:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
            code = self.proc.wait(timeout=30)
        self._log.close()
        return code

    def kill(self) -> None:
        """For a worker wedged where SIGTERM cannot reach it."""
        for pid in [self.pid, *self.children().values()]:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
        self.proc.wait(timeout=30)
        self._log.close()


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def argv(pid: int) -> str:
    return subprocess.run(  # noqa: S603
        ["ps", "-ww", "-o", "args=", "-p", str(pid)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()


def probe(
    tmp_path: Path,
    base: Path,
    *args: str,
    age: str | float = FRESH_AGE,
    cwd: Path = REPO,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "django",
            "ox_health",
            f"--heartbeat-file={base}",
            "--max-heartbeat-age",
            str(age),
            *args,
        ],
        cwd=cwd,
        env=environment(tmp_path),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


# -- observing files ----------------------------------------------------------


def mtime(path: Path | str) -> float | None:
    try:
        return Path(path).lstat().st_mtime
    except FileNotFoundError:
        return None


def advances(path: Path | str, times: int = 2, timeout: float = 30.0) -> bool:
    """Whether ``path``'s time is seen to change ``times`` times from now."""
    last = mtime(path)
    seen = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = mtime(path)
        if current is not None and current != last:
            seen += 1
            last = current
            if seen >= times:
                return True
        time.sleep(0.01)
    return False


def goes_stale(path: Path | str, age: float = STALE_AGE, timeout=30.0) -> bool:
    """Whether ``path`` is seen, still there, older than ``age`` seconds."""

    def stale() -> bool:
        current = mtime(path)
        return current is not None and time.time() - current > age + 0.25

    return wait_for(stale, timeout=timeout)


def all_advance(paths, timeout: float = 30.0) -> bool:
    return all(advances(path, timeout=timeout) for path in paths)


# -- database faults ----------------------------------------------------------


class Relay:
    """A TCP relay to the test database that a test can refuse or freeze."""

    def __init__(self, host: str, port: int) -> None:
        self.target = (host or "127.0.0.1", int(port))
        self.port = 0
        self._frozen = threading.Event()
        self._flows: list[socket.socket] = []
        self._lock = threading.Lock()
        self._closing: threading.Event | None = None
        self._acceptor: threading.Thread | None = None
        self.listen()

    def listen(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(64)
        listener.settimeout(0.05)
        self.port = listener.getsockname()[1]
        self._closing = threading.Event()
        self._acceptor = threading.Thread(
            target=self._accept, args=(listener, self._closing), daemon=True
        )
        self._acceptor.start()

    def _accept(self, listener: socket.socket, closing: threading.Event) -> None:
        # Polled rather than woken: closing a socket another thread is
        # blocked in accept() on does not wake it on every platform.
        with listener:
            while not closing.is_set():
                try:
                    client, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    return
                try:
                    server = socket.create_connection(self.target, timeout=10)
                except OSError:
                    client.close()
                    continue
                client.settimeout(None)
                server.settimeout(None)
                with self._lock:
                    self._flows += [client, server]
                for src, dst in ((client, server), (server, client)):
                    threading.Thread(
                        target=self._pump, args=(src, dst), daemon=True
                    ).start()

    def _pump(self, src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                if self._frozen.is_set():
                    time.sleep(0.01)
                    continue
                ready, _, _ = select.select([src], [], [], 0.05)
                if not ready:
                    continue
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            for sock in (src, dst):
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
                sock.close()

    def refuse(self) -> None:
        """Close the port and every open flow: the database has gone."""
        if self._closing is not None and self._acceptor is not None:
            self._closing.set()
            self._acceptor.join(timeout=10)
            self._closing = None
        with self._lock:
            flows, self._flows = self._flows, []
        for sock in flows:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()

    def freeze(self) -> None:
        """Relay nothing more, in either direction, and close nothing."""
        self._frozen.set()

    def thaw(self) -> None:
        self._frozen.clear()

    def close(self) -> None:
        self._frozen.clear()
        self.refuse()


class Fault:
    """A way to make the worker's database fail after it has started."""

    env: dict[str, str]

    def begin(self) -> None:
        raise NotImplementedError

    def end(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class RelayFault(Fault):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.relay = Relay(
            connection.settings_dict["HOST"], connection.settings_dict["PORT"]
        )
        self.env = {"OX_TEST_DB_PORT": str(self.relay.port)}
        self._broken = False

    def begin(self) -> None:
        self._broken = True
        if self.kind == "refuse":
            self.relay.refuse()
        else:
            self.relay.freeze()

    def end(self) -> None:
        if not self._broken:
            return
        self._broken = False
        if self.kind == "refuse":
            self.relay.listen()
        else:
            self.relay.thaw()

    def close(self) -> None:
        self.relay.close()


class SqliteLockFault(Fault):
    def __init__(self, kind: str) -> None:
        # Refused: a statement that meets the lock fails at once. Wedged: it
        # waits far longer than any test runs.
        self.env = {"OX_TEST_SQLITE_TIMEOUT": "0.05" if kind == "refuse" else "600"}
        self._db: sqlite3.Connection | None = None

    def begin(self) -> None:
        self._db = sqlite3.connect(
            str(connection.settings_dict["NAME"]), timeout=30, isolation_level=None
        )
        self._db.execute("BEGIN EXCLUSIVE")

    def end(self) -> None:
        if self._db is not None:
            self._db.execute("ROLLBACK")
            self._db.close()
            self._db = None

    close = end


@pytest.fixture
def fault(request):
    kind = request.param
    if connection.vendor == "sqlite":
        made: Fault = SqliteLockFault(kind)
    elif connection.vendor in ("postgresql", "mysql"):
        made = RelayFault(kind)
    else:  # pragma: no cover - the suite runs on these three
        pytest.skip(f"no way to break a {connection.vendor} database here")
    yield made
    made.close()


def succeeds(result_id, timeout: float = 30.0) -> bool:
    return wait_for(
        lambda: OxTask.objects.get(id=result_id).status == OxTask.Status.SUCCESSFUL,
        timeout=timeout,
    )


def expected(base: Path, processes: int) -> list[str]:
    """The files a fleet writes, spelled out as an operator's probe sees them."""
    if processes == 1:
        return [str(base)]
    return [f"{base}.supervisor", *(f"{base}.{i}" for i in range(processes))]


def names(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


def heartbeat_base(tmp_path: Path) -> Path:
    """A directory of its own for the heartbeat files, and the base path."""
    directory = tmp_path / "run"
    directory.mkdir()
    return directory / "hb"


# -- one worker ---------------------------------------------------------------


class TestOneWorker:
    def test_an_idle_worker_is_fresh_and_so_is_a_busy_one(self, tmp_path):
        base = heartbeat_base(tmp_path)
        worker = Running(
            tmp_path, "--interval", INTERVAL, "--heartbeat-file", str(base)
        )
        try:
            # Nothing is queued and nothing has ever been claimed, which is
            # where the claim-age probe fails every pod.
            assert advances(base, times=3), worker.log()
            result = probe(tmp_path, base)
            assert result.returncode == 0, result.stderr
            assert result.stdout.startswith("OK: heartbeat_files=1 ")

            task = add.enqueue(1, 2)
            assert succeeds(task.id), worker.log()
            assert advances(base)
            assert probe(tmp_path, base).returncode == 0
        finally:
            code = worker.stop()
        assert code == 0, worker.log()
        assert names(base.parent) == ["hb"]

    def test_a_held_slot_and_its_drain_leave_the_file_fresh(self, tmp_path):
        """
        The expected pass, stated as one: the only task slot is held and the
        loop keeps turning, so the file is fresh. It says nothing about the
        slot; TASK_TIMEOUT is what recovers one that never comes back. After
        SIGTERM the drain keeps the file fresh for as long as it waits.
        """
        base = heartbeat_base(tmp_path)
        marks = tmp_path / "marks.log"
        release = tmp_path / "release"
        task = query_and_hold.enqueue(str(marks), str(release))
        worker = Running(
            tmp_path,
            "--concurrency",
            "1",
            "--interval",
            INTERVAL,
            "--heartbeat-file",
            str(base),
        )
        try:
            assert wait_for(
                lambda: marks.exists() and "HELD" in marks.read_text(), timeout=30
            ), worker.log()
            assert advances(base, times=3), worker.log()
            assert probe(tmp_path, base).returncode == 0

            worker.proc.send_signal(signal.SIGTERM)
            assert wait_for(
                lambda: "draining 1 in-flight task(s)" in worker.log(), timeout=30
            ), worker.log()
            assert advances(base, times=2), worker.log()
            assert worker.proc.poll() is None
            release.touch()
            assert worker.proc.wait(timeout=30) == 0, worker.log()
        finally:
            release.touch()
            code = worker.stop()
        assert code == 0, worker.log()
        assert OxTask.objects.get(id=task.id).status == OxTask.Status.SUCCESSFUL

    @pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root ignores the permission bits this test relies on",
    )
    def test_a_read_only_file_of_its_own_keeps_moving(self, tmp_path):
        """
        What a umask without owner-write leaves after the first pass, or an
        operator's chmod: a file the worker owns and cannot open for
        writing. Its time is still the worker's to set.
        """
        base = heartbeat_base(tmp_path)
        base.write_bytes(b"")
        base.chmod(0o400)
        worker = Running(
            tmp_path, "--interval", INTERVAL, "--heartbeat-file", str(base)
        )
        try:
            assert advances(base, times=3), worker.log()
            result = probe(tmp_path, base)
            assert result.returncode == 0, result.stderr
        finally:
            code = worker.stop()
        assert code == 0, worker.log()
        assert stat.S_IMODE(base.lstat().st_mode) == 0o400
        assert "could not update its heartbeat file" not in worker.log()

    def test_a_stopped_worker_goes_stale_and_recovers(self, tmp_path):
        base = heartbeat_base(tmp_path)
        worker = Running(
            tmp_path, "--interval", INTERVAL, "--heartbeat-file", str(base)
        )
        try:
            assert advances(base), worker.log()
            os.kill(worker.pid, signal.SIGSTOP)
            assert goes_stale(base)
            result = probe(tmp_path, base, age=STALE_AGE)
            assert result.returncode == 1
            assert re.fullmatch(
                rf"CommandError: heartbeat file {re.escape(str(base))} is "
                r"[\d.]+s old, over --max-heartbeat-age 1s\n",
                result.stderr,
            ), result.stderr

            os.kill(worker.pid, signal.SIGCONT)
            assert advances(base)
            assert probe(tmp_path, base).returncode == 0
        finally:
            code = worker.stop()
        assert code == 0, worker.log()

    @pytest.mark.parametrize("fault", ["refuse"], indirect=True)
    def test_a_database_that_fails_after_startup_leaves_the_file_fresh(
        self, tmp_path, fault
    ):
        """
        The worker rides a database outage out, pass after pass, so the file
        says so. A probe that restarted it here would buy nothing.
        """
        base = heartbeat_base(tmp_path)
        worker = Running(
            tmp_path, "--interval", INTERVAL, "--heartbeat-file", str(base), **fault.env
        )
        try:
            assert advances(base), worker.log()
            fault.begin()
            assert wait_for(lambda: worker.log().count(POLL_FAILED) >= 3, timeout=30), (
                worker.log()
            )
            # Every one of these is a pass whose database work failed.
            assert advances(base, times=5), worker.log()
            result = probe(tmp_path, base)
            assert result.returncode == 0, result.stderr
            assert worker.proc.poll() is None

            fault.end()
            task = add.enqueue(2, 3)
            assert succeeds(task.id), worker.log()
        finally:
            fault.end()
            code = worker.stop()
        assert code == 0, worker.log()

    @pytest.mark.parametrize("fault", ["wedge"], indirect=True)
    def test_a_loop_wedged_in_a_statement_goes_stale_and_recovers(
        self, tmp_path, fault
    ):
        base = heartbeat_base(tmp_path)
        worker = Running(
            tmp_path, "--interval", INTERVAL, "--heartbeat-file", str(base), **fault.env
        )
        wedged = True
        try:
            assert advances(base), worker.log()
            fault.begin()
            assert goes_stale(base), worker.log()
            result = probe(tmp_path, base, age=STALE_AGE)
            assert result.returncode == 1
            assert "old, over --max-heartbeat-age 1s" in result.stderr
            # Wedged, not dead, and not failing either: nothing to log.
            assert worker.proc.poll() is None
            assert POLL_FAILED not in worker.log()

            fault.end()
            wedged = False
            assert advances(base), worker.log()
            assert probe(tmp_path, base).returncode == 0
        finally:
            fault.end()
            if wedged:
                worker.kill()
            else:
                assert worker.stop() == 0, worker.log()


# -- a supervised fleet -------------------------------------------------------


def start_fleet(tmp_path: Path, base: Path, processes: int = 2, *flags: str) -> Running:
    fleet = Running(
        tmp_path,
        "--processes",
        str(processes),
        "--interval",
        INTERVAL,
        "--heartbeat-file",
        str(base),
        *flags,
    )
    try:
        assert all_advance(expected(base, processes)), fleet.log()
        assert wait_for(lambda: len(fleet.children()) == processes, timeout=30)
    except BaseException:
        # The caller's finally never runs for a fleet it was not handed.
        fleet.stop()
        raise
    return fleet


class TestAFleet:
    def test_the_exact_files_and_the_flag_reaches_every_child(self, tmp_path):
        base = heartbeat_base(tmp_path)
        directory = base.parent
        fleet = start_fleet(tmp_path, base)
        try:
            assert names(directory) == ["hb.0", "hb.1", "hb.supervisor"]
            children = fleet.children()
            assert sorted(children) == [0, 1]
            for index, pid in children.items():
                args = argv(pid)
                assert f"--heartbeat-file={base} " in args
                assert f"--worker-index {index}" in args

            result = probe(tmp_path, base, "--processes", "2")
            assert result.returncode == 0, result.stderr
            assert result.stdout.startswith("OK: heartbeat_files=3 ")
            # The single-process file was never written, and a third slot
            # never existed: neither check can pass.
            single = probe(tmp_path, base)
            assert single.returncode == 1
            assert f"heartbeat file {base} is missing" in single.stderr
            three = probe(tmp_path, base, "--processes", "3")
            assert three.returncode == 1
            assert three.stderr == (
                f"CommandError: heartbeat file {base}.2 is missing\n"
            )
        finally:
            code = fleet.stop()
        assert code == 0, fleet.log()
        # Each slot's file went with it; the supervisor's own ages out.
        assert names(directory) == ["hb.supervisor"]

    def test_a_stopped_child_fails_the_check_while_its_siblings_run(self, tmp_path):
        base = heartbeat_base(tmp_path)
        fleet = start_fleet(tmp_path, base)
        try:
            stopped = fleet.children()[0]
            os.kill(stopped, signal.SIGSTOP)
            assert goes_stale(f"{base}.0")
            # The sibling and the supervisor carry on.
            assert all_advance([f"{base}.1", f"{base}.supervisor"])
            result = probe(tmp_path, base, "--processes", "2", age=STALE_AGE)
            assert result.returncode == 1
            assert result.stderr.startswith(
                f"CommandError: heartbeat file {base}.0 is "
            )
            assert result.stderr.count("heartbeat file") == 1

            os.kill(stopped, signal.SIGCONT)
            assert advances(f"{base}.0")
            assert probe(tmp_path, base, "--processes", "2").returncode == 0
        finally:
            code = fleet.stop()
        assert code == 0, fleet.log()

    def test_a_killed_child_is_missing_until_its_replacement_runs(self, tmp_path):
        base = heartbeat_base(tmp_path)
        slot = Path(f"{base}.0")
        fleet = start_fleet(tmp_path, base)
        try:
            killed = fleet.children()[0]
            os.kill(killed, signal.SIGKILL)
            # The supervisor removes the slot's file once it sees the exit,
            # rather than leave evidence that is still fresh for a process
            # that is gone.
            assert wait_for(lambda: not slot.exists(), timeout=30), fleet.log()
            with pytest.raises(
                CommandError, match=rf"{re.escape(str(slot))} is missing"
            ):
                call_command(
                    "ox_health",
                    heartbeat_file=str(base),
                    processes=2,
                    max_heartbeat_age=float(FRESH_AGE),
                )
            # Only that slot: its sibling and the supervisor are still there.
            assert Path(f"{base}.1").exists()
            assert Path(f"{base}.supervisor").exists()

            assert wait_for(
                lambda: fleet.children().get(0) not in (None, killed), timeout=30
            ), fleet.log()
            assert advances(slot), fleet.log()
            assert probe(tmp_path, base, "--processes", "2").returncode == 0
        finally:
            code = fleet.stop()
        assert code == 0, fleet.log()
        assert re.search(r"Worker process 0 exited with signal SIGKILL", fleet.log())

    def test_a_stopped_supervisor_fails_the_check_while_its_children_run(
        self, tmp_path
    ):
        base = heartbeat_base(tmp_path)
        fleet = start_fleet(tmp_path, base)
        try:
            os.kill(fleet.pid, signal.SIGSTOP)
            assert goes_stale(f"{base}.supervisor")
            assert all_advance([f"{base}.0", f"{base}.1"])
            result = probe(tmp_path, base, "--processes", "2", age=STALE_AGE)
            assert result.returncode == 1
            assert result.stderr.startswith(
                f"CommandError: heartbeat file {base}.supervisor is "
            )
            assert result.stderr.count("heartbeat file") == 1

            os.kill(fleet.pid, signal.SIGCONT)
            assert advances(f"{base}.supervisor")
            assert probe(tmp_path, base, "--processes", "2").returncode == 0
        finally:
            code = fleet.stop()
        assert code == 0, fleet.log()

    def test_a_dead_supervisor_fails_the_check(self, tmp_path):
        base = heartbeat_base(tmp_path)
        fleet = start_fleet(tmp_path, base)
        children = list(fleet.children().values())
        try:
            os.kill(fleet.pid, signal.SIGKILL)
            fleet.proc.wait(timeout=30)
            # Its children see the supervisor gone and drain, and nothing
            # writes its file again.
            assert wait_for(lambda: not any(map(alive, children)), timeout=30)
            assert goes_stale(f"{base}.supervisor")
            result = probe(tmp_path, base, "--processes", "2", age=STALE_AGE)
            assert result.returncode == 1
            assert f"heartbeat file {base}.supervisor is " in result.stderr
        finally:
            for pid in children:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
            fleet.stop()

    def test_an_orphan_stops_vouching_for_its_slot_while_it_drains(self, tmp_path):
        """
        A supervisor killed on its own, outside a container, leaves its
        children running; each sees it gone and drains. A drain can last as
        long as the longest task, and a supervisor started in its place
        expects the same slot files. An orphan that kept writing its file
        would pass the check for a slot the new fleet may have stopped.
        """
        base = heartbeat_base(tmp_path)
        marks = tmp_path / "marks.log"
        release = tmp_path / "release"
        task = query_and_hold.enqueue(str(marks), str(release))
        fleet = start_fleet(tmp_path, base, 2, "--concurrency", "1")
        children = fleet.children()
        try:
            assert wait_for(
                lambda: marks.exists() and "HELD" in marks.read_text(), timeout=30
            ), fleet.log()
            # The worker id ends in the slot number.
            locked_by = OxTask.objects.get(id=task.id).locked_by
            slot = int(locked_by.rsplit("-", 1)[1])
            holder, idle = children[slot], children[1 - slot]
            assert advances(f"{base}.{slot}"), fleet.log()

            os.kill(fleet.pid, signal.SIGKILL)
            fleet.proc.wait(timeout=30)
            # The idle orphan has nothing to drain and goes at once.
            assert wait_for(lambda: not alive(idle), timeout=30), fleet.log()
            assert alive(holder), fleet.log()

            assert goes_stale(f"{base}.{slot}"), fleet.log()
            # Still draining: the file stopped, the process did not.
            assert alive(holder), fleet.log()
            result = probe(tmp_path, base, "--processes", "2", age=STALE_AGE)
            assert result.returncode == 1
            assert f"heartbeat file {base}.{slot} is " in result.stderr

            release.touch()
            assert wait_for(lambda: not alive(holder), timeout=30), fleet.log()
        finally:
            release.touch()
            for pid in children.values():
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
            fleet.stop()
        assert OxTask.objects.get(id=task.id).status == OxTask.Status.SUCCESSFUL

    def test_a_path_that_starts_with_a_dash_reaches_every_child(self, tmp_path):
        """
        The supervisor forwards the path to each child. Forwarded as a token
        of its own, a path that starts with a dash reads as an option, and
        every child exits with a usage error before it writes anything.
        """
        directory = tmp_path / "run"
        directory.mkdir()
        fleet = Running(
            tmp_path,
            "--processes",
            "2",
            "--interval",
            INTERVAL,
            "--heartbeat-file=-hb",
            cwd=directory,
        )
        try:
            assert all_advance(
                [directory / "-hb.supervisor", directory / "-hb.0", directory / "-hb.1"]
            ), fleet.log()
            result = probe(tmp_path, Path("-hb"), "--processes", "2", cwd=directory)
            assert result.returncode == 0, result.stderr
        finally:
            code = fleet.stop()
        assert code == 0, fleet.log()
        assert "expected one argument" not in fleet.log()
        assert "exited with exit code" not in fleet.log()


# -- a worker class that cannot take the file ----------------------------------


class TestAWorkerClassWithoutTheKeyword:
    """
    A WORKER_CLASS written before --heartbeat-file existed: the flag is
    refused in one sentence, before any worker process starts, and the class
    keeps working without it.
    """

    ENV = {
        "OX_TEST_TASKS_OPTIONS": (
            '{"WORKER_CLASS": "tests.test_command.FixedSignatureWorker"}'
        )
    }
    REFUSED = (
        "CommandError: WORKER_CLASS tests.test_command.FixedSignatureWorker on "
        "backend 'default' does not accept the keyword argument heartbeat_file, "
        "which --heartbeat-file passes to it. Accept it and pass it on to "
        "Worker.__init__(), or run without --heartbeat-file.\n"
    )

    def worker(self, tmp_path: Path, *flags: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [sys.executable, "-m", "django", "ox_worker", *flags],
            cwd=REPO,
            env=environment(tmp_path, **self.ENV),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    @pytest.mark.parametrize("processes", ["1", "2"])
    def test_the_flag_is_refused_in_one_line(self, tmp_path, processes):
        base = heartbeat_base(tmp_path)
        result = self.worker(
            tmp_path, "--processes", processes, "--heartbeat-file", str(base)
        )
        assert result.returncode == 1
        assert result.stderr == self.REFUSED
        assert names(base.parent) == []

    def test_without_the_flag_it_still_runs(self, tmp_path):
        # FixedSignatureWorker.run() returns at once, so a run that reaches
        # it exits 0.
        result = self.worker(tmp_path)
        assert result.returncode == 0, result.stderr
        assert "CommandError" not in result.stderr
