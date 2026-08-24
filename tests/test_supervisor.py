"""
ox_worker --processes N, end to end: real child interpreters against the
test database. Every test here spawns processes, so counts are small and
polls are short.
"""

import contextlib
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection

from django_ox.models import OxTask
from django_ox.supervisor import Supervisor, child_command
from django_ox.worker import Worker

from .conftest import start_worker_thread, wait_for
from .tasks import add, slow

REPO = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        os.name != "posix", reason="--processes relies on POSIX signals"
    ),
]


def child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    env["OX_TEST_LOG_LEVEL"] = "INFO"
    return env


def start_worker(
    tmp_path: Path,
    *flags: str,
    argv: list[str] | None = None,
    cwd: Path = REPO,
    env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    log = (tmp_path / "worker.log").open("wb")
    return subprocess.Popen(  # noqa: S603
        [*(argv or [sys.executable, "-m", "django"]), "ox_worker", *flags],
        cwd=cwd,
        env=child_env() if env is None else env,
        stderr=log,
        stdout=log,
    )


def scratch_project(tmp_path: Path, startup_delay: float = 0.0) -> Path:
    """
    A project directory with its own manage.py and settings package, the way
    a deployment has one. The settings re-export the test settings so the
    children reach the test database.

    ``startup_delay`` holds the children inside the settings import, which is
    the window before their signal handlers exist. Only the children: the
    supervisor's own argv carries no --worker-index.
    """
    project = tmp_path / "proj"
    (project / "scratchproj").mkdir(parents=True)
    (project / "manage.py").write_text(
        "import os, sys\n"
        "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scratchproj.settings')\n"
        "from django.core.management import execute_from_command_line\n"
        "execute_from_command_line(sys.argv)\n"
    )
    (project / "scratchproj" / "__init__.py").write_text("")
    (project / "scratchproj" / "settings.py").write_text(
        f"import sys\nsys.path.insert(0, {str(REPO)!r})\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        + (
            "if '--worker-index' in sys.argv:\n"
            f"    import time; time.sleep({startup_delay})\n"
            if startup_delay
            else ""
        )
    )
    return project


def run_in_thread(supervisor: Supervisor) -> tuple[threading.Thread, list[int]]:
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(supervisor.run()))
    thread.start()
    return thread, result


def slot_pid(supervisor: Supervisor, index: int) -> int | None:
    proc = supervisor._children.get(index)
    return proc.pid if proc is not None and proc.poll() is None else None


def in_process_env(monkeypatch, tmp_path: Path | None = None) -> None:
    """
    Give this process the environment a child worker needs, so a Supervisor
    run from the test itself starts children against the test database.

    With ``tmp_path``, the children also append their lifecycle lines to
    ``worker.log`` under it, which is what ``wait_for_workers`` reads.
    """
    for key, value in child_env().items():
        monkeypatch.setenv(key, value)
    if tmp_path is not None:
        monkeypatch.setenv("OX_TEST_LOG_FILE", str(tmp_path / "worker.log"))


def stop_worker(proc: subprocess.Popen[bytes], tmp_path: Path) -> tuple[int, str]:
    """SIGTERM the supervisor, wait for it, and return (exit code, log)."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        code = proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        code = proc.wait()
    return code, (tmp_path / "worker.log").read_text()


WORKER_STARTED = re.compile(r"Worker \S+ starting: queues=")


def wait_for_workers(tmp_path: Path, count: int, timeout: float = 20.0) -> bool:
    """
    Wait until ``count`` children have logged the line they log once running.

    Until a child reaches that line it has not installed its signal handler,
    and a stop lands on the default disposition instead: it dies, having
    claimed nothing. The supervisor treats that as a clean stop
    (TestStopDuringStartup), so a test about draining live workers has to
    wait past the window or it is a test about the window.
    """
    log = tmp_path / "worker.log"
    return wait_for(
        lambda: log.exists() and len(WORKER_STARTED.findall(log.read_text())) >= count,
        timeout=timeout,
    )


def child_pids(proc: subprocess.Popen[bytes]) -> list[int]:
    out = subprocess.run(  # noqa: S603
        ["pgrep", "-P", str(proc.pid)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    return [int(pid) for pid in out]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def all_finished() -> bool:
    return not OxTask.objects.exclude(status=OxTask.Status.SUCCESSFUL).exists()


needs_pgrep = pytest.mark.skipif(
    shutil.which("pgrep") is None, reason="pgrep is needed to find child pids"
)


class TestProcessesOne:
    def test_is_the_plain_worker(self, tmp_path):
        result = add.enqueue(1, 2)
        proc = start_worker(tmp_path, "--processes", "1", "--interval", "0.05")
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
                ),
                timeout=20,
            )
        finally:
            code, log = stop_worker(proc, tmp_path)

        assert code == 0
        assert re.search(r"Worker \S+ starting: queues=", log)
        assert "Supervisor" not in log
        (worker_id,) = OxTask.objects.get(id=result.id).worker_ids
        # hostname-pid-random, with no slot suffix.
        assert re.fullmatch(r".+-\d+-[A-Za-z0-9]{8}", worker_id)


class TestProcessesTwo:
    def test_drains_fifty_tasks_across_two_workers(self, tmp_path):
        for _ in range(50):
            slow.enqueue(0.05)
        proc = start_worker(tmp_path, "--processes", "2", "--interval", "0.05")
        try:
            assert wait_for(all_finished, timeout=30)
        finally:
            code, log = stop_worker(proc, tmp_path)

        assert code == 0
        assert re.search(r"Supervisor \d+ starting 2 worker process\(es\)", log)
        assert re.search(r"Supervisor \d+ stopped with exit code 0", log)
        ids = {wid for row in OxTask.objects.all() for wid in row.worker_ids}
        assert len(ids) == 2
        assert {wid.rsplit("-", 1)[1] for wid in ids} == {"0", "1"}

    @needs_pgrep
    def test_sigterm_drains_children_and_exits_zero(self, tmp_path):
        result = slow.enqueue(1.0)
        proc = start_worker(tmp_path, "--processes", "2", "--interval", "0.05")
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
                ),
                timeout=20,
            )
            # A running task proves one child is up. This test is about two
            # live workers draining, so wait for the second one as well; a
            # stop during a child's startup is TestStopDuringStartup.
            assert wait_for_workers(tmp_path, 2)
            pids = child_pids(proc)
            assert len(pids) == 2
        finally:
            code, log = stop_worker(proc, tmp_path)

        assert code == 0, log
        assert not any(alive(pid) for pid in pids)
        assert not OxTask.objects.filter(status=OxTask.Status.RUNNING).exists()
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
        # Both children took the signal and drained on it. Without this the
        # exit code alone cannot tell a drain apart from a child that died
        # in its startup window, which also exits the supervisor zero.
        assert len(re.findall(r"Worker \S+ received SIGTERM; draining", log)) == 2
        assert len(re.findall(r"Worker \S+ stopped", log)) == 2
        assert "worker_process_restarted" not in log
        # Covers the startup-window line too: it also begins "Worker process".
        assert "Worker process" not in log

    @needs_pgrep
    def test_killed_child_is_restarted_and_its_task_reaped(self, tmp_path):
        result = slow.enqueue(3.0)
        proc = start_worker(
            tmp_path,
            "--processes",
            "2",
            "--interval",
            "0.05",
            "--lock-timeout",
            "0.5",
        )
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
                ),
                timeout=20,
            )
            before = set(child_pids(proc))
            holder = OxTask.objects.get(id=result.id).locked_by
            holder_pid = int(holder.split("-")[-3])
            assert holder_pid in before
            os.kill(holder_pid, signal.SIGKILL)

            # The survivor's reaper hands the task back, the restarted slot
            # or the survivor runs it again, and it finishes.
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
                ),
                timeout=30,
            )
            after = set(child_pids(proc))
            assert len(after) == 2
            assert holder_pid not in after
            # Three starts: the original two and the replacement. The stop
            # below is about draining live workers, not about catching the
            # replacement mid-import.
            assert wait_for_workers(tmp_path, 3)
        finally:
            code, log = stop_worker(proc, tmp_path)

        assert code == 0, log
        assert re.search(
            r"Worker process \d exited with signal SIGKILL; restarting", log
        )
        row = OxTask.objects.get(id=result.id)
        assert row.attempts == 2
        assert len(row.worker_ids) == 2
        assert row.worker_ids[0] == holder


class TestReinvocation:
    """The children start from any cwd, the way the supervisor was started."""

    def _runs_two_workers(self, tmp_path, proc):
        result = add.enqueue(1, 2)
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
                ),
                timeout=20,
            )
            # Both children live before the stop: this test is about two
            # reinvoked workers, so it has to see two of them.
            assert wait_for_workers(tmp_path, 2)
        finally:
            code, log = stop_worker(proc, tmp_path)
        assert code == 0, log
        assert "ModuleNotFoundError" not in log
        assert "Worker process" not in log
        assert len(WORKER_STARTED.findall(log)) == 2

    def test_absolute_manage_py_from_another_cwd(self, tmp_path):
        project = scratch_project(tmp_path)
        env = child_env()
        del env["DJANGO_SETTINGS_MODULE"]
        proc = start_worker(
            tmp_path,
            "--processes",
            "2",
            "--interval",
            "0.05",
            argv=[sys.executable, str(project / "manage.py")],
            cwd=Path("/"),
            env=env,
        )
        self._runs_two_workers(tmp_path, proc)

    def test_pythonpath_and_settings_are_forwarded(self, tmp_path):
        project = scratch_project(tmp_path)
        env = child_env()
        del env["DJANGO_SETTINGS_MODULE"]
        proc = start_worker(
            tmp_path,
            "--pythonpath",
            str(project),
            "--settings",
            "scratchproj.settings",
            "--processes",
            "2",
            "--interval",
            "0.05",
            cwd=Path("/"),
            env=env,
        )
        self._runs_two_workers(tmp_path, proc)


class TestStopDuringStartup:
    """
    A child cannot act on a signal until it has installed its handler, and
    that is after Django has been imported. A stop that lands before then
    kills the child outright, on the default disposition, with the very
    SIGTERM the supervisor forwarded.

    That window is not a test artefact. Anything that stops a service
    shortly after starting it -- a restart, a deploy that rolls twice, a
    health check that never goes green -- lands in it. The supervisor used
    to report the child's 143 as its own exit code, which a unit on
    ``Restart=on-failure`` reads as a fault and starts again.
    """

    # How long the children sit inside their settings import. The stop is
    # sent as soon as both child processes exist, so this only has to
    # outlast one pgrep; a slower machine makes the test more certain, not
    # less. Nothing waits it out: the children die inside it.
    IMPORT_SECONDS = 15.0

    @needs_pgrep
    def test_a_stop_in_the_startup_window_is_not_a_failure(self, tmp_path):
        project = scratch_project(tmp_path, startup_delay=self.IMPORT_SECONDS)
        env = child_env()
        del env["DJANGO_SETTINGS_MODULE"]
        proc = start_worker(
            tmp_path,
            "--pythonpath",
            str(project),
            "--settings",
            "scratchproj.settings",
            "--processes",
            "2",
            "--interval",
            "0.05",
            cwd=Path("/"),
            env=env,
        )
        try:
            assert wait_for(lambda: len(child_pids(proc)) == 2, timeout=20)
            pids = child_pids(proc)
        finally:
            code, log = stop_worker(proc, tmp_path)

        assert code == 0, log
        # The window is what was tested: neither child ever got as far as
        # the line it logs once it is running, so neither could have
        # handled the signal.
        assert not WORKER_STARTED.search(log), log
        assert log.count("was still starting when it was stopped") == 2, log
        assert not any(alive(pid) for pid in pids)
        assert "worker_process_restarted" not in log

    def test_a_child_killed_outside_a_stop_is_still_a_failure(self):
        """
        The rule is about the supervisor's own signal, not about 143 in
        general. A worker something else killed is a death like any other.
        """
        supervisor = Supervisor(processes=1, worker_args=[])
        supervisor._record_exit(0, -signal.SIGTERM)
        assert supervisor._exit_codes == {0: -signal.SIGTERM}

        supervisor.request_stop()
        supervisor._record_exit(0, -signal.SIGTERM)
        assert supervisor._exit_codes == {0: 0}
        # A child that ignored the signal and had to be killed still counts.
        supervisor._record_exit(1, -signal.SIGKILL)
        assert supervisor._exit_codes[1] == -signal.SIGKILL


class TestRestartCap:
    def test_over_the_cap_exits_nonzero(self, caplog, monkeypatch):
        caplog.set_level(logging.WARNING, logger="django_ox")
        in_process_env(monkeypatch)
        # A backend alias that does not exist: every child fails at startup.
        supervisor = Supervisor(
            processes=1,
            worker_args=["--backend", "nope", "--verbosity", "0"],
            restart_delay=0.05,
            restart_cap=2,
        )

        code = supervisor.run()

        assert code == 1
        events = [getattr(r, "event", None) for r in caplog.records]
        assert events.count("worker_process_restarted") == 2
        assert events.count("supervisor_restart_cap") == 1
        assert {r.exit_code for r in caplog.records if hasattr(r, "exit_code")} == {1}

    def test_exit_code_is_one_even_when_children_exit_zero(self, caplog, monkeypatch):
        """A child that exits 0 on its own is a death too, and the cap exits 1."""
        caplog.set_level(logging.WARNING, logger="django_ox")
        in_process_env(monkeypatch)
        # `ox_worker --help` prints and exits 0.
        supervisor = Supervisor(
            processes=1,
            worker_args=["--help"],
            restart_delay=0.05,
            restart_cap=2,
        )

        code = supervisor.run()

        assert code == 1
        cap = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "supervisor_restart_cap"
        ]
        assert len(cap) == 1
        assert cap[0].worker_index == 0
        assert cap[0].exit_code == 0

    def test_every_slot_dying_at_once_is_one_restart_each(
        self, caplog, monkeypatch, tmp_path
    ):
        caplog.set_level(logging.WARNING, logger="django_ox")
        in_process_env(monkeypatch, tmp_path)
        supervisor = Supervisor(
            processes=4,
            worker_args=["--interval", "0.05", "--verbosity", "0"],
            restart_delay=0.05,
        )
        thread, result = run_in_thread(supervisor)
        try:
            # Four running workers, not four processes that exist: what is
            # under test is what the supervisor does when live slots die
            # together.
            assert wait_for_workers(tmp_path, 4)
            before = [slot_pid(supervisor, i) for i in range(4)]
            for pid in before:
                os.kill(pid, signal.SIGKILL)
            assert wait_for(
                lambda: all(
                    slot_pid(supervisor, i) not in (None, before[i]) for i in range(4)
                ),
                timeout=20,
            )
            assert thread.is_alive()
            assert not supervisor.stopping
            # Eight starts: four originals and four replacements. The drain
            # below is the claim being tested, so it has to find four live
            # workers rather than four that are still importing.
            assert wait_for_workers(tmp_path, 8)
        finally:
            supervisor.request_stop()
            supervisor._signal_children(signal.SIGTERM)
            thread.join(timeout=20)
        assert result == [0]
        # Four deaths, four restarts: one each, not a slot counted twice.
        events = [getattr(r, "event", None) for r in caplog.records]
        assert events.count("worker_process_restarted") == 4
        assert events.count("supervisor_restart_cap") == 0

    def test_one_slot_over_the_cap_stops_the_supervisor(self, caplog, monkeypatch):
        caplog.set_level(logging.WARNING, logger="django_ox")
        in_process_env(monkeypatch)
        supervisor = Supervisor(
            processes=2,
            worker_args=["--interval", "0.05", "--verbosity", "0"],
            restart_delay=0.05,
            backoff_max=0.05,
        )
        thread, result = run_in_thread(supervisor)
        try:
            for death in range(6):
                assert wait_for(lambda: slot_pid(supervisor, 0), timeout=20)
                pid = slot_pid(supervisor, 0)
                os.kill(pid, signal.SIGKILL)
                if death < 5:
                    assert wait_for(
                        lambda: slot_pid(supervisor, 0) not in (None, pid),  # noqa: B023
                        timeout=20,
                    )
            thread.join(timeout=20)
        finally:
            supervisor.request_stop()
            supervisor._signal_children(signal.SIGTERM)
            thread.join(timeout=20)
        assert result == [1]
        cap = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "supervisor_restart_cap"
        ]
        assert [r.worker_index for r in cap] == [0]
        assert cap[0].restarts == 6

    def test_backoff_doubles_and_resets(self, monkeypatch):
        supervisor = Supervisor(
            processes=1, worker_args=[], backoff_max=4.0, backoff_reset=10.0
        )
        clock = 100.0
        supervisor._started_at[0] = clock
        delays = [supervisor._next_delay(0, clock) for _ in range(5)]
        assert delays == [1.0, 2.0, 4.0, 4.0, 4.0]
        # A child that lived a long time starts over.
        supervisor._started_at[0] = clock
        assert supervisor._next_delay(0, clock + 10.0) == 1.0
        # Slots back off independently.
        assert supervisor._next_delay(1, clock) == 1.0


def test_child_command_reinvokes_a_single_worker():
    assert child_command(["--concurrency", "4"], 3, argv0="django-admin") == [
        sys.executable,
        "-m",
        "django",
        "ox_worker",
        "--concurrency",
        "4",
        "--processes",
        "1",
        "--worker-index",
        "3",
    ]


def test_child_command_reuses_the_script_by_absolute_path(tmp_path, monkeypatch):
    script = tmp_path / "manage.py"
    script.write_text("")
    monkeypatch.chdir(tmp_path)
    assert child_command([], 0, argv0="manage.py")[:2] == [
        sys.executable,
        str(script.resolve()),
    ]
    # `python -m django` has a __main__.py for argv[0]; that is not a script.
    assert child_command([], 0, argv0=str(tmp_path / "__main__.py"))[1] == "-m"
    # A script that no longer exists falls back to the module.
    assert child_command([], 0, argv0=str(tmp_path / "gone.py"))[1] == "-m"


class TestOrphans:
    @needs_pgrep
    def test_children_drain_when_the_supervisor_is_killed(self, tmp_path):
        proc = start_worker(tmp_path, "--processes", "2", "--interval", "0.05")
        try:
            assert wait_for(lambda: len(child_pids(proc)) == 2, timeout=20)
            pids = child_pids(proc)
            assert wait_for(
                lambda: (
                    "starting: queues" in (tmp_path / "worker.log").read_text()
                    and (tmp_path / "worker.log").read_text().count("starting") >= 3
                ),
                timeout=20,
            )
            os.kill(proc.pid, signal.SIGKILL)
            assert proc.wait(timeout=5) == -signal.SIGKILL
            assert wait_for(lambda: not any(alive(pid) for pid in pids), timeout=10)
        finally:
            for pid in pids:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        log = (tmp_path / "worker.log").read_text()
        # Both children drained cleanly rather than being killed.
        assert log.count("stopped") == 2, log
        if sys.platform.startswith("linux"):
            # The kernel delivers SIGTERM the moment the supervisor dies
            # (PR_SET_PDEATHSIG), before the next ppid poll runs, so the
            # children drain through their ordinary signal path.
            assert log.count("received SIGTERM; draining") == 2, log
        else:
            # No parent-death signal here: the ppid poll is the drain path.
            assert log.count("lost its supervisor") == 2, log

    @needs_pgrep
    def test_sighup_is_a_drain(self, tmp_path):
        proc = start_worker(tmp_path, "--processes", "2", "--interval", "0.05")
        try:
            assert wait_for(
                lambda: (tmp_path / "worker.log").read_text().count("starting") >= 3,
                timeout=20,
            )
            pids = child_pids(proc)
            assert len(pids) == 2
            proc.send_signal(signal.SIGHUP)
            proc.wait(timeout=20)
        finally:
            code, log = stop_worker(proc, tmp_path)
        assert code == 0
        assert "Received SIGHUP; stopping 2 worker process(es)" in log
        assert not any(alive(pid) for pid in pids)

    def test_worker_drains_when_its_parent_changes(self, caplog):
        caplog.set_level(logging.WARNING, logger="django_ox")
        worker = Worker(poll_interval=0.05, parent_pid=os.getppid() + 100000)
        thread = start_worker_thread(worker)
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert [getattr(r, "event", None) for r in caplog.records] == [
            "worker_orphaned"
        ]


class TestEscalation:
    def test_second_signal_kills_a_stuck_child_after_the_grace(
        self, caplog, monkeypatch, tmp_path
    ):
        caplog.set_level(logging.INFO, logger="django_ox")
        in_process_env(monkeypatch, tmp_path)
        supervisor = Supervisor(
            processes=2,
            worker_args=["--interval", "0.05", "--verbosity", "0"],
            kill_grace=0.5,
        )
        thread, result = run_in_thread(supervisor)
        stuck = None
        try:
            assert wait_for(
                lambda: all(slot_pid(supervisor, i) for i in range(2)), timeout=20
            )
            # Both children running, handlers installed: what is under test
            # is a worker that will not act on a signal it can receive, not
            # one that never reached the point of receiving it.
            assert wait_for_workers(tmp_path, 2)
            stuck = slot_pid(supervisor, 0)
            os.kill(stuck, signal.SIGSTOP)
            started = time.monotonic()
            supervisor.handle_signal(signal.SIGTERM, None)
            time.sleep(0.3)
            assert thread.is_alive()
            supervisor.handle_signal(signal.SIGTERM, None)
            thread.join(timeout=10)
            elapsed = time.monotonic() - started
        finally:
            if stuck is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(stuck, signal.SIGKILL)
            thread.join(timeout=10)
        assert not thread.is_alive()
        assert elapsed < 5
        assert result == [128 + signal.SIGKILL]
        events = [getattr(r, "event", None) for r in caplog.records]
        assert events.count("supervisor_killed_workers") == 1
        assert not alive(stuck)


class TestStopRestartRace:
    def test_a_stop_just_before_the_restart_starts_nothing(self, monkeypatch):
        """A stop that lands between the snapshot and Popen must start nothing."""
        in_process_env(monkeypatch)
        supervisor = Supervisor(
            processes=1,
            worker_args=["--interval", "0.05", "--verbosity", "0"],
            restart_delay=0.05,
        )
        original = supervisor._start
        starts = 0

        def patched(index):
            nonlocal starts
            starts += 1
            if starts == 2:
                supervisor.handle_signal(signal.SIGTERM, None)
            original(index)

        monkeypatch.setattr(supervisor, "_start", patched)
        thread, result = run_in_thread(supervisor)
        try:
            assert wait_for(lambda: slot_pid(supervisor, 0), timeout=20)
            os.kill(slot_pid(supervisor, 0), signal.SIGKILL)
            thread.join(timeout=10)
        finally:
            supervisor.request_stop()
            supervisor._signal_children(signal.SIGTERM)
            thread.join(timeout=10)
        assert starts == 2
        assert supervisor._children == {}
        assert result == [128 + signal.SIGKILL]


def test_a_second_signal_forwards_again(monkeypatch):
    supervisor = Supervisor(processes=1, worker_args=[])
    sent: list[int] = []
    monkeypatch.setattr(supervisor, "_signal_children", sent.append)

    supervisor.handle_signal(signal.SIGINT, None)
    supervisor.handle_signal(signal.SIGINT, None)

    assert supervisor.stopping
    assert sent == [signal.SIGTERM, signal.SIGTERM]
