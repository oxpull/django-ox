"""
A stop signal is recorded by its handler and acted on by ordinary code.

Python runs a signal handler on the main thread, between two bytecodes of
whatever that thread was doing. When that is the inside of ``Event.wait()``,
the Event's lock is held, and a handler that sets the Event waits forever on
a lock its own thread owns. These tests put the handler exactly there rather
than hoping a timed signal lands in a window a few microseconds wide.
"""

import builtins
import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import types

import pytest
from django.core.management import call_command

from django_ox.management.commands import ox_worker
from django_ox.models import OxTask
from django_ox.supervisor import Supervisor
from django_ox.worker import Worker

from .conftest import wait_for
from .tasks import add, slow
from .test_supervisor import (
    REPO,
    child_env,
    in_process_env,
    slot_pid,
    start_worker,
    wait_for_workers,
)

# Spelled out rather than imported, so this file still collects against a
# release without the variable and its tests fail there for the real reason.
SUPERVISOR_PID_ENV = "OX_SUPERVISOR_PID"

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="the handlers under test are POSIX signal handlers"
)

# Runs the real ox_worker command. Condition.__exit__ is wrapped so that the
# first time the main thread leaves Event.wait() on the worker's stop Event,
# with that Event's lock still held, SIGTERM is raised. raise_signal runs the
# Python handler before it returns, so the handler runs inside the lock.
_SIGNAL_INSIDE_EVENT_WAIT = textwrap.dedent(
    """
    import signal
    import threading

    import django

    django.setup()

    from django.core.management import call_command

    from django_ox.worker import Worker

    target = {}
    real_run = Worker.run
    real_exit = threading.Condition.__exit__

    def run(self):
        target["cond"] = self._stop._cond
        return real_run(self)

    def exit_holding_the_lock(self, *args):
        if (
            self is target.get("cond")
            and threading.current_thread() is threading.main_thread()
            and not target.get("raised")
        ):
            target["raised"] = True
            signal.raise_signal(signal.SIGTERM)
        return real_exit(self, *args)

    Worker.run = run
    threading.Condition.__exit__ = exit_holding_the_lock
    try:
        call_command("ox_worker", "--interval", "0.05")
    finally:
        print("RAISED=%s" % bool(target.get("raised")), flush=True)
    """
)


@pytest.mark.django_db(transaction=True)
def test_a_signal_inside_the_idle_wait_drains_the_worker():
    try:
        done = subprocess.run(  # noqa: S603
            [sys.executable, "-c", _SIGNAL_INSIDE_EVENT_WAIT],
            cwd=REPO,
            env=child_env(),
            capture_output=True,
            text=True,
            # A drain with nothing in flight takes well under a second.
            timeout=15,
            check=False,
        )
    except subprocess.TimeoutExpired as hung:
        # subprocess.run has already killed it.
        pytest.fail(
            "the worker hung after a signal inside Event.wait(); output:\n"
            f"{hung.stdout!r}\n{hung.stderr!r}"
        )
    output = done.stdout + done.stderr
    # The signal did land inside the lock; without this a run that never
    # reached the wait would pass.
    assert "RAISED=True" in done.stdout, output
    assert done.returncode == 0, output
    assert "received SIGTERM; draining" in output, output
    assert "stopped" in output, output


@pytest.fixture
def restore_signal_handlers():
    saved = {
        signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)
    }
    yield
    for signum, handler in saved.items():
        signal.signal(signum, handler)


def test_the_worker_handler_only_hands_the_signal_on(restore_signal_handlers):
    """
    The handler returns without calling request_stop(), and the helper
    thread does the stop. The handler is called by the thread that holds
    the stop Event's lock, which is the hang itself, so a handler that still
    set the Event would never return. That thread is a daemon with a bounded
    join, so such a handler fails this test rather than hanging the suite.
    """
    worker = Worker(poll_interval=0.05)
    retire = ox_worker.install_stop_handlers(worker)
    handler = signal.getsignal(signal.SIGTERM)
    assert callable(handler)
    seen: dict[str, bool] = {}

    def signal_while_holding_the_lock():
        with worker._stop._cond:
            handler(signal.SIGTERM, None)
            # The helper cannot set the Event until this lock is released.
            seen["stopping_inside"] = worker.stopping

    holder = threading.Thread(target=signal_while_holding_the_lock, daemon=True)
    try:
        holder.start()
        holder.join(timeout=5)
        assert not holder.is_alive(), "the handler blocked on the stop Event"
        assert seen == {"stopping_inside": False}
        assert wait_for(lambda: worker.stopping, timeout=5)
    finally:
        retire()
    assert not any(t.name == "ox-signal" for t in threading.enumerate())


class _Exited(Exception):
    pass


def test_the_worker_handler_writes_nothing(restore_signal_handlers, monkeypatch):
    """
    Neither signal makes the handler log, write or look anything up: the
    first only queues, the second only calls os._exit(130). Logging takes
    locks, and a write to a stderr pipe nobody reads blocks, so either
    could stop the handler from returning or the forced exit from
    happening. Every module the handler could reach them through is
    replaced by one that records what the handler's thread touches: every
    module the command module imports, its logger, print and open.
    """
    worker = Worker(poll_interval=0.05)
    handler_thread = threading.get_ident()
    in_handler = False
    touched: list[str] = []
    exits: list[int] = []

    def fake_exit(code):
        exits.append(code)
        raise _Exited

    class Recording:
        def __init__(self, name, real):
            self._name = name
            self._real = real

        def __getattr__(self, attr):
            if in_handler and threading.get_ident() == handler_thread:
                touched.append(f"{self._name}.{attr}")
            if self._name == "os" and attr == "_exit":
                return fake_exit
            return getattr(self._real, attr)

    watched = [
        name
        for name, value in vars(ox_worker).items()
        if isinstance(value, types.ModuleType) or name == "logger"
    ]
    assert {"logger", "logging", "os", "sys", "signal", "threading"} <= set(watched)
    for name in watched:
        monkeypatch.setattr(ox_worker, name, Recording(name, getattr(ox_worker, name)))
    for name in ("print", "open"):
        real = getattr(builtins, name)

        def recorded(*args, _name=name, _real=real, **kwargs):
            if in_handler and threading.get_ident() == handler_thread:
                touched.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(builtins, name, recorded)
    retire = ox_worker.install_stop_handlers(worker)
    handler = signal.getsignal(signal.SIGTERM)
    assert callable(handler)
    try:
        in_handler = True
        handler(signal.SIGTERM, None)
        in_handler = False
        assert touched == []
        assert wait_for(lambda: worker.stopping, timeout=5)

        in_handler = True
        with pytest.raises(_Exited):
            handler(signal.SIGTERM, None)
        in_handler = False
    finally:
        in_handler = False
        retire()
    assert touched == ["os._exit"]
    assert exits == [130]


def test_the_helper_stops_the_worker_before_it_logs(
    restore_signal_handlers, monkeypatch
):
    """A log handler that blocks must not hold up the drain."""
    worker = Worker(poll_interval=0.05)
    release = threading.Event()

    class BlockingLogger:
        def __getattr__(self, attr):
            return lambda *args, **kwargs: release.wait(10)

    monkeypatch.setattr(ox_worker, "logger", BlockingLogger())
    retire = ox_worker.install_stop_handlers(worker)
    try:
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        stopped = wait_for(lambda: worker.stopping, timeout=2)
    finally:
        release.set()
        retire()
    assert stopped


def test_a_signal_before_the_helper_starts_is_kept(
    restore_signal_handlers, monkeypatch
):
    """
    The handlers are installed before the helper thread starts, and a
    signal in between waits in the queue for it.
    """
    worker = Worker(poll_interval=0.05)
    real_start = threading.Thread.start
    installed: list[bool] = []

    def start(self):
        if self.name == "ox-signal":
            handler = signal.getsignal(signal.SIGTERM)
            installed.append(callable(handler))
            if callable(handler):
                handler(signal.SIGTERM, None)
        real_start(self)

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", start)
        retire = ox_worker.install_stop_handlers(worker)
    try:
        assert installed == [True]
        assert wait_for(lambda: worker.stopping, timeout=5)
    finally:
        retire()


def test_handlers_are_restored_when_the_helper_cannot_start(
    restore_signal_handlers, monkeypatch
):
    before = [signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)]

    def start(self):
        raise RuntimeError("can't start new thread")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", start)
        with pytest.raises(RuntimeError):
            ox_worker.install_stop_handlers(Worker(poll_interval=0.05))
    assert [signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)] == before


def test_a_signal_queued_before_a_failed_helper_start_still_stops(
    restore_signal_handlers, monkeypatch
):
    """The queue that held it has no reader, so the start-up code acts on it."""
    worker = Worker(poll_interval=0.05)

    def start(self):
        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        raise RuntimeError("can't start new thread")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", start)
        with pytest.raises(RuntimeError):
            ox_worker.install_stop_handlers(worker)
    assert worker.stopping


@pytest.mark.django_db(transaction=True)
def test_a_second_signal_exits_130_without_waiting_for_the_task(tmp_path):
    """
    The real command, mid-task: the first SIGTERM starts the drain, the
    second ends the process at once with 130 and leaves the task where it
    was. The row stays RUNNING for the reaper, as a crash would leave it.
    """
    result = slow.enqueue(30)
    log = tmp_path / "worker.log"
    proc = start_worker(tmp_path, "--interval", "0.05")
    try:
        assert wait_for(
            lambda: OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING,
            timeout=30,
        ), log.read_text()
        proc.send_signal(signal.SIGTERM)
        # Logged by the helper once the first signal has been acted on, so
        # the next one cannot be merged into it.
        assert wait_for(
            lambda: "received SIGTERM; draining" in log.read_text(), timeout=10
        ), log.read_text()
        proc.send_signal(signal.SIGTERM)
        try:
            code = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail(f"the second signal did not end the worker:\n{log.read_text()}")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert code == 130, log.read_text()
    assert OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
    assert "stopped" not in log.read_text()


class TestSupervisorHandler:
    def test_the_handler_neither_logs_nor_locks(self, caplog, monkeypatch):
        """
        With the supervisor's lock held elsewhere and every log record
        blocking, the handler still returns at once: it takes neither. The
        loop's processing step then does what the handler used to.
        """
        caplog.set_level(logging.INFO, logger="django_ox")
        supervisor = Supervisor(processes=1, worker_args=[], kill_grace=5.0)
        sent: list[int] = []
        monkeypatch.setattr(supervisor, "_signal_children", sent.append)

        release = threading.Event()
        locked = threading.Event()

        class BlockingHandler(logging.Handler):
            def emit(self, record):
                release.wait()

        def hold_the_lock():
            with supervisor._lock:
                locked.set()
                release.wait()

        blocking = BlockingHandler()
        logging.getLogger("django_ox").addHandler(blocking)
        holder = threading.Thread(target=hold_the_lock, daemon=True)
        holder.start()
        caller = threading.Thread(
            target=lambda: [
                supervisor.handle_signal(signal.SIGTERM, None) for _ in range(3)
            ],
            daemon=True,
        )
        try:
            assert locked.wait(5)
            caller.start()
            caller.join(timeout=2)
            returned = not caller.is_alive()
        finally:
            release.set()
            logging.getLogger("django_ox").removeHandler(blocking)
            holder.join(timeout=5)
            caller.join(timeout=5)

        assert returned, "handle_signal blocked on a log handler or the lock"
        assert sent == []
        assert not supervisor.stopping

        supervisor._process_signals()

        assert supervisor.stopping
        assert sent == [signal.SIGTERM] * 3
        messages = [r.getMessage() for r in caplog.records]
        assert "Received SIGTERM; stopping 0 worker process(es)." in messages[0]
        assert messages[1].startswith("Second signal received; forcing worker exit")
        # The third signal brought the SIGKILL forward to now.
        assert supervisor._kill_at is not None
        assert supervisor._kill_at <= time.monotonic()

    @pytest.mark.django_db(transaction=True)
    def test_escalation_still_works_after_the_loop_has_failed(
        self, caplog, monkeypatch, tmp_path
    ):
        """
        The run loop's clean-up waits for every child. The second and third
        signals are acted on only by a loop, so that wait has to be one: a
        child that will not exit must still be SIGKILLed.
        """
        caplog.set_level(logging.INFO, logger="django_ox")
        in_process_env(monkeypatch, tmp_path)
        supervisor = Supervisor(
            processes=1,
            worker_args=["--interval", "0.05", "--verbosity", "0"],
            kill_grace=0.5,
        )
        errors: list[BaseException] = []

        def run():
            try:
                supervisor.run()
            except RuntimeError as exc:
                errors.append(exc)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        stuck = None
        try:
            assert wait_for_workers(tmp_path, 1)
            assert slot_pid(supervisor, 0) is not None
            # The Popen, not its pid: once the supervisor has reaped the child
            # the number can belong to another process.
            stuck = supervisor._children[0]
            stuck.send_signal(signal.SIGSTOP)

            def boom():
                raise RuntimeError("the loop failed")

            monkeypatch.setattr(supervisor, "_start_due", boom)
            assert wait_for(lambda: supervisor.stopping, timeout=5)
            time.sleep(0.3)
            assert thread.is_alive()
            supervisor.handle_signal(signal.SIGTERM, None)
            supervisor.handle_signal(signal.SIGTERM, None)
            thread.join(timeout=10)
            finished = not thread.is_alive()
        finally:
            if thread.is_alive():
                # An assertion above failed: escalate so the supervisor thread
                # and its child do not outlive the test.
                for _ in range(3):
                    supervisor.handle_signal(signal.SIGTERM, None)
            if stuck is not None and stuck.poll() is None:
                stuck.kill()
            thread.join(timeout=10)

        assert finished, "the clean-up wait never acted on the second signal"
        assert [str(e) for e in errors] == ["the loop failed"]
        events = [getattr(r, "event", None) for r in caplog.records]
        assert events.count("supervisor_killed_workers") == 1
        assert supervisor._exit_codes == {0: -signal.SIGKILL}
        assert stuck.poll() == -signal.SIGKILL


class RecordingWorker(Worker):
    """Records what was already true when run() was entered."""

    seen: dict[str, object] = {}

    def run(self):
        RecordingWorker.seen["parent_pid"] = self.parent_pid
        RecordingWorker.seen["worker_id"] = self.worker_id
        # What a task that starts a process of its own would pass on.
        RecordingWorker.seen["env"] = os.environ.get(SUPERVISOR_PID_ENV)


def new_threads(before: set[threading.Thread], name: str) -> list[threading.Thread]:
    return [
        t
        for t in threading.enumerate()
        if t not in before and t.name == name and t.is_alive()
    ]


class TestExpectedParent:
    @pytest.fixture
    def recording(self, settings, monkeypatch, restore_signal_handlers):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "OPTIONS": {"WORKER_CLASS": "tests.test_stop_signals.RecordingWorker"},
            }
        }
        RecordingWorker.seen = {}
        # Restored afterwards, whatever the command did with it.
        monkeypatch.delenv(SUPERVISOR_PID_ENV, raising=False)
        armed: dict[str, object] = {}

        def die_with_parent():
            # What is in place at the moment the parent-death signal is armed.
            armed["handler"] = signal.getsignal(signal.SIGTERM)
            armed["helper"] = any(t.name == "ox-signal" for t in threading.enumerate())

        monkeypatch.setattr(ox_worker, "_die_with_parent", die_with_parent)
        return armed

    def test_the_supervisor_pid_comes_from_the_environment(
        self, recording, monkeypatch
    ):
        gone = os.getppid() + 100000
        monkeypatch.setenv(SUPERVISOR_PID_ENV, str(gone))
        with pytest.raises(SystemExit) as excinfo:
            call_command("ox_worker", "--worker-index=3", verbosity=0)
        assert excinfo.value.code == 0
        assert RecordingWorker.seen["parent_pid"] == gone
        assert str(RecordingWorker.seen["worker_id"]).endswith("-3")
        # Gone before the worker ran, so nothing it starts inherits it.
        assert RecordingWorker.seen["env"] is None
        assert SUPERVISOR_PID_ENV not in os.environ
        # The handler and the thread that acts on it existed before arming.
        assert callable(recording["handler"])
        assert recording["handler"] not in (signal.SIG_DFL, signal.SIG_IGN)
        assert recording["helper"] is True

    def test_without_the_variable_the_parent_is_read_at_start_up(self, recording):
        with pytest.raises(SystemExit):
            call_command("ox_worker", "--worker-index=0", verbosity=0)
        assert RecordingWorker.seen["parent_pid"] == os.getppid()

    def test_an_unreadable_variable_falls_back_to_the_parent(
        self, recording, monkeypatch
    ):
        monkeypatch.setenv(SUPERVISOR_PID_ENV, "not-a-pid")
        with pytest.raises(SystemExit):
            call_command("ox_worker", "--worker-index=0", verbosity=0)
        assert RecordingWorker.seen["parent_pid"] == os.getppid()
        assert SUPERVISOR_PID_ENV not in os.environ

    def test_a_worker_that_is_not_a_child_ignores_the_variable(
        self, recording, monkeypatch
    ):
        """A task's own ox_worker, say, run under a leftover variable."""
        monkeypatch.setenv(SUPERVISOR_PID_ENV, str(os.getppid() + 100000))
        with pytest.raises(SystemExit):
            call_command("ox_worker", verbosity=0)
        assert RecordingWorker.seen["parent_pid"] is None
        assert RecordingWorker.seen["env"] is None

    def test_the_command_ends_its_helper_thread(self, recording):
        before = set(threading.enumerate())
        with pytest.raises(SystemExit):
            call_command("ox_worker", verbosity=0)
        assert wait_for(lambda: not new_threads(before, "ox-signal"), timeout=2)

    def test_off_the_main_thread_nothing_is_left_behind(self, recording):
        """
        signal.signal() refuses to run off the main thread. The command
        fails there, as it always has, and leaves no helper thread behind.
        """
        before = set(threading.enumerate())
        errors: list[BaseException] = []

        def run():
            try:
                call_command("ox_worker", verbosity=0)
            except BaseException as exc:
                errors.append(exc)

        caller = threading.Thread(target=run)
        caller.start()
        caller.join(timeout=10)
        assert [type(e) for e in errors] == [ValueError]
        assert RecordingWorker.seen == {}
        assert new_threads(before, "ox-signal") == []

    @pytest.mark.django_db(transaction=True)
    def test_an_orphaned_child_claims_nothing(self):
        result = add.enqueue(1, 2)
        gone = os.getpid() + 100000
        env = child_env()
        env[SUPERVISOR_PID_ENV] = str(gone)
        done = subprocess.run(
            [
                sys.executable,
                "-m",
                "django",
                "ox_worker",
                "--interval",
                "0.05",
                "--worker-index",
                "0",
            ],
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        output = done.stdout + done.stderr
        assert done.returncode == 0, output
        lost = f"lost its supervisor (pid {gone}); draining"
        assert output.count(lost) == 1, output
        # Checked once, by the run loop, after the worker says it is starting.
        assert output.index("starting: queues") < output.index(lost), output
        assert "stopped" in output, output
        row = OxTask.objects.get(id=result.id)
        assert row.status == OxTask.Status.READY
        assert row.attempts == 0
