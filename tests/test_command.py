import os
import subprocess
import sysconfig
from pathlib import Path

import pytest
from django.core.management import CommandError, call_command

from django_ox.compat import DEFAULT_TASK_BACKEND_ALIAS
from django_ox.management.commands import ox_worker
from django_ox.worker import Worker


class WorkerRecorder:
    """Stands in for the worker class to verify CLI flag wiring without a run loop."""

    instances: list["WorkerRecorder"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.stopping = False
        self.recycling = False
        WorkerRecorder.instances.append(self)

    def run(self):
        pass


class StoppedWorker(Worker):
    """Runs one pass and returns, so the command exits without a queue."""

    started = False

    def run(self):
        StoppedWorker.started = True


class FixedSignatureWorker(Worker):
    """A WORKER_CLASS written before --batch and --max-tasks existed."""

    started = False

    def __init__(
        self,
        *,
        backend_alias,
        queues,
        concurrency,
        poll_interval,
        lock_timeout,
        worker_index,
        parent_pid,
        db_alias,
    ):
        super().__init__(
            backend_alias=backend_alias,
            queues=queues,
            concurrency=concurrency,
            poll_interval=poll_interval,
            lock_timeout=lock_timeout,
            worker_index=worker_index,
            parent_pid=parent_pid,
            db_alias=db_alias,
        )

    def run(self):
        FixedSignatureWorker.started = True


@pytest.fixture
def recorded_worker(monkeypatch):
    WorkerRecorder.instances = []
    monkeypatch.setattr(ox_worker, "worker_class", lambda alias: WorkerRecorder)
    return WorkerRecorder


def test_command_runs_the_configured_worker_class(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "OPTIONS": {"WORKER_CLASS": "tests.test_command.StoppedWorker"},
        }
    }
    StoppedWorker.started = False

    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker")

    assert excinfo.value.code == 0
    assert StoppedWorker.started is True


def test_command_passes_flags_to_worker(recorded_worker):
    with pytest.raises(SystemExit) as excinfo:
        call_command(
            "ox_worker",
            "--queues=emails, default",
            "--concurrency=4",
            "--interval=0.5",
            "--lock-timeout=60",
        )
    assert excinfo.value.code == 0
    (worker,) = recorded_worker.instances
    assert worker.kwargs == {
        "backend_alias": "default",
        "queues": ["emails", "default"],
        "concurrency": 4,
        "poll_interval": 0.5,
        "lock_timeout": 60.0,
        "worker_index": None,
        "parent_pid": None,
        "db_alias": "default",
    }


def test_command_defaults(recorded_worker):
    with pytest.raises(SystemExit):
        call_command("ox_worker", verbosity=0)
    (worker,) = recorded_worker.instances
    assert worker.kwargs["queues"] is None
    assert worker.kwargs["concurrency"] == 1
    assert worker.kwargs["lock_timeout"] is None


def test_processes_one_never_starts_a_supervisor(recorded_worker, monkeypatch):
    def boom(**kwargs):
        raise AssertionError("supervisor constructed")

    monkeypatch.setattr(ox_worker, "Supervisor", boom)
    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", "--processes=1", verbosity=0)
    assert excinfo.value.code == 0
    (worker,) = recorded_worker.instances
    assert worker.kwargs["worker_index"] is None


def test_a_supervisor_child_runs_the_configured_worker_class(settings, monkeypatch):
    """
    The reason WORKER_CLASS is a setting rather than a second command.

    Supervisor.child_command spawns children by the name ``ox_worker``, so a
    worker class supplied on the parent's command line would not reach them:
    every child above --processes 1 would silently run the stock worker. The
    class comes from settings, which the child reads for itself, and this
    asserts the child really does.
    """
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "OPTIONS": {"WORKER_CLASS": "tests.test_command.StoppedWorker"},
        }
    }
    StoppedWorker.started = False
    monkeypatch.setattr(ox_worker, "_die_with_parent", lambda: None)

    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", "--worker-index=0", verbosity=0)

    assert excinfo.value.code == 0
    assert StoppedWorker.started is True


def test_the_child_command_the_supervisor_spawns_is_ox_worker(settings):
    """
    The other half of the pair above. If this name ever changes, the child
    stops being the command that reads WORKER_CLASS and the test above stops
    covering anything real.
    """
    from django_ox.supervisor import child_command

    assert "ox_worker" in child_command([], 0, argv0="manage.py")


def test_processes_below_one_is_rejected(recorded_worker):
    with pytest.raises(CommandError):
        call_command("ox_worker", "--processes=0", verbosity=0)
    assert recorded_worker.instances == []


def test_worker_args_carry_every_flag_but_processes():
    options = {
        "backend": "default",
        "queues": "emails,default",
        "concurrency": 4,
        "interval": 0.5,
        "lock_timeout": 60.0,
        "verbosity": 1,
        "processes": 3,
    }
    assert ox_worker.worker_args(options, "default") == [
        "--backend",
        "default",
        "--database",
        "default",
        "--concurrency",
        "4",
        "--interval",
        "0.5",
        "--verbosity",
        "1",
        # One token each, so a value that starts with a dash reaches the
        # child as a value rather than as an option.
        "--queues=emails,default",
        "--lock-timeout=60.0",
    ]


def test_worker_index_is_not_in_the_help():
    """--worker-index is the supervisor's child-side flag, not an operator's."""
    parser = ox_worker.Command().create_parser("manage.py", "ox_worker")
    assert "worker-index" not in parser.format_help()


def test_worker_args_forward_djangos_global_flags():
    options = {
        "backend": "default",
        "queues": None,
        "concurrency": 1,
        "interval": 1.0,
        "lock_timeout": None,
        "verbosity": 1,
        "processes": 2,
        "skip_checks": True,
        "traceback": True,
        "no_color": True,
        "force_color": False,
    }
    args = ox_worker.worker_args(options, "default")
    assert args[-3:] == ["--skip-checks", "--traceback", "--no-color"]
    assert "--force-color" not in args


def test_worker_args_forward_settings_and_pythonpath(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    options = {
        "backend": "default",
        "queues": None,
        "concurrency": 1,
        "interval": 1.0,
        "lock_timeout": None,
        "verbosity": 1,
        "processes": 2,
        "settings": "myproj.settings",
        "pythonpath": "src",
    }
    args = ox_worker.worker_args(options, "default")
    assert args[-4:] == [
        "--settings",
        "myproj.settings",
        "--pythonpath",
        str(tmp_path.resolve() / "src"),
    ]


class TestTheRecycleExitCodeIsPinned:
    """
    The exit code is the seam between the worker and the supervisor: the
    command calls `os._exit(RECYCLE_EXIT_CODE)` and the supervisor reads that
    number to tell a recycle from a crash. Nothing asserted the value, so a
    change on either side would silently become a crash loop.
    """

    def test_the_code_is_75(self):
        from django_ox.timeouts import RECYCLE_EXIT_CODE

        assert RECYCLE_EXIT_CODE == 75

    def test_the_command_exits_with_it_when_recycling(self):
        import inspect

        from django_ox.management.commands import ox_worker

        source = inspect.getsource(ox_worker)
        assert "os._exit(RECYCLE_EXIT_CODE)" in source, (
            "the command no longer forces the exit, so an abandoned thread "
            "blocks interpreter shutdown and the supervisor never gets its "
            "replacement"
        )

    def test_the_supervisor_reads_the_same_code(self):
        import inspect

        from django_ox import supervisor

        assert "RECYCLE_EXIT_CODE" in inspect.getsource(supervisor), (
            "the supervisor stopped recognising a recycle, so it reads one "
            "as a crash and spends its restart budget"
        )


@pytest.mark.parametrize("processes", [1, 4])
def test_unknown_backend_is_rejected_before_startup(settings, monkeypatch, processes):
    settings.TASKS = {
        alias: {"BACKEND": "django_ox.backend.OxBackend"}
        for alias in ["zebra", "default", "emails"]
    }

    def boom(*args, **kwargs):
        raise AssertionError("worker or supervisor reached")

    monkeypatch.setattr(ox_worker, "worker_class", boom)
    monkeypatch.setattr(ox_worker, "Supervisor", boom)
    with pytest.raises(CommandError) as excinfo:
        call_command("ox_worker", backend="missing", processes=processes, verbosity=0)
    assert str(excinfo.value) == (
        "No task backend alias 'missing' in TASKS. "
        "Known aliases: default, emails, zebra."
    )


def test_unknown_backend_names_no_aliases_when_tasks_is_empty(settings, monkeypatch):
    # TASKS = {} passes Django's own checks, so this message is the only thing
    # the operator hears. Without a word for the empty list it ends on a bare
    # colon. DATABASES cannot reach this state; Django injects a default alias.
    settings.TASKS = {}

    def boom(*args, **kwargs):
        raise AssertionError("worker or supervisor reached")

    monkeypatch.setattr(ox_worker, "worker_class", boom)
    monkeypatch.setattr(ox_worker, "Supervisor", boom)
    with pytest.raises(CommandError) as excinfo:
        call_command("ox_worker", verbosity=0)
    assert str(excinfo.value) == (
        "No task backend alias 'default' in TASKS. Known aliases: none."
    )


@pytest.mark.parametrize("backend", ["default", "emails"])
def test_backend_reaches_the_single_process_worker(settings, recorded_worker, backend):
    # The alias the parent validated is the one the worker runs. A single
    # process takes a different path to the worker than the supervisor does,
    # and only this pins it: a command that validated --backend and then ran
    # the default alias would be the silent no-op the guard exists to stop.
    settings.TASKS = {
        alias: {"BACKEND": "django_ox.backend.OxBackend"}
        for alias in ["default", "emails"]
    }
    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", backend=backend, verbosity=0)
    assert excinfo.value.code == 0
    (worker,) = recorded_worker.instances
    assert worker.kwargs["backend_alias"] == backend


def test_worker_class_comes_from_the_named_alias(settings):
    # The other half of the alias wiring. The test above stubs worker_class
    # out, so it cannot see which alias the class was looked up under; only
    # WORKER_CLASS on a non-default alias can. A command that read it from
    # "default" would hand the operator the stock worker while the logs
    # still named theirs.
    settings.TASKS = {
        "default": {"BACKEND": "django_ox.backend.OxBackend"},
        "emails": {
            "BACKEND": "django_ox.backend.OxBackend",
            "OPTIONS": {"WORKER_CLASS": "tests.test_command.StoppedWorker"},
        },
    }
    StoppedWorker.started = False

    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", backend="emails", verbosity=0)

    assert excinfo.value.code == 0
    assert StoppedWorker.started is True


def test_backend_defaults_to_the_framework_alias(settings, recorded_worker):
    # With no --backend the command runs the framework's default alias.
    # That alias is the string "default", so this cannot tell a flag that
    # flowed through from one that was ignored; the [emails] case above is
    # what pins that. What it does hold is the no-flag path, which deleting
    # TASKS could not: that reads whatever the handler had already cached.
    settings.TASKS = {
        alias: {"BACKEND": "django_ox.backend.OxBackend"}
        for alias in [DEFAULT_TASK_BACKEND_ALIAS, "emails"]
    }
    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", verbosity=0)
    assert excinfo.value.code == 0
    (worker,) = recorded_worker.instances
    assert worker.kwargs["backend_alias"] == DEFAULT_TASK_BACKEND_ALIAS


@pytest.mark.skipif(os.name != "posix", reason="supervisor requires POSIX signals")
@pytest.mark.parametrize("backend", ["default", "emails"])
def test_valid_backend_reaches_supervisor(settings, monkeypatch, backend):
    settings.TASKS = {
        alias: {"BACKEND": "django_ox.backend.OxBackend"}
        for alias in ["default", "emails"]
    }
    calls = []

    class SupervisorRecorder:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def handle_signal(self, *args):
            pass

        def run(self):
            return 0

    monkeypatch.setattr(ox_worker, "Supervisor", SupervisorRecorder)
    monkeypatch.setattr(ox_worker.signal, "signal", lambda *args: None)
    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", backend=backend, processes=4, verbosity=0)
    assert excinfo.value.code == 0
    (call,) = calls
    assert call["processes"] == 4
    assert call["worker_args"][:2] == ["--backend", backend]


@pytest.mark.parametrize("processes", [1, 4])
@pytest.mark.parametrize("traceback", [False, True])
def test_unknown_backend_cli(tmp_path, processes, traceback):
    (tmp_path / "backend_settings.py").write_text(
        "SECRET_KEY = 'test-only'\n"
        "INSTALLED_APPS = ['django_ox']\n"
        "DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3'}}\n"
        "TASKS = {alias: {'BACKEND': 'django_ox.backend.OxBackend'} "
        "for alias in ['zebra', 'default', 'emails']}\n"
    )
    command = [
        str(Path(sysconfig.get_path("scripts")) / "django-admin"),
        "ox_worker",
        "--settings=backend_settings",
        f"--pythonpath={tmp_path}",
        "--backend=missing",
        f"--processes={processes}",
        "--no-color",
    ]
    if traceback:
        command.append("--traceback")
    result = subprocess.run(  # noqa: S603
        command,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env={
            key: value
            for key, value in os.environ.items()
            if key != "DJANGO_SETTINGS_MODULE"
        },
    )
    message = (
        "CommandError: No task backend alias 'missing' in TASKS. "
        "Known aliases: default, emails, zebra.\n"
    )
    assert result.returncode == 1
    assert result.stdout == ""
    if traceback:
        assert "Traceback (most recent call last):" in result.stderr
        assert result.stderr.endswith(message)
    else:
        assert result.stderr == message


def test_a_fixed_signature_worker_class_runs_without_the_new_flags(settings):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "OPTIONS": {"WORKER_CLASS": "tests.test_command.FixedSignatureWorker"},
        }
    }
    FixedSignatureWorker.started = False

    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker")

    assert excinfo.value.code == 0
    assert FixedSignatureWorker.started is True


def test_batch_and_max_tasks_reach_the_worker(recorded_worker):
    with pytest.raises(SystemExit) as excinfo:
        call_command("ox_worker", "--batch", "--max-tasks=3")
    assert excinfo.value.code == 0
    (worker,) = recorded_worker.instances
    assert worker.kwargs["batch"] is True
    assert worker.kwargs["max_tasks"] == 3


def test_neither_is_passed_unless_asked_for(recorded_worker):
    with pytest.raises(SystemExit):
        call_command("ox_worker", verbosity=0)
    (worker,) = recorded_worker.instances
    assert "batch" not in worker.kwargs
    assert "max_tasks" not in worker.kwargs
