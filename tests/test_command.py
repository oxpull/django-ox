import pytest
from django.core.management import CommandError, call_command

from django_ox.management.commands import ox_worker


class WorkerRecorder:
    """Stands in for Worker to verify CLI flag wiring without a run loop."""

    instances: list["WorkerRecorder"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.stopping = False
        self.recycling = False
        WorkerRecorder.instances.append(self)

    def run(self):
        pass


@pytest.fixture
def recorded_worker(monkeypatch):
    WorkerRecorder.instances = []
    monkeypatch.setattr(ox_worker, "Worker", WorkerRecorder)
    return WorkerRecorder


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
    assert ox_worker.worker_args(options) == [
        "--backend",
        "default",
        "--concurrency",
        "4",
        "--interval",
        "0.5",
        "--verbosity",
        "1",
        "--queues",
        "emails,default",
        "--lock-timeout",
        "60.0",
    ]


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
    args = ox_worker.worker_args(options)
    assert args[-4:] == [
        "--settings",
        "myproj.settings",
        "--pythonpath",
        str(tmp_path.resolve() / "src"),
    ]
