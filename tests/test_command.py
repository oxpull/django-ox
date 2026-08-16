import pytest
from django.core.management import call_command

from django_ox.management.commands import ox_worker


class WorkerRecorder:
    """Stands in for Worker to verify CLI flag wiring without a run loop."""

    instances: list["WorkerRecorder"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.stopping = False
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
    }


def test_command_defaults(recorded_worker):
    with pytest.raises(SystemExit):
        call_command("ox_worker", verbosity=0)
    (worker,) = recorded_worker.instances
    assert worker.kwargs["queues"] is None
    assert worker.kwargs["concurrency"] == 1
    assert worker.kwargs["lock_timeout"] is None
