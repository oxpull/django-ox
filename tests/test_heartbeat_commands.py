"""
The heartbeat flags on the commands: ox_health --heartbeat-file as a
database-free branch of its own, ox_worker --heartbeat-file as far as the
worker class and the supervisor's children, and where the loop and the
supervisor write.

The file names are spelled out here rather than taken from
django_ox.heartbeat, because they are the interface an operator's probe
depends on and a test that asked the code for them could not see them
change.
"""

import itertools
import json
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from django.core.checks.registry import CheckRegistry
from django.core.management import CommandError, call_command, execute_from_command_line
from django.core.management.base import BaseCommand
from django.db import OperationalError, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorWrapper

from django_ox import stats
from django_ox.backend import OxBackend
from django_ox.compat import task_backends
from django_ox.management._database import DatabaseCommand
from django_ox.management.commands import ox_worker
from django_ox.models import OxTask
from django_ox.supervisor import Supervisor
from django_ox.worker import Worker

from .conftest import start_worker_thread, wait_for
from .tasks import add, slow

REPO = Path(__file__).resolve().parent.parent

posix_only = pytest.mark.skipif(os.name != "posix", reason="POSIX file semantics")


def set_mtime(path: Path, seconds: float) -> None:
    ns = int(seconds * 1_000_000_000)
    os.utime(path, ns=(ns, ns), follow_symlinks=False)


def expected(base: Path, processes: int) -> list[str]:
    if processes == 1:
        return [str(base)]
    return [f"{base}.supervisor", *(f"{base}.{i}" for i in range(processes))]


# -- ox_health --heartbeat-file -----------------------------------------------


def fresh(path: Path) -> Path:
    path.write_bytes(b"")
    return path


def health(*args: str, capsys) -> tuple[int, str, str]:
    """ox_health through the real command-line entry point."""
    try:
        execute_from_command_line(["manage.py", "ox_health", *args])
    except SystemExit as exc:
        code = exc.code
    else:
        code = 0
    out, err = capsys.readouterr()
    return code, out, err


class TestHealthOptions:
    @pytest.mark.parametrize(
        "flag",
        [
            ["--database", "default"],
            ["--queue", "default"],
            ["--max-backlog", "5"],
            ["--max-age", "60"],
            ["--worker-timeout", "60"],
        ],
    )
    def test_database_options_are_refused_with_a_file(self, tmp_path, flag, capsys):
        code, out, err = health(
            "--heartbeat-file", str(fresh(tmp_path / "hb")), *flag, capsys=capsys
        )
        assert code == 1
        assert out == ""
        assert err == (
            "CommandError: --heartbeat-file checks files, not the database, "
            f"so it cannot be combined with {flag[0]}; run those checks as a "
            "separate ox_health.\n"
        )

    def test_every_database_option_given_is_named(self, tmp_path, capsys):
        code, _, err = health(
            "--heartbeat-file",
            str(fresh(tmp_path / "hb")),
            "--queue",
            "default",
            "--worker-timeout",
            "60",
            capsys=capsys,
        )
        assert code == 1
        assert "cannot be combined with --queue, --worker-timeout;" in err

    @pytest.mark.parametrize(
        "flag", [["--max-heartbeat-age", "30"], ["--processes", "2"]]
    )
    def test_heartbeat_options_need_the_file(self, flag, capsys):
        code, out, err = health(*flag, capsys=capsys)
        assert code == 1
        assert out == ""
        assert err == f"CommandError: {flag[0]} needs --heartbeat-file.\n"

    @pytest.mark.parametrize("value", ["0", "-1", "0.0"])
    def test_the_age_must_be_positive(self, tmp_path, value, capsys):
        code, _, err = health(
            "--heartbeat-file",
            str(fresh(tmp_path / "hb")),
            f"--max-heartbeat-age={value}",
            capsys=capsys,
        )
        assert code == 1
        assert err == (
            "CommandError: --max-heartbeat-age must be a positive number of seconds.\n"
        )

    @pytest.mark.parametrize("value", ["nan", "inf", "-inf", "soon"])
    def test_the_age_must_be_a_finite_duration(self, tmp_path, value, capsys):
        code, _, err = health(
            "--heartbeat-file",
            str(fresh(tmp_path / "hb")),
            f"--max-heartbeat-age={value}",
            capsys=capsys,
        )
        assert code == 2
        assert "invalid duration" in err

    @pytest.mark.parametrize("value", ["0", "-2"])
    def test_processes_must_be_at_least_one(self, tmp_path, value, capsys):
        code, _, err = health(
            "--heartbeat-file",
            str(fresh(tmp_path / "hb")),
            f"--processes={value}",
            capsys=capsys,
        )
        assert code == 1
        assert err == "CommandError: --processes must be at least 1.\n"

    def test_an_empty_path_is_refused(self, capsys):
        code, _, err = health("--heartbeat-file=", capsys=capsys)
        assert code == 1
        assert err == "CommandError: --heartbeat-file needs a path.\n"

    def test_an_invalid_option_still_prints_the_json_object(self, tmp_path, capsys):
        path = str(fresh(tmp_path / "hb"))
        code, out, _ = health(
            "--format", "json", "--heartbeat-file", path, "--queue", "x", capsys=capsys
        )
        assert code == 1
        report = json.loads(out)
        assert report["ok"] is False
        assert report["heartbeat_file"] == path
        assert report["files"] is None
        assert report["problems"][0].startswith("--heartbeat-file checks files")

    def test_age_accepts_the_duration_forms(self, tmp_path, capsys):
        path = fresh(tmp_path / "hb")
        set_mtime(path, time.time() - 90)
        assert (
            health(
                "--heartbeat-file",
                str(path),
                "--max-heartbeat-age",
                "2m",
                capsys=capsys,
            )[0]
            == 0
        )
        assert (
            health(
                "--heartbeat-file",
                str(path),
                "--max-heartbeat-age",
                "1m",
                capsys=capsys,
            )[0]
            == 1
        )


class TestHealthResults:
    def test_fresh_text(self, tmp_path, capsys):
        path = fresh(tmp_path / "hb")
        code, out, err = health("--heartbeat-file", str(path), capsys=capsys)
        assert (code, err) == (0, "")
        assert out.startswith("OK: heartbeat_files=1 oldest_heartbeat_age=")
        assert out.endswith("s max_heartbeat_age=60s\n")

    def test_the_default_age_is_sixty_seconds(self, tmp_path, capsys):
        path = fresh(tmp_path / "hb")
        set_mtime(path, time.time() - 55)
        assert health("--heartbeat-file", str(path), capsys=capsys)[0] == 0
        set_mtime(path, time.time() - 65)
        code, _, err = health("--heartbeat-file", str(path), capsys=capsys)
        assert code == 1
        assert "over --max-heartbeat-age 60s" in err

    def test_stale_text_is_one_line_naming_every_failure(self, tmp_path, capsys):
        base = tmp_path / "hb"
        fresh(Path(f"{base}.supervisor"))
        set_mtime(fresh(Path(f"{base}.0")), time.time() - 600)
        code, out, err = health(
            "--heartbeat-file", str(base), "--processes", "2", capsys=capsys
        )
        assert code == 1
        assert out == ""
        assert err.count("\n") == 1
        assert err.startswith(f"CommandError: heartbeat file {base}.0 is ")
        assert err.endswith(f"; heartbeat file {base}.1 is missing\n")

    def test_json_fresh(self, tmp_path, capsys):
        base = tmp_path / "hb"
        for name in expected(base, 2):
            fresh(Path(name))
        code, out, _ = health(
            "--heartbeat-file",
            str(base),
            "--processes",
            "2",
            "--max-heartbeat-age",
            "30",
            "--format",
            "json",
            capsys=capsys,
        )
        assert code == 0
        report = json.loads(out)
        assert report["ok"] is True
        assert report["heartbeat_file"] == str(base)
        assert report["processes"] == 2
        assert report["max_heartbeat_age_seconds"] == 30.0
        assert report["problems"] == []
        assert [f["path"] for f in report["files"]] == expected(base, 2)
        assert all(f["ok"] and f["problem"] is None for f in report["files"])
        assert all(0 <= f["age_seconds"] < 30 for f in report["files"])

    def test_json_failure_prints_the_object_and_exits_non_zero(self, tmp_path, capsys):
        code, out, err = health(
            "--heartbeat-file", str(tmp_path / "hb"), "--format", "json", capsys=capsys
        )
        assert code == 1
        report = json.loads(out)
        assert report == {
            "ok": False,
            "heartbeat_file": str(tmp_path / "hb"),
            "processes": 1,
            "max_heartbeat_age_seconds": 60.0,
            "files": [
                {
                    "path": str(tmp_path / "hb"),
                    "ok": False,
                    "age_seconds": None,
                    "problem": f"heartbeat file {tmp_path / 'hb'} is missing",
                }
            ],
            "problems": [f"heartbeat file {tmp_path / 'hb'} is missing"],
        }
        assert err == f"CommandError: heartbeat file {tmp_path / 'hb'} is missing\n"

    def test_call_command_reaches_the_same_branch(self, tmp_path):
        path = fresh(tmp_path / "hb")
        call_command("ox_health", heartbeat_file=str(path))
        path.unlink()
        with pytest.raises(CommandError, match="is missing"):
            call_command("ox_health", heartbeat_file=str(path))


class SentinelHit(BaseException):
    """
    A BaseException, so no ``except Exception`` or ``except DatabaseError``
    on the way out can swallow the evidence that the database was reached.
    """


@pytest.fixture
def no_database(monkeypatch):
    """
    Make every road to the database fail loudly, and record who took it.

    Connection acquisition, cursor creation and statement execution, the
    system checks and the migration check, the stats queries, and task
    backend construction. The file mode must touch none of them; the
    database mode, run under the same fixture, must hit at least one, or
    the sentinels prove nothing.
    """
    hits: list[str] = []

    def sentinel(name):
        def hit(*args, **kwargs):
            hits.append(name)
            raise SentinelHit(name)

        return hit

    targets = [
        (BaseDatabaseWrapper, "connect"),
        (BaseDatabaseWrapper, "ensure_connection"),
        (BaseDatabaseWrapper, "cursor"),
        (CursorWrapper, "execute"),
        (CursorWrapper, "executemany"),
        (CursorWrapper, "callproc"),
        (CheckRegistry, "run_checks"),
        (BaseCommand, "check"),
        (BaseCommand, "check_migrations"),
        (DatabaseCommand, "check"),
        (DatabaseCommand, "get_check_kwargs"),
        (OxBackend, "__init__"),
        (type(task_backends), "__getitem__"),
    ]
    for alias in connections:
        targets.append((type(connections[alias]), "get_new_connection"))
    for cls, attr in targets:
        monkeypatch.setattr(cls, attr, sentinel(f"{cls.__qualname__}.{attr}"))
    for name in ("ready_count", "oldest_ready_age", "last_claim_age"):
        monkeypatch.setattr(stats, name, sentinel(f"stats.{name}"))
    return hits


class TestTheFileModeNeverReachesTheDatabase:
    def test_the_sentinels_are_live(self, no_database, capsys):
        # The control. Without it a sentinel on the wrong method would pass
        # every test below.
        with pytest.raises(SentinelHit):
            execute_from_command_line(["manage.py", "ox_health"])
        assert no_database

    def test_fresh(self, tmp_path, no_database, capsys):
        path = fresh(tmp_path / "hb")
        assert health("--heartbeat-file", str(path), capsys=capsys)[0] == 0
        assert no_database == []

    def test_fresh_json_with_processes(self, tmp_path, no_database, capsys):
        base = tmp_path / "hb"
        for name in expected(base, 3):
            fresh(Path(name))
        code, out, _ = health(
            "--heartbeat-file",
            str(base),
            "--processes",
            "3",
            "--format",
            "json",
            capsys=capsys,
        )
        assert code == 0
        assert json.loads(out)["ok"] is True
        assert no_database == []

    def test_stale(self, tmp_path, no_database, capsys):
        path = fresh(tmp_path / "hb")
        set_mtime(path, time.time() - 3600)
        assert health("--heartbeat-file", str(path), capsys=capsys)[0] == 1
        assert no_database == []

    def test_missing(self, tmp_path, no_database, capsys):
        code, _, err = health("--heartbeat-file", str(tmp_path / "hb"), capsys=capsys)
        assert code == 1
        assert "is missing" in err
        assert no_database == []

    def test_a_directory(self, tmp_path, no_database, capsys):
        (tmp_path / "hb").mkdir()
        code, _, err = health("--heartbeat-file", str(tmp_path / "hb"), capsys=capsys)
        assert code == 1
        assert "is a directory, not a regular file" in err
        assert no_database == []

    @posix_only
    def test_a_symlink(self, tmp_path, no_database, capsys):
        fresh(tmp_path / "real")
        (tmp_path / "hb").symlink_to(tmp_path / "real")
        code, _, err = health("--heartbeat-file", str(tmp_path / "hb"), capsys=capsys)
        assert code == 1
        assert "is a symlink, not a regular file" in err
        assert no_database == []

    def test_an_invalid_combination(self, tmp_path, no_database, capsys):
        code, _, _ = health(
            "--heartbeat-file",
            str(fresh(tmp_path / "hb")),
            "--queue",
            "x",
            capsys=capsys,
        )
        assert code == 1
        assert no_database == []


def unreachable_settings(tmp_path: Path, engine: str, **extra) -> str:
    """
    The suite's settings, the admin, auth and sessions apps included, with a
    default database that cannot be reached, so that anything in the probe or
    in the startup it goes through that tries to reach it fails where it can
    be seen.
    """
    database = {"ENGINE": f"django.db.backends.{engine}", **extra}
    (tmp_path / "unreachable_settings.py").write_text(
        "from tests.settings import *  # noqa: F403\n"
        f"DATABASES = {{'default': {database!r}}}\n"
    )
    return "unreachable_settings"


def probe(tmp_path: Path, module: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = module
    env["PYTHONPATH"] = os.pathsep.join(
        [str(tmp_path), str(REPO / "src"), str(REPO), env.get("PYTHONPATH", "")]
    )
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "django", "ox_health", *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


class TestInAProcessWithNoDatabase:
    """The same guarantee from outside: a real interpreter, a real argv."""

    def test_sqlite_in_a_directory_that_does_not_exist(self, tmp_path):
        module = unreachable_settings(
            tmp_path, "sqlite3", NAME=str(tmp_path / "no" / "such" / "db.sqlite3")
        )
        control = probe(tmp_path, module)
        assert control.returncode == 1
        assert "Database unreachable" in control.stderr

        path = fresh(tmp_path / "hb")
        result = probe(tmp_path, module, "--heartbeat-file", str(path))
        assert (result.returncode, result.stderr) == (0, "")
        assert result.stdout.startswith("OK: heartbeat_files=1 ")

        set_mtime(path, time.time() - 3600)
        result = probe(tmp_path, module, "--heartbeat-file", str(path))
        assert result.returncode == 1
        assert result.stderr.startswith(f"CommandError: heartbeat file {path} is ")
        assert "Database" not in result.stderr

    def test_a_postgresql_server_that_accepts_and_never_answers(self, tmp_path):
        """
        The hung database the file mode exists for: the TCP connection is
        accepted and nothing ever comes back. The control needs a connect
        timeout to finish at all; the file mode needs nothing.
        """
        pytest.importorskip("psycopg")
        import socket

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(16)
        try:
            module = unreachable_settings(
                tmp_path,
                "postgresql",
                NAME="nowhere",
                USER="nobody",
                HOST="127.0.0.1",
                PORT=str(listener.getsockname()[1]),
                OPTIONS={"connect_timeout": 2},
            )
            control = probe(tmp_path, module)
            assert control.returncode == 1
            assert "Database unreachable" in control.stderr

            path = fresh(tmp_path / "hb")
            result = probe(
                tmp_path, module, "--heartbeat-file", str(path), "--format", "json"
            )
            assert result.returncode == 0, result.stderr
            assert json.loads(result.stdout)["ok"] is True
        finally:
            listener.close()


# -- ox_worker wiring -----------------------------------------------------------


class Recorder:
    instances: list["Recorder"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.stopping = False
        self.recycling = False
        # What Worker.__init__ does with it, which ox_worker checks.
        self.heartbeat_file = kwargs.get("heartbeat_file")
        Recorder.instances.append(self)

    def run(self):
        pass


@pytest.fixture
def recorded(monkeypatch):
    Recorder.instances = []
    monkeypatch.setattr(ox_worker, "worker_class", lambda alias: Recorder)
    monkeypatch.setattr(ox_worker, "_die_with_parent", lambda: None)
    return Recorder


class TestTheWorkerFlag:
    def test_passed_to_the_worker_class_only_when_set(self, recorded):
        with pytest.raises(SystemExit):
            call_command("ox_worker", verbosity=0)
        with pytest.raises(SystemExit):
            call_command("ox_worker", "--heartbeat-file=/run/ox/hb", verbosity=0)
        without, with_ = recorded.instances
        assert "heartbeat_file" not in without.kwargs
        assert with_.kwargs["heartbeat_file"] == "/run/ox/hb"

    def test_a_supervised_child_writes_its_slot_file(self, recorded):
        with pytest.raises(SystemExit):
            call_command(
                "ox_worker",
                "--heartbeat-file=/run/ox/hb",
                "--worker-index=3",
                verbosity=0,
            )
        (worker,) = recorded.instances
        assert worker.kwargs["heartbeat_file"] == "/run/ox/hb.3"

    def test_an_empty_path_is_refused(self, recorded):
        with pytest.raises(CommandError, match="--heartbeat-file needs a path"):
            call_command("ox_worker", "--heartbeat-file=", verbosity=0)
        assert recorded.instances == []

    def test_forwarded_to_children_only_when_set(self):
        options = {
            "backend": "default",
            "queues": None,
            "concurrency": 1,
            "interval": 1.0,
            "lock_timeout": None,
            "verbosity": 1,
            "processes": 2,
        }
        assert not any(
            arg.startswith("--heartbeat-file")
            for arg in ox_worker.worker_args(options, "default")
        )
        args = ox_worker.worker_args(
            {**options, "heartbeat_file": "/run/ox/hb", "skip_checks": True},
            "default",
        )
        assert args[-2:] == ["--heartbeat-file=/run/ox/hb", "--skip-checks"]

    @pytest.mark.parametrize("path", ["-hb", "--hb", "-", "run/-hb"])
    def test_a_forwarded_value_that_starts_with_a_dash_reaches_the_child(self, path):
        """
        The child parses what the supervisor forwards. As a separate token,
        a value that starts with a dash reads as an option of its own, and
        every child exits with a usage error before its loop starts.
        """
        options = {
            "backend": "default",
            "queues": "-x,default",
            "concurrency": 2,
            "interval": 0.5,
            "lock_timeout": 60.0,
            "verbosity": 1,
            "processes": 2,
            "heartbeat_file": path,
        }
        parser = ox_worker.Command().create_parser("manage.py", "ox_worker")
        parsed = parser.parse_args(ox_worker.worker_args(options, "default"))
        assert parsed.heartbeat_file == path
        assert parsed.queues == "-x,default"
        assert parsed.lock_timeout == 60.0
        assert parsed.concurrency == 2

    @pytest.mark.skipif(os.name != "posix", reason="supervisor requires POSIX")
    def test_the_supervisor_gets_the_base_path(self, monkeypatch):
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
        with pytest.raises(SystemExit):
            call_command(
                "ox_worker", "--processes=2", "--heartbeat-file=/run/ox/hb", verbosity=0
            )
        (call,) = calls
        assert call["heartbeat_file"] == "/run/ox/hb"
        assert "--heartbeat-file=/run/ox/hb" in call["worker_args"]

    def test_a_fixed_signature_child_runs_without_the_flag(self, settings, monkeypatch):
        # The supervised path of the fixed-signature guarantee: a child
        # started with --worker-index and no heartbeat flag passes nothing
        # new to a WORKER_CLASS written before this flag existed.
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "OPTIONS": {"WORKER_CLASS": "tests.test_command.FixedSignatureWorker"},
            }
        }
        from .test_command import FixedSignatureWorker

        FixedSignatureWorker.started = False
        monkeypatch.setattr(ox_worker, "_die_with_parent", lambda: None)
        with pytest.raises(SystemExit) as excinfo:
            call_command("ox_worker", "--worker-index=0", verbosity=0)
        assert excinfo.value.code == 0
        assert FixedSignatureWorker.started is True


def use_worker_class(settings, path: str) -> None:
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "OPTIONS": {"WORKER_CLASS": path},
        }
    }


class DroppingWorker(Worker):
    """Takes any keyword argument, and passes heartbeat_file on to nobody."""

    started = False

    def __init__(self, **kwargs):
        kwargs.pop("heartbeat_file", None)
        Worker.__init__(self, **kwargs)

    def run(self):
        DroppingWorker.started = True


class TestAWorkerClassThatCannotWriteTheFile:
    """
    --heartbeat-file with a WORKER_CLASS that cannot pass heartbeat_file on
    to Worker.__init__: one sentence naming the class and the keyword, not a
    TypeError traceback, and not a worker that runs and writes nothing.
    """

    FIXED = "tests.test_command.FixedSignatureWorker"
    REFUSED = (
        "WORKER_CLASS tests.test_command.FixedSignatureWorker on backend "
        "'default' does not accept the keyword argument heartbeat_file, which "
        "--heartbeat-file passes to it. Accept it and pass it on to "
        "Worker.__init__(), or run without --heartbeat-file."
    )

    @pytest.fixture(autouse=True)
    def no_parent_death_signal(self, monkeypatch):
        monkeypatch.setattr(ox_worker, "_die_with_parent", lambda: None)

    @pytest.mark.parametrize("extra", [[], ["--worker-index=0"]])
    def test_a_fixed_signature_is_refused_in_a_sentence(self, settings, extra):
        from .test_command import FixedSignatureWorker

        use_worker_class(settings, self.FIXED)
        FixedSignatureWorker.started = False
        with pytest.raises(CommandError) as caught:
            call_command(
                "ox_worker", "--heartbeat-file=/run/ox/hb", *extra, verbosity=0
            )
        assert str(caught.value) == self.REFUSED
        assert FixedSignatureWorker.started is False

        # And without the flag it runs as it always did.
        with pytest.raises(SystemExit) as excinfo:
            call_command("ox_worker", *extra, verbosity=0)
        assert excinfo.value.code == 0
        assert FixedSignatureWorker.started is True

    @pytest.mark.skipif(os.name != "posix", reason="supervisor requires POSIX")
    def test_the_supervisor_refuses_it_before_starting_a_child(
        self, settings, monkeypatch
    ):
        started = []

        class SupervisorRecorder:
            def __init__(self, **kwargs):
                started.append(kwargs)

            def handle_signal(self, *args):
                pass

            def run(self):
                return 0

        use_worker_class(settings, self.FIXED)
        monkeypatch.setattr(ox_worker, "Supervisor", SupervisorRecorder)
        monkeypatch.setattr(ox_worker.signal, "signal", lambda *args: None)
        with pytest.raises(CommandError) as caught:
            call_command(
                "ox_worker", "--processes=2", "--heartbeat-file=/run/ox/hb", verbosity=0
            )
        assert str(caught.value) == self.REFUSED
        assert started == []

        with pytest.raises(SystemExit):
            call_command("ox_worker", "--processes=2", verbosity=0)
        assert len(started) == 1

    def test_one_that_takes_it_and_drops_it_is_refused_in_a_sentence(self, settings):
        use_worker_class(settings, "tests.test_heartbeat_commands.DroppingWorker")
        DroppingWorker.started = False
        with pytest.raises(CommandError) as caught:
            call_command("ox_worker", "--heartbeat-file=/run/ox/hb", verbosity=0)
        assert str(caught.value) == (
            "WORKER_CLASS tests.test_heartbeat_commands.DroppingWorker on backend "
            "'default' accepted heartbeat_file but did not pass it on to "
            "Worker.__init__(), so no heartbeat file would be written. Pass it "
            "on, or run without --heartbeat-file."
        )
        assert DroppingWorker.started is False


# -- the loop -------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
class TestTheLoopBeats:
    def test_before_the_database_on_every_pass_including_failed_ones(
        self, tmp_path, monkeypatch
    ):
        worker = Worker(poll_interval=0.01, heartbeat_file=str(tmp_path / "hb"))
        events: list[str] = []
        threads: set[int] = set()
        real_touch = worker._heartbeat.touch

        def touch():
            threads.add(threading.get_ident())
            events.append("beat")
            return real_touch()

        def claim_one():
            events.append("claim")
            raise OperationalError("connection refused")

        monkeypatch.setattr(worker._heartbeat, "touch", touch)
        monkeypatch.setattr(worker, "claim_one", claim_one)
        monkeypatch.setattr(worker, "reap", lambda: 0)
        thread = start_worker_thread(worker)
        try:
            assert wait_for(lambda: events.count("claim") >= 20, timeout=20)
        finally:
            worker.request_stop()
            thread.join(timeout=20)

        claims = [i for i, event in enumerate(events) if event == "claim"]
        assert events[0] == "beat"
        # Exactly one beat before each claim: the head of the pass, and
        # nothing between a failure and the next pass's head.
        for previous, current in itertools.pairwise(claims):
            assert events[previous + 1 : current] == ["beat", "beat"]
        assert threads == {thread.ident}
        assert time.time() - (tmp_path / "hb").stat().st_mtime < 60

    def test_every_claim_of_a_busy_pass_beats_first(self, tmp_path, monkeypatch):
        """
        A pass with a backlog claims until every slot is full, one round
        trip or more per claim, so on a database that answers slowly the
        pass lasts --concurrency claims. A beat before each claim keeps the
        file's age to one claim rather than one pass.
        """
        concurrency = 6
        delay = 0.3
        path = tmp_path / "hb"
        worker = Worker(
            concurrency=concurrency, poll_interval=0.05, heartbeat_file=str(path)
        )
        events: list[str] = []
        ages: list[float] = []
        threads: set[int] = set()
        real_touch = worker._heartbeat.touch
        real_claim = worker.claim_one

        def touch():
            threads.add(threading.get_ident())
            events.append("beat")
            return real_touch()

        def claim_one():
            events.append("claim")
            ages.append(time.time() - path.stat().st_mtime)
            # The database answers, slowly.
            time.sleep(delay)
            return real_claim()

        monkeypatch.setattr(worker._heartbeat, "touch", touch)
        monkeypatch.setattr(worker, "claim_one", claim_one)
        results = [add.enqueue(i, i) for i in range(concurrency)]
        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: all(
                    OxTask.objects.get(id=r.id).status == OxTask.Status.SUCCESSFUL
                    for r in results
                ),
                timeout=60,
            )
        finally:
            worker.request_stop()
            thread.join(timeout=30)
        assert not thread.is_alive()

        claims = [i for i, event in enumerate(events) if event == "claim"]
        assert len(claims) >= concurrency
        assert all(events[i - 1] == "beat" for i in claims)
        # One pass of six claims ages the file by five of them, 1.5 s, when
        # only its head beats; one beat per claim leaves it microseconds old.
        assert max(ages) < delay * (concurrency - 1) / 2, ages
        assert threads == {thread.ident}

    def test_an_orphan_does_not_write_its_slot_file(self, tmp_path):
        """
        A worker whose supervisor has gone no longer speaks for the slot: a
        supervisor started in its place expects that file from its own
        child, and the orphan's drain must not keep it fresh.
        """
        path = tmp_path / "hb.0"
        orphan = Worker(parent_pid=os.getppid() + 1, heartbeat_file=str(path))
        orphan._beat()
        assert not path.exists()

        supervised = Worker(parent_pid=os.getppid(), heartbeat_file=str(path))
        supervised._beat()
        assert path.exists()

    def test_every_drain_pass_beats(self, tmp_path, monkeypatch):
        worker = Worker(poll_interval=0.05, heartbeat_file=str(tmp_path / "hb"))
        beats_while_stopping: list[float] = []
        threads: set[int] = set()
        real_touch = worker._heartbeat.touch

        def touch():
            threads.add(threading.get_ident())
            if worker.stopping:
                beats_while_stopping.append(time.monotonic())
            return real_touch()

        monkeypatch.setattr(worker._heartbeat, "touch", touch)
        result = slow.enqueue(1.5)
        thread = start_worker_thread(worker)
        try:
            assert wait_for(
                lambda: (
                    OxTask.objects.get(id=result.id).status == OxTask.Status.RUNNING
                ),
                timeout=20,
            )
            worker.request_stop()
            thread.join(timeout=30)
        finally:
            worker.request_stop()
        assert not thread.is_alive()
        assert OxTask.objects.get(id=result.id).status == OxTask.Status.SUCCESSFUL
        # The drain re-polls every 0.25 s for a task with more than a
        # second left; one beat per pass.
        assert len(beats_while_stopping) >= 3
        assert threads == {thread.ident}

    def test_no_file_no_writer(self):
        worker = Worker()
        assert worker.heartbeat_file is None
        assert worker._heartbeat is None


# -- the supervisor's bookkeeping ---------------------------------------------


class FakeProc:
    def __init__(self, code=None):
        self.code = code
        self.pid = 999_999

    def poll(self):
        return self.code

    def send_signal(self, signum):
        pass


class TestSupervisorFiles:
    def test_a_slot_file_is_removed_before_its_process_starts(
        self, tmp_path, monkeypatch
    ):
        base = str(tmp_path / "hb")
        leftover = Path(f"{base}.0")
        fresh(leftover)
        seen_at_spawn = []

        def popen(*args, **kwargs):
            seen_at_spawn.append(leftover.exists())
            return FakeProc()

        from django_ox import supervisor as supervisor_module

        monkeypatch.setattr(supervisor_module.subprocess, "Popen", popen)
        Supervisor(processes=1, worker_args=[], heartbeat_file=base)._start(0)
        assert seen_at_spawn == [False]

    def test_a_slot_file_is_removed_once_its_exit_is_seen(self, tmp_path):
        base = str(tmp_path / "hb")
        supervisor = Supervisor(processes=2, worker_args=[], heartbeat_file=base)
        for index in (0, 1):
            fresh(Path(f"{base}.{index}"))
        supervisor._children = {0: FakeProc(code=1), 1: FakeProc()}
        supervisor._started_at = {0: time.monotonic(), 1: time.monotonic()}
        supervisor.request_stop()

        supervisor._reap_exited()

        assert not Path(f"{base}.0").exists()
        assert Path(f"{base}.1").exists()

    def test_without_the_flag_no_file_is_touched(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        supervisor = Supervisor(processes=1, worker_args=[])
        supervisor._beat()
        supervisor._invalidate(0)
        assert list(tmp_path.iterdir()) == []

    def test_its_own_file_is_the_supervisor_file(self, tmp_path):
        base = str(tmp_path / "hb")
        Supervisor(processes=2, worker_args=[], heartbeat_file=base)._beat()
        assert sorted(p.name for p in tmp_path.iterdir()) == ["hb.supervisor"]

    def test_a_removal_that_fails_is_reported_once(self, tmp_path, caplog):
        base = str(tmp_path / "hb")
        # A non-empty directory at the slot's path cannot be unlinked.
        blocker = Path(f"{base}.0")
        blocker.mkdir()
        (blocker / "x").write_bytes(b"")
        caplog.set_level(logging.WARNING, logger="django_ox")
        supervisor = Supervisor(processes=1, worker_args=[], heartbeat_file=base)
        supervisor._invalidate(0)
        supervisor._invalidate(0)
        records = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "heartbeat_invalidate_failed"
        ]
        assert len(records) == 1
        assert records[0].worker_index == 0
        # A directory never counts as fresh, so the warning must not say it
        # does: the probe fails that slot for as long as it is there.
        message = records[0].getMessage()
        assert "counts as fresh" not in message
        assert message.endswith(
            "; it is not a regular file, so ox_health --heartbeat-file fails "
            "that slot until it is removed"
        )

    @posix_only
    def test_a_path_that_cannot_be_read_is_not_said_to_count(self, tmp_path, caplog):
        # A base path under a plain file: removing and reading the slot path
        # both fail with ENOTDIR, and so does the probe.
        (tmp_path / "file").write_bytes(b"")
        base = str(tmp_path / "file" / "hb")
        caplog.set_level(logging.WARNING, logger="django_ox")
        Supervisor(processes=1, worker_args=[], heartbeat_file=base)._invalidate(0)
        (record,) = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "heartbeat_invalidate_failed"
        ]
        assert record.getMessage().endswith(
            "; it cannot be read, so ox_health --heartbeat-file fails that slot"
        )

    @posix_only
    @pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root ignores the permission bits this test relies on",
    )
    def test_a_regular_file_that_cannot_be_removed_counts_until_it_ages_out(
        self, tmp_path, caplog
    ):
        directory = tmp_path / "run"
        directory.mkdir()
        base = str(directory / "hb")
        fresh(Path(f"{base}.0"))
        directory.chmod(0o500)
        caplog.set_level(logging.WARNING, logger="django_ox")
        try:
            Supervisor(processes=1, worker_args=[], heartbeat_file=base)._invalidate(0)
        finally:
            directory.chmod(0o700)
        (record,) = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "heartbeat_invalidate_failed"
        ]
        assert Path(f"{base}.0").exists()
        assert record.getMessage() == (
            f"Supervisor {os.getpid()} could not remove the heartbeat file "
            f"{base}.0 of worker process 0 ([Errno 13] Permission denied: "
            f"'{base}.0'); it counts as fresh until it ages out"
        )
