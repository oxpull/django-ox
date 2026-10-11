"""
The `oxpull.worker.OxpullWorker` under `ox_worker --heartbeat-file` and `ox_health
--heartbeat-file`, through the real commands, for the backend and worker
shipped together in django-ox.

django-ox owns the heartbeat. Worker.run() touches the file at the head of
every poll pass and every drain pass, on the thread that runs the loop. Under
--processes the supervisor touches PATH.supervisor from its own loop and
removes a slot's PATH.i when that slot starts or exits. oxpull changes none
of that, and these tests hold what oxpull puts around the loop:

  * OxpullWorker takes heartbeat_file through **kwargs and hands it on, so
    every `oxpull.worker.OxpullWorker` process, each supervised slot included,
    writes its own file. The flag reaches the processes that actually run.
  * OxpullWorker.run() checks the workflow schema before it enters the
    django-ox loop, so a workflow worker's first heartbeat comes after that
    check. When the database refuses connections the check gives up with a
    warning and the loop starts and turns anyway: a heartbeat says the loop
    is turning, not that the database answers.
  * The batch and workflow reconcilers are ordinary scheduled tasks. A batch
    and a workflow complete only through them, and here they complete on a
    fleet whose every heartbeat file stays fresh the whole time.
  * Nothing runs twice. Three worker processes race for one queue with the
    flag on, and every row is claimed once, by one worker.
  * `ox_health --heartbeat-file` reads file metadata only. With oxpull
    installed and the database refusing connections, it answers from the
    files, while the database mode in the same environment cannot answer.

A fresh file means one thing: the loops it belongs to have advanced
recently. Not that a task is making progress, and not that the database
answers.

Every wait ends on something observed (a file's modification time moving, a
row's status) and fails only at a deadline that a passing run never reaches.
Age is never waited out: a file whose writer is stopped is aged by setting its
modification time, which the stopped writer cannot move back.

Not covered here, and covered by django-ox's own suite: the exact age
boundaries, symlinks and other files that are not regular files, a loop wedged
in a statement, a stopped or killed child and its restart, and the drain.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection

from django_ox.models import OxTask
from oxpull import batches
from oxpull.models import OxpullBatch, OxpullWorkflow

from . import tasks
from .subprocesses import django_ox_path
from .test_workflows import diamond

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

# A safety bound on every wait, not an expected time.
DEADLINE = 120.0

# The suite's settings, run as an operator would run them, before each test
# adds its own part. "flows_ungated" is the suite's deliberate oxpull.E007
# fixture, which would stop ox_worker at start-up; its alias stays because
# tests_oxpull.tasks declares a task on it.
BASE_SETTINGS = (
    "import copy\n"
    "from {module} import *  # noqa: F403\n"
    "TASKS = copy.deepcopy(TASKS)  # noqa: F405\n"
    "TASKS['flows_ungated']['OPTIONS']['WORKFLOWS'] = False\n"
)

# Both OxpullBackend aliases correctly configured for the same fleet: the
# worker on "default" is an OxpullWorker too, because it serves the queue the
# workflow nodes are on. The reconcilers keep the schedule entries the suite
# gives them, run every second instead of every minute, so the batch and the
# workflow complete within seconds of their last member rather than at the next
# minute. DEBUG django_ox logs, whose "Claimed task" lines are the claim record.
FLEET_SETTINGS = (
    "TASKS['default']['OPTIONS']['WORKER_CLASS'] = 'oxpull.worker.OxpullWorker'\n"
    "for _name in ('oxpull-batch-reconcile', 'oxpull-workflow-reconcile'):\n"
    "    _schedule = TASKS['default']['OPTIONS']['SCHEDULES'][_name]\n"
    "    del _schedule['cron']\n"
    "    _schedule['every'] = 1\n"
    "LOGGING = {\n"
    "    'version': 1,\n"
    "    'disable_existing_loggers': False,\n"
    "    'handlers': {'console': {'class': 'logging.StreamHandler'}},\n"
    "    'loggers': {'django_ox': {'handlers': ['console'], 'level': 'DEBUG'}},\n"
    "}\n"
)

# Every alias on a database that refuses connections: a directory that does
# not exist for SQLite, a port nothing listens on for a server.
REFUSED_SETTINGS = (
    "DATABASES = copy.deepcopy(DATABASES)  # noqa: F405\n"
    "for _alias, _database in DATABASES.items():\n"
    "    if _database['ENGINE'].endswith('sqlite3'):\n"
    "        _database['NAME'] = {absent!r} + '/' + _alias + '.sqlite3'\n"
    "    else:\n"
    "        _database['HOST'] = '127.0.0.1'\n"
    "        _database['PORT'] = {port!r}\n"
)

CLAIMED = re.compile(r"Claimed task id=([0-9a-f-]{36}) ")
PASS_FAILED = "could not reach the database this pass"
SCHEMA_CHECK_FAILED = "Could not check the workflow schema before starting"


def process_env(tmp_path, extra):
    """
    A separate process on this session's test database, with the suite's
    settings module underneath the test's own part.
    """
    (tmp_path / "heartbeat_settings.py").write_text(
        BASE_SETTINGS.format(module=settings.SETTINGS_MODULE) + extra
    )
    name = connection.settings_dict["NAME"]
    if connection.vendor == "sqlite":
        name = Path(name).resolve()
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "heartbeat_settings",
        "PYTHONPATH": os.pathsep.join(
            [str(SRC), django_ox_path(), str(tmp_path), str(REPO_ROOT)]
        ),
        "OX_TEST_DB_NAME": str(name),
    }


def run_python(env, *args):
    return subprocess.run(  # noqa: S603
        [sys.executable, *args],
        env=env,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class WorkerProcess:
    """One `ox_worker` process and the file it logs to."""

    def __init__(self, env, log_path, *args):
        self.args = args
        self.log_path = log_path
        with log_path.open("w") as log:
            self.process = subprocess.Popen(  # noqa: S603
                [sys.executable, "-m", "django", "ox_worker", *args],
                env=env,
                cwd=REPO_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
            )

    @property
    def pid(self):
        return self.process.pid

    def log(self):
        return self.log_path.read_text()

    def assert_running(self):
        code = self.process.poll()
        if code is not None:
            pytest.fail(
                f"ox_worker {' '.join(self.args)} exited {code}:\n{self.log()[-3000:]}"
            )

    def stop(self):
        """SIGTERM, as a deploy does, after a SIGCONT in case a test stopped it."""
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.pid, signal.SIGCONT)
            self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        return self.process.returncode


def probe(env, path, *extra):
    """`ox_health --heartbeat-file`, as a container probe runs it."""
    result = run_python(
        env,
        "-m",
        "django",
        "ox_health",
        "--heartbeat-file",
        str(path),
        "--format",
        "json",
        *extra,
    )
    try:
        report = json.loads(result.stdout)
    except ValueError:
        report = None
    return result.returncode, report, result.stdout + result.stderr


def failing(report):
    return [entry["path"] for entry in report["files"] if not entry["ok"]]


def mtime(path):
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def watch(paths, times, *, still=(), running=()):
    """
    Sample the files until each of `paths` has shown `times` new modification
    times; a file appearing counts as one. Every path in `still` must keep the
    modification time it had when the watch began, and every process in
    `running` must still be running.
    """
    last = {path: mtime(path) for path in [*paths, *still]}
    seen = Counter()
    give_up = time.monotonic() + DEADLINE
    while any(seen[path] < times for path in paths):
        for worker in running:
            worker.assert_running()
        if time.monotonic() > give_up:
            pytest.fail(
                f"after {DEADLINE}s, updates seen: "
                f"{ {path.name: seen[path] for path in paths} }, wanted {times} each"
            )
        time.sleep(0.02)
        for path in paths:
            now = mtime(path)
            if now is not None and now != last[path]:
                seen[path] += 1
                last[path] = now
        for path in still:
            assert mtime(path) == last[path], (
                f"{path.name} moved while its writer was stopped"
            )


def wait_until(condition, what, *, running=()):
    give_up = time.monotonic() + DEADLINE
    while not condition():
        for worker in running:
            worker.assert_running()
        if time.monotonic() > give_up:
            pytest.fail(f"after {DEADLINE}s: {what}")
        time.sleep(0.1)


def age(path, seconds):
    """Put the file's modification time `seconds` in the past."""
    past = time.time() - seconds
    os.utime(path, (past, past))


def children_of(pid):
    found = subprocess.run(  # noqa: S603
        ["pgrep", "-P", str(pid)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    return sorted(int(line) for line in found.stdout.split())


def argv_of(pid):
    found = subprocess.run(  # noqa: S603
        ["ps", "-ww", "-o", "args=", "-p", str(pid)],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    return found.stdout.split()


def flag_value(argv, flag):
    # Read both "--flag value" and "--flag=value"; the supervisor passes each
    # child the second form.
    for i, arg in enumerate(argv):
        if arg == flag:
            return argv[i + 1] if i + 1 < len(argv) else None
        if arg.startswith(flag + "="):
            return arg[len(flag) + 1 :]
    return None


@pytest.mark.django_db(transaction=True)
def test_a_heartbeat_fleet_completes_workflows_and_batches_and_runs_each_task_once(
    tmp_path,
):
    env = process_env(tmp_path, FLEET_SETTINGS)
    # Which oxpull the processes import, and which worker ox_worker builds for
    # each alias, before believing anything they do. A failure here is the
    # set-up, not the heartbeat.
    setup = run_python(
        env,
        "-c",
        "import django; django.setup(); import oxpull; "
        "from django_ox.worker import worker_class; print(oxpull.__file__); "
        "print(*(cls.__module__ + '.' + cls.__qualname__ for cls in"
        " map(worker_class, ('flows', 'default'))))",
    )
    lines = setup.stdout.split()
    if lines[:1] != [str(SRC / "oxpull" / "__init__.py")] or lines[1:] != [
        "oxpull.worker.OxpullWorker",
        "oxpull.worker.OxpullWorker",
    ]:
        pytest.fail(f"unexpected worker set-up:\n{setup.stdout}{setup.stderr}")

    beats = tmp_path / "run"
    beats.mkdir()
    fleet_file = beats / "flows"
    supervisor_file = beats / "flows.supervisor"
    slot_files = [beats / "flows.0", beats / "flows.1"]
    single_file = beats / "default"
    every_file = [supervisor_file, *slot_files, single_file]

    fleet = WorkerProcess(
        env,
        tmp_path / "flows.log",
        "--backend",
        "flows",
        "--processes",
        "2",
        "--interval",
        "0.1",
        "--heartbeat-file",
        str(fleet_file),
    )
    single = WorkerProcess(
        env,
        tmp_path / "default.log",
        "--backend",
        "default",
        "--interval",
        "0.1",
        "--heartbeat-file",
        str(single_file),
    )
    workers = [fleet, single]
    try:
        watch(every_file, 3, running=workers)
        # Exactly the expected set: no PATH for the fleet, no file per pid.
        assert sorted(path.name for path in beats.iterdir()) == [
            "default",
            "flows.0",
            "flows.1",
            "flows.supervisor",
        ]
        # The supervisor hands each slot the base path and its index, and
        # each slot writes PATH.<index>: the flag reached the workers that run.
        slots = {
            flag_value(argv, "--worker-index"): flag_value(argv, "--heartbeat-file")
            for argv in map(argv_of, children_of(fleet.pid))
        }
        assert slots == {"0": str(fleet_file), "1": str(fleet_file)}, slots

        code, report, text = probe(env, fleet_file, "--processes", "2")
        assert code == 0 and report["ok"], text
        assert [entry["path"] for entry in report["files"]] == [
            str(path) for path in [supervisor_file, *slot_files]
        ], text
        code, report, text = probe(env, single_file)
        assert code == 0 and report["ok"], text

        # The work: a workflow and a batch, which only the scheduled
        # reconcilers complete, and plain tasks for all three workers to race.
        flow = diamond("heartbeat-fleet")
        batch = batches.create(
            "heartbeat-fleet",
            on_complete=tasks.record_callback,
            on_complete_args=["heartbeat-fleet"],
        )
        for number in range(5):
            batch.add(tasks.plain_echo, number)
        batch.seal()
        plain = [tasks.plain_echo.enqueue(number).id for number in range(30)]

        def done():
            return (
                OxpullWorkflow.objects.get(pk=flow.id).status
                == OxpullWorkflow.Status.COMPLETE
                and OxpullBatch.objects.get(pk=batch.id).status
                == OxpullBatch.Status.COMPLETE
                and OxTask.objects.filter(
                    task_path="tests_oxpull.tasks.record_callback",
                    status=OxTask.Status.SUCCESSFUL,
                ).exists()
                and not OxTask.objects.filter(pk__in=plain)
                .exclude(status=OxTask.Status.SUCCESSFUL)
                .exists()
            )

        wait_until(
            done,
            "the workflow, the batch and the plain tasks to finish",
            running=workers,
        )

        # Still turning after the work, and both probes still pass.
        watch(every_file, 3, running=workers)
        code, report, text = probe(env, fleet_file, "--processes", "2")
        assert code == 0 and report["ok"], text
        code, report, text = probe(env, single_file)
        assert code == 0 and report["ok"], text

        # A stopped supervisor fails the fleet's probe while its children,
        # still turning, cannot hide it.
        os.kill(fleet.pid, signal.SIGSTOP)
        try:
            watch([*slot_files, single_file], 1, running=workers)
            watch(
                [*slot_files, single_file], 3, still=[supervisor_file], running=workers
            )
            age(supervisor_file, 120)
            code, report, text = probe(env, fleet_file, "--processes", "2")
            assert code == 1 and not report["ok"], text
            assert failing(report) == [str(supervisor_file)], text
            assert "old" in report["problems"][0], text
        finally:
            os.kill(fleet.pid, signal.SIGCONT)
        watch([supervisor_file], 1, running=workers)
        code, report, text = probe(env, fleet_file, "--processes", "2")
        assert code == 0 and report["ok"], text
    finally:
        codes = [worker.stop() for worker in workers]
    text = "".join(worker.log() for worker in workers)

    assert codes == [0, 0], f"exit codes {codes}\n{text[-3000:]}"
    # The supervisor removes a slot's file once it sees the slot exit, so a
    # stopped fleet fails its probe at once rather than when the files age.
    assert not any(path.exists() for path in slot_files)
    code, report, _ = probe(env, fleet_file, "--processes", "2")
    assert code == 1 and failing(report) == [str(path) for path in slot_files]

    # Once each. attempts is the number of claims (django-ox keeps it equal to
    # len(worker_ids)), and the logs of all three workers name each claim.
    claims = Counter(CLAIMED.findall(text))
    twice = {task_id: count for task_id, count in claims.items() if count > 1}
    assert twice == {}, f"claimed more than once: {twice}"
    rows = list(OxTask.objects.values_list("id", "task_path", "status", "attempts"))
    finished = {
        str(pk) for pk, _, status, _ in rows if status == OxTask.Status.SUCCESSFUL
    }
    expected = {
        *map(str, flow.node_ids().values()),
        *map(str, plain),
        *(
            str(pk)
            for pk, path, _, _ in rows
            if path
            in ("tests_oxpull.tasks.plain_echo", "tests_oxpull.tasks.record_callback")
        ),
    }
    assert expected <= finished
    assert len(expected) == 4 + 30 + 5 + 1
    over = [(str(pk), path, attempts) for pk, path, _, attempts in rows if attempts > 1]
    assert over == []
    assert all(claims[task_id] == 1 for task_id in finished), {
        task_id: claims[task_id] for task_id in finished if claims[task_id] != 1
    }
    # Both reconcilers ran from the schedule, through the workers.
    ran = {path for pk, path, status, _ in rows if str(pk) in finished}
    assert {"oxpull.batches.reconcile", "oxpull.workflows.reconcile"} <= ran


def refused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return str(sock.getsockname()[1])


def test_a_workflow_worker_whose_database_refuses_connections_keeps_turning(tmp_path):
    """
    Started while the database refuses every connection. The workflow schema
    check gives up (on MySQL, the one database where it asks the server
    anything), the loop starts anyway, every pass fails, and the heartbeat
    stays fresh: the file says the loop turns, which it does. The probe
    answers from the files alone in that same environment, with oxpull
    installed, while the database mode there cannot answer at all.
    """
    env = process_env(
        tmp_path,
        REFUSED_SETTINGS.format(absent=str(tmp_path / "absent"), port=refused_port()),
    )
    beats = tmp_path / "run"
    beats.mkdir()
    path = beats / "flows"
    worker = WorkerProcess(
        env,
        tmp_path / "flows.log",
        "--backend",
        "flows",
        "--interval",
        "0.1",
        "--heartbeat-file",
        str(path),
    )
    try:
        watch([path], 3, running=[worker])
        wait_until(
            lambda: PASS_FAILED in worker.log(),
            "a failed pass in the log",
            running=[worker],
        )
        watch([path], 3, running=[worker])
        code, report, text = probe(env, path)
        assert code == 0 and report["ok"], text

        stale = beats / "stale"
        stale.touch()
        age(stale, 120)
        code, report, text = probe(env, stale)
        assert code == 1 and failing(report) == [str(stale)], text
        assert "old" in report["problems"][0], text
        code, report, text = probe(env, beats / "missing")
        assert code == 1 and "missing" in report["problems"][0], text

        # The control: the database mode, in the same environment.
        control = run_python(env, "-m", "django", "ox_health")
        assert control.returncode == 1, control.stdout + control.stderr
        assert "Database unreachable" in control.stderr, control.stderr
    finally:
        code = worker.stop()
    text = worker.log()

    assert code == 0, text[-3000:]
    if connection.vendor == "mysql":
        assert SCHEMA_CHECK_FAILED in text, text[-3000:]
