"""
Following oxpull.W003's hint verbatim stops workers starting.

W003 is emitted once per WORKFLOWS backend, and its hint is one fixed
mapping: a schedule named "oxpull-workflow-reconcile" with no backend and
no queue_name. Two common multi-backend shapes turn that paste into an
Error-level configuration:

  (a) two WORKFLOWS backends. Each warns, each hint says the same thing,
      and pasting each into its own backend defines one schedule name on
      two backends: django_ox.E003.
  (b) a WORKFLOWS backend with its own QUEUES. The reconciler Task is
      built on the default backend's queue, and schedules_from_options
      rebinds it to this backend and revalidates: django_ox.E002.

Every step runs in a fresh interpreter, the way an operator meets it:
`manage.py check` prints the hint, the hint is parsed out of that output
and pasted where it points, and then `manage.py check`, `manage.py
migrate`, the worker construction `ox_worker` performs, and `ox_worker`
itself run against the pasted settings. The subprocesses use this leg's
test database, so the same file runs on SQLite, PostgreSQL and MySQL.

Each test asserts the correct behaviour: a configuration that was
warning-only and ran its workers before the paste still runs them after
it. Today both fail.

CHECK_SCRIPT in test_workflows.py is the shape this follows. It is not
imported because its TASKS are fixed; these tests need their own.
"""

import ast
import copy
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from django.db import connection

import oxpull
from oxpull import workflows

# transaction=True: the subprocesses share this leg's test database, so
# nothing here may hold a transaction open while they run, and whatever
# a started worker writes is flushed afterwards.
pytestmark = pytest.mark.django_db(transaction=True)

# How long a started `ox_worker` must stay up to count as running. A
# configuration error exits within the interpreter's startup; the grace
# only has to outlast startup under load.
STARTUP_GRACE = 10.0

RUNNER = """
import json
import os
import sys

try:
    import MySQLdb  # noqa: F401
except ModuleNotFoundError:
    try:
        import pymysql

        pymysql.install_as_MySQLdb()
    except ModuleNotFoundError:
        pass

import django
from django.conf import settings

settings.configure(
    SECRET_KEY="check-only",
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    INSTALLED_APPS=["django_ox", "oxpull"],
    DATABASES={"default": json.loads(os.environ["OX_HAZARD_DATABASE"])},
    TASKS=json.loads(os.environ["OX_HAZARD_TASKS"]),
)
django.setup()

import oxpull

if os.path.dirname(oxpull.__file__) != os.environ["OX_HAZARD_OXPULL_DIR"]:
    sys.exit(f"wrong oxpull imported: {oxpull.__file__}")

if sys.argv[1] == "construct":
    # The line ox_worker's handle() runs, minus the command around it.
    from django_ox.worker import worker_class

    alias = sys.argv[2]
    try:
        worker_class(alias)(backend_alias=alias)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}")
        sys.exit(1)
    print("constructed")
else:
    from django.core.management import execute_from_command_line

    execute_from_command_line(["manage.py", *sys.argv[1:]])
"""

BATCH_RECONCILE = {
    "oxpull-batch-reconcile": {"task": "oxpull.batches.reconcile", "cron": "* * * * *"},
}


def _default_backend():
    """
    The project's default backend, with the batch reconciler scheduled.

    Scheduled so oxpull.W002 stays silent and W003 is the only thing the
    base configuration warns about.
    """
    return {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default"],
        "OPTIONS": {"SCHEDULES": copy.deepcopy(BATCH_RECONCILE)},
    }


def _workflow_backend(queues):
    """A correctly declared workflow backend: WORKFLOWS and a gating worker."""
    return {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": queues,
        "OPTIONS": {
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "WORKFLOWS": True,
        },
    }


# -- running the operator's commands -----------------------------------


def _database():
    """This leg's test database, as a subprocess's settings need it."""
    source = connection.settings_dict
    database = {
        key: source[key]
        for key in ("ENGINE", "NAME", "USER", "PASSWORD", "HOST", "PORT", "OPTIONS")
        if key in source
    }
    if source["ENGINE"].endswith("sqlite3"):
        database["NAME"] = str(Path(source["NAME"]).resolve())
    return database


def _env(tasks):
    env = {k: v for k, v in os.environ.items() if k != "DJANGO_SETTINGS_MODULE"}
    env["OX_HAZARD_TASKS"] = json.dumps(tasks)
    env["OX_HAZARD_DATABASE"] = json.dumps(_database())
    env["OX_HAZARD_OXPULL_DIR"] = str(Path(oxpull.__file__).parent)
    return env


def _run(tasks, *argv):
    """One command in a fresh interpreter; returns (returncode, output)."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", RUNNER, *argv],
        capture_output=True,
        text=True,
        env=_env(tasks),
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


def _say(text):
    """Printed, so a failing run's captured stdout carries the whole sequence."""
    print(text)  # noqa: T201


def _check_ids(output, level):
    return re.findall(rf"\((\w+\.{level}\d{{3}})\)", output)


def _start_workers(tasks, aliases, logdir):
    """
    `manage.py ox_worker --backend ALIAS` for every alias, concurrently.

    "running" when the process is still up after STARTUP_GRACE and logged
    no traceback; otherwise how it exited. Survivors get SIGTERM, which is
    how an operator stops one.
    """
    logdir.mkdir(parents=True, exist_ok=True)
    env = _env(tasks)
    procs = {}
    for alias in aliases:
        log = (logdir / f"{alias}.log").open("w")
        procs[alias] = (
            subprocess.Popen(  # noqa: S603
                [sys.executable, "-c", RUNNER, "ox_worker", "--backend", alias],
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            ),
            log,
        )
    deadline = time.monotonic() + STARTUP_GRACE
    while time.monotonic() < deadline and any(
        proc.poll() is None for proc, _ in procs.values()
    ):
        time.sleep(0.2)
    outcomes, outputs = {}, {}
    for alias, (proc, log) in procs.items():
        survived = proc.poll() is None
        if survived:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        log.close()
        output = (logdir / f"{alias}.log").read_text()
        outputs[alias] = output
        if survived and "Traceback" not in output:
            outcomes[alias] = "running"
        elif survived:
            outcomes[alias] = "running, but logged a traceback"
        else:
            errors = _check_ids(output, "E")
            last = output.strip().splitlines()[-1:] or [""]
            outcomes[alias] = f"exited rc={proc.returncode} {errors or last}"
    return outcomes, outputs


# -- following the hint ------------------------------------------------

W003_BLOCK = re.compile(
    r"^\?: \(oxpull\.W003\) (?P<msg>.*?)\n\s*HINT: (?P<hint>.*?)(?=\n\?: |\n\n|\Z)",
    re.S | re.M,
)


def _mapping_in(hint):
    """The first brace-balanced literal in the hint, as Python data."""
    start = hint.index("{")
    depth = 0
    for index, char in enumerate(hint[start:], start):
        depth += char == "{"
        depth -= char == "}"
        if depth == 0:
            return ast.literal_eval(hint[start : index + 1])
    raise AssertionError(f"no complete mapping in hint: {hint!r}")


def _where_the_hint_points(message, hint):
    """
    The backend whose OPTIONS the operator pastes into.

    The backend the hint names, if it names one (TASKS['x']); otherwise
    the backend the warning is about, which is the only one today's text
    names; otherwise the default backend.
    """
    named = re.search(r"TASKS\[['\"]([^'\"]+)['\"]\]", hint) or re.search(
        r"[Bb]ackend '([^']+)'", message
    )
    return named.group(1) if named else "default"


def _follow_every_w003_hint(tasks, check_output):
    pasted = copy.deepcopy(tasks)
    pastes = []
    for block in W003_BLOCK.finditer(check_output):
        alias = _where_the_hint_points(block["msg"], block["hint"])
        mapping = _mapping_in(block["hint"])
        options = pasted[alias].setdefault("OPTIONS", {})
        options.setdefault("SCHEDULES", {}).update(mapping)
        pastes.append((alias, mapping))
    return pasted, pastes


def _follow_the_hint_and_start_everything(tasks, tmp_path):
    """
    The operator's sequence, returning what each command did afterwards.

    Premises are asserted on the way: the base configuration is
    warning-only, it prints W003, and every backend's worker starts.
    """
    _say(f"oxpull: {oxpull.__file__}")
    _say(f"database: {connection.vendor} {_database()['NAME']}")

    rc, base_output = _run(tasks, "check")
    _say(f"== manage.py check, before the paste (rc={rc})\n{base_output}")
    assert rc == 0, base_output
    assert _check_ids(base_output, "E") == [], base_output
    assert "oxpull.W003" in base_output, base_output

    base_workers, base_logs = _start_workers(tasks, list(tasks), tmp_path / "base")
    _say(f"== ox_worker, before the paste: {base_workers}")
    assert base_workers == dict.fromkeys(tasks, "running"), base_logs

    pasted, pastes = _follow_every_w003_hint(tasks, base_output)
    _say(f"== pasted, one entry per W003 printed: {pastes}")
    assert pastes, base_output

    check_rc, check_output = _run(pasted, "check")
    _say(f"== manage.py check, after the paste (rc={check_rc})\n{check_output}")
    migrate_rc, migrate_output = _run(pasted, "migrate", "--noinput")
    _say(f"== manage.py migrate, after (rc={migrate_rc})\n{migrate_output}")
    constructed = {}
    for alias in pasted:
        _, output = _run(pasted, "construct", alias)
        constructed[alias] = output.strip()
    _say(f"== worker_class(alias)(backend_alias=alias), after: {constructed}")
    workers, worker_logs = _start_workers(pasted, list(pasted), tmp_path / "pasted")
    _say(f"== ox_worker, after the paste: {workers}")
    for alias, output in worker_logs.items():
        _say(f"-- ox_worker --backend {alias} output:\n{output}")

    return {
        "check rc": check_rc,
        "check errors": _check_ids(check_output, "E"),
        "W003 still printed": "oxpull.W003" in check_output,
        "migrate rc": migrate_rc,
        "worker construction": constructed,
        "ox_worker": workers,
    }


def _started_cleanly(tasks):
    return {
        "check rc": 0,
        "check errors": [],
        "W003 still printed": False,
        "migrate rc": 0,
        "worker construction": dict.fromkeys(tasks, "constructed"),
        "ox_worker": dict.fromkeys(tasks, "running"),
    }


# -- the hazards -------------------------------------------------------


def test_pasting_each_w003_hint_into_its_backend_keeps_two_workflow_backends_running(
    tmp_path,
):
    """
    (a) Two WORKFLOWS backends, each warned about, each hint pasted into
    the backend its warning names. Today: django_ox.E003 on both, rc=1,
    and every command that runs the checks refuses, ox_worker included,
    for every backend, the default one too.
    """
    tasks = {
        "default": _default_backend(),
        "flows": _workflow_backend(["default"]),
        "flows_b": _workflow_backend(["default"]),
    }
    outcome = _follow_the_hint_and_start_everything(tasks, tmp_path)
    assert outcome == _started_cleanly(tasks)


def test_pasting_the_w003_hint_into_a_backend_with_its_own_queues_keeps_it_running(
    tmp_path,
):
    """
    (b) The workflow backend has its own QUEUES, and the default backend
    does not share them. The hint gives no queue_name. Today:
    django_ox.E002 ("Queue 'default' is not valid for backend"), rc=1.
    """
    tasks = {
        "default": _default_backend(),
        "flows": _workflow_backend(["pipelines"]),
    }
    assert workflows.RECONCILE_TASK_PATH == "oxpull.workflows.reconcile"
    outcome = _follow_the_hint_and_start_everything(tasks, tmp_path)
    assert outcome == _started_cleanly(tasks)
