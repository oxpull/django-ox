"""
Run django-ox 1.4.0 and this checkout as one mixed fleet, and check what
per-task policy does there.

Per-task policy (``@task(max_attempts=..., backoff=..., timeout=...)``) adds no
migration. ``max_attempts`` is written into the column every release already
has, and ``backoff`` and ``timeout`` are read from the code the worker imports
for each attempt. So a fleet in the middle of a rollout has 1.4.0 processes and
new processes on one table, each enqueuing rows the other may claim. The pytest
suite cannot show what that does, because it only ever has one django-ox
importable. This script builds two isolated virtual environments, one with
django-ox 1.4.0 from PyPI and one with this checkout, both on the same Django,
points them at one PostgreSQL database, and runs real ``manage.py ox_worker``
processes from each.

What it checks, each "ignored" against a control run in which the same rows
are enforced, so that nothing is taken on trust:

A. A row the candidate enqueues carries the task's own ``max_attempts``, and a
   1.4.0 worker spends exactly that budget.
B. A row 1.4.0 enqueues carries 1.4.0's ``MAX_ATTEMPTS``, and a candidate
   worker spends that budget even though its own backend default and the
   task's live declaration both say otherwise. The live ``timeout`` does apply.
C. A 1.4.0 worker running a task whose module imports in 1.4.0 ignores the
   task's ``backoff`` and ``timeout`` and keeps running; a candidate worker on
   the same rows enforces both.
D. The cold import: a module that passes the new keyword arguments to ``@task``
   does not import under 1.4.0 at all. The exact error is printed, with what a
   1.4.0 worker does with rows of such a task, and what a 1.4.0 worker does
   when the app imports that module at startup.
E. Both workers on one queue at once, rows enqueued from both sides: every row
   spends exactly its stored budget, whichever worker claims each attempt.

D is why the rollout order is: the new django-ox (and its oxpull pair)
everywhere first, declarations second. While a fleet is genuinely mixed, a
declaration has to be import-compatible (``mixapp/compat.py`` in PROJECT below
is one way) and neither the per-task timeout nor the backoff can be relied on.

Before anything runs, the script establishes which code each side imports:
the 1.4.0 side must be the PyPI release (no ``direct_url.json``, and when the
checkout has the ``v1.4.0`` tag, byte-identical to it) and the candidate side
must be this checkout, installed as a wheel, byte-identical to ``src/``. Both
sides must list the same applied migrations and detect no model changes.

Requirements: a disposable PostgreSQL server, network access to PyPI, and git.
The named database is dropped and recreated on every run. The password is read
from MIX_DB_PASSWORD (or PGPASSWORD), never from the command line::

    MIX_DB_PASSWORD=ox python tools/mixed_fleet_policy.py \\
        --workdir ../mixed-fleet --db-port 54329 --report ../mixed-fleet/run.md

The virtual environments are kept in the work directory and reused. The 1.4.0
one is checked, not reinstalled; the candidate is reinstalled from the checkout
on every run so a stale build is never what runs. Exit status is 0 when every
check passed and 1 otherwise; the report says which. Every wait is bounded
here rather than by a shell ``timeout``, so it runs the same on Linux and
macOS. It lives outside ``tests/`` so pytest never collects it.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

REPO = Path(__file__).resolve().parent.parent

POLL_INTERVAL = 0.3
SETTLE_TIMEOUT = 120.0
STOP_TIMEOUT = 30.0
COMMAND_TIMEOUT = 180.0
INSTALL_TIMEOUT = 900.0
# How long the candidate worker must stay up in D3's control.
STARTUP_WINDOW = 3.0

PENDING = ("READY", "RUNNING", "WAITING")
TIMEOUT_ERROR = "django_ox.exceptions.TaskTimeout"
OLD = "1.4.0"
NEW = "candidate"

# The project both environments run. Only mixapp/declared.py differs in what
# it needs: it is written for the new release and imports nowhere else.
PROJECT: dict[str, str] = {
    "manage.py": """\
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mixproj.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
""",
    "mixproj/__init__.py": "",
    "mixproj/settings.py": """\
import os

SECRET_KEY = "mixed-fleet-harness"
DEBUG = False
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["django_ox", "mixapp.apps.MixappConfig"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ["MIX_DB_NAME"],
        "USER": os.environ["MIX_DB_USER"],
        "PASSWORD": os.environ["MIX_DB_PASSWORD"],
        "HOST": os.environ["MIX_DB_HOST"],
        "PORT": os.environ["MIX_DB_PORT"],
    }
}

# One configuration for both sides, as one project deployed twice would have.
# MIX_MAX_ATTEMPTS lets a run give one process a different backend default.
# Retries come round in a fifth of a second. No TASK_TIMEOUT: every timeout
# seen in a run is a task's own.
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "MAX_ATTEMPTS": int(os.environ.get("MIX_MAX_ATTEMPTS", "2")),
            "BACKOFF_INITIAL": 0.1,
            "BACKOFF_MAX": 0.2,
        },
    }
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"plain": {"()": "mixproj.logfmt.ExtrasFormatter"}},
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "plain"}
    },
    "loggers": {
        "django_ox": {"handlers": ["console"], "level": "INFO", "propagate": False}
    },
}
""",
    "mixproj/logfmt.py": """\
import logging

# The structured keys django-ox attaches, printed so the worker log says which
# path each outcome took (reason=backoff_declined, event=task_policy_error...).
KEYS = ("event", "reason", "exception", "error", "retry_in_s", "timeout_s")


class ExtrasFormatter(logging.Formatter):
    def __init__(self):
        super().__init__("%(asctime)s %(levelname)s %(name)s %(message)s")

    def format(self, record):
        line = super().format(record)
        extras = [f"{k}={getattr(record, k)!r}" for k in KEYS if hasattr(record, k)]
        return f"{line} [{' '.join(extras)}]" if extras else line
""",
    "mixapp/__init__.py": "",
    "mixapp/apps.py": """\
import os

from django.apps import AppConfig


class MixappConfig(AppConfig):
    name = "mixapp"

    def ready(self):
        # An app that imports its task modules at startup, as many do to
        # connect signals. Off unless a scenario turns it on.
        if os.environ.get("MIX_IMPORT_DECLARED_AT_READY") == "1":
            import mixapp.declared  # noqa: F401
""",
    "mixapp/markers.py": '''\
import json
import os
import time
from pathlib import Path

import django_ox


def mark(event, **fields):
    """One JSON line to MIX_MARKERS: who ran what, from which django-ox."""
    path = os.environ.get("MIX_MARKERS")
    if not path:
        return
    line = {
        "event": event,
        "env": os.environ.get("MIX_ENV_LABEL"),
        "django_ox": django_ox.__file__,
        "pid": os.getpid(),
        "t": time.time(),
        **fields,
    }
    with Path(path).open("a") as out:
        out.write(json.dumps(line) + "\\n")


def attempt(context, name, event="start"):
    result = context.task_result
    mark(event, task=name, id=str(result.id), attempt=result.attempts)
''',
    "mixapp/shared.py": '''\
"""Declarations with no policy: the same task on both sides of a rollout."""

from django.tasks import task

from .markers import attempt


@task(takes_context=True)
def always_fails(context):
    attempt(context, "shared.always_fails")
    raise ValueError("always_fails fails every attempt")


@task(takes_context=True)
def succeeds(context):
    attempt(context, "shared.succeeds")
    return "ok"
''',
    "mixapp/compat.py": '''\
"""
Policy declared only where the installed django-ox has it.

This module imports under 1.4.0 and under the candidate. Under the candidate
the three tasks carry their policy; under 1.4.0 the keyword arguments are left
out, because 1.4.0's @task refuses them, and the tasks are plain.
"""

import time

from django.tasks import task

from .markers import attempt, mark

try:
    import django_ox.tasks  # noqa: F401
except ImportError:
    POLICY_SUPPORTED = False
else:
    POLICY_SUPPORTED = True


def declare(**policy):
    return task(takes_context=True, **(policy if POLICY_SUPPORTED else {}))


def stop_now(exc, task_result):
    mark(
        "backoff_called",
        id=str(task_result.id),
        attempts=task_result.attempts,
        max_attempts=task_result.task.max_attempts,
        status=str(task_result.status),
        exception=type(exc).__name__,
    )
    return None


@declare(max_attempts=5)
def fails_budget5(context):
    attempt(context, "compat.fails_budget5")
    raise ValueError("fails_budget5 fails every attempt")


@declare(max_attempts=4, backoff=stop_now)
def stops_at_once(context):
    attempt(context, "compat.stops_at_once")
    raise ValueError("stops_at_once fails every attempt")


@declare(timeout=1)
def slow_with_timeout(context):
    attempt(context, "compat.slow_with_timeout")
    # Short sleeps, not one long one: a timeout is delivered between them.
    end = time.monotonic() + 3
    while time.monotonic() < end:
        time.sleep(0.02)
    attempt(context, "compat.slow_with_timeout", event="end")
    return "finished"
''',
    "mixapp/declared.py": '''\
"""Policy declared outright, as an app written for the new release does."""

from django.tasks import task

from .markers import attempt


def retry_in_one_second(exc, task_result):
    return 1


@task(takes_context=True, max_attempts=3, timeout=30, backoff=retry_in_one_second)
def declared_succeeds(context):
    attempt(context, "declared.declared_succeeds")
    return "ok"
''',
    "mixapp/management/__init__.py": "",
    "mixapp/management/commands/__init__.py": "",
    "mixapp/management/commands/mixprobe.py": '''\
"""Read and write the shared table as one environment sees it; JSON out."""

import importlib.metadata
import importlib.util
import json
import sys

import django
from django.core.management.base import BaseCommand
from django.tasks import task_backends
from django.utils.module_loading import import_string

import django_ox
from django_ox.models import OxTask


def last_line(text):
    lines = [line for line in (text or "").splitlines() if line.strip()]
    return lines[-1] if lines else ""


def row_json(row):
    return {
        "id": str(row.id),
        "task_path": row.task_path,
        "status": row.status,
        "attempts": row.attempts,
        "max_attempts": row.max_attempts,
        "worker_ids": row.worker_ids,
        "errors": [e.get("exception_class_path") for e in row.errors],
        "last_error": last_line(row.errors[-1].get("traceback")) if row.errors else "",
        "return_value": row.return_value,
        "enqueued_at": row.enqueued_at.isoformat(),
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "run_after": row.run_after.isoformat() if row.run_after else None,
    }


class Command(BaseCommand):
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument(
            "action", choices=["identity", "reset", "enqueue", "rows", "result"]
        )
        parser.add_argument("targets", nargs="*")
        parser.add_argument("--count", type=int, default=1)

    def handle(self, *args, **options):
        action = getattr(self, f"do_{options['action']}")
        out = action(options["targets"], options["count"])
        self.stdout.write(json.dumps(out))

    def do_identity(self, targets, count):
        dist = importlib.metadata.distribution("django-ox")
        task_class = type(task_backends["default"]).task_class
        return {
            "python": sys.version.split()[0],
            "prefix": sys.prefix,
            "django": django.get_version(),
            "django_ox_file": django_ox.__file__,
            "django_ox_dist_version": dist.version,
            "django_ox_version_attr": getattr(django_ox, "__version__", None),
            "direct_url": dist.read_text("direct_url.json"),
            "has_tasks_module": importlib.util.find_spec("django_ox.tasks")
            is not None,
            "task_class": f"{task_class.__module__}.{task_class.__qualname__}",
        }

    def do_reset(self, targets, count):
        deleted, _ = OxTask.objects.all().delete()
        return {"deleted": deleted}

    def do_enqueue(self, targets, count):
        out = []
        for path in targets:
            declared = import_string(path)
            for _ in range(count):
                result = declared.enqueue()
                row = OxTask.objects.get(id=result.id)
                out.append(
                    {
                        "id": str(result.id),
                        "task_path": row.task_path,
                        "row_max_attempts": row.max_attempts,
                        "result_task_class": type(result.task).__name__,
                        "result_task_max_attempts": getattr(
                            result.task, "max_attempts", "no such field"
                        ),
                        "declared_max_attempts": getattr(
                            declared, "max_attempts", "no such field"
                        ),
                        "result_task_is_declared": result.task == declared,
                    }
                )
        return out

    def do_rows(self, targets, count):
        rows = OxTask.objects.all()
        if targets:
            rows = rows.filter(id__in=targets)
        return [row_json(row) for row in rows.order_by("enqueued_at", "id")]

    def do_result(self, targets, count):
        out = []
        for task_id in targets:
            try:
                result = task_backends["default"].get_result(task_id)
            except Exception as exc:  # noqa: BLE001
                out.append({"id": task_id, "raised": f"{type(exc).__name__}: {exc}"})
                continue
            out.append(
                {
                    "id": task_id,
                    "status": str(result.status),
                    "attempts": result.attempts,
                    "task_class": type(result.task).__name__,
                    "task_max_attempts": getattr(
                        result.task, "max_attempts", "no such field"
                    ),
                    "errors": [e.exception_class_path for e in result.errors],
                }
            )
        return out
''',
}

# Run with ``python -c`` from the project directory, after django.setup().
IMPORT_DECLARED = (
    "import django; django.setup(); import mixapp.declared as m; "
    "t = m.declared_succeeds; "
    "print(type(t).__name__, t.max_attempts, t.timeout, t.backoff.__name__)"
)
IMPORT_COMPAT = (
    "import json, django; django.setup(); import mixapp.compat as m; "
    "print(json.dumps({'POLICY_SUPPORTED': m.POLICY_SUPPORTED, "
    "'class': type(m.fails_budget5).__name__, "
    "'max_attempts': getattr(m.fails_budget5, 'max_attempts', 'no such field')}))"
)
RECREATE_DB = """\
import os

import psycopg
from psycopg import sql

name = os.environ["MIX_DB_NAME"]
with psycopg.connect(
    host=os.environ["MIX_DB_HOST"],
    port=os.environ["MIX_DB_PORT"],
    user=os.environ["MIX_DB_USER"],
    password=os.environ["MIX_DB_PASSWORD"],
    dbname="postgres",
    autocommit=True,
) as conn:
    conn.execute(
        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
    )
    conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
print("recreated", name)
"""


class HarnessFailure(Exception):
    """A step that the rest of the run depends on did not work."""


@dataclass
class Env:
    label: str
    venv: Path

    @property
    def python(self) -> Path:
        return self.venv / "bin" / "python"


@dataclass
class Worker:
    env: Env
    name: str
    proc: "subprocess.Popen[bytes]"
    log: Path
    handle: IO[bytes]


class Harness:
    def __init__(self, args: argparse.Namespace, password: str) -> None:
        self.args = args
        self.workdir: Path = args.workdir.resolve()
        self.project = self.workdir / "project"
        self.logs = self.workdir / "logs"
        self.old = Env(OLD, self.workdir / f"venv-django-ox-{args.old_version}")
        self.new = Env(NEW, self.workdir / "venv-candidate")
        self.password = password
        self.report: list[str] = []
        self.failures: list[str] = []
        self.checks = 0
        self.enqueued_by: dict[str, str] = {}
        self.markers: Path = self.logs / "markers.jsonl"

    # -- reporting ---------------------------------------------------------

    def say(self, text: str = "") -> None:
        print(text, flush=True)
        self.report.append(text)

    def block(self, text: str) -> None:
        self.say("```")
        for line in text.rstrip("\n").splitlines():
            self.say(line)
        self.say("```")

    def check(self, *, ok: bool, claim: str, observed: object) -> bool:
        self.checks += 1
        self.say(f"- {'PASS' if ok else 'FAIL'}: {claim}. Observed: {observed}")
        if not ok:
            self.failures.append(claim)
        return ok

    # -- processes ---------------------------------------------------------

    def child_env(
        self, env: Env, extra: dict[str, str] | None = None
    ) -> dict[str, str]:
        # Nothing from the calling shell may put another django-ox on the
        # path: no PYTHONPATH, no user site, no active virtualenv.
        base = {
            k: v
            for k, v in os.environ.items()
            if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")
            and not k.startswith(("DJANGO_", "MIX_"))
        }
        base.update(
            {
                "DJANGO_SETTINGS_MODULE": "mixproj.settings",
                "PYTHONNOUSERSITE": "1",
                "PYTHONUNBUFFERED": "1",
                "PATH": f"{env.venv / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}",
                "MIX_ENV_LABEL": env.label,
                "MIX_MARKERS": str(self.markers),
                "MIX_DB_HOST": self.args.db_host,
                "MIX_DB_PORT": str(self.args.db_port),
                "MIX_DB_USER": self.args.db_user,
                "MIX_DB_PASSWORD": self.password,
                "MIX_DB_NAME": self.args.db_name,
            }
        )
        base.update(extra or {})
        return base

    def run(
        self,
        env: Env,
        argv: list[str],
        *,
        extra: dict[str, str] | None = None,
        timeout: float = COMMAND_TIMEOUT,
        cwd: Path | None = None,
    ) -> "subprocess.CompletedProcess[str]":
        try:
            return subprocess.run(  # noqa: S603
                [str(env.python), *argv],
                cwd=cwd or self.project,
                env=self.child_env(env, extra),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as hung:
            raise HarnessFailure(
                f"[{env.label}] {' '.join(argv)} did not finish in {timeout:.0f}s"
            ) from hung

    def must(
        self, env: Env, argv: list[str], **kwargs: Any
    ) -> "subprocess.CompletedProcess[str]":
        done = self.run(env, argv, **kwargs)
        if done.returncode != 0:
            raise HarnessFailure(
                f"[{env.label}] {' '.join(argv)} exited {done.returncode}:\n"
                f"{done.stdout}{done.stderr}"
            )
        return done

    def probe(self, env: Env, *argv: str, count: int = 1) -> Any:
        done = self.must(env, ["manage.py", "mixprobe", *argv, "--count", str(count)])
        return json.loads(done.stdout)

    def start_worker(
        self, env: Env, name: str, extra: dict[str, str] | None = None
    ) -> Worker:
        log = self.logs / f"worker-{name}.log"
        handle = log.open("wb")
        proc = subprocess.Popen(  # noqa: S603
            [
                str(env.python),
                "manage.py",
                "ox_worker",
                "--interval",
                "0.05",
                "--concurrency",
                "4",
            ],
            cwd=self.project,
            env=self.child_env(env, extra),
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        self.say(f"Started `{env.label}` worker `{name}` (pid {proc.pid}), log {log}")
        return Worker(env, name, proc, log, handle)

    def stop_worker(self, worker: Worker) -> None:
        """SIGTERM, bounded wait; check it was up until then and exited 0."""
        early = worker.proc.poll()
        if early is None:
            worker.proc.send_signal(signal.SIGTERM)
            try:
                code = worker.proc.wait(STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                worker.proc.kill()
                code = worker.proc.wait()
                self.check(
                    ok=False,
                    claim=f"{worker.name} stops within {STOP_TIMEOUT:.0f}s of SIGTERM",
                    observed="killed",
                )
        else:
            code = early
        worker.handle.close()
        self.check(
            ok=early is None and code == 0,
            claim=f"{worker.env.label} worker {worker.name} was still running when "
            "asked to stop, and exited 0",
            observed=f"running until SIGTERM: {early is None}, exit status {code}",
        )
        self.show_log(worker)

    def show_log(self, worker: Worker, limit: int = 14) -> None:
        lines = worker.log.read_text(errors="replace").splitlines()
        interesting = [
            line
            for line in lines
            if " django_ox" in line or line.startswith(("Traceback", "TypeError"))
        ]
        self.say(f"Worker `{worker.name}` log ({len(lines)} lines), excerpt:")
        excerpt = interesting[:limit]
        if len(interesting) > limit:
            excerpt.append(f"... {len(interesting) - limit} more in {worker.log}")
        self.block("\n".join(excerpt) or "(empty)")

    # -- the shared table --------------------------------------------------

    def reset(self) -> None:
        self.probe(self.new, "reset")
        self.markers.write_text("")

    def enqueue(self, env: Env, path: str, *, count: int = 1, budget: int) -> list[str]:
        """
        Enqueue from `env`; the row must say `budget`, and the returned result
        must carry the task as declared, whatever the row stores.
        """
        made = self.probe(env, "enqueue", path, count=count)
        for item in made:
            self.enqueued_by[item["id"]] = env.label
        first = made[0]
        # 1.4.0's Task has no max_attempts field; there both sides say so.
        reported = {item["result_task_max_attempts"] for item in made}
        declared = {item["declared_max_attempts"] for item in made}
        self.check(
            ok={item["row_max_attempts"] for item in made} == {budget}
            and reported == declared
            and all(item["result_task_is_declared"] for item in made),
            claim=f"{env.label} enqueues {count} x {path} with {budget} stored on "
            "the row, and its result carries the task as declared, max_attempts: "
            f"{first['declared_max_attempts']}",
            observed=f"row max_attempts {first['row_max_attempts']}, result "
            f"{first['result_task_class']}.max_attempts "
            f"{first['result_task_max_attempts']}, equal to the declared task: "
            f"{first['result_task_is_declared']}",
        )
        return [item["id"] for item in made]

    def settle(self, ids: list[str], workers: list[Worker]) -> dict[str, Any]:
        deadline = time.monotonic() + SETTLE_TIMEOUT
        while True:
            rows = {row["id"]: row for row in self.probe(self.new, "rows", *ids)}
            if all(row["status"] not in PENDING for row in rows.values()):
                return rows
            if any(w.proc.poll() is not None for w in workers):
                self.say("A worker exited before the rows settled.")
                return rows
            if time.monotonic() > deadline:
                self.say(f"Rows still pending after {SETTLE_TIMEOUT:.0f}s.")
                return rows
            time.sleep(POLL_INTERVAL)

    def read_markers(self) -> list[dict[str, Any]]:
        text = self.markers.read_text() if self.markers.exists() else ""
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def table(self, rows: dict[str, Any], markers: list[dict[str, Any]]) -> None:
        ran = Counter((m["id"], m["env"]) for m in markers if m["event"] == "start")
        self.say(
            "| task | enqueued by | stored budget | status | attempts "
            "| errors | body ran on |"
        )
        self.say("|---|---|---|---|---|---|---|")
        for row in rows.values():
            errors = Counter(e.rsplit(".", 1)[-1] for e in row["errors"])
            where = ", ".join(
                f"{env} x{n}"
                for (tid, env), n in sorted(ran.items())
                if tid == row["id"]
            )
            self.say(
                f"| {row['task_path'].removeprefix('mixapp.')} "
                f"| {self.enqueued_by.get(row['id'], '?')} | {row['max_attempts']} "
                f"| {row['status']} | {row['attempts']} "
                f"| {', '.join(f'{k} x{n}' for k, n in errors.items()) or '-'} "
                f"| {where or 'never'} |"
            )

    def starts(
        self, markers: list[dict[str, Any]], task_id: str
    ) -> list[dict[str, Any]]:
        return [m for m in markers if m["event"] == "start" and m["id"] == task_id]

    def expect_row(
        self,
        rows: dict[str, Any],
        markers: list[dict[str, Any]],
        task_id: str,
        *,
        claim: str,
        status: str,
        attempts: int,
        error: str | None = None,
        ran_on: str | None = None,
    ) -> None:
        row = rows[task_id]
        runs = self.starts(markers, task_id)
        envs = sorted({m["env"] for m in runs})
        errors_ok = (
            error is None
            or (row["errors"] == [error] * attempts)
            or (status == "SUCCESSFUL" and not row["errors"])
        )
        self.check(
            ok=row["status"] == status
            and row["attempts"] == attempts
            and len(row["worker_ids"]) == attempts
            and errors_ok
            and (ran_on is None or envs == [ran_on]),
            claim=claim,
            observed=f"{row['status']}, attempts {row['attempts']}/"
            f"{row['max_attempts']}, {len(row['worker_ids'])} claims recorded, "
            f"errors {[e.rsplit('.', 1)[-1] for e in row['errors']]}, body ran "
            f"{len(runs)} time(s) on {envs or 'nothing'}",
        )

    # -- setup -------------------------------------------------------------

    def write_project(self) -> None:
        # Written afresh every run, so no file left from an earlier one is
        # importable.
        if self.project.exists():
            shutil.rmtree(self.project)
        for relative, text in PROJECT.items():
            path = self.project / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.logs.mkdir(parents=True, exist_ok=True)

    def base_python(self, env: Env) -> None:
        if env.python.exists():
            return
        done = subprocess.run(  # noqa: S603
            [self.args.python, "-m", "venv", str(env.venv)],
            capture_output=True,
            text=True,
            timeout=INSTALL_TIMEOUT,
            check=False,
        )
        if done.returncode != 0:
            raise HarnessFailure(f"venv for {env.label} failed:\n{done.stderr}")

    def pip(self, env: Env, *argv: str) -> None:
        self.must(
            env,
            ["-m", "pip", "install", "--quiet", "--disable-pip-version-check", *argv],
            timeout=INSTALL_TIMEOUT,
            cwd=self.workdir,
        )

    def install(self) -> None:
        deps = [f"Django=={self.args.django}", f"psycopg[binary]=={self.args.psycopg}"]
        self.base_python(self.old)
        self.pip(self.old, *deps, f"django-ox=={self.args.old_version}")
        self.base_python(self.new)
        self.pip(self.new, *deps)
        # A fresh wheel of the checkout every run: what is tested is what is
        # on disk now, not whatever an earlier run built.
        self.pip(self.new, "--force-reinstall", "--no-deps", str(REPO))

    def identity(self) -> None:
        self.say("## The two environments")
        self.say()
        ids = {env.label: self.probe(env, "identity") for env in (self.old, self.new)}
        for label, data in ids.items():
            self.say(f"`{label}`:")
            self.block(json.dumps(data, indent=2))
        old, new = ids[OLD], ids[NEW]
        self.check(
            ok=old["django_ox_dist_version"] == self.args.old_version
            and old["direct_url"] is None
            and old["django_ox_file"].startswith(str(self.old.venv))
            and not old["has_tasks_module"],
            claim=f"the old side imports django-ox {self.args.old_version} "
            "installed from the package index into its own venv, with no "
            "django_ox.tasks",
            observed=f"version {old['django_ox_dist_version']}, direct_url "
            f"{old['direct_url']}, file {old['django_ox_file']}, tasks module "
            f"{old['has_tasks_module']}",
        )
        new_url = json.loads(new["direct_url"] or "{}").get("url")
        self.check(
            ok=new_url == REPO.as_uri()
            and new["django_ox_file"].startswith(str(self.new.venv))
            and new["has_tasks_module"]
            and new["task_class"] == "django_ox.tasks.PolicyTask",
            claim="the candidate side imports a wheel built from this checkout, "
            "installed into its own venv, whose OxBackend builds PolicyTask",
            observed=f"direct_url {new_url}, file {new['django_ox_file']}, "
            f"task_class {new['task_class']}",
        )
        self.check(
            ok=old["django"] == new["django"] == self.args.django
            and old["python"] == new["python"],
            claim=f"both sides run Django {self.args.django} on one Python",
            observed=f"{old['django']}/{new['django']} on "
            f"{old['python']}/{new['python']}",
        )
        installed_new = Path(new["django_ox_file"]).parent
        mismatched = compare_trees(
            tree_files(REPO / "src" / "django_ox"), installed_new
        )
        head = git("rev-parse", "HEAD").strip()
        dirty = git("status", "--porcelain", "--", "src").strip()
        self.check(
            ok=not mismatched,
            claim="every file the candidate imports is byte-identical to src/ in "
            "the checkout",
            observed=f"checkout HEAD {head}, src/ "
            f"{'modified' if dirty else 'clean'}, differing files "
            f"{mismatched or 'none'}",
        )
        tag = f"v{self.args.old_version}"
        if git("tag", "--list", tag).strip():
            installed_old = Path(old["django_ox_file"]).parent
            mismatched = compare_trees(tagged_files(tag), installed_old)
            self.check(
                ok=not mismatched,
                claim=f"every file the old side imports is byte-identical to tag {tag}",
                observed=f"differing files {mismatched or 'none'}",
            )
        else:
            self.say(f"(No {tag} tag in this checkout; the tag comparison is skipped.)")
        self.say()

    def database(self) -> None:
        self.say("## One database, one schema")
        self.say()
        done = self.must(self.new, ["-c", RECREATE_DB])
        self.say(
            f"`{NEW}`: {done.stdout.strip()} on {self.args.db_host}:{self.args.db_port}"
        )
        self.must(self.new, ["manage.py", "migrate", "--noinput"])
        self.say(f"`{NEW}`: manage.py migrate --noinput, exit 0")
        shown = {
            env.label: self.must(
                env, ["manage.py", "showmigrations", "django_ox"]
            ).stdout
            for env in (self.old, self.new)
        }
        self.say(f"`manage.py showmigrations django_ox` from `{OLD}`:")
        self.block(shown[OLD])
        applied = [line for line in shown[OLD].splitlines() if "[X]" in line]
        self.check(
            ok=shown[OLD] == shown[NEW]
            and len(applied) == len(shown[OLD].splitlines()) - 1,
            claim="both sides list the same migrations, all applied by the "
            "candidate's migrate",
            observed=f"identical output: {shown[OLD] == shown[NEW]}, "
            f"{len(applied)} applied",
        )
        for env in (self.old, self.new):
            done = self.run(env, ["manage.py", "migrate", "--check"])
            pending = self.run(
                env,
                ["manage.py", "makemigrations", "django_ox", "--check", "--dry-run"],
            )
            self.check(
                ok=done.returncode == 0 and pending.returncode == 0,
                claim=f"{env.label} sees no unapplied migration and no model change "
                "without a migration",
                observed=f"migrate --check exit {done.returncode}; makemigrations "
                f"--check exit {pending.returncode}: {pending.stdout.strip()}",
            )
        old_dir = Path(self.probe(self.old, "identity")["django_ox_file"]).parent
        new_dir = Path(self.probe(self.new, "identity")["django_ox_file"]).parent
        differ = compare_trees(
            tree_files(old_dir / "migrations"), new_dir / "migrations"
        )
        self.check(
            ok=not differ,
            claim="the migration files both sides ship are byte-identical",
            observed=f"{len(tree_files(new_dir / 'migrations'))} files, differing "
            f"{differ or 'none'}",
        )
        self.say()

    # -- scenarios ---------------------------------------------------------

    def scenario_a(self) -> None:
        self.say("## A. The candidate enqueues, a 1.4.0 worker runs")
        self.say()
        self.reset()
        budget5 = self.enqueue(self.new, "mixapp.compat.fails_budget5", budget=5)[0]
        plain = self.enqueue(self.new, "mixapp.shared.always_fails", budget=2)[0]
        worker = self.start_worker(self.old, "A-old")
        rows = self.settle([budget5, plain], [worker])
        self.stop_worker(worker)
        markers = self.read_markers()
        self.table(rows, markers)
        self.expect_row(
            rows,
            markers,
            budget5,
            claim="the 1.4.0 worker spends the task's own budget of 5 stored by "
            "the candidate, not its backend's MAX_ATTEMPTS of 2",
            status="FAILED",
            attempts=5,
            error="builtins.ValueError",
            ran_on=OLD,
        )
        self.expect_row(
            rows,
            markers,
            plain,
            claim="a task that declares nothing gets the backend's 2",
            status="FAILED",
            attempts=2,
            error="builtins.ValueError",
            ran_on=OLD,
        )
        sequence = [m["attempt"] for m in self.starts(markers, budget5)]
        self.check(
            ok=sequence == [1, 2, 3, 4, 5],
            claim="each of the five claims ran the body once, in order",
            observed=f"attempt numbers the body saw: {sequence}",
        )
        seen = self.probe(self.old, "result", budget5)[0]
        self.check(
            ok=seen.get("status") == "FAILED" and seen.get("attempts") == 5,
            claim="1.4.0's get_result reads the candidate's row",
            observed=seen,
        )
        self.say()

    def scenario_b(self) -> None:
        self.say("## B. 1.4.0 enqueues, a candidate worker runs")
        self.say()
        self.say(
            "The candidate worker runs with MAX_ATTEMPTS=3, so its own backend "
            "default differs from the 2 that 1.4.0 stored."
        )
        self.reset()
        plain = self.enqueue(self.old, "mixapp.shared.always_fails", budget=2)[0]
        budget5 = self.enqueue(self.old, "mixapp.compat.fails_budget5", budget=2)[0]
        slow = self.enqueue(self.old, "mixapp.compat.slow_with_timeout", budget=2)[0]
        fine = self.enqueue(self.old, "mixapp.shared.succeeds", budget=2)[0]
        worker = self.start_worker(self.new, "B-new", {"MIX_MAX_ATTEMPTS": "3"})
        rows = self.settle([plain, budget5, slow, fine], [worker])
        self.stop_worker(worker)
        markers = self.read_markers()
        self.table(rows, markers)
        self.expect_row(
            rows,
            markers,
            plain,
            claim="the candidate worker spends the 2 that 1.4.0 stored, not its "
            "own backend's 3",
            status="FAILED",
            attempts=2,
            error="builtins.ValueError",
            ran_on=NEW,
        )
        self.expect_row(
            rows,
            markers,
            budget5,
            claim="a stored budget of 2 beats the max_attempts=5 the task declares "
            "in the candidate's code",
            status="FAILED",
            attempts=2,
            error="builtins.ValueError",
            ran_on=NEW,
        )
        self.expect_row(
            rows,
            markers,
            slow,
            claim="the task's live timeout=1 applies to a row 1.4.0 enqueued: both "
            "attempts time out",
            status="FAILED",
            attempts=2,
            error=TIMEOUT_ERROR,
            ran_on=NEW,
        )
        ended = [m for m in markers if m["event"] == "end" and m["id"] == slow]
        self.check(
            ok=not ended,
            claim="no timed-out attempt reached the end of its body",
            observed=f"{len(ended)} end marker(s)",
        )
        self.expect_row(
            rows,
            markers,
            fine,
            claim="an ordinary task 1.4.0 enqueued succeeds on the candidate",
            status="SUCCESSFUL",
            attempts=1,
            ran_on=NEW,
        )
        seen = self.probe(self.new, "result", plain)[0]
        self.check(
            ok=seen.get("task_class") == "PolicyTask"
            and "task_max_attempts" in seen
            and seen["task_max_attempts"] is None,
            claim="the candidate's get_result on a 1.4.0 row reports the task as "
            "the candidate declares it, a PolicyTask declaring no max_attempts, "
            "not the stored 2",
            observed=seen,
        )
        self.say()

    def scenario_c(self) -> None:
        self.say("## C. A declared backoff and timeout: 1.4.0 against the candidate")
        self.say()
        shape = self.must(self.old, ["-c", IMPORT_COMPAT]).stdout.strip()
        self.say(f"`{OLD}` imports mixapp.compat: {shape}")
        compat = json.loads(shape)
        self.check(
            ok=compat["POLICY_SUPPORTED"] is False and compat["class"] == "Task",
            claim="the import-compatible module imports under 1.4.0, as plain tasks",
            observed=compat,
        )
        for side, env in (("C1", self.old), ("C2", self.new)):
            self.say()
            self.say(f"### {side}: the candidate enqueues, a `{env.label}` worker runs")
            self.say()
            self.reset()
            stops = self.enqueue(self.new, "mixapp.compat.stops_at_once", budget=4)[0]
            slow = self.enqueue(self.new, "mixapp.compat.slow_with_timeout", budget=2)[
                0
            ]
            worker = self.start_worker(
                env, f"{side}-{'old' if env is self.old else 'new'}"
            )
            rows = self.settle([stops, slow], [worker])
            self.stop_worker(worker)
            markers = self.read_markers()
            self.table(rows, markers)
            calls = [m for m in markers if m["event"] == "backoff_called"]
            ended = [m for m in markers if m["event"] == "end" and m["id"] == slow]
            log = worker.log.read_text(errors="replace")
            if env is self.old:
                self.expect_row(
                    rows,
                    markers,
                    stops,
                    claim="1.4.0 ignores backoff=stop_now and retries to the "
                    "stored budget of 4",
                    status="FAILED",
                    attempts=4,
                    error="builtins.ValueError",
                    ran_on=OLD,
                )
                self.check(
                    ok=not calls,
                    claim="1.4.0 never calls the backoff",
                    observed=f"{len(calls)} backoff call(s)",
                )
                self.expect_row(
                    rows,
                    markers,
                    slow,
                    claim="1.4.0 ignores timeout=1: the 3-second body runs to the end",
                    status="SUCCESSFUL",
                    attempts=1,
                    ran_on=OLD,
                )
                self.check(
                    ok=len(ended) == 1 and rows[slow]["return_value"] == "finished",
                    claim="the body finished and its return value was stored",
                    observed=f"{len(ended)} end marker(s), return value "
                    f"{rows[slow]['return_value']!r}",
                )
            else:
                self.expect_row(
                    rows,
                    markers,
                    stops,
                    claim="the candidate calls backoff=stop_now after attempt 1 and "
                    "fails the task there, budget 4 notwithstanding",
                    status="FAILED",
                    attempts=1,
                    error="builtins.ValueError",
                    ran_on=NEW,
                )
                snapshot = calls[0] if len(calls) == 1 else {}
                self.check(
                    ok=snapshot.get("attempts") == 1
                    and snapshot.get("max_attempts") == 4
                    and snapshot.get("status") == "FAILED"
                    and snapshot.get("exception") == "ValueError"
                    and "'backoff_declined'" in log,
                    claim="the backoff ran once, saw attempt 1 of 4 as FAILED with "
                    "the task's ValueError, and the log says backoff_declined",
                    observed=f"{len(calls)} call(s), {snapshot or 'none'}; "
                    f"backoff_declined in log: {'backoff_declined' in log}",
                )
                self.expect_row(
                    rows,
                    markers,
                    slow,
                    claim="the candidate enforces timeout=1 on both attempts",
                    status="FAILED",
                    attempts=2,
                    error=TIMEOUT_ERROR,
                    ran_on=NEW,
                )
                self.check(
                    ok=not ended,
                    claim="no timed-out attempt reached the end of its body",
                    observed=f"{len(ended)} end marker(s)",
                )
        self.say()

    def scenario_d(self) -> None:
        self.say("## D. The cold import: new keyword arguments under 1.4.0")
        self.say()
        self.say("### D1: importing a module that declares policy")
        self.say()
        old = self.run(self.old, ["-c", IMPORT_DECLARED])
        tail = [line for line in old.stderr.splitlines() if line.strip()]
        self.say(
            f"`{OLD}`: python -c 'django.setup(); import mixapp.declared' exits "
            f"{old.returncode}; the end of its traceback:"
        )
        self.block("\n".join(tail[-6:]))
        self.check(
            ok=old.returncode != 0
            and bool(tail)
            and tail[-1].startswith("TypeError:")
            and "unexpected keyword argument 'max_attempts'" in tail[-1],
            claim="under 1.4.0 the module does not import: @task refuses max_attempts",
            observed=tail[-1] if tail else "(no stderr)",
        )
        new = self.run(self.new, ["-c", IMPORT_DECLARED])
        self.check(
            ok=new.returncode == 0
            and new.stdout.strip() == "PolicyTask 3 30 retry_in_one_second",
            claim="under the candidate it imports, as a PolicyTask with its policy",
            observed=f"exit {new.returncode}: "
            f"{new.stdout.strip() or new.stderr[-300:]}",
        )
        for side, env in (("D2", self.old), ("D2 control", self.new)):
            self.say()
            self.say(
                f"### {side}: rows of that task, enqueued by the candidate, claimed "
                f"by a `{env.label}` worker"
            )
            self.say()
            self.reset()
            tid = self.enqueue(self.new, "mixapp.declared.declared_succeeds", budget=3)[
                0
            ]
            worker = self.start_worker(env, side.replace(" ", "-"))
            rows = self.settle([tid], [worker])
            self.stop_worker(worker)
            markers = self.read_markers()
            self.table(rows, markers)
            if env is self.old:
                self.say(
                    f"Last line of the stored traceback: `{rows[tid]['last_error']}`"
                )
                self.expect_row(
                    rows,
                    markers,
                    tid,
                    claim="a 1.4.0 worker cannot import the task: every claim fails "
                    "with TypeError, the body never runs, and the row ends FAILED "
                    "once its stored 3 attempts are spent",
                    status="FAILED",
                    attempts=3,
                    error="builtins.TypeError",
                )
                self.check(
                    ok=not self.starts(markers, tid),
                    claim="the body never ran",
                    observed=f"{len(self.starts(markers, tid))} start marker(s)",
                )
                # 1.4.0 catches only ImportError when it rebuilds the result
                # for the final attempt's task_finished signal, so this
                # TypeError escapes there: the row is already FAILED, but
                # the log says worker_error instead of task_failed and no
                # signal is sent. Pinned so the rollout docs can say so.
                log = worker.log.read_text(errors="replace")
                unhandled = f"Unhandled error executing task id={tid}" in log
                failed_line = any(
                    f"Task id={tid} " in line and "failed after" in line
                    for line in log.splitlines()
                )
                self.check(
                    ok=unhandled and not failed_line,
                    claim="1.4.0 logs the final claim as worker_error ('Unhandled "
                    "error executing task'), with no task_failed line",
                    observed=f"unhandled-error line: {unhandled}, task_failed line: "
                    f"{failed_line}",
                )
            else:
                self.expect_row(
                    rows,
                    markers,
                    tid,
                    claim="the same row shape succeeds at once on a candidate worker",
                    status="SUCCESSFUL",
                    attempts=1,
                    ran_on=NEW,
                )
        self.say()
        self.say("### D3: a 1.4.0 worker whose app imports that module at startup")
        self.say()
        self.reset()
        at_ready = {"MIX_IMPORT_DECLARED_AT_READY": "1"}
        old = self.run(
            self.old,
            ["manage.py", "ox_worker", "--interval", "0.05"],
            extra=at_ready,
            timeout=60,
        )
        tail = [line for line in (old.stdout + old.stderr).splitlines() if line.strip()]
        self.say(
            f"`{OLD}`: manage.py ox_worker exits {old.returncode}; the end of "
            "its output:"
        )
        self.block("\n".join(tail[-3:]))
        self.check(
            ok=old.returncode != 0
            and bool(tail)
            and "unexpected keyword argument 'max_attempts'" in tail[-1],
            claim="the 1.4.0 worker does not start at all",
            observed=tail[-1] if tail else "(no output)",
        )
        worker = self.start_worker(self.new, "D3-control", at_ready)
        time.sleep(STARTUP_WINDOW)
        self.say(f"`{NEW}` worker with the same app, after {STARTUP_WINDOW:.0f}s:")
        self.stop_worker(worker)
        self.say()

    def scenario_e(self) -> None:
        self.say("## E. Both workers on one queue, rows from both sides")
        self.say()
        self.reset()
        from_new = self.enqueue(
            self.new, "mixapp.compat.fails_budget5", count=6, budget=5
        )
        from_old = self.enqueue(
            self.old, "mixapp.compat.fails_budget5", count=6, budget=2
        )
        ok_new = self.enqueue(self.new, "mixapp.shared.succeeds", count=4, budget=2)
        ok_old = self.enqueue(self.old, "mixapp.shared.succeeds", count=4, budget=2)
        ids = from_new + from_old + ok_new + ok_old
        workers = [
            self.start_worker(self.old, "E-old"),
            self.start_worker(self.new, "E-new"),
        ]
        rows = self.settle(ids, workers)
        for worker in workers:
            self.stop_worker(worker)
        markers = self.read_markers()
        self.table(rows, markers)
        wrong = [
            (rows[i]["status"], rows[i]["attempts"])
            for i in from_new
            if (rows[i]["status"], rows[i]["attempts"]) != ("FAILED", 5)
        ] + [
            (rows[i]["status"], rows[i]["attempts"])
            for i in from_old
            if (rows[i]["status"], rows[i]["attempts"]) != ("FAILED", 2)
        ]
        self.check(
            ok=not wrong,
            claim="all 12 failing rows spent exactly their stored budget: 5 for the "
            "candidate's, 2 for 1.4.0's",
            observed=f"{len(wrong)} row(s) off: {wrong or 'none'}",
        )
        good = [rows[i]["status"] for i in ok_new + ok_old]
        self.check(
            ok=good == ["SUCCESSFUL"] * 8,
            claim="all 8 succeeding rows succeeded",
            observed=Counter(good),
        )
        mismatch = []
        for tid in ids:
            seen = [m["attempt"] for m in self.starts(markers, tid)]
            row = rows[tid]
            if (
                seen != list(range(1, row["attempts"] + 1))
                or len(row["worker_ids"]) != row["attempts"]
            ):
                mismatch.append(
                    (tid[:8], seen, row["attempts"], len(row["worker_ids"]))
                )
        self.check(
            ok=not mismatch,
            claim="every claim ran the body exactly once, attempt numbers in order, "
            "and the row's claim history matches",
            observed=f"{len(mismatch)} row(s) off: {mismatch or 'none'}",
        )
        by_env = Counter(m["env"] for m in markers if m["event"] == "start")
        shared_rows = sum(
            1 for tid in ids if len({m["env"] for m in self.starts(markers, tid)}) == 2
        )
        self.check(
            ok=by_env[OLD] > 0 and by_env[NEW] > 0,
            claim="both workers executed attempts",
            observed=f"attempts by side {dict(by_env)}; rows whose attempts ran on "
            f"both sides: {shared_rows}",
        )
        self.say()


def tree_files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    }


def compare_trees(expected: dict[str, bytes], installed_root: Path) -> list[str]:
    installed = tree_files(installed_root)
    return sorted(
        name
        for name in expected.keys() | installed.keys()
        if expected.get(name) != installed.get(name)
    )


def git(*argv: str) -> str:
    done = subprocess.run(  # noqa: S603
        ["git", "-C", str(REPO), *argv],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout


def tagged_files(tag: str) -> dict[str, bytes]:
    prefix = "src/django_ox/"
    names = git("ls-tree", "-r", "--name-only", tag, "--", prefix).split()
    out = {}
    for name in names:
        blob = subprocess.run(  # noqa: S603
            ["git", "-C", str(REPO), "show", f"{tag}:{name}"],  # noqa: S607
            capture_output=True,
            check=True,
        )
        out[name.removeprefix(prefix)] = blob.stdout
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--old-version", default="1.4.0")
    parser.add_argument("--django", default="6.1.1")
    parser.add_argument("--psycopg", default="3.3.6")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-name", default="ox_mixed_fleet")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    password = os.environ.get("MIX_DB_PASSWORD", os.environ.get("PGPASSWORD"))
    if password is None:
        parser.error("set MIX_DB_PASSWORD (or PGPASSWORD) to the server's password")

    harness = Harness(args, password)
    started = time.monotonic()
    harness.say("# Mixed fleet: django-ox 1.4.0 and the candidate on one table")
    harness.say()
    harness.say(
        f"Checkout {REPO} at {git('rev-parse', 'HEAD').strip()}; database "
        f"{args.db_name} on {args.db_host}:{args.db_port}; work directory "
        f"{harness.workdir}."
    )
    harness.say()
    try:
        harness.write_project()
        harness.install()
        harness.identity()
        harness.database()
        harness.scenario_a()
        harness.scenario_b()
        harness.scenario_c()
        harness.scenario_d()
        harness.scenario_e()
    except HarnessFailure as failure:
        harness.say(f"STOPPED: {failure}")
        harness.failures.append(str(failure).splitlines()[0])
    elapsed = time.monotonic() - started
    harness.say("## Result")
    harness.say()
    if harness.failures:
        harness.say(
            f"FAILED: {len(harness.failures)} of {harness.checks} checks, in "
            f"{elapsed:.0f}s:"
        )
        for claim in harness.failures:
            harness.say(f"- {claim}")
    else:
        harness.say(f"PASSED: all {harness.checks} checks, in {elapsed:.0f}s.")
    if args.report is not None:
        args.report.write_text("\n".join(harness.report) + "\n")
    return 1 if harness.failures else 0


if __name__ == "__main__":
    sys.exit(main())
