"""
A WORKER_CLASS subclass of OxpullWorker that narrows claim_filter_q() alone
must keep its condition on every database, keep the product's own conditions,
and not be refused.

django-ox promises that a subclass overriding claim_filter_q() and not
claim_filter_sql() "keeps its condition on every database"
(django_ox/worker.py, Worker.claim_filter_q). On PostgreSQL django-ox notices
the empty fragment and claims through SELECT ... FOR UPDATE SKIP LOCKED,
which applies the Q, and logs claim_filter_sql_missing once
(Worker._postgresql_honours_the_claim_filter). Up to 1.2.0 OxpullWorker wrote
a fragment of its own: the dependency gate on a WORKFLOWS backend, and the
rate-limit exclusion once a limit is spent. A subclass inherited that
fragment, so django-ox read it as a subclass that wrote both hooks and took
the single-statement claim. That claim read only the fragment. The gate and
the limit held, and the subclass's own condition was gone. SQLite and MySQL
built the candidate from claim_filter_q() and kept it.

OxpullWorker now binds its own conditions on the instance, in __init__, and
reads the class's hooks past them. django-ox sees a subclass that wrote
claim_filter_q() alone as exactly that, on every database.

PinnedWorker below represents a custom pool that leaves the top priority band
to another pool (the same shape as region, tenant or hardware pinning). It
extends the product's condition through super(), as an override must, and
it is wired in through WORKER_CLASS. The worker is built by
django_ox.worker.worker_class(), as ox_worker builds it. A subclass that
replaces the condition without calling super() isn't supported, and nothing
here tests it.

What a fix has to do to pass:

  * Claim exactly the rows that both the subclass and the product allow. The
    pinned worker claims the row inside its band and then nothing, and the
    row above the band stays READY with no attempt spent. In a workflow it
    runs root, then child, and leaves the node above the band untouched.
    With a rate limit spent, the limited row is untouched as well.
  * Keep the product's conditions. child outranks root, so a claim that kept
    the band but lost the dependency condition would run child first. One
    case clears child's run_after so the deferral can't hide that. A claim
    that kept the band but lost the rate limit would take the limited row.
  * Refuse nothing. This subclass is supported, so each of these fails the
    test as a refusal: an Error-level system check in this process, a
    non-zero `manage.py check`, an ox_worker that exits at start-up, and
    ImproperlyConfigured from worker_class(), from building the worker, or
    from add() or seal(). Warnings are allowed.
  * The claim path isn't asserted. django-ox's fallback passes, and so would
    a single statement that carried the subclass's condition.

So a fix that refuses the subclass fails here, and so does one that keeps
today's single-statement claim, or one that stops the pinned worker claiming
what it should.

Every test here passes on SQLite, MySQL and PostgreSQL. They were written
against 1.2.0, where the PostgreSQL cases were expected failures; the markers
came off once the instance-bound claim filter landed. A refusal or a broken
setup goes through pytest.fail, so it reports as a failure and not as a wrong
claim. The "default" parameters are the controls: the same subclass on a
backend that itself declares neither feature. The last test is the same drop
on a rate-limited backend.
"""

from __future__ import annotations

import copy
import inspect
import logging
import os
import re
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.db import connection
from django.db.models import Q
from django.test import override_settings

from django_ox.models import OxTask
from django_ox.worker import worker_class
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .subprocesses import django_ox_path
from .test_rate_limits import ECHO, captured_sql, open_window, rate_limits, window
from .test_workflows import drain, order

# Django 6.1 hands every configured alias to the model and field checks
# unless the command pins one, so a system check run here opens "replica"
# to ask SQLite or MySQL whether it supports JSONField. That read leaves
# the alias unwritten, but it has to be declared or the test database
# guard refuses the connection.
pytestmark = pytest.mark.django_db(databases=["default", "replica"])

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

BAND = 100  # PinnedWorker claims only priority < BAND
BELOW_BAND = 50  # inside the band, and above priority 0


class PinnedWorker(OxpullWorker):
    """
    A user's pool that leaves the top priority band to another pool.

    Extends the product's claim condition through super() and overrides
    claim_filter_q() alone, which django-ox documents as keeping the
    condition on every database.
    """

    def claim_filter_q(self):
        base = super().claim_filter_q()
        pinned = Q(priority__lt=100)
        return pinned if base is None else base & pinned


WORKER_PATH = f"{__name__}.PinnedWorker"

TASK_ON = {
    "default": tasks.default_step,
    "flows": tasks.step,
    "flows_deferred": tasks.deferred_step,
}


@contextmanager
def pinned(*aliases):
    """The suite's TASKS, with PinnedWorker as WORKER_CLASS on these aliases."""
    configured = copy.deepcopy(settings.TASKS)
    for alias in aliases:
        configured[alias]["OPTIONS"]["WORKER_CLASS"] = WORKER_PATH
    with override_settings(TASKS=configured):
        yield


def _is_suite_fixture(message):
    """tests_oxpull/settings.py's deliberate oxpull.E007 on "flows_ungated"."""
    return message.id == "oxpull.E007" and "'flows_ungated'" in message.msg


def build(alias):
    """
    The worker ox_worker would start on `alias`, once the system checks it
    runs at start-up have passed.

    A refusal fails the test through pytest.fail, not an assertion, so it
    never counts as the expected failure: an Error-level check (the suite's
    own oxpull.E007 fixture aside), or ImproperlyConfigured from
    worker_class() or from building the worker.
    """
    errors = [
        f"{message.id}: {message.msg}"
        for message in checks.run_checks()
        if message.level >= checks.ERROR and not _is_suite_fixture(message)
    ]
    if errors:
        pytest.fail(f"{alias}: Error-level checks refused PinnedWorker: {errors}")
    try:
        cls = worker_class(alias)
        worker = cls(backend_alias=alias, backoff_initial=0, poll_interval=0.01)
    except ImproperlyConfigured as exc:
        pytest.fail(f"{alias}: refused PinnedWorker: ImproperlyConfigured: {exc}")
    if cls is not PinnedWorker:
        pytest.fail(f"premise: WORKER_CLASS on {alias!r} built {cls}, not PinnedWorker")
    return worker


def claim_all(worker, limit=5):
    """Claim until the worker finds nothing, without running anything."""
    claimed = []
    while len(claimed) < limit:
        row = worker.claim_one()
        if row is None:
            return claimed
        claimed.append(row)
    pytest.fail(f"premise: still claiming after {limit} rows")


def untouched(task_id):
    """READY, with no attempt spent and no lease ever taken."""
    row = OxTask.objects.get(pk=task_id)
    return (row.status, row.attempts, row.lease_epoch) == (OxTask.Status.READY, 0, 0)


def state_of(task_id):
    row = OxTask.objects.get(pk=task_id)
    return f"{row.status} attempts={row.attempts} lease_epoch={row.lease_epoch}"


def pin_in(statements):
    return any(re.search(r"priority[\"`]? < ", sql) for sql in statements)


def note(line):
    """Which outcome a test saw, shown for passing tests under -rA."""
    sys.stdout.write(line + "\n")


# -- in process, through worker_class() ----------------------------------


@pytest.mark.parametrize(
    "alias",
    [
        "default",
        "flows",
        "flows_deferred",
    ],
)
def test_a_pinned_worker_claims_its_band_and_nothing_else(alias, caplog):
    task = TASK_ON[alias]
    with pinned(alias):
        excluded = task.using(priority=BAND).enqueue("excluded")
        allowed = task.using(priority=0).enqueue("allowed")
        worker = build(alias)
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            captured_sql(worker._db_alias) as statements,
        ):
            rows = claim_all(worker)
        fragment = bool(worker.claim_filter_sql()[0])
    label = {excluded.id: "excluded", allowed.id: "allowed"}
    claimed = [label.get(str(row.id), row.args) for row in rows]
    notice = any(
        getattr(record, "event", None) == "claim_filter_sql_missing"
        for record in caplog.records
    )
    note(
        f"OUTCOME {connection.vendor}/{alias}: claimed {claimed}; pin in the SQL: "
        f"{pin_in(statements)}; notice: {notice}"
    )
    # Claim order is priority DESC, so a claim without the band takes
    # "excluded" first.
    assert claimed == ["allowed"] and untouched(excluded.id), (
        f"{connection.vendor}/{alias}: PinnedWorker claimed {claimed}, not "
        f"['allowed']. Excluded row: {state_of(excluded.id)}. Statements: "
        f"{[sql.split()[0] for sql in statements]}; pin in the SQL: "
        f"{pin_in(statements)}; django-ox claim_filter_sql_missing notice: {notice}; "
        f"claim_filter_sql() fragment non-empty: {fragment}"
    )


@pytest.mark.parametrize(
    ("alias", "gate_alone"),
    [
        pytest.param("flows", False, id="flows"),
        pytest.param("flows_deferred", False, id="flows_deferred"),
        pytest.param("flows_deferred", True, id="flows_deferred-put-back-to-ready"),
    ],
)
def test_a_pinned_worker_runs_its_band_of_a_workflow_in_order(alias, gate_alone):
    """
    root (priority 0) -> {vip (priority 100), child (priority 50)}. The
    pinned pool runs root, then child, and leaves vip untouched for the pool
    that owns the band.

    child outranks root, so a claim that lost the dependency condition runs
    child first. The blocked nodes wait, which would hide that, so the
    put-back case moves them to READY after the build, the shape a django-ox
    retry leaves. Only the condition in the pinned worker's claim holds them
    then.
    """
    task = TASK_ON[alias]
    with pinned(alias):
        try:
            flow = workflows.create(f"banded-{alias}")
            flow.add("root", task.using(priority=0), "root")
            flow.add("vip", task.using(priority=BAND), "vip", depends_on="root")
            flow.add(
                "child", task.using(priority=BELOW_BAND), "child", depends_on="root"
            )
            flow.seal()
        except ImproperlyConfigured as exc:
            pytest.fail(f"{alias}: add() or seal() refused PinnedWorker: {exc}")
        ids = flow.node_ids()
        if gate_alone:
            OxTask.objects.filter(pk__in=[ids["vip"], ids["child"]]).update(
                status=OxTask.Status.READY
            )
        worker = build(alias)
        ran = drain(worker, limit=10)
    states = {name: state_of(pk) for name, pk in ids.items()}
    note(f"OUTCOME {connection.vendor}/{alias}: ran {order()}; nodes {states}")
    assert order() == ["root", "child"] and untouched(ids["vip"]), (
        f"{connection.vendor}/{alias}: PinnedWorker ran {order()} ({ran} tasks), "
        f"not ['root', 'child'] with vip untouched; nodes {states}"
    )


# -- a real `ox_worker` process -------------------------------------------


def process_env(tmp_path):
    """
    A separate process on this session's test database. PinnedWorker lives in
    a user module of its own (its source, verbatim) and is WORKER_CLASS on
    "default" and "flows". The workflow reconciler is scheduled so the checks
    are otherwise silent, and "flows_ungated" stops declaring WORKFLOWS: it is
    the suite's deliberate oxpull.E007 fixture and would stop ox_worker at
    start-up for a reason that has nothing to do with PinnedWorker.
    """
    source = inspect.getsource(PinnedWorker)
    (tmp_path / "customer_workers.py").write_text(
        "from django.db.models import Q\n"
        "from oxpull.worker import OxpullWorker\n\n\n" + source
    )
    (tmp_path / "pinned_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)  # noqa: F405\n"
        "WORKER = 'customer_workers.PinnedWorker'\n"
        "for alias in ('default', 'flows'):\n"
        "    TASKS[alias]['OPTIONS']['WORKER_CLASS'] = WORKER\n"
        "TASKS['flows_ungated']['OPTIONS']['WORKFLOWS'] = False\n"
        "TASKS['default']['OPTIONS']['SCHEDULES']['oxpull-workflow-reconcile'] = "
        "{'task': 'oxpull.workflows.reconcile', 'cron': '* * * * *'}\n"
        "LOGGING = {\n"
        "    'version': 1,\n"
        "    'disable_existing_loggers': False,\n"
        "    'handlers': {'console': {'class': 'logging.StreamHandler'}},\n"
        "    'loggers': {'django_ox': {'handlers': ['console'], 'level': 'DEBUG'}},\n"
        "}\n"
    )
    name = connection.settings_dict["NAME"]
    if connection.vendor == "sqlite":
        name = Path(name).resolve()
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "pinned_settings",
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


def stop(process):
    """SIGTERM, as a deploy does; the worker drains and exits."""
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    return process.returncode


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("alias", ["default", "flows"])
def test_an_ox_worker_process_running_a_pinned_worker_class_stays_in_its_band(
    alias, tmp_path
):
    env = process_env(tmp_path)
    # Prove which oxpull the process imports and which class ox_worker builds.
    probe = run_python(
        env,
        "-c",
        "import django; django.setup(); import oxpull; "
        "from django_ox.worker import worker_class; "
        "from oxpull.worker import OxpullWorker; "
        f"cls = worker_class({alias!r}); print(oxpull.__file__); "
        "print(cls.__module__ + '.' + cls.__qualname__, issubclass(cls, OxpullWorker))",
    )
    lines = probe.stdout.split()
    if "ImproperlyConfigured" in probe.stderr:
        pytest.fail(f"{alias}: worker_class() refused PinnedWorker: {probe.stderr}")
    if not (
        len(lines) == 3
        and lines[0].startswith(str(SRC))
        and lines[1:] == ["customer_workers.PinnedWorker", "True"]
    ):
        pytest.fail(f"premise: the process imports {lines}: {probe.stderr}")
    check = run_python(env, "-m", "django", "check")
    check_output = (check.stdout + check.stderr).strip()
    if check.returncode != 0:
        pytest.fail(
            f"{alias}: `manage.py check` refused PinnedWorker (exit "
            f"{check.returncode}): {check_output}"
        )

    task = TASK_ON[alias]
    excluded = task.using(priority=BAND).enqueue("excluded")
    allowed = task.using(priority=0).enqueue("allowed")
    label_of = {excluded.id: "excluded", allowed.id: "allowed"}

    log_path = tmp_path / "ox_worker.log"
    with log_path.open("w") as log:
        worker = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                "django",
                "ox_worker",
                "--backend",
                alias,
                "--concurrency",
                "1",
                "--interval",
                "0.05",
            ],
            env=env,
            cwd=REPO_ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            # Claim order is priority DESC, so a claim without the band takes
            # "excluded" before "allowed": once "allowed" has run, the
            # excluded row's fate is already decided. The deadline is only a
            # ceiling for a loaded machine; a passing run leaves at once.
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and worker.poll() is None:
                if OxTask.objects.get(pk=allowed.id).status == OxTask.Status.SUCCESSFUL:
                    break
                time.sleep(0.1)
            exited_on_its_own = worker.poll() is not None
        finally:
            code = stop(worker)
    text = log_path.read_text()
    if exited_on_its_own:
        pytest.fail(
            f"{alias}: ox_worker with WORKER_CLASS customer_workers.PinnedWorker "
            f"exited {code} on its own. Log tail: {text[-2000:]}"
        )
    claimed = [
        label_of[task_id]
        for task_id in re.findall(r"Claimed task id=([0-9a-f-]{36}) ", text)
        if task_id in label_of
    ]
    notice = "but not claim_filter_sql()" in text
    note(
        f"OUTCOME {connection.vendor}/{alias} process: claimed {claimed}; excluded "
        f"{state_of(excluded.id)}; exit {code}; notice in log: {notice}; check: "
        f"{check_output!r}"
    )
    assert claimed == ["allowed"] and untouched(excluded.id) and code == 0, (
        f"{connection.vendor}: `ox_worker --backend {alias}` with WORKER_CLASS "
        f"customer_workers.PinnedWorker claimed {claimed}, not ['allowed']; "
        f"excluded row {state_of(excluded.id)}, allowed row {state_of(allowed.id)}; "
        f"exit {code}. django-ox claim_filter_sql_missing notice in its log: "
        f"{notice}. `manage.py check` in that process: {check_output!r}"
    )


# -- scope: the same class on a rate-limited backend (oxpull 1.2.0 code) ----


def test_scope_a_pinned_worker_on_a_rate_limited_backend_keeps_its_band_once_a_limit_is_spent():  # noqa: E501
    with rate_limits(
        {"echo": {"limit": 1, "per": 300, "tasks": [ECHO]}}, WORKER_CLASS=WORKER_PATH
    ):
        open_window("echo")
        tasks.plain_echo.enqueue("spend")
        worker = build("default")
        if not (worker.run_once() and window("echo").used == 1):
            pytest.fail("premise: the pinned worker did not spend the echo limit")
        excluded = tasks.default_step.using(priority=BAND).enqueue("excluded")
        limited = tasks.plain_echo.using(priority=BELOW_BAND).enqueue("limited")
        allowed = tasks.default_step.using(priority=0).enqueue("allowed")
        with captured_sql(worker._db_alias) as statements:
            rows = claim_all(worker)
        used = window("echo").used
    label = {excluded.id: "excluded", limited.id: "limited", allowed.id: "allowed"}
    claimed = [label.get(str(row.id), row.args) for row in rows]
    note(
        f"OUTCOME {connection.vendor}/default+RATE_LIMITS: claimed {claimed}; "
        f"pin in the SQL: {pin_in(statements)}; echo used {used}"
    )
    assert (
        claimed == ["allowed"]
        and untouched(excluded.id)
        and untouched(limited.id)
        and used == 1
    ), (
        f"{connection.vendor}/default+RATE_LIMITS: PinnedWorker claimed {claimed} "
        f"once the echo limit was spent, not ['allowed']. Excluded row: "
        f"{state_of(excluded.id)}; limited row: {state_of(limited.id)}; echo used "
        f"{used}. Statements: {[sql.split()[0] for sql in statements]}; pin in the "
        f"SQL: {pin_in(statements)}"
    )
