"""
A correctly configured worker on another backend alias, sharing a queue
name, must not claim a workflow node before its upstreams have succeeded.

A django-ox claim filters on status, run_after and queue_name, never on
backend_name, and a worker started without --queues takes every queue its
own backend lists (every queue when QUEUES is empty). So a second
OxpullBackend alias on the same database, with WORKER_CLASS set correctly and
no WORKFLOWS, reaches a workflow's nodes. tests_oxpull/settings.py is itself this
layout: "limited" (WORKER_CLASS = OxpullWorker, RATE_LIMITS, no WORKFLOWS)
serves queue "default" beside "flows". Its "default" alias serves the same
names but sets no WORKER_CLASS, so where a test needs "default" to be a
correctly configured OxpullBackend alias it constructs OxpullWorker directly (in
process) or adds WORKER_CLASS (the real process).

What a fix has to do to pass:

  * Ordering holds whatever the other alias runs, for two reasons that are
    tested apart. Every OxpullWorker gates its claim whenever any backend on
    its database declares WORKFLOWS, so the gate holds a blocked node even
    after it has been put back to READY. And every blocked node waits in
    django-ox's WAITING status, including one the caller gave its own
    run_after, so a worker that does not gate at all cannot claim it either.
  * The other alias still runs the nodes, in order. A fix that stops it
    claiming, or has it skip workflow rows, fails here: each test requires
    the runnable node to be claimed, or every node to run.
  * Nothing is refused. add() and seal() accept the nodes and `manage.py
    check` exits 0, because an Error would stop every ox_worker on upgrade.
    Instead the check reports a Warning, naming both aliases, whenever a
    backend without WORKFLOWS serves a queue that a WORKFLOWS backend uses.
    An empty QUEUES serves every queue, on either side. The Warning is
    still needed once this version gates everywhere: a check in one process
    cannot see which worker another deploy runs on that queue.

Tests that still fail are marked xfail(strict=True, raises=AssertionError),
so a broken setup (the wrong oxpull imported, say) reports as a failure
rather than as the expected one. The WAITING status closed the tests that a
held row answers. The gate everywhere and the Warning are still open.

Not covered: that the Warning is limited to one database. django-ox routes
every backend's task rows to a single database, so one settings module
cannot hold two aliases on different databases.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.db import connection
from django.tasks import task_backends
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .subprocesses import django_ox_path
from .test_workflows import diamond, drain, order, reconcile

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

NODES = ["extract", "transform-a", "transform-b", "load"]
EDGES = [
    ("extract", "transform-a"),
    ("extract", "transform-b"),
    ("transform-a", "load"),
    ("transform-b", "load"),
]


def out_of_order(sequence):
    """Edges whose downstream was claimed before, or without, its upstream."""
    first: dict[str, int] = {}
    for index, label in enumerate(sequence):
        first.setdefault(label, index)
    return [
        (upstream, downstream)
        for upstream, downstream in EDGES
        if downstream in first
        and (upstream not in first or first[upstream] > first[downstream])
    ]


def check_ids(*aliases):
    return {alias: [m.id for m in task_backends[alias].check()] for alias in aliases}


def queues(*aliases):
    return {alias: sorted(task_backends[alias].queues) for alias in aliases}


def oxpull_worker(alias):
    """This branch's OxpullWorker, correctly configured, on `alias`."""
    return OxpullWorker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)


def free_worker(alias):
    """django-ox's own Worker, which has no dependency gate, on `alias`."""
    return Worker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)


def root_and_child(name, *, child_run_after=None, **using):
    """
    root (priority 0) -> child (priority 10): a claim that ignores the
    dependency takes child first.
    """
    flow = workflows.create(name)
    root = flow.add("root", tasks.step.using(priority=0, **using), "root")
    child_task = tasks.step.using(priority=10, **using)
    if child_run_after is not None:
        child_task = child_task.using(run_after=child_run_after)
    child = flow.add("child", child_task, "child", depends_on="root")
    flow.seal()
    return root, child


def claimed_root(claimed, root, child):
    """
    The assertion for a single claim, and what to say when it fails.

    The runnable root has to be the one claimed: None would pass a fix that
    stops the other alias claiming at all, and child is the ordering bug.
    """
    if claimed is None:
        return False, "nothing"
    label = {str(root.id): "root", str(child.id): "child"}.get(
        str(claimed.pk), f"an unrelated task {claimed.args}"
    )
    return label == "root", f"{label} from queue {claimed.queue_name!r}"


def rows(root, child):
    root_row = OxTask.objects.get(pk=root.id)
    child_row = OxTask.objects.get(pk=child.id)
    return (
        f"root {root_row.status}, child {child_row.status} with run_after "
        f"{child_row.run_after}"
    )


# -- in process, the suite's own settings ---------------------------------


@pytest.mark.django_db
def test_a_worker_on_a_plain_alias_sharing_the_queue_claims_the_root_not_the_child():
    """
    "default" is an OxpullBackend with no WORKFLOWS, serving ["default",
    "emails"]; "flows" serves the same names. The worker is OxpullWorker on
    that alias, which is what OPTIONS['WORKER_CLASS'] = OxpullWorker would
    start (tests_oxpull/settings.py leaves the key out; the claim is the same).
    """
    root, child = root_and_child("cross-alias-pair")
    claimed = oxpull_worker("default").claim_one()

    ok, what = claimed_root(claimed, root, child)
    assert ok, (
        f"OxpullWorker(backend_alias='default') claimed {what}; {rows(root, child)}. "
        f"check ids: {check_ids('default', 'flows')}; queues: "
        f"{queues('default', 'flows')}"
    )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "A `oxpull.worker.OxpullWorker` on a backend without workflows puts no "
        "dependency condition in its claim, so a blocked node put back to READY is "
        "claimed ahead of its upstream."
    ),
)
@pytest.mark.django_db
def test_a_worker_on_a_plain_alias_gates_a_blocked_node_that_is_not_deferred():
    """
    The gate on its own. The child is put back to READY after the build, the
    shape a django-ox retry or an application's own UPDATE leaves, so
    nothing but the dependency condition in this worker's claim can hold it.
    A fix that only holds blocked nodes in WAITING fails here.
    """
    root, child = root_and_child("cross-alias-gate")
    moved = OxTask.objects.filter(pk=child.id, status=OxTask.Status.WAITING).update(
        status=OxTask.Status.READY, run_after=None
    )
    if moved != 1:
        pytest.fail(f"setup: the child was not WAITING; {rows(root, child)}")
    claimed = oxpull_worker("default").claim_one()

    ok, what = claimed_root(claimed, root, child)
    assert ok, (
        f"OxpullWorker(backend_alias='default') claimed {what}; {rows(root, child)}. "
        f"check ids: {check_ids('default', 'flows')}"
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    "own_run_after",
    [False, True],
    ids=["without-a-run-after", "with-its-own-run-after-already-due"],
)
def test_a_worker_without_the_gate_cannot_claim_a_blocked_node(own_run_after):
    """
    The held row on its own. django-ox's Worker on "default" stands in for
    a process using the base worker or an earlier version on that queue, which no check
    in this process can see. It has no gate, so only the node's own row can
    keep it out. The second case gives the child a run_after that is already
    due: a fix that holds only nodes without one fails there.
    """
    child_run_after = timezone.now() - timedelta(hours=1) if own_run_after else None
    root, child = root_and_child("cross-alias-fence", child_run_after=child_run_after)
    claimed = free_worker("default").claim_one()

    ok, what = claimed_root(claimed, root, child)
    assert ok, (
        f"django_ox Worker(backend_alias='default') claimed {what}; "
        f"{rows(root, child)}; the caller's run_after was {child_run_after}"
    )


@pytest.mark.django_db
def test_a_rate_limited_backends_worker_runs_every_node_after_its_parents():
    """
    "limited" carries RATE_LIMITS and WORKER_CLASS = OxpullWorker, serves
    ["default"], and declares no WORKFLOWS. The DAG is test_workflows.diamond,
    whose priorities make claim order the reverse of dependency order.
    """
    diamond("cross-alias-limited")
    worker = oxpull_worker("limited")
    # Drain, then one reconciler pass as the schedule would run it, so a
    # waiting node is released the way production releases it when the
    # worker that ran its upstream does not.
    for _ in range(10):
        drain(worker)
        reconcile()
        if len(order()) >= len(NODES):
            break
    ran = order()

    assert sorted(ran) == sorted(NODES) and out_of_order(ran) == [], (
        f"OxpullWorker(backend_alias='limited') ran {ran}; edges violated: "
        f"{out_of_order(ran)}. check ids: "
        f"{check_ids('limited', 'default', 'flows')}; queues: "
        f"{queues('limited', 'flows')}"
    )


# -- in process, an alias with empty QUEUES --------------------------------


def _catch_all_tasks():
    """
    The suite's TASKS, with "flows" moved to its own queue "pipelines" and a
    plain OxpullBackend "catchall" whose QUEUES is empty. No other alias names
    "pipelines", so only the empty list can put the nodes in its claim.
    """
    configured = {alias: dict(config) for alias, config in settings.TASKS.items()}
    configured["flows"]["QUEUES"] = ["pipelines"]
    configured["catchall"] = {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": [],
        "OPTIONS": {"WORKER_CLASS": "oxpull.worker.OxpullWorker"},
    }
    return configured


@pytest.mark.django_db
def test_a_plain_alias_with_empty_queues_claims_the_root_not_the_child():
    with override_settings(TASKS=_catch_all_tasks()):
        root, child = root_and_child("catch-all", queue_name="pipelines")
        claimed = oxpull_worker("catchall").claim_one()

        ok, what = claimed_root(claimed, root, child)
        assert ok, (
            f"OxpullWorker(backend_alias='catchall') claimed {what}; "
            f"{rows(root, child)}. check ids: {check_ids('catchall', 'flows')}; "
            f"queues: {queues('catchall', 'flows')}"
        )


# -- `manage.py check`, in a fresh interpreter -----------------------------

CHECK_SCRIPT = """
import json
import sys

import django
from django.conf import settings

settings.configure(
    SECRET_KEY="check-only",
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    INSTALLED_APPS=["django_ox", "oxpull"],
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    TASKS=json.loads(sys.argv[1]),
)
django.setup()
import oxpull

print("oxpull from", oxpull.__file__)
from django.core.management import execute_from_command_line

execute_from_command_line(["manage.py", "check"])
"""

# Both reconcilers scheduled, so oxpull.W002 and oxpull.W003 have nothing to
# say and the only thing left to report is the overlap.
RECONCILERS = {
    "oxpull-batch-reconcile": {"task": "oxpull.batches.reconcile", "cron": "* * * * *"},
    "oxpull-workflow-reconcile": {
        "task": "oxpull.workflows.reconcile",
        "cron": "* * * * *",
    },
}

# Alias names that cannot turn up in a message by accident, the way
# "default" (also a queue name) or "flows" (inside "workflows") would, so
# finding one in a Warning means the Warning named that alias.
PLAIN = "shop"
FLOWS = "etl"

# The reconcilers resolve their Task on the "default" task backend, so that
# alias has to exist. It carries the schedules on a queue of its own that no
# workflow backend below serves, unless that backend's QUEUES is empty.
HOUSEKEEPING = {
    "BACKEND": "oxpull.backend.OxpullBackend",
    "QUEUES": ["housekeeping"],
    "OPTIONS": {
        "WORKER_CLASS": "oxpull.worker.OxpullWorker",
        "SCHEDULES": RECONCILERS,
    },
}


def two_aliases(plain_queues, flow_queues, *, plain_workflows=False):
    """
    A user's TASKS after adding Workflows on a second backend: the
    existing backend keeps its correctly configured OxpullWorker, and the
    new one declares WORKFLOWS. None means the QUEUES key is left out, which
    Django reads as ["default"].
    """
    plain = {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "OPTIONS": {"WORKER_CLASS": "oxpull.worker.OxpullWorker"},
    }
    if plain_workflows:
        plain["OPTIONS"]["WORKFLOWS"] = True
    flows = {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "OPTIONS": {"WORKER_CLASS": "oxpull.worker.OxpullWorker", "WORKFLOWS": True},
    }
    if plain_queues is not None:
        plain["QUEUES"] = plain_queues
    if flow_queues is not None:
        flows["QUEUES"] = flow_queues
    return {"default": HOUSEKEEPING, PLAIN: plain, FLOWS: flows}


OVERLAPPING = {
    "a-shared-queue-name": two_aliases(["default", "emails"], ["default"]),
    "both-on-djangos-default-queue": two_aliases(None, None),
    "a-catch-all-plain-alias": two_aliases([], ["pipelines"]),
    "a-catch-all-workflow-alias": two_aliases(["emails"], []),
}

APART = {
    "queues-kept-apart": two_aliases(["default"], ["pipelines"]),
    # A missing QUEUES key is ["default"], not every queue.
    "plain-alias-left-on-djangos-default-queue": two_aliases(None, ["pipelines"]),
    # Both backends gate, so sharing a queue is a correct install.
    "both-aliases-declare-workflows": two_aliases(
        ["default", "emails"], ["default"], plain_workflows=True
    ),
}


def run_check(configured):
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", CHECK_SCRIPT, json.dumps(configured)],
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(SRC), django_ox_path()]),
        },
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = result.stdout + result.stderr
    # Prove which oxpull answered before believing what it said. A failure
    # here is the setup, not the defect, so it must not count as the
    # expected failure.
    if f"oxpull from {SRC}" not in combined:
        pytest.fail(f"the check did not import oxpull from {SRC}:\n{combined}")
    return result, combined


def reported(combined, section):
    """
    The messages `manage.py check` printed under WARNINGS: or ERRORS:, each
    with its hint. Django prints every message at the start of a line and
    indents its hint.
    """
    match = re.search(rf"^{section}:\n(.*?)(?:\n\n|\Z)", combined, re.S | re.M)
    if match is None:
        return []
    return re.split(r"\n(?=\S)", match.group(1))


def names(alias, message):
    return re.search(rf"(?<![\w-]){re.escape(alias)}(?![\w-])", message) is not None


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "manage.py check says nothing when a backend without workflows serves "
        "a queue that a workflow backend uses."
    ),
)
@pytest.mark.parametrize("shape", list(OVERLAPPING))
def test_manage_py_check_warns_without_refusing_a_plain_alias_on_a_workflow_queue(
    shape,
):
    result, combined = run_check(OVERLAPPING[shape])
    overlap = [
        message
        for message in reported(combined, "WARNINGS")
        if names(PLAIN, message) and names(FLOWS, message)
    ]
    assert result.returncode == 0 and "ERRORS:" not in combined and overlap, (
        f"{shape}: `manage.py check` must exit 0 with a Warning naming both "
        f"{PLAIN!r} and {FLOWS!r}. It exited {result.returncode}:\n{combined}"
    )


@pytest.mark.parametrize("shape", list(APART))
def test_control_check_is_silent_when_no_plain_alias_serves_a_workflow_queue(shape):
    """
    Passes today and must keep passing. Each is a correct install that
    differs from an overlapping shape above only in its queues or in both
    backends declaring WORKFLOWS, so a Warning above can only be about a
    backend without workflows sharing a workflow queue.
    """
    result, combined = run_check(APART[shape])
    assert result.returncode == 0, combined
    assert "no issues" in combined, combined


# -- a real `ox_worker` process on the other alias -------------------------


def process_env(tmp_path):
    """
    A separate process on this session's test database, with the suite's
    settings as an operator would run them: DEBUG django_ox logs (the
    "Claimed task" line is the claim order) and the workflow reconciler
    scheduled, so its checks are otherwise silent. "default" gets
    WORKER_CLASS = OxpullWorker, which tests_oxpull/settings.py leaves out, so that
    both aliases are correctly configured OxpullBackend backends. "flows_ungated"
    stops declaring WORKFLOWS: it is the suite's deliberate oxpull.E007
    fixture, unrelated to this test, and would stop ox_worker at start-up.
    Its alias stays because tests_oxpull.tasks declares a task on it.
    """
    (tmp_path / "overlap_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)  # noqa: F405\n"
        "TASKS['default']['OPTIONS']['WORKER_CLASS'] = 'oxpull.worker.OxpullWorker'\n"
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
        "DJANGO_SETTINGS_MODULE": "overlap_settings",
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
@pytest.mark.parametrize("alias", ["default", "limited"])
def test_an_ox_worker_process_on_a_plain_alias_runs_every_node_after_its_parents(
    alias, tmp_path
):
    env = process_env(tmp_path)
    # Prove which oxpull the process imports, and which worker ox_worker
    # would build for the alias, before believing anything it does. A
    # failure here is the setup, not the defect.
    probe = run_python(
        env,
        "-c",
        "import django; django.setup(); import oxpull; "
        "from django_ox.worker import worker_class; "
        f"cls = worker_class({alias!r}); print(oxpull.__file__); "
        "print(cls.__module__ + '.' + cls.__qualname__)",
    )
    lines = probe.stdout.split()
    if not (
        len(lines) == 2
        and lines[0].startswith(str(SRC))
        and lines[1] == "oxpull.worker.OxpullWorker"
    ):
        pytest.fail(f"unexpected worker set-up:\n{probe.stdout}{probe.stderr}")
    check = run_python(env, "-m", "django", "check")

    flow = diamond(f"cross-alias-process-{alias}")
    ids = flow.node_ids()
    label_of = {str(task_id): name for name, task_id in ids.items()}

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
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and worker.poll() is None:
                # The schedule's reconciler pass, so a waiting node is
                # released the way production releases it.
                reconcile()
                unfinished = OxTask.objects.filter(pk__in=ids.values()).exclude(
                    status=OxTask.Status.SUCCESSFUL
                )
                if not unfinished.exists():
                    break
                time.sleep(0.1)
            exited_on_its_own = worker.poll() is not None
        finally:
            code = stop(worker)
    text = log_path.read_text()
    claimed = [
        label_of[task_id]
        for task_id in re.findall(r"Claimed task id=([0-9a-f-]{36}) ", text)
        if task_id in label_of
    ]

    assert (
        not exited_on_its_own
        and sorted(claimed) == sorted(NODES)
        and out_of_order(claimed) == []
    ), (
        f"`ox_worker --backend {alias}` claimed {claimed}; edges violated: "
        f"{out_of_order(claimed)}; exited on its own: {exited_on_its_own}, "
        f"exit {code}. `manage.py check` in that process: "
        f"{(check.stdout + check.stderr).strip()!r}\nworker log tail:\n{text[-2000:]}"
    )
