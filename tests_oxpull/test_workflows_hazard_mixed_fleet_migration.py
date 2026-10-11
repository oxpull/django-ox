"""
A mixed fleet, and migration order.

A workflow node that depends on something unfinished can be held back in two
places, and this file asserts each one separately.

  * In the node's own row. A blocked node waits in django-ox's WAITING
    status. Every django-ox claim reads only READY rows, so every claimer
    honours it: the `django_ox.worker.Worker`, oxpull 1.2.0's worker, and this
    version's. A worker that knows nothing about workflows never releases the
    node; the reconciler does, one tick later.
  * In the claim. OxpullWorker adds a dependency check to its claim. That is
    the only thing holding a node whose row has been put back to READY by
    something other than oxpull's release: django-ox's retry writes that
    shape on the rows it moves, and so does an application's own UPDATE.

A django-ox claim filters on status, run_after and queue name. It never reads
the backend alias, and a worker started without --queues takes every queue its
backend lists (every queue at all, when that list is empty). So workers
configured for other backends claim workflow nodes too.

Part (i-a), the row. Claimers that can never check dependencies (the
`django_ox.worker.Worker` in process, and oxpull 1.2.0 as a real ox_worker
process from the released source) run the same DAG, with claim order the exact
reverse of dependency order. No node may run before every node it depends on.
That holds whether or not the caller passed its own run_after: one already due,
one that comes due while the parent is still unfinished, and one an hour out.
Every blocked row is WAITING before anything runs, and a requested time still
in the future survives the release. The in-process `django_ox.worker.Worker`'s
claim adds no condition of its own (claim_filter_q() is None), which each of
these tests checks first, so nothing but the row can be what holds the node.
Both workflow backends hold the row the same way; "flows_deferred" carries the
removed DEFER_BLOCKED_NODES option, which changes nothing.

Part (i-b), the claim. Every `oxpull.worker.OxpullWorker` on a database where
some backend declares workflows checks dependencies in its claim, whatever its
own backend declares and whatever queues it lists, including after a deploy has
moved the workflow backend off the queue its waiting nodes carry. The blocked
rows are put back to READY with no run_after first, so only the claim can hold
them. The workflow backend's own worker is the control and passes.

Part (ii) rewinds oxpull to the migration before workflows (0003) with
tests_oxpull.test_upgrade's helpers. It asks what an operator sees when a workflow
worker, a plain `oxpull.worker.OxpullWorker`, or the new prune runs before the
workflow migration (0004) is applied. The plain `oxpull.worker.OxpullWorker`'s
case passes today, and stays as a guard: once every
`oxpull.worker.OxpullWorker` checks dependencies, its claim meets the missing
edge table too.

Each test with an expected-failure marker asserts the correct behaviour, and
still fails.
"""

from __future__ import annotations

import contextlib
import copy
import io
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
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError, connection
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullUniqueLock, OxpullWorkflowEdge
from oxpull.worker import OxpullWorker

from . import tasks
from ._historical import extract_released_oxpull
from .subprocesses import django_ox_path
from .test_upgrade import _latest_migration, _migrate
from .test_workflows import drain, flow_worker, free_worker, order, reconcile

REPO_ROOT = Path(__file__).resolve().parent.parent
BEFORE_WORKFLOWS = "0003_oxpullratewindow"

NODES = ["extract", "transform-a", "transform-b", "load"]
EDGES = [
    ("extract", "transform-a"),
    ("extract", "transform-b"),
    ("transform-a", "load"),
    ("transform-b", "load"),
]
BLOCKED_NODES = ["transform-a", "transform-b", "load"]
NODE_TASKS = {"flows": tasks.step, "flows_deferred": tasks.deferred_step}

# Drain-then-reconcile rounds a DAG gets before the test reads what ran.
ROUNDS = 10

# How long the pre-0004 worker is watched before anything else happens.
STALL_SECONDS = 5.0


def inverted_diamond(name, task, **using):
    """
    tests_oxpull.test_workflows.diamond for any node task: priorities make claim
    order the exact reverse of dependency order, so only a block can stop
    `load` from running first.
    """
    flow = workflows.create(name)
    flow.add("extract", task.using(priority=0, **using), "extract")
    for branch in ("transform-a", "transform-b"):
        flow.add(branch, task.using(priority=5, **using), branch, depends_on="extract")
    flow.add(
        "load",
        task.using(priority=10, **using),
        "load",
        depends_on=["transform-a", "transform-b"],
    )
    flow.seal()
    return flow


def out_of_order(sequence, edges=EDGES):
    """Edges whose downstream was claimed before, or without, its upstream."""
    first: dict[str, int] = {}
    for index, label in enumerate(sequence):
        first.setdefault(label, index)
    return [
        (upstream, downstream)
        for upstream, downstream in edges
        if downstream in first
        and (upstream not in first or first[upstream] > first[downstream])
    ]


def drain_and_reconcile(worker, expected):
    """
    Drain, then one reconciler pass as the schedule would run it, until every
    label in `expected` has run. The pass releases what a worker that knows
    nothing about workflows never releases.
    """
    for _ in range(ROUNDS):
        drain(worker)
        reconcile()
        if set(expected) <= set(order()):
            break
    return order()


def run_after_of(ids):
    """Each node's run_after as the database holds it now, for messages."""
    rows = dict(
        OxTask.objects.filter(pk__in=ids.values()).values_list("pk", "run_after")
    )
    return {
        name: (rows[task_id].isoformat() if rows.get(task_id) else None)
        for name, task_id in ids.items()
    }


def _check_ids(*aliases):
    from django.tasks import task_backends

    return {alias: [m.id for m in task_backends[alias].check()] for alias in aliases}


def premise(condition, message):
    """
    A fact the test stands on, not the behaviour under test. pytest.fail is
    not an AssertionError, and strict markers that expect an assertion
    never absorb it.
    """
    if not condition:
        pytest.fail(f"premise: {message}")


def require_the_free_worker_checks_nothing():
    """Only the row can hold a node back from the `django_ox.worker.Worker`."""
    condition = free_worker().claim_filter_q()
    premise(
        condition is None,
        f"the `django_ox.worker.Worker`'s claim carries a condition of its own: "
        f"{condition!r}",
    )


# -- (i-a) the row holds the node: claimers that never check dependencies --


@pytest.mark.django_db
@pytest.mark.parametrize(
    "backend",
    ["flows", "flows_deferred"],
)
def test_the_free_worker_runs_no_node_before_its_parent(backend):
    """
    The `django_ox.worker.Worker` is not this package's code and can never check
    dependencies, so only the node's own row can hold a node back from it.
    """
    require_the_free_worker_checks_nothing()
    flow = inverted_diamond(f"free-worker-{backend}", NODE_TASKS[backend])
    enqueued = run_after_of(flow.node_ids())

    ran = drain_and_reconcile(free_worker(), NODES)

    assert out_of_order(ran) == [], (
        f"the `django_ox.worker.Worker` on backend {backend!r} ran {ran}; "
        f"run_after of each node as enqueued: {enqueued}"
    )
    assert sorted(ran) == sorted(NODES), (
        f"ran {ran} after {ROUNDS} rounds; run_after now: "
        f"{run_after_of(flow.node_ids())}"
    )


OWN_TIME_EDGES = [("parent", "due"), ("parent", "soon"), ("parent", "scheduled")]
OWN_TIME_BLOCKED = ["due", "soon", "scheduled"]

# How far out "soon" asks to run. The test's clock stands still while the
# nodes are added and is moved past this before anything is drained, so the
# figure is never waited out. It is minutes rather than seconds so that a
# hold deciding by the database's clock, which the test does not move, would
# still read `soon` as not yet due however long the adds take.
SOON = timedelta(minutes=10)


class HeldClock:
    """
    django.utils.timezone.now, held at one instant until the test moves it.

    Replaced on the module, so every caller that looks it up when it calls
    it reads the held instant: add(), the release, and the run_after cutoff
    of django-ox's claim, which is the worker's clock on every database and
    never the server's. django-ox's leases run on the database clock, which
    this does not move, so moving it neither expires nor extends a lease.
    """

    def __init__(self, monkeypatch):
        self.at = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: self.at)


@pytest.fixture
def held_clock(monkeypatch):
    return HeldClock(monkeypatch)


@pytest.mark.django_db
@pytest.mark.parametrize(
    "backend",
    ["flows", "flows_deferred"],
)
def test_a_node_with_its_own_run_after_waits_for_its_parent_and_keeps_its_time(
    backend, held_clock
):
    """
    Three children of one parent, each given the caller's own run_after:
    `due`, already due when it is added; `soon`, ten minutes out when it is
    added and due by the time anything runs; and `scheduled`, an hour away.
    While the parent is unfinished all three are blocked, and the row must
    hold them like any other node, whatever time the caller asked for and
    whether or not that time had passed when add() ran: before anything
    runs, each of their rows is WAITING, which no claim reads. Once the
    parent succeeds, `due` and `soon` run, and `scheduled` keeps the time the
    caller asked for instead of losing it to the release.

    A hold applied only to nodes whose own time has already come when they
    are added lets `soon` through: its time passes while the parent waits.
    """
    require_the_free_worker_checks_nothing()
    task = NODE_TASKS[backend]
    now = held_clock.at
    due = now - timedelta(hours=1)
    soon = now + SOON
    scheduled = now + timedelta(hours=1)
    flow = workflows.create(f"own-run-after-{backend}")
    flow.add("parent", task.using(priority=0), "parent")
    flow.add("due", task.using(priority=10, run_after=due), "due", depends_on="parent")
    flow.add(
        "soon", task.using(priority=10, run_after=soon), "soon", depends_on="parent"
    )
    flow.add(
        "scheduled",
        task.using(priority=10, run_after=scheduled),
        "scheduled",
        depends_on="parent",
    )
    flow.seal()
    ids = flow.node_ids()
    enqueued = run_after_of(ids)
    rows = dict(
        OxTask.objects.filter(
            pk__in=[ids[name] for name in OWN_TIME_BLOCKED]
        ).values_list("pk", "status")
    )
    unfenced = {
        name: rows[ids[name]]
        for name in OWN_TIME_BLOCKED
        if rows[ids[name]] != OxTask.Status.WAITING
    }
    # soon's time passes while the parent is still unfinished. Every add()
    # above read the clock at `now`, before soon; every claim below reads it
    # after.
    held_clock.at = soon + timedelta(seconds=1)

    ran = drain_and_reconcile(free_worker(), ["parent", "due", "soon"])

    assert out_of_order(ran, OWN_TIME_EDGES) == [], (
        f"the `django_ox.worker.Worker` on backend {backend!r} ran {ran}; "
        f"the caller asked for due={due.isoformat()}, soon={soon.isoformat()} "
        f"and scheduled={scheduled.isoformat()}; run_after as enqueued: {enqueued}"
    )
    assert sorted(ran) == ["due", "parent", "soon"], (
        f"expected parent, due and soon to run and scheduled to wait for "
        f"{scheduled.isoformat()}; ran {ran}; run_after now: {run_after_of(ids)}"
    )
    kept = OxTask.objects.get(pk=ids["scheduled"]).run_after
    assert kept is not None and abs(kept - scheduled) < timedelta(seconds=1), (
        f"scheduled asked for run_after={scheduled.isoformat()} and holds "
        f"{kept} after its parent succeeded"
    )
    assert unfenced == {}, (
        f"while their parent was unfinished, these blocked rows were not "
        f"WAITING, so nothing in the row held them back: {unfenced}"
    )


# -- (i-a) the row holds the node: oxpull 1.2.0 as a real ox_worker process --


@pytest.fixture(scope="module")
def released_oxpull(tmp_path_factory):
    """The released package's source, read from the vendored archive."""
    root = tmp_path_factory.mktemp("oxpull-released")
    return extract_released_oxpull(root)


def process_env(tmp_path, src, *, schedule_reconciler):
    """
    The environment for a separate process on this session's test
    database: `src` first on the path, and the session's settings as an
    operator would have them. That means DEBUG django_ox logs (the
    "Claimed task" line is the claim order). `flows_ungated` is a
    deliberate oxpull.E007 fixture that stops `ox_worker` at its system
    checks. Its alias stays, because tests_oxpull.tasks declares a task on it, but
    it stops declaring WORKFLOWS. Optionally the workflow reconciler is
    scheduled, so the checks are otherwise silent. The session's settings
    schedule it already, and oxpull 1.2.0 cannot import it, so without
    the option it is taken out.
    """
    schedule = (
        "TASKS['default']['OPTIONS']['SCHEDULES']['oxpull-workflow-reconcile'] = "
        "{'task': 'oxpull.workflows.reconcile', 'cron': '* * * * *'}\n"
        if schedule_reconciler
        else "TASKS['default']['OPTIONS']['SCHEDULES'].pop("
        "'oxpull-workflow-reconcile', None)\n"
    )
    (tmp_path / "fleet_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)\n"
        "TASKS['flows_ungated']['OPTIONS']['WORKFLOWS'] = False\n"
        f"{schedule}"
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
        "DJANGO_SETTINGS_MODULE": "fleet_settings",
        # The current source runs against this process's django-ox. A released
        # oxpull runs against the django-ox the interpreter has installed.
        "PYTHONPATH": os.pathsep.join(
            [str(src)]
            + ([django_ox_path()] if src == REPO_ROOT / "src" else [])
            + [str(tmp_path), str(REPO_ROOT)]
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


def assert_imports(env, src, version):
    """Prove which oxpull the process runs before believing anything it does."""
    probe = run_python(
        env, "-c", "import oxpull; print(oxpull.__version__); print(oxpull.__file__)"
    )
    lines = probe.stdout.split()
    assert probe.returncode == 0, probe.stderr
    assert lines[0] == version, lines
    assert lines[1].startswith(str(src)), lines


def start_ox_worker(env, log, backend, interval):
    return subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "django",
            "ox_worker",
            "--backend",
            backend,
            "--interval",
            str(interval),
        ],
        env=env,
        cwd=REPO_ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
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
@pytest.mark.parametrize(
    "backend",
    ["flows", "flows_deferred"],
)
def test_a_released_oxpull_worker_process_runs_no_node_before_its_parent(
    backend, released_oxpull, tmp_path
):
    """
    oxpull 1.2.0 does not know the WORKFLOWS key, so its claim never checks
    dependencies. The test process plays the reconciler's schedule.
    """
    env = process_env(tmp_path, released_oxpull, schedule_reconciler=False)
    assert_imports(env, released_oxpull, "1.2.0")
    check = run_python(env, "-m", "django", "check")

    flow = inverted_diamond(f"released-{backend}", NODE_TASKS[backend])
    ids = flow.node_ids()
    enqueued = run_after_of(ids)
    label_of = {str(task_id): name for name, task_id in ids.items()}

    log_path = tmp_path / "ox_worker.log"
    with log_path.open("w") as log:
        worker = start_ox_worker(env, log, backend, interval=0.05)
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and worker.poll() is None:
                reconcile()
                unfinished = OxTask.objects.filter(pk__in=ids.values()).exclude(
                    status=OxTask.Status.SUCCESSFUL
                )
                if not unfinished.exists():
                    break
                time.sleep(0.1)
        finally:
            code = stop(worker)
    text = log_path.read_text()
    claimed = [
        label_of[task_id]
        for task_id in re.findall(r"Claimed task id=([0-9a-f-]{36}) ", text)
        if task_id in label_of
    ]

    assert out_of_order(claimed) == [], (
        f"oxpull 1.2.0 `ox_worker --backend {backend}` claimed {claimed}; "
        f"run_after of each node as enqueued: {enqueued}; `manage.py check` "
        f"in that process: {(check.stdout + check.stderr).strip()!r}"
    )
    assert sorted(claimed) == sorted(NODES), f"exit {code}\n{text[-4000:]}"


# -- (i-b) the claim holds the node: every `oxpull.worker.OxpullWorker` on the database

PIPELINES = "pipelines"


def pipelines_tasks():
    """
    The suite's TASKS with every backend that declares WORKFLOWS moved to
    its own queue, "pipelines", plus a plain `oxpull.backend.OxpullBackend`,
    "catchall", whose empty QUEUES accepts every queue name. After the move no
    plain alias's queue list shares a name with any workflow backend's, so a
    worker that checks dependencies only where queue lists overlap does not
    check here.
    """
    configured = copy.deepcopy(settings.TASKS)
    for config in configured.values():
        if config.get("OPTIONS", {}).get("WORKFLOWS") is True:
            config["QUEUES"] = [PIPELINES]
    configured["catchall"] = {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": [],
        "OPTIONS": {"WORKER_CLASS": "oxpull.worker.OxpullWorker"},
    }
    return configured


def oxpull_worker(alias, **kwargs):
    """This version's OxpullWorker on `alias`, as its ox_worker would build it."""
    return OxpullWorker(
        backend_alias=alias, backoff_initial=0, poll_interval=0.01, **kwargs
    )


# claimer -> (TASKS for the test or None for the suite's, the nodes' queue,
# the worker). None of the plain aliases declares WORKFLOWS; every one shares
# the database with "flows", which does.
CLAIMERS = {
    "control-the-workflow-backends-own-worker": (None, "default", flow_worker),
    "control-the-workflow-backends-own-worker-on-its-own-queue": (
        pipelines_tasks,
        PIPELINES,
        flow_worker,
    ),
    "plain-alias-sharing-the-queue-name": (
        None,
        "default",
        lambda: oxpull_worker("default"),
    ),
    "plain-alias-with-empty-queues": (
        pipelines_tasks,
        PIPELINES,
        lambda: oxpull_worker("catchall"),
    ),
    "plain-alias-started-with-queues-naming-the-workflow-queue": (
        pipelines_tasks,
        PIPELINES,
        lambda: oxpull_worker("default", queues=[PIPELINES]),
    ),
}


@pytest.mark.django_db
@pytest.mark.parametrize(
    "claimer",
    [
        "control-the-workflow-backends-own-worker",
        "control-the-workflow-backends-own-worker-on-its-own-queue",
        pytest.param(
            "plain-alias-sharing-the-queue-name",
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "A `oxpull.worker.OxpullWorker` on a backend without workflows "
                    "that shares a queue name with a workflow backend checks no "
                    "dependencies in its claim, so it runs a node before its upstream "
                    "once the node's row no longer holds it back"
                ),
            ),
        ),
        pytest.param(
            "plain-alias-with-empty-queues",
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "A `oxpull.worker.OxpullWorker` on a backend with an empty queue "
                    "list and no workflows checks no dependencies in its claim, so it "
                    "runs a node from a workflow backend's queue before its upstream "
                    "once the node's row no longer holds it back"
                ),
            ),
        ),
        pytest.param(
            "plain-alias-started-with-queues-naming-the-workflow-queue",
            marks=pytest.mark.xfail(
                strict=True,
                reason=(
                    "A `oxpull.worker.OxpullWorker` started with --queues naming a "
                    "workflow backend's queue, on a backend whose own queue list "
                    "shares nothing with it, checks no dependencies in its claim and "
                    "runs a node before its upstream once the node's row no longer "
                    "holds it back"
                ),
            ),
        ),
    ],
)
def test_every_oxpull_worker_on_the_database_holds_a_node_its_row_no_longer_holds(
    claimer,
):
    """
    The blocked nodes are put back to READY, the shape django-ox's retry
    writes on the rows it moves and an application's own UPDATE writes.
    Nothing in those rows holds them now, and `extract` has not run, so only
    a dependency check in the claim can keep `load` from running first. No
    reconciler pass runs, so nothing can put a block back.
    """
    layout, queue, make_worker = CLAIMERS[claimer]
    overridden = (
        override_settings(TASKS=layout()) if layout else contextlib.nullcontext()
    )
    with overridden:
        flow = inverted_diamond(f"row-cleared-{claimer}", tasks.step, queue_name=queue)
        ids = flow.node_ids()
        OxTask.objects.filter(pk__in=[ids[name] for name in BLOCKED_NODES]).update(
            status=OxTask.Status.READY, run_after=None
        )
        # The premise: every node is READY with no run_after, extract included.
        rows = OxTask.objects.filter(pk__in=ids.values())
        assert {(row.status, row.run_after) for row in rows} == {
            (OxTask.Status.READY, None)
        }

        worker = make_worker()
        drain(worker)
        ran = order()

        assert out_of_order(ran) == [], (
            f"{claimer}: OxpullWorker(backend_alias={worker.backend.alias!r}, "
            f"queues={worker.queues}) ran {ran}; check ids: "
            f"{_check_ids(worker.backend.alias, 'flows')}"
        )
        assert sorted(ran) == sorted(NODES), ran


@pytest.mark.xfail(
    strict=True,
    reason=(
        "After the workflow backends move to their own queue, a "
        "`oxpull.worker.OxpullWorker` on the old queue checks no dependencies in its "
        "claim, so it claims the nodes still waiting there before their upstream once "
        "their rows no longer hold them back"
    ),
)
@pytest.mark.django_db
def test_an_oxpull_worker_holds_nodes_left_on_a_queue_the_workflow_backends_moved_off():
    """
    The nodes are enqueued under the suite's settings, on "default". Then a
    deploy moves every workflow backend to its own queue, "pipelines", while
    they wait. A plain `oxpull.worker.OxpullWorker` on "default" now lists no
    queue that any workflow backend lists, and it still claims those nodes,
    because a claim reads the row's queue name, not the backend that wrote it.

    Every `oxpull.worker.OxpullWorker` on the database checks dependencies, so
    the only node this one may claim is `extract`, and while `extract` runs it
    claims nothing else. A worker that checks only where its queue list meets a
    workflow backend's current one does not check here, and fails.

    Claims are read, not executions: after the move "default" is not a queue
    the workflow backend accepts, so executing one of these nodes would fail
    task validation, which is a separate matter from the order of claims.
    """
    flow = inverted_diamond("queue-moved", tasks.step, queue_name="default")
    ids = flow.node_ids()
    label_of = {str(task_id): name for name, task_id in ids.items()}
    OxTask.objects.filter(pk__in=[ids[name] for name in BLOCKED_NODES]).update(
        status=OxTask.Status.READY, run_after=None
    )
    rows = OxTask.objects.filter(pk__in=ids.values())
    premise(
        {(row.status, row.run_after, row.queue_name) for row in rows}
        == {(OxTask.Status.READY, None, "default")},
        "every node is READY on 'default' with no run_after",
    )

    with override_settings(TASKS=pipelines_tasks()):
        worker = oxpull_worker("default")
        premise(
            PIPELINES not in worker.queues and "default" in worker.queues,
            f"the worker on 'default' lists {worker.queues}",
        )
        claimed = []
        for _ in NODES:
            task_row = worker.claim_one()
            if task_row is None:
                break
            claimed.append(label_of[str(task_row.pk)])

        assert claimed == ["extract"], (
            f"OxpullWorker(backend_alias='default', queues={worker.queues}) "
            f"claimed {claimed} from 'default' after the workflow backends moved "
            f"to {PIPELINES!r}; only extract has no unfinished upstream. Check "
            f"ids: {_check_ids('default', 'flows')}"
        )


# -- (ii) migration order --------------------------------------------------


@pytest.fixture
def before_workflows_migration():
    """oxpull at 0003 for one test, restored to the leaf even on failure."""
    latest = _latest_migration()
    _migrate(BEFORE_WORKFLOWS)
    try:
        yield
    finally:
        _migrate(latest)


@pytest.mark.django_db(transaction=True)
def test_the_rewound_schema_has_no_workflow_tables(before_workflows_migration):
    """Pins the premise of every test below."""
    tables = connection.introspection.table_names()
    assert OxpullWorkflowEdge._meta.db_table not in tables


@pytest.mark.django_db(transaction=True)
def test_before_0004_a_claim_without_a_dependency_check_still_runs_the_queue(
    before_workflows_migration,
):
    """
    Control: the task row and the queue are fine at 0003, so the stall below
    belongs to the claim that checks dependencies. The `django_ox.worker.Worker`
    never checks them, whatever any backend declares.
    """
    loose = tasks.step.enqueue("loose")
    assert free_worker().run_once() is True
    assert OxTask.objects.get(pk=loose.id).status == OxTask.Status.SUCCESSFUL


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Before the workflow migration (0004) is applied, a workflow worker's "
        "claim raises a raw database error about the missing edge table "
        "instead of claiming the plain task or saying to run migrate"
    ),
)
@pytest.mark.django_db(transaction=True)
def test_before_0004_a_workflows_claim_returns_the_plain_task_or_names_the_migration(
    before_workflows_migration,
):
    """
    No workflow can exist without the edge table, so the check has nothing
    to hold. The claim either returns the plain task or refuses in words
    that say `migrate`.
    """
    loose = tasks.step.enqueue("loose")
    worker = flow_worker()
    claimed = raised = None
    try:
        claimed = worker.claim_one()
    except (DatabaseError, ImproperlyConfigured) as exc:
        raised = exc

    if raised is None:
        assert claimed is not None
        assert str(claimed.pk) == loose.id
        return
    kind = f"{type(raised).__module__}.{type(raised).__qualname__}"
    assert isinstance(raised, ImproperlyConfigured), f"{kind}: {raised}"
    assert "migrate" in str(raised), f"{kind}: {raised}"


@pytest.mark.django_db(transaction=True)
def test_before_0004_a_plain_oxpull_worker_claims_the_plain_task_or_names_the_migration(
    before_workflows_migration,
):
    """
    Guard, green today. A `oxpull.worker.OxpullWorker` on a backend without
    workflows, on the database where "flows" declares them. Once every
    `oxpull.worker.OxpullWorker` there checks dependencies in its claim, this
    claim meets the missing edge table exactly as the workflow backend's own
    does above, and it must behave the same way: return the plain task, or
    refuse in words that say `migrate`. Today it checks nothing and returns the
    task.
    """
    loose = tasks.step.enqueue("loose")
    worker = oxpull_worker("default")
    claimed = raised = None
    try:
        claimed = worker.claim_one()
    except (DatabaseError, ImproperlyConfigured) as exc:
        raised = exc

    if raised is None:
        assert claimed is not None
        assert str(claimed.pk) == loose.id
        return
    kind = f"{type(raised).__module__}.{type(raised).__qualname__}"
    assert isinstance(raised, ImproperlyConfigured), f"{kind}: {raised}"
    assert "migrate" in str(raised), f"{kind}: {raised}"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Before the workflow migration (0004) is applied, a workflow ox_worker "
        "stays up, runs no plain task and logs a database error every pass, "
        "instead of running the task or exiting with a message naming migrate"
    ),
)
@pytest.mark.django_db(transaction=True)
def test_before_0004_a_workflows_ox_worker_runs_plain_tasks_or_refuses_to_start(
    before_workflows_migration, tmp_path
):
    """
    The operator's view: the real `ox_worker` for the workflow backend,
    this version's code, the schema one migration behind. Correct is either
    the plain task running or the process exiting non-zero with `migrate`
    in what it printed. Recorded along the way: how the loop behaves, what
    it logs, and whether it recovers once `migrate` runs underneath it.
    """
    src = REPO_ROOT / "src"
    env = process_env(tmp_path, src, schedule_reconciler=True)
    assert_imports(env, src, __import__("oxpull").__version__)
    check = run_python(env, "-m", "django", "check")
    loose = tasks.step.enqueue("loose")

    log_path = tmp_path / "ox_worker.log"
    recovered_after = None
    with log_path.open("w") as log:
        worker = start_ox_worker(env, log, "flows", interval=0.2)
        try:
            deadline = time.monotonic() + STALL_SECONDS
            while time.monotonic() < deadline and worker.poll() is None:
                time.sleep(0.1)
            alive = worker.poll() is None
            status = OxTask.objects.get(pk=loose.id).status
            stalled_text = log_path.read_text()
            if alive and status != OxTask.Status.SUCCESSFUL:
                # Apply the missing migrations under the running worker.
                started = time.monotonic()
                _migrate(_latest_migration())
                while time.monotonic() - started < 30:
                    row = OxTask.objects.get(pk=loose.id)
                    if row.status == OxTask.Status.SUCCESSFUL:
                        recovered_after = round(time.monotonic() - started, 2)
                        break
                    time.sleep(0.1)
        finally:
            code = stop(worker)

    poll_failures = stalled_text.count("could not reach the database this pass")
    error = re.search(r"^django\.db\.utils\.\w+: .*$", stalled_text, re.MULTILINE)
    ran = status == OxTask.Status.SUCCESSFUL
    refused = not alive and code != 0 and "migrate" in stalled_text
    assert ran or refused, (
        f"schema at {BEFORE_WORKFLOWS}; `ox_worker --backend flows` watched "
        f"{STALL_SECONDS}s: alive={alive}, plain task {status}, "
        f"{poll_failures} pass(es) logged 'could not reach the database this "
        f"pass', error={error.group(0) if error else None!r}; after migrate "
        f"under the running worker the task ran in {recovered_after}s "
        f"(None = never); exit code after SIGTERM {code}; `manage.py check`: "
        f"{(check.stdout + check.stderr).strip()!r}"
    )


@pytest.mark.django_db(transaction=True)
def test_before_0004_oxpull_prune_completes_or_names_the_migration(
    before_workflows_migration,
):
    """
    The new prune also works on the workflow tables. Before 0004 it must
    either finish, or stop with a CommandError that says `migrate`. Deleting
    stale unique locks first is independent, correct work, so whether that
    already happened is recorded in the message and not judged.
    """
    tasks.unique_echo.enqueue("prune-me")
    assert free_worker().run_once() is True
    assert OxpullUniqueLock.objects.count() == 1

    out = io.StringIO()
    raised = None
    try:
        call_command("oxpull_prune", stdout=out)
    except (DatabaseError, CommandError) as exc:
        raised = exc
    locks_left = OxpullUniqueLock.objects.count()

    if raised is None:
        assert locks_left == 0, out.getvalue()
        return
    kind = f"{type(raised).__module__}.{type(raised).__qualname__}"
    detail = (
        f"{kind}: {raised}; printed before it: {out.getvalue()!r}; "
        f"stale locks left {locks_left} of 1"
    )
    assert isinstance(raised, CommandError), detail
    assert "migrate" in str(raised), detail
