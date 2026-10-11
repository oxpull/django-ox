"""
On MySQL the cancellation's locking read locks rows it does not
return, so live work waits on, or is skipped because of, a pass that is not
about it.

workflows._cancel_one_level, one transaction per level:

  1. UPDATE the workflow row under SEALED;
  2. SELECT pk FROM task WHERE id IN (the workflow's node task ids)
     AND status IN (FAILED, LOST, DISCARDED) ORDER BY pk FOR UPDATE;
  3. SELECT the READY or WAITING nodes blocked by a dead upstream;
  4. UPDATE those nodes to DISCARDED;
  5. COMMIT.

The docstring's guard is "only terminal statuses are locked, which no worker
ever claims". InnoDB does not keep to it. Which rows it locks depends on the
plan it picks for step 2, and the plan depends on what the task table holds:

- Node-driven: the node table read for the workflow's id, by index or by a
  filtered scan, then a PRIMARY lookup per node, with the status test above
  the storage engine. Picked here whenever the table holds a few hundred
  DISCARDED rows, from history or from the workflow's own cancelled cone.
  At REPEATABLE READ every node row of the workflow stays X-locked to
  COMMIT, RUNNING and READY included. At READ COMMITTED InnoDB releases
  rows that fail the filter, except the last row the scan reads, which
  stays locked; with a workflow of mostly live nodes that is a live node.
- A range on ox_reaper_idx (status, locked_at) over the dead statuses,
  picked when dead rows are few. At REPEATABLE READ this takes next-key and
  gap locks over that index range, table-wide, so any task, in a workflow
  or not, whose status moves into FAILED, LOST or DISCARDED waits for the
  level to commit.

Who runs at REPEATABLE READ: Django's MySQL backend sets READ COMMITTED on
every connection unless DATABASES[...]["OPTIONS"]["isolation_level"] says
otherwise; a project that sets "repeatable read", or None for InnoDB's
server default, runs at REPEATABLE READ. oxpull already treats that as a
supported configuration (OxpullBackend._reads_a_snapshot, the batch seal
tests). The fixture sets the level the way a project does, through OPTIONS,
and every test checks the level of the pass's own open transaction, read
while it is held.

Every step is a production path. 600 unrelated tasks are enqueued and
discarded with django_ox.actions.discard_many, where a cell asks for that
history. A real worker drives the root through three failing attempts to
FAILED. A second real worker claims the "running" node and executes it
(run_once), its body parked on an Event, so the row holds a real lease and
sits in that worker's in-flight set. The bystander, a task outside any
workflow, is failed twice by a real worker and claimed for its last attempt.
OPTIMIZE TABLE then rebuilds the tables and refreshes their statistics. A
test that ran before this one can leave tens of thousands of deleted task
rows that InnoDB has not purged yet. Its estimate of the dead-status range
then falls to a few dozen rows, and it picks the other plan. Each cell names
the plan it is about, and the pass's own optimizer trace says which plan the
locking read ran. The EXPLAIN printed with each result can disagree with it
while InnoDB is still purging. A pass that ran another plan fails the test as
not provoked, so it counts neither as the expected failure nor as a fix.

The pass is workflows._reconcile, the scheduled task's body, on its own
thread and connection. The probes are the running worker's
renew_leases(), that worker finishing its task (_write_outcome), another
worker claiming everything it can, and the bystander's worker executing its
last attempt. No task row is written by hand.

The pass is held open at one of two statements, found by the SQL the
reconciler's connection sends: just after the locking read (step 2), and just
after the discard UPDATE (step 4, the last statement before COMMIT). The pass
stays held until the probe is done, and no verdict compares wall-clock
intervals: the claim probe is a hundred transactions, and how long they take
is the server's speed, not a wait. While the probe runs, the database is
asked who waits on a lock the pass's connection holds (data_lock_waits on
MySQL, pg_blocking_pids on PostgreSQL). A wait found there is the verdict; the
pass is then released, so the waiter finishes, and the wait is reported. When
the probe is done the pass's transaction is read once more, still open, so
everything the probe did happened while the pass held its locks. The plan
InnoDB chose for the locking read is printed with every result.

Every test asserts the correct behaviour, so each cell here fails today on
MySQL. The cells where the same assertion holds today (the other plan, the
other isolation level) are the controls, run from a separate file.
PostgreSQL runs the probe tests too and is not expected to be exposed: its
FOR UPDATE locks only the rows the filter returns. The data_locks test is
InnoDB's own. SQLite is skipped: it has no row locks, its writers serialise,
and the pass opens with a write, so every write waits on it by design.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from django.db import connections, transaction
from django.tasks import task

from django_ox import actions
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.models import OxpullWorkflowNode
from oxpull.routing import oxpull_db_alias

from . import isolation, tasks
from .test_workflows import flow_worker, order, reconcile

pytestmark = pytest.mark.django_db(transaction=True)

TASK_TABLE = OxTask._meta.db_table.upper()

# The cells in this file fail on MySQL only; PostgreSQL runs them green.
ON_MYSQL = connections[oxpull_db_alias()].vendor == "mysql"

AFTER_THE_LOCKING_READ = "after_the_locking_read"
AFTER_THE_DISCARD = "after_the_discard"
POINTS = [AFTER_THE_LOCKING_READ, AFTER_THE_DISCARD]
LABEL_AT = {
    AFTER_THE_LOCKING_READ: "locking read",
    AFTER_THE_DISCARD: "discard update",
}

REPEATABLE_READ = "repeatable read"
READ_COMMITTED = "read committed"

# What the task table holds before the workflow is built. It decides the
# plan: see the module docstring.
NO_HISTORY = "no_history"
DISCARDED_HISTORY = "discarded_history"
HISTORY_WIDTH = 600

# The plan each history is there to provoke on MySQL. The bystander cells
# take whichever plan InnoDB picks, so they name none.
PLAN_OF_HISTORY = {DISCARDED_HISTORY: "node-driven"}


class PlanNotProvoked(Exception):
    """
    The locking read ran under another plan than the cell is about.

    Not an AssertionError. The xfail markers expect one, so this is reported
    as a failure: a pass under the wrong plan proves neither the hazard nor
    its fix.
    """


# Claimable READY nodes of the workflow, so that "the last row the scan
# reads" is almost always a live one.
LIVE_READY = 100

# How often the database is asked who waits on the pass while a probe runs.
WAIT_POLL = 0.05
# Safety limits, never reached on a passing run. A probe that waits on the pass
# is reported by the database within a poll or two. PROBE_LIMIT is what a probe
# held up by anything the database does not report as a lock wait runs into,
# and it ends inside HOLD_LIMIT, so the pass is still held when it does.
PASS_REACH_LIMIT = 20.0
PROBE_LIMIT = 20.0
HOLD_LIMIT = 30.0
GATE_LIMIT = 60.0


def cell(history: str, level: str):
    return pytest.param(history, level, id=f"{history}-{level.replace(' ', '_')}")


# The cells that fail today. The controls file runs every other cell.
LIVE_NODE_CELLS = [cell(DISCARDED_HISTORY, REPEATABLE_READ)]
CLAIMABLE_CELLS = [
    cell(DISCARDED_HISTORY, REPEATABLE_READ),
    cell(DISCARDED_HISTORY, READ_COMMITTED),
]
BYSTANDER_CELLS = [cell(NO_HISTORY, REPEATABLE_READ)]


# -- the parked node ---------------------------------------------------


@task(backend="flows")
def held_step(label):
    """A node whose body runs until the test releases it."""
    tasks._record(label)
    if not tasks.STATE["gate"].wait(GATE_LIMIT):
        raise TimeoutError(f"{label} was never released")
    return label


# -- configuration -----------------------------------------------------


@pytest.fixture
def project_isolation():
    """The isolation level the whole project is configured at."""
    return REPEATABLE_READ


@pytest.fixture
def history():
    return DISCARDED_HISTORY


def session_facts(connection) -> tuple[str, Any]:
    """This connection's isolation level, normalised, and its server id."""
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            cursor.execute("SELECT @@transaction_isolation, CONNECTION_ID()")
        else:
            cursor.execute(
                "SELECT current_setting('transaction_isolation'), pg_backend_pid()"
            )
        level, ident = cursor.fetchone()
    return str(level).lower().replace("-", " ").replace("_", " "), ident


def transaction_level(connection) -> str:
    """
    The isolation level the connection's open transaction runs at, read on
    the raw DB-API cursor so no execute wrapper sees it. On PostgreSQL the
    configured level applies from BEGIN, so only a read inside the
    transaction shows it.
    """
    if connection.vendor == "mysql":
        query = "SELECT @@transaction_isolation"
    else:
        query = "SELECT current_setting('transaction_isolation')"
    cursor = connection.connection.cursor()
    try:
        cursor.execute(query)
        (level,) = cursor.fetchone()
    finally:
        cursor.close()
    return str(level).lower().replace("-", " ").replace("_", " ")


@pytest.fixture
def isolated_project(project_isolation, monkeypatch):
    """
    DATABASES OPTIONS["isolation_level"], as a project sets it, for every
    connection opened during the test. Threads open their own connections
    from these settings; the main thread's is reopened.
    """
    alias = oxpull_db_alias()
    connection = connections[alias]
    if connection.vendor == "sqlite":
        pytest.skip(
            "SQLite has no row locks: writers serialise and the pass opens "
            "with a write, so every write waits on it by design"
        )
    if connection.vendor == "mysql":
        value: Any = project_isolation
    else:
        from psycopg import IsolationLevel

        value = IsolationLevel[project_isolation.upper().replace(" ", "_")]
    targets = {
        id(options): options
        for options in (
            connections.settings[alias]["OPTIONS"],
            connection.settings_dict["OPTIONS"],
        )
    }
    isolation.outlives_the_transaction(project_isolation)
    for options in targets.values():
        monkeypatch.setitem(options, "isolation_level", value)
    connection.close()
    yield project_isolation
    connection.close()


# -- threads that record what they send --------------------------------


class Recorded:
    """A callable on its own thread and connection, timing each statement."""

    def __init__(self, name: str, fn: Callable[[], Any]) -> None:
        self.fn = fn
        self.result: Any = None
        self.error: BaseException | None = None
        self.elapsed: float | None = None
        self.statements: list[tuple[str, float]] = []
        # Set once fn has returned or raised, with its connection still open.
        self.done = threading.Event()
        self._started = 0.0
        self.thread = threading.Thread(target=self._run, name=name)

    def _record(self, execute, sql, params, many, context):
        started = time.monotonic()
        try:
            return execute(sql, params, many, context)
        finally:
            text = " ".join(sql.split())
            self.statements.append((text[:70], time.monotonic() - started))

    def _run(self) -> None:
        connection = connections[oxpull_db_alias()]
        try:
            with connection.execute_wrapper(self._record):
                self.result = self.fn()
        except BaseException as exc:
            self.error = exc
        finally:
            self.elapsed = time.monotonic() - self._started
            self.done.set()
            connections.close_all()

    def start(self) -> None:
        self._started = time.monotonic()
        self.thread.start()

    def slowest(self) -> str:
        if not self.statements:
            return "no statements recorded"
        text, took = max(self.statements, key=lambda item: item[1])
        return f"slowest statement {took:.3f}s: {text}"


# -- the workflow ------------------------------------------------------


@dataclass
class Scenario:
    ids: dict[str, Any]
    live_ready: list[Any]
    running_worker: Any
    runner: Recorded
    bystander_worker: Any
    bystander: OxTask
    history: str

    @property
    def live(self) -> set[Any]:
        return {self.ids["running"], *self.live_ready}

    def status(self, pk: Any) -> str:
        return OxTask.objects.get(pk=pk).status

    def name_of(self, lock_data: str) -> str:
        value = lock_data.strip().strip("'")
        if value == self.bystander.pk.hex:
            return "bystander"
        for name, pk in self.ids.items():
            if value == pk.hex:
                return name
        return f"other:{lock_data}"


def analyze_tables() -> None:
    connection = connections[oxpull_db_alias()]
    tables = [
        OxTask._meta.db_table,
        OxpullWorkflowNode._meta.db_table,
        "oxpull_oxpullworkflowedge",
    ]
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            # A rebuild as well as the statistics: see the module docstring.
            cursor.execute("OPTIMIZE TABLE " + ", ".join(tables))
            cursor.fetchall()
        else:
            for table in tables:
                cursor.execute(f"ANALYZE {table}")


@pytest.fixture
def workflow(isolated_project, history):
    """
    root (FAILED) -> child (WAITING, blocked); running (RUNNING, a real
    lease); LIVE_READY claimable READY nodes with no edges. Sealed, so the
    reconciler sweeps it, and never complete while live nodes are pending.
    Beside it, the bystander: no workflow's node, RUNNING its last attempt.
    """
    alias = oxpull_db_alias()
    tasks.STATE["gate"] = threading.Event()
    if history == DISCARDED_HISTORY:
        results = [tasks.plain_echo.enqueue(i) for i in range(HISTORY_WIDTH)]
        changed, _ = actions.discard_many([result.id for result in results])
        assert changed == HISTORY_WIDTH

    flow = workflows.create("cancellation-locks")
    with transaction.atomic(using=alias):
        flow.add("root", tasks.failing_step.using(priority=10), "root")
        flow.add("child", tasks.step, "child", depends_on="root")
        flow.add("running", held_step.using(queue_name="emails", priority=5), "running")
        for index in range(LIVE_READY):
            name = f"zz-ready-{index:03d}"
            flow.add(name, tasks.step.using(queue_name="emails", priority=0), name)
    flow.seal()
    ids = dict(
        OxpullWorkflowNode.objects.using(alias)
        .filter(workflow_id=flow.id)
        .values_list("name", "task_id")
    )
    live_ready = [pk for name, pk in ids.items() if name.startswith("zz-ready-")]

    setup = flow_worker(queues=["default"])
    for _ in range(3):
        assert setup.run_once() is True
    assert order() == ["root"] * 3, order()
    assert setup.run_once() is False, "the child ran past its FAILED root"

    tasks.failing_step.using(priority=10).enqueue("bystander")
    bystander_worker = flow_worker(queues=["default"])
    for _ in range(2):
        assert bystander_worker.run_once() is True
    bystander = bystander_worker.claim_one()
    assert bystander is not None and bystander.args == ["bystander"]
    assert bystander.attempts == bystander.max_attempts

    running_worker = flow_worker(queues=["emails"])
    runner = Recorded("runner", running_worker.run_once)
    runner.start()
    scenario = Scenario(
        ids=ids,
        live_ready=live_ready,
        running_worker=running_worker,
        runner=runner,
        bystander_worker=bystander_worker,
        bystander=bystander,
        history=history,
    )
    try:
        deadline = time.monotonic() + 10
        while scenario.status(ids["running"]) != OxTask.Status.RUNNING:
            assert time.monotonic() < deadline, "the runner never claimed its node"
            time.sleep(0.02)
        assert scenario.status(ids["root"]) == OxTask.Status.FAILED
        assert scenario.status(ids["child"]) == OxTask.Status.WAITING
        assert scenario.status(bystander.pk) == OxTask.Status.RUNNING
        assert (
            OxTask.objects.filter(pk__in=live_ready, status=OxTask.Status.READY).count()
            == LIVE_READY
        )
        # The renewal probe works when nothing is in its way.
        assert running_worker.renew_leases() == 1
        analyze_tables()
        yield scenario
    finally:
        tasks.STATE["gate"].set()
        runner.thread.join(GATE_LIMIT + 30)


# -- the held pass -----------------------------------------------------


class HeldPass:
    """One real reconciler pass on its own thread, paused at one statement."""

    def __init__(self, point: str) -> None:
        self.point = point
        self.reached = threading.Event()
        self.resume = threading.Event()
        self.events: list[str] = []
        self.level: str | None = None
        self.session_level: str | None = None
        self.session: Any = None
        self.in_atomic_block: bool | None = None
        self.held_for: float | None = None
        self.returned: Any = None
        self.error: BaseException | None = None
        self.locking_sql: str | None = None
        self.locking_params: Any = None
        self.join_order: list[str] | None = None
        self._fired = False
        self.thread = threading.Thread(target=self._run, name="reconciler")

    def _run(self) -> None:
        connection = connections[oxpull_db_alias()]
        try:
            self.session_level, self.session = session_facts(connection)
            with connection.execute_wrapper(self._statement):
                self.returned = reconcile()
        except BaseException as exc:
            self.error = exc
        finally:
            connections.close_all()

    def _statement(self, execute, sql, params, many, context):
        text = " ".join(sql.split()).upper()
        on_tasks = TASK_TABLE in text
        locking = on_tasks and text.startswith("SELECT") and "FOR UPDATE" in text
        # MySQL's optimizer trace of the locking read itself says which plan
        # this execution ran. An EXPLAIN run afterwards can disagree.
        trace = ON_MYSQL and locking and self.locking_sql is None
        connection = connections[oxpull_db_alias()]
        if trace:
            raw_statement(connection, "SET SESSION optimizer_trace = 'enabled=on'")
        result = execute(sql, params, many, context)
        if trace:
            self.join_order = traced_join_order(connection)
            raw_statement(connection, "SET SESSION optimizer_trace = 'enabled=off'")
        if locking:
            label = "locking read"
            if self.locking_sql is None:
                self.locking_sql, self.locking_params = sql, params
        elif on_tasks and text.startswith("UPDATE") and self.locking_sql:
            label = "discard update"
        else:
            label = text[:48]
        self.events.append(label)
        if not self._fired and label == LABEL_AT[self.point]:
            self._fired = True
            self.in_atomic_block = connections[oxpull_db_alias()].in_atomic_block
            self.level = transaction_level(connections[oxpull_db_alias()])
            started = time.monotonic()
            self.reached.set()
            self.resume.wait(HOLD_LIMIT)
            self.held_for = time.monotonic() - started
        return result


def raw_statement(connection, sql: str) -> list[tuple[Any, ...]]:
    """One statement on the raw DB-API cursor, where no execute wrapper sees it."""
    cursor = connection.connection.cursor()
    try:
        cursor.execute(sql)
        return list(cursor.fetchall())
    finally:
        cursor.close()


def traced_join_order(connection) -> list[str]:
    """
    The tables of the statement this connection just traced, in the join order
    it ran, from refine_plan in MySQL's optimizer trace.
    """
    rows = raw_statement(
        connection, "SELECT TRACE FROM information_schema.OPTIMIZER_TRACE"
    )
    pending = [json.loads(rows[0][0])] if rows else []
    while pending:
        item = pending.pop(0)
        if isinstance(item, dict):
            if isinstance(item.get("refine_plan"), list):
                return [str(entry.get("table", "")) for entry in item["refine_plan"]]
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return []


def executed_plan(held: HeldPass) -> str:
    """node-driven when the traced locking read read the node table first."""
    if not held.join_order:
        return "unknown"
    if OxpullWorkflowNode._meta.db_table in held.join_order[0]:
        return "node-driven"
    return "task-driven"


def plan_of(held: HeldPass) -> tuple[str, str]:
    """EXPLAIN of the statement the pass locked with; it takes no locks."""
    if held.locking_sql is None:
        return "none", ""
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            cursor.execute(
                "EXPLAIN FORMAT=TREE " + held.locking_sql, held.locking_params
            )
            text = str(cursor.fetchone()[0])
        else:
            cursor.execute("EXPLAIN " + held.locking_sql, held.locking_params)
            text = "\n".join(str(row[0]) for row in cursor.fetchall())
    # Node-driven: the node table, read for the workflow's id by index or by a
    # filtered scan, drives the join, and the task table follows by primary key.
    nodes = re.search(
        r"lookup on U0 using oxpull_wf_node_\w+ \(workflow_id='\w+'"
        r"|Filter: \(U0\.workflow_id = '\w+'\)[^\n]*\n\s*-> Table scan on U0",
        text,
    )
    tasks_by_pk = text.find(f"on {OxTask._meta.db_table} using PRIMARY")
    if "using ox_reaper_idx" in text:
        kind = "reaper-index-range"
    elif nodes is not None and tasks_by_pk > nodes.start():
        kind = "node-driven"
    else:
        kind = "other"
    return kind, text


def waits_on_the_pass(held: HeldPass) -> list[tuple[Any, ...]]:
    """
    The lock waits the database reports, right now, behind the pass's
    connection: what each waiter asks for and the statement it is running.

    A lock the pass holds is held until the pass commits, and the pass is not
    released while a probe runs, so a probe waiting on the pass is still
    waiting at the next poll, whenever that comes.
    """
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            cursor.execute(
                "SELECT l.INDEX_NAME, l.LOCK_MODE, l.LOCK_DATA, "
                "LEFT(waiter.PROCESSLIST_INFO, 70) "
                "FROM performance_schema.data_lock_waits w "
                "JOIN performance_schema.threads holder "
                "ON holder.THREAD_ID = w.BLOCKING_THREAD_ID "
                "JOIN performance_schema.threads waiter "
                "ON waiter.THREAD_ID = w.REQUESTING_THREAD_ID "
                "JOIN performance_schema.data_locks l "
                "ON l.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID "
                "WHERE holder.PROCESSLIST_ID = %s",
                [held.session],
            )
        else:
            cursor.execute(
                "SELECT wait_event, pid, LEFT(query, 70) FROM pg_stat_activity "
                "WHERE wait_event_type = 'Lock' AND %s = ANY(pg_blocking_pids(pid))",
                [held.session],
            )
        return list(cursor.fetchall())


def open_transaction_of(held: HeldPass) -> tuple[Any, ...] | None:
    """
    The pass's transaction as the database has it, or None when its connection
    has none open: on MySQL InnoDB's state for it and the rows it has locked,
    on PostgreSQL the session's state and its transaction id.
    """
    connection = connections[oxpull_db_alias()]
    with connection.cursor() as cursor:
        if connection.vendor == "mysql":
            cursor.execute(
                "SELECT trx_state, trx_rows_locked "
                "FROM information_schema.INNODB_TRX "
                "WHERE trx_mysql_thread_id = %s AND trx_rows_locked > 0",
                [held.session],
            )
        else:
            cursor.execute(
                "SELECT state, backend_xid FROM pg_stat_activity WHERE pid = %s "
                "AND state = 'idle in transaction' AND backend_xid IS NOT NULL",
                [held.session],
            )
        return cursor.fetchone()


@dataclass
class Outcome:
    held: HeldPass
    probe: Recorded
    inside: bool | None
    plan: str = "none"
    plan_text: str = ""
    snapshot: Any = None
    waits: list[tuple[Any, ...]] = field(default_factory=list)
    open_transaction: tuple[Any, ...] | None = None
    notes: list[str] = field(default_factory=list)

    def describe(self) -> str:
        elapsed = self.probe.elapsed
        held_for = self.held.held_for
        return (
            f"level={self.held.level} plan={self.plan} "
            f"ran={executed_plan(self.held)} {self.held.join_order} "
            f"point={self.held.point} "
            f"probe finished inside the pass={self.inside} "
            f"lock waits on the pass={self.waits} "
            f"the pass's transaction after the probe={self.open_transaction} "
            f"probe elapsed={None if elapsed is None else round(elapsed, 3)}s "
            f"({self.probe.slowest()}) error={self.probe.error!r} "
            f"pass held={None if held_for is None else round(held_for, 3)}s"
        )


def during_the_held_pass(
    point: str,
    probe_fn: Callable[[], Any],
    before_probe: Callable[[HeldPass], Any] | None = None,
) -> Outcome:
    """
    Run the probe while the pass is held, and release the pass only once the
    probe is done, the database reports a wait on the pass, or PROBE_LIMIT is
    up.

    inside is the verdict. It is True only when the probe finished, nothing was
    seen waiting on the pass, and after the probe finished the hold had not
    been released and the database still showed the pass's transaction open
    with its locks.
    """
    held = HeldPass(point)
    held.thread.start()
    probe = Recorded("probe", probe_fn)
    outcome = Outcome(held=held, probe=probe, inside=None)
    # The pass is released whatever a read in here raises, so a failure is
    # reported at once and not after HOLD_LIMIT, with the pass's locks still
    # in the way of the next test.
    try:
        if held.reached.wait(PASS_REACH_LIMIT):
            outcome.plan, outcome.plan_text = plan_of(held)
            if before_probe is not None:
                outcome.snapshot = before_probe(held)
            probe.start()
            deadline = time.monotonic() + PROBE_LIMIT
            while not probe.done.wait(WAIT_POLL):
                outcome.waits = waits_on_the_pass(held)
                if outcome.waits or time.monotonic() >= deadline:
                    break
            finished = probe.done.is_set()
            outcome.open_transaction = open_transaction_of(held)
            outcome.inside = (
                finished
                and not outcome.waits
                and held.held_for is None
                and outcome.open_transaction is not None
            )
    finally:
        held.resume.set()
        held.thread.join(60)
        if probe.thread.ident is not None:
            probe.thread.join(GATE_LIMIT + 30)
    return outcome


def assert_the_pass_was_real_and_held(
    outcome: Outcome, scenario: Scenario, project_isolation: str
) -> None:
    held = outcome.held
    assert held.reached.is_set(), (
        f"the pass never reached {held.point}: {held.events} error={held.error!r}"
    )
    assert held.level == project_isolation, held.level
    assert held.in_atomic_block is True
    assert not held.thread.is_alive(), "the pass never finished"
    assert held.error is None, repr(held.error)
    assert not outcome.probe.thread.is_alive(), "the probe never finished"
    # The pass did its real work and completed nothing: live nodes were
    # pending when it read them.
    assert held.returned == 0
    assert scenario.status(scenario.ids["child"]) == OxTask.Status.DISCARDED
    print(f"\nPLAN {outcome.plan}:\n{outcome.plan_text}")  # noqa: T201
    expected = PLAN_OF_HISTORY.get(scenario.history)
    # Only a locking read that names the node table can run a node-driven
    # plan. One that names task rows by primary key alone has no such plan to
    # provoke, and the cell then tests that read as it runs.
    joins_nodes = (
        OxpullWorkflowNode._meta.db_table.upper()
        in " ".join((held.locking_sql or "").split()).upper()
    )
    if (
        ON_MYSQL
        and expected is not None
        and joins_nodes
        and executed_plan(held) != expected
    ):
        raise PlanNotProvoked(
            f"the {scenario.history} cell is about the {expected} plan, and the "
            f"pass ran {executed_plan(held)}: " + outcome.describe()
        )


# -- the checks, shared with the controls file --------------------------


def check_renewal(workflow: Scenario, project_isolation: str, point: str) -> None:
    outcome = during_the_held_pass(point, workflow.running_worker.renew_leases)
    assert_the_pass_was_real_and_held(outcome, workflow, project_isolation)
    print("RESULT renewal", outcome.describe())  # noqa: T201

    assert outcome.inside, (
        "the RUNNING node's lease renewal waited on the cancellation pass: "
        + outcome.describe()
    )
    assert outcome.probe.error is None
    assert outcome.probe.result == 1


def check_outcome_write(workflow: Scenario, project_isolation: str, point: str) -> None:
    runner = workflow.runner

    def finish():
        tasks.STATE["gate"].set()
        runner.thread.join(GATE_LIMIT)
        return runner.result

    outcome = during_the_held_pass(point, finish)
    assert_the_pass_was_real_and_held(outcome, workflow, project_isolation)
    print("RESULT outcome", outcome.describe(), "runner", runner.slowest())  # noqa: T201

    assert outcome.inside, (
        "the RUNNING node's outcome write waited on the cancellation pass: "
        + outcome.describe()
        + "; runner "
        + runner.slowest()
    )
    assert runner.error is None, repr(runner.error)
    assert runner.result is True
    assert workflow.status(workflow.ids["running"]) == OxTask.Status.SUCCESSFUL


def check_claimable(workflow: Scenario, project_isolation: str, point: str) -> None:
    claimer = flow_worker(queues=["emails"])

    def claim_everything():
        claimed = []
        while (row := claimer.claim_one()) is not None:
            claimed.append(row.pk)
        return claimed

    outcome = during_the_held_pass(point, claim_everything)
    assert_the_pass_was_real_and_held(outcome, workflow, project_isolation)
    claimed = set(outcome.probe.result or [])
    passed_over = set(workflow.live_ready) - claimed
    names = sorted(name for name, pk in workflow.ids.items() if pk in passed_over)
    print(  # noqa: T201
        "RESULT claim",
        outcome.describe(),
        f"claimed={len(claimed)} passed over={len(passed_over)} {names[:5]}",
    )

    assert outcome.probe.error is None, repr(outcome.probe.error)
    assert not passed_over, (
        f"a worker claiming during the cancellation pass passed over "
        f"{len(passed_over)} of {LIVE_READY} claimable READY nodes {names[:5]}: "
        + outcome.describe()
    )
    assert claimed == set(workflow.live_ready)
    assert outcome.inside, outcome.describe()


def locks_held_by_the_pass(held: HeldPass) -> list[tuple[Any, ...]]:
    with connections[oxpull_db_alias()].cursor() as cursor:
        cursor.execute(
            "SELECT l.INDEX_NAME, l.LOCK_MODE, l.LOCK_STATUS, l.LOCK_DATA "
            "FROM performance_schema.data_locks l "
            "JOIN performance_schema.threads t ON t.THREAD_ID = l.THREAD_ID "
            "WHERE t.PROCESSLIST_ID = %s AND l.OBJECT_SCHEMA = DATABASE() "
            "AND l.OBJECT_NAME = %s AND l.LOCK_TYPE = 'RECORD'",
            [held.session, OxTask._meta.db_table],
        )
        return list(cursor.fetchall())


def check_no_live_node_locked(
    workflow: Scenario, project_isolation: str, point: str
) -> None:
    if connections[oxpull_db_alias()].vendor != "mysql":
        pytest.skip("performance_schema.data_locks is InnoDB's")
    outcome = during_the_held_pass(point, lambda: None, locks_held_by_the_pass)
    assert_the_pass_was_real_and_held(outcome, workflow, project_isolation)
    live_hex = {pk.hex: pk for pk in workflow.live}
    root_hex = workflow.ids["root"].hex
    locked_live = []
    root_locked = False
    other = []
    for index, mode, status, data in outcome.snapshot:
        key = (data or "").strip().strip("'")
        if index == "PRIMARY" and status == "GRANTED" and key == root_hex:
            root_locked = True
        elif index == "PRIMARY" and status == "GRANTED" and key in live_hex:
            locked_live.append(workflow.name_of(data))
        else:
            other.append((index, mode, status, workflow.name_of(data or "")))
    print(  # noqa: T201
        "RESULT data_locks",
        outcome.describe(),
        f"live nodes locked={len(locked_live)} {sorted(locked_live)[:5]} "
        f"other task-table locks={other[:8]}",
    )

    # The interlock's own lock is visible, so this really is the pass's
    # transaction being read.
    assert root_locked, outcome.snapshot
    assert not locked_live, (
        f"the cancellation pass holds row locks on {len(locked_live)} live "
        f"node(s) {sorted(locked_live)[:5]}: " + outcome.describe()
    )


def check_bystander_failure(
    workflow: Scenario, project_isolation: str, point: str
) -> None:
    worker, row = workflow.bystander_worker, workflow.bystander
    outcome = during_the_held_pass(point, lambda: worker.execute(row))
    assert_the_pass_was_real_and_held(outcome, workflow, project_isolation)
    print("RESULT bystander", outcome.describe())  # noqa: T201

    assert outcome.inside, (
        "the final failure of a task in no workflow waited on the cancellation "
        "pass: " + outcome.describe()
    )
    assert outcome.probe.error is None, repr(outcome.probe.error)
    assert workflow.status(row.pk) == OxTask.Status.FAILED


# -- the tests ---------------------------------------------------------


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("history,project_isolation", LIVE_NODE_CELLS)
def test_a_running_nodes_lease_renewal_does_not_wait_on_the_cancellation(
    workflow, project_isolation, point
):
    """renew_leases() touches only this worker's RUNNING row."""
    check_renewal(workflow, project_isolation, point)


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("history,project_isolation", LIVE_NODE_CELLS)
def test_a_running_nodes_outcome_write_does_not_wait_on_the_cancellation(
    workflow, project_isolation, point
):
    """The task returns while the pass is open; its outcome write must land."""
    check_outcome_write(workflow, project_isolation, point)


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("history,project_isolation", CLAIMABLE_CELLS)
def test_every_claimable_node_can_be_claimed_during_the_cancellation(
    workflow, project_isolation, point
):
    """No claimable READY node is skipped as locked by a pass not about it."""
    check_claimable(workflow, project_isolation, point)


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("history,project_isolation", CLAIMABLE_CELLS)
def test_the_cancellation_locks_no_live_node_of_the_workflow(
    workflow, project_isolation, point
):
    """The docstring's guard, read from InnoDB's own lock table."""
    check_no_live_node_locked(workflow, project_isolation, point)


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("history,project_isolation", BYSTANDER_CELLS)
def test_an_unrelated_tasks_final_failure_does_not_wait_on_the_cancellation(
    workflow, project_isolation, point
):
    """A task in no workflow fails its last attempt while the pass is open."""
    check_bystander_failure(workflow, project_isolation, point)
