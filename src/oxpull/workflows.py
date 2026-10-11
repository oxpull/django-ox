"""
Workflows: a DAG of tasks, where a node is claimable only once every
node it depends on has succeeded.

    from oxpull import workflows

    flow = workflows.create("nightly", on_complete=summarize)
    flow.add("extract", extract)
    flow.add("transform-a", transform, "a", depends_on="extract")
    flow.add("transform-b", transform, "b", depends_on="extract")
    flow.add("load", load, depends_on=["transform-a", "transform-b"])
    flow.seal()

Workflows create blocked task rows with status WAITING. Release moves a
WAITING task to READY and updates run_after. Cancellation moves a READY or
WAITING task to DISCARDED. These writes use a compare-and-set on the task
row's status and lease epoch; the workflow node stores no lease epoch.
run_after is never NULL, and a DISCARDED node is not revived.

The workflow generation field is reserved and remains 0. It is not a
workflow lease epoch.

Every move out of WAITING is a compare-and-set on the row's status and
lease epoch. Two releasers cannot both move a row, and a release decided
before a cancellation matches nothing. The worker's claim still checks the
edges as a second line of defence. It refuses a READY row while any edge
into it is not marked satisfied and its upstream row is absent or not
SUCCESSFUL. A missing upstream row with no record is never taken as
success.

Completion reads every node in one grouped statement, beside its task row.
A node is pending while its row is there and not settled (SUCCESSFUL, FAILED,
LOST or DISCARDED). WAITING is pending, and so is a status this package does
not know. A settled node counts by its record, the outcome a worker, the
reconciler or the cancellation wrote on the node row, so a success or a
failure still counts after ox_prune deletes its row. A record behind its row
is repaired first, and the workflow completes on a later tick. A row gone with
no record is finished and unattributed. State is never accumulated from
task_finished signals, for the reason written down in batches.py: the
django-ox reaper settles a dead worker's task through a queryset update
that emits no signal at all, so a workflow that counted signals would hang
in exactly the case workflows exist to survive.

The claim's dependency check lives in the worker, so it applies only
where OPTIONS names one that carries it:

    TASKS = {
        "default": {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "OPTIONS": {
                "WORKER_CLASS": "oxpull.worker.OxpullWorker",
                "SCHEDULES": {
                    "oxpull-workflow-reconcile": {
                        "task": "oxpull.workflows.reconcile",
                        "cron": "* * * * *",
                    },
                },
            },
        }
    }

A workflow is created at runtime, not declared in settings, so no system
check can see that one is in use. add() therefore resolves the task's own
backend and refuses a worker that would not apply the gate, on the line
that creates the work rather than three hours later out of order.

A unique task cannot be a node, and add() refuses one before it enqueues
anything, asking the backend the task enqueues through, as the enqueue
does. A unique enqueue returns whichever task already holds the key, so
the node could be another workflow's node or a call the application
enqueued itself, and one workflow's failure would cancel it. A shared
expensive job belongs outside the workflow, as a unique task. To make a
task unique that is already used as a node, drain its pending nodes
first: from then on add() refuses it, and a node added before the change
holds no uniqueness lock, so a unique enqueue of the same call is not
deduplicated against it and runs beside it.

The reconciler does three things no single write can. Its backstop pages
through each open workflow's unsatisfied edges, then its nodes, then its
waiting nodes. It marks the edges whose upstream succeeded, records the
outcomes no worker recorded, and releases what is left waiting. It cancels
the downstream cone of a node that settled without succeeding. And it runs
the compare-and-set that completes a sealed workflow and enqueues its
callbacks. Each is idempotent, and a crash leaves the next tick to redo it.
"""

from __future__ import annotations

# The gate raises on Django 5.2, before django.tasks is imported below.
from oxpull import _requires_django_6  # noqa: F401  # isort: skip

import bisect
import hashlib
import logging
import math
import random
import re
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext, suppress
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from django.conf import settings
from django.core.exceptions import EmptyResultSet, ImproperlyConfigured
from django.db import (
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    connections,
    transaction,
)
from django.db.models import (
    BooleanField,
    Case,
    CharField,
    Count,
    Exists,
    F,
    Max,
    Model,
    OuterRef,
    Q,
    QuerySet,
    Subquery,
    Value,
    When,
)
from django.db.models.functions import Coalesce
from django.db.models.lookups import Exact, In
from django.db.models.sql.datastructures import BaseTable
from django.tasks.base import Task, TaskResult
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.datastructures import OrderedSet

from django_ox import _waiting, remaining
from django_ox.actions import RETRYABLE_STATUSES
from django_ox.models import OxTask

from .batches import (
    ADD_RETRY_JITTER,
    DEADLOCK,
    NAME_MAX_LENGTH,
    ReconcilerSchedule,
    _binds,
    _build_task_on_a_queue,
    _enqueue_callback,
    _ox_backend_options,
    _reissue,
    _reissue_add,
    _require_ox_backend,
    _resolve_callback,
    _sealed_without_a_total,
    _sweep,
    _timed_out,
    reconcile_schedule,
    schedule_entry_hint,
    task_backend_classes,
    unbound_reconciler_hint,
    unbound_reconciler_message,
)
from .batches import PENDING_STATUSES as PENDING_STATUSES
from .batches import SETTLED_STATUSES as SETTLED_STATUSES
from .batches import UNATTRIBUTED_STATUSES as UNATTRIBUTED_STATUSES
from .exceptions import UnknownWorkflowNode, WorkflowNodeExists, WorkflowSealed
from .models import (
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
    OxpullWorkflowRound,
    OxpullWorkflowScan,
)
from .routing import oxpull_db_alias

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from django.db.models import Field

    # Built by __getattr__ at the bottom of this module, for the reason
    # given there; declared here so the public name is visible to type
    # checkers and linters.
    reconcile: Task[[], int]

__all__ = [
    "RECONCILE_TASK_PATH",
    "Workflow",
    "WorkflowProgress",
    "create",
    "get",
    "name_key",
    "node_flag",
    "node_flag_for_a_locking_read",
    "node_flag_sql",
    "reconcile",
    "record_node_outcome",
    "release_downstream",
    "runnable_filter_q",
    "runnable_filter_sql",
    "workflows_enabled_from_options",
    "workflows_on_database",
]

# The dotted path the reconciler's task row carries, and therefore the
# string a SCHEDULES entry must name. Written once here and read by
# OxpullBackend._check_workflow_reconciler, so the check and the schedule
# cannot drift apart: a rename that missed one of them would leave the
# check warning about a schedule that is present, or silent about one
# that is not.
RECONCILE_TASK_PATH = f"{__name__}.reconcile"

# The schedule name every hint gives the reconciler.
RECONCILE_SCHEDULE_NAME = "oxpull-workflow-reconcile"

# The hint for a project whose schedules are rows; batches'
# RECONCILE_SCHEDULABLE_HINT says why it is not the settings block.
RECONCILE_SCHEDULABLE_HINT = (
    "Expose the reconciler to the schedule rows: add to a django-ox "
    'backend\'s OPTIONS "SCHEDULABLE_TASKS": '
    f'{{"{RECONCILE_SCHEDULE_NAME}": "{RECONCILE_TASK_PATH}"}}, then add a '
    "schedule for that key."
)

# What happens to workflows with nothing running the reconciler.
UNSCHEDULED_CONSEQUENCE = (
    "a sealed workflow would never complete, and the dependents of a failed "
    "node would never be cancelled"
)

# An upstream in one of these has settled without succeeding, so nothing
# downstream of it can ever run. LOST is included deliberately: it says
# the attempt was never observed, and "we do not know that it worked" is
# not "it worked". DISCARDED says an operator closed it.
DEAD_STATUSES = (
    OxTask.Status.FAILED,
    OxTask.Status.LOST,
    OxTask.Status.DISCARDED,
)

# The dead statuses django_ox.actions.retry moves back to READY.
RETRYABLE_DEAD_STATUSES = RETRYABLE_STATUSES

# A node in one of these has not run, so the cancellation may close it.
CANCELLABLE_STATUSES = (OxTask.Status.READY, OxTask.Status.WAITING)

# Completion and progress() read SETTLED_STATUSES, through _classify:
# SUCCESSFUL and the dead statuses above are settled, and every other status
# is pending, including one a later django-ox adds. A status this package does
# not know therefore holds a workflow open rather than completing it and
# attributing an outcome to it. The suite asserts that SETTLED_STATUSES is
# exactly SUCCESSFUL plus DEAD_STATUSES, and that it covers every OxTask.Status
# with PENDING_STATUSES.

# Most task ids one statement carries as a literal IN list. A fan-out or a
# doomed cone can be any width, and one list of all of it runs into a
# limit on every engine: PostgreSQL refuses more than 65,535 bound
# parameters under server-side binding, SQLite refuses more than its
# variable limit (32,766 by default, 999 before 3.32), and MySQL past its
# range optimizer's memory budget (about 6,000 ids at the 8.4 default)
# plans a scan of the whole task table. The same size as django-ox's
# UPDATE_CHUNK_SIZE, for the same reason.
ID_CHUNK_SIZE = 1000

# Bound parameters a chunked statement carries besides its ids.
PARAMETERS_BESIDE_IDS = 8

# Bound parameters one id takes on SQLite, where an edge read spells each id
# as a range and binds both of its ends (_KeysProbe).
SQLITE_PARAMETERS_PER_ID = 2


def _id_chunk_size(db_alias: str) -> int:
    """How many ids one statement on this database carries at most."""
    size = ID_CHUNK_SIZE
    connection = connections[db_alias]
    limit = connection.features.max_query_params
    if limit:
        per_id = SQLITE_PARAMETERS_PER_ID if connection.vendor == "sqlite" else 1
        size = max(1, min(size, (limit - PARAMETERS_BESIDE_IDS) // per_id))
    return size


def _id_chunks(ids: Iterable[Any], db_alias: str) -> Iterator[list[Any]]:
    """`ids` sorted, in lists short enough for one statement on this database."""
    size = _id_chunk_size(db_alias)
    ordered = sorted(ids)
    for start in range(0, len(ordered), size):
        yield ordered[start : start + size]


# Rows one page of the reconciler's backstop reads: edges to mark satisfied,
# nodes whose record to repair, or nodes to release.
BACKSTOP_PAGE = 1000

# Most pages one visit gives one workflow. A visit pages on only while its
# pages come back full, so a workflow with many open rows gets up to this many
# pages a visit, and a workflow with few gets one short page a phase.
VISIT_PAGES = 8

# Seconds after which a reconciler tick starts no further backstop visit or
# page. Under a task timeout it is a quarter of what is left, if that is less.
# It is checked only between visits and between pages, so it does not bound a
# tick: see _release_backstop for what is bounded.
BACKSTOP_BUDGET = 5.0

# Seconds a backstop write waits for a row lock, and runs one statement.
BACKSTOP_LOCK_TIMEOUT = 1.0
BACKSTOP_STATEMENT_TIMEOUT = 2.0

# Bound on the cancellation fixpoint, per reconciler pass. A pass only
# iterates while it is still changing rows, and each iteration cancels at
# least one node, so this is reached only by a DAG deeper than this many
# levels, which the next tick continues from where this one stopped.
MAX_CANCEL_PASSES = 64


@dataclass(frozen=True)
class WorkflowProgress:
    """
    Live counts for one workflow, computed on demand from node task rows.

    total is the sealed snapshot once the workflow is sealed, and the
    live node count while it is open. blocked is the part of pending that
    cannot run yet: nodes WAITING for an upstream, plus READY nodes the
    claim's dependency check refuses. The rest of pending is runnable or
    running. The split is a second query and is the number an operator
    actually wants when a DAG looks stuck.

    succeeded and failed count how each node settled: its record where the
    record is the last word, so a failure whose row ox_prune deleted still
    counts as failed. pending, succeeded and failed need not add up to
    total. The difference is unattributed, and it means the same three
    things it means for a batch: a node whose task row ox_prune deleted
    before anything recorded how it finished, a node the reaper marked
    LOST, and a node an operator (or this workflow's own cancellation)
    discarded. All are finished, and none is attributable to
    success or failure, which is why on_success stays withheld while it
    is above zero.
    """

    total: int
    pending: int
    blocked: int
    succeeded: int
    failed: int

    @property
    def unattributed(self) -> int:
        return max(self.total - (self.pending + self.succeeded + self.failed), 0)

    @property
    def finished(self) -> int:
        return self.total - self.pending


# -- the gate ----------------------------------------------------------


def name_key(name: str) -> str:
    """
    A node name's identity: the SHA-256 hex digest of its UTF-8 bytes.

    Two names are one name only when they are spelled the same. The name
    column's collation plays no part, so MySQL's folding of case, accents
    and trailing spaces neither refuses a second name nor resolves the wrong
    node.
    """
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


class _KeyProbe(Exact):
    """
    `column = key`, with a range on SQLite that holds the same one key.

    From 3.45 to 3.49 SQLite marks an index whose statistics show one key
    for every row, which is what one wide fan-in or fan-out alone in the edge
    table leaves after ANALYZE. It does not probe a marked index with =, IN
    or IS, and the mark stays on the connection until its schema is read
    again, whatever statistics come later. A range is still allowed, so
    SQLite gets `column BETWEEN key AND key` beside the equality. It holds
    exactly the rows the equality holds. A planner that may use the equality
    keeps its plan and its estimate, and one that may not seeks the range
    instead of reading the whole index.

    Every other database compiles what `exact` compiles. The lookup keeps
    that name, which the backends read for the operator and the casts, and
    is registered on the edge columns as `probe`.
    """

    def as_sqlite(self, compiler: Any, connection: Any) -> tuple[str, Any]:
        equality, equality_params = self.as_sql(compiler, connection)
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        sql = f"({equality} AND {lhs} BETWEEN {rhs} AND {rhs})"
        return sql, [*equality_params, *lhs_params, *rhs_params, *rhs_params]


class _KeysProbe(In):
    """
    `column IN (keys)`, which SQLite gets as one range per key.

    The list form of _KeyProbe, for the same reason. Each key is its own
    `column BETWEEN key AND key`, so the ranges hold exactly the keys in the
    list and never one that sorts between two of them. There is no IN beside
    them: given both, SQLite 3.53 spends longer choosing between a long IN
    and a long OR than either takes to run, and then tests each row it finds
    against the whole OR. The terms are nested as a balanced tree, because
    SQLite refuses an expression more than 1,000 levels deep and a flat OR is
    as deep as it is long. Each key binds twice (SQLITE_PARAMETERS_PER_ID).

    The ranges are what a marked index may be read by; whether the planner
    reads it at all is a second question, and a read of more columns than the
    index carries is where it answers no. So the three reads that take a chunk
    of ids name their index as well (_NamedIndex).

    A list that is a subquery, or that holds an expression, compiles as `in`
    does. So does every other database. Registered as `probe_in`.
    """

    def as_sqlite(self, compiler: Any, connection: Any) -> tuple[str, Any]:
        if not self.rhs_is_direct_value() or any(
            hasattr(key, "resolve_expression") for key in self.rhs
        ):
            return self.as_sql(compiler, connection)
        # As `in` reads its list: NULL equals nothing, and a key counts once.
        keys = OrderedSet(self.rhs)
        keys.discard(None)
        if not keys:
            raise EmptyResultSet
        lhs, lhs_params = self.process_lhs(compiler, connection)
        placeholders, values = self.batch_process_rhs(compiler, connection, keys)
        ranges = [f"{lhs} BETWEEN {mark} AND {mark}" for mark in placeholders]
        params: list[Any] = []
        for value in values:
            params += [*lhs_params, value, value]
        return _any_of(ranges), params


def _any_of(terms: list[str]) -> str:
    """`terms` joined by OR, nested no deeper than the logarithm of their number."""
    if len(terms) == 1:
        return terms[0]
    half = len(terms) // 2
    return f"({_any_of(terms[:half])} OR {_any_of(terms[half:])})"


class _AsideProbe(Exact):
    """
    `column = value`, put beyond every index's reach on SQLite.

    oxpull_wf_edge_down_idx is (downstream_task_id, satisfied), and Django
    spells `satisfied=False` as an equality from 6.1 on where 6.0 negated the
    column. A marked index may not be probed with an equality, and one offered
    on its second column costs the planner the branch an id that the ranges on
    the first give it: it reads the named index whole instead, for every row
    the list is long. SQLite's unary plus is a no-op that puts a term beyond
    any index, which is where this term belongs.

    Unary plus also drops the column's affinity, so this goes on a column
    whose values and whose keys are of one type, and on no other. Every other
    database compiles what `exact` compiles. Registered on the edge's
    `satisfied` as `aside`.

    The comparison is written out rather than taken from `exact`, which up to
    Django 6.0 gives a boolean column the column itself and a plus in front of
    a NOT is not an expression SQLite reads. So SQLite is sent one spelling
    whatever Django's version.
    """

    def as_sqlite(self, compiler: Any, connection: Any) -> tuple[str, Any]:
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        return (
            f"+{lhs} {self.get_rhs_op(connection, rhs)}",
            [*lhs_params, *rhs_params],
        )


def _register_probes() -> None:
    """
    Give the two columns an edge is looked up by with no workflow in hand
    their `probe` and `probe_in`. Each leads an index that one wide fan-in,
    or one wide fan-out, fills with a single key. The lookups go on these two
    fields and on no other UUIDField, and `aside` goes on the column that
    follows one of them in its index.
    """
    for column in ("downstream_task_id", "upstream_task_id"):
        field = cast("Field[Any, Any]", OxpullWorkflowEdge._meta.get_field(column))
        field.register_lookup(_KeyProbe, "probe")
        field.register_lookup(_KeysProbe, "probe_in")
    settled = cast("Field[Any, Any]", OxpullWorkflowEdge._meta.get_field("satisfied"))
    settled.register_lookup(_AsideProbe, "aside")


_register_probes()


def _blocking_edges(downstream: Any) -> QuerySet[OxpullWorkflowEdge]:
    """
    The edges into `downstream` that still hold it back.

    An edge holds its downstream back unless it is marked satisfied or its
    upstream row is there and SUCCESSFUL. A missing upstream row with no
    mark blocks: a row can be missing because ox_prune deleted a DISCARDED
    or FAILED upstream, or because someone deleted it by hand, and neither
    says it succeeded.

    The upstream status is a scalar subquery on the task table's primary
    key, coalesced to "" when the row is gone. A satisfied edge costs one
    index entry on (downstream_task_id, satisfied) and no read of the task
    table. The scalar form, rather than a nested NOT EXISTS, is what keeps
    PostgreSQL from planning a hash anti-join over the whole task table.

    The downstream id is matched with _KeyProbe, here rather than in
    _EdgeProbe, so that progress(), which reads these edges with no hint,
    seeks the index on SQLite as the claim does.
    """
    upstream_status = Subquery(
        OxTask.objects.filter(pk=OuterRef("upstream_task_id")).values("status")[:1]
    )
    return (
        OxpullWorkflowEdge.objects.filter(
            downstream_task_id__probe=downstream, satisfied=False
        )
        .alias(upstream_status=Coalesce(upstream_status, Value("")))
        .exclude(upstream_status=OxTask.Status.SUCCESSFUL)
    )


# The index the claim's dependency check reads, one probe per candidate row.
EDGE_DOWNSTREAM_INDEX = "oxpull_wf_edge_down_idx"

# The index the reads out of a node take, on the other task id column.
EDGE_UPSTREAM_INDEX = "oxpull_wf_edge_up_idx"


class _EdgeProbe(Exists):
    """
    Exists over _blocking_edges that tells SQLite and MySQL to probe
    EDGE_DOWNSTREAM_INDEX for each candidate row.

    Their planners choose from statistics, and statistics taken while one wide
    fan-in fills the edge table say a downstream id matches thousands of edges.
    SQLite then scans the whole edge table for each candidate row, and MySQL
    reads the whole table into a temporary table for each claim. A candidate
    has no edge, or a few, so both are wrong. SQLite gets INDEXED BY, which
    names the index the check reads. It does not say how: a read of the whole
    named index satisfies it. On SQLite 3.45 to 3.49 an index whose
    statistics show one key cannot be probed with =, IN or IS, so the range
    _KeyProbe adds to the equality is what keeps the read a probe there.
    MySQL gets FORCE INDEX, and a NO_SEMIJOIN hint, which keeps the check a
    probe per candidate row instead of a join against the whole table.
    PostgreSQL has no hints, and 0004 sets the column's n_distinct so its
    estimate stays small (see that migration).

    The hint is added to the SQL Django compiles, so the probe keeps whatever
    alias Django gives the outer table. Where it goes depends on how Django
    renders the edge table's own FROM item, and the alias there is quoted on
    some versions and bare on others: see pin_edge_probe.
    """

    def as_sql(
        self,
        compiler: Any,
        connection: Any,
        template: str | None = None,
        **extra_context: Any,
    ) -> tuple[str, Any]:
        sql, params = super().as_sql(compiler, connection, template, **extra_context)
        return pin_edge_probe(sql, connection), params


def pin_edge_probe(sql: str, connection: Any) -> str:
    """
    The dependency check's SQL, with the hints _EdgeProbe describes for this
    connection. SQL that does not read the edge table the expected way is
    returned unchanged.

    Both engines want the hint after the FROM item, alias and all. Django
    quotes a table alias there from 6.1 on and did not before, so the
    alias is matched in either spelling. The WHERE ahead of it is what
    stops an unaliased FROM item handing the keyword over as its alias.
    """
    if connection.vendor not in ("sqlite", "mysql"):
        return sql
    quote = connection.ops.quote_name
    quoted = quote(OxpullWorkflowEdge._meta.db_table)
    table = re.escape(quoted)
    mark = re.escape(quoted[0])
    alias = rf"(?: (?:{mark}\w+{mark}|[A-Za-z]\w*))?"
    found = re.search(rf"FROM {table}{alias}(?= WHERE )", sql)
    if found is None:
        return sql
    index = quote(EDGE_DOWNSTREAM_INDEX)
    if connection.vendor == "sqlite":
        hint = f" INDEXED BY {index}"
    else:
        hint = f" FORCE INDEX ({index})"
    sql = sql[: found.end()] + hint + sql[found.end() :]
    if connection.vendor == "mysql":
        sql = re.sub(r"^EXISTS\(SELECT ", "EXISTS(SELECT /*+ NO_SEMIJOIN() */ ", sql)
    return sql


class _NamedIndex(BaseTable):
    """
    The edge table in a FROM clause, naming on SQLite the index the read takes.

    A chunk of ids reaches the edge table as one range an id (_KeysProbe). A
    planner whose statistics show one key estimates every one of those ranges
    at the whole row count, so a branch an id looks dearer than a read of the
    table, and it reads the table and tests every row against the whole OR:
    the row count times the list length, and it grows with both. INDEXED BY
    leaves it no table to read. The claim's check names the same index for the
    same reason, in the same words (_EdgeProbe).

    Every other database gets the FROM item Django writes.
    """

    def __init__(self, table_name: str, alias: str | None, index: str) -> None:
        super().__init__(table_name, alias)
        self.index = index

    @property
    def identity(self) -> tuple[Any, ...]:
        """What BaseTable compares by, and the index besides: two FROM items
        that name different indexes are not the same FROM item."""
        return (*super().identity, self.index)

    def relabeled_clone(self, change_map: dict[str | None, str]) -> _NamedIndex:
        alias = change_map.get(self.table_alias, self.table_alias)
        return type(self)(self.table_name, alias, self.index)

    def as_sql(self, compiler: Any, connection: Any) -> tuple[str, Any]:
        sql, params = super().as_sql(compiler, connection)
        if connection.vendor == "sqlite":
            sql += f" INDEXED BY {connection.ops.quote_name(self.index)}"
        return sql, params


def _reading_index[Row](
    queryset: QuerySet[OxpullWorkflowEdge, Row], index: str
) -> QuerySet[OxpullWorkflowEdge, Row]:
    """`queryset` with its edge table naming `index` on SQLite (_NamedIndex)."""
    pinned = queryset.all()
    alias = pinned.query.get_initial_alias()
    pinned.query.alias_map[alias] = _NamedIndex(
        OxpullWorkflowEdge._meta.db_table, alias, index
    )
    return pinned


def runnable_filter_q() -> Q:
    """
    The claim's extra condition: nothing still holds this candidate back.

    A node that waits for an upstream is WAITING, which the claim never
    reads, so on the normal path this condition only ever meets READY nodes
    whose edges are all satisfied. It is kept in the claim for the READY
    rows nothing in this package released: a django-ox retry, an
    application's own UPDATE, or a faulty release. Fail-closed: see
    _blocking_edges. It probes the edge index once per candidate row, whatever
    the planner's statistics say (_EdgeProbe).
    """
    return Q(~_EdgeProbe(_blocking_edges(OuterRef("pk"))))


def runnable_filter_sql() -> tuple[str, dict[str, Any]]:
    """
    runnable_filter_q() as a WHERE fragment, for the single-statement
    PostgreSQL claim, which builds its own SQL.

    The fragment is appended to the candidate select's conditions as a
    bare AND conjunct against the task table, per Worker.claim_filter_sql's
    contract. Table and column names come from the models' _meta, the same
    way OxpullWorker takes its rate-limit columns, so a rename in a future
    django-ox is an import error rather than a fragment that silently
    matches nothing.

    The unqualified task-table reference resolves to the candidate
    subselect's own FROM item, which is the innermost scope carrying that
    name. Both correlated sub-selects use aliases, so nothing else is
    ambiguous.
    """
    task_table = OxTask._meta.db_table
    edge_table = OxpullWorkflowEdge._meta.db_table
    task_pk = OxTask._meta.pk.column
    status = OxTask._meta.get_field("status").column
    downstream = OxpullWorkflowEdge._meta.get_field("downstream_task_id").column
    upstream = OxpullWorkflowEdge._meta.get_field("upstream_task_id").column
    satisfied = OxpullWorkflowEdge._meta.get_field("satisfied").column
    # S608: every interpolated name comes from a model's own _meta, and
    # every value in the statement is a bound parameter. The same
    # construction as OxpullWorker.claim_filter_sql and as django-ox's
    # POSTGRES_CLAIM_SQL, both of which format table and column names
    # into a statement for the same reason: a literal here would be a
    # table name that a future release could rename without a test
    # noticing.
    fragment = (
        f" AND NOT EXISTS ("  # noqa: S608
        f'SELECT 1 FROM "{edge_table}" ox_wf_edge'
        f' WHERE ox_wf_edge."{downstream}" = "{task_table}"."{task_pk}"'
        f' AND ox_wf_edge."{satisfied}" = %(ox_wf_unsatisfied)s'
        f' AND COALESCE((SELECT ox_wf_up."{status}" FROM "{task_table}" ox_wf_up'
        f' WHERE ox_wf_up."{task_pk}" = ox_wf_edge."{upstream}"), \'\')'
        f" <> %(ox_wf_success)s)"
    )
    return fragment, {
        "ox_wf_success": OxTask.Status.SUCCESSFUL,
        "ox_wf_unsatisfied": False,
    }


def _cancellable_edges(
    workflow_pk: uuid.UUID, db_alias: str
) -> QuerySet[OxpullWorkflowEdge]:
    """
    The edges of this workflow that doom a node the cancellation may close.

    An edge not marked satisfied, into a READY or WAITING row, from an upstream
    that settled without succeeding: its row is there in a dead status, or its
    row is gone and its node's record holds a dead status. A row that is gone
    with no record dooms nothing, because a pruned row says nothing about how
    it finished.

    Not the negation of the gate. A node whose upstream is merely still
    running is blocked but not doomed, and cancelling it would be a verdict
    on work still in progress. An edge marked satisfied never dooms anything:
    its upstream succeeded.

    Each row it reads is read by key with a scalar subquery: the upstream's
    status, its node's record and the downstream's status. An EXISTS beside an
    OR is what PostgreSQL turns into a hashed subplan over the whole task
    table.
    """
    return (
        OxpullWorkflowEdge.objects.using(db_alias)
        .filter(workflow_id=workflow_pk, satisfied=False)
        .annotate(
            upstream_status=Coalesce(_status_of("upstream_task_id"), Value("")),
        )
        .alias(
            upstream_record=Coalesce(
                Subquery(
                    OxpullWorkflowNode.objects.filter(
                        task_id=OuterRef("upstream_task_id")
                    ).values("outcome")[:1]
                ),
                Value(""),
            ),
            downstream_status=_status_of("downstream_task_id"),
        )
        .filter(
            Q(upstream_status__in=DEAD_STATUSES)
            | Q(upstream_status="", upstream_record__in=DEAD_STATUSES)
        )
        .filter(downstream_status__in=CANCELLABLE_STATUSES)
    )


def _status_of(column: str) -> Subquery:
    """The status of the task row a column names, as a scalar subquery by key."""
    return Subquery(OxTask.objects.filter(pk=OuterRef(column)).values("status")[:1])


# -- counting ----------------------------------------------------------


def _node_task_ids(workflow_pk: uuid.UUID, db_alias: str) -> Any:
    return (
        OxpullWorkflowNode.objects.using(db_alias)
        .filter(workflow_id=workflow_pk)
        .values("task_id")
    )


def _node_task_counts(workflow_pk: uuid.UUID, db_alias: str) -> dict[str, int]:
    """
    Node task rows grouped by status: the join the whole design rests on.

    Deliberately the same query batches runs over its membership rows,
    written out again rather than shared, for the reason recorded in
    oxpull_prune: the two features must be free to diverge without one
    silently changing the other. Both sides are pinned to the one alias
    oxpull_db_alias() resolves, because a join does not cross aliases.

    A node whose task row is gone joins to nothing and appears in no
    bucket: pruned reads as finished, which is what keeps a long-lived
    workflow completing after ox_prune has been through.
    """
    rows = (
        OxTask.objects.using(db_alias)
        .filter(id__in=_node_task_ids(workflow_pk, db_alias))
        .values("status")
        .annotate(count=Count("id"))
        .order_by()
    )
    return {row["status"]: row["count"] for row in rows}


# -- the handle --------------------------------------------------------


class Workflow:
    """
    A handle on one OxpullWorkflow row. Build one with create() or get().

    The handle holds no derived state: status and progress are read from
    the database on each call, because the reconciler that completes a
    workflow runs in another process.
    """

    def __init__(self, row: OxpullWorkflow) -> None:
        self._row = row

    def __repr__(self) -> str:
        return f"<Workflow {self._row.name!r} {self._row.pk}>"

    @property
    def id(self) -> uuid.UUID:
        return self._row.pk

    @property
    def name(self) -> str:
        return self._row.name

    @property
    def status(self) -> str:
        """The status as stored now, not as it was when this handle was built."""
        db_alias = oxpull_db_alias()
        return str(
            OxpullWorkflow.objects.using(db_alias)
            .values_list("status", flat=True)
            .get(pk=self._row.pk)
        )

    def add(
        self,
        node_name: str,
        task: Task[..., Any],
        *args: Any,
        depends_on: str | Iterable[str] | None = None,
        **kwargs: Any,
    ) -> TaskResult[Any, Any]:
        """
        Enqueue a task as a named node, with its dependencies, in one
        transaction.

        Deliberately not ParamSpec-typed, unlike Batch.add: a keyword-only
        parameter cannot follow ``*args: P.args``, so keeping the task's
        own argument types would mean spelling dependencies as a second
        positional, ``flow.add("load", None, load_task, 42)``, with a
        `None` on every node that has none. The declaration reads more
        than it type-checks here, so the argument types go. ``depends_on``
        is reserved as a result: a task with a keyword argument of that
        name must be wrapped or renamed.

        depends_on names nodes already added to this workflow. That
        restriction is what makes a cycle unconstructible: every edge
        points backwards in insertion order, so there is no cycle
        detector here because there is nothing for one to find.

        One transaction, taking its locks in the order every workflow
        operation takes them: the workflow row first, then this node's
        task row, then the upstream nodes, then its own node and edges. A
        node with no dependencies is enqueued READY. A node with
        dependencies is inserted WAITING, which no worker claims, and each
        of its edges is inserted satisfied when the upstream row reads
        SUCCESSFUL. When every edge is satisfied, the node is released in
        the same transaction. Otherwise it is checked again once the
        transaction commits: an upstream can succeed after this read and
        before this node's edges are visible to that upstream's worker. A
        run_after the caller gave stays on the row, and the release keeps
        it.

        The task row, the node row and every edge commit or roll back
        together, so no worker sees a node without the edges that hold it.

        When add() owns its transaction, a lost deadlock (InnoDB's 1213,
        PostgreSQL's 40P01) or PostgreSQL serialization failure (40001, which
        two overlapping add()s raise at REPEATABLE READ) reissues the whole of
        it, enqueue included, as Batch.add does. add() owns its transaction
        when the connection is in autocommit and no atomic block is open.
        Inside a caller's transaction the error propagates and the caller
        retries its own transaction.

        The caller's side of that contract. A transaction that adds to or
        seals several workflows takes them in ascending id order, because two
        transactions that take the same workflow rows in opposite orders
        deadlock. It takes no locking read or UPDATE of an existing task row.
        And it does not claim or run workflow nodes. A worker's claim or
        execution inside a caller's transaction is not supported for them.

        Raises WorkflowSealed if the workflow is no longer OPEN,
        UnknownWorkflowNode for a dependency this workflow does not have,
        and WorkflowNodeExists for a duplicate node name or task. A unique
        task raises ImproperlyConfigured before anything is enqueued; the
        module docstring says why, and what to do instead.
        """
        db_alias = oxpull_db_alias()
        _require_ox_backend(task, role="nodes", label="Workflow")
        _require_workflow_backend(task)
        _refuse_unique_node(task)
        if len(node_name) > NAME_MAX_LENGTH:
            raise ValueError(f"Node name exceeds {NAME_MAX_LENGTH} characters.")
        wanted = _dependency_names(depends_on)
        return _reissue_add(
            lambda: self._add_once(db_alias, node_name, task, wanted, args, kwargs),
            db_alias,
            label="Workflow",
            owner_pk=self._row.pk,
            task_path=task.module_path,
            log=logger,
        )

    def _add_once(
        self,
        db_alias: str,
        node_name: str,
        task: Task[..., Any],
        wanted: list[str],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> TaskResult[Any, Any]:
        """One attempt of add(): the transaction that add() may reissue."""
        with transaction.atomic(using=db_alias):
            status = self._take_for_add(db_alias)
            if status != OxpullWorkflow.Status.OPEN:
                raise self._not_open_error(status)
            if wanted:
                # Born WAITING, never inserted READY and then moved: a row
                # moved out of READY leaves index entries in the range every
                # claim scans. django-ox's helper sends no task_enqueued, so
                # it is sent here, where task.enqueue() would have sent it.
                result = _waiting.enqueue(task, args, kwargs, using=db_alias)
                task_enqueued.send_robust(type(task.get_backend()), task_result=result)
            else:
                result = task.enqueue(*args, **kwargs)
            upstream_ids = self._resolve_dependencies(wanted, db_alias)
            # A plain read. SUCCESSFUL is final, so an upstream this read
            # sees SUCCESSFUL stays so, and its edge can be marked now. One
            # it misses is found by the check after commit.
            succeeded = _succeeded_among(upstream_ids, db_alias)
            now = timezone.now()
            try:
                # Its own savepoint, so a duplicate does not poison a
                # caller's surrounding transaction; the confinement the
                # unique enqueue path uses.
                with transaction.atomic(using=db_alias):
                    OxpullWorkflowNode.objects.using(db_alias).create(
                        workflow_id=self._row.pk,
                        name=node_name,
                        name_key=name_key(node_name),
                        task_id=result.id,
                        created_at=now,
                    )
                    OxpullWorkflowEdge.objects.using(db_alias).bulk_create(
                        [
                            OxpullWorkflowEdge(
                                workflow_id=self._row.pk,
                                upstream_task_id=upstream_id,
                                downstream_task_id=result.id,
                                satisfied=upstream_id in succeeded,
                                created_at=now,
                            )
                            for upstream_id in upstream_ids
                        ]
                    )
            except IntegrityError as exc:
                raise WorkflowNodeExists(
                    f"Workflow {self._row.pk} already has a node named "
                    f"{node_name!r} or holding task {result.id}."
                ) from exc
            if upstream_ids:
                node_id = uuid.UUID(str(result.id))
                if succeeded.issuperset(upstream_ids):
                    _release_added_node(node_id, db_alias)
                else:
                    transaction.on_commit(
                        lambda: _recheck_added_node(node_id, db_alias),
                        using=db_alias,
                        robust=True,
                    )
        return result

    def _take_for_add(self, db_alias: str) -> str | None:
        """
        Take the workflow row's lock, count the node, and return the status.

        The UPDATE matches only an OPEN row and increments node_count, so
        either seal() waits for this add() and counts its node, or this add()
        finds the workflow no longer OPEN and its whole transaction rolls
        back, the increment with it. The UPDATE always changes the row it
        matches, so its rowcount is right on a MySQL connection without
        CLIENT_FOUND_ROWS. A zero rowcount is answered by a status read under
        the row lock, which is current even under REPEATABLE READ.
        """
        rows = OxpullWorkflow.objects.using(db_alias).filter(pk=self._row.pk)
        if rows.filter(status=OxpullWorkflow.Status.OPEN).update(
            node_count=F("node_count") + 1
        ):
            return OxpullWorkflow.Status.OPEN
        return rows.select_for_update().values_list("status", flat=True).first()

    def _resolve_dependencies(
        self, wanted: list[str], db_alias: str
    ) -> list[uuid.UUID]:
        """
        Node names to task ids, read under the workflow row lock add()
        has already taken.

        Read under the lock and not before it: a concurrent add() of the
        named node commits before this transaction gets the lock, or
        after this one releases it, so the name either resolves or does
        not, and never resolves to a row that then disappears.

        Names are matched by name_key, so a name resolves only to the node
        spelled exactly that way, whatever the column's collation.

        A locking read, which is a current read. Under MySQL's REPEATABLE
        READ a plain SELECT answers from the read view fixed by the
        transaction's first consistent read, a caller's for one, and that
        view can predate a node committed before the lock was taken. The
        rows it locks are this workflow's own node rows, which nothing else
        writes while add() holds the workflow row. PostgreSQL has no current
        read at REPEATABLE READ, and needs none here: the other add()
        rewrote the workflow row, so the lock UPDATE above has already
        raised a serialization failure.
        """
        if not wanted:
            return []
        keys = {name_key(name): name for name in wanted}
        known: dict[str, uuid.UUID] = {}
        for chunk in _id_chunks(keys, db_alias):
            nodes = (
                OxpullWorkflowNode.objects.using(db_alias)
                .filter(workflow_id=self._row.pk, name_key__in=chunk)
                .order_by("pk")
            )
            if connections[db_alias].features.has_select_for_update:
                nodes = nodes.select_for_update()
            known.update(nodes.values_list("name_key", "task_id"))
        missing = [name for key, name in keys.items() if key not in known]
        if missing:
            raise UnknownWorkflowNode(
                f"Workflow {self._row.pk} has no node(s) {sorted(missing)}; "
                "depends_on may only name nodes already added, which is "
                "also what makes a cycle unconstructible."
            )
        return [known[name_key(name)] for name in wanted]

    def seal(self) -> None:
        """
        Freeze membership and snapshot total_count. Completion is
        possible only after this; a workflow that is never sealed never
        completes, though its runnable nodes do run.

        Inside a caller's transaction the contract add() describes applies,
        including the ascending id order across workflows.

        Raises WorkflowSealed if it was already sealed or completed.
        There is no un-seal. Raises ImproperlyConfigured if no django-ox
        backend schedules the reconciler, as Batch.seal() does, since the
        workflow would then stay SEALED forever; the status is checked
        first, so a workflow that is already sealed says so whatever the
        settings.
        """
        db_alias = oxpull_db_alias()

        def retrying(conflict: str, attempt: int, bound: int) -> None:
            logger.info(
                "Workflow %s: seal() lost a serialization failure on attempt %d "
                "of %d; reissuing its transaction",
                self._row.pk,
                attempt,
                bound,
                extra={
                    "event": "workflow_seal_serialization_retry",
                    "workflow_id": str(self._row.pk),
                    "attempt": attempt,
                },
            )

        # At PostgreSQL REPEATABLE READ the status write below, having
        # waited on an add() in flight, raises a serialization failure when
        # that add() commits: the row changed after this transaction's
        # snapshot. Owning the transaction, seal() runs it again on a fresh
        # snapshot. Inside a caller's transaction the error propagates.
        _reissue(
            lambda: self._seal_once(db_alias),
            db_alias,
            deadlocks=False,
            retrying=retrying,
        )

    def _seal_once(self, db_alias: str) -> None:
        """
        One attempt of seal(): the transaction that seal() may reissue.

        total_count is copied from node_count by the status write itself, and
        no node row is read. add() increments node_count in the statement that
        takes the workflow row's lock, and its whole transaction commits or
        rolls back with it, so node_count counts exactly the nodes that
        committed. The status write waits on any add() still holding the row,
        and an UPDATE reads the row's latest committed version, so the copy is
        current even inside a caller's transaction whose read view is older.
        At PostgreSQL REPEATABLE READ such a row raises a serialization failure
        instead. No node row is locked, so seal() never stalls a node's outcome
        or an add() to another workflow.
        """
        with transaction.atomic(using=db_alias):
            sealed = (
                OxpullWorkflow.objects.using(db_alias)
                .filter(pk=self._row.pk, status=OxpullWorkflow.Status.OPEN)
                .update(
                    status=OxpullWorkflow.Status.SEALED,
                    sealed_at=timezone.now(),
                    total_count=F("node_count"),
                )
            )
            if not sealed:
                raise self._not_open_error(self._stored_status(db_alias))
            # After the status, so a workflow already SEALED or COMPLETE
            # reports that rather than the settings. Inside the block, so a
            # refusal here rolls the status write back and the workflow
            # stays OPEN.
            _require_reconcile_schedule()

    def node_ids(self) -> dict[str, uuid.UUID]:
        """This workflow's node names, mapped to their task ids."""
        db_alias = oxpull_db_alias()
        return dict(
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(workflow_id=self._row.pk)
            .values_list("name", "task_id")
        )

    def progress(self) -> WorkflowProgress:
        """
        Counts for this workflow, queried now from the node task rows and the
        nodes' records, classified as completion classifies them (_classify).
        """
        db_alias = oxpull_db_alias()
        row = OxpullWorkflow.objects.using(db_alias).get(pk=self._row.pk)
        if row.total_count is not None:
            total = row.total_count
            classes = _classify(row.pk, db_alias)
        else:
            # An open workflow has no snapshot, so the live node count is
            # the only honest denominator. Read it first and let the
            # classified nodes raise it: an add() committing between the two
            # then makes this reading lag by a node, which is harmless.
            # The other order inflates the denominator instead.
            nodes = (
                OxpullWorkflowNode.objects.using(db_alias)
                .filter(workflow_id=row.pk)
                .count()
            )
            classes = _classify(row.pk, db_alias)
            total = max(nodes, classes.nodes)
        blocked = (
            OxTask.objects.using(db_alias)
            .filter(id__in=_node_task_ids(row.pk, db_alias))
            .filter(
                Q(status=OxTask.Status.WAITING)
                | Q(
                    Q(status=OxTask.Status.READY),
                    Exists(_blocking_edges(OuterRef("pk"))),
                )
            )
            .count()
        )
        return WorkflowProgress(
            total=total,
            pending=classes.pending,
            blocked=blocked,
            succeeded=classes.succeeded,
            failed=classes.failed,
        )

    def _stored_status(self, db_alias: str) -> str | None:
        return (
            OxpullWorkflow.objects.using(db_alias)
            .values_list("status", flat=True)
            .filter(pk=self._row.pk)
            .first()
        )

    def _not_open_error(self, status: str | None) -> WorkflowSealed:
        if status is None:
            raise OxpullWorkflow.DoesNotExist(
                f"Workflow {self._row.pk} no longer exists."
            )
        return WorkflowSealed(
            f"Workflow {self._row.pk} is {status}; membership is frozen."
        )


def _dependency_names(depends_on: str | Iterable[str] | None) -> list[str]:
    if depends_on is None:
        return []
    names = [depends_on] if isinstance(depends_on, str) else list(depends_on)
    if not all(isinstance(name, str) for name in names):
        raise ImproperlyConfigured(
            "depends_on must be a node name or an iterable of node names."
        )
    # Order preserved, duplicates dropped: the unique constraint on the
    # edge would refuse the second copy and abort the whole add().
    return list(dict.fromkeys(names))


def workflows_enabled_from_options(options: dict[str, Any]) -> bool:
    """
    WORKFLOWS from a backend's OPTIONS, as a bool.

    The gate is declared rather than inferred, for one measured reason:
    it is a conjunct in the claim statement, and a worker cannot decide
    at run time whether to include it without either a query per claim
    pass or a cached answer that is a window in which a node runs out of
    order. Declaring it keeps a backend with no workflows emitting
    exactly the SQL django-ox emits on its own, the property rate
    limits already hold to, and it gives
    the configuration a name a system check can see, which is what turns
    "your nodes will run out of order" from a run-time surprise into a
    start-up error.

    Raises ImproperlyConfigured on a non-bool so a bad deploy fails in
    system checks rather than silently at claim time; the same idiom as
    django_ox.schedules.schedules_from_options.
    """
    raw = options.get("WORKFLOWS", False)
    if not isinstance(raw, bool):
        raise ImproperlyConfigured(
            f"WORKFLOWS must be True or False, not {type(raw).__name__}."
        )
    return raw


# -- the release -------------------------------------------------------


def _succeeded_among(task_ids: list[uuid.UUID], db_alias: str) -> set[uuid.UUID]:
    """The ids among task_ids whose row is there and SUCCESSFUL, by a plain read."""
    found: set[uuid.UUID] = set()
    for chunk in _id_chunks(task_ids, db_alias):
        found.update(
            uuid.UUID(str(pk))
            for pk in OxTask.objects.using(db_alias)
            .filter(pk__in=chunk, status=OxTask.Status.SUCCESSFUL)
            .values_list("pk", flat=True)
        )
    return found


def _release_added_node(task_id: uuid.UUID, db_alias: str) -> None:
    """
    Release a node add() just inserted with every edge satisfied, inside
    add()'s transaction.

    The epoch comes from the row itself rather than being assumed, so the
    move stays pinned the way every other release is.
    """
    epoch = (
        OxTask.objects.using(db_alias)
        .filter(pk=task_id, status=OxTask.Status.WAITING)
        .values_list("lease_epoch", flat=True)
        .first()
    )
    if epoch is not None:
        _waiting.release(task_id, lease_epoch=epoch, using=db_alias)


def _recheck_added_node(task_id: uuid.UUID, db_alias: str) -> None:
    """
    Release a node add() left WAITING, if its upstreams have all succeeded
    by the time add() committed.

    add() read its upstreams inside its own transaction. An upstream that
    succeeded after that read ran its release before this node's edges were
    visible to it, so nothing else would move this node until the
    reconciler's backstop. A failure here must not surface from the caller's
    commit, so it is logged and left to the backstop.
    """
    try:
        _satisfy_edges_into([task_id], db_alias)
        release_downstream([task_id], db_alias)
    except DatabaseError:
        logger.warning(
            "Could not re-check workflow node task id=%s after add(). The "
            "reconciler will release it",
            task_id,
            exc_info=True,
            extra={"event": "workflow_release_failed", "task_id": str(task_id)},
        )


def _mark_satisfied(edge_ids: Iterable[int], db_alias: str) -> int:
    """
    Mark edges satisfied by primary key. Returns how many changed.

    Only a caller that saw each edge's upstream row SUCCESSFUL may pass it.
    The UPDATE carries no subquery on the task table: on MySQL at REPEATABLE
    READ that would take shared locks on every task row it reads.
    """
    marked = 0
    for chunk in _id_chunks(edge_ids, db_alias):
        marked += (
            OxpullWorkflowEdge.objects.using(db_alias)
            .filter(pk__in=chunk, satisfied=False)
            .update(satisfied=True)
        )
    return marked


def _succeeded_or_recorded(task_ids: Iterable[Any], db_alias: str) -> set[uuid.UUID]:
    """
    The ids among task_ids known to have succeeded, by plain reads: the row is
    there and SUCCESSFUL, or the row is gone and its node's record says
    SUCCESSFUL.

    This is the proof a writer needs before it marks an edge satisfied. A row
    that is gone with no such record proves nothing. The reads can only be
    stale in the safe direction: SUCCESSFUL is final, a pruned row does not
    come back, and only a revival clears a SUCCESSFUL record, which needs the
    row.
    """
    ids = sorted({uuid.UUID(str(task_id)) for task_id in task_ids})
    proven: set[uuid.UUID] = set()
    gone: list[uuid.UUID] = []
    for chunk in _id_chunks(ids, db_alias):
        found = {
            uuid.UUID(str(pk)): status
            for pk, status in OxTask.objects.using(db_alias)
            .filter(pk__in=chunk)
            .values_list("pk", "status")
        }
        proven.update(
            pk for pk, status in found.items() if status == OxTask.Status.SUCCESSFUL
        )
        gone.extend(pk for pk in chunk if pk not in found)
    for chunk in _id_chunks(gone, db_alias):
        proven.update(
            uuid.UUID(str(task_id))
            for task_id in OxpullWorkflowNode.objects.using(db_alias)
            .filter(task_id__in=chunk, outcome=OxTask.Status.SUCCESSFUL)
            .values_list("task_id", flat=True)
        )
    return proven


def _satisfiable_edges_into(
    task_ids: Iterable[Any], db_alias: str
) -> dict[int, uuid.UUID]:
    """
    The edges into task_ids not yet marked whose upstream is known to have
    succeeded, mapped to the node each points at.
    """
    ids = sorted({uuid.UUID(str(task_id)) for task_id in task_ids})
    open_edges: list[tuple[int, Any, Any]] = []
    for chunk in _id_chunks(ids, db_alias):
        open_edges.extend(
            _reading_index(
                OxpullWorkflowEdge.objects.using(db_alias)
                .filter(downstream_task_id__probe_in=chunk, satisfied__aside=False)
                .values_list("pk", "upstream_task_id", "downstream_task_id"),
                EDGE_DOWNSTREAM_INDEX,
            )
        )
    proven = _succeeded_or_recorded({up for _, up, _ in open_edges}, db_alias)
    return {
        pk: uuid.UUID(str(down))
        for pk, up, down in open_edges
        if uuid.UUID(str(up)) in proven
    }


def _satisfy_edges_into(task_ids: Iterable[Any], db_alias: str) -> int:
    """
    Mark satisfied the edges into task_ids whose upstream is known to have
    succeeded (_succeeded_or_recorded). Returns how many changed.

    The edges are locked in key order before the marks, waiting for a held
    lock (_write_locked). When the call owns its transaction and loses a lock
    conflict, the nodes behind the edges it could not mark go to their
    workflows' skip queues.
    """
    wanted = _satisfiable_edges_into(task_ids, db_alias)
    if not wanted:
        return 0
    marked: list[int] = []
    _, skipped = _write_locked(
        OxpullWorkflowEdge,
        wanted,
        lambda pks: marked.append(_mark_satisfied(pks, db_alias)),
        db_alias,
        skip_locked=False,
    )
    queue_skipped({wanted[pk] for pk in skipped}, db_alias)
    return sum(marked)


# -- what a node's record says ---------------------------------------------

# The statuses a node's record can hold: a task row's settled statuses.
RECORDED_STATUSES = frozenset(
    (OxTask.Status.SUCCESSFUL, *DEAD_STATUSES),
)


def outcome_accepts_q(status: str, epoch: int) -> Q:
    """
    The node records that a write of `status` at lease epoch `epoch` replaces.

    A record is (outcome, outcome_epoch). A write lands when the record has no
    epoch, when its epoch is lower, or when its epoch is the same and either it
    holds no outcome or it holds LOST and the write is not LOST. So a later
    incarnation always wins, an earlier one never does, and at one epoch only
    an observation of LOST gives way, to the outcome the same execution wrote.
    A failure recorded at an epoch stays failed when a discard at that epoch
    arrives, and a settled record at an epoch is never rewritten at it.
    """
    if status not in RECORDED_STATUSES:
        raise ValueError(f"{status!r} is not a settled status a node can record.")
    same_epoch = Q(outcome__isnull=True)
    if status != OxTask.Status.LOST:
        same_epoch |= Q(outcome=OxTask.Status.LOST)
    return (
        Q(outcome_epoch__isnull=True)
        | Q(outcome_epoch__lt=epoch)
        | (Q(outcome_epoch=epoch) & same_epoch)
    )


def record_takes(
    outcome: str | None, outcome_epoch: int | None, status: str, epoch: int
) -> bool:
    """outcome_accepts_q() for one record read into memory."""
    if outcome_epoch is None or outcome_epoch < epoch:
        return True
    if outcome_epoch > epoch:
        return False
    return outcome is None or (
        outcome == OxTask.Status.LOST and status != OxTask.Status.LOST
    )


def _record_many(records: dict[int, tuple[str, int]], db_alias: str) -> int:
    """
    record_outcome() for many nodes, keyed by node pk. One guarded UPDATE per
    chunk of nodes that share a status and an epoch. Returns how many took it.
    """
    groups: dict[tuple[str, int], list[int]] = {}
    for node_pk, (status, epoch) in records.items():
        groups.setdefault((status, int(epoch)), []).append(node_pk)
    taken = 0
    for (status, epoch), node_pks in sorted(groups.items()):
        for chunk in _id_chunks(node_pks, db_alias):
            taken += (
                OxpullWorkflowNode.objects.using(db_alias)
                .filter(pk__in=chunk)
                .filter(outcome_accepts_q(status, epoch))
                .update(
                    outcome=status,
                    outcome_epoch=epoch,
                    succeeded=status == OxTask.Status.SUCCESSFUL,
                )
            )
    return taken


def record_outcome(node_pk: int, status: str, epoch: int, db_alias: str) -> bool:
    """
    Write (status, epoch) onto one node's record if the record accepts it. True
    when it did.

    `succeeded` is set by the same guarded UPDATE, so it changes only when the
    record does.
    """
    return bool(
        OxpullWorkflowNode.objects.using(db_alias)
        .filter(pk=node_pk)
        .filter(outcome_accepts_q(status, epoch))
        .update(
            outcome=status,
            outcome_epoch=epoch,
            succeeded=status == OxTask.Status.SUCCESSFUL,
        )
    )


def record_node_outcome(
    task_id: Any, status: str, epoch: int, db_alias: str
) -> list[uuid.UUID] | None:
    """
    Record a node's outcome and, on success, mark the edges out of it. Returns
    the ids of the nodes those edges point at, or None when the task is not a
    node.

    Runs inside the transaction that wrote the task row's outcome, straight
    after that write landed, so the row, the record and the marks commit
    together or not at all. It locks in the order every workflow operation
    locks: the task row is already this transaction's, then the node row,
    then the edges out of it in key order. At READ COMMITTED the node lock is
    what serialises this against an add() that names this node as an upstream:
    whichever commits first, the other sees its rows. At PostgreSQL REPEATABLE
    READ the edge read answers from the snapshot this transaction's first write
    opened. An edge committed after that snapshot is invisible here, so this
    marks nothing for it and its downstream node waits for a backstop page.

    Nothing here waits in any way of its own. A lock wait, a deadlock or a
    serialization failure raises, and the caller decides whether the whole
    transaction runs again.
    """
    locks = connections[db_alias].features.has_select_for_update
    nodes = OxpullWorkflowNode.objects.using(db_alias).filter(task_id=task_id)
    if locks:
        nodes = nodes.select_for_update()
    found = list(nodes.values_list("pk", flat=True))
    if not found:
        return None
    record_outcome(found[0], status, epoch, db_alias)
    if status != OxTask.Status.SUCCESSFUL:
        return []
    edges = (
        OxpullWorkflowEdge.objects.using(db_alias)
        .filter(upstream_task_id__probe=task_id, satisfied=False)
        .order_by("pk")
    )
    if locks:
        edges = edges.select_for_update()
    pairs = list(edges.values_list("pk", "downstream_task_id"))
    _mark_satisfied([pk for pk, _ in pairs], db_alias)
    return sorted({uuid.UUID(str(downstream)) for _, downstream in pairs})


# The attribute a claimed task row carries when the claim computed whether it
# is a workflow node.
NODE_FLAG = "ox_wf_node"


def node_flag() -> Exists:
    """Whether the task row is a workflow node, as an annotation for a claim."""
    return Exists(OxpullWorkflowNode.objects.filter(task_id=OuterRef("pk")))


def node_flag_for_a_locking_read(vendor: str) -> Case | Exists:
    """
    node_flag() for the SKIP LOCKED claim on `vendor`. On MySQL it is NULL
    wherever the claim cannot tell.

    MySQL answers the subqueries of a locking read from a read view. The view
    opens at the statement's first plain read, which can be the gate on a row
    the scan passes before the one it claims. The locking read still returns a
    row committed after the view opened, but that row's node is outside the
    view. So a node that add() commits while the scan runs would read as no
    node, and its outcome would be written without its record or its marks.

    Whether the claimed row itself is in the view settles it. add() commits a
    task row and its node together, and nothing deletes the node of a READY
    row. So where the row is in the view, its node is too, and the flag is
    exact. Where the row is not, the flag is NULL, and the outcome's locking
    read of the node decides. The row is looked for first, so the flag is never
    read from an older view than the look.

    PostgreSQL reads the whole statement from one snapshot, and its scan
    returns no row committed after that snapshot. There the flag is node_flag().
    """
    if vendor != "mysql":
        return node_flag()
    return Case(
        When(Exists(OxTask.objects.filter(pk=OuterRef("pk"))), then=node_flag()),
        default=Value(None),
        output_field=BooleanField(null=True),
    )


def node_flag_sql() -> str:
    """
    node_flag() as an extra column of the PostgreSQL claim's RETURNING list.

    The unqualified task table resolves to the row the UPDATE wrote. Names come
    from the models' _meta, as in runnable_filter_sql().
    """
    task_table = OxTask._meta.db_table
    node_table = OxpullWorkflowNode._meta.db_table
    task_pk = OxTask._meta.pk.column
    task_id = OxpullWorkflowNode._meta.get_field("task_id").column
    # S608: every interpolated name comes from a model's own _meta, as in
    # runnable_filter_sql().
    return (
        f', EXISTS (SELECT 1 FROM "{node_table}" ox_wf_n'  # noqa: S608
        f' WHERE ox_wf_n."{task_id}" = "{task_table}"."{task_pk}")'
        f' AS "{NODE_FLAG}"'
    )


def release_downstream(task_ids: Iterable[Any], db_alias: str) -> int:
    """
    Move to READY every WAITING row among task_ids whose incoming edges are
    all marked satisfied. Returns how many moved.

    A node leaves WAITING only when every edge into it is satisfied, which
    no later write can undo short of a revival that also clears the marks.
    So the plain reads that decide can only be stale in the safe direction.
    They can miss a mark and leave a node for a later release. They cannot
    release a node early.

    Each chunk is one transaction. It reads the edges into the chunk not yet
    marked, then the WAITING rows among the rest with their epochs, then
    moves those rows through django-ox's release_many, pinned to the epochs
    it read. release_many locks the chunk's rows in key order before its
    UPDATE, inside the same transaction. A row cancelled, revived or
    released since the read matches nothing. Split in that order the two
    reads are as safe as one: a node gains no incoming edge after add()
    commits it, and nothing clears a mark on an edge whose downstream still
    waits. One statement with NOT EXISTS plans on MySQL as a hash join that
    scans the chunk's edges.

    When the call owns its transaction, a chunk waits at most
    RELEASE_LOCK_TIMEOUT seconds for a lock, and a statement runs at most
    RELEASE_STATEMENT_TIMEOUT seconds. MySQL applies the statement bound to
    the reads only. A chunk that loses a lock conflict is decided again once,
    from fresh reads. A lock conflict is a deadlock, a serialization failure,
    a lock wait that timed out or a statement that ran past its bound, and
    each rolls the chunk's transaction back whole. A chunk that loses twice
    is logged, and its ids go to their workflows' skip queues, which the
    reconciler retries first. The chunks after it still run.

    Inside a caller's transaction nothing is bounded and nothing is decided
    again. Every error propagates, because the caller's work is lost with
    it.
    """
    ids = {uuid.UUID(str(task_id)) for task_id in task_ids}
    if not ids:
        return 0
    connection = connections[db_alias]
    if not (connection.get_autocommit() and not connection.in_atomic_block):
        return sum(
            _release_chunk(chunk, db_alias) for chunk in _id_chunks(ids, db_alias)
        )
    released = 0
    deferred: list[uuid.UUID] = []
    with session_timeouts(
        db_alias, lock_wait=RELEASE_LOCK_TIMEOUT, statement=RELEASE_STATEMENT_TIMEOUT
    ):
        for chunk in _id_chunks(ids, db_alias):
            for attempt in range(1, RELEASE_CHUNK_ATTEMPTS + 1):
                try:
                    with transaction.atomic(using=db_alias):
                        transaction_timeouts(
                            db_alias,
                            lock_wait=RELEASE_LOCK_TIMEOUT,
                            statement=RELEASE_STATEMENT_TIMEOUT,
                        )
                        moved = _release_chunk(chunk, db_alias)
                except OperationalError as exc:
                    if not lost_lock_conflict(exc, db_alias):
                        raise
                    if attempt < RELEASE_CHUNK_ATTEMPTS:
                        time.sleep(random.uniform(0, ADD_RETRY_JITTER))  # noqa: S311
                        continue
                    logger.warning(
                        "Could not release up to %d waiting workflow node task "
                        "row(s) after %d lock conflicts. The reconciler will retry "
                        "them",
                        len(chunk),
                        attempt,
                        exc_info=True,
                        extra={
                            "event": "workflow_release_deferred",
                            "task_count": len(chunk),
                        },
                    )
                    deferred.extend(chunk)
                else:
                    released += moved
                break
    if deferred:
        queue_skipped(deferred, db_alias)
    return released


# Tries of one release chunk that loses a lock conflict: the first, and one
# more from fresh reads. A chunk that loses both goes to the skip queue.
RELEASE_CHUNK_ATTEMPTS = 2

# Seconds one release chunk waits for a row lock, and runs one statement.
RELEASE_LOCK_TIMEOUT = 1.0
RELEASE_STATEMENT_TIMEOUT = 2.0

# PostgreSQL's query_canceled, which a statement that runs past
# statement_timeout raises.
PG_QUERY_CANCELED = "57014"

# MySQL's ER_QUERY_TIMEOUT, past max_execution_time, and MariaDB's
# ER_STATEMENT_TIMEOUT, past max_statement_time.
ER_QUERY_TIMEOUT = 3024
ER_STATEMENT_TIMEOUT = 1969


def _release_chunk(chunk: list[uuid.UUID], db_alias: str) -> int:
    """One chunk of release_downstream, from its two reads to its release."""
    held = {
        uuid.UUID(str(task_id))
        for task_id in _reading_index(
            OxpullWorkflowEdge.objects.using(db_alias)
            .filter(downstream_task_id__probe_in=chunk, satisfied__aside=False)
            .values_list("downstream_task_id", flat=True),
            EDGE_DOWNSTREAM_INDEX,
        )
    }
    free = [task_id for task_id in chunk if task_id not in held]
    if not free:
        return 0
    pairs = list(
        OxTask.objects.using(db_alias)
        .filter(pk__in=free, status=OxTask.Status.WAITING)
        .values_list("pk", "lease_epoch")
    )
    if not pairs:
        return 0
    changed, _ = _waiting.release_many(pairs, using=db_alias)
    return changed


def lost_lock_conflict(exc: BaseException, db_alias: str) -> bool:
    """
    Whether a transaction lost to another transaction's locks: a deadlock, a
    serialization failure, a lock wait that timed out, or a statement that ran
    past its time bound.

    Each of these rolls back the whole transaction, or leaves it to be rolled
    back, and running the transaction again from the start is what every
    engine's manual says to do.
    """
    from .backend import (
        _is_lock_wait_timeout,
        is_deadlock,
        is_serialization_failure,
    )

    if not isinstance(exc, OperationalError):
        return False
    vendor = connections[db_alias].vendor
    if (
        vendor == "postgresql"
        and getattr(exc.__cause__, "sqlstate", None) == PG_QUERY_CANCELED
    ):
        return True
    if (
        vendor == "mysql"
        and exc.args
        and exc.args[0]
        in (
            ER_QUERY_TIMEOUT,
            ER_STATEMENT_TIMEOUT,
        )
    ):
        return True
    return (
        is_deadlock(exc, db_alias)
        or is_serialization_failure(exc, db_alias)
        or _is_lock_wait_timeout(exc, db_alias)
    )


@contextmanager
def session_timeouts(
    db_alias: str, *, lock_wait: float, statement: float | None = None
) -> Iterator[None]:
    """
    On MySQL, bound this session's lock waits, and its statements when
    `statement` is given, for the block. Seconds. Other databases pass
    through.

    MySQL has no bound scoped to one transaction, so the session's own values
    are read first and put back when the block ends, however it ends. A
    session whose values cannot be put back is closed, so that no later use
    of the connection runs under bounds its owner never set, and that is
    logged as workflow_session_bounds_not_restored.

    Putting them back never takes the place of an error leaving the block:
    that error is raised, and a failure to put the values back beside it is
    logged. Nothing is put back on a session that is gone, because its bounds
    went with it: one Django has dropped, having opened another connection or
    not, and one that no longer answers after the block's error was a lost
    connection's, which is closed for its owner to reopen.
    """
    connection = connections[db_alias]
    if connection.vendor != "mysql":
        yield
        return
    names = ["innodb_lock_wait_timeout"]
    values: list[Any] = [max(1, math.ceil(lock_wait))]
    if statement is not None:
        if getattr(connection, "mysql_is_mariadb", False):
            names.append("max_statement_time")
            values.append(statement)
        else:
            names.append("max_execution_time")
            values.append(max(1, math.ceil(statement * 1000)))
    read = "SELECT " + ", ".join(f"@@SESSION.{name}" for name in names)
    assign = "SET SESSION " + ", ".join(f"{name} = %s" for name in names)
    with connection.cursor() as cursor:
        cursor.execute(read)
        saved = list(cursor.fetchone())
        cursor.execute(assign, values)
    session = connection.connection
    try:
        yield
    except BaseException as exc:
        if connection.connection is not session:
            raise
        if _lost_with(connection, exc):
            with suppress(Error):
                connection.close()
            raise
        try:
            _put_back_session_bounds(connection, assign, saved)
        except Exception:
            _session_bounds_not_restored(connection)
        raise
    if connection.connection is not session:
        return
    try:
        _put_back_session_bounds(connection, assign, saved)
    except BaseException as exc:
        _session_bounds_not_restored(connection)
        if not isinstance(exc, DatabaseError):
            raise


def _lost_with(connection: Any, exc: BaseException) -> bool:
    """Whether `exc` is a lost connection's error and `connection` no longer answers."""
    if not isinstance(exc, (InterfaceError, OperationalError)):
        return False
    try:
        return not connection.is_usable()
    except Exception:
        return True


def _put_back_session_bounds(connection: Any, assign: str, saved: list[Any]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(assign, saved)


def _session_bounds_not_restored(connection: Any) -> None:
    """Close a session whose bounds could not be put back, and say so."""
    with suppress(Error):
        connection.close()
    logger.warning(
        "Could not put back the session's lock wait and statement "
        "bounds. The connection was closed",
        exc_info=True,
        extra={"event": "workflow_session_bounds_not_restored"},
    )


def transaction_timeouts(
    db_alias: str, *, lock_wait: float, statement: float | None = None
) -> None:
    """
    On PostgreSQL, bound lock waits, and statements when `statement` is given,
    for the rest of the current transaction. Seconds. Other databases pass
    through.
    """
    connection = connections[db_alias]
    if connection.vendor != "postgresql":
        return
    bounds = [("lock_timeout", lock_wait)]
    if statement is not None:
        bounds.append(("statement_timeout", statement))
    params: list[str] = []
    for name, seconds in bounds:
        params.extend((name, f"{max(1, round(seconds * 1000))}ms"))
    sql = "SELECT " + ", ".join("set_config(%s, %s, true)" for _ in bounds)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)


# -- the skip queue ------------------------------------------------------------

# Ids one workflow's skip queue holds. Ids past it are dropped with a log line.
# Nothing is lost by that: the nodes stay WAITING, and the reconciler's pass
# over waiting nodes still finds them.
SKIP_QUEUE_LIMIT = 100

# Tries of the compare-and-set that adds ids to one workflow's queue.
SKIP_QUEUE_ATTEMPTS = 5


def queue_skipped(task_ids: Iterable[Any], db_alias: str) -> None:
    """
    Put node task ids a release could not move into their workflows' skip
    queues, OxpullWorkflowScan.skipped. Each id is kept once, oldest first,
    up to SKIP_QUEUE_LIMIT per workflow.

    An id that is not a node is dropped. A database error is logged and not
    raised. The release that called this has already given up on those rows,
    and the reconciler's pass over waiting nodes still finds them.
    """
    ids = sorted({uuid.UUID(str(task_id)) for task_id in task_ids})
    try:
        by_workflow: dict[uuid.UUID, list[str]] = {}
        for chunk in _id_chunks(ids, db_alias):
            for workflow_id, task_id in (
                OxpullWorkflowNode.objects.using(db_alias)
                .filter(task_id__in=chunk)
                .values_list("workflow_id", "task_id")
            ):
                by_workflow.setdefault(workflow_id, []).append(str(task_id))
        for workflow_id in sorted(by_workflow):
            _add_to_skip_queue(workflow_id, sorted(by_workflow[workflow_id]), db_alias)
    except DatabaseError:
        logger.warning(
            "Could not queue %d waiting workflow node(s) for the reconciler to "
            "retry. Its pass over waiting nodes still finds them",
            len(ids),
            exc_info=True,
            extra={"event": "workflow_release_skip_unqueued", "task_count": len(ids)},
        )


def _add_to_skip_queue(workflow_id: uuid.UUID, ids: list[str], db_alias: str) -> None:
    """Add ids to one workflow's skip queue by compare-and-set on its version."""
    scans = OxpullWorkflowScan.objects.using(db_alias)
    for _ in range(SKIP_QUEUE_ATTEMPTS):
        row = (
            scans.filter(workflow_id=workflow_id)
            .values_list("skipped", "version")
            .first()
        )
        queued: list[str] = list(row[0] or []) if row is not None else []
        merged = list(dict.fromkeys([*queued, *ids]))
        kept, dropped = merged[:SKIP_QUEUE_LIMIT], merged[SKIP_QUEUE_LIMIT:]
        if row is None:
            try:
                with transaction.atomic(using=db_alias):
                    scans.create(workflow_id=workflow_id, skipped=kept)
            except IntegrityError:
                continue
        elif not scans.filter(workflow_id=workflow_id, version=row[1]).update(
            skipped=kept, version=row[1] + 1
        ):
            continue
        if dropped:
            logger.warning(
                "Workflow %s: its skip queue is full, and %d waiting node(s) were "
                "not queued. The reconciler's pass over waiting nodes still finds "
                "them",
                workflow_id,
                len(dropped),
                extra={
                    "event": "workflow_release_skip_overflow",
                    "workflow_id": str(workflow_id),
                    "dropped_count": len(dropped),
                },
            )
        return
    logger.warning(
        "Workflow %s: could not queue %d waiting node(s) for the reconciler to "
        "retry. Its pass over waiting nodes still finds them",
        workflow_id,
        len(ids),
        extra={
            "event": "workflow_release_skip_unqueued",
            "workflow_id": str(workflow_id),
            "task_count": len(ids),
        },
    )


def _take_skipped(db_alias: str) -> list[str]:
    """
    Take up to one chunk of ids out of the workflows' skip queues, by
    compare-and-set, starting after the workflow the round last visited. A
    queue that moved under the compare-and-set keeps its ids for a later tick.
    """
    scans = OxpullWorkflowScan.objects.using(db_alias)
    queues = list(
        scans.order_by("workflow_id").values_list("workflow_id", "skipped", "version")
    )
    after = (
        OxpullWorkflowRound.objects.using(db_alias)
        .filter(pk=ROUND_ROW)
        .values_list("workflow_id", flat=True)
        .first()
    )
    if after is not None:
        start = bisect.bisect_right([uuid.UUID(str(q[0])) for q in queues], after)
        queues = queues[start:] + queues[:start]
    taken: list[str] = []
    for workflow_id, queued, version in queues:
        room = ID_CHUNK_SIZE - len(taken)
        if room <= 0:
            break
        if not queued:
            continue
        take, rest = list(queued[:room]), list(queued[room:])
        if scans.filter(workflow_id=workflow_id, version=version).update(
            skipped=rest, version=version + 1
        ):
            taken.extend(take)
    return taken


def _revisit(task_ids: Iterable[Any], db_alias: str) -> int:
    """
    Everything the backstop does for a node, for nodes whose rows it could not
    write when it paged past them. Returns how many were released.

    Marks the edges into them whose upstream is known to have succeeded,
    repairs their records, and releases the ones left waiting with every edge
    satisfied. These writes wait for a held lock, up to BACKSTOP_LOCK_TIMEOUT,
    rather than skipping it. A write that still loses puts the ids back in the
    queue, so a row is tried again on every tick until its lock is released.
    """
    ids = sorted({uuid.UUID(str(task_id)) for task_id in task_ids})
    wanted = _satisfiable_edges_into(ids, db_alias)
    if wanted:
        _, skipped = _write_locked(
            OxpullWorkflowEdge,
            wanted,
            lambda pks: _mark_satisfied(pks, db_alias),
            db_alias,
            skip_locked=False,
        )
        queue_skipped({wanted[pk] for pk in skipped}, db_alias)
    nodes: list[tuple[int, Any, str | None, int | None]] = []
    for chunk in _id_chunks(ids, db_alias):
        nodes.extend(
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(task_id__in=chunk)
            .values_list("pk", "task_id", "outcome", "outcome_epoch")
        )
    _repair_node_records(nodes, db_alias, skip_locked=False)
    return release_downstream(ids, db_alias)


def _write_locked(
    model: type[Model],
    pks: Iterable[int],
    write: Callable[[list[int]], object],
    db_alias: str,
    *,
    skip_locked: bool = True,
) -> tuple[list[int], list[int]]:
    """
    Lock rows of `model` by primary key in key order and run `write` over the
    ones this transaction locked, in one transaction. Returns (written,
    skipped).

    With skip_locked, a row another transaction holds is skipped at once.
    Without it, the lock is waited for. When the call owns its transaction, a
    lock wait is bounded by BACKSTOP_LOCK_TIMEOUT and a statement by
    BACKSTOP_STATEMENT_TIMEOUT, and a lost lock conflict or a timeout skips
    every row of the call. Inside a caller's transaction nothing is bounded and
    every error propagates.
    """
    wanted = sorted(set(pks))
    connection = connections[db_alias]
    owned = connection.get_autocommit() and not connection.in_atomic_block
    bounds: AbstractContextManager[None] = (
        session_timeouts(
            db_alias,
            lock_wait=BACKSTOP_LOCK_TIMEOUT,
            statement=BACKSTOP_STATEMENT_TIMEOUT,
        )
        if owned
        else nullcontext()
    )
    locked = wanted
    try:
        with bounds, transaction.atomic(using=db_alias):
            if owned:
                transaction_timeouts(
                    db_alias,
                    lock_wait=BACKSTOP_LOCK_TIMEOUT,
                    statement=BACKSTOP_STATEMENT_TIMEOUT,
                )
            locked = _lock_in_key_order(
                model, wanted, db_alias, skip_locked=skip_locked
            )
            if locked:
                write(locked)
    except OperationalError as exc:
        if not (owned and lost_lock_conflict(exc, db_alias)):
            raise
        logger.info(
            "The workflow backstop could not lock %d %s row(s) and will try them again",
            len(wanted),
            model._meta.model_name,
            exc_info=True,
            extra={
                "event": "workflow_backstop_skipped",
                "row_count": len(wanted),
                "table": model._meta.db_table,
            },
        )
        return [], wanted
    written = set(locked)
    return sorted(written), [pk for pk in wanted if pk not in written]


def _lock_in_key_order(
    model: type[Model], pks: Iterable[int], db_alias: str, *, skip_locked: bool = False
) -> list[int]:
    """
    Lock rows of `model` by primary key, in key order, inside the current
    transaction. Returns the keys this transaction locked, in key order.

    Almost every multi-row lock on a workflow's node or edge rows is taken this
    way before the rows are written. Workflow._resolve_dependencies is the
    exception: it finds its upstreams by (workflow_id, name_key) and only sorts
    by primary key, so MySQL locks those rows in unique-index order. It can
    deadlock there with _repair_node_records, which takes the same rows by key
    and waits. Both retry their whole call, so no work is lost.

    An UPDATE locks rows in the order its plan reads them, which on PostgreSQL
    can be table order, so two writers of the same rows could otherwise lock
    them in opposite orders. PostgreSQL sorts before it locks. MySQL locks as it
    reads the primary key range, which is key order. With skip_locked, a row
    another transaction holds is left out. A database without row locks returns
    every key and locks nothing.
    """
    wanted = sorted(set(pks))
    connection = connections[db_alias]
    if not connection.features.has_select_for_update:
        return wanted
    skip = skip_locked and connection.features.has_select_for_update_skip_locked
    locked: list[int] = []
    for chunk in _id_chunks(wanted, db_alias):
        locked.extend(
            model._default_manager.using(db_alias)
            .filter(pk__in=chunk)
            .order_by("pk")
            .select_for_update(skip_locked=skip)
            .values_list("pk", flat=True)
        )
    return locked


def _repair_node_records(
    nodes: Iterable[tuple[int, Any, str | None, int | None]],
    db_alias: str,
    *,
    skip_locked: bool = True,
) -> None:
    """
    Record on each node what its task row says now, where the transition table
    takes it. `nodes` are (node pk, task id, outcome, outcome_epoch).

    The rows are read by primary key with a plain read, and each record is
    written by the guarded UPDATE record_outcome() uses, so a stale reading
    lands nowhere a later one already did. A node row another transaction
    holds is skipped and its task id queued.
    """
    nodes = list(nodes)
    rows: dict[uuid.UUID, tuple[str, int]] = {}
    for chunk in _id_chunks([task_id for _, task_id, _, _ in nodes], db_alias):
        rows.update(
            (uuid.UUID(str(pk)), (status, epoch))
            for pk, status, epoch in OxTask.objects.using(db_alias)
            .filter(pk__in=chunk)
            .values_list("pk", "status", "lease_epoch")
        )
    wanted: dict[int, tuple[str, int, uuid.UUID]] = {}
    for node_pk, task_id, outcome, outcome_epoch in nodes:
        key = uuid.UUID(str(task_id))
        row = rows.get(key)
        if row is None or row[0] not in RECORDED_STATUSES:
            continue
        if record_takes(outcome, outcome_epoch, row[0], row[1]):
            wanted[node_pk] = (row[0], row[1], key)
    if not wanted:
        return
    _, skipped = _write_locked(
        OxpullWorkflowNode,
        wanted,
        lambda pks: _record_many(
            {pk: (wanted[pk][0], wanted[pk][1]) for pk in pks}, db_alias
        ),
        db_alias,
        skip_locked=skip_locked,
    )
    queue_skipped({wanted[pk][2] for pk in skipped}, db_alias)


# The one row that holds the backstop's round-robin position.
ROUND_ROW = 1


# Where a pass stands: (phase, position, edge_hwm, node_hwm).
Place = tuple[str, int, int | None, int | None]

# The place of a workflow with no pass under way, and of a scan row that holds
# only a skip queue.
AT_REST: Place = (OxpullWorkflowScan.Phase.EDGES, 0, None, None)


@dataclass
class _Scan:
    """One workflow's place in its backstop pass, as a visit carries it."""

    phase: str
    position: int
    edge_hwm: int | None
    node_hwm: int | None
    # The place this visit last read or stored, which its next store must
    # still find. AT_REST when no row was found.
    stored: Place

    @property
    def place(self) -> Place:
        return (self.phase, self.position, self.edge_hwm, self.node_hwm)


def _release_backstop(db_alias: str) -> int:
    """
    The reconciler's backstop: satisfy, record and release what no worker did.
    Returns how many nodes were released.

    First the skip queues, up to one chunk of ids a tick, retried by _revisit.
    The budget below does not count this. However long it takes, the round
    below still runs at least one page.

    Then a round over the workflows that are OPEN or SEALED, in id order,
    starting after the one the last tick stopped at, and wrapping. Each
    workflow gets a visit (_visit). The deadline is BACKSTOP_BUDGET after the
    round starts, or a quarter of what the task timeout leaves if that is
    sooner. The round starts no visit after the deadline, and a visit starts no
    page after it, but the first visit and its first page always run. A tick
    that stops part-way stores the last workflow it visited, by compare-and-set
    on OxpullWorkflowRound, so the next tick goes on from there and every
    workflow gets a visit each round.

    What the budget does not bound. A page that starts before the deadline runs
    to its end. Its reads carry no time bound. Each of its writes waits at most
    BACKSTOP_LOCK_TIMEOUT for a lock and runs each statement for at most
    BACKSTOP_STATEMENT_TIMEOUT, and each release chunk it runs may try twice
    with RELEASE_LOCK_TIMEOUT and RELEASE_STATEMENT_TIMEOUT. MySQL applies the
    statement bound to SELECTs only. So a tick can run past its deadline by a
    page's work, and the skip queue's retries come on top.

    Task rows are read by primary key only, with plain reads. The writes lock
    only oxpull's own edge and node rows, and release_downstream moves task
    rows through django-ox's pinned helper after each write commits.

    How long a row can wait, if the locks on it are eventually released. A pass
    over workflow w takes at most ceil(E / BACKSTOP_PAGE) + 2 * ceil(N /
    BACKSTOP_PAGE) + 3 pages, for its E open edges and N open nodes. Each round
    gives w one visit of at least one page. A row open when a pass begins is
    read in that pass, and a row added after it began is read in the next one,
    so any open row is read within two passes of w, which is at most twice
    that many rounds. A row that was locked is queued and tried again on every
    tick, and every later pass reads it again, so no row is ever given up on.
    """
    released = _revisit_skipped(db_alias)
    return released + _backstop_round(db_alias, time.monotonic() + _backstop_budget())


def _revisit_skipped(db_alias: str) -> int:
    """Retry up to one chunk of queued ids (_take_skipped, _revisit)."""
    taken = _take_skipped(db_alias)
    return _revisit(taken, db_alias) if taken else 0


def _backstop_budget() -> float:
    """Seconds this tick may spend paging: BACKSTOP_BUDGET, or less under a timeout."""
    left = remaining()
    if left is None:
        return BACKSTOP_BUDGET
    return max(0.0, min(BACKSTOP_BUDGET, left / 4))


def _backstop_round(db_alias: str, deadline: float) -> int:
    """One tick's share of the round-robin over open workflows (_release_backstop)."""
    live = [
        uuid.UUID(str(pk))
        for pk in OxpullWorkflow.objects.using(db_alias)
        .filter(status__in=(OxpullWorkflow.Status.OPEN, OxpullWorkflow.Status.SEALED))
        .order_by("pk")
        .values_list("pk", flat=True)
    ]
    if not live:
        return 0
    rounds = OxpullWorkflowRound.objects.using(db_alias)
    stored = rounds.filter(pk=ROUND_ROW).values_list("workflow_id", "version").first()
    after, version = stored if stored is not None else (None, None)
    start = bisect.bisect_right(live, after) if after is not None else 0
    visiting = live[start:] + live[:start]
    marks = (
        _highest_pk(OxpullWorkflowEdge, db_alias),
        _highest_pk(OxpullWorkflowNode, db_alias),
    )
    released = 0
    visited = 0
    for workflow_id in visiting:
        if visited and time.monotonic() >= deadline:
            break
        released += _visit(workflow_id, marks, deadline, db_alias)
        visited += 1
    if visited < len(visiting):
        _move_round(visiting[visited - 1], version, db_alias)
    return released


def _highest_pk(model: type[Model], db_alias: str) -> int:
    top = model._default_manager.using(db_alias).aggregate(top=Max("pk"))["top"]
    return int(top or 0)


def _move_round(workflow_id: uuid.UUID, version: int | None, db_alias: str) -> None:
    """Store where the round stopped, by compare-and-set. A loser leaves it."""
    rounds = OxpullWorkflowRound.objects.using(db_alias)
    if version is None:
        with suppress(IntegrityError), transaction.atomic(using=db_alias):
            rounds.create(pk=ROUND_ROW, workflow_id=workflow_id, version=1)
        return
    rounds.filter(pk=ROUND_ROW, version=version).update(
        workflow_id=workflow_id, version=version + 1
    )


def _visit(
    workflow_id: uuid.UUID, marks: tuple[int, int], deadline: float, db_alias: str
) -> int:
    """
    One backstop visit to one workflow. Returns how many nodes it released.

    A pass over a workflow has three phases, each paged in key order up to the
    high-water marks the pass recorded when it began: the edges not marked
    satisfied (_edge_page), then the nodes not recorded as succeeded
    (_node_page), then the same nodes again for release (_release_page). A
    visit runs pages from where the workflow's pass stands. It pages on while
    its pages come back full, up to VISIT_PAGES, and a short page ends its
    phase. It stops when the pass ends, or once the tick's deadline has passed
    after its first page.

    The place is stored on OxpullWorkflowScan by compare-and-set on the place
    this visit read, after every full page and when the visit stops part-way,
    and a visit that loses the compare-and-set stops. A pass that ends puts a
    stored place back at rest. A pass whose pages all come back short, and
    which begins and ends inside one visit, stores nothing. A page always moves
    the position past every row it read, whether it could write them or not: a
    row it could not lock goes to the skip queue, which the same row holds
    beside the place.
    """
    stored = (
        OxpullWorkflowScan.objects.using(db_alias)
        .filter(workflow_id=workflow_id)
        .values_list("phase", "position", "edge_hwm", "node_hwm")
        .first()
    )
    place: Place = AT_REST if stored is None else cast("Place", tuple(stored))
    scan = _Scan(*place, stored=place)
    if scan.edge_hwm is None or scan.node_hwm is None:
        scan.phase, scan.position = OxpullWorkflowScan.Phase.EDGES, 0
        scan.edge_hwm, scan.node_hwm = marks
    released = 0
    pages = 0
    while pages < VISIT_PAGES and not (pages and time.monotonic() >= deadline):
        pages += 1
        full, last, moved = _PAGES[scan.phase](workflow_id, scan, db_alias)
        released += moved
        if full:
            scan.position = last
            if not _store_scan(workflow_id, scan, db_alias):
                return released
            continue
        following = _FOLLOWING_PHASE.get(scan.phase)
        if following is None:
            if scan.stored != AT_REST:
                scan.phase, scan.position, scan.edge_hwm, scan.node_hwm = AT_REST
                _store_scan(workflow_id, scan, db_alias)
            return released
        scan.phase, scan.position = following, 0
    _store_scan(workflow_id, scan, db_alias)
    return released


def _store_scan(workflow_id: uuid.UUID, scan: _Scan, db_alias: str) -> bool:
    """
    Store a visit's place by compare-and-set on the place it last saw. False
    when another visit moved it.

    The skip queue shares the row and moves only its own column and the
    version, so a visit's own queued ids do not make it lose its place.
    """
    scans = OxpullWorkflowScan.objects.using(db_alias)
    if scan.place == scan.stored:
        return True
    phase, position, edge_hwm, node_hwm = scan.stored
    fields = {
        "phase": scan.phase,
        "position": scan.position,
        "edge_hwm": scan.edge_hwm,
        "node_hwm": scan.node_hwm,
    }
    moved = scans.filter(
        workflow_id=workflow_id,
        phase=phase,
        position=position,
        edge_hwm=edge_hwm,
        node_hwm=node_hwm,
    ).update(version=F("version") + 1, **fields)
    if not moved:
        if scan.stored != AT_REST:
            return False
        try:
            with transaction.atomic(using=db_alias):
                scans.create(workflow_id=workflow_id, **fields)
        except IntegrityError:
            return False
    scan.stored = scan.place
    return True


def _edge_page(
    workflow_id: uuid.UUID, scan: _Scan, db_alias: str
) -> tuple[bool, int, int]:
    """
    Mark satisfied the edges on this page whose upstream is known to have
    succeeded, then release what they held. Returns (full, last key, released).

    The page reads the workflow's unmarked edges on oxpull_wf_edge_open_idx.
    The marks commit before the release runs.
    """
    rows = list(
        OxpullWorkflowEdge.objects.using(db_alias)
        .filter(
            workflow_id=workflow_id,
            satisfied=False,
            pk__gt=scan.position,
            pk__lte=scan.edge_hwm,
        )
        .order_by("pk")
        .values_list("pk", "upstream_task_id", "downstream_task_id")[:BACKSTOP_PAGE]
    )
    if not rows:
        return False, scan.position, 0
    proven = _succeeded_or_recorded({up for _, up, _ in rows}, db_alias)
    wanted = {
        pk: uuid.UUID(str(down))
        for pk, up, down in rows
        if uuid.UUID(str(up)) in proven
    }
    released = 0
    if wanted:
        marked, skipped = _write_locked(
            OxpullWorkflowEdge,
            wanted,
            lambda pks: _mark_satisfied(pks, db_alias),
            db_alias,
        )
        queue_skipped({wanted[pk] for pk in skipped}, db_alias)
        downstream = {wanted[pk] for pk in marked}
        if downstream:
            released = release_downstream(downstream, db_alias)
    return len(rows) == BACKSTOP_PAGE, rows[-1][0], released


def _open_node_page(
    workflow_id: uuid.UUID, scan: _Scan, db_alias: str
) -> list[tuple[int, Any, str | None, int | None]]:
    """The page of nodes not recorded as succeeded, on oxpull_wf_node_open_idx."""
    return list(
        OxpullWorkflowNode.objects.using(db_alias)
        .filter(
            workflow_id=workflow_id,
            succeeded=False,
            pk__gt=scan.position,
            pk__lte=scan.node_hwm,
        )
        .order_by("pk")
        .values_list("pk", "task_id", "outcome", "outcome_epoch")[:BACKSTOP_PAGE]
    )


def _node_page(
    workflow_id: uuid.UUID, scan: _Scan, db_alias: str
) -> tuple[bool, int, int]:
    """Repair the records on this page of open nodes. Returns (full, last key, 0)."""
    rows = _open_node_page(workflow_id, scan, db_alias)
    if not rows:
        return False, scan.position, 0
    _repair_node_records(rows, db_alias)
    return len(rows) == BACKSTOP_PAGE, rows[-1][0], 0


def _release_page(
    workflow_id: uuid.UUID, scan: _Scan, db_alias: str
) -> tuple[bool, int, int]:
    """
    Release the waiting nodes on this page of open nodes whose edges are all
    satisfied. Returns (full, last key, released).
    """
    rows = _open_node_page(workflow_id, scan, db_alias)
    if not rows:
        return False, scan.position, 0
    waiting: list[uuid.UUID] = []
    for chunk in _id_chunks([task_id for _, task_id, _, _ in rows], db_alias):
        waiting.extend(
            uuid.UUID(str(pk))
            for pk in OxTask.objects.using(db_alias)
            .filter(pk__in=chunk, status=OxTask.Status.WAITING)
            .values_list("pk", flat=True)
        )
    released = release_downstream(waiting, db_alias) if waiting else 0
    return len(rows) == BACKSTOP_PAGE, rows[-1][0], released


_PAGES: dict[str, Callable[[uuid.UUID, _Scan, str], tuple[bool, int, int]]] = {
    OxpullWorkflowScan.Phase.EDGES: _edge_page,
    OxpullWorkflowScan.Phase.NODES: _node_page,
    OxpullWorkflowScan.Phase.RELEASE: _release_page,
}

_FOLLOWING_PHASE: dict[str, str] = {
    OxpullWorkflowScan.Phase.EDGES: OxpullWorkflowScan.Phase.NODES,
    OxpullWorkflowScan.Phase.NODES: OxpullWorkflowScan.Phase.RELEASE,
}


def _require_workflow_backend(task: Task[..., Any]) -> None:
    """
    Refuse a node whose backend would not run it under the gate.

    Two ways that happens, and both are silent without this. A backend
    that does not declare WORKFLOWS emits the ungated claim, so every
    node is claimed as soon as it is enqueued, in no particular order. A
    backend that declares it but names a WORKER_CLASS that is not an
    OxpullWorker does the same. The second is also oxpull.E007, a
    start-up error; this is the line that catches the first, and it
    catches both for anyone who reaches add() anyway.

    Ordering is the whole product here, so it fails on the add() call
    rather than three hours later, out of order, with a healthy-looking
    queue.
    """
    from django.tasks import task_backends

    from django_ox.worker import worker_class

    from .worker import OxpullWorker

    alias = task.backend
    if not workflows_enabled_from_options(task_backends[alias].options):
        raise ImproperlyConfigured(
            f"Backend {alias!r} does not declare OPTIONS['WORKFLOWS'] = True, "
            "so its claim carries no dependency gate: every node of this "
            "workflow would be claimed as soon as it was enqueued, in no "
            "particular order."
        )
    runs = worker_class(alias)
    if not issubclass(runs, OxpullWorker):
        raise ImproperlyConfigured(
            f"Workflow nodes on backend {alias!r} would be run by "
            f"{runs.__module__}.{runs.__qualname__}, which does not apply "
            "dependency gating: every node would be claimed as soon as it "
            "was enqueued, in no particular order. Set "
            "OPTIONS['WORKER_CLASS'] to 'oxpull.worker.OxpullWorker'."
        )


def _refuse_unique_node(task: Task[..., Any]) -> None:
    """
    Refuse a unique task as a node, before anything is enqueued.

    A unique enqueue returns whichever task already holds the key, so the
    node's row could be another workflow's node or a call the application
    enqueued on its own, and a later enqueue of the same call would return
    it too. Every holder's edges would gate that one row and every
    holder's reconciler could discard it: one workflow's failure cancels
    another's node, the application's call is discarded without running,
    a row already running becomes a node ahead of its upstream, and two
    workflows that add the same calls in opposite orders wait on each
    other for good. Refusing only when the key is already held would not
    do, because the application's enqueue can come after the node.

    Decided by the backend the task enqueues through, with the check its
    enqueue() makes, so a task listed in UNIQUE_TASKS on another backend
    is an ordinary node here. A refused add() calls no enqueue(), sends no
    task_enqueued and writes no row.
    """
    from .backend import OxpullBackend

    backend = task.get_backend()
    if isinstance(backend, OxpullBackend) and backend._is_unique(task):
        raise ImproperlyConfigured(
            f"Workflow node {task.module_path!r} is a unique task on backend "
            f"{task.backend!r}, and a unique task cannot be a workflow node. "
            "Its enqueue can return a task that another workflow or the "
            "application already holds, and this workflow's failures would "
            "cancel that task. A shared expensive job belongs outside the "
            "workflow as a unique task; add a task that is not unique as the "
            "node."
        )


def declaring_aliases() -> list[str]:
    """The OxpullBackend aliases whose OPTIONS declare WORKFLOWS = True, sorted."""
    from .backend import OxpullBackend

    found = []
    for alias, backend_class in task_backend_classes().items():
        if not issubclass(backend_class, OxpullBackend):
            continue
        options = settings.TASKS[alias].get("OPTIONS")
        if isinstance(options, dict) and options.get("WORKFLOWS") is True:
            found.append(alias)
    return sorted(found)


def workflows_on_database() -> bool:
    """
    Whether any backend declares WORKFLOWS on the database the task rows live
    on.

    django-ox writes every backend's task rows to one database, the one
    oxpull_db_alias() resolves, so one declaring backend puts workflow nodes
    on it. Any OxpullWorker there may claim a node, because claims are scoped
    to queues and not to backends, so every one of them records a node's
    outcome and releases what it held.
    """
    return bool(declaring_aliases())


def reconcile_schedule_hint(schedule: ReconcilerSchedule | None = None) -> str:
    """
    What to do about a missing workflow reconciler schedule.

    A project that reads schedule rows is told to expose the reconciler to
    them, when a backend reading them can run it; batches'
    reconcile_schedule_hint says why. Otherwise it gets one SCHEDULES
    entry and the backend to add it to: a backend declaring WORKFLOWS
    first, then any django-ox backend, and only one the reconciler binds
    to. The warning is printed once and names one backend, so following it
    cannot define one schedule name on two backends (django_ox.E003) or put
    the reconciler on a queue its backend refuses (django_ox.E002).
    """
    if schedule is None:
        schedule = reconcile_schedule(RECONCILE_TASK_PATH)
    if schedule.row_readers and not schedule.exposed:
        try:
            public = _build_task_on_a_queue(_reconcile)
        except Exception:
            public = None
        if public is not None and any(
            _binds(public, alias) for alias in schedule.row_readers
        ):
            return RECONCILE_SCHEDULABLE_HINT
    declaring = declaring_aliases()
    others = sorted(alias for alias in _ox_backend_options() if alias not in declaring)
    return schedule_entry_hint(
        RECONCILE_TASK_PATH, RECONCILE_SCHEDULE_NAME, [*declaring, *others]
    )


def _require_reconcile_schedule() -> None:
    """
    Refuse to create or seal a workflow that nothing would ever complete.

    The same refusal batches make, for the same reasons: completion and
    the cancellation of a failed node's dependents happen only in the
    reconciler, only a django-ox schedule runs it, and oxpull.W003 can be
    silenced or never read.
    """
    schedule = reconcile_schedule(RECONCILE_TASK_PATH)
    if schedule.aliases:
        return
    if schedule.unbound:
        raise ImproperlyConfigured(
            f"Workflows need a schedule for {RECONCILE_TASK_PATH} that a worker "
            "can run. "
            + unbound_reconciler_message(
                RECONCILE_TASK_PATH, schedule, UNSCHEDULED_CONSEQUENCE
            )
            + " "
            + unbound_reconciler_hint(
                RECONCILE_TASK_PATH, RECONCILE_SCHEDULE_NAME, schedule
            )
            + " The system check reports the same condition as oxpull.E008."
        )
    raise ImproperlyConfigured(
        f"Workflows need a schedule for {RECONCILE_TASK_PATH}, and no django-ox "
        f"backend in settings.TASKS declares one, so {UNSCHEDULED_CONSEQUENCE}. "
        f"{reconcile_schedule_hint(schedule)} The system check reports the same "
        "condition as oxpull.W003."
    )


def create(
    name: str,
    *,
    on_complete: Task[..., Any] | str | None = None,
    on_complete_args: list[Any] | tuple[Any, ...] | None = None,
    on_complete_kwargs: dict[str, Any] | None = None,
    on_success: Task[..., Any] | str | None = None,
    on_success_args: list[Any] | tuple[Any, ...] | None = None,
    on_success_kwargs: dict[str, Any] | None = None,
) -> Workflow:
    """
    Create an OPEN workflow.

    on_complete fires once every node has finished, whatever the outcome,
    cancellations included. on_success fires in addition, and only when
    every node's outcome is known and none of them FAILED, so a
    workflow with a cancelled branch completes without it. Each is a Task
    or its dotted path, with optional arguments; both are enqueued as
    ordinary tasks and are not supervised beyond storing their ids.

    Raises ImproperlyConfigured if no django-ox backend schedules the
    reconciler, as batches.create() does: a workflow nothing completes
    should fail on the line that creates it.
    """
    _require_reconcile_schedule()
    if len(name) > NAME_MAX_LENGTH:
        # SQLite would store it, MySQL and PostgreSQL would not; reject
        # it identically everywhere.
        raise ValueError(f"Workflow name exceeds {NAME_MAX_LENGTH} characters.")
    complete_path, complete_args, complete_kwargs = _resolve_callback(
        on_complete, on_complete_args, on_complete_kwargs, label="Workflow"
    )
    success_path, success_args, success_kwargs = _resolve_callback(
        on_success, on_success_args, on_success_kwargs, label="Workflow"
    )
    db_alias = oxpull_db_alias()
    row = OxpullWorkflow.objects.using(db_alias).create(
        name=name,
        status=OxpullWorkflow.Status.OPEN,
        on_complete_path=complete_path,
        on_complete_args=complete_args,
        on_complete_kwargs=complete_kwargs,
        on_success_path=success_path,
        on_success_args=success_args,
        on_success_kwargs=success_kwargs,
        created_at=timezone.now(),
    )
    return Workflow(row)


def get(workflow_id: uuid.UUID | str) -> Workflow:
    """
    The workflow with this id. Raises OxpullWorkflow.DoesNotExist if
    there is none.
    """
    db_alias = oxpull_db_alias()
    return Workflow(OxpullWorkflow.objects.using(db_alias).get(pk=workflow_id))


# -- the reconciler ----------------------------------------------------


def _reconcile() -> int:
    """
    Satisfy, record and release what no worker did, cancel what can no longer
    run, complete every SEALED workflow whose nodes have all finished, and
    return how many were completed.

    Run it through django-ox's schedule dispatcher once a minute. Workflow
    completion and callback enqueue share a transaction, so a crash leaves
    either an incomplete workflow or a completed workflow with its callbacks
    enqueued. A later pass can revisit an incomplete workflow. The release
    backstop (_release_backstop) resumes from its recorded position.

    Cancellation is not on the critical path of anything that succeeds.
    The worker that writes an upstream's SUCCESSFUL releases what it held.
    The backstop here is for what did not happen. A backstop that fails is
    logged, and the tick still cancels and completes workflows, because
    neither of those waits on a release.

    A tick under a task timeout visits sealed workflows in a new order,
    stops between workflows when the next would probably not finish in
    time, and lets a TaskTimeout out when one lands anyway, so django-ox
    records that attempt as timed out (see batches._sweep).
    """
    db_alias = oxpull_db_alias()
    try:
        released = _release_backstop(db_alias)
    except Exception as exc:
        if _timed_out(exc):
            raise
        released = 0
        logger.exception(
            "The workflow release backstop failed. This tick still cancels and "
            "completes workflows",
            extra={"event": "workflow_release_backstop_failed"},
        )
    if released:
        logger.info(
            "Released %d waiting workflow node(s) no worker released",
            released,
            extra={"event": "workflow_nodes_released", "released_count": released},
        )
    sealed = (
        OxpullWorkflow.objects.using(db_alias)
        .filter(status=OxpullWorkflow.Status.SEALED)
        .order_by()
        .values_list("pk", flat=True)
    )
    completed = 0
    for workflow_pk in _sweep(list(sealed), "workflow", "workflows", logger):
        try:
            _cancel_unreachable(workflow_pk, db_alias)
            if _complete_if_finished(workflow_pk, db_alias):
                completed += 1
        except Exception as exc:
            # The tick ran out of time; this workflow did not fail.
            if _timed_out(exc):
                raise
            logger.exception(
                "Workflow %s could not be reconciled",
                workflow_pk,
                extra={
                    "event": "workflow_reconcile_failed",
                    "workflow_id": str(workflow_pk),
                },
            )
    return completed


def _cancel_unreachable(workflow_pk: uuid.UUID, db_alias: str) -> int:
    """
    Discard every READY or WAITING node of this workflow that depends,
    directly or transitively, on a node that settled without succeeding.

    Without this those rows wait forever: the workflow never completes and
    ox_prune never reaches them, because it prunes only settled rows.

    Only READY and WAITING nodes are cancelled. A node that already ran
    holds its own outcome, and a RUNNING node belongs to the worker holding
    its lease.

    It first asks, with a plain read and no transaction, whether anything is
    cancellable at all (_cancellable_edges). A workflow whose cones are
    already closed costs one read a tick, and no lock and no write. The read
    can only be stale in one direction that matters, a node dying just after
    it, and the next tick cancels that cone.

    Then one transaction per level of the cone (_cancel_one_level), until a
    level moves nothing or MAX_CANCEL_PASSES levels have run. A level that
    loses a deadlock or a serialization failure runs its transaction again
    from its first read, as completion does. At PostgreSQL REPEATABLE READ a
    retry that commits after the level's snapshot and before its lock raises
    the serialization failure, and the level run again decides on the row as
    it is now.
    """
    if not _cancellable_edges(workflow_pk, db_alias).exists():
        return 0

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        _log_reissue(workflow_pk, "the cancellation", conflict, attempt, bound)

    cancelled = 0
    for _ in range(MAX_CANCEL_PASSES):
        moved = _reissue(
            lambda: _cancel_one_level(workflow_pk, db_alias),
            db_alias,
            deadlocks=True,
            retrying=retrying,
        )
        cancelled += moved
        if not moved:
            break
    if cancelled:
        logger.info(
            "Workflow %s cancelled %d unreachable node(s)",
            workflow_pk,
            cancelled,
            extra={
                "event": "workflow_nodes_cancelled",
                "workflow_id": str(workflow_pk),
                "cancelled_count": cancelled,
            },
        )
    return cancelled


def _cancel_one_level(workflow_pk: uuid.UUID, db_alias: str) -> int:
    """
    Cancel the nodes doomed by the upstreams that are dead now. Returns how
    many moved.

    The concurrency argument, which is the whole reason this is not one
    statement:

    An operator may call django_ox.actions.retry on a FAILED or LOST upstream
    at any moment, and a retry that then succeeds makes the downstream node
    legitimately runnable. So the decision rests only on upstream rows this
    transaction holds. In order:

    1. The workflow row, by a no-op UPDATE under SEALED. It makes this
       transaction a writer at once, which keeps SQLite off its read-to-write
       lock upgrade, and two reconcilers cancelling one workflow serialise.
    2. A plain read of the upstreams that doom a cancellable node
       (_cancellable_edges), noting which rows it found.
    3. A locking read of the rows it found, by primary key in key order, and
       nothing else. Only the ones still in a dead status under that lock are
       doomed. retry() changes a row with an UPDATE that needs the same lock,
       so a retry either committed before the lock and the row is not dead, or
       waits until this transaction commits. An upstream that died after step
       2 is not in the set, and its cone waits for the next tick.
    4. The rows step 2 did not find, whose node records a dead status. A
       pruned row does not come back, and nothing writes the record of a
       missing row, so they stay doomed.
    5. A plain read of the READY and WAITING rows behind an unmarked edge from
       those upstreams, and only those, with their lease epochs.
    6. django-ox's cancel_many over those pairs. It locks the rows in key
       order and moves only a row still READY or WAITING at the epoch read. A
       row claimed since is RUNNING, and a row reaped back to READY has a new
       epoch, so neither is discarded.
    7. The DISCARDED record on the nodes that moved, at their epochs.

    The lock order is the one every workflow operation takes: the workflow
    row, then task rows, then node rows. The locking read in step 3 names
    task rows by primary key alone, so on MySQL it locks those rows and no
    node of the workflow still running. SQLite has no row locks and needs
    none: writers are serialised.
    """
    locks = connections[db_alias].features.has_select_for_update
    with transaction.atomic(using=db_alias):
        if not (
            OxpullWorkflow.objects.using(db_alias)
            .filter(pk=workflow_pk, status=OxpullWorkflow.Status.SEALED)
            .update(status=OxpullWorkflow.Status.SEALED)
        ):
            return 0
        # upstream_status is annotated by _cancellable_edges, which the type
        # of its return value cannot carry.
        fields = ("upstream_task_id", "upstream_status")
        candidates: dict[uuid.UUID, bool] = {
            uuid.UUID(str(upstream)): status != ""
            for upstream, status in _cancellable_edges(
                workflow_pk, db_alias
            ).values_list(*fields)
        }
        if not candidates:
            return 0
        doomed: set[uuid.UUID] = set()
        found = sorted(pk for pk, present in candidates.items() if present)
        for chunk in _id_chunks(found, db_alias):
            rows = OxTask.objects.using(db_alias).filter(pk__in=chunk).order_by("pk")
            if locks:
                rows = rows.select_for_update()
            doomed.update(
                uuid.UUID(str(pk))
                for pk, status in rows.values_list("pk", "status")
                if status in DEAD_STATUSES
            )
        gone = sorted(pk for pk, present in candidates.items() if not present)
        for chunk in _id_chunks(gone, db_alias):
            doomed.update(
                uuid.UUID(str(task_id))
                for task_id in OxpullWorkflowNode.objects.using(db_alias)
                .filter(task_id__in=chunk, outcome__in=DEAD_STATUSES)
                .values_list("task_id", flat=True)
            )
        if not doomed:
            return 0
        cone: set[uuid.UUID] = set()
        for chunk in _id_chunks(doomed, db_alias):
            cone.update(
                uuid.UUID(str(downstream))
                for downstream in OxpullWorkflowEdge.objects.using(db_alias)
                .filter(
                    workflow_id=workflow_pk,
                    satisfied=False,
                    upstream_task_id__in=chunk,
                )
                .values_list("downstream_task_id", flat=True)
            )
        pairs: list[tuple[uuid.UUID, int]] = []
        for chunk in _id_chunks(cone, db_alias):
            pairs.extend(
                (uuid.UUID(str(pk)), int(epoch))
                for pk, epoch in OxTask.objects.using(db_alias)
                .filter(pk__in=chunk, status__in=CANCELLABLE_STATUSES)
                .values_list("pk", "lease_epoch")
            )
        if not pairs:
            return 0
        moved = _waiting.cancel_many(pairs, using=db_alias)
        if moved:
            _record_cancelled([pk for pk, _ in pairs], db_alias)
        return moved


def _record_cancelled(task_ids: list[uuid.UUID], db_alias: str) -> None:
    """
    Record DISCARDED on the nodes among task_ids whose rows are now DISCARDED,
    locking their node rows in key order first.
    """
    records: dict[int, tuple[str, int]] = {}
    for chunk in _id_chunks(task_ids, db_alias):
        epochs = dict(
            OxTask.objects.using(db_alias)
            .filter(pk__in=chunk, status=OxTask.Status.DISCARDED)
            .values_list("pk", "lease_epoch")
        )
        if not epochs:
            continue
        for node_pk, task_id in (
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(task_id__in=list(epochs))
            .values_list("pk", "task_id")
        ):
            epoch = epochs.get(task_id)
            if epoch is None:
                epoch = epochs[uuid.UUID(str(task_id))]
            records[node_pk] = (OxTask.Status.DISCARDED, int(epoch))
    _lock_in_key_order(OxpullWorkflowNode, records, db_alias)
    _record_many(records, db_alias)


class _NodePending(Exception):
    """
    Raised inside the completing transaction to roll it back.

    Not an error anyone sees: it is how a node that became pending again
    between the cheap filter and the compare-and-set undoes the CAS.
    Django rolls an atomic block back on any exception, which is the
    shortest way out of a write already issued. batches._MemberPending,
    for the same reason.
    """


# How completion reads one node: its task row is there and not settled
# (pending), settled with a record the transition table would change (repair),
# settled with a record it would not change or gone with a record (recorded),
# or gone with no record (unknown).
PENDING_NODE = "pending"
REPAIR_NODE = "repair"
RECORDED_NODE = "recorded"
UNKNOWN_NODE = "unknown"


def _row_taken_q() -> Q:
    """outcome_accepts_q() against the node's own task row (_classified_nodes)."""
    same_epoch = Q(outcome__isnull=True) | (
        Q(outcome=OxTask.Status.LOST) & ~Q(row_status=OxTask.Status.LOST)
    )
    return (
        Q(outcome_epoch__isnull=True)
        | Q(outcome_epoch__lt=F("row_epoch"))
        | (Q(outcome_epoch=F("row_epoch")) & same_epoch)
    )


def _classified_nodes(workflow_pk: uuid.UUID, db_alias: str) -> Any:
    """
    This workflow's nodes, each annotated with its task row's status and epoch,
    how completion reads it (kind), and the settled status it counts as
    (settled_as): the record's for a recorded node, the row's for one the
    record does not yet say.
    """
    return _with_classes(
        OxpullWorkflowNode.objects.using(db_alias).filter(workflow_id=workflow_pk)
    )


def _with_rows(nodes: QuerySet[OxpullWorkflowNode]) -> Any:
    """`nodes` annotated with their task row's status and epoch, NULL if gone."""
    row = OxTask.objects.filter(pk=OuterRef("task_id"))
    return nodes.annotate(
        row_status=Subquery(row.values("status")[:1]),
        row_epoch=Subquery(row.values("lease_epoch")[:1]),
    )


def _with_classes(nodes: QuerySet[OxpullWorkflowNode]) -> Any:
    """`nodes` annotated as _classified_nodes describes."""
    return (
        _with_rows(nodes)
        .annotate(
            kind=Case(
                When(
                    Q(row_status__isnull=True, outcome__isnull=True),
                    then=Value(UNKNOWN_NODE),
                ),
                When(row_status__isnull=True, then=Value(RECORDED_NODE)),
                When(~Q(row_status__in=SETTLED_STATUSES), then=Value(PENDING_NODE)),
                When(_row_taken_q(), then=Value(REPAIR_NODE)),
                default=Value(RECORDED_NODE),
                output_field=CharField(),
            ),
        )
        .annotate(
            settled_as=Case(
                When(kind=REPAIR_NODE, then=F("row_status")),
                When(row_status__isnull=True, then=F("outcome")),
                When(outcome__isnull=True, then=F("row_status")),
                default=F("outcome"),
                output_field=CharField(null=True),
            ),
        )
    )


@dataclass(frozen=True)
class _Classes:
    """One workflow's nodes, counted the way completion reads them."""

    nodes: int
    pending: int
    repair: int
    succeeded: int
    failed: int
    # Nodes whose task row is there and DISCARDED.
    discarded_rows: int
    # Nodes whose task row is there and FAILED or LOST.
    dead_rows: int


def _classify(workflow_pk: uuid.UUID, db_alias: str) -> _Classes:
    """
    Count this workflow's nodes by how completion reads them, in one statement.

    succeeded and failed count how each node settled. That is its record where
    the record is the last word, so a failure that was then discarded, or whose
    row was pruned, still counts as failed, and a success whose row was pruned
    still counts as succeeded. Where the record is behind the row, it is the
    row. A node that is gone with no record counts in neither.
    """
    nodes = pending = repair = succeeded = failed = discarded = dead = 0
    for kind, settled_as, row_status, count in (
        _classified_nodes(workflow_pk, db_alias)
        .values("kind", "settled_as", "row_status")
        .annotate(count=Count("pk"))
        .order_by()
        .values_list("kind", "settled_as", "row_status", "count")
    ):
        nodes += count
        if row_status == OxTask.Status.DISCARDED:
            discarded += count
        elif row_status in RETRYABLE_DEAD_STATUSES:
            dead += count
        if kind == PENDING_NODE:
            pending += count
            continue
        if kind == REPAIR_NODE:
            repair += count
        if settled_as == OxTask.Status.SUCCESSFUL:
            succeeded += count
        elif settled_as == OxTask.Status.FAILED:
            failed += count
    return _Classes(
        nodes=nodes,
        pending=pending,
        repair=repair,
        succeeded=succeeded,
        failed=failed,
        discarded_rows=discarded,
        dead_rows=dead,
    )


def _log_reissue(
    workflow_pk: uuid.UUID | None, what: str, conflict: str, attempt: int, bound: int
) -> None:
    """Log a reissue, for one workflow or, with no workflow_pk, for several."""
    kind = "a deadlock" if conflict == DEADLOCK else "a serialization failure"
    owner = "Workflows" if workflow_pk is None else f"Workflow {workflow_pk}"
    extra: dict[str, Any] = {
        "event": "workflow_reconcile_conflict_retry",
        "conflict": conflict,
        "attempt": attempt,
    }
    if workflow_pk is not None:
        extra["workflow_id"] = str(workflow_pk)
    logger.info(
        "%s: %s lost %s on attempt %d of %d. Running its transaction again",
        owner,
        what,
        kind,
        attempt,
        bound,
        extra=extra,
    )


def _repair_records(workflow_pk: uuid.UUID, db_alias: str) -> None:
    """
    Record, in a transaction of its own, the outcome of every node of this
    workflow whose record the transition table would change, and mark the edges
    out of each that succeeded. The nodes those edges held are released after
    the commit.

    The node rows, then the edge rows, are locked in key order before they are
    written (_lock_in_key_order). A repair that loses a deadlock or a
    serialization failure reads the nodes again and runs its transaction
    again. At PostgreSQL REPEATABLE READ a writer that records the same node
    while the repair waits for its lock raises the serialization failure.
    """

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        _log_reissue(workflow_pk, "the record repair", conflict, attempt, bound)

    downstream = _reissue(
        lambda: _repair_records_once(workflow_pk, db_alias),
        db_alias,
        deadlocks=True,
        retrying=retrying,
    )
    if downstream:
        release_downstream(downstream, db_alias)


def _repair_records_once(workflow_pk: uuid.UUID, db_alias: str) -> set[uuid.UUID]:
    """One attempt of _repair_records. Returns the nodes the new marks point at."""
    rows = list(
        _classified_nodes(workflow_pk, db_alias)
        .filter(kind=REPAIR_NODE)
        .values_list("pk", "task_id", "row_status", "row_epoch")
    )
    if not rows:
        return set()
    succeeded = sorted(
        uuid.UUID(str(task_id))
        for _, task_id, status, _ in rows
        if status == OxTask.Status.SUCCESSFUL
    )
    downstream: set[uuid.UUID] = set()
    with transaction.atomic(using=db_alias):
        records = {pk: (status, int(epoch)) for pk, _, status, epoch in rows}
        _lock_in_key_order(OxpullWorkflowNode, records, db_alias)
        _record_many(records, db_alias)
        edges: dict[int, uuid.UUID] = {}
        for chunk in _id_chunks(succeeded, db_alias):
            edges.update(
                (pk, uuid.UUID(str(down)))
                for pk, down in _reading_index(
                    OxpullWorkflowEdge.objects.using(db_alias)
                    .filter(upstream_task_id__probe_in=chunk, satisfied__aside=False)
                    .values_list("pk", "downstream_task_id"),
                    EDGE_UPSTREAM_INDEX,
                )
            )
        if edges:
            _lock_in_key_order(OxpullWorkflowEdge, edges, db_alias)
            _mark_satisfied(edges, db_alias)
            downstream.update(edges.values())
    logger.info(
        "Workflow %s recorded %d node outcome(s) no worker recorded",
        workflow_pk,
        len(rows),
        extra={
            "event": "workflow_records_repaired",
            "workflow_id": str(workflow_pk),
            "node_count": len(rows),
        },
    )
    return downstream


def _complete_if_finished(workflow_pk: uuid.UUID, db_alias: str) -> bool:
    """
    Complete one sealed workflow if every node has finished and every record
    is written. True if this call is the one that completed it.

    The nodes are classified first, outside any transaction (_classify). A
    node whose record is behind its task row is repaired in a transaction of
    its own, and the workflow waits for the next tick, so the records commit
    before the completion that reads them. A pending node, one whose row is
    READY, WAITING, RUNNING or a status this package does not know, holds the
    workflow. A node whose row is gone with no record does not hold it: it is
    finished, and unattributed, so on_success is withheld.

    The classification is a filter, not the authority. django_ox.actions.retry
    moves a FAILED or LOST row back to READY and takes no workflow lock, so
    "nothing is pending" is not monotone. The compare-and-set therefore locks
    the FAILED and LOST rows and classifies again under its locks
    (_complete_once).

    Everything after the CAS rides in the same transaction, so a crash
    between completing the workflow and enqueueing its callbacks rolls
    both back and the next tick redoes them.

    At PostgreSQL REPEATABLE READ a reconciler whose CAS waited on another
    one completing the same workflow gets a serialization failure when that
    one commits, not a rowcount of 0. The transaction is reissued, and the
    CAS then matches no SEALED row.
    """
    classes = _classify(workflow_pk, db_alias)
    if classes.repair:
        _repair_records(workflow_pk, db_alias)
        return False
    if classes.pending:
        return False

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        logger.info(
            "Workflow %s: completion lost a serialization failure on attempt %d "
            "of %d; reissuing its transaction",
            workflow_pk,
            attempt,
            bound,
            extra={
                "event": "workflow_complete_serialization_retry",
                "workflow_id": str(workflow_pk),
                "attempt": attempt,
            },
        )

    outcome = _reissue(
        lambda: _complete_once(workflow_pk, db_alias),
        db_alias,
        deadlocks=False,
        retrying=retrying,
    )
    if outcome is None:
        return False
    failed, cancelled, unattributed = outcome
    logger.info(
        "Workflow %s complete (%d failed, %d cancelled, %d unattributed)",
        workflow_pk,
        failed,
        cancelled,
        unattributed,
        extra={
            "event": "workflow_completed",
            "workflow_id": str(workflow_pk),
            "failed_count": failed,
            "cancelled_count": cancelled,
            "unattributed_count": unattributed,
        },
    )
    return True


def _complete_once(
    workflow_pk: uuid.UUID, db_alias: str
) -> tuple[int, int, int] | None:
    """
    The completing transaction: (failed, cancelled, unattributed) when this
    call completed the workflow, None when it did not.

    The lock order is the workflow row, then task rows. The CAS takes the
    workflow row. Then the FAILED and LOST node rows a plain read finds are
    locked by primary key in key order, and each must still be in the status
    and at the epoch that read saw. A retry that committed first fails that
    check, and one that comes later waits for this transaction and lands on a
    COMPLETE workflow, the case that waits for django-ox's retry seam. Then the
    nodes are classified again. A pending node, a record to repair, or a FAILED
    or LOST row that is not one of the rows locked rolls the completion back.
    The last is a node that was retried and claimed before the read and failed
    again after it: under READ COMMITTED the classification sees it settled,
    and nothing holds it against another retry.

    What the completion counted, its callback ids and when are written into
    completions under this generation, once, and never rewritten.
    """
    locks = connections[db_alias].features.has_select_for_update
    try:
        with transaction.atomic(using=db_alias):
            # The callbacks are enqueued once because of this rowcount:
            # racing reconcilers all attempt the same UPDATE, one matches
            # a SEALED row, and the losers' transactions change nothing.
            # Each enqueued task then runs like any other, which is at
            # least once. Never check-then-act.
            completed_at = timezone.now()
            won = (
                OxpullWorkflow.objects.using(db_alias)
                .filter(pk=workflow_pk, status=OxpullWorkflow.Status.SEALED)
                .update(
                    status=OxpullWorkflow.Status.COMPLETE, completed_at=completed_at
                )
            )
            if not won:
                return None
            dead = {
                uuid.UUID(str(pk)): (status, int(epoch))
                for pk, status, epoch in OxTask.objects.using(db_alias)
                .filter(
                    id__in=_node_task_ids(workflow_pk, db_alias),
                    status__in=RETRYABLE_DEAD_STATUSES,
                )
                .values_list("pk", "status", "lease_epoch")
            }
            if locks:
                for chunk in _id_chunks(dead, db_alias):
                    for pk, status, epoch in (
                        OxTask.objects.using(db_alias)
                        .filter(pk__in=chunk)
                        .order_by("pk")
                        .select_for_update()
                        .values_list("pk", "status", "lease_epoch")
                    ):
                        if dead[uuid.UUID(str(pk))] != (status, int(epoch)):
                            # Retried since the read. The next tick decides.
                            raise _NodePending
            classes = _classify(workflow_pk, db_alias)
            # A FAILED or LOST row the classification counts that is not one
            # of the rows locked above settled after that read, so a retry of
            # it would not wait for this commit. The next tick decides.
            if classes.pending or classes.repair or classes.dead_rows != len(dead):
                raise _NodePending
            row = OxpullWorkflow.objects.using(db_alias).get(pk=workflow_pk)
            failed = classes.failed
            cancelled = classes.discarded_rows
            fields: dict[str, Any] = {
                "failed_count": failed,
                "cancelled_count": cancelled,
            }
            total = row.total_count
            if total is None:
                total = _sealed_without_a_total(
                    OxpullWorkflowNode.objects.using(db_alias).filter(
                        workflow_id=workflow_pk
                    )
                )
                fields["total_count"] = total
            unattributed = max(total - (classes.succeeded + failed), 0)
            if row.on_complete_path:
                fields["callback_task_id"] = _enqueue_callback(
                    row.on_complete_path,
                    row.on_complete_args,
                    row.on_complete_kwargs,
                    label="Workflow",
                )
            # An outcome nobody recorded is not a success, and neither is
            # a cancelled branch: on_success is the operator's "the run
            # was clean" signal. A cancelled node is DISCARDED, which is
            # unattributed, so the shortfall below already withholds it
            # and there is no second rule here.
            if failed == 0 and unattributed == 0 and row.on_success_path:
                fields["success_task_id"] = _enqueue_callback(
                    row.on_success_path,
                    row.on_success_args,
                    row.on_success_kwargs,
                    label="Workflow",
                )
            completions = dict(row.completions or {})
            generation = str(row.generation)
            if generation not in completions:
                completions[generation] = {
                    "total": total,
                    "succeeded": classes.succeeded,
                    "failed": failed,
                    "cancelled": cancelled,
                    "unattributed": unattributed,
                    "callback_task_id": _id_text(fields.get("callback_task_id")),
                    "success_task_id": _id_text(fields.get("success_task_id")),
                    "completed_at": completed_at.isoformat(),
                }
                fields["completions"] = completions
            OxpullWorkflow.objects.using(db_alias).filter(pk=workflow_pk).update(
                **fields
            )
            # A complete workflow has nothing left for the backstop to visit.
            OxpullWorkflowScan.objects.using(db_alias).filter(
                workflow_id=workflow_pk
            ).delete()
    except _NodePending:
        return None
    return failed, cancelled, unattributed


def _id_text(value: Any) -> str | None:
    return None if value is None else str(value)


# -- retention: what oxpull_prune deletes ---------------------------------------

# The task row statuses no retry moves. A node whose row is in one of these, or
# whose row is gone, and whose outcome is recorded, can have its node and the
# edges into it deleted.
RETIRABLE_STATUSES = (OxTask.Status.SUCCESSFUL, OxTask.Status.DISCARDED)


@dataclass(frozen=True)
class RetiredRows:
    """What one oxpull_prune pass over one workflow did."""

    nodes: int = 0
    edges: int = 0
    # True when a node was still pending, so nothing was deleted.
    pending: bool = False


@dataclass(frozen=True)
class RetainedRows:
    """
    Node and edge rows of workflows oxpull_prune has begun to retire and still
    keeps.

    They are the nodes a retry can still move, their rows FAILED or LOST, the
    nodes whose outcome nothing recorded before their row was deleted, and the
    edges into both. A number that only grows means rows that ox_prune
    --include-failed has not removed, or workflows with a node still pending.
    """

    workflows: int
    nodes: int
    edges: int


def retained_rows() -> RetainedRows:
    """
    Count the node and edge rows oxpull_prune keeps, for monitoring. Three
    plain counts on the database oxpull_db_alias() names.
    """
    db_alias = oxpull_db_alias()
    retired = OxpullWorkflow.objects.using(db_alias).filter(
        metadata_retired_at__isnull=False
    )
    ids = retired.values("pk")
    return RetainedRows(
        workflows=retired.count(),
        nodes=OxpullWorkflowNode.objects.using(db_alias)
        .filter(workflow_id__in=ids)
        .count(),
        edges=OxpullWorkflowEdge.objects.using(db_alias)
        .filter(workflow_id__in=ids)
        .count(),
    )


def retirable_workflows(cutoff: Any, db_alias: str) -> list[uuid.UUID]:
    """
    The COMPLETE workflows that completed before `cutoff` and that
    retire_workflow can still act on, in id order.

    Every such workflow not yet stamped is one. A stamped one counts only
    while a node of it is pending, has a record to repair, or can be deleted
    (_prune_can_act_q). So a workflow whose node rows are all gone is left out,
    and so is one that keeps only recorded FAILED or LOST nodes and nodes gone
    with no record. Workflow rows are never deleted, and without this a
    scheduled run would visit every workflow that ever completed. A retry of a
    kept node, or ox_prune deleting its row, makes the workflow count again.

    Plain reads decide. One reads the workflows not yet stamped. Another reads
    the stamped workflows that still have a node row. Then one read per chunk
    of those reads their nodes, with a task row probed by key for each node. So
    the reads grow with the workflows past the cutoff and with the nodes those
    workflows keep.

    The two workflow reads stay apart. Joined by OR, the node read is a
    subquery PostgreSQL can only hash. Once the node table is too large for
    work_mem to hold that hash, PostgreSQL reads the whole node table again for
    each stamped workflow.
    """
    past = OxpullWorkflow.objects.using(db_alias).filter(
        status=OxpullWorkflow.Status.COMPLETE, completed_at__lt=cutoff
    )
    unstamped = {
        uuid.UUID(str(pk))
        for pk in past.filter(metadata_retired_at__isnull=True).values_list(
            "pk", flat=True
        )
    }
    stamped = sorted(
        uuid.UUID(str(pk))
        for pk in past.filter(metadata_retired_at__isnull=False)
        .filter(
            Exists(
                OxpullWorkflowNode.objects.using(db_alias).filter(
                    workflow_id=OuterRef("pk")
                )
            )
        )
        .values_list("pk", flat=True)
    )
    acting: set[uuid.UUID] = set()
    for chunk in _id_chunks(stamped, db_alias):
        acting.update(
            uuid.UUID(str(pk))
            for pk in _with_rows(
                OxpullWorkflowNode.objects.using(db_alias).filter(workflow_id__in=chunk)
            )
            .filter(_prune_can_act_q())
            .order_by()
            .values_list("workflow_id", flat=True)
            .distinct()
        )
    # A workflow another run stamps between the two reads can be in both.
    return sorted(unstamped | acting)


def _prune_can_act_q() -> Q:
    """
    A node of a stamped workflow that retire_workflow can act on (_with_rows).
    Its row is gone and its outcome recorded, or its row is in any status but
    FAILED and LOST, which covers pending and deletable rows. Or its row is
    FAILED or LOST and its record is one completion would repair.

    Left out: a node gone with no record, and a FAILED or LOST node recorded as
    its row says. Completion classifies nodes the same way (_with_classes).
    """
    kept = RETRYABLE_DEAD_STATUSES
    return (
        Q(row_status__isnull=True, outcome__isnull=False)
        | (Q(row_status__isnull=False) & ~Q(row_status__in=kept))
        | (Q(row_status__in=kept) & _row_taken_q())
    )


def retire_workflow(
    workflow_pk: uuid.UUID, db_alias: str, *, chunk_size: int = ID_CHUNK_SIZE
) -> RetiredRows:
    """
    Delete the node and edge rows of one COMPLETE workflow that nothing can move
    again. oxpull_prune calls it for each workflow retirable_workflows returns.

    1. The stamp. metadata_retired_at is set under the workflow row's lock, once,
       and it stays set. Nothing that revives a node may touch a stamped
       workflow.
    2. The nodes are classified as completion classifies them. If any is
       pending, such as a node an operator retried after completion, nothing is
       deleted and the workflow is tried again on the next run.
    3. Records behind their task rows are repaired (_repair_records), which
       also marks the edges out of each node the repair finds succeeded.
    4. The deletes, chunk by chunk (_retire_chunk). A node goes, with the edges
       into it, only when its outcome is recorded and its task row is
       SUCCESSFUL, DISCARDED or gone, which no retry can move. A node whose row
       is FAILED or LOST stays with the edges into it while the row exists, so
       a retry of it is still gated. It goes once ox_prune --include-failed
       deletes the row. A node with no record is never deleted. The edges out
       of a successful node that goes, into a node that stays, are marked
       satisfied in the same transaction, because the node's record is the
       proof of that success once its row is pruned. The nodes those marks
       point at are released after the commit.

    The workflow row stays, and so do batch members, which this never reads.
    """
    if not _stamp_retired(workflow_pk, db_alias):
        return RetiredRows()
    if _classify(workflow_pk, db_alias).pending:
        return RetiredRows(pending=True)
    _repair_records(workflow_pk, db_alias)

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        _log_reissue(workflow_pk, "a prune delete", conflict, attempt, bound)

    size = max(1, min(chunk_size, _id_chunk_size(db_alias)))
    nodes = edges = 0
    after = 0
    while True:
        chunk = list(
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(workflow_id=workflow_pk, outcome__isnull=False, pk__gt=after)
            .order_by("pk")
            .values_list("pk", "task_id", "outcome")[:size]
        )
        if not chunk:
            break
        after = chunk[-1][0]
        deleted_nodes, deleted_edges, marked = _reissue(
            partial(_retire_chunk, workflow_pk, chunk, db_alias, size=size),
            db_alias,
            deadlocks=True,
            retrying=retrying,
        )
        nodes += deleted_nodes
        edges += deleted_edges
        if marked:
            release_downstream(marked, db_alias)
    return RetiredRows(nodes=nodes, edges=edges)


def retirable_rows(workflow_pk: uuid.UUID, db_alias: str) -> RetiredRows:
    """
    What retire_workflow would delete from this workflow now, by plain reads,
    stamping nothing. A node whose row is SUCCESSFUL or DISCARDED counts even
    before its record is written, because retire_workflow repairs records
    first.
    """
    if _classify(workflow_pk, db_alias).pending:
        return RetiredRows(pending=True)
    going = _classified_nodes(workflow_pk, db_alias).filter(
        Q(row_status__in=RETIRABLE_STATUSES)
        | Q(row_status__isnull=True, outcome__isnull=False)
    )
    return RetiredRows(
        nodes=going.count(),
        edges=OxpullWorkflowEdge.objects.using(db_alias)
        .filter(
            workflow_id=workflow_pk,
            downstream_task_id__in=going.values("task_id"),
        )
        .count(),
    )


def _stamp_retired(workflow_pk: uuid.UUID, db_alias: str) -> bool:
    """
    Set metadata_retired_at on a COMPLETE workflow if it is not set, under the
    workflow row's lock. True when the workflow is COMPLETE and stamped.

    A stamp that loses a deadlock or a serialization failure runs its
    transaction again when it owns it. At PostgreSQL REPEATABLE READ a stamp
    that waited on another oxpull_prune stamping the same workflow raises the
    serialization failure once that one commits, and the stamp run again finds
    the workflow stamped.
    """

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        _log_reissue(workflow_pk, "the prune stamp", conflict, attempt, bound)

    return _reissue(
        partial(_stamp_retired_once, workflow_pk, db_alias),
        db_alias,
        deadlocks=True,
        retrying=retrying,
    )


def _stamp_retired_once(workflow_pk: uuid.UUID, db_alias: str) -> bool:
    """One attempt of _stamp_retired."""
    with transaction.atomic(using=db_alias):
        rows = OxpullWorkflow.objects.using(db_alias).filter(
            pk=workflow_pk, status=OxpullWorkflow.Status.COMPLETE
        )
        if rows.filter(metadata_retired_at__isnull=True).update(
            metadata_retired_at=timezone.now()
        ):
            return True
        locked = rows.filter(metadata_retired_at__isnull=False)
        if connections[db_alias].features.has_select_for_update:
            locked = locked.select_for_update()
        return locked.exists()


def _retire_chunk(
    workflow_pk: uuid.UUID,
    chunk: list[tuple[int, Any, str | None]],
    db_alias: str,
    *,
    size: int | None = None,
) -> tuple[int, int, set[uuid.UUID]]:
    """
    One transaction of retire_workflow's deletes, over recorded nodes. Returns
    (nodes deleted, edges deleted, the nodes of the edges it marked). Each
    edge DELETE names at most `size` rows, and at most what one statement on
    this database carries.

    The locks follow every workflow operation's order: the workflow row, then
    the chunk's task rows by primary key in key order, then node rows, then
    edge rows, each in key order. Under the task row lock each node's row is
    read again, and only a row that is SUCCESSFUL, DISCARDED or gone lets its
    node go. So a row a revival moved between the classification and this
    transaction keeps its node. The node DELETE repeats outcome IS NOT NULL.

    The edges out of a node that goes and succeeded, not yet marked and not
    going with it, are marked satisfied here. The node's record is the proof of
    its success once its row is pruned, and it is deleted in this transaction.
    """
    locks = connections[db_alias].features.has_select_for_update
    with transaction.atomic(using=db_alias):
        workflow = OxpullWorkflow.objects.using(db_alias).filter(
            pk=workflow_pk, metadata_retired_at__isnull=False
        )
        if locks:
            if not workflow.select_for_update().exists():
                return 0, 0, set()
        elif not workflow.update(metadata_retired_at=F("metadata_retired_at")):
            return 0, 0, set()
        by_task = {
            uuid.UUID(str(task_id)): (pk, outcome) for pk, task_id, outcome in chunk
        }
        present = sorted(
            uuid.UUID(str(pk))
            for pk in OxTask.objects.using(db_alias)
            .filter(pk__in=sorted(by_task))
            .values_list("pk", flat=True)
        )
        # Only the rows the plain read found are locked: a pruned row does not
        # come back, and a locking read of a missing key would lock a gap on
        # MySQL.
        rows = OxTask.objects.using(db_alias).filter(pk__in=present).order_by("pk")
        if locks:
            rows = rows.select_for_update()
        statuses = {
            uuid.UUID(str(pk)): status
            for pk, status in rows.values_list("pk", "status")
        }
        going: dict[int, uuid.UUID] = {}
        succeeded: set[uuid.UUID] = set()
        for task_id, (node_pk, outcome) in by_task.items():
            status = statuses.get(task_id)
            if status is None or status in RETIRABLE_STATUSES:
                going[node_pk] = task_id
                if status == OxTask.Status.SUCCESSFUL or (
                    status is None and outcome == OxTask.Status.SUCCESSFUL
                ):
                    succeeded.add(task_id)
        if not going:
            return 0, 0, set()
        _lock_in_key_order(OxpullWorkflowNode, going, db_alias)
        # Under the node lock, keep only the nodes this transaction will really
        # delete. The node DELETE below takes recorded nodes, so an unrecorded
        # one stays; computing the edges from anything wider would delete the
        # edges into a node that stays, and leave it with nothing gating it.
        # That is the harm the fail-closed gate exists to prevent, so both
        # DELETEs read from the same set.
        recorded = set(
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(pk__in=sorted(going), outcome__isnull=False)
            .values_list("pk", flat=True)
        )
        going = {pk: task_id for pk, task_id in going.items() if pk in recorded}
        if not going:
            return 0, 0, set()
        succeeded &= set(going.values())
        into: dict[int, Any] = dict(
            OxpullWorkflowEdge.objects.using(db_alias)
            .filter(
                workflow_id=workflow_pk, downstream_task_id__in=sorted(going.values())
            )
            .values_list("pk", "downstream_task_id")
        )
        out: dict[int, Any] = {}
        for ids in _id_chunks(succeeded, db_alias):
            out.update(
                (pk, downstream)
                for pk, downstream in OxpullWorkflowEdge.objects.using(db_alias)
                .filter(
                    workflow_id=workflow_pk, satisfied=False, upstream_task_id__in=ids
                )
                .values_list("pk", "downstream_task_id")
                if pk not in into
            )
        _lock_in_key_order(OxpullWorkflowEdge, {*into, *out}, db_alias)
        deleted_nodes = (
            OxpullWorkflowNode.objects.using(db_alias)
            .filter(pk__in=sorted(going), outcome__isnull=False)
            .delete()[0]
        )
        # going is already the recorded set, so the filter above removes
        # nothing more. It stays as the statement's own guard.
        deleted_edges = 0
        step = min(ID_CHUNK_SIZE if size is None else size, _id_chunk_size(db_alias))
        ordered = sorted(into)
        for start in range(0, len(ordered), step):
            ids = ordered[start : start + step]
            deleted_edges += (
                OxpullWorkflowEdge.objects.using(db_alias)
                .filter(pk__in=ids)
                .delete()[0]
            )
        if out:
            _mark_satisfied(out, db_alias)
        return deleted_nodes, deleted_edges, {uuid.UUID(str(d)) for d in out.values()}


def clean_scan_rows(db_alias: str, *, chunk_size: int = ID_CHUNK_SIZE) -> int:
    """
    Delete the backstop's scan rows of workflows that are COMPLETE or gone.
    Returns how many. The backstop visits only OPEN and SEALED workflows, so
    these rows hold nothing it reads again. Each DELETE names at most
    `chunk_size` rows, and at most what one statement on this database carries.

    Plain reads decide, and each DELETE names scan rows by key: the scan rows
    first, then, per chunk, their workflows' statuses, so a workflow a scan row
    names is already committed when its status is read. A chunk opens no
    transaction for its read and its DELETE. A chunk that loses a deadlock or a
    serialization failure reads the statuses again and runs again when the call
    owns its transaction. At PostgreSQL REPEATABLE READ a DELETE that waited on
    the skip queue's compare-and-set of the same row raises the serialization
    failure once that commits.

    No transaction holds the read open for the DELETE. It would guard nothing,
    because only a status read under the workflow row's lock could, and on
    SQLite it breaks the DELETE. A connection that has read inside a
    transaction cannot take the write lock while another connection is
    writing, and SQLite refuses it at once with "database is locked".
    """

    def retrying(conflict: str, attempt: int, bound: int) -> None:
        _log_reissue(None, "the scan row cleanup", conflict, attempt, bound)

    scans = OxpullWorkflowScan.objects.using(db_alias)
    ordered = sorted(
        uuid.UUID(str(pk)) for pk in scans.values_list("workflow_id", flat=True)
    )
    step = max(1, min(chunk_size, _id_chunk_size(db_alias)))
    deleted = 0
    for start in range(0, len(ordered), step):
        ids = ordered[start : start + step]
        deleted += _reissue(
            partial(_clean_scan_chunk, ids, db_alias),
            db_alias,
            deadlocks=True,
            retrying=retrying,
        )
    return deleted


def _clean_scan_chunk(ids: list[uuid.UUID], db_alias: str) -> int:
    """One attempt of clean_scan_rows over one chunk. It opens no transaction."""
    live = {
        uuid.UUID(str(pk))
        for pk in OxpullWorkflow.objects.using(db_alias)
        .filter(
            pk__in=ids,
            status__in=(OxpullWorkflow.Status.OPEN, OxpullWorkflow.Status.SEALED),
        )
        .values_list("pk", flat=True)
    }
    done = [pk for pk in ids if pk not in live]
    if not done:
        return 0
    return int(
        OxpullWorkflowScan.objects.using(db_alias)
        .filter(workflow_id__in=done)
        .delete()[0]
    )


# The task row stores f"{func.__module__}.{func.__qualname__}", and both
# the schedule and the worker resolve the reconciler by that path, so the
# function carries its public name however it is spelled above.
_reconcile.__name__ = _reconcile.__qualname__ = "reconcile"


def __getattr__(name: str) -> Any:
    """Resolve the module's lazily built public names."""
    if name == "reconcile":
        return cast("Task[[], int]", _build_task_on_a_queue(_reconcile))
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
