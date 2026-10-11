"""
What oxpull writes on django-ox's tables.

The package's promise, as its backend module and README are to state it:
oxpull adds no table, column, index, constraint or migration to a django-ox
table, and it writes django-ox task rows in exactly four ways, which it
names.

- **Cancellation.** A READY or WAITING node whose upstream failed or was
  discarded is set to DISCARDED. The UPDATE matches status READY or WAITING
  and the lease epoch the reconciler read, and writes only columns
  django_ox.actions.discard writes.
- **The waiting insert.** A node with dependencies is inserted WAITING
  through django_ox._waiting.enqueue, the row django-ox's own enqueue
  would insert in every other column.
- **Release.** A WAITING node moves to READY, matched on status WAITING and
  the lease epoch the release read, and its run_after becomes the later of
  now and the time its caller asked for. Never NULL, and nothing else.
- **Revival.** Workflow.retry() puts a cancelled node back to WAITING with a
  new lease epoch. Only columns django_ox.actions.retry writes. A FAILED
  upstream goes back through django-ox's own retry or through the same
  READY write, so a revival may leave run_after NULL the way that retry
  does, but only on a node nothing blocks.

Any other statement oxpull issues against a django-ox table is a write the
package does not document, and fails here: another column, another status,
a discard that is not epoch-checked, a release not pinned to its epoch, a
run_after cleared to NULL, another INSERT, a DELETE, a table lock, or DDL.

How. Every statement on every connection is captured and attributed to the
package whose code issued it: the innermost stack frame in either package,
with one exception. django-ox code that oxpull code calls issues oxpull's
statements, unless oxpull reached it from the class that subclasses it:
OxpullBackend's enqueue calling django-ox's, or OxpullWorker's claim and
outcome methods calling Worker's. So django-ox's enqueue INSERT, claim
UPDATE and outcome UPDATE are django-ox's, even when the claim carries an
oxpull conjunct. On PostgreSQL OxpullWorker runs a copy of django-ox's
single-statement claim with one read column added, the node flag. That
statement counts as django-ox's only when its text is django-ox's statement
byte for byte once the flag is taken out. A QuerySet.update() called from
oxpull/workflows.py is oxpull's, and so are the INSERT and UPDATEs of
django_ox._waiting that oxpull code calls, and the UPDATE of a
django_ox.actions.discard or retry that oxpull code calls: a discard made
through django-ox's operator API is held to the same READY-only,
epoch-checked shape as one oxpull writes itself. Each write is then checked
twice: as a statement (which columns it sets, what its WHERE clause
matches), and as a change to the node rows (which columns moved, and to
what).

The tests marked xfail fail on this branch for the reason each gives. The
tests named test_control_* pass: the inventory tells the documented shapes
from every other shape in this database's own SQL, and add, the gated
claim, oxpull_prune, the released unique and batch paths and every oxpull
migration write nothing undocumented. Every test prints the statements it
saw against a django-ox table, so the log is the inventory per database.

Known limits. The revival test calls Workflow.retry() with no arguments,
the workflow-wide form: the method does not exist yet and its signature is
not settled. If it lands taking a required argument, the call raises
TypeError, which the marker does not accept, so the test fails loudly and
the call needs adapting. The documentation test reads the backend module's
docstring and README.md, not the pages under docs/.
"""

from __future__ import annotations

import re
import threading
import time
import traceback
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from django.apps import apps
from django.core.management import call_command
from django.db import DatabaseError, connection, connections, transaction
from django.db.models import Case, DateTimeField, F, Value, When
from django.utils import timezone

import django_ox
import oxpull
import oxpull.backend
from django_ox.models import OxTask
from oxpull import batches, workflows
from oxpull.models import (
    OxpullBatch,
    OxpullWorkflow,
    OxpullWorkflowEdge,
    OxpullWorkflowNode,
)
from oxpull.routing import oxpull_db_alias

from . import tasks
from .test_upgrade import _latest_migration
from .test_workflows import drain, flow_worker, free_worker, reconcile
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

OXPULL_DIR = Path(oxpull.__file__).resolve().parent
OX_DIR = Path(django_ox.__file__).resolve().parent
TASK_TABLE = OxTask._meta.db_table
EDGE_TABLE = OxpullWorkflowEdge._meta.db_table
OX_TABLES = frozenset(
    model._meta.db_table for model in apps.get_app_config("django_ox").get_models()
)
PREVIOUS_RELEASE_MIGRATION = "0003_oxpullratewindow"
MIGRATIONS = sorted(
    path.stem for path in (OXPULL_DIR / "migrations").glob("[0-9][0-9][0-9][0-9]_*.py")
)

READY = OxTask.Status.READY
WAITING = OxTask.Status.WAITING
DISCARDED = OxTask.Status.DISCARDED
FAILED = OxTask.Status.FAILED
SUCCESSFUL = OxTask.Status.SUCCESSFUL


def _column(name: str) -> str:
    return OxTask._meta.get_field(name).column


STATUS = _column("status")
RUN_AFTER = _column("run_after")
LEASE_EPOCH = _column("lease_epoch")

# What django_ox.actions.discard writes. A cancelled node looks like an
# operator discard in django-ox's admin, and writes nothing more.
DISCARD_COLUMNS = frozenset(
    _column(name)
    for name in ("status", "finished_at", "locked_by", "locked_at", "lease_expires_at")
)
# What django_ox.actions.retry writes: status, a new lease_epoch,
# max_attempts, run_after and the lock and finish fields.
REVIVE_COLUMNS = frozenset(
    _column(name)
    for name in (
        "status",
        "lease_epoch",
        "max_attempts",
        "run_after",
        "finished_at",
        "locked_by",
        "locked_at",
        "lease_expires_at",
    )
)

CANCELLATION = "cancellation"
HOLD = "waiting insert"
RELEASE = "release"
REVIVAL = "revival"
REVIVAL_UNBLOCKED = "revival of a node nothing blocks"

PK = OxTask._meta.pk.column

# A release writes the later of now and the requested time. "Now" is the
# database's clock or the worker's; either is within minutes of the test's.
CLOCK_SLACK = timedelta(minutes=5)

_NAME = r'[`"]?(\w+)[`"]?'
_WRITE_TARGETS = tuple(
    re.compile(pattern, re.IGNORECASE | re.DOTALL)
    for pattern in (
        rf"^\s*UPDATE\s+{_NAME}",
        rf"^\s*DELETE\s+(?:FROM\s+)?{_NAME}",
        rf"^\s*(?:INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+{_NAME}",
        rf"^\s*(?:ALTER|DROP|TRUNCATE)\s+TABLE\s+(?:IF\s+EXISTS\s+)?{_NAME}",
        rf"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{_NAME}",
        (
            r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
            rf'(?:IF\s+NOT\s+EXISTS\s+)?[`"]?\w+[`"]?\s+ON\s+{_NAME}'
        ),
        rf"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+.*?\bON\s+{_NAME}",
        rf"^\s*COMMENT\s+ON\s+(?:TABLE|COLUMN)\s+{_NAME}",
        rf"^\s*LOCK\s+TABLES?\s+{_NAME}",
    )
)
_SCHEMA_CHANGE = re.compile(
    r"^\s*(?:ALTER|CREATE|DROP|TRUNCATE|COMMENT)\b", re.IGNORECASE
)


# -- the instrument ----------------------------------------------------


# django-ox modules oxpull extends by subclassing, each from the oxpull
# module of the same name: OxpullWorker(Worker) and OxpullBackend(OxBackend).
SUBCLASSED = frozenset({"worker.py", "backend.py"})


def _issuer() -> str:
    """
    Who issued this statement: the innermost frame in oxpull or django_ox,
    with one exception. django-ox code called from oxpull code issues the
    statement on oxpull's behalf, so it is oxpull's, unless oxpull called
    it from the subclass of that django-ox class: django_ox/worker.py
    reached from oxpull/worker.py, or django_ox/backend.py from
    oxpull/backend.py.
    """
    innermost_ox = None
    nearest_ox_path = None
    for frame in reversed(traceback.extract_stack()):
        path = Path(frame.filename).resolve()
        if path.is_relative_to(OXPULL_DIR):
            innermost_oxpull = f"oxpull/{path.relative_to(OXPULL_DIR)}:{frame.name}"
            if innermost_ox is None:
                return innermost_oxpull
            if (
                nearest_ox_path.parent == OX_DIR
                and path.parent == OXPULL_DIR
                and nearest_ox_path.name == path.name
                and path.name in SUBCLASSED
            ):
                return innermost_ox
            return f"{innermost_oxpull} via {innermost_ox}"
        if path.is_relative_to(OX_DIR):
            if innermost_ox is None:
                innermost_ox = f"django_ox/{path.relative_to(OX_DIR)}:{frame.name}"
            nearest_ox_path = path
    return innermost_ox or "test"


@dataclass(frozen=True)
class Statement:
    sql: str
    params: Any
    issuer: str
    thread: str
    many: bool = False

    @property
    def write_target(self) -> str | None:
        """The table a write or DDL statement acts on, or None for a read."""
        for pattern in _WRITE_TARGETS:
            match = pattern.match(self.sql)
            if match:
                return match.group(1)
        return None

    @property
    def is_schema_change(self) -> bool:
        return self.write_target is not None and bool(_SCHEMA_CHANGE.match(self.sql))

    @property
    def locking_read_target(self) -> str | None:
        """The outer FROM table of a SELECT ... FOR UPDATE, or None."""
        if not re.match(r"^\s*SELECT\b", self.sql, re.IGNORECASE):
            return None
        if "FOR UPDATE" not in self.sql.upper():
            return None
        match = re.search(rf"\bFROM\s+{_NAME}", self.sql, re.IGNORECASE)
        return match.group(1) if match else None

    @property
    def by_oxpull(self) -> bool:
        return self.issuer.startswith("oxpull/")

    def names(self, table: str) -> bool:
        return re.search(rf"\b{re.escape(table)}\b", self.sql) is not None


# OxpullWorker's copy of django-ox's single-statement PostgreSQL claim.
COPIED_CLAIM = "oxpull/worker.py:_claim_one_postgresql"


def is_django_ox_claim_with_the_node_flag(sql: str) -> bool:
    """
    Is this django-ox's POSTGRES_CLAIM_SQL, with the node flag after RETURNING *
    and nothing else changed?

    The queue and extra clauses are django-ox's own hook points and may hold
    anything. Every other byte must be django-ox's.
    """
    from django_ox.worker import POSTGRES_CLAIM_SQL

    flag = workflows.node_flag_sql()
    if sql.count(flag) != 1 or not sql.rstrip().endswith(f"RETURNING *{flag}"):
        return False
    unflagged = sql.replace(flag, "")
    for lease_clock in ("STATEMENT_TIMESTAMP()", "%(lease_now)s"):
        template = re.escape(
            POSTGRES_CLAIM_SQL.format(
                table=TASK_TABLE,
                queue_clause="\x00QUEUE\x00",
                extra_clause="\x00EXTRA\x00",
                lease_clock=lease_clock,
            )
        )
        pattern = template.replace(re.escape("\x00QUEUE\x00"), "(.*?)").replace(
            re.escape("\x00EXTRA\x00"), "(.*?)"
        )
        if re.fullmatch(pattern, unflagged, re.DOTALL):
            return True
    return False


@contextmanager
def statements_on():
    """
    Record every statement this thread runs on any database alias, with
    its issuer.

    Connections are thread-local, so a worker thread opens its own capture
    inside the thread.
    """
    seen: list[Statement] = []

    def wrapper(execute, sql, params, many, context):
        issuer = _issuer()
        if issuer == COPIED_CLAIM and is_django_ox_claim_with_the_node_flag(sql):
            issuer = f"django_ox/worker.py:_claim_one_postgresql, copied as {issuer}"
        seen.append(
            Statement(sql, params, issuer, threading.current_thread().name, many)
        )
        return execute(sql, params, many, context)

    with ExitStack() as stack:
        for alias in connections:
            stack.enter_context(connections[alias].execute_wrapper(wrapper))
        yield seen


# -- reading an UPDATE -------------------------------------------------


class _Expression:
    """A SET or WHERE operand that is SQL rather than one bound value."""

    def __repr__(self) -> str:
        return "<expression>"


EXPRESSION = _Expression()


def _scan(text: str):
    """(index, depth) of every character outside a quoted string or name."""
    depth = 0
    quote = ""
    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = ""
            continue
        if char in "'\"`":
            quote = char
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        yield index, depth


def _placeholders(text: str) -> int:
    return sum(1 for match in re.finditer(r"%%|%s", text) if match.group() == "%s")


def _split_top_level(text: str) -> list[str]:
    parts, start = [], 0
    for index, depth in _scan(text):
        if depth == 0 and text[index] == ",":
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return parts


def _top_level_where(text: str) -> int | None:
    for index, depth in _scan(text):
        if (
            depth == 0
            and text[index] in "Ww"
            and re.match(r"WHERE\b", text[index:], re.IGNORECASE)
            and (index == 0 or not re.match(r"\w", text[index - 1]))
        ):
            return index
    return None


def _parenthesised(text: str) -> str:
    """The inside of the parenthesis `text` starts with."""
    for index, depth in _scan(text):
        if depth == 0 and text[index] == ")":
            return text[1:index]
    return text[1:]


@dataclass(frozen=True)
class Assignment:
    column: str
    expression: str
    params: tuple[Any, ...]

    @property
    def value(self) -> Any:
        """The bound or literal value, None for NULL, or EXPRESSION."""
        text = self.expression.strip()
        if text == "%s" and len(self.params) == 1:
            return self.params[0]
        if text.upper() == "NULL":
            return None
        literal = re.fullmatch(r"'([^']*)'", text)
        if literal:
            return literal.group(1)
        return EXPRESSION


@dataclass(frozen=True)
class Update:
    table: str
    sets: dict[str, Assignment]
    where: str
    where_params: tuple[Any, ...]

    def where_values(self, column: str) -> list[Any]:
        """
        What the WHERE clause compares this table's `column` to with = or
        IN, in order. A subquery or a column reference is EXPRESSION.
        """
        pattern = re.compile(
            rf'(?:[`"]{re.escape(self.table)}[`"]\.|(?<![.\w`"]))'
            rf'[`"]{re.escape(column)}[`"]\s*(?:=|\bIN\b)\s*',
            re.IGNORECASE,
        )
        values: list[Any] = []
        for match in pattern.finditer(self.where):
            rest = self.where[match.end() :]
            offset = _placeholders(self.where[: match.end()])
            if rest.startswith("%s"):
                values.extend(self.where_params[offset : offset + 1] or [EXPRESSION])
            elif rest.startswith("("):
                inner = _parenthesised(rest)
                count = _placeholders(inner)
                if re.match(r"\s*SELECT\b", inner, re.IGNORECASE) or not count:
                    values.append(EXPRESSION)
                else:
                    bound = self.where_params[offset : offset + count]
                    values.extend(bound if len(bound) == count else [EXPRESSION])
            elif re.match(r"'([^']*)'", rest):
                values.append(re.match(r"'([^']*)'", rest).group(1))
            else:
                values.append(EXPRESSION)
        return values

    def matches_a_value_of(self, column: str) -> bool:
        """Does the WHERE clause pin `column` to a bound value, alone or in a row?"""
        if any(value is not EXPRESSION for value in self.where_values(column)):
            return True
        return bool(
            re.search(
                rf'[`"]{re.escape(column)}[`"]\s*\)\s*IN\s*\(\s*\(',
                self.where,
                re.IGNORECASE,
            )
        )


_UPDATE_HEAD = re.compile(rf"^\s*UPDATE\s+{_NAME}\s+SET\s+", re.IGNORECASE)


def parse_update(sql: str, params: Any) -> Update | None:
    head = _UPDATE_HEAD.match(sql)
    if head is None:
        return None
    body = sql[head.end() :]
    cut = _top_level_where(body)
    set_text, where = (body, "") if cut is None else (body[:cut], body[cut + 5 :])
    bound = tuple(params or ())
    used = 0
    sets: dict[str, Assignment] = {}
    for part in _split_top_level(set_text):
        match = re.match(r'^\s*[`"]?(\w+)[`"]?\s*=\s*(.*?)\s*$', part, re.DOTALL)
        if match is None:
            return None
        count = _placeholders(match.group(2))
        sets[match.group(1)] = Assignment(
            match.group(1), match.group(2), bound[used : used + count]
        )
        used += count
    return Update(head.group(1), sets, where, bound[used:])


def _instant(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


# -- the inventory -----------------------------------------------------


def _epoch_pinned(update: Update) -> bool:
    """Does the WHERE clause pin the lease epoch, by value or per row by CASE?"""
    if update.matches_a_value_of(LEASE_EPOCH):
        return True
    return bool(
        re.search(rf'[`"]{re.escape(LEASE_EPOCH)}[`"]\s*=\s*CASE\b', update.where, re.I)
    )


def classify(statement: Statement) -> tuple[str | None, str]:
    """
    Which documented write this statement is, or None and why it is none
    of them.
    """
    target = statement.write_target
    if statement.is_schema_change:
        return None, f"changes the schema of {target}"
    verb = statement.sql.split(None, 1)[0].upper()
    if verb == "INSERT" and target == TASK_TABLE:
        values = statement.params if isinstance(statement.params, (list, tuple)) else ()
        if "django_ox/_waiting.py:enqueue" in statement.issuer and WAITING in values:
            return HOLD, "inserts a node born WAITING through django_ox._waiting"
        return None, (
            "an INSERT into the task table that is not a node born WAITING "
            "through django_ox._waiting"
        )
    update = None if statement.many else parse_update(statement.sql, statement.params)
    if update is None:
        if verb == "UPDATE":
            return None, "an UPDATE whose SET clause this inventory cannot read"
        return None, (
            f"a {verb} on {target}; every documented write is the waiting insert "
            "or an UPDATE of existing task rows"
        )
    if update.table != TASK_TABLE:
        return None, f"an UPDATE of {update.table}, not of the task table"

    columns = frozenset(update.sets)
    status = update.sets.get(STATUS)
    guards = update.where_values(STATUS)
    if status is None:
        return None, f"writes {sorted(columns)}, which no documented write sets"

    if status.value == DISCARDED:
        outside = sorted(columns - DISCARD_COLUMNS)
        if outside:
            return None, (
                f"discards and also writes {outside}, which "
                "django_ox.actions.discard never writes"
            )
        if not guards or any(guard not in (READY, WAITING) for guard in guards):
            return None, (
                "discards a row without matching status READY or WAITING alone"
            )
        if not _epoch_pinned(update):
            return None, (
                "discards by id and status without matching the lease epoch "
                "the decision was made on"
            )
        return (
            CANCELLATION,
            "discards a READY or WAITING row at the lease epoch it read",
        )

    if status.value == READY and guards and all(guard == WAITING for guard in guards):
        if columns != {STATUS, RUN_AFTER}:
            return None, (
                f"releases a WAITING row writing {sorted(columns)}, where a "
                "release writes status and run_after and nothing else"
            )
        if update.sets[RUN_AFTER].value is None:
            return None, (
                "releases a WAITING row with run_after NULL, where a release "
                "writes the later of now and the time the caller asked for"
            )
        if not _epoch_pinned(update):
            return None, (
                "releases a WAITING row without matching the lease epoch the "
                "release decided on"
            )
        return RELEASE, "moves a WAITING row to READY at the lease epoch it read"

    if status.value == READY:
        outside = sorted(columns - REVIVE_COLUMNS)
        if outside:
            return None, (
                f"puts a row back to READY and also writes {outside}, which "
                "django_ox.actions.retry never writes"
            )
        if LEASE_EPOCH not in columns:
            return None, "puts a row back to READY without a new lease epoch"
        return REVIVAL_UNBLOCKED, (
            "puts a row back to READY with a new lease epoch, which a worker "
            "without the dependency gate claims at once, so it is documented "
            "only on a node nothing blocks"
        )

    if status.value == WAITING:
        outside = sorted(columns - REVIVE_COLUMNS)
        if outside:
            return None, (
                f"revives a row and also writes {outside}, which "
                "django_ox.actions.retry never writes"
            )
        if not guards or any(guard != DISCARDED for guard in guards):
            return None, "moves a row to WAITING from something other than DISCARDED"
        if LEASE_EPOCH not in columns:
            return None, "revives a row without a new lease epoch"
        return REVIVAL, "revives a DISCARDED row to WAITING with a new lease epoch"

    return None, (
        f"writes status {status.value!r}; the documented writes set only "
        "DISCARDED, READY and WAITING"
    )


def undocumented(statements: list[Statement]) -> list[tuple[Statement, str]]:
    """oxpull's writes, and anyone's schema changes, on a django-ox table
    that are none of the documented writes."""
    found = []
    for statement in statements:
        if statement.write_target not in OX_TABLES:
            continue
        if not (statement.by_oxpull or statement.is_schema_change):
            continue
        kind, reason = classify(statement)
        if kind is None:
            found.append((statement, reason))
        elif kind == REVIVAL_UNBLOCKED:
            problem = _revived_while_blocked(statement)
            if problem:
                found.append((statement, problem))
    return found


def _revived_while_blocked(statement: Statement) -> str | None:
    """
    A revival to READY is documented only on rows nothing blocks. Read which
    rows its WHERE clause names by id, and whether any of them has an
    upstream that is absent or not SUCCESSFUL, now.
    """
    update = parse_update(statement.sql, statement.params)
    ids = update.where_values(PK) if update else []
    if not ids or any(value is EXPRESSION for value in ids):
        return (
            "puts rows back to READY without naming them by id, so nothing "
            "shows that no upstream blocks them"
        )
    succeeded = OxTask.objects.filter(status=SUCCESSFUL).values("pk")
    blocked = sorted(
        str(value)
        for value in ids
        if OxpullWorkflowEdge.objects.filter(downstream_task_id=value)
        .exclude(upstream_task_id__in=succeeded)
        .exists()
    )
    if not blocked:
        return None
    return (
        f"puts {blocked} back to READY while an upstream of each has not "
        "succeeded, so a worker without the dependency gate claims it at once; "
        "a blocked node is revived WAITING"
    )


def oxpull_writes_to_django_ox(statements: list[Statement]) -> list[Statement]:
    return [s for s in statements if s.by_oxpull and s.write_target in OX_TABLES]


def _one_line(sql: str, limit: int = 600) -> str:
    flat = " ".join(sql.split())
    return flat if len(flat) <= limit else flat[:limit] + " ..."


def _params(params: Any, limit: int = 240) -> str:
    text = repr(params)
    return text if len(text) <= limit else text[:limit] + " ..."


def _emit(text: str) -> None:
    """Write to the test's stdout, which -rA carries into the log."""
    print(text)  # noqa: T201


def inventory(title: str, statements: list[Statement]) -> None:
    """Print every statement that writes, locks or reads a django-ox table."""
    _emit(f"\n=== {title} [{connection.vendor}] ===")
    shown = 0
    for s in statements:
        if s.write_target in OX_TABLES:
            if s.by_oxpull or s.is_schema_change:
                documented, reason = classify(s)
                offences = undocumented([s])
                if offences:
                    documented, reason = None, offences[0][1]
                kind = f"WRITE {documented or 'UNDOCUMENTED'}"
            else:
                kind, reason = "WRITE", ""
        elif s.locking_read_target in OX_TABLES:
            kind, reason = "LOCKING READ", ""
        elif s.write_target is not None and any(s.names(t) for t in OX_TABLES):
            kind, reason = f"write {s.write_target}, reads ox", ""
        elif any(s.names(table) for table in OX_TABLES):
            kind, reason = "read", ""
        else:
            continue
        shown += 1
        owner = "OXPULL" if s.by_oxpull else s.issuer.split("/", 1)[0]
        _emit(f"- {kind:<20} by {owner:<9} {s.issuer}  [{s.thread}]")
        if reason:
            _emit(f"    why:    {reason}")
        _emit(f"    sql:    {_one_line(s.sql)}")
        _emit(f"    params: {_params(s.params)}")
    _emit(
        f"({shown} statement(s) touching a django-ox table, "
        f"of {len(statements)} captured)"
    )


def documented_problem(statements: list[Statement], where: str) -> str | None:
    offenders = undocumented(statements)
    if not offenders:
        return None
    return (
        f"{where}: {len(offenders)} write(s) to a django-ox table on "
        f"{connection.vendor} that are not among the writes oxpull documents:\n"
        + "\n".join(
            f"  {s.issuer}: {reason}\n"
            f"    {_one_line(s.sql, 300)}  params={_params(s.params, 160)}"
            for s, reason in offenders
        )
    )


def assert_only_documented_writes(statements: list[Statement], where: str) -> None:
    problem = documented_problem(statements, where)
    assert problem is None, problem


def assert_no_oxpull_writes(statements: list[Statement], where: str) -> None:
    """For the paths no documented write belongs to at all."""
    offenders = oxpull_writes_to_django_ox(statements)
    assert offenders == [], (
        f"{where}: oxpull issued {len(offenders)} write(s) to a django-ox table "
        f"on {connection.vendor}, and no documented write belongs on this path:\n"
        + "\n".join(
            f"  {s.issuer}: {_one_line(s.sql, 300)}  params={_params(s.params, 160)}"
            for s in offenders
        )
    )


# -- rows --------------------------------------------------------------


def row(label: str) -> OxTask:
    return OxTask.objects.get(args=[label])


def snapshot(*labels: str) -> dict[str, dict[str, Any]]:
    return {
        label: OxTask.objects.filter(args=[label]).values().get() for label in labels
    }


def assert_changed_only(before, after, label: str, allowed, write: str) -> None:
    changed = sorted(f for f, v in after[label].items() if before[label][f] != v)
    outside = [f for f in changed if f not in allowed]
    assert not outside, (
        f"{write} changed {outside} on the {label!r} node's task row, which the "
        f"documented write never touches (it may change only {sorted(allowed)})"
    )


def released_problem(after, label: str, requested, started, finished) -> str | None:
    """run_after is the later of now and the time the caller asked for."""
    value = after[label]["run_after"]
    if value is None:
        return (
            f"the release left the {label!r} node's run_after NULL, not the later "
            "of now and the time its caller asked for"
        )
    if requested is None:
        if not started - CLOCK_SLACK <= value <= finished + CLOCK_SLACK:
            return (
                f"the {label!r} node was released to {value}, not to now "
                f"(between {started} and {finished})"
            )
    elif value != requested:
        return (
            f"the {label!r} node was released to {value}, losing the run_after "
            f"{requested} its caller asked for"
        )
    return None


def release_problems(seen, where, after, held, later, started, finished):
    """What the documented waiting insert and release require of the children."""
    problems = [
        documented_problem(seen, where),
        released_problem(after, "child", None, started, finished),
    ]
    if held != WAITING:
        problems.append(
            f"the 'scheduled' node, whose caller asked for run_after {later}, "
            f"was {held} when it was added, not WAITING, so while its upstream "
            "was unfinished nothing in its row held it back once that time came"
        )
    else:
        problems.append(released_problem(after, "scheduled", later, started, finished))
    return [problem for problem in problems if problem]


def failed_root_and_waiting_child(name: str):
    """root -> child on the gating backend, with root FAILED for good."""
    flow = workflows.create(name)
    flow.add("root", tasks.failing_step, "root")
    flow.add("child", tasks.step, "child", depends_on="root")
    flow.seal()
    worker = flow_worker()
    for _ in range(3):
        assert worker.run_once() is True
    assert row("root").status == FAILED
    assert row("child").status == WAITING
    return flow, worker


def parent_and_waiting_children(name: str) -> tuple[datetime, Any]:
    """
    parent -> {child, scheduled} on the second workflow backend. `scheduled`
    carries a run_after its caller chose, an hour out. Returns that time,
    and the status `scheduled` has once added, WAITING when the waiting
    insert covers a node given its own time.
    """
    later = timezone.now() + timedelta(hours=1)
    flow = workflows.create(name)
    flow.add("parent", tasks.deferred_step, "parent")
    flow.add("child", tasks.deferred_step, "child", depends_on="parent")
    flow.add(
        "scheduled",
        tasks.deferred_step.using(run_after=later),
        "scheduled",
        depends_on="parent",
    )
    flow.seal()
    assert row("child").status == WAITING
    return later, row("scheduled").status


# -- the documented writes, on the Workflows runtime paths -------------


def test_the_reconciler_cancels_a_doomed_node_with_only_the_documented_discard():
    """
    A node fails for good; the reconciler, run by a real worker as a
    scheduled task, cancels the node downstream of it.

    Production sequence: a sealed workflow root -> child, the root
    exhausts its attempts, the once-a-minute reconcile schedule fires and
    a worker runs it. The only departure is that the reconcile task is
    enqueued here rather than by the schedule dispatcher.
    """
    assert oxpull_db_alias() == "default"
    _flow, worker = failed_root_and_waiting_child("doomed")
    before = snapshot("child")

    tick = workflows.reconcile.enqueue()
    with statements_on() as seen:
        assert worker.run_once() is True  # claims and runs the reconciler
    after = snapshot("child")

    # The run provoked what it is meant to observe.
    assert OxTask.objects.get(id=tick.id).status == SUCCESSFUL
    assert after["child"]["status"] == DISCARDED

    inventory("reconciler cancels a doomed node (worker runs reconcile)", seen)
    assert_changed_only(before, after, "child", DISCARD_COLUMNS, "the cancellation")
    assert_only_documented_writes(seen, "the reconciler's cancellation")


# Upper bound on the placements below: far more statements than one
# reconciler tick issues for a two-node workflow.
MAX_PLACEMENTS = 400


@dataclass(frozen=True)
class Placement:
    before_statement: int
    statements: int
    claimed: bool
    new_epoch: int
    first_discard: str | None
    child_after_it: tuple[Any, ...] | None
    child_at_the_end: tuple[Any, ...]


def _claimed_then_failed_back(n: int) -> Placement | None:
    """
    One sealed workflow root -> child with root FAILED for good, and one
    reconciler tick run by a worker, with the child turned into a new
    incarnation in two steps on the reconciler's own connection.

    Before the n-th statement of the tick, a worker claims the child if it
    is still READY or WAITING, as a release and a claim together can:
    RUNNING, lease epoch + 1, one more attempt. Just before the first
    statement after that which sets DISCARDED on the task table, the attempt
    fails back to READY under the same new epoch, as a failed attempt with
    retries left does. Returns None when the tick issued fewer than n
    statements.
    """
    _flow, worker = failed_root_and_waiting_child(f"stale-discard-{n}")
    child = row("child")
    state: dict[str, Any] = {
        "count": 0,
        "claimable": None,
        "claimed": False,
        "busy": False,
        "first_discard": None,
        "after_it": None,
    }

    def place(execute, sql, params, many, context):
        if state["busy"]:
            return execute(sql, params, many, context)
        state["busy"] = True
        try:
            state["count"] += 1
            if state["count"] == n:
                now = timezone.now()
                state["claimed"] = state["claimable"] = bool(
                    OxTask.objects.filter(
                        pk=child.pk, status__in=[READY, WAITING]
                    ).update(
                        status=OxTask.Status.RUNNING,
                        lease_epoch=F("lease_epoch") + 1,
                        attempts=F("attempts") + 1,
                        locked_by="another-worker",
                        locked_at=now,
                        started_at=now,
                        lease_expires_at=now + timedelta(minutes=5),
                    )
                )
            update = None if many else parse_update(sql, params)
            sets_discarded = (
                update is not None
                and update.table == TASK_TABLE
                and STATUS in update.sets
                and update.sets[STATUS].value == DISCARDED
            )
            if state["first_discard"] or not (state["claimed"] and sets_discarded):
                return execute(sql, params, many, context)
            OxTask.objects.filter(pk=child.pk).update(
                status=READY, locked_by=None, locked_at=None, lease_expires_at=None
            )
            state["first_discard"] = _one_line(sql, 300)
            result = execute(sql, params, many, context)
            state["after_it"] = tuple(
                OxTask.objects.filter(pk=child.pk)
                .values_list("status", "lease_epoch")
                .get()
            )
            return result
        finally:
            state["busy"] = False

    workflows.reconcile.enqueue()
    with connection.execute_wrapper(place):
        ran = worker.run_once()
    if ran is not True:
        pytest.fail(f"setup: the worker ran nothing at placement {n}")
    if state["count"] < n:
        return None
    return Placement(
        before_statement=n,
        statements=state["count"],
        claimed=state["claimable"],
        new_epoch=child.lease_epoch + 1,
        first_discard=state["first_discard"],
        child_after_it=state["after_it"],
        child_at_the_end=tuple(
            OxTask.objects.filter(pk=child.pk)
            .values_list("status", "lease_epoch")
            .get()
        ),
    )


def test_a_discard_decided_on_one_lease_epoch_does_not_land_on_the_next():
    """
    Between the reconciler deciding to cancel a node and its write, the
    node becomes a new incarnation: a worker claims it (RUNNING, lease
    epoch + 1) and the attempt fails back to READY under that new epoch.
    That incarnation is not the one the decision was about, and the
    discard must not land on it.

    Where the decision falls among a tick's statements depends on the
    implementation, so the claim is placed before each statement of the
    tick in turn: one fresh workflow and one tick per placement, rolled
    back afterwards, until a tick runs out of statements first. The
    fail-back to READY comes just before the first statement after the
    claim that sets DISCARDED on the task table. Both changes are made on
    the reconciler's own connection, so it sees them whatever transaction
    it holds.

    Required at every placement where the claim took (the node was still
    READY): that DISCARDED statement leaves the node READY at the new
    epoch, and a claimed node that no such statement followed is not
    DISCARDED at its new epoch at the end. What is asserted is that
    statement's own effect; a later statement that decides again on the
    new epoch is not ruled out. A placement after the discard finds the
    node no longer READY and claims nothing, and one whose claim the
    reconciler's own rollback undoes ends at the old epoch; neither is
    judged. At least one placement must be followed by a DISCARDED
    statement, or nothing was provoked.

    A discard that binds the epoch it read with its decision passes every
    placement: placed before the decision, the claim shows the node RUNNING
    and it is not chosen; placed after it, the epoch no longer matches.
    Rejected: an UPDATE that matches no epoch, as today's does, at every
    placement before it; and an implementation that decides, then re-reads
    the epoch just before writing, at the placement between the two.
    """
    placements: list[Placement] = []
    for n in range(1, MAX_PLACEMENTS + 1):
        with transaction.atomic():
            placement = _claimed_then_failed_back(n)
            transaction.set_rollback(True)
        if placement is None:
            break
        placements.append(placement)
    else:
        pytest.fail(f"setup: a tick issued more than {MAX_PLACEMENTS} statements")

    report = "\n".join(
        f"  claim before statement {p.before_statement} of {p.statements} "
        f"({'took' if p.claimed else 'found the child no longer READY'}): "
        f"child after the first discard that followed: {p.child_after_it} "
        f"({p.first_discard or 'no discard followed'}); "
        f"at the end: {p.child_at_the_end}"
        for p in placements
    )
    _emit(f"\nplacements [{connection.vendor}]:\n{report}")
    provoked = [p for p in placements if p.child_after_it is not None]
    if not provoked:
        pytest.fail(
            "no placement was followed by a statement that sets DISCARDED, so "
            "nothing was provoked:\n" + report
        )
    if any(p.child_after_it[1] != p.new_epoch for p in provoked):
        pytest.fail("setup: the new incarnation's epoch did not take:\n" + report)
    landed = [p.before_statement for p in provoked if p.child_after_it[0] != READY]
    discarded_while_running = [
        p.before_statement
        for p in placements
        if p.claimed
        and p.child_after_it is None
        and p.child_at_the_end == (DISCARDED, p.new_epoch)
    ]
    assert (landed, discarded_while_running) == ([], []), (
        f"on {connection.vendor}, with the claim placed before statements "
        f"{landed}, the discard decided on the child's earlier epoch landed on "
        f"its new incarnation at epoch {provoked[0].new_epoch}; placements that "
        f"left it RUNNING and still discarded it: {discarded_while_running}\n"
        f"{report}"
    )


@pytest.mark.django_db(transaction=True)
def test_a_worker_that_finishes_a_parent_writes_only_the_documented_release():
    """
    A real worker on its own thread and connection runs the parent; its
    outcome write lands and the worker releases the children.
    Production sequence: exactly this, in a worker process.
    """
    later, held = parent_and_waiting_children("released")
    before = snapshot("child", "scheduled")
    seen: list[Statement] = []
    errors: list[BaseException] = []

    def run():
        try:
            worker = deferring_worker()
            with statements_on() as statements:
                ran = worker.run_once()
            seen.extend(statements)
            assert ran is True
        except BaseException as exc:
            errors.append(exc)
        finally:
            connections.close_all()

    started = timezone.now()
    thread = threading.Thread(target=run, name="oxpull-worker-thread")
    thread.start()
    thread.join(timeout=60)
    finished = timezone.now()
    assert not thread.is_alive()
    assert not errors, errors
    after = snapshot("child", "scheduled")

    assert row("parent").status == SUCCESSFUL
    assert after["child"]["status"] == READY, (
        "the worker released nothing, so nothing was provoked"
    )
    assert all(s.thread == "oxpull-worker-thread" for s in seen)

    inventory("worker thread writes SUCCESSFUL and releases the children", seen)
    for label in ("child", "scheduled"):
        assert_changed_only(before, after, label, {STATUS, RUN_AFTER}, "the release")
    problems = release_problems(
        seen, "the worker's release", after, held, later, started, finished
    )
    assert not problems, "\n".join(problems)


def test_the_reconcilers_release_writes_only_the_documented_release():
    """
    The backstop: a release no worker made is made by the reconciler.

    Production sequence: a worker without the release, django-ox's own
    Worker in a fleet still being upgraded, never claims the waiting
    children, runs the parent and writes SUCCESSFUL, and releases nothing. The next
    reconcile tick, run by an oxpull worker, releases the children.
    """
    later, held = parent_and_waiting_children("backstop")
    assert free_worker().run_once() is True  # the parent, and no release
    assert row("parent").status == SUCCESSFUL
    assert row("child").status == WAITING
    before = snapshot("child", "scheduled")

    workflows.reconcile.enqueue()
    started = timezone.now()
    with statements_on() as seen:
        assert deferring_worker().run_once() is True  # claims and runs reconcile
    finished = timezone.now()
    after = snapshot("child", "scheduled")

    assert after["child"]["status"] == READY, (
        "the reconciler released nothing, so nothing was provoked"
    )
    inventory("reconciler release after a parent no oxpull worker ran", seen)
    for label in ("child", "scheduled"):
        assert_changed_only(before, after, label, {STATUS, RUN_AFTER}, "the release")
    problems = release_problems(
        seen, "the reconciler's release", after, held, later, started, finished
    )
    assert not problems, "\n".join(problems)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "Workflow.retry() does not exist yet, so nothing puts a cancelled node "
        "back to WAITING with a new lease epoch, and django-ox's own retry "
        "never moves a DISCARDED row"
    ),
)
def test_workflow_retry_revives_a_cancelled_node_with_only_the_documented_write():
    """
    Production sequence: root fails for good, a reconcile tick cancels
    child, and the operator calls Workflow.retry().

    root, which nothing blocks, may come back READY with run_after NULL, the
    way django-ox's own retry leaves it; child, still blocked by root, must
    come back WAITING. The call takes no arguments, the
    workflow-wide form. A Workflow.retry() that requires one raises
    TypeError, which the marker does not accept, so this fails loudly and
    the call needs adapting.
    """
    flow, worker = failed_root_and_waiting_child("revived")
    workflows.reconcile.enqueue()
    assert worker.run_once() is True
    assert row("child").status == DISCARDED
    before = snapshot("root", "child")

    retry = getattr(workflows.get(flow.id), "retry", None)
    assert callable(retry), "Workflow.retry() does not exist"
    with statements_on() as seen:
        retry()
    after = snapshot("root", "child")

    assert after["child"]["status"] == WAITING, "Workflow.retry() revived nothing"
    inventory("Workflow.retry() revives a cancelled node", seen)
    for label in ("root", "child"):
        assert_changed_only(before, after, label, REVIVE_COLUMNS, "the revival")
    assert after["child"]["lease_epoch"] > before["child"]["lease_epoch"], (
        "the revived node kept its lease epoch, so a discard decided on its "
        "cancelled incarnation can still land on it"
    )
    assert_only_documented_writes(seen, "Workflow.retry()")


# The schema promise names all five. A sentence that names only these, and
# no rows or state, is about schema.
SCHEMA_NOUNS = ("table", "column", "index", "constraint", "migration")
_NEGATION = re.compile(r"\b(no|not|never|nothing|none|without|cannot)\b|n't\b", re.I)
_WRITE_VERB = re.compile(r"\b(writ|modif|touch|chang|alter|updat|mutat|edit)\w*", re.I)
_ROW_NOUN = re.compile(r"\b(rows?|state|data|records?|values?)\b", re.I)
_SCHEMA_NOUN = re.compile(
    r"\b(tables?|columns?|index(es)?|indices|constraints?|migrations?|schema)\b", re.I
)
_SUBJECT = re.compile(r"\b(oxpull|this module|this package|the package)\b", re.I)
_RESTRICTION = re.compile(r"\b(other|else|except|beyond|besides|only)\b", re.I)
_BROAD_DENIAL = re.compile(
    r"nothing (owned by|belonging to) django-ox|no django-ox (rows?|state|data)", re.I
)


def _paragraphs(text: str) -> list[str]:
    return [" ".join(block.split()) for block in re.split(r"\n\s*\n", text) if block]


def _sentences(paragraph: str) -> list[str]:
    return [s for s in re.split(r"(?<=[.!?])\s+(?=[A-Z`*(\[])", paragraph) if s]


def promises_no_schema_change(paragraphs: list[str]) -> bool:
    """One sentence: no table, column, index, constraint or migration on django-ox."""
    return any(
        "django-ox" in sentence.lower()
        and _NEGATION.search(sentence)
        and all(re.search(rf"\b{noun}", sentence, re.I) for noun in SCHEMA_NOUNS)
        for paragraph in paragraphs
        for sentence in _sentences(paragraph)
    )


def names_the_row_writes(paragraphs: list[str]) -> bool:
    """One paragraph naming DISCARDED, run_after, the release and revival."""
    return any(
        "DISCARDED" in paragraph
        and "run_after" in paragraph
        and re.search(r"releas", paragraph, re.I)
        and re.search(r"reviv|Workflow\.retry", paragraph, re.I)
        for paragraph in paragraphs
    )


def denials_of_row_writes(paragraphs: list[str]) -> list[str]:
    """
    Sentences saying oxpull does not write django-ox: "nothing owned by
    django-ox", or oxpull, this module or the package with a negation and a
    write verb, unless the sentence is only about schema or only restricts
    the writes to some ("no other", "except").
    """
    found = []
    for paragraph in paragraphs:
        for sentence in _sentences(paragraph):
            if "django-ox" not in sentence.lower():
                continue
            if _BROAD_DENIAL.search(sentence):
                found.append(sentence)
                continue
            if not (
                _SUBJECT.search(sentence)
                and _NEGATION.search(sentence)
                and _WRITE_VERB.search(sentence)
            ):
                continue
            only_schema = _SCHEMA_NOUN.search(sentence) and not _ROW_NOUN.search(
                sentence
            )
            if not (only_schema or _RESTRICTION.search(sentence)):
                found.append(sentence)
    return found


def test_the_package_names_its_row_writes_instead_of_denying_them():
    """
    The backend module's docstring and README.md must each:

    - state the schema promise in one sentence: no table, column, index,
      constraint or migration on a django-ox table;
    - name the row writes in one paragraph: DISCARDED, run_after, the
      release, and revival or Workflow.retry();
    - deny row writes nowhere. "Nothing owned by django-ox" is a denial, and
      so is a sentence in which oxpull, this module or the package does not
      write, modify, touch or change django-ox, unless it is only about
      schema or only rules out writes beyond the named ones.

    A denial reworded, or the writes named in one place while the other
    still denies them, fails here.
    """
    readme = (OXPULL_DIR.parent.parent / "README.md").read_text(encoding="utf-8")
    places = {
        "the backend module's docstring": oxpull.backend.__doc__ or "",
        "README.md": readme,
    }
    problems = []
    for where, text in places.items():
        paragraphs = _paragraphs(text)
        if not promises_no_schema_change(paragraphs):
            problems.append(
                f"{where} has no sentence promising no table, column, index, "
                "constraint or migration on a django-ox table"
            )
        if not names_the_row_writes(paragraphs):
            problems.append(
                f"{where} has no paragraph naming the DISCARDED, run_after, "
                "release and revival writes"
            )
        problems.extend(
            f"{where} still denies writing django-ox: {sentence!r}"
            for sentence in denials_of_row_writes(paragraphs)
        )
    assert not problems, "\n".join(problems)


# -- controls: the instrument, and the paths with nothing to hide (green) --


def test_control_the_inventory_accepts_the_documented_shapes_and_nothing_else():
    """
    The classifier on real statements from this database's own compiler,
    so an inventory that accepted everything, or nothing, shows here
    first. Every UPDATE matches no row, so nothing changes. One names a
    real node that is still blocked, so a revival to READY is shown to be
    caught by the rows it names.
    """
    flow = workflows.create("inventory-control")
    flow.add("up", tasks.step, "up")
    blocked = OxTask.objects.filter(
        pk=flow.add("down", tasks.step, "down", depends_on="up").id
    )
    task = OxTask.objects.filter(pk=uuid.uuid4())
    now = timezone.now()
    discard = {
        "status": DISCARDED,
        "finished_at": now,
        "locked_by": None,
        "locked_at": None,
        "lease_expires_at": None,
    }
    epoch_bump = F("lease_epoch") + 1
    later_of_now = Case(
        When(run_after__gt=now, then=F("run_after")),
        default=Value(now, output_field=DateTimeField()),
    )
    cases = [
        (
            "epoch-checked READY discard",
            CANCELLATION,
            lambda: task.filter(status=READY, lease_epoch=0).update(**discard),
        ),
        (
            "epoch-checked READY or WAITING discard",
            CANCELLATION,
            lambda: task.filter(status__in=[READY, WAITING], lease_epoch=0).update(
                **discard
            ),
        ),
        (
            "discard by id and status",
            None,
            lambda: task.filter(status=READY).update(**discard),
        ),
        (
            "discard of any status",
            None,
            lambda: task.filter(lease_epoch=0).update(**discard),
        ),
        (
            "discard of READY or FAILED",
            None,
            lambda: task.filter(status__in=[READY, FAILED], lease_epoch=0).update(
                **discard
            ),
        ),
        (
            "discard against its own epoch",
            None,
            lambda: task.filter(status=READY, lease_epoch=F("lease_epoch")).update(
                **discard
            ),
        ),
        (
            "discard writing errors",
            None,
            lambda: task.filter(status=READY, lease_epoch=0).update(
                errors=["cancelled"], **discard
            ),
        ),
        (
            "release pinned to its epoch",
            RELEASE,
            lambda: task.filter(status=WAITING, lease_epoch=0).update(
                status=READY, run_after=later_of_now
            ),
        ),
        (
            "release to a bound time",
            RELEASE,
            lambda: task.filter(status=WAITING, lease_epoch=0).update(
                status=READY, run_after=now
            ),
        ),
        (
            "release to NULL",
            None,
            lambda: task.filter(status=WAITING, lease_epoch=0).update(
                status=READY, run_after=None
            ),
        ),
        (
            "release without its epoch",
            None,
            lambda: task.filter(status=WAITING).update(
                status=READY, run_after=later_of_now
            ),
        ),
        (
            "release that leaves run_after alone",
            None,
            lambda: task.filter(status=WAITING, lease_epoch=0).update(status=READY),
        ),
        ("run_after moved off anything", None, lambda: task.update(run_after=now)),
        (
            "revival",
            REVIVAL,
            lambda: task.filter(status=DISCARDED).update(
                status=WAITING, lease_epoch=epoch_bump, finished_at=None
            ),
        ),
        (
            "revival to WAITING with no new epoch",
            None,
            lambda: task.filter(status=DISCARDED).update(status=WAITING),
        ),
        (
            "revival leaving run_after",
            REVIVAL_UNBLOCKED,
            lambda: task.filter(status=DISCARDED).update(
                status=READY, lease_epoch=epoch_bump
            ),
        ),
        (
            "revival clearing run_after",
            REVIVAL_UNBLOCKED,
            lambda: task.filter(status=DISCARDED).update(
                status=READY, run_after=None, lease_epoch=epoch_bump
            ),
        ),
        (
            "revival clearing a blocked node's run_after",
            REVIVAL_UNBLOCKED,
            lambda: blocked.filter(status=DISCARDED).update(
                status=READY, run_after=None, lease_epoch=epoch_bump
            ),
        ),
        (
            "revival clearing run_after, rows not named by id",
            REVIVAL_UNBLOCKED,
            lambda: OxTask.objects.filter(
                task_path="no.such.task", status=DISCARDED
            ).update(status=READY, run_after=None, lease_epoch=epoch_bump),
        ),
        (
            "revival with no new epoch",
            None,
            lambda: task.filter(status=DISCARDED).update(status=READY),
        ),
        (
            "revival resetting attempts",
            None,
            lambda: task.filter(status=DISCARDED).update(
                status=READY, lease_epoch=epoch_bump, attempts=0
            ),
        ),
        ("attempts", None, lambda: task.update(attempts=0)),
        ("FAILED", None, lambda: task.update(status=FAILED)),
        ("DELETE", None, lambda: task._raw_delete(connection.alias)),
    ]
    # Classified as a revival that leaves run_after NULL, and undocumented
    # all the same, because the rows it names are blocked or not named.
    undocumented_by_its_rows = {
        "revival clearing a blocked node's run_after",
        "revival clearing run_after, rows not named by id",
    }
    wrong = []
    for label, expected, run in cases:
        with statements_on() as seen:
            run()
        writes = [s for s in seen if s.write_target == TASK_TABLE]
        assert len(writes) == 1, (label, seen)
        as_oxpull = replace(writes[0], issuer="oxpull/control")
        kind, reason = classify(as_oxpull)
        offences = undocumented([as_oxpull])
        _emit(f"- {label:<48} -> {kind or 'UNDOCUMENTED'}: {reason}")
        if offences:
            _emit(f"    undocumented: {offences[0][1]}")
        _emit(
            f"    {_one_line(as_oxpull.sql, 300)}  params={_params(as_oxpull.params)}"
        )
        if kind != expected:
            wrong.append((label, expected, kind, reason, as_oxpull.sql))
        documented = expected is not None and label not in undocumented_by_its_rows
        if (offences == []) != documented:
            wrong.append((label, documented, offences, reason, as_oxpull.sql))

    quote = connection.ops.quote_name
    for sql in (
        f"CREATE INDEX {quote('ox_extra')} ON {quote(TASK_TABLE)} ({quote(RUN_AFTER)})",
        f"ALTER TABLE {quote(TASK_TABLE)} ADD COLUMN {quote('cause')} varchar(8)",
        " ".join(("INSERT INTO", quote(TASK_TABLE), f"({quote('id')}) VALUES (%s)")),
        f"LOCK TABLES {quote(TASK_TABLE)} WRITE",
    ):
        statement = Statement(sql, (), "oxpull/control", "MainThread")
        assert statement.write_target == TASK_TABLE, sql
        kind, reason = classify(statement)
        _emit(f"- {sql:<60} -> {kind or 'UNDOCUMENTED'}: {reason}")
        if kind is not None:
            wrong.append((sql, None, kind, reason, sql))
    # Anyone's DDL on a django-ox table counts, not only oxpull's.
    assert undocumented(
        [Statement(f"ALTER TABLE {quote(TASK_TABLE)} ADD x int", (), "test", "t")]
    )
    assert wrong == []


def test_control_adding_blocked_nodes_writes_only_the_waiting_insert():
    """
    add() of a blocked node, and of a blocked node whose caller chose its
    own run_after. Production sequence: exactly these calls.
    """
    later = timezone.now() + timedelta(hours=1)
    flow = workflows.create("deferred-add")
    flow.add("parent", tasks.deferred_step, "parent")
    with statements_on() as seen:
        flow.add("child", tasks.deferred_step, "child", depends_on="parent")
        flow.add(
            "scheduled",
            tasks.deferred_step.using(run_after=later),
            "scheduled",
            depends_on="parent",
        )

    assert row("child").status == WAITING
    assert any(
        s.write_target == TASK_TABLE and "django_ox/_waiting.py:enqueue" in s.issuer
        for s in seen
    ), "the waiting insert was not seen; the instrument is blind"

    inventory("add() of blocked nodes", seen)
    assert_only_documented_writes(seen, "add() of blocked nodes")


def test_control_adding_nodes_on_the_gating_backend_writes_nothing_undocumented():
    with statements_on() as seen:
        flow = workflows.create("plain")
        flow.add("a", tasks.step, "a")
        flow.add("b", tasks.step, "b", depends_on="a")
        flow.seal()

    inserts = [s for s in seen if s.write_target == TASK_TABLE]
    by_django_ox = [s for s in inserts if s.issuer.startswith("django_ox/")]
    waiting = [s for s in inserts if "django_ox/_waiting.py:enqueue" in s.issuer]
    assert (len(by_django_ox), len(waiting), len(inserts)) == (1, 1, 2), (
        "expected django-ox's enqueue INSERT for a and the waiting insert for b"
    )
    inventory("create/add/seal on the gating backend", seen)
    assert_only_documented_writes(seen, "create/add/seal on the gating backend")


def test_control_the_gated_claim_is_a_django_ox_write_carrying_an_oxpull_read():
    flow = workflows.create("claimed")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    with statements_on() as seen:
        claimed = flow_worker().claim_one()

    assert claimed is not None and claimed.args == ["a"]
    claim_writes = [s for s in seen if s.write_target == TASK_TABLE]
    assert claim_writes
    assert all(s.issuer.startswith("django_ox/") for s in claim_writes)
    # The gate reaches django-ox's own statement: the edge table and a
    # second reference to the task table sit inside it.
    assert any(s.issuer.startswith("django_ox/") and s.names(EDGE_TABLE) for s in seen)
    inventory("one gated claim", seen)
    assert_no_oxpull_writes(seen, "the gated claim")


def test_control_oxpull_prune_writes_no_django_ox_table():
    """
    Setup shortcut: completed_at is moved back 30 days, which stands in
    for waiting past --older-than. Everything else is the real worker,
    reconciler and management command.
    """
    flow = workflows.create("old")
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    assert drain(flow_worker()) == 2
    assert reconcile() == 1
    OxpullWorkflow.objects.filter(pk=flow.id).update(
        completed_at=timezone.now() - timedelta(days=30)
    )

    with statements_on() as seen:
        call_command("oxpull_prune", stdout=StringIO())

    assert OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count() == 0
    assert OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count() == 0
    assert OxTask.objects.count() == 2
    inventory("oxpull_prune", seen)
    assert [s for s in seen if s.write_target in OX_TABLES] == []


def test_control_the_released_unique_and_batch_paths_write_no_django_ox_row(worker):
    """
    1.2.0's features on the same instrument: a unique task whose stale
    lock is released by a second enqueue, and a batch completed by its
    reconciler. No documented write belongs on either path.
    """
    with statements_on() as seen:
        first = tasks.unique_echo.enqueue("key")
        assert worker.run_once() is True
        second = tasks.unique_echo.enqueue("key")
        batch = batches.create("released")
        batch.add(tasks.plain_echo, "member")
        batch.seal()
        assert drain(worker) == 2
        assert batches._reconcile() == 1

    assert second.id != first.id
    assert OxpullBatch.objects.get(pk=batch.id).status == OxpullBatch.Status.COMPLETE
    inventory("1.2.0 unique stale-lock release and batch completion", seen)
    assert_no_oxpull_writes(seen, "released unique and batch paths")


# transaction=True: SQLite's schema editor, which sqlmigrate drives, refuses
# to start inside the atomic block a plain django_db test runs in.
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("migration", MIGRATIONS)
def test_control_no_oxpull_migration_names_a_django_ox_table(migration):
    out = StringIO()
    call_command("sqlmigrate", "oxpull", migration, stdout=out)
    sql = out.getvalue()
    _emit(f"\n=== sqlmigrate oxpull {migration} [{connection.vendor}] ===\n{sql}")
    assert sql.strip()
    named = sorted(t for t in OX_TABLES if re.search(rf"\b{t}\b", sql))
    assert named == []


def _schema(tables):
    with connection.cursor() as cursor:
        return {
            table: (
                [
                    tuple(c)
                    for c in connection.introspection.get_table_description(
                        cursor, table
                    )
                ],
                connection.introspection.get_constraints(cursor, table),
            )
            for table in sorted(tables)
        }


@pytest.mark.django_db(transaction=True)
def test_control_migrating_from_the_last_release_changes_no_django_ox_table():
    """
    The real `migrate` command from the previous release's schema to the
    newest migration, with a django-ox task row present so a data
    migration would have something to touch, every statement captured,
    and the django-ox tables' columns, constraints and indexes
    introspected before and after.
    """
    latest = _latest_migration()
    call_command("migrate", "oxpull", PREVIOUS_RELEASE_MIGRATION, verbosity=0)
    try:
        assert EDGE_TABLE not in connection.introspection.table_names()
        OxTask.objects.create(
            task_path="tests_oxpull.tasks.plain_echo",
            backend_name="default",
            enqueued_at=timezone.now(),
        )
        before = _schema(OX_TABLES)
        with statements_on() as seen:
            call_command("migrate", "oxpull", latest, verbosity=0)
        after = _schema(OX_TABLES)
        assert EDGE_TABLE in connection.introspection.table_names()
    finally:
        call_command("migrate", "oxpull", latest, verbosity=0)

    _emit(
        f"\n=== migrate oxpull {PREVIOUS_RELEASE_MIGRATION} -> {latest} "
        f"[{connection.vendor}] ==="
    )
    ddl = [s for s in seen if s.write_target is not None]
    for s in ddl:
        _emit(f"- {s.write_target:<32} {_one_line(s.sql, 400)}")
    _emit(f"({len(ddl)} write/DDL statement(s) of {len(seen)} captured)")
    for table in sorted(OX_TABLES):
        _emit(f"{table}: {sorted(after[table][1])}")

    assert ddl, "the migrate command ran no DDL; the instrument saw nothing"
    assert [s for s in seen if s.write_target in OX_TABLES] == []
    assert [s for s in seen if any(s.names(t) for t in OX_TABLES)] == []
    assert before == after
    oxpull_tables = [
        name
        for name in connection.introspection.table_names()
        if name.startswith("oxpull_")
    ]
    foreign_keys = {
        (table, name): info["foreign_key"]
        for table, (_columns, constraints) in _schema(oxpull_tables).items()
        for name, info in constraints.items()
        if info["foreign_key"]
    }
    _emit(f"foreign keys on oxpull tables: {foreign_keys}")
    assert not [fk for fk in foreign_keys.values() if fk[0] in OX_TABLES]


@pytest.mark.django_db(transaction=True)
def test_control_migrating_from_the_last_release_waits_on_no_task_table_lock():
    """
    A second session holds the strongest lock the database has on
    django-ox's task table, as a long ALTER or LOCK on a big live table
    would. The migrating session gives up after three seconds of waiting.
    A probe that does read the table must time out (the lock is real);
    the migration must not.

    The second session keeps its lock until the migration has returned, so
    a statement of the migration that waits on it is never let through: it
    ends at the three-second timeout and the migration raises. That error
    is the whole verdict. How long the migration's own DDL takes is the
    server's speed, and is printed rather than judged.
    """
    vendor = connection.vendor
    if vendor not in ("postgresql", "mysql"):
        pytest.skip(
            "SQLite has one database-wide write lock and no table locks, so "
            "there is no per-table lock for a migration to wait on."
        )
    quoted = connection.ops.quote_name(TASK_TABLE)
    latest = _latest_migration()
    call_command("migrate", "oxpull", PREVIOUS_RELEASE_MIGRATION, verbosity=0)
    blocker = connection.copy()
    probe_error = None
    migrate_error = None
    seen: list[Statement] = []
    try:
        blocker.ensure_connection()
        if vendor == "postgresql":
            blocker.set_autocommit(False)
            with blocker.cursor() as cursor:
                cursor.execute(f"LOCK TABLE {quoted} IN ACCESS EXCLUSIVE MODE")
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '3s'")
        else:
            with blocker.cursor() as cursor:
                cursor.execute(f"LOCK TABLES {quoted} WRITE")
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION lock_wait_timeout = 3")

        started = time.monotonic()
        try:
            with connection.cursor() as cursor:
                # The name comes from the model and is quoted by the backend.
                cursor.execute(f"SELECT COUNT(*) FROM {quoted}")  # noqa: S608
        except DatabaseError as exc:
            probe_error = exc
        probe_seconds = time.monotonic() - started

        started = time.monotonic()
        try:
            with statements_on() as seen:
                call_command("migrate", "oxpull", latest, verbosity=0)
        except DatabaseError as exc:
            migrate_error = exc
        migrate_seconds = time.monotonic() - started
    finally:
        if vendor == "postgresql":
            blocker.rollback()
        else:
            with blocker.cursor() as cursor:
                cursor.execute("UNLOCK TABLES")
        blocker.close()
        with connection.cursor() as cursor:
            if vendor == "postgresql":
                cursor.execute("RESET lock_timeout")
            else:
                cursor.execute("SET SESSION lock_wait_timeout = DEFAULT")
        call_command("migrate", "oxpull", latest, verbosity=0)

    _emit(
        f"\n=== migrate under a held lock on {TASK_TABLE} [{vendor}] ===\n"
        f"probe SELECT on the locked table: {probe_seconds:.2f}s, "
        f"error={probe_error!r}\n"
        f"migrate {PREVIOUS_RELEASE_MIGRATION} -> {latest}: {migrate_seconds:.2f}s, "
        f"error={migrate_error!r}, {len(seen)} statement(s)"
    )
    assert probe_error is not None, "the probe did not wait: the lock is not real"
    assert migrate_error is None, f"migrate waited on the locked {TASK_TABLE}"
