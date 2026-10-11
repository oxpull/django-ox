"""
Workflow calls inside a caller's transaction, and the transactions a call owns.

A call owns its transaction when the connection is in autocommit and no atomic
block is open. Then add(), seal(), the cancellation and completion run their
transaction again after a deadlock (InnoDB's 1213, PostgreSQL's 40P01) or a
serialization failure where each says so. Inside a caller's transaction,
whether the caller opened it with atomic or by turning autocommit off, the
server rolled back the caller's work too, so the error propagates and the
caller runs its own transaction again.

A caller that touches several workflows in one transaction takes them in
ascending id order. Two callers that add to the same two workflows in opposite
orders each hold one workflow row and wait for the other's, and the database
ends one of them with a deadlock.

Shortcut: the simulated conflicts raise from Workflow._take_for_add, the first
statement of add()'s transaction, with the error code the database would send.
"""

from __future__ import annotations

import contextlib
import threading
import time
from typing import Any

import pytest
from django.db import OperationalError, connection, connections, transaction

from oxpull import workflows
from oxpull.models import OxpullWorkflowNode

from . import tasks

pytestmark = pytest.mark.django_db(transaction=True)

needs_row_locks = pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason="SQLite has no row locks, and its writers serialise",
)


class SimulatedDriverError(Exception):
    """The driver error a conflict carries as its cause."""


def conflict(code: Any) -> OperationalError:
    if connection.vendor == "mysql":
        return OperationalError(code, "simulated")
    cause = SimulatedDriverError("simulated")
    cause.sqlstate = code  # type: ignore[attr-defined]
    error = OperationalError("simulated")
    error.__cause__ = cause
    return error


DEADLOCK_CODE = {"postgresql": "40P01", "mysql": 1213}


def code_of(error: OperationalError) -> Any:
    if connection.vendor == "mysql":
        return error.args[0] if error.args else None
    return getattr(error.__cause__, "sqlstate", None)


SERIALIZATION_CODE = {"postgresql": "40001", "mysql": 1213}


def losing_the_first_take(monkeypatch, code: Any) -> list[int]:
    """Make add()'s first attempt lose `code` at its first statement."""
    real = workflows.Workflow._take_for_add
    calls: list[int] = []

    def take_for_add(self, db_alias):
        calls.append(1)
        if len(calls) == 1:
            raise conflict(code)
        return real(self, db_alias)

    monkeypatch.setattr(workflows.Workflow, "_take_for_add", take_for_add)
    return calls


@needs_row_locks
def test_an_add_that_owns_its_transaction_runs_it_again_after_a_deadlock(monkeypatch):
    flow = workflows.create("owned-deadlock")
    calls = losing_the_first_take(monkeypatch, DEADLOCK_CODE[connection.vendor])

    flow.add("a", tasks.step, "a")

    assert len(calls) == 2
    assert list(flow.node_ids()) == ["a"]


@needs_row_locks
@pytest.mark.parametrize("kind", ["deadlock", "serialization failure"])
def test_an_add_in_a_transaction_opened_by_turning_autocommit_off_does_not_run_it_again(
    monkeypatch, kind
):
    flow = workflows.create("autocommit-off")
    codes = DEADLOCK_CODE if kind == "deadlock" else SERIALIZATION_CODE
    calls = losing_the_first_take(monkeypatch, codes[connection.vendor])

    connection.set_autocommit(False)
    try:
        with pytest.raises(OperationalError):
            flow.add("a", tasks.step, "a")
    finally:
        connection.rollback()
        connection.set_autocommit(True)

    assert len(calls) == 1
    assert flow.node_ids() == {}


@needs_row_locks
def test_an_add_in_an_atomic_block_does_not_run_it_again(monkeypatch):
    flow = workflows.create("atomic")
    calls = losing_the_first_take(monkeypatch, DEADLOCK_CODE[connection.vendor])

    with pytest.raises(OperationalError), transaction.atomic():
        flow.add("a", tasks.step, "a")

    assert len(calls) == 1
    assert flow.node_ids() == {}


def _run_callers(flows, orders, *, meet):
    """
    Two caller transactions, "one" and "two", each adding one node to two of
    `flows` in the order given. With `meet`, each waits after its first add()
    until the other has made its own. Without it, "two" starts once "one" has
    made its first add(), and "one" waits half a second before its second, so
    "two" is already waiting on the lock. Returns what each raised, or None.
    """
    both_started = threading.Barrier(2, timeout=30)
    one_added = threading.Event()
    outcome: dict[str, BaseException | None] = {}

    def caller(label, names):
        try:
            with transaction.atomic():
                first, second = (flows[name] for name in names)
                first.add(f"{label}-first", tasks.step, label)
                if meet:
                    with contextlib.suppress(threading.BrokenBarrierError):
                        both_started.wait()
                elif label == "one":
                    one_added.set()
                    time.sleep(0.5)
                second.add(f"{label}-second", tasks.step, label)
            outcome[label] = None
        except BaseException as exc:
            outcome[label] = exc
            both_started.abort()
            one_added.set()
        finally:
            connections.close_all()

    one = threading.Thread(target=caller, args=("one", orders[0]))
    two = threading.Thread(target=caller, args=("two", orders[1]))
    one.start()
    if not meet:
        assert one_added.wait(30)
    two.start()
    for thread in (one, two):
        thread.join(120)
        assert not thread.is_alive()
    return outcome


def two_workflows():
    flows = {name: workflows.create(name) for name in ("w1", "w2")}
    ascending = tuple(flow.name for flow in sorted(flows.values(), key=lambda f: f.id))
    return flows, ascending


@needs_row_locks
def test_callers_taking_two_workflows_in_opposite_orders_deadlock_and_nothing_retries():
    """
    The contract's reason. One caller is ended by the deadlock detector, the
    error reaches it, and nothing of its transaction is left. The other
    commits both its nodes.
    """
    flows, ascending = two_workflows()
    outcome = _run_callers(flows, (ascending, ascending[::-1]), meet=True)

    raised = {label: exc for label, exc in outcome.items() if exc is not None}
    assert len(raised) == 1, outcome
    ((loser, error),) = raised.items()
    assert isinstance(error, OperationalError), error
    assert code_of(error) == DEADLOCK_CODE[connection.vendor], error
    winner = "two" if loser == "one" else "one"
    names = set(OxpullWorkflowNode.objects.values_list("name", flat=True))
    assert names == {f"{winner}-first", f"{winner}-second"}, names


@needs_row_locks
def test_callers_that_take_two_workflows_in_ascending_id_order_both_commit():
    flows, ascending = two_workflows()
    outcome = _run_callers(flows, (ascending, ascending), meet=False)

    assert outcome == {"one": None, "two": None}, outcome
    assert OxpullWorkflowNode.objects.count() == 4
