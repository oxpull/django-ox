"""
A claim whose commit reached the database but whose reply never reached the
worker.

The worker claims a row, the database commits the claim, and the connection
drops before the worker hears so. The claim raises. Before the fix the poll
loop logged worker_poll_failed and moved on, and the row stayed RUNNING under
this worker's own id with an attempt charged, though nothing executed it and
nothing renewed its lease. Once the lease expired the reaper took it back: to
READY with the attempt spent, or, when that was the last attempt, to LOST,
though the body never ran. The 1.5.0 chaos runs saw both on MySQL.

The tests run the real Worker.run() loop on a thread, run_once() and
run_tasks() inline, and lose one reply, once, at a chosen point of the first
claim; tests/lost_reply.py says how. The "before" windows are the control:
the session ends before the claim commits, the server rolls it back, and the
row is left READY with no attempt charged.

The loop asserts the outcome: the task's body runs once, the row ends
SUCCESSFUL, and the attempts it records are the attempts that ran. The inline
calls raise the claim's error, which is theirs to raise, and the contract is
that the next call runs the released task.
"""

import logging

import pytest
from django.db import DatabaseError, close_old_connections, connections

from django_ox.models import OxTask
from django_ox.testing import run_tasks
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .lost_reply import WINDOWS, Seams

pytestmark = pytest.mark.django_db(transaction=True)

SETTLED = {OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED, OxTask.Status.LOST}

#: The loop's look happens on the pass after the failure, a poll interval
#: later. The lease is ten times anything that takes on a loaded machine, and
#: short enough that, without the look, the reaper reaches the stranded row
#: well inside the settle limit, so the defect shows as what it did.
LOCK_TIMEOUT = 10.0
SETTLE_LIMIT = 60.0

#: The inline cases never wait on a lease: nothing reaps.
INLINE_LOCK_TIMEOUT = 300.0

CLAIM_WINDOWS = list(WINDOWS["claim"])


@pytest.fixture(autouse=True)
def no_dead_connection_outlives_the_test():
    """
    A test here ends sessions, the test thread's own included. Before the
    database is flushed, drop any connection that no longer answers, so the
    next test does not inherit it, whatever the code under test did.
    """
    yield
    for conn in connections.all(initialized_only=True):
        if (
            conn.connection is not None
            and not conn.in_atomic_block
            and not conn.is_usable()
        ):
            conn.close()


@pytest.fixture
def lose_the_reply():
    seams = Seams()
    yield seams.arm
    seams.remove_all()


def _budget(settings, attempts):
    settings.TASKS = {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"MAX_ATTEMPTS": attempts},
        }
    }


def _runs():
    return tasks.STATE.get("order", []).count("lost-reply")


def _assert_the_seam_fired_where_intended(seam, worker):
    """The instrument first: the session ended on the intended side of the commit."""
    assert seam.fired, "the seam never fired, so this run provoked nothing"
    if seam.landed:
        assert seam.seen == [(OxTask.Status.RUNNING, worker.worker_id, 1, 1)], (
            f"the claim had not committed when the session ended: {seam.seen}"
        )
    else:
        assert seam.seen == [(OxTask.Status.READY, None, 0, 0)], (
            f"the claim had committed before the session ended: {seam.seen}"
        )


def _assert_it_ran_once_uncharged(seam, worker, result, told):
    """
    What the worker was told is reported rather than asserted: a worker that
    recognises its claim need not raise at all.
    """
    row = OxTask.objects.get(id=result.id)
    _assert_the_seam_fired_where_intended(seam, worker)
    assert row.status in SETTLED, f"not settled after {SETTLE_LIMIT}s: {row.status}"
    assert (row.status, row.attempts, _runs()) == (OxTask.Status.SUCCESSFUL, 1, 1), (
        f"a claim that landed while its reply was lost ended {row.status} after "
        f"{row.attempts} attempt(s) of {row.max_attempts}, and the body ran "
        f"{_runs()} time(s); the worker was told {told[:1] or 'nothing'}; errors "
        f"{[e['exception_class_path'] for e in row.errors]}"
    )
    assert row.worker_ids == [worker.worker_id]


def _settled(result):
    return OxTask.objects.get(id=result.id).status in SETTLED


def _released_records(caplog, worker):
    return [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "worker_claim_released"
        and getattr(r, "worker_id", None) == worker.worker_id
    ]


@pytest.mark.parametrize(
    "budget", [1, 2], ids=["on-the-final-attempt", "with-an-attempt-to-spare"]
)
@pytest.mark.parametrize("window", CLAIM_WINDOWS)
def test_run_a_claim_whose_reply_is_lost_runs_once_uncharged(
    window, budget, settings, caplog, lose_the_reply
):
    seam = lose_the_reply(window)
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, budget)
    worker = Worker(lock_timeout=LOCK_TIMEOUT, poll_interval=0.05, backoff_initial=0)
    result = tasks.record.enqueue("lost-reply")

    thread = start_worker_thread(worker)
    try:
        wait_for(lambda: _settled(result), timeout=SETTLE_LIMIT)
    finally:
        worker.request_stop()
        thread.join(timeout=60)

    told = [
        r.exc_info[1]
        for r in caplog.records
        if getattr(r, "event", None) == "worker_poll_failed"
        and getattr(r, "worker_id", None) == worker.worker_id
        and r.exc_info
    ]
    _assert_it_ran_once_uncharged(seam, worker, result, told)
    released = _released_records(caplog, worker)
    messages = [r.getMessage() for r in released]
    assert len(released) == (1 if seam.landed else 0), messages


def _call_raises_then_the_next_runs_it(seam, worker, result, call, caplog):
    """
    The inline contract. The call whose claim lost its reply raises the
    claim's own error; the row is then READY with nothing charged, released
    if the claim had landed and untouched if it had not; and the next call
    runs it, once.
    """
    with pytest.raises(DatabaseError):
        call()
    # As any caller does after a database error: drop a connection that died.
    close_old_connections()
    _assert_the_seam_fired_where_intended(seam, worker)
    row = OxTask.objects.get(id=result.id)
    if seam.landed:
        expected = (OxTask.Status.READY, None, 0, 2, [], None)
    else:
        expected = (OxTask.Status.READY, None, 0, 0, [], None)
    assert (
        row.status,
        row.locked_by,
        row.attempts,
        row.lease_epoch,
        row.worker_ids,
        row.started_at,
    ) == expected, "the failed call left the row as it was not before the claim"
    assert len(_released_records(caplog, worker)) == (1 if seam.landed else 0)
    assert _runs() == 0

    call()
    row = OxTask.objects.get(id=result.id)
    assert (row.status, row.attempts, _runs()) == (OxTask.Status.SUCCESSFUL, 1, 1)
    assert row.worker_ids == [worker.worker_id]


@pytest.mark.parametrize("window", CLAIM_WINDOWS)
def test_run_once_raises_and_the_next_call_runs_the_task(
    window, settings, caplog, lose_the_reply
):
    """
    run_once() on the test's own thread and connection, which is in
    autocommit: nothing reaps, so a row the failed call did not release would
    still be RUNNING when the next call looks for work, and that call would
    find none.
    """
    seam = lose_the_reply(window, on=connections["default"])
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    worker = Worker(
        lock_timeout=INLINE_LOCK_TIMEOUT, poll_interval=0.05, backoff_initial=0
    )
    result = tasks.record.enqueue("lost-reply")
    _call_raises_then_the_next_runs_it(
        seam, worker, result, lambda: worker.run_once(), caplog
    )


@pytest.mark.parametrize("window", CLAIM_WINDOWS)
def test_run_tasks_raises_and_the_next_call_runs_the_task(
    window, settings, caplog, monkeypatch, lose_the_reply
):
    """
    run_tasks() in autocommit, as from a TransactionTestCase: the same
    contract as run_once(), through the drain's own worker.
    """
    seam = lose_the_reply(window, on=connections["default"])
    caplog.set_level(logging.WARNING, logger="django_ox")
    _budget(settings, 1)
    result = tasks.record.enqueue("lost-reply")
    workers = []
    real_init = Worker.__init__

    def remember(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        workers.append(self)

    monkeypatch.setattr(Worker, "__init__", remember)
    with pytest.raises(DatabaseError):
        run_tasks()
    close_old_connections()
    monkeypatch.undo()
    (worker,) = workers
    _assert_the_seam_fired_where_intended(seam, worker)
    row = OxTask.objects.get(id=result.id)
    assert (row.status, row.attempts, row.worker_ids) == (OxTask.Status.READY, 0, [])
    assert len(_released_records(caplog, worker)) == (1 if seam.landed else 0)
    assert _runs() == 0

    (ran,) = run_tasks()
    row = OxTask.objects.get(id=result.id)
    assert ran.status == "SUCCESSFUL"
    assert (row.status, row.attempts, _runs()) == (OxTask.Status.SUCCESSFUL, 1, 1)
