"""
Per-task policy under the races a fleet actually has, through real workers.

Every worker here is `manage.py ox_worker` in a process of its own, on the
test database. The scenarios are the adversarial ones: two workers claiming
the same rows at once, a task timeout longer than the lease, an owner that
comes back after its lease was taken, a task that swallows its TaskTimeout,
the watchdog's grace backstop racing the reaper, different policies side by
side in one worker, and a backoff called after the task broke its own
database connection, or one that breaks it itself.

Nothing is asserted on the final status alone. Task bodies record which claim
ran them (policy_hazard_tasks), and each test holds the row's claim history
(worker_ids, attempts, lease_epoch), its errors and its outcome against those
records: a duplicate invocation, an overlap, or a write that landed from an
epoch that no longer owned the row is a record that should not exist.

Losing a lease is provoked with SIGSTOP rather than by waiting on a clock the
test cannot control: a stopped worker renews nothing, so its lease expires by
construction, and it resumes only when the test has seen the other worker
take the row. Those tests run on PostgreSQL: SQLite's file lock can be held by
the stopped process, which would stall the other worker behind it rather than
race it. The broken-connection tests end a connection with PostgreSQL's own
statements, and those where the backoff ends it use MySQL's KILL there too.
The rest run on SQLite too. Every wait has a generous limit and is a wait for
a recorded event, never a sleep sized to the machine.
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
from contextlib import suppress
from datetime import timedelta

import pytest
from django.db import connection
from django.utils import timezone

from django_ox import actions
from django_ox.exceptions import TaskAbandoned, TaskTimeout
from django_ox.models import OxTask

from . import policy_hazard_tasks as hazard
from .policy_hazard_tasks import gate, read_notes

TIMEOUT_PATH = f"{TaskTimeout.__module__}.{TaskTimeout.__qualname__}"
ABANDONED_PATH = f"{TaskAbandoned.__module__}.{TaskAbandoned.__qualname__}"
VALUE_ERROR = "builtins.ValueError"

#: The longest any test waits for a worker to reach a state. Far past what a
#: loaded machine needs; reaching it means the state never came.
LIMIT = 90.0

#: The lease the lease-losing tests run with, so a stopped worker loses its
#: rows within seconds. Renewal runs every third of it.
SHORT_LEASE = "1.5"

#: The taker's own lease in those tests. Longer, because the taker is never
#: stopped and must not lose the row it takes on a loaded machine; it still
#: reaps every three seconds.
TAKER_LEASE = "6"

#: The lease the renewal test holds for several periods. Renewal every second
#: leaves two seconds of slack a loaded machine can spend without losing it.
RENEWED_LEASE = "3"

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(os.name != "posix", reason="runs workers under POSIX signals"),
]

stops_a_worker = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason=(
        "stops a worker with SIGSTOP, run on PostgreSQL: on SQLite the stopped "
        "process can hold the database file lock and stall the other worker"
    ),
)
breaks_a_connection = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="uses pg_terminate_backend or SELECT ... FOR UPDATE NOWAIT",
)

ends_a_connection = pytest.mark.skipif(
    connection.vendor not in ("postgresql", "mysql"),
    reason="has the server end a connection, with pg_terminate_backend or KILL",
)

SETTLED = (OxTask.Status.SUCCESSFUL, OxTask.Status.FAILED)


class WorkerProcess:
    """One `manage.py ox_worker`, its output going to a file."""

    def __init__(self, name, popen, log):
        self.name = name
        self.popen = popen
        self.log = log

    @property
    def pid(self):
        return self.popen.pid

    def text(self):
        return self.log.read_text() if self.log.exists() else ""

    @property
    def worker_id(self):
        found = re.search(r"Worker (\S+) starting", self.text())
        return found.group(1) if found else None

    def pause(self):
        os.kill(self.pid, signal.SIGSTOP)

    def resume(self):
        os.kill(self.pid, signal.SIGCONT)

    def wait(self, limit=LIMIT):
        try:
            return self.popen.wait(timeout=limit)
        except subprocess.TimeoutExpired:
            self.kill()
            pytest.fail(f"worker {self.name} did not exit in {limit}s\n{self.text()}")

    def stop(self, limit=LIMIT):
        """SIGTERM, which drains; the exit code."""
        self.popen.send_signal(signal.SIGTERM)
        return self.wait(limit)

    def kill(self):
        with suppress(ProcessLookupError):
            os.kill(self.pid, signal.SIGCONT)
            self.popen.kill()
        self.popen.wait(timeout=30)


class Workers:
    """Starts workers for one test, and kills whatever it left running."""

    def __init__(self, tmp_path, policy_log):
        self.tmp_path = tmp_path
        self.policy_log = policy_log
        self.started = []

    def start(self, name, *args, options=None):
        from django.conf import settings

        # Coverage's hooks stay out of the child: a thread a tracer watches is
        # never interrupted, which would turn every timeout into the backstop.
        env = {k: v for k, v in os.environ.items() if not k.startswith("COV_CORE")}
        env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
        env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
        env["OX_TEST_LOG_LEVEL"] = "INFO"
        env["OX_TEST_POLICY_LOG"] = str(self.policy_log)
        if options is not None:
            env["OX_TEST_TASKS_OPTIONS"] = json.dumps(options)
        log = self.tmp_path / f"worker-{name}.log"
        with log.open("wb") as out:
            popen = subprocess.Popen(  # noqa: S603
                [
                    sys.executable,
                    "-m",
                    "django",
                    "ox_worker",
                    "--interval",
                    "0.05",
                    *args,
                ],
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        process = WorkerProcess(name, popen, log)
        self.started.append(process)
        return process

    def logs(self):
        return "\n".join(f"--- {w.name} ---\n{w.text()}" for w in self.started)

    def wait_until(self, predicate, what, limit=LIMIT):
        end = time.monotonic() + limit
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(0.05)
        pytest.fail(
            f"{what}: not within {limit}s\nnotes: {read_notes(self.policy_log)}\n"
            f"{self.logs()}"
        )

    def wait_running(self, *processes):
        self.wait_until(lambda: all(p.worker_id for p in processes), "workers starting")

    def kill_all(self):
        for process in self.started:
            if process.popen.poll() is None:
                process.kill()


@pytest.fixture
def policy_log(tmp_path):
    return tmp_path / "policy.jsonl"


@pytest.fixture
def workers(tmp_path, policy_log):
    started = Workers(tmp_path, policy_log)
    yield started
    started.kill_all()


def row(result):
    return OxTask.objects.get(id=result.id)


def starts(notes, result):
    """The bodies that ran for `result`, in attempt order."""
    found = [n for n in notes if n.get("event") == "start" and n["task"] == result.id]
    return sorted(found, key=lambda n: n["attempt"])


def ends(notes, result):
    found = [n for n in notes if n.get("event") == "end" and n["task"] == result.id]
    return sorted(found, key=lambda n: n["attempt"])


def callbacks(notes, result=None):
    return [
        n
        for n in notes
        if n.get("event") == "callback" and (result is None or n["task"] == result.id)
    ]


def error_paths(stored):
    return [error["exception_class_path"] for error in stored.errors]


def assert_history(stored, notes, result, claimers, *, sequential=True):
    """
    One body per claim, run by the claim that says so: attempt n ran on the
    n-th worker in worker_ids, in one of `claimers`. Sequential, every body
    also ended before the next one started; a test that takes a lease from a
    live owner says it is not, because that overlap is the documented
    at-least-once case it provokes.
    """
    ran = starts(notes, result)
    assert stored.attempts == len(stored.worker_ids) == len(ran), (stored, ran)
    assert [n["attempt"] for n in ran] == list(range(1, stored.attempts + 1))
    assert [n["worker_id"] for n in ran] == stored.worker_ids
    for record in ran:
        assert f"-{record['pid']}-" in record["worker_id"], record
        assert record["pid"] in claimers, record
    if not sequential:
        return
    finished = ends(notes, result)
    assert [n["attempt"] for n in finished] == [n["attempt"] for n in ran]
    for later, earlier in zip(ran[1:], finished, strict=False):
        assert earlier["at"] <= later["at"], (earlier, later)


def assert_held_by(stored, process, *, epoch, errors):
    """
    The row is the running attempt of `process`, at `epoch`, with nothing
    anybody else wrote on it: no outcome, no retry time, no error.
    """
    assert (stored.status, stored.locked_by, stored.lease_epoch) == (
        OxTask.Status.RUNNING,
        process.worker_id,
        epoch,
    ), stored.__dict__
    assert error_paths(stored) == errors
    assert stored.run_after is None
    assert stored.finished_at is None


def seconds_after(when, since):
    return (when - since).total_seconds()


def test_two_workers_claiming_at_once_never_share_an_attempt(workers, policy_log):
    kinds = [
        (hazard.race_succeeds, OxTask.Status.SUCCESSFUL, 1),
        (hazard.race_fails_once, OxTask.Status.SUCCESSFUL, 2),
        (hazard.race_fails_thrice, OxTask.Status.FAILED, 3),
        (hazard.race_fails_once_for_good, OxTask.Status.FAILED, 1),
    ]
    # Not due yet, so neither worker can take a row before the other is
    # polling; they become due together, in one statement.
    later = timezone.now() + timedelta(hours=1)
    enqueued = [
        (kind.using(run_after=later).enqueue(n), status, attempts)
        for n in range(6)
        for kind, status, attempts in kinds
    ]
    first = workers.start("a", "--concurrency", "3")
    second = workers.start("b", "--concurrency", "3")
    workers.wait_running(first, second)

    assert OxTask.objects.update(run_after=None) == len(enqueued)
    workers.wait_until(
        lambda: not OxTask.objects.exclude(status__in=SETTLED).exists(),
        "every row settled",
    )
    assert first.stop() == 0, first.text()
    assert second.stop() == 0, second.text()

    notes = read_notes(policy_log)
    claimers = {first.pid, second.pid}
    for result, status, attempts in enqueued:
        stored = row(result)
        assert (stored.status, stored.attempts) == (status, attempts), result.task
        # No reaper moved anything: one epoch per claim, each claim's own.
        assert stored.lease_epoch == attempts
        assert [n["epoch"] for n in starts(notes, result)] == list(
            range(1, attempts + 1)
        )
        assert_history(stored, notes, result, claimers)
        failed = attempts if status == OxTask.Status.FAILED else attempts - 1
        assert error_paths(stored) == [VALUE_ERROR] * failed
    # Every body ran for a claim a row records, and both workers ran bodies
    # while the other was claiming.
    ran = [n for n in notes if n.get("event") == "start"]
    assert len(ran) == sum(attempts for _, _, attempts in enqueued)
    assert {n["pid"] for n in ran} == claimers
    assert all(n["both"] for n in notes if n.get("event") == "end")
    for process in (first, second):
        assert "lost its lease" not in process.text()
        assert "Reclaimed" not in process.text()


def test_renewal_holds_attempts_that_outlast_the_lease(workers, policy_log):
    first = workers.start("a", "--concurrency", "3", "--lock-timeout", RENEWED_LEASE)
    reaper = workers.start("b", "--lock-timeout", RENEWED_LEASE)
    workers.wait_running(first, reaper)

    before = timezone.now()
    # Over three lease periods under a 30 second timeout; a 7 second timeout,
    # over two periods, struck while it runs; and a backoff that takes over
    # two periods to answer. Both workers reap every 1.5 seconds throughout.
    outlives = hazard.outlives_its_lease.enqueue(10)
    times_out = hazard.times_out_past_its_lease.enqueue()
    slow = hazard.fails_into_a_slow_backoff.enqueue()
    workers.wait_until(
        lambda: (
            row(outlives).status == OxTask.Status.SUCCESSFUL
            and row(times_out).status == OxTask.Status.SUCCESSFUL
            and row(slow).status == OxTask.Status.READY
            and len(callbacks(read_notes(policy_log), slow)) == 2
        ),
        "every attempt recorded",
    )
    assert first.stop() == 0, first.text()
    assert reaper.stop() == 0, reaper.text()

    notes = read_notes(policy_log)
    claimers = {first.pid, reaper.pid}

    stored = row(outlives)
    assert (stored.attempts, stored.lease_epoch, stored.errors) == (1, 1, [])
    assert stored.return_value == "outlived"
    assert_history(stored, notes, outlives, claimers)
    (finished,) = ends(notes, outlives)
    holders = {holder for holder, _ in finished["samples"]}
    assert holders == set(stored.worker_ids)
    expiries = [expiry for _, expiry in finished["samples"]]
    assert expiries == sorted(expiries)
    # Ten seconds on a three second lease is not survivable without renewing
    # at least three times.
    assert len(set(expiries)) >= 4, expiries

    stored = row(times_out)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.SUCCESSFUL,
        2,
        2,
    )
    assert stored.return_value == "second"
    assert_history(stored, notes, times_out, claimers)
    (error,) = stored.errors
    assert error["exception_class_path"] == TIMEOUT_PATH
    assert "past the 7s timeout" in error["traceback"]
    first_attempt = starts(notes, times_out)[0]
    struck = ends(notes, times_out)[0]
    assert 6.9 < struck["at"] - first_attempt["at"] < 30

    stored = row(slow)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.READY,
        1,
        1,
    )
    assert error_paths(stored) == [VALUE_ERROR]
    assert 3590 < seconds_after(stored.run_after, before) < 3700
    (ran,) = starts(notes, slow)
    called, answered = callbacks(notes, slow)
    assert (called["phase"], answered["phase"]) == ("called", "answered")
    assert answered["at"] - called["at"] >= 6.9
    # On the attempt's own thread, inside its lease: the reaper saw a live
    # lease the whole time the callback took.
    assert called["thread"] == ran["thread"]
    assert called["pid"] == ran["pid"]

    for process in (first, reaper):
        assert "Reclaimed" not in process.text(), process.text()
        assert "lost its lease" not in process.text(), process.text()


@stops_a_worker
def test_an_owner_back_after_losing_its_lease_writes_nothing(workers, policy_log):
    result = hazard.late_owner.enqueue()
    owner = workers.start("owner", "--max-tasks", "1", "--lock-timeout", SHORT_LEASE)
    workers.wait_until(lambda: starts(read_notes(policy_log), result), "owner running")
    owner.pause()

    taker = workers.start("taker", "--max-tasks", "1", "--lock-timeout", TAKER_LEASE)
    workers.wait_until(
        lambda: len(starts(read_notes(policy_log), result)) == 2, "the taker running"
    )
    # The owner comes back while the taker is inside the task, onto a row that
    # is RUNNING under someone else: only the epoch tells them apart.
    gate(policy_log.parent, "late-owner").touch()
    owner.resume()
    assert owner.wait() == 0, owner.text()
    assert_held_by(row(result), taker, epoch=3, errors=[])

    gate(policy_log.parent, "late-owner-taker").touch()
    workers.wait_until(
        lambda: row(result).status == OxTask.Status.SUCCESSFUL, "the taker's success"
    )
    assert taker.wait() == 0, taker.text()

    stored = row(result)
    notes = read_notes(policy_log)
    assert stored.return_value == "second"
    assert stored.worker_ids == [owner.worker_id, taker.worker_id]
    # Claimed, reaped, claimed again: three epochs, and the taker's is last.
    assert stored.lease_epoch == 3
    assert [n["epoch"] for n in starts(notes, result)] == [1, 3]
    assert_history(stored, notes, result, {owner.pid, taker.pid}, sequential=False)
    # The owner's failure, its backoff's hour and its READY never landed.
    assert stored.errors == []
    assert stored.run_after is None
    # The owner still asked its backoff, while the taker held the row, and
    # the answer went nowhere.
    (asked,) = callbacks(notes, result)
    assert asked["pid"] == owner.pid
    assert (asked["attempts"], asked["status"]) == (1, "FAILED")
    assert asked["errors"] == [VALUE_ERROR]
    assert starts(notes, result)[1]["at"] < asked["at"] < ends(notes, result)[1]["at"]
    assert "lost its lease on attempt 1/3; dropping the READY write" in owner.text(), (
        owner.text()
    )
    assert "retrying in" not in owner.text()
    assert f"Reclaimed stuck task id={result.id}" in taker.text()


@stops_a_worker
def test_a_late_outcome_answers_lost_but_not_an_operator_retry(workers, policy_log):
    kept = hazard.answers_after_lost.enqueue("kept")
    retried = hazard.answers_after_lost.enqueue("retried")
    owner = workers.start(
        "owner",
        "--concurrency",
        "2",
        "--max-tasks",
        "2",
        "--lock-timeout",
        SHORT_LEASE,
    )
    workers.wait_until(
        lambda: (
            starts(read_notes(policy_log), kept)
            and starts(read_notes(policy_log), retried)
        ),
        "owner running both",
    )
    owner.pause()

    taker = workers.start("taker", "--max-tasks", "1", "--lock-timeout", TAKER_LEASE)
    # No attempts left, so the reaper can only say the lease was lost.
    workers.wait_until(
        lambda: OxTask.objects.filter(status=OxTask.Status.LOST).count() == 2,
        "both rows LOST",
    )
    assert row(kept).lease_epoch == row(retried).lease_epoch == 1
    assert actions.retry(retried.id) is True
    workers.wait_until(
        lambda: len(starts(read_notes(policy_log), retried)) == 2, "the retry running"
    )
    gate(policy_log.parent, "kept").touch()
    gate(policy_log.parent, "retried").touch()
    owner.resume()
    assert owner.wait() == 0, owner.text()
    # The retry is still the taker's, untouched by the owner's late failure.
    assert_held_by(row(retried), taker, epoch=3, errors=[ABANDONED_PATH])

    gate(policy_log.parent, "retried-taker").touch()
    workers.wait_until(
        lambda: row(retried).status == OxTask.Status.SUCCESSFUL, "the retry's success"
    )
    assert taker.wait() == 0, taker.text()

    notes = read_notes(policy_log)
    # LOST keeps the epoch, so the owner, the only one who saw the attempt,
    # still answers it, and its answer replaces the reaper's note.
    stored = row(kept)
    assert stored.status == OxTask.Status.FAILED
    assert (stored.attempts, stored.lease_epoch) == (1, 1)
    assert stored.worker_ids == [owner.worker_id]
    assert error_paths(stored) == [VALUE_ERROR]
    assert stored.finished_at is not None
    assert_history(stored, notes, kept, {owner.pid})
    # The operator's retry moved the epoch, so the same late answer lands
    # nowhere on the retried row.
    stored = row(retried)
    assert stored.return_value == "retried"
    assert (stored.attempts, stored.max_attempts, stored.lease_epoch) == (2, 2, 3)
    assert stored.worker_ids == [owner.worker_id, taker.worker_id]
    assert error_paths(stored) == [ABANDONED_PATH]
    assert_history(stored, notes, retried, {owner.pid, taker.pid}, sequential=False)
    # Both were on their last attempt, so no backoff was asked.
    assert callbacks(notes) == []
    assert "dropping the FAILED write" in owner.text()
    assert owner.text().count("lost its lease") == 1
    assert taker.text().count("-> LOST") == 2


def test_the_backstop_fences_a_swallowed_timeout_and_its_late_return(
    workers, policy_log
):
    swallower = hazard.swallows_its_timeout.enqueue()
    sibling = hazard.holds_the_drain.enqueue()
    owner = workers.start(
        "owner",
        "--concurrency",
        "2",
        "--max-tasks",
        "2",
        options={"TASK_TIMEOUT_GRACE": 2, "BACKOFF_INITIAL": 1, "BACKOFF_MAX": 1},
    )
    # The backstop recycles: exit 75, after the sibling has drained.
    assert owner.wait() == 75, owner.text()

    notes = read_notes(policy_log)
    stored = row(swallower)
    # The watchdog took the attempt away with the epoch moved, on the row's
    # budget and the worker's one-second backoff, not the task's hour.
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.READY,
        1,
        2,
    )
    (error,) = stored.errors
    assert error["exception_class_path"] == TIMEOUT_PATH
    assert "within the 2s grace" in error["traceback"]
    swallowed = [n for n in notes if n.get("event") == "swallowed"]
    (saw,) = [n for n in notes if n.get("event") == "saw_epoch"]
    assert swallowed
    assert saw["epoch"] == 2
    assert stored.run_after.timestamp() - swallowed[0]["at"] < 30
    assert callbacks(notes) == []
    text = owner.text()
    assert "did not stop 2s after its 1s timeout on attempt 1/2" in text
    # The thread did come back, and what it returned was refused.
    assert "lost its lease on attempt 1/2; dropping the SUCCESSFUL write" in text
    assert "recycling" in text
    drained = row(sibling)
    assert drained.status == OxTask.Status.SUCCESSFUL
    assert drained.worker_ids == [owner.worker_id]
    assert ends(notes, sibling)[0]["at"] > saw["at"]

    taker = workers.start("taker", "--max-tasks", "1")
    assert taker.wait() == 0, taker.text()
    stored = row(swallower)
    notes = read_notes(policy_log)
    assert stored.status == OxTask.Status.SUCCESSFUL
    assert stored.return_value == "second"
    assert stored.worker_ids == [owner.worker_id, taker.worker_id]
    assert stored.lease_epoch == 3
    assert [n["epoch"] for n in starts(notes, swallower)] == [1, 3]
    assert error_paths(stored) == [TIMEOUT_PATH]
    assert callbacks(notes) == []


@stops_a_worker
def test_a_backstop_record_after_the_reaper_took_the_row_is_fenced(workers, policy_log):
    result = hazard.stubborn.enqueue()
    owner = workers.start(
        "owner",
        "--max-tasks",
        "1",
        "--lock-timeout",
        SHORT_LEASE,
        options={"TASK_TIMEOUT_GRACE": 2},
    )
    workers.wait_until(lambda: starts(read_notes(policy_log), result), "owner running")
    owner.pause()

    # The taker's attempt has the same two seconds, and a grace that lets it
    # swallow them and hold the row for as long as the test needs.
    taker = workers.start(
        "taker",
        "--max-tasks",
        "1",
        "--lock-timeout",
        TAKER_LEASE,
        options={"TASK_TIMEOUT_GRACE": 60},
    )
    workers.wait_until(
        lambda: len(starts(read_notes(policy_log), result)) == 2, "the taker running"
    )
    owner.resume()
    # Its deadline passed while it was stopped: the timeout strikes, the task
    # swallows it, the grace runs out, and the backstop's record, which moves
    # the epoch on as it writes, meets a row the taker is running. The
    # thread is still inside the task, so the worker recycles.
    assert owner.wait() == 75, owner.text()
    assert_held_by(row(result), taker, epoch=3, errors=[])

    gate(policy_log.parent, "stubborn-taker").touch()
    workers.wait_until(
        lambda: row(result).status == OxTask.Status.SUCCESSFUL, "the taker's success"
    )
    assert taker.wait() == 0, taker.text()

    stored = row(result)
    notes = read_notes(policy_log)
    assert stored.return_value == "second"
    assert stored.worker_ids == [owner.worker_id, taker.worker_id]
    assert stored.lease_epoch == 3
    assert [n["epoch"] for n in starts(notes, result)] == [1, 3]
    assert stored.errors == []
    assert stored.run_after is None
    swallowed = [
        n for n in notes if n.get("event") == "swallowed" and n["attempt"] == 1
    ]
    assert swallowed
    assert swallowed[0]["at"] > starts(notes, result)[1]["at"]
    assert callbacks(notes) == []
    text = owner.text()
    assert "did not stop 2s after its 2s timeout on attempt 1/2" in text
    assert "lost its lease on attempt 1/2; dropping the READY write" in text
    assert f"Reclaimed stuck task id={result.id}" in taker.text()


def test_one_worker_runs_different_policies_side_by_side(workers, policy_log):
    before = timezone.now()
    times_out = hazard.mixed_times_out.enqueue()
    runs_long = hazard.mixed_runs_long.enqueue()
    waits = hazard.mixed_waits_an_hour.enqueue()
    declines = hazard.mixed_declines.enqueue()
    retries = hazard.mixed_retries_now.enqueue()
    plain = hazard.mixed_declares_nothing.enqueue()
    everything = [times_out, runs_long, waits, declines, retries, plain]
    assert len(everything) == hazard.MIXED
    assert len({r.task.queue_name for r in everything}) == 1

    worker = workers.start(
        "one",
        "--batch",
        "--concurrency",
        str(hazard.MIXED),
        options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900, "TASK_TIMEOUT_GRACE": 10},
    )
    assert worker.wait() == 0, worker.text()

    notes = read_notes(policy_log)
    first_attempts = [
        n for n in notes if n.get("event") == "start" and n["attempt"] == 1
    ]
    assert len(first_attempts) == hazard.MIXED
    # All six in flight at once, on six threads of one process.
    assert len({n["thread"] for n in first_attempts}) == hazard.MIXED
    assert {n["pid"] for n in first_attempts} == {worker.pid}
    others = [
        n
        for n in notes
        if n.get("event") == "end" and n["attempt"] == 1 and n["task"] != times_out.id
    ]
    assert max(n["at"] for n in first_attempts) < min(n["at"] for n in others)

    def started(result, attempt=1):
        return starts(notes, result)[attempt - 1]

    stored = row(times_out)
    assert (stored.status, stored.attempts) == (OxTask.Status.FAILED, 1)
    (error,) = stored.errors
    assert error["exception_class_path"] == TIMEOUT_PATH
    assert "past the 1s timeout" in error["traceback"]
    assert 0 < started(times_out)["remaining"] <= 1

    # Its sibling's one second never reached it.
    stored = row(runs_long)
    assert (stored.status, stored.return_value) == (OxTask.Status.SUCCESSFUL, "long")
    assert started(runs_long)["remaining"] is None
    assert started(runs_long)["deadline"] is None
    assert ends(notes, runs_long)[0]["at"] - started(runs_long)["at"] >= 3

    stored = row(waits)
    assert (stored.status, stored.attempts) == (OxTask.Status.READY, 1)
    assert 3590 < seconds_after(stored.run_after, before) < 3700
    (asked,) = callbacks(notes, waits)
    assert asked["callback"] == "hour"
    assert (asked["attempts"], asked["status"], asked["max_attempts"]) == (
        1,
        "FAILED",
        3,
    )

    stored = row(declines)
    assert (stored.status, stored.attempts, stored.max_attempts) == (
        OxTask.Status.FAILED,
        1,
        5,
    )
    (asked,) = callbacks(notes, declines)
    assert asked["callback"] == "decline"

    stored = row(retries)
    assert (stored.status, stored.attempts) == (OxTask.Status.SUCCESSFUL, 2)
    assert stored.return_value == "second"
    # A fresh five seconds on each attempt, not a sibling's one.
    for attempt in (1, 2):
        assert 4 < started(retries, attempt)["remaining"] <= 5
    (asked,) = callbacks(notes, retries)
    assert asked["callback"] == "now"

    # Nothing declared, so nothing a neighbour declared: the worker's backoff.
    stored = row(plain)
    assert (stored.status, stored.attempts) == (OxTask.Status.READY, 1)
    assert 890 < seconds_after(stored.run_after, before) < 1000
    assert started(plain)["remaining"] is None
    assert callbacks(notes, plain) == []

    # Each callback ran once, for its own task, on that task's own thread.
    asked = callbacks(notes)
    assert len(asked) == 3
    for record in asked:
        (ran,) = [
            n
            for n in first_attempts
            if n["task"] == record["task"] and n["attempt"] == record["attempts"]
        ]
        assert record["thread"] == ran["thread"]
    text = worker.text()
    assert text.count("ran past its 1s timeout") == 1
    assert "its backoff returned None, so it is not retried" in text
    assert "retrying on the worker's backoff instead" not in text


def test_a_pool_thread_carries_no_policy_into_its_next_attempt(workers, policy_log):
    before = timezone.now()
    # Claimed in this order. The one that never imports comes straight after
    # the declared backoff: it fails before any policy of its own is resolved,
    # so a policy left behind on the thread would be the one it found.
    declared = hazard.first_on_the_thread.enqueue()
    moved = hazard.next_on_the_thread.enqueue()
    plain = hazard.next_on_the_thread.enqueue()
    OxTask.objects.filter(id=moved.id).update(
        task_path="tests.policy_hazard_tasks.not_there"
    )

    # One pool thread, so each attempt runs where the last one did.
    worker = workers.start(
        "one",
        "--batch",
        "--concurrency",
        "1",
        options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900},
    )
    assert worker.wait() == 0, worker.text()

    notes = read_notes(policy_log)
    (first,) = starts(notes, declared)
    (then,) = starts(notes, plain)
    assert first["thread"] == then["thread"]
    assert first["at"] < then["at"]
    stored = row(declared)
    assert stored.status == OxTask.Status.READY
    assert 3590 < seconds_after(stored.run_after, before) < 3700
    # The declared backoff stayed with its own attempt: the next two, one
    # that never imported and one that declares nothing, take the worker's.
    for result, error in ((moved, "builtins.ImportError"), (plain, VALUE_ERROR)):
        stored = row(result)
        assert (stored.status, stored.attempts) == (OxTask.Status.READY, 1)
        assert error_paths(stored) == [error]
        assert 890 < seconds_after(stored.run_after, before) < 1000
    (asked,) = callbacks(notes)
    assert asked["task"] == declared.id


@breaks_a_connection
def test_a_backoff_runs_on_a_fresh_connection_after_the_task_broke_its_own(
    workers, policy_log
):
    before = timezone.now()
    result = hazard.kills_its_connection.enqueue()

    worker = workers.start(
        "one", "--max-tasks", "1", options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900}
    )
    assert worker.wait() == 0, worker.text()

    stored = row(result)
    notes = read_notes(policy_log)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.READY,
        1,
        1,
    )
    # The task's own failure is the one recorded, and the callback's hour,
    # not the worker's fifteen minutes, is the delay.
    assert error_paths(stored) == ["django.db.utils.OperationalError"]
    assert 3590 < seconds_after(stored.run_after, before) < 3700
    (killed,) = [n for n in notes if n.get("event") == "backend"]
    (asked,) = callbacks(notes, result)
    assert asked["exception"] == "OperationalError"
    # A working connection, not the one the task ended, and it read the row
    # before the outcome was written.
    assert asked["backend_pid"] != killed["backend_pid"]
    assert (asked["row_status"], asked["row_epoch"]) == ("RUNNING", 1)
    assert "Unhandled error" not in worker.text()
    assert "retrying in 3600.0s" in worker.text()


@breaks_a_connection
def test_a_backoff_after_a_timeout_in_a_transaction_finds_the_row_unlocked(
    workers, policy_log
):
    before = timezone.now()
    result = hazard.times_out_holding_its_row.enqueue()

    worker = workers.start(
        "one", "--max-tasks", "1", options={"TASK_TIMEOUT_GRACE": 10}
    )
    assert worker.wait() == 0, worker.text()

    stored = row(result)
    notes = read_notes(policy_log)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.READY,
        1,
        1,
    )
    assert error_paths(stored) == [TIMEOUT_PATH]
    assert 3590 < seconds_after(stored.run_after, before) < 3700
    assert [n for n in notes if n.get("event") == "locked"]
    # NOWAIT succeeded: the transaction the timeout interrupted is gone, and
    # the row was still the attempt's, RUNNING, when the callback asked.
    (asked,) = callbacks(notes, result)
    assert asked["callback"] == "lock"
    assert asked["row_status"] == "RUNNING"
    assert "ran past its 1s timeout" in worker.text()
    assert "retrying on the worker's backoff instead" not in worker.text()


@breaks_a_connection
@pytest.mark.parametrize(
    ("declared", "status", "delay"),
    [
        # No backoff to ask: the worker's own.
        ("kills_its_connection_and_declares_no_backoff", OxTask.Status.READY, 900),
        # A backoff, but no attempt left to ask it about.
        ("kills_its_connection_on_its_last_attempt", OxTask.Status.FAILED, None),
    ],
)
def test_a_failure_is_recorded_after_the_task_broke_its_connection(
    workers, policy_log, declared, status, delay
):
    before = timezone.now()
    result = getattr(hazard, declared).enqueue()

    worker = workers.start(
        "one", "--max-tasks", "1", options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900}
    )
    assert worker.wait() == 0, worker.text()

    stored = row(result)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        status,
        1,
        1,
    ), worker.text()
    assert error_paths(stored) == ["django.db.utils.OperationalError"]
    if delay is None:
        assert stored.finished_at is not None
    else:
        assert delay - 10 < seconds_after(stored.run_after, before) < delay + 100
    assert callbacks(read_notes(policy_log)) == []
    assert "Unhandled error" not in worker.text()


@breaks_a_connection
def test_a_success_is_recorded_after_the_task_broke_its_connection(workers, policy_log):
    result = hazard.kills_its_connection_and_succeeds.enqueue()

    worker = workers.start("one", "--max-tasks", "1")
    assert worker.wait() == 0, worker.text()

    stored = row(result)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        OxTask.Status.SUCCESSFUL,
        1,
        1,
    ), worker.text()
    assert stored.return_value == "succeeded anyway"
    assert stored.errors == []
    notes = read_notes(policy_log)
    assert [n["exception"] for n in notes if n.get("event") == "caught"] == [
        "OperationalError"
    ]
    assert callbacks(notes) == []
    assert "Unhandled error" not in worker.text()


@ends_a_connection
@pytest.mark.parametrize(
    ("declared", "status", "delay", "policy_error"),
    [
        # Caught, and a delay answered: the callback's hour.
        (
            "fails_into_a_backoff_that_ends_the_connection_and_waits",
            OxTask.Status.READY,
            3600,
            False,
        ),
        # Raised out of the callback: the worker's fifteen minutes.
        (
            "fails_into_a_backoff_that_ends_the_connection_and_raises",
            OxTask.Status.READY,
            900,
            True,
        ),
        # Caught, and None answered: FAILED now.
        (
            "fails_into_a_backoff_that_ends_the_connection_and_declines",
            OxTask.Status.FAILED,
            None,
            False,
        ),
    ],
    ids=["returns-a-delay", "raises", "declines"],
)
def test_the_outcome_lands_after_the_backoff_broke_its_connection(
    workers, policy_log, declared, status, delay, policy_error
):
    """
    The task fails on a healthy connection, and its backoff callback, which
    runs on the attempt's thread before the outcome write, has the server end
    that connection. The write that follows still lands, at the attempt's
    epoch, recording the task's own error: the connection the callback left
    dead is dropped before it. Written on the dead one, the write would raise
    and leave the row RUNNING with its lease no longer renewed.
    """
    before = timezone.now()
    result = getattr(hazard, declared).enqueue()

    worker = workers.start(
        "one", "--max-tasks", "1", options={"BACKOFF_INITIAL": 900, "BACKOFF_MAX": 900}
    )
    assert worker.wait() == 0, worker.text()
    output = worker.text()

    stored = row(result)
    assert (stored.status, stored.attempts, stored.lease_epoch) == (
        status,
        1,
        1,
    ), output
    # The task's failure, never the one the callback's statement raised.
    assert error_paths(stored) == [VALUE_ERROR]
    assert stored.locked_by is None
    if delay is None:
        assert stored.finished_at is not None
        assert stored.run_after is None
        assert "its backoff returned None, so it is not retried" in output
    else:
        assert delay - 10 < seconds_after(stored.run_after, before) < delay + 100
        assert f"retrying in {delay:.1f}s" in output
    notes = read_notes(policy_log)
    (asked,) = callbacks(notes, result)
    assert asked["exception"] == "ValueError"
    # The server did end it, from the callback, on the attempt's thread, and
    # Django knew: the drop acts only on a connection an error flagged.
    (ended,) = [n for n in notes if n.get("event") == "callback-ended"]
    assert (ended["task"], ended["flagged"]) == (result.id, True)
    assert ended["thread"] == asked["thread"]
    assert ("retrying on the worker's backoff instead" in output) is policy_error
    assert "Unhandled error" not in output
