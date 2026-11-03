"""
Concurrency-limit configuration, the pre-claim exclusion and its bound.

Separate from test_concurrency.py, which races unique-key enqueues. The
load-bearing tests are the ones a no-op limit fails: a blocked task is
checked for the attempt and the lease it must not have spent, the ceiling
is sampled under real run() loops and real ox_worker processes with more
slots than the ceiling, and the release of a slot is proved through each
path that frees one, with the refresh interval set too long to hide a
missing mark.
"""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import pytest
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError, connection, connections
from django.db.models import QuerySet
from django.tasks import task_backends
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull.backend import OxpullBackend
from oxpull.concurrency import ConcurrencyLimit, concurrency_limits_from_options
from oxpull.worker import OxpullWorker

from . import tasks
from .subprocesses import django_ox_path
from .test_rate_limits import without_savepoint_names

pytestmark = pytest.mark.django_db

GAUGED = "tests_oxpull.tasks.gauged"
UNGAUGED = "tests_oxpull.tasks.ungauged"
ECHO = "tests_oxpull.tasks.plain_echo"
FLAKY = "tests_oxpull.tasks.plain_flaky"
EMAILS_ECHO = "tests_oxpull.tasks.emails_echo"
HELD = "tests_oxpull.tasks.held_until_released"
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

# Long enough that only a mark can make a worker look again inside a test.
NEVER = 3600


@contextmanager
def concurrency_limits(config, **options):
    """The default backend, with these limits and the worker that applies them."""
    default = settings.TASKS["default"]
    with override_settings(
        TASKS={
            **settings.TASKS,
            "default": {
                **default,
                "OPTIONS": {
                    **default["OPTIONS"],
                    "WORKER_CLASS": "oxpull.worker.OxpullWorker",
                    "CONCURRENCY_LIMITS": config,
                    **options,
                },
            },
        }
    ):
        yield


def oxpull_worker(**kwargs):
    return OxpullWorker(**{"backoff_initial": 0, "poll_interval": 0.01, **kwargs})


def running_row(task_path, *, queue="default", backend="default", expired=False):
    """A RUNNING row another worker holds, or held before it died."""
    now = timezone.now()
    locked_at = now - timedelta(hours=1) if expired else now
    return OxTask.objects.create(
        task_path=task_path,
        queue_name=queue,
        backend_name=backend,
        status=OxTask.Status.RUNNING,
        enqueued_at=locked_at,
        started_at=locked_at,
        locked_by="elsewhere",
        locked_at=locked_at,
        lease_expires_at=locked_at + timedelta(seconds=30),
        attempts=1,
        lease_epoch=1,
    )


@contextmanager
def captured_sql(alias):
    statements = []

    def wrapper(execute, sql, params, many, context):
        statements.append(sql)
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(wrapper):
        yield statements


def counts(statements):
    """The concurrency count statements among the captured ones."""
    return [sql for sql in statements if "running_0" in sql]


def unspent(row):
    row.refresh_from_db()
    return (row.status, row.attempts, row.lease_epoch, row.locked_by) == (
        OxTask.Status.READY,
        0,
        0,
        None,
    )


# -- the parser ----------------------------------------------------------


def test_a_limit_resolves_to_its_ceiling_and_coverage():
    limits = concurrency_limits_from_options(
        {
            "CONCURRENCY_LIMITS": {
                "reports": {"max": 2, "tasks": [GAUGED]},
                "mail": {"max": 0, "queues": ["emails"]},
            }
        },
        "default",
        ["default", "emails"],
    )

    assert list(limits) == ["mail", "reports"]
    assert limits["reports"] == ConcurrencyLimit(
        "reports", 2, frozenset({GAUGED}), frozenset()
    )
    assert limits["mail"].covers(ECHO, "emails")
    assert not limits["reports"].covers(ECHO, "default")


@pytest.mark.parametrize(
    "config",
    [
        [GAUGED],
        {1: {"max": 1, "tasks": [GAUGED]}},
        {"a": 1},
        {"": {"max": 1, "tasks": [GAUGED]}},
        {"a": {"tasks": [GAUGED]}},
        {"a": {"max": -1, "tasks": [GAUGED]}},
        {"a": {"max": True, "tasks": [GAUGED]}},
        {"a": {"max": 1.0, "tasks": [GAUGED]}},
        {"a": {"max": "2", "tasks": [GAUGED]}},
        {"a": {"max": 1}},
        {"a": {"max": 1, "tasks": GAUGED}},
        {"a": {"max": 1, "queues": "emails"}},
        {"a": {"max": 1, "queues": ["nowhere"]}},
        {"a": {"max": 1, "tasks": [GAUGED], "per": 60}},
    ],
)
def test_a_malformed_limit_is_refused(config):
    with pytest.raises(ImproperlyConfigured):
        concurrency_limits_from_options(
            {"CONCURRENCY_LIMITS": config}, "default", ["default", "emails"]
        )


def test_names_that_differ_only_by_case_are_two_limits():
    """A name keys no row, so no collation can make these one limit."""
    limits = concurrency_limits_from_options(
        {
            "CONCURRENCY_LIMITS": {
                "Reports": {"max": 1, "tasks": [GAUGED]},
                "reports": {"max": 2, "tasks": [UNGAUGED]},
            }
        },
        "default",
    )
    assert sorted(limits) == ["Reports", "reports"]


def test_a_worker_refuses_a_malformed_limit_at_construction():
    """--skip-checks must not start a worker that would limit nothing."""
    with (
        concurrency_limits({"a": {"max": -1, "tasks": [GAUGED]}}),
        pytest.raises(ImproperlyConfigured),
    ):
        oxpull_worker()


# -- the system checks -----------------------------------------------------


def check_ids(alias="default"):
    return [message.id for message in task_backends[alias].check()]


def test_check_reports_a_malformed_limit():
    with concurrency_limits({"a": {"max": -1, "tasks": [GAUGED]}}):
        assert "oxpull.E010" in check_ids()


def test_check_reports_a_task_entry_that_is_not_a_task():
    with concurrency_limits({"a": {"max": 1, "tasks": ["tests_oxpull.tasks.nope"]}}):
        errors = [m for m in task_backends["default"].check() if m.id == "oxpull.E010"]
    assert len(errors) == 1
    assert "tests_oxpull.tasks.nope" in errors[0].msg


def test_check_refuses_limits_without_a_worker_that_applies_them():
    default = settings.TASKS["default"]
    with override_settings(
        TASKS={
            **settings.TASKS,
            "default": {
                **default,
                "OPTIONS": {
                    **default["OPTIONS"],
                    "CONCURRENCY_LIMITS": {"a": {"max": 1, "tasks": [GAUGED]}},
                },
            },
        }
    ):
        errors = [m for m in task_backends["default"].check() if m.id == "oxpull.E011"]
    assert len(errors) == 1
    assert "django_ox.worker.Worker" in errors[0].msg


def test_check_reports_a_worker_class_that_is_not_a_worker():
    with concurrency_limits(
        {"a": {"max": 1, "tasks": [GAUGED]}},
        WORKER_CLASS="oxpull.models.OxpullRateWindow",
    ):
        assert "oxpull.E011" in check_ids()


def test_check_accepts_limits_with_the_oxpull_worker():
    with concurrency_limits({"a": {"max": 1, "tasks": [GAUGED]}}):
        backend = OxpullBackend("default", settings.TASKS["default"])
        assert backend.check() == []


@contextmanager
def two_backends(first, second):
    """Two more OxpullBackend aliases, "aa" and "ab", with these limits."""

    def backend(limits):
        return {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "WORKER_CLASS": "oxpull.worker.OxpullWorker",
                "CONCURRENCY_LIMITS": limits,
            },
        }

    with override_settings(
        TASKS={**settings.TASKS, "aa": backend(first), "ab": backend(second)}
    ):
        yield


def w008():
    return [
        message
        for alias in settings.TASKS
        if isinstance(task_backends[alias], OxpullBackend)
        for message in task_backends[alias].check()
        if message.id == "oxpull.W008"
    ]


def test_overlapping_coverage_with_different_ceilings_is_warned_once():
    with two_backends(
        {"reports": {"max": 2, "tasks": [GAUGED, ECHO]}},
        {"heavy": {"max": 5, "tasks": [GAUGED], "queues": ["emails"]}},
    ):
        warnings = w008()
    assert len(warnings) == 1
    assert "'reports' on backend 'aa' (max=2)" in warnings[0].msg
    assert "'heavy' on backend 'ab' (max=5)" in warnings[0].msg
    assert f"task {GAUGED!r}" in warnings[0].msg


def test_overlapping_queues_with_different_ceilings_are_warned():
    with two_backends(
        {"mail": {"max": 2, "queues": ["emails"]}},
        {"mail": {"max": 3, "queues": ["emails"]}},
    ):
        assert len(w008()) == 1


def test_same_coverage_with_different_ceilings_allows_the_larger():
    with two_backends(
        {"r": {"max": 2, "tasks": [GAUGED]}},
        {"r": {"max": 5, "tasks": [GAUGED]}},
    ):
        warnings = w008()
    assert len(warnings) == 1
    assert "up to the larger one" in warnings[0].msg


@pytest.mark.parametrize(
    ("first", "second"),
    [
        # Nested: the second covers part of the first, under the same max.
        (
            {"r": {"max": 2, "tasks": [GAUGED, ECHO]}},
            {"r": {"max": 2, "tasks": [GAUGED]}},
        ),
        # Crossing: each covers a task the other does not.
        (
            {"r": {"max": 2, "tasks": [GAUGED, ECHO]}},
            {"r": {"max": 2, "tasks": [GAUGED, UNGAUGED]}},
        ),
        # The same tasks, and one side adds a queue.
        (
            {"r": {"max": 2, "tasks": [GAUGED]}},
            {"r": {"max": 2, "tasks": [GAUGED], "queues": ["emails"]}},
        ),
    ],
)
def test_differing_coverage_under_one_ceiling_is_warned(first, second):
    """
    Each backend counts only the rows its own limit covers, so equal ceilings
    over differing coverage do not share a count.
    """
    with two_backends(first, second):
        warnings = w008()
    assert len(warnings) == 1
    assert "the sum of both ceilings" in warnings[0].msg


@pytest.mark.parametrize(
    ("first", "second"),
    [
        # One ceiling over one coverage is one count, harmlessly.
        ({"r": {"max": 2, "tasks": [GAUGED]}}, {"r": {"max": 2, "tasks": [GAUGED]}}),
        # One name over disjoint coverage is two unrelated limits.
        ({"r": {"max": 2, "tasks": [GAUGED]}}, {"r": {"max": 5, "tasks": [ECHO]}}),
        # A task on one side and a queue on the other: not literal overlap.
        ({"r": {"max": 2, "tasks": [GAUGED]}}, {"q": {"max": 5, "queues": ["emails"]}}),
    ],
)
def test_coverage_that_does_not_overlap_or_agrees_is_not_warned(first, second):
    with two_backends(first, second):
        assert w008() == []


def test_nesting_on_one_backend_is_not_warned():
    with two_backends(
        {
            "sms": {"max": 10, "queues": ["emails"]},
            "one": {"max": 2, "tasks": [EMAILS_ECHO]},
        },
        {},
    ):
        assert w008() == []


# -- the exclusion -------------------------------------------------------


def test_a_blocked_task_stays_ready_and_uncovered_tasks_are_still_claimed():
    """
    The pre-claim exclusion is the whole design: the claim UPDATE spends an
    attempt and moves the lease epoch, so a blocked row must never reach it.
    """
    running_row(GAUGED)
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        blocked = OxTask.objects.get(pk=tasks.gauged.enqueue(0).id)
        free = tasks.plain_echo.enqueue("a").id
        worker = oxpull_worker()

        claimed = worker.claim_one()
        assert claimed is not None
        assert str(claimed.pk) == str(free)
        assert worker.claim_one() is None
    assert unspent(blocked)


def test_one_worker_claims_exactly_up_to_max():
    """
    A view refreshed after every covered claim holds one process to max
    exactly. Without that refresh it would claim all three.
    """
    with concurrency_limits({"g": {"max": 2, "tasks": [GAUGED]}}):
        rows = [tasks.gauged.enqueue(0).id for _ in range(3)]
        worker = oxpull_worker()
        claimed = [worker.claim_one(), worker.claim_one(), worker.claim_one()]

    assert [row is not None for row in claimed] == [True, True, False]
    assert OxTask.objects.filter(pk__in=rows, status=OxTask.Status.RUNNING).count() == 2


def test_max_zero_pauses_covered_tasks_without_a_count():
    with concurrency_limits({"g": {"max": 0, "tasks": [GAUGED]}}):
        paused = OxTask.objects.get(pk=tasks.gauged.enqueue(0).id)
        free = tasks.plain_echo.enqueue("a").id
        worker = oxpull_worker()
        with captured_sql(worker._db_alias) as statements:
            claimed = worker.claim_one()
            assert worker.claim_one() is None

    assert str(claimed.pk) == str(free)
    assert unspent(paused)
    assert counts(statements) == []


def test_a_queue_limit_counts_every_task_on_that_queue():
    running_row(ECHO, queue="emails")
    with concurrency_limits({"mail": {"max": 1, "queues": ["emails"]}}):
        blocked = OxTask.objects.get(pk=tasks.emails_echo.enqueue("a").id)
        free = tasks.plain_echo.enqueue("a").id
        worker = oxpull_worker()
        claimed = worker.claim_one()
        assert worker.claim_one() is None

    assert str(claimed.pk) == str(free)
    assert unspent(blocked)


def test_rows_enqueued_by_another_backend_count():
    """A claim selects by queue, never by backend, so the count does not either."""
    running_row(GAUGED, backend="limited")
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        blocked = OxTask.objects.get(pk=tasks.gauged.enqueue(0).id)
        assert oxpull_worker().claim_one() is None
    assert unspent(blocked)


def test_the_count_and_the_exclusion_agree_on_what_a_limit_covers():
    """
    For each shape of coverage, a READY row the exclusion holds back is a row
    the count counts once RUNNING, and a row it lets through is not.
    """
    shapes = {
        "tasks": {"max": 1, "tasks": [GAUGED]},
        "queues": {"max": 1, "queues": ["emails"]},
        "both": {"max": 1, "tasks": [GAUGED], "queues": ["emails"]},
    }
    candidates = [(GAUGED, "default"), (ECHO, "emails"), (ECHO, "default")]
    for shape, config in shapes.items():
        limit = concurrency_limits_from_options(
            {"CONCURRENCY_LIMITS": {shape: config}}, "default", ["default", "emails"]
        )[shape]
        for path, queue in candidates:
            OxTask.objects.all().delete()
            row = running_row(path, queue=queue)
            counted = OxTask.objects.filter(limit.covered_q(), pk=row.pk).exists()
            assert counted is limit.covers(path, queue), (shape, path, queue)


@pytest.mark.django_db(databases=["default", "replica"])
def test_the_count_runs_on_the_write_alias_before_the_claim():
    """
    A replica that lags would answer that there is room, and a count inside
    the claim's transaction would read its snapshot on MySQL.
    """
    with (
        override_settings(DATABASE_ROUTERS=["tests_oxpull.routers.ReplicaReadRouter"]),
        concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}),
    ):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()
        with (
            captured_sql(worker._db_alias) as statements,
            captured_sql("replica") as replica,
        ):
            assert worker.claim_one() is not None

    assert counts(replica) == []
    assert len(counts(statements)) == 1
    at = statements.index(counts(statements)[0])
    # Nothing of the claim, its transaction or its UPDATE, comes before it.
    assert not any(
        word in sql.upper()
        for sql in statements[:at]
        for word in ("UPDATE", "SAVEPOINT", "BEGIN")
    ), statements[:at]
    assert any("UPDATE" in sql.upper() for sql in statements[at:])


def test_a_dead_workers_row_counts_until_the_reaper_takes_it_back():
    """
    The row of a worker that died holds its slot until its lease is reaped.
    The refresh interval is an hour, so the slot comes back through the
    reaper's mark and nothing else.
    """
    dead = running_row(GAUGED, expired=True)
    with concurrency_limits(
        {"g": {"max": 1, "tasks": [GAUGED]}}, RATE_REFRESH_INTERVAL=NEVER
    ):
        waiting = tasks.gauged.enqueue(0).id
        worker = oxpull_worker(lock_timeout=30)
        assert worker.claim_one() is None
        assert worker.reap() == 1
        claimed = worker.claim_one()

    assert claimed is not None
    assert str(claimed.pk) in {str(dead.pk), str(waiting)}


@pytest.mark.parametrize(
    ("enqueue", "expected"),
    [
        (lambda: tasks.gauged.enqueue(0), OxTask.Status.SUCCESSFUL),
        (lambda: tasks.plain_flaky.enqueue(2), OxTask.Status.READY),
    ],
    ids=["success", "retry"],
)
def test_this_workers_outcome_frees_the_slot_at_once(enqueue, expected):
    """
    A success and a retry put back to READY both leave RUNNING, and both
    reach the outcome hook. The refresh interval is an hour, so the second
    claim proves the mark rather than the clock.
    """
    with concurrency_limits(
        {"g": {"max": 1, "tasks": [GAUGED, FLAKY]}}, RATE_REFRESH_INTERVAL=NEVER
    ):
        first = enqueue()
        tasks.gauged.enqueue(0)
        worker = oxpull_worker()
        claimed = worker.claim_one()
        assert str(claimed.pk) == str(first.id)
        assert worker.claim_one() is None

        worker.execute(claimed)
        claimed.refresh_from_db()
        assert claimed.status == expected
        assert worker.claim_one() is not None


def test_a_count_that_raises_spends_nothing_and_leaves_the_view_stale(monkeypatch):
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        covered = OxTask.objects.get(pk=tasks.gauged.enqueue(0).id)
        uncovered = OxTask.objects.get(pk=tasks.plain_echo.enqueue("a").id)
        worker = oxpull_worker()

        def refused(self, *args, **kwargs):
            raise DatabaseError("the count is refused")

        with monkeypatch.context() as patched:
            patched.setattr(QuerySet, "aggregate", refused)
            with pytest.raises(DatabaseError):
                worker.claim_one()

        assert unspent(covered)
        assert unspent(uncovered)
        assert worker._concurrency_stale is True
        assert worker.claim_one() is not None


def test_a_mark_set_while_the_count_runs_survives_it(monkeypatch):
    """
    An outcome written on a pool thread while the count is in flight has to
    make the next pass count again. Clearing the flag after the count would
    lose it, and the freed slot would wait out the interval and a poll.
    """
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()
        real = QuerySet.aggregate

        def completing_meanwhile(self, *args, **kwargs):
            worker._concurrency_stale = True
            return real(self, *args, **kwargs)

        monkeypatch.setattr(QuerySet, "aggregate", completing_meanwhile)
        assert worker.claim_one() is not None
    assert worker._concurrency_stale is True


def test_rate_and_concurrency_exclusions_do_not_overwrite_each_other():
    """
    Each view replaces its own blocked sets whole. Kept in one pair, a rate
    refresh would drop every concurrency exclusion, and the reverse.
    """
    with concurrency_limits(
        {"mail": {"max": 0, "queues": ["emails"]}},
        RATE_LIMITS={"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}},
    ):
        echo = OxTask.objects.get(pk=tasks.plain_echo.enqueue("a").id)
        mail = OxTask.objects.get(pk=tasks.emails_echo.enqueue("a").id)
        free = tasks.ungauged.enqueue(0).id
        worker = oxpull_worker()
        assert str(worker.claim_one().pk) == str(free)
        for refresh in ("_view_stale", "_concurrency_stale", "_view_stale"):
            setattr(worker, refresh, True)
            assert worker.claim_one() is None, refresh
            assert worker._oxpull_condition_q() is not None

    assert unspent(echo)
    assert unspent(mail)


def test_a_rate_window_that_defers_its_read_does_not_defer_the_concurrency_read():
    """
    With every rate limit exhausted, the rate view puts its next read off to
    the window's end, ten minutes away here. A slot freed by another worker
    must come back within one refresh interval all the same.
    """
    other = running_row(GAUGED)
    with concurrency_limits(
        {"g": {"max": 1, "tasks": [GAUGED]}},
        RATE_LIMITS={"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}},
        RATE_REFRESH_INTERVAL=0.05,
    ):
        waiting = tasks.gauged.enqueue(0).id
        worker = oxpull_worker()
        assert worker.claim_one() is None
        assert worker._refresh_after - time.monotonic() > 60

        OxTask.objects.filter(pk=other.pk).update(status=OxTask.Status.SUCCESSFUL)
        time.sleep(0.1)
        claimed = worker.claim_one()
    assert claimed is not None
    assert str(claimed.pk) == str(waiting)


def test_block_and_unblock_are_logged_once_per_transition(caplog):
    other = running_row(GAUGED)
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        tasks.gauged.enqueue(0)
        worker = oxpull_worker()
        with caplog.at_level(logging.DEBUG, logger="oxpull"):
            for _ in range(3):
                worker._concurrency_stale = True
                assert worker.claim_one() is None
            OxTask.objects.filter(pk=other.pk).update(status=OxTask.Status.SUCCESSFUL)
            worker._concurrency_stale = True
            assert worker.claim_one() is not None

    events = [
        (record.event, record.limit, record.running, record.max)
        for record in caplog.records
        if getattr(record, "event", "").startswith("concurrency_limit_")
    ]
    assert events == [
        ("concurrency_limit_blocked", "g", 1, 1),
        ("concurrency_limit_unblocked", "g", 0, 1),
    ]


def test_a_worker_without_the_limit_is_counted_but_not_bounded():
    """
    A base Worker claims past the ceiling, and its rows hold the slots
    against a worker that applies the limit.
    """
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        rows = [tasks.gauged.enqueue(0).id for _ in range(3)]
        base = Worker(backoff_initial=0, poll_interval=0.01)
        assert base.claim_one() is not None
        assert base.claim_one() is not None
        assert oxpull_worker().claim_one() is None
    assert OxTask.objects.filter(pk__in=rows, status=OxTask.Status.RUNNING).count() == 2


# -- the claim's cost ----------------------------------------------------


def without_workflow_backends():
    return override_settings(
        TASKS={
            alias: config
            for alias, config in settings.TASKS.items()
            if not config.get("OPTIONS", {}).get("WORKFLOWS")
        }
    )


def test_a_limit_adds_one_count_per_refresh_and_nothing_else():
    """
    With nothing blocked, the claim is the base worker's statements plus the
    one count, and with no limit configured, the base worker's statements.
    """
    with without_workflow_backends():
        tasks.plain_echo.enqueue("a")
        with captured_sql("default") as base:
            assert Worker(backoff_initial=0, poll_interval=0.01).claim_one()
        OxTask.objects.all().delete()

        with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
            tasks.plain_echo.enqueue("a")
            worker = oxpull_worker()
            with captured_sql("default") as limited:
                assert worker.claim_one()

    assert len(counts(limited)) == 1
    assert without_savepoint_names(
        [sql for sql in limited if sql not in counts(limited)]
    ) == without_savepoint_names(base)


# -- the bound under real loops ------------------------------------------


def wait_for(condition, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


@contextmanager
def running(*workers):
    """Each worker's real run() loop on a thread of its own."""

    def run(worker):
        try:
            worker.run()
        finally:
            connections.close_all()

    threads = [threading.Thread(target=run, args=(w,), daemon=True) for w in workers]
    for thread in threads:
        thread.start()
    try:
        yield
    finally:
        for worker in workers:
            worker.request_stop()
        for thread in threads:
            thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads)


def finished(ids):
    return (
        not OxTask.objects.filter(pk__in=ids)
        .exclude(status=OxTask.Status.SUCCESSFUL)
        .exists()
    )


@pytest.mark.django_db(transaction=True)
def test_a_stuck_handover_frees_the_slot_while_the_body_runs_on(caplog):
    """
    The stuck-thread watchdog hands a row over, and the row stops counting,
    whether or not the thread is still inside the body. The body here waits
    in a lock acquire, where TaskTimeout cannot land. After task_stuck no
    covered row is RUNNING, and the same worker claims the next covered task
    while the first body is still alive. The refresh interval is an hour, so
    that claim proves the handover's mark rather than the clock.
    """
    caplog.set_level(logging.ERROR, logger="django_ox")
    tasks.STATE["release"] = threading.Event()
    with concurrency_limits(
        {"g": {"max": 1, "tasks": [HELD, GAUGED]}}, RATE_REFRESH_INTERVAL=NEVER
    ):
        stuck = tasks.held_until_released.enqueue().id
        # One attempt, so the handover records a failure rather than a retry
        # that would put the same row back in front of the second claim.
        OxTask.objects.filter(pk=stuck).update(max_attempts=1)
        waiting = tasks.gauged.enqueue(0).id
        worker = oxpull_worker(task_timeout=0.2, task_timeout_grace=0.3)
        claimed = worker.claim_one()
        assert str(claimed.pk) == str(stuck)
        assert worker.claim_one() is None, "the ceiling of one is not reached"

        def attempt():
            try:
                worker.execute(claimed)
            finally:
                connections.close_all()

        body = threading.Thread(target=attempt, daemon=True)
        body.start()
        try:
            assert wait_for(lambda: worker.recycling, 30), "never handed over"
            assert [
                record.task_id
                for record in caplog.records
                if getattr(record, "event", None) == "task_stuck"
            ] == [str(stuck)]
            assert body.is_alive(), "the body ended before the handover"
            handed_over = OxTask.objects.get(pk=stuck)
            assert handed_over.status == OxTask.Status.FAILED
            assert not OxTask.objects.filter(
                task_path__in=[HELD, GAUGED], status=OxTask.Status.RUNNING
            ).exists()

            second = worker.claim_one()
            assert second is not None, "the handed-over row still holds the slot"
            assert str(second.pk) == str(waiting)
            assert body.is_alive()
        finally:
            tasks.STATE["release"].set()
            body.join(timeout=30)
    assert not body.is_alive()
    # The body's own outcome, once released, is refused by the moved lease.
    assert OxTask.objects.get(pk=stuck).status == OxTask.Status.FAILED


@pytest.mark.django_db(transaction=True)
def test_one_worker_with_four_slots_runs_one_covered_task_at_a_time():
    """
    Four slots and a ceiling of one: the covered tasks run one at a time
    while the uncovered ones fill the other slots in the same passes.
    """
    tasks.reset_gauge()
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        covered = [tasks.gauged.enqueue(0.3).id for _ in range(4)]
        uncovered = [tasks.ungauged.enqueue(0.05).id for _ in range(8)]
        with running(oxpull_worker(concurrency=4, poll_interval=0.05)):
            assert wait_for(lambda: finished(covered + uncovered), 30)

    assert tasks.GAUGE["peak"] == 1
    last_uncovered = max(
        OxTask.objects.filter(pk__in=uncovered).values_list("finished_at", flat=True)
    )
    last_covered_start = max(
        OxTask.objects.filter(pk__in=covered).values_list("started_at", flat=True)
    )
    assert last_uncovered < last_covered_start


@pytest.mark.django_db(transaction=True)
def test_two_claiming_loops_stay_within_max_plus_one():
    """
    Two loops with four slots each and a ceiling of one: max + W - 1 = 2. A
    limit that did nothing would reach eight.
    """
    tasks.reset_gauge()
    with concurrency_limits({"g": {"max": 1, "tasks": [GAUGED]}}):
        covered = [tasks.gauged.enqueue(0.1).id for _ in range(12)]
        with running(
            oxpull_worker(concurrency=4, poll_interval=0.05),
            oxpull_worker(concurrency=4, poll_interval=0.05),
        ):
            assert wait_for(lambda: finished(covered), 60)

    assert 1 <= tasks.GAUGE["peak"] <= 2


@pytest.mark.django_db(transaction=True)
def test_a_slot_comes_back_on_the_pass_that_the_completion_wakes():
    """
    run() wakes when an in-flight task settles. With the refresh interval at
    an hour and a poll interval of five seconds, the second covered task
    starts at once only because the first one's outcome marked the view.
    """
    with concurrency_limits(
        {"g": {"max": 1, "tasks": [GAUGED]}}, RATE_REFRESH_INTERVAL=NEVER
    ):
        first, second = (tasks.gauged.enqueue(0.05).id for _ in range(2))
        with running(oxpull_worker(concurrency=2, poll_interval=5)):
            assert wait_for(lambda: finished([first, second]), 20)

    rows = {str(row.pk): row for row in OxTask.objects.filter(pk__in=[first, second])}
    earlier, later = sorted(rows.values(), key=lambda row: row.started_at)
    assert (later.started_at - earlier.finished_at) < timedelta(seconds=1)


# -- the bound across real ox_worker processes -----------------------------


def process_env(tmp_path):
    (tmp_path / "concurrency_settings.py").write_text(
        "import copy\n"
        f"from {settings.SETTINGS_MODULE} import *  # noqa: F403\n"
        "TASKS = copy.deepcopy(TASKS)  # noqa: F405\n"
        "TASKS['default']['OPTIONS']['WORKER_CLASS'] = 'oxpull.worker.OxpullWorker'\n"
        "TASKS['default']['OPTIONS']['CONCURRENCY_LIMITS'] = "
        f"{{'g': {{'max': 1, 'tasks': [{GAUGED!r}]}}}}\n"
        "TASKS['flows_ungated']['OPTIONS']['WORKFLOWS'] = False\n"
    )
    name = connection.settings_dict["NAME"]
    if connection.vendor == "sqlite":
        name = Path(name).resolve()
    return {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "concurrency_settings",
        "PYTHONPATH": os.pathsep.join(
            [str(SRC), django_ox_path(), str(tmp_path), str(REPO_ROOT)]
        ),
        "OX_TEST_DB_NAME": str(name),
    }


@pytest.mark.django_db(transaction=True)
def test_two_ox_worker_processes_stay_within_max_plus_one(tmp_path):
    """
    `ox_worker --processes 2 --concurrency 4` with a ceiling of one. Sampled
    covered RUNNING rows stay at or below max + W - 1 = 2, where a limit that
    did nothing would let eight run.
    """
    if connection.vendor == "sqlite" and connection.settings_dict["NAME"] in {
        "",
        ":memory:",
    }:
        pytest.skip("another process cannot reach an in-memory database")
    covered = [tasks.gauged.enqueue(0.2).id for _ in range(16)]
    log_path = tmp_path / "ox_worker.log"
    peak = 0
    with log_path.open("w") as log:
        supervisor = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "django",
                "ox_worker",
                "--processes",
                "2",
                "--concurrency",
                "4",
                "--interval",
                "0.05",
            ],
            env=process_env(tmp_path),
            cwd=REPO_ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and supervisor.poll() is None:
                now = OxTask.objects.filter(
                    pk__in=covered, status=OxTask.Status.RUNNING
                ).count()
                peak = max(peak, now)
                if finished(covered):
                    break
                time.sleep(0.01)
        finally:
            if supervisor.poll() is None:
                supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=30)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                supervisor.wait()
    text = log_path.read_text()
    assert finished(covered), text[-3000:]
    assert 1 <= peak <= 2, text[-3000:]
