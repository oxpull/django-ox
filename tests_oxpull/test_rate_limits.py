"""
Rate-limit configuration, admission and shared-window behavior.

The load-bearing tests are the ones a no-op limiter fails. Throughput is
asserted between a floor and a ceiling, so a limiter that blocks nothing
and a limiter that blocks everything both fail. A throttled task is
checked for the attempt and the lease it must not have spent, because the
claim UPDATE writes both and neither can be given back. And the exclusion
is proved on each claim path in turn: reverting either hook has to break
this file on the databases that take it.
"""

import logging
import re
import time
from contextlib import contextmanager
from datetime import timedelta
from functools import partial

import pytest
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.db import DatabaseError, IntegrityError, connections
from django.db.models import F, QuerySet
from django.db.models.functions import Now
from django.tasks import TaskResultStatus
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import batches
from oxpull.backend import OxpullBackend
from oxpull.models import OxpullBatch, OxpullRateWindow
from oxpull.rate_limits import (
    MAX_LIMIT_NAME_LENGTH,
    RateLimit,
    rate_limits_from_options,
    refresh_interval_from_options,
)
from oxpull.worker import MAX_COUNT_FAILURES, MAX_RATE_ATTEMPTS, OxpullWorker

from . import tasks
from .racing import THREADS, race, race_all
from .workers import admissions, run_workers

pytestmark = pytest.mark.django_db

ECHO = "tests_oxpull.tasks.plain_echo"
FAIL = "tests_oxpull.tasks.plain_flaky"
WINDOW_TABLE = OxpullRateWindow._meta.db_table


@contextmanager
def rate_limits(config, **options):
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
                    "RATE_LIMITS": config,
                    **options,
                },
            },
        }
    ):
        yield


def oxpull_worker(**kwargs):
    return OxpullWorker(backoff_initial=0, poll_interval=0.01, **kwargs)


def open_window(name, *, age=0):
    """Put a limit's window at a known age on the database's own clock."""
    OxpullRateWindow.objects.update_or_create(
        limit_name=name,
        defaults={
            "window_id": 0,
            "used": 0,
            "window_started_at": Now() - timedelta(seconds=age),
        },
    )
    return OxpullRateWindow.objects.get(limit_name=name)


def window(name):
    return OxpullRateWindow.objects.get(limit_name=name)


@contextmanager
def captured_sql(alias):
    statements = []

    def wrapper(execute, sql, params, many, context):
        statements.append(sql)
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(wrapper):
        yield statements


# The savepoint counter increments once per atomic block in the process,
# so two runs of the same code path name their savepoints differently. It
# is the only part of a statement that is not a function of the path.
SAVEPOINT_NAME = re.compile(r"(SAVEPOINT) [`\"]?\w+[`\"]?")


def without_savepoint_names(statements):
    return [SAVEPOINT_NAME.sub(r"\1 <n>", sql) for sql in statements]


def refresh_selects(statements):
    return [
        sql
        for sql in statements
        if sql.lstrip().upper().startswith("SELECT") and WINDOW_TABLE in sql
    ]


def failing_count():
    """
    A QuerySet.update that refuses the limiter's increment and its probe.

    Both are the one-column write on the window row, which is what the
    fail-closed path counts; everything else, the seed and the roll
    included, goes through. Set `.raising = False` to let the database
    work again.
    """
    real_update = QuerySet.update

    def failing(self, **kwargs):
        if (
            failing.raising
            and self.model is OxpullRateWindow
            and set(kwargs) == {"used"}
        ):
            raise DatabaseError("no room on device")
        return real_update(self, **kwargs)

    failing.raising = True
    return failing


def alternating_count():
    """
    An increment that raises on one admission and matches nothing on the
    next, so no admission is ever recorded and no statement ever fails
    twice running. "Matches nothing" is what the increment returns when
    the window rolled between the refresh and the write, and the worker
    re-reads and tries again, MAX_RATE_ATTEMPTS times, so one admission
    in that mode is that many zero rowcounts. The probe raises, so a
    limit that closes stays closed.
    """
    real_update = QuerySet.update
    state = {"mode": "raise", "zeros": 0}

    def alternating(self, **kwargs):
        if self.model is not OxpullRateWindow or set(kwargs) != {"used"}:
            return real_update(self, **kwargs)
        if isinstance(kwargs["used"], F):
            # The reopen probe, used=F("used"): not an increment.
            raise DatabaseError("no room on device")
        if state["mode"] == "raise":
            state["mode"], state["zeros"] = "zero", 0
            raise DatabaseError("no room on device")
        state["zeros"] += 1
        if state["zeros"] == MAX_RATE_ATTEMPTS:
            state["mode"] = "raise"
        return 0

    return alternating


# -- declaring limits --------------------------------------------------


def test_a_limit_is_read_from_the_backend_that_configures_it():
    """
    OPTIONS is the only declaration site. A limit belongs to the alias
    whose OPTIONS carry it, which is what lets one backend be limited
    while another is not.
    """
    backend = settings.TASKS["limited"]

    limits = rate_limits_from_options(backend["OPTIONS"], "limited", backend["QUEUES"])

    assert limits == {
        "hourly": RateLimit(
            name="hourly",
            limit=1,
            per=3600,
            task_paths=frozenset(
                {"tests_oxpull.tasks.limited_echo", "tests_oxpull.tasks.limited_add"}
            ),
            queues=frozenset(),
        )
    }


def test_default_backend_has_no_limits():
    backend = settings.TASKS["default"]
    assert (
        rate_limits_from_options(backend["OPTIONS"], "default", backend["QUEUES"]) == {}
    )


def test_a_name_too_long_for_its_column_is_refused():
    """
    limit_name is the window table's primary key. An over-long name is a
    DataError at the first admission on PostgreSQL and MySQL and silently
    fine on SQLite, so it is refused where the configuration is read.
    """
    name = "s" * (MAX_LIMIT_NAME_LENGTH + 1)

    with pytest.raises(ImproperlyConfigured, match="characters"):
        rate_limits_from_options(
            {"RATE_LIMITS": {name: {"limit": 5, "per": 60, "tasks": [ECHO]}}},
            "default",
            ["default"],
        )


def test_a_name_the_length_of_its_column_is_accepted():
    name = "s" * MAX_LIMIT_NAME_LENGTH
    limits = rate_limits_from_options(
        {"RATE_LIMITS": {name: {"limit": 5, "per": 60, "tasks": [ECHO]}}},
        "default",
        ["default"],
    )
    assert set(limits) == {name}


def test_an_empty_limit_name_is_refused():
    with pytest.raises(ImproperlyConfigured, match="cannot be empty"):
        rate_limits_from_options(
            {"RATE_LIMITS": {"": {"limit": 5, "per": 60, "tasks": [ECHO]}}},
            "default",
            ["default"],
        )


@pytest.mark.parametrize(
    "value",
    ["soon", None, -1, True, [0.25], float("inf"), float("nan"), 10**1000, -(10**1000)],
)
def test_a_refresh_interval_that_is_not_a_number_is_refused(value):
    """
    The interval is validated where the configuration is read, so
    `manage.py check` reports a bad value rather than the first worker of
    a deploy raising at startup. inf and nan are floats that compare as
    "not below zero", so a numeric check alone lets them through: inf
    would stop the worker ever refreshing its view, and nan would fail
    every comparison the refresh schedule makes.
    """
    with pytest.raises(ImproperlyConfigured, match="RATE_REFRESH_INTERVAL"):
        refresh_interval_from_options({"RATE_REFRESH_INTERVAL": value})


def test_the_refresh_interval_defaults_and_accepts_a_number():
    assert refresh_interval_from_options({}) == 0.25
    assert refresh_interval_from_options({"RATE_REFRESH_INTERVAL": 1}) == 1.0


def test_period_accepts_a_timedelta():
    limits = rate_limits_from_options(
        {
            "RATE_LIMITS": {
                "a": {"limit": 5, "per": timedelta(minutes=2), "queues": ["default"]}
            }
        },
        "default",
        ["default"],
    )
    assert limits["a"].per == 120


@pytest.mark.parametrize(
    "config",
    [
        {"a": {"limit": 5, "per": 0.5, "tasks": [ECHO]}},
        {"a": {"limit": 5, "per": 0, "tasks": [ECHO]}},
        {"a": {"limit": -1, "per": 60, "tasks": [ECHO]}},
        {"a": {"limit": 5, "tasks": [ECHO]}},
        {"a": {"per": 60, "tasks": [ECHO]}},
        {"a": {"limit": 5, "per": 60}},
        {"a": {"limit": 5, "per": 60, "tasks": ECHO}},
        {"a": {"limit": 5, "per": 60, "queues": ["nowhere"]}},
        {"a": {"limit": 5, "per": 60, "rate": 1}},
        {"a": "not a dict"},
    ],
)
def test_malformed_config_is_refused(config):
    with pytest.raises(ImproperlyConfigured):
        rate_limits_from_options({"RATE_LIMITS": config}, "default", ["default"])


def test_rate_limits_must_be_a_dict():
    with pytest.raises(ImproperlyConfigured):
        rate_limits_from_options({"RATE_LIMITS": [ECHO]}, "default", ["default"])


# -- the system checks -------------------------------------------------


def test_check_reports_malformed_limits():
    backend = OxpullBackend(
        "default", {"OPTIONS": {"RATE_LIMITS": {"a": {"limit": 5, "per": 0}}}}
    )
    assert "oxpull.E005" in [message.id for message in backend.check()]


def test_check_refuses_limits_without_a_worker_that_applies_them():
    with override_settings(
        TASKS={
            **settings.TASKS,
            "default": {
                **settings.TASKS["default"],
                "OPTIONS": {
                    "RATE_LIMITS": {"a": {"limit": 5, "per": 60, "tasks": [ECHO]}}
                },
            },
        }
    ):
        backend = OxpullBackend("default", settings.TASKS["default"])
        errors = [message for message in backend.check() if message.id == "oxpull.E006"]

    assert len(errors) == 1
    assert "django_ox.worker.Worker" in errors[0].msg


def test_check_accepts_limits_with_the_oxpull_worker():
    with rate_limits({"a": {"limit": 5, "per": 60, "tasks": [ECHO]}}):
        backend = OxpullBackend("default", settings.TASKS["default"])
        assert backend.check() == []


def test_check_reports_a_refresh_interval_that_is_not_a_number():
    with rate_limits(
        {"a": {"limit": 5, "per": 60, "tasks": [ECHO]}}, RATE_REFRESH_INTERVAL="soon"
    ):
        backend = OxpullBackend("default", settings.TASKS["default"])
        assert [message.id for message in backend.check()] == ["oxpull.E005"]


def test_check_reports_a_limited_task_path_that_is_not_a_task():
    """
    A limit matches on the stored task path, so a typo covers nothing and
    throttles nothing while the queue looks healthy.
    """
    with rate_limits(
        {"a": {"limit": 5, "per": 60, "tasks": ["tests_oxpull.tasks.nope"]}}
    ):
        backend = OxpullBackend("default", settings.TASKS["default"])
        errors = backend.check()

    assert [message.id for message in errors] == ["oxpull.E005"]
    assert "tests_oxpull.tasks.nope" in errors[0].msg


def test_check_reports_a_worker_class_that_is_not_a_worker():
    with rate_limits(
        {"a": {"limit": 5, "per": 60, "tasks": [ECHO]}},
        WORKER_CLASS="oxpull.models.OxpullRateWindow",
    ):
        backend = OxpullBackend("default", settings.TASKS["default"])
        assert [m.id for m in backend.check()] == ["oxpull.E006"]


# -- zero cost when unused ---------------------------------------------


# The attribute a claim adds for a workflow node (oxpull.workflows.NODE_FLAG).
NODE_FLAG = "ox_wf_node"


def without_workflow_backends():
    """settings.TASKS without the backends that declare WORKFLOWS."""
    return override_settings(
        TASKS={
            alias: config
            for alias, config in settings.TASKS.items()
            if not config.get("OPTIONS", {}).get("WORKFLOWS")
        }
    )


def base_and_oxpull_statements(worker=None):
    """
    The statements a `django_ox.worker.Worker` and then a
    `oxpull.worker.OxpullWorker` run for one plain task.
    """
    worker = worker or oxpull_worker()
    alias = worker._db_alias
    tasks.plain_echo.enqueue("a")
    with captured_sql(alias) as base:
        assert Worker(backoff_initial=0, poll_interval=0.01).run_once() is True

    OxTask.objects.all().delete()
    tasks.plain_echo.enqueue("a")
    with captured_sql(alias) as oxpull_sql:
        assert worker.run_once() is True
    return without_savepoint_names(base), without_savepoint_names(oxpull_sql)


def test_no_limits_configured_emits_exactly_the_base_worker_sql():
    """
    The whole cost argument rests on this. With nothing configured, and no
    backend on the database declaring WORKFLOWS, the
    `oxpull.worker.OxpullWorker`'s statements must be the
    `django_ox.worker.Worker`'s statements, not statements that resemble them.
    """
    with without_workflow_backends():
        assert oxpull_worker()._limits == {}
        assert oxpull_worker()._records is False
        base, oxpull_sql = base_and_oxpull_statements()

    assert oxpull_sql == base
    assert refresh_selects(oxpull_sql) == []


def test_beside_a_workflows_backend_the_claim_also_says_whether_a_task_is_a_node():
    """
    Where any backend on the database declares WORKFLOWS, every
    `oxpull.worker.OxpullWorker` there records workflow outcomes, because a
    claim is scoped to queues and not to backends. That costs one column on the
    statement that returns the claimed row, and nothing else for a task that is
    not a node. A worker on a backend without workflows first looks once for
    the workflow node table, which is a cost per worker and not per claim, so
    it is taken here first.
    """
    worker = oxpull_worker()
    assert worker._records is True
    assert worker._recording() is True
    base, oxpull_sql = base_and_oxpull_statements(worker)

    assert len(oxpull_sql) == len(base)
    differing = [
        (base_line, oxpull_line)
        for base_line, oxpull_line in zip(base, oxpull_sql, strict=True)
        if base_line != oxpull_line
    ]
    assert len(differing) == 1, differing
    assert NODE_FLAG not in differing[0][0]
    assert NODE_FLAG in differing[0][1]


# -- the guarantee -----------------------------------------------------


LIMIT = 10
WORKERS = 4


@pytest.mark.django_db(transaction=True)
def test_limit_caps_throughput_over_one_window():
    """
    The marquee test. A no-op limiter finishes all 100 tasks and fails the
    ceiling by an order of magnitude; a limiter that blocks everything
    fails the floor.

    The window outlasts any run, so every admission is counted against the
    one budget, and the workers run until all of them have stopped taking
    tasks, so the count is what the limiter let through and not what fitted
    into a number of seconds.
    """
    with rate_limits({"echo": {"limit": LIMIT, "per": 600, "tasks": [ECHO]}}):
        for index in range(100):
            tasks.plain_echo.enqueue(index)
        open_window("echo")

        claims = run_workers(WORKERS, seconds=60, until_idle=True)

    admitted = len(admissions(claims))
    assert LIMIT <= admitted <= LIMIT + WORKERS - 1
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 100 - admitted


@pytest.mark.django_db(transaction=True)
class BoundedOxpullWorker(OxpullWorker):
    """The production worker with a run loop that ends, so a test can run it."""

    def run(self) -> None:
        while self.run_once():
            pass


def test_the_real_command_applies_a_configured_limit():
    """
    End to end through ox_worker, not through OxpullWorker directly.

    Every other test here builds the worker itself, so all of them would
    still pass if a configured limit never reached the command that runs
    in production: a limit declared, a clean system check, and nothing
    throttled. The command resolves WORKER_CLASS from the same settings
    the limit is declared in, and only this test proves the two meet.

    The subclass replaces the run loop and nothing else, so the claim path
    under test is the shipped one.
    """
    limits = {"echo": {"limit": LIMIT, "per": 300, "tasks": [ECHO]}}
    with rate_limits(
        limits, WORKER_CLASS="tests_oxpull.test_rate_limits.BoundedOxpullWorker"
    ):
        for index in range(LIMIT + 5):
            tasks.plain_echo.enqueue(index)
        open_window("echo")

        with pytest.raises(SystemExit) as exit_code:
            call_command("ox_worker", "--processes=1", verbosity=0)

    assert exit_code.value.code == 0
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 5


@pytest.mark.django_db(transaction=True)
def test_the_next_window_is_what_admits_the_next_task():
    """
    The property in the time domain. One worker, because the guarantee is
    exact only with one: with more, the overshoot could put the (N+1)-th
    admission inside the first window and there would be nothing to
    measure.

    Every instant compared is one the database wrote: open_window and the
    roll both stamp window_started_at with Now(). The worker claims until it
    has spent a window and been admitted once more, however long the machine
    takes over that, so what bounds the run is what the limiter does rather
    than a number of seconds. One worker is also the only writer of the
    window row, so the row read after a claim is the row that admission
    left behind.
    """
    limit, period = 2, 1
    windows = []
    rolled = False
    with rate_limits({"echo": {"limit": limit, "per": period, "tasks": [ECHO]}}):
        for index in range(20):
            tasks.plain_echo.enqueue(index)
        open_window("echo")
        worker = oxpull_worker()

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            db_task = worker.claim_one()
            if db_task is None:
                time.sleep(0.005)
                continue
            windows.append(window("echo"))
            worker.execute(db_task)
            if len(windows) > 1 and windows[-2].used >= limit:
                rolled = True
                break

    assert rolled, "the worker never spent a window and was admitted again"
    spent, following = windows[-2:]
    assert spent.used == limit
    # The admission after a spent window is the first of the next one, and
    # that window opened no sooner than a period after the spent one did.
    assert (following.window_id, following.used) == (spent.window_id + 1, 1)
    assert following.window_started_at - spent.window_started_at >= timedelta(
        seconds=period
    )


@pytest.mark.django_db(transaction=True)
def test_racing_the_roll_opens_exactly_one_window():
    """
    Eight workers find the same window a second past its period and race to
    open the next. The period is one no run outlasts, so the roll they race
    for is the only one the database's clock allows, and a second window
    can only be a second winner. The budget covers every task, so the run
    ends on the empty queue rather than on a clock.
    """
    with rate_limits({"echo": {"limit": 100, "per": 600, "tasks": [ECHO]}}):
        for index in range(8):
            tasks.plain_echo.enqueue(index)
        open_window("echo", age=601)

        claims = run_workers(8, seconds=30, until_idle=True)

        row = window("echo")

    assert row.window_id == 1
    assert row.used == len(admissions(claims))


@pytest.mark.django_db(transaction=True)
def test_racing_the_last_token_overshoots_by_at_most_one_per_worker():
    """
    Both workers that read the last token get it; the increment is
    deliberately unconditional, because a compare-and-set that could
    refuse would run after the attempt was already spent. What no worker
    can do is take a second one. If this is ever tightened to == 1 the
    published guarantee has been rewritten.
    """
    workers = 8
    with rate_limits({"echo": {"limit": 1, "per": 600, "tasks": [ECHO]}}):
        for index in range(20):
            tasks.plain_echo.enqueue(index)
        open_window("echo")

        first = len(admissions(run_workers(workers, seconds=0.5)))
        second = len(admissions(run_workers(workers, seconds=0.5)))

    assert 1 <= first <= workers
    assert second == 0


@pytest.mark.django_db(transaction=True)
def test_exactly_one_racer_opens_the_window():
    """
    The roll's compare-and-set, on its own. The winner moves window_id, so
    every loser's predicate stops matching and its rowcount is zero.
    """
    with rate_limits({"echo": {"limit": 5, "per": 1, "tasks": [ECHO]}}):
        open_window("echo", age=5)
        workers = [oxpull_worker() for _ in range(THREADS)]

        results, errors = race_all(
            [partial(worker._roll, "echo", 0, 1) for worker in workers]
        )

        row = window("echo")

    assert not errors
    assert sum(results) == 1
    assert (row.window_id, row.used) == (1, 0)


@pytest.mark.django_db(transaction=True)
def test_racing_increments_lose_no_count():
    """
    The increment is what the bound's arithmetic counts, so it has to be
    exact under contention even though it is never a gate.
    """
    with rate_limits({"echo": {"limit": 100, "per": 600, "tasks": [ECHO]}}):
        open_window("echo")
        db_task = OxTask.objects.create(
            task_path=ECHO, queue_name="default", enqueued_at=timezone.now()
        )

        def count():
            worker = oxpull_worker()
            worker._refresh_view()
            worker._count("echo", db_task)

        _, errors = race(count)

        row = window("echo")

    assert not errors
    assert row.used == THREADS


@pytest.mark.django_db(transaction=True)
def test_an_unchanged_update_still_reports_a_rowcount():
    """
    Every decision here is a rowcount, and on MySQL that rests on Django
    connecting with CLIENT_FOUND_ROWS: matched rows, not changed rows.
    That is a connection flag, not a property of the SQL.
    """
    OxpullRateWindow.objects.create(
        limit_name="echo", window_started_at=timezone.now(), used=3
    )
    assert OxpullRateWindow.objects.filter(limit_name="echo").update(used=3) == 1


# -- what a throttled task must not cost -------------------------------


def test_a_throttled_task_burns_no_attempt_and_no_lease():
    with rate_limits({"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()

        for _ in range(5):
            assert worker.run_once() is False

        assert worker.reap() == 0

    row = OxTask.objects.get()
    assert row.status == OxTask.Status.READY
    assert row.attempts == 0
    assert row.lease_epoch == 0
    assert row.worker_ids == []
    assert row.locked_by is None
    assert row.locked_at is None
    assert row.started_at is None


def test_a_blocked_path_does_not_block_the_head_of_the_queue():
    with rate_limits({"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}}):
        for index in range(50):
            tasks.plain_echo.using(priority=10).enqueue(index)
        runnable = tasks.emails_echo.using(priority=0).enqueue("run me")

        assert oxpull_worker().run_once() is True

    assert OxTask.objects.get(id=runnable.id).status == OxTask.Status.SUCCESSFUL
    assert OxTask.objects.filter(status=OxTask.Status.READY).count() == 50


def test_a_blocked_queue_does_not_block_another_queue():
    with rate_limits({"emails": {"limit": 0, "per": 600, "queues": ["emails"]}}):
        for index in range(50):
            tasks.emails_echo.using(priority=10).enqueue(index)
        runnable = tasks.plain_echo.using(priority=0).enqueue("run me")

        assert oxpull_worker().run_once() is True

    assert OxTask.objects.get(id=runnable.id).status == OxTask.Status.SUCCESSFUL


# -- windows -----------------------------------------------------------


def test_the_window_rolls_after_its_period():
    with rate_limits({"echo": {"limit": 1, "per": 1, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        tasks.plain_echo.enqueue("b")
        worker = oxpull_worker()

        assert worker.run_once() is True
        assert worker.run_once() is False
        assert window("echo").used == 1

        time.sleep(1.1)
        assert worker.run_once() is True

    row = window("echo")
    assert row.window_id == 1
    assert row.used == 1


def test_a_retry_consumes_another_admission():
    with rate_limits({"flaky": {"limit": 5, "per": 600, "tasks": [FAIL]}}):
        tasks.plain_flaky.enqueue(2)
        worker = oxpull_worker()

        assert worker.run_once() is True  # attempt 1 fails and requeues
        assert OxTask.objects.get().status == OxTask.Status.READY
        assert window("flaky").used == 1

        assert worker.run_once() is True

    assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
    assert window("flaky").used == 2


def test_a_task_under_two_limits_needs_both_and_counts_both():
    config = {
        "by-path": {"limit": 5, "per": 600, "tasks": [ECHO]},
        "by-queue": {"limit": 0, "per": 600, "queues": ["default"]},
    }
    with rate_limits(config):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()

        assert worker.run_once() is False
        assert window("by-path").used == 0

        OxpullRateWindow.objects.filter(limit_name="by-queue").delete()
        with rate_limits(
            {**config, "by-queue": {"limit": 5, "per": 600, "queues": ["default"]}}
        ):
            assert oxpull_worker().run_once() is True

    assert window("by-path").used == 1
    assert window("by-queue").used == 1


def test_a_limit_the_worker_has_never_seen_is_seeded_lazily():
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        assert OxpullRateWindow.objects.count() == 0
        tasks.plain_echo.enqueue("a")

        assert oxpull_worker().run_once() is True

    row = window("echo")
    assert (row.window_id, row.used) == (0, 1)


def test_a_blocked_worker_refreshes_no_faster_than_its_cooldown():
    """
    run() re-enters the claim on every task completion, not on the poll
    interval, so without the cooldown a worker with fast in-flight work
    would issue a refresh SELECT per completion.
    """
    interval = 0.25
    config = {
        "blocked": {"limit": 0, "per": 600, "tasks": [ECHO]},
        # A second, open limit, so the worker takes the cooldown rather
        # than the every-limit-blocked path that sleeps to the window end.
        "open": {"limit": 1000, "per": 600, "queues": ["emails"]},
    }
    with rate_limits(config, RATE_REFRESH_INTERVAL=interval):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()
        alias = worker._db_alias

        with captured_sql(alias) as statements:
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                assert worker.claim_one() is None

    assert len(refresh_selects(statements)) <= 1 / interval + 1


def test_the_postgresql_fragment_binds_one_array_per_column():
    """
    The fragment that reaches the raw claim, checked on every database
    rather than only where it is used: it is the exclusion that a test
    running elsewhere cannot see, and the array is one bound parameter
    however many names it holds.
    """
    config = {
        "echo": {"limit": 0, "per": 600, "tasks": [ECHO]},
        "emails": {"limit": 0, "per": 600, "queues": ["emails"]},
    }
    with rate_limits(config):
        worker = oxpull_worker()
        assert worker.claim_filter_sql() == ("", {})

        worker._refresh_view()
        fragment, params = worker.claim_filter_sql()

    assert fragment == (
        ' AND "task_path" <> ALL(%(ox_blocked_paths)s)'
        ' AND "queue_name" <> ALL(%(ox_blocked_queues)s)'
    )
    assert params == {"ox_blocked_paths": [ECHO], "ox_blocked_queues": ["emails"]}


def test_an_increment_that_raises_lets_the_task_run(monkeypatch, caplog):
    """
    The claim is already committed by the time the count runs. Losing the
    count is better than losing the task.
    """
    monkeypatch.setattr(QuerySet, "update", failing_count())
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")

        with caplog.at_level(logging.WARNING, logger="oxpull"):
            assert oxpull_worker().run_once() is True

    assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL
    assert "runs uncounted" in caplog.text
    assert window("echo").used == 0


def test_a_counter_that_keeps_failing_stops_admitting(monkeypatch, caplog):
    """
    A counter that cannot be written leaves `used` at zero, so every
    refresh reads room that is not there and a worker would admit the
    whole backlog at full speed with a warning per task. A run of
    failures closes the limit instead, and it closes once.
    """
    monkeypatch.setattr(QuerySet, "update", failing_count())
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        for index in range(40):
            tasks.plain_echo.enqueue(index)
        worker = oxpull_worker()

        with caplog.at_level(logging.WARNING, logger="oxpull"):
            admitted = sum(worker.run_once() for _ in range(40))

    assert admitted == MAX_COUNT_FAILURES
    assert (
        OxTask.objects.filter(status=OxTask.Status.READY).count()
        == 40 - MAX_COUNT_FAILURES
    )
    assert "admitting nothing under it" in caplog.text
    # Once, on the transition, not once per refused claim.
    assert caplog.text.count("admitting nothing under it") == 1


def test_a_count_that_matched_nothing_does_not_end_the_run_of_failures(
    monkeypatch, caplog
):
    """
    The run that closes a limit is a run of admissions that were not
    recorded. An increment that raised is one; an increment that ran and
    matched no row is another, because the admission is no more on the
    counter than if the statement had failed. Any statement that reached
    the database used to end the run, so a counter that alternated
    between raising and matching nothing never closed its limit and the
    worker admitted the whole backlog at full speed, one warning per
    task. Now the run ends only when an increment lands.
    """
    monkeypatch.setattr(QuerySet, "update", alternating_count())
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        for index in range(40):
            tasks.plain_echo.enqueue(index)
        worker = oxpull_worker()

        with caplog.at_level(logging.WARNING, logger="oxpull"):
            admitted = sum(worker.run_once() for _ in range(40))

    assert admitted == MAX_COUNT_FAILURES
    assert (
        OxTask.objects.filter(status=OxTask.Status.READY).count()
        == 40 - MAX_COUNT_FAILURES
    )
    assert caplog.text.count("admitting nothing under it") == 1


def test_a_closed_limit_admits_again_once_its_counter_takes_a_write(monkeypatch):
    """
    Fail-closed has to be recoverable without a worker restart. A closed
    limit admits nothing, so nothing else would ever run a count: the
    refresh probes the row and reopens the limit when the write lands.
    """
    failing = failing_count()
    monkeypatch.setattr(QuerySet, "update", failing)
    # No cooldown, so the probe rides the next claim rather than the next
    # quarter second. What paces the probe is covered by its own test.
    with rate_limits(
        {"echo": {"limit": 20, "per": 600, "tasks": [ECHO]}}, RATE_REFRESH_INTERVAL=0
    ):
        for index in range(20):
            tasks.plain_echo.enqueue(index)
        worker = oxpull_worker()

        while worker.run_once():
            pass
        assert OxTask.objects.filter(status=OxTask.Status.READY).count() > 0

        failing.raising = False
        assert worker.run_once() is True

    assert window("echo").used == 1


def failing_window_selects(armed):
    """
    An execute wrapper that refuses every SELECT on the window table
    while `armed["fail"]` is set. Everything else goes through.
    """

    def wrapper(execute, sql, params, many, context):
        if (
            armed["fail"]
            and sql.lstrip().upper().startswith("SELECT")
            and WINDOW_TABLE in sql
        ):
            raise DatabaseError("connection reset by peer")
        return execute(sql, params, many, context)

    return wrapper


def test_a_refresh_that_fails_after_the_claim_still_hands_the_task_over(
    monkeypatch, caplog
):
    """
    Everything after the claim is accounting, and accounting fails open:
    the claim is committed and the attempt is spent, so the task must
    run whether or not its count lands. The increment was already
    covered. The re-read that follows an increment that matched no row
    was not: it ran outside the handler, so a database fault there
    escaped claim_one() after the claim had landed. The poll loop
    contained the error but never received the task, which sat RUNNING
    until the reaper took it back, one attempt poorer, and LOST if that
    attempt was its last.

    The sequence: refresh, claim, another worker rolls the window, the
    increment matches nothing, the re-read fails. The claimed task comes
    back, the loss is logged, and the task runs.
    """
    armed = {"fail": False}
    real_claim = Worker.claim_one

    def claim_then_lose_the_window(self):
        db_task = real_claim(self)
        # Another worker rolls the window the moment the claim lands,
        # and the database goes away before this worker can re-read it.
        OxpullRateWindow.objects.filter(limit_name="echo").update(
            window_id=F("window_id") + 1, used=0, window_started_at=Now()
        )
        armed["fail"] = True
        return db_task

    monkeypatch.setattr(Worker, "claim_one", claim_then_lose_the_window)
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()

        with (
            connections["default"].execute_wrapper(failing_window_selects(armed)),
            caplog.at_level(logging.WARNING, logger="oxpull"),
        ):
            db_task = worker.claim_one()

        assert db_task is not None
        assert OxTask.objects.get(id=db_task.id).status == OxTask.Status.RUNNING
        assert "runs uncounted" in caplog.text
        worker.execute(db_task, inline=True)

    assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL


def refusing_window_inserts(armed):
    """
    An execute wrapper that refuses every INSERT on the window table as a
    duplicate while `armed["refuse"]` is set: the seed finding a row in
    its way that the collation treats as the same name.
    """

    def wrapper(execute, sql, params, many, context):
        if (
            armed["refuse"]
            and sql.lstrip().upper().startswith("INSERT")
            and WINDOW_TABLE in sql
        ):
            raise IntegrityError("duplicate key")
        return execute(sql, params, many, context)

    return wrapper


def test_a_window_row_missing_after_the_claim_still_hands_the_task_over(
    monkeypatch, caplog
):
    """
    The re-read after an increment that matched no row can also find the
    limit's row gone: the seed's INSERT is refused as a duplicate and the
    read under the configured name returns nothing, which _adopt reports
    as ImproperlyConfigured. Before the claim that is the right answer
    and it propagates. After the claim it is accounting, and the handler
    around the re-read caught DatabaseError alone, so the configuration
    error escaped claim_one() with the task claimed and unreturned. The
    claimed task now comes back, the loss is logged, and the task runs.

    The sequence: refresh, claim, the row is deleted under the worker,
    the increment matches nothing, the re-read seeds and is refused,
    and the adopt read finds no row.
    """
    armed = {"refuse": False}
    real_claim = Worker.claim_one

    def claim_then_lose_the_row(self):
        db_task = real_claim(self)
        OxpullRateWindow.objects.filter(limit_name="echo").delete()
        armed["refuse"] = True
        return db_task

    monkeypatch.setattr(Worker, "claim_one", claim_then_lose_the_row)
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()

        with (
            connections["default"].execute_wrapper(refusing_window_inserts(armed)),
            caplog.at_level(logging.WARNING, logger="oxpull"),
        ):
            db_task = worker.claim_one()

        assert db_task is not None
        assert OxTask.objects.get(id=db_task.id).status == OxTask.Status.RUNNING
        assert "runs uncounted" in caplog.text
        assert "holds none that matches it" in caplog.text
        worker.execute(db_task, inline=True)

    assert OxTask.objects.get().status == OxTask.Status.SUCCESSFUL


def test_a_refresh_that_fails_before_the_claim_claims_nothing():
    """
    The other side of the line. Before the claim nothing is spent, so a
    database fault there fails closed: the error propagates, the poll
    loop skips the pass, and the task stays READY for the next one.
    """
    armed = {"fail": True}
    with rate_limits({"echo": {"limit": 5, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        worker = oxpull_worker()

        with (
            connections["default"].execute_wrapper(failing_window_selects(armed)),
            pytest.raises(DatabaseError),
        ):
            worker.claim_one()

    assert OxTask.objects.get().status == OxTask.Status.READY


# -- interaction with the other task-coordination features -------------


def test_a_throttled_unique_task_still_deduplicates():
    """
    A throttled task is pending, and @unique promises unique while
    pending, so throttling extends the dedup window. Releasing the lock
    instead would stack duplicates up behind the throttle.
    """
    path = "tests_oxpull.tasks.unique_echo"
    with rate_limits({"unique": {"limit": 0, "per": 600, "tasks": [path]}}):
        first = tasks.unique_echo.enqueue("a")
        assert oxpull_worker().run_once() is False

        second = tasks.unique_echo.enqueue("a")

    assert second.id == first.id
    assert OxTask.objects.count() == 1


def test_a_batch_waits_for_its_throttled_member():
    with rate_limits({"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}}):
        batch = batches.create(
            "throttled", on_complete=tasks.record_callback, on_complete_args=["done"]
        )
        batch.add(tasks.plain_echo, 1)
        batch.seal()

        assert oxpull_worker().run_once() is False
        assert batches.reconcile.call() == 0

    assert batch.status == OxpullBatch.Status.SEALED
    assert "done" not in tasks.STATE


def test_a_throttled_task_reads_as_backlog():
    """
    The ready-age metric climbs while work is throttled, because the work
    is queued and not running. A queue-age alert tuned before rate limits
    were introduced will fire on it.
    """
    from django_ox import stats

    with rate_limits({"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}}):
        tasks.plain_echo.enqueue("a")
        assert oxpull_worker().run_once() is False

        assert stats.ready_count() == 1
        assert stats.oldest_ready_age() is not None


def test_a_throttled_task_still_reports_as_ready_to_its_caller():
    with rate_limits({"echo": {"limit": 0, "per": 600, "tasks": [ECHO]}}):
        result = tasks.plain_echo.enqueue("a")
        assert oxpull_worker().run_once() is False

        result.refresh()
        assert result.status == TaskResultStatus.READY
