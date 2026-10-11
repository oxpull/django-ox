"""
Worker subclasses that narrow what they claim, on top of rate limits.

django-ox promises that a Worker subclass overriding claim_filter_q() alone
keeps its condition on every database. On PostgreSQL the worker gives up the
single-statement claim, which reads only claim_filter_sql(), for the
SELECT ... FOR UPDATE SKIP LOCKED path, which applies claim_filter_q().

A subclass of OxpullWorker narrows what it claims through those same hooks,
and the worker adds its rate limits to whatever they return. These tests hold
both halves. The rows a subclass leaves alone stay READY, and a limit admits
what it should whether or not the subclass calls super(). A subclass that
writes both hooks keeps the single statement, and so does one that writes
neither. The rows claimed are asserted on every database. The claim path is
asserted on PostgreSQL, the one database where it is a choice.
"""

import logging
from contextlib import contextmanager

import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.db.models import Q
from django.db.models.functions import Now
from django.test import override_settings

from django_ox.models import OxTask
from django_ox.worker import Worker, worker_class
from oxpull.models import OxpullRateWindow
from oxpull.worker import OxpullWorker

from . import tasks
from .workers import admissions, run_workers

pytestmark = pytest.mark.django_db

ECHO = "tests_oxpull.tasks.plain_echo"
LIMITED = "tests_oxpull.tasks.record_callback"
RESERVED_QUEUE = "emails"
RESERVED_PATH = "tests_oxpull.tasks.plain_flaky"
TASK_TABLE = OxTask._meta.db_table
NOTICE = "claim_filter_sql_missing"

SINGLE_STATEMENT = "single statement"
SELECT_FOR_UPDATE = "select for update"
COMPARE_AND_SET = "compare and set"


def _narrowed(inherited, mine):
    return mine if inherited is None else inherited & mine


class LeavesAQueue(OxpullWorker):
    """Leaves one queue to another pool, through claim_filter_q() alone."""

    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), ~Q(queue_name=RESERVED_QUEUE))


class LeavesATaskPath(OxpullWorker):
    """Leaves one task to another pool, through claim_filter_q() alone."""

    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), ~Q(task_path=RESERVED_PATH))


class LeavesAQueueMixin:
    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), ~Q(queue_name=RESERVED_QUEUE))


class LeavesAQueueThroughAMixin(LeavesAQueueMixin, OxpullWorker):
    """The same condition, supplied by a mixin listed ahead of OxpullWorker."""


class LeavesAQueueInBothHooks(OxpullWorker):
    """The same condition, written in both hooks, each through super()."""

    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), ~Q(queue_name=RESERVED_QUEUE))

    def claim_filter_sql(self):
        fragment, params = super().claim_filter_sql()
        return (
            fragment + ' AND "queue_name" <> %(reserved_queue)s',
            {**params, "reserved_queue": RESERVED_QUEUE},
        )


class LeavesAQueueWithoutSuper(OxpullWorker):
    """The queue condition, returned alone. It never calls super()."""

    def claim_filter_q(self):
        return ~Q(queue_name=RESERVED_QUEUE)


class LeavesAQueueFromAStaticMethod(OxpullWorker):
    """The queue condition, from a static method, as django-ox allows."""

    @staticmethod
    def claim_filter_q():
        return ~Q(queue_name=RESERVED_QUEUE)


class LeavesAQueueInBothHooksWithoutSuper(OxpullWorker):
    """The queue condition in both hooks, neither calling super()."""

    def claim_filter_q(self):
        return ~Q(queue_name=RESERVED_QUEUE)

    def claim_filter_sql(self):
        return ' AND "queue_name" <> %(reserved_queue)s', {
            "reserved_queue": RESERVED_QUEUE
        }


class ClaimsOnlyTheDefaultQueue(OxpullWorker):
    """An allow-list rather than an exclusion, through claim_filter_q() alone."""

    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), Q(queue_name="default"))


class OverridesNeitherHook(OxpullWorker):
    """Changes how it runs and leaves both claim filter hooks alone."""

    def run(self):
        return super().run()


class PlainLeavesAQueue(Worker):
    """django-ox's own Worker with the same override, to compare claim costs."""

    def claim_filter_q(self):
        return _narrowed(super().claim_filter_q(), ~Q(queue_name=RESERVED_QUEUE))


class _RunsUntilIdle:
    """A run loop that ends, so the real command returns. The claim is untouched."""

    def run(self):
        while self.run_once():
            pass


class LeavesAQueueUntilIdle(_RunsUntilIdle, LeavesAQueue):
    pass


class LeavesATaskPathUntilIdle(_RunsUntilIdle, LeavesATaskPath):
    pass


class LeavesAQueueInBothHooksUntilIdle(_RunsUntilIdle, LeavesAQueueInBothHooks):
    pass


class LeavesAQueueWithoutSuperUntilIdle(_RunsUntilIdle, LeavesAQueueWithoutSuper):
    pass


class LeavesAQueueInBothHooksWithoutSuperUntilIdle(
    _RunsUntilIdle, LeavesAQueueInBothHooksWithoutSuper
):
    pass


# class, what it excludes, whether it writes claim_filter_sql() too
SHAPES = {
    "queue-q-only": (LeavesAQueue, "queue", False),
    "path-q-only": (LeavesATaskPath, "path", False),
    "queue-q-mixin": (LeavesAQueueThroughAMixin, "queue", False),
    "queue-q-no-super": (LeavesAQueueWithoutSuper, "queue", False),
    "queue-q-staticmethod": (LeavesAQueueFromAStaticMethod, "queue", False),
    "allow-list-q-only": (ClaimsOnlyTheDefaultQueue, "queue", False),
    "queue-both-hooks": (LeavesAQueueInBothHooks, "queue", True),
    "queue-both-hooks-no-super": (LeavesAQueueInBothHooksWithoutSuper, "queue", True),
}

COMMAND_SHAPES = {
    "queue-q-only": (LeavesAQueueUntilIdle, "queue", False),
    "path-q-only": (LeavesATaskPathUntilIdle, "path", False),
    "queue-q-no-super": (LeavesAQueueWithoutSuperUntilIdle, "queue", False),
    "queue-both-hooks": (LeavesAQueueInBothHooksUntilIdle, "queue", True),
    "queue-both-hooks-no-super": (
        LeavesAQueueInBothHooksWithoutSuperUntilIdle,
        "queue",
        True,
    ),
}


def one_per_hour(limit):
    return {"spend": {"limit": limit, "per": 3600, "tasks": [LIMITED]}}


@contextmanager
def configured(worker, limits=None):
    """The default backend, running this worker class, with these limits."""
    default = settings.TASKS["default"]
    options = {
        **default["OPTIONS"],
        "WORKER_CLASS": f"{worker.__module__}.{worker.__qualname__}",
    }
    if limits is not None:
        options["RATE_LIMITS"] = limits
    with override_settings(
        TASKS={**settings.TASKS, "default": {**default, "OPTIONS": options}}
    ):
        yield


def built():
    """The worker ox_worker would build from WORKER_CLASS."""
    return worker_class("default")(backoff_initial=0, poll_interval=0.01)


@contextmanager
def captured(alias):
    statements = []

    def wrapper(execute, sql, params, many, context):
        statements.append(sql)
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(wrapper):
        yield statements


def on_the_task_table(statements):
    return [" ".join(sql.split()) for sql in statements if TASK_TABLE in sql]


def claim_path(statements):
    upper = [sql.upper() for sql in on_the_task_table(statements)]
    if any(sql.startswith("UPDATE") and "RETURNING" in sql for sql in upper):
        return SINGLE_STATEMENT
    if any(
        sql.startswith("SELECT") and "FOR UPDATE SKIP LOCKED" in sql for sql in upper
    ):
        return SELECT_FOR_UPDATE
    return COMPARE_AND_SET


def enqueue_excluded(kind):
    if kind == "queue":
        return tasks.emails_echo.using(priority=100).enqueue("excluded")
    return tasks.plain_flaky.using(priority=100).enqueue(1)


def drain(worker):
    """Claim until the worker finds nothing, recording each row and its path."""
    claims = []
    for _ in range(10):
        with captured(worker._db_alias) as statements:
            row = worker.claim_one()
        if row is None:
            return claims
        claims.append((str(row.id), claim_path(statements)))
    raise AssertionError("still claiming after more rows than the test enqueued")


def notices(caplog):
    return [r for r in caplog.records if getattr(r, "event", None) == NOTICE]


def used():
    return dict(OxpullRateWindow.objects.values_list("limit_name", "used"))


def on_postgresql():
    return connections["default"].vendor == "postgresql"


def assert_untouched(result):
    row = OxTask.objects.get(id=result.id)
    assert (row.status, row.attempts, row.lease_epoch) == (OxTask.Status.READY, 0, 0)


# limits, labels claimed in order, window counts afterwards
STATES = {
    "no-limits": (None, ["limited", "limited", "allowed"], {}),
    "limit-not-spent": (
        one_per_hour(100),
        ["limited", "limited", "allowed"],
        {"spend": 2},
    ),
    "limit-spent-by-the-claims": (
        one_per_hour(1),
        ["limited", "allowed"],
        {"spend": 1},
    ),
    "limit-spent-before-start": (one_per_hour(1), ["allowed"], {"spend": 1}),
}


@pytest.mark.parametrize("state", list(STATES))
@pytest.mark.parametrize("shape", list(SHAPES))
def test_a_narrowing_subclass_claims_only_what_it_allows(shape, state, caplog):
    cls, kind, writes_sql = SHAPES[shape]
    limits, expected, counts = STATES[state]
    with configured(cls, limits), caplog.at_level(logging.WARNING, logger="django_ox"):
        if state == "limit-spent-before-start":
            # A real admission by another worker, then a fresh worker, as
            # after a restart or a deploy inside the window.
            spend = tasks.record_callback.enqueue("spend")
            assert built().run_once() is True
            assert OxTask.objects.get(id=spend.id).status == OxTask.Status.SUCCESSFUL
            caplog.clear()
        worker = built()
        assert type(worker) is cls
        excluded = enqueue_excluded(kind)
        labels = {excluded.id: "excluded"}
        for index in range(2):
            limited = tasks.record_callback.using(priority=50).enqueue(
                f"limited-{index}"
            )
            labels[limited.id] = "limited"
        labels[tasks.plain_echo.using(priority=0).enqueue("allowed").id] = "allowed"

        claims = drain(worker)

    assert [labels.get(task_id, task_id) for task_id, _ in claims] == expected
    assert_untouched(excluded)
    assert used() == counts
    if on_postgresql():
        if writes_sql:
            assert {path for _, path in claims} == {SINGLE_STATEMENT}
            assert notices(caplog) == []
        else:
            assert {path for _, path in claims} == {SELECT_FOR_UPDATE}
            assert len(notices(caplog)) == 1


def test_a_spent_queue_limit_keeps_a_task_path_condition():
    """The queue half of the rate-limit fragment, under a path condition."""
    limits = {"mail": {"limit": 1, "per": 3600, "queues": [RESERVED_QUEUE]}}
    with configured(LeavesATaskPath, limits):
        tasks.emails_echo.enqueue("spend")
        assert built().run_once() is True
        worker = built()
        excluded = tasks.plain_flaky.using(priority=100).enqueue(1)
        limited = tasks.emails_echo.using(priority=50).enqueue("limited")
        allowed = tasks.plain_echo.using(priority=0).enqueue("allowed")
        labels = {excluded.id: "excluded", limited.id: "limited", allowed.id: "allowed"}

        claims = drain(worker)

    assert [labels.get(task_id, task_id) for task_id, _ in claims] == ["allowed"]
    assert_untouched(excluded)
    assert_untouched(limited)
    assert used() == {"mail": 1}


def run_the_command():
    with pytest.raises(SystemExit) as exit_code:
        call_command("ox_worker", "--processes=1", verbosity=0)
    assert exit_code.value.code == 0


def statuses(results):
    return sorted(OxTask.objects.get(id=result.id).status for result in results)


@pytest.mark.parametrize("shape", list(COMMAND_SHAPES))
def test_the_command_admits_the_limit_and_leaves_the_excluded_rows(shape):
    """
    End to end through ox_worker. The worker spends the limit itself, part
    way through, and from then on every claim is made with the limit spent.
    """
    cls, kind, _ = COMMAND_SHAPES[shape]
    with configured(cls, one_per_hour(3)):
        excluded = [enqueue_excluded(kind) for _ in range(5)]
        limited = [
            tasks.record_callback.using(priority=50).enqueue(f"limited-{index}")
            for index in range(8)
        ]
        allowed = [tasks.plain_echo.using(priority=0).enqueue(i) for i in range(4)]

        run_the_command()

    for result in excluded:
        assert_untouched(result)
    assert (
        statuses(limited) == [OxTask.Status.READY] * 5 + [OxTask.Status.SUCCESSFUL] * 3
    )
    assert statuses(allowed) == [OxTask.Status.SUCCESSFUL] * 4
    assert used() == {"spend": 3}


@pytest.mark.parametrize("shape", list(COMMAND_SHAPES))
def test_the_command_restarted_inside_a_spent_window_keeps_the_condition(shape, caplog):
    cls, kind, writes_sql = COMMAND_SHAPES[shape]
    with configured(cls, one_per_hour(1)):
        limited = [
            tasks.record_callback.using(priority=50).enqueue(f"limited-{index}")
            for index in range(2)
        ]
        run_the_command()
        assert statuses(limited) == [OxTask.Status.READY, OxTask.Status.SUCCESSFUL]

        excluded = enqueue_excluded(kind)
        allowed = tasks.plain_echo.using(priority=0).enqueue("allowed")
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            run_the_command()

    assert_untouched(excluded)
    assert statuses([allowed]) == [OxTask.Status.SUCCESSFUL]
    assert statuses(limited) == [OxTask.Status.READY, OxTask.Status.SUCCESSFUL]
    assert used() == {"spend": 1}
    if on_postgresql():
        assert len(notices(caplog)) == (0 if writes_sql else 1)


def test_a_subclass_overriding_neither_hook_keeps_the_single_statement(caplog):
    """
    Nothing to apply beyond the rate limits, so nothing to give up. The first
    claim spends the limit and the second is made with it spent.
    """
    with (
        configured(OverridesNeitherHook, one_per_hour(1)),
        caplog.at_level(logging.WARNING, logger="django_ox"),
    ):
        worker = built()
        labels = {}
        for index in range(2):
            limited = tasks.record_callback.using(priority=50).enqueue(
                f"limited-{index}"
            )
            labels[limited.id] = "limited"
        labels[tasks.emails_echo.using(priority=0).enqueue("other").id] = "other"

        claims = drain(worker)

    assert [labels.get(task_id, task_id) for task_id, _ in claims] == [
        "limited",
        "other",
    ]
    assert used() == {"spend": 1}
    if on_postgresql():
        assert [path for _, path in claims] == [SINGLE_STATEMENT] * 2
        assert notices(caplog) == []


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("cls", [LeavesAQueue, LeavesAQueueWithoutSuper])
def test_the_limit_bounds_concurrent_subclass_workers(cls):
    """
    The published bound, N + W - 1, on the path that applies claim_filter_q().
    A limiter that stops excluding once the limit is spent claims the reserved
    queue and blows the ceiling; one that excludes everything fails the floor.
    A subclass that drops the limits from its hook blows the ceiling too.

    The window outlasts any run and the workers run until all of them have
    stopped taking tasks, so neither bound depends on how fast they are.
    """
    limit, workers = 10, 4
    with configured(cls, {"echo": {"limit": limit, "per": 600, "tasks": [ECHO]}}):
        reserved = [tasks.emails_echo.using(priority=10).enqueue(i) for i in range(30)]
        for index in range(100):
            tasks.plain_echo.enqueue(index)
        OxpullRateWindow.objects.create(
            limit_name="echo", window_id=0, used=0, window_started_at=Now()
        )

        claims = run_workers(workers, seconds=60, worker_class=cls, until_idle=True)

    admitted = len(admissions(claims))
    assert statuses(reserved) == [OxTask.Status.READY] * 30
    assert limit <= admitted <= limit + workers - 1
    ready = OxTask.objects.filter(task_path=ECHO, status=OxTask.Status.READY).count()
    assert ready == 100 - admitted


def one_claim(worker):
    with captured(worker._db_alias) as statements:
        row = worker.claim_one()
    assert row is not None
    return row, on_the_task_table(statements)


def verbs(statements):
    return [sql.split(" ", 1)[0].upper() for sql in statements]


def test_on_postgresql_the_fallback_costs_what_it_costs_django_ox():
    """
    A subclass that writes claim_filter_q() alone claims exactly as django-ox's
    own Worker does with the same override. OxpullWorker itself, with a limit
    spent, keeps the single statement.
    """
    if not on_postgresql():
        pytest.skip("only PostgreSQL has a single-statement claim to give up")
    with configured(LeavesAQueue, one_per_hour(1)):
        tasks.record_callback.enqueue("spend")
        assert built().run_once() is True
        for index in range(3):
            tasks.plain_echo.enqueue(index)
        tasks.record_callback.using(priority=50).enqueue("limited")

        _, subclass = one_claim(built())
        oxpull_row, oxpull_sql = one_claim(
            OxpullWorker(backoff_initial=0, poll_interval=0.01)
        )
    _, plain = one_claim(PlainLeavesAQueue(backoff_initial=0, poll_interval=0.01))

    assert oxpull_row.task_path == ECHO
    assert verbs(oxpull_sql) == ["UPDATE"]
    assert verbs(subclass) == verbs(plain)
    assert claim_path(subclass) == claim_path(plain) == SELECT_FOR_UPDATE
    assert used() == {"spend": 1}
