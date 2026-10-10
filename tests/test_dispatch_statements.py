"""
What one dispatch pass sends to the database, statement by statement.

A worker's pass runs in autocommit, and each schedule's dispatch in a
transaction of its own. On the way to a dispatch the pass reads stored
values it does not trust: the newest tick of every schedule, a stored
schedule's row under its lock, a settings schedule's earliest tick. Each
is fetched as the database holds it and decoded in the worker, and that
has to cost a healthy pass nothing. The pass sends the statements 1.8.0
sent and no more: no savepoint around a read, and on SQLite no statement
asking whether a row the lock was taken on is there, since the read of
the row says so.

Counted in autocommit, as a worker runs, with exactly one tick due. Inside
a test's own transaction every schedule's transaction is a savepoint, and
a count taken there says nothing about a worker.

With no savepoint, a read the database itself refuses inside a schedule's
transaction is not read again more narrowly: on PostgreSQL the transaction
takes nothing more. The database's own error is what reaches the dispatch
loop, which rolls that schedule back, reports it, and goes on to the next.

A read the database refuses before any schedule's transaction, among the
reads the pass makes for all of its schedules, is not read again more
narrowly either. Its error says nothing of one row or one key, so the pass
is abandoned with it, and the next pass reads again.
"""

import logging
from datetime import timedelta

import pytest
from django.db import DataError, connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.models import OxSchedule, OxScheduleTick, OxTask
from django_ox.registry import ScheduleKind, register
from django_ox.stored import boundary_digest, create_schedule
from django_ox.worker import Worker

from . import tasks
from .conftest import start_worker_thread, wait_for
from .isolation import daily_cron_at, labels, twelve_hours_ago, with_history
from .test_stored_read import Reads, Refusing

pytestmark = pytest.mark.django_db(transaction=True)

SCHEDULES = "django_ox_oxschedule"

#: The statements of one pass with exactly one tick due, by their first
#: word, as 1.8.0 sends them on each database: for a stored schedule, for a
#: settings schedule with a tick older than the pass reads back to, and
#: for a settings schedule at its first sighting. Each begins with the
#: stored source's read of its change marker and the read of the newest
#: ticks. PostgreSQL and MySQL lock a stored row with the read of it;
#: SQLite writes first to become the writer. A first sighting writes its
#: latch, reads again under it, and takes the latch back.
STORED = {
    "postgresql": "SELECT SELECT BEGIN SELECT INSERT INSERT UPDATE COMMIT",
    "mysql": "SELECT SELECT BEGIN SELECT INSERT INSERT UPDATE COMMIT",
    "sqlite": "SELECT SELECT BEGIN UPDATE SELECT INSERT INSERT UPDATE COMMIT",
}
SETTINGS = "SELECT SELECT BEGIN INSERT SELECT INSERT UPDATE COMMIT"
FIRST_SIGHTING = "SELECT SELECT BEGIN INSERT SELECT INSERT SELECT DELETE COMMIT"


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="labelled", task=tasks.labelled))


def tasks_setting(schedules):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {
                "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
                "SCHEDULES": schedules,
            },
        }
    }


def entry(name, base):
    """A settings schedule daily at `base`'s time of day, labelled `name`."""
    return {"task": "tests.tasks.labelled", "cron": daily_cron_at(base), "args": [name]}


def a_row(name, base):
    """A stored schedule daily at `base`'s time of day, labelled `name`."""
    return create_schedule(
        name=name,
        task_key="labelled",
        trigger="cron",
        cron=daily_cron_at(base),
        arguments={"label": name},
        start_time=timezone.now() - timedelta(days=1),
    )


def one_pass(worker):
    """A whole dispatch pass in autocommit: what it dispatched, and its statements."""
    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with CaptureQueriesContext(connection) as captured:
        dispatched = worker.dispatch_schedules()
    return dispatched, [query["sql"] for query in captured.captured_queries]


def first_words(statements):
    return " ".join(sql.split()[0].upper() for sql in statements)


def assert_no_savepoint(statements):
    """Not one made, released or gone back to."""
    assert not [sql for sql in statements if "SAVEPOINT" in sql.upper()], statements


def on_the_schedule_table(sql):
    """Whether a statement is on the schedule table, not on one named after it."""
    others = (f"{SCHEDULES}tick", f"{SCHEDULES}change")
    return SCHEDULES in sql.replace(others[0], "").replace(others[1], "")


def ticks_of(key):
    return OxScheduleTick.objects.filter(schedule_name=key)


class TestAHealthyPassSendsWhatItSentBefore:
    def test_for_a_stored_schedule(self, settings):
        settings.TASKS = tasks_setting({})
        row = a_row("r", twelve_hours_ago())
        worker = Worker(backoff_initial=0)
        # The rows are read in full when the marker moves or the reconcile
        # is due, not on a pass that has a tick to fire.
        worker._schedule_source.schedules()

        dispatched, statements = one_pass(worker)

        assert dispatched == 1
        assert labels() == ["r"]
        assert first_words(statements) == STORED[connection.vendor]
        assert_no_savepoint(statements)
        on_the_row = [sql for sql in statements if on_the_schedule_table(sql)]
        if connection.features.has_select_for_update:
            # The lock and the row in one read.
            (read,) = on_the_row
            assert "FOR UPDATE" in read
        else:
            # The writer first, and then the row, whose absence from that
            # read is what would say it had gone: no statement asks.
            write, read = on_the_row
            assert write.lstrip().upper().startswith("UPDATE")
        assert read.lstrip().upper().startswith("SELECT")
        assert str(row.pk) in read

    def test_for_a_settings_schedule_with_older_history(self, settings):
        settings.TASKS = tasks_setting({"s": entry("s", twelve_hours_ago())})
        with_history("s")
        worker = Worker(backoff_initial=0)

        dispatched, statements = one_pass(worker)

        assert dispatched == 1
        assert labels() == ["s"]
        assert first_words(statements) == SETTINGS
        assert_no_savepoint(statements)

    def test_for_a_settings_schedule_at_its_first_sighting(self, settings):
        settings.TASKS = tasks_setting({"s": entry("s", twelve_hours_ago())})
        worker = Worker(backoff_initial=0)

        dispatched, statements = one_pass(worker)

        # Anchored, not fired: the tick is recorded with no task.
        assert dispatched == 0
        (anchor,) = ticks_of("s")
        assert anchor.task_id is None
        assert not OxTask.objects.exists()
        assert first_words(statements) == FIRST_SIGHTING
        assert_no_savepoint(statements)

    def test_a_pass_with_nothing_due_reads_twice_and_writes_nothing(self, settings):
        settings.TASKS = tasks_setting({})
        a_row("r", twelve_hours_ago())
        worker = Worker(backoff_initial=0)
        assert worker.dispatch_schedules() == 1

        dispatched, statements = one_pass(worker)

        assert dispatched == 0
        assert first_words(statements) == "SELECT SELECT"


class TestTheReadOfEveryStoredRow:
    """
    The stored rows are read in full when their change marker moves and at
    each reconcile interval: the marker, the keys, and then the rows five
    hundred a statement. That is what a worker pays for a table of any
    size, on a pass that has that read to make.
    """

    def test_twelve_hundred_rows_are_their_keys_and_three_statements(self, settings):
        settings.TASKS = tasks_setting({})
        now = timezone.now()
        rows = [
            OxSchedule(
                name=f"r-{index:04}",
                task_key="labelled",
                trigger="cron",
                cron="0 3 * * *",
                arguments={"label": f"r-{index:04}"},
                start_time=now,
                created_at=now,
                updated_at=now,
            )
            for index in range(1200)
        ]
        for row in rows:
            # As the write functions leave it, so that no row is met with a
            # boundary to move.
            row.boundary_for = boundary_digest(row)
        OxSchedule.objects.bulk_create(rows)
        worker = Worker(backoff_initial=0)
        # Read again in full, as it is when the reconcile interval is up.
        worker._schedule_source._last_read = None
        reads = Reads(SCHEDULES)

        assert connection.get_autocommit()
        assert not connection.in_atomic_block
        with connection.execute_wrapper(reads):
            built = worker._schedule_source.schedules()

        assert len(built) == 1200
        marker, *on_the_rows = reads.sent
        assert not on_the_schedule_table(marker[0])
        assert all(on_the_schedule_table(sql) for sql, _params in on_the_rows)
        # The keys, which take no parameter, and then the rows, a key a
        # parameter.
        assert [len(params) for _sql, params in on_the_rows] == [0, 500, 500, 200]


def events(caplog, name):
    return [r for r in caplog.records if getattr(r, "event", None) == name]


def a_pass_with_the_first_read_refused(worker, mentions, caplog):
    """
    One pass in which the database refuses the first read whose SQL holds
    `mentions`, inside the transaction of the schedule it belongs to.
    Returns what was dispatched and the first word of every statement of
    the pass, transactions' BEGIN, COMMIT and ROLLBACK among them. The
    refused read is "(refused)" there: a driver records a statement that
    failed in its own way, or not at all.
    """
    refusing = Refusing(mentions)
    with (
        caplog.at_level(logging.INFO, logger="django_ox"),
        connection.execute_wrapper(refusing),
        CaptureQueriesContext(connection) as captured,
    ):
        assert connection.get_autocommit()
        assert not connection.in_atomic_block
        dispatched = worker.dispatch_schedules()
    assert refusing.refused is not None, "the read was never sent"
    assert refusing.after, "the pass ended with the refused read"
    sent = [str(query["sql"]).split()[0].upper() for query in captured.captured_queries]
    assert not [word for word in sent if word in ("SAVEPOINT", "RELEASE")], sent
    # The refused read is the statement the first rollback follows.
    sent[sent.index("ROLLBACK") - 1] = "(refused)"
    return dispatched, sent


def up_to_the_rollback(sent):
    """
    The pass from its first BEGIN to the first ROLLBACK: the transaction of
    the schedule whose read was refused. The refused read is the last
    statement in it, so nothing was read after it, more narrowly or
    otherwise, before the transaction was rolled back.
    """
    return sent[sent.index("BEGIN") : sent.index("ROLLBACK") + 1]


def assert_reported_as_the_databases_own_error(caplog, schedule):
    (failed,) = events(caplog, "schedule_dispatch_error")
    assert failed.schedule == schedule
    assert failed.error == "DataError"
    raised = failed.exc_info[1]
    assert type(raised) is DataError
    # What the database said of the read it refused, not what it says of
    # every statement sent into a transaction that has already failed.
    assert Refusing.SAYS[connection.vendor] in str(raised)
    assert not events(caplog, "schedule_row_skipped")
    assert not events(caplog, "schedule_tick_unreadable")
    assert not events(caplog, "schedule_dispatch_failed")


class TestAReadTheDatabaseRefusesInsideASchedulesTransaction:
    """
    On PostgreSQL and MySQL the server refuses the read for real, with a
    statement it raises a DataError on, so PostgreSQL's transaction is left
    as a refused read leaves it. SQLite has no such statement, and the
    error is raised in the read's place.
    """

    def test_the_read_of_a_stored_row_under_its_lock(self, settings, caplog):
        settings.TASKS = tasks_setting({})
        base = twelve_hours_ago()
        first = a_row("r-first", base)
        second = a_row("r-second", base)
        worker = Worker(backoff_initial=0)
        worker._schedule_source.schedules()

        dispatched, sent = a_pass_with_the_first_read_refused(
            worker, "ox_stored_start_time", caplog
        )

        # The first row's dispatch is rolled back, and the second fires.
        assert dispatched == 1
        assert labels() == ["r-second"]
        assert not ticks_of(f"db:{first.pk}").exists()
        assert ticks_of(f"db:{second.pk}").count() == 1
        locked = (
            []
            if connection.features.has_select_for_update
            # The no-op write that makes the transaction the writer.
            else ["UPDATE"]
        )
        assert up_to_the_rollback(sent) == ["BEGIN", *locked, "(refused)", "ROLLBACK"]
        # The second row's whole dispatch comes after it.
        assert sent[-1] == "COMMIT"
        assert_reported_as_the_databases_own_error(caplog, "r-first")

    def test_the_read_of_a_settings_schedules_earliest_tick(self, settings, caplog):
        base = twelve_hours_ago()
        settings.TASKS = tasks_setting(
            {"s-first": entry("s-first", base), "s-second": entry("s-second", base)}
        )
        with_history("s-first", "s-second")
        (history,) = ticks_of("s-first").values_list("pk", flat=True)
        worker = Worker(backoff_initial=0)

        dispatched, sent = a_pass_with_the_first_read_refused(
            worker, "ox_stored_scheduled_for", caplog
        )

        assert dispatched == 1
        assert labels() == ["s-second"]
        # The tick written for the first schedule before the read went
        # with its transaction: what it had before is all it has.
        assert list(ticks_of("s-first").values_list("pk", flat=True)) == [history]
        assert ticks_of("s-second").exclude(task_id=None).count() == 1
        assert up_to_the_rollback(sent) == ["BEGIN", "INSERT", "(refused)", "ROLLBACK"]
        assert sent[-1] == "COMMIT"
        assert_reported_as_the_databases_own_error(caplog, "s-first")

    def test_the_next_pass_dispatches_the_schedule_it_cost(self, settings, caplog):
        settings.TASKS = tasks_setting({})
        base = twelve_hours_ago()
        a_row("r-first", base)
        a_row("r-second", base)
        worker = Worker(backoff_initial=0)
        worker._schedule_source.schedules()
        a_pass_with_the_first_read_refused(worker, "ox_stored_start_time", caplog)

        with caplog.at_level(logging.INFO, logger="django_ox"):
            dispatched, statements = one_pass(worker)

        assert dispatched == 1
        assert labels() == ["r-first", "r-second"]
        assert_no_savepoint(statements)
        (recovered,) = events(caplog, "schedule_dispatch_recovered")
        assert recovered.schedule == "r-first"


class TestAReadTheDatabaseRefusesWithNoTransactionOpen:
    """
    The reads a pass makes for all of its schedules run with no transaction
    open: the stored rows, read in full when their change marker moves, and
    the newest tick of every schedule that is due. One the database refuses
    is not one schedule's, and no row or key is looked for in it: the pass
    is abandoned with the database's own error, which the worker's loop
    takes as it takes any other of the database's, and the next pass reads
    again. On PostgreSQL and MySQL the server refuses the read for real;
    SQLite has no such statement, and the error is raised in the read's
    place.
    """

    #: Each of those reads, by what its statement holds.
    READS = [
        pytest.param("ox_stored_start_time", id="the-stored-rows"),
        pytest.param("MAX(", id="the-newest-ticks"),
    ]

    @pytest.mark.parametrize("mentions", READS)
    def test_the_pass_is_abandoned_with_the_databases_own_error(
        self, settings, caplog, mentions
    ):
        settings.TASKS = tasks_setting({})
        base = twelve_hours_ago()
        a_row("r-first", base)
        worker = Worker(backoff_initial=0)
        # Written since the worker read the rows, so its next read of them
        # is in full again.
        a_row("r-second", base)
        if "MAX(" in mentions:
            # Made here, so that the pass comes to the newest ticks.
            worker._schedule_source.schedules()
        refusing = Refusing(mentions)

        assert connection.get_autocommit()
        assert not connection.in_atomic_block
        with (
            caplog.at_level(logging.INFO, logger="django_ox"),
            pytest.raises(DataError) as caught,
            connection.execute_wrapper(refusing),
        ):
            worker.dispatch_schedules()

        # Raised as it was, with no half of the rows or the keys read after.
        refusing.assert_raised_as_it_was(caught)
        assert not OxTask.objects.exists()
        assert not OxScheduleTick.objects.exists()
        for event in (
            "schedule_row_skipped",
            "schedule_tick_unreadable",
            "schedule_dispatch_error",
        ):
            assert not events(caplog, event), event

        dispatched, statements = one_pass(worker)

        assert dispatched == 2
        assert labels() == ["r-first", "r-second"]
        assert_no_savepoint(statements)

    def test_the_workers_loop_reports_it_as_the_databases_and_goes_on(
        self, settings, caplog, monkeypatch
    ):
        settings.TASKS = tasks_setting({})
        a_row("r-due", twelve_hours_ago())
        worker = Worker(
            backoff_initial=0,
            poll_interval=0.02,
            reap_interval=0.0,
            schedule_interval=0.1,
        )
        refusing = Refusing("MAX(")
        dispatch = worker.dispatch_schedules

        def with_the_first_pass_refused():
            # Called on the worker's thread, and so on its own connection.
            if refusing.refused is None:
                with connection.execute_wrapper(refusing):
                    return dispatch()
            return dispatch()

        monkeypatch.setattr(worker, "dispatch_schedules", with_the_first_pass_refused)

        def ran():
            return OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL).exists()

        with caplog.at_level(logging.INFO, logger="django_ox"):
            thread = start_worker_thread(worker)
            reached = wait_for(lambda: ran() or not thread.is_alive(), timeout=30)
            alive = thread.is_alive()
            worker.request_stop()
            thread.join(timeout=30)

        assert reached, "the schedule never fired"
        assert alive, "run() ended on its own"
        (failed,) = events(caplog, "schedule_dispatch_failed")
        assert failed.category == "database"
        assert failed.error == "DataError"
        raised = failed.exc_info[1]
        assert type(raised) is DataError
        assert Refusing.SAYS[connection.vendor] in str(raised)
        # Nothing was read after it in the pass it ended.
        assert refusing.after == []
        assert not events(caplog, "schedule_tick_unreadable")
        (resumed,) = events(caplog, "schedule_dispatch_resumed")
        assert resumed.failures == 1
        assert labels() == ["r-due"]
        assert OxSchedule.objects.count() == 1
