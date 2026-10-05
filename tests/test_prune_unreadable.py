"""
ox_prune beside schedule tick rows that cannot be read.

A tick written around django-ox, by SQL, a fixture or a restore, can hold a
scheduled time its column accepts and Django's read cannot convert:
PostgreSQL keeps year 10000, 'infinity' and '-infinity', SQLite keeps any
text, MySQL keeps zero dates. Django's read of such a value raises in the
middle of a result, or on SQLite can read as nothing at all. SQLite also
keeps a schedule key that is not UTF-8, which its driver refuses to hand
over at all.

Ordinary pruning keeps every such tick and says so on stderr, keeps each
schedule's newest tick that reads, and prunes the rest by the cutoff as it
always did. `--purge-unreadable-ticks` removes the unreadable ticks it is
pointed at and nothing else.

Every unreadable value is written by SQL, in the form the engine keeps it.
A test for another engine's value skips and says why.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timedelta
from io import StringIO

import pytest
from django.core.management import ManagementUtility, call_command
from django.core.management.base import CommandError
from django.db import (
    DEFAULT_DB_ALIAS,
    DatabaseError,
    DataError,
    connection,
    connections,
    transaction,
)
from django.db.models import QuerySet
from django.db.models.signals import post_delete, pre_delete
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox.models import OxScheduleTick, OxTask

from .conftest import wait_for
from .contention import failing, simulated
from .test_schedule_isolation_worker import GUARD, finish, project, start
from .test_stored_read import Refusing

TICKS = "django_ox_oxscheduletick"

#: (engine, SQL literal, id): a tick each engine keeps that Django's read
#: raises on or misreads, and that sorts after every real date.
NEWEST = [
    pytest.param("postgresql", "'10000-01-01 00:00:00+00'", id="postgresql-year-10000"),
    pytest.param("postgresql", "'infinity'", id="postgresql-infinity"),
    pytest.param("sqlite", "'banana'", id="sqlite-banana"),
    pytest.param("sqlite", "'9999-02-30 00:00:00'", id="sqlite-does-not-exist"),
    pytest.param("mysql", "'9999-00-00 00:00:00'", id="mysql-zero-month"),
]

#: The same, sorting before every real date.
OLDEST = [
    pytest.param("postgresql", "'-infinity'", id="postgresql-minus-infinity"),
    pytest.param("postgresql", "'0001-01-01 00:00:00+00 BC'", id="postgresql-bc"),
    pytest.param("sqlite", "'0000-02-30 00:00:00'", id="sqlite-year-zero"),
    pytest.param("mysql", "'0000-00-00 00:00:00'", id="mysql-zero-date"),
]

#: The same, sorting between the real dates around year 2020. PostgreSQL
#: keeps no such value: what it cannot hand over is out of range at one end.
BETWEEN = [
    pytest.param("sqlite", "'2020-02-30 00:00:00'", id="sqlite-does-not-exist"),
    pytest.param("mysql", "'2020-00-10 00:00:00'", id="mysql-zero-month"),
]

#: Text SQLite keeps and its driver cannot decode as UTF-8, as a schedule key.
UNDECODABLE_KEY = "CAST(X'6B80FF' AS TEXT)"

#: Bytes SQLite keeps where a schedule key should be, and hands over as bytes.
BYTES_KEY = "X'6E696768746C79'"

#: One of each for the engine this run is on.
NEWEST_HERE = {
    "postgresql": "'infinity'",
    "sqlite": "'9999-02-30 00:00:00'",
    "mysql": "'9999-00-00 00:00:00'",
}
OLDEST_HERE = {
    "postgresql": "'-infinity'",
    "sqlite": "'0000-02-30 00:00:00'",
    "mysql": "'0000-00-00 00:00:00'",
}


def by_sql(statement, params=(), *, using=None):
    """
    Run one statement the way only SQL can write what it writes.

    MySQL refuses a zero date in the strict mode the suite runs in, so its
    session is let off for the statement and put back.
    """
    target = connection if using is None else using
    with target.cursor() as cursor:
        if target.vendor != "mysql":
            cursor.execute(statement, params)
            return
        cursor.execute("SELECT @@SESSION.sql_mode")
        (mode,) = cursor.fetchone()
        cursor.execute("SET SESSION sql_mode = ''")
        try:
            cursor.execute(statement, params)
        finally:
            cursor.execute("SET SESSION sql_mode = %s", [mode])


def insert_tick(key, literal, *, created=None):
    """A tick row whose scheduled_for is `literal`, written by SQL; its key."""
    if created is None:
        created = {"postgresql": "now()", "mysql": "NOW(6)"}.get(
            connection.vendor, "'2026-10-05 00:00:00'"
        )
    by_sql(
        f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
        f"VALUES (%s, {literal}, NULL, {created})",
        [key],
    )
    return (
        OxScheduleTick.objects.filter(schedule_name=key)
        .order_by("-pk")
        .values_list("pk", flat=True)
        .first()
    )


def insert_tick_keyed(key_literal, *, days_ago):
    """A tick row whose schedule key is `key_literal`, written by SQL; its key."""
    before = remaining()
    when = connection.ops.adapt_datetimefield_value(
        timezone.now() - timedelta(days=days_ago)
    )
    by_sql(
        f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
        f"VALUES ({key_literal}, %s, NULL, %s)",
        [when, when],
    )
    (pk,) = remaining() - before
    return pk


def make_tick(name, *, days_ago):
    when = timezone.now() - timedelta(days=days_ago)
    return OxScheduleTick.objects.create(
        schedule_name=name, scheduled_for=when, created_at=when
    ).pk


def only_on(vendor, why="kept by that engine only"):
    if connection.vendor != vendor:
        pytest.skip(f"a {vendor} value: {why}; this run is on {connection.vendor}")


def prune(*args):
    out, err = StringIO(), StringIO()
    call_command("ox_prune", *args, stdout=out, stderr=err)
    return out.getvalue(), err.getvalue()


def remaining():
    return set(OxScheduleTick.objects.values_list("pk", flat=True))


def _sql(sql):
    return sql.replace('"', "").replace("`", "").upper()


def in_another_connection(statement, params=()):
    """Run and commit one statement on a connection of its own."""
    other = connections.create_connection(DEFAULT_DB_ALIAS)
    try:
        by_sql(statement, params, using=other)
    finally:
        other.close()


@pytest.mark.django_db
class TestOrdinaryPruningKeepsWhatItCannotRead:
    @pytest.mark.parametrize(("vendor", "literal"), NEWEST)
    def test_an_unreadable_newest_tick_and_the_newest_one_that_reads_are_kept(
        self, vendor, literal
    ):
        only_on(vendor)
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=20)
        newest_readable = make_tick("k", days_ago=10)
        bad = insert_tick("k", literal)

        out, err = prune()

        assert remaining() == {newest_readable, bad}
        assert "Deleted 2 schedule tick row(s)" in out
        assert "Kept 1 unreadable schedule tick row(s) encountered in this run." in out
        assert "Kept 1 schedule tick row(s) scheduled before " in out
        assert f"  tick {bad}, schedule 'k': scheduled_for " in err
        assert "--purge-unreadable-ticks" in err

    @pytest.mark.parametrize(("vendor", "literal"), OLDEST)
    def test_an_unreadable_oldest_tick_is_kept(self, vendor, literal):
        only_on(vendor)
        bad = insert_tick("k", literal)
        make_tick("k", days_ago=30)
        newest = make_tick("k", days_ago=10)

        out, err = prune()

        assert remaining() == {bad, newest}
        assert "Deleted 1 schedule tick row(s)" in out
        assert f"  tick {bad}, schedule 'k': scheduled_for " in err

    @pytest.mark.parametrize(("vendor", "literal"), BETWEEN)
    def test_an_unreadable_tick_between_readable_ones_is_kept(self, vendor, literal):
        only_on(vendor)
        make_tick("k", days_ago=3000)
        bad = insert_tick("k", literal)
        make_tick("k", days_ago=30)
        newest = make_tick("k", days_ago=10)

        out, err = prune()

        assert remaining() == {bad, newest}
        assert "Deleted 2 schedule tick row(s)" in out
        assert "Kept 1 unreadable schedule tick row(s) encountered in this run." in out
        assert f"  tick {bad}, schedule 'k': scheduled_for " in err

    @pytest.mark.parametrize(("vendor", "literal"), OLDEST + NEWEST)
    def test_an_only_tick_that_cannot_be_read_is_kept(self, vendor, literal):
        only_on(vendor)
        bad = insert_tick("settings-every-minute", literal)

        out, err = prune()

        assert remaining() == {bad}
        assert "Deleted 0 schedule tick row(s)" in out
        assert "Kept 1 unreadable schedule tick row(s) encountered in this run." in out
        assert f"  tick {bad}, schedule 'settings-every-minute': " in err

    def test_a_key_no_schedule_has_is_pruned_by_the_same_rules(self):
        make_tick("db:999999", days_ago=30)
        newest_readable = make_tick("db:999999", days_ago=20)
        bad = insert_tick("db:999999", NEWEST_HERE[connection.vendor])

        out, err = prune()

        assert remaining() == {newest_readable, bad}
        assert "Deleted 1 schedule tick row(s)" in out
        assert f"  tick {bad}, schedule 'db:999999': " in err

    def test_a_tick_whose_created_at_cannot_be_read_prunes_without_being_loaded(self):
        # Django's delete() loads each row it deletes once anything listens
        # for its signals. This tick's scheduled time reads and is old, so
        # it goes; its creation time does not read, so loading it would
        # fail.
        literal = {
            "postgresql": "'10000-01-01 00:00:00+00'",
            "sqlite": "'9999-02-30 00:00:00'",
            "mysql": "'0000-00-00 00:00:00'",
        }[connection.vendor]
        old = timezone.now() - timedelta(days=30)
        by_sql(
            f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
            f"VALUES (%s, %s, NULL, {literal})",
            ["k", connection.ops.adapt_datetimefield_value(old)],
        )
        newest = make_tick("k", days_ago=1)

        def listener(sender, **kwargs):
            pass

        pre_delete.connect(listener, weak=False, dispatch_uid="test_prune_listener")
        try:
            out, _ = prune()
        finally:
            pre_delete.disconnect(dispatch_uid="test_prune_listener")

        assert remaining() == {newest}
        assert "Deleted 1 schedule tick row(s)" in out

    def test_dry_run_chooses_what_the_run_deletes_and_writes_nothing(self):
        newest = NEWEST_HERE[connection.vendor]
        oldest = OLDEST_HERE[connection.vendor]
        insert_tick("a", newest)
        for days in (30, 20, 10):
            make_tick("a", days_ago=days)
        insert_tick("b", oldest)
        make_tick("b", days_ago=30)
        make_tick("b", days_ago=1)
        insert_tick("c", oldest)
        before = remaining()

        dry = json.loads(prune("--dry-run", "--format", "json")[0])

        assert remaining() == before
        done = json.loads(prune("--format", "json")[0])
        for figure in ("tick_rows", "unreadable_tick_rows", "anchor_tick_rows"):
            assert dry[figure] == done[figure], figure

        def named(data):
            return sorted(data["unreadable_ticks"], key=lambda named: named["pk"])

        assert named(dry) == named(done)
        assert (dry["tick_rows"], dry["unreadable_tick_rows"]) == (3, 3)
        assert dry["anchor_tick_rows"] == 1
        assert dry["dry_run"] is True
        assert len(remaining()) == len(before) - 3

    def test_dry_run_text_says_what_it_would_keep(self):
        bad = insert_tick("k", OLDEST_HERE[connection.vendor])
        make_tick("k", days_ago=30)

        out, err = prune("--dry-run")

        assert "Would delete 0 schedule tick row(s)" in out
        assert (
            "Would keep 1 unreadable schedule tick row(s) encountered in this run."
            in out
        )
        assert "Would keep 1 schedule tick row(s) scheduled before " in out
        assert f"  tick {bad}, schedule 'k': " in err

    def test_json_is_one_object_on_stdout_and_the_warning_goes_to_stderr(self):
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=10)
        bad = insert_tick("k", NEWEST_HERE[connection.vendor])

        out, err = prune("--format", "json")

        data = json.loads(out)
        assert out.strip() == json.dumps(data)
        assert data["tick_rows"] == 1
        assert data["unreadable_tick_rows"] == 1
        assert data["anchor_tick_rows"] == 1
        ((named,),) = [data["unreadable_ticks"]]
        assert named["pk"] == bad
        assert named["schedule_name"] == "k"
        assert named["reason"].startswith("scheduled_for ")
        assert "Warning" not in out
        assert err.startswith(
            "Warning: this run encountered 1 schedule tick row(s) it cannot"
        )
        assert f"  tick {bad}, schedule 'k': " in err

    def test_from_the_command_line_it_completes_and_warns(self, capsys):
        bad = insert_tick("k", OLDEST_HERE[connection.vendor])
        argv = ["manage.py", "ox_prune", "--format", "json", "--skip-checks"]

        # Returns, rather than ending in SystemExit as a failed command does.
        ManagementUtility(argv).execute()

        captured = capsys.readouterr()
        assert json.loads(captured.out)["unreadable_tick_rows"] == 1
        assert f"  tick {bad}, schedule 'k': " in captured.err
        assert "Traceback" not in captured.err

    def test_many_are_counted_and_the_first_hundred_named(self):
        literal = NEWEST_HERE[connection.vendor]
        bad = [insert_tick(f"k{i:03}", literal) for i in range(103)]

        out, err = prune("--format", "json")

        data = json.loads(out)
        assert data["unreadable_tick_rows"] == 103
        assert len(data["unreadable_ticks"]) == 100
        assert {named["pk"] for named in data["unreadable_ticks"]} < set(bad)
        assert err.count("\n  tick ") == 100
        assert "\n  and 3 more.\n" in err
        assert remaining() == set(bad)

    def test_each_kept_tick_is_logged_by_event(self, caplog):
        bad = insert_tick("k", OLDEST_HERE[connection.vendor])

        with caplog.at_level(logging.WARNING, logger="django_ox"):
            prune()

        (record,) = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_tick_unreadable"
        ]
        assert record.surface == "prune"
        assert record.database == DEFAULT_DB_ALIAS
        assert record.schedule_key == "k"
        assert record.tick_pk == bad
        assert record.reason.startswith("scheduled_for ")

    def test_a_key_holding_a_line_break_is_logged_escaped(self, caplog):
        # A tick row's key is whatever SQL wrote there, and a record's
        # `schedule_key` is printed by any format that names it.
        key = "k\nCRITICAL forged\x1b[31m"
        bad = insert_tick(key, OLDEST_HERE[connection.vendor])

        with caplog.at_level(logging.WARNING, logger="django_ox"):
            prune()
            prune("--purge-unreadable-ticks", "--tick-pk", str(bad))

        kept, removed = (
            next(r for r in caplog.records if getattr(r, "event", None) == event)
            for event in ("schedule_tick_unreadable", "schedule_tick_history_removed")
        )
        for record in (kept, removed):
            assert record.tick_pk == bad
            assert record.schedule_key == "k\\nCRITICAL forged\\x1b[31m"
            assert record.schedule_key.isprintable()
            assert record.getMessage().isprintable()
        assert remaining() == set()

    def test_inside_a_callers_transaction_the_transaction_works_after(self):
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=10)
        insert_tick("k", NEWEST_HERE[connection.vendor])

        with transaction.atomic():
            with CaptureQueriesContext(connection) as queries:
                out, _ = prune()
            assert OxTask.objects.count() == 0
            assert OxScheduleTick.objects.filter(schedule_name="k").count() == 2

        assert "Deleted 1 schedule tick row(s)" in out
        # No read of the tick log takes a savepoint in the caller's
        # transaction, and with no task row to delete nothing else does.
        assert not [
            q["sql"] for q in queries.captured_queries if "SAVEPOINT" in q["sql"]
        ]

    @pytest.mark.parametrize(
        "args",
        [(), ("--dry-run",), ("--purge-unreadable-ticks", "--schedule-key", "k")],
        ids=["prune", "dry-run", "purge"],
    )
    def test_inside_a_callers_transaction_a_refused_read_is_raised_as_it_was(
        self, args
    ):
        """
        A read of the tick log the database itself refuses ends a
        PostgreSQL transaction for every statement after it, and the
        command takes no savepoint to go back to. It ends with the
        database's own error, nothing read after it and nothing deleted,
        and the caller's transaction, rolled back, takes the caller's own
        writes with it. On PostgreSQL and MySQL the server refuses the read
        for real; SQLite has no statement it refuses that way, and the
        error is raised in the read's place.
        """
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=10)
        insert_tick("k", NEWEST_HERE[connection.vendor])
        before = remaining()
        refusing = Refusing(TICKS)

        with (
            pytest.raises(DataError) as caught,
            transaction.atomic(),
            connection.execute_wrapper(refusing),
        ):
            make_tick("the-callers-own", days_ago=1)
            prune(*args)

        refusing.assert_raised_as_it_was(caught)
        assert remaining() == before

    @pytest.mark.parametrize(
        "literal",
        [
            pytest.param("CAST(X'62616480FF' AS TEXT)", id="sorts-newest"),
            pytest.param("CAST(X'30303030FF' AS TEXT)", id="sorts-oldest"),
        ],
    )
    @pytest.mark.parametrize("position", [0, 4, 8], ids=["first", "middle", "last"])
    def test_a_tick_the_driver_cannot_hand_over_is_kept(self, literal, position):
        # Eight readable old ticks over four schedules, each schedule also
        # with a young newest one, and the bad tick written at `position`
        # among the old ones: one batch of nine, read whole or in halves.
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        old = [(f"k{i % 4}", 30 - i) for i in range(8)]
        bad_key = "k1"
        bad = None
        for index, (key, days) in enumerate(old):
            if index == position:
                bad = insert_tick(bad_key, literal)
            make_tick(key, days_ago=days)
        if bad is None:
            bad = insert_tick(bad_key, literal)
        newest = {make_tick(f"k{i}", days_ago=1) for i in range(4)}

        out, err = prune("--batch-size", "9")

        assert remaining() == newest | {bad}
        assert "Deleted 8 schedule tick row(s)" in out
        assert f"  tick {bad}, schedule '{bad_key}': scheduled_for could not" in err
        assert "UTF-8" in err

    @pytest.mark.parametrize("args", [(), ("--dry-run",)], ids=["run", "dry-run"])
    def test_a_tick_whose_schedule_key_cannot_be_handed_over_is_kept(self, args):
        # One old and one young, among two schedules' ordinary history. The
        # read of every schedule's key is the first the tick log gets, and
        # on these two rows it fails as a whole.
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        old = [make_tick(key, days_ago=30) for key in ("j", "k")]
        newest = {make_tick(key, days_ago=1) for key in ("j", "k")}
        bad = {
            insert_tick_keyed(UNDECODABLE_KEY, days_ago=30),
            insert_tick_keyed(UNDECODABLE_KEY, days_ago=1),
        }
        before = remaining()

        out, err = prune("--batch-size", "2", *args)

        verb, kept = ("Would delete", "Would keep") if args else ("Deleted", "Kept")
        assert remaining() == (before if args else newest | bad)
        assert remaining() >= newest | bad
        assert f"{verb} {len(old)} schedule tick row(s)" in out
        assert (
            f"{kept} 2 unreadable schedule tick row(s) encountered in this run." in out
        )
        for pk in bad:
            assert f"  tick {pk}, schedule '': schedule_name could not be read" in err

    @pytest.mark.parametrize(
        "args",
        [(), ("--dry-run",), ("--format", "json")],
        ids=["run", "dry-run", "json"],
    )
    def test_a_tick_whose_schedule_key_is_not_text_is_kept(self, args):
        # Two rows under bytes for a key, both old and both with a tick that
        # reads, and above them one whose tick does not. The driver hands
        # the bytes over without an error, so nothing fails on these rows.
        only_on("sqlite", "SQLite keeps bytes where a schedule key should be")
        old = make_tick("k", days_ago=30)
        newest = make_tick("k", days_ago=1)
        kept = {
            insert_tick_keyed(BYTES_KEY, days_ago=30),
            insert_tick_keyed(BYTES_KEY, days_ago=20),
        }
        by_sql(
            f"INSERT INTO {TICKS} (schedule_name, scheduled_for, task_id, created_at) "  # noqa: S608
            f"VALUES ({BYTES_KEY}, 'banana', NULL, '2026-10-05 00:00:00')"
        )
        before = remaining()

        out, err = prune(*args)

        dry = "--dry-run" in args
        assert remaining() == (before if dry else before - {old})
        assert newest in remaining()
        for pk in kept:
            assert (
                f"  tick {pk}, schedule '': schedule_name holds b'nightly', "
                "which is not text"
            ) in err
        if "json" in args:
            data = json.loads(out)
            assert out.strip() == json.dumps(data)
            assert data["tick_rows"] == 1
            named = {row["pk"]: row for row in data["unreadable_ticks"]}
            assert set(named) == kept
            assert {row["schedule_name"] for row in named.values()} == {""}
        else:
            verb = "Would delete" if dry else "Deleted"
            assert f"{verb} 1 schedule tick row(s)" in out

    def test_text_carrying_an_offset_with_use_tz_off_is_kept(self, settings):
        only_on("sqlite", "SQLite keeps text with an offset whatever USE_TZ says")
        if settings.USE_TZ:
            pytest.skip("read with USE_TZ off; this run has it on")
        make_tick("k", days_ago=30)
        newest_readable = make_tick("k", days_ago=10)
        bad = insert_tick("k", "'9999-01-01 00:00:00+00:00'")

        _, err = prune()

        assert remaining() == {newest_readable, bad}
        assert f"  tick {bad}, schedule 'k': scheduled_for " in err


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "args",
    [(), ("--dry-run",), ("--purge-unreadable-ticks", "--schedule-key", "k")],
    ids=["prune", "dry-run", "purge"],
)
def test_with_no_transaction_open_a_refused_read_ends_the_command_as_it_was(args):
    """
    Run as an operator runs it, with no transaction open, the command does
    not read the tick log again more narrowly after a read the database
    itself refused, though nothing would be in the way. That error says
    nothing of one tick, so no tick is kept or named for it: the command
    ends with the database's own error, nothing read after it and nothing
    deleted.
    """
    make_tick("k", days_ago=30)
    make_tick("k", days_ago=10)
    insert_tick("k", NEWEST_HERE[connection.vendor])
    before = remaining()
    refusing = Refusing(TICKS)

    assert connection.get_autocommit()
    assert not connection.in_atomic_block
    with pytest.raises(DataError) as caught, connection.execute_wrapper(refusing):
        prune(*args)

    refusing.assert_raised_as_it_was(caught)
    assert remaining() == before


@pytest.mark.django_db(transaction=True)
class TestAnchorsUnderConcurrentChange:
    def test_a_tick_that_became_its_schedules_newest_since_the_read_is_kept(self):
        # Between the read that found each schedule's newest tick and the
        # delete, that tick goes, deleted on another connection. The older
        # one the batch was going to delete is now the schedule's newest,
        # and deleting it as well would leave the schedule no history.
        oldest = make_tick("k", days_ago=30)
        middle = make_tick("k", days_ago=20)
        newest = make_tick("k", days_ago=10)
        state = {"selected": False, "done": False}

        def wrapper(execute, sql, params, many, context):
            if state["selected"] and not state["done"]:
                state["done"] = True
                in_another_connection(
                    f"DELETE FROM {TICKS} WHERE id = %s",  # noqa: S608
                    [newest],
                )
            result = execute(sql, params, many, context)
            text = _sql(sql)
            if (
                text.lstrip().startswith("SELECT")
                and f"{TICKS.upper()}.SCHEDULED_FOR <" in text
            ):
                state["selected"] = True
            return result

        with connection.execute_wrapper(wrapper):
            out, _ = prune()

        assert state["done"], "the prune never read a batch"
        assert remaining() == {middle}
        assert oldest not in remaining()
        assert "Deleted 1 schedule tick row(s)" in out

    def test_a_tick_dispatched_during_the_prune_is_kept(self):
        # A worker dispatches while the prune is going through its batches,
        # late, for an instant before the cutoff: the new tick is selected
        # like any old one. It is its schedule's newest, the tick the next
        # pass compares against, and deleting it would let that pass run
        # the same instant again.
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=20)
        selection_newest = make_tick("k", days_ago=10)
        late = timezone.now() - timedelta(hours=1)
        state = {"selected": False, "done": False}

        def wrapper(execute, sql, params, many, context):
            if state["selected"] and not state["done"]:
                state["done"] = True
                in_another_connection(
                    f"INSERT INTO {TICKS} "  # noqa: S608
                    "(schedule_name, scheduled_for, task_id, created_at) "
                    "VALUES ('k', %s, NULL, %s)",
                    [connection.ops.adapt_datetimefield_value(late)] * 2,
                )
            result = execute(sql, params, many, context)
            text = _sql(sql)
            if (
                text.lstrip().startswith("SELECT")
                and f"{TICKS.upper()}.SCHEDULED_FOR <" in text
            ):
                state["selected"] = True
            return result

        with connection.execute_wrapper(wrapper):
            out, _ = prune("--older-than", "0", "--batch-size", "1")

        assert state["done"], "the prune never read a batch"
        dispatched = OxScheduleTick.objects.get(scheduled_for=late).pk
        assert remaining() == {selection_newest, dispatched}
        assert "Deleted 2 schedule tick row(s)" in out

    def test_a_prune_holds_no_lock_on_the_tick_it_keeps(self):
        # A worker's first-sighting read is a locking read that skips locked
        # tick rows. A prune whose rows are still locked, inside a caller's
        # transaction, must leave that read the tick it keeps: had it locked
        # that one too, the schedule would look like one never seen.
        if not connection.features.has_select_for_update_skip_locked:
            pytest.skip(f"{connection.vendor} has no SKIP LOCKED")
        make_tick("k", days_ago=30)
        make_tick("k", days_ago=20)
        kept = make_tick("k", days_ago=10)
        pruned = threading.Event()
        release = threading.Event()
        errors = []

        def prune_and_hold():
            try:
                with transaction.atomic():
                    prune("--older-than", "0")
                    pruned.set()
                    release.wait(30)
            except BaseException as exc:
                errors.append(exc)
            finally:
                pruned.set()
                connections.close_all()

        thread = threading.Thread(target=prune_and_hold)
        thread.start()
        try:
            assert pruned.wait(30)
            assert not errors, errors
            with transaction.atomic():
                earliest = (
                    OxScheduleTick.objects.filter(schedule_name="k")
                    .order_by("scheduled_for")
                    .select_for_update(skip_locked=True)
                    .values_list("pk", flat=True)
                    .first()
                )
        finally:
            release.set()
            thread.join(60)
        assert not errors, errors
        assert earliest == kept
        assert remaining() == {kept}


@pytest.mark.django_db(transaction=True)
def test_the_tick_pass_reads_and_deletes_a_batch_at_a_time():
    # Outside any transaction, so each read is its statement alone. The
    # schedule-name read, one read a schedule for its newest tick, then for
    # each batch its read, the check for any that became its schedule's
    # newest, and the DELETE. The last batch is short, so no empty read
    # follows it.
    for key in ("a", "b", "c"):
        make_tick(key, days_ago=30)
        make_tick(key, days_ago=20)
        make_tick(key, days_ago=1)

    with CaptureQueriesContext(connection) as queries:
        out, _ = prune("--batch-size", "4")

    statements = [_sql(q["sql"]).lstrip() for q in queries.captured_queries]
    ticks = [s for s in statements if TICKS.upper() in s]
    selects = [s for s in ticks if s.startswith("SELECT")]
    deletes = [s for s in ticks if s.startswith("DELETE")]
    assert len(selects) == 1 + 3 + 2 + 2, ticks
    assert len(deletes) == 2, ticks
    assert len(ticks) == len(selects) + len(deletes), ticks
    # None of them asks, row by row, whether its schedule has a newer tick.
    # That question is kept for a row whose schedule's kept tick has gone:
    # on MySQL each answer walks the schedule's history up to the row, and
    # a dry run, which deletes nothing on the way, would walk all of it for
    # every row.
    assert not [s for s in ticks if "EXISTS" in s], ticks
    assert "Deleted 6 schedule tick row(s)" in out
    assert OxScheduleTick.objects.count() == 3


@pytest.mark.django_db(transaction=True)
def test_a_key_in_two_spellings_is_pruned_a_batch_at_a_time():
    # MySQL's collation takes "Report" and "report" for one key: one unique
    # index entry a tick, one history to every read by key, and one of the
    # two spellings back from the read of every schedule's key. The rows
    # that hold the other spelling are still checked with their batch.
    # Asked row by row instead, whether its schedule has a newer tick, each
    # answer walks the schedule's history up to the row.
    if connection.vendor != "mysql":
        pytest.skip(
            "MySQL compares schedule keys by a collation that ignores case; "
            f"this run is on {connection.vendor}, which does not"
        )
    for days in (40, 30, 20):
        make_tick("Report", days_ago=days)
    for days in (35, 25, 15):
        make_tick("report", days_ago=days)
    newest = make_tick("report", days_ago=1)

    with CaptureQueriesContext(connection) as queries:
        out, _ = prune("--batch-size", "4")

    statements = [_sql(q["sql"]).lstrip() for q in queries.captured_queries]
    ticks = [s for s in statements if TICKS.upper() in s]
    assert not [s for s in ticks if "EXISTS" in s], ticks
    assert "Deleted 6 schedule tick row(s)" in out
    assert remaining() == {newest}


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("args", [(), ("--dry-run",)], ids=["run", "dry-run"])
def test_a_schedule_first_dispatched_during_the_prune_keeps_its_tick(args):
    # Its key was not among those read at the start, and its one tick is
    # older than a cutoff of now: the row is its schedule's whole history.
    make_tick("k", days_ago=30)
    kept = make_tick("k", days_ago=10)
    first = timezone.now() - timedelta(hours=1)
    state = {"keys_read": False, "done": False}

    def wrapper(execute, sql, params, many, context):
        # Before the statement that follows the read of the keys, when that
        # read's cursor is closed and SQLite lets another connection write.
        if state["keys_read"] and not state["done"]:
            state["done"] = True
            in_another_connection(
                f"INSERT INTO {TICKS} "  # noqa: S608
                "(schedule_name, scheduled_for, task_id, created_at) "
                "VALUES ('late', %s, NULL, %s)",
                [connection.ops.adapt_datetimefield_value(first)] * 2,
            )
        result = execute(sql, params, many, context)
        text = _sql(sql)
        if "DISTINCT" in text and TICKS.upper() in text:
            state["keys_read"] = True
        return result

    before = remaining()
    with connection.execute_wrapper(wrapper):
        out, _ = prune("--older-than", "0", "--batch-size", "1", *args)

    assert state["done"], "the prune never read the schedule keys"
    late = OxScheduleTick.objects.get(schedule_name="late").pk
    assert remaining() == (before | {late} if args else {kept, late})
    verb, keeps = ("Would delete", "Would keep") if args else ("Deleted", "Kept")
    assert f"{verb} 1 schedule tick row(s)" in out
    assert f"{keeps} 2 schedule tick row(s) scheduled before " in out


@pytest.mark.django_db
class TestPurgeTakesATarget:
    def test_the_flag_alone_is_refused(self):
        bad = insert_tick("k", OLDEST_HERE[connection.vendor])

        with pytest.raises(CommandError) as info:
            prune("--purge-unreadable-ticks")

        assert str(info.value) == (
            "--purge-unreadable-ticks needs at least one --schedule-key or "
            "--tick-pk. On MySQL, --schedule-key uses the tick table's collation, "
            "which may match keys differing in case, accents or trailing spaces. "
            "Inspect --dry-run before removing rows, or use --tick-pk to target "
            "individual rows."
        )
        assert remaining() == {bad}

    @pytest.mark.parametrize(
        "args", [("--schedule-key", "k"), ("--tick-pk", "1")], ids=["key", "pk"]
    )
    def test_a_target_without_the_flag_is_refused(self, args):
        bad = insert_tick("k", OLDEST_HERE[connection.vendor])

        with pytest.raises(CommandError) as info:
            prune(*args)

        assert str(info.value) == (
            "Use --schedule-key and --tick-pk with --purge-unreadable-ticks."
        )
        assert remaining() == {bad}

    def test_prune_options_beside_it_are_refused(self):
        with pytest.raises(CommandError) as info:
            prune(
                "--purge-unreadable-ticks",
                "--schedule-key",
                "k",
                "--queue",
                "emails",
                "--older-than",
                "1d",
                "--include-failed",
            )

        assert str(info.value) == (
            "--purge-unreadable-ticks cannot be combined with --queue, "
            "--older-than, --include-failed. Run ordinary pruning separately."
        )


@pytest.fixture
def two_schedules():
    """
    Schedule "k" with an unreadable oldest and newest tick around readable
    ones, and schedule "j" with one unreadable tick. Their keys by name.
    """
    vendor = connection.vendor
    return {
        "k_oldest": insert_tick("k", OLDEST_HERE[vendor]),
        "k_readable": [make_tick("k", days_ago=days) for days in (30, 1)],
        "k_newest": insert_tick("k", NEWEST_HERE[vendor]),
        "j": insert_tick("j", OLDEST_HERE[vendor]),
    }


@pytest.mark.django_db
class TestPurgeUnreadableTicks:
    def test_dry_run_names_each_tick_and_changes_nothing(self, two_schedules):
        before = remaining()

        out, err = prune("--purge-unreadable-ticks", "--schedule-key", "k", "--dry-run")

        assert remaining() == before
        lines = out.splitlines()
        assert lines[0] == "Would remove 2 unreadable schedule tick row(s):"
        for pk in (two_schedules["k_oldest"], two_schedules["k_newest"]):
            assert any(
                line.startswith(f"  tick {pk}, schedule 'k': scheduled_for ")
                for line in lines
            ), out
        assert err.startswith("Warning: removing dispatch history can make")

    def test_removes_only_the_named_schedules_unreadable_ticks(self, two_schedules):
        out, err = prune("--purge-unreadable-ticks", "--schedule-key", "k")

        assert remaining() == {*two_schedules["k_readable"], two_schedules["j"]}
        assert out.startswith("Removed 2 unreadable schedule tick row(s):")
        assert "Warning: removing dispatch history" in err

    def test_json_names_what_it_removed(self, two_schedules):
        out, _ = prune(
            "--purge-unreadable-ticks",
            "--schedule-key",
            "k",
            "--schedule-key",
            "j",
            "--format",
            "json",
        )

        data = json.loads(out)
        assert out.strip() == json.dumps(data)
        assert data["tick_rows"] == 3
        assert data["dry_run"] is False
        assert data["schedule_keys"] == ["k", "j"]
        assert {named["pk"] for named in data["unreadable_ticks"]} == {
            two_schedules["k_oldest"],
            two_schedules["k_newest"],
            two_schedules["j"],
        }
        assert data["kept_ticks"] == []
        assert data["missing_tick_pks"] == []
        assert remaining() == set(two_schedules["k_readable"])

    def test_by_tick_pk_removes_only_that_tick(self, two_schedules):
        readable = two_schedules["k_readable"][0]

        out, _ = prune(
            "--purge-unreadable-ticks",
            "--tick-pk",
            str(two_schedules["j"]),
            "--tick-pk",
            str(readable),
            "--tick-pk",
            "987654321",
        )

        assert two_schedules["j"] not in remaining()
        assert remaining() == {
            two_schedules["k_oldest"],
            two_schedules["k_newest"],
            *two_schedules["k_readable"],
        }
        assert out.startswith("Removed 1 unreadable schedule tick row(s):")
        assert f"  tick {two_schedules['j']}, schedule 'j': " in out
        assert (
            f"Kept tick {readable}, schedule 'k': the row is readable, with "
            "scheduled_for " in out
        )
        assert "No tick row 987654321." in out

    @pytest.mark.parametrize(
        "number",
        [2**63, -(2**63) - 1, 10**30],
        ids=["one-past-the-largest", "one-before-the-smallest", "thirty-digits"],
    )
    def test_a_number_no_primary_key_can_be_names_no_tick(self, two_schedules, number):
        # Beside a tick that is there and does not read, which still goes.
        out, _ = prune(
            "--purge-unreadable-ticks",
            "--tick-pk",
            str(number),
            "--tick-pk",
            str(two_schedules["j"]),
        )

        assert two_schedules["j"] not in remaining()
        assert len(remaining()) == 4
        assert out.startswith("Removed 1 unreadable schedule tick row(s):")
        assert f"No tick row {number}." in out

    def test_a_schedule_with_nothing_unreadable_is_left_alone(self):
        ticks = {make_tick("k", days_ago=days) for days in (30, 1)}

        out, err = prune("--purge-unreadable-ticks", "--schedule-key", "k")

        assert remaining() == ticks
        assert out == "Removed 0 unreadable schedule tick row(s).\n"
        assert err == ""

    def test_each_removed_tick_is_logged_by_event(self, two_schedules, caplog):
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            prune("--purge-unreadable-ticks", "--tick-pk", str(two_schedules["j"]))

        (record,) = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_tick_history_removed"
        ]
        assert record.database == DEFAULT_DB_ALIAS
        assert record.schedule_key == "j"
        assert record.tick_pk == two_schedules["j"]
        assert record.reason.startswith("scheduled_for ")

    def test_removes_without_loading_the_rows(self, two_schedules):
        # With anything listening for delete signals, Django's delete() loads
        # each row first, and Django's read of this one raises, or on MySQL
        # with USE_TZ off reads the zero date as text.
        try:
            with transaction.atomic():
                loaded = OxScheduleTick.objects.get(pk=two_schedules["j"])
        except Exception:  # what Django's read raised; nothing to keep
            loaded = None
        assert loaded is None or not isinstance(loaded.scheduled_for, datetime)

        def listener(sender, **kwargs):
            pass

        pre_delete.connect(listener, weak=False, dispatch_uid="test_purge_listener")
        try:
            out, _ = prune("--purge-unreadable-ticks", "--schedule-key", "j")
        finally:
            pre_delete.disconnect(dispatch_uid="test_purge_listener")

        assert two_schedules["j"] not in remaining()
        assert out.startswith("Removed 1 unreadable schedule tick row(s):")

    def test_a_failure_part_way_reports_what_is_already_gone(
        self, two_schedules, caplog
    ):
        out = StringIO()
        with (
            caplog.at_level(logging.WARNING, logger="django_ox"),
            failing(
                "DELETE FROM DJANGO_OX_OXSCHEDULETICK",
                lambda: simulated("mysql-deadlock"),
                lambda n: n == 2,
            ),
            pytest.raises(DatabaseError),
        ):
            call_command(
                "ox_prune",
                "--purge-unreadable-ticks",
                "--schedule-key",
                "k",
                "--batch-size",
                "1",
                "--format",
                "json",
                stdout=out,
                stderr=StringIO(),
            )

        data = json.loads(out.getvalue())
        assert data["tick_rows"] == 1
        ((gone,),) = [data["unreadable_ticks"]]
        assert gone["pk"] not in remaining()
        assert (
            len(remaining() & {two_schedules["k_oldest"], two_schedules["k_newest"]})
            == 1
        )
        assert [
            r.tick_pk
            for r in caplog.records
            if getattr(r, "event", None) == "schedule_tick_history_removed"
        ] == [gone["pk"]]

    def test_a_tick_whose_schedule_key_cannot_be_handed_over_goes_by_its_pk(self):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        readable = {make_tick("k", days_ago=days) for days in (30, 1)}
        bad = insert_tick_keyed(UNDECODABLE_KEY, days_ago=30)

        out, _ = prune("--purge-unreadable-ticks", "--tick-pk", str(bad))

        assert remaining() == readable
        assert out.startswith("Removed 1 unreadable schedule tick row(s):")
        assert f"  tick {bad}, schedule '': schedule_name could not be read" in out

    @pytest.mark.parametrize("fmt", ["text", "json"])
    def test_a_tick_whose_schedule_key_is_not_text_goes_by_its_primary_key(self, fmt):
        only_on("sqlite", "SQLite keeps bytes where a schedule key should be")
        bad = insert_tick_keyed(BYTES_KEY, days_ago=30)
        other = insert_tick_keyed(BYTES_KEY, days_ago=1)
        readable = make_tick("k", days_ago=30)

        out, _ = prune(
            "--purge-unreadable-ticks", "--tick-pk", str(bad), "--format", fmt
        )

        assert remaining() == {other, readable}
        reason = "schedule_name holds b'nightly', which is not text"
        if fmt == "json":
            data = json.loads(out)
            assert data["unreadable_ticks"] == [
                {"pk": bad, "schedule_name": "", "reason": reason}
            ]
        else:
            assert out.startswith("Removed 1 unreadable schedule tick row(s):")
            assert f"  tick {bad}, schedule '': {reason}" in out

    def test_a_tick_the_driver_cannot_hand_over_is_removed(self):
        only_on("sqlite", "SQLite keeps text its driver cannot decode as UTF-8")
        readable = [make_tick("k", days_ago=days) for days in (30, 20, 10, 1)]
        bad = insert_tick("k", "CAST(X'30303030FF' AS TEXT)")

        out, _ = prune(
            "--purge-unreadable-ticks", "--schedule-key", "k", "--batch-size", "2"
        )

        assert remaining() == set(readable)
        assert f"  tick {bad}, schedule 'k': scheduled_for could not be read" in out


@pytest.mark.django_db(transaction=True)
def test_a_tick_repaired_since_it_was_found_is_not_removed():
    # Between the read that found the tick and its removal, the tick is
    # repaired on another connection. The removal reads each row again
    # under its lock, and only rows that still cannot be read go.
    bad = insert_tick("k", OLDEST_HERE[connection.vendor])
    repaired_to = (timezone.now() - timedelta(days=3)).replace(microsecond=0)
    state = {"done": False}

    def wrapper(execute, sql, params, many, context):
        text = _sql(sql).lstrip()
        removing = TICKS.upper() in text and (
            "FOR UPDATE" in text or text.startswith("UPDATE")
        )
        if removing and not state["done"]:
            state["done"] = True
            in_another_connection(
                f"UPDATE {TICKS} SET scheduled_for = %s WHERE id = %s",  # noqa: S608
                [connection.ops.adapt_datetimefield_value(repaired_to), bad],
            )
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        out, _ = prune("--purge-unreadable-ticks", "--schedule-key", "k")

    assert state["done"], "the purge never reached its removal"
    assert remaining() == {bad}
    assert OxScheduleTick.objects.get(pk=bad).scheduled_for == repaired_to
    assert out.startswith("Removed 0 unreadable schedule tick row(s).")
    assert (
        f"Kept tick {bad}, schedule 'k': the row is now readable, with scheduled_for "
        in out
    )


# -- how a tick row is deleted ------------------------------------------------


class Listening:
    """
    Receivers for both delete signals of every model, connected for a
    block. `sent` is each one sent, as (signal, model, primary key).
    """

    def __init__(self):
        self.sent = []

    def _pre(self, sender, instance, **kwargs):
        self.sent.append(("pre_delete", sender, instance.pk))

    def _post(self, sender, instance, **kwargs):
        self.sent.append(("post_delete", sender, instance.pk))

    def __enter__(self):
        pre_delete.connect(self._pre, weak=False, dispatch_uid="test_prune_pre")
        post_delete.connect(self._post, weak=False, dispatch_uid="test_prune_post")
        return self

    def __exit__(self, *exc_info):
        pre_delete.disconnect(dispatch_uid="test_prune_pre")
        post_delete.disconnect(dispatch_uid="test_prune_post")

    def of(self, model):
        return [(signal, pk) for signal, sender, pk in self.sent if sender is model]


class Deletes:
    """connection.execute_wrapper() keeping each DELETE on the tick table as sent."""

    def __init__(self):
        self.sent = []

    def __call__(self, execute, sql, params, many, context):
        if _sql(sql).lstrip().startswith("DELETE FROM DJANGO_OX_OXSCHEDULETICK"):
            self.sent.append((sql, list(params)))
        return execute(sql, params, many, context)


def a_finished_task(*, days_ago):
    when = timezone.now() - timedelta(days=days_ago)
    return OxTask.objects.create(
        task_path="tests.tasks.add",
        backend_name="default",
        queue_name="default",
        enqueued_at=when,
        status=OxTask.Status.SUCCESSFUL,
        finished_at=when,
    )


#: Each way the command removes tick rows, as its arguments.
REMOVALS = [
    pytest.param((), id="prune"),
    pytest.param(("--purge-unreadable-ticks", "--schedule-key", "k"), id="purge-key"),
    pytest.param(("--purge-unreadable-ticks", "--tick-pk", "{oldest}"), id="purge-pk"),
]


def some_of_each():
    """
    Schedule "k" with an unreadable oldest and newest tick around three
    that read, two of them past the cutoff, and schedule "h" with only
    ticks that read. Their keys by name.
    """
    vendor = connection.vendor
    return {
        "oldest": insert_tick("k", OLDEST_HERE[vendor]),
        "k_old": [make_tick("k", days_ago=days) for days in (40, 30)],
        "k_kept": make_tick("k", days_ago=1),
        "newest": insert_tick("k", NEWEST_HERE[vendor]),
        "h_old": [make_tick("h", days_ago=days) for days in (40, 30)],
        "h_kept": make_tick("h", days_ago=1),
    }


def gone_by(args, ticks):
    """The tick rows a run with these arguments removes, of `some_of_each`."""
    if not args:
        return {*ticks["k_old"], *ticks["h_old"]}
    if "--tick-pk" in args:
        return {ticks["oldest"]}
    return {ticks["oldest"], ticks["newest"]}


@pytest.mark.django_db
class TestHowATickRowIsDeleted:
    """
    A tick row goes in a DELETE of the command's own, by primary key: the
    same statement for a batch of ticks that read as for one holding a
    tick that does not, in ordinary pruning and in a named removal. No
    delete signal is sent for a tick row, since sending one means loading
    the row. Task rows are deleted as they were, and send theirs.
    """

    @pytest.mark.parametrize("args", REMOVALS)
    def test_no_delete_signal_is_sent_for_a_tick_row(self, args):
        ticks = some_of_each()
        before = remaining()
        args = [arg.format(oldest=ticks["oldest"]) for arg in args]

        with Listening() as listening:
            prune(*args)

        assert before - remaining() == gone_by(args, ticks)
        assert listening.of(OxScheduleTick) == []

    def test_nor_for_a_batch_in_which_every_tick_reads(self):
        old = [make_tick("h", days_ago=days) for days in (40, 30)]
        kept = make_tick("h", days_ago=1)

        with Listening() as listening:
            out, err = prune()

        assert remaining() == {kept}
        assert "Deleted 2 schedule tick row(s)" in out
        assert err == ""
        assert set(old).isdisjoint(remaining())
        assert listening.sent == []

    def test_a_task_row_still_sends_both_of_its_own(self):
        old = [a_finished_task(days_ago=30) for _ in range(3)]
        recent = a_finished_task(days_ago=1)
        when = timezone.now() - timedelta(days=30)
        # A tick that points at a task that goes: kept as its schedule's
        # newest, with its task cleared and no signal of its own.
        tick = OxScheduleTick.objects.create(
            schedule_name="h", scheduled_for=when, task=old[0], created_at=when
        )

        with Listening() as listening:
            out, _ = prune()

        assert "Deleted 3 SUCCESSFUL/DISCARDED task row(s)" in out
        assert set(OxTask.objects.values_list("pk", flat=True)) == {recent.pk}
        sent = listening.of(OxTask)
        assert sorted(pk for signal, pk in sent if signal == "pre_delete") == sorted(
            task.pk for task in old
        )
        assert sorted(pk for signal, pk in sent if signal == "post_delete") == sorted(
            task.pk for task in old
        )
        assert len(sent) == 6
        assert OxScheduleTick.objects.get(pk=tick.pk).task_id is None
        assert listening.of(OxScheduleTick) == []

    @pytest.mark.parametrize("args", REMOVALS)
    def test_it_goes_in_one_statement_of_the_commands_own(self, args):
        ticks = some_of_each()
        args = [arg.format(oldest=ticks["oldest"]) for arg in args]
        expected = sorted(gone_by(args, ticks))
        deletes = Deletes()

        with connection.execute_wrapper(deletes):
            prune(*args)

        # The table and its key quoted as this database quotes them, and
        # the keys as parameters: none of them written into the statement.
        quote = connection.ops.quote_name
        table, key = quote(TICKS), quote("id")
        # One statement for the batch: every old row ordinary pruning read,
        # or every row a named removal found.
        ((statement, params),) = deletes.sent
        marks = ", ".join(["%s"] * len(params))
        assert statement == " ".join(
            ["DELETE FROM", table, "WHERE", key, f"IN ({marks})"]
        )
        assert not [char for char in statement if char.isdigit()]
        assert all(type(pk) is int for pk in params)
        assert sorted(params) == expected

    @pytest.mark.parametrize("args", REMOVALS[:2])
    def test_a_statement_carries_no_more_keys_than_a_batch(self, args):
        ticks = some_of_each()
        deletes = Deletes()

        with connection.execute_wrapper(deletes):
            prune(*args, "--batch-size", "1")

        removed = gone_by(args, ticks)
        assert [len(params) for _, params in deletes.sent] == [1] * len(removed)
        assert {pk for _, (pk,) in deletes.sent} == removed

    @pytest.mark.parametrize("args", REMOVALS)
    def test_no_tick_row_goes_through_a_delete_of_djangos(self, args, monkeypatch):
        ticks = some_of_each()
        before = remaining()
        args = [arg.format(oldest=ticks["oldest"]) for arg in args]
        raw_delete = QuerySet._raw_delete
        through_django = []

        def recording(queryset, using):
            through_django.append(queryset.model)
            return raw_delete(queryset, using)

        monkeypatch.setattr(QuerySet, "_raw_delete", recording)
        with CaptureQueriesContext(connection) as queries:
            prune(*args)

        assert before - remaining() == gone_by(args, ticks)
        assert OxScheduleTick not in through_django
        # Nor is a tick row read to be deleted: no statement takes the
        # column a delete of Django's would load every row for.
        read_whole = [
            q["sql"]
            for q in queries.captured_queries
            if _sql(q["sql"]).lstrip().startswith("SELECT")
            and f"{TICKS.upper()}.CREATED_AT" in _sql(q["sql"])
        ]
        assert read_whole == []

    def test_a_dry_run_sends_none(self):
        some_of_each()
        before = remaining()
        deletes = Deletes()

        with Listening() as listening, connection.execute_wrapper(deletes):
            prune("--dry-run")
            prune("--purge-unreadable-ticks", "--schedule-key", "k", "--dry-run")

        assert remaining() == before
        assert deletes.sent == []
        assert listening.sent == []


@pytest.mark.django_db(transaction=True)
def test_a_named_removal_deletes_inside_the_transaction_that_read_the_rows_again():
    # With no transaction of the caller's, so the command's own shows: the
    # batch is locked and read again, and its DELETE goes before the
    # commit that lets the locks go.
    bad = insert_tick("k", OLDEST_HERE[connection.vendor])

    with CaptureQueriesContext(connection) as queries:
        prune("--purge-unreadable-ticks", "--tick-pk", str(bad))

    words = [_sql(q["sql"]).split()[0] for q in queries.captured_queries]
    removal = words[words.index("BEGIN") :]
    locking = (
        ["SELECT"]
        if connection.features.has_select_for_update
        else ["UPDATE", "SELECT"]
    )
    assert removal == ["BEGIN", *locking, "DELETE", "COMMIT"]
    assert remaining() == set()


# -- a settings schedule an unreadable tick holds up -------------------------

BLOCKED = "blocked-every-second"
CONTROL = "control-every-second"


def command(root, *args):
    """A real `manage.py` run in the project at `root`: (exit code, out, err)."""
    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = "isoproj.settings"
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    # The arguments are this test's own literals.
    done = subprocess.run(  # noqa: S603
        [sys.executable, "manage.py", *args],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=GUARD,
        check=False,
    )
    return done.returncode, done.stdout, done.stderr


def ran(label):
    """How many tasks a schedule labelled `label` has enqueued that ran."""
    finished = OxTask.objects.filter(status=OxTask.Status.SUCCESSFUL)
    return sum(args == [label] for args in finished.values_list("args", flat=True))


def run_worker_until(root, log, what, predicate):
    proc = start(root, log, batch=False)
    try:
        if not wait_for(predicate, timeout=GUARD):
            pytest.fail(f"no {what} within {GUARD}s:\n{log.read_text()[-4000:]}")
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
        finish(proc, log)


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(os.name != "posix", reason="worker processes")
def test_a_settings_schedule_held_up_by_an_unreadable_tick_fires_after_its_removal(
    tmp_path,
):
    every_second = {"task": "tests.tasks.labelled", "every": 1}
    root = project(
        tmp_path,
        {
            BLOCKED: {**every_second, "args": [BLOCKED]},
            CONTROL: {**every_second, "args": [CONTROL]},
        },
    )
    bad = insert_tick(BLOCKED, OLDEST_HERE[connection.vendor])

    # Held up: the schedule beside it fires twice, and this one never does.
    run_worker_until(
        root, tmp_path / "first.log", "second control run", lambda: ran(CONTROL) >= 2
    )
    assert ran(BLOCKED) == 0
    blocked = OxScheduleTick.objects.filter(schedule_name=BLOCKED)
    assert list(blocked.values_list("pk", flat=True)) == [bad]

    # Ordinary pruning keeps it, completes and says so.
    code, out, err = command(root, "ox_prune", "--format", "json")
    assert code == 0, err
    data = json.loads(out)
    assert [named["pk"] for named in data["unreadable_ticks"]] == [bad]
    assert f"  tick {bad}, schedule '{BLOCKED}': " in err

    # A dry run names it and leaves it.
    code, out, err = command(
        root,
        "ox_prune",
        "--purge-unreadable-ticks",
        "--schedule-key",
        BLOCKED,
        "--dry-run",
    )
    assert code == 0, err
    assert out.startswith("Would remove 1 unreadable schedule tick row(s):")
    assert OxScheduleTick.objects.filter(pk=bad).exists()

    code, out, err = command(
        root, "ox_prune", "--purge-unreadable-ticks", "--schedule-key", BLOCKED
    )
    assert code == 0, err
    assert out.startswith("Removed 1 unreadable schedule tick row(s):")
    assert "Warning: removing dispatch history" in err
    assert not OxScheduleTick.objects.filter(schedule_name=BLOCKED).exists()

    # Started again, the schedule is a first sighting: it anchors without
    # running, then fires from its next tick on.
    run_worker_until(
        root,
        tmp_path / "second.log",
        "run of the released schedule",
        lambda: ran(BLOCKED) >= 1,
    )
    history = list(
        OxScheduleTick.objects.filter(schedule_name=BLOCKED)
        .order_by("scheduled_for")
        .values_list("scheduled_for", "task_id")
    )
    assert history[0][1] is None, history
    assert all(task is not None for _, task in history[1:]), history
    assert len(history) >= 2, history


#: Four spellings of one key and, for each, an unreadable newest tick. They
#: differ in case, in an accent and in a trailing space, which a database
#: compares as equal or not by the collation of the column.
SPELLINGS = ["nightly", "Nightly", "nightly ", "nightl\u00fd"]


def spelled_ticks():
    """An unreadable tick under each spelling, with a readable one beside it."""
    literals = {
        "postgresql": lambda i: f"'{10000 + i}-01-01 00:00:00+00'",
        "mysql": lambda i: f"'9999-00-0{i + 1} 00:00:00'",
        "sqlite": lambda i: f"'banana {i}'",
    }[connection.vendor]
    unreadable = {
        key: insert_tick(key, literals(index)) for index, key in enumerate(SPELLINGS)
    }
    readable = {
        key: make_tick(key, days_ago=3 + index) for index, key in enumerate(SPELLINGS)
    }
    return unreadable, readable


def key_collation():
    """The collation MySQL compares the tick table's schedule keys by."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "AND COLUMN_NAME = 'schedule_name'",
            [TICKS],
        )
        return cursor.fetchone()[0]


def spellings_the_database_calls_nightly():
    """
    The spellings above that `schedule_name = 'nightly'` matches here: only
    the exact one where keys compare exactly (PostgreSQL, SQLite), and on
    MySQL the ones its collation calls equal.
    """
    if connection.vendor != "mysql":
        return {"nightly"}
    collation = key_collation()
    if collation == "utf8mb4_unicode_ci":
        # Ignores case and accents, and pads with spaces when comparing.
        return set(SPELLINGS)
    if collation == "utf8mb4_0900_ai_ci":
        # Ignores case and accents, and does not pad.
        return set(SPELLINGS) - {"nightly "}
    pytest.skip(f"no expectation is written for the collation {collation}")


@pytest.mark.django_db
class TestAKeyIsWhatTheDatabaseCallsTheSameKey:
    """
    `--schedule-key` selects with the database's own comparison of the key.
    PostgreSQL and SQLite compare exactly. MySQL compares by the collation
    of the column, so a purge by one spelling can take the unreadable ticks
    of spellings it calls equal. The dry run and the purge agree with each
    other either way, and a tick named by its primary key is that tick alone.
    """

    def test_a_purge_takes_the_spellings_the_comparison_matches(self):
        unreadable, readable = spelled_ticks()
        expected = {unreadable[key] for key in spellings_the_database_calls_nightly()}

        out, _ = prune(
            "--purge-unreadable-ticks", "--schedule-key", "nightly", "--format", "json"
        )

        data = json.loads(out)
        assert {named["pk"] for named in data["unreadable_ticks"]} == expected
        assert remaining() == (
            set(unreadable.values()) - expected | set(readable.values())
        )

    def test_the_dry_run_names_what_the_purge_then_removes(self):
        unreadable, _ = spelled_ticks()
        before = remaining()

        out, _ = prune(
            "--purge-unreadable-ticks",
            "--schedule-key",
            "nightly",
            "--dry-run",
            "--format",
            "json",
        )
        listed = {named["pk"] for named in json.loads(out)["unreadable_ticks"]}
        assert remaining() == before

        prune("--purge-unreadable-ticks", "--schedule-key", "nightly")

        assert before - remaining() == listed
        assert listed == {
            unreadable[key] for key in spellings_the_database_calls_nightly()
        }

    def test_a_readable_tick_is_never_taken(self):
        _, readable = spelled_ticks()

        prune("--purge-unreadable-ticks", "--schedule-key", "nightly")

        assert set(readable.values()) <= remaining()

    @pytest.mark.parametrize("spelling", SPELLINGS)
    def test_a_tick_pk_removes_that_tick_whatever_the_comparison_says(self, spelling):
        unreadable, readable = spelled_ticks()

        out, _ = prune(
            "--purge-unreadable-ticks", "--tick-pk", str(unreadable[spelling])
        )

        assert out.startswith("Removed 1 unreadable schedule tick row(s):")
        assert remaining() == (
            set(unreadable.values()) - {unreadable[spelling]} | set(readable.values())
        )
