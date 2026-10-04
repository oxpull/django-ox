"""
Several stored schedules created in one call: all of them or none.

`create_schedules` is what the beat importer's output calls, so the rows it
is given were written by a program and pasted by a person. Two things
follow, and they are what these tests hold. A batch that is refused has
written nothing, so a paste that fails can be corrected and pasted again
whole. And the refusal names every failing row at once, by its position,
because a name can be missing or repeated and a position cannot.
"""

import itertools
import threading
import time
import uuid
from datetime import timedelta

import pytest
from django import forms
from django.core.exceptions import (
    NON_FIELD_ERRORS,
    PermissionDenied,
    ValidationError,
)
from django.db import (
    DataError,
    IntegrityError,
    OperationalError,
    connection,
    connections,
    transaction,
)
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from django_ox import stored
from django_ox.models import OxSchedule, OxScheduleChange, OxTask
from django_ox.registry import ArgsForm, ScheduleKind, register
from django_ox.stored import boundary_digest, create_schedule, create_schedules

from . import tasks

pytestmark = pytest.mark.django_db

GUARD = "payroll.run_payroll"


class _Args(ArgsForm):
    region = forms.CharField()


@pytest.fixture(autouse=True)
def _registry(monkeypatch):
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)
    register(ScheduleKind(key="report", task=tasks.add))
    register(ScheduleKind(key="checked", task=tasks.add, form=_Args))
    register(ScheduleKind(key="guarded", task=tasks.add, permission=GUARD))


def a_row(name="nightly", **over):
    fields = {
        "name": name,
        "task_key": "report",
        "trigger": "cron",
        "cron": "0 2 * * *",
    }
    fields.update(over)
    return fields


def an_interval(name="every-minute", **over):
    return a_row(
        name, **{"trigger": "interval", "cron": "", "every_seconds": 60, **over}
    )


def reported(caught):
    """Each entry of a refused batch as (index, supplied name, field, message)."""
    return [
        (
            entry.params["index"],
            entry.params["name"],
            entry.params["field"],
            entry.params["message"],
        )
        for entry in caught.value.error_list
    ]


def alone(row, **kwargs):
    """What `create_schedule` says about this row by itself, as (field, message)."""
    with pytest.raises(ValidationError) as caught:
        create_schedule(**row, **kwargs)
    return [
        (field, message)
        for field, messages in caught.value.message_dict.items()
        for message in messages
    ]


def _writes_the_schedule_table(sql):
    return sql.lstrip().upper().startswith("INSERT") and (
        "oxschedule" in sql and "oxschedulechange" not in sql
    )


def writes(captured):
    """The statements that change a row, out of everything a call ran."""
    return [
        query["sql"]
        for query in captured.captured_queries
        if query["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
    ]


def columns(row):
    """Everything a stored row holds apart from its identity."""
    return {
        field.attname: getattr(row, field.attname)
        for field in OxSchedule._meta.concrete_fields
        if field.attname not in {"id", "name"}
    }


class TestABatchIsCreatedWhole:
    def test_every_row_is_created_and_returned_in_the_order_given(self):
        created = create_schedules(
            [
                a_row("a"),
                an_interval("b"),
                a_row("c", task_key="checked", arguments={"region": "emea"}),
            ]
        )
        assert [row.name for row in created] == ["a", "b", "c"]
        assert all(row.pk is not None for row in created)
        assert [row.pk for row in OxSchedule.objects.order_by("pk")] == [
            row.pk for row in created
        ]

    @pytest.mark.parametrize(
        "over",
        [
            {},
            {"trigger": "interval", "cron": "", "every_seconds": 90},
            {
                "trigger": "interval",
                "cron": "",
                "every_seconds": "90",
                "phase_seconds": 30,
                "enabled": False,
                "starting_deadline_seconds": 10,
            },
            {"trigger": OxSchedule.Trigger.CRON},
            {"task_key": "checked", "arguments": {"region": "emea"}},
        ],
    )
    def test_a_row_is_stored_as_create_schedule_stores_it(self, monkeypatch, over):
        # The digest, the count of boundary writes and the three stamps are
        # this module's to write, and a batch writes them the way the single
        # call does. A row whose digest differed would read as stale at its
        # first dispatch and lose the tick it was created for.
        fixed = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: fixed)
        single = create_schedule(**a_row("single", **over))
        (batched,) = create_schedules([a_row("batched", **over)])
        on_disk = OxSchedule.objects.get(pk=batched.pk)
        assert columns(on_disk) == columns(OxSchedule.objects.get(pk=single.pk))
        assert on_disk.boundary_for == boundary_digest(on_disk)

    def test_a_given_start_and_end_are_kept(self):
        start = timezone.now() - timedelta(days=2)
        end = timezone.now() + timedelta(days=2)
        (row,) = create_schedules([a_row(start_time=start, end_time=end)])
        row.refresh_from_db()
        assert (row.start_time, row.end_time) == (start, end)

    def test_any_iterable_of_rows_is_taken(self):
        created = create_schedules(a_row(name) for name in ("a", "b"))
        assert [row.name for row in created] == ["a", "b"]

    def test_the_caller_s_rows_are_left_as_they_were_given(self):
        # The start time defaults per row, and the rows are the caller's:
        # filling it in on their mapping would hand back a batch that, pasted
        # again later, pins every schedule to the first attempt's clock.
        rows = [a_row("a"), an_interval("b")]
        before = [dict(row) for row in rows]
        create_schedules(rows)
        assert rows == before

    def test_an_empty_batch_creates_nothing_and_tells_no_one(self):
        assert create_schedules([]) == []
        assert not OxScheduleChange.objects.exists()


class TestARefusedBatchWritesNothing:
    @pytest.mark.parametrize("broken", [0, 1, 2])
    def test_one_failing_row_and_no_statement_writes(self, broken):
        rows = [a_row("a"), a_row("b"), a_row("c")]
        rows[broken]["cron"] = "banana"
        with (
            CaptureQueriesContext(connection) as captured,
            pytest.raises(ValidationError),
        ):
            create_schedules(rows)
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists(), (
            "a batch that created nothing told the workers the schedules moved"
        )
        # Not merely rolled back. A row written and then undone is a write
        # that happened before every row had passed.
        assert writes(captured) == []

    def test_the_marker_stays_where_it_was(self):
        create_schedule(**a_row("already-here"))
        before = OxScheduleChange.objects.get(id=1).changed_at
        with pytest.raises(ValidationError):
            create_schedules([a_row("fine"), a_row("broken", cron="banana")])
        assert OxScheduleChange.objects.get(id=1).changed_at == before
        assert [row.name for row in OxSchedule.objects.all()] == ["already-here"]

    def test_nothing_is_written_until_the_last_row_has_been_checked(self):
        with CaptureQueriesContext(connection) as captured:
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        statements = [query["sql"] for query in captured.captured_queries]
        name_checks = [
            position
            for position, sql in enumerate(statements)
            if sql.lstrip().upper().startswith("SELECT")
            and "oxschedule" in sql
            and "oxschedulechange" not in sql
        ]
        first_write = statements.index(writes(captured)[0])
        assert len(name_checks) == 3, "each row's name is checked against the table"
        assert max(name_checks) < first_write

    def test_a_keyword_that_is_not_a_field_is_refused_and_the_row_named(self):
        # What `create_schedule` does with a misspelled keyword, which is
        # to say so. Set on the instance instead it would be dropped, and
        # the row created on whatever the real field defaulted to.
        with pytest.raises(TypeError) as caught:
            create_schedules([a_row("a"), a_row("b", cronn="0 3 * * *")])
        assert "cronn" in str(caught.value)
        assert stored._IN_ROW.format(index=1) in str(caught.value)
        assert not OxSchedule.objects.exists()

    def test_the_boundary_s_own_columns_are_refused_as_the_single_call_refuses(self):
        with pytest.raises(TypeError) as caught:
            create_schedules([a_row("a", boundary_generation=99)])
        assert "boundary_generation" in str(caught.value)
        assert not OxSchedule.objects.exists()

    def test_a_row_that_is_not_a_mapping_is_refused_and_named(self):
        with pytest.raises(TypeError) as caught:
            create_schedules([a_row("a"), ("b", "report", "cron", "0 2 * * *")])
        assert str(caught.value) == stored._NOT_A_MAPPING.format(index=1, kind="tuple")
        assert not OxSchedule.objects.exists()


#: One way for a row to be refused each, with the field it is refused on.
FAILING = [
    pytest.param(
        {"task_key": "report", "trigger": "cron", "cron": "0 2 * * *"},
        None,
        "name",
        id="a missing name",
    ),
    pytest.param(
        a_row("unregistered", task_key="os.system"),
        "unregistered",
        "task_key",
        id="an unregistered task_key",
    ),
    pytest.param(a_row("bad-cron", cron="banana"), "bad-cron", "cron", id="a bad cron"),
    pytest.param(
        an_interval("too-fast", every_seconds=0),
        "too-fast",
        "every_seconds",
        id="an interval below one second",
    ),
    pytest.param(
        a_row("not-a-mapping", arguments=[1, 2]),
        "not-a-mapping",
        "arguments",
        id="arguments that are not a mapping",
    ),
    pytest.param(
        a_row("wrong-arguments", task_key="checked", arguments={"wrong": 1}),
        "wrong-arguments",
        "arguments",
        id="arguments the registered form refuses",
    ),
    pytest.param(
        a_row("x" * 129), "x" * 129, "name", id="a name the column cannot hold"
    ),
]


class TestEveryFailureIsReportedAtOnce:
    def test_one_error_holds_every_failing_row(self):
        create_schedule(**a_row("taken"))
        now = timezone.now()
        rows = [
            a_row("fine"),
            {"task_key": "report", "trigger": "cron", "cron": "0 2 * * *"},
            a_row("unregistered", task_key="os.system"),
            a_row("bad-cron", cron="banana"),
            an_interval("too-fast", every_seconds=0),
            a_row("ends-first", start_time=now, end_time=now - timedelta(hours=1)),
            a_row("not-a-mapping", arguments=[1, 2]),
            a_row("taken"),
            a_row("fine", cron="0 3 * * *"),
        ]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        found = reported(caught)
        assert [(index, name, field) for index, name, field, _ in found] == [
            (1, None, "name"),
            (2, "unregistered", "task_key"),
            (3, "bad-cron", "cron"),
            (4, "too-fast", "every_seconds"),
            (5, "ends-first", "end_time"),
            (6, "not-a-mapping", "arguments"),
            (7, "taken", "name"),
            (8, "fine", "name"),
        ]
        # Each message is the one the single call gives for that row. The
        # last is the batch's own: nothing is wrong with that row alone.
        for index, _name, field, message in found[:-1]:
            assert [(field, message)] == alone(rows[index])
        assert found[-1][3] == stored._REPEATED_NAME % {"first": 0}
        assert [row.name for row in OxSchedule.objects.all()] == ["taken"]

    def test_the_error_is_one_flat_list_and_each_entry_reads_on_its_own(self):
        rows = [a_row("a", cron="banana"), an_interval("b", every_seconds=0)]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        error = caught.value
        # A dict keyed by name is the shape that cannot tell two rows with
        # one name apart, or say anything about a row with none.
        assert not hasattr(error, "error_dict")
        assert len(error.error_list) == 2
        assert error.messages == [
            stored._ROW_FAILURE % entry.params for entry in error.error_list
        ]
        # Whatever the wording, each line carries the four things a person
        # needs to find the row and mend it.
        for line, entry, row in zip(
            error.messages, error.error_list, rows, strict=True
        ):
            assert set(entry.params) == {"index", "name", "field", "message"}
            assert str(entry.params["index"]) in line
            assert row["name"] in line
            assert entry.params["field"] in line
            assert entry.params["message"] in line

    @pytest.mark.parametrize(("row", "name", "field"), FAILING)
    def test_a_failing_row_is_reported_by_position_name_and_field(
        self, row, name, field
    ):
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("before"), row, a_row("after")])
        assert reported(caught) == [(1, name, f, m) for f, m in alone(row)]
        assert {f for _, _, f, _ in reported(caught)} == {field}
        assert not OxSchedule.objects.exists()

    def test_an_end_that_is_not_after_the_start_is_reported(self):
        now = timezone.now()
        row = a_row("ends-first", start_time=now, end_time=now - timedelta(hours=1))
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("before"), row])
        assert reported(caught) == [(1, "ends-first", f, m) for f, m in alone(row)]
        assert [f for _, _, f, _ in reported(caught)] == ["end_time"]

    def test_a_row_with_several_things_wrong_has_an_entry_for_each(self):
        row = a_row("x", task_key="nope", cron="banana", starting_deadline_seconds=0)
        with pytest.raises(ValidationError) as caught:
            create_schedules([row])
        assert reported(caught) == [(0, "x", f, m) for f, m in alone(row)]
        assert len(reported(caught)) == 3

    def test_each_entry_keeps_the_code_django_gave_the_failure(self):
        create_schedule(**a_row("taken"))
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("x" * 129), a_row("taken"), a_row("")])
        assert [entry.code for entry in caught.value.error_list] == [
            "max_length",
            "unique",
            "blank",
        ]

    def test_a_failure_of_no_one_field_is_reported_without_one(self, monkeypatch):
        def refuse_the_row(self):
            raise ValidationError("These two do not go together.")

        monkeypatch.setattr(OxSchedule, "clean", refuse_the_row)
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("whole")])
        (entry,) = caught.value.error_list
        assert reported(caught) == [(0, "whole", "", "These two do not go together.")]
        assert caught.value.messages == [
            stored._ROW_FAILURE_WITHOUT_FIELD % entry.params
        ]


class TestNamesWithinTheBatch:
    def test_a_repeated_name_is_reported_on_each_later_row(self):
        rows = [
            a_row("same"),
            a_row("other"),
            a_row("same", cron="0 3 * * *"),
            a_row("same", cron="0 4 * * *"),
        ]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        again = stored._REPEATED_NAME % {"first": 0}
        assert reported(caught) == [
            (2, "same", "name", again),
            (3, "same", "name", again),
        ]
        assert {entry.code for entry in caught.value.error_list} == {"repeated_name"}
        assert not OxSchedule.objects.exists()

    def test_a_repeat_is_counted_from_a_first_row_that_fails_for_another_reason(self):
        rows = [a_row("same", cron="banana"), a_row("same")]
        with pytest.raises(ValidationError) as caught:
            create_schedules(rows)
        assert [(index, field) for index, _, field, _ in reported(caught)] == [
            (0, "cron"),
            (1, "name"),
        ]

    def test_names_are_compared_as_they_will_be_stored(self):
        # The column is text, and the clean turns 5 into "5" on the way in.
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row(5), a_row("5")])
        assert [(index, name) for index, name, _, _ in reported(caught)] == [(1, "5")]

    def test_rows_with_no_name_are_not_each_other_s_duplicate(self):
        nameless = {"task_key": "report", "trigger": "cron", "cron": "0 2 * * *"}
        with pytest.raises(ValidationError) as caught:
            create_schedules([dict(nameless), dict(nameless)])
        assert reported(caught) == [
            (index, None, f, m) for index in (0, 1) for f, m in alone(nameless)
        ]

    def test_a_name_already_in_the_table_is_reported_not_left_to_the_insert(self):
        create_schedule(**a_row("taken"))
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("new"), a_row("taken", cron="0 3 * * *")])
        assert reported(caught) == [
            (1, "taken", f, m) for f, m in alone(a_row("taken"))
        ]
        assert [row.name for row in OxSchedule.objects.all()] == ["taken"]

    def test_two_rows_taking_one_existing_name_are_each_told_it_exists(self):
        create_schedule(**a_row("taken"))
        with pytest.raises(ValidationError) as caught:
            create_schedules([a_row("taken"), a_row("taken")])
        assert [entry.code for entry in caught.value.error_list] == [
            "unique",
            "unique",
        ]


class TestOneReadingOfTheClock:
    def test_every_row_carries_the_same_instant(self, monkeypatch):
        # A clock that moves a second each time it is read, so a reading per
        # row would give each row its own.
        begun = timezone.now()
        ticks = itertools.count()
        monkeypatch.setattr(
            timezone, "now", lambda: begun + timedelta(seconds=next(ticks))
        )
        given = begun - timedelta(days=2)
        create_schedules([a_row("a"), an_interval("b"), a_row("c", start_time=given)])
        rows = list(OxSchedule.objects.order_by("pk"))
        assert {(row.created_at, row.updated_at) for row in rows} == {(begun, begun)}
        assert [row.start_time for row in rows] == [begun, begun, given]


class TestTheTransactionHoldsOnlyTheWrites:
    """
    The clock is read, every row checked and every permission asked before
    any transaction is opened, and the one that is opened then only writes.

    On SQLite the order is what lets a batch wait for another writer, which
    `TestBesideAnotherWriter` shows with a real one. This holds the order
    itself, on every database.
    """

    def test_everything_that_only_reads_is_done_before_it_opens(self, monkeypatch):
        seen = []
        real_now = timezone.now

        class _Transactions:
            """`django.db.transaction` as `stored` names it, noting what it opens."""

            def atomic(self, using=None, **kwargs):
                seen.append("transaction")
                return transaction.atomic(using=using, **kwargs)

            def __getattr__(self, name):
                return getattr(transaction, name)

        class _Asked(_User):
            def has_perm(self, perm, obj=None):
                seen.append("permission")
                return super().has_perm(perm, obj)

        def now():
            seen.append("clock")
            return real_now()

        def note(execute, sql, params, many, context):
            statement = sql.lstrip().upper()
            if statement.startswith("SELECT") and (
                "oxschedule" in sql and "oxschedulechange" not in sql
            ):
                seen.append("check")
            elif statement.startswith(("INSERT", "UPDATE", "DELETE")):
                seen.append("write")
            return execute(sql, params, many, context)

        monkeypatch.setattr(stored, "transaction", _Transactions())
        monkeypatch.setattr(timezone, "now", now)
        with connection.execute_wrapper(note):
            create_schedules(
                [a_row("a"), a_row("b", task_key="guarded")], user=_Asked(GUARD)
            )
        opened = seen.index("transaction")
        assert seen[:opened] == ["clock", "check", "check", "permission"]
        inside = seen[opened + 1 :]
        assert "transaction" not in inside, "the batch opened a second transaction"
        assert "check" not in inside
        assert "permission" not in inside
        # The two rows and the change row.
        assert inside.count("write") == 3


class TestTheWorkersAreToldOnce:
    def test_a_batch_touches_the_change_row_exactly_once(self):
        with CaptureQueriesContext(connection) as captured:
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        touches = [sql for sql in writes(captured) if "oxschedulechange" in sql]
        assert len(touches) == 1, touches
        assert OxScheduleChange.objects.count() == 1

    def test_a_marker_that_is_already_there_moves_once(self):
        create_schedule(**a_row("already-here"))
        before = OxScheduleChange.objects.get(id=1).changed_at
        with CaptureQueriesContext(connection) as captured:
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        touches = [sql for sql in writes(captured) if "oxschedulechange" in sql]
        assert len(touches) == 1, touches
        assert OxScheduleChange.objects.get(id=1).changed_at > before


class _User:
    """A user who holds what they are given, and remembers being asked."""

    def __init__(self, *held):
        self.held = set(held)
        self.asked = []

    def has_perm(self, perm, obj=None):
        self.asked.append((perm, getattr(obj, "name", None)))
        return perm in self.held


class TestTheRegistryPermission:
    """
    Asked as `create_schedule` asks it: once the clean has passed, and
    answered with PermissionDenied. For a batch that is once every row has
    passed, so a batch with anything invalid in it is told so and no
    permission backend is asked anything.
    """

    def test_a_denied_batch_is_refused_as_the_single_call_is_and_writes_nothing(self):
        user = _User()
        rows = [
            a_row("open"),
            a_row("first-guarded", task_key="guarded"),
            a_row("second-guarded", task_key="guarded"),
        ]
        with (
            CaptureQueriesContext(connection) as captured,
            pytest.raises(PermissionDenied) as caught,
        ):
            create_schedules(rows, user=user)
        with pytest.raises(PermissionDenied) as single:
            create_schedule(user=user, **rows[1])
        # Every row that was denied, each with what the single call says
        # about it, and not the row that needed no permission.
        assert str(caught.value) == stored._ROWS_DENIED_SEPARATOR.join(
            stored._ROW_DENIED.format(
                index=index, name=rows[index]["name"], message=single.value
            )
            for index in (1, 2)
        )
        for index in (1, 2):
            assert str(index) in str(caught.value)
            assert rows[index]["name"] in str(caught.value)
        assert "open" not in str(caught.value)
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists()
        assert writes(captured) == [], "a row was written before its permission was"

    def test_every_guarded_row_is_asked_about_and_no_other(self):
        user = _User()
        with pytest.raises(PermissionDenied):
            create_schedules(
                [
                    a_row("open"),
                    a_row("first-guarded", task_key="guarded"),
                    a_row("second-guarded", task_key="guarded"),
                ],
                user=user,
            )
        assert {perm for perm, _ in user.asked} == {GUARD}
        # The row itself goes to the backend, so an object-level one gets
        # its say on each row, as it does through the single call.
        assert [name for _, name in user.asked if name is not None] == [
            "first-guarded",
            "second-guarded",
        ]

    def test_a_user_holding_it_creates_the_batch(self):
        created = create_schedules(
            [a_row("open"), a_row("guarded", task_key="guarded")], user=_User(GUARD)
        )
        assert [row.name for row in created] == ["open", "guarded"]
        assert OxSchedule.objects.count() == 2

    def test_no_user_means_no_check_as_it_does_for_the_single_call(self):
        create_schedule(**a_row("single", task_key="guarded"))
        created = create_schedules([a_row("batched", task_key="guarded")])
        assert [row.name for row in created] == ["batched"]

    def test_a_batch_that_does_not_validate_is_told_that_and_nothing_else(self):
        # The single call cleans before it asks, so a row that is both
        # invalid and not permitted is refused for being invalid, and a
        # permission backend only ever sees a row that validates. A batch
        # keeps to both: one invalid row anywhere, and no row of it is put
        # to the backend, the valid guarded one included.
        user = _User()
        with pytest.raises(ValidationError) as caught:
            create_schedules(
                [
                    a_row("guarded", task_key="guarded"),
                    a_row("broken", task_key="guarded", cron="banana"),
                ],
                user=user,
            )
        assert [(index, field) for index, _, field, _ in reported(caught)] == [
            (1, "cron")
        ]
        assert user.asked == []


class TestTheUniqueIndexIsTheLastWord:
    """
    Checking a name does not reserve it. Between the check and the write
    someone else can create a schedule of that name, and only the unique
    index can see that. Its refusal is raised as it is, and the rows of the
    batch written before it go with the transaction.
    """

    def test_a_name_taken_after_the_check_refuses_the_batch_whole(self):
        landed = []

        def a_schedule_of_the_last_name_lands_first(
            execute, sql, params, many, context
        ):
            # Ahead of the batch's first INSERT, so after every row of it
            # was checked. On this connection, because under the test's
            # transaction no other could see the batch's rows to collide
            # with; the row therefore goes back with the batch, which is
            # why the assertion is on the batch's own rows.
            if not landed and _writes_the_schedule_table(sql):
                landed.append(True)
                now = timezone.now()
                OxSchedule.objects.create(
                    **a_row("c", cron="0 5 * * *"),
                    start_time=now,
                    created_at=now,
                    updated_at=now,
                )
            return execute(sql, params, many, context)

        with (
            connection.execute_wrapper(a_schedule_of_the_last_name_lands_first),
            pytest.raises(IntegrityError),
        ):
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        assert landed, "the competing row was never written"
        assert not OxSchedule.objects.filter(name__in=["a", "b"]).exists(), (
            "rows written before the refused one outlived the batch"
        )
        assert not OxScheduleChange.objects.exists()

    @pytest.mark.django_db(transaction=True)
    def test_a_second_writer_keeps_the_name_and_the_batch_leaves_nothing(self):
        """
        The same race with a real second connection, which commits.

        The batch's transaction is opened to write and has read nothing, so
        on SQLite it waits for the other writer where a transaction that
        had read would be refused, and on PostgreSQL and MySQL nothing
        waits at all. On all three its INSERT of the name then meets the
        unique index, the batch leaves nothing, and the other writer's
        schedule stands.
        """
        wrote = threading.Event()
        raised = []

        def compete():
            def say_when_the_row_is_in(execute, sql, params, many, context):
                result = execute(sql, params, many, context)
                if _writes_the_schedule_table(sql):
                    wrote.set()
                return result

            try:
                with connection.execute_wrapper(say_when_the_row_is_in):
                    create_schedule(**a_row("c", cron="0 5 * * *"))
            except BaseException as exc:
                raised.append(exc)
            finally:
                wrote.set()
                for conn in connections.all():
                    conn.close()

        competitor = threading.Thread(target=compete, name="competing-writer")
        begun = []

        def let_the_other_writer_in_first(execute, sql, params, many, context):
            if not begun and _writes_the_schedule_table(sql):
                begun.append(True)
                competitor.start()
                assert wrote.wait(timeout=30), "the competing writer never wrote"
            return execute(sql, params, many, context)

        try:
            with (
                connection.execute_wrapper(let_the_other_writer_in_first),
                pytest.raises(IntegrityError),
            ):
                create_schedules([a_row("a"), a_row("b"), a_row("c")])
        finally:
            if begun:
                competitor.join(timeout=30)
        assert begun, "the batch never reached its first INSERT"
        assert not competitor.is_alive(), "the competing writer did not finish"
        assert not raised, raised
        assert [(row.name, row.cron) for row in OxSchedule.objects.all()] == [
            ("c", "0 5 * * *")
        ]


class TestTheRowTheDatabaseRefusedIsNamed:
    """
    A refusal only the database gives, at the write: its own error, with its
    class and its cause, and a note saying which row of the batch it was, by
    position and by the name the row supplied. The statement alone does not
    say, and in a batch of two hundred the operator cannot tell. Nothing of
    the batch is left behind.
    """

    @staticmethod
    def _the_name_lands_first(name):
        landed = []

        def wrapper(execute, sql, params, many, context):
            # As in TestTheUniqueIndexIsTheLastWord: ahead of the batch's
            # first INSERT, on this connection, gone with the batch.
            if not landed and _writes_the_schedule_table(sql):
                landed.append(True)
                now = timezone.now()
                OxSchedule.objects.create(
                    **a_row(name, cron="0 5 * * *"),
                    start_time=now,
                    created_at=now,
                    updated_at=now,
                )
            return execute(sql, params, many, context)

        return wrapper

    def test_a_name_taken_after_the_check(self):
        with (
            connection.execute_wrapper(self._the_name_lands_first("c")),
            pytest.raises(IntegrityError) as caught,
        ):
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        assert caught.value.__notes__ == [
            stored._ROW_REFUSED_AT_THE_WRITE.format(index=2, name="'c'")
        ]
        # The driver's own error underneath, as Django raised it.
        assert isinstance(caught.value.__cause__, connection.Database.IntegrityError)
        assert not OxSchedule.objects.filter(name__in=["a", "b"]).exists()
        assert not OxScheduleChange.objects.exists()

    def test_a_nul_postgresql_will_not_keep(self):
        if connection.vendor != "postgresql":
            pytest.skip(
                f"PostgreSQL refuses a NUL in JSON text; {connection.vendor} keeps it"
            )
        rows = [a_row("a"), a_row("b"), a_row("c", arguments={"note": "a\x00b"})]
        with pytest.raises(DataError) as caught:
            create_schedules(rows)
        assert caught.value.__notes__ == [
            stored._ROW_REFUSED_AT_THE_WRITE.format(index=2, name="'c'")
        ]
        assert caught.value.__cause__ is not None
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists()

    def test_a_nul_in_a_name_postgresql_refuses_before_the_write(self):
        """
        A NUL in a name never reaches the write on PostgreSQL: validation's
        query for names already taken carries the name, and that is where
        it is refused. The database's own error is raised with its class, its
        cause and a note naming the row, and nothing is written.
        """
        if connection.vendor != "postgresql":
            pytest.skip(
                f"PostgreSQL refuses a NUL in text; {connection.vendor} keeps it"
            )
        create_schedule(**a_row("kept"))
        stored_before = list(OxSchedule.objects.values_list("name", "updated_at"))
        marker = OxScheduleChange.objects.get().changed_at
        rows = [a_row("a"), a_row("b"), a_row("c\x00d"), a_row("e")]
        with pytest.raises(DataError) as caught:
            create_schedules(rows)
        assert caught.value.__notes__ == [
            stored._ROW_REFUSED_AT_THE_CHECK.format(index=2, name="'c\\x00d'")
        ]
        assert type(caught.value) is DataError
        assert isinstance(caught.value.__cause__, connection.Database.DataError)
        assert list(OxSchedule.objects.values_list("name", "updated_at")) == (
            stored_before
        )
        assert OxScheduleChange.objects.get().changed_at == marker

    def test_whatever_the_database_raises_in_the_check_names_the_row(self):
        # Not only a value it refuses: an error in the query for names
        # already taken is raised as it was, with the row named, on every
        # database.
        def fail_on_the_third_name(execute, sql, params, many, context):
            if sql.lstrip().upper().startswith("SELECT") and "c" in (params or ()):
                raise OperationalError("the database went away")
            return execute(sql, params, many, context)

        with (
            connection.execute_wrapper(fail_on_the_third_name),
            pytest.raises(OperationalError) as caught,
        ):
            create_schedules([a_row("a"), a_row("b"), a_row("c")])
        assert str(caught.value) == "the database went away"
        assert caught.value.__notes__ == [
            stored._ROW_REFUSED_AT_THE_CHECK.format(index=2, name="'c'")
        ]
        assert not OxSchedule.objects.exists()
        assert not OxScheduleChange.objects.exists()

    def test_a_nul_in_a_name_is_kept_where_the_database_keeps_it(self):
        # SQLite and MySQL keep a NUL in text, and the batch is created
        # whole.
        if connection.vendor == "postgresql":
            pytest.skip("PostgreSQL refuses a NUL in text")
        created = create_schedules([a_row("a"), a_row("b"), a_row("c\x00d")])
        assert [row.name for row in created] == ["a", "b", "c\x00d"]
        assert sorted(OxSchedule.objects.values_list("name", flat=True)) == [
            "a",
            "b",
            "c\x00d",
        ]

    def test_a_name_is_escaped_and_cut(self):
        # The name the row supplied, whatever it holds, cannot reach a
        # terminal as itself when the traceback is printed.
        hostile = "\x1b]0;owned\x07\x1b[2J\r\nforged " + "x" * 90
        with (
            connection.execute_wrapper(self._the_name_lands_first(hostile)),
            pytest.raises(IntegrityError) as caught,
        ):
            create_schedules([a_row("a"), a_row(hostile)])
        (note,) = caught.value.__notes__
        assert note.startswith("The database refused rows[1] (name '")
        assert note.isprintable() and note.isascii()
        assert "\x1b" not in note and "\n" not in note and "\r" not in note
        assert len(note) < 300
        assert not OxSchedule.objects.exists()

    def test_a_name_refused_in_the_check_is_escaped_and_cut(self):
        # The same of a name PostgreSQL refuses before the write: as long as
        # a name may be, and every character of it written as an escape.
        if connection.vendor != "postgresql":
            pytest.skip(
                f"PostgreSQL refuses a NUL in text; {connection.vendor} keeps it"
            )
        hostile = "\x00\x1b]0;owned\x07\x1b[2J\r\n" * 7 + "\x00" * 9
        assert len(hostile) == OxSchedule._meta.get_field("name").max_length
        with pytest.raises(DataError) as caught:
            create_schedules([a_row("a"), a_row(hostile)])
        (note,) = caught.value.__notes__
        assert note.startswith(
            "The database raised this error while checking rows[1] (name "
            "'\\x00\\x1b]0;owned\\x07"
        )
        assert note.isprintable() and note.isascii()
        assert "\x1b" not in note and "\n" not in note and "\r" not in note
        # Bounded by the note's own text and the cut name, not by the name.
        fixed = len(stored._ROW_REFUSED_AT_THE_CHECK.format(index=1, name=""))
        assert len(note) <= fixed + stored._PRINTABLE_LIMIT + 2
        assert not OxSchedule.objects.exists()


#: How long the other connection goes on holding its write transaction
#: open once the batch has reached its first write.
HELD_FOR = 0.25


@pytest.mark.django_db(transaction=True)
class TestBesideAnotherWriter:
    """
    The importer's output is pasted beside running workers, and a worker
    writes to the same database all day.
    """

    def test_the_batch_waits_for_the_writer_rather_than_being_refused(self):
        """
        SQLite has one write lock for the whole file. A transaction that
        has read is not made to wait for it: it is refused at once,
        whatever the busy timeout. A batch that checked its rows inside its
        own transaction would therefore be refused with "database is locked"
        whenever anything else was writing. One that opens its transaction
        only to write waits its turn.

        By construction rather than by timing: the other connection holds
        its write transaction open from before the call until a fixed time
        after the batch has reached its first write, so the batch meets the
        lock on every run.

        On PostgreSQL and MySQL the other connection holds a lock on a row
        the batch never touches, so nothing waits and the batch succeeds
        at once. The test passes there for that reason, and the last
        assertion, that the batch did wait, is SQLite's alone.
        """
        at_its_first_write = threading.Event()
        letting_go = threading.Event()
        outcome = {}

        def paste():
            def say_before_the_first_write(execute, sql, params, many, context):
                if _writes_the_schedule_table(sql):
                    at_its_first_write.set()
                return execute(sql, params, many, context)

            try:
                with connection.execute_wrapper(say_before_the_first_write):
                    created = create_schedules([a_row("a"), a_row("b")])
                outcome["created"] = [row.name for row in created]
            except BaseException as exc:
                outcome["raised"] = exc
            finally:
                outcome["after the other writer let go"] = letting_go.is_set()
                at_its_first_write.set()
                for conn in connections.all():
                    conn.close()

        batch = threading.Thread(target=paste, name="batch")
        with transaction.atomic():
            # What a worker does: a small write, in a transaction that is
            # still open when the batch arrives.
            OxTask.objects.create(
                id=uuid.uuid4(),
                task_path="tests.tasks.add",
                backend_name="default",
                enqueued_at=timezone.now(),
            )
            batch.start()
            assert at_its_first_write.wait(timeout=30), "the batch never wrote"
            time.sleep(HELD_FOR)
            letting_go.set()
        batch.join(timeout=30)
        assert not batch.is_alive(), "the batch did not finish"
        assert "raised" not in outcome, repr(outcome["raised"])
        assert outcome["created"] == ["a", "b"]
        assert sorted(OxSchedule.objects.values_list("name", flat=True)) == ["a", "b"]
        if connection.vendor == "sqlite":
            # The control. Had the batch finished while the other
            # connection still held its transaction, the lock was never in
            # the batch's way and the test showed nothing.
            assert outcome["after the other writer let go"], (
                "the batch wrote while another connection held the write lock"
            )


def _outcome(call):
    """What a write call did, in a form two calls can be compared by."""
    try:
        created = call()
    except ValidationError as exc:
        if hasattr(exc, "error_dict"):
            return "refused", [
                ("" if field == NON_FIELD_ERRORS else field, message)
                for field, messages in exc.message_dict.items()
                for message in messages
            ]
        return "refused", [
            (entry.params["field"], entry.params["message"]) for entry in exc.error_list
        ]
    except Exception as exc:
        return "raised", type(exc).__name__
    if isinstance(created, list):
        (created,) = created
    return "created", columns(OxSchedule.objects.get(pk=created.pk))


#: Rows of every kind, good and bad, for the two calls to be compared on.
EITHER_WAY = [
    pytest.param({}, id="a cron"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60}, id="an interval"
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": "60", "phase_seconds": 5},
        id="an interval from strings",
    ),
    pytest.param({"enabled": False, "starting_deadline_seconds": 30}, id="paused"),
    pytest.param(
        {"task_key": "checked", "arguments": {"region": "emea"}}, id="checked"
    ),
    pytest.param({"name": ""}, id="blank name"),
    pytest.param({"name": None}, id="null name"),
    pytest.param({"name": "x" * 129}, id="long name"),
    pytest.param({"task_key": "os.system"}, id="unregistered"),
    pytest.param({"cron": "banana"}, id="bad cron"),
    pytest.param({"cron": ""}, id="empty cron"),
    pytest.param({"cron": "0 2 * * *", "every_seconds": 5}, id="cron with interval"),
    pytest.param({"trigger": "interval", "every_seconds": 60}, id="interval with cron"),
    pytest.param({"trigger": "interval", "cron": ""}, id="interval without one"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 0}, id="interval of zero"
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": -5},
        id="negative interval",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60, "phase_seconds": 60},
        id="phase not under the interval",
    ),
    pytest.param({"trigger": "solar"}, id="unknown trigger"),
    pytest.param({"starting_deadline_seconds": 0}, id="deadline of zero"),
    pytest.param({"arguments": [1, 2]}, id="arguments a list"),
    pytest.param({"arguments": {"when": {1, 2}}}, id="arguments not JSON"),
    pytest.param(
        {"task_key": "checked", "arguments": {"wrong": 1}}, id="arguments refused"
    ),
    pytest.param({"enabled": "maybe"}, id="enabled not a boolean"),
    pytest.param({"start_time": None}, id="no start"),
    pytest.param(
        {"name": "", "task_key": "nope", "trigger": "solar", "arguments": [1]},
        id="several at once",
    ),
    # Values a field's own cleaning refuses. The rules used to go on and
    # compare them and raise TypeError.
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": "soon"},
        id="interval not a number",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 60, "phase_seconds": None},
        id="no phase",
    ),
    pytest.param({"starting_deadline_seconds": "soon"}, id="deadline not a number"),
    pytest.param({"end_time": "soon"}, id="end not a time"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": ""},
        id="interval an empty string",
    ),
    pytest.param({"starting_deadline_seconds": ""}, id="deadline an empty string"),
    pytest.param({"end_time": []}, id="end an empty list"),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 10**15},
        id="interval no timedelta holds",
    ),
    pytest.param(
        {"trigger": "interval", "cron": "", "every_seconds": 62_135_596_801},
        id="interval one second past the longest",
    ),
    pytest.param(
        {"starting_deadline_seconds": 86_400_000_000_000},
        id="deadline one second past a timedelta",
    ),
    # Not a refusal of the row but of the call: what the single call raises,
    # the batch raises.
    pytest.param({"colour": "red"}, id="not a field"),
]


class TestTheBatchAndTheSingleCallAgree:
    """
    One row through each: the same schedule, or the same refusal.

    The two share their validation rather than each carrying a copy, and
    this is what holds them to it. A rule added to one path alone shows up
    here as a row the importer's output creates and `create_schedule` would
    have refused, or the reverse.
    """

    @pytest.fixture(autouse=True)
    def _one_instant(self, monkeypatch):
        fixed = timezone.now()
        monkeypatch.setattr(timezone, "now", lambda: fixed)
        return fixed

    @pytest.mark.parametrize("over", EITHER_WAY)
    def test_one_row_fares_the_same_through_both(self, over):
        row = a_row(**over)
        single = _outcome(lambda: create_schedule(**row))
        OxSchedule.objects.all().delete()
        assert _outcome(lambda: create_schedules([row])) == single

    def test_an_end_at_or_before_the_start_fares_the_same(self, _one_instant):
        for end in (_one_instant, _one_instant - timedelta(hours=1)):
            row = a_row(end_time=end)
            single = _outcome(lambda row=row: create_schedule(**row))
            assert single[0] == "refused"
            assert _outcome(lambda row=row: create_schedules([row])) == single

    def test_a_name_already_taken_fares_the_same(self):
        create_schedule(**a_row("taken"))
        single = _outcome(lambda: create_schedule(**a_row("taken")))
        assert single[0] == "refused"
        assert _outcome(lambda: create_schedules([a_row("taken")])) == single

    def test_a_user_without_the_permission_fares_the_same(self):
        # Denied when the row is otherwise good, and told about the row
        # first when it is not.
        user = _User()
        denied = a_row(task_key="guarded")
        single = _outcome(lambda: create_schedule(user=user, **denied))
        assert single == ("raised", "PermissionDenied")
        assert _outcome(lambda: create_schedules([denied], user=user)) == single
        invalid = a_row(task_key="guarded", cron="banana")
        single = _outcome(lambda: create_schedule(user=user, **invalid))
        assert single[0] == "refused"
        assert _outcome(lambda: create_schedules([invalid], user=user)) == single
        assert not OxSchedule.objects.exists()
