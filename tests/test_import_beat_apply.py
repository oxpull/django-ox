"""
ox_import_beat_schedules where its output meets the database it is applied
to: the arguments beat decoded, what the destination refuses, and the
program a person pastes.

The beat tables are made on the "alt" alias, which is SQLite on every leg,
so a row can hold what only SQLite keeps (bytes in a text column, NUL in a
name) whatever the destination is. The schedules go to the default
database, the leg's own, unless a test routes them.
"""

import itertools
import json
import uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal
from io import StringIO

import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from kombu.utils import json as kombu_json

from django_ox.management.commands import ox_import_beat_schedules as command
from django_ox.models import OxSchedule

from . import tasks
from .test_import_beat import (
    LIST_CUT,
    NOT_TRANSLATED,
    POSITIONAL,
    drop_beat_tables,
    insert_task,
    listed_under,
    make_beat_tables,
    printed_rows,
    run_as_module,
    section_2,
)

pytestmark = [pytest.mark.django_db(transaction=True, databases=["default", "alt"])]

BEAT = "alt"

#: What the command says, as the templates it fills.
INVALID_JSON = (
    "its {column} contains invalid JSON. beat disables a row whose arguments it cannot "
    "decode, so it did not run this row."
)
ARGUMENT_TYPE_UNSUPPORTED = (
    "beat passes a non-JSON value of type {kind} in its {column}, decoded from a "
    "Celery type marker. A stored schedule's JSON arguments cannot carry that value."
)
ARGUMENT_TYPE_UNKNOWN = (
    "its {column} holds a Celery type marker {kind} that kombu here cannot decode. "
    "beat disables a row whose arguments it cannot decode, so it did not run this row."
)
ARGUMENT_MARKER_INVALID = (
    "its {column} holds a Celery {kind} type marker whose value kombu cannot decode. "
    "beat disables a row whose arguments it cannot decode, so it did not run this row."
)
ARGUMENT_NOT_LOADABLE = (
    "its {column} is stored as {kind}, which beat cannot decode: loading the schedule "
    "fails on this row."
)
ARGUMENT_MARKER_BREAKS_LOAD = (
    "its {column} holds a Celery {kind} type marker whose value kombu cannot use: "
    "loading the schedule fails on this row."
)
ARGUMENT_MARKER_UNCHECKED = (
    "its {column} holds a Celery type marker {kind}, and kombu is not installed here "
    "to tell what beat made of it."
)


@pytest.fixture(autouse=True)
def _use_tz(settings):
    settings.USE_TZ = True


@pytest.fixture
def beat():
    """beat's tables on the alt alias, with the fixture's three rows."""
    try:
        make_beat_tables(db=BEAT)
        yield
    finally:
        drop_beat_tables(BEAT)


@pytest.fixture
def schedulable(monkeypatch):
    """Every task path the tests print, registered as a schedulable key."""
    from django_ox.registry import ScheduleKind, register

    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)

    def expose(*keys):
        for key in keys:
            register(ScheduleKind(key=key, task=tasks.add))

    expose("reports.tasks.daily", "mail.tasks.poll", "x.y.z")
    return expose


def run(**options):
    out = StringIO()
    call_command("ox_import_beat_schedules", database=BEAT, stdout=out, **options)
    return out.getvalue()


def refusals(output):
    return listed_under(NOT_TRANSLATED, output)


def store(name, **columns):
    """One more beat row on the fixture's crontab, with the columns given."""
    with connections[BEAT].cursor() as cursor:
        cursor.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM django_celery_beat_periodictask"
        )
        pk = cursor.fetchone()[0]
    insert_task(pk, name, "x.y.z", db=BEAT, crontab_id=1, **columns)


def beat_keeps_bytes():
    """Only SQLite keeps bytes as they are in a text column."""
    if connections[BEAT].vendor != "sqlite":
        pytest.skip("only SQLite keeps bytes in a text column")


def same_json(a, b):
    """Equal as JSON values and of the same types all the way down."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same_json(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(map(same_json, a, b))
    return a == b


# -------------------------------------------------------- Argument decoding


def kombu_encoded(value):
    """value as kombu writes it into a message: Celery type markers and all."""
    return kombu_json.dumps({"v": value})


MARKED = {
    "datetime": datetime(2026, 1, 2, 3, 4, 5),
    "aware datetime": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    "date": date(2026, 1, 2),
    "time": time(3, 4, 5),
    "decimal": Decimal("1.50"),
    "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
    "bytes": b"payload",
}


def test_every_type_marker_the_installed_kombu_registers_is_covered():
    # The cases below are meant to cover each marker kombu decodes. A kombu
    # that registers another one makes this fail rather than leave it out.
    written = {json.loads(kombu_encoded(v))["v"]["__type__"] for v in MARKED.values()}
    assert set(kombu_json._decoders) - {"base64"} <= written


@pytest.mark.parametrize("label", sorted(MARKED))
@pytest.mark.parametrize("column", ["args", "kwargs"])
def test_an_argument_beat_decodes_to_a_non_json_type_is_listed(beat, label, column):
    """
    beat loads args and kwargs with kombu, which turns a Celery type marker
    into the type it names. The command once printed the marker as the
    plain dictionary its JSON spells, and the task got that dictionary
    where beat had passed a datetime.
    """
    value = MARKED[label]
    stored = kombu_encoded(value) if column == "kwargs" else f"[{kombu_encoded(value)}]"
    store("marked", **{column: stored})
    beat_value = kombu_json.loads(stored)
    kind = type(value).__name__
    assert refusals(run())["marked"] == ARGUMENT_TYPE_UNSUPPORTED.format(
        column=column, kind=kind
    )
    # The oracle agrees that it is not JSON.
    assert command._not_json(beat_value) == kind


def test_a_marker_nested_deep_inside_the_arguments_is_found(beat):
    stored = json.dumps({"a": [1, {"b": [json.loads(kombu_encoded(Decimal("2")))]}]})
    store("deep-marker", kwargs=stored)
    assert refusals(run())["deep-marker"] == ARGUMENT_TYPE_UNSUPPORTED.format(
        column="kwargs", kind="Decimal"
    )


def test_a_marker_kombu_does_not_know_is_a_row_beat_disabled(beat):
    store("unknown", kwargs='{"x": {"__type__": "nonesuch", "__value__": 1}}')
    assert refusals(run())["unknown"] == ARGUMENT_TYPE_UNKNOWN.format(
        column="kwargs", kind="'nonesuch'"
    )


@pytest.mark.parametrize(
    ("stored", "reason"),
    [
        (
            '{"__type__": "datetime", "__value__": "not a date"}',
            ARGUMENT_MARKER_INVALID,
        ),
        ('{"__type__": "decimal", "__value__": "x"}', ARGUMENT_MARKER_BREAKS_LOAD),
        (
            '{"__type__": "uuid", "__value__": "not a mapping"}',
            ARGUMENT_MARKER_BREAKS_LOAD,
        ),
    ],
    ids=["value-error", "decimal-invalid-operation", "type-error"],
)
def test_a_marker_whose_value_kombu_cannot_decode_is_listed(beat, stored, reason):
    """
    A ValueError from the marker's decoder is caught by beat's ModelEntry,
    which disables the row. Anything else is not caught there, and beat's
    load of the schedule fails on the row.
    """
    store("bad-marker", kwargs=json.dumps({"x": json.loads(stored)}))
    kind = ascii(json.loads(stored)["__type__"])
    assert refusals(run())["bad-marker"] == reason.format(column="kwargs", kind=kind)


@pytest.mark.parametrize(
    "stored",
    [
        '{"x": {"__type__": "datetime", "__value__": "2026-01-01", "extra": 1}}',
        '{"x": {"__type__": "datetime"}}',
        '{"x": {"__value__": "2026-01-01T00:00:00"}}',
        '{"__type__": "datetime", "__value__": "2026-01-01T00:00:00", "also": 2}',
    ],
    ids=["three-keys", "type-only", "value-only", "top-level-three-keys"],
)
def test_a_dictionary_that_only_looks_like_a_marker_is_translated_as_beat_reads_it(
    beat, schedulable, stored
):
    store("lookalike", kwargs=stored)
    output = run()
    assert "lookalike" not in refusals(output)
    printed = printed_rows(output)["lookalike"]["arguments"]
    assert same_json(printed, kombu_json.loads(stored))
    # And applied, it is what the schedule passes to the task.
    run_as_module(section_2(output))
    assert same_json(OxSchedule.objects.get(name="lookalike").arguments, printed)


@pytest.mark.parametrize(
    ("column", "stored", "outcome"),
    [
        ("kwargs", b'{"a": 1, "b": ["c"]}', {"a": 1, "b": ["c"]}),
        ("args", b"[]", {}),
        ("args", b"", {}),
        ("kwargs", b"", {}),
        ("args", b"[1]", POSITIONAL),
        ("args", b"{bad", INVALID_JSON.format(column="args")),
        ("kwargs", b"\xff\xfe", INVALID_JSON.format(column="kwargs")),
        ("kwargs", b'{"a": "\xc3\xa9"}', {"a": "é"}),
    ],
    ids=[
        "kwargs-object",
        "args-empty-list",
        "args-empty-blob",
        "kwargs-empty-blob",
        "args-nonempty",
        "args-malformed",
        "kwargs-not-utf8",
        "kwargs-utf8-text",
    ],
)
def test_arguments_stored_as_bytes_are_decoded_as_beat_decodes_them(
    beat, schedulable, column, stored, outcome
):
    """
    SQLite keeps bytes in a text column. kombu decodes them as UTF-8 JSON,
    so beat ran `[]` stored as bytes with no positional arguments and passed
    `{"a": 1}` stored as bytes as keyword arguments. The command once listed
    the first as positional and printed the second with no arguments at all.
    """
    beat_keeps_bytes()
    store("blob", **{column: stored})
    output = run()
    if isinstance(outcome, str):
        assert refusals(output)["blob"] == outcome
        return
    assert "blob" not in refusals(output)
    printed = printed_rows(output)["blob"]
    assert printed.get("arguments", {}) == outcome
    decoded = kombu_json.loads(stored or {"args": "[]", "kwargs": "{}"}[column])
    if column == "kwargs":
        assert same_json(printed.get("arguments", {}), decoded)
    else:
        assert not decoded


@pytest.mark.parametrize(
    ("raw", "column", "expected"),
    [
        (memoryview(b"[]"), "args", []),
        (bytearray(b'{"a": 2}'), "kwargs", {"a": 2}),
        (memoryview(b'{"a": [1]}'), "kwargs", {"a": [1]}),
        (0, "args", []),
        (None, "kwargs", {}),
    ],
    ids=["memoryview-args", "bytearray-kwargs", "memoryview-kwargs", "zero", "none"],
)
def test_the_binary_wrappers_a_driver_may_return_are_decoded(raw, column, expected):
    # psycopg2 returns bytea as a memoryview; kombu decodes each of these.
    assert command.Command._decode(raw, column) == expected
    assert (
        kombu_json.loads(raw if raw else "[]" if column == "args" else "{}") == expected
    )


def test_a_value_that_is_neither_text_nor_bytes_is_a_row_beat_cannot_load():
    with pytest.raises(command._Refused) as refused:
        command.Command._decode(17, "kwargs")
    assert refused.value.reason == ARGUMENT_NOT_LOADABLE.format(
        column="kwargs", kind="int"
    )
    with pytest.raises(TypeError):
        kombu_json.loads(17)


def test_without_kombu_a_marker_is_listed_and_plain_json_still_decodes(
    beat, schedulable, monkeypatch
):
    beat_keeps_bytes()
    monkeypatch.setattr(command, "_kombu_json", lambda: None)
    marked = '{"when": {"__type__": "datetime", "__value__": "2026-01-01"}}'
    store("marked", kwargs=marked)
    store("plain", kwargs=b'{"a": 1}')
    output = run()
    assert refusals(output)["marked"] == ARGUMENT_MARKER_UNCHECKED.format(
        column="kwargs", kind="'datetime'"
    )
    assert printed_rows(output)["plain"]["arguments"] == {"a": 1}


def test_a_kombu_from_before_type_markers_reads_one_as_a_plain_object(
    beat, schedulable, monkeypatch
):
    monkeypatch.setattr(command, "_kombu_json", lambda: command._OLD_KOMBU)
    monkeypatch.setattr(command, "_old_kombu_loads", json.loads)
    stored = '{"when": {"__type__": "datetime", "__value__": "2026-01-01"}}'
    store("marked", kwargs=stored)
    assert printed_rows(run())["marked"]["arguments"] == json.loads(stored)


ORDINARY = [
    '{"region": "emea", "n": [1, 2.5, null, true, false], "nested": {"k": "v"}}',
    '{"unicode": "\\u00e9\\u65e5\\ud83d\\ude00", "empty": {}, "list": []}',
    '{"big": 9223372036854775807, "low": -9007199254740993, "u": 18446744073709551615}',
    '{"float": 0.1, "exp": 1e-07, "zero": -0.0}',
]


@pytest.mark.parametrize(
    "stored", ORDINARY, ids=["mixed", "unicode", "big-ints", "floats"]
)
def test_what_is_printed_and_applied_is_what_beat_passed(beat, schedulable, stored):
    """
    The oracle is kombu's own loads, which is what beat's ModelEntry calls.
    Printed, and then applied and read back from the destination, the
    arguments are those values, of those types.
    """
    store("ordinary", kwargs=stored)
    output = run()
    listed = refusals(output)
    if "ordinary" in listed:
        # Only a destination that cannot hold a value lists the row; what it
        # says is the destination's own reason, tested where that is built.
        assert connections["default"].vendor != "sqlite", listed["ordinary"]
        return
    printed = printed_rows(output)["ordinary"]["arguments"]
    beat_passed = kombu_json.loads(stored)
    assert same_json(printed, beat_passed)
    run_as_module(section_2(output))
    assert same_json(OxSchedule.objects.get(name="ordinary").arguments, beat_passed)


def test_a_value_json_cannot_hold_is_found_anywhere_in_what_beat_decoded():
    # Keys are text in anything JSON spells; a type a project registered
    # with kombu can decode to other keys, and to other containers.
    assert command._not_json({1: "x"}) == "int"
    assert command._not_json({"a": [1, (2,)]}) == "tuple"
    assert command._not_json({"a": [1, {"b": None, "c": 2.5, "d": True}]}) is None


def test_without_kombu_bytes_are_read_as_kombu_reads_them(monkeypatch):
    """
    kombu decodes bytes as UTF-8 before JSON sees them. json.loads on its
    own would also take UTF-16 with a byte order mark, which beat could not
    decode and disabled the row for.
    """
    monkeypatch.setattr(command, "_kombu_json", lambda: None)
    utf16 = "{}".encode("utf-16")
    with pytest.raises(UnicodeDecodeError):
        kombu_json.loads(utf16)
    with pytest.raises(command._Refused) as refused:
        command.Command._decode(utf16, "kwargs")
    assert refused.value.reason == INVALID_JSON.format(column="kwargs")
    assert command.Command._decode(memoryview(b"[]"), "args") == []
    assert command.Command._decode(bytearray(b'{"a": 1}'), "kwargs") == {"a": 1}


# -------------------------------------------------------- Destination checks

DESTINATION_REFUSES = (
    "database {alias} ({database}) cannot store its {column} as it is: {why}"
)
WHY_NUL = "it holds a NUL character, which PostgreSQL text cannot hold."
WHY_SURROGATE = "it holds a lone UTF-16 surrogate, which {database} JSON refuses."
WHY_TOO_DEEP = "it is nested more than {limit} levels deep, which MySQL JSON refuses."
WHY_BIG_INTEGER = (
    "it holds the integer {number}, outside the 64-bit range MySQL JSON keeps exactly; "
    "MySQL would store an approximate float."
)
WHY_EXPONENT = (
    "it holds the number {number}, which PostgreSQL JSON stores without its exponent "
    "and hands back as an integer."
)
NAME_TAKEN = (
    "database {alias} already has a schedule named {existing}, which equals this name "
    "under that column's comparison rules. Rename one of them, then import again."
)
NAME_CLASH = (
    "its name equals the name of {others} under the {collation} collation of database "
    "{alias}, so create_schedules would refuse the batch. Rename all but one of them, "
    "then import again."
)
NAMES_UNCHECKED = (
    "# Database {alias} has no schedule table yet, so names already in use there were "
    "not checked."
)
DESTINATION_UNREADABLE = (
    "Cannot read the schedule table of database {alias} to check the names in use "
    "there. Check database access."
)
DATABASE_NAMES = {"postgresql": "PostgreSQL", "mysql": "MySQL", "sqlite": "SQLite"}


class _SchedulesOnAlt:
    """Every django-ox model on the alt alias."""

    def db_for_read(self, model, **hints):
        return "alt" if model._meta.app_label == "django_ox" else None

    db_for_write = db_for_read

    def allow_migrate(self, db, app_label, **hints):
        return None


@pytest.fixture(params=["default", "alt"], ids=["unrouted", "routed"])
def destination(request, settings):
    """
    Where the schedules go: the leg's default database, or routed to alt.
    On the suite's legs alt is SQLite; a run with alt on another server
    routes them to that vendor.
    """
    if request.param == "alt":
        settings.DATABASE_ROUTERS = [_SchedulesOnAlt()]
    from django_ox.stored import schedule_db_alias

    assert schedule_db_alias() == request.param
    return request.param


def vendor_of(alias):
    return connections[alias].vendor


def refuses(alias, column, why):
    return DESTINATION_REFUSES.format(
        alias=ascii(alias),
        database=DATABASE_NAMES[vendor_of(alias)],
        column=column,
        why=why,
    )


def nested(levels, inner):
    text = inner
    for _ in range(levels):
        text = f"[{text}]"
    return text


#: kwargs as stored, and what each database the schedules may go to does
#: with them: None prints and applies, else the reason the row is listed.
STORED_ARGUMENTS = {
    "nul-value": ('{"sep": "a\\u0000b"}', {"postgresql": WHY_NUL}),
    "nul-key": ('{"a\\u0000": 1}', {"postgresql": WHY_NUL}),
    "surrogate": (
        '{"s": "\\ud800"}',
        {"postgresql": "surrogate", "mysql": "surrogate"},
    ),
    "surrogate-key": (
        '{"\\udfff": 1}',
        {"postgresql": "surrogate", "mysql": "surrogate"},
    ),
    "paired-surrogates": ('{"s": "\\ud83d\\ude00"}', {}),
    "depth-100": ('{"a": ' + nested(99, "0") + "}", {}),
    "depth-101": ('{"a": ' + nested(100, "0") + "}", {"mysql": "deep"}),
    "depth-101-empty-list": ('{"a": ' + nested(99, "[]") + "}", {"mysql": "deep"}),
    "uint64-max": ('{"n": 18446744073709551615}', {}),
    "uint64-max-plus-one": ('{"n": 18446744073709551616}', {"mysql": "big"}),
    "int64-min": ('{"n": -9223372036854775808}', {}),
    "int64-min-minus-one": ('{"n": -9223372036854775809}', {"mysql": "big"}),
    "exponent": ('{"x": 1e16}', {"postgresql": "exponent"}),
    "big-float": ('{"x": 1.7976931348623157e+308}', {"postgresql": "exponent"}),
    "small-float": ('{"x": 1e-07, "y": 0.1, "z": 123.0}', {}),
}


def expected_reason(alias, outcome):
    database = DATABASE_NAMES[vendor_of(alias)]
    why = {
        "surrogate": WHY_SURROGATE.format(database=database),
        "deep": WHY_TOO_DEEP.format(limit=100),
    }.get(outcome, outcome)
    return refuses(alias, "arguments", why)


@pytest.mark.parametrize("label", sorted(STORED_ARGUMENTS))
def test_arguments_the_destination_cannot_keep_are_listed_and_the_rest_apply(
    beat, schedulable, destination, label
):
    """
    PostgreSQL refuses NUL and lone surrogates in JSON, and MySQL refuses
    surrogates and nesting past 100. Two more are not refused: MySQL keeps
    an integer past 64 bits as a float and PostgreSQL hands 1e16 back as an
    integer, so the task would get another value. Each is listed by the
    destination's own rule; a row the
    destination keeps exactly is printed and applies.
    """
    stored, outcomes = STORED_ARGUMENTS[label]
    store("subject", kwargs=stored)
    output = run()
    outcome = outcomes.get(vendor_of(destination))
    if outcome is not None:
        reason = refusals(output)["subject"]
        if outcome in ("big", "exponent"):
            number = kombu_json.loads(stored)
            (value,) = number.values()
            why = (
                WHY_BIG_INTEGER.format(number=ascii(value))
                if outcome == "big"
                else WHY_EXPONENT.format(number=repr(value))
            )
            assert reason == refuses(destination, "arguments", why)
        else:
            assert reason == expected_reason(destination, outcome)
        assert "subject" not in printed_rows(output)
    else:
        assert "subject" not in refusals(output)
    # Whatever is printed applies, and holds what beat passed.
    run_as_module(section_2(output))
    names = set(OxSchedule.objects.using(destination).values_list("name", flat=True))
    assert names == set(printed_rows(output))
    if outcome is None:
        kept = OxSchedule.objects.using(destination).get(name="subject").arguments
        assert same_json(kept, kombu_json.loads(stored))


@pytest.mark.parametrize("column", ["name", "task"])
def test_a_nul_in_a_name_or_task_is_listed_where_the_destination_refuses_it(
    beat, schedulable, destination, column
):
    value = "with\x00nul"
    if vendor_of(BEAT) == "postgresql":
        pytest.skip("beat's table here is PostgreSQL, which cannot hold a NUL")
    if column == "name":
        store(value)
    else:
        with connections[BEAT].cursor() as cursor:
            cursor.execute(
                "UPDATE django_celery_beat_periodictask SET task = %s WHERE id = 1",
                [value],
            )
        schedulable(value)
    output = run()
    name = value if column == "name" else "nightly"
    listed = refusals(output)
    if vendor_of(destination) == "postgresql":
        field = "name" if column == "name" else "task_key"
        assert listed[name] == refuses(destination, field, WHY_NUL)
    else:
        assert name not in listed
        run_as_module(section_2(output))
        assert OxSchedule.objects.using(destination).filter(name=name).exists()


#: Names a case-, accent- or pad-insensitive collation can call one name.
TWINS = [
    "Twin Report",
    "twin report",
    "café job",
    "cafe job",
    "straße",
    "strasse",
    "Ångström",
    "Ångström",
    "padded",
    "padded ",
    "alone",
]


class _RolledBack(Exception):
    pass


def equal_under_the_unique_index(alias, names):
    """
    The oracle: which names the destination's own unique index calls one,
    found by inserting them one by one in a transaction that is rolled back.
    """
    from django.db import IntegrityError, transaction
    from django.utils import timezone

    groups = {}
    now = timezone.now()
    try:
        with transaction.atomic(using=alias):
            for name in names:
                try:
                    with transaction.atomic(using=alias):
                        OxSchedule.objects.using(alias).create(
                            name=name,
                            task_key="x.y.z",
                            trigger="cron",
                            cron="* * * * *",
                            start_time=now,
                            created_at=now,
                            updated_at=now,
                        )
                    groups[name] = [name]
                except IntegrityError:
                    holder = OxSchedule.objects.using(alias).get(name=name).name
                    groups[holder].append(name)
            raise _RolledBack
    except _RolledBack:
        pass
    return [group for group in groups.values() if len(group) > 1]


def test_names_the_destination_calls_one_name_are_all_listed(
    beat, schedulable, destination
):
    """
    beat's table can hold two names the destination's unique index calls
    one (a binary SQLite or PostgreSQL table, a MySQL column of another
    collation), so create_schedules would refuse the batch. Every member is
    listed, none chosen to keep the name, and what is printed applies.
    """
    for name in TWINS:
        store(name)
    output = run()
    oracle = equal_under_the_unique_index(destination, TWINS)
    listed = refusals(output)
    clashing = {name for group in oracle for name in group}
    for group in oracle:
        for name in group:
            assert name in listed, (name, group)
            assert listed[name].startswith("its name equals the name of "), listed[name]
            for other in group:
                if other != name:
                    assert ascii(other) in listed[name]
    for name in TWINS:
        if name not in clashing:
            assert name in printed_rows(output), name
    if vendor_of(destination) != "mysql":
        assert oracle == []
    run_as_module(section_2(output))


#: How a reason for a name clash begins, before the others it names.
CLASHES_WITH = "its name equals the name of "


def clash_lines(output):
    """The lines that list a row for a name clash, in the order listed."""
    return [
        line
        for line in output.splitlines()
        if line.startswith("#   ") and f": {CLASHES_WITH}" in line
    ]


def others_named(line):
    """The {others} of one such line: what it names, and what follows them."""
    reason = line.split(f": {CLASHES_WITH}", 1)[1]
    return reason[: reason.index(" under the ")]


def test_a_name_eleven_others_share_names_ten_of_them_and_counts_the_last(
    beat, schedulable
):
    """
    A reason for a name clash names the first ten other rows of the name,
    in the order the rows are listed, and how many more there are.
    """
    if vendor_of("default") == "mysql":
        # Twelve spellings the destination's collation calls one name, so
        # which ten are named, and in what order, can be seen.
        names = [
            "".join(case)
            for case in itertools.product(*zip("dupe", "DUPE", strict=True))
        ]
        names = names[:12]
        assert equal_under_the_unique_index("default", names) == [names]
    else:
        names = ["dupe"] * 12
    for name in names:
        store(name)
    lines = clash_lines(run())
    expected = []
    for index in range(12):
        others = names[:index] + names[index + 1 :]
        expected.append(", ".join(map(ascii, others[:10])) + LIST_CUT.format(more=1))
    assert [others_named(line) for line in lines] == expected


def test_a_name_many_rows_share_gives_lines_that_do_not_grow_with_them(
    beat, schedulable
):
    """
    Every row is still listed, each line as long for 300 rows as for 30.
    """
    said = {}
    stored_rows = 0
    for count in (30, 300):
        while stored_rows < count:
            store("dupe")
            stored_rows += 1
        said[count] = clash_lines(run())
        assert len(said[count]) == count
        assert len(set(said[count])) == 1
    ten = ", ".join(["'dupe'"] * 10)
    assert others_named(said[30][0]) == ten + LIST_CUT.format(more=19)
    assert said[300][0] == said[30][0].replace(
        LIST_CUT.format(more=19), LIST_CUT.format(more=289)
    )


def test_a_name_the_destination_already_holds_is_listed(beat, schedulable, destination):
    from django_ox import stored

    stored.create_schedule(
        name="NIGHTLY", task_key="x.y.z", trigger="cron", cron="0 2 * * *"
    )
    stored.create_schedule(
        name="poller", task_key="x.y.z", trigger="cron", cron="0 2 * * *"
    )
    output = run()
    listed = refusals(output)
    assert listed["poller"] == NAME_TAKEN.format(
        alias=ascii(destination), existing="'poller'"
    )
    if vendor_of(destination) == "mysql":
        assert listed["nightly"] == NAME_TAKEN.format(
            alias=ascii(destination), existing="'NIGHTLY'"
        )
    else:
        assert "nightly" in printed_rows(output)
        run_as_module(section_2(output))


def test_without_a_schedule_table_the_names_in_use_are_not_checked_and_it_says_so(
    beat, monkeypatch
):
    # The instance's own attribute, which is what the command reads: another
    # test may have left one there that a patch of the class would not reach.
    introspection = connections["default"].introspection
    real = introspection.table_names

    def without_schedules(*args, **kwargs):
        return [t for t in real(*args, **kwargs) if t != "django_ox_oxschedule"]

    monkeypatch.setattr(introspection, "table_names", without_schedules)
    output = run()
    assert NAMES_UNCHECKED.format(alias="'default'") in output.splitlines()
    assert "nightly" in printed_rows(output)


def test_a_destination_that_does_not_answer_stops_the_import_in_one_line(
    beat, monkeypatch
):
    from django.core.management.base import CommandError
    from django.db import OperationalError

    def unreachable(self, names):
        raise OperationalError("server has gone away")

    monkeypatch.setattr(command._Destination, "_existing", unreachable)
    out = StringIO()
    with pytest.raises(CommandError) as stopped:
        call_command("ox_import_beat_schedules", database=BEAT, stdout=out)
    assert str(stopped.value) == DESTINATION_UNREADABLE.format(alias="'default'")
    assert out.getvalue() == ""


# ----------------------------------------------------------- Interval limits

INTERVAL_PAST_TICKS = (
    "its interval is {seconds} seconds. A stored schedule's interval can be at most "
    "{limit} seconds, the longest a worker can count ticks for."
)
INTERVAL_PAST_COLUMN = (
    "its interval is {seconds} seconds. The every_seconds column of database {alias} "
    "({database}) holds at most {limit}."
)
INTERVAL_PAST_VALIDATION = (
    "its interval is {seconds} seconds. create_schedules accepts at most {limit} in "
    "every_seconds: Django validates that field against the range of the default "
    "database ({database}), whichever database the schedules are on."
)
#: The longest interval a worker counts ticks for: year 1 to 1970.
TICKS = 62_135_596_800


def limits(alias):
    """Each limit an interval written to `alias` meets, lowest first."""
    column = connections[alias].ops.integer_field_range("PositiveIntegerField")[1]
    default = connections["default"].ops.integer_field_range("PositiveIntegerField")
    candidates = [(column, "column"), (default[1], "validation"), (TICKS, "ticks")]
    return sorted(candidates, key=lambda limit: limit[0])


def interval_reason(alias, seconds):
    (limit, rule), *_ = limits(alias)
    if rule == "column":
        return INTERVAL_PAST_COLUMN.format(
            seconds=seconds,
            alias=ascii(alias),
            database=DATABASE_NAMES[vendor_of(alias)],
            limit=limit,
        )
    if rule == "validation":
        return INTERVAL_PAST_VALIDATION.format(
            seconds=seconds,
            limit=limit,
            database=DATABASE_NAMES[vendor_of("default")],
        )
    return INTERVAL_PAST_TICKS.format(seconds=seconds, limit=limit)


def set_beat_interval(every, period):
    with connections[BEAT].cursor() as cursor:
        cursor.execute(
            "UPDATE django_celery_beat_intervalschedule SET every = %s, period = %s "
            "WHERE id = 1",
            [every, period],
        )


def test_an_interval_past_its_limit_is_told_that_limit_and_no_other(
    beat, schedulable, destination
):
    """
    With the default database PostgreSQL and the schedules on MySQL, a value
    can be past both 2147483647 and 4294967295. One applies, the lowest, and
    the reason names it and what sets it: the destination's column, Django's
    validation against the default database, or the tick arithmetic.
    """
    (limit, _rule), *others = limits(destination)
    days = limit // 86_400
    set_beat_interval(days, "days")
    output = run()
    assert "poller" not in refusals(output)
    assert printed_rows(output)["poller"]["every_seconds"] == days * 86_400
    run_as_module(section_2(output))
    assert OxSchedule.objects.using(destination).get(name="poller").every_seconds == (
        days * 86_400
    )
    OxSchedule.objects.using(destination).all().delete()

    set_beat_interval(days + 1, "days")
    reason = refusals(run())["poller"]
    assert reason == interval_reason(destination, (days + 1) * 86_400)
    for other, _ in others:
        if other != limit:
            assert str(other) not in reason


def test_past_every_limit_at_once_an_interval_is_told_the_lowest(beat, destination):
    # 999,999,999 days: what beat's timedelta still builds, past every limit.
    set_beat_interval(999_999_999, "days")
    assert refusals(run())["poller"] == interval_reason(
        destination, 999_999_999 * 86_400
    )


def test_the_limit_is_exact_to_the_second(beat, schedulable, destination):
    if vendor_of(BEAT) != "sqlite":
        pytest.skip("beat's table here holds 32 bits in its interval column")
    (limit, _), *_ = limits(destination)
    set_beat_interval(limit, "seconds")
    assert printed_rows(run())["poller"]["every_seconds"] == limit
    set_beat_interval(limit + 1, "seconds")
    assert refusals(run())["poller"] == interval_reason(destination, limit + 1)


# -------------------------------------------------------------- Shell output

#: Names whose characters a terminal acts on: an OSC window-title set, a
#: screen clear and cursor home, a full reset, a bell, a bare carriage return.
CONTROL_NAMES = [
    "daily\x1b]0;PWNED-TITLE\x07\x1b[2J\x1b[Hall schedules created, 0 errors\r",
    "reset\x1bc",
    "bell\x07",
    "cr\roverwritten",
]


def test_applying_the_output_in_a_shell_echoes_no_schedule_and_no_control_character(
    beat, schedulable, capsys
):
    """
    A bare create_schedules call at the end of the printed program would
    make a shell echo its value, `[<OxSchedule: name>, ...]`, sending each
    stored name to the terminal as it is: escape sequences, a carriage
    return. The result is assigned, and a shell has nothing to echo.
    """
    from .test_import_beat import paste_into_shell

    for name in CONTROL_NAMES:
        store(name)
    output = run()
    section = section_2(output)
    assert section.isascii()
    capsys.readouterr()
    paste_into_shell(section)
    echoed = capsys.readouterr().out
    assert "OxSchedule" not in echoed
    assert not any(ch in echoed for ch in "\x1b\x07\r")
    assert set(OxSchedule.objects.values_list("name", flat=True)) == {
        *CONTROL_NAMES,
        "nightly",
        "poller",
    }


def test_the_call_s_result_is_assigned_not_left_to_be_echoed(beat):
    import ast

    tree = ast.parse(section_2(run()))
    (statement,) = [s for s in tree.body if not isinstance(s, ast.ImportFrom)]
    assert isinstance(statement, ast.Assign)
    assert statement.value.func.id == "create_schedules"
    assert not any(
        isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
        for node in tree.body
    )


# -------------------------------------------------------- Diagnostic quoting

EXCERPT_CUT = " [cut; {length} {unit} in all]"
#: The longest quotation of one value, and so with the mark after it.
EXCERPT_LIMIT = 1300


def test_a_quoted_value_is_one_bounded_printable_ascii_literal():
    assert command._excerpt("short") == "'short'"
    digits = "1" * 100_000
    cut = command._excerpt(digits)
    assert cut == ascii("1" * 1298) + EXCERPT_CUT.format(
        length=100_000, unit="characters"
    )
    hostile = "\u202e\x1b[2J\r\n\u2028\x00" * 30_000
    cut = command._excerpt(hostile)
    assert cut.isascii() and cut.isprintable()
    literal, mark = cut.split(" [cut; ")
    assert len(literal) <= EXCERPT_LIMIT
    # What is quoted still reads as a literal, of the start of the value.
    assert hostile.startswith(eval(literal))  # noqa: S307
    assert mark == f"{len(hostile)} characters in all]"
    cut = command._excerpt(b"\xff" * 5000)
    assert cut.endswith(EXCERPT_CUT.format(length=5000, unit="bytes"))
    assert len(cut.split(" [cut; ")[0]) <= EXCERPT_LIMIT
    # The longest name or task a stored schedule can hold is never cut.
    assert "[cut" not in command._excerpt("\U0001f600" * 128)
    assert command._excerpt(10**4000).endswith(" characters in all]")


def test_a_stored_value_keeps_its_text_where_it_is_printed_as_code():
    value = command._source("x" * 2000)
    assert command._plain(value) == "x" * 2000
    assert type(command._plain(value)) is str
    assert len(repr(value)) < 2000
    assert str(value) == "x" * 2000


def test_hostile_and_huge_stored_values_give_bounded_lines_one_per_row(
    beat, schedulable
):
    """
    Every quoted value is cut to one bounded literal with a mark saying it
    was, the reason's own words kept around it, and each row is still
    printed or listed once.
    """
    if vendor_of(BEAT) != "sqlite":
        pytest.skip("only SQLite keeps values this long in beat's columns")
    hostile = "\x1b[2J\u202e\r\u2028"
    with connections[BEAT].cursor() as cursor:
        cursor.execute(
            "INSERT INTO django_celery_beat_crontabschedule (id, minute, hour, "
            "day_of_month, month_of_year, day_of_week, timezone) VALUES "
            "(2, %s, '2', '*', '*', '*', %s), (3, '0', %s, '*', '*', '*', %s), "
            "(4, '0', '2', '*', '*', '*', %s)",
            [
                "1" * 100_000,
                settings.TIME_ZONE,
                hostile * 20_000,
                settings.TIME_ZONE,
                "Z" * 100_000,
            ],
        )
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (4, 60, %s), (5, %s, 'seconds')",
            [b"\xfe" * 100_000, b"\x01" * 100_000],
        )
        cursor.execute(
            "INSERT INTO django_celery_beat_intervalschedule (id, every, period) "
            "VALUES (2, 60, %s), (3, %s, 'seconds')",
            # Digits and a letter, so SQLite keeps it as text rather than
            # turning a numeral that long into an infinite REAL.
            ["p" * 100_000, "9" * 100_000 + "x"],
        )
    insert_task(10, "long-field", "x.y.z", db=BEAT, crontab_id=2)
    insert_task(11, "control-field", "x.y.z", db=BEAT, crontab_id=3)
    insert_task(12, "long-zone", "x.y.z", db=BEAT, crontab_id=4)
    insert_task(13, "long-period", "x.y.z", db=BEAT, interval_id=2)
    insert_task(14, "long-every", "x.y.z", db=BEAT, interval_id=3)
    insert_task(15, "long-name-" + "n" * 100_000, "x.y.z", db=BEAT, crontab_id=1)
    insert_task(16, "control-name-" + hostile * 30_000, "x.y.z", db=BEAT, crontab_id=1)
    marker = json.dumps({"x": {"__type__": "t" * 100_000, "__value__": 1}})
    insert_task(17, "long-marker", "x.y.z", db=BEAT, crontab_id=1, kwargs=marker)
    insert_task(18, b"blob-" + b"\xff" * 100_000, "x.y.z", db=BEAT, crontab_id=1)
    insert_task(19, "blob-period", "x.y.z", db=BEAT, interval_id=4)
    insert_task(20, "blob-every", "x.y.z", db=BEAT, interval_id=5)
    output = run()
    lines = output.splitlines()
    assert all(line.isascii() and line.isprintable() for line in lines)
    longest = max(len(line) for line in lines)
    assert longest < 3 * EXCERPT_LIMIT, longest
    listed = [line for line in lines if line.startswith("#   ")]
    reasons = refusals(output)
    # Every row once: printed, or listed under one name.
    with connections[BEAT].cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM django_celery_beat_periodictask")
        (rows,) = cursor.fetchone()
    differing = {line for line in listed if line in output.split(NOT_TRANSLATED)[0]}
    assert len(reasons) == len(listed) - len(differing)
    assert len(reasons) + len(printed_rows(output)) == rows
    cut = " [cut; "
    for name in ("long-field", "control-field", "long-zone", "long-period"):
        assert cut in reasons[name], name
    assert "Celery refuses" in reasons["long-field"]
    assert "cannot load" in reasons["long-zone"]
    assert cut in reasons["long-every"]
    assert "not a number" in reasons["long-every"]
    assert cut in reasons["long-marker"]
    assert cut in reasons["blob-period"] and "bytes in all" in reasons["blob-period"]
    assert cut in reasons["blob-every"] and "bytes in all" in reasons["blob-every"]
    assert sum(1 for line in listed if cut in line.split(": ", 1)[0]) >= 3
