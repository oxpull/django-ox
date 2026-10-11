"""
Rate-limit names: the names a collation would fold into one, and one name
configured differently on two backends.

A limit's name is the primary key of its window row. A primary key decides
identity with the column's collation, not with Python's ``==``, and MySQL's
usual collation folds case: `Reports` and `reports` are two limits to the
settings parser and one row to the database. They would share a window,
count each other's admissions, and throttle traffic neither was configured
for, with nothing raised anywhere.

The same row is shared across backends. A name is keyed alone, with no
alias, so two backends that configure one name with different periods or
capacities count on one window, and `manage.py check` reports it. It
reports two spellings that differ only by case or accents on two backends
too, which MySQL's default collation counts on one window and PostgreSQL
and SQLite on two.
"""

from contextlib import contextmanager
from datetime import timedelta
from io import StringIO

import pytest
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command, load_command_class
from django.core.management.base import SystemCheckError
from django.db import connection
from django.db.models.functions import Now
from django.tasks import task_backends
from django.test import override_settings
from django.test.utils import CaptureQueriesContext

from django_ox.models import OxTask
from oxpull.backend import OxpullBackend
from oxpull.models import OxpullRateWindow
from oxpull.rate_limits import rate_limits_from_options

from . import tasks
from .test_rate_limits import ECHO, oxpull_worker, rate_limits


def options(*names):
    return {
        "RATE_LIMITS": {
            name: {"limit": 10, "per": 60, "queues": ["default"]} for name in names
        }
    }


class TestNamesThatACollationWouldFold:
    def test_two_names_differing_only_by_case_are_refused(self):
        with pytest.raises(ImproperlyConfigured, match="differ only"):
            rate_limits_from_options(
                options("Reports", "reports"), "default", ["default"]
            )

    def test_the_message_names_both(self):
        with pytest.raises(ImproperlyConfigured) as caught:
            rate_limits_from_options(
                options("Reports", "reports"), "default", ["default"]
            )
        assert "'Reports'" in str(caught.value)
        assert "'reports'" in str(caught.value)

    @pytest.mark.parametrize(
        "pair",
        [("emails", "EMAILS"), ("send-mail", "Send-Mail"), ("a", "A")],
    )
    def test_it_holds_however_the_case_differs(self, pair):
        with pytest.raises(ImproperlyConfigured, match="differ only"):
            rate_limits_from_options(options(*pair), "default", ["default"])

    def test_names_differing_by_more_than_case_are_accepted(self):
        limits = rate_limits_from_options(
            options("reports-daily", "reports-hourly"), "default", ["default"]
        )
        assert sorted(limits) == ["reports-daily", "reports-hourly"]

    def test_one_name_is_fine(self):
        limits = rate_limits_from_options(options("reports"), "default", ["default"])
        assert sorted(limits) == ["reports"]

    def test_no_limits_at_all_is_fine(self):
        assert rate_limits_from_options({}, "default", ["default"]) == {}


# Stored spelling, configured spelling. The first pair differs by case,
# which every case-insensitive collation folds; the second by an accent,
# which MySQL's utf8mb4_unicode_ci and utf8mb4_0900_ai_ci fold as well and
# no Python string operation predicts.
RENAMES = [("Stripe", "stripe"), ("Café", "cafe")]


@pytest.mark.django_db
class TestALimitRenamedToASpellingTheCollationFolds:
    """
    One configured name whose window row was written under another spelling.

    A limit deployed as `Stripe` and renamed to `stripe` leaves a row the
    parser above cannot see: it only compares configured names with each
    other. On MySQL the INSERT for `stripe` is refused as a duplicate of
    `Stripe`, the SELECT keyed by the stored spelling has no `stripe`
    entry, and claim_one() raised KeyError, which django-ox's run() does
    not contain. The worker died on every claim, and five deaths in a
    minute trip the supervisor's restart cap.
    """

    @pytest.mark.parametrize(("stored", "configured"), RENAMES)
    def test_the_claim_survives_and_the_admission_lands_on_the_limits_row(
        self, stored, configured
    ):
        OxpullRateWindow.objects.create(limit_name=stored, window_started_at=Now())
        tasks.plain_echo.enqueue("a")
        with rate_limits({configured: {"limit": 5, "per": 60, "tasks": [ECHO]}}):
            claimed = oxpull_worker().claim_one()
        assert claimed is not None
        # The database decides which row the configured name names: the
        # stored row where the collation folds the two, a freshly seeded
        # one where spelling is identity. Either way the admission lands
        # on it.
        assert OxpullRateWindow.objects.get(limit_name=configured).used == 1
        # The next refresh reads the same row under the configured name
        # without another seed: the attribution is learned once.
        tasks.plain_echo.enqueue("b")
        with rate_limits({configured: {"limit": 5, "per": 60, "tasks": [ECHO]}}):
            worker = oxpull_worker()
            assert worker.claim_one() is not None
            with CaptureQueriesContext(connection) as captured:
                assert worker.claim_one() is None
        assert not any("INSERT" in q["sql"].upper() for q in captured.captured_queries)
        assert OxpullRateWindow.objects.get(limit_name=configured).used == 2
        rows = sorted(OxpullRateWindow.objects.values_list("limit_name", flat=True))
        if connection.vendor == "mysql":
            assert rows == [stored]
        else:
            assert rows == sorted([stored, configured])

    @pytest.mark.parametrize(("stored", "configured"), RENAMES)
    def test_the_stored_rows_count_still_throttles(self, stored, configured):
        """
        Where the collation folds the two, the stored row is the limit's
        row and its count is the limit's count: a worker that adopted it
        must honour a window that is already full, not read it as empty.
        """
        if connection.vendor != "mysql":
            pytest.skip("only a folding collation makes one row of the two")
        OxpullRateWindow.objects.create(
            limit_name=stored, window_started_at=Now(), used=5
        )
        tasks.plain_echo.enqueue("a")
        with rate_limits({configured: {"limit": 5, "per": 60, "tasks": [ECHO]}}):
            assert oxpull_worker().claim_one() is None


# Two configured names that no Python string operation folds together and
# that MySQL's utf8mb4_unicode_ci and utf8mb4_0900_ai_ci treat as one.
ACCENT_PAIRS = [("Café", "cafe"), ("naïve", "naive")]


@pytest.mark.django_db
class TestTwoConfiguredNamesTheDatabaseFoldsIntoOneRow:
    """
    The parser refuses names that differ only by case, which is what every
    case-insensitive collation folds. Accent folding is the collation's
    alone: `casefold()` keeps `Café` and `cafe` apart, MySQL's primary key
    does not. Both limits were seeded onto one row, both counted on it and
    each throttled the other's traffic, with nothing raised anywhere. Only
    the database can say which names it folds, so the worker asks it: the
    seed writes one row, the second name's seed is refused, and the adopt
    read for it comes back with a row another configured limit already
    owns. That is refused at the first refresh, before any claim.
    """

    @pytest.mark.parametrize(("first", "second"), ACCENT_PAIRS)
    def test_the_first_refresh_refuses_and_names_both(self, first, second):
        if connection.vendor != "mysql":
            pytest.skip("only a folding collation makes one row of the two")
        tasks.plain_echo.enqueue("a")
        config = {
            first: {"limit": 5, "per": 60, "tasks": [ECHO]},
            second: {"limit": 5, "per": 60, "queues": ["default"]},
        }
        with rate_limits(config), pytest.raises(ImproperlyConfigured) as caught:
            oxpull_worker().claim_one()
        assert repr(first) in str(caught.value)
        assert repr(second) in str(caught.value)
        assert OxTask.objects.get().status == OxTask.Status.READY

    @pytest.mark.parametrize(("first", "second"), ACCENT_PAIRS)
    def test_a_row_stored_under_a_third_spelling_is_refused_too(self, first, second):
        if connection.vendor != "mysql":
            pytest.skip("only a folding collation makes one row of the two")
        OxpullRateWindow.objects.create(
            limit_name=first.upper(), window_started_at=Now()
        )
        tasks.plain_echo.enqueue("a")
        config = {
            first: {"limit": 5, "per": 60, "tasks": [ECHO]},
            second: {"limit": 5, "per": 60, "queues": ["default"]},
        }
        with rate_limits(config), pytest.raises(ImproperlyConfigured, match="one row"):
            oxpull_worker().claim_one()

    @pytest.mark.parametrize(("first", "second"), ACCENT_PAIRS)
    def test_where_spelling_is_identity_both_limits_get_their_own_row(
        self, first, second
    ):
        if connection.vendor == "mysql":
            pytest.skip("the folding collation is the other test's subject")
        tasks.plain_echo.enqueue("a")
        config = {
            first: {"limit": 5, "per": 60, "tasks": [ECHO]},
            second: {"limit": 5, "per": 60, "queues": ["default"]},
        }
        with rate_limits(config):
            assert oxpull_worker().claim_one() is not None
        rows = sorted(OxpullRateWindow.objects.values_list("limit_name", flat=True))
        assert rows == sorted([first, second])


# Pairs as MySQL 8.4 matches them under utf8mb4_0900_ai_ci and
# utf8mb4_unicode_ci. Each pair in the second list is two names to both, and
# a comparison slightly wider than oxpull.W006's would call it one.
ONE_ROW_ON_MYSQL = [
    ("Stripe", "stripe"),
    ("stripe", "stripé"),
    ("café", "cafe\u0301"),
    ("Straße", "strasse"),
    ("\u1fb3", "\u03b1"),  # alpha with ypogegrammeni, and alpha
]
TWO_ROWS_ON_MYSQL = [
    ("\u0439", "\u0438"),  # Cyrillic short i is a letter, not i with a breve
    ("a\u0363", "a"),  # U+0363 is a small a written above, not an accent
    ("\u1c90", "\u10d0"),  # Georgian Mtavruli is newer than MySQL's weights
    ("\u1fb3", "\u03b1\u03b9"),  # casefold() spells the ypogegrammeni as iota
    ("\u0b94", "\u0b92\u0bd7"),  # Tamil au, precomposed and decomposed
]
FOLDING_COLLATIONS = ("utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci")


def window_collation():
    """The window table's limit_name collation on MySQL, else None."""
    if connection.vendor != "mysql":
        return None
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLLATION_NAME FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "AND column_name = 'limit_name'",
            [OxpullRateWindow._meta.db_table],
        )
        return cursor.fetchone()[0]


def oxpull_backend(limits, queues=("default",)):
    return {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": list(queues),
        "OPTIONS": {
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "RATE_LIMITS": limits,
        },
    }


# The installation each check test extends. Its one backend sorts first,
# names no limit and schedules the reconciler, so its own checks report
# nothing. It is written out here, not read from the suite's TASKS. Tests of
# another feature can add an alias there whose own check is an Error on
# purpose, and that Error would raise in every test here that expects none.
INSTALLATION = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "SCHEDULES": {
                "oxpull-batch-reconcile": {
                    "task": "oxpull.batches.reconcile",
                    "cron": "* * * * *",
                },
            },
        },
    },
}


@contextmanager
def backends(whole=None, /, **limits_by_alias):
    """
    INSTALLATION, plus one OxpullBackend alias per keyword, and any alias
    given whole in `whole`.
    """
    added = {alias: oxpull_backend(limits) for alias, limits in limits_by_alias.items()}
    with override_settings(TASKS={**INSTALLATION, **(whole or {}), **added}):
        yield


def run_check(**options):
    """
    `manage.py check`, as a deploy runs it. Warnings come back as text, and
    an Error raises SystemCheckError, whose text is the whole report.
    """
    out, err = StringIO(), StringIO()
    call_command("check", stdout=out, stderr=err, **options)
    return out.getvalue() + err.getvalue()


def check_report(**options):
    """The whole `check` report, whether or not an Error raised."""
    try:
        return run_check(**options)
    except SystemCheckError as raised:
        return str(raised)


def named(name, limit, per, **covers):
    return {name: {"limit": limit, "per": per, **(covers or {"tasks": [ECHO]})}}


def stripe(limit, per, **covers):
    return named("stripe", limit, per, **covers)


def emitted_by(check_id):
    """The aliases whose own check() returns check_id, under current TASKS."""
    return [
        alias
        for alias in sorted(settings.TASKS)
        if isinstance(task_backends[alias], OxpullBackend)
        and check_id in [message.id for message in task_backends[alias].check()]
    ]


def messages(check_id):
    """Every message with check_id, under current TASKS."""
    return [
        message
        for alias in sorted(settings.TASKS)
        if isinstance(task_backends[alias], OxpullBackend)
        for message in task_backends[alias].check()
        if message.id == check_id
    ]


def levels(check_id):
    """The level of every message with check_id, under current TASKS."""
    return [message.level for message in messages(check_id)]


class ProjectBackend(OxpullBackend):
    """A project's own subclass of OxpullBackend."""


PROJECT_BACKEND = f"{__name__}.ProjectBackend"


def ox_worker_start(alias, started):
    """
    django-ox's `ox_worker` for one backend, run through BaseCommand.execute
    as `manage.py` runs it, with handle() replaced so no worker loop starts.
    Reaching handle() appends the backend to `started`. Returns what the
    system checks wrote. An Error raises SystemCheckError before handle().
    """
    command = load_command_class("django_ox", "ox_worker")
    command.handle = lambda *args, **options: started.append(options["backend"])
    out, err = StringIO(), StringIO()
    call_command(command, "--backend", alias, skip_checks=False, stdout=out, stderr=err)
    return out.getvalue() + err.getvalue()


@pytest.mark.django_db(databases="__all__")
class TestOneNameConfiguredDifferentlyOnTwoBackends:
    """
    A limit's window row is keyed on its name alone, so every backend that
    configures one name counts on one shared window, and nothing reported
    two backends that disagree about it. Two periods cannot both hold on
    one window. A shorter one rolls it and restarts its count, so the limit
    is not enforced as configured. Two capacities over one period are a
    shared budget, which can be deliberate. Each is a Warning with an id of
    its own. Two spellings that differ only by case or accents are one window
    on MySQL's default collation and two on PostgreSQL and SQLite. That is a
    Warning too, under a third id.

    `check` runs here as a deploy runs it. From Django 6.1 that reads a
    database feature on every alias for Django's own model checks, so these
    tests allow every database. The class below proves this check reads
    none.
    """

    def test_a_different_per_is_a_warning_naming_both_backends_and_periods(self):
        with backends(billing=stripe(5, 1), reports=stripe(5, 60)):
            report = run_check()
            assert levels("oxpull.W005") == [checks.WARNING]
        assert report.count("oxpull.W005") == 1
        assert "Rate limit 'stripe' has a different period" in report
        assert "not enforced as configured" in report
        assert "'billing' (limit=5, per=1)" in report
        assert "'reports' (limit=5, per=60)" in report
        assert "oxpull.W004" not in report
        assert "oxpull.W006" not in report

    def test_a_different_limit_over_the_same_per_is_a_warning_of_its_own(self):
        with backends(
            billing=stripe(5, 60), reports=stripe(10, 60, queues=["default"])
        ):
            report = run_check()
            assert levels("oxpull.W004") == [checks.WARNING]
        assert report.count("oxpull.W004") == 1
        assert "Rate limit 'stripe' has a different limit" in report
        assert "'billing' (limit=5, per=60)" in report
        assert "'reports' (limit=10, per=60)" in report
        assert "oxpull.W005" not in report

    def test_a_different_per_is_the_one_reported_when_the_limit_differs_too(self):
        with backends(billing=stripe(5, 1), reports=stripe(10, 60)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "oxpull.W004" not in report

    @pytest.mark.parametrize(
        ("shorter", "longer"),
        [(60, 61), (60, 90), (3600, timedelta(minutes=61))],
    )
    def test_close_periods_are_reported(self, shorter, longer):
        with backends(billing=stripe(5, shorter), reports=stripe(5, longer)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "oxpull.W004" not in report

    def test_it_is_reported_once_however_many_backends_name_it(self):
        """
        From the first OxpullBackend alias in sort order, as oxpull.W002 is,
        so one id silences it and a report does not repeat per alias.
        """
        with backends(billing=stripe(5, 1), reports=stripe(5, 60), sms=stripe(5, 60)):
            report = run_check()
            assert emitted_by("oxpull.W005") == ["billing"]
        assert report.count("oxpull.W005") == 1
        assert "'reports' (limit=5, per=60)" in report
        assert "'sms' (limit=5, per=60)" in report

    def test_it_is_reported_by_the_first_backend_when_that_one_names_no_limit(self):
        """
        INSTALLATION's `default` sorts first and configures no limit, the
        usual shape of a project that limits one backend of several.
        """
        with backends(reports=stripe(5, 1), zulu=stripe(5, 60)):
            report = run_check()
            assert emitted_by("oxpull.W005") == ["default"]
        assert report.count("oxpull.W005") == 1

    def test_a_disagreement_is_found_behind_backends_that_agree(self):
        with backends(billing=stripe(5, 60), reports=stripe(5, 60), sms=stripe(5, 1)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "'sms' (limit=5, per=1)" in report

    def test_a_disagreement_is_found_between_backends_that_agree(self):
        with backends(billing=stripe(5, 60), reports=stripe(5, 1), sms=stripe(5, 60)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "'reports' (limit=5, per=1)" in report

    def test_every_name_that_disagrees_is_reported(self):
        with backends(
            billing={**stripe(5, 1), **named("sms", 5, 60)},
            reports={**stripe(5, 60), **named("sms", 10, 60)},
        ):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "Rate limit 'stripe' has a different period" in report
        assert report.count("oxpull.W004") == 1
        assert "Rate limit 'sms' has a different limit" in report

    @pytest.mark.parametrize(
        ("billing", "reports"),
        [
            # One configuration. Still one budget on MySQL and two elsewhere.
            ((5, 60), (5, 60)),
            # Two configurations. On MySQL that is oxpull.W005 unreported.
            ((5, 1), (10, 60)),
        ],
    )
    def test_names_that_differ_only_by_case_are_a_warning(self, billing, reports):
        with backends(
            billing=named("Stripe", *billing), reports=named("stripe", *reports)
        ):
            report = run_check()
            assert levels("oxpull.W006") == [checks.WARNING]
        assert report.count("oxpull.W006") == 1
        assert "'billing' ('Stripe') and 'reports' ('stripe')" in report
        assert "oxpull.W005" not in report
        assert "oxpull.W004" not in report

    @pytest.mark.parametrize(
        ("first", "second", "shown"),
        [
            # Each pair is one name to casefold() and to MySQL's
            # utf8mb4_0900_ai_ci. The first two are not one name to lower().
            ("Straße", "strasse", "'billing' ('Straße') and 'reports' ('strasse')"),
            # The "fi" ligature can print like f and i, so it is escaped.
            ("\ufb01le", "file", "'billing' ('\\ufb01le') and 'reports' ('file')"),
            # The Kelvin sign.
            ("\u212aey", "key", "'billing' ('\u212aey') and 'reports' ('key')"),
        ],
    )
    def test_names_equal_once_case_is_folded_are_a_warning(self, first, second, shown):
        with backends(billing=named(first, 5, 60), reports=named(second, 5, 60)):
            report = run_check()
        assert report.count("oxpull.W006") == 1
        assert shown in report

    @pytest.mark.parametrize(
        ("first", "second", "shown"),
        [
            ("stripe", "stripé", "'billing' ('stripe') and 'reports' ('stripé')"),
            ("Café", "cafe", "'billing' ('Café') and 'reports' ('cafe')"),
            # One accent precomposed, the other a combining mark. They print
            # alike, so both are shown as code points.
            (
                "café",
                "cafe\u0301",
                "'billing' ('caf\\u00e9') and 'reports' ('cafe\\u0301')",
            ),
            ("Stripe", "STRIPÉ", "'billing' ('Stripe') and 'reports' ('STRIPÉ')"),
        ],
    )
    def test_names_that_differ_only_by_accents_are_a_warning(
        self, first, second, shown
    ):
        with backends(billing=named(first, 5, 60), reports=named(second, 5, 60)):
            report = run_check()
            assert levels("oxpull.W006") == [checks.WARNING]
        assert report.count("oxpull.W006") == 1
        assert shown in report

    @pytest.mark.parametrize(
        ("billing", "reports", "sms"),
        [("stripe", "Stripe", "STRIPE"), ("stripe", "Stripe", "stripé")],
    )
    def test_three_spellings_on_three_backends_are_one_warning(
        self, billing, reports, sms
    ):
        with backends(
            billing=named(billing, 5, 60),
            reports=named(reports, 5, 60),
            sms=named(sms, 5, 60),
        ):
            report = run_check()
        assert report.count("oxpull.W006") == 1
        assert (
            f"'billing' ({billing!r}), 'reports' ({reports!r}) and 'sms' ({sms!r})"
            in report
        )

    @pytest.mark.parametrize(("first", "second"), ONE_ROW_ON_MYSQL + TWO_ROWS_ON_MYSQL)
    def test_it_is_reported_exactly_when_mysql_keeps_one_row(self, first, second):
        """
        On MySQL the suite's database uses utf8mb4_unicode_ci, which matches
        these pairs as the default utf8mb4_0900_ai_ci does, and the row is
        read back under the second spelling.
        """
        with backends(billing=named(first, 5, 60), reports=named(second, 5, 60)):
            report = run_check()
        one_row = (first, second) in ONE_ROW_ON_MYSQL
        assert report.count("oxpull.W006") == one_row
        if window_collation() in FOLDING_COLLATIONS:
            OxpullRateWindow.objects.create(limit_name=first, window_started_at=Now())
            found = OxpullRateWindow.objects.filter(limit_name=second).exists()
            assert found == one_row

    @pytest.mark.parametrize(
        ("first", "second", "one_row_on_unicode_ci"),
        [
            # Georgian capital an folds to a letter newer than Unicode 3.2.
            # utf8mb4_unicode_ci keeps the two apart. utf8mb4_0900_ai_ci
            # makes one name of them, which the key misses.
            ("\u10a0", "\u2d00", False),
            # A mark on a letter that is neither Latin nor Greek stays in the
            # key. Both collations make one name of these, which it misses.
            ("\u0438\u0306", "\u0438", True),
        ],
    )
    def test_folds_the_key_leaves_to_the_database_are_not_reported(
        self, first, second, one_row_on_unicode_ci
    ):
        with backends(billing=named(first, 5, 60), reports=named(second, 5, 60)):
            report = run_check()
        assert "oxpull.W006" not in report
        if window_collation() == "utf8mb4_unicode_ci":
            OxpullRateWindow.objects.create(limit_name=first, window_started_at=Now())
            found = OxpullRateWindow.objects.filter(limit_name=second).exists()
            assert found == one_row_on_unicode_ci

    def test_a_case_variant_is_named_beside_the_names_own_disagreement(self):
        with backends(
            billing=stripe(5, 1),
            reports=stripe(5, 60),
            sms=named("STRIPE", 5, 60),
        ):
            report = run_check()
        assert report.count("oxpull.W006") == 1
        assert (
            "'billing' ('stripe'), 'reports' ('stripe') and 'sms' ('STRIPE')" in report
        )
        assert report.count("oxpull.W005") == 1
        assert "'sms' (limit" not in report

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            # One period written two ways is one period to the worker.
            (stripe(5, 60), stripe(5, timedelta(minutes=1))),
            (stripe(5, 60), stripe(5, timedelta(seconds=60.4))),
            # One budget shared on purpose, over different tasks.
            (stripe(5, 60), stripe(5, 60, queues=["default"])),
            # Names that differ by more than case are different windows.
            (stripe(5, 1), named("stripe-reports", 5, 60)),
        ],
    )
    def test_configurations_that_agree_are_not_reported(self, first, second):
        with backends(billing=first, reports=second):
            report = run_check()
        assert "oxpull.W005" not in report
        assert "oxpull.W004" not in report
        assert "oxpull.W006" not in report

    def test_a_backend_whose_limits_do_not_parse_is_left_to_its_own_error(self):
        with (
            backends(billing=stripe(5, 1), broken=stripe(5, 0), reports=stripe(5, 60)),
            pytest.raises(SystemCheckError) as caught,
        ):
            run_check()
        report = str(caught.value)
        assert "oxpull.E005" in report
        assert report.count("oxpull.W005") == 1
        assert "'broken' (limit" not in report

    def test_each_backend_is_read_with_its_own_queues(self):
        reports = oxpull_backend(stripe(5, 60, queues=["reports"]), queues=["reports"])
        with backends({"reports": reports}, billing=stripe(5, 1)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "oxpull.E005" not in report

    def test_a_backend_of_another_kind_sorting_first_does_not_hide_it(self):
        immediate = {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}
        with backends({"aaa": immediate}, billing=stripe(5, 1), reports=stripe(5, 60)):
            report = run_check()
        assert report.count("oxpull.W005") == 1

    def test_a_django_ox_backend_sorting_first_does_not_hide_it(self):
        """
        A plain django-ox backend is not an OxpullBackend. Its check() never
        runs this check, so it must not be the alias this is emitted from.
        """
        plain = {"BACKEND": "django_ox.backend.OxBackend", "QUEUES": ["default"]}
        with backends({"aaa": plain}, billing=stripe(5, 1), reports=stripe(5, 60)):
            report = run_check()
            assert emitted_by("oxpull.W005") == ["billing"]
        assert report.count("oxpull.W005") == 1

    def test_a_subclass_of_oxpull_backend_is_compared(self):
        reports = {**oxpull_backend(stripe(5, 60)), "BACKEND": PROJECT_BACKEND}
        with backends({"reports": reports}, billing=stripe(5, 1)):
            report = run_check()
        assert report.count("oxpull.W005") == 1
        assert "'reports' (limit=5, per=60)" in report

    def test_a_subclass_of_oxpull_backend_sorting_first_reports_it(self):
        first = {"BACKEND": PROJECT_BACKEND, "QUEUES": ["default"]}
        with backends({"aaa": first}, billing=stripe(5, 1), reports=stripe(5, 60)):
            report = run_check()
            assert emitted_by("oxpull.W005") == ["aaa"]
        assert report.count("oxpull.W005") == 1

    def test_it_is_reported_when_the_first_backend_fails_its_own_limit_check(self):
        """
        Fixing the first backend's own oxpull.E006 should not be what it
        takes to see this one.
        """
        no_worker = {
            "BACKEND": "oxpull.backend.OxpullBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"RATE_LIMITS": named("other", 5, 60, queues=["default"])},
        }
        with (
            backends({"aaa": no_worker}, billing=stripe(5, 1), reports=stripe(5, 60)),
            pytest.raises(SystemCheckError) as caught,
        ):
            run_check()
        report = str(caught.value)
        assert "oxpull.E006" in report
        assert report.count("oxpull.W005") == 1

    def test_the_suites_own_settings_report_none_of_them(self):
        """
        The suite's TASKS as they are. Another feature's alias there can fail
        its own check on purpose, so only these ids are asserted.
        """
        report = check_report()
        assert "oxpull.W005" not in report
        assert "oxpull.W004" not in report
        assert "oxpull.W006" not in report

    def test_a_different_per_says_why_the_limit_does_not_hold_and_the_fix(self):
        with backends(billing=stripe(5, 1), reports=stripe(5, 60)):
            report = run_check()
            [message] = messages("oxpull.W005")
        assert message.msg == (
            "Rate limit 'stripe' has a different period on backends 'billing' "
            "(limit=5, per=1) and 'reports' (limit=5, per=60), and it is not "
            "enforced as configured. A limit's window is keyed on its name alone, "
            "so these backends roll one shared window, each at its own period. "
            "Each roll restarts the count, so a backend on a longer period can "
            "admit more than its limit in that period."
        )
        assert message.hint == (
            "Use one period for this name on every backend, or give each "
            "backend's limit a name of its own."
        )
        assert message.msg in report
        assert f"HINT: {message.hint}" in report

    def test_a_different_limit_says_what_is_shared_and_the_fix(self):
        with backends(billing=stripe(5, 60), reports=stripe(10, 60)):
            report = run_check()
            [message] = messages("oxpull.W004")
        assert message.msg == (
            "Rate limit 'stripe' has a different limit on backends 'billing' "
            "(limit=5, per=60) and 'reports' (limit=10, per=60). A limit's window is "
            "keyed on its name alone, so they share one window and count each "
            "other's admissions. Each backend stops when the shared count reaches "
            "its own limit, so together they admit up to the largest one."
        )
        assert message.hint == (
            "If a shared budget is what you want, use the same limit for this name "
            "on every backend. Otherwise give each backend's limit a name of its "
            "own."
        )
        assert message.msg in report
        assert f"HINT: {message.hint}" in report

    def test_a_spelling_difference_says_what_each_database_does_and_the_fix(self):
        with backends(billing=named("Stripe", 5, 60), reports=named("stripe", 5, 60)):
            report = run_check()
            [message] = messages("oxpull.W006")
        assert message.msg == (
            "Rate limit names on backends 'billing' ('Stripe') and 'reports' "
            "('stripe') differ only by case or accents. A limit's window is keyed on "
            "its name, and the database's collation decides which spellings match. "
            "On MySQL's default collation, which ignores case and accents, these "
            "spellings are one name, so the backends share one window and count each "
            "other's admissions. On PostgreSQL and SQLite, and on a binary MySQL "
            "collation such as utf8mb4_bin, they are two separate limits."
        )
        assert message.hint == (
            "Use one spelling of this name on every backend. If each backend "
            "needs a limit of its own, put the backend's alias in each name."
        )
        assert message.msg in report
        assert f"HINT: {message.hint}" in report

    def test_two_spellings_on_one_backend_say_its_workers_refuse_them(self):
        """
        The parser refuses a case difference within one backend, not an
        accent. That backend's workers refuse the two on MySQL's default
        collation, so it is not one of several backends counting together.
        """
        with backends(billing={**named("stripe", 5, 60), **named("stripé", 5, 60)}):
            report = run_check()
            [message] = messages("oxpull.W006")
        assert message.msg == (
            "Rate limit names 'stripe' and 'stripé' on backend 'billing' differ only "
            "by case or accents. A limit's window is keyed on its name, and the "
            "database's collation decides which spellings match. On MySQL's default "
            "collation, which ignores case and accents, these spellings are one "
            "name, so the workers for backend 'billing' refuse them before claiming "
            "any task. On PostgreSQL and SQLite, and on a binary MySQL collation "
            "such as utf8mb4_bin, they are two separate limits."
        )
        assert message.hint == (
            "Keep one of these limits, or give each one a name that differs by "
            "more than case or accents."
        )
        assert message.msg in report
        assert f"HINT: {message.hint}" in report

    def test_a_backend_with_two_spellings_beside_backends_with_one(self):
        with backends(
            billing={**named("stripe", 5, 60), **named("stripé", 5, 60)},
            reports=named("Stripe", 5, 60),
            sms=named("STRIPE", 5, 60),
        ):
            report = run_check()
            [message] = messages("oxpull.W006")
        assert message.msg == (
            "Rate limit names on backends 'billing' ('stripe' and 'stripé'), "
            "'reports' ('Stripe') and 'sms' ('STRIPE') differ only by case or "
            "accents. A limit's window is keyed on its name, and the database's "
            "collation decides which spellings match. On MySQL's default collation, "
            "which ignores case and accents, these spellings are one name, so the "
            "workers for backend 'billing' refuse them before claiming any task. "
            "Backends 'reports' and 'sms' count each other's admissions. On "
            "PostgreSQL and SQLite, and on a binary MySQL collation such as "
            "utf8mb4_bin, they are two separate limits."
        )
        assert report.count("oxpull.W006") == 1

    def test_one_backend_with_two_spellings_beside_one_other_counts_with_none(self):
        with backends(
            billing={**named("stripe", 5, 60), **named("stripé", 5, 60)},
            reports=named("Stripe", 5, 60),
        ):
            run_check()
            [message] = messages("oxpull.W006")
        assert message.msg.startswith(
            "Rate limit names on backends 'billing' ('stripe' and 'stripé') and "
            "'reports' ('Stripe') differ only by case or accents."
        )
        assert "the workers for backend 'billing' refuse them" in message.msg
        assert "count each other's admissions" not in message.msg

    @pytest.mark.parametrize(
        ("billing", "reports", "sms", "shown"),
        [
            (
                "naïve",
                "NAIVE",
                "nai\u0308ve",
                (
                    "'billing' ('na\\u00efve'), 'reports' ('NAIVE') and "
                    "'sms' ('nai\\u0308ve')"
                ),
            ),
            (
                "\u212aey",
                "Key",
                "KEY",
                "'billing' ('\\u212aey'), 'reports' ('Key') and 'sms' ('KEY')",
            ),
            (
                # A character above U+FFFF is written with its eight-digit escape.
                "caf\u00e9\U0001d42c",
                "CAFE\U0001d42c",
                "cafe\u0301\U0001d42c",
                (
                    "'billing' ('caf\\u00e9\\U0001d42c'), 'reports' ('CAFE\U0001d42c') "
                    "and 'sms' ('cafe\\u0301\\U0001d42c')"
                ),
            ),
        ],
    )
    def test_spellings_that_print_alike_are_shown_as_code_points(
        self, billing, reports, sms, shown
    ):
        with backends(
            billing=named(billing, 5, 60),
            reports=named(reports, 5, 60),
            sms=named(sms, 5, 60),
        ):
            report = run_check()
        assert report.count("oxpull.W006") == 1
        assert shown in report

    def test_two_spellings_that_print_alike_on_one_backend_are_shown_as_code_points(
        self,
    ):
        with backends(billing={**named("café", 5, 60), **named("cafe\u0301", 5, 60)}):
            report = run_check()
        assert report.count("oxpull.W006") == 1
        assert (
            "Rate limit names 'cafe\\u0301' and 'caf\\u00e9' on backend 'billing'"
            in report
        )


class TestTheNameCheckReadsSettingsOnly:
    """
    No database access is allowed here. `check` runs with `databases=[]`,
    which leaves out the model checks that read a database feature from
    Django 6.1 on. What is left includes every task backend's check.
    """

    def test_every_disagreement_is_reported_without_the_database(self):
        with backends(
            billing={**stripe(5, 1), **named("sms", 5, 60), **named("Mail", 5, 60)},
            reports={
                **stripe(5, 60),
                **named("sms", 10, 60),
                **named("mail", 5, 60),
            },
        ):
            report = run_check(databases=[])
        assert report.count("oxpull.W005") == 1
        assert report.count("oxpull.W004") == 1
        assert report.count("oxpull.W006") == 1

    @pytest.mark.parametrize(
        ("limits_by_alias", "reported"),
        [
            # No limit named anywhere.
            ({}, set()),
            # One period written two ways, which is not reported.
            (
                {"billing": stripe(5, 60), "reports": stripe(5, timedelta(minutes=1))},
                set(),
            ),
            # Each Warning alone, which does not raise.
            ({"billing": stripe(5, 60), "reports": stripe(10, 60)}, {"oxpull.W004"}),
            ({"billing": stripe(5, 1), "reports": stripe(5, 60)}, {"oxpull.W005"}),
            (
                {"billing": named("Stripe", 5, 60), "reports": stripe(5, 60)},
                {"oxpull.W006"},
            ),
        ],
    )
    def test_a_configuration_with_no_error_is_checked_without_the_database(
        self, limits_by_alias, reported
    ):
        with backends(**limits_by_alias):
            report = run_check(databases=[])
        for check_id in ("oxpull.W004", "oxpull.W005", "oxpull.W006"):
            assert report.count(check_id) == (check_id in reported)


@pytest.mark.django_db(databases="__all__")
class TestTheWorkerStartsBesideEveryWarning:
    """
    django-ox's `ox_worker` runs every system check before it starts, as a
    Django command does unless it opts out, whichever backend it serves.
    An Error from any backend's check stops the workers of every backend.
    One here would stop task processing at upgrade, and a supervisor that
    restarts on exit would loop. So all three reports are Warnings, and
    these tests run the real command's start to prove the worker is not
    blocked by them.
    """

    @pytest.mark.parametrize("alias", ["billing", "reports", "default"])
    def test_ox_worker_starts_beside_a_different_per(self, alias):
        started = []
        with backends(billing=stripe(5, 1), reports=stripe(5, 60)):
            report = ox_worker_start(alias, started)
        assert started == [alias]
        assert report.count("oxpull.W005") == 1

    def test_ox_worker_starts_beside_a_different_limit(self):
        started = []
        with backends(billing=stripe(5, 60), reports=stripe(10, 60)):
            report = ox_worker_start("billing", started)
        assert started == ["billing"]
        assert report.count("oxpull.W004") == 1

    @pytest.mark.parametrize("alias", ["billing", "reports", "default"])
    def test_ox_worker_starts_beside_names_that_differ_only_by_case(self, alias):
        """
        On PostgreSQL and SQLite these are two limits that work as meant, so
        an Error would stop an installation that has nothing wrong with it.
        """
        started = []
        with backends(billing=named("Stripe", 5, 60), reports=named("stripe", 5, 1)):
            report = ox_worker_start(alias, started)
        assert started == [alias]
        assert report.count("oxpull.W006") == 1

    def test_an_error_on_another_backend_still_stops_it(self):
        """
        The control. It shows the start above runs the system checks, so a
        worker that stopped running them could not pass those tests.
        """
        started = []
        with (
            backends(billing=stripe(5, 1), broken=stripe(5, 0)),
            pytest.raises(SystemCheckError, match=r"oxpull\.E005"),
        ):
            ox_worker_start("billing", started)
        assert started == []
