"""
The options behind per-task policy: MAX_ATTEMPTS, the worker's fallback
backoff, and the attempt ceiling an operator retry has to respect.

Nothing here declares a policy, so these run against any release: they pin
what the options accept and where a bad one is refused.
"""

import os
import subprocess
import sys

import pytest
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.db import connections, router

from django_ox import actions
from django_ox import backend as backend_module
from django_ox.bulk import enqueue_many
from django_ox.compat import default_task_backend, task_backends
from django_ox.models import OxTask
from django_ox.timeouts import MAX_SECONDS
from django_ox.worker import Worker

from .tasks import add, fail_always

#: The most attempts a row can hold: a PositiveSmallIntegerField.
CEILING = 32767


def tasks_setting(**options):
    return {
        "default": {
            "BACKEND": "django_ox.backend.OxBackend",
            "QUEUES": ["default", "emails"],
            "OPTIONS": {"MAX_ATTEMPTS": 3, **options},
        }
    }


#: Values that never worked on any database: 1.4.0 could not build the backend
#: from the first four, and no column stores the negative ones.
NEVER_WORKED = [None, "abc", "3.5", float("inf"), -1, "-1", -1.5]

#: Values 1.4.0 converted with int() and ran, whatever the database, with the
#: budget each still gives a row.
CONVERTED = [
    ("3", 3),
    (" 7 ", 7),
    (3.0, 3),
    (3.7, 3),
    (True, 1),
    (False, 0),
    (0, 0),
    ("0", 0),
    (0.5, 0),
    (-0.5, 0),
]

#: A budget above CEILING, per database vendor: W004 where 1.4.0 stored it,
#: E011 where every INSERT failed. PostgreSQL's column is a signed smallint,
#: MySQL's a smallint UNSIGNED, SQLite's an INTEGER sqlite3 passes up to
#: 2**63 - 1; a vendor with no known ceiling is deprecated, not refused.
ABOVE_THE_CEILING = [
    ("postgresql", CEILING + 1, "django_ox.E011"),
    ("postgresql", 65535, "django_ox.E011"),
    ("postgresql", "40000", "django_ox.E011"),
    ("mysql", CEILING + 1, "django_ox.W004"),
    ("mysql", 65535, "django_ox.W004"),
    ("mysql", "40000", "django_ox.W004"),
    ("mysql", 65536, "django_ox.E011"),
    ("sqlite", CEILING + 1, "django_ox.W004"),
    ("sqlite", 65536, "django_ox.W004"),
    ("sqlite", 2**63 - 1, "django_ox.W004"),
    ("sqlite", 2**63, "django_ox.E011"),
    ("oracle", 2**63, "django_ox.W004"),
]


def written_to():
    """The connection OxTask rows are written through."""
    return connections[router.db_for_write(OxTask)]


def run_until_idle(worker, limit=10):
    for _ in range(limit):
        if not worker.run_once():
            return


@pytest.mark.django_db
class TestMaxAttemptsIsValidated:
    """
    OPTIONS['MAX_ATTEMPTS'] in three buckets: valid, deprecated (W004, still
    converted with int() as 1.4.0 did) and refused (E011, on every enqueue
    and at worker startup). A value is refused only if it never worked.
    """

    @pytest.mark.parametrize("value", [1, 3, CEILING])
    def test_a_valid_value_raises_nothing(self, settings, value):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=value)
        assert default_task_backend.check() == []
        assert default_task_backend.max_attempts == value

    def test_a_plain_valid_configuration_adds_no_check_message(self):
        assert default_task_backend.check() == []

    @pytest.mark.parametrize("value", NEVER_WORKED, ids=repr)
    def test_a_value_that_never_worked_is_e011_and_refused_on_use(
        self, settings, value
    ):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=value)
        backend = task_backends["default"]
        errors = backend.check()
        assert [e.id for e in errors] == ["django_ox.E011"]
        assert errors[0].msg.startswith(
            "OPTIONS['MAX_ATTEMPTS'] must be an integer from 1 to 32767 that is "
            f"not a bool, got {value!r}"
        )
        with pytest.raises(ImproperlyConfigured, match="MAX_ATTEMPTS"):
            add.enqueue(1, 2)
        with pytest.raises(ImproperlyConfigured, match="MAX_ATTEMPTS"):
            Worker()
        assert not OxTask.objects.exists()

    @pytest.mark.parametrize(("value", "budget"), CONVERTED, ids=repr)
    @pytest.mark.parametrize("vendor", ["postgresql", "mysql", "sqlite"])
    def test_a_value_1_4_0_converted_is_w004_on_every_database(
        self, settings, monkeypatch, vendor, value, budget
    ):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=value)
        backend = task_backends["default"]
        monkeypatch.setattr(written_to(), "vendor", vendor)
        (warning,) = backend.check()
        assert warning.id == "django_ox.W004"
        assert warning.level == checks.WARNING
        assert warning.msg.startswith(f"OPTIONS['MAX_ATTEMPTS'] is {value!r}")
        assert warning.msg.endswith(
            "This is deprecated, and a future major release will refuse it: set "
            "MAX_ATTEMPTS to an integer from 1 to 32767 that is not a bool."
        )
        if budget == 0:
            assert "A budget of 0 gives each task one attempt, as 1 does." in (
                warning.msg
            )
        assert warning.hint == (
            f"Until then it works as it did before: each row gets a budget of {budget}."
        )
        assert backend.max_attempts == budget

    @pytest.mark.parametrize(
        ("vendor", "value", "outcome"), ABOVE_THE_CEILING, ids=repr
    )
    def test_above_32767_depends_on_what_the_database_stored(
        self, settings, monkeypatch, vendor, value, outcome
    ):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=value)
        backend = task_backends["default"]
        monkeypatch.setattr(written_to(), "vendor", vendor)
        (message,) = backend.check()
        assert message.id == outcome
        if outcome == "django_ox.E011":
            ceiling = {"postgresql": 32767, "mysql": 65535, "sqlite": 2**63 - 1}
            assert (
                f"The max_attempts column holds at most {ceiling[vendor]} on "
                in message.msg
            )
            with pytest.raises(ImproperlyConfigured, match="MAX_ATTEMPTS"):
                _ = backend.max_attempts
        else:
            assert (
                "A budget above 32767 is more than the max_attempts column "
                "holds on PostgreSQL." in message.msg
            )
            assert backend.max_attempts == int(value)

    def test_the_vendor_is_the_one_the_rows_are_written_to(self, settings, monkeypatch):
        class ToAlt:
            def db_for_write(self, model, **hints):
                return "alt" if model._meta.app_label == "django_ox" else None

        settings.TASKS = tasks_setting(MAX_ATTEMPTS=CEILING + 1)
        settings.DATABASE_ROUTERS = [ToAlt()]
        monkeypatch.setattr(connections["default"], "vendor", "sqlite")
        monkeypatch.setattr(connections["alt"], "vendor", "postgresql")
        assert [m.id for m in task_backends["default"].check()] == ["django_ox.E011"]
        monkeypatch.setattr(connections["default"], "vendor", "postgresql")
        monkeypatch.setattr(connections["alt"], "vendor", "sqlite")
        assert [m.id for m in task_backends["default"].check()] == ["django_ox.W004"]

    def test_a_valid_value_never_asks_the_router(self, settings, monkeypatch):
        def refuse(model, **hints):
            raise AssertionError("the router was asked")

        settings.TASKS = tasks_setting(MAX_ATTEMPTS=3)
        backend = task_backends["default"]
        monkeypatch.setattr(backend_module.router, "db_for_write", refuse)
        assert backend.max_attempts == 3

    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize(
        ("value", "budget", "failing_attempts"),
        [("3", 3, 3), (3.7, 3, 3), (True, 1, 1), (0, 0, 1), ("0", 0, 1)],
        ids=repr,
    )
    def test_a_deprecated_value_still_runs_as_it_did_on_1_4_0(
        self, settings, value, budget, failing_attempts
    ):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=value)
        worker = Worker(backoff_initial=0, backoff_max=0, poll_interval=0.01)
        succeeds = add.enqueue(1, 2)
        fails = fail_always.enqueue()
        results = enqueue_many(add, [((1, 1), {}), ((2, 2), {})])
        stored = OxTask.objects.filter(id__in=[r.id for r in results])
        assert list(stored.values_list("max_attempts", flat=True)) == [budget, budget]

        run_until_idle(worker)

        ok, failed = OxTask.objects.get(id=succeeds.id), OxTask.objects.get(id=fails.id)
        assert (ok.status, ok.attempts, ok.max_attempts) == ("SUCCESSFUL", 1, budget)
        assert (failed.status, failed.attempts, failed.max_attempts) == (
            "FAILED",
            failing_attempts,
            budget,
        )
        assert len(failed.errors) == failing_attempts

    def test_above_32767_is_stored_where_1_4_0_stored_it_and_refused_elsewhere(
        self, settings
    ):
        settings.TASKS = tasks_setting(MAX_ATTEMPTS=40000)
        vendor = written_to().vendor
        if vendor == "postgresql":
            with pytest.raises(
                ImproperlyConfigured, match="at most 32767 on PostgreSQL"
            ):
                add.enqueue(1, 2)
            assert not OxTask.objects.exists()
        else:
            result = add.enqueue(1, 2)
            assert OxTask.objects.get(id=result.id).max_attempts == 40000

    @pytest.mark.parametrize(
        ("options", "flags", "message"),
        [
            ('{"MAX_ATTEMPTS": -1}', [], "django_ox.E011"),
            ('{"MAX_ATTEMPTS": -1}', ["--skip-checks"], "MAX_ATTEMPTS"),
            ('{"MAX_ATTEMPTS": "three"}', ["--skip-checks"], "MAX_ATTEMPTS"),
            ('{"BACKOFF_INITIAL": -1}', ["--skip-checks"], "BACKOFF_INITIAL"),
            ('{"BACKOFF_MAX": 0}', ["--skip-checks"], "BACKOFF_MAX"),
        ],
    )
    def test_ox_worker_refuses_to_start_with_or_without_checks(
        self, options, flags, message
    ):
        completed = ox_worker(options, *flags)
        assert completed.returncode == 1, completed.stdout + completed.stderr
        assert message in completed.stderr, completed.stderr

    @pytest.mark.parametrize("value", ['"3"', "0", "2.5", "true"])
    def test_ox_worker_starts_with_a_deprecated_value_and_says_so(self, value):
        completed = ox_worker(f'{{"MAX_ATTEMPTS": {value}}}')
        assert completed.returncode == 0, completed.stdout + completed.stderr
        assert "django_ox.W004" in completed.stderr, completed.stderr
        assert "a future major release will refuse it" in completed.stderr

    @pytest.mark.parametrize(
        ("value", "returncode", "check_id"),
        [("-1", 1, "django_ox.E011"), ("2.5", 0, "django_ox.W004")],
    )
    def test_manage_py_check_reports_it_in_a_plain_project(
        self, value, returncode, check_id
    ):
        env = dict(os.environ)
        env["DJANGO_SETTINGS_MODULE"] = "tests.settings_plain"
        env["OX_TEST_TASKS_OPTIONS"] = f'{{"MAX_ATTEMPTS": {value}}}'
        completed = subprocess.run(
            [sys.executable, "-m", "django", "check"],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert completed.returncode == returncode, completed.stdout + completed.stderr
        assert check_id in completed.stderr


def ox_worker(options, *flags):
    """`manage.py ox_worker --batch` on this test database with extra OPTIONS."""
    from django.conf import settings
    from django.db import connection

    env = dict(os.environ)
    env["DJANGO_SETTINGS_MODULE"] = settings.SETTINGS_MODULE
    env["OX_TEST_TASKS_OPTIONS"] = options
    env["OX_TEST_DB_NAME"] = str(connection.settings_dict["NAME"])
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "django", "ox_worker", "--batch", *flags],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.django_db
class TestTheWorkersFallbackBackoffIsValidated:
    @pytest.mark.parametrize("value", [0, 0.0, 0.5, 5, MAX_SECONDS])
    def test_a_constructor_override_may_be_zero(self, value):
        assert Worker(backoff_initial=value).backoff_initial == value
        assert Worker(backoff_max=value).backoff_max == value

    @pytest.mark.parametrize(
        "value", [-1, -0.5, True, float("nan"), float("inf"), MAX_SECONDS * 2, "5"]
    )
    def test_a_constructor_override_that_is_not_a_delay_is_refused(self, value):
        with pytest.raises(ImproperlyConfigured, match="backoff_initial"):
            Worker(backoff_initial=value)
        with pytest.raises(ImproperlyConfigured, match="backoff_max"):
            Worker(backoff_max=value)

    @pytest.mark.parametrize("name", ["BACKOFF_INITIAL", "BACKOFF_MAX"])
    @pytest.mark.parametrize("value", [0, -1, True, "5", float("inf")])
    def test_the_options_keep_their_documented_validation(self, settings, name, value):
        settings.TASKS = tasks_setting(**{name: value})
        with pytest.raises(ImproperlyConfigured, match=name):
            Worker()

    def test_an_invalid_option_the_constructor_overrides_is_not_read(self, settings):
        settings.TASKS = tasks_setting(BACKOFF_INITIAL=0)
        assert Worker(backoff_initial=0).backoff_initial == 0


@pytest.mark.django_db
class TestAnOperatorRetryRespectsTheCeiling:
    def test_a_retry_that_would_overflow_the_budget_is_refused(self):
        result = fail_always.enqueue()
        OxTask.objects.filter(id=result.id).update(
            status=OxTask.Status.FAILED,
            attempts=CEILING,
            max_attempts=CEILING,
            lease_epoch=7,
        )
        before = OxTask.objects.filter(id=result.id).values().get()

        assert actions.retry(result.id) is False
        assert actions.retry_many([result.id]) == (0, 1)

        assert OxTask.objects.filter(id=result.id).values().get() == before

    def test_one_below_the_ceiling_still_fits(self):
        result = fail_always.enqueue()
        OxTask.objects.filter(id=result.id).update(
            status=OxTask.Status.FAILED, attempts=CEILING - 1
        )
        assert actions.retry(result.id) is True
        stored = OxTask.objects.get(id=result.id)
        assert (stored.status, stored.max_attempts) == ("READY", CEILING)

    def test_retry_many_moves_the_rows_that_fit_and_skips_the_one_that_does_not(
        self,
    ):
        full = fail_always.enqueue()
        fits = fail_always.enqueue()
        OxTask.objects.filter(id=full.id).update(
            status=OxTask.Status.FAILED, attempts=CEILING, max_attempts=CEILING
        )
        OxTask.objects.filter(id=fits.id).update(
            status=OxTask.Status.FAILED, attempts=3, max_attempts=3
        )
        assert actions.retry_many([full.id, fits.id]) == (1, 1)
        assert OxTask.objects.get(id=full.id).status == OxTask.Status.FAILED
        moved = OxTask.objects.get(id=fits.id)
        assert (moved.status, moved.max_attempts) == ("READY", 4)
