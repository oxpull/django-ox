"""
A read-splitting router must not make django-ox read a replica.

With the replica router from Django's own multi-database documentation,
`db_for_read` and `db_for_write` name two databases, and the replica is
reachable and behind, which is what a replica is. Every django-ox reading of
its own table then has to name the write alias, or the package answers
questions about the wrong database.

Two symptoms are what this file exists for.

`ox_prune` batches. Each batch reads the primary keys that are still
prunable, then deletes them. An unqualified read follows `db_for_read` to
the replica while the DELETE lands on the primary, so the loop's exit
condition is a replica read that never comes back empty: the primary empties
and the command keeps going.

`ox_health` reads the backlog, the oldest eligible task and the last claim.
Unqualified, all three come from the replica, and a replica that is behind
answers with a queue nobody is running. A container healthcheck then reports
green over a stuck queue, which is the worst shape this can take.

`no_statement_on` is why the prune test finishes. The unfixed loop does not
terminate, so waiting for it would hang the suite; refusing the first
statement on the read alias turns that into a failed assertion instead.
"""

import contextlib
import json
import uuid
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import OperationalError, connections
from django.utils import timezone

from django_ox import actions, metrics, stats
from django_ox.models import OxTask

# The replica is `default` and the primary is `alt`, so a statement that
# routes itself lands somewhere a pinned one does not. Both aliases are
# migrated, so a misrouted statement fails on the alias rather than on a
# table that is not there.
REPLICA = "default"
PRIMARY = "alt"


class _ReplicaReadRouter:
    """The read-replica router from Django's own multi-database docs."""

    def db_for_read(self, model, **hints):
        return REPLICA if model._meta.app_label == "django_ox" else None

    def db_for_write(self, model, **hints):
        return PRIMARY if model._meta.app_label == "django_ox" else None

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, **hints):
        return None


@contextlib.contextmanager
def no_statement_on(alias):
    """Make the first statement on `alias` a failure rather than a wait."""

    def refuse(execute, sql, params, many, context):
        raise AssertionError(f"statement issued on {alias!r}: {sql}")

    with connections[alias].execute_wrapper(refuse):
        yield


@contextlib.contextmanager
def budget_on(alias, limit):
    """Let `alias` serve `limit` statements, then fail instead of running on."""
    spent = 0

    def count(execute, sql, params, many, context):
        nonlocal spent
        spent += 1
        if spent > limit:
            raise AssertionError(
                f"{alias!r} served more than {limit} statements; the one over was {sql}"
            )
        return execute(sql, params, many, context)

    with connections[alias].execute_wrapper(count):
        yield


def _rows(n, *, status, enqueued_at, finished_at=None):
    return [
        OxTask(
            id=uuid.uuid4(),
            task_path="tests.tasks.add",
            backend_name="default",
            queue_name="default",
            enqueued_at=enqueued_at,
            status=status,
            finished_at=finished_at,
        )
        for _ in range(n)
    ]


def _copy(rows):
    """The same rows again, as fresh unsaved objects with the same keys."""
    return [
        OxTask(
            id=row.id,
            task_path=row.task_path,
            backend_name=row.backend_name,
            queue_name=row.queue_name,
            enqueued_at=row.enqueued_at,
            status=row.status,
            finished_at=row.finished_at,
        )
        for row in rows
    ]


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestUnderAReplicaThatIsBehind:
    @pytest.fixture(autouse=True)
    def _router(self, settings):
        settings.DATABASE_ROUTERS = [_ReplicaReadRouter()]

    def _prunable(self, n=20):
        """`n` finished rows on the primary, and a snapshot of them on the replica."""
        old = timezone.now() - timedelta(days=30)
        rows = _rows(
            n,
            status=OxTask.Status.SUCCESSFUL,
            enqueued_at=old,
            finished_at=old,
        )
        OxTask.objects.using(PRIMARY).bulk_create(rows)
        OxTask.objects.using(REPLICA).bulk_create(_copy(rows))
        return rows

    def _backlog(self, n=40):
        """`n` READY rows six hours old on the primary, and none on the replica."""
        rows = _rows(
            n,
            status=OxTask.Status.READY,
            enqueued_at=timezone.now() - timedelta(hours=6),
        )
        OxTask.objects.using(PRIMARY).bulk_create(rows)
        return rows

    def test_prune_terminates_and_deletes_what_it_should(self):
        self._prunable(20)
        out = StringIO()
        with no_statement_on(REPLICA):
            call_command("ox_prune", "--older-than", "1d", stdout=out)
        assert "Deleted 20 " in out.getvalue()
        assert OxTask.objects.using(PRIMARY).count() == 0
        # Nothing wrote to the replica either: it is still the snapshot.
        assert OxTask.objects.using(REPLICA).count() == 20

    def test_prune_terminates_with_a_batch_size_below_the_row_count(self):
        # More than one pass round the loop, which is where the exit
        # condition is read again.
        self._prunable(20)
        out = StringIO()
        with no_statement_on(REPLICA):
            call_command(
                "ox_prune", "--older-than", "1d", "--batch-size", "3", stdout=out
            )
        assert "Deleted 20 " in out.getvalue()
        assert OxTask.objects.using(PRIMARY).count() == 0

    def test_prune_does_not_loop_over_a_replica_that_never_empties(self):
        """
        The batch loop ends when its candidate read comes back empty, and a
        replica that is behind never does. The budget is what keeps a
        regression here from hanging the suite rather than failing it; a
        prune that reads the alias it deletes on spends none of it.
        """
        self._prunable(20)
        with budget_on(REPLICA, 100):
            call_command("ox_prune", "--older-than", "1d", "--batch-size", "3")
        assert OxTask.objects.using(PRIMARY).count() == 0

    def test_health_reports_the_primary_backlog(self):
        self._backlog(40)
        with no_statement_on(REPLICA), pytest.raises(CommandError) as caught:
            call_command("ox_health", "--max-backlog", "5")
        assert "backlog is 40" in str(caught.value)

    def test_health_json_reports_the_primary_backlog(self):
        self._backlog(40)
        out = StringIO()
        with no_statement_on(REPLICA), pytest.raises(CommandError):
            call_command(
                "ox_health", "--max-backlog", "5", "--format", "json", stdout=out
            )
        assert '"ok": false' in out.getvalue()
        assert '"backlog": 40' in out.getvalue()

    def test_health_max_age_sees_the_primary(self):
        self._backlog(1)
        with no_statement_on(REPLICA), pytest.raises(CommandError) as caught:
            call_command("ox_health", "--max-age", "60s")
        assert "over --max-age" in str(caught.value)

    def test_stats_read_the_primary(self):
        self._backlog(40)
        with no_statement_on(REPLICA):
            assert stats.ready_count() == 40
            assert stats.oldest_ready_age() is not None
            assert [entry.ready for entry in stats.queue_stats()] == [40]
            assert stats.waiting_counts() == {}
            assert stats.throughput() == 0.0
            assert stats.failure_rate() is None
            assert stats.last_claim_age() is None

    def test_the_metrics_endpoint_reads_the_primary(self):
        self._backlog(40)
        with no_statement_on(REPLICA):
            rendered = metrics.render_prometheus()
        assert 'django_ox_ready_tasks{queue="default"} 40' in rendered

    def test_the_shipped_view_reads_the_primary(self):
        from django.test import RequestFactory

        from django_ox import views

        self._backlog(40)
        with no_statement_on(REPLICA):
            response = views.metrics(RequestFactory().get("/metrics"))
        assert 'django_ox_ready_tasks{queue="default"} 40' in response.content.decode()

    def test_the_shipped_view_can_be_mounted_on_another_alias(self):
        """
        A project that served scrapes off a replica before this release has
        to be able to keep doing it, and the view is the only way in: a
        query parameter would let whoever scrapes choose the database.
        """
        from django.test import RequestFactory

        from django_ox import views

        self._backlog(40)
        response = views.metrics(RequestFactory().get("/metrics"), using=REPLICA)
        # The replica holds none of the backlog, which is the point.
        assert 'django_ox_ready_tasks{queue="default"} 40' not in (
            response.content.decode()
        )
        assert OxTask.objects.using(REPLICA).count() == 0

    def test_actions_read_the_primary(self):
        rows = _rows(
            1,
            status=OxTask.Status.FAILED,
            enqueued_at=timezone.now() - timedelta(hours=1),
            finished_at=timezone.now() - timedelta(hours=1),
        )
        OxTask.objects.using(PRIMARY).bulk_create(rows)
        with no_statement_on(REPLICA):
            assert actions.retry(rows[0].pk) is True
            assert actions.discard(rows[0].pk) is True
            assert actions.retry_many([rows[0].pk]) == (0, 1)
        assert (
            OxTask.objects.using(PRIMARY).get(pk=rows[0].pk).status
            == OxTask.Status.DISCARDED
        )

    def test_a_result_is_read_back_from_the_primary(self):
        from django.tasks import task_backends

        backend = task_backends["default"]
        rows = _rows(
            1,
            status=OxTask.Status.READY,
            enqueued_at=timezone.now(),
        )
        OxTask.objects.using(PRIMARY).bulk_create(rows)
        with no_statement_on(REPLICA):
            assert backend.get_result(str(rows[0].pk)).id == str(rows[0].pk)


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheDatabaseFlag:
    @pytest.fixture(autouse=True)
    def _router(self, settings):
        settings.DATABASE_ROUTERS = [_ReplicaReadRouter()]

    def test_prune_works_on_the_alias_it_is_given(self):
        old = timezone.now() - timedelta(days=30)
        OxTask.objects.using(REPLICA).bulk_create(
            _rows(
                4,
                status=OxTask.Status.SUCCESSFUL,
                enqueued_at=old,
                finished_at=old,
            )
        )
        out = StringIO()
        call_command(
            "ox_prune", "--older-than", "1d", "--database", REPLICA, stdout=out
        )
        assert "Deleted 4 " in out.getvalue()
        assert OxTask.objects.using(REPLICA).count() == 0

    def test_health_checks_the_alias_it_is_given(self):
        OxTask.objects.using(REPLICA).bulk_create(
            _rows(
                7,
                status=OxTask.Status.READY,
                enqueued_at=timezone.now() - timedelta(hours=6),
            )
        )
        with pytest.raises(CommandError) as caught:
            call_command("ox_health", "--max-backlog", "1", "--database", REPLICA)
        assert "backlog is 7" in str(caught.value)

    def test_the_worker_runs_on_the_alias_it_is_given(self, monkeypatch):
        from django_ox.management.commands import ox_worker

        seen = {}

        class _Stub:
            recycling = False

            def __init__(self, **kwargs):
                seen.update(kwargs)

            def run(self):
                return None

            def request_stop(self):
                return None

        monkeypatch.setattr(ox_worker, "worker_class", lambda alias: _Stub)
        with pytest.raises(SystemExit):
            call_command("ox_worker", "--database", REPLICA)
        assert seen["db_alias"] == REPLICA

    def test_the_worker_names_the_alias_for_each_child(self):
        from django_ox.management.commands.ox_worker import worker_args

        options = {
            "backend": "default",
            "concurrency": 1,
            "interval": 1.0,
            "verbosity": 1,
            "queues": None,
            "lock_timeout": None,
        }
        args = worker_args(options, PRIMARY)
        assert args[args.index("--database") + 1] == PRIMARY

    @pytest.mark.parametrize("name", ["ox_prune", "ox_health", "ox_worker"])
    def test_an_unknown_alias_is_a_sentence(self, name):
        with pytest.raises(CommandError) as caught:
            call_command(name, "--database", "nowhere")
        assert "No database alias 'nowhere' in DATABASES" in str(caught.value)


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheChecksAreScopedToOneAlias:
    """
    Django 6.1 runs the system checks against every alias in DATABASES when
    a command names none, and checking a SQLite or MySQL alias opens a
    connection to it. A --database flag that leaves that alone is half a
    flag: the command still ends on an alias it was told not to touch.
    Django's own `migrate` scopes them, and so do the three commands that
    run once and report. The worker is the exception, below.
    """

    @pytest.mark.parametrize(
        "name",
        ["ox_prune", "ox_health", "ox_import_beat_schedules"],
    )
    def test_the_flag_reaches_the_checks(self, name):
        from django.core.management import load_command_class

        command = load_command_class("django_ox", name)
        kwargs = command.get_check_kwargs({"database": PRIMARY})
        assert kwargs["databases"] == [PRIMARY]

    @pytest.mark.parametrize("name", ["ox_prune", "ox_health"])
    def test_without_the_flag_the_checks_take_the_write_alias(self, name, settings):
        from django.core.management import load_command_class

        settings.DATABASE_ROUTERS = [_ReplicaReadRouter()]
        command = load_command_class("django_ox", name)
        kwargs = command.get_check_kwargs({"database": None})
        assert kwargs["databases"] == [PRIMARY]


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestTheWorkerStartsWithoutItsDatabase:
    """
    A worker has to outlive the database it works on.

    `ox_worker` already handles a database that goes away: the poll logs
    `worker_poll_failed`, drops the connection and waits, and the next pass
    reconnects. Scoping its system checks to an alias undid that, because
    JSONField's support check opens the connection before `handle()` runs,
    so a worker started while the database was down exited instead of
    waiting for it. Measured on Django 5.2, 6.0 and 6.1.

    A configuration error is a different thing and still stops it at once:
    that distinction is what these two hold together.
    """

    def test_the_worker_hands_the_checks_no_alias(self):
        from django.core.management import load_command_class

        command = load_command_class("django_ox", "ox_worker")
        assert command.get_check_kwargs({"database": None})["databases"] == []
        assert command.get_check_kwargs({"database": PRIMARY})["databases"] == []

    def test_the_worker_s_checks_do_not_open_the_database(self, monkeypatch):
        """
        The effect rather than the argument. With the alias named, the
        checks reach a database that is down and the command ends there.
        """
        from django.core import checks
        from django.core.management import load_command_class

        connection = connections[PRIMARY]
        # The support check asks once per process and remembers; a suite
        # that has already run has the answer cached.
        monkeypatch.delitem(
            connection.features.__dict__, "supports_json_field", raising=False
        )

        def unreachable():
            raise OperationalError("could not connect to server")

        monkeypatch.setattr(connection, "ensure_connection", unreachable)
        worker = load_command_class("django_ox", "ox_worker")
        prune = load_command_class("django_ox", "ox_prune")
        # No raise: nothing the worker's checks run opens a connection.
        checks.run_checks(**worker.get_check_kwargs({"database": PRIMARY}))
        with pytest.raises(OperationalError):
            checks.run_checks(**prune.get_check_kwargs({"database": PRIMARY}))

    def test_a_configuration_error_still_stops_the_worker(self, settings):
        """
        Retrying an unreachable database is not the same as retrying a
        misdeploy, and the checks that find a misdeploy still run.
        """
        from django.core.management.base import SystemCheckError

        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default"],
                "OPTIONS": {"SCHEDULE_SOURCE": "nowhere.NotAClass"},
            }
        }
        # skip_checks=False: call_command skips them by default, and the
        # checks are the whole of what this asserts.
        with pytest.raises(SystemCheckError) as caught:
            call_command("ox_worker", "--interval", "0.01", skip_checks=False)
        assert "django_ox.E006" in str(caught.value)


@pytest.mark.django_db(databases=[REPLICA, PRIMARY])
class TestADatabaseThatIsDownIsOneLine:
    """
    Scoping the checks to an alias is what opens the connection, so a
    database that is down is now found inside Django's check framework.
    Left alone that arrives as a driver traceback, in cron mail and in a
    container's probe log, where every other failure these commands have is
    one line. `ox_health --format json` is the sharpest case: its object is
    documented to carry the reason, and a healthcheck reads the object.
    """

    @pytest.fixture
    def unreachable(self, monkeypatch):
        """The write alias, refusing to connect the way a dead server does."""
        connection = connections[PRIMARY]
        # Asked once per process and remembered, so a suite that has
        # already run has the answer cached.
        monkeypatch.delitem(
            connection.features.__dict__, "supports_json_field", raising=False
        )

        def refuse():
            raise OperationalError("could not connect to server")

        monkeypatch.setattr(connection, "ensure_connection", refuse)

    @pytest.mark.parametrize("name", ["ox_prune", "ox_health"])
    def test_the_checks_report_it_as_a_sentence(self, unreachable, name):
        with pytest.raises(CommandError) as caught:
            call_command(name, "--database", PRIMARY, skip_checks=False)
        assert "Database unreachable" in str(caught.value)

    def test_health_json_still_prints_its_object(self, unreachable):
        out = StringIO()
        with pytest.raises(CommandError):
            call_command(
                "ox_health",
                "--database",
                PRIMARY,
                "--format",
                "json",
                skip_checks=False,
                stdout=out,
            )
        reported = json.loads(out.getvalue())
        assert reported["ok"] is False
        assert reported["backlog"] is None
        assert "Database unreachable" in reported["problems"][0]

    def test_the_object_is_printed_once(self, settings):
        """
        A failure inside handle() prints its own object, and the wrapper
        must not print a second one on the way out.
        """
        settings.DATABASE_ROUTERS = [_ReplicaReadRouter()]
        out = StringIO()
        with pytest.raises(CommandError):
            call_command(
                "ox_health", "--max-backlog", "-1", "--format", "json", stdout=out
            )
        assert out.getvalue().count('"ok"') == 1
