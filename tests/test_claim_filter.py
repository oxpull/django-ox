import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Q

from django_ox.models import OxTask
from django_ox.worker import POSTGRES_CLAIM_SQL, Worker, worker_class

from .tasks import add, echo, send_email

# The statement this package emits, spelled out rather than derived from the
# template: a test that formatted the template twice would agree with any stray
# newline the template grew. Written line by line because the condition line
# renders as bare indentation when nothing filters, and an editor would strip
# it.
#
# Editing this fixture is how a change to the claim statement is declared
# deliberate. The test exists to catch the other kind.
EXPECTED_CLAIM_SQL_NO_QUEUES = (
    "\n"
    'UPDATE "ox_task" SET\n'
    '    "status" = %(running)s,\n'
    '    "locked_by" = %(worker_id)s,\n'
    '    "locked_at" = STATEMENT_TIMESTAMP(),\n'
    '    "lease_expires_at" = STATEMENT_TIMESTAMP() + %(lease_ttl)s,\n'
    '    "lease_epoch" = "lease_epoch" + 1,\n'
    '    "attempts" = "attempts" + 1,\n'
    '    "started_at" = COALESCE("started_at", STATEMENT_TIMESTAMP()),\n'
    '    "last_attempted_at" = STATEMENT_TIMESTAMP(),\n'
    '    "worker_ids" = "worker_ids" || %(worker_id_json)s::jsonb\n'
    'WHERE "id" = (\n'
    '    SELECT "id" FROM "ox_task"\n'
    '    WHERE "status" = %(ready)s\n'
    '        AND ("run_after" IS NULL OR "run_after" <= %(now)s)\n'
    "        \n"
    '    ORDER BY "priority" DESC, "enqueued_at"\n'
    "    FOR UPDATE SKIP LOCKED\n"
    "    LIMIT 1\n"
    ")\n"
    "RETURNING *\n"
)

EXPECTED_CLAIM_SQL_WITH_QUEUES = EXPECTED_CLAIM_SQL_NO_QUEUES.replace(
    "        \n", '        AND "queue_name" = ANY(%(queues)s)\n'
)

BLOCKED_PATH = "tests.tasks.add"


class BlockAdd(Worker):
    """A worker that declines one task path, on every claim path."""

    def claim_filter_q(self):
        return ~Q(task_path=BLOCKED_PATH)

    def claim_filter_sql(self):
        return ' AND "task_path" <> ALL(%(blocked)s)', {"blocked": [BLOCKED_PATH]}


class TestRenderedClaimSql:
    def test_the_statement_renders_byte_for_byte(self):
        assert (
            POSTGRES_CLAIM_SQL.format(
                lease_clock="STATEMENT_TIMESTAMP()",
                table="ox_task",
                queue_clause="",
                extra_clause="",
            )
            == EXPECTED_CLAIM_SQL_NO_QUEUES
        )
        assert (
            POSTGRES_CLAIM_SQL.format(
                lease_clock="STATEMENT_TIMESTAMP()",
                table="ox_task",
                queue_clause='AND "queue_name" = ANY(%(queues)s)',
                extra_clause="",
            )
            == EXPECTED_CLAIM_SQL_WITH_QUEUES
        )

    def test_fragment_lands_inside_the_candidate_select(self):
        sql = POSTGRES_CLAIM_SQL.format(
            lease_clock="STATEMENT_TIMESTAMP()",
            table="ox_task",
            queue_clause="",
            extra_clause=' AND "task_path" <> ALL(%(blocked)s)',
        )
        # Ahead of the ordering and the limit, so the candidate is picked
        # from rows the fragment already accepts. Outside the subselect the
        # statement would choose a declined head row and then discard it,
        # claiming nothing while runnable work waited behind it.
        assert sql.index("<> ALL") < sql.index("ORDER BY")


@pytest.mark.django_db
class TestClaimFilter:
    def test_base_worker_claims_everything(self, worker):
        add.enqueue(1, 2)
        assert worker.claim_one() is not None

    def test_filter_declines_a_path_and_claims_the_rest(self):
        add.enqueue(1, 2)
        echo.enqueue("kept")
        blocking = BlockAdd(backoff_initial=0)

        claimed = blocking.claim_one()

        assert claimed is not None
        assert claimed.task_path == "tests.tasks.echo"
        assert blocking.claim_one() is None
        # A declined row is never claimed, so it spends no attempt.
        assert OxTask.objects.get(task_path=BLOCKED_PATH).attempts == 0

    def test_filter_does_not_block_the_head_of_the_queue(self):
        # The declined rows sort first and there are more of them than the
        # optimistic path fetches per pass, so a filter applied after the
        # candidates are chosen would claim nothing here.
        for _ in range(10):
            add.using(priority=10).enqueue(1, 2)
        send_email.using(priority=0).enqueue("someone@example.com")

        claimed = BlockAdd(backoff_initial=0).claim_one()

        assert claimed is not None
        assert claimed.task_path == "tests.tasks.send_email"


class TestWorkerClass:
    def test_defaults_to_worker(self):
        assert worker_class() is Worker

    def test_resolves_the_configured_path(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "OPTIONS": {"WORKER_CLASS": "tests.test_claim_filter.BlockAdd"},
            }
        }
        assert worker_class() is BlockAdd

    def test_refuses_a_class_that_is_not_a_worker(self, settings):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "OPTIONS": {"WORKER_CLASS": "django_ox.models.OxTask"},
            }
        }
        with pytest.raises(ImproperlyConfigured, match="is not a"):
            worker_class()


class _QOnly(Worker):
    """A subclass that narrows what it may claim, the queryset hook only."""

    def claim_filter_q(self):
        return Q(queue_name="allowed")


class _Both(_QOnly):
    """The same exclusion, told to both claim paths."""

    def claim_filter_sql(self):
        return 'AND "queue_name" = %(only_queue)s', {"only_queue": "allowed"}


@pytest.mark.django_db
class TestAFilterThatReachesOnlyOneClaimPath:
    """
    The single-statement PostgreSQL claim builds its own SQL, so it reads
    `claim_filter_sql()` and cannot see `claim_filter_q()`. A subclass that
    overrides the queryset hook alone would narrow SQLite and MySQL and claim
    the excluded rows on PostgreSQL, with nothing raised and nothing logged.
    """

    def _settings(self, settings, worker_class_path):
        settings.TASKS = {
            "default": {
                "BACKEND": "django_ox.backend.OxBackend",
                "QUEUES": ["default", "allowed"],
                "OPTIONS": {"WORKER_CLASS": worker_class_path},
            }
        }

    def test_the_exclusion_holds_on_every_database(self, settings):
        self._settings(settings, f"{__name__}._QOnly")
        add.using(queue_name="default").enqueue(1, 2)
        worker = _QOnly(backoff_initial=0)
        assert worker.claim_one() is None, (
            "a row the subclass excluded was claimed anyway; on PostgreSQL "
            "the fast path never saw the filter"
        )

    def test_an_allowed_row_is_still_claimed(self, settings):
        self._settings(settings, f"{__name__}._QOnly")
        add.using(queue_name="allowed").enqueue(1, 2)
        worker = _QOnly(backoff_initial=0)
        claimed = worker.claim_one()
        assert claimed is not None, "the filter excluded a row it allows"
        assert claimed.queue_name == "allowed"

    def test_it_says_why_it_gave_up_the_fast_path(self, settings, caplog):
        import logging

        self._settings(settings, f"{__name__}._QOnly")
        worker = _QOnly(backoff_initial=0)
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            # Asked directly: the check only runs on the PostgreSQL fast path,
            # so going through claim_one() would say nothing on SQLite and
            # this is about the notice being said once, not about the vendor.
            worker._postgresql_honours_the_claim_filter()
            worker._postgresql_honours_the_claim_filter()
        said = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "claim_filter_sql_missing"
        ]
        assert len(said) == 1, f"said it {len(said)} times; once per worker"

    def test_a_subclass_that_implements_both_keeps_the_fast_path(self, settings):
        self._settings(settings, f"{__name__}._Both")
        worker = _Both(backoff_initial=0)
        assert worker._postgresql_honours_the_claim_filter()

    def test_a_worker_with_no_filter_keeps_the_fast_path(self, settings):
        self._settings(settings, f"{__name__}._QOnly")
        plain = Worker(backoff_initial=0)
        assert plain._postgresql_honours_the_claim_filter()


class TestTheLeaseClockInTheRenderedSql:
    """
    One clock stamps the lease, and this statement has to agree with
    `_lease_now()`. With USE_TZ on both are the database's. With it off
    `_lease_now()` is the worker's clock, so hard coding the server's here
    would put two clocks on one column: a worker whose clock ran behind the
    server would renew to a timestamp the reaper already read as expired.
    """

    def test_with_time_zone_support_the_server_stamps_it(self, settings):
        settings.USE_TZ = True
        sql = POSTGRES_CLAIM_SQL.format(
            lease_clock="STATEMENT_TIMESTAMP()",
            table="ox_task",
            queue_clause="",
            extra_clause="",
        )
        assert '"locked_at" = STATEMENT_TIMESTAMP()' in sql

    def test_without_it_the_worker_stamps_it(self):
        sql = POSTGRES_CLAIM_SQL.format(
            lease_clock="%(lease_now)s",
            table="ox_task",
            queue_clause="",
            extra_clause="",
        )
        assert '"locked_at" = %(lease_now)s' in sql
        assert "STATEMENT_TIMESTAMP()" not in sql, (
            "one of the claim's three timestamps still comes from the server "
            "while the renewal uses the worker's clock"
        )
