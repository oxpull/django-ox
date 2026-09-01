import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Q

from django_ox.models import OxTask
from django_ox.worker import POSTGRES_CLAIM_SQL, Worker, worker_class

from .tasks import add, echo, send_email

# What 0.3.1 emitted, spelled out rather than derived from the template: a
# test that formatted the template twice would agree with any stray newline
# the template grew. Written line by line because the condition line renders
# as bare indentation when nothing filters, and an editor would strip it.
CLAIM_SQL_0_3_1_NO_QUEUES = (
    "\n"
    'UPDATE "ox_task" SET\n'
    '    "status" = %(running)s,\n'
    '    "locked_by" = %(worker_id)s,\n'
    '    "locked_at" = STATEMENT_TIMESTAMP(),\n'
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

CLAIM_SQL_0_3_1_WITH_QUEUES = CLAIM_SQL_0_3_1_NO_QUEUES.replace(
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
    def test_empty_fragment_renders_0_3_1_byte_for_byte(self):
        assert (
            POSTGRES_CLAIM_SQL.format(table="ox_task", queue_clause="", extra_clause="")
            == CLAIM_SQL_0_3_1_NO_QUEUES
        )
        assert (
            POSTGRES_CLAIM_SQL.format(
                table="ox_task",
                queue_clause='AND "queue_name" = ANY(%(queues)s)',
                extra_clause="",
            )
            == CLAIM_SQL_0_3_1_WITH_QUEUES
        )

    def test_fragment_lands_inside_the_candidate_select(self):
        sql = POSTGRES_CLAIM_SQL.format(
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
