"""
A released workflow node must not read as backlog for the whole time it
was blocked.

django-ox measures a waiting task's age from the moment it became
eligible: Coalesce(run_after, enqueued_at) in stats.oldest_ready_age,
the django_ox_oldest_ready_age_seconds gauge, and ox_health --max-age.
Its own re-eligibility writer, the retry path, writes run_after=now+delay
so the clock restarts. A blocked node used to carry a far-future run_after,
and its release wrote run_after=NULL, so its age fell back to enqueued_at:
the moment add() ran, before the upstream chain executed.

A blocked node now waits in django-ox's WAITING status, which no backlog
reading counts, and the release writes run_after as the later of now and
the time the caller asked for, through the worker's release and through
the reconciler's backstop alike.

Every test asserts that a node that just became claimable reads as just
eligible. The first test is the control, django-ox's own retry.
"""

import re
import time
from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import DatabaseError
from django.db.models.functions import Coalesce
from django.tasks import task
from django.utils import timezone

from django_ox import metrics, stats
from django_ox.models import OxTask
from oxpull import workflows

from . import tasks
from .test_workflows_deferred import deferring_worker

pytestmark = pytest.mark.django_db

# How long the upstream chain took, standing in for a long extract.
BLOCKED_FOR = timedelta(hours=2)
# How far apart the database server's clock and the worker's may be.
CLOCK_SKEW = timedelta(seconds=1)
# An ox_health threshold an operator pages on (docs/monitoring.md's cron
# example uses 600); far above any real claim latency in these tests.
MAX_AGE_SECONDS = 300


@task(backend="flows_deferred")
def slow_deferred_step(label, seconds):
    time.sleep(seconds)
    return tasks._record(label)


def order():
    return list(tasks.STATE.get("order", []))


def eligible_since(task_id):
    """The instant django-ox's age readings measure this row from."""
    return (
        OxTask.objects.filter(pk=task_id)
        .annotate(since=Coalesce("run_after", "enqueued_at"))
        .values_list("since", flat=True)
        .get()
    )


def gauge(queue="default"):
    """django_ox_oldest_ready_age_seconds for a queue, as a scrape renders it."""
    match = re.search(
        rf'^django_ox_oldest_ready_age_seconds\{{queue="{queue}"\}} (\S+)$',
        metrics.render_prometheus(),
        re.MULTILINE,
    )
    return None if match is None else float(match.group(1))


def health(max_age):
    """None when ox_health --max-age exits 0, else the reason it gave."""
    try:
        call_command("ox_health", "--max-age", str(max_age), stdout=StringIO())
    except CommandError as exc:
        return str(exc)
    return None


def backlog_problems(max_age=MAX_AGE_SECONDS):
    """Every age reading an operator alerts on, and which ones fire."""
    age = stats.oldest_ready_age()
    scraped = gauge()
    reason = health(max_age)
    problems = []
    if age is None or age.total_seconds() > max_age:
        problems.append(f"stats.oldest_ready_age() = {age}")
    if scraped is None or scraped > max_age:
        problems.append(f"django_ox_oldest_ready_age_seconds = {scraped}")
    if reason is not None:
        problems.append(f"ox_health --max-age {max_age} failed: {reason}")
    return problems


def released_problems(child_id, upstream):
    """backlog_problems, plus the eligibility instant against the upstream."""
    problems = []
    since = eligible_since(child_id)
    if since < upstream.finished_at:
        problems.append(
            f"child eligible since {since.isoformat()}, "
            f"{upstream.finished_at - since} before its only upstream "
            f"finished at {upstream.finished_at.isoformat()}"
        )
    return problems + backlog_problems()


def backdate_every_row(by=BLOCKED_FOR):
    OxTask.objects.update(enqueued_at=timezone.now() - by)


def deferred_chain(name):
    flow = workflows.create(name)
    root = flow.add("root", tasks.deferred_step, "root")
    child = flow.add("child", tasks.deferred_step, "child", depends_on="root")
    flow.seal()
    assert OxTask.objects.get(pk=child.id).status == OxTask.Status.WAITING
    return root, child


# -- control -----------------------------------------------------------


def test_control_a_retry_restarts_the_age_clock(worker):
    """
    django-ox's own contract, through its own writer: a task that failed
    and went back to READY is measured from its retry, not its enqueue.
    Passes today; it is what the released node should match.
    """
    result = tasks.plain_flaky.enqueue(2)
    backdate_every_row()
    assert worker.run_once() is True
    row = OxTask.objects.get(pk=result.id)
    assert row.status == OxTask.Status.READY and row.run_after is not None
    # last_attempted_at is written by the claim that ran the failed attempt,
    # on the database's clock; run_after by the retry, on the worker's. A
    # second either way is still two hours clear of the backdated enqueue.
    assert eligible_since(row.pk) >= row.last_attempted_at - CLOCK_SKEW
    assert backlog_problems() == []


# -- the hazard --------------------------------------------------------


def test_a_node_released_by_its_upstreams_worker_reads_as_just_eligible():
    """
    The eager path: OxpullWorker._write_outcome writes the root
    SUCCESSFUL and releases the child in the same pass. The child became
    claimable at that moment.
    """
    root, child = deferred_chain("eager")
    backdate_every_row()
    assert deferring_worker().run_once() is True
    root_row = OxTask.objects.get(pk=root.id)
    child_row = OxTask.objects.get(pk=child.id)
    assert root_row.status == OxTask.Status.SUCCESSFUL
    assert child_row.status == OxTask.Status.READY, "the release did not run"
    assert stats.ready_count() == 1

    problems = released_problems(child.id, root_row)
    assert problems == [], "\n".join(problems)


def test_a_node_released_by_the_reconciler_backstop_reads_as_just_eligible(
    monkeypatch, worker
):
    """
    The backstop path. The worker's release fails after the outcome
    landed (OxpullWorker._write_outcome logs a DatabaseError and returns
    the outcome, as it does for a lock wait timeout or a dropped
    connection), so the child waits until the scheduled reconciler task
    runs on a real worker and releases it.
    """
    root, child = deferred_chain("backstop")
    backdate_every_row()

    def release_fails(task_ids, db_alias):
        raise DatabaseError("simulated: the release statement failed")

    with monkeypatch.context() as patch:
        patch.setattr("oxpull.worker.release_downstream", release_fails)
        assert deferring_worker().run_once() is True
    root_row = OxTask.objects.get(pk=root.id)
    assert root_row.status == OxTask.Status.SUCCESSFUL
    assert OxTask.objects.get(pk=child.id).status == OxTask.Status.WAITING

    reconciler = workflows.reconcile.enqueue()
    assert worker.run_once() is True
    assert OxTask.objects.get(pk=reconciler.id).status == OxTask.Status.SUCCESSFUL
    child_row = OxTask.objects.get(pk=child.id)
    assert child_row.status == OxTask.Status.READY, "the backstop did not run"
    assert stats.ready_count() == 1

    problems = released_problems(child.id, root_row)
    assert problems == [], "\n".join(problems)


def test_every_stage_of_a_real_time_chain_reads_as_just_eligible():
    """
    No row written by hand: extract and transform really take 1.5 s
    each, and the threshold is 1 s. Each release should read as
    milliseconds old. Today the age grows by one stage's run time per
    stage, so the page fires at every stage and gets louder down the DAG.
    """
    flow = workflows.create("etl")
    flow.add("extract", slow_deferred_step, "extract", 1.5)
    transform = flow.add(
        "transform", slow_deferred_step, "transform", 1.5, depends_on="extract"
    )
    load = flow.add("load", tasks.deferred_step, "load", depends_on="transform")
    flow.seal()

    readings = {}
    for released in (transform, load):
        assert deferring_worker().run_once() is True
        row = OxTask.objects.get(pk=released.id)
        assert row.status == OxTask.Status.READY
        readings[row.args[0]] = backlog_problems(max_age=1)
    assert order() == ["extract", "transform"]
    assert readings == {"transform": [], "load": []}, "\n".join(
        f"{stage}: {problems}" for stage, problems in readings.items()
    )
