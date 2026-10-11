"""
django_ox.stats, ox_health and the /ox/metrics gauges count
workflow nodes that no worker can claim as eligible backlog.

django-ox documents ready_count() as "READY tasks eligible to run now,
mirroring the worker's dequeue predicate ... This is the backlog number",
oldest_ready_age() as measured "from when it became eligible", and tells
operators to alert on those two numbers through ox_health --max-backlog /
--max-age and the django_ox_ready_tasks / django_ox_oldest_ready_age_seconds
gauges. stats._ready() is READY and due. A workflow node whose upstream
has not succeeded used to be READY behind a dependency gate stats cannot
see. It is WAITING now, which none of these surfaces counts as backlog, and
its release writes a run_after of the moment it became eligible.

Every test asserts what the documented surface promises, read through the
real public entry points (stats functions, call_command("ox_health"), a GET
on django_ox.urls). The upstream is held RUNNING by a real OxpullWorker
executing it on its own thread, and ages come from real elapsed time, not
from backdated rows. Two controls at the bottom pass today and show the
same harness can reach the passing branch and does see real backlog.
"""

from __future__ import annotations

import io
import re
import threading
import time

import pytest
from django.core.management import CommandError, call_command
from django.db import connections
from django.db.models import Q
from django.tasks import task
from django.test import Client, override_settings
from django.urls import include, path
from django.utils import timezone

from django_ox import stats
from django_ox.models import OxTask
from oxpull import workflows
from oxpull.worker import OxpullWorker

from . import tasks
from .test_workflows import flow_worker

pytestmark = pytest.mark.django_db(transaction=True)

# django_ox.urls mounted the way docs/monitoring.md tells a project to.
urlpatterns = [path("ox/", include("django_ox.urls"))]

QUEUE = "default"
FAN_OUT = 20

# Released by the test once it has read the surfaces. A module-level Event is
# shared by the worker thread and the test because both are one process.
HOLD = threading.Event()


@task(backend="flows")
def held_extract(label):
    """An upstream that stays RUNNING until the test releases it."""
    HOLD.wait(timeout=60)
    return label


@task(backend="flows_deferred")
def held_extract_deferred(label):
    HOLD.wait(timeout=60)
    return label


NODE_TASKS = {
    "flows": (held_extract, tasks.step),
    "flows_deferred": (held_extract_deferred, tasks.deferred_step),
}


@pytest.fixture(autouse=True)
def hold_reset():
    HOLD.clear()
    yield
    HOLD.set()


def gating_worker(alias):
    if alias == "flows":
        return flow_worker()
    return OxpullWorker(backend_alias=alias, backoff_initial=0, poll_interval=0.01)


def fan_out_flow(alias, name):
    """extract -> FAN_OUT parts -> load, sealed. Returns (extract id, part ids)."""
    upstream, step = NODE_TASKS[alias]
    flow = workflows.create(name)
    extract = flow.add("extract", upstream.using(priority=0), "extract")
    parts = [f"part-{i:02d}" for i in range(FAN_OUT)]
    part_ids = [
        flow.add(part, step.using(priority=5), part, depends_on="extract").id
        for part in parts
    ]
    flow.add("load", step.using(priority=10), "load", depends_on=parts)
    flow.seal()
    return extract.id, part_ids


class RunningUpstream:
    """
    A real OxpullWorker on its own thread, claiming and executing the held
    extract through run_once(): a real lease, a real renewal thread, and the
    real outcome write, and the release that follows it, when the test lets
    it finish.
    """

    def __init__(self, alias, extract_id):
        self.alias = alias
        self.extract_id = extract_id
        self.result = []
        self.errors = []
        self.thread = threading.Thread(target=self._work)

    def _work(self):
        try:
            self.result.append(gating_worker(self.alias).run_once())
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            connections.close_all()

    def __enter__(self):
        self.thread.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if OxTask.objects.filter(
                pk=self.extract_id, status=OxTask.Status.RUNNING
            ).exists():
                return self
            time.sleep(0.02)
        HOLD.set()
        self.thread.join(timeout=30)
        raise AssertionError(f"extract never went RUNNING; errors={self.errors}")

    def finish(self):
        HOLD.set()
        self.thread.join(timeout=30)
        assert not self.thread.is_alive()
        assert self.errors == [], self.errors
        assert self.result == [True], self.result
        assert OxTask.objects.get(pk=self.extract_id).status == OxTask.Status.SUCCESSFUL

    def __exit__(self, *exc_info):
        if self.thread.is_alive() or not HOLD.is_set():
            HOLD.set()
            self.thread.join(timeout=30)
        return False


def health(*flags):
    """manage.py ox_health as a cron line or probe sees it: OK text or the failure."""
    out = io.StringIO()
    try:
        call_command("ox_health", "--queue", QUEUE, *flags, stdout=out)
    except CommandError as exc:
        return f"FAIL: {exc}"
    return out.getvalue().strip()


@override_settings(ROOT_URLCONF=__name__)
def scrape():
    """GET /ox/metrics through django_ox's own view, parsed for QUEUE."""
    response = Client().get("/ox/metrics")
    assert response.status_code == 200
    body = response.content.decode()
    samples = {}
    for name in ("django_ox_ready_tasks", "django_ox_oldest_ready_age_seconds"):
        match = re.search(
            rf'^{name}{{queue="{QUEUE}"}} (\S+)$', body, flags=re.MULTILINE
        )
        samples[name] = None if match is None else float(match.group(1))
    return samples


def readings(max_backlog, max_age):
    age = stats.oldest_ready_age(QUEUE)
    return {
        "ready_count": stats.ready_count(QUEUE),
        "oldest_ready_age_s": None if age is None else round(age.total_seconds(), 2),
        "ox_health": health(
            "--max-backlog", str(max_backlog), "--max-age", str(max_age)
        ),
        "metrics": scrape(),
    }


def nothing_is_claimable(alias, workers=3):
    """The autoscaler's extra workers: every gating claim comes back empty."""
    return [gating_worker(alias).claim_one() for _ in range(workers)]


# -- the hazard ----------------------------------------------------------


def test_nodes_behind_a_running_upstream_are_not_reported_as_backlog():
    """
    extract is RUNNING on a real worker. Its 20 parts and the load behind
    them wait for it, and no worker can claim any of them. After a real
    1.5 s wait, the backlog surfaces must read an empty backlog: ready_count
    0, no oldest age, ox_health OK at --max-backlog 10 --max-age 1, gauge 0
    and no age sample.
    """
    extract_id, _ = fan_out_flow("flows", "backlog")
    with RunningUpstream("flows", extract_id) as upstream:
        assert nothing_is_claimable("flows") == [None, None, None]
        time.sleep(1.5)
        seen = readings(max_backlog=10, max_age=1)
        assert nothing_is_claimable("flows") == [None, None, None]
        upstream.finish()

    observed = (
        seen["ready_count"],
        seen["oldest_ready_age_s"],
        seen["ox_health"].startswith("OK: backlog=0 oldest_age=none"),
        seen["metrics"],
    )
    correct = (
        0,
        None,
        True,
        {"django_ox_ready_tasks": 0.0, "django_ox_oldest_ready_age_seconds": None},
    )
    assert observed == correct, f"no worker could claim any of these; observed {seen}"


@pytest.mark.parametrize("alias", ["flows", "flows_deferred"])
def test_a_released_node_ages_from_when_it_became_eligible(alias):
    """
    django-ox measures age "from when it became eligible ... so a task
    deferred by a week does not read as a week of backlog". A part becomes
    eligible when extract turns SUCCESSFUL. extract runs for 3 s on a real
    worker; the moment it finishes, the parts are the oldest waiting work,
    and they have been eligible for milliseconds. --max-age 2 must pass.

    Run on the backend that carries the leftover DEFER_BLOCKED_NODES option
    as well, which must change nothing. A release that set run_after back
    to NULL would make Coalesce(run_after, enqueued_at) fall back to
    enqueued_at.
    """
    extract_id, part_ids = fan_out_flow(alias, f"aging-{alias}")
    with RunningUpstream(alias, extract_id) as upstream:
        time.sleep(3.0)
        upstream.finish()
        age = stats.oldest_ready_age(QUEUE)
        status = health("--max-age", "2")
        gauge = scrape()["django_ox_oldest_ready_age_seconds"]

    # Premise: the parts really are claimable now: READY, due (whatever a
    # release writes to run_after, it is not in the future), and through
    # the claim's own gate.
    released = (
        OxTask.objects.filter(pk__in=part_ids, status=OxTask.Status.READY)
        .filter(Q(run_after__isnull=True) | Q(run_after__lte=timezone.now()))
        .filter(workflows.runnable_filter_q())
    )
    assert released.count() == FAN_OUT

    seen = {
        "oldest_ready_age_s": None if age is None else round(age.total_seconds(), 2),
        "ox_health": status,
        "gauge_s": gauge,
    }
    assert age is not None and age.total_seconds() < 2.0, seen
    assert status.startswith("OK"), seen
    assert gauge is not None and gauge < 2.0, seen


# -- controls: pass today ------------------------------------------------


def test_control_the_same_harness_reads_empty_backlog_on_a_deferring_backend():
    """
    The scenario of the first test on flows_deferred, the backend with the
    leftover option. The same readings, the same expected values, so the
    option changes nothing a monitor reads.
    """
    extract_id, _ = fan_out_flow("flows_deferred", "backlog-deferred")
    with RunningUpstream("flows_deferred", extract_id) as upstream:
        assert nothing_is_claimable("flows_deferred") == [None, None, None]
        time.sleep(1.5)
        seen = readings(max_backlog=10, max_age=1)
        upstream.finish()

    assert seen["ready_count"] == 0, seen
    assert seen["oldest_ready_age_s"] is None, seen
    assert seen["ox_health"].startswith("OK: backlog=0 oldest_age=none"), seen
    assert seen["metrics"] == {
        "django_ox_ready_tasks": 0.0,
        "django_ox_oldest_ready_age_seconds": None,
    }, seen


def test_control_the_same_flags_page_on_real_stale_backlog():
    """
    The instrument sees real backlog: 11 claimable tasks with no worker,
    after the same 1.5 s, fail the same ox_health flags and show in the
    gauges. Without this, a green first test could be a blind probe.
    """
    for i in range(11):
        tasks.default_step.enqueue(f"plain-{i}")
    time.sleep(1.5)
    seen = readings(max_backlog=10, max_age=1)

    assert seen["ready_count"] == 11, seen
    assert seen["oldest_ready_age_s"] >= 1.5, seen
    status = seen["ox_health"]
    assert status.startswith("FAIL: backlog is 11, over --max-backlog 10"), seen
    assert "over --max-age 1s" in status, seen
    assert seen["metrics"]["django_ox_ready_tasks"] == 11.0, seen
    assert seen["metrics"]["django_ox_oldest_ready_age_seconds"] >= 1.5, seen
