"""
A stored (admin-edited) reconciler schedule on a
backend whose QUEUES lack the default backend's queue.

oxpull.workflows.reconcile and oxpull.batches.reconcile are built lazily by
batches._build_task_on_a_queue, which takes the queue from the DEFAULT
backend: "default" if that backend accepts it, else its first queue. A
django-ox 1.2.0 stored schedule is dispatched by the backend whose OPTIONS
name SCHEDULE_SOURCE, and DatabaseScheduleSource._to_schedule rebinds the
registry task with Task.using(backend=<that alias>), which re-runs
validate_task. When that backend's QUEUES does not contain the queue the
default backend handed the reconciler, the rebind raises InvalidTask, and
_build logs "Skipping stored schedule ..." and drops the row. The row has
no queue_name column, so nothing on the row can fix it. The settings
channel raises ImproperlyConfigured for the same layout instead.

The layout: "default" keeps its own QUEUES (it contains "default"), and a
"cron" backend with QUEUES ["pipelines"] reads schedule rows and exposes
the reconciler through SCHEDULABLE_TASKS. No SCHEDULES block anywhere.

Every non-control test asserts what a user needs: either the row the
admin and create_schedule accept is one the dispatching worker builds
and runs (so workflows complete, dead cones are cancelled, missed
releases are released, and batches complete), or the configuration is
refused where an operator cannot miss it. Valid refusal points are
create_schedule, the admin form, workflows.create(), batches.create(),
or a backend check reporting an Error that also stops `ox_worker`.

Every test named test_control_* runs the same row, cron backend, QUEUES
and worker as its regression case, with one difference:
SCHEDULABLE_TASKS exposes the reconciler already bound to the cron
backend's own queue (reconcile.using(backend="cron",
queue_name="pipelines"), built below). These controls isolate failures
caused by the queue the reconciler is built on from harness failures.
One more control shows the settings channel refuses the same layout loudly.

Batch tests import nothing from oxpull.workflows, so `-k batch` also runs
against oxpull 1.2.0's src (a control run on the released package).
"""

import copy
import logging
from datetime import timedelta

import pytest
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import OperationalError
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.stored import create_schedule
from django_ox.worker import Worker
from oxpull import batches
from oxpull.models import OxpullBatch

from . import tasks

pytestmark = pytest.mark.django_db

WORKFLOW_RECONCILE = "oxpull.workflows.reconcile"
BATCH_RECONCILE = "oxpull.batches.reconcile"
# The controls' reconcilers, bound to the cron backend's own queue.
WORKFLOW_RECONCILE_ON_CRON = f"{__name__}.workflow_reconcile_on_cron"
BATCH_RECONCILE_ON_CRON = f"{__name__}.batch_reconcile_on_cron"
ON_CRON = {
    WORKFLOW_RECONCILE: WORKFLOW_RECONCILE_ON_CRON,
    BATCH_RECONCILE: BATCH_RECONCILE_ON_CRON,
}
ROW_KEY = "reconcile"
ROW_NAME = "reconcile-every-minute"
SKIP_LOG = "Skipping stored schedule"


def __getattr__(name):
    """
    The controls' reconcilers, built on access as oxpull builds its own:
    the "cron" alias exists only inside each test's override_settings. The
    function is the same one, so the task row still stores the reconciler's
    public path and the worker resolves it by that path.
    """
    if name == "workflow_reconcile_on_cron":
        from oxpull import workflows

        return workflows.reconcile.using(backend="cron", queue_name="pipelines")
    if name == "batch_reconcile_on_cron":
        return batches.reconcile.using(backend="cron", queue_name="pipelines")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class Refused(Exception):
    """The layout was refused where an operator cannot miss it."""


@pytest.fixture(autouse=True)
def empty_registry(monkeypatch):
    """
    django-ox's schedulable registry is process-wide and refuses a second,
    different kind under one key. Start it empty, as django-ox's own
    test_dst_schedules does; the settings channel refills it from
    SCHEDULABLE_TASKS on every lookup, under whatever TASKS is active.
    """
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)


def layout(exposed, *, channel="rows"):
    """
    settings.TASKS as the hazard describes it.

    `exposed` is the SCHEDULABLE_TASKS mapping the cron backend carries; the
    row names ROW_KEY. Every SCHEDULES block is removed, so the cron backend
    is the only thing that can dispatch a reconciler. flows_ungated is
    dropped: it is oxpull.E007 by design, and an Error there would be
    mistaken for a refusal of this layout.
    """
    rebuilt = {}
    for alias, params in copy.deepcopy(settings.TASKS).items():
        if alias == "flows_ungated":
            continue
        params.get("OPTIONS", {}).pop("SCHEDULES", None)
        rebuilt[alias] = params
    if channel == "rows":
        options = {
            "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
            "SCHEDULABLE_TASKS": dict(exposed),
        }
    else:
        options = {
            "SCHEDULES": {
                ROW_KEY: {"task": exposed[ROW_KEY], "cron": "* * * * *"},
            },
        }
    rebuilt["cron"] = {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["pipelines"],
        "OPTIONS": options,
    }
    return rebuilt


def hazard(path):
    """The row's key exposes the reconciler exactly as a project would."""
    return {ROW_KEY: path}


def control(path):
    """
    The row's key exposes the reconciler bound to the cron queue. The public
    path is exposed as well, under a key no row names, so oxpull 1.2.0's
    batch gate (which looks for that path in SCHEDULABLE_TASKS) counts this
    layout exactly as it counts the hazard's.
    """
    return {ROW_KEY: ON_CRON[path], "public-path": path}


def refuse_on_check_errors():
    """
    `manage.py check` Errors stop every management command, ox_worker too.

    Django's check_tasks, which is how `manage.py check` reaches every
    backend's check() (oxpull's and django-ox's checks all live there; neither
    registers a check of its own). Called directly rather than through
    run_checks(): the admin test installs contrib apps for its duration, and
    their checks stay registered for the rest of the process.
    """
    from django.tasks.checks import check_tasks

    errors = [m for m in check_tasks() if m.level >= checks.ERROR]
    if errors:
        raise Refused(errors)


def write_row(**fields):
    """
    The row, through django_ox.stored.create_schedule: the write path the
    admin's save_model calls, and the documented programmatic one.
    """
    fields = {
        "name": ROW_NAME,
        "task_key": ROW_KEY,
        "trigger": "cron",
        "cron": "* * * * *",
        # A row fires from its boundary; five minutes back puts the latest
        # minute tick after it whatever second this runs at.
        "start_time": timezone.now() - timedelta(minutes=5),
        **fields,
    }
    try:
        return create_schedule(**fields)
    except ValidationError as exc:
        raise Refused(exc) from exc


def cron_worker():
    """What `ox_worker --backend cron` builds: django-ox's Worker on that alias."""
    return Worker(backend_alias="cron", backoff_initial=0, poll_interval=0.01)


def run_until_idle(worker, limit=50):
    ran = 0
    while ran < limit and worker.run_once():
        ran += 1
    return ran


def skipped_lines(caplog):
    return "\n".join(
        r.getMessage() for r in caplog.records if SKIP_LOG in r.getMessage()
    )


def dispatch_pass(caplog):
    """
    One pass of the cron worker's loop: build (worker start), dispatch due
    ticks, run what was enqueued. Returns (tasks enqueued, skip log lines).
    """
    with caplog.at_level(logging.WARNING, logger="django_ox"):
        dispatcher = cron_worker()
        enqueued = dispatcher.dispatch_schedules()
        run_until_idle(dispatcher)
    return enqueued, skipped_lines(caplog)


# -- the row is accepted, then never built --------------------------------


@pytest.mark.parametrize("path", [WORKFLOW_RECONCILE, BATCH_RECONCILE])
def test_a_reconciler_row_create_schedule_accepts_is_built_by_its_worker(path, caplog):
    """
    create_schedule validates the row (registry membership, trigger, cron)
    and saves it enabled. The worker `ox_worker --backend cron` builds must
    then hold it among its schedules, or the write must have been refused.
    """
    with override_settings(TASKS=layout(hazard(path))):
        try:
            refuse_on_check_errors()
            row = write_row()
        except Refused:
            return
        assert row.enabled
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            built = [schedule.name for schedule in cron_worker().schedules]
    assert ROW_NAME in built, (
        f"create_schedule accepted an enabled row naming {path}, and the "
        f"worker that dispatches it built {built}.\nWorker log:\n"
        + skipped_lines(caplog)
    )


class _Operator:
    """A staff user holding every permission, as the admin's request carries."""

    is_active = is_staff = is_superuser = is_authenticated = True
    pk = id = 1

    def has_perm(self, perm, obj=None):
        return True

    def has_perms(self, perms, obj=None):
        return True

    def has_module_perms(self, app_label):
        return True


def admin_save_then_build(exposed, caplog, rf):
    """
    The row through the admin: OxScheduleAdmin.get_form() builds the form
    the add view renders, the POST data is validated by it, and
    OxScheduleAdmin.save_model() writes it. Only the HTTP view layer is
    skipped (tests_oxpull/settings.py installs no admin, auth or sessions app, so
    they are installed for the duration). Then the worker for the cron
    backend is built. Raises Refused if anything on the way refuses.
    """
    installed = [
        *settings.INSTALLED_APPS,
        "django.contrib.contenttypes",
        "django.contrib.auth",
        "django.contrib.messages",
        "django.contrib.admin",
    ]
    with override_settings(TASKS=layout(exposed), INSTALLED_APPS=installed):
        from django.contrib import admin

        from django_ox.admin import OxScheduleAdmin
        from django_ox.models import OxSchedule

        refuse_on_check_errors()
        model_admin = OxScheduleAdmin(OxSchedule, admin.site)
        request = rf.post("/admin/django_ox/oxschedule/add/")
        request.user = _Operator()
        form_class = model_admin.get_form(request, None, change=False)
        form = form_class(
            data={
                "name": ROW_NAME,
                "task_key": ROW_KEY,
                "trigger": "cron",
                "cron": "* * * * *",
                "every_seconds": "",
                "phase_seconds": "0",
                "arguments": "{}",
                "enabled": "on",
                "end_time": "",
                "starting_deadline_seconds": "",
            }
        )
        if not form.is_valid():
            raise Refused(form.errors)
        try:
            model_admin.save_model(request, form.save(commit=False), form, change=False)
        except ValidationError as exc:
            raise Refused(exc) from exc
        row = OxSchedule.objects.get(name=ROW_NAME)
        assert row.enabled
        with caplog.at_level(logging.WARNING, logger="django_ox"):
            built = [schedule.name for schedule in cron_worker().schedules]
    return built, skipped_lines(caplog)


@pytest.mark.parametrize("path", [WORKFLOW_RECONCILE, BATCH_RECONCILE])
def test_a_reconciler_row_the_admin_saves_is_built_by_its_worker(path, caplog, rf):
    """The hazard's layout, through the admin form and save_model."""
    try:
        built, log = admin_save_then_build(hazard(path), caplog, rf)
    except Refused:
        return  # refused in front of the person, which is acceptable
    assert ROW_NAME in built, (
        f"The admin form validated and saved an enabled row naming {path}, "
        f"and the worker that dispatches it built {built}.\nWorker log:\n{log}"
    )


@pytest.mark.parametrize("path", [WORKFLOW_RECONCILE, BATCH_RECONCILE])
def test_control_the_admin_saves_and_the_worker_builds_a_cron_queue_reconciler(
    path, caplog, rf
):
    """
    Control: the same form data and save, with the reconciler exposed on
    the cron queue. Nothing refuses and the row builds, so the admin test
    above cannot pass through an unrelated refusal.
    """
    built, log = admin_save_then_build(control(path), caplog, rf)
    assert log == ""
    assert ROW_NAME in built


# -- what it costs: workflows ----------------------------------------------


def run_workflows_reconciled_by_the_row(caplog):
    """
    Two sealed workflows on "flows", drained by a real gating worker:
    "doomed" (root fails three times; leaf depends on it) and "fine" (one
    node, succeeds). Then one pass of the cron worker that dispatches the
    row. Raises Refused if the layout is refused anywhere on the way.
    """
    from oxpull import workflows

    from .test_workflows import drain, flow_worker, stored

    refuse_on_check_errors()
    try:
        doomed = workflows.create(
            "doomed", on_complete=tasks.record_callback, on_complete_args=["doomed"]
        )
        doomed.add("root", tasks.failing_step, "root")
        doomed.add("leaf", tasks.step, "leaf", depends_on="root")
        doomed.seal()
        fine = workflows.create(
            "fine", on_success=tasks.record_callback, on_success_args=["fine"]
        )
        fine.add("only", tasks.step, "only")
        fine.seal()
    except ImproperlyConfigured as exc:
        raise Refused(exc) from exc
    write_row()

    drain(flow_worker())
    assert OxTask.objects.get(args=["root"]).status == OxTask.Status.FAILED
    assert OxTask.objects.get(args=["only"]).status == OxTask.Status.SUCCESSFUL
    assert OxTask.objects.get(args=["leaf"]).status == OxTask.Status.WAITING
    assert stored(doomed).status == stored(fine).status == "SEALED"

    enqueued, log = dispatch_pass(caplog)
    return {
        "enqueued": enqueued,
        "reconciler_rows": OxTask.objects.filter(task_path=WORKFLOW_RECONCILE).count(),
        "log": log,
        "doomed": stored(doomed),
        "fine": stored(fine),
        "leaf": OxTask.objects.get(args=["leaf"]).status,
    }


def assert_reconciled(seen):
    report = (
        f"cron worker enqueued {seen['enqueued']} task(s), "
        f"{seen['reconciler_rows']} reconciler row(s) exist; doomed is "
        f"{seen['doomed'].status}, fine is {seen['fine'].status}, leaf is "
        f"{seen['leaf']}.\nWorker log:\n{seen['log']}"
    )
    assert seen["doomed"].status == "COMPLETE", report
    assert seen["leaf"] == OxTask.Status.DISCARDED, report
    assert seen["doomed"].callback_task_id is not None, report
    assert seen["fine"].status == "COMPLETE", report
    assert seen["fine"].success_task_id is not None, report


def test_workflows_reconciled_only_by_a_stored_row_complete_and_cancel_dead_cones(
    caplog,
):
    """
    The hazard's layout. The accepted row must run the reconciler: the dead
    cone cancelled, both workflows COMPLETE, both callbacks enqueued.
    """
    with override_settings(TASKS=layout(hazard(WORKFLOW_RECONCILE))):
        try:
            seen = run_workflows_reconciled_by_the_row(caplog)
        except Refused:
            return
    assert_reconciled(seen)


def test_control_workflows_complete_when_the_row_runs_a_cron_queue_reconciler(
    caplog,
):
    """Control: the same run, with the reconciler on the cron queue."""
    with override_settings(TASKS=layout(control(WORKFLOW_RECONCILE))):
        seen = run_workflows_reconciled_by_the_row(caplog)
    assert seen["log"] == ""
    assert seen["enqueued"] == 1
    assert_reconciled(seen)


def test_control_the_settings_channel_refuses_the_same_layout_loudly():
    """
    Control: the same reconciler in the cron backend's OPTIONS["SCHEDULES"]
    prevents the worker from starting.
    """
    with (
        override_settings(TASKS=layout(hazard(WORKFLOW_RECONCILE), channel="settings")),
        pytest.raises(
            ImproperlyConfigured, match="Queue 'default' is not valid for backend"
        ),
    ):
        cron_worker()


def run_a_missed_release(caplog, monkeypatch):
    """
    parent -> child on "flows_deferred". The parent runs on a real worker
    whose release fails (only release_downstream is made to raise, as a lock
    wait timeout or dropped connection would; the worker logs it and moves
    on). Then one pass of the cron worker. Returns (child's status, whether a
    worker then claims it, tasks enqueued, skip log).
    """
    from .test_workflows_deferred import chain, deferring_worker

    def child_status():
        return OxTask.objects.get(args=["child"]).status

    refuse_on_check_errors()
    try:
        chain()
    except ImproperlyConfigured as exc:
        raise Refused(exc) from exc
    write_row()

    def lock_wait_timeout(*args, **kwargs):
        raise OperationalError("simulated: lock wait timeout exceeded")

    with monkeypatch.context() as patched:
        patched.setattr("oxpull.worker.release_downstream", lock_wait_timeout)
        assert deferring_worker().run_once() is True
    assert OxTask.objects.get(args=["parent"]).status == OxTask.Status.SUCCESSFUL
    assert child_status() == OxTask.Status.WAITING
    assert deferring_worker().run_once() is False

    enqueued, log = dispatch_pass(caplog)
    return child_status(), deferring_worker().run_once(), enqueued, log


def test_with_deferred_nodes_a_missed_release_is_released_by_the_stored_row(
    caplog, monkeypatch
):
    """
    The reconciler is the only backstop for a release the worker missed. The
    accepted row must run it, and the child must become claimable.
    """
    with override_settings(TASKS=layout(hazard(WORKFLOW_RECONCILE))):
        try:
            released, ran, enqueued, log = run_a_missed_release(caplog, monkeypatch)
        except Refused:
            return
    assert released == OxTask.Status.READY and ran is True, (
        f"child still {released} after the cron worker's pass "
        f"(enqueued {enqueued}); a deferring worker claimed it: {ran}.\n"
        f"Worker log:\n{log}"
    )


def test_control_a_missed_release_is_released_by_a_cron_queue_reconciler(
    caplog, monkeypatch
):
    """Control: the same run, with the reconciler on the cron queue."""
    with override_settings(TASKS=layout(control(WORKFLOW_RECONCILE))):
        released, ran, enqueued, log = run_a_missed_release(caplog, monkeypatch)
    assert (released, ran, enqueued, log) == (OxTask.Status.READY, True, 1, "")


# -- what it costs: batches (the same helper, and 1.2.0's gate passes it) ---


def run_a_batch_reconciled_by_the_row(caplog):
    """
    A sealed one-member batch on "default", its member run by a real worker,
    then one pass of the cron worker. Returns (the 1.2.0 gate's answer, tasks
    enqueued, batch status, skip log). Raises Refused if anything refuses.
    """
    gate = batches.reconcile_schedule_aliases()
    refuse_on_check_errors()
    try:
        batch = batches.create("nightly")
        batch.add(tasks.plain_echo, "x")
        batch.seal()
    except ImproperlyConfigured as exc:
        raise Refused(exc) from exc
    write_row()
    run_until_idle(Worker(backoff_initial=0, poll_interval=0.01))
    assert OxTask.objects.get(task_path="tests_oxpull.tasks.plain_echo").status == (
        OxTask.Status.SUCCESSFUL
    )
    assert OxpullBatch.objects.get(pk=batch.id).status == "SEALED"

    enqueued, log = dispatch_pass(caplog)
    return gate, enqueued, OxpullBatch.objects.get(pk=batch.id).status, log


def test_a_batch_reconciled_only_by_a_stored_row_completes(caplog):
    """
    oxpull 1.2.0 refuses batches.create() unless something schedules the
    batch reconciler, and counts a row-reading backend that exposes it.
    This layout passes that gate. The accepted row must then complete the
    batch, or something must refuse.
    """
    with override_settings(TASKS=layout(hazard(BATCH_RECONCILE))):
        try:
            gate, enqueued, status, log = run_a_batch_reconciled_by_the_row(caplog)
        except Refused:
            return
    assert status == "COMPLETE", (
        f"reconcile_schedule_aliases() said {gate}, create() and seal() "
        f"accepted the batch, create_schedule accepted the row; the cron "
        f"worker enqueued {enqueued} task(s) and the batch is {status}.\n"
        f"Worker log:\n{log}"
    )


def test_control_a_batch_completes_when_the_row_runs_a_cron_queue_reconciler(caplog):
    """Control: reconciler on the cron queue; also passes on `oxpull` 1.2.0."""
    with override_settings(TASKS=layout(control(BATCH_RECONCILE))):
        gate, enqueued, status, log = run_a_batch_reconciled_by_the_row(caplog)
    assert (gate, enqueued, status, log) == (["cron"], 1, "COMPLETE", "")
