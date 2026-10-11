"""
The workflow reconciler's schedule requirement.

1.2.0 hardened the batch reconciler's requirement in three ways the
workflow reconciler did not get:

(a) batches.reconcile_schedule_aliases() counts a django-ox 1.2.0 stored
    schedule (SCHEDULE_SOURCE plus SCHEDULABLE_TASKS exposing the
    reconciler), and counts only schedules on a django-ox backend.
    backend._reconcile_is_scheduled reads OPTIONS["SCHEDULES"] of every
    alias and nothing else.
(b) batches.create() and Batch.seal() refuse when nothing schedules the
    reconciler. workflows.create() and Workflow.seal() do not.
(c) oxpull.W002 is emitted once. oxpull.W003 is emitted by every backend
    that declares WORKFLOWS.

Every test asserts the behaviour a user should get. Everything is driven
through public entry points: call_command("check"),
call_command("ox_worker"), django_ox.stored's write API, a real worker's
dispatch_schedules() and run_once(), and workflows.create()/seal().
"""

import copy
import io
from datetime import timedelta

import pytest
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone

from django_ox.models import OxTask
from django_ox.worker import Worker
from oxpull import workflows
from oxpull.models import OxpullWorkflow

from . import tasks
from .test_workflows import drain, flow_worker, stored

# Django 6.1 hands every configured alias to the model and field checks
# unless the command pins one, so a system check run here opens "replica"
# to ask SQLite or MySQL whether it supports JSONField. That read leaves
# the alias unwritten, but it has to be declared or the test database
# guard refuses the connection.
pytestmark = pytest.mark.django_db(databases=["default", "replica"])

WORKFLOW_RECONCILE = workflows.RECONCILE_TASK_PATH
BATCH_RECONCILE = "oxpull.batches.reconcile"


def _names_task(config, path):
    """Whether one SCHEDULES or SCHEDULABLE_TASKS entry names `path`."""
    if isinstance(config, str):
        return config == path
    return isinstance(config, dict) and config.get("task") == path


def project_tasks(*, batch_reconciler=True):
    """
    settings.TASKS as a project would deploy it, with nothing scheduling the
    workflow reconciler in either channel.

    flows_ungated is dropped: it is oxpull.E007 by design, and an Error makes
    `manage.py check` exit before the warnings under test are printed.
    """
    rebuilt = {}
    for alias, params in copy.deepcopy(settings.TASKS).items():
        if alias == "flows_ungated":
            continue
        options = params.get("OPTIONS", {})
        for key in ("SCHEDULES", "SCHEDULABLE_TASKS"):
            entries = options.get(key)
            if not isinstance(entries, dict):
                continue
            unwanted = [WORKFLOW_RECONCILE]
            if not batch_reconciler:
                unwanted.append(BATCH_RECONCILE)
            kept = {
                name: config
                for name, config in entries.items()
                if not any(_names_task(config, path) for path in unwanted)
            }
            if kept:
                options[key] = kept
            else:
                options.pop(key)
        rebuilt[alias] = params
    return rebuilt


def scheduled_project_tasks():
    """project_tasks() with the workflow reconciler scheduled in settings."""
    rebuilt = project_tasks()
    options = rebuilt["flows"].setdefault("OPTIONS", {})
    options.setdefault("SCHEDULES", {})["oxpull-workflow-reconcile"] = {
        "task": WORKFLOW_RECONCILE,
        "cron": "* * * * *",
    }
    return rebuilt


def run_check():
    """`manage.py check`, as an operator types it; returns what it printed."""
    out, err = io.StringIO(), io.StringIO()
    call_command("check", stdout=out, stderr=err)
    return out.getvalue() + err.getvalue()


# -- (a) the check reads one channel, and reads it on any backend -------


def test_a_stored_schedule_that_runs_the_workflow_reconciler_is_not_reported_missing(
    monkeypatch,
):
    """
    django-ox 1.2.0 lets a project keep schedules as rows. This project has
    no SCHEDULES block anywhere: "default" reads rows, and SCHEDULABLE_TASKS
    exposes both reconcilers. A row naming the workflow reconciler is
    written through django_ox.stored, a real worker dispatches it, the
    reconciler task runs and completes a sealed workflow.

    The configuration demonstrably runs the reconciler. `manage.py check`
    must not then tell the operator that nothing schedules it. The batch
    reconciler, configured the same way in the same run, is not reported.
    """
    from django_ox.stored import create_schedule

    # The registry is process-wide. Start it empty so this test neither
    # relies on nor leaves behind another test's keys; the settings channel
    # refills it from SCHEDULABLE_TASKS on every lookup.
    monkeypatch.setattr("django_ox.registry._registry", {})
    monkeypatch.setattr("django_ox.registry._discovered", True)

    config = project_tasks()
    options = config["default"]["OPTIONS"]
    options.pop("SCHEDULES")
    options["SCHEDULE_SOURCE"] = "django_ox.stored.DatabaseScheduleSource"
    options["SCHEDULABLE_TASKS"] = {
        "oxpull-batch-reconcile": BATCH_RECONCILE,
        "oxpull-workflow-reconcile": WORKFLOW_RECONCILE,
    }
    assert not any("SCHEDULES" in p.get("OPTIONS", {}) for p in config.values())

    with override_settings(TASKS=config):
        flow = workflows.create("stored-schedule")
        flow.add("only", tasks.step, "only")
        flow.seal()
        assert drain(flow_worker()) == 1
        assert stored(flow).status == OxpullWorkflow.Status.SEALED

        create_schedule(
            name="workflow-reconcile",
            task_key="oxpull-workflow-reconcile",
            trigger="cron",
            cron="* * * * *",
            # A row fires from its boundary; five minutes back puts the
            # latest minute tick after it, whatever second this runs at.
            start_time=timezone.now() - timedelta(minutes=5),
        )
        dispatcher = Worker(
            backend_alias="default", backoff_initial=0, poll_interval=0.01
        )
        assert dispatcher.dispatch_schedules() == 1
        assert OxTask.objects.filter(task_path=WORKFLOW_RECONCILE).count() == 1
        drain(dispatcher)
        assert stored(flow).status == OxpullWorkflow.Status.COMPLETE, (
            "premise: the stored row must run the workflow reconciler"
        )

        output = run_check()

    assert "oxpull.W002" not in output, (
        "control: batches count a stored schedule\n" + output
    )
    assert "oxpull.W003" not in output, (
        "oxpull.W003 says nothing schedules oxpull.workflows.reconcile, on a "
        "project whose stored schedule just ran it\n" + output
    )


def test_a_schedule_on_a_backend_no_worker_can_run_does_not_satisfy_the_check():
    """
    The only entry naming the workflow reconciler sits under the OPTIONS of
    "immediate", which is Django's ImmediateBackend. Schedules are dispatched
    by a django-ox worker for its own backend alias, and no worker can be
    built on that one: `ox_worker --backend immediate` refuses. Nothing will
    ever tick the entry, so the project has no reconciler, and the check
    must say so.
    """
    config = project_tasks()
    config["immediate"] = {
        **config["immediate"],
        "OPTIONS": {
            "SCHEDULES": {
                "oxpull-workflow-reconcile": {
                    "task": WORKFLOW_RECONCILE,
                    "cron": "* * * * *",
                },
            },
        },
    }
    with override_settings(TASKS=config):
        with pytest.raises(ImproperlyConfigured, match="not an OxBackend"):
            call_command("ox_worker", "--backend", "immediate")
        output = run_check()

    assert "oxpull.W003" in output, (
        "a SCHEDULES entry on a backend no worker can dispatch silenced "
        "oxpull.W003\n" + output
    )


# -- (b) nothing refuses a workflow that nothing will complete ----------


def test_create_refuses_when_nothing_schedules_the_workflow_reconciler():
    """
    batches.create() refuses without a reconciler schedule, on the line that
    creates the batch. workflows.create() does not: the check warns, the
    warning can be silenced or never read, and the workflow that follows
    never completes and never fires a callback.
    """
    with override_settings(TASKS=project_tasks()):
        assert "oxpull.W003" in run_check(), "premise: the check sees the gap"
        with pytest.raises(ImproperlyConfigured, match=r"oxpull\.workflows\.reconcile"):
            workflows.create("orphan")
    assert not OxpullWorkflow.objects.filter(name="orphan").exists()


def test_seal_refuses_when_nothing_schedules_the_workflow_reconciler():
    """
    Settings can change under a workflow that already exists, so Batch.seal()
    refuses as well as create(), and a refused seal leaves the batch OPEN.
    Workflow.seal() seals regardless.
    """
    with override_settings(TASKS=scheduled_project_tasks()):
        flow = workflows.create("before-the-schedule-went")
        flow.add("only", tasks.step, "only")
    with (
        override_settings(TASKS=project_tasks()),
        pytest.raises(ImproperlyConfigured, match=r"oxpull\.workflows\.reconcile"),
    ):
        flow.seal()
    assert stored(flow).status == OxpullWorkflow.Status.OPEN
    with override_settings(TASKS=scheduled_project_tasks()):
        flow.seal()
    assert stored(flow).status == OxpullWorkflow.Status.SEALED


# -- (c) one condition, one warning --------------------------------------


def test_the_missing_workflow_reconciler_is_reported_once():
    """
    Nothing schedules either reconciler. Both conditions are global: one
    schedule anywhere fixes each. W002 is printed once. W003 is printed once
    for the backends declaring WORKFLOWS, here "flows" and "flows_deferred".

    That one W003 says what a missing reconciler costs a node that waits: a
    release a worker missed is never made.
    """
    with override_settings(TASKS=project_tasks(batch_reconciler=False)):
        declaring = [
            alias
            for alias, params in settings.TASKS.items()
            if params.get("OPTIONS", {}).get("WORKFLOWS") is True
        ]
        output = run_check()
    assert sorted(declaring) == ["flows", "flows_deferred"]
    assert output.count("oxpull.W002") == 1, "control: W002 is once\n" + output
    assert output.count("oxpull.W003") == 1, (
        f"oxpull.W003 printed {output.count('oxpull.W003')} times for one "
        "missing schedule\n" + output
    )
    assert "waits forever" in output, output
