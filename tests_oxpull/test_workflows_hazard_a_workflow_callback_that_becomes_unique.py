"""
A workflow whose stored callback stops passing the completion checks after
create() stays SEALED, says so at every tick, and keeps its rows.

create() validates on_complete/on_success once and stores only the dotted
path. Completion imports that path again and re-runs _require_ox_backend
and _reject_unique inside the transaction that wins SEALED -> COMPLETE.
When a later deploy or config change makes the path unique, removes it, or
binds it to a backend that is not django-ox, that completion is refused.

The rule is the one 1.2.0 documents for batches, applied to workflows:
- the workflow stays SEALED and neither callback is enqueued. Completing
  anyway would drop the callback, or coalesce a unique one onto another
  workflow's task;
- the refusal is visible: every tick logs an ERROR about this workflow that
  names the callback and why it was refused;
- oxpull_prune keeps its edge and node rows while it is stranded;
- nothing is lost: once the deploy is fixed, the next tick completes it,
  both callbacks run, and prune then reaps it.

Each change is made the way production makes it, after create():
- unique_tasks_option: the operator adds the path to UNIQUE_TASKS. The
  override resets task_backends, as a process restart would.
- unique_decorator: the real oxpull.unique.unique decorator is applied to
  the module-level task, as a deploy that adds @unique would.
- renamed: the module no longer has the name, as after a rename or move.
- moved_off_django_ox: the module-level name is now a Task on a backend
  that is not django-ox, as after @task(backend="immediate").

The reconciler runs as the task a SCHEDULES entry enqueues, claimed and
executed by a worker, and prune runs through the real management command.
A healthy workflow sealed beside the stranded one completes on the first
tick and is reaped by the same prune call, so a reconciler that never ran
and a prune that deletes nothing cannot pass.
"""

import io
import logging
from contextlib import ExitStack

import pytest
from django.conf import settings
from django.core.management import call_command
from django.tasks import task_backends
from django.test import override_settings
from django.utils.module_loading import import_string

from django_ox.backend import OxBackend
from django_ox.models import OxTask
from oxpull import unique as unique_module
from oxpull import workflows
from oxpull.models import OxpullWorkflow, OxpullWorkflowEdge, OxpullWorkflowNode

from . import tasks
from .test_workflows import drain, flow_worker, stored

pytestmark = pytest.mark.django_db

CALLBACK = "tests_oxpull.tasks.record_callback"
TICKS = 5
CHANGES = [
    "unique_tasks_option",
    "unique_decorator",
    "renamed",
    "moved_off_django_ox",
]

# A word the refusal must carry, besides the callback's name, to say why
# the callback was refused. Matched case-insensitively against the log
# message and every exception in the record's chain, type names included.
REFUSAL_REASON = {
    "unique_tasks_option": "unique",
    "unique_decorator": "unique",
    "renamed": "import",
    "moved_off_django_ox": "django-ox",
}


def premise(condition, message):
    """
    A fact the test stands on, not the behaviour under test. pytest.fail is
    not an AssertionError, so a strict marker never absorbs it.
    """
    if not condition:
        pytest.fail(f"premise: {message}")


def change_the_callback(how, monkeypatch, stack):
    """Make the stored callback path fail the completion checks, for real."""
    if how == "unique_tasks_option":
        default = settings.TASKS["default"]
        options = default["OPTIONS"]
        stack.enter_context(
            override_settings(
                TASKS={
                    **settings.TASKS,
                    "default": {
                        **default,
                        "OPTIONS": {
                            **options,
                            "UNIQUE_TASKS": [*options["UNIQUE_TASKS"], CALLBACK],
                        },
                    },
                }
            )
        )
        premise(
            CALLBACK in task_backends["default"].unique_task_paths,
            "UNIQUE_TASKS did not take",
        )
    elif how == "unique_decorator":
        monkeypatch.setattr(unique_module, "_registry", set(unique_module._registry))
        unique_module.unique(tasks.record_callback)
        premise(unique_module.is_registered(CALLBACK), "@unique did not register")
    elif how == "renamed":
        monkeypatch.delattr(tasks, "record_callback")
        try:
            import_string(CALLBACK)
        except ImportError:
            pass
        else:
            pytest.fail(f"premise: {CALLBACK} still imports")
    elif how == "moved_off_django_ox":
        monkeypatch.setattr(
            tasks, "record_callback", tasks.record_callback.using(backend="immediate")
        )
        premise(
            not isinstance(import_string(CALLBACK).get_backend(), OxBackend),
            "the callback is still on a django-ox backend",
        )
    else:  # pragma: no cover
        raise AssertionError(how)


def reconciler_tick():
    """
    One reconciler tick, run the way a SCHEDULES entry runs it: enqueued as
    the task, then claimed and executed by a worker. Returns the reconcile
    task's final status and return value.
    """
    result = workflows.reconcile.enqueue()
    runner = flow_worker()
    for _ in range(20):
        if OxTask.objects.get(id=result.id).status not in (
            OxTask.Status.READY,
            OxTask.Status.RUNNING,
        ):
            break
        runner.run_once()
    row = OxTask.objects.get(id=result.id)
    premise(
        row.status not in (OxTask.Status.READY, OxTask.Status.RUNNING),
        f"the reconcile task never finished: {row.status}",
    )
    return row.status, row.return_value


def two_node_workflow(name, callback, label):
    flow = workflows.create(
        name,
        on_complete=callback,
        on_complete_args=[f"{label}-complete"],
        on_success=callback,
        on_success_args=[f"{label}-success"],
    )
    flow.add("a", tasks.step, "a")
    flow.add("b", tasks.step, "b", depends_on="a")
    flow.seal()
    return flow


def rows_of(flow):
    """(edge rows, node rows) oxpull still holds for this workflow."""
    return (
        OxpullWorkflowEdge.objects.filter(workflow_id=flow.id).count(),
        OxpullWorkflowNode.objects.filter(workflow_id=flow.id).count(),
    )


def prune():
    out = io.StringIO()
    call_command("oxpull_prune", "--older-than", "0", stdout=out)
    return out.getvalue().strip().replace("\n", "; ")


def record_text(record):
    """A record's message and every exception in its chain, as one string."""
    parts = [record.getMessage()]
    exc = record.exc_info[1] if record.exc_info else None
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        parts.append(f"{type(exc).__name__}: {exc}")
        exc = exc.__cause__ or exc.__context__
    return " | ".join(parts)


def refusals(records, flow, how):
    """
    ERROR records about this workflow that name its callback and say why the
    callback was refused.
    """
    reason = REFUSAL_REASON[how]
    found = []
    for record in records:
        if record.levelno < logging.ERROR:
            continue
        text = record_text(record)
        about = (
            getattr(record, "workflow_id", None) == str(flow.id) or str(flow.id) in text
        )
        if about and "record_callback" in text and reason in text.lower():
            found.append(record)
    return found


def describe(records):
    return [
        (record.levelname, getattr(record, "event", None), record_text(record)[:240])
        for record in records
    ] or "no records at ERROR"


@pytest.mark.parametrize("how", CHANGES)
def test_a_workflow_whose_callback_changed_after_create_stays_sealed_and_says_so(
    how, monkeypatch, caplog
):
    flow = two_node_workflow(f"callback-{how}", tasks.record_callback, "stranded")
    healthy = two_node_workflow(f"healthy-{how}", tasks.plain_echo, "healthy")
    row = stored(flow)
    premise(
        (row.on_complete_path, row.on_success_path) == (CALLBACK, CALLBACK),
        f"stored callbacks {row.on_complete_path}, {row.on_success_path}",
    )
    premise(drain(flow_worker()) == 4, "the worker did not run all four nodes")
    node_ids = [*flow.node_ids().values(), *healthy.node_ids().values()]
    premise(
        set(OxTask.objects.filter(id__in=node_ids).values_list("status", flat=True))
        == {OxTask.Status.SUCCESSFUL},
        "not every node succeeded",
    )
    premise(rows_of(flow) == (1, 2), f"rows before anything: {rows_of(flow)}")

    problems = []
    with ExitStack() as stack:
        change_the_callback(how, monkeypatch, stack)

        ticks = []
        for tick in range(1, TICKS + 1):
            caplog.clear()
            with caplog.at_level(logging.ERROR, logger="oxpull"):
                ticks.append(reconciler_tick())
                # Anything a fix enqueued gets its chance to run.
                drain(flow_worker())
            if not refusals(caplog.records, flow, how):
                problems.append(
                    f"tick {tick} (reconcile task {ticks[-1]}) logged no ERROR "
                    f"about workflow {flow.id} naming {CALLBACK!r} and "
                    f"{REFUSAL_REASON[how]!r}: {describe(caplog.records)}"
                )

        pruned = prune()
        row = stored(flow)
        progress = flow.progress()
        callback_rows = OxTask.objects.filter(task_path=CALLBACK).count()
        stranded_rows = rows_of(flow)

    healthy_row = stored(healthy)
    premise(
        healthy_row.status == OxpullWorkflow.Status.COMPLETE
        and healthy_row.callback_task_id is not None
        and healthy_row.success_task_id is not None,
        f"the healthy workflow sealed beside it did not complete: "
        f"{healthy_row.status}, ticks {ticks}",
    )
    premise(
        rows_of(healthy) == (0, 0),
        f"oxpull_prune did not reap the completed healthy workflow "
        f"({rows_of(healthy)} edge and node rows left): {pruned}",
    )

    if row.status != OxpullWorkflow.Status.SEALED:
        problems.append(
            f"left SEALED for {row.status} while its callback could not be "
            f"enqueued (progress total={progress.total} pending={progress.pending} "
            f"succeeded={progress.succeeded}); ticks {ticks}"
        )
    if row.callback_task_id is not None or row.success_task_id is not None:
        problems.append(
            f"recorded callback ids callback_task_id={row.callback_task_id} "
            f"success_task_id={row.success_task_id} for a refused callback"
        )
    if callback_rows:
        problems.append(
            f"{callback_rows} task row(s) for the refused callback {CALLBACK} exist"
        )
    if stranded_rows != (1, 2):
        problems.append(
            f"oxpull_prune --older-than 0 left {stranded_rows[0]} edge and "
            f"{stranded_rows[1]} node row(s) of the stranded workflow, not 1 and "
            f"2: {pruned}"
        )
    assert not problems, f"[{how}] " + " | ".join(problems)

    # The deploy is fixed: the path imports again, is not unique, and is on
    # django-ox. The held completion goes through on the next tick.
    monkeypatch.undo()
    restored = reconciler_tick()
    drain(flow_worker())
    row = stored(flow)
    assert row.status == OxpullWorkflow.Status.COMPLETE, (
        f"[{how}] still {row.status} after the callback was put back; tick {restored}"
    )
    ran = (tasks.STATE.get("stranded-complete"), tasks.STATE.get("stranded-success"))
    assert ran == (1, 1), (
        f"[{how}] on_complete and on_success ran {ran} times after the fix; "
        f"callback_task_id={row.callback_task_id} "
        f"success_task_id={row.success_task_id}"
    )
    pruned = prune()
    assert rows_of(flow) == (0, 0), (
        f"[{how}] oxpull_prune left {rows_of(flow)} edge and node rows once the "
        f"workflow completed: {pruned}"
    )
