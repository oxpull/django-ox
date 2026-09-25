# Schedules in the database

Settings-declared schedules deploy with your code. That is the default and it
suits most schedules.

Sometimes it doesn't. An operator needs to pause a job at 2am. A support team
adjusts a report's timing without waiting for a release. Someone who cannot
deploy still has to be able to stop something. For those cases django-ox reads
schedules from a database table instead, editable in the Django admin.

The trade is worth being clear about. A settings entry is reviewed and versioned.
A row is neither. What follows is mostly about keeping that difference from
becoming a problem.

## Setting it up

Three steps.

**1. Say which tasks may be scheduled.** A row cannot name a task you haven't
exposed. Put `@schedulable` above `@task`:

```python
from django.tasks import task
from django_ox.registry import schedulable


@schedulable("reports.daily")
@task
def daily_report(): ...
```

`@schedulable` only takes effect when the module it sits in is imported.
django-ox imports each installed app's `tasks` module and nothing else, so a
decorator in `myapp/jobs.py` registers nothing until something else imports
that module. Put it in `myapp/tasks.py`.

Or declare them in settings, which is the only channel `manage.py check` can
validate:

```python
"OPTIONS": {
    "SCHEDULABLE_TASKS": {"reports.daily": "reports.tasks.daily_report"},
}
```

**2. Point the backend at the database source:**

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource",
        },
    }
}
```

**3. Run `manage.py migrate django_ox`.**

Schedules now appear in the admin under django-ox. Nothing else changes: the
worker is the same process, there is still no scheduler to deploy, and
`SCHEDULES` entries keep working if you use both.

All three steps matter. The admin
section appears once the migration has run, whether or not a backend sets
`SCHEDULE_SOURCE`. Without that option a row saves and then never dispatches:
no error, no log line, and nothing from `manage.py check`. "Run selected
schedules once now" still enqueues the task, so it is not evidence that the
schedule is wired up. If schedules created in the admin never fire on their
own, check that a backend's `OPTIONS` sets `SCHEDULE_SOURCE` to
`django_ox.stored.DatabaseScheduleSource`.

## What a person with admin access can do

They can pick a task from the list you exposed, set its timing and arguments,
enable it, disable it, and run it once immediately.

Running one immediately ignores both the pause and the end time: a schedule
that is disabled or past its `end_time` still runs.

They cannot name a task you haven't exposed. The field is a list, not a text box,
and a hand-written POST is refused too.

That is the unusual part. The two packages closest to this one both let a row
name anything, and what that buys differs between them.

In `django-q2` a `Schedule` row's `func` column is a plain text field, and the
worker resolves whatever it holds with `pydoc.locate` and calls the result
[^q2-func]. Change permission on that table is close to permission to run any
importable callable.

`django-celery-beat` is not the same. A `PeriodicTask` row's `task` column is
free text too [^beat-task], but a Celery worker never imports what it finds
there. It looks the name up in the registry of tasks it has already loaded, and
a name that isn't in the registry is rejected as `NotRegistered`
[^celery-names]. So change permission on that table is permission to run any
task the application registered, with the arguments and the cadence of your
choosing. That is a lot. It is not arbitrary code.

Here a row names a registry key the code exposed, never an import path. The
code decides what is reachable and the row picks from it.

You can narrow it further. A registry entry may require a permission of its own:

```python
@schedulable("payroll.run", permission="payroll.run_payroll")
@task
def run_payroll(): ...
```

Anyone without `payroll.run_payroll` can see that schedule and cannot change it,
whatever their permissions on the schedule table.

Arguments get validated if you say how:

```python
from django import forms
from django_ox.registry import ArgsForm, schedulable


class DailyArgs(ArgsForm):
    region = forms.CharField()


@schedulable("reports.daily", form=DailyArgs)
@task
def daily_report(region): ...
```

An unknown argument is rejected rather than ignored, and a number in a text field
is rejected rather than quietly turned into a string.

Form validation does not establish database acceptance. For example, an
`ArgsForm` field declared as `forms.JSONField` takes a JSON string.
The string `{"max_age": Infinity}` passes `create_schedule`, but its parsed
value is rejected at enqueue by PostgreSQL, MySQL and SQLite. Such a dispatch
is reported as `schedule_dispatch_error`, not `schedule_row_skipped`.

## When a schedule fires

This is the part worth reading properly. A stored schedule can be changed while
workers are running, and what should happen isn't always obvious.

Every rule below follows from one idea: **a tick fires only if it falls at or
after the schedule's `start_time`.**

That field is the activation boundary. It is set when the row is created. A few
events move it forward, and each of those is a rule below.

`end_time` is the other bound: no tick after it fires, and it is how a schedule
is stopped on a date rather than by hand. Both can be passed to
`create_schedule`; `start_time` defaults to the moment of creation and
`end_time` to none, meaning the schedule runs until it is disabled or deleted.

### A new schedule waits for its next tick

Create a daily 02:00 schedule at 15:00 and it first runs at 02:00 tomorrow. It
does not run immediately, because 02:00 today passed before the schedule
existed.

Create a minutely schedule at 14:37:41 and it first runs at 14:38:00, not
14:37:00, for the same reason.

The boundary is written when the row is created, not when a worker first notices
it. So a schedule created at 12:00 and first due at 12:05 still runs, even if
every worker was down until 12:06.

### Retiming reschedules from the moment you change it

Change a schedule from `0 2 * * *` to `0 3 * * *` at 15:00. It next runs at 03:00
tomorrow. It does **not** run at 03:00 today, even though that time has passed
and the new expression matches it: at 03:00 today, nobody could have expected a
run.

The same rule covers stranger cases. Retimed from 02:00 to 16:00 at 15:00, it
runs at 16:00 today, an hour later, because that tick is still ahead of the
change.

Changing what a schedule *runs* does not change *when* it runs. Editing its
arguments leaves the boundary alone. This matters: if every edit re-anchored
the schedule, one edited more often than its own period would never run at
all.

A task already enqueued keeps the arguments it was enqueued with. Edits apply to
the next tick.

### Pausing does not build up a backlog

Disable a schedule and it stops. Enable it again and it resumes from now.
Anything that came due while it was disabled does not run.

That is deliberate, and it differs from some systems you may know. Kubernetes
documents that when a CronJob with no starting deadline is unsuspended, "the
missed Jobs are scheduled immediately" [^k8s-suspend]. Quartz applies a
trigger's misfire instruction when the trigger is resumed [^quartz-resume],
and for a cron trigger the default instruction is to fire once, now
[^quartz-cron]. Temporal is closer to django-ox: while a schedule is paused its
spec "has no effect", and the runs a pause missed are something you ask for
with a backfill [^temporal-pause].

The point of pausing is that things stop. A resume that fires everything you
paused through fails at the same moment, one step later.

If you did want those runs, enqueue them yourself. Backfilling is a decision,
not a side effect of resuming.

### After downtime, only the most recent tick runs

If every worker was down across several ticks, the latest one runs on recovery
and the older ones are skipped. A nightly job still runs after an unlucky deploy.
A weekend of downtime on a five-minute schedule does not replay hundreds of
runs.

Tasks should be safe to run late for the same reason they should be safe to run
twice.

### Dropping a tick that is too late to be useful

Some jobs are worse than useless when they are hours late. A 09:00 standup
reminder at 14:00 is noise.

Set a starting deadline in seconds and a tick later than that is dropped instead
of run:

```python
from django_ox.stored import create_schedule

create_schedule(
    name="standup-reminder",
    task_key="reminders.standup",
    trigger="cron",
    cron="0 9 * * 1-5",
    starting_deadline_seconds=1800,
)
```

The default is no deadline, which is what settings-declared schedules have always
done: run however late.

The deadline is judged when the tick is admitted, under the row's lock. A tick
inside its deadline when the pass began and past it by the time the lock was
granted, because another worker or an admin save held the row, is dropped rather
than run late.

A dropped tick logs `schedule_tick_dropped` with how late it was, so a drop is
a signal rather than an absence. Each worker reports a given tick once, not once
per dispatch pass.

### An edit takes effect immediately, even mid-dispatch

Change a schedule one second before a worker was going to fire it and the worker
uses the change. Disable it and it does not fire. Retime it and the tick it was
about to record is no longer one this schedule wants, so it is not recorded.
Change its arguments and the task that runs gets the new ones.

A worker reads the schedules every second or so, but it does not decide from
what it read. Inside the transaction that would record the tick, it locks the
row, reads it, and works out from that row whether this exact tick is still due.
So the answer comes from the schedule as it stands, not from a copy of it, and
that holds for a change made any way at all, including a bulk update that runs
no application code.

Already-enqueued tasks are not cancelled. An edit applies to the next tick.

One limit worth knowing. A schedule carries a record of the timing and the
pause state its boundary was set for, so a retime or a pause done with
`queryset.update()` or a fixture is noticed at the next read: the tick from the
old definition does not fire, and the boundary moves to the moment the change
was found, which is not the moment it was made. However many workers find the
change together, only the first of them moves the boundary. Two things that
record cannot see. A change made and reverted between two reads, a pause and a
resume inside one `SCHEDULE_RECONCILE_INTERVAL` with no read between them,
leaves the row as it was, so nothing notices and one tick from inside the pause
can fire on the resume. And a tick between a raw edit and the read that finds it
is judged by the old definition until then. `update_schedule`, and the admin
that calls it, move the boundary at the moment of the change and have neither
gap.

### Renaming is safe

A schedule's name is a label. What its ticks are recorded against is the row
itself, so renaming one keeps its history, and a tick is not enqueued twice
while workers hold different views of the name. Execution stays at-least-once,
as it is for every task.

One consequence: a settings-declared schedule may not be named with a `db:`
prefix, which is reserved for exactly this. `manage.py check` refuses it.

## Writing schedules from code

The admin is one way in. `django_ox.stored` is the other, and it is what the
admin itself calls:

```python
from django_ox.stored import create_schedule, update_schedule

schedule = create_schedule(
    name="nightly-report",
    task_key="reports.daily",
    trigger="cron",
    cron="0 2 * * *",
    arguments={"region": "emea"},
)

update_schedule(schedule, cron="0 3 * * *")
```

Use these rather than `OxSchedule.objects.create()`. Django's `save()` does not
run model validation, so a direct write skips the checks, leaves the activation
boundary set for the old timing, and doesn't tell workers the row moved.

A row written that way is validated when a worker reads it. One that does not
validate is skipped and logged as `schedule_row_skipped`. One that does validate
has its activation boundary moved to the moment a worker noticed the row,
logged as `schedule_boundary_healed`, so any `start_time` the writer chose is
discarded. Validation does not establish database acceptance at dispatch.
A schedule-scoped dispatch failure is reported as `schedule_dispatch_error`.

`update_schedule` and `create_schedule` take an optional `user=`, and enforce any
per-entry permission when you pass one.

## Coming from django-celery-beat

The shape is familiar. The differences that will surprise you:

| | django-celery-beat | django-ox |
| --- | --- | --- |
| What a row names | any registered task, free text | a key you exposed in code |
| Intervals | measured from the last run | counted from a fixed instant |
| Pause and resume | depends how you paused; one path fires on resume | fires nothing from inside the pause; a bulk pause and resume with no read between them can fire one tick |
| Retiming | evaluated against the old `last_run_at` | reschedules from the moment of the change |
| Scheduler | one beat process, and only one | every worker, coordinated by a unique constraint |
| Bulk `update()` | needs `PeriodicTasks.update_changed()` by hand | noticed at the next read, from the row itself; not a change made and reverted between two reads |

`manage.py ox_import_beat_schedules` reads your existing table and prints the
registry entries and `create_schedule` calls it would take. It writes nothing:
retiming production is a decision, so you read the output, edit it and apply it
yourself. `--database` names the alias holding the `django_celery_beat`
tables, not django-ox's; it defaults to the alias `OxSchedule` reads from.

The interval difference is the one to watch. `every=timedelta(minutes=90)` fires
at 00:00, 01:30, 03:00 and so on, whatever time you created it. Celery would
measure ninety minutes from the last run. Use `phase` to shift the sequence if
the alignment matters.

## What this costs

Worth knowing before you turn it on:

- **A row is unreviewed input.** Someone with admin access can retime a
  production job without anyone seeing a diff. The registry limits *what* they
  can run, not *when*.
- **An edit stops the old tick immediately**; the new timing is used from the
  next dispatch pass, about a second later.
- **A change made without `django_ox.stored`**, a bulk update, a fixture or a
  data migration, is found within a minute rather than a second. Nothing about
  such a write tells a worker to look. Set
  `OPTIONS["SCHEDULE_RECONCILE_INTERVAL"]` if you want that sooner; it is one
  indexed read of a small table.
- **One extra query per pass.** Workers read a single row to learn whether
  anything changed, and re-read the schedules only when it did.
- **Read-time validation failures are skipped.** A row that no longer
  validates is logged as `schedule_row_skipped` and ignored so the others
  keep running.
- **Dispatch failures have a separate boundary.** A row can validate and
  still fail when dispatched. A schedule-scoped failure rolls back its tick
  and task and is reported as `schedule_dispatch_error`. If rollback succeeds
  and the same connection remains usable, later schedules are still attempted.
  The failed schedule is retried on subsequent passes under the usual
  due-tick and deadline rules; only its reporting is rate-limited.
- **An abandoned pass stops further traversal.** A `django.db.DatabaseError`
  escaping a shared dispatch read, failed rollback or unusable connection is
  reported as `schedule_dispatch_failed`. The stored source's marker read and
  boundary heal retain their own events: a failed marker read reports
  `schedule_source_unavailable` and dispatch continues from the cached set; a
  failed boundary heal reports `schedule_boundary_heal_failed`. Dispatch is
  retried on a later pass. Ticks already committed are not undone. Alert on
  `schedule_row_skipped`, `schedule_dispatch_error`, `schedule_dispatch_failed`
  and `schedule_source_unavailable`.
- **`manage.py check` cannot see rows.** Checks run before `migrate`, so a bad
  schedule in the database is a log line, not a start-up error.
  Settings-declared schedules still fail fast for errors their checks can
  detect. A missing `SCHEDULE_SOURCE` is not a check error either: leaving it
  out is the default, and a check cannot read the rows that would make it a
  mistake.
- **`task_enqueued` receivers run inside the dispatch transaction**, while the
  worker holds the schedule row's lock. A receiver that takes row locks of its
  own can deadlock against an application transaction that holds those rows and
  then writes the schedule, through `update_schedule` or the admin; the database
  ends one of the two, and the tick is retried on the next pass. Keep receivers
  to work that locks nothing a schedule-writing transaction may hold, and put
  anything else in `transaction.on_commit`. A callback registered that way that
  raises, whatever it raises, is logged as `schedule_dispatch_callback_failed`;
  the task it followed is enqueued and counted.

## Monitoring

The events below distinguish dispatched ticks, skipped rows and dispatch
failures. The full set, with every field, is on the
[monitoring](monitoring.md) page:

| Event | Meaning |
| --- | --- |
| `schedule_dispatched` | A tick enqueued its task. |
| `schedule_tick_dropped` | A tick was past its starting deadline. Carries `late_seconds`. |
| `schedule_row_skipped` | A row failed read-time validation and could not be used. Carries `reason`. |
| `schedule_dispatch_error` | A schedule-scoped failure, database or not. Its tick and task rolled back, and the pass continues. The schedule is retried under the usual due-tick and deadline rules. Reporting is rate-limited. |
| `schedule_dispatch_failed` | A dispatch pass was abandoned. It is retried on a later pass. Reporting is rate-limited. |
| `schedule_dispatch_recovered` | A schedule that had failed on this worker committed a tick again. Carries `failures`. |

Pausing a stored schedule preserves its failure state on each worker; deleting
the row drops it without an event. A paused, fixed and resumed row reports
`schedule_dispatch_recovered` when it next commits a tick on a worker that
retained its failure state.

Alert on `schedule_row_skipped`, `schedule_dispatch_error`,
`schedule_dispatch_failed` and `schedule_source_unavailable`, regardless of a
batch's exit code. `schedule_row_skipped` usually means a task key was removed
from the code while a row still names it. A row can pass validation and still
fail at dispatch, so that event alone is not enough.

Read `failures` and `suppressed` on dispatch failure events rather than
counting log lines. `ox_health` has no schedule check, and a rolled-back
dispatch leaves no tick row.

[^q2-func]: django-q2 1.11.1. `django_q/models.py`: `Schedule.func` is `models.CharField(max_length=256)` with no `choices` and no validators. `django_q/scheduler.py` passes the stored value to `async_task(s.func, ...)`, and `django_q/worker.py` runs `f = pydoc.locate(f)` and then `res = f(*task["args"], **task["kwargs"])`. The documentation describes the field as "the function to schedule. Dotted strings only." <https://django-q2.readthedocs.io/en/master/schedules.html>, checked 2026-09-12.
[^beat-task]: django-celery-beat 2.9.0. `django_celery_beat/models.py`: `PeriodicTask.task` is `models.CharField(max_length=200)` with no `choices` and no validators, and the model defines no `clean()`. The documentation says periodic tasks "can be managed from the Django Admin interface". <https://django-celery-beat.readthedocs.io/en/latest/>, checked 2026-09-12.
[^celery-names]: Celery 5.6.3, "Tasks", under "Names": "When tasks are sent, no actual function code is sent with it, just the name of the task to execute. When the worker then receives the message it can look up the name in its task registry to find the execution code." <https://docs.celeryq.dev/en/stable/userguide/tasks.html>. In the code, `celery/app/registry.py` is a `dict` whose `__missing__` raises `NotRegistered`, and `celery/worker/consumer/consumer.py` builds its strategies from `app.tasks` alone, sending a miss to `on_unknown_task`, which rejects the message and marks it failed with `NotRegistered`. The periodic-task page adds that the stored name "is not the import path of the task, even though the default naming pattern is built like it is" <https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html>. Both pages checked 2026-09-12.
[^k8s-suspend]: Kubernetes, "CronJob", under "Schedule suspension": "When `.spec.suspend` changes from `true` to `false` on an existing CronJob without a starting deadline, the missed Jobs are scheduled immediately." <https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#schedule-suspension>, checked 2026-09-12.
[^quartz-resume]: Quartz 2.3.0 API, `Scheduler.resumeTrigger`: "If the `Trigger` missed one or more fire-times, then the `Trigger`'s misfire instruction will be applied." The same sentence is on `resumeJob`, `resumeTriggers` and `resumeAll`. <https://www.quartz-scheduler.org/api/2.3.0/org/quartz/Scheduler.html>, checked 2026-09-12.
[^quartz-cron]: Quartz 2.3.0 tutorial, lesson 6, "CronTrigger Misfire Instructions": the smart policy "is also the default for all trigger types" and "is interpreted by CronTrigger as MISFIRE_INSTRUCTION_FIRE_NOW". The `CronTrigger` API names that constant `MISFIRE_INSTRUCTION_FIRE_ONCE_NOW`: "upon a mis-fire situation, the CronTrigger wants to be fired now by Scheduler." <https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/tutorial-lesson-06.html>, checked 2026-09-12.
[^temporal-pause]: Temporal, "Schedules": "When a Schedule is Paused, the Spec has no effect", and under "Backfill": "You might use this to fill in runs from a time period when the Schedule was paused due to an external condition that's now resolved". <https://docs.temporal.io/schedule>, checked 2026-09-12.
