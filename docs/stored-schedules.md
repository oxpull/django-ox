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
enable it, disable it, and run it once immediately. Disabling a readable row
alone can pause it even if it no longer validates. This does not repair the row.
Enabling it again is refused until it is corrected.

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

A row written that way has its activation boundary moved to the moment a
worker noticed it, logged as `schedule_boundary_healed`, whether or not it
validates. This replaces any `start_time` the writer chose. A row is skipped
and logged as `schedule_row_skipped` if a value cannot be converted, its start
or end cannot be compared with the worker's clock, or it cannot be built.
Skipping the row does not repair its invalid values. Database acceptance is
determined at dispatch; a schedule-scoped dispatch failure is reported as
`schedule_dispatch_error`.

To pause a readable row that no longer validates, call `update_schedule` with
`enabled=False` alone. This writes the disabled state without validating the
rest of the row. It does not repair the row. Enabling it again is refused
until the row is corrected.

`create_schedule`, `create_schedules` and `update_schedule` take an optional
`user=`, and enforce any per-entry permission when you pass one.

Use `create_schedules(rows, *, user=None)` for several schedules that should
exist together, such as schedules added by a migration or the importer's
output.

For stored-schedule writes, start and end times must fall within years 1 to
9999 in the database's time zone. With `USE_TZ` off, they must have no time
zone.

If the database raises an error while `create_schedules` checks a row, the
error carries a note identifying its `rows` index and name and stating that
no rows in the batch have been written. If the database raises an error
during the write, the error carries a note identifying the row's index and
name.

Pass a list of mappings with the keyword fields that `create_schedule`
takes:

```python
from django_ox.stored import create_schedules

schedules = create_schedules(
    [
        {
            "name": "morning-report",
            "task_key": "reports.daily",
            "trigger": "cron",
            "cron": "0 8 * * *",
        },
        {
            "name": "evening-report",
            "task_key": "reports.daily",
            "trigger": "cron",
            "cron": "0 18 * * *",
        },
    ]
)
```

The function returns a list of `OxSchedule` instances. An empty list returns
`[]`. The batch is all or nothing: nothing is written unless every row passes
validation and any permission check.

The batch uses one clock reading for every row's `created_at` and `updated_at`,
and for any `start_time` that is not supplied. Each row receives the same
validation as `create_schedule`. Names must also be unique within the batch.
Only after the whole batch validates does the function check `user`'s
permission for every row, if `user` is supplied.

Validation and permission checks happen before the write transaction. The
transaction runs on the database that stores the schedules and contains only
writes: every schedule save and one change-row touch. Workers are told once
for the whole batch.

The function raises:

- `TypeError` immediately if a row is not a mapping or a key is not a writable
  field. The message identifies the row.
- One `ValidationError` with a flat list of every validation failure across
  the batch. Each entry's `params` contains `index`, the zero-based position
  in `rows`; `name`, as supplied; `field`, or `""` for a failure that belongs
  to no one field; and `message`. Each message identifies its row and field,
  where there is one.
- `PermissionDenied` naming every denied row, after the whole batch validates
  and before any write.
- `IntegrityError` from the unique index if another writer takes a name
  between validation and the write. The whole batch is rolled back.

On SQLite, inside a caller's `transaction.atomic()` that has already read,
`create_schedule` and `create_schedules` fail immediately with
"database is locked" if another connection holds the write lock.

Stored-schedule writes reject start and end times outside years 1 to
9999 in the database's time zone, and reject times with a time zone when
`USE_TZ` is off.

Stored-schedule reads isolate rows whose values cannot be converted,
whose start or end cannot be compared with the worker's clock, or which
cannot be built. Other schedules and the queue keep running. Database read
errors stop the pass rather than being attributed to a row. Skips
carry the row's key, name when readable, and reason. They are logged once,
then at most once a minute per row per worker while the row stays skipped.
Locked dispatch skips include a traceback only on the first line.

An unreadable change marker reports `schedule_source_unavailable` once,
then at most once a minute. The worker reads the schedules in full at each
reconcile interval while the marker remains unreadable. For any schedule
source, a tick that cannot be derived or bounds that cannot be compared
with the worker's clock are reported as `schedule_dispatch_error`.

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

Run `manage.py ox_import_beat_schedules` in the Python environment and with
the Django settings beat ran with. Use the same timezone data, `USE_TZ` and
`DJANGO_CELERY_BEAT_TZ_AWARE`. The command cannot check that these match.

`--database` names the alias holding the `django_celery_beat` tables. It
defaults to the alias `OxSchedule` reads from.

### What the command prints

The command writes nothing itself. Read the output, edit it and apply it
yourself. It prints, in order:

1. A header stating the conditions the translation depends on, with
   additional notices where needed.
2. Section 1: a `SCHEDULABLE_TASKS` fragment built only from the schedule rows
   printed below it, followed by a comment about `SCHEDULE_SOURCE`. The same
   `OPTIONS` must include
   `"SCHEDULE_SOURCE": "django_ox.stored.DatabaseScheduleSource"`.
   Without it, no stored schedule ever runs.
3. Section 2: one `created = create_schedules([...])` call, with one
   `dict(...)` per schedule. The assignment keeps a shell from echoing the
   result. Applying the call creates every schedule or none.
4. A list headed `# Translated, with a difference from beat:`.
5. A list headed `# Not translated, and why:`.
6. A footer with instructions and limits on the checks.

Before printing a row, the command checks what `create_schedules` will
accept, including name, task key and cron lengths, against the database
the schedules will be written to. It also checks that the generated call
compiles. Rows that fail these checks are listed instead.

The destination checks include integer limits and text and JSON
restrictions. PostgreSQL refuses NUL and lone surrogates. MySQL refuses
lone surrogates and JSON nested more than 100 levels, and stores integers
outside its exact range as approximate floats. Names are compared using the destination column's comparison
rules: rows whose names compare equal to another name in the batch or to
an existing schedule name are all listed instead of printed for creation.
If the schedule table does not exist yet, the output says names were not
checked.

These are checks at the time the command runs, not a guarantee that
applying will succeed. A name may since have been taken, permissions or
the schema may have changed, or the database may not answer. An expiry
may also pass before applying. Regenerate stale output. If the call
raises, nothing was created; after a successful call, do not paste it
again.

A refused database read exits non-zero with a one-line error. A stored
value that cannot be converted can also stop the command before any
translation output. Row-level exclusions are described below. Quoted
stored values in the output are shortened to a bounded length, with a
mark where they have been cut.

### What is translated exactly

Crontab fields are read with Celery's own grammar. The resulting django-ox
expression fires on the same minutes, hours, days and months, subject to
the differences and exclusions below. This includes wrap-around ranges
such as `22-2`, names in any field read by their first three letters such
as `monday`, `*/n` steps, and lists. A field covering its whole range is
written as `*`.

The command prints its canonical expression when that fits the
128-character cron column. Otherwise it tries a deterministic covering
form, which may use django-ox's value-to-top steps such as `5/15`.
Every printed expression is re-parsed and must give the same times as
Celery's reading. A row with no verified expression within the limit is
listed instead. The project's tests check the field reading against
Celery's own parser.

When a weekday is restricted, a day-of-month field covering every date
of the allowed months is written as `*`. For example, a schedule for
Mondays in February can be translated even if its day-of-month field
does not cover all 31 possible dates.

Intervals are computed as beat computes them:
`timedelta(**{period: every})`. Accepted periods include `weeks` and
`milliseconds`. Whole seconds are taken from the timedelta's own fields.
For example, 2,000,000 microseconds becomes 2 seconds. A fractional
`every` can be stored only on SQLite; PostgreSQL and MySQL use an integer
column. On SQLite, 0.1 days becomes 8,640 seconds. Python's timedelta
rounds fractions to the microsecond before the whole-seconds check.

An interval must have a supported period and a numeric value whose
resulting timedelta amounts to a whole number of seconds, at least one
second. For example, 1,500,000 microseconds is refused. Intervals must
also fit the destination column and the 62,135,596,800-second ceiling.
For an interval past a limit, the reason names the limit that refuses it.

Arguments are decoded as beat decodes them, using kombu's JSON decoder
for text or bytes. A stored `[]` blob means no positional arguments.
Values that decode to non-JSON types, such as datetime, Decimal, UUID or
bytes, are not translated. Rows that beat disables because of an unknown
type marker or undecodable arguments are also listed instead.

A stored schedule has no timezone of its own. It runs in `TIME_ZONE`.
A crontab is translated only when its zone's timezone data is identical to
`TIME_ZONE`'s. The command compares the two zone files. Aliases such as
`US/Eastern` and `America/New_York` qualify. Zones that merely share
offsets today do not.

Enabled states are preserved. Schedules start when created; enabling a
disabled schedule resets its start time. Expiry is carried over as an
inclusive `end_time` one microsecond earlier than beat's expiry. A queue
or priority set on a beat task has no equivalent on a stored schedule;
set it on the task.

### Translated differences

The command checks clock changes over ten years from the import. It
names affected crontabs with the next date of a difference:

- With `USE_TZ` on, a stored schedule runs a repeated clock time on both
  passes; beat runs it on the first pass only. With `USE_TZ` off, both
  engines run a repeated time once, so there is no two-pass notice.
- With `USE_TZ` on, beat fires the first skipped run at the end of a
  spring-forward gap and counts on from it; a stored schedule fires the
  last skipped run. A notice is printed where these produce different
  run times.
- With `USE_TZ` off, a run inside a skipped hour is named where a stored
  schedule fires it early.

Within those ten years, a printed crontab with no clock-change notice
runs through skipped and repeated clock times at the times beat ran it.
Later years are not checked.

The command also checks django-celery-beat 2.9.0's hour filter. For a
plain-number crontab hour, beat loads the row only when that hour is
within two hours of the server's hour or is 4, using the crontab's
timezone column as of 2023-01-01 for the calculation. This applies whatever
`DJANGO_CELERY_BEAT_TZ_AWARE` says. Rows the filter leaves out at their
scheduled hour are named with the first date. Beat may still run them
on time if it reloads soon enough; a stored schedule runs them at their
hour. This analysis applies to django-celery-beat 2.9.0. If another
version is installed, or none is installed, a header line says so.

Every translated interval is listed as different. A stored schedule counts
the interval from a fixed instant. Celery counts it from the last run.
Its run times may differ.

`every=timedelta(minutes=90)` fires at 00:00, 01:30, 03:00 and so on,
whatever time you created it. Celery would measure ninety minutes from the
last run. Use `phase` to shift the sequence if the alignment matters.

### Rows that are not translated

The command lists each omitted row with its reason. The main groups are:

- **Different day or start-time rules.** A crontab is omitted when its
  `day_of_month` leaves out a date of a month it runs in and its
  `day_of_week` leaves out a weekday. Celery requires both to match.
  A stored schedule runs when either matches. A future start is also
  omitted: beat runs the task once when the start arrives, while a
  stored schedule waits for its next tick. A start already past is
  dropped from translated rows. Expired rows, including a year-1
  expiry, are listed as expired.
- **Unsupported schedules or arguments.** These include one-off, solar
  and clocked schedules, rows with both a crontab and an interval, beat's
  cleanup task, and arguments the stored-schedule API cannot accept.
  Use `ox_prune` to remove django-ox's finished rows rather than importing
  `celery.backend_cleanup`.
- **Invalid or unrepresentable values.** These include crontab fields
  Celery refuses, intervals outside the rules above, and values or names
  that fail the destination checks.
- **Incompatible timezone data or settings.** Crontab zones must satisfy
  the zone-file comparison above. The settings restrictions below also
  apply.

Some rows stop beat for the whole table, rather than merely failing to
run themselves. These include rows beat cannot build, empty or unloadable
crontab timezones, and, under some settings, bounds beat cannot compare.
The command lists these rows and adds a header comment saying beat ran
nothing from the table while they existed. The header names at most ten
such rows and counts the rest. Applying the other rows starts work beat
was not running.

Naive start and expiry bounds are judged by beat's clock, not the
importing process's zone. The timezone settings impose these restrictions:

- With `USE_TZ=True` and `DJANGO_CELERY_BEAT_TZ_AWARE=False`, rows are
  translated only when `TIME_ZONE` has timezone data identical to UTC.
  Rows carrying any start time or expiry are omitted.
- With `USE_TZ=False` and `DJANGO_CELERY_BEAT_TZ_AWARE=True` or unset,
  no row is translated.
- With both settings false, a row with a start time or expiry stored
  with a UTC offset is omitted. In django-celery-beat 2.9.0, beat's due
  check raises `TypeError` for such a row.
  A naive expiry is translated only when `TIME_ZONE` has timezone data
  identical to UTC.

Where these restrictions require a comparison with UTC, a `TIME_ZONE`
that this Python cannot load is reported as such. If it loads but its
zone file or UTC's cannot be found in the TZPATH directories or the
tzdata package, the command reports that the comparison could not be
made, not that the data differs. Check the setting and this environment's
timezone data.

Resolve or replace omitted schedules before switching schedulers.

### Supplying beat's timezone

`--beat-timezone ZONE` names the timezone the Celery app ran beat in.
Supply it when the crontab table has no timezone column. It is also
required for every crontab when `DJANGO_CELERY_BEAT_TZ_AWARE=False`,
because beat ignores the row's zone for scheduling in that mode.
The 2.9.0 hour filter described above still uses the timezone column.

The supplied zone must satisfy the same zone-file comparison with
`TIME_ZONE`. An empty zone in a table that has the timezone column is
not translated.

If the option was given but was not needed, and at least one crontab row
was read, the command prints a comment directly under the header
explaining that every crontab has its own timezone and
`DJANGO_CELERY_BEAT_TZ_AWARE` is on. It does not print this comment when
there are no crontab rows.

### Scope of the translation

The translation describes when schedules run while their scheduler is
running, under the environment and settings stated above. It does not
cover behavior after downtime, when a row is loaded late, or on a
never-run row's first run.

The beat behavior behind these rules was measured with
django-celery-beat 2.9.0 and Celery 5.6.3.

### Check output applied from earlier versions

Output from versions 1.2.0 through 1.7.0 passed crontab fields through as
stored and judged zones by two sample offsets. If you applied that output,
check:

- Imported schedules restricting both day fields where the day-of-month
  restriction excludes a date of an allowed month. They run when either
  field matches. A "first Monday" expression using `day_of_month="1-7"`
  and `day_of_week="mon"` runs about 124 times a year instead of 12.
- Imported crontabs from a zone other than `TIME_ZONE`.
- Beat rows with microsecond intervals. They were listed as below one
  second, including values that represent whole seconds.

Regenerate output with this version to review the translations and
named differences or exclusions. Long crontabs that 1.7.0 printed as
stored can print again when the command finds a verified expression
within the column limit.

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
  anything changed. They re-read the schedules when it did and at each
  reconcile interval. If the change marker cannot be read, full reads still
  run at each reconcile interval.
- **Unreadable or invalid rows are skipped.** A row is left out if a value
  cannot be converted, its start or end cannot be compared with the worker's
  clock, or it cannot be built. The other schedules and the queue keep
  running. The skip is logged as `schedule_row_skipped` with `schedule_pk`,
  `schedule` and `reason`. If the name cannot be read, `schedule` may be
  `None`. A conversion failure's reason names the column. Each row is logged
  once, then at most once a minute per worker while it stays skipped. A skip
  during the locked dispatch read includes a traceback only on its first
  line.
- **Skipped rows need repair.** Skipping does not repair invalid values. Find
  the row by `schedule_pk` and correct it in the admin change form or with
  `update_schedule`. Use SQL if the value is one the ORM cannot read. To pause
  a readable row that no longer validates, use `update_schedule` with
  `enabled=False`
  alone. Enabling it again is refused until it is corrected.
- **Dispatch failures have a separate boundary.** A schedule can validate and
  still fail when dispatched. For any schedule source, a tick that cannot be
  derived or bounds that cannot be compared with the worker's clock are
  reported as `schedule_dispatch_error`. A schedule-scoped failure inside
  the dispatch transaction rolls back its tick and task. If rollback succeeds
  and the same connection remains usable, later schedules are still attempted.
  The failed schedule is retried on subsequent passes under the usual
  due-tick and deadline rules; only its reporting is rate-limited.
- **Database read failures are not bad rows.** A database error during a full
  schedule read or a shared dispatch read, such as a lost connection, missing
  table or lock error, is not attributed to an individual row. It stops the
  pass rather than skipping a row.
- **An abandoned pass stops further traversal.** A `django.db.DatabaseError`
  escaping a shared dispatch read, failed rollback or unusable connection is
  reported as `schedule_dispatch_failed`. The stored source's marker read and
  boundary heal retain their own events. An unreadable change-marker value
  reports `schedule_source_unavailable` once, then at most once a minute while
  it remains unreadable, and the worker reads the schedules in full at each
  reconcile interval during that time. A database error on the marker read
  reports the same event on every dispatch pass, and the worker keeps using
  the last schedules it read. A failed boundary heal reports
  `schedule_boundary_heal_failed`. Dispatch is retried on a later pass.
  Ticks already committed are not undone. Alert on `schedule_row_skipped`,
  `schedule_dispatch_error`, `schedule_dispatch_failed` and
  `schedule_source_unavailable`.
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
| `schedule_row_skipped` | A row could not be built. A skip during a periodic full read carries `schedule`, `schedule_pk` and `reason`. A skip during the locked dispatch read carries `schedule_pk` and a traceback. |
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
