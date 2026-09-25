# Recurring tasks

django-ox runs cron-style schedules with no separate scheduler process.

Declare them in settings and they deploy with your code, versioned alongside the
tasks they enqueue. That is the default, and for most schedules it is the right
one: a schedule is part of how the application behaves, and a change to it
belongs in a review and a release like any other.

Store them in the database instead and someone can retime or pause one without a
deploy. See [Schedules in the database](stored-schedules.md) for when that is
worth the trade and what it costs.

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "SCHEDULES": {
                "nightly-report": {
                    "task": "reports.tasks.build_report",
                    "cron": "0 3 * * *",
                    "kwargs": {"full": True},
                },
                "warm-cache": {
                    "task": "core.tasks.warm_cache",
                    "cron": "*/15 * * * *",
                },
            },
        },
    }
}
```

Each tick enqueues an ordinary task. Workers claim it through the normal queue,
so retries, backoff, priorities and the result store all work as usual, and
execution is at-least-once exactly as it is everywhere else.

## Schedule keys

The dictionary keys are the schedule names. They must be non-empty strings of at
most 128 characters.

**Names must be unique across every backend, not just within one.** The dispatch
log is keyed by name alone, so two backends sharing a name would suppress each
other's ticks. A duplicate is rejected at worker startup and by
`manage.py check`, as `django_ox.E003`.

Each entry accepts exactly these keys. Anything else is rejected at startup:

| Key | Required | Meaning |
| --- | --- | --- |
| `task` | yes | Dotted path to a `@task` callable, e.g. `"reports.tasks.build_report"`. |
| `cron` | one of | Five-field cron expression or `@` shortcut, syntax below. |
| `every` | one of | A fixed interval, as a `timedelta` or a number of seconds. |
| `phase` | no | Shifts an `every` sequence. A `timedelta` or seconds, less than `every`. |
| `args` | no | Positional arguments, as a list. Must be JSON-serializable and accepted by the database. |
| `kwargs` | no | Keyword arguments, as a dict. Must be JSON-serializable and accepted by the database. |
| `queue_name` | no | Queue override; defaults to the task's own queue. |
| `priority` | no | Priority override, -100 to 100. |

JSON serialization does not establish database acceptance. Non-finite floats
such as `float("inf")` and `float("nan")` pass `manage.py check` and worker
startup, but PostgreSQL, MySQL and SQLite reject them at enqueue. For
`float("inf")`, PostgreSQL raises `DataError`, MySQL through PyMySQL raises
`OperationalError` 3140, and SQLite raises `IntegrityError` through Django's
`JSON_VALID` check. PostgreSQL also rejects a NUL character (`"\x00"`) in a
string; MySQL and SQLite accept it.

These are database restrictions, not startup validation. A rejected dispatch
is rolled back and reported as `schedule_dispatch_error`. If the same
connection remains usable after rollback, later schedules are still attempted.

### Overriding the queue and priority

A schedule can put its task somewhere other than the task's own queue, which is
useful when a nightly job would otherwise sit behind interactive work:

```python
"SCHEDULES": {
    "nightly-export": {
        "task": "exports.tasks.rebuild",
        "cron": "0 2 * * *",
        "queue_name": "exports",
        "priority": -50,
        "kwargs": {"full": True},
    },
}
```

The queue must be listed in that backend's `QUEUES`, and some worker must be
processing it. Priority runs from -100 to 100, and lower runs later.

A schedule enqueues through the backend it is declared under. That holds even if
the task itself was declared against a different backend alias.

## Cron syntax

Five fields, in order: minute (0-59), hour (0-23), day of month (1-31), month
(1-12), day of week (0-7, where 0 and 7 both mean Sunday).

| Form | Example | Meaning |
| --- | --- | --- |
| Any | `*` | Every value. |
| Value | `30` | That value. |
| List | `1,15` | Any listed value. Elements can be ranges or steps. |
| Range | `9-17` | Every value in the range, inclusive. Descending ranges are rejected. |
| Step | `*/15`, `1-10/2` | Every Nth value across the range. |
| Value with step | `5/15` | From that value to the field maximum. In the minute field, `5/15` means 5, 20, 35, 50. |
| Month names | `jan`, `DEC` | Case-insensitive, three letters, month field only. |
| Weekday names | `sun`, `Mon-Fri` | Case-insensitive, three letters, day-of-week field only. |

Shortcuts work in place of a full expression: `@hourly`, `@daily` and
`@midnight`, `@weekly`, `@monthly`, `@yearly` and `@annually`.

### Common schedules

```python
"SCHEDULES": {
    # Every fifteen minutes.
    "warm-cache":     {"task": "core.tasks.warm_cache",    "cron": "*/15 * * * *"},
    # 03:00 every day.
    "nightly-report": {"task": "reports.tasks.build",      "cron": "0 3 * * *"},
    # Every weekday at 09:30.
    "weekday-digest": {"task": "core.tasks.digest",        "cron": "30 9 * * mon-fri"},
    # 00:00 on the 1st and 15th.
    "twice-monthly":  {"task": "billing.tasks.invoice",    "cron": "0 0 1,15 * *"},
    # Top of every hour.
    "hourly-sync":    {"task": "sync.tasks.pull",          "cron": "@hourly"},
    # 02:15 on the first of the month.
    "monthly-prune":  {"task": "core.tasks.prune_archive", "cron": "15 2 1 * *"},
}
```

Expressions that can never fire are rejected when the configuration loads. So
`0 0 30 2 *` (February 30th) is a startup error, not a schedule that silently
never runs.

### Day-of-month and day-of-week combine with OR

This follows vixie cron. When both fields are restricted, a day matches if
*either* one matches.

`0 0 1,15 * mon` fires on the 1st, on the 15th, and every Monday. It does not
fire only on Mondays that land on the 1st or 15th.

One deliberate divergence from vixie: a stepped star like `*/2` counts as
restricted here, which follows the croniter interpretation common in Python. So
`0 0 */2 * 1` fires on every odd day of the month *and* every Monday. Vixie
would require both.

### Timezones

Cron fields are wall-clock time in your project's timezone. With `USE_TZ = True`,
`0 3 * * *` means 03:00 local all year. With `USE_TZ = False` it is naive local
time.

Across a DST transition, a tick whose wall-clock time is missing or ambiguous is
resolved by zoneinfo fold handling. It shifts rather than raising. If a job must
not land inside a transition window, schedule it outside your zone's transition
hours.

**On the fall-back day a schedule can fire twice.** Take `30 1 * * *` in a zone
that replays 01:00 to 02:00: it fires once per pass. That is at most one extra
run per year, and it is consistent with the at-least-once delivery the rest of
the system already assumes. Hourly and more frequent schedules in the repeated
hour fire on each pass by design.

Changing `TIME_ZONE` to a zone behind the old one can also re-fire one
wall-clock label on the day you change it, because ticks are stored as distinct
UTC instants.

## Many workers, one tick

Every worker is also the scheduler. Alongside its polling, each one checks about
once a second whether a tick is due.

Coordination is a database unique constraint on (schedule name, tick time). It
works like this:

1. Every worker derives the same tick datetimes from the cron expression.
2. Dispatch writes the tick-log row first, then enqueues the task, in one
   transaction.
3. When workers race the same tick, exactly one `INSERT` succeeds.
4. The losers stop at the constraint and enqueue nothing.

The constraint deduplicates each due tick across workers. Dispatch needs at
least one running worker and a usable database. A schedule-scoped failure
does not stop later entries: after rollback, the worker continues if the
same database connection remains usable. An abandoned pass stops further
dispatch until a later pass. Scheduling is a property of the workers you
already run. There is no separate scheduler process to deploy or fail over.

A pass visits settings schedules in `SCHEDULES` insertion order, followed by
stored schedules in primary-key order. This makes traversal reproducible.
It does not establish fairness or priority, and it does not coordinate workers.
The tick's unique constraint does that.

This deduplicates the enqueue, not the execution. The task that a tick enqueues
runs under the same at-least-once contract as every other task, so write it to
be safe to run twice.

## Fixed intervals

`every` is the alternative to `cron`, for a cadence cron cannot express:

```python
"SCHEDULES": {
    "poll-inbox": {"task": "mail.tasks.poll", "every": timedelta(minutes=90)},
}
```

Exactly one of `cron` and `every` is required.

**Interval ticks are counted from a fixed instant, not from the last run.**
Every ninety minutes means 00:00, 01:30, 03:00 and so on, whatever time the
schedule was deployed and whatever the workers have been doing. Nothing about
a restart, a pause, a resume or an edit moves the sequence, because none of
them is an input to it: every worker computes the same instants from the
definition and the clock, which is what keeps dispatch leaderless.

That is the difference from `django-celery-beat`, where an interval is measured
from the previous run, so the sequence depends on when that run happened. If you
want a sequence offset from the grid, say so with `phase`:

```python
# Hourly, at ten past, rather than on the hour.
{
    "task": "reports.tasks.hourly",
    "every": timedelta(hours=1),
    "phase": timedelta(minutes=10),
}
```

`every` must be at least one second. The dispatch loop looks about once a
second and only the latest due tick fires, so anything faster would be
coalesced away rather than run.

Interval ticks are wall-clock, exactly as cron ticks are, so the
daylight-saving behaviour above applies to them too.

## Missed ticks

**If every worker was down when a tick passed, the most recent missed tick fires
once on recovery. Older ones are skipped**, so a tick can be enqueued late, and
some ticks are deliberately never enqueued at all.

A nightly job due during an unlucky deploy still runs when workers return. A
weekend of downtime on a five-minute schedule does not replay hundreds of stale
runs.

**A new schedule never fires for times before it existed.** The first time a
worker sees a schedule with no history, it records the current tick as an anchor
and enqueues nothing. The schedule first fires at its next tick.

### Retrying a failed dispatch

A schedule-scoped failure rolls back that schedule's transaction. It leaves
no tick row or task from the attempt, and the tick remains unclaimed.

The worker tries the schedule again on subsequent dispatch passes under the
usual due-tick, deadline and coordination rules. The command's dispatch
interval is `max(1, min(--interval, 30))` seconds. Only the latest due tick is
considered; failed dispatches do not create a backlog of ticks to replay.

A stored schedule's `starting_deadline_seconds` still applies. Once a tick
is too late, it is dropped and reported as `schedule_tick_dropped`, not as
another dispatch failure.

There is no execution backoff, failure-based tick skipping, stored failure
state or automatic disabling. While an enqueue rejection persists, each attempt
is a transaction that inserts a tick row, attempts the enqueue and rolls back,
followed by a `SELECT 1` connection check, on every dispatch pass on every
worker. A stored schedule also takes its row lock. Workers attempting a
settings schedule can queue on the same tick key. On MySQL with three or more
dispatchers, the claimant's rollback can leave those workers deadlocking,
producing 1213 `schedule_lock_unavailable` warnings; read these beside
`schedule_dispatch_error` for the same schedule. Contention reports remain
unthrottled, and report suppression does not reduce database attempts.

When the cause is resolved, the next dispatch pass can enqueue the latest
eligible, unclaimed tick. An `update_schedule` edit that changes only a stored
schedule's arguments keeps the same due tick. Recovery does not replay every
tick missed during the failure.

See [Monitoring](monitoring.md#schedule-failure-reporting) for failure counts
and the per-worker recovery event.

## Checking a schedule is live

`manage.py check` validates every settings-declared schedule without starting
a worker. It reports a bad task path, an unparseable or never-firing cron
expression, arguments that are not JSON-serializable, and duplicate names
across backends:

```
python manage.py check
```

Passing these checks does not establish database acceptance. A schedule can
pass validation and still fail at dispatch. Alert on `schedule_dispatch_error`
for schedule-scoped failures and `schedule_dispatch_failed` for abandoned
passes. For stored schedules, also alert on `schedule_row_skipped`.
`ox_health` has no schedule check.

To see what has actually dispatched, read the tick log:

```python
from django_ox.models import OxScheduleTick

OxScheduleTick.objects.filter(schedule_name="nightly-report").order_by(
    "-scheduled_for"
)[:5]
```

Each row is one dispatched tick. A row whose `task` is `None` is the anchor
written the first time a worker saw the schedule; it enqueued nothing and marks
the tick before the first real fire. A schedule-scoped failure rolls back its
tick row and task, so the tick log alone does not report failed dispatches.

The recovery baseline is each schedule's most recent tick row, which is why
`ox_prune` always keeps it. See
[Configuration](configuration.md#ox_prune).
