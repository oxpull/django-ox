# Recurring tasks

django-ox runs cron-style schedules with no separate scheduler process. You
declare them in settings, next to the backend they enqueue through, so they are
versioned and deployed with your code. The database holds a dispatch log and
nothing else. There are no schedule rows to edit by hand.

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
so retries, backoff, priorities and the result store all work as usual.

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
| `cron` | yes | Five-field cron expression or `@` shortcut, syntax below. |
| `args` | no | Positional arguments, as a list. Must be JSON-serializable. |
| `kwargs` | no | Keyword arguments, as a dict. Must be JSON-serializable. |
| `queue_name` | no | Queue override; defaults to the task's own queue. |
| `priority` | no | Priority override, -100 to 100. |

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
2. Dispatch wraps the tick-log `INSERT` and the task enqueue in one transaction.
3. When workers race the same tick, exactly one `INSERT` commits.
4. The losers hit the constraint and roll back, task row included.

So each tick fires exactly once, whatever the worker count, and dispatch keeps
working as long as one worker is alive. Scheduling is a property of the workers
you already run. There is nothing extra to deploy, monitor or fail over.

## Missed ticks

**If every worker was down when a tick passed, the most recent missed tick fires
once on recovery. Older ones are skipped.**

A nightly job due during an unlucky deploy still runs when workers return. A
weekend of downtime on a five-minute schedule does not replay hundreds of stale
runs.

**A new schedule never fires for times before it existed.** The first time a
worker sees a schedule with no history, it records the current tick as an anchor
and enqueues nothing. The schedule first fires at its next tick.

The recovery baseline is each schedule's most recent tick row, which is why
`ox_prune` always keeps it. See
[Configuration](configuration.md#ox_prune).
