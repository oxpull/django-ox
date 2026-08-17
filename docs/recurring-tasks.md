# Recurring tasks

django-ox runs cron-style recurring tasks without a separate scheduler
process. Schedules are declared in settings, next to the backend they
enqueue through, so they are versioned and deployed with the code that
defines the tasks. The database holds only a dispatch log; there are no
schedule rows to edit by hand.

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

Each tick enqueues a normal task instance, which workers claim and execute
through the ordinary queue. Retries, backoff, priorities and the result
store all apply unchanged.

## Schedule keys

Schedule names are the dictionary keys: non-empty strings, at most 128
characters. Schedule names must be unique across all configured backends,
not just within one; the dispatch log is keyed by name alone, so a name
shared between two backends is rejected at startup and by `manage.py
check` (`django_ox.E003`) instead of letting the backends suppress each
other's ticks. Each entry accepts exactly these keys (anything else is
rejected at startup):

| Key | Required | Meaning |
| --- | --- | --- |
| `task` | yes | Dotted path to a `@task` callable, e.g. `"reports.tasks.build_report"`. |
| `cron` | yes | Five-field cron expression or `@` shortcut, syntax below. |
| `args` | no | Positional arguments, as a list. Must be JSON-serializable. |
| `kwargs` | no | Keyword arguments, as a dict. Must be JSON-serializable. |
| `queue_name` | no | Queue override; defaults to the task's own queue. |
| `priority` | no | Priority override, -100 to 100. |

A schedule always enqueues through the backend it is declared under, even
if the task was declared with a different backend alias.

## Cron syntax

Expressions use the classic five fields, in order: minute (0-59), hour
(0-23), day of month (1-31), month (1-12), day of week (0-7, where both 0
and 7 mean Sunday). The parser supports vixie cron syntax:

| Form | Example | Meaning |
| --- | --- | --- |
| Any | `*` | Every value. |
| Value | `30` | That value. |
| List | `1,15` | Any listed value. Elements can themselves be ranges or steps. |
| Range | `9-17` | Every value in the range, inclusive. Descending ranges are rejected. |
| Step | `*/15`, `1-10/2` | Every Nth value across the range. |
| Value with step | `5/15` | From the value to the field maximum, stepping: `5/15` in the minute field means minutes 5, 20, 35, 50. |
| Month names | `jan`, `DEC` | Case-insensitive three-letter names in the month field. |
| Weekday names | `sun`, `Mon-Fri` | Case-insensitive three-letter names in the day-of-week field. |

The `@` shortcuts are also accepted in place of a full expression:
`@hourly` (`0 * * * *`), `@daily` and `@midnight` (`0 0 * * *`), `@weekly`
(`0 0 * * 0`), `@monthly` (`0 0 1 * *`), `@yearly` and `@annually`
(`0 0 1 1 *`).

**Day-of-month and day-of-week combine with OR**, as in vixie cron: when
both fields are restricted, a day matches if either field matches. So
`0 0 1,15 * mon` fires on the 1st, the 15th, and every Monday, not only on
Mondays that fall on the 1st or 15th. One divergence from vixie: a stepped
star such as `*/2` counts as a restricted field here, following the
Python-ecosystem (croniter-style) interpretation, so `0 0 */2 * 1` fires
on every odd day of the month and on every Monday, where vixie would
require the day to satisfy both fields.

Expressions that can never fire, such as `0 0 30 2 *` (February 30th), are
rejected when the configuration loads, not discovered as a schedule that
silently never runs.

### Timezones

Cron fields describe wall-clock time in your project's current timezone
(`TIME_ZONE`, with `USE_TZ = True`); `0 3 * * *` means 03:00 local, year
round. With `USE_TZ = False` everything is naive local time. Around DST
transitions, a tick whose wall-clock time does not exist or is ambiguous
resolves via zoneinfo fold handling and shifts rather than erroring. If a
job must not run inside a transition window, schedule it outside your
zone's transition hours.

On the fall-back day, a schedule whose wall-clock time
falls inside the repeated hour (for example `30 1 * * *` in a zone that
replays 01:00-02:00) can fire twice, once per pass of that hour. That is
at most one duplicate run per year and is consistent with the at-least-once
delivery the rest of the system already assumes; hourly and more frequent
jobs in the repeated hour intentionally fire on each pass. Changing
`TIME_ZONE` to a zone whose wall clock is behind the old one can likewise
re-fire the same wall-clock label once on the day of the change, because
ticks are stored as distinct UTC instants.

## Multi-worker behavior

Every running worker doubles as the scheduler: alongside its polling, each
worker checks (about once a second by default) whether any schedule has a
tick due. Coordination is a database unique constraint on
(schedule name, tick time). Each worker derives identical tick datetimes
from the cron expression, and dispatch wraps the tick-log INSERT and the
task enqueue in one transaction. When several workers race the same tick,
exactly one INSERT commits; the losers hit the constraint and their
transactions roll back, task row included. Each tick therefore fires
exactly once, with any number of workers, and dispatching survives as long
as at least one worker is running.

Scheduling is a property of the workers you already run; it adds nothing
extra to deploy, monitor, or fail over.

## Missed ticks

The policy, stated plainly:

- **If every worker was down when a tick passed, the latest missed tick
  fires once on recovery. Older missed ticks are skipped.** A nightly job
  that was due during an unlucky deploy window still runs when workers
  come back, but a weekend of downtime for a five-minute schedule does not
  replay hundreds of stale runs.
- **A newly deployed schedule does not fire for times before it existed.**
  The first time a worker sees a schedule with no tick history, it records
  the current tick as an anchor without enqueueing anything; the schedule
  first fires at its next tick after deployment.
- The recovery baseline is each schedule's most recent tick row, which is
  why `ox_prune` always preserves it (see
  [Configuration](configuration.md#ox_prune)).

If your recurring job genuinely needs every interval processed, encode the
interval in the task's arguments or derive it from your data inside the
task; the scheduler guarantees at most one enqueue per tick, not a replay
of history.

## Validation

Misconfigured schedules fail fast, in two places: the worker refuses to
start, and `manage.py check` reports `django_ox.E002`. Checked at load
time: the task path imports and is a `django.tasks` Task, the cron
expression parses and can fire, `args`/`kwargs` are JSON-serializable, and
`queue_name`/`priority` overrides pass the backend's own task validation.
A schedule name that appears on more than one backend is reported
separately as `django_ox.E003`.

One caveat: the `task_enqueued` signal fires inside the dispatch
transaction. If the tick loses the unique-constraint race and rolls back,
the signal has already fired for a task that never committed. Django core
gives no guidance on signals inside rolled-back transactions; treat
`task_enqueued` as advisory if you listen to it.
