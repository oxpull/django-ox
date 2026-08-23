<img src="https://oxpull.com/django-ox/assets/lockup.png" alt="django-ox" width="380">

[![PyPI](https://img.shields.io/pypi/v/django-ox)](https://pypi.org/project/django-ox/)
[![CI](https://github.com/oxpull/django-ox/actions/workflows/ci.yml/badge.svg)](https://github.com/oxpull/django-ox/actions/workflows/ci.yml)
[![Python versions](https://img.shields.io/pypi/pyversions/django-ox)](https://pypi.org/project/django-ox/)
[![License](https://img.shields.io/pypi/l/django-ox)](https://github.com/oxpull/django-ox/blob/main/LICENSE)

A database-backed worker backend for Django's Tasks framework (`django.tasks`, Django 6.0+).

Documentation: <https://oxpull.com/django-ox/>

Django 6.0 ships the Tasks API but no production backend: the built-in
`ImmediateBackend` and `DummyBackend` are for development and testing only.
django-ox stores background tasks in the database you already run and executes
them with a worker process. There is no broker to provision, secure, upgrade or
back up, and because `enqueue()` is an INSERT on your default connection, a
task enqueued inside `transaction.atomic()` commits or rolls back with your
data. No `transaction.on_commit()` needed.
Comparing backends? See [Choosing a task backend](https://oxpull.com/django-ox/choosing/).

## Install

Requires Python 3.12+ and Django 6.0+.

```
pip install django-ox
```

```python
INSTALLED_APPS = [
    # ...
    "django_ox",
]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}
```

```
python manage.py migrate django_ox
python manage.py ox_worker
```

Tasks are plain `django.tasks` tasks; django-ox adds nothing to learn on the
producer side. The worker is a separate process, and tasks run only while one
is running. Every option and flag is on the
[Configuration](https://oxpull.com/django-ox/configuration/) page.

## One fewer service to run

A broker-based task queue adds a second datastore to your deployment. Redis or
RabbitMQ has to be provisioned, monitored, secured and upgraded, and it has to
be running before a single task executes. For an application that already
depends on a database, that is a full operational surface added for one feature.

django-ox uses the database you already run. A deployment is your application,
a worker process, and one migration. Backups already cover the queue, because
the queue is a table.

## Transactional enqueue

`enqueue()` is a single INSERT on your default database connection, so it
participates in the caller's open transaction. A task enqueued inside
`transaction.atomic()` becomes visible to workers only when the transaction
commits, and disappears on rollback. There is no window where business data
exists without its task, or a task without its data, and no
`transaction.on_commit()` boilerplate. Execution is at-least-once: workers
claim tasks with `SELECT ... FOR UPDATE SKIP LOCKED` on databases that support
it (PostgreSQL, MySQL 8+) and an atomic compare-and-set UPDATE elsewhere
(including SQLite), and a reaper returns tasks whose worker died to the queue.
Failed tasks retry with exponential backoff up to a configurable attempt
limit, keeping the full traceback of every attempt.

## Configuration

Every option has a default; add one when you have a reason to.

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],  # [] allows any queue name
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,  # executions per task before FAILED
            "LOCK_TIMEOUT": 300,  # seconds before a dead worker's task is reclaimed
            "BACKOFF_INITIAL": 5,  # first retry delay, seconds; doubles per attempt
            "BACKOFF_MAX": 600,  # retry delay ceiling, seconds
        },
    }
}
```

## Quickstart

```python
from django.tasks import task


@task
def send_welcome_email(user_id): ...


result = send_welcome_email.enqueue(user_id=42)
result.refresh()  # later: status, return_value, errors
```

Run a worker:

```
python manage.py ox_worker
```

## Worker CLI

| Flag | Default | Meaning |
| --- | --- | --- |
| `--backend` | `default` | Backend alias from the `TASKS` setting. |
| `--queues` | all configured queues | Comma-separated queue names to process. |
| `--concurrency` | `1` | Tasks executed concurrently (thread pool). |
| `--interval` | `1.0` | Polling interval in seconds when idle. |
| `--lock-timeout` | backend `LOCK_TIMEOUT` | Seconds before a stuck task is reclaimed. |

On SIGTERM or SIGINT the worker stops claiming, finishes in-flight tasks, then
exits. A second signal forces an immediate exit.

## Pruning

Finished task rows stay in the table until pruned. Run `ox_prune` on your
own schedule (cron, systemd timer):

```
python manage.py ox_prune --older-than 7d
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--older-than` | `7d` | Minimum time since the task finished. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--include-failed` | off | Also delete FAILED and LOST rows. By default they are kept: they hold the per-attempt tracebacks and can be retried. |
| `--batch-size` | `1000` | Rows per DELETE statement, so pruning a large table never takes a long lock or builds a giant IN clause. |
| `--dry-run` | off | Report how many rows would be deleted without deleting any. |

Only SUCCESSFUL and DISCARDED rows (and, with `--include-failed`, FAILED and
LOST rows) past the cutoff are deleted. READY and RUNNING rows are never touched, whatever their
age. Old rows from the recurring-schedule tick log are cleared with the same
cutoff, always keeping each schedule's most recent tick.

## Health and monitoring

`django_ox.stats` exposes queue metrics as plain functions, each a single
ORM query: per-queue status counts, backlog depth and age, throughput,
and failure rate. The `ox_health` command turns thresholds on those
numbers into an exit code for cron alerting and container probes:

```
python manage.py ox_health --max-backlog 1000 --max-age 600
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Restrict the checks to one queue. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. |
| `--max-age` | off | Fail when the oldest waiting task has waited longer than this many seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this many seconds. |

When `django.contrib.admin` is installed, the task table is registered with
it: a filterable list, a read-only detail page with every attempt's
traceback, and **Retry selected tasks** and **Discard selected tasks**
actions. The same two operations are `django_ox.actions.retry(result_id)`
and `django_ox.actions.discard(result_id)`. A retry is one more attempt on
a FAILED or LOST task; a discard closes a READY, FAILED or LOST task without
running it. Neither touches a running task.

Worker lifecycle events (claim, start, success, retry, failure, reclaim,
shutdown) log to the `django_ox` logger with stable extra keys (task id,
queue, attempt, duration), ready for JSON log handlers.

## Recurring tasks

Schedules are declared in settings, next to the backend they enqueue
through, and deploy with your code. There are no rows to edit by hand and
no separate scheduler process to keep alive:

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
through the ordinary queue: retries, backoff, priorities and the result
store all apply unchanged. Every running worker doubles as the scheduler,
and a unique constraint on (schedule name, tick time) makes each tick fire
exactly once however many workers are polling.

| Key | Required | Meaning |
| --- | --- | --- |
| `task` | yes | Dotted path to a `@task` callable, e.g. `"reports.tasks.build_report"`. |
| `cron` | yes | Five-field cron expression. |
| `args`, `kwargs` | no | JSON-serializable arguments passed to each enqueue. |
| `queue_name` | no | Queue override; defaults to the task's own queue. |
| `priority` | no | Priority override (-100 to 100). |

Cron expressions use the classic five-field syntax: `*`, lists (`1,15`),
ranges (`mon-fri`), steps (`*/15`), month and weekday names, 0 or 7 for
Sunday, and the `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly`
shortcuts. When both day-of-month and day-of-week are restricted, a day
matches if either field does, as in vixie cron. Times are wall-clock in
your `TIME_ZONE`.

Misconfigured schedules (a task path that does not import, an expression
that can never fire) fail at worker startup and in `manage.py check`, not
silently at dispatch time.

Missed ticks: if every worker was down when a tick passed, the latest
missed tick fires once on recovery and older ones are skipped, so a
nightly job still runs after an unlucky deploy window but a backlog never
stampedes. A newly deployed schedule waits for its next tick rather than
firing for a time before it existed.

## Behavior details

- `run_after` (deferred tasks), `priority` (-100 to 100, higher runs first),
  `get_result()` and the async variants are all supported; the backend
  declares `supports_defer`, `supports_priority`, `supports_get_result` and
  `supports_async_task` accordingly.
- Retry state is visible in the database: attempts, per-attempt tracebacks,
  and the next scheduled run (`run_after`).
- Because execution is at-least-once, tasks should be idempotent. A task is
  retried both when it raises and when its worker dies mid-run.
- Concurrency uses a thread pool. That fits I/O-bound tasks (email, HTTP,
  ORM); for CPU-bound work, run multiple worker processes with
  `--concurrency=1` instead.

## Scope and roadmap

The core is deliberately small: a durable queue, a worker, recurring
schedules, monitoring, and nothing else to operate. Outside the current
scope: stopping a task that is already running, and multi-database routing
(tasks are stored on the default database for the model).

Batches and unique tasks ship in [Oxpull Pro](https://oxpull.com/django-ox/pro/);
the waitlist is at <https://oxpull.com/>. Metrics stay in this package:
`django_ox.stats` and `ox_health` are free and stay free. Rate limiting is in
neither tier.

## Stability

What counts as public API, the pre-1.0 versioning and deprecation policy,
and the supported Python and Django versions are documented in
[the stability policy](https://oxpull.com/django-ox/stability/).

## License

BSD 3-Clause.
