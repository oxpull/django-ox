<img src="https://oxpull.com/django-ox/assets/lockup.png" alt="django-ox" width="380">

[![PyPI](https://img.shields.io/pypi/v/django-ox)](https://pypi.org/project/django-ox/)
[![CI](https://github.com/oxpull/django-ox/actions/workflows/ci.yml/badge.svg)](https://github.com/oxpull/django-ox/actions/workflows/ci.yml)
[![Python versions](https://img.shields.io/pypi/pyversions/django-ox)](https://pypi.org/project/django-ox/)
[![License](https://img.shields.io/pypi/l/django-ox)](https://github.com/oxpull/django-ox/blob/main/LICENSE)

# Django tasks in your database

Stop running Redis to send an email. django-ox runs Django's Tasks framework in your existing database, where a dead worker doesn't mean lost tasks. Run a worker process, not a separate broker.

Supports Django 5.2 LTS through the `django-tasks` backport, and Django 6.0 / 6.1 through core `django.tasks`. Requires Python 3.12+.

## Install

```
pip install django-ox

# on Django 5.2 LTS
pip install "django-ox[backport]"
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

On Django 6.0+, use `from django.tasks import task`. On Django 5.2 LTS, use `from django_tasks import task`.

**[Follow the step-by-step guide](https://oxpull.com/django-ox/background-tasks/)** to build a working queue.

## Keep the work when a worker dies

Workers claim tasks with `SELECT ... FOR UPDATE SKIP LOCKED` on PostgreSQL and MySQL 8+, or an atomic compare-and-set UPDATE on SQLite. A reaper returns unfinished tasks to the queue when their worker dies. Failed tasks retry with exponential backoff up to `MAX_ATTEMPTS`, with every attempt's traceback kept.

Execution is at-least-once: make tasks safe to repeat.

Enqueue inside `transaction.atomic()` on the database holding `OxTask`, and the task commits or rolls back with your application data. No `transaction.on_commit()` needed.

Inspect attempts in Django admin, then retry or discard tasks there. Edit recurring schedules in admin or declare them in settings; workers dispatch them without a scheduler process.

For deployment probes, `ox_health` turns queue thresholds into an exit code, with `--format json` for structured output. Monitor through `django_ox.stats` or `/ox/metrics`, and clear finished rows with `ox_prune`. Use `--database` on `ox_worker`, `ox_prune` and `ox_health` to select the database alias.

In the [published benchmarks](https://oxpull.com/django-ox/benchmarks/), 2,000 of 2,000 tasks finished after 20 worker kills per trial, queue drain was up to 20% faster than django-tasks-db, and enqueueing 10,000 tasks took 0.71 s.

1,086 test functions, with CI covering Python 3.12 to 3.14, Django 5.2 to 6.1, PostgreSQL, MySQL and SQLite. Open source under the BSD 3-Clause licence.

Need to coordinate work across tasks? [Oxpull Pro](https://oxpull.com/) adds batches, unique tasks, rate limiting and workflows.

Documentation: <https://oxpull.com/django-ox/>

## One fewer service to run

A broker-based task queue adds a second datastore to your deployment. Redis or
RabbitMQ has to be provisioned, monitored, secured and upgraded, and it has to
be running before a single task executes. For an application that already
depends on a database, that is a full operational surface added for one feature.

django-ox uses the database you already run. A deployment is your application,
a worker process, and one migration. Backups already cover the queue, because
the queue is a table.

## Transactional enqueue

`enqueue()` is a single INSERT on the database that holds `OxTask`, your
default one unless a router sends it elsewhere. Open `transaction.atomic()`
on that database and the task becomes visible to workers only when the
transaction commits, and disappears on rollback. Rows you write to that
database in the same block go with it. There is no window where business data
exists without its task, or a task without its data, and no
`transaction.on_commit()` boilerplate. Execution is at-least-once: workers
claim tasks with `SELECT ... FOR UPDATE SKIP LOCKED` on databases that support
it (PostgreSQL, MySQL 8+) and an atomic compare-and-set UPDATE elsewhere
(including SQLite), and a reaper returns tasks whose worker died to the queue.
Failed tasks retry with exponential backoff up to a configurable attempt
limit, keeping the full traceback of every attempt.

## Measured

[The benchmarks page](https://oxpull.com/django-ox/benchmarks/) compares
django-ox with `django-tasks-db`, another database backend for the Tasks
framework, on one machine with both arms on Django core `django.tasks` and
PostgreSQL 16: backlog drain at two queue depths with matched worker
processes, enqueue latency, bulk enqueue, worker death under repeated SIGKILL
with restarts, and exception retry. Every figure there is a median over five
interleaved runs with its range, computed from the raw JSON files committed
beside the page, and every run is reported.

## How it compares

The four backends a Django team is most likely to shortlist. Every cell about
another project comes from that project's own documentation or issue tracker,
each carrying a link and the date it was read on the
[Choosing a task backend](https://oxpull.com/django-ox/choosing/) page.

| | django-ox | django-tasks-db | Celery | huey |
| --- | --- | --- | --- | --- |
| `django.tasks` backend | **Yes**, native | **Yes**, native | **No** | **Yes**, in `huey.contrib.djhuey` |
| Broker to run | **None.** The queue is a table in the database you already run | **None.** Django ORM | RabbitMQ, Redis or SQS | Redis, SQLite, PostgreSQL, file or memory |
| Transactional enqueue | **Yes.** Enqueue is one INSERT on your default database; a task written inside `atomic()` commits or rolls back with the rows beside it | Not claimed | **No.** Django's own docs name this as the case for `on_commit()` | Not claimed |
| Worker killed mid-task | **Retried.** The lease expires and the task goes back on the queue | **Stuck.** The task stays `PROCESSING`, never retried and never failed. Open since 2024-06-11 | **Lost** when the child process is killed, even with `acks_late` | **Lost.** "will not be retried automatically" |
| Retries and backoff | **Exponential**, keeping every attempt's traceback | **None** | Yes | Yes |
| Recurring schedules | **Cron or a fixed interval, and no scheduler process.** Editable in the Django admin, limited to the tasks your code exposes | **None** | `celery beat`, a separate process you must run exactly one of | Yes |

The full version has six more backends, a footnote and a date on every cell,
and a [section on when django-ox is the wrong choice](https://oxpull.com/django-ox/choosing/#when-not-to-use-django-ox).

## Configuration

Every option has a default; add one when you have a reason to.

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],  # [] allows any queue name
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,  # claims per task before FAILED
            "LOCK_TIMEOUT": 300,  # seconds a worker may stop renewing its lease
            "BACKOFF_INITIAL": 5,  # first retry delay, seconds; doubles per attempt
            "BACKOFF_MAX": 600,  # retry delay ceiling, seconds
        },
    }
}
```

## Quickstart

```python
from django.tasks import task  # Django 6.0+
# On Django 5.2 the Tasks framework comes from the backport:
# from django_tasks import task


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
| `--concurrency` | `1` | Tasks executed concurrently (thread pool). With Django's PostgreSQL pool, check [pool sizing](https://oxpull.com/django-ox/production/#database-connections-and-postgresql-pooling). |
| `--processes` | `1` | Worker processes under one supervisor. Each is a full worker with its own connections, reaper and `--concurrency` thread pool; budget database connections per process. A process that dies is restarted. POSIX only. |
| `--interval` | `1.0` | Polling interval in seconds when idle. |
| `--lock-timeout` | backend `LOCK_TIMEOUT` | Seconds a RUNNING task's lock may go unrefreshed before the task is reclaimed. |
| `--database` | the alias `OxTask` writes to | Database alias to run against. Every `--processes` child is given the same one. It is not checked against the router. |

On SIGTERM or SIGINT the worker stops claiming, finishes in-flight tasks, then
exits. A second signal forces an immediate exit. With `--processes` above 1,
send the signal to the supervisor; it forwards once and restarts a worker that
dies.

## Pruning

Finished task rows stay in the table until pruned. Run `ox_prune` on your
own schedule (cron, systemd timer):

```
python manage.py ox_prune --older-than 7d
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Delete only this queue's task rows, so queues with different retention needs can be pruned separately. |
| `--older-than` | `7d` | Minimum time since the task finished. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--include-failed` | off | Also delete FAILED and LOST rows. By default they are kept: they hold the per-attempt tracebacks and can be retried. |
| `--batch-size` | `1000` | Rows per DELETE statement, so pruning a large table never takes a long lock or builds a giant IN clause. |
| `--dry-run` | off | Report how many rows would be deleted without deleting any. |
| `--format` | `text` | `json` prints one object on stdout instead of the two report lines: `queue`, `cutoff`, `statuses`, `task_rows`, `tick_rows` and `dry_run`. `queue` is `null` when no `--queue` is given, `cutoff` is ISO 8601, and `statuses` is a list. On a database error during deletion, the object is printed too, with counts of rows already deleted in committed batches, before the same non-zero exit. |
| `--database` | the alias `OxTask` writes to | Database alias to prune. The rows it reads and the rows it deletes are on that one alias. |

Only SUCCESSFUL and DISCARDED rows (and, with `--include-failed`, FAILED and
LOST rows) past the cutoff are deleted. READY, WAITING and RUNNING rows are
never touched, whatever their age. Old rows from the recurring-schedule tick
log are cleared with the same
cutoff, always keeping each schedule's most recent tick, and `--queue` does
not narrow that.

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
| `--format` | `text` | `json` prints one object on stdout instead of the `OK:` line: `ok`, `queue`, `backlog`, `oldest_age_seconds`, `last_claim_age_seconds` and `problems`. `queue` is `null` when no `--queue` is given. The figures are `null` when there is nothing to measure or the check could not run, as with an unreachable database or an invalid threshold. The object is printed on failure too, before the same non-zero exit. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. |
| `--max-age` | off | Fail when a READY task has been eligible to run for longer than this. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this long. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--database` | the alias `OxTask` writes to | Database alias to check. The figures come from that alias, so the check reports the queue your workers are running. |

Mounting `path("ox/", include("django_ox.urls"))` exposes `GET /ox/metrics`,
the same numbers as Prometheus gauges; the view has no authentication of its
own.

When `django.contrib.admin` is installed, the task table is registered with
it: a filterable list, a read-only detail page with every attempt's
traceback, and **Retry selected tasks** and **Discard selected tasks**
actions. The same two operations are `django_ox.actions.retry(result_id)`
and `django_ox.actions.discard(result_id)`. A retry is one more attempt on
a FAILED or LOST task; a discard closes a READY, WAITING, FAILED or LOST task
without running it. Neither touches a running task.

Worker lifecycle events (claim, start, success, retry, failure, reclaim,
shutdown) log to the `django_ox` logger with stable extra keys (task id,
queue, attempt, duration), ready for JSON log handlers.

## Recurring tasks

Schedules are declared in settings, next to the backend they enqueue
through, so they deploy with your code. There is no separate scheduler
process to keep alive:

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
and a unique constraint on (schedule name, tick time) enqueues each due tick
once however many workers are polling. Execution stays at-least-once.

| Key | Required | Meaning |
| --- | --- | --- |
| `task` | yes | Dotted path to a `@task` callable, e.g. `"reports.tasks.build_report"`. |
| `cron` | one of | Five-field cron expression. |
| `every` | one of | A fixed interval, as a `timedelta` or seconds, counted from a fixed instant rather than from the last run. |
| `phase` | no | Shifts an `every` sequence. |
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

Schedules can also live in the database and be edited in the Django admin
without a deploy, for the cases where whoever needs to pause a job cannot
ship one. A row names a task the code has exposed rather than an import
path, so admin access does not become permission to run anything. See
[Schedules in the database](https://oxpull.com/django-ox/stored-schedules/).

## Behavior details

- `run_after` (deferred tasks), `priority` (-100 to 100, higher runs first),
  `get_result()` and the async variants are all supported; the backend
  declares `supports_defer`, `supports_priority`, `supports_get_result` and
  `supports_async_task` accordingly.
- Retry state is visible in the database: attempts, per-attempt tracebacks,
  and the next scheduled run (`run_after`).
- Because execution is at-least-once, tasks should be idempotent. A task is
  retried both when it raises and when its worker dies mid-run. The lease
  number stops two workers writing the same row; it does not stop two threads
  running the same task body, which is a property of every at-least-once
  queue. [What the lease guarantees, precisely](https://oxpull.com/django-ox/production/#what-the-lease-guarantees-precisely).
- Concurrency uses a thread pool. That fits I/O-bound tasks (email, HTTP,
  ORM); for CPU-bound work, run `--processes N --concurrency 1`, which is N
  worker processes under one supervisor.

## Scope

The core is finite on purpose: a durable queue, a worker, recurring
schedules, monitoring, and nothing else to operate. Outside the current
scope: interrupting one chosen running task on demand (every attempt can be
bounded with `TASK_TIMEOUT`).

django-ox keeps all its own tables on one database, the one your router
sends `OxTask` to. `django_ox.E008` reports a router that splits them.
Under a router that sends reads to a replica, django-ox reads its own rows
on the alias it writes them to. The admin has no way out of that: every
page reads the primary, and no setting changes it. `ox_worker`, `ox_prune`
and `ox_health` take `--database` to name the alias django-ox works on. It
defaults to the alias `OxTask` writes to, `default` unless you wrote a
router. The flag is not checked against the router: a worker pointed at
another alias works there and nothing warns, so leave it unset unless you
mean it. See
[Read replicas](https://oxpull.com/django-ox/configuration/#read-replicas).

Batches, unique tasks, rate limiting and workflows are in
[Oxpull Pro](https://oxpull.com/django-ox/pro/), a paid add-on; <https://oxpull.com/> has the details. Metrics stay in this
package: `django_ox.stats` and `ox_health` are free and stay free.

## Stability

What counts as public API, the versioning and deprecation policy,
and the supported Python and Django versions are documented in
[the stability policy](https://oxpull.com/django-ox/stability/).

## License

BSD 3-Clause.
