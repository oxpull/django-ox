# Configuration

Everything django-ox reads lives in the standard `TASKS` setting, plus
three management commands. A full entry with every option spelled out:

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,
            "LOCK_TIMEOUT": 300,
            "BACKOFF_INITIAL": 5,
            "BACKOFF_MAX": 600,
            "TASK_TIMEOUT": None,
            "TASK_TIMEOUTS": {},
            "TASK_TIMEOUT_GRACE": 30,
            "SCHEDULES": {},  # see the Recurring tasks page
        },
    }
}
```

## The smallest working entry

Every option has a default. This is enough to run tasks:

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}
```

That gives you the `default` queue, three attempts, and a five second first
retry. Add options when you have a reason to.

## Backend entry

| Key | Default | Meaning |
| --- | --- | --- |
| `BACKEND` | required | `"django_ox.backend.OxBackend"`. |
| `QUEUES` | `["default"]` | Queue names tasks may be enqueued to. An empty list (`[]`) allows any queue name. Read by Django's Tasks framework itself. |
| `OPTIONS` | `{}` | Backend options, below. |

## OPTIONS

| Key | Default | Meaning |
| --- | --- | --- |
| `MAX_ATTEMPTS` | `3` | Claims a task gets before it is marked FAILED. The count is claims rather than invocations: it goes up in the statement that hands the task to a worker, before the function is reached. That is what keeps retries bounded when a worker dies mid-run, and it is what lets a task that loses its worker between the claim and the call use an attempt without running. See [Attempts count claims](production.md#attempts-count-claims). |
| `LOCK_TIMEOUT` | `300` | Seconds a RUNNING task's lock may go unrefreshed before the reaper takes the task back. A worker refreshes the lock every `LOCK_TIMEOUT / 3` seconds while it is executing, so this is a limit on how long a worker may be unresponsive, not on how long a task may run. |
| `BACKOFF_INITIAL` | `5` | Delay in seconds before the first retry. |
| `BACKOFF_MAX` | `600` | Ceiling on the retry delay, in seconds. |
| `TASK_TIMEOUT` | `None` | Seconds one attempt may run. `None` means no limit. At the deadline the worker raises `django_ox.exceptions.TaskTimeout` inside the task, on the task's own thread, and records the attempt as failed: retried on the usual backoff, or FAILED when attempts are spent. An async task is cancelled at the deadline instead. A sync task on a thread a coverage tool or debugger is watching is left alone, and `TASK_TIMEOUT_GRACE` is the whole enforcement for it. See [Task timeouts](production.md#task-timeouts). |
| `TASK_TIMEOUTS` | `{}` | Per-queue timeouts, `{"queue name": seconds}`. A queue in the mapping uses its own value instead of `TASK_TIMEOUT`; `None` as a value exempts that queue. Every key must be a queue named in `QUEUES`, unless `QUEUES` is `[]`. Per queue rather than per task because `django.tasks` gives a task no field a backend could read a timeout from, and a queue is its unit of routing. |
| `TASK_TIMEOUT_GRACE` | `30` | Seconds a timed-out attempt gets to stop. A thread still running after that is treated as stuck, which usually means it is in a call that never returns to Python, where the exception cannot land: the worker records the attempt as failed, stops claiming, drains its other tasks and exits with code 75 so its supervisor restarts it. A task that catches `TaskTimeout` has the same deadline to return or raise, and so does a task on a watched thread, where nothing was raised at all. |
| `WORKER_CLASS` | unset | Dotted path of a `django_ox.worker.Worker` subclass for `ox_worker` to run, on every process it starts. See [Stability](stability.md). |
| `SCHEDULES` | `{}` | Recurring task definitions. Documented on the [Recurring tasks](recurring-tasks.md) page. |
| `SCHEDULE_SOURCE` | settings | Dotted path to the class a worker asks for its active schedules. Set it to `django_ox.stored.DatabaseScheduleSource` to read them from the database. See [Schedules in the database](stored-schedules.md). |
| `SCHEDULE_RECONCILE_INTERVAL` | `60.0` | Seconds between full reads of the stored schedules, whether or not anything is known to have changed. The backstop for a row written without `django_ox.stored`. |
| `SCHEDULABLE_TASKS` | `{}` | Tasks a stored schedule may name, as `{key: dotted path}` or `{key: {"task": ..., "form": ..., "permission": ...}}`. The alternative to the `@schedulable` decorator. |

The retry delay after attempt *n* fails is
`BACKOFF_INITIAL * 2 ** (n - 1)`, capped at `BACKOFF_MAX`. With the
defaults: 5 s, 10 s, 20 s, 40 s, and so on up to 600 s. There is no
jitter.

## ox_worker

```
python manage.py ox_worker [options]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--backend` | `default` | Backend alias from the `TASKS` setting. |
| `--queues` | all configured queues | Comma-separated queue names this worker processes. |
| `--concurrency` | `1` | Tasks executed concurrently, as a thread pool inside each worker process. |
| `--processes` | `1` | Worker processes to run. At `1` the command is the worker. Above `1` it supervises that many copies of itself, each a full worker with its own connections, lease renewal, reaper and `--concurrency` thread pool, so `--processes 2 --concurrency 4` runs eight tasks at once. See [Threads and processes](production.md#threads-and-processes). |
| `--interval` | `1.0` | Polling interval in seconds when idle. When tasks are in flight the worker wakes as soon as one finishes, so this does not bound throughput. |
| `--lock-timeout` | backend `LOCK_TIMEOUT`, or 300 | Seconds a RUNNING task's lock may go unrefreshed before the task is reclaimed. |
| `--database` | the alias `OxTask` writes to | Database alias to run against. Each `--processes` child is given the same one, so one router answering differently in two processes can't split a fleet across two databases. It is not checked against the router; see [Read replicas](#read-replicas). |

The command also honors Django's standard `-v/--verbosity`: at the default
verbosity it logs worker lifecycle and warnings to stderr, and `-v 2`
enables debug logging. `-v 0` attaches no log handler. With `--processes`
above 1 every flag is passed on to each worker process unchanged, including
`--settings` and `--pythonpath`, and each worker process is started the way
the supervisor was (`manage.py` by absolute path, or `python -m django`), so
the command works from any working directory.

Two intervals are derived rather than flagged:

- The reaper runs every `min(30, max(lock_timeout / 2, 1))` seconds.
- Lease renewal runs every `max(lock_timeout / 3, 0.1)` seconds, on its own
  thread, and keeps running until the last in-flight task has drained.
- Schedule dispatch runs every `max(1, min(interval, 30))` seconds, about once
  a second at the default polling interval. It runs whether or not any schedule
  is configured, because a source that reads the database can gain one at any
  time; a pass with no schedules configured at all returns on a list check,
  before any query.

### Routing a queue to its own worker

Declare every queue on the backend, then give each worker a subset. Slow work
stops blocking fast work without a second backend or a second database.

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default", "emails", "exports"],
    }
}
```

```
python manage.py ox_worker --queues default,emails --concurrency 4
python manage.py ox_worker --queues exports --concurrency 1 --lock-timeout 3600
```

The exports worker runs one task at a time and tolerates hour-long jobs. The
other worker keeps short work moving at four at a time. A queue with no worker
assigned to it accumulates tasks and never runs them, so make sure every queue
in `QUEUES` is covered by some worker.

### Tasks that run longer than the lock timeout

A long task is not by itself a problem. A worker schedules lock renewal for
the tasks it is running every `LOCK_TIMEOUT / 3` seconds. An hour-long task
keeps its lease while renewals succeed. Renewal needs a database connection,
so a live worker that cannot get one can still lose the lease; see
[PostgreSQL pooling](production.md#database-connections-and-postgresql-pooling).

What `LOCK_TIMEOUT` bounds is how long a worker may stop refreshing before its
work is handed to somebody else. Set it above the longest pause you are
willing to tolerate from a worker: a long garbage-collection pause, a
throttled container, a slow database, a host that swapped. If a queue runs on
hardware that stalls, give it its own worker with its own timeout rather than
raising the global value and delaying recovery for everything else:

```
python manage.py ox_worker --queues exports --lock-timeout 7200
```

If you embed the worker programmatically, `django_ox.worker.Worker`
accepts `reap_interval`, `renew_interval`, `schedule_interval`,
`backoff_initial`, `backoff_max`, `task_timeout` and `task_timeout_grace`
keyword overrides, which have no flag; they win over the `OPTIONS` values.

## ox_prune

Finished task rows stay in the table until pruned; the queue table doubles
as the result store, and django-ox does not guess at your retention
needs. Run `ox_prune` on your own schedule (cron or a systemd timer;
there is an example unit on the [Production](production.md#pruning-on-a-timer)
page):

```
python manage.py ox_prune --older-than 7d
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Delete only this queue's task rows, so queues with different retention needs can be pruned separately. Same name and meaning as `ox_health --queue`. Old schedule ticks are still pruned for every schedule. |
| `--older-than` | `7d` | Minimum time since the task finished. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--include-failed` | off | Also delete FAILED and LOST rows. By default they are kept, because they hold the per-attempt tracebacks and can be retried. |
| `--batch-size` | `1000` | Rows per DELETE statement, so pruning a large table never takes a long lock or builds a giant IN clause. Must be at least 1. |
| `--dry-run` | off | Report how many rows would be deleted without deleting any. |
| `--format` | `text` | `json` prints one object on stdout instead of the two report lines: `queue`, `cutoff`, `statuses`, `task_rows`, `tick_rows` and `dry_run`. `queue` is `null` when no `--queue` is given, `cutoff` is ISO 8601, and `statuses` is a list. On a database error during deletion, the object is printed too, with counts of rows already deleted in committed batches, before the same non-zero exit. |
| `--database` | the alias `OxTask` writes to | Database alias to prune. The rows it reads and the rows it deletes are on that one alias. |

Only SUCCESSFUL and DISCARDED rows (and, with `--include-failed`, FAILED and
LOST rows) whose `finished_at` is past the cutoff are deleted. READY, WAITING
and RUNNING rows are never touched, whatever their age. Rows from the
recurring-schedule tick
log are pruned with the same cutoff, always keeping each schedule's most
recent tick; that row anchors missed-tick recovery and deleting it would
make the schedule re-anchor. The latest tick row of a schedule that has
been removed from settings is kept by the same rule; such rows are
harmless and can be deleted by hand if unwanted. See
[Recurring tasks](recurring-tasks.md#missed-ticks).

`--queue` narrows the task rows, not the tick log. A run for one queue
prunes every schedule's old ticks at its own cutoff, so when queues are
pruned separately, the shortest `--older-than` decides how much tick
history stays.

`ox_prune` deletes task rows one batch at a time, and each batch commits by
itself. It checks each batch again under a lock before it deletes it, and it
takes those locks in primary key order. A batch that hits a deadlock or a
serialization failure runs again in a new transaction, three attempts in all.
If it still fails, `ox_prune` stops and exits non-zero. The batches it deleted
before that stay deleted. Run it again and it deletes the rest. Called inside
a transaction of your own, it doesn't retry, and the error reaches you.

## ox_health

A health check for cron alerting and container probes: exits 0 when
every enabled check passes, non-zero with a one-line reason otherwise.
With no flags it verifies only that the database answers.

```
python manage.py ox_health --max-backlog 1000 --max-age 600
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Restrict the checks to one queue. |
| `--format` | `text` | `json` prints one object on stdout instead of the `OK:` line: `ok`, `queue`, `backlog`, `oldest_age_seconds`, `last_claim_age_seconds` and `problems`. `queue` is `null` when no `--queue` is given. The figures are `null` when there is nothing to measure or the check could not run, as with an unreachable database or an invalid threshold. The object is printed on failure too, before the same non-zero exit. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Tasks deferred to a future `run_after` do not count. |
| `--max-age` | off | Fail when a READY task has been eligible to run for longer than this. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this long, or no claim was ever recorded. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--database` | the alias `OxTask` writes to | Database alias to check. The figures come from that alias, so the check reports the queue your workers are running. |

Check semantics, probe examples, and guidance on which check fits which
alert are on the
[Monitoring](monitoring.md#health-checks-ox_health) page.

## System checks

`manage.py check` validates the setup:

- `django_ox.E001`: `django_ox` is missing from `INSTALLED_APPS`. The app
  itself registers these checks with Django, so a project that also lacks
  any other import of `django.tasks` gets no `E001` from `manage.py check`;
  the reliable symptom is `Unknown command: 'ox_worker'`, because the
  command ships with the app.
- `django_ox.E002`: a `SCHEDULES` entry is invalid (task path does not
  import, cron expression does not parse or can never fire, arguments not
  JSON-serializable, bad queue name or priority).
- `django_ox.E003`: the same schedule name is defined on more than one
  backend; schedule names must be unique across backends.
- `django_ox.E004`: `TASK_TIMEOUT`, a `TASK_TIMEOUTS` value or
  `TASK_TIMEOUT_GRACE` is not a positive, finite number of seconds, at most
  a thousand years (the first two may also be `None`, which means no limit;
  `float("inf")` does not), or `TASK_TIMEOUTS` is not a mapping keyed by
  queue name. Every bad value is reported in one run.
- `django_ox.E005`: a `TASK_TIMEOUTS` key names a queue that is not in
  `QUEUES`, so the entry would never apply.
- `django_ox.E006`: `SCHEDULE_SOURCE` does not name a class that can be
  built and answers `schedules()`. Without this a worker would start,
  dispatch nothing and report nothing.
- `django_ox.E007`: a `SCHEDULABLE_TASKS` entry is invalid (task or form
  path does not import, the form is not an `ArgsForm`, unknown keys, or a
  key already registered for a different task).
- `django_ox.E009` / `django_ox.W002`: two configured schedule names differ
  only by case. The tick log decides identity with its column's collation, so
  on a case-insensitive one, MySQL's default among them, the two share a key:
  their ticks collide and one schedule stops running. Refused on MySQL,
  reported as a warning elsewhere, because the same settings deployed against
  MySQL would starve one of the two.
- `django_ox.W001`: `USE_TZ` is off and `TIME_ZONE` puts the clock back once
  a year. Tick times are stored against the wall clock, so the repeated hour
  has one label for two instants: an interval schedule loses about half its
  runs for the length of it, and a cron schedule inside it fires once rather
  than twice. Set `USE_TZ = True`, or a zone with no transition.
- `django_ox.E008`: a database router sends the django-ox models to more
  than one database. A task row and its schedule tick row are written in one
  transaction, which is what makes a due tick enqueue once, so they have to
  live on the same database. Route the `django_ox` app to a single one; it
  need not be the default.
- `django_ox.E010`: `LOCK_TIMEOUT`, `BACKOFF_INITIAL` or `BACKOFF_MAX` is not a
  positive, finite number of seconds. The worker reads all three, so the check stops a bad value at deploy time.
- `django_ox.W003`: `BACKOFF_INITIAL` and `BACKOFF_MAX` are both set to valid
  numbers and the initial delay is above the cap. Every retry then waits
  `BACKOFF_MAX`, so the configured first delay never takes effect. The
  configuration still runs; lower the initial delay or raise the cap.

The worker performs the same schedule and timeout validation at startup, so
a bad deploy fails loudly rather than skipping dispatches.

## Read replicas

Under a router that sends reads to a replica, django-ox reads its own rows
on the alias it writes them to. That covers `django_ox.stats`, the metrics
renderings and the endpoint, `django_ox.actions`, `get_result()`,
`enqueue()` and `enqueue_many()`, the worker, the reaper, the stored
schedules, and both admins. A replica is behind by design, and each of
those reads is a read of something just written.

The admin has no way out of that. Every page reads the primary, and no
setting changes it, because the admin writes back what it read. A change
form submits every field, including the ones nobody edited, so a form
built from a replica overwrites newer values on the primary. Nothing
raises and nothing is logged. Expect the admin's read load on the database
your workers use.

What you can point at a replica is what you ask for by name. `stats`,
`collect()`, both metrics renderings and the metrics view take `using`:

```python
path("ox/metrics", metrics, {"using": "replica"})  # scrape a replica
```

A replica that is behind reports the queue as it was, which is the trade.
Your own queries are untouched: a router you wrote still sends your reads
of the task table where you send them.

### Two things django-ox cannot pin for you

**Your own `ModelAdmin` with a foreign key to `OxTask` or `OxSchedule`.**
Django builds that field from `db_for_read` twice: once for the choices
the select offers, and again when the model validates what was posted. On
a lagging replica the select offers no row the replica has not seen.
Pinning the form field alone does not help: the second check still reads
the replica. If your project has such a field, have your router
answer the write alias for django-ox's models. django-ox reads its own
rows there anyway, so nothing else moves.

**`ox_worker --database`.** The flag is not checked against the router. A
worker pointed at another alias works on that one alone, while the admin,
`stats` and `ox_health` without the flag read the router's alias.
`django_ox.E008` constrains routers, not this flag. Leave it unset unless
you mean it.
