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
| `MAX_ATTEMPTS` | `3` | Executions a task gets before it is marked FAILED. An attempt is consumed when a worker claims the task, so a worker dying mid-run counts too and retries stay bounded. |
| `LOCK_TIMEOUT` | `300` | Seconds a RUNNING task's lock may go unrefreshed before the reaper takes the task back. A worker refreshes the lock every `LOCK_TIMEOUT / 3` seconds while it is executing, so this is a limit on how long a worker may be unresponsive, not on how long a task may run. |
| `BACKOFF_INITIAL` | `5` | Delay in seconds before the first retry. |
| `BACKOFF_MAX` | `600` | Ceiling on the retry delay, in seconds. |
| `TASK_TIMEOUT` | `None` | Seconds one attempt may run. `None` means no limit. At the deadline the worker raises `django_ox.exceptions.TaskTimeout` inside the task, on the task's own thread, and records the attempt as failed: retried on the usual backoff, or FAILED when attempts are spent. An async task is cancelled at the deadline instead. See [Task timeouts](production.md#task-timeouts). |
| `TASK_TIMEOUTS` | `{}` | Per-queue timeouts, `{"queue name": seconds}`. A queue in the mapping uses its own value instead of `TASK_TIMEOUT`; `None` as a value exempts that queue. Every key must be a queue named in `QUEUES`, unless `QUEUES` is `[]`. Per queue rather than per task because `django.tasks` gives a task no field a backend could read a timeout from, and a queue is its unit of routing. |
| `TASK_TIMEOUT_GRACE` | `30` | Seconds a timed-out attempt gets to stop. A thread still running after that is treated as stuck, which usually means it is in a call that never returns to Python, where the exception cannot land: the worker records the attempt as failed, stops claiming, drains its other tasks and exits with code 75 so its supervisor restarts it. A task that catches `TaskTimeout` has the same deadline to return or raise. |
| `SCHEDULES` | `{}` | Recurring task definitions. Documented on the [Recurring tasks](recurring-tasks.md) page. |

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
- Schedule dispatch (when `SCHEDULES` is configured) runs every
  `max(1, min(interval, 30))` seconds, about once a second at the default
  polling interval.

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

A long task is not by itself a problem. A worker refreshes the lock on the
tasks it is running every `LOCK_TIMEOUT / 3` seconds, so an hour-long task on
a healthy worker keeps its lease for the hour.

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
keyword overrides in addition to the flag equivalents.

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
| `--older-than` | `7d` | Minimum time since the task finished. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--include-failed` | off | Also delete FAILED and LOST rows. By default they are kept, because they hold the per-attempt tracebacks and can be retried. |
| `--batch-size` | `1000` | Rows per DELETE statement, so pruning a large table never takes a long lock or builds a giant IN clause. Must be at least 1. |
| `--dry-run` | off | Report how many rows would be deleted without deleting any. |

Only SUCCESSFUL and DISCARDED rows (and, with `--include-failed`, FAILED and
LOST rows) whose `finished_at` is past the cutoff are deleted. READY and RUNNING rows are
never touched, whatever their age. Rows from the recurring-schedule tick
log are pruned with the same cutoff, always keeping each schedule's most
recent tick; that row anchors missed-tick recovery and deleting it would
make the schedule re-anchor. The latest tick row of a schedule that has
been removed from settings is kept by the same rule; such rows are
harmless and can be deleted by hand if unwanted. See
[Recurring tasks](recurring-tasks.md#missed-ticks).

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
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Tasks deferred to a future `run_after` do not count. |
| `--max-age` | off | Fail when the oldest waiting task has waited longer than this many seconds since becoming eligible. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this many seconds, or no claim was ever recorded. |

Check semantics, probe examples, and guidance on which check fits which
alert are on the
[Monitoring](monitoring.md#health-checks-ox_health) page.

## System checks

`manage.py check` validates the setup:

- `django_ox.E001`: `django_ox` is missing from `INSTALLED_APPS`.
- `django_ox.E002`: a `SCHEDULES` entry is invalid (task path does not
  import, cron expression does not parse or can never fire, arguments not
  JSON-serializable, bad queue name or priority).
- `django_ox.E003`: the same schedule name is defined on more than one
  backend; schedule names must be unique across backends.
- `django_ox.E004`: `TASK_TIMEOUT`, a `TASK_TIMEOUTS` value or
  `TASK_TIMEOUT_GRACE` is not a positive number of seconds (the first two
  may also be `None`), or `TASK_TIMEOUTS` is not a mapping keyed by queue
  name.
- `django_ox.E005`: a `TASK_TIMEOUTS` key names a queue that is not in
  `QUEUES`, so the entry would never apply.

The worker performs the same schedule and timeout validation at startup, so
a bad deploy fails loudly rather than skipping dispatches.
