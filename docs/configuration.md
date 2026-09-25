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
| `MAX_ATTEMPTS` | `3` | Default claim budget for tasks that declare no `max_attempts`. Use an integer from 1 to 32767, not a bool. Previously accepted conversions, zero and larger values supported by the write database remain accepted with `django_ox.W004`; see System checks below. The task's value, or this default, is stored at enqueue and decides the budget from then on. Claims include the first and are counted before the function is reached. See [Attempts count claims](production.md#attempts-count-claims). |
| `LOCK_TIMEOUT` | `300` | Seconds a RUNNING task's lock may go unrefreshed before the reaper takes the task back. |
| `BACKOFF_INITIAL` | `5` | Initial delay in seconds for the worker's exponential backoff. Used when no task callback decides, including callback errors and task import failures. Validated at worker startup as well as by system checks. |
| `BACKOFF_MAX` | `600` | Ceiling on the worker's exponential retry delay, in seconds. It does not cap a valid delay returned by a task's backoff callback. Validated at worker startup as well as by system checks. |
| `TASK_TIMEOUT` | `None` | Default execution timeout in seconds per attempt. `None` means no limit. A task's `timeout` takes precedence, then its queue's `TASK_TIMEOUTS` entry. At the deadline the worker raises `django_ox.exceptions.TaskTimeout` inside a sync task on its own thread; an async task is cancelled. A recorded timeout follows the task's retry policy. A sync task on a thread a coverage tool or debugger is watching is left alone, and `TASK_TIMEOUT_GRACE` is the whole enforcement for it. See [Task timeouts](production.md#task-timeouts). |
| `TASK_TIMEOUTS` | `{}` | Per-queue timeouts, `{"queue name": seconds}`. A queue in the mapping uses its own value instead of `TASK_TIMEOUT`; `None` exempts tasks that declare no `timeout`. An explicit task timeout still applies on that queue. Every key must be a queue named in `QUEUES`, unless `QUEUES` is `[]`. `Task.using()` overrides only priority, queue name, run-after time, and backend. |
| `TASK_TIMEOUT_GRACE` | `30` | Seconds a timed-out attempt gets to stop. A thread still running after that is treated as stuck, which usually means it is in a call that never returns to Python, where the exception cannot land: the worker records the attempt as failed, stops claiming, drains its other tasks and exits with code 75 so its supervisor restarts it. A task that catches `TaskTimeout` has the same deadline to return or raise, and so does a task on a watched thread, where nothing was raised at all. This grace period is worker-wide, including for task-declared timeouts. |
| `WORKER_CLASS` | unset | Dotted path of a `django_ox.worker.Worker` subclass for `ox_worker` to run, on every process it starts. See [Stability](stability.md). |
| `SCHEDULES` | `{}` | Recurring task definitions. Documented on the [Recurring tasks](recurring-tasks.md) page. |
| `SCHEDULE_SOURCE` | settings | Dotted path to the class a worker asks for its active schedules. Set it to `django_ox.stored.DatabaseScheduleSource` to read them from the database. See [Schedules in the database](stored-schedules.md). |
| `SCHEDULE_RECONCILE_INTERVAL` | `60.0` | Seconds between full reads of the stored schedules, whether or not anything is known to have changed. The backstop for a row written without `django_ox.stored`. |
| `SCHEDULABLE_TASKS` | `{}` | Tasks a stored schedule may name, as `{key: dotted path}` or `{key: {"task": ..., "form": ..., "permission": ...}}`. The alternative to the `@schedulable` decorator. |

By default, a worker refreshes leases at an interval of
`max(LOCK_TIMEOUT / 3, 0.1)` seconds while executing tasks. `LOCK_TIMEOUT`
limits how long a worker can be unresponsive, not how long a task can run.
The `Worker` constructor's `renew_interval` argument overrides this interval.

Renewal ticks are scheduled from the start of the previous tick. After an
overrun, the next tick starts immediately and becomes the new anchor.
Ticks do not overlap or catch up missed slots. This intentionally changes
pooled and unpooled workers from a full-interval wait after each tick.

With Django's PostgreSQL pool, private renewal connection attempts use a
deadline of `min(5 seconds, renew_interval)`, shortened by a smaller positive
database `OPTIONS["connect_timeout"]`. Pool fallback waits at most 100 ms.

One private-connect deadline covers all hosts. Synchronous DNS can exceed
it. The deadline does not cover Django's post-connect setup queries or
renewal statements. It is not a deadline for the whole tick. See
[Database connections and PostgreSQL pooling](production.md#database-connections-and-postgresql-pooling)
for capacity requirements and timeout limits.

An idle pooled tick calls `renew_leases()` once without first opening a
private connection. The stock method returns 0 and opens no connection.
Custom `WORKER_CLASS` overrides retain their idle calls. An override that
queries can acquire a connection under the tick's connect deadline.

The worker's exponential retry delay after attempt *n* fails is
`BACKOFF_INITIAL * 2 ** (n - 1)`, capped at `BACKOFF_MAX`. With the
defaults: 5 s, 10 s, 20 s, 40 s, and so on up to 600 s. There is no
jitter.

This backoff applies when a task has no callback, its callback raises or
returns an invalid value, or its task cannot be imported. The stuck-thread
path also uses it without calling user code. A valid callback delay is not
capped by `BACKOFF_MAX`. The reaper requeues immediately when the stored
budget allows; it does not call callbacks.

## Per-task policy

On Django 6.1 or Django 5.2 with django-tasks 0.12+, a task declared
under an `OxBackend` alias can set its retry budget, backoff callback and
attempt timeout:

```python
from django.tasks import task


def retry_connection_errors(exception, task_result):
    if isinstance(exception, ConnectionError):
        return min(5 * 2 ** (task_result.attempts - 1), 300)
    return None


@task(max_attempts=5, backoff=retry_connection_errors, timeout=60)
def sync_customer(customer_id): ...
```

On Django 5.2, import `task` from `django_tasks` instead.
The backend builds a `django_ox.tasks.PolicyTask`, a frozen subclass of
the framework's `Task`. `PolicyTask` and the callback type alias
`BackoffCallback` are also available from `django_ox`.

These types and the three policy fields are provisional. They follow
Django new-features proposals #142 and #144, but do not promise
compatibility with whatever Django core eventually ships. See
[API stability](stability.md).

### Fields and validation

| Field | Accepted declaration | Meaning of `None` |
| --- | --- | --- |
| `max_attempts` | A non-bool integer from 1 to 32767, counting claims including the first | Use the backend's `MAX_ATTEMPTS`, defaulting to 3 |
| `backoff` | A synchronous callable taking `(exception, task_result)` | Use the worker's exponential backoff |
| `timeout` | A non-bool integer number of seconds, greater than zero and at most 31557600000 | Use the queue timeout, then the worker default |

Strings, floats and bools are not accepted for either integer field.
The compatibility handling for backend `MAX_ATTEMPTS` does not apply
to the new per-task field.

A declared timeout cannot disable an inherited limit: `None` inherits,
and zero is invalid. A declared timeout takes precedence even on a queue
whose `TASK_TIMEOUTS` entry is `None`.

Validation runs when the task is built, normally at module import, and
again when django-ox validates it, including enqueue and worker rebuild.
`.using()` and `dataclasses.replace()` also revalidate the task.
Invalid declarations raise the framework's `InvalidTask`
(`InvalidTaskError` on the backport). Multiple problems are joined into
one message.

These are the messages for representative invalid values:

```text
max_attempts must be a whole number from 1 to 32767, or None to use the backend's MAX_ATTEMPTS, got 0.
```

```text
backoff must be a callable taking (exception, task_result), or None to use the worker's exponential backoff, got 5.
```

```text
backoff must be a synchronous callable, got <function a at 0x...>. The worker calls it on the attempt's own thread and does not await what it returns.
```

```text
timeout must be a whole number of seconds greater than zero and at most 31557600000 (a thousand years), or None to use the queue's timeout, got 1.5.
```

The function representation in the synchronous-callable error varies.
Async functions, async generator functions, partials of either, and
instances with an async `__call__` are rejected. Passing a class does not
cause its call implementation to be inspected. Validation is not a
guarantee that a callable accepts the required arguments or returns a
valid delay; those failures use the
[callback fallback](production.md#backoff-callbacks).

### Precedence and persistence

| Policy | Precedence | When resolved |
| --- | --- | --- |
| Attempt budget | Task `max_attempts`, then backend `MAX_ATTEMPTS`, then 3 | At enqueue, stored on the row |
| Retry delay | Task `backoff`, then worker exponential backoff | After an eligible failed attempt |
| Attempt timeout | Task `timeout`, then `TASK_TIMEOUTS` for the row's queue, then worker `TASK_TIMEOUT` | For each attempt |

Worker constructor overrides supply the corresponding worker defaults.
An invalid backend `MAX_ATTEMPTS` configuration still prevents enqueue
and worker startup, even if a task declares its own valid budget.

The stored budget governs both workers and the reaper. Changing a task's
declaration does not change the budget of rows already queued.
An operator retry is a separate override: it grants one more claim by
setting the budget to `attempts + 1`, unless the claim count has already
reached 32767.

Backoff and timeout are read from the module-level task in the code the
worker runs. Deploying a new declaration therefore changes those policies
for queued rows, including rows enqueued before the feature was installed.

Each retry receives a fresh full timeout. `TASK_TIMEOUT_GRACE` remains
worker-wide, and `--lock-timeout` still controls leases rather than
execution deadlines. A task timeout longer than the lease relies on
lease renewal.

The row's `max_attempts` is fixed at enqueue unless an operator action
changes it. Workers and the reaper use that stored value. `result.task`
preserves the declared class and policy fields, with routing reconstructed
from the row. Its `max_attempts`, when present, is the declaration rather
than the stored budget. Re-enqueueing uses that declaration or, when it is
`None`, the backend's current value. `TaskResult.attempts` is the number of
claims already taken.

Legacy rows with a stored budget of zero remain readable, and workers
continue to use that stored budget. Task validation uses the declaration
when reading a legacy row, so otherwise-valid `using()` and
`dataclasses.replace()` calls remain supported.

`result.task.backoff` and `result.task.timeout` also describe the
live declaration loaded when the task is rebuilt, not a historical record
of an earlier attempt's policy.

### Task copies and backend changes

`Task.using()` still accepts only its standard options: `priority`,
`queue_name`, `run_after` and `backend`. It preserves the task's class
and policy, but cannot set the policy fields.

`dataclasses.replace()` can set policy fields on a `PolicyTask` and
revalidates them. Only a replaced `max_attempts` is persisted at enqueue.
Replacing `backoff` or `timeout` on a local task copy does not change what
the worker runs: the worker imports the module-level declaration.

Only a `PolicyTask` supplies policy fields. A plain framework `Task`, or
a task subclass from another library, rebound to django-ox with
`.using(backend=...)` inherits all three policies. Same-named attributes
on that object do not become django-ox policy declarations.

### Framework and typing compatibility

Django 6.0 supports django-ox but does not forward these decorator
keywords. For example, `@task(max_attempts=5)` fails at module import
with:

```text
TypeError: task() got an unexpected keyword argument 'max_attempts'
```

Changing the backend does not make Django 6.0 accept the keyword.
Bare `@task` still works under `OxBackend`, with all three policy fields
set to `None`.

On supported framework versions, use `OxBackend` or the policy-aware
[test backends](patterns.md#test-without-a-worker) when importing
declaring modules. Stock framework test backends do not accept these
declarations.

django-stubs 6.1.1 does not include the forwarded keywords in its `task`
overloads. On Django 6.1 with django-stubs 6.1.1, mypy does not recognise
the forwarded policy keywords. The following suppression also covers strict
mypy's `untyped-decorator` error:

```python
@task(max_attempts=5, timeout=60)  # type: ignore[call-overload, untyped-decorator]
def sync_customer(customer_id: int) -> None: ...
```

The suppression makes the decorated task's static type `Any`, so calls such
as `enqueue()` lose argument checking. It does not enable policy
declarations on Django 6.0 or on an unsupported backend. Django 5.2 with
django-tasks 0.12+ needs no ignore; adding one can fail checks for unused
ignores.

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
| `--heartbeat-file PATH` | off | Update a local file's modification time at the head of every poll and drain pass, before database work. Above one process, the supervisor writes `PATH.supervisor` and slot i writes `PATH.i`; `PATH` itself is not written. The directory must already exist, be writable and be private to the container. Failed updates warn once per writer and path and are retried without stopping the worker. See [heartbeat liveness](monitoring.md#local-heartbeat-files). |
| `--batch` | off | Exit once a poll pass succeeds, sees no task it can claim, and began with no task of its own in flight, then drain and exit 0. Tasks it can't claim at that moment stay READY for a later worker. These include future `run_after` tasks, backed-off retries, locked rows and tasks its claim filter excludes. A poll pass that hits a database error does not count. After an abandoned dispatch pass, no poll pass counts until a dispatch pass completes. Schedule-scoped failures do not prevent completion: exit 0 means the batch finished, not that every schedule enqueued. Rejected with `--processes` above 1. See [Running as a job](production.md#running-as-a-job). |
| `--max-tasks N` | none | Exit after claiming N task attempts, then drain and exit 0. Every claim counts, a failed attempt and a retry's repeat claim included, and concurrency never claims past N. Without `--batch` the worker keeps polling an empty queue until it reaches N or is stopped. N must be an integer of at least 1. Rejected with `--processes` above 1. |

The command also honors Django's standard `-v/--verbosity`: at the default
verbosity it logs worker lifecycle and warnings to stderr, and `-v 2`
enables debug logging. `-v 0` attaches no log handler. With `--processes`
above 1 every flag is passed on to each worker process unchanged, including
`--settings` and `--pythonpath`, and each worker process is started the way
the supervisor was (`manage.py` by absolute path, or `python -m django`), so
the command works from any working directory. For `--heartbeat-file`, each
child derives its own slot filename from the forwarded base path.

Use an absolute heartbeat path. Relative paths resolve against the worker's
working directory, which may differ from the probe's. An empty path exits 1
with `--heartbeat-file needs a path.` The flag combines with every other
worker flag, including `--batch` and `--max-tasks`; updates stop when the
worker exits.

These intervals are derived rather than flagged:

- The reaper runs every `min(30, max(lock_timeout / 2, 1))` seconds.
- Lease renewal runs every `max(lock_timeout / 3, 0.1)` seconds, on its own
  thread, and keeps running until the last in-flight task has drained.
- Schedule dispatch runs every `max(1, min(interval, 30))` seconds, about once
  a second at the default polling interval. It runs whether or not any schedule
  is configured, because a source that reads the database can gain one at any
  time; a pass with no schedules configured at all returns on a list check,
  before any query.
- With `--heartbeat-file`, the worker updates its file at the head of each poll and drain pass: about once per `--interval` when idle, and every 0.25 seconds while draining. The supervisor updates its own file every supervision pass, about every 0.1 seconds, and during shutdown.

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
accepts keyword overrides: `reap_interval`, `renew_interval` and
`schedule_interval` replace the derived intervals; `backoff_initial`,
`backoff_max`, `task_timeout` and `task_timeout_grace` replace
`BACKOFF_INITIAL`, `BACKOFF_MAX`, `TASK_TIMEOUT` and `TASK_TIMEOUT_GRACE`
in `OPTIONS`, respectively. These overrides are available through the
constructor.

`backoff_initial` and `backoff_max` set the worker's fallback backoff;
a task callback takes precedence. Constructor values must be finite
numbers from 0 to 31557600000 seconds, not strings or bools. Zero is
allowed here, unlike in `OPTIONS`. Validation runs at worker construction,
and an `OPTIONS` value replaced by an override is not read.

`task_timeout` replaces only the backend-wide `TASK_TIMEOUT`. A task's
`timeout` still wins, followed by its queue's `TASK_TIMEOUTS` entry.

`Worker` also accepts the keyword-only argument `heartbeat_file`, defaulting
to `None`. `ox_worker` passes it to `WORKER_CLASS` only when
`--heartbeat-file` is enabled. With multiple processes, each child receives
its own slot path. Existing fixed-signature constructors remain compatible
without the flag.

With `--heartbeat-file`, the constructor must accept `heartbeat_file`
explicitly or through `**kwargs`, and pass it to `Worker.__init__()`.
A constructor whose signature cannot accept the keyword is refused before
construction with exit code 1 and a one-line `CommandError`, without a
traceback. With multiple processes, the supervisor performs this check
before starting any children.

A constructor that accepts the keyword but does not pass it on is refused
after construction, before the worker loop runs, with exit code 1 and a
one-line `CommandError`. With multiple processes, this check runs in each
child. The supervisor restarts those children as for other crashes, and
the missing slot files keep the heartbeat probe failing.

Heartbeat updates belong to the base worker's poll and drain loops.
A custom worker that replaces `run()` without calling the base loop does
not get those updates merely by accepting `heartbeat_file`.

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

Use database checks for dependency monitoring and fleet alerting, and
`--heartbeat-file` for local controlling-loop liveness. The command exits 0
when every enabled check passes, non-zero with a one-line reason otherwise.
With no flags it verifies only that the database answers.

```
python manage.py ox_health --max-backlog 1000 --max-age 600
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Restrict the database checks to one queue. |
| `--format` | `text` | `json` prints one object on stdout instead of the `OK:` line, on success and failure. In database mode its fields are `ok`, `queue`, `backlog`, `oldest_age_seconds`, `last_claim_age_seconds` and `problems`. `queue` is `null` when no `--queue` is given. The figures are `null` when there is nothing to measure or the check could not run, as with an unreachable database or an invalid threshold. File mode uses the [heartbeat JSON object](monitoring.md#file-mode-json). The exit status is unchanged. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Tasks deferred to a future `run_after` do not count. |
| `--max-age` | off | Fail when a READY task has been eligible to run for longer than this. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this long, or no claim was ever recorded. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. Measures fleet claim activity, not per-worker liveness. |
| `--database` | the alias `OxTask` writes to | Database alias to check. The figures come from that alias, so the check reports the queue your workers are running. |
| `--heartbeat-file PATH` | off | Check the local file set written by `ox_worker --heartbeat-file PATH`, instead of the database. Reads metadata only, runs no system or migration checks, opens no database connection and constructs no task backend. Cannot be combined with `--database`, `--queue`, `--max-backlog`, `--max-age` or `--worker-timeout`. |
| `--max-heartbeat-age SECONDS` | `60` | Maximum heartbeat age, inclusive. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds, including fractions. Must be finite and strictly positive. Requires `--heartbeat-file`. |
| `--processes N` | `1` | Expected worker process count; must match the worker's `--processes`. At 1, check `PATH`. Above 1, require `PATH.supervisor` and every slot file from `PATH.0` through `PATH.(N-1)` to pass. Must be an integer of at least 1. Requires `--heartbeat-file`. |

File mode does not need `--skip-checks`. The command makes no database
calls, but project startup, including `AppConfig.ready()`, must also avoid
database access for a probe to survive a database outage. Database mode
keeps its alias-scoped checks and `--skip-checks` behavior.

An empty heartbeat path, a refused flag combination, a zero or negative
plain number of seconds for the maximum age, or a process count below 1
exits 1. A signed duration with a suffix, such as
`--max-heartbeat-age=-5s`, is an argparse error and exits 2 instead.
Non-finite or malformed durations and non-integer process counts are
argparse errors and exit 2. See
[heartbeat option validation](monitoring.md#options-and-refusals) for the
option rules.

The exit-1 refusal diagnostics are:

| Condition | Diagnostic |
| --- | --- |
| Empty heartbeat path | `CommandError: --heartbeat-file needs a path.` |
| Zero or negative plain number of seconds | `CommandError: --max-heartbeat-age must be a positive number of seconds.` |
| Process count below 1 | `CommandError: --processes must be at least 1.` |
| `--max-heartbeat-age` without `--heartbeat-file` | `CommandError: --max-heartbeat-age needs --heartbeat-file.` |
| `--processes` without `--heartbeat-file` | `CommandError: --processes needs --heartbeat-file.` |

Combining file mode with database or queue flags reports, for example:

```
CommandError: --heartbeat-file checks files, not the database, so it cannot be combined with --queue, --worker-timeout; run those checks as a separate ox_health.
```

The message lists every conflicting flag given, comma-separated, in this
order: `--database`, `--queue`, `--max-backlog`, `--max-age`,
`--worker-timeout`.

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
  JSON-serializable, bad queue name or priority). JSON serialization does
  not establish database acceptance. For example, `float("inf")` passes
  this check but is rejected at enqueue by PostgreSQL, MySQL and SQLite.
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
- `django_ox.E010`: `LOCK_TIMEOUT`, `BACKOFF_INITIAL` and `BACKOFF_MAX` must
  each be a positive, finite number of seconds, at most 31557600000
  seconds, a thousand years. Strings and bools are rejected.
- `django_ox.E011`: `MAX_ATTEMPTS` cannot be converted by `int()`, converts
  to a negative budget, or exceeds the write database's storage ceiling.
  These values could not be used successfully under the supported database
  settings. The ceilings are 32767 on PostgreSQL, 65535 on MySQL in strict
  mode, and 9223372036854775807 on SQLite.
- `django_ox.W003`: `BACKOFF_INITIAL` and `BACKOFF_MAX` are both set to valid
  numbers and the initial delay is above the cap. Every retry using the
  worker's exponential backoff then waits `BACKOFF_MAX`, so the configured
  first delay never takes effect. The configuration still runs; lower the
  initial delay or raise the cap.
- `django_ox.W004`: `MAX_ATTEMPTS` uses a deprecated value that remains
  accepted: a numeric string, float or bool converted by `int()`, zero, or
  a value above 32767 that the write database can store. Set it to an
  integer from 1 to 32767 that is not a bool. Strict enforcement is no
  earlier than the next major release.

For `MAX_ATTEMPTS`, compatibility conversions retain their existing
behaviour. For example, `"3"` and `3.7` give a budget of 3. A budget of 0
gives each task one attempt: a success is SUCCESSFUL and a failure is FAILED.
Above 32767, the ceiling comes from the database your router uses to write
`OxTask`. Other database vendors have no known ceiling for this check, so
values above 32767 produce `W004`, not `E011`. The MySQL ceiling assumes the
strict mode Django recommends; non-strict mode can clamp values instead.

`W004` is a system-check warning, not a runtime `DeprecationWarning`.
`manage.py check` succeeds with the warning, and `ox_worker` prints it and
starts. A process that skips checks gets no warning.

Backend construction stores the configured `MAX_ATTEMPTS` value. Validation
runs during system checks and on every read of `OxBackend.max_attempts`.
Reading `OxBackend.max_attempts`, enqueueing any task on that backend, or
constructing a worker raises `ImproperlyConfigured` for an `E011` value. A
task's own budget does not bypass an invalid backend default.

At startup, the worker validates schedules, the schedule source,
`TASK_TIMEOUT`, `TASK_TIMEOUTS`, `TASK_TIMEOUT_GRACE`, `MAX_ATTEMPTS` and
the effective `BACKOFF_INITIAL` and `BACKOFF_MAX` values, including with
`ox_worker --skip-checks`. Invalid values in this set stop startup.
`LOCK_TIMEOUT` validation runs through the `django_ox.E010` system check.
Database acceptance of a schedule's arguments is determined at dispatch.
A schedule-scoped failure is logged as `schedule_dispatch_error`, and the
worker continues to later schedules if rollback succeeds and the same
connection remains usable.

Invalid per-task policy fields raise the framework's `InvalidTask`
(`InvalidTaskError` on the backport) when the task is built, normally at
import. They are not system-check messages. Valid task overrides and valid
test-backend configurations add no system-check warning.

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
