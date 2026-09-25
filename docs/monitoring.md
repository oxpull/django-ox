# Monitoring

Monitor django-ox workers and queues in production through queue metrics,
health checks and liveness probes, Prometheus, and structured log events.
The queue and result store share one database table, so metrics come
directly from queries. There are four interfaces:

- **`django_ox.stats`**, plain functions returning queue metrics.
- **`manage.py ox_health`**, queue thresholds as an exit code for fleet
  alerting, or local heartbeat-file checks for controlling-loop liveness.
- **A Prometheus endpoint**, the same numbers as gauges, rendered by a view
  you mount where your scraper can reach it.
- **Structured log events** on the `django_ox` logger, with stable extra
  keys for JSON log handlers.

## Queue statistics

`django_ox.stats` is a small module of read-only functions. Each one is a single
ORM query over the task table. No extra state, no signals. Safe to call from a
request, a shell or a metrics collector, on every tested database.

```python
from datetime import timedelta

from django_ox import stats

stats.queue_stats()
# [QueueStats(queue_name="default", ready=3, running=1, failed=0, successful=214, lost=0, discarded=0),
#  QueueStats(queue_name="emails", ready=0, running=0, failed=2, successful=560, lost=0, discarded=0)]

stats.ready_count()  # tasks eligible to run right now
stats.oldest_ready_age()  # timedelta, or None when no READY task is eligible
stats.throughput(timedelta(minutes=5))  # terminal outcomes per minute
stats.failure_rate(timedelta(minutes=5))  # 0.0 to 1.0, or None
stats.last_claim_age()  # time since a worker last claimed
stats.waiting_counts()  # WAITING tasks per queue, such as {"default": 4}
```

| Function | Returns | Semantics |
| --- | --- | --- |
| `queue_stats()` | `list[QueueStats]` | Raw row counts per queue and status (`ready`, `running`, `failed`, `successful`, `lost`, `discarded`), one entry per queue with any rows. The `ready` column counts every READY row, including tasks deferred to a future `run_after`. `lost` counts tasks whose worker stopped reporting with no attempts left; see [the reaper](production.md#the-reaper). `discarded` counts tasks closed without running; see [Retrying and discarding](#retrying-and-discarding). A WAITING task is in no column. `waiting_counts()` counts those. A queue whose tasks are all WAITING gets an entry of zeros. |
| `ready_count()` | `int` | READY tasks eligible to run now, mirroring the worker's dequeue predicate: deferred tasks do not count until `run_after` passes. This is the backlog number. |
| `oldest_ready_age()` | `timedelta \| None` | Age of the oldest eligible READY task, measured from when it became eligible (`run_after` when set, `enqueued_at` otherwise), so a task deferred by a week does not read as a week of backlog. |
| `throughput(window)` | `float` | Tasks reaching a terminal state (SUCCESSFUL or FAILED) per minute over the trailing window (default 5 minutes). |
| `failure_rate(window)` | `float \| None` | Fraction of terminal outcomes in the window that FAILED, or `None` when nothing finished. Retries still pending are not outcomes and do not count. |
| `last_claim_age()` | `timedelta \| None` | Time since any worker last claimed a task, or `None` if none ever was. This is claim activity, not a heartbeat: idle workers over an empty queue record nothing. |
| `waiting_counts()` | `dict[str, int]` | WAITING tasks per queue, for each queue that has any. A waiting task is held back from every worker until something releases it. django-ox never puts a task there by itself, and it isn't backlog. |

Every function except `queue_stats()` and `waiting_counts()` accepts a `queue_name` keyword to
scope the metric to one queue.

Every one of them also accepts `using`, the database alias to read. Left
out, they read the alias `OxTask` is written to, never the one
`db_for_read` points at. On a project with no database router those are the
same connection. On one that sends reads to a replica they are not, and a
replica that is behind would answer for a queue nobody is running. The same
goes for `ox_health` and the Prometheus endpoint, which read these
functions.

**Alert on two numbers: backlog depth (`ready_count`) and backlog age
(`oldest_ready_age`).** Neither works alone. Depth looks fine while one poisoned
task starves the queue. Age looks fine during a flood of fresh work.

## Health checks: ox_health

`ox_health` has two modes. With no flags, it checks only that the database
answers. Queue thresholds turn metrics into an exit code for fleet alerting.
With `--heartbeat-file`, it checks local file timestamps instead of the
database.

Zero means every enabled check passes. A failed check exits non-zero with a
one-line reason on stderr.

```
python manage.py ox_health --max-backlog 1000 --max-age 600
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Restrict the database checks to one queue. |
| `--format` | `text` | `json` prints one object on stdout instead of the `OK:` line, on success and failure. In database mode its fields are `ok`, `queue`, `backlog`, `oldest_age_seconds`, `last_claim_age_seconds` and `problems`. `queue` is `null` when no `--queue` is given. The figures are `null` when there is nothing to measure or the check could not run, as with an unreachable database or an invalid threshold. File mode uses the [heartbeat JSON object](#file-mode-json). The exit status is unchanged. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Deferred tasks do not count. |
| `--max-age` | off | Fail when a READY task has been eligible to run for longer than this. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this long, or no claim was ever recorded. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. This measures queue-wide claim activity, not one worker's liveness. |
| `--database` | the alias `OxTask` writes to | Database alias to check. The figures come from that alias, so the check reports the queue your workers are running. |
| `--heartbeat-file PATH` | off | Check the local file set written by `ox_worker --heartbeat-file PATH`, instead of the database. Reads metadata only and runs no system or migration checks. |
| `--max-heartbeat-age SECONDS` | `60` | Maximum file age, inclusive. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds, including fractions. Must be finite and strictly positive. Requires `--heartbeat-file`. |
| `--processes N` | `1` | Expected worker process count. Must match the worker's `--processes` and be at least 1. At 1, check `PATH`. Above 1, require `PATH.supervisor` and every slot file from `PATH.0` through `PATH.(N-1)` to pass. Requires `--heartbeat-file`. |

File mode cannot be combined with `--database`, `--queue`, `--max-backlog`,
`--max-age` or `--worker-timeout`. Run dependency and queue checks as separate
commands.

On success, database mode prints the measured values, which is useful in
cron mail:

```
OK: backlog=3 oldest_age=12s last_claim_age=2s
```

### Local heartbeat files

`ox_worker --heartbeat-file PATH` enables local loop-liveness evidence.
It is off by default. `ox_health --heartbeat-file PATH` checks that
evidence instead of the database.

A passing file probe means **the expected controlling loops have advanced
recently**. It does not mean that tasks are progressing or that the database
answers.

**A hung database can cause a restart storm.** A statement that never
returns stops the controlling loop and makes its heartbeat stale. If this
happens across the fleet, automatic liveness restarts can restart every
container. A single half-open connection looks the same to the worker.
No connect, statement or claim timeout is added. Omit this automatic
restart trigger if that tradeoff is unacceptable.

#### Updates and scope

The worker updates its file on the main thread at the head of each poll
pass, before database work, and at the head of each drain pass. Updates
resume when database work returns control to the loop. On a pooled
PostgreSQL alias, a `worker_poll_failed` pass is followed by a synchronous
test of each idle pooled connection on the main thread, then the polling
wait, before the next update. Include pool acquisition and these connection
tests in the freshness budget. A connection test that never returns stalls
updates just as a hung statement does.

Startup work before `Worker.run()` writes no heartbeat. This includes
Django setup and configured startup work such as reading stored schedules
through `DatabaseScheduleSource`. Allow time for startup before treating a
missing file as a liveness failure.

A stopped process or a controlling loop wedged in a call eventually fails
the probe. The probe does not detect:

- All task slots stuck while the controlling loop continues. Use
  [task timeouts](production.md#task-timeouts) for task execution limits.
- A dead lease-renewal thread.
- Failed claims or a lack of workflow progress.

Drain passes continue updating the file, but this does not bound shutdown.
A worker hung in a claim cannot return to the loop to begin draining after
the first stop signal. It may need a second signal or SIGKILL.

#### Expected files

Use the same absolute base path and process count for the worker and probe.

| Worker process count | Expected files |
| --- | --- |
| `1`, the default | `PATH` |
| `N > 1` | `PATH.supervisor`, then `PATH.0` through `PATH.(N-1)` |

Above one process, `PATH` itself is not written. Child names use stable
slot numbers, not PIDs. The supervisor updates only its own file, never a
child's file.

Every expected file must pass. The probe does not discover files by
listing the directory. Missing slots fail; extra files do not help.
A stopped supervisor therefore fails once its own file expires, even
while children continue updating theirs.

The supervisor makes a best-effort attempt to remove each child's file
before starting or replacing that child and when it observes the child
exit. A replacement slot is missing until its loop starts. Allow for the
restart delay, which starts at one second and doubles to 30 seconds on
repeated deaths, plus child startup.

If removal fails, `heartbeat_invalidate_failed` is logged once per slot
path. The old file can still pass until it ages out. The single-process
worker and supervisor do not remove their own files on exit. A leftover
single-process file can also pass until it ages out.

With `--processes N` where N is greater than 1, a slot whose supervisor has
died stops updating `PATH.i`, including during drain. The worker still
finishes its in-flight tasks, but its file ages without further updates.
An orphan therefore cannot keep a replacement worker's slot fresh by
continuing to write the same path.

#### Directory and file requirements

Provision a dedicated, writable directory before starting the worker.
It must be local and private to the container. Never share it between
replicas: a live writer can keep a dead worker's evidence fresh.
django-ox does not create the directory.

Use an absolute path because the worker and probe resolve relative paths
against their own working directories. Run the probe as the worker UID.

New files are created with mode `0600`, subject to the umask. Existing
files are not truncated and their permissions are not changed. Contents
are neither read nor written. The heartbeat's meaning is its modification
time; updates also change its access time. There are no locks, temporary
file-and-rename publication, or `fsync` calls.

Writers refuse symlinks and non-regular files without following them or
blocking on them. An update failure is never fatal to the worker or
supervisor. It logs `heartbeat_write_failed` once per writer and path and
retries on every later pass.

The probe reads metadata with `lstat`, not file contents. The file's read
permission bits do not determine whether the probe can check it; the
directory must be searchable by the probe user.

Use a pre-existing, dedicated directory on the container-local filesystem.
The worker needs directory permissions to create its heartbeat file; with
multiple processes, the supervisor also needs permission to remove slot
files. The probe needs permission to traverse the directory and inspect
each expected file's metadata, not to read its contents.

New files are created with mode `0600`, subject to the umask. An existing
heartbeat file must be a regular file owned by, or writable by, the
worker's effective UID. If opening it for writing is denied but it is a
regular file owned by that UID, the worker falls back to updating its
access and modification times without following symlinks. A restrictive
umask that removes owner-write permission, or an existing worker-owned
`0400` file, therefore does not stop updates. Existing file modes and
contents are left unchanged.

A non-regular file, including a read-only FIFO, is refused. A file owned
by another user that the worker cannot write is also refused. These
failures produce a `heartbeat_write_failed` warning.

A host bind mount under Docker Desktop can receive modification times
from the host's clock rather than the container's clock. Even a small
difference can make the probe report a file as being in the future. Keep
the heartbeat directory container-local rather than bind-mounted from
the host.

#### Freshness and database isolation

The worker updates its heartbeat on the controlling thread at the head of
each poll and drain pass, and before every claim attempt. A busy pass can
make up to `--concurrency` claims, each requiring one or more database
round trips, but the file is updated between claims. Freshness therefore
does not need to cover concurrency multiplied by claim latency. Allow for
reap and dispatch work plus one claim, the poll interval, and scheduling
margin. A claim that never returns still stops heartbeat updates.

An idle poll pass makes two heartbeat updates: one at the pass head and
one before its claim attempt. A busy pass makes one at the pass head plus
one per claim attempt.

Size `--max-heartbeat-age` above those expected gaps. With Django's
PostgreSQL connection pool, a refused database can hold a pass for the
pool's `timeout`, which defaults to 30 seconds, before the error returns
control to the loop. The freshness budget must exceed the pool timeout,
idle-connection testing after a failed pass, `--interval` and scheduling
margin, with further allowance for other reap, dispatch and claim work.

A file passes only if it is regular and:

```text
0 <= probe wall clock - file modification time <= max heartbeat age
```

Both boundaries are inclusive. `--max-heartbeat-age` defaults to 60
seconds and must be finite and strictly positive. It accepts durations
such as `7d`, `24h`, `90m`, `45s`, or a plain number of seconds, including
fractions.

Freshness uses wall time, not a monotonic clock. A clock step changes the
measured ages. Any modification time ahead of the probe's clock fails
with a clock-skew diagnostic. Worker and probe are expected on the same
machine. Detection is not immediate: evidence can remain valid for the
configured age window after updates stop.

File mode is selected before command system checks. It runs no system
or migration checks, opens no database connection, runs no queries and
constructs no task backend. `--skip-checks` is not required.

This guarantee covers django-ox's command. Django startup still runs
before the command. A project's `AppConfig.ready()` or other startup
code can access the database first. A probe that must survive a database
outage requires database-free project startup.

#### Options and refusals

`--processes N` defaults to `1`, must be an integer of at least one, and
must match the worker's process count.

File mode refuses `--database`, `--queue`, `--max-backlog`, `--max-age`
and `--worker-timeout`, even when their values match database-mode
defaults. Run database checks as a separate invocation.

`--max-heartbeat-age` and `--processes` require `--heartbeat-file`.
An empty heartbeat path is refused.

For a path beginning with `-`, use the equals form,
`--heartbeat-file=-hb`, with both `ox_worker` and `ox_health`. These paths
also work with multiple processes. The supervisor forwards the base path
to each child as one argument, `--heartbeat-file=<base>`, alongside
`--processes 1` and the child's `--worker-index`. It likewise forwards
queue and lock-timeout values as `--queues=<value>` and
`--lock-timeout=<value>`.

#### Text output and exit codes

Text success has this form:

```text
OK: heartbeat_files=<count> oldest_heartbeat_age=<age>s max_heartbeat_age=<max>s
```

For example:

```text
OK: heartbeat_files=3 oldest_heartbeat_age=0.1s max_heartbeat_age=60s
```

A failed file contributes one problem in expected-file order. Problems
are joined with `; ` in the command's error line on stderr.

| Condition | Problem text |
| --- | --- |
| Missing file or directory | `heartbeat file <path> is missing` |
| Too old | `heartbeat file <path> is <age>s old, over --max-heartbeat-age <max>s` |
| Future modification time | `heartbeat file <path> was updated <s>s in the future; the clocks of the worker and the check disagree` |
| Symlink | `heartbeat file <path> is a symlink, not a regular file` |
| Directory | `heartbeat file <path> is a directory, not a regular file` |
| Other non-regular file | `heartbeat file <path> is a special file, not a regular file` |
| Metadata access error | `heartbeat file <path> cannot be read: <strerror>` |

The accepted age window is inclusive: `0 <= age <= max`. A file exactly
`--max-heartbeat-age` seconds old passes; any future modification time
fails. Display rounding does not affect this decision. If rounding an
over-age value to one decimal place would show it at or below the limit,
the message instead shows the first tenth above the limit. For example,
an age of 60.001 seconds with a 60-second limit is reported as
`60.1s old, over --max-heartbeat-age 60s`. A future offset below
0.05 seconds is reported as `<0.1s in the future`.

Exit codes are:

- `0`: every expected file passes.
- `1`: a file fails or the command refuses an option value or combination.
  Examples include an empty path, zero maximum age or a process count
  below one.
- `2`: an argument-parsing error. Examples include `nan`, `inf` or an
  invalid duration for `--max-heartbeat-age`, or a non-integer process
  count.

#### File-mode JSON

`--format json` prints this file-mode object on stdout.

```json
{
  "ok": true,
  "heartbeat_file": "/run/ox/hb",
  "processes": 2,
  "max_heartbeat_age_seconds": 60.0,
  "files": [
    {
      "path": "/run/ox/hb.supervisor",
      "ok": true,
      "age_seconds": 0.08,
      "problem": null
    },
    {
      "path": "/run/ox/hb.0",
      "ok": true,
      "age_seconds": 0.05,
      "problem": null
    },
    {
      "path": "/run/ox/hb.1",
      "ok": true,
      "age_seconds": 0.02,
      "problem": null
    }
  ],
  "problems": []
}
```

| Field | Meaning |
| --- | --- |
| `ok` | Whether the check passes. |
| `heartbeat_file` | The base path supplied to the command. |
| `processes` | The requested worker process count. |
| `max_heartbeat_age_seconds` | The maximum permitted age in seconds. |
| `files` | Reports in expected-file order, or `null` when options were refused before files were checked. |
| `files[].path` | The expected file's path. |
| `files[].ok` | Whether this file passes. |
| `files[].age_seconds` | Age as a float; negative for a future modification time, or `null` when there is no regular file to measure. |
| `files[].problem` | The file's problem string, or `null` when it passes. |
| `problems` | Problem strings; empty on success. |

`age_seconds` is the raw floating-point age, not the rounded value used
in text messages.

The object is printed on success, file-check failure and command-level
option refusal. Exit status is unchanged. Argument-parsing errors are
not guaranteed to produce JSON.

### Choosing checks and probe thresholds

- **`--max-backlog` and `--max-age` measure the whole queue.** Put them in
  fleet-level alerting, from cron or a monitoring agent. Do not put them in a
  per-worker liveness probe: a shared backlog would fail every worker's
  probe without shifting the backlog.
- **`--worker-timeout` measures fleet claim activity.** It can alert on a
  queue with steady traffic. An idle queue can fail it, and another worker's
  claims can make it pass while one worker is stopped.
- **For bursty queues, prefer `--max-age`.** It only fires when work exists
  and is not being picked up.
- **Use `--heartbeat-file` for local controlling-loop liveness.** It checks
  every expected worker loop and, with multiple processes, the supervisor.
  It does not check task progress or database availability.

Size the freshness budget for the database's worst expected stall; see
[freshness and database isolation](#freshness-and-database-isolation).
A configured `OPTIONS["connect_timeout"]` bounds connects; PyMySQL defaults
to 10 seconds. PostgreSQL statements are unbounded by default. MySQL
row-lock waits use `innodb_lock_wait_timeout`, 50 seconds by default, while
PyMySQL's client read timeout is unbounded by default. SQLite's busy timeout,
5 seconds by default, bounds each lock wait. With Django's PostgreSQL pool,
include its acquisition `timeout`, 30 seconds by default, and the time
spent testing idle connections after a failed pass.

Choose `--max-heartbeat-age` above the polling interval (`--interval`,
default 1 second), expected loop latency and a scheduling margin. The
default maximum age is 60 seconds.

Allow time for startup. Django setup and worker initialization write no
heartbeat; the first update happens when `Worker.run()` enters its loop.
Configured startup work, including loading database-backed schedules, can
delay that point.

With `--processes`, a replacement slot has a missing file until its loop
starts. Restart delays begin at 1 second and double to 30 seconds on repeated
deaths, plus child startup time. Set probe failure thresholds to accommodate
ordinary replacement.

### Kubernetes liveness

This worker-container fragment uses two processes, a 60-second maximum age,
a startup allowance of about five minutes, and six consecutive liveness
failures before restart. Adjust these values for startup time, loop latency
and replacement backoff.

Before starting the worker, provision `/run/ox` as a dedicated, writable
directory private to this container. Do not share it between replicas.
Run the probe as the worker's UID.

```yaml
command: ["python", "manage.py", "ox_worker"]
args:
  - "--processes"
  - "2"
  - "--heartbeat-file"
  - "/run/ox/heartbeat"
startupProbe:
  exec:
    command:
      - python
      - manage.py
      - ox_health
      - --heartbeat-file
      - /run/ox/heartbeat
      - --processes
      - "2"
      - --max-heartbeat-age
      - "60"
  periodSeconds: 5
  timeoutSeconds: 10
  failureThreshold: 60
livenessProbe:
  exec:
    command:
      - python
      - manage.py
      - ox_health
      - --heartbeat-file
      - /run/ox/heartbeat
      - --processes
      - "2"
      - --max-heartbeat-age
      - "60"
  periodSeconds: 10
  timeoutSeconds: 10
  failureThreshold: 6
```

The command's file mode makes no database calls, but `manage.py` still runs
Django startup. A project's `AppConfig.ready()` or other startup code can
access the database before the command runs. A probe that must survive a
database outage needs database-free project startup.

A separate readiness or dependency check may report database availability.
Do not use that result to trigger liveness restarts. Marking a worker pod
unready does not pause its task claims.

### Fleet alerting from cron

Queue thresholds report fleet conditions, not local worker-loop liveness.
Use these checks for alerting, not as per-container restart triggers.

From cron, for alerting on the queue itself:

```
*/5 * * * * cd /srv/myproject && .venv/bin/python manage.py ox_health \
    --max-backlog 1000 --max-age 600 || /usr/local/bin/page-someone
```

## Prometheus

`django_ox.metrics` renders the stats functions in the Prometheus text
format, from the standard library alone. Mount the view and point a scrape
job at it:

```python
# urls.py
from django.urls import include, path

urlpatterns = [
    path("ox/", include("django_ox.urls")),  # GET /ox/metrics
]
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: django_ox
    metrics_path: /ox/metrics
    static_configs:
      - targets: ["app.internal:8000"]
```

The response is `text/plain; version=0.0.4`. A scraper that sends
`Accept: application/openmetrics-text` gets the OpenMetrics form of the
same text, and `HEAD` is answered for load-balancer checks. Each scrape is
five aggregate queries over the task table, however many queues there are.

The scrape reads the alias `OxTask` is written to, which is the queue your
workers are running. To keep scrape traffic off that database, name another
alias where you mount the view:

```python
from django_ox.views import metrics

path("ox/metrics", metrics, {"using": "replica"})  # scrape a replica instead
```

A replica that is behind reports the queue as it was, which is the trade.
The alias comes from the URLconf rather than from the request, so whoever
scrapes cannot choose the database.

**The endpoint has no authentication of its own.** The numbers are not
secret, but the queue names and the shape of your traffic are yours to keep,
so put the route behind the project's policy before it goes near the public
side of a load balancer. Two one-line ways:

```python
from django.contrib.auth.decorators import login_required
from django_ox.views import metrics

path("ox/metrics", login_required(metrics))  # session auth, for a human
```

```python
from django.http import HttpResponseForbidden


def from_prometheus(view):
    def guard(request, *args, **kwargs):
        if request.META["REMOTE_ADDR"] not in {"10.0.0.12"}:
            return HttpResponseForbidden()
        return view(request, *args, **kwargs)

    return guard


path("ox/metrics", from_prometheus(metrics))  # the scraper's address only
```

A network policy that only admits the scraper to the path does the same
job without code.

Every metric is a gauge, with one sample per queue that has any row:

| Metric | Labels | Value |
| --- | --- | --- |
| `django_ox_tasks` | `queue`, `status` | Rows by status, one of `ready`, `running`, `failed`, `successful`, `lost`, `discarded`, `waiting`. The same numbers as `queue_stats()` and `waiting_counts()`, so `ready` includes deferred tasks. A sum over `status` includes waiting tasks, which aren't backlog. |
| `django_ox_ready_tasks` | `queue` | `ready_count()`: READY tasks eligible to run now. The backlog number. |
| `django_ox_oldest_ready_age_seconds` | `queue` | `oldest_ready_age()` in seconds. Absent when no READY task is eligible. |
| `django_ox_last_claim_age_seconds` | `queue` | `last_claim_age()` in seconds. Absent until a worker has claimed on that queue. |
| `django_ox_throughput_per_minute` | `queue` | `throughput()` over the default five-minute window. |
| `django_ox_failure_rate` | `queue` | `failure_rate()` over the same window, 0 to 1. Absent when nothing finished. |

There are no counters. The table is pruned, so a monotonic count of finished
tasks cannot be read from it; throughput and the failure rate are
trailing-window readings instead, and `rate()` in PromQL is not the tool
for them. Alert on `django_ox_ready_tasks` and
`django_ox_oldest_ready_age_seconds` the same way as on the functions.

The metric names and label names above are public API from the release that
ships them; see [API stability](stability.md). The help text is not.

### With an existing registry

A project that already runs `prometheus_client` (on its own or through
django-prometheus) can register the same numbers with its registry instead
of mounting a second endpoint. `prometheus_client` is not a dependency of
django-ox; `collector()` imports it when called and raises `ImportError`
when it is missing.

```python
from prometheus_client import REGISTRY

from django_ox import metrics

REGISTRY.register(metrics.collector())
```

### OpenTelemetry

django-ox does not ship an OpenTelemetry exporter. The stats functions fit
an observable gauge callback, so a project that already has an OTel meter
can read the queue through it in a few lines. This is the whole recipe;
nothing in django-ox imports `opentelemetry`.

```python
from opentelemetry import metrics as otel
from opentelemetry.metrics import Observation

from django_ox import stats


def observe_backlog(options):
    for row in stats.queue_stats():
        yield Observation(row.ready, {"queue": row.queue_name})


meter = otel.get_meter("django_ox")
meter.create_observable_gauge("django_ox.ready_tasks", callbacks=[observe_backlog])
```

The same shape reads `oldest_ready_age()` or `failure_rate()` per queue.

## Log events

The worker logs through the standard library logger named `django_ox`. No
logging dependency, no imposed format. Configure handlers and formatters in
`LOGGING` as usual. The `django_ox.testing` backends also emit the
`task_policy_inert` event below.

Lifecycle events carry an `extra` dictionary with stable keys, so a JSON
formatter that serialises record attributes gets consistent fields to index.
The message text is not part of the contract. The keys are.

| Event | Level | When |
| --- | --- | --- |
| `worker_started` | INFO | The run loop starts. |
| `connection_pool_too_small` | WARNING | Once per `Worker.run()`, after `worker_started` and before threads start: the worker alias's effective PostgreSQL pool maximum is below `concurrency + 1`. Each `--processes` child checks separately. The warning does not resize the pool, refuse startup, measure available server slots, or reserve fallback capacity. |
| `task_claimed` | DEBUG | A task was claimed from the queue. |
| `task_started` | DEBUG | Execution of an attempt begins. |
| `task_succeeded` | INFO | The task reached SUCCESSFUL. |
| `task_timed_out` | WARNING | An attempt ran past its resolved task, queue or backend timeout and is recorded as failed; a `task_retrying` or `task_failed` record follows. It counts timeouts recorded as failures, not deadlines that passed: a task that catches `TaskTimeout` and returns produces no event, and neither does a timeout on a worker logging `timeouts_backstop_only`, where the attempt ends in `task_stuck` or in whatever the task went on to do. |
| `task_stuck` | ERROR | A timed-out attempt's thread did not stop within `TASK_TIMEOUT_GRACE`. The attempt is recorded as failed and the worker is recycling. On a worker logging `timeouts_backstop_only` nothing is raised inside the task, so this is the ordinary end of a timeout there rather than a pathological one. |
| `worker_recycling` | WARNING | The worker stopped claiming after a stuck thread; it drains its other tasks and exits with code 75. It follows a `task_stuck` whose thread is still inside the attempt, which is the usual case, so on a worker logging `timeouts_backstop_only` every timeout that reaches the backstop costs a worker restart. |
| `timeouts_backstop_only` | WARNING | Once per worker: `TaskTimeout` is not raised inside a running sync task because the interpreter cannot raise an exception inside another thread (`reason=interpreter`) or a coverage tool or debugger is watching the worker's threads (`reason=tracing_tool`). The interpreter warning appears at startup if `OPTIONS` configures a timeout, otherwise on the first attempt that arms one. The tracing warning appears on the first attempt registered under the tool. `TASK_TIMEOUT_GRACE` is the whole enforcement while it stands. See [Task timeouts](production.md#task-timeouts). |
| `task_retrying` | WARNING | An attempt failed and is scheduled for retry. `retry_in_s` gives the delay in seconds. |
| `task_failed` | ERROR | The task reached FAILED because its attempts are exhausted (`reason="attempts_exhausted"`) or its backoff callback returned `None` (`reason="backoff_declined"`). An attempt whose task cannot be rebuilt also logs this event on its final claim, without sending `task_finished`. |
| `task_policy_error` | ERROR | A backoff callback raised or returned an invalid value. The worker uses its exponential backoff instead and records the task's original exception, not the callback error. |
| `task_policy_inert` | WARNING | A `django_ox.testing` backend first enqueues a task with explicitly declared policy. Once per task path per backend instance. These backends accept and validate policy but do not retry, call backoff callbacks or enforce timeouts. |
| `task_reclaimed` | WARNING | The reaper took a task back from a worker that stopped refreshing its lock. One record per task. A pass whose stuck set changed while it ran (a lease renewed, or one more lease expired) instead emits a single record carrying `count` and no `task_id`, because it cannot say which tasks the reclaim covered. |
| `task_lease_lost` | WARNING | A worker finished an attempt whose lease had already been reclaimed, so its write was dropped and no result was signalled. |
| `task_outcome_reconnected` | WARNING | After a connection-level failure while recording an attempt's outcome, a new connection either recorded it or confirmed that the first write had already committed. Carries `outcome`, `already_written` and `duration_ms`; the usual outcome log follows. Recovery retries outcome persistence at most once, never the task body. Does not cover the watchdog's stuck-attempt records. |
| `task_outcome_unrecorded` | ERROR | After a connection-level failure while recording an attempt's outcome, recovery on a new connection also failed. Carries `dropped_status` and `duration_ms`, with a traceback of the second failure. The outcome is unconfirmed, not necessarily absent: the first write may have committed, or another recovery path may already have fenced this attempt. If the row still awaits recovery, the reaper handles it after lease expiry. Even a brief outage spanning both write attempts can cause this event; the worker adds no backoff before the single retry. Does not cover the watchdog's stuck-attempt records. |
| `lease_renew_failed` | WARNING | A lease renewal statement failed. Outside Django's PostgreSQL pool, this also covers connection failures during renewal. The thread continues. The next tick is due within one renewal interval and starts immediately after an overrun. |
| `lease_renew_degraded` | WARNING | On a pooled PostgreSQL database, renewal left its private connection path. The worker renewed through the pool or failed to renew through either path. `fallback` is `succeeded` or `failed`. Logged once on entering degraded renewal, not on every degraded tick. Alert on this event; `lease_renew_missed` can arrive after live work has already been reclaimed. |
| `lease_renew_fallback` | DEBUG | On a pooled PostgreSQL database, a later renewal succeeded through the pool while renewal remained degraded. |
| `lease_renew_missed` | WARNING | On a pooled PostgreSQL database, renewal missed a tick while degraded. Reports missed-renewal counts at most once per 30 seconds. `lease_renew_missed` means leases were not renewed on that tick. After two consecutive misses, the next tick lands at the lease boundary; the reaper can reclaim the task while its body is still running, allowing another run. A degraded warning with `fallback=failed` also means that tick did not renew leases and starts missed-tick reporting. One with `fallback=succeeded` does not. The 30-second missed-renewal reporting window persists across recovery. |
| `lease_renew_recovered` | INFO | On a pooled PostgreSQL database, renewal succeeded on its private connection again. Includes counts of successful fallback renewals and missed renewals during the degraded period. |
| `schedule_dispatched` | INFO | A recurring tick enqueued its task. |
| `schedule_tick_dropped` | WARNING | A tick was past its starting deadline and was not run. Carries `late_seconds`. Each worker reports a given tick once, not once per dispatch pass, so a schedule that stays droppable does not repeat the warning every second. |
| `schedule_row_skipped` | WARNING | A row could not be built. A skip during a periodic full read carries `schedule`, `schedule_pk` and `reason`. A skip during the locked dispatch read carries `schedule_pk` and a traceback. |
| `schedule_dispatch_error` | ERROR | A schedule-scoped failure, database or not. Its transaction rolled back and the same database connection remains usable, so the rest of the pass continues. The schedule is retried under the usual due-tick and deadline rules. The first failure has a traceback; continued failures produce at most one summary per 60 seconds. Carries `schedule`, `database`, `error`, `failures` and `suppressed`. |
| `schedule_dispatch_recovered` | INFO | A schedule that had been logging `schedule_dispatch_error` on this worker committed a tick again, by enqueueing a task or recording a first-sighting anchor. Logged once per run of failures. Carries `schedule`, `database` and `failures`, the number of failed attempts in that run. |
| `schedule_dispatch_callback_failed` | WARNING | A `transaction.on_commit` callback registered by a `task_enqueued` receiver raised after the dispatch transaction committed. The task is enqueued, the tick is recorded and the dispatch is counted; the failure is the callback's. Carries `task_id`. |
| `schedule_dispatch_failed` | WARNING | A dispatch pass was abandoned: a `django.db.DatabaseError` escaped a shared read, rollback failed, the database session changed, or the connection was unusable after rollback. The stored source's marker read and boundary heal retain their own events below. Dispatch is retried on the next pass; an unusable connection is dropped, and the claim still runs. The first abandoned pass has a traceback; continued failures produce at most one summary per 60 seconds. Carries `database`, `error`, `failures` and `suppressed`. |
| `schedule_source_unavailable` | WARNING | The stored schedules could not be read, so the worker is running on the set it last read rather than on none. Repeated every dispatch pass while the read keeps failing. A stream of it right after a deploy means the code is running ahead of migration `0007`. |
| `schedule_lock_unavailable` | WARNING | The database gave up waiting for a lock another worker held: a stored schedule's row, or the tick row a settings-declared schedule's dispatch claims. MySQL's lock-wait timeout or a deadlock it resolved against this worker, SQLite's busy timeout, PostgreSQL's `lock_timeout` or a deadlock. The schedule is skipped this pass and its tick fires on a later one if still unclaimed. No traceback; a stream can indicate long dispatch transactions, often from a slow `task_enqueued` receiver. On MySQL with three or more dispatchers, a refused settings schedule can also produce 1213 deadlock warnings when the claimant rolls back and workers queued on its tick key deadlock; read these beside `schedule_dispatch_error` for the same schedule. |
| `schedule_boundary_healed` | INFO | A stored schedule's timing had changed without its activation boundary moving, so the boundary was moved onto the current timing. Expected after a bulk update; repeated for one schedule is not. |
| `schedule_boundary_heal_failed` | WARNING | That move failed and will be retried. |
| `worker_error` | ERROR | The execution wrapper itself raised (an internal worker error, not a task failure). A connection-level outcome-write failure whose recovery on a new connection also failed is reported as `task_outcome_unrecorded` instead. |
| `worker_poll_failed` | WARNING | A database error ended one pass of the poll loop. The pass is abandoned and retried on the next one; the worker keeps running. A steady stream of it means the database is unreachable rather than slow. |
| `watchdog_error` | ERROR | The timeout watchdog failed while handling an armed attempt or cleaning up a batch's connection. Cleanup includes closing a private connection or returning a borrowed connection. A database error while closing the private connection is suppressed without this event. The watchdog thread continues. |
| `task_stuck_unrecorded` | WARNING | A timed-out attempt could not be recorded as failed. The worker recycles regardless, so the row is recovered by the reaper rather than by this write. |
| `watchdog_connection_unavailable` | WARNING | On a pooled PostgreSQL database, the timeout watchdog could get neither a private connection nor one from the pool. Logged once per batch. Every record in the batch fails and logs `task_stuck_unrecorded`. The worker recycles regardless. This event has no time-based rate limit. |
| `worker_drain_abandoned` | WARNING | A recycling worker stopped waiting on tasks that had not finished. Their leases expire and the reaper requeues them. Carries `pending`. |
| `claim_filter_sql_missing` | WARNING | Once per worker: a subclass overrides `claim_filter_q()` without `claim_filter_sql()`, so the single-statement PostgreSQL claim is given up for the path that applies the hook. |
| `worker_draining` | INFO | Shutdown began with tasks still in flight. |
| `worker_batch_empty` | INFO | Under `--batch`, a poll pass succeeded, claimed nothing, and left no task running. The worker drains and stops. A schedule-scoped failure does not prevent this event. An abandoned dispatch pass prevents it until a later dispatch pass completes. This event does not certify that every schedule dispatched. Carries `claimed`. |
| `worker_max_tasks_reached` | INFO | Under `--max-tasks`, the worker claimed its limit. It drains and stops. Carries `claimed`. |
| `worker_stopped` | INFO | The run loop exited. |
| `heartbeat_write_failed` | WARNING | A worker or supervisor could not update its heartbeat file. Logged once per writer and path, without a traceback. Never fatal; every later pass tries again. Carries `heartbeat_file` and `error`. |
| `heartbeat_invalidate_failed` | WARNING | The supervisor could not remove a slot's heartbeat file before starting or replacing it, or after observing its exit. Logged once per slot path, without a traceback. A regular file can still count as fresh until it ages out. A non-regular path fails that slot until it is removed; a path that cannot be inspected also fails that slot. The warning distinguishes these cases. Carries `heartbeat_file`, `worker_index` and `error`. |
| `supervisor_started` | INFO | `ox_worker --processes N` started its worker processes. |
| `worker_process_restarted` | WARNING | A worker process exited on its own and is being restarted. |
| `worker_process_recycled` | WARNING | A worker process exited with code 75 after a stuck task thread and is being restarted. Not counted against the restart cap. |
| `worker_process_stopped_early` | INFO | A stop signal reached a worker process before it had finished starting, so it died on the signal rather than draining. It had claimed no work, and the supervisor does not count it as a failure. |
| `supervisor_restart_cap` | ERROR | More than five deaths of one slot in a minute; the supervisor is stopping with exit code 1. |
| `supervisor_killed_workers` | ERROR | Worker processes still running five seconds after the second stop signal were sent SIGKILL. |
| `supervisor_stopped` | INFO | Every worker process has exited. |
| `worker_orphaned` | WARNING | A worker process found its supervisor gone and is draining. |

The four renewal connection events and `watchdog_connection_unavailable`
include `worker_id` and no traceback. They apply only to Django's
PostgreSQL pool.

`heartbeat_write_failed` and `heartbeat_invalidate_failed` also have no
traceback. Neither carries a `worker_id` key, including when a worker emits
`heartbeat_write_failed`.

On that path, connection-acquisition failures use the new events rather
than `lease_renew_failed`. Update alerts that previously relied on
`lease_renew_failed` alone. Renewal statement failures still use
`lease_renew_failed`.

Elsewhere, a failed connect during renewal still logs `lease_renew_failed`.
A failed connect while recording a stuck attempt logs
`task_stuck_unrecorded`.

| Key | Present on | Meaning |
| --- | --- | --- |
| `event` | all events | The event name from the table above. |
| `worker_id` | Worker events except `heartbeat_write_failed`, `schedule_source_unavailable`, `schedule_boundary_healed`, `schedule_boundary_heal_failed`, `schedule_row_skipped` and `schedule_lock_unavailable` for a stored schedule | Unique id of the worker emitting the record. With `--processes`, the slot number is the last part of the id. Settings-declared `schedule_lock_unavailable` events carry this key. The test-backend event `task_policy_inert` omits it. |
| `worker_class` | `claim_filter_sql_missing` | The Worker subclass's class name. |
| `claimed` | `worker_batch_empty`, `worker_max_tasks_reached` | Task attempts this worker claimed in its run, failed attempts and retries included. |
| `task_id` | worker task events | The task's UUID, as a string. Absent from `task_policy_inert`. |
| `task_path` | task events | Dotted path of the task function. |
| `queue` | worker task events | Queue name. Absent from `task_policy_inert`. |
| `attempt` | worker task events | Attempts consumed so far, including the current one. Absent from `task_policy_inert`. |
| `duration_ms` | `task_succeeded`, `task_retrying`, `task_failed`, `task_timed_out`, `task_stuck`, `task_lease_lost`, `task_outcome_reconnected`, `task_outcome_unrecorded` | Wall-clock duration of the attempt, in milliseconds. |
| `timeout_s` | `task_timed_out`, `task_stuck` | The timeout that applied, in seconds. |
| `grace_s` | `task_stuck`, `timeouts_backstop_only` | `TASK_TIMEOUT_GRACE`, in seconds. |
| `reason` | `timeouts_backstop_only` | Why the backstop is the whole enforcement: `interpreter` or `tracing_tool`. |
| `reason` | `task_failed` | Why the task reached FAILED: `attempts_exhausted` or `backoff_declined`. |
| `tracer` | `timeouts_backstop_only` with `reason=tracing_tool` | How the worker's threads are being watched: `sys.settrace` when a trace function is installed, which does not say which tool installed it, or `sys.monitoring (NAME)` for a registered tool, which names itself. |
| `exception` | `task_retrying`, `task_failed`, `task_policy_error` | Exception class name of the task failure, not a callback failure. |
| `retry_in_s` | `task_retrying` | Retry delay in seconds, as a float. |
| `policy` | `task_policy_error` | The policy field that failed; currently `backoff`. |
| `error` | `task_policy_error` | What the callback did: raised, returned an awaitable, or returned an invalid value. A raised exception includes its traceback. |
| `backend` | `task_policy_inert` | Test-backend alias. |
| `declared` | `task_policy_inert` | Sorted list of explicitly declared policy field names. |
| `status` | `task_reclaimed` | Status after reclaim: `READY` (requeued) or `LOST` (out of attempts). |
| `count` | `task_reclaimed` without `task_id` | How many tasks that pass reclaimed. Present only on the batch record described above. |
| `held_by` | `task_reclaimed` | The worker that stopped refreshing the lock, from the row. `worker_id` on the same record is the reaper that noticed. Absent on the batch record, along with `task_id`, `task_path`, `queue` and `attempt`. |
| `dropped_status` | `task_lease_lost`, `task_outcome_unrecorded` | The status the dropped write would have set: `SUCCESSFUL`, `FAILED` or `READY`. |
| `outcome` | `task_outcome_reconnected` | Confirmed outcome status: `SUCCESSFUL`, `READY` or `FAILED`. `READY` means the failed attempt was recorded for retry with its backoff. |
| `already_written` | `task_outcome_reconnected` | Boolean. `true` when the new connection found the first write already committed; `false` when recovery wrote the outcome on the new connection. |
| `schedule` | `schedule_dispatched`, `schedule_row_skipped` during a periodic full read, `schedule_dispatch_error`, `schedule_dispatch_recovered`, `schedule_dispatch_callback_failed`, `schedule_tick_dropped`, `schedule_lock_unavailable` for a settings-declared schedule | The schedule's name, from `SCHEDULES` or from its row. |
| `schedule_pk` | `schedule_row_skipped`, `schedule_lock_unavailable` for a stored schedule, `schedule_boundary_healed` | The stored schedule's row id. Absent for a settings-declared schedule, which has no row. |
| `scheduled_for`, `late_seconds` | `schedule_tick_dropped` | The tick that was dropped, and how late it was when the deadline rejected it. |
| `reason` | `schedule_row_skipped` during a periodic full read | Why the row could not be used. A skip during the locked dispatch read carries `schedule_pk` and a traceback instead. |
| `queues` | `worker_started` | The worker's queues. |
| `concurrency` | `worker_started`, `connection_pool_too_small` | The worker's task-thread concurrency, set by `--concurrency`. |
| `database` | `connection_pool_too_small`, `schedule_dispatch_error`, `schedule_dispatch_failed`, `schedule_dispatch_recovered` | The worker's database alias: `--database`, or the alias used to write `OxTask`. |
| `max_size` | `connection_pool_too_small` | The effective pool maximum. `pool=True` means 4. For a non-empty mapping, use `max_size`; if absent or `None`, use `min_size`, defaulting to 4. The sizing check skips values that are not an `int` of at least 1. It excludes booleans and floats such as `10.0`. |
| `recommended_max_size` | `connection_pool_too_small` | `concurrency + 1`: one pooled connection per task thread and one for the poll loop. This does not reserve fallback capacity or validate the server budget. |
| `unpooled_connections` | `connection_pool_too_small` | Worst-case additional private-connection budget per worker process, not a count of open connections. Always 2: one for lease renewal and one possible timeout-watchdog connection. A worker whose tasks never use timeouts needs only the first. An absent timeout in `OPTIONS` alone does not establish that, because a task can declare its own. |
| `pending` | `worker_draining` | In-flight tasks at shutdown. |
| `processes` | `supervisor_started` | Worker processes the supervisor runs. |
| `worker_index`, `exit_code` | `worker_process_restarted`, `worker_process_recycled`, `supervisor_restart_cap` | Which slot exited and how. A negative code is the signal that killed it. |
| `worker_index` | `heartbeat_invalidate_failed` | The slot whose heartbeat file could not be removed. This event has no `exit_code`. |
| `heartbeat_file` | `heartbeat_write_failed`, `heartbeat_invalidate_failed` | The affected file path: `PATH` for a single worker, `PATH.i` for slot i, or `PATH.supervisor` for the supervisor. |
| `delay` | `worker_process_restarted`, `worker_process_recycled` | Seconds until the slot is started again. |
| `task_id`, `exit_code` | `worker_recycling` | The stuck task that started the recycle, and the code the worker will exit with, 75. |
| `restarts` | `supervisor_restart_cap` | Deaths of that slot inside the window. |
| `worker_indexes` | `supervisor_killed_workers` | The slots that were killed. |
| `parent_pid` | `worker_orphaned` | The supervisor pid the worker was started under. |
| `exit_code` | `supervisor_stopped` | The code the supervisor exits with. |
| `error` | `lease_renew_degraded`, `lease_renew_fallback`, `lease_renew_missed`, `watchdog_connection_unavailable`, `schedule_dispatch_error`, `schedule_dispatch_failed`, `heartbeat_write_failed`, `heartbeat_invalidate_failed` | Schedule dispatch: the exception class name of the latest failure. Heartbeat: the operating-system error text from the failed update or removal. Renewal and watchdog: the private-connection failure reason, except that a pool-first renewal tick reports that the pool was tried first and gives the remaining lease time. |
| `failures` | `schedule_dispatch_error`, `schedule_dispatch_failed`, `schedule_dispatch_recovered` | On `schedule_dispatch_error`, total failed attempts in this schedule's current run of failures on this worker, including the first. On `schedule_dispatch_failed`, total abandoned passes in the current outage. On `schedule_dispatch_recovered`, failed attempts in the run that just ended. |
| `suppressed` | `schedule_dispatch_error`, `schedule_dispatch_failed` | Failures counted since the previous report and not logged individually. Zero on the first report of a run. |
| `fallback` | `lease_renew_degraded` | `succeeded` if pooled renewal succeeded; `failed` if fallback did not renew the leases. |
| `fallback_error` | `lease_renew_degraded`, `lease_renew_missed`, `watchdog_connection_unavailable` | The fallback failure reason. On `lease_renew_degraded`, present when `fallback` is `failed`. A borrowed renewal statement failure is reported as "the renewal statement failed". |
| `missed` | `lease_renew_missed` | Missed renewal ticks since the last `lease_renew_degraded` or `lease_renew_missed` record, or since recovery if more recent. |
| `fallback_renewals` | `lease_renew_recovered` | Successful pooled renewals during the degraded period. |
| `missed_renewals` | `lease_renew_recovered` | Missed renewal ticks during the degraded period. |

`task_claimed` and `task_started` are DEBUG because they fire once per
attempt; run `ox_worker -v 2` (or set the logger to DEBUG) when you want
them. Everything a dashboard usually wants survives at INFO.

`task_lease_lost` should be rare. It means a worker went unresponsive long
enough for the reaper to take its task away, and the worker's own result was
dropped when it finally finished, because the row no longer belonged to it.
Treat a steady trickle as a signal that `LOCK_TIMEOUT` is short relative to
how long your workers stall, rather than as noise; the
[Production](production.md#tuning-lock_timeout) page covers the tuning.

`throughput()` and `failure_rate()` count SUCCESSFUL and FAILED rows only. A
LOST task is not an outcome, so it is in neither number; read the `lost`
column from `queue_stats()` for it.

### Schedule failure isolation

Each schedule has its own transaction. Its isolation boundary includes the
future-tick coverage read, a stored row's lock and refresh, the tick insert,
the first-sighting anchor read and write, enqueue through the configured
backend, the tick's task update and commit. `task_enqueued` receivers run
inside that transaction.

An exception from this work is caught after rollback. Its class does not
decide whether the pass continues. The worker checks the same database alias
and session without reconnecting. Rollback must have succeeded, any enclosing
transaction must remain usable, and the same connection must answer
`SELECT 1`. A replacement connection does not establish recovery.

If those checks pass, the worker reports `schedule_dispatch_error` and
continues to the next schedule. This is a schedule-scoped failure. The
connection check establishes usability, not that the failure was caused by
invalid arguments or will recur. There is no consecutive-failure cutoff:
several failed schedules do not prevent an attempt at a later healthy one.

A failed rollback, changed session or unusable connection abandons the pass.
So does a `django.db.DatabaseError` escaping a shared read, such as the
schedule source's full read or the tick-log read before traversal. A
non-database exception from the schedule source stops the worker with a
traceback and exit 1; a `django.db.InterfaceError` escaping that read is
reported as `worker_poll_failed` instead. The stored source's marker read and
boundary heal retain their separate handling and events. Any exception
escaping dispatch leaves the pass incomplete, whatever its class.

An abandoned pass reports `schedule_dispatch_failed` and stops further
traversal. It does not undo earlier committed ticks. Normal batch-empty
completion remains blocked until a later dispatch pass completes. A pass
that traverses every schedule completes even when some schedules have
schedule-scoped failures.

Genuine duplicate-key races on the tick insert remain silent. Other integrity
errors there are reported as schedule failures if the connection checks pass.
Recognized lock contention remains `schedule_lock_unavailable`, without a
traceback. It is not rate-limited or counted as a schedule rejection. On
PostgreSQL, only `lock_timeout` (`55P03`) and deadlocks take this path. A
lock wait ended by `statement_timeout` (`57014`) is reported as
`schedule_dispatch_error` if rollback and the connection checks succeed, so
set `lock_timeout` below any `statement_timeout`.

A `transaction.on_commit` callback runs after the dispatch transaction has
committed. Its failure is `schedule_dispatch_callback_failed`. The committed
tick is not retried because its callback raised.

### Schedule failure reporting

`schedule_dispatch_error` reporting is limited per worker process, database
alias and schedule. A stored schedule is tracked by primary key, so renaming
it does not reset reporting.

The first failure in a run is logged at ERROR with a traceback, `failures=1`
and `suppressed=0`. Continued failures are counted. At most once per
60 seconds, measured from that schedule's last report with a monotonic clock,
the worker logs an ERROR summary without a traceback. `failures` is the total
for the run, `suppressed` counts failures since the previous report that were
not logged individually, and `error` is the latest exception class name.
The interval is fixed, not a setting. Only reporting is throttled.

`schedule_dispatch_recovered` is logged once when that schedule next commits
a tick on this worker. That can enqueue a task or record a first-sighting
anchor; an anchor does not also emit `schedule_dispatched`. If another worker
wins the tick, this worker does not report recovery yet. Reporting state is
bounded and local to the worker. A stored schedule's failure state survives a
pause and is dropped without an event only when the row is deleted. If a
paused row is fixed and resumed, its next successful tick commit on that
worker reports `schedule_dispatch_recovered`.

`schedule_dispatch_failed` uses the same first-traceback and later-summary
pattern at WARNING, per worker. Its `failures` counts abandoned passes in
the outage. A completed dispatch pass ends the outage, so the next abandoned
pass is reported in full. There is no pass recovery event.

These events do not include schedule arguments as fields. The first traceback
includes the database's own error message, which can quote part of a rejected
value. For example, PostgreSQL can report `Token "Infinity" is invalid`.
Account for that when granting access to logs.

Alert on event names and structured keys, not message text. Read `failures`
and `suppressed` rather than treating each log line as one failed attempt.

## Monitoring recipes

- **Alerting.** Alert on `ready_count` and `oldest_ready_age` (via
  `ox_health` thresholds or the functions directly), and on
  `failure_rate` rising above your normal baseline. Use last-claim age
  only for fleet alerting on queues with steady traffic. Throughput is
  better as a dashboard line than an alert: its healthy value depends
  entirely on offered load. Also alert on `schedule_dispatch_error` and
  `schedule_dispatch_failed`, plus `schedule_row_skipped` for stored
  schedules. For stored schedules, also alert on
  `schedule_source_unavailable`: a failed marker read leaves dispatch running
  from the cached set rather than abandoning the pass. Read `failures` and
  `suppressed` rather than counting dispatch error log lines. A rejected
  dispatch leaves no task or tick row. `ox_health` has no schedule check, and
  neither `worker_batch_empty` nor batch exit 0 certifies that every schedule
  dispatched.
- **Prometheus.** Mount `django_ox.urls` and scrape `/ox/metrics`, or
  register `django_ox.metrics.collector()` with a registry you already run.
  Both are covered [above](#prometheus).
- **journald.** Configure a level-aware journal handler, such as
  `systemd.journal.JournalHandler`, or syslog-priority prefixes so
  `journalctl -u ox-worker -p warning` selects retries, reclaims and
  failures. With the default stderr handler and systemd settings, all lines
  enter the journal at info priority. A timer can run `ox_health` for
  monitoring, including file-based loop-liveness checks. Keep process
  restarts under systemd's process supervision; the file check does not
  notify its watchdog.
- **Poisoned-task triage.** When `failure_rate` spikes, the rows have the
  forensics: filter FAILED rows and read `errors` (per-attempt
  tracebacks), `attempts` and `worker_ids` to see what died where. The
  admin page below shows the same fields, and the two actions close the
  loop once the cause is fixed.

### What `errors` holds

One entry per failed attempt, each with the exception's dotted class path and
its formatted traceback, plus one the reaper writes when it gives a lease up
with no attempts left. A successful attempt adds nothing, so the entries count
failures rather than runs. A traceback is whatever Python produced for that failure,
so if an exception message or a chained cause carried a connection string, a
token or a customer's data, that is what lands in the column: the same
material your application's own error reporting already receives. Treat the
column as you treat those reports.

Two things bound it. Each traceback is stored up to 16,384 bytes of UTF-8,
marker included, so one pathological failure cannot write an unbounded string
onto the row. Bytes rather than characters, because that is the unit the column
is sized in. And `ox_prune --include-failed` is the retention
control: FAILED and LOST rows are kept by default so tracebacks survive until
somebody has looked at them, and that flag is what eventually removes them.

The admin's task page renders `errors` in full to anyone who can open it, which
is a staff user with view permission on the model. If that is a wider audience
than your error reporting has, narrow the permission rather than the column.

## Retrying and discarding

Two operator actions live in `django_ox.actions`. Each is one
compare-and-set UPDATE on the row's status and lease number, so it either
moves the row from the state it read or does nothing and says so. Neither
touches a RUNNING row: that row belongs to the worker holding its lease,
and only the reaper takes a lease away.

```python
from django_ox import actions

actions.retry(result.id)  # True if the row was requeued
actions.discard(result.id)  # True if the row was closed
```

| Function | Accepts | Does |
| --- | --- | --- |
| `retry(result_id)` | FAILED, LOST, with fewer than 32767 claims | Sets the row back to READY for one more attempt, clears `run_after` so it is eligible at once, and sets `max_attempts` to `attempts + 1`. This is an operator override, not the task's declared budget. The count, `worker_ids` and every per-attempt traceback stay as they were, so the record still says what happened before. The lease number goes up, so a LOST row's last worker, if it is still alive somewhere, writes nothing over the retry. A row at the claim ceiling is left untouched and returns `False`. |
| `expire_lease(result_id)` | RUNNING | Sets the lease's expiry into the past so the next reaper pass reclaims the task; a renewal that lands before that pass restores the lease, so read the result and call it again if needed. The task itself keeps running; the lease number refuses its finish write once another worker holds the row, so this brings the ordinary reclaim forward rather than cancelling anything. For a lease granted with a timeout that turned out to be wrong: the row carries its own deadline, so changing the setting on the workers does not move it. |
| `discard(result_id)` | READY, WAITING, FAILED, LOST | Marks the row DISCARDED. A READY or WAITING task that is discarded never runs; a discarded FAILED or LOST task is not retried. The attempt records stay. |

`retry_many(selection)` and `discard_many(selection)` make the same move
for a queryset or a list of ids. They run one conditional UPDATE per
thousand rows inside one transaction and return `(changed, skipped)`. The
admin actions use them, so a select-across of a hundred thousand rows is a
hundred UPDATEs, and either all of it lands or none does.

`retry_many()` counts each row at the claim ceiling as skipped; other
eligible rows still move. The ceiling check is part of the same UPDATE
that would grant the extra attempt.

They sort the ids first and take the rows in primary key order. On
PostgreSQL and MySQL each UPDATE follows a locking read of its thousand
rows, which is one more statement per thousand. `ox_prune` takes rows in
the same order. So a bulk retry or discard of rows a prune is deleting
waits for the prune. Before, it could fail with a deadlock. The call locks
every row it was given, whatever its status, until it ends. A worker that
writes to one of those rows waits for it.

A deadlock is still possible with other writers. After one, a call made
with no transaction open starts again from its first row. It stops after
three attempts in all. Called inside a transaction of your own, it doesn't.
The error reaches you, and your transaction has to start over.

The actions write the table directly and send no `django.tasks` signal: a
discard finishes the result without `task_finished`, and a retry requeues
it without `task_enqueued`.

Both single-row functions return `False` for any other state, for an id
that is not in the table, and for a malformed id. `retry()` also returns
`False` when `attempts` is already 32767 or higher. `RUNNING` and
`SUCCESSFUL` rows are never matched, and `retry` never matches a `WAITING`
row. A retry that races a second retry of the same row, or a discard that
races a worker's claim, resolves to exactly one winner: the UPDATE pins
the lease number it read, and the loser matches zero rows.

A retried task is one more attempt, not a fresh set. If the new attempt
fails, the row is FAILED again with one more traceback, and can be retried
again. A task retried while its worker is still missing gets the same
treatment as any at-least-once task: make the body idempotent.

DISCARDED is the sixth value in the row's status column and reads as
`FAILED` through `django.tasks`, which has four statuses, so `is_finished`
is true and a loop polling the result ends. `queue_stats()` reports
it in its own `discarded` column, and `ox_prune` deletes discarded rows
with successful ones.

WAITING is the seventh value. It reads as `READY` through `django.tasks`, so
`is_finished` is false. There, `READY` only means the task hasn't finished.
It doesn't mean a worker can take it now. `OxTask.status` still says WAITING,
and `waiting_counts()` counts it. Workers never claim a waiting task,
`ox_prune` never deletes it, and retry skips it.

### The admin page

When `django.contrib.admin` is installed, django-ox registers the task
table with it. Nothing is added to a project without the admin. The
change list shows id, task path, queue, status, attempts, and the enqueue
and finish times, filters on status and queue, and searches by id and
path. The detail page is read-only and lays out every attempt's
traceback. The two actions, **Retry selected tasks** and **Discard
selected tasks**, call `retry_many` and `discard_many` on the selection and
report how many moved and how many were skipped for being in a state the
action does not accept.

The admin does not add, edit or delete rows. A hand-edited status would
bypass the lease, and a delete could take a row from under a running
worker; `ox_prune` is the way rows leave the table. The actions need
`change_oxtask`; viewing accepts either `view_oxtask` or `change_oxtask`.

Open **Queue overview** from the task change list. It shows one row per
queue with retained task rows: status counts, eligible READY tasks, the
oldest eligible task's age, throughput per minute, failure rate, and time
since the last claim.

READY includes deferred tasks; Eligible ready excludes them. Throughput and
failure rate cover the trailing five minutes. Both display a dash when no
task finished in that window. Status totals count retained rows, not
lifetime activity. An absent oldest eligible age displays a dash; no
recorded claim displays `never`. Last claim age is not a heartbeat or proof
that a worker is alive.

Each visit scans retained task rows, so cost grows with retention. The page
does not refresh automatically. It uses the database alias selected by
`router.db_for_write(OxTask)`, not the worker's `--database` flag. To show
rows processed by `ox_worker --database other`, that router selection must
also resolve to `other`.
