# Monitoring

The queue and the result store are one database table. That means metrics are
just queries, with no agent or exporter process to run. There are four ways in:

- **`django_ox.stats`**, plain functions returning queue metrics.
- **`manage.py ox_health`**, the same numbers as an exit code, for cron
  alerting and container probes.
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

`ox_health` turns thresholds on those metrics into an exit code. Zero when every
enabled check passes. Non-zero with a one-line reason on stderr when one fails.
With no flags, it checks only that the database answers.

```
python manage.py ox_health --max-backlog 1000 --max-age 600
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--queue` | all queues | Restrict the checks to one queue. |
| `--format` | `text` | `json` prints one object on stdout instead of the `OK:` line: `ok`, `queue`, `backlog`, `oldest_age_seconds`, `last_claim_age_seconds` and `problems`. `queue` is `null` when no `--queue` is given. The figures are `null` when there is nothing to measure or the check could not run, as with an unreachable database or an invalid threshold. The object is printed on failure too, before the same non-zero exit. |
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Deferred tasks do not count. |
| `--max-age` | off | Fail when a READY task has been eligible to run for longer than this. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this long, or no claim was ever recorded. Accepts `7d`, `24h`, `90m`, `45s`, or a plain number of seconds. |
| `--database` | the alias `OxTask` writes to | Database alias to check. The figures come from that alias, so the check reports the queue your workers are running. |

On success it prints the measured values, which is useful in cron mail
and probe logs:

```
OK: backlog=3 oldest_age=12s last_claim_age=2s
```

Which check goes where:

- **`--max-backlog` and `--max-age` measure the whole queue.** Put them in
  fleet-level alerting, from cron or a monitoring agent. Do not put them in a
  per-worker probe: a shared backlog would fail every worker's probe and restart
  healthy workers without shifting the backlog.
- **`--worker-timeout` is the closest thing to a liveness check here.** Claiming
  is the only trace a worker leaves, so it works on queues with steady traffic
  and will false-alarm on ones that are legitimately idle.
- **For bursty queues, prefer `--max-age`.** It only fires when work exists and
  is not being picked up.

As a Kubernetes liveness probe on the worker container, for a queue with
steady traffic:

```yaml
livenessProbe:
  exec:
    command:
      ["python", "manage.py", "ox_health", "--worker-timeout", "300"]
  periodSeconds: 60
  timeoutSeconds: 10
  failureThreshold: 3
```

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
`LOGGING` as usual.

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
| `task_timed_out` | WARNING | An attempt ran past its `TASK_TIMEOUT` and is recorded as failed; a `task_retrying` or `task_failed` record follows. It counts timeouts recorded as failures, not deadlines that passed: a task that catches `TaskTimeout` and returns produces no event, and neither does a timeout on a worker logging `timeouts_backstop_only`, where the attempt ends in `task_stuck` or in whatever the task went on to do. |
| `task_stuck` | ERROR | A timed-out attempt's thread did not stop within `TASK_TIMEOUT_GRACE`. The attempt is recorded as failed and the worker is recycling. On a worker logging `timeouts_backstop_only` nothing is raised inside the task, so this is the ordinary end of a timeout there rather than a pathological one. |
| `worker_recycling` | WARNING | The worker stopped claiming after a stuck thread; it drains its other tasks and exits with code 75. It follows a `task_stuck` whose thread is still inside the attempt, which is the usual case, so on a worker logging `timeouts_backstop_only` every timeout that reaches the backstop costs a worker restart. |
| `timeouts_backstop_only` | WARNING | Once per worker: `TaskTimeout` is not raised inside a running sync task, because the interpreter cannot raise an exception inside another thread (`reason=interpreter`, logged at startup) or a coverage tool or debugger is watching the worker's threads (`reason=tracing_tool`, logged on the first attempt registered under it). `TASK_TIMEOUT_GRACE` is the whole enforcement while it stands. See [Task timeouts](production.md#task-timeouts). |
| `task_retrying` | WARNING | An attempt failed with retries remaining. |
| `task_failed` | ERROR | The task reached FAILED, out of attempts. |
| `task_reclaimed` | WARNING | The reaper took a task back from a worker that stopped refreshing its lock. One record per task. A pass whose stuck set changed while it ran (a lease renewed, or one more lease expired) instead emits a single record carrying `count` and no `task_id`, because it cannot say which tasks the reclaim covered. |
| `task_lease_lost` | WARNING | A worker finished an attempt whose lease had already been reclaimed, so its write was dropped and no result was signalled. |
| `lease_renew_failed` | WARNING | A lease renewal statement failed. Outside Django's PostgreSQL pool, this also covers connection failures during renewal. The thread continues. The next tick is due within one renewal interval and starts immediately after an overrun. |
| `lease_renew_degraded` | WARNING | On a pooled PostgreSQL database, renewal left its private connection path. The worker renewed through the pool or failed to renew through either path. `fallback` is `succeeded` or `failed`. Logged once on entering degraded renewal, not on every degraded tick. Alert on this event; `lease_renew_missed` can arrive after live work has already been reclaimed. |
| `lease_renew_fallback` | DEBUG | On a pooled PostgreSQL database, a later renewal succeeded through the pool while renewal remained degraded. |
| `lease_renew_missed` | WARNING | On a pooled PostgreSQL database, renewal missed a tick while degraded. Reports missed-renewal counts at most once per 30 seconds. `lease_renew_missed` means leases were not renewed on that tick. After two consecutive misses, the next tick lands at the lease boundary; the reaper can reclaim the task while its body is still running, allowing another run. A degraded warning with `fallback=failed` also means that tick did not renew leases and starts missed-tick reporting. One with `fallback=succeeded` does not. The 30-second missed-renewal reporting window persists across recovery. |
| `lease_renew_recovered` | INFO | On a pooled PostgreSQL database, renewal succeeded on its private connection again. Includes counts of successful fallback renewals and missed renewals during the degraded period. |
| `schedule_dispatched` | INFO | A recurring tick enqueued its task. |
| `schedule_tick_dropped` | WARNING | A tick was past its starting deadline and was not run. Carries `late_seconds`. Each worker reports a given tick once, not once per dispatch pass, so a schedule that stays droppable does not repeat the warning every second. |
| `schedule_row_skipped` | WARNING | A stored schedule could not be used: its task key is not registered, its arguments no longer validate, or its timing does not parse. The others in the same pass still run. Carries `reason`. |
| `schedule_dispatch_error` | ERROR | One schedule raised something unexpected: its task would not enqueue, its row raised. The rest of the pass continues. Not a database error; those end the pass as `schedule_dispatch_failed`. |
| `schedule_dispatch_callback_failed` | WARNING | A `transaction.on_commit` callback registered by a `task_enqueued` receiver raised after the dispatch transaction committed. The task is enqueued, the tick is recorded and the dispatch is counted; the failure is the callback's. Carries `task_id`. |
| `schedule_dispatch_failed` | WARNING | The dispatch pass hit a database error, wherever in the pass it was raised; the stored source's marker read and boundary heal are the exceptions, with events of their own below. The pass is abandoned and retried on the next one; a connection that is no longer usable is dropped, and the claim still runs. |
| `schedule_source_unavailable` | WARNING | The stored schedules could not be read, so the worker is running on the set it last read rather than on none. Repeated every dispatch pass while the read keeps failing. A stream of it right after a deploy means the code is running ahead of migration `0007`. |
| `schedule_lock_unavailable` | WARNING | The database gave up waiting for a lock another worker held: a stored schedule's row, or the tick row a settings-declared schedule's dispatch claims. MySQL's lock-wait timeout or a deadlock it resolved against this worker, SQLite's busy timeout, PostgreSQL's `lock_timeout`. The schedule is skipped this pass and its tick fires on a later one if still unclaimed. No traceback; a stream of it means one worker's dispatch transactions are long, which is usually a slow `task_enqueued` receiver. |
| `schedule_boundary_healed` | INFO | A stored schedule's timing had changed without its activation boundary moving, so the boundary was moved onto the current timing. Expected after a bulk update; repeated for one schedule is not. |
| `schedule_boundary_heal_failed` | WARNING | That move failed and will be retried. |
| `worker_error` | ERROR | The execution wrapper itself raised (an internal worker error, not a task failure). |
| `worker_poll_failed` | WARNING | A database error ended one pass of the poll loop. The pass is abandoned and retried on the next one; the worker keeps running. A steady stream of it means the database is unreachable rather than slow. |
| `watchdog_error` | ERROR | The timeout watchdog failed while handling an armed attempt or cleaning up a batch's connection. Cleanup includes closing a private connection or returning a borrowed connection. A database error while closing the private connection is suppressed without this event. The watchdog thread continues. |
| `task_stuck_unrecorded` | WARNING | A timed-out attempt could not be recorded as failed. The worker recycles regardless, so the row is recovered by the reaper rather than by this write. |
| `watchdog_connection_unavailable` | WARNING | On a pooled PostgreSQL database, the timeout watchdog could get neither a private connection nor one from the pool. Logged once per batch. Every record in the batch fails and logs `task_stuck_unrecorded`. The worker recycles regardless. This event has no time-based rate limit. |
| `worker_drain_abandoned` | WARNING | A recycling worker stopped waiting on tasks that had not finished. Their leases expire and the reaper requeues them. Carries `pending`. |
| `claim_filter_sql_missing` | WARNING | Once per worker: a subclass overrides `claim_filter_q()` without `claim_filter_sql()`, so the single-statement PostgreSQL claim is given up for the path that applies the hook. |
| `worker_draining` | INFO | Shutdown began with tasks still in flight. |
| `worker_stopped` | INFO | The run loop exited. |
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
| `worker_id` | all worker events | Unique id of the worker emitting the record. With `--processes`, the slot number is the last part of the id. |
| `task_id` | task events | The task's UUID, as a string. |
| `task_path` | task events | Dotted path of the task function. |
| `queue` | task events | Queue name. |
| `attempt` | task events | Attempts consumed so far, including the current one. |
| `duration_ms` | `task_succeeded`, `task_retrying`, `task_failed`, `task_timed_out`, `task_stuck`, `task_lease_lost` | Wall-clock duration of the attempt, in milliseconds. |
| `timeout_s` | `task_timed_out`, `task_stuck` | The timeout that applied, in seconds. |
| `grace_s` | `task_stuck`, `timeouts_backstop_only` | `TASK_TIMEOUT_GRACE`, in seconds. |
| `reason` | `timeouts_backstop_only` | Why the backstop is the whole enforcement: `interpreter` or `tracing_tool`. |
| `tracer` | `timeouts_backstop_only` with `reason=tracing_tool` | How the worker's threads are being watched: `sys.settrace` when a trace function is installed, which does not say which tool installed it, or `sys.monitoring (NAME)` for a registered tool, which names itself. |
| `exception` | `task_retrying`, `task_failed` | Exception class name of the failure. |
| `status` | `task_reclaimed` | Status after reclaim: `READY` (requeued) or `LOST` (out of attempts). |
| `count` | `task_reclaimed` without `task_id` | How many tasks that pass reclaimed. Present only on the batch record described above. |
| `held_by` | `task_reclaimed` | The worker that stopped refreshing the lock, from the row. `worker_id` on the same record is the reaper that noticed. Absent on the batch record, along with `task_id`, `task_path`, `queue` and `attempt`. |
| `dropped_status` | `task_lease_lost` | Status the dropped write would have set: `SUCCESSFUL`, `FAILED` or `READY`. |
| `schedule` | `schedule_dispatched`, `schedule_row_skipped`, `schedule_dispatch_error`, `schedule_dispatch_callback_failed`, `schedule_tick_dropped`, `schedule_lock_unavailable` for a settings-declared schedule | The schedule's name, from `SCHEDULES` or from its row. |
| `schedule_pk` | `schedule_row_skipped`, `schedule_lock_unavailable` for a stored schedule, `schedule_boundary_healed` | The stored schedule's row id. Absent for a settings-declared schedule, which has no row. |
| `scheduled_for`, `late_seconds` | `schedule_tick_dropped` | The tick that was dropped, and how late it was when the deadline rejected it. |
| `reason` | `schedule_row_skipped` | Why the row could not be used. |
| `queues` | `worker_started` | The worker's queues. |
| `concurrency` | `worker_started`, `connection_pool_too_small` | The worker's task-thread concurrency, set by `--concurrency`. |
| `database` | `connection_pool_too_small` | The worker's database alias: `--database`, or the alias used to write `OxTask`. |
| `max_size` | `connection_pool_too_small` | The effective pool maximum. `pool=True` means 4. For a non-empty mapping, use `max_size`; if absent or `None`, use `min_size`, defaulting to 4. The sizing check skips values that are not an `int` of at least 1. It excludes booleans and floats such as `10.0`. |
| `recommended_max_size` | `connection_pool_too_small` | `concurrency + 1`: one pooled connection per task thread and one for the poll loop. This does not reserve fallback capacity or validate the server budget. |
| `unpooled_connections` | `connection_pool_too_small` | Additional private-connection budget per worker process, not a count of open connections. 2 if `TASK_TIMEOUT` is set or any `TASK_TIMEOUTS` value is not `None`; otherwise 1. |
| `pending` | `worker_draining` | In-flight tasks at shutdown. |
| `processes` | `supervisor_started` | Worker processes the supervisor runs. |
| `worker_index`, `exit_code` | `worker_process_restarted`, `worker_process_recycled`, `supervisor_restart_cap` | Which slot exited and how. A negative code is the signal that killed it. |
| `delay` | `worker_process_restarted`, `worker_process_recycled` | Seconds until the slot is started again. |
| `task_id`, `exit_code` | `worker_recycling` | The stuck task that started the recycle, and the code the worker will exit with, 75. |
| `restarts` | `supervisor_restart_cap` | Deaths of that slot inside the window. |
| `worker_indexes` | `supervisor_killed_workers` | The slots that were killed. |
| `parent_pid` | `worker_orphaned` | The supervisor pid the worker was started under. |
| `exit_code` | `supervisor_stopped` | The code the supervisor exits with. |
| `error` | `lease_renew_degraded`, `lease_renew_fallback`, `lease_renew_missed`, `watchdog_connection_unavailable` | The private-connection failure reason. When the pool serves a pool-first renewal tick, this instead says the private connection was not tried first and gives the remaining lease time. |
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

## Monitoring recipes

- **Alerting.** Alert on `ready_count` and `oldest_ready_age` (via
  `ox_health` thresholds or the functions directly), and on
  `failure_rate` rising above your normal baseline. Throughput is better
  as a dashboard line than an alert: its healthy value depends entirely
  on offered load.
- **Prometheus.** Mount `django_ox.urls` and scrape `/ox/metrics`, or
  register `django_ox.metrics.collector()` with a registry you already run.
  Both are covered [above](#prometheus).
- **journald.** Under systemd, WARNING and above maps onto journal
  priorities, so `journalctl -u ox-worker -p warning` shows exactly
  retries, reclaims and failures. Pair it with `ox_health` in a timer for
  active checks.
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
| `retry(result_id)` | FAILED, LOST | Sets the row back to READY for one more attempt, clears `run_after` so it is eligible at once, and raises `max_attempts` to `attempts + 1`. The count, `worker_ids` and every per-attempt traceback stay as they were, so the record still says what happened before. The lease number goes up, so a LOST row's last worker, if it is still alive somewhere, writes nothing over the retry. |
| `expire_lease(result_id)` | RUNNING | Sets the lease's expiry into the past so the next reaper pass reclaims the task; a renewal that lands before that pass restores the lease, so read the result and call it again if needed. The task itself keeps running; the lease number refuses its finish write once another worker holds the row, so this brings the ordinary reclaim forward rather than cancelling anything. For a lease granted with a timeout that turned out to be wrong: the row carries its own deadline, so changing the setting on the workers does not move it. |
| `discard(result_id)` | READY, WAITING, FAILED, LOST | Marks the row DISCARDED. A READY or WAITING task that is discarded never runs; a discarded FAILED or LOST task is not retried. The attempt records stay. |

`retry_many(selection)` and `discard_many(selection)` make the same move
for a queryset or a list of ids. They run one conditional UPDATE per
thousand rows inside one transaction and return `(changed, skipped)`. The
admin actions use them, so a select-across of a hundred thousand rows is a
hundred UPDATEs, and either all of it lands or none does.

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
that is not in the table, and for a malformed id. `RUNNING` and `SUCCESSFUL` rows are never
matched, and `retry` never matches a `WAITING` row. A retry that races a second retry of the same row, or a discard
that races a worker's claim, resolves to exactly one winner: the UPDATE
pins the lease number it read, and the loser matches zero rows.

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
worker; `ox_prune` is the way rows leave the table. The actions need the
`change_oxtask` permission; viewing needs `view_oxtask`.
