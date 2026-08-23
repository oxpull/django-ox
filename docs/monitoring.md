# Monitoring

The queue and the result store are one database table. That means metrics are
just queries, with no agent or exporter process to run. There are three ways in:

- **`django_ox.stats`**, plain functions returning queue metrics.
- **`manage.py ox_health`**, the same numbers as an exit code, for cron
  alerting and container probes.
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
# [QueueStats(queue_name="default", ready=3, running=1, failed=0, successful=214),
#  QueueStats(queue_name="emails", ready=0, running=0, failed=2, successful=560)]

stats.ready_count()  # tasks eligible to run right now
stats.oldest_ready_age()  # timedelta, or None when nothing waits
stats.throughput(timedelta(minutes=5))  # terminal outcomes per minute
stats.failure_rate(timedelta(minutes=5))  # 0.0 to 1.0, or None
stats.last_claim_age()  # time since a worker last claimed
```

| Function | Returns | Semantics |
| --- | --- | --- |
| `queue_stats()` | `list[QueueStats]` | Raw row counts per queue and status (`ready`, `running`, `failed`, `successful`, `lost`, `discarded`), one entry per queue with any rows. The `ready` column counts every READY row, including tasks deferred to a future `run_after`. `lost` counts tasks whose worker stopped reporting with no attempts left; see [the reaper](production.md#the-reaper). `discarded` counts tasks an operator closed without running; see [Retrying and discarding](#retrying-and-discarding). |
| `ready_count()` | `int` | READY tasks eligible to run now, mirroring the worker's dequeue predicate: deferred tasks do not count until `run_after` passes. This is the backlog number. |
| `oldest_ready_age()` | `timedelta \| None` | Age of the oldest task waiting to run, measured from when it became eligible (`run_after` when set, `enqueued_at` otherwise), so a task deferred by a week does not read as a week of backlog. |
| `throughput(window)` | `float` | Tasks reaching a terminal state (SUCCESSFUL or FAILED) per minute over the trailing window (default 5 minutes). |
| `failure_rate(window)` | `float \| None` | Fraction of terminal outcomes in the window that FAILED, or `None` when nothing finished. Retries still pending are not outcomes and do not count. |
| `last_claim_age()` | `timedelta \| None` | Time since any worker last claimed a task, or `None` if none ever was. This is claim activity, not a heartbeat: idle workers over an empty queue record nothing. |

Every function except `queue_stats()` accepts a `queue_name` keyword to
scope the metric to one queue.

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
| `--max-backlog` | off | Fail when more than this many READY tasks are eligible to run. Deferred tasks do not count. |
| `--max-age` | off | Fail when the oldest waiting task has waited longer than this many seconds since becoming eligible. |
| `--worker-timeout` | off | Fail when no worker has claimed a task within this many seconds, or no claim was ever recorded. |

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
| `task_claimed` | DEBUG | A task was claimed from the queue. |
| `task_started` | DEBUG | Execution of an attempt begins. |
| `task_succeeded` | INFO | The task reached SUCCESSFUL. |
| `task_timed_out` | WARNING | An attempt ran past its `TASK_TIMEOUT`. The worker recorded it as failed and left the thread running; a `task_retrying` or `task_failed` record follows. |
| `task_retrying` | WARNING | An attempt failed with retries remaining. |
| `task_failed` | ERROR | The task reached FAILED, out of attempts. |
| `task_reclaimed` | WARNING | The reaper took a task back from a worker that stopped refreshing its lock. |
| `task_lease_lost` | WARNING | A worker finished an attempt whose lease had already been reclaimed, so its write was dropped and no result was signalled. |
| `lease_renew_failed` | WARNING | A lease renewal statement failed. The worker keeps going and tries again on the next interval. |
| `schedule_dispatched` | INFO | A recurring tick enqueued its task. |
| `worker_error` | ERROR | The execution wrapper itself raised (an internal worker error, not a task failure). |
| `worker_draining` | INFO | Shutdown began with tasks still in flight. |
| `worker_stopped` | INFO | The run loop exited. |

| Key | Present on | Meaning |
| --- | --- | --- |
| `event` | all events | The event name from the table above. |
| `worker_id` | all events | Unique id of the worker emitting the record. |
| `task_id` | task events | The task's UUID, as a string. |
| `task_path` | task events | Dotted path of the task function. |
| `queue` | task events | Queue name. |
| `attempt` | task events | Attempts consumed so far, including the current one. |
| `duration_ms` | `task_succeeded`, `task_retrying`, `task_failed`, `task_timed_out`, `task_lease_lost` | Wall-clock duration of the attempt, in milliseconds. |
| `timeout_s` | `task_timed_out` | The timeout that applied, in seconds. |
| `exception` | `task_retrying`, `task_failed` | Exception class name of the failure. |
| `status` | `task_reclaimed` | Status after reclaim: `READY` (requeued) or `LOST` (out of attempts). |
| `dropped_status` | `task_lease_lost` | Status the dropped write would have set: `SUCCESSFUL`, `FAILED` or `READY`. |
| `schedule` | `schedule_dispatched` | Schedule name from `SCHEDULES`. |
| `queues`, `concurrency` | `worker_started` | The worker's configuration. |
| `pending` | `worker_draining` | In-flight tasks at shutdown. |

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
- **Prometheus.** No exporter ships with django-ox, and none ships in
  [Pro](pro.md) either. The stats functions drop into any Django metrics setup
  though. Either register a custom collector with django-prometheus, whose
  `collect()` calls `queue_stats()`, `ready_count()` and `oldest_ready_age()`
  and yields gauges, or render the same numbers from a plain Django view in the
  Prometheus text format and point a scrape job at it. Label by `queue_name`.
- **journald.** Under systemd, WARNING and above maps onto journal
  priorities, so `journalctl -u ox-worker -p warning` shows exactly
  retries, reclaims and failures. Pair it with `ox_health` in a timer for
  active checks.
- **Poisoned-task triage.** When `failure_rate` spikes, the rows have the
  forensics: filter FAILED rows and read `errors` (per-attempt
  tracebacks), `attempts` and `worker_ids` to see what died where. The
  admin page below shows the same fields, and the two actions close the
  loop once the cause is fixed.

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
| `discard(result_id)` | READY, FAILED, LOST | Marks the row DISCARDED. A READY task that is discarded never runs; a discarded FAILED or LOST task is not retried. The attempt records stay. |

Both return `False` for any other state, for an id that is not in the
table, and for a malformed id. `RUNNING` and `SUCCESSFUL` rows are never
matched. A retry that races a second retry of the same row, or a discard
that races a worker's claim, resolves to exactly one winner: the UPDATE
pins the lease number it read, and the loser matches zero rows.

A retried task is one more attempt, not a fresh set. If the new attempt
fails, the row is FAILED again with one more traceback, and can be retried
again. A task retried while its worker is still missing gets the same
treatment as any at-least-once task: make the body idempotent.

DISCARDED is the sixth value in the row's status column and reads as
`FAILED` through `django.tasks`, which has four statuses, so `is_finished`
is true and callers waiting on the result return. `queue_stats()` reports
it in its own `discarded` column, and `ox_prune` deletes discarded rows
with successful ones.

### The admin page

When `django.contrib.admin` is installed, django-ox registers the task
table with it. Nothing is added to a project without the admin. The
change list shows id, task path, queue, status, attempts, and the enqueue
and finish times, filters on status and queue, and searches by id and
path. The detail page is read-only and lays out every attempt's
traceback. The two actions, **Retry selected tasks** and **Discard
selected tasks**, call the functions above on each selected row and
report how many moved and how many were skipped for being in a state the
action does not accept.

The admin does not add, edit or delete rows. A hand-edited status would
bypass the lease, and a delete could take a row from under a running
worker; `ox_prune` is the way rows leave the table. The actions need the
`change_oxtask` permission; viewing needs `view_oxtask`.
