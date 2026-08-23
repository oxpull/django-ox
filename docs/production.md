# Production

The worker is a plain foreground process: `manage.py ox_worker`, run
under whatever supervises your other processes. It treats a lost database
connection as fatal rather than retrying blind, and relies on the
supervisor to restart it. Run it under `Restart=always` (as in the unit
below). This page covers systemd,
scaling, shutdown, the reaper, and monitoring.

## Running under systemd

```ini
# /etc/systemd/system/ox-worker.service
[Unit]
Description=django-ox worker
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=exec
User=app
Group=app
WorkingDirectory=/srv/myproject
Environment=DJANGO_SETTINGS_MODULE=myproject.settings
ExecStart=/srv/myproject/.venv/bin/python manage.py ox_worker --processes 2 --concurrency 4
Restart=always
RestartSec=5

# systemd sends SIGTERM on stop; the worker drains in-flight tasks and
# exits 0. Give the drain at least as long as your longest task before
# systemd escalates to SIGKILL. KillMode=mixed sends that SIGTERM to the
# supervisor alone, which forwards it once; the default of sending it to
# every process in the group would reach each worker twice, and a second
# signal is the force-exit.
KillSignal=SIGTERM
KillMode=mixed
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl enable --now ox-worker
journalctl -u ox-worker -f
```

There are two ways to put several worker processes on one host. The unit
above uses `--processes 2`: one unit, one supervisor, two workers, and one
place to set the flags. The other is a template unit (`ox-worker@.service`
with the same `[Service]` body and `--processes 1`), started as
`ox-worker@1`, `ox-worker@2`, and so on, which makes each worker its own
unit with its own journal entry and restart counter.

Use `--processes` unless you need to stop, restart or give flags to one
worker at a time. A supervisor that restarts a dead worker in a second, with
one command to edit, is the common case. The template unit is the right
shape when the workers differ, for instance one unit per queue with its own
`--lock-timeout`, and for that a queue flag per unit says more than a
process count.

## Running in containers

The worker is a foreground process that exits 0 on SIGTERM, so it needs no
special entrypoint. The setting that matters is the grace period: give the
runtime longer than your slowest task before it escalates to SIGKILL.

```yaml
services:
  worker:
    image: myapp:latest
    command: python manage.py ox_worker --processes 2 --concurrency 4
    stop_grace_period: 5m
    restart: unless-stopped
    depends_on:
      - db
    healthcheck:
      test: ["CMD", "python", "manage.py", "ox_health"]
      interval: 60s
      timeout: 15s
      start_period: 30s
```

Docker's default grace period is 10 seconds, which will kill a worker mid-task
and leave the reaper to clean up. `stop_grace_period` is the container
equivalent of `TimeoutStopSec`. On Kubernetes it is
`terminationGracePeriodSeconds` on the pod spec.

`--processes` inside one container, or one worker per container with the
replica count doing the scaling, both work. The runtime restarts a container,
the supervisor restarts a worker process, and each takes about a second. One
container per worker keeps the runtime's own health and restart accounting
per worker, which is worth having on an orchestrator; `--processes` keeps the
number of containers down on a single host.

With no flags, `ox_health` checks that the database answers, which is what a
per-container probe should test. Queue-wide checks belong in fleet alerting
rather than in a probe: see
[which check goes where](monitoring.md#health-checks-ox_health), and the
liveness probe example there for queues with steady traffic.

Run migrations before rolling workers, as an init container or a job, not from
the worker itself. Several workers starting at once would race the same
migration.

## Graceful shutdown

On SIGTERM or SIGINT the worker:

1. Stops claiming new tasks immediately.
2. Waits for in-flight tasks to finish, however long they take.
3. Closes its database connections and exits with code 0.

A second signal during the drain forces an immediate exit, code 130. Whatever
was running is abandoned mid-flight. The reaper on a surviving worker reclaims
it later, and it counts as a failed attempt.

This maps directly onto rolling deploys: send SIGTERM, wait, start the new
version. The only tuning point is the process manager's kill escalation
(`TimeoutStopSec` above) relative to your longest task.

With `--processes` above 1, the signal goes to the supervisor. It forwards
SIGTERM to every worker process, waits for each to drain, and exits 0 when all
of them did, otherwise with the first non-zero code. A second signal is
forwarded again, which is the force-exit on each worker. Send the signal to
the supervisor only: a worker that also receives the terminal's copy of a
Ctrl-C has seen two signals, which is why the workers run in their own process
group and why the systemd unit above sets `KillMode=mixed`.

## Scaling out

Run as many workers as you need, on as many hosts as you need, pointed at
the same database. No coordinator, no leader election. Two things make
concurrent workers safe:

- **Claiming is atomic.** On PostgreSQL, a claim is one
  `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`
  statement, so workers never block each other on the head of the queue.
  On other databases with `SKIP LOCKED` support (MySQL 8+), the worker
  uses `SELECT ... FOR UPDATE SKIP LOCKED` in a short transaction. Databases without it, SQLite included, fall back to an optimistic
compare-and-set UPDATE. That is atomic everywhere, but under contention workers
can retry against each other for the head of the queue.
- **Recurring schedules need no dedicated node.** Every worker dispatches;
  a unique constraint guarantees each tick fires once. See
  [Recurring tasks](recurring-tasks.md#many-workers-one-tick).

Workers can also be split by queue: run
`ox_worker --queues emails --concurrency 8` next to
`ox_worker --queues default` to isolate slow or bursty workloads.

### Threads and processes

`--concurrency N` is a thread pool inside one process. That fits the
common Django task profile: email, HTTP calls to third parties, ORM work.
For CPU-bound tasks the GIL makes threads the wrong tool; use
`--processes N` there, with `--concurrency 1`, and the same command on one
host gives you N interpreters.

```
python manage.py ox_worker --processes 4 --concurrency 1
```

Each process is a complete worker with its own database connections, lease
renewal and reaper, and its own worker id with the slot number on the end, so
`worker_ids` on a task row says which process ran it. Nothing is shared
across the processes except the database: the supervisor starts each one as a
fresh interpreter running `ox_worker --processes 1` with the same flags, so a
worker under the supervisor is the same code as a worker started by hand. The
supervisor itself never opens a database connection.

A worker process that exits on its own, whatever the cause, is restarted a
second later and the supervisor logs `worker_process_restarted` with the
exit code at WARNING. Its in-flight tasks go through the ordinary lease path:
the reaper on a surviving process takes them back after `LOCK_TIMEOUT`. More
than five restarts inside one minute, counted across all processes, stops the
supervisor with exit code 1 and `supervisor_restart_cap` at ERROR, so a
worker that cannot start hands the fault to the process manager and its
restart policy rather than logging a restart a second forever.

Two things `--processes` does not do. It does not run on Windows, where
there are no POSIX signals to forward; run one `ox_worker` per process there.
And it does not replace a process manager: the supervisor is a foreground
process that expects to be restarted itself, like the single worker.

## The lease

A worker that claims a task takes a lease on it: the row records who holds
it, when the lock was last refreshed, and a lease number that goes up by one
every time the task changes hands. Three things follow from that number, and
they are worth understanding together because they are what makes recovery
safe.

**The worker keeps its own lease alive.** While a task is executing, its
worker refreshes the lock timestamp on the rows it is running, one statement
per interval however many are in flight, and it keeps doing so through a
graceful drain. So a task that takes an hour does not look abandoned after
five minutes. The reaper reclaims work from workers that stopped reporting,
not from tasks that are merely slow.

**A finish write only lands while the lease still holds.** When a worker
records success, failure or a retry, the UPDATE carries the lease number it
was given at claim time. If the task was taken off it in the meantime, that
number no longer matches and the write is dropped rather than applied, and no
completion is signalled for it. This is arithmetic rather than timing: no
pause is long enough to get around it, so a task that finished cannot be put
back on the queue by a straggler.

**Timestamps come from the database.** With `USE_TZ` on, the lock time is
written by the database server and compared against the database server's
clock, so two hosts with drifting clocks do not produce false reclaims. If you
run workers on more than one host, that is the setting which gives them one
clock, and it is Django's default.

With `USE_TZ` off the worker's clock is used instead. A database's own clock
does not always match what these columns hold there: SQLite's is UTC while the
columns carry naive local time, and reading one against the other would make
`ox_prune --older-than` treat rows that finished seconds ago as hours old.
Under that setting, keep `TIME_ZONE` and the timezone your workers run in the
same, which is what Django assumes of it anyway.

## The reaper

Workers still die: OOM kills, node failures, `kill -9`. A dead worker stops
refreshing its lock, and the reaper picks the task up. It runs inside every
worker, on an interval derived from the lock timeout.

A RUNNING task whose lock has not been refreshed for `LOCK_TIMEOUT` (default
300 seconds, per-worker override `--lock-timeout`) is taken back, and what
happens next depends on whether the task has attempts left:

- **Attempts remaining.** The task goes back to READY and the lease number
  goes up, so the old worker cannot write to it again. This is the ordinary
  case, and it is a guess the system already absorbs: at-least-once execution
  means the task may run twice, which is why task bodies must be idempotent.
- **No attempts remaining.** The row is marked LOST. LOST means what it says:
  the worker holding this task stopped reporting and nobody observed how the
  attempt ended. The reaper does not record a failure, because it did not see
  one. It has watched a lock go quiet, and that is all it writes down.

The attempt was already consumed when the task was claimed, so a
crash-looping task cannot retry forever; it stops after `MAX_ATTEMPTS` like
any other task.

The reclaim is a compare-and-set on the lease number, so a reaper running
late cannot stomp a task that finished or was already reclaimed.

### What LOST looks like from the outside

`django.tasks` has four result statuses and django-ox does not add a fifth to
them. A LOST task reads as `FAILED` through `get_result()`, and
`result.errors` ends with a `TaskAbandoned` record whose text says the lease
was lost and the outcome was never observed. `is_finished` is true, so code
that waits for a result terminates instead of waiting for a worker that is
not coming back.

The row keeps the distinction the four statuses cannot carry. Its status
column is LOST rather than FAILED, `queue_stats()` reports it in its own
`lost` column, and `ox_prune --include-failed` treats it like a failed row
for retention. Like a failed row it can be retried or discarded, from the
admin or with `django_ox.actions`; see
[Retrying and discarding](monitoring.md#retrying-and-discarding).

One case is worth knowing about before it surprises you. If the worker
holding a LOST task was starved rather than dead, and it comes back and
records a success, the row becomes SUCCESSFUL and a caller reading it twice
sees `FAILED` and then `SUCCESSFUL`. Only that one execution can do this, and
only while the row is still LOST. It is the honest cost of giving a
four-valued API an answer for a task whose outcome nobody saw, and the
alternative, reporting it as still running forever, hangs every caller that
waits on it.

It takes two things at once: the task's attempts spent, and its lease allowed
to lapse. Renewal holds the lease for as long as the worker is answering, so
what reaches this state is a worker that went unresponsive for longer than
`LOCK_TIMEOUT` and then came back, which is the case you asked the reaper to
act on in the first place. If your callers cannot tolerate seeing it, raise
`LOCK_TIMEOUT` until a merely slow worker is never reclaimed; the cost is that
a genuinely dead one takes that much longer to notice.

### Tuning LOCK_TIMEOUT

Set `LOCK_TIMEOUT` above the longest gap you expect between a worker's lease
renewals, not above your longest task. Renewal runs every `LOCK_TIMEOUT / 3`
seconds, so two consecutive renewals can be missed before anything is
reclaimed. The value is really a statement about how long a worker may be
unresponsive before you want its work handed to somebody else: too low and a
paused or overloaded worker loses tasks it was going to finish, too high and
recovery after a real crash is slow.

Watch for `task_lease_lost` in the logs. It records an attempt whose result
was discarded because the lease had already been reclaimed, and a steady
trickle of it means the timeout is short relative to how long your workers
go unresponsive.

**Tasks must be idempotent.** Execution is at-least-once by design: a task
is retried both when it raises and when its worker dies mid-run. Write
task bodies so that running twice is harmless (upserts, idempotency keys,
"already sent?" checks).

## PostgreSQL or SQLite

Both are fully supported and both run the full worker suite in CI.
Guidance:

- **PostgreSQL** is the production recommendation. It gets the
  single-statement `SKIP LOCKED` claim path, and it handles many workers
  and high write concurrency the way you would expect.
- **SQLite** is fine for development, tests, and small single-host
  deployments in the same situations where SQLite is fine as your Django
  database at all. Claiming uses the compare-and-set path and remains
  correct with multiple workers, but SQLite's single-writer nature makes
  many busy workers on one file a poor fit.

The queue lives in your default database, inside your existing backup and
migration story. That is the point: one system of record, one thing to
operate.

## Monitoring

Monitoring has a [dedicated page](monitoring.md). The operational
summary:

- **The table is the queue.** `django_ox.stats` exposes queue depth,
  backlog age, throughput and failure rate as plain functions. Backlog
  depth and backlog age are the two numbers worth alerting on.
- **`manage.py ox_health`** turns thresholds on those numbers into an
  exit code, for cron alerting and container probes.
- **Logs.** The worker logs to the `django_ox` logger: lifecycle at
  INFO, retries and reaper reclaims at WARNING, terminal failures and
  unhandled worker errors at ERROR, each with stable extra keys for JSON
  log handlers. Under systemd this lands in the journal.
- **Per-task forensics.** Each row keeps its attempts count, the id of
  every worker that ran it, timestamps for enqueue/start/finish, and the
  full traceback of every failed attempt.

## Pruning on a timer

Finished rows accumulate; prune them on a schedule sized to how long you
need results and tracebacks to stay queryable. With systemd:

```ini
# /etc/systemd/system/ox-prune.service
[Unit]
Description=Prune finished django-ox tasks

[Service]
Type=oneshot
User=app
WorkingDirectory=/srv/myproject
Environment=DJANGO_SETTINGS_MODULE=myproject.settings
ExecStart=/srv/myproject/.venv/bin/python manage.py ox_prune --older-than 7d
```

```ini
# /etc/systemd/system/ox-prune.timer
[Unit]
Description=Daily django-ox prune

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Or from cron, or as a recurring task pointed at a small wrapper task of
your own. Flag reference on the
[Configuration](configuration.md#ox_prune) page. FAILED rows are kept by
default so tracebacks survive until you have looked at them; add
`--include-failed` once that is not needed.
