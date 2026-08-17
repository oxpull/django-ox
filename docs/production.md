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
ExecStart=/srv/myproject/.venv/bin/python manage.py ox_worker --concurrency 4
Restart=always
RestartSec=5

# systemd sends SIGTERM on stop; the worker drains in-flight tasks and
# exits 0. Give the drain at least as long as your longest task before
# systemd escalates to SIGKILL.
KillSignal=SIGTERM
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl enable --now ox-worker
journalctl -u ox-worker -f
```

To run several workers on one host, use a template unit
(`ox-worker@.service` with the same `[Service]` body) and start
`ox-worker@1`, `ox-worker@2`, and so on. No flag distinguishes the
instances; every worker generates its own unique id.

## Graceful shutdown

On SIGTERM or SIGINT the worker:

1. Stops claiming new tasks immediately.
2. Waits for in-flight tasks to finish, however long they take.
3. Closes its database connections and exits with code 0.

A second signal during the drain forces an immediate exit, code 130. Whatever
was running is abandoned mid-flight. The reaper on a surviving worker reclaims
it later, and it counts as a failed attempt.

This maps directly onto rolling deploys: send SIGTERM, wait, start the new
version. The only tuning point is the supervisor's kill escalation
(`TimeoutStopSec` above) relative to your longest task.

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
For CPU-bound tasks the GIL makes threads the wrong tool. Run multiple worker
processes with `--concurrency 1` instead. That is also the natural shape for
systemd template units, or one container per worker.

## The reaper

Workers die: OOM kills, node failures, `kill -9`. When a worker dies mid-task, its task stays RUNNING behind a stale lock. The
reaper returns it to the queue. It runs inside every worker, on an interval
derived from the lock timeout:

- A RUNNING task whose lock is older than `LOCK_TIMEOUT` (default 300
  seconds, per-worker override `--lock-timeout`) is put back to READY, or
  marked FAILED with a `TaskAbandoned` error record if it has no attempts
  left.
- The attempt was already consumed when the task was claimed, so a
  crash-looping task cannot retry forever; it fails after `MAX_ATTEMPTS`
  like any other failure.
- The reclaim is a compare-and-set on the lock timestamp, so a reaper
  running late cannot stomp a task that finished or was already reclaimed.

Set `LOCK_TIMEOUT` comfortably above your longest task's runtime. A task
that legitimately runs longer than the timeout will be reclaimed and run
again while the original attempt is still executing.

**Tasks must be idempotent.** Execution is at-least-once by design: a task
is retried both when it raises and when its worker dies mid-run. Write
task bodies so that running twice is harmless (upserts, idempotency keys,
"already sent?" checks).

## PostgreSQL or SQLite

Both are fully supported and both run the full worker suite (191 tests
each). Guidance:

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
