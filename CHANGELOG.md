# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-09-11

**Two migrations ship with this release.** `0005_dequeue_index` rebuilds the
index the claim reads and adds a second one; `0006_lease_expiry` adds a
nullable column and an index on it. Migrate before rolling any process that
imports django-ox 1.1.0, web processes included: `enqueue()` writes the column
0006 adds.

On PostgreSQL, `CREATE INDEX` takes a lock that blocks enqueues and claims for
the duration; MySQL 8 builds a secondary index online. On a large PostgreSQL
task table, build the indexes by hand and fake the migrations, in an order
that keeps the claim indexed throughout. Build the replacement
`ox_dequeue_idx` under a temporary name, swap it in, then add the other two:

```
CREATE INDEX CONCURRENTLY ox_dequeue_idx_new
    ON django_ox_oxtask (status, priority DESC, enqueued_at);
DROP INDEX CONCURRENTLY ox_dequeue_idx;
ALTER INDEX ox_dequeue_idx_new RENAME TO ox_dequeue_idx;
CREATE INDEX CONCURRENTLY ox_dequeue_queue_idx
    ON django_ox_oxtask (status, queue_name, priority DESC, enqueued_at);
ALTER TABLE django_ox_oxtask
    ADD COLUMN lease_expires_at timestamp with time zone NULL;
CREATE INDEX CONCURRENTLY ox_reaper_expiry_idx
    ON django_ox_oxtask (status, lease_expires_at);
```

then `migrate --fake django_ox 0006`. The `ADD COLUMN` is nullable with no
default, which is a catalogue change. `sqlmigrate django_ox 0005` and
`sqlmigrate django_ox 0006` print the same statements without
`CONCURRENTLY`, for checking against your settings.

### Added

- `lease_expires_at` on the task row: when the lease stops being valid,
  written by the worker that took it and refreshed on every renewal. Every
  reaper judges that column instead of deriving a deadline from its own
  `LOCK_TIMEOUT`, so the setting can be changed in a rolling deploy.

  A row claimed before the column existed has it empty, and the reaper keeps
  comparing `locked_at` against its own timeout for those. The first renewal
  after the upgrade fills it in, so a fleet converges lease by lease with
  nothing for an operator to run. An expiry older than the row's own
  `locked_at` was left there by a worker that does not know the column and
  counts as absent too, so with `LOCK_TIMEOUT` unchanged across the rollout
  a 1.0.0 worker that re-claims a row is judged on `locked_at`.
- `django_ox.actions.expire_lease(result_id)` expires a RUNNING task's lease so
  the next reaper pass reclaims it. The row carries its own deadline now, so a
  lease granted with a timeout that turned out to be wrong outlives the
  configuration that granted it; this is how to shorten one. It does not stop
  the task.
- `lease_expires_at` appears in the admin's Lease fieldset.

### Fixed

- A worker that renews its lease late, but renews, keeps its task. The reaper
  checks the expiry in the reclaim itself, against the same cutoff it selected
  with, so only a lease still stale at the moment of the write is taken back.
- The lease renewal thread survives a dropped connection. It discards the
  connection, reconnects on the next interval, and keeps `locked_at` moving on
  every row the worker holds.
- The poll loop survives a database error. The pass is abandoned, logged as
  `worker_poll_failed`, and retried on the next one with a fresh connection,
  so one blip is not a worker death and cannot reach the supervisor's restart
  cap.
- The timeout watchdog handles each stuck attempt on its own. A failure
  recording one is logged as `watchdog_error` and every other armed attempt
  keeps its deadline, and the recycle that frees the worker happens whether
  or not the record was written.
- A worker whose timeout backstop gives up on a thread recycles on whether
  that thread is still running, not on whether its own outcome write landed.
  The pool slot is freed and the drain does not wait on the thread.
- The drain waits for healthy work and stops waiting for abandoned work. It
  counts threads still inside the attempt that was abandoned, rather than
  pool threads that happen to be alive, and it observes a backstop that fires
  part way through an ordinary drain.
- The drain is safe to run while a second attempt goes stuck: the stuck set
  is written under the same lock the drain reads it under.
- Every statement in the claim protocol runs on the database the worker
  writes to, including the raw PostgreSQL claim, so a read replica cannot
  answer a question that decides who holds a task.
- `enqueue_many` opens its transaction on the connection `OxTask` routes to,
  so its all-or-nothing guarantee holds under a database router.
- A worker subclass that overrides `claim_filter_q()` alone takes the claim
  path that applies it, on every database. It logs
  `claim_filter_sql_missing` once to say it gave up the single-statement
  PostgreSQL claim to do so.
- The PostgreSQL claim, the renewal and the reaper stamp and judge the lease
  on one clock: the database's with `USE_TZ` on, the worker's with it off.
- The reaper requeues abandoned rows in one UPDATE per pass, up to
  `reap_batch` rows at a time, and retires rows whose attempts are spent in
  the same bounded batches. Its cost per pass is bounded whatever the size of
  the stuck set.
- The reclaim record names only tasks the pass reclaimed. A pass
  whose stuck set changed while it ran reports a `count` and names nobody.
- The claim reads its candidate out of an index, in order, for a worker that
  names one queue, several, or none. Two index shapes ship and the planner
  picks per query. Migration `0005_dequeue_index` builds them.
- Dispatching schedules reads only the ticks it is asking about: the tick log
  is bounded to the oldest due tick across the configured schedules, which the
  unique index can seek to, and the read is pinned to the database the worker
  writes to.
- The tick row is written before its task is enqueued, so on every tick
  exactly one worker enqueues and the others announce nothing. A tick already
  in the log is not dispatched again whatever a fast-clocked worker recorded
  after it, and `task_enqueued` fires only for a task that exists.
- A signal receiver that raises is not charged to the task. A `task_started`
  receiver's exception does not spend an attempt, and a `task_enqueued`
  receiver's exception does not surface from `enqueue()` over a task that is
  already committed.
- A task run through `run_once()` keeps its lease renewed for the duration,
  the same as a task on the pool.
- `KeyboardInterrupt` and `SystemExit` reach the caller when a task runs
  through `run_once()`. On the worker pool they are recorded as a failed
  attempt, so one task calling `sys.exit()` cannot stop a fleet.
- The retry delay stays in range for any `MAX_ATTEMPTS`. The doubling is
  capped before the multiplication, so the failure path never raises on the
  arithmetic.
- `LOCK_TIMEOUT`, `BACKOFF_INITIAL` and `BACKOFF_MAX` are validated at
  `manage.py check` as `django_ox.E010`, by the same rule as the timeout
  options: a positive, finite number of seconds.
- Each stored traceback is capped at 16,384 bytes, marker included, with both
  ends kept. One failure cannot write an unbounded string onto its own row.
- `django_ox.remaining()` is measured on the monotonic clock the timeout is
  enforced with, so a clock correction cannot put the two on different sides
  of the same instant. `django_ox.deadline()` still answers with a wall-clock
  time.
- `manage.py ox_worker` starts on Windows. Stop signals are built from the
  ones the platform has; `--processes` above 1 still refuses off POSIX with
  its usual message.

### Changed

- A recycling worker now bounds how long it waits for its other in-flight
  tasks, at `LOCK_TIMEOUT`. Past that it stops renewing their leases and exits,
  and the reaper requeues them. This can end a task that was going to finish:
  it is recorded as a lost lease and retried, and one on its last attempt
  reaches LOST. The bound is what makes a recycle certain on a process that
  has stopped trusting one of its threads.
- `MAX_ATTEMPTS` and the `attempt` log key count claims, not invocations. The
  behaviour is unchanged; the reference table now says so.
- Five log events that only fire while something is wrong are documented:
  `worker_poll_failed`, `watchdog_error`, `task_stuck_unrecorded`,
  `worker_drain_abandoned` and `claim_filter_sql_missing`.
- `task_reclaimed` carries `held_by`, the worker that stopped refreshing the
  lock. `worker_id` on the same record is the reaper that noticed.
- SQLite guidance: run one worker and give it threads with `--concurrency`,
  rather than several worker processes. `SKIP LOCKED` is what lets workers step
  past each other's rows, and a database without it hands out the head of the
  queue one worker at a time.
- `task_reclaimed` is still one record per task on the ordinary path. When the
  stuck set changes underneath a reap pass (a lease renewed, or one more
  expired), that pass emits a single record carrying `count` and no `task_id`,
  rather than naming tasks it cannot vouch for.
- The production page says which databases give the lease one shared clock.
  SQLite computes `Now()` inside the process that runs the statement, so every
  worker uses its own clock there whatever `USE_TZ` says; run SQLite on one
  host. With `USE_TZ` off the lease is on the worker's clock on every
  database, so keep worker clocks within two thirds of `LOCK_TIMEOUT` of
  each other, the timeout less the renewal interval.

## [1.0.0] - 2026-09-05

This release marks django-ox as production ready. The public API is stable from
here: anything documented keeps working until a 2.0, and breaking changes get a
major version. The package runs on Django 5.2 LTS through 6.1, on SQLite,
PostgreSQL and MySQL, and the suite covers all of them.

### Added

- Django 5.2 LTS support. Django ships the Tasks framework in core from 6.0;
  on 5.2 the same framework comes from the `django-tasks` backport, and a new
  `backport` extra pulls it in: `pip install "django-ox[backport]"`. Every
  import of the framework now goes through `django_ox.compat`, which picks
  core or the backport at runtime, so nothing else in the package changed.
  CI runs the whole suite on 5.2 against the backport, on SQLite, PostgreSQL
  and MySQL.
  Python 3.14 is not in the 5.2 legs, since Django 5.2 does not support it.
- `Django>=5.2` replaces `Django>=6.0` as the declared dependency, and
  `Framework :: Django :: 5.2` joins the classifiers.

### Changed

- `finished_at` is stamped from the process clock instead of being computed by
  the database and read back, so an outcome write is one statement. The lease
  is untouched: `locked_at` and the reaper's cutoff stay on the database
  clock, which is where a lease is judged.
- The `backport` extra requires `django-tasks>=0.12`. Earlier backport releases
  change the framework API in ways django-ox does not support: 0.9.0 has no
  `enqueue_on_commit` on `Task`, 0.10.0 rejects the result statuses django-ox
  stores, and 0.11.0 adds an abstract `save_metadata` that the backend does not
  implement. CI now installs the floor with `==` and asserts the resolved
  version.
- `Development Status :: 5 - Production/Stable` replaces the beta classifier.

## [0.4.0] - 2026-09-01

### Added

- `WORKER_CLASS` in a backend's `OPTIONS`: the dotted path of the `Worker`
  subclass `ox_worker` runs. Resolved from settings rather than from the
  command line, so a worker chosen here is the worker in every child
  process under `ox_worker --processes N`, not only at `--processes 1`.
- `Worker.claim_filter_q()` and `Worker.claim_filter_sql()`, two hooks a
  subclass overrides to narrow what it may claim. The condition is applied
  inside the candidate select on all three claim paths, ahead of its
  ordering and its limit, so a narrowed worker still claims runnable work
  behind rows it declines. `claim_filter_q()` covers the two paths that
  build a queryset and `claim_filter_sql()` the PostgreSQL claim, which
  builds its own SQL, so implement both. The defaults are `None` and an
  empty fragment, so the emitted SQL is unchanged without them.

## [0.3.1] - 2026-08-24

### Fixed

- `ox_worker --processes N` exits 0 when a stop signal arrives while a
  worker process is still starting up. A worker cannot act on a signal
  until it has installed its handler, and that is after Django has been
  imported. A signal landing before then killed the worker outright, and
  the supervisor reported that worker's 143 as its own exit code. A unit
  on `Restart=on-failure` reads that as a fault and starts the service
  again. The worker had claimed no work, so there is nothing to report:
  the supervisor now logs `worker_process_stopped_early` and exits 0.

### Changed

- A worker enforces `TASK_TIMEOUT` on a sync task through the grace backstop
  alone while a coverage tool or a debugger is watching the thread the
  attempt runs on. Two things count: a trace function (`sys.settrace`, which
  `coverage run` installs before Python 3.14, and which `pdb` and most
  debuggers install), and a registered `sys.monitoring` tool with events
  enabled (which `coverage run` uses from Python 3.14 on). Nothing is raised
  inside the task. A task that returns within `TASK_TIMEOUT` plus
  `TASK_TIMEOUT_GRACE` is recorded as whatever it did, however long it ran,
  with no `task_timed_out` event; one still running then is recorded as
  failed and recycles the worker. An async task is cancelled at its deadline
  either way, and a profile hook (`sys.setprofile`) is not consulted.
- The worker logs `timeouts_backstop_only` once under such a tool, on the
  first attempt it registers rather than at startup. That event now carries
  `reason` (`interpreter` or `tracing_tool`) and, for a tool, `tracer`:
  `sys.settrace`, or `sys.monitoring (NAME)`.

## [0.3.0] - 2026-08-24

### Upgrading

- Run `python manage.py migrate django_ox`, and roll every process, web and
  worker, to 0.3.0 before the first discard. This release adds the
  DISCARDED status: a 0.2.1 process that reads a DISCARDED row raises
  `ValueError` from `get_result()` and `refresh()`, and 0.2.1's `ox_prune`
  cannot delete such rows. Rolling back with DISCARDED rows present keeps
  that crash until the rows are removed by hand
  (`DELETE FROM django_ox_oxtask WHERE status = 'DISCARDED'`); reversing
  the migration does not remove them.

### Added

- `TASK_TIMEOUT`, `TASK_TIMEOUTS` and `TASK_TIMEOUT_GRACE` backend options, a
  limit on how long one attempt may run. At the deadline the worker raises
  `django_ox.exceptions.TaskTimeout` inside the task, on the task's own
  thread, so `finally` blocks run and an open `transaction.atomic()` rolls
  back; an async task is cancelled inside its event loop instead. The attempt
  is recorded as failed with the `TaskTimeout` error and retried on the usual
  backoff, or marked FAILED when attempts are spent. A thread that has not
  stopped `TASK_TIMEOUT_GRACE` seconds later (default 30) is treated as
  stuck: the worker records the attempt as failed, moves the lease number so
  the thread can write nothing to the row, stops claiming, drains its other
  tasks and exits with code 75, which `--processes` restarts without counting
  it against the restart cap. `TASK_TIMEOUTS` maps a queue name to its own
  value, and a key that is not in `QUEUES` fails `manage.py check` as
  `django_ox.E005`; a bad value fails it as `django_ox.E004`. Off by default.
  `django_ox.deadline()` and `django_ox.remaining()` read the attempt's
  deadline from inside a task. `TaskTimeout` subclasses `TimeoutError`. New
  log events: `task_timed_out`, `task_stuck`, `worker_recycling`,
  `worker_process_recycled` and `timeouts_backstop_only`.
- `ox_worker --processes N`. Above 1, the command supervises N copies of
  itself, each a full worker with its own database connections, lease renewal,
  reaper and `--concurrency` thread pool, so `--processes 2 --concurrency 4`
  runs eight tasks at once. Every worker id ends in its slot number. SIGTERM,
  SIGINT or SIGHUP to the supervisor is forwarded once and the supervisor exits
  0 when every worker drained or recycled, or with the first other non-zero
  code; a second signal is the force-exit, and a worker that has not exited
  five seconds later is SIGKILLed. A worker process that dies is restarted with
  `worker_process_restarted` at WARNING, after one second and then with a
  doubling delay up to 30 seconds; a worker that recycled itself, exit code 75,
  comes back after one second under `worker_process_recycled`, outside the
  death count and the backoff. More than five deaths of one slot in a minute
  stops the supervisor with exit code 1 and `supervisor_restart_cap` at ERROR.
  A worker whose supervisor dies drains and exits (`worker_orphaned`). The
  children are started the way the supervisor was, `manage.py` by absolute path
  or `python -m django`, with `--settings` and `--pythonpath` passed on, so the
  command works from any working directory. `--processes 1`, the default, is
  the worker as before. POSIX only.
- A Prometheus endpoint. `path("ox/", include("django_ox.urls"))` mounts
  `GET /ox/metrics`, which renders the `django_ox.stats` numbers as gauges in
  the Prometheus text format (OpenMetrics on request), from the standard
  library alone. The metric names are `django_ox_tasks{queue,status}`,
  `django_ox_ready_tasks`, `django_ox_oldest_ready_age_seconds`,
  `django_ox_last_claim_age_seconds`, `django_ox_throughput_per_minute` and
  `django_ox_failure_rate`, and they are public API from this release. The
  view has no authentication of its own. `django_ox.metrics.collector()`
  returns a collector for a `prometheus_client` registry when that package is
  installed; it is not a dependency.
- `django_ox.actions.retry(result_id)` and `django_ox.actions.discard(result_id)`.
  A retry puts a FAILED or LOST task back to READY for one more attempt, keeping
  its attempt count, worker ids and every traceback, and clearing the backoff so
  it is eligible at once. A discard closes a READY, FAILED or LOST task without
  running it. Each is one compare-and-set on the row's status and lease number,
  so two retries of one row requeue it once, a discard that races a claim loses
  to it, and a LOST row's missing worker cannot write over its retry. Neither
  touches a RUNNING task. `retry_many(selection)` and `discard_many(selection)`
  take a queryset or a list of ids and make the same move in one conditional
  UPDATE per thousand rows inside one transaction, returning
  `(changed, skipped)`.
- The task table in the Django admin, registered only when
  `django.contrib.admin` is installed: a list with status and queue filters and
  search by id or path, a read-only detail page with every attempt's traceback,
  and **Retry selected tasks** and **Discard selected tasks** actions that
  report how many rows moved and how many were skipped. The actions call
  `retry_many` and `discard_many`, so a select-across of any size is a few
  statements in one transaction. The admin does not add, edit or delete rows.
- `django_ox.bulk.enqueue_many(task, calls)`, the bulk form of `enqueue()`.
  `calls` is a list of `(args, kwargs)` pairs; the rows are written with one
  INSERT per 1,000 inside one transaction and the `TaskResult` list comes back
  in input order. The task is validated and every argument serialised before
  the first write, so a rejected call inserts nothing. Each row is built by the
  same code as `enqueue()`, so workers see no difference.
- `OxTask.Status.DISCARDED`, a sixth value in django-ox's own status column. It
  reads as `FAILED` through `django.tasks` and `is_finished` is true for it.
  `queue_stats()` reports it in a `discarded` column, and `ox_prune` deletes
  discarded rows with successful ones.

### Fixed

- `manage.py check` runs the django-ox checks, `django_ox.E001` to `E005`,
  in a project that imports `django.tasks` nowhere else. Django registers its
  tasks check when that module is first imported, and a project without the
  admin or a task module on its import path reached `check` without it, so
  every django-ox check passed silently. The worker's own startup check was
  unaffected.

### Changed

- **A migration ships with this release.** Run `python manage.py migrate
  django_ox` when you upgrade. It adds the new status choice.
- `QueueStats` has a sixth field, `discarded`, keyword-defaulted like `lost`.
- A queued task can now be discarded and a failed or lost one retried, and
  every attempt can be bounded with `TASK_TIMEOUT`.
- A worker that is already draining, because it is recycling, treats the
  operator's first signal as the drain it is doing rather than as the
  force-exit; the second signal is still the force-exit.

## [0.2.1] - 2026-08-20

### Fixed

- A task that succeeds after its lease was lost drops the reaper's
  lost-lease record from `errors`. That record says the outcome was never
  observed, and the success write is that observation, so nothing reading
  `result.errors` is handed an exception nobody raised. Every earlier
  attempt's traceback stays on the row. A failure resolving the same way
  already dropped it, and the two now agree. A task that is still LOST
  keeps the record: it is the only thing on the row that says why the
  result reads as failed.

### Added

- Built distributions are checked for every migration before release.

## [0.2.0] - 2026-08-20

### Fixed

- A worker whose task had been taken back by the reaper could still write its
  own outcome over the row, so a task that had already finished could be moved
  back to READY and run a second time after its result had been reported. Every
  claim now stamps the row with a lease number, and every finish write carries
  that number in its WHERE clause, so a write from a worker that no longer
  holds the task matches nothing and is dropped instead of applied. No
  completion is signalled for a dropped write.
- A lock that ages out with no attempts left is recorded as LOST rather than
  FAILED. LOST says the worker stopped reporting and the outcome was never
  seen, and nothing more. The `TaskAbandoned` record the reaper leaves in
  `errors` is the lost lease, not a cause of failure.
- Lock timestamps are written and compared using the database server's clock
  rather than each worker's own, so two hosts with drifting clocks no longer
  produce false reclaims. This applies when `USE_TZ` is on. With `USE_TZ` off
  the worker's clock is used instead, because the database's clock does not
  always match what these columns hold: SQLite's is UTC while the columns carry
  naive local time, and reading one against the other would make `ox_prune
  --older-than` treat rows that finished seconds ago as hours old.
- On databases without `SELECT ... FOR UPDATE SKIP LOCKED`, which includes
  SQLite, a claim read its row back in a second statement and could come away
  holding a lease granted to a different worker, if the reaper reclaimed the
  row in the gap between the two. The read is now pinned to the lease the claim
  was granted, so a worker that lost the row inside that gap comes back with
  nothing rather than with someone else's lease.

### Added

- **Lease renewal.** A worker refreshes the lock on the tasks it is running,
  one statement per interval however many are in flight, and keeps doing so
  through a graceful drain. A long task on a healthy worker is no longer
  reclaimed while it is still running. `LOCK_TIMEOUT` now bounds how long a
  worker may go unresponsive, not how long a task may take. The renewal
  interval is `LOCK_TIMEOUT / 3`, overridable as `renew_interval` when
  embedding `Worker` directly.
- `OxTask.Status.LOST`, a fifth value in django-ox's own status column. It
  reads as `FAILED` through `django.tasks`, which has four statuses and no
  fifth, and `is_finished` is true for it, so callers waiting on a
  result still terminate. The row keeps the distinction: `queue_stats()`
  reports a `lost` column and `ox_prune --include-failed` covers it. If the
  worker holding a LOST task comes back and records a real outcome, that
  outcome replaces LOST; only that one execution can.
- `task_lease_lost` and `lease_renew_failed`, two WARNING log events. Both are
  documented on the Monitoring page.

### Changed

- **A migration ships with this release.** Run `python manage.py migrate
  django_ox` when you upgrade. It adds the `lease_epoch` column and the new
  status choice.
- `task_reclaimed` now reports `status` as `READY` or `LOST`, where it
  previously reported `READY` or `FAILED`.
- `QueueStats` has a fifth field, `lost`. It is keyword-defaulted, so existing
  code that constructs one keeps working.

## [0.1.2] - 2026-08-18

The worker, the public API and the database schema are unchanged. This
release updates the project description that appears on the package page,
and the documentation that ships with it.

### Changed

- README now leads with what the backend removes from a deployment: the
  queue lives in the database the application already runs, so there is no
  broker to provision, secure, upgrade or back up. The transactional
  guarantee follows it rather than opening.

### Added

- Migration guidance now covers moving *away* from django-ox as well as to
  it: which behaviour carries over to a broker-backed backend, which does
  not, and how to keep the option open.
- Worked examples for routing a queue to its own worker, choosing a lock
  timeout for long tasks, overriding a schedule's queue and priority,
  verifying that a schedule is live, and running the worker in containers.
- `context7.json`, so documentation indexers read the project description,
  the supported versions and the setup steps rather than inferring them.

## [0.1.1] - 2026-08-17

The worker, the public API and the database schema are unchanged. This
release updates the packaging metadata and the project description that
appears on the package page.

### Changed

- Packaging metadata now carries a `Documentation` URL, so the
  documentation site is linked directly from the package page.
- README now carries release and CI status badges, a link to the
  documentation site, and a scope statement: what the core covers, what is
  deliberately outside it, and which features belong to the commercial
  tier.

## [0.1.0] - 2026-08-16

Initial release.

### Added

- `OxBackend`, a database-backed backend for Django's Tasks framework
  (`django.tasks`, Django 6.0+). Tasks are stored in the application database;
  no broker required.
- Transactional enqueue: `enqueue()` is a single INSERT on the caller's
  connection, so a task enqueued inside `transaction.atomic()` commits or
  rolls back with the business data.
- `ox_worker` management command: claims tasks with
  `SELECT ... FOR UPDATE SKIP LOCKED` where supported (PostgreSQL, MySQL 8+)
  and an atomic compare-and-set UPDATE elsewhere (including SQLite).
  Configurable via `--backend`, `--queues`, `--concurrency` (thread pool),
  `--interval`, and `--lock-timeout`.
- Retries with exponential backoff (`MAX_ATTEMPTS`, `BACKOFF_INITIAL`,
  `BACKOFF_MAX`), keeping the full traceback of every attempt.
- Reaper: tasks whose worker died are returned to the queue after
  `LOCK_TIMEOUT` and count as a failed attempt.
- Graceful drain: on SIGTERM/SIGINT the worker stops claiming, finishes
  in-flight tasks, then exits; a second signal forces an immediate exit.
- Priorities (-100 to 100, higher first) and deferred tasks (`run_after`),
  with the corresponding `supports_*` flags declared on the backend.
- Result store: `get_result()`, `refresh()`, and the async variants, with
  status, return value, and per-attempt errors readable from the database.
- `ox_prune` management command: batched deletion of finished task rows
  (`--older-than`, `--include-failed`, `--batch-size`, `--dry-run`).
- `django_ox.stats`: read-only queue metrics as plain ORM queries, on
  every supported database: per-queue status counts, backlog depth and
  age, throughput and failure rate over a trailing window, and time
  since the last task claim.
- `ox_health` management command: exits non-zero with a one-line reason
  when the database is unreachable or a `--max-backlog`, `--max-age` or
  `--worker-timeout` threshold is breached; built for cron alerting and
  container probes.
- Structured logging: worker lifecycle events (claim, start, success,
  retry, failure, reclaim, dispatch, shutdown) log to the `django_ox`
  logger with stable extra keys (`event`, `task_id`, `queue`, `attempt`,
  `duration_ms`, ...) for JSON log handlers.
- Recurring tasks: cron schedules declared in the `TASKS` setting
  (`SCHEDULES` option), dispatched by the workers themselves; a unique
  constraint on (schedule, tick) makes each tick fire exactly once across
  any number of workers. Five-field cron syntax plus `@hourly`-style
  shortcuts; misconfigured schedules fail at startup and in
  `manage.py check`. On recovery after downtime, only the latest missed
  tick fires.
- System check `django_ox.E003`: a schedule name defined on more than
  one backend is rejected, at worker startup and in `manage.py check`,
  because the tick log is keyed by schedule name alone and shared names
  would let the backends suppress each other's ticks.
- Strict cron validation: expressions that can never fire and step values
  larger than a field's range (such as `*/61` in the minute field) are
  rejected at parse time rather than misfiring silently. Schedule
  dispatch holds under clock skew between workers: a tick row dated in
  the future cannot suppress ticks that are due.

### Security

- A stored `task_path` must resolve to a `django.tasks` Task (a function
  registered with `@task`). A row naming any other importable callable is
  rejected as an un-runnable task instead of being executed, so the
  worker never invokes an arbitrary dotted path pulled from the table.
  `SECURITY.md` documents the full trust model, the JSON-only
  serialization, and the guidance to keep secrets out of task arguments.
- An API stability and deprecation policy (`docs/stability.md`) covers
  the public API surface, the pre-1.0 SemVer rule, the deprecation
  window, and the supported Python and Django matrix.

[1.1.0]: https://github.com/oxpull/django-ox/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/oxpull/django-ox/compare/v0.4.0...v1.0.0
[0.4.0]: https://github.com/oxpull/django-ox/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/oxpull/django-ox/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/oxpull/django-ox/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/oxpull/django-ox/releases/tag/v0.2.1
[0.2.0]: https://github.com/oxpull/django-ox/releases/tag/v0.2.0
[0.1.2]: https://github.com/oxpull/django-ox/releases/tag/v0.1.2
[0.1.1]: https://github.com/oxpull/django-ox/releases/tag/v0.1.1
[0.1.0]: https://github.com/oxpull/django-ox/releases/tag/v0.1.0
