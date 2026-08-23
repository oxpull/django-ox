# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ox_worker --processes N`. Above 1, the command supervises N copies of
  itself, each a full worker with its own database connections, lease
  renewal, reaper and `--concurrency` thread pool, so `--processes 2
  --concurrency 4` runs eight tasks at once. Every worker id ends in its slot
  number. SIGTERM or SIGINT to the supervisor is forwarded once and the
  supervisor exits 0 when every worker drained, or with the first non-zero
  code; a second signal is the force-exit. A worker process that dies is
  restarted after a second with `worker_process_restarted` at WARNING; more
  than five restarts in a minute stops the supervisor with exit code 1 and
  `supervisor_restart_cap` at ERROR. `--processes 1`, the default, is the
  worker as before. POSIX only.
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
  statements in one transaction rather than two queries per row. The admin does not add,
  edit or delete rows.
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

### Changed

- **A migration ships with this release.** Run `python manage.py migrate
  django_ox` when you upgrade. It adds the new status choice.
- `QueueStats` has a sixth field, `discarded`, keyword-defaulted like `lost`.
- The scope statement no longer says tasks cannot be revoked after enqueue. A
  queued task can be discarded; a running one still cannot be stopped from
  outside.

## [0.2.1] - 2026-08-20

### Fixed

- A task that succeeded after its lease was lost no longer keeps the reaper's
  lost-lease record in `errors`. That record says the outcome was never
  observed, and the success write is that observation, so anything reading
  `result.errors` was handed an exception nobody raised on a task that worked.
  A failure resolving the same way already dropped it, and the two now agree. A task that is still LOST keeps the record: it is
  the only thing on the row that says why the result reads as failed.

### Added

- `tools/check_release.py --dist` opens the built wheel and sdist and checks
  that each one carries every migration in the source tree, along with the
  licence and the package modules. A packaging rule that stops shipping a
  migration leaves a distribution that imports and passes its tests, and fails
  on somebody's upgrade against a column that is not there. The release
  workflow runs it after the build.

## [0.2.0] - 2026-08-20

### Fixed

- A worker whose task had been taken back by the reaper could still write its
  own outcome over the row, so a task that had already finished could be moved
  back to READY and run a second time after its result had been reported. Every
  claim now stamps the row with a lease number, and every finish write carries
  that number in its WHERE clause, so a write from a worker that no longer
  holds the task matches nothing and is dropped instead of applied. No
  completion is signalled for a dropped write.
- The reaper no longer records a failure it did not observe. When a lock aged
  out with no attempts left it wrote FAILED and invented a `TaskAbandoned`
  exception to explain it, on no evidence beyond a clock. It now records the
  task as LOST, which says the worker stopped reporting and the outcome was
  never seen, and nothing more.
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
  reads as `FAILED` through `django.tasks`, which has four statuses and gets no
  fifth from us, and `is_finished` is true for it, so callers waiting on a
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
  both supported databases: per-queue status counts, backlog depth and
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
  dispatch is robust to clock skew between workers: a tick row dated in
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

[Unreleased]: https://github.com/oxpull/django-ox/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/oxpull/django-ox/releases/tag/v0.2.1
[0.2.0]: https://github.com/oxpull/django-ox/releases/tag/v0.2.0
[0.1.2]: https://github.com/oxpull/django-ox/releases/tag/v0.1.2
[0.1.1]: https://github.com/oxpull/django-ox/releases/tag/v0.1.1
[0.1.0]: https://github.com/oxpull/django-ox/releases/tag/v0.1.0
