# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-13

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

### Fixed

Findings from a pre-release adversarial review:

- A tick row dated in the future (for example written by a worker with a
  fast clock) no longer suppresses schedule dispatch fleet-wide; ticks
  that are due now fire regardless, and the unique constraint still
  protects the future instant itself.
- Cron step values larger than the field's range (such as `*/61` in the
  minute field, which silently collapsed to minute 0) are rejected at
  parse time.

### Security

- A stored `task_path` is now required to resolve to a `django.tasks` Task
  (a function registered with `@task`). A row naming any other importable
  callable is rejected as an un-runnable task instead of being executed, so
  the worker never invokes an arbitrary dotted path pulled from the table.
  `SECURITY.md` documents the full trust model, the JSON-only
  serialization, and the guidance to keep secrets out of task arguments.
- Added an API stability and deprecation policy (`docs/stability.md`):
  the public API surface, the pre-1.0 SemVer rule, the deprecation window,
  and the supported Python and Django matrix.

[0.1.0]: https://github.com/oxpull/django-ox/releases/tag/v0.1.0
