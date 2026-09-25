# API stability

This page states what counts as django-ox's public API, how versions
change, and which Python and Django versions are supported. It is a
promise about compatibility, so you can pin `django-ox` with confidence.

## Public API

These are the supported surfaces. Changes to them are versioned and
announced in the [changelog](changelog.md). This page, not a module's
`__all__`, is the statement of what is public. A module's `__all__` may
export names this page does not list; those names are not public.

- **The backend path** `django_ox.backend.OxBackend`, referenced as a
  string in the `TASKS` setting, `QUEUES` beside `OPTIONS`, and every
  `OPTIONS` key it reads: `MAX_ATTEMPTS`, `LOCK_TIMEOUT`,
  `BACKOFF_INITIAL`, `BACKOFF_MAX`, `TASK_TIMEOUT`, `TASK_TIMEOUTS`,
  `TASK_TIMEOUT_GRACE`, `SCHEDULES` (with its documented per-schedule
  keys), and `WORKER_CLASS`, the dotted path of the `Worker` subclass
  `ox_worker` runs.
- **The management commands** and their flags: `ox_worker`, `ox_prune`,
  `ox_health`. `ox_worker`'s exit codes: 0 after a drain, 130 on a forced
  exit, 75 when the worker recycles itself after a stuck task thread.
  A worker that finishes under `--batch` or `--max-tasks` drains and exits
  0, even when task attempts or individual schedule dispatches failed.
  Task outcomes stay on their task rows; schedule-scoped failures are
  reported in logs and leave no tick or task row. Exit 0 means the batch
  finished, not that every schedule enqueued. An abandoned dispatch pass
  prevents normal batch-empty completion until a later pass completes;
  reaching `--max-tasks` still ends the run. An invalid `--max-tasks`, or
  either flag with `--processes` above 1, exits 1 before any worker starts.
  Under `--processes`, the supervisor exits 0 when every worker drained
  or recycled, 1 when a slot hit the restart cap, and otherwise with the
  first other non-zero worker code. A worker killed by a signal reports
  `128 + the signal number`, following the shell convention.
- **The heartbeat-file protocol**: one process writes `PATH`; above one
  process, the supervisor writes `PATH.supervisor` and slot i writes
  `PATH.i`. The modification time is the signal; file contents are not
  read or written. Every expected file must be regular and have an age
  between zero and the configured maximum, inclusive. A passing check
  means the expected controlling loops have advanced recently, not that
  tasks are progressing. The documented
  [file-mode JSON fields](monitoring.md#file-mode-json) are public too.
- **The system check IDs**, including the `django_ox.E0xx` and
  `django_ox.W0xx` identifiers, which you may list in
  `SILENCED_SYSTEM_CHECKS`. The IDs are stable; the messages are not.
- **`ox_health`'s exit codes**: 0 when every enabled check passes, 1 when
  a check fails. In file mode, passing means every expected heartbeat file
  is fresh. A value the command itself rejects exits 1 as well, such as
  `--max-age 0`, `--max-heartbeat-age 0`, `--processes 0`, a refused flag
  combination, or a `--database` alias that isn't in `DATABASES`. A value
  argparse rejects exits 2, such as `--max-age nonsense`,
  `--max-heartbeat-age nan`, `--processes 1.5` or `--format bogus`, and so
  does an unknown flag.
- **The claim filter hooks** `Worker.claim_filter_q()` and
  `Worker.claim_filter_sql()`, and where their result is applied: the
  fragment is conjoined to the conditions the candidate select filters on,
  ahead of its ordering and its limit. The statement around it is not
  promised.
- **The timeout helpers** `django_ox.deadline()` and `django_ox.remaining()`,
  callable from inside a task. `deadline()` answers with a wall-clock time, and
  `remaining()` with seconds measured on the same monotonic clock the worker
  enforces the deadline with, so the two can differ by the size of a clock
  correction. `remaining()` is the one the watchdog agrees with.
- **The metrics module** `django_ox.stats`: `queue_stats`, `ready_count`,
  `oldest_ready_age`, `throughput`, `failure_rate`, `last_claim_age`,
  `waiting_counts`, the `QueueStats` dataclass, and `DEFAULT_WINDOW`, the
  trailing window the rate functions default to.
- **The Prometheus surface**: `django_ox.metrics.render_prometheus`,
  `render_openmetrics` and `collector`, the view `django_ox.views.metrics`
  with the `using` argument it takes from the URLconf,
  the `django_ox.urls` module with its `metrics` route name, and the metric
  names and label names listed on the [Monitoring](monitoring.md#prometheus)
  page. `METRIC_NAMES` is that list in code; `CONTENT_TYPE_PROMETHEUS` and
  `CONTENT_TYPE_OPENMETRICS` are the content types the view serves. A
  scraped name is a contract with every dashboard that reads it, so a
  rename is a breaking change. Help text is not part of the contract.
- **The actions module** `django_ox.actions`: `retry`, `discard` and
  `expire_lease`, their
  accepted states, and their return values; `retry_many` and
  `discard_many`, the selections they accept and their `(changed, skipped)`
  return. `RETRYABLE_STATUSES` and `DISCARDABLE_STATUSES` are those
  accepted states in code; `UPDATE_CHUNK_SIZE` is exported for reading and
  its value may change. The admin page that calls
  them is a convenience over this module; its layout is not a contract,
  the two action names are.
- **The bulk module** `django_ox.bulk`: `enqueue_many(task, calls)`, its
  `(args, kwargs)` call shape, the input-order return and the
  all-or-nothing write. `INSERT_CHUNK_SIZE` is exported for reading; its
  value may change.
- **The exceptions** `django_ox.exceptions.TaskAbandoned`, recorded against
  tasks whose worker stopped reporting with no attempts left (it records the
  lost lease, not a cause of failure), and `django_ox.exceptions.TaskTimeout`,
  a `TimeoutError` raised inside a task that ran past its `TASK_TIMEOUT` and
  recorded against the attempt.
- **The structured-log contract**: the event names and stable `extra` keys
  documented on the [Monitoring](monitoring.md) page.
- **The database schema** of `OxTask` and `OxScheduleTick`, evolved only
  through shipped migrations.
- **`django_ox.__version__`.**

The producer-side API is `django.tasks` itself (`@task`, `.enqueue()`,
`get_result()`); django-ox adds nothing there and follows Django's
contract.

### Not public

Everything else is an implementation detail and may change in any release
without notice. That covers the `django_ox.worker.Worker` internals, the cron
parser (`django_ox.cron`), the row-to-dataclass conversion
(`django_ox.results`), the schedule loader (`django_ox.schedules`), the
supervisor behind `--processes` (`django_ox.supervisor`) and the hidden
`--worker-index` flag it starts each child with, and any name starting
with an underscore. The exact SQL a claim emits and the
model's non-schema helper methods are not part of the contract.

`django_ox.heartbeat` is also an implementation detail, not a public Python API. The documented heartbeat filenames, modification-time meaning, freshness rule and `ox_health` JSON fields are public contracts.

## Versioning

django-ox follows [Semantic Versioning](https://semver.org/):

- **Breaking changes to any public surface above require a major version.**
  They are called out in the changelog under a `Changed` or `Removed`
  heading, with the migration step.
- **Minor releases add, they do not break.** Patch releases fix bugs or update
  documentation and package metadata; they add no features and change no
  behaviour beyond bug fixes.

A minor release may add a status value. A process still on the previous
minor release can't read a task in the new status. `get_result()` and
`refresh()` raise `ValueError` on it. The admin shows its status as `-` and
has no filter for it. Bulk discards skip it, and `queue_stats()`, the
`django_ox_tasks` gauge and `ox_health` don't count it. Upgrade every
process that shares a database before anything writes the new status. The
release notes name the value, say how it reads through `django.tasks`, and
give the upgrade and rollback steps.

Pin accordingly: `django-ox~=1.4.0` accepts patch releases only;
`django-ox~=1.3` accepts the current major line.

## Deprecation policy

When a public surface is going to be removed or changed incompatibly, and
a compatible path exists, it is deprecated before removal rather than
dropped outright:

- The deprecation is documented in the changelog and, where it can be,
  surfaced at runtime (a `DeprecationWarning` or a `manage.py check`
  message).
- A deprecated surface is announced in a minor release and removed no earlier
  than the next major release.

Security fixes are exempt. A surface that cannot be kept without leaving a
vulnerability open may change in a patch release. That is documented in the
changelog, and in a security advisory where relevant.

## Supported Python and Django

Each django-ox release is tested against the matrix below in CI, on SQLite
and PostgreSQL 16 across the grid and MySQL 8 on the oldest and newest
corners; these are the supported combinations.

| | Django 5.2 LTS | Django 6.0 | Django 6.1 |
| --- | --- | --- | --- |
| **Python 3.12** | tested | tested | tested |
| **Python 3.13** | tested | tested | tested |
| **Python 3.14** | not supported by Django 5.2 | tested | tested |

Django 6.0 and later ship the Tasks framework in core. The Django 5.2 legs
install the `django-tasks` backport and run the whole suite against it, which
is what `django-ox[backport]` pulls in.

Django 6.1 changed which databases the system checks run against. A command
that runs the full checks and does not name a database now checks every alias
in `DATABASES`. Checking a SQLite or MySQL alias opens a connection and runs
one query. The cost grows with the number of aliases, and every such command
pays it. An alias that cannot be reached ends the command.

`ox_prune`, `ox_health` in database mode, and `ox_import_beat_schedules`
name the alias they work on and pass it to the checks. `ox_worker` passes
an empty list, so no alias is checked. Its poll loop can keep retrying a
database that refuses connections. Startup work before the loop, such as
loading database-backed schedules, can still access the database.

The system checks that need a database don't run for `ox_worker`.
A SQLite build without JSON support fails `fields.E180`.
`manage.py check --database <alias>` reports it, as do `ox_prune`,
`ox_health` in database mode, and `ox_import_beat_schedules`; each exits
non-zero. A worker on that alias runs tasks anyway, because SQLite stores
those columns as text, and it logs nothing. On MySQL, Django's column-type
checks emit warnings, so `check` still exits 0.

A database with no django-ox tables is a separate case.
`check --database <alias>` does not report that: it exits 0 and reports no
issues. `migrate --check --database <alias>` is what exits non-zero, and it
prints nothing at all. A worker that reaches the poll loop logs
`worker_poll_failed` on every pass. Run `migrate --check` for the alias
before you start a worker on it. A configuration error still stops a worker
at startup, because those checks don't need a database.

`ox_health --heartbeat-file` selects a separate branch before command
checks run. It runs neither system nor migration checks, validates no
database alias, constructs no task backend and makes no database calls.
It does not need `--skip-checks`. Project startup still runs before the
command: `AppConfig.ready()` and other startup code must be database-free
if the probe needs to survive a database outage.

For commands that run system checks, pass `--skip-checks`, or pass
`--database` to `manage.py check`, as appropriate. Django 6.0 is unaffected
by the check-scope change, and so is an alias on PostgreSQL. The
[changelog](changelog.md) has the mechanism, what a router does and does
not fix, and why `--database` alone is not enough on a command that does
not pass it on.

The support floor tracks Django's own: when a Python or Django version
reaches end of life upstream, a later django-ox minor release may drop it,
announced in the changelog. Databases: PostgreSQL, SQLite and MySQL 8 are
tested in CI. MariaDB 10.6+ uses the same claim path, since Django's own
floor guarantees `SELECT ... FOR UPDATE SKIP LOCKED` there, but it is not
part of the tested matrix.
