# API stability

This page states what counts as django-ox's public API, how versions
change, and which Python and Django versions are supported. It is a
promise about compatibility, so you can pin `django-ox` with confidence.

## Public API

These are the supported surfaces. Changes to them are versioned and
announced in the [changelog](changelog.md). This page, not a module's
`__all__`, is the statement of what is public; today the two agree.

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
  Under `--processes`, the supervisor exits 0 when every worker drained
  or recycled, 1 when a slot hit the restart cap, and otherwise with the
  first other non-zero worker code.
- **The claim filter hooks** `Worker.claim_filter_q()` and
  `Worker.claim_filter_sql()`, and where their result is applied: the
  fragment is conjoined to the conditions the candidate select filters on,
  ahead of its ordering and its limit. The statement around it is not
  promised.
- **The timeout helpers** `django_ox.deadline()` and `django_ox.remaining()`,
  callable from inside a task.
- **The metrics module** `django_ox.stats`: `queue_stats`, `ready_count`,
  `oldest_ready_age`, `throughput`, `failure_rate`, `last_claim_age`, the
  `QueueStats` dataclass, and `DEFAULT_WINDOW`, the trailing window the
  rate functions default to.
- **The Prometheus surface**: `django_ox.metrics.render_prometheus`,
  `render_openmetrics` and `collector`, the view `django_ox.views.metrics`,
  the `django_ox.urls` module with its `metrics` route name, and the metric
  names and label names listed on the [Monitoring](monitoring.md#prometheus)
  page. `METRIC_NAMES` is that list in code; `CONTENT_TYPE_PROMETHEUS` and
  `CONTENT_TYPE_OPENMETRICS` are the content types the view serves. A
  scraped name is a contract with every dashboard that reads it, so a
  rename is a breaking change. Help text is not part of the contract.
- **The actions module** `django_ox.actions`: `retry` and `discard`, their
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

## Versioning

django-ox follows [Semantic Versioning](https://semver.org/). Before 1.0,
the pre-1.0 rule applies:

- **0.x minor releases may contain breaking changes.** Any
  break to a public surface above is called out in the changelog under a
  `Changed` or `Removed` heading, with the migration step.
- **0.x.y patch releases are bug fixes only** and never break a public
  surface.

Pin accordingly: `django-ox~=0.4.0` accepts patch releases only;
`django-ox>=0.4,<0.5` accepts the current minor line.

Once 1.0 ships, breaking changes to the public API will require a major
version bump, in the usual SemVer way.

## Deprecation policy

When a public surface is going to be removed or changed incompatibly, and
a compatible path exists, it is deprecated before removal rather than
dropped outright:

- The deprecation is documented in the changelog and, where it can be,
  surfaced at runtime (a `DeprecationWarning` or a `manage.py check`
  message).
- A deprecated surface keeps working for **at least one full minor release**
  (pre-1.0) or one major release (post-1.0) before it is removed.

Security fixes are exempt. A surface that cannot be kept without leaving a
vulnerability open may change in a patch release. That is documented in the
changelog, and in a security advisory where relevant.

## Supported Python and Django

Each django-ox release is tested against the matrix below in CI, on SQLite
and PostgreSQL 16 across the grid and MySQL 8 on the oldest and newest
corners; these are the supported combinations.

| | Django 6.0 | Django 6.1 |
| --- | --- | --- |
| **Python 3.12** | tested | tested |
| **Python 3.13** | tested | tested |
| **Python 3.14** | tested | tested |

The support floor tracks Django's own: when a Python or Django version
reaches end of life upstream, a later django-ox minor release may drop it,
announced in the changelog. Databases: PostgreSQL, SQLite and MySQL 8 are
tested in CI. MariaDB 10.6+ uses the same claim path, since Django's own
floor guarantees `SELECT ... FOR UPDATE SKIP LOCKED` there, but it is not
part of the tested matrix.
