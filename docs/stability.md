# API stability

This page states what counts as django-ox's public API, how versions
change, and which Python and Django versions are supported. It is a
promise about compatibility, so you can pin `django-ox` with confidence.

## Public API

These are the supported surfaces. Changes to them are versioned and
announced in the [changelog](changelog.md).

- **The backend path** `django_ox.backend.OxBackend`, referenced as a
  string in the `TASKS` setting, and every `OPTIONS` key it reads:
  `MAX_ATTEMPTS`, `LOCK_TIMEOUT`, `BACKOFF_INITIAL`, `BACKOFF_MAX`,
  `TASK_TIMEOUT`, `TASK_TIMEOUTS`, `QUEUES`, and `SCHEDULES` (with its
  documented per-schedule keys).
- **The management commands** and their flags: `ox_worker`, `ox_prune`,
  `ox_health`.
- **The metrics module** `django_ox.stats`: the functions listed in its
  `__all__` (`queue_stats`, `ready_count`, `oldest_ready_age`,
  `throughput`, `failure_rate`, `last_claim_age`) and the `QueueStats`
  dataclass.
- **The actions module** `django_ox.actions`: `retry` and `discard`, their
  accepted states, and their return values. The admin page that calls
  them is a convenience over this module; its layout is not a contract,
  the two action names are.
- **The bulk module** `django_ox.bulk`: `enqueue_many(task, calls)`, its
  `(args, kwargs)` call shape, the input-order return and the
  all-or-nothing write. `INSERT_CHUNK_SIZE` is exported for reading; its
  value may change.
- **The exceptions** `django_ox.exceptions.TaskAbandoned`, recorded against
  tasks whose worker stopped reporting with no attempts left (it records the
  lost lease, not a cause of failure), and `django_ox.exceptions.TaskTimeout`,
  recorded against an attempt that ran past its `TASK_TIMEOUT`.
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
(`django_ox.results`), the schedule loader (`django_ox.schedules`), and any name
starting with an underscore. The exact SQL a claim emits and the
model's non-schema helper methods are not part of the contract.

## Versioning

django-ox follows [Semantic Versioning](https://semver.org/). Before 1.0,
the pre-1.0 rule applies:

- **0.x minor releases (0.1 to 0.2) may contain breaking changes.** Any
  break to a public surface above is called out in the changelog under a
  `Changed` or `Removed` heading, with the migration step.
- **0.x.y patch releases are bug fixes only** and never break a public
  surface.

Pin accordingly: `django-ox~=0.1.0` accepts patch releases only;
`django-ox>=0.1,<0.2` accepts the current minor line.

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

Each django-ox release is tested against the matrix below in CI (SQLite
and PostgreSQL); these are the supported combinations.

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
