# Why django-ox

## Recover tasks when workers stop

django-ox returns a dead worker's tasks to your database queue. Run background work without adding a separate broker.

### Retry failed work

Exceptions trigger retries with exponential backoff. Inspect each attempt's traceback in Django admin.

In every kill-test trial, django-ox finished 2,000 of 2,000 tasks. django-tasks-db left 13 to 19 tasks RUNNING.

<small>Results with LOCK_TIMEOUT set to 15 s. Across 10,000 tasks, django-ox recorded 7 repeated effects. Execution is at-least-once.</small>

### Commit tasks with application rows

Create an order and enqueue its confirmation inside `transaction.atomic()`. Both rows commit together or roll back.

`enqueue()` is one INSERT in the caller's transaction. Task and application rows must use the same database.

### Manage the queue in Django admin

Inspect tracebacks and use Retry or Discard on failed tasks. Edit recurring schedules in admin. Workers dispatch them.

`ox_health` provides queue-threshold checks and local heartbeat-file checks, with exit codes and JSON output. A Prometheus endpoint exports metrics.

### django-ox vs django-tasks-db

| Capability | django-ox | django-tasks-db |
|---|---|---|
| Retry after an exception | Automatic backoff | Task marked FAILED |
| Dead worker's task | Automatically requeued | Remains RUNNING |
| Recurring schedules | Cron and interval | Not built in |
| Health command | `ox_health` | Not built in |
| Bulk enqueue | `enqueue_many()` | No bulk API |

### django-ox vs Celery

| Operation | django-ox | Celery |
|---|---|---|
| Queue storage | Application database | RabbitMQ, Redis or SQS |
| Task and model commit | Shared transaction | Separate broker publish |
| Schedule dispatch | Inside workers | Separate Beat process |
| Task inspection | Django admin | Flower |

### django-ox vs huey

Both provide a `django.tasks` backend. django-ox automatically requeues a dead worker's tasks. huey's guide says mid-execution tasks are lost and aren't retried automatically.

### Do retries and recovery require Pro?

No. The free package includes both, plus schedules and Django admin integration. Health checks and metrics are also included.

### How do I make recovery safe?

Write idempotent tasks. A repeated execution must not duplicate effects such as charges.

### Which versions can I use?

Python 3.12+ and Django 5.2 LTS, 6.0 or 6.1. Django 5.2 uses the `django-tasks` backport. PostgreSQL, MySQL 8 and SQLite run in CI.

### What does Pro add?

Batches, unique tasks, shared rate limits and workflows, with maintainer email support.

Oxpull Pro costs $399 per year, per company, excluding VAT where it applies. Every environment is included. Pro requires Django 6.0+.

[**Get started**](background-tasks.md)

### Sources and test setup

Oxpull's 2026-09-19 benchmark compares django-ox 1.3.0 with django-tasks-db 0.13.0. Tests used PostgreSQL 16 in Docker, an Apple M1 Max and no-op tasks.

Feature comparisons use django-tasks-db 0.13.0 source and documentation. Celery and huey comparisons use their project documentation.

[**Read the benchmarks and raw results**](benchmarks.md)
