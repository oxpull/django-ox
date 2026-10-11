# Optional features

django-ox 1.9.0 includes the optional `oxpull` app. It adds unique tasks, batches, rate limiting and workflows without a separate package installation.

The app requires Django 6.0+. django-ox itself continues to support Django 5.2 through the `django-tasks` backport.

## Setup

1. Add `"oxpull"` to `INSTALLED_APPS` and run migrations.
2. Set the relevant task alias's `BACKEND` to `"oxpull.backend.OxpullBackend"`.
3. Keep the `django_ox` and `oxpull` tables on the same database. A router that splits them is refused at backend construction with `ImproperlyConfigured`. The check consults routers by model, without hints. Hint-dependent routing that sends task rows elsewhere is unsupported and may escape the startup check. See the [database-routing constraints](unique-tasks.md#database-routing-constraints).
4. Configure the features you use:
    - Unique tasks use `@unique` or `OPTIONS["UNIQUE_TASKS"]`.
    - Batches need a reconciler schedule.
    - Rate limits need `OPTIONS["RATE_LIMITS"]` and `OPTIONS["WORKER_CLASS"] = "oxpull.worker.OxpullWorker"`.
    - Workflows need `OPTIONS["WORKFLOWS"] = True`, `OxpullWorker` and a reconciler schedule.

Apply the relevant backend and worker settings in every process that enqueues or claims those tasks. Follow the feature page for the complete configuration, including the reconciler's queue and schedule.

Already have a separately installed `oxpull` distribution? Follow [Earlier `oxpull` installs](../migrating.md#earlier-oxpull-installs) before upgrading.

## Unique tasks

[Unique tasks](unique-tasks.md) deduplicate pending tasks at enqueue time. While a task with the same key is READY or RUNNING, another enqueue returns the pending result.

This is not a guarantee that two executions of the same key cannot overlap. A LOST task releases the key even if its original execution is still running.

## Batches

[Batches](batches.md) collect tasks and arrange a callback after the sealed batch settles. Create a batch, add its members, then seal it. A reconciler schedule is required.

`Batch.add()` raises `oxpull.exceptions.BatchMemberExists` if the task is already a member of that batch.

## Rate limiting

[Rate limiting](rate-limiting.md) controls admissions through `OxpullWorker`. Configure limits on task paths, queues or both.

A limit name identifies one shared window across the backends that configure it. A backend setting alone does not make a plain django-ox worker enforce that limit.

## Workflows

[Workflows](workflows.md) coordinate tasks with dependencies. A workflow is created at runtime. Enable `OPTIONS["WORKFLOWS"]`, use `OxpullWorker`, and configure the reconciler schedule before creating or sealing workflows.

Workflow APIs, settings and log events are provisional.

## Database and task rows

The app creates its own `oxpull_*` tables. It does not change any django-ox table, column, index, constraint or migration.

Workflows do write task rows. They insert blocked nodes as WAITING tasks. Cancellation moves a READY or WAITING task to DISCARDED by a compare-and-set on the task row's status and lease epoch. Release moves a WAITING task to READY and updates `run_after`; `run_after` is never NULL. DISCARDED nodes are not revived.

Use `ox_prune` for task rows and `oxpull_prune` for the optional app's records.

## Signals

Tasks enqueued through `OxpullBackend` use that backend as the signal sender. Receivers restricted to the base backend's sender must account for this when an alias changes backend. This applies to every task sent through `OxpullBackend`, not only unique tasks.

## Stability

The backend, its unique-task and rate-limit options, the worker, the unique-task decorator and the documented batches API are stable. The `oxpull_*` schema, check ids, `oxpull_prune` and non-workflow log events are also stable.

Everything in the documented workflow surface is provisional, including the `WORKFLOWS` option and every `workflow_*` event. Workflow internals are not public.

See the [stability policy](../stability.md) for the complete lists and versioning rules.
