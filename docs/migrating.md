# Migrating to django-ox

Your task code stays the same. django-ox implements Django's `django.tasks` API,
so `@task` functions and `.enqueue()` calls do not change.

Three things do change: the backend in your `TASKS` setting, the worker command,
and the table the queue lives in.

Find your section below, then read [Switching over](#switching-over). That last
part is where migrations go wrong.

## From another `django.tasks` backend

Configuration only. Here it is with `django-tasks-db`, the reference database backend:

```python
# before
INSTALLED_APPS = ["django_tasks_db", ...]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_db.DatabaseBackend",
        "QUEUES": ["default"],
    }
}
```

```python
# after
INSTALLED_APPS = ["django_ox", ...]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default"],
    }
}
```

Run `python manage.py migrate django_ox` to create the table.

| | Before | After |
| --- | --- | --- |
| Worker | `manage.py db_worker` | `manage.py ox_worker` |
| Clean up finished rows | `manage.py prune_db_task_results` | `manage.py ox_prune --older-than 7d` |

The decorator, `.enqueue()`, and result API stay the same. Worker options
and queue selection differ.

Of `db_worker`'s worker-specific options, `--backend`, `--interval`,
`--batch` and `--max-tasks` carry over by name.

- Replace `--queue-name` with `--queues`.
- `--batch` maps by name to `--batch`. In `ox_worker`, it ends after an
  error-free empty claim pass with no local tasks in flight. It does not
  wait for future tasks, retry backoff or deferred releases. `db_worker`
  exits with a traceback when the database is unreachable or its tables are
  missing. `ox_worker` keeps retrying. An abandoned dispatch pass prevents
  batch completion until a later dispatch pass completes. A schedule-scoped
  failure is reported as `schedule_dispatch_error` and does not hold the
  batch open. Exit 0 means the batch finished. It does not mean that every
  schedule enqueued. Give the job a timeout, as
  [Running as a job](production.md#running-as-a-job) explains.
- `--max-tasks N` maps by name to `--max-tasks N`. In `ox_worker`, every
  claimed attempt consumes one of N, including failed attempts and repeat
  claims of the same task. These mappings do not imply identical completion
  or retry semantics between the workers.
- Remove `--reload`, `--no-reload`, `--exclude-queues`, `--worker-id`, and
  `--no-startup-delay`. `ox_worker` rejects these options as unrecognized
  arguments.
- `ox_worker` does not autoreload. Without `--batch`, `db_worker` enables
  autoreload by default when `settings.DEBUG` is true.
- `db_worker` runs only the `default` queue unless configured otherwise.
  `ox_worker` runs every configured queue unless `--queues` selects queues.
  Use `--queues default` to preserve the old default.

Do not use `--queues '*'` to select all queues. `*` is treated as a literal
queue name. Omit `--queues` to run every configured queue.

## From Celery

Celery needs a broker and its own workers. django-ox uses the database you
already have, so there is no broker to run.

```python
# before
from celery import shared_task


@shared_task
def send_confirmation(order_id): ...


send_confirmation.delay(order_id=42)
```

```python
# after
from django.tasks import task  # Django 6.0+
# On Django 5.2 the Tasks framework comes from the backport:
# from django_tasks import task


@task
def send_confirmation(order_id): ...


send_confirmation.enqueue(order_id=42)
```

| Celery | django-ox |
| --- | --- |
| Broker URL (Redis, RabbitMQ) | none. The queue is a table in your own database, your default one unless you route `OxTask` elsewhere. |
| `celery -A proj worker` | `manage.py ox_worker` |
| `celery -A proj beat` | nothing to run. Schedules go in `TASKS` and every worker dispatches them. See [Recurring tasks](recurring-tasks.md). |
| `.delay(...)`, `.apply_async(...)` | `.enqueue(...)` |
| `apply_async(countdown=..., eta=...)` | `run_after` |
| `autoretry_for`, `self.retry` | automatic retries on task exceptions. Set backend defaults with `MAX_ATTEMPTS`, `BACKOFF_INITIAL` and `BACKOFF_MAX`, or declare per-task `max_attempts` and `backoff`. A callback can decline a retry for a particular exception. |
| `soft_time_limit`, `time_limit` | not a one-to-one mapping. A task's `timeout` sets its attempt deadline; `TASK_TIMEOUT_GRACE` and worker recycling remain worker-wide. See [Task timeouts](production.md#task-timeouts). |
| Result backend | the same table, read through the standard result API. |
| Flower | the [stats API, `ox_health`, the Prometheus endpoint and the admin page](monitoring.md) |
| `task_always_eager` | Use `django_ox.testing.ImmediateBackend` to run once at enqueue. It runs even if the enclosing transaction later rolls back, and rejects `run_after`. To test queued execution, keep `OxBackend` and use the public, provisional [`run_tasks()`](patterns.md#run-queued-tasks-in-tests) helper. It runs due attempts with retry and backoff policy, without enforcing timeouts. |

Per-task policy declarations require Django 6.1 or Django 5.2 with
django-tasks 0.12+. They are not accepted by Django 6.0's `@task`
decorator. See [Per-task policy](configuration.md#per-task-policy).

In django-ox, `max_attempts` counts worker claims, including the first.
A worker that dies after claiming a task consumes an attempt too. Choose
the budget from that definition. Do not copy a retry count from another
task runner.

One difference in behaviour to read before you switch. With a broker,
`enqueue` leaves your process immediately. If the surrounding transaction then
rolls back, a worker can pick up an order that no longer exists. The usual fix
is to wrap every call in `transaction.on_commit()`.

Here the enqueue is an `INSERT` on the database that holds `OxTask`, your
default one. Open the transaction there and the task commits or rolls back
with the row it belongs to, so there is nothing to wrap.

Queues, priorities and `run_after` map directly. django-ox does not
implement Celery's chain, group or chord APIs, or routing across multiple
brokers. [Batches](features/batches.md) provide grouped work, and
provisional [workflows](features/workflows.md) provide dependency DAGs.
Both require Django 6.0+ and the optional `oxpull` app. Review their
semantics. They are not drop-in Celery replacements.

## From huey

Closer to django-ox than Celery is, since huey can already store tasks in
SQLite or Postgres. What changes is the API, and where schedules live.

```python
# before
from huey.contrib.djhuey import task, periodic_task
from huey import crontab


@task()
def send_confirmation(order_id): ...


@periodic_task(crontab(minute="0", hour="3"))
def nightly_report(): ...
```

```python
# after
from django.tasks import task  # Django 6.0+
# On Django 5.2 the Tasks framework comes from the backport:
# from django_tasks import task


@task
def send_confirmation(order_id): ...


@task
def nightly_report(): ...
```

The schedule moves off the function and into settings:

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "SCHEDULES": {
                "nightly-report": {
                    "task": "reports.tasks.nightly_report",
                    "cron": "0 3 * * *",
                },
            },
        },
    }
}
```

Schedules in settings deploy with your code, and a typo fails at
`manage.py check` instead of at dispatch time. If your team edits schedules in
the admin today, see [Schedules in the database](stored-schedules.md).

| huey | django-ox |
| --- | --- |
| `manage.py run_huey` | `manage.py ox_worker` |
| `@periodic_task(crontab(...))` | a `SCHEDULES` entry, same five-field cron syntax |
| `.schedule(delay=...)` | `run_after` |
| `retries`, `retry_delay` | Backend defaults in `MAX_ATTEMPTS` and the backoff options, or per-task `max_attempts` and `backoff`. django-ox counts total claims, including the first. Per-task declarations require Django 6.1, or Django 5.2 with django-tasks 0.12+. |
| `huey.immediate` in tests | Use `django_ox.testing.ImmediateBackend` to run once at enqueue, or `DummyBackend` to record enqueues. Both accept policy declarations without enforcing retries, backoff or timeouts. For queued execution, keep `OxBackend` and use the public, provisional [`run_tasks()`](patterns.md#run-queued-tasks-in-tests) helper. It runs due attempts with retry and backoff policy. Test timeout enforcement against a real worker. |

## Earlier `oxpull` installs

The optional `oxpull` app is included in django-ox 1.9.0 and later. You no longer need a separately installed `oxpull` distribution.

The PyPI `oxpull` 1.9.0 distribution is a compatibility metapackage. It depends on django-ox 1.9.0 or later and contains no Python module. The importable package comes from django-ox.

The former private package index is retired. Remove it and its credentials from installer configuration.

### Upgrade an existing installation

The order matters.

1. Stop every affected worker and other process that enqueues or claims tasks.

2. Back up the database.

3. Uninstall the earlier `oxpull` distribution before upgrading django-ox.

    ```console
    python -m pip uninstall oxpull
    ```

4. Remove the former private index and its credentials from your pip configuration, requirements and deployment secrets.

5. Upgrade django-ox.

    ```console
    python -m pip install -U django-ox
    ```

6. Run migrations and check the installation.

    ```console
    python manage.py migrate
    python -m pip check
    ```

    For the verified oxpull 1.8.0 schema, the migration run reports "No migrations to apply." Earlier schemas may have migrations to apply.

7. Restart the affected processes with the updated installation and existing application settings.

Keep the existing `INSTALLED_APPS` and `TASKS` configuration. Import paths, task data, the `oxpull` app label, table names, the five migrations and persisted task paths are unchanged.

The optional app requires Django 6.0+. django-ox without that app continues to support Django 5.2.

### Repair an overlapping installation

If you ran `pip install -U oxpull` or `pip install -U django-ox` before removing an earlier `oxpull` distribution, inspect and repair the installation before restarting processes.

If the earlier distribution is still installed, remove it and then restore django-ox's files:

```console
python -m pip uninstall oxpull
python -m pip install --force-reinstall --no-deps django-ox
python -m pip check
```

Do both steps even if imports still worked before the uninstall. The old distribution can leave stale metadata that makes `pip check` fail, and uninstalling it can remove files now supplied by django-ox.

If you already uninstalled the earlier distribution, restore the files with:

```console
python -m pip install --force-reinstall --no-deps django-ox
python -m pip check
```

The same repair applies if uninstalling the old 0.0.1 stub removed `oxpull/__init__.py`, leaving a namespace import without `oxpull.__version__`.

These package-file failures do not alter the database data. Run migrations and complete the normal checks before restarting.

This shared-file warning concerns earlier distributions that installed an `oxpull` module. Uninstalling the module-free 1.9.0 metapackage does not remove django-ox's module files.

## Switching over

The two systems use different tables. Neither reads the other's rows. So if you
flip the setting and deploy, anything still queued in the old table has nothing
left to run it.

1. **Stop enqueueing to the old system.** Leave its workers running.
2. **Let it drain.** Watch until pending work hits zero. Check scheduled tasks
   too: a job due in six hours still counts.
3. **Deploy django-ox.** Run `migrate django_ox`, then switch `TASKS`.
4. **Start `ox_worker`** and check it picks up work. `manage.py ox_health` will
   tell you, and the worker logs every claim to the `django_ox` logger.
   Using Django's PostgreSQL pool? First check
   [pool sizing](production.md#database-connections-and-postgresql-pooling).
5. **Retire the old worker,** then its tables and broker.

No drain window available? Run both. Old workers keep serving the old table
while new work goes to django-ox. They cannot see each other's rows.

Both systems run tasks at least once, so your tasks should already be
idempotent. Confirm it before you start.

## Migrating away

Tasks without django-ox policy declarations use the standard `django.tasks`
API. Moving them to another backend is a settings change and a drain, run
in the same order as above with the roles reversed.

Tasks that declare `max_attempts`, `backoff` or `timeout` need a policy
migration too. A backend whose task class does not accept those fields
rejects the declaration at import. Remove or translate the declarations
before switching. Same-named fields on another backend are not a
compatibility guarantee. In the other direction, django-ox reads policy
only from `PolicyTask`; other Task classes inherit its backend defaults
when rebound with `.using(backend=...)`.

Transactional enqueue also needs deciding on the way in. Enqueueing inside
`transaction.atomic()` on the database that holds `OxTask` ties the task to
that transaction, so it disappears on rollback. A broker-based backend
cannot do this: the enqueue leaves your process the moment you call it. Code
that depends on a rollback removing a task will behave differently once the
queue lives in a broker. It will do so quietly.

If you want to keep that option open, wrap enqueues in
`transaction.on_commit()`, the way a broker-based backend requires. django-ox
runs correctly either way, and the task is enqueued after the commit instead of
inside it. You give up the guarantee and keep the portability.

If you would rather have the guarantee, take it, and write the dependency down
somewhere the next person will find it.
