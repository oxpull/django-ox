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
  error-free empty claim pass with no local tasks in flight; it does not
  wait for future tasks, retry backoff or deferred releases. `db_worker`
  exits with a traceback when the database is unreachable or its tables are
  missing. `ox_worker` keeps retrying. An abandoned dispatch pass prevents
  batch completion until a later dispatch pass completes. A schedule-scoped
  failure is reported as `schedule_dispatch_error` and does not hold the
  batch open. Exit 0 means the batch finished, not that every schedule
  enqueued. Give the job a timeout, as
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
queue name, not a wildcard. Omit `--queues` to run every configured queue.

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

Per-task policy declarations require Django 6.1 or Django 5.2 with
django-tasks 0.12+. They are not accepted by Django 6.0's `@task`
decorator. See [Per-task policy](configuration.md#per-task-policy).

In django-ox, `max_attempts` counts worker claims, including the first.
A worker that dies after claiming a task consumes an attempt too. Choose
the budget from that definition rather than copying a retry count from
another task runner.

One difference in behaviour to read before you switch. With a broker,
`enqueue` leaves your process immediately. If the surrounding transaction then
rolls back, a worker can pick up an order that no longer exists. The usual fix
is to wrap every call in `transaction.on_commit()`.

Here the enqueue is an `INSERT` on the database that holds `OxTask`, your
default one. Open the transaction there and the task commits or rolls back
with the row it belongs to, so there is nothing to wrap.

Queues, priorities and `run_after` map directly. Celery's chains,
groups and chords, and routing across multiple brokers, are outside the
package; workflows are in [Oxpull Pro](pro.md), a paid add-on, and chains
are on its roadmap, undated.

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
| `huey.immediate` in tests | `django_ox.testing.ImmediateBackend` or `django_ox.testing.DummyBackend`. They accept policy declarations but do not enforce retries, backoff or timeouts. Use a real worker to test enforcement. |

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
idempotent. Confirm it before you start rather than halfway through.

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

Transactional enqueue also needs deciding on the way in rather than on
the way out. Enqueueing inside `transaction.atomic()` on the database that
holds `OxTask` ties the task to that transaction, so it disappears on
rollback. A broker-based backend cannot do this: the enqueue leaves your
process the moment you call it. Code that depends on a rollback removing
a task will behave differently once the queue lives in a broker, and it
will do so quietly.

If you want to keep that option open, wrap enqueues in
`transaction.on_commit()`, the way a broker-based backend requires. django-ox
runs correctly either way, and the task is enqueued after the commit instead of
inside it. You give up the guarantee and keep the portability.

If you would rather have the guarantee, take it, and write the dependency down
somewhere the next person will find it.
