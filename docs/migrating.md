# Migrating to django-ox

Your task code stays the same. django-ox implements Django's `django.tasks` API,
so `@task` functions and `.enqueue()` calls do not change.

Three things do change: the backend in your `TASKS` setting, the worker command,
and the table the queue lives in.

Find your section below, then read [Switching over](#switching-over). That last
part is where migrations go wrong.

## From another `django.tasks` backend

Configuration only. Here it is with `django-tasks-db`, the most common one:

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

Nothing else moves. Same decorator, same `.enqueue()`, same result API.

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
from django.tasks import task


@task
def send_confirmation(order_id): ...


send_confirmation.enqueue(order_id=42)
```

| Celery | django-ox |
| --- | --- |
| Broker URL (Redis, RabbitMQ) | none. The queue is a table on your default database. |
| `celery -A proj worker` | `manage.py ox_worker` |
| `celery -A proj beat` | nothing to run. Schedules go in `TASKS` and every worker dispatches them. See [Recurring tasks](recurring-tasks.md). |
| `.delay(...)`, `.apply_async(...)` | `.enqueue(...)` |
| `apply_async(countdown=..., eta=...)` | `run_after` |
| `autoretry_for`, `self.retry` | automatic. Tune with `MAX_ATTEMPTS`, `BACKOFF_INITIAL`, `BACKOFF_MAX`. |
| Result backend | the same table, read through the standard result API. |
| Flower | the [stats API, `ox_health`, the Prometheus endpoint and the admin page](monitoring.md) |

One difference in behaviour is worth reading before you switch. With a broker,
`enqueue` leaves your process immediately. If the surrounding transaction then
rolls back, a worker can pick up an order that no longer exists. The usual fix
is to wrap every call in `transaction.on_commit()`.

Here the enqueue is an `INSERT` on your own connection. It commits or rolls back
with the row it belongs to, so there is nothing to wrap.

Queues, priorities and `run_after` are what exist today. Celery's chains,
groups and chords, and routing across multiple brokers, are outside the
package; chains and workflows are on the [Oxpull Pro](pro.md) roadmap,
undated.

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
from django.tasks import task


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

Schedules in settings deploy with your code, so there are no rows to edit by
hand. A typo fails at `manage.py check` instead of at dispatch time.

| huey | django-ox |
| --- | --- |
| `manage.py run_huey` | `manage.py ox_worker` |
| `@periodic_task(crontab(...))` | a `SCHEDULES` entry, same five-field cron syntax |
| `.schedule(delay=...)` | `run_after` |
| `retries`, `retry_delay` | `MAX_ATTEMPTS` and the backoff options |
| `huey.immediate` in tests | Django's `ImmediateBackend` or `DummyBackend` |

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
5. **Retire the old worker,** then its tables and broker.

No drain window available? Run both. Old workers keep serving the old table
while new work goes to django-ox. They cannot see each other's rows.

Both systems run tasks at least once, so your tasks should already be
idempotent. Worth confirming before you start rather than halfway through.

## Migrating away

Task functions are portable. django-ox adds nothing to the producer side, so
tasks stay ordinary `django.tasks` tasks and moving to another backend is a
settings change and a drain, run in the same order as above with the roles
reversed.

One behaviour does not travel, and it is worth deciding about on the way in
rather than on the way out. Enqueueing inside `transaction.atomic()` ties the
task to that transaction, so it disappears on rollback. A broker-based backend
cannot do this: the enqueue leaves your process the moment you call it. Code
that depends on a rollback removing a task will behave differently once the
queue lives in a broker, and it will do so quietly.

If you want to keep that option open, wrap enqueues in
`transaction.on_commit()`, the way a broker-based backend requires. django-ox
runs correctly either way, and the task is enqueued after the commit instead of
inside it. You give up the guarantee and keep the portability.

If you would rather have the guarantee, take it, and write the dependency down
somewhere the next person will find it.
