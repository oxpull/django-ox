# django-ox

A database-backed worker backend for Django's Tasks framework
(`django.tasks`, Django 6.0+).

Django 6.0 ships the Tasks API but no production backend: the built-in
`ImmediateBackend` runs tasks inline and `DummyBackend` runs nothing.
django-ox stores background tasks in the database you already run and executes
them with a worker process. You get a durable queue with retries, priorities,
scheduling and a result store, and no broker to provision, secure, upgrade or
back up.

## One fewer service to run

A broker-based task queue adds a second datastore to your deployment. Redis or
RabbitMQ has to be provisioned, monitored, secured and upgraded, and it has to
be running before a single task executes. For an application that already
depends on a database, that is a full operational surface added for one
feature.

django-ox uses the database you already run. A deployment is your application,
a worker process, and one migration. Backups already cover the queue, because
the queue is a table, and there is no second datastore that can fail on its
own.

## Transactional enqueue

The queue lives in your database, so enqueueing a task is a single INSERT
on your default connection. That gives you a guarantee no broker-based
queue can offer: **the task and your data commit or roll back together.**

```python
from django.db import transaction

with transaction.atomic():
    order = Order.objects.create(...)
    send_confirmation.enqueue(order_id=order.pk)
    # If anything below raises, the order AND the task vanish together.
    charge(order)
```

With a broker, the enqueue leaves your process the moment you call it. If
the transaction then rolls back, a worker races to process an order that
does not exist. The standard workaround is wrapping every enqueue in
`transaction.on_commit()`, and remembering to, everywhere, forever. With
django-ox there is nothing to remember: a task enqueued inside
`transaction.atomic()` becomes visible to workers only when the
transaction commits, and disappears on rollback. There is no window where
business data exists without its task, or a task without its data.

Execution is at-least-once. Workers claim tasks atomically (`SKIP LOCKED`
on databases that support it, with a single-statement fast path on
PostgreSQL; an atomic compare-and-set elsewhere, including SQLite),
failed tasks retry with
exponential backoff, and a reaper returns tasks whose worker died to the
queue. Details in [Production](production.md).

## What happens when a worker dies

A worker that claims a task takes a lease on it, and the attempt is counted at
that moment. While the task runs, the worker renews the lease every
`LOCK_TIMEOUT / 3` seconds, so a slow task on a live worker is never reclaimed.
If the worker is killed, the lease goes stale. After `LOCK_TIMEOUT` (default
300 seconds) the reaper in any surviving worker takes the task back: to READY
if attempts remain, or to LOST if they are spent. LOST reads as `FAILED`
through the result API, so nothing waits forever on a worker that is not
coming back. The mechanics, and the one case worth knowing about, are in
[Production](production.md#the-lease).

## What you get

- Transactional enqueue, as above. No `on_commit` boilerplate.
- Retries with exponential backoff and the full traceback of every attempt.
- A reaper that reclaims tasks from dead workers, and a lease that keeps it
  away from live slow ones.
- Graceful drain on SIGTERM: in-flight tasks finish before the worker exits.
- Priorities (-100 to 100) and deferred tasks (`run_after`).
- [Recurring tasks](recurring-tasks.md): cron schedules declared in
  settings, no separate scheduler process.
- A result store: status, return value and errors readable through the
  standard `django.tasks` result API.
- A [prune command](configuration.md#ox_prune) to keep the table small.
- [Monitoring](monitoring.md): a queue-stats API, an `ox_health` command
  for probes and cron alerting, and structured log events.

The worker is covered by 221 tests, green on both SQLite and PostgreSQL 16.

## Install

Requires Python 3.12+ and Django 6.0+.

```
pip install django-ox
```

Add the app and point the Tasks framework at the backend:

```python
INSTALLED_APPS = [
    # ...
    "django_ox",
]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}
```

Create the tables:

```
python manage.py migrate django_ox
```

## Quickstart

Tasks are plain `django.tasks` tasks. django-ox adds nothing to learn on
the producer side.

```python
# myapp/tasks.py
from django.tasks import task


@task
def send_welcome_email(user_id): ...
```

Enqueue one:

```python
from myapp.tasks import send_welcome_email

result = send_welcome_email.enqueue(user_id=42)
```

Run a worker in a second terminal:

```
python manage.py ox_worker
```

Check on the result later:

```python
result.refresh()
result.status  # READY, RUNNING, FAILED, or SUCCESSFUL
result.return_value  # once SUCCESSFUL
result.errors  # per-attempt tracebacks, if any
```

That is the whole integration. Next steps:

- [Configuration](configuration.md) for every setting, option and command
  flag.
- [Recurring tasks](recurring-tasks.md) for cron schedules.
- [Production](production.md) for systemd units, scaling and shutdown
  semantics.

## Scope

The core is deliberately small: a durable queue, a worker, recurring
schedules, and monitoring, with nothing extra to operate. Design
decisions worth knowing before you commit:

- A queued task can be discarded before a worker claims it, and a failed
  one retried, from the admin or with `django_ox.actions`. `TASK_TIMEOUT`
  bounds how long any attempt may run; a particular running task cannot be
  interrupted on demand.
- Tasks are stored on the default database for the model; multi-database
  routing is not part of the current scope.
- Worker concurrency is a thread pool, which fits I/O-bound tasks. For
  CPU-bound work, run `--processes N --concurrency 1`, which is N worker
  processes under one supervisor. See
  [Production](production.md#threads-and-processes).

Batches and unique tasks ship in [Oxpull Pro](pro.md). Metrics stay in this
package: `django_ox.stats` and `ox_health` are free and stay free. Rate limiting
is in neither tier.

## License

BSD 3-Clause.
