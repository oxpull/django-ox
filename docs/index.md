# Django tasks in your database

Stop running Redis to send an email. django-ox runs Django tasks in your existing database. A worker process executes them; if it dies, unfinished tasks return to the queue.

Enqueue inside `transaction.atomic()` on the same database as your models, and the task commits or rolls back with your data. Failures retry with exponential backoff. Inspect attempts and tracebacks in Django admin, or edit a recurring schedule without deploying a scheduler.

Use `django.tasks` on Django 6.0 or 6.1, or the `django-tasks` backport on Django 5.2 LTS. Python 3.12+ is required. Execution is at-least-once: write tasks that tolerate repeated execution.

The step-by-step guide takes you from `pip install django-ox` to a working queue, a welcome email, and a retry you can watch.

## Start here

- [Step-by-step guide](background-tasks.md): install django-ox and run your first tasks.
- [Why django-ox](why-django-ox.md): choose a backend for your application.
- [Benchmarks](benchmarks.md): see the worker-kill tests and queue measurements.
- [Use cases](use-cases.md): find the setup for the job you need done.

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

The queue lives in your database, so enqueueing a task is a single INSERT on
the database that holds `OxTask`, your default one unless you route it
elsewhere. Enqueue inside a transaction on that database and you get a
guarantee a broker cannot offer: **the task and every other row you write
there commit or roll back together.**

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
`transaction.atomic()` on that database becomes visible to workers only
when the transaction commits, and disappears on rollback. There is no
window where business data written there exists without its task, or a
task without its data.

Execution is at-least-once. Workers claim tasks atomically (`SKIP LOCKED`
on databases that support it, with a single-statement fast path on
PostgreSQL; an atomic compare-and-set elsewhere, including SQLite),
failed tasks retry with
exponential backoff, and a reaper returns tasks whose worker died to the
queue. Details in [Production](production.md).

## What happens when a worker dies

A worker that claims a task takes a lease on it, and the attempt is counted at
that moment. While the task runs, renewal is scheduled start-to-start every
`max(LOCK_TIMEOUT / 3, 0.1)` seconds. A slow task is not reclaimed while renewal
reaches the database on time. A live worker whose renewals are delayed or
starved for `LOCK_TIMEOUT` can have its task reclaimed; see
[Tuning LOCK_TIMEOUT](production.md#tuning-lock_timeout) and
[PostgreSQL pooling](production.md#database-connections-and-postgresql-pooling).
If the worker is killed, the lease goes stale. After `LOCK_TIMEOUT` (default
300 seconds) the reaper in any surviving worker takes the task back: to READY
if attempts remain, or to LOST if they are spent. LOST reads as `FAILED`
through the result API, so nothing waits forever on a worker that is not
coming back. The mechanics, and the one case to know about, are in
[Production](production.md#the-lease).

## What you get

- Transactional enqueue, as above. No `on_commit` boilerplate.
- Retries with exponential backoff and the full traceback of every attempt.
  [Per-task policy](configuration.md#per-task-policy) adds an attempt budget,
  backoff callback and attempt timeout on Django 6.1 or Django 5.2 with
  django-tasks 0.12+.
- A reaper that reclaims tasks after their leases expire, and a lease that
  protects slow tasks while renewal reaches the database on time.
- Graceful drain on SIGTERM: in-flight tasks finish before the worker exits.
- Priorities (-100 to 100) and deferred tasks (`run_after`).
- [Recurring tasks](recurring-tasks.md): cron or fixed-interval schedules
  declared in settings, or [rows edited in the Django admin](stored-schedules.md);
  every worker dispatches, so there is no separate scheduler process.
- A result store: status, return value and errors readable through the
  standard `django.tasks` result API.
- A [prune command](configuration.md#ox_prune) to keep the table small.
- [Monitoring](monitoring.md): a queue-stats API, an `ox_health` command
  for fleet alerting and optional local heartbeat probes, a Prometheus
  endpoint, structured log events, and an admin page with retry and discard.

## Install

Requires Python 3.12+ and Django 5.2+. Django 6.0 and later ship the Tasks
framework in core. On Django 5.2 LTS it comes from the `django-tasks`
backport, so install the `backport` extra there. **Your own imports differ
with it**: on Django 6.0+ you write `from django.tasks import task`, and on
Django 5.2 you write `from django_tasks import task`. django-ox itself
handles both.

```
pip install django-ox

# on Django 5.2 LTS
pip install "django-ox[backport]"
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

Tasks use the standard `django.tasks` decorator and enqueue API. A bare
`@task` inherits django-ox's backend defaults. Optional
[per-task policy](configuration.md#per-task-policy) sets a claim budget,
backoff callback and timeout on Django 6.1, or Django 5.2 with
django-tasks 0.12+.

```python
# myapp/tasks.py
from django.tasks import task  # Django 6.0+
# On Django 5.2 the Tasks framework comes from the backport:
# from django_tasks import task


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
- [Recurring tasks](recurring-tasks.md) for cron and interval schedules, and
  [Schedules in the database](stored-schedules.md) for rows edited in the
  admin.
- [Production](production.md) for systemd units, scaling and shutdown
  semantics.

## Scope

The core is finite on purpose: a durable queue, a worker, recurring
schedules, and monitoring, with nothing extra to operate. Design
decisions to know before you commit:

- A queued task can be discarded before a worker claims it, and a failed
  one retried, from the admin or with `django_ox.actions`. Attempt
  deadlines come from a task's `timeout`, a queue's `TASK_TIMEOUTS`
  entry, or `TASK_TIMEOUT`. Per-task declarations require Django 6.1 or
  Django 5.2 with django-tasks 0.12+. A particular running task cannot be
  interrupted on demand. See [Task timeouts](production.md#task-timeouts)
  for enforcement and its limits.
- Every django-ox table lives on one database, the one your router sends
`OxTask` to. A queue on a different database from the rows it refers to gives
up the transactional enqueue.
- Worker concurrency is a thread pool, which fits I/O-bound tasks. For
  CPU-bound work, run `--processes N --concurrency 1`, which is N worker
  processes under one supervisor. See
  [Production](production.md#threads-and-processes).

Batches, unique tasks, rate limiting and workflows are in [Oxpull Pro](pro.md), a paid add-on; <https://oxpull.com/> has the details.
Metrics stay in this package: `django_ox.stats` and `ox_health` are free and
stay free.

## License

BSD 3-Clause.
