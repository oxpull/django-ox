# Django tasks without another datastore

Send email, build reports and run scheduled jobs using the database your Django app already needs. django-ox runs the worker and returns unfinished tasks to the queue when a worker dies. No Redis or RabbitMQ.

Start with the situation that brought you here. These examples use `from django.tasks import task`; on Django 5.2 LTS, use `from django_tasks import task`. django-ox supports Django 5.2 LTS, 6.0 and 6.1 on Python 3.12+.

## Recover tasks left RUNNING after a worker dies

A deploy, the OOM killer or `kill -9` stops a django-tasks-db worker mid-task. The row stays RUNNING, but no worker picks it up again.

Use django-ox so tasks queued through the new backend have a recovery path.

```python
# settings.py: the task code stays as it is
TASKS = {
    "default": {
        # was "django_tasks_db.DatabaseBackend"
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "LOCK_TIMEOUT": 300,  # seconds a silent worker keeps its task
            "MAX_ATTEMPTS": 3,  # claims per task, a dead worker's included
        },
    }
}
```

Both values are the defaults. Swap the app in `INSTALLED_APPS`, run
`python manage.py migrate django_ox`, and start `python manage.py ox_worker`
where you ran `db_worker`. Existing django-tasks-db rows need separate handling; changing the backend does not recover them.

If using Django's PostgreSQL pool, check
[pool sizing](production.md#database-connections-and-postgresql-pooling)
before starting workers.

Every django-ox worker runs a reaper. When a worker's lease expires, its unfinished task returns to READY for another attempt, subject to `MAX_ATTEMPTS`. No separate recovery script.

At the default timeout, recovery takes up to about 330 seconds. Execution is at-least-once: the previous attempt may already have applied an effect, so make that effect safe to repeat.

## Run Django background jobs without Redis or RabbitMQ

An email or report needs to run outside the request. Keep that work in your database instead of adding a broker just to get it off the response path.

```python
# reports/tasks.py
from django.tasks import task


@task
def build_report(account_id):
    return f"report for account {account_id}"


# from a view, a signal handler or a command
build_report.enqueue(account_id=42)
```

`enqueue()` writes a task row. Run
`python manage.py ox_worker --concurrency 4` to execute the work.
With Django's PostgreSQL pool, check
[pool sizing](production.md#database-connections-and-postgresql-pooling).

Failed tasks retry with exponential backoff up to `MAX_ATTEMPTS`. When a job needs attention, open Django admin to inspect each attempt's traceback. You supervise a worker process, but there is no second datastore to provision or monitor. The queue uses your database's capacity.

## Enqueue a task inside transaction.atomic()

Save the user and queue their welcome email together. If signup rolls back, its email task should disappear too.

```python
from django.contrib.auth import get_user_model
from django.db import transaction

from .tasks import send_welcome  # a plain @task function


def register(username, email):
    with transaction.atomic():
        user = get_user_model().objects.create_user(username, email)
        send_welcome.enqueue(user.pk)
    return user
```

`enqueue()` is one INSERT on the database holding `OxTask`. Put the task and user row in the same database transaction, and both commit or neither does. Workers see the task only after the commit. No `transaction.on_commit()` needed.

That is the advantage over sending to a separate broker: the enqueue can share your database transaction. django-tasks-db shares this property too.

The transaction covers the database writes, not email delivery. Make the task safe to repeat.

## Run recurring Django tasks without a scheduler process

Run the nightly report and refresh exchange rates every five minutes without deploying a separate dispatcher such as `celery beat`.

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
                "refresh-rates": {"task": "reports.tasks.refresh_rates", "every": 300},
            },
        },
    }
}
```

Every worker dispatches due schedules. A unique constraint on the schedule name and tick time prevents workers from enqueueing the same tick twice.

Keep schedules in settings, or let your team pause and edit them as [rows in Django admin](stored-schedules.md). See [Recurring tasks](recurring-tasks.md) for the configuration.

Schedules run while at least one worker is running. After an outage, only the most recent missed tick is enqueued.

## Deduplicate a repeated webhook with Oxpull Pro

A payment provider sends the same event twice. Your webhook queues the same invoice sync twice. Stop the duplicate at enqueue rather than making the queue do the same work again.

With django-ox alone, these calls create separate tasks:

```python
from django.tasks import task


@task
def sync_invoice(invoice_id): ...


sync_invoice.enqueue(1001)
sync_invoice.enqueue(1001)  # django-ox: a second row, a second run
```

Oxpull Pro's unique tasks deduplicate inside the task's own transaction. While a task with the same key is READY or RUNNING, enqueueing it again returns the pending task's result without writing another row.

Once the task settles, the same call can enqueue again. This prevents duplicate pending work; it does not make external effects exactly-once. Tasks still need to be safe to repeat.

Pro also lets you track a batch through completion, control task starts with shared rate limits, and connect dependent tasks into workflows. You get email support from the maintainer and priority feature requests.

[Order Oxpull Pro](https://oxpull.com/) for $399 per year, per company, excluding VAT where it applies. Submit the form; your invoice arrives within two business days. Once paid, private package index credentials arrive by email. Installation stays `pip install`. Pro requires Django 6.0+.

## When to choose another tool

Choose another backend if you need to revoke a chosen running task, use chains, groups or chords, or exceed the throughput one database can serve. Celery supports revocation and those composition patterns.

Transactional enqueue requires the task and model rows to share a database. A separate queue database cannot give you that shared commit.

For CPU-bound work, the thread pool is not enough on its own. Use `--processes N` and measure your workload.

[**Follow the step-by-step guide**](background-tasks.md) to install django-ox and run your first tasks.
