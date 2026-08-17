# Common patterns

Worked examples for the things people actually reach for. Every snippet uses the
standard `django.tasks` API, so it works on any backend; the notes point out
where django-ox behaves differently from a broker.

## Send an email after signup

The common case, and the one where a database queue behaves differently from a
broker.

```python
from django.db import transaction
from django.tasks import task


@task
def send_welcome_email(user_id):
    user = User.objects.get(pk=user_id)
    send_mail("Welcome", "...", None, [user.email])


def register(request):
    with transaction.atomic():
        user = User.objects.create_user(...)
        send_welcome_email.enqueue(user.pk)
```

No `transaction.on_commit()` here. The enqueue is an `INSERT` on the same
connection, so the task becomes visible to workers only when the transaction
commits. If `create_user` is rolled back further up, the email task disappears
with it.

Pass the id, not the object. Arguments are stored as JSON, and a stale copy of a
model is a bug waiting to happen.

## Retry a flaky third-party call

Retries are automatic. Raise, and the worker schedules the next attempt with
exponential backoff.

```python
@task
def sync_to_crm(order_id):
    order = Order.objects.get(pk=order_id)
    response = httpx.post("https://crm.example.com/orders", json=order.payload)
    response.raise_for_status()  # a 5xx raises, so the task retries
```

Tune the envelope on the backend, not per task:

```python
"OPTIONS": {
    "MAX_ATTEMPTS": 5,
    "BACKOFF_INITIAL": 10,   # seconds before the second attempt
    "BACKOFF_MAX": 600,      # ceiling
}
```

Every attempt keeps its own traceback, so a task that failed four times shows
all four. After `MAX_ATTEMPTS` the task is `FAILED` and stays in the table:
`ox_prune` keeps failed rows unless you pass `--include-failed`.

## Answer a webhook fast

Do the minimum in the request, then hand off. The sender gets its `200` straight
away, however long the work behind it takes.

```python
@task
def process_payment_event(event_id):
    ...


@csrf_exempt
def stripe_webhook(request):
    event = WebhookEvent.objects.create(payload=json.loads(request.body))
    process_payment_event.enqueue(event.pk)
    return HttpResponse(status=200)
```

Both writes are in the same transaction, so you cannot acknowledge an event you
failed to record, or record one that never gets processed.

## Run a job and check on it later

`enqueue()` returns a result you can look up by id.

```python
result = build_report.enqueue(month="2026-08")
request.session["report_task_id"] = result.id
```

```python
from django.tasks import TaskResultStatus

result = build_report.get_result(request.session["report_task_id"])
result.refresh()

if result.status == TaskResultStatus.SUCCESSFUL:
    return redirect(result.return_value)
if result.status == TaskResultStatus.FAILED:
    return render(request, "report_failed.html", {"errors": result.errors})
return render(request, "report_pending.html", {"attempts": result.attempts})
```

`refresh()` re-reads from the database, so call it before checking status.
`status` is one of `READY`, `RUNNING`, `SUCCESSFUL` or `FAILED`, and
`is_finished` covers the last two. Return values are stored as JSON, so return a
URL or an id rather than a file or a model.

## Defer work to a specific time

Use `.using(run_after=...)`. It returns a copy of the task with that setting
applied.

```python
from datetime import timedelta
from django.utils import timezone

send_reminder.using(run_after=timezone.now() + timedelta(days=1)).enqueue(booking.pk)
```

Workers ignore the row until then. For anything on a repeating clock, use a
[schedule](recurring-tasks.md) instead of enqueueing the next one from inside
the task.

## Keep slow work off the fast queue

Give slow tasks their own queue and run a separate worker for it, so a batch of
report builds cannot delay password resets.

```python
@task(queue_name="reports")
def build_report(month): ...


@task(queue_name="emails", priority=50)
def send_password_reset(user_id): ...
```

```
python manage.py ox_worker --queues emails --concurrency 4
python manage.py ox_worker --queues reports --concurrency 1
```

Priority runs from -100 to 100, higher first, and applies within a queue rather
than across queues. List every queue you use in the backend's `QUEUES`, or set
`QUEUES: []` to accept any name.

## Clean up on a schedule

```python
"OPTIONS": {
    "SCHEDULES": {
        "expire-carts": {
            "task": "shop.tasks.expire_abandoned_carts",
            "cron": "*/30 * * * *",
        },
    },
}
```

Nothing extra to run: the workers you already have dispatch the ticks. Details
in [Recurring tasks](recurring-tasks.md).

Task rows are not deleted for you. Run `ox_prune` on your own schedule, from
cron, a systemd timer, or a django-ox schedule:

```
python manage.py ox_prune --older-than 7d
```

## Make a task safe to run twice

The one habit worth building. Execution is at-least-once, so a task retries both
when it raises and when its worker dies mid-run. Assume every task can run
again.

```python
from django.db import transaction


@task
def charge_order(order_id):
    with transaction.atomic():
        order = Order.objects.select_for_update().get(pk=order_id)
        if order.charged_at:
            return  # a previous attempt already did this
        charge(order, idempotency_key=f"order-{order.pk}")
        order.charged_at = timezone.now()
        order.save(update_fields=["charged_at"])
```

Guard on state you have written, not on a flag you set in memory. The row lock
serialises concurrent attempts, and the idempotency key covers the gap where the
charge succeeds but the transaction does not commit. Use one whenever the
external system offers it.

**Your task is not run inside a transaction.** The worker manages its own for
claiming and bookkeeping, but your function is called outside them, so
`select_for_update()` and anything else needing an open transaction must open
one, as above.

## Test without a worker

Point the test settings at Django's own backends. No django-ox tables, no worker
process.

```python
# runs tasks inline, so an assertion right after enqueue sees the effect
TASKS = {"default": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}}
```

```python
# records tasks without running them, for asserting what was enqueued
TASKS = {"default": {"BACKEND": "django.tasks.backends.dummy.DummyBackend"}}
```

Keep django-ox in the settings you use for integration tests, where the point is
to exercise claiming and retries for real.

## Not in the core

Batches, unique or deduplicated tasks, rate limiting and metrics export are not
part of django-ox. They are the [Oxpull Pro](pro.md) feature set. Chains and
workflows are on that roadmap and do not exist yet in either tier.
