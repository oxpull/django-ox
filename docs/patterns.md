# Common patterns

Use these examples to build common task workflows with django-ox.
Most use the standard `django.tasks` API; django-ox-specific features
and differences from broker-based queues are identified where they appear.

## Send an email after signup

The common case, and the one where a database queue behaves differently from a
broker.

```python
from django.db import transaction
from django.tasks import task  # Django 6.0+; on 5.2: from django_tasks import task


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
database as the `User` row, your default one, so the task becomes visible to
workers only when the transaction commits. If `create_user` is rolled back
further up, the email task disappears with it.

Pass the id, not the object. Arguments are stored as JSON, and a stale copy of a
model is a bug waiting to happen.

## Retry a flaky third-party call

Retries are automatic. Without a task override, raise and the worker
schedules the next attempt with exponential backoff.

On Django 6.1, or Django 5.2 with django-tasks 0.12+, django-ox backends
(`OxBackend` and `django_ox.testing`) support declaring a task's claim
budget, backoff callback and timeout through `@task`:

```python
import httpx
from django.tasks import task  # On Django 5.2: from django_tasks import task


def crm_backoff(exception, task_result):
    if isinstance(exception, httpx.TransportError):
        return 10 * task_result.attempts
    if isinstance(exception, httpx.HTTPStatusError):
        if exception.response.status_code >= 500:
            return 10 * task_result.attempts
    return None


@task(max_attempts=5, backoff=crm_backoff, timeout=30)
def sync_to_crm(order_id):
    order = Order.objects.get(pk=order_id)
    response = httpx.post("https://crm.example.com/orders", json=order.payload)
    response.raise_for_status()
```

This task gets up to five claims, including the first. The callback retries
transport errors and HTTP 5xx responses, and stops retries for other errors
by returning `None`. It receives the original exception and a failed-attempt
snapshot whose `attempts` includes the current claim.

On Django 6.1 with django-stubs 6.1.1, mypy reports `call-overload` for
these policy keywords and also `untyped-decorator` under strict checking.
A declaration-scoped `# type: ignore[call-overload]`, or
`# type: ignore[call-overload, untyped-decorator]` under strict checking,
suppresses those diagnostics; the task's static type becomes `Any`.
The Django 5.2 backport supports the declaration without a suppression.
On Django 6.0, use bare `@task` and backend-level policy options.

Callbacks must be fast, synchronous and side-effect-free. They are not
bounded by the task timeout. Return integer seconds or a `timedelta` for a
delay, `0` for immediate eligibility, or `None` to stop retrying. Invalid
returns and callback errors log `task_policy_error` and fall back to the
worker's exponential backoff. See the
[callback contract](production.md#backoff-callbacks).

Backend defaults still apply to fields a task leaves as `None`:

```python
"OPTIONS": {
    "MAX_ATTEMPTS": 5,
    "BACKOFF_INITIAL": 10,  # seconds before the second attempt
    "BACKOFF_MAX": 600,  # ceiling for the worker's exponential backoff
}
```

The stored claim budget decides how many attempts remain. Backoff and
timeout come from the worker's live task declaration on each attempt.
`BACKOFF_MAX` does not cap a valid callback delay.

Every attempt keeps its own traceback, so a task that failed four times shows
all four. After its attempts are spent, or its callback declines a retry,
the task is `FAILED` and stays in the table: `ox_prune` keeps failed rows
unless you pass `--include-failed`.

These policy declarations are provisional. See
[API stability](stability.md#provisional-task-policy).

## Answer a webhook fast

Do the minimum in the request, then hand off. The sender gets its `200` straight
away, however long the work behind it takes.

```python
@task
def process_payment_event(event_id): ...


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
# On Django 5.2: from django_tasks import TaskResultStatus
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
`is_finished` covers the last two. A task whose worker vanished without
reporting also reads as `FAILED`; [the reaper](production.md#the-reaper)
explains what that record contains. Return values are stored as JSON, so
return a URL or an id rather than a file or a model.

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

## Enqueuing many tasks at once

A loop of `enqueue()` calls is one `INSERT` per task. For a few dozen that is
fine; for a mailing run or a nightly fan-out it is the slow part of the
request. `django_ox.bulk.enqueue_many()` writes them in one statement per
thousand rows.

```python
from django_ox.bulk import enqueue_many

results = enqueue_many(
    send_digest,
    [((user.pk,), {}) for user in User.objects.filter(digest=True)],
)
```

Each element of the list is an `(args, kwargs)` pair, the same two values
`enqueue(*args, **kwargs)` passes to the backend. The return value is one
`TaskResult` per pair, in the order given. Queue, priority and `run_after`
belong to the task, so set them once with `.using(...)`:

```python
results = enqueue_many(
    send_digest.using(queue_name="emails", run_after=tonight),
    [((user.pk,), {}) for user in users],
)
```

The task and every argument are checked before the first row is written: a
queue the backend does not accept, a task bound to another backend, or an
argument that will not serialise to JSON raises with nothing inserted. The
rows go in one `INSERT` per 1,000 (SQLite caps the variables a statement can
bind) inside one transaction, so a call of 5,000 commits all 5,000 or none,
and inside a `transaction.atomic()` opened on the database that holds
`OxTask` it commits or rolls back with the rest of the work you write there,
as `enqueue()` does.

This is a bulk insert and nothing more. Grouping the
tasks, reading their progress as one number and firing a callback when the
last one settles is a [batch](pro.md), in Oxpull Pro.

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

With Django's PostgreSQL pool, size each worker's pool for its concurrency:
[pool sizing](production.md#database-connections-and-postgresql-pooling).

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

The one habit to build. Execution is at-least-once, so a task retries both
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

Point the test settings at django-ox's policy-aware test backends. No
django-ox tables, no worker process. The paths are the same on every
supported Django version.

```python
# Runs each task once on the caller's thread.
TASKS = {"default": {"BACKEND": "django_ox.testing.ImmediateBackend"}}
```

```python
# Records tasks without running them, for asserting what was enqueued.
TASKS = {"default": {"BACKEND": "django_ox.testing.DummyBackend"}}
```

Both backends accept and validate `PolicyTask` fields. Neither retries,
calls a backoff callback or enforces a timeout. Each backend instance logs
a `task_policy_inert` warning on the first enqueue of each task path that
explicitly declares policy. This is not a system-check warning.

Use these paths instead of the framework's stock test backends when tasks
declare policy. On fresh import, the stock backends reject the extra task
fields. A task built under `OxBackend` before `override_settings` switches
to stock Immediate can instead retain policy fields that the backend
silently ignores. These test backends do not make policy declarations work
on Django 6.0, whose decorator rejects the keyword arguments.

Keep `OxBackend` for tests that exercise claiming, retries and backoff.
Use [`run_tasks()`](#run-queued-tasks-in-tests) to drain due tasks in test code.
Test timeout enforcement and worker infrastructure against a real worker.

## Run queued tasks in tests

`django_ox.testing.run_tasks()` is public and provisional. Keep `OxBackend` in your test settings and create its tables through migrations. Use this helper to run queued tasks without a worker process.

```python
run_tasks(*, backend="default", queues=None, max_tasks=None, raise_failures=False) -> list[TaskResult]
```

All arguments are keyword-only. `backend` must name an `OxBackend` alias. It selects the worker class and its default queues, including a configured `WORKER_CLASS`. Rows are claimed by queue, regardless of which backend enqueued them. Aliases sharing queues share work. For Pro workflows and rate limits, pass the Pro alias as `backend`.

`queues=None` uses the worker's queues. An empty list does the same. Empty backend `QUEUES` means every queue. An explicit list restricts claiming to those queues.

Each attempt uses the configured worker's claim and execution methods on the caller's thread and database connection. It runs due `READY` rows that pass the worker's claim filters. Future `run_after` values, pending retry delays and other statuses are left alone. Tasks enqueued by tasks or their emulated commit callbacks can run in the same call. Async task bodies can see the test's uncommitted rows. Worker outcome recording, backoff and lifecycle signals apply.

The return value is a list of framework `TaskResult` objects in attempt order. Each object captures that attempt's recorded state. Retries produce separate results. Failures are recorded without raising by default. With `raise_failures=True`, the same exception is raised after recording and attempt callbacks, including for retryable failures. Later tasks are left unclaimed. A retried attempt's result is `READY` (a retry is pending), and reading its `return_value` raises, following the framework's rule for unfinished results.

By default, it claims until no task is claimable. `max_tasks=N` limits attempts, including retries, and returns quietly when the limit is reached. Zero makes no claim. Use a nonnegative integer, excluding booleans. It runs at most 1,000 attempts without an explicit limit. It raises `RuntimeError` if due `READY` work remains then, even if that work is gated or rate-limited. Attempt limits do not bound elapsed time.

Call it from synchronous test code. Django's async `TestCase` methods can use `sync_to_async`. Do not call it from tasks, their callbacks or signal receivers. Nested drains are refused. Broken caller transactions are refused before any claim.

### Enqueue, roll back and advance time

These examples use Django 6.x imports. On Django 5.2, import `task` from `django_tasks` instead. Keep the task function at module scope so the worker can import it.

```python
from django.db import transaction
from django.tasks import task
from django.test import TestCase

from django_ox.testing import run_tasks


@task
def add(a, b):
    return a + b


class TaskTests(TestCase):
    def test_runs_queued_task(self):
        add.enqueue(2, 3)
        results = run_tasks(raise_failures=True)
        self.assertEqual([result.return_value for result in results], [5])

    def test_rolled_back_enqueue_never_runs(self):
        with self.assertRaises(ValueError):
            with transaction.atomic():
                add.enqueue(2, 3)
                raise ValueError("Cancel this transaction")

        self.assertEqual(run_tasks(), [])
```

The rolled-back enqueue leaves no task row to claim. `ImmediateBackend` runs during enqueue, even if the transaction later rolls back, and rejects `run_after`.

Patch `timezone.now()` to move the due cutoff. A task is due at its exact `run_after` time.

```python
from datetime import timedelta
from unittest.mock import patch

from django.utils import timezone


class DelayedTaskTests(TestCase):
    def test_run_after(self):
        now = timezone.now()
        due = now + timedelta(minutes=5)

        with patch("django.utils.timezone.now", return_value=now):
            add.using(run_after=due).enqueue(2, 3)
            self.assertEqual(run_tasks(), [])

        with patch("django.utils.timezone.now", return_value=due):
            results = run_tasks(raise_failures=True)
            self.assertEqual([result.return_value for result in results], [5])
```

For retries, advance time between calls by at least the configured backoff delay. Freezing a later time does not drain every retry. Each new retry is scheduled relative to that frozen instant. A zero backoff can retry within one call. Pro rate-limit windows use the database clock, which this patch does not advance.

### Transactions and callbacks

`TestCase` holds database transactions open. `run_tasks()` uses savepoints on every connection already inside an atomic block when an attempt starts. Task-owned atomic blocks still follow Django's rules. Under `TransactionTestCase` in autocommit, no savepoints or callback emulation are added. Writes and commit callbacks then follow worker autocommit behaviour. pytest-django equivalents are `transactional_db` and `django_db(transaction=True)`.

In an atomic block, commit callbacks registered by the body run when the body finishes, before outcome recording. Callbacks registered during outcome handling run afterwards. This emulates a commit without committing the transaction. The caller's existing callbacks stay pending. If application code enqueues through `on_commit()`, exit `captureOnCommitCallbacks(execute=True)` before calling `run_tasks()`.

Callbacks run in registration order, one database at a time. A callback registered by another callback runs after those already waiting. On a worker in autocommit, that new callback runs immediately. Callbacks discarded by a savepoint rollback do not run. A surrounding callback-capture context does not execute the task's callbacks twice.

Each emulated callback has its own savepoints. If it raises, its writes and newly registered callbacks roll back. A worker in autocommit keeps writes made before the error. A callback returning with a transaction marked for rollback fails with `TransactionManagementError`, naming the task, callback and database.

Robust callback failures are logged and leave the attempt's outcome unchanged. A non-robust body callback can fail an otherwise successful attempt. If the body already failed, its error is kept and the callback error is logged. Remaining callbacks are dropped after a non-robust failure. A non-robust outcome callback failure stops the drain without changing the recorded outcome.

Do not edit Django's pending callback list directly. If its caller-owned prefix changes, `run_tasks()` raises `RuntimeError` and stops handling callbacks on that database. Detection during body callback emulation also records the attempt as failed.

### Differences to test against a worker

Inside a `TestCase` transaction:

- A plain task-body exception keeps preceding writes. An ORM error that marks rollback discards the attempt's writes on that database. Other databases follow their own transaction state.
- A body returning with rollback marked fails with `TransactionManagementError`. This includes caught ORM errors and `set_rollback(True)`. A worker in autocommit keeps preceding writes and can succeed. Put a statement whose database error you catch inside its own `atomic()` block.
- PostgreSQL raw SQL errors can abort the transaction without Django marking rollback. The attempt's savepoint then rolls back, including when the body caught the error. A caught error then fails with `InternalError`. SQLite and MySQL undo only the failed statement and can keep preceding writes.
- `select_for_update()` can succeed without a task-owned atomic block, hiding a production error on databases that require one. Test task transaction boundaries with `TransactionTestCase`.
- Body callbacks wait until the body finishes, even if a task-owned block exits earlier. In autocommit, a callback registered outside an atomic block runs immediately. Code reading state set by its own callback can behave differently.
- Other connections cannot see the test's uncommitted rows. Callback timing and cross-database order differ from production.
- A later caller rollback removes database writes, but cannot undo external callback effects such as sent mail. A worker would wait for the enqueue to commit before claiming it.

Use a real worker to test timeout enforcement and worker infrastructure. `run_tasks()` runs no poll loop, thread pool, lease renewal, timeout watchdog, reaper, schedule dispatcher or process recycling. It does not dispatch reconcilers. Enqueue scheduled tasks and reconcilers explicitly if the test needs their work.

Timeouts are inert. `deadline()` and `remaining()` return `None`. A configured timeout produces `run_tasks_timeout_inert` once per task path per call. A hanging body or callback hangs the test.

## Not in the core

Batches, unique or deduplicated tasks, rate limiting and workflows are in
[Oxpull Pro](pro.md), a paid add-on. Metrics are in the
free tier: `django_ox.stats` and `manage.py ox_health` ship in the core. Chains
are on the Pro roadmap, undated.
