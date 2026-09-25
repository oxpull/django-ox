# Django background tasks, step by step

Send a welcome email without running Redis or RabbitMQ. This guide uses django-ox to queue Django tasks in your existing database and run them with a worker. If a worker dies, its unfinished tasks return to the queue.

You will save a user and enqueue their welcome email inside the same `transaction.atomic()` block. Both rows commit, or neither does. No task is left behind for a signup that rolled back.

Every command and output block below was run as written on Django 6.0 with SQLite. The guide calls out the changes for Django 5.2 LTS. django-ox also supports Django 6.1 and requires Python 3.12+.

## What you will build

A project called `myproject` with an `accounts` app. Its `register` view creates a user and queues their welcome email in one database transaction. A background worker sends the email, printed to its terminal for this walk-through.

Then you will watch a failed task succeed on retry, schedule a task to run every minute as a stand-in for a nightly report, and inspect the tasks, attempts and tracebacks in Django admin.

## Prerequisites

- Python 3.12 or later.
- Django 6.0 or later, or Django 5.2 LTS. Django 6.0 and later ship the
  Tasks framework in core. On Django 5.2 LTS it comes from the
  `django-tasks` backport, which the `backport` extra installs for you.
- SQLite is enough for this walk-through, and it is what a fresh
  `startproject` uses. Use PostgreSQL in production; the
  [Production](production.md#postgresql-mysql-or-sqlite) page says why.
- Two terminal windows: one for the web server, one for the worker.

The two Django versions differ in one import line. On Django 6.0 and later:

```python
from django.tasks import task
```

On Django 5.2 LTS:

```python
from django_tasks import task
```

django-ox itself handles both, and everything else on this page is the same
on both versions.

## Step 1: Install django-ox and point the Tasks framework at it

Create a project and an app, or use ones you already have:

```
django-admin startproject myproject .
python manage.py startapp accounts
```

Install the package:

```
pip install django-ox
```

On Django 5.2 LTS, install the backport with it:

```
pip install "django-ox[backport]"
```

Add the app, name the backend, and route email to the terminal for now:

```python
# myproject/settings.py
INSTALLED_APPS = [
    # ...
    "django_ox",
    "accounts",
]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    }
}

# Print emails to the terminal instead of sending them.
EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
```

`TASKS` is Django's own setting. `django_ox` provides the backend, the
worker command and the table the queue lives in. The email backend is for
this walk-through only: the welcome email will appear in the worker's
terminal instead of going anywhere.

Create the tables:

```
python manage.py migrate
```

```
Operations to perform:
  Apply all migrations: admin, auth, contenttypes, django_ox, sessions
Running migrations:
  Applying contenttypes.0001_initial... OK
  ...
  Applying django_ox.0001_initial... OK
  ...
  Applying sessions.0001_initial... OK
```

In a project that already has its tables, `python manage.py migrate
django_ox` creates django-ox's alone.

Check that django-ox can reach the database:

```
python manage.py ox_health
```

```
OK: backlog=0 oldest_age=none last_claim_age=none
```

No tasks yet, no worker yet. Both numbers will change below.

## Step 2: Write your first background task

A task is a function with the `@task` decorator. Put it in a module called
`tasks.py` inside the app:

```python
# accounts/tasks.py
from django.contrib.auth.models import User
from django.core.mail import send_mail

# Django 6.0+. On Django 5.2: from django_tasks import task
from django.tasks import task


@task
def send_welcome_email(user_id):
    user = User.objects.get(pk=user_id)
    send_mail(
        subject="Welcome",
        message=f"Hi {user.username}, thanks for signing up.",
        from_email=None,
        recipient_list=[user.email],
    )
```

Nothing here comes from django-ox. `@task` is the Tasks framework, and the
function is ordinary Django code. Arguments are stored as JSON, so pass the
user's id rather than the user.

Try it from the shell before wiring up a view:

```
python manage.py shell
```

```pycon
>>> from django.contrib.auth.models import User
>>> from accounts.tasks import send_welcome_email
>>> user = User.objects.create_user("alice", "alice@example.com")
>>> result = send_welcome_email.enqueue(user.pk)
>>> result.status
TaskResultStatus.READY
>>> result.id
'9cb98ba6-46e0-46a7-8394-1046fa2ff1fd'
>>> result.is_finished
False
```

`enqueue()` wrote one row to the queue table and returned a result you can
look up later by that id. Nothing has run. Tasks run only while a worker is
running, and you have not started one yet, so the row waits. Leave this
shell open.

## Step 3: Enqueue the task from a view

The view creates the user and enqueues the email in one transaction:

```python
# accounts/views.py
from django.contrib.auth.models import User
from django.db import transaction
from django.http import HttpResponse
from django.shortcuts import render

from .tasks import send_welcome_email


def register(request):
    if request.method != "POST":
        return render(request, "accounts/register.html")
    with transaction.atomic():
        user = User.objects.create_user(
            username=request.POST["username"],
            email=request.POST["email"],
        )
        send_welcome_email.enqueue(user.pk)
    return HttpResponse(f"Thanks, {user.username}. Your welcome email is on its way.")
```

The form skips the password field to stay short; a real project would use
`UserCreationForm`. Route the view and give it a template:

```python
# accounts/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("register/", views.register, name="register"),
]
```

```python
# myproject/urls.py
from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("accounts.urls")),
]
```

```html
<!-- accounts/templates/accounts/register.html -->
<form method="post">
  {% csrf_token %}
  <label>Username <input name="username" required></label>
  <label>Email <input name="email" type="email" required></label>
  <button>Sign up</button>
</form>
```

Start the development server in your first terminal:

```
python manage.py runserver
```

Open <http://127.0.0.1:8000/register/>, sign up as `bob` with the address
`bob@example.com`, and submit. The response comes back at once:

```
Thanks, bob. Your welcome email is on its way.
```

```
[19/Sep/2026 07:55:20] "POST /register/ HTTP/1.1" 200 46
```

The request did not send an email. It wrote a user row and a task row and
returned. The email is the worker's job, and the worker is still not
running.

### Why there is no `transaction.on_commit()`

`enqueue()` is one INSERT on the database that holds django-ox's table,
your default database unless you route it elsewhere. Inside
`transaction.atomic()` on that database, the task row is part of the same
transaction as the user row. It becomes visible to workers when the
transaction commits, and it disappears if the transaction rolls back.

Watch that happen. In the shell from Step 2:

```pycon
>>> from django.db import transaction
>>> from django_ox.models import OxTask
>>> OxTask.objects.count()
2
>>> try:
...     with transaction.atomic():
...         user = User.objects.create_user("carol", "carol@example.com")
...         result = send_welcome_email.enqueue(user.pk)
...         raise RuntimeError("something after the enqueue failed")
... except RuntimeError:
...     pass
>>> User.objects.filter(username="carol").exists()
False
>>> OxTask.objects.count()
2
>>> send_welcome_email.get_result(result.id)
Traceback (most recent call last):
  ...
django.tasks.exceptions.TaskResultDoesNotExist: 7e12477a-3e41-401f-b902-f74545a4d7e7
```

The two rows in the table are alice's task and bob's. Carol's user and
carol's task were written inside the block and rolled back together, and
the id `enqueue()` handed back now refers to nothing. No worker can ever
pick up a welcome email for a user who does not exist.

With a queue that lives outside your database, the enqueue leaves your
process the moment you call it, and code has to wrap every call in
`transaction.on_commit()` to close that gap. Here there is nothing to wrap.
The guarantee covers the rows you write on the same database as the task:
a bare `atomic()` opens on `default`, and `default` is where django-ox's
table is unless you wrote a database router.

## Step 4: Run the worker and read its output

Open your second terminal and start a worker:

```
python manage.py ox_worker
```

It picks up both waiting tasks straight away:

```
2026-09-19 07:55:55,442 INFO django_ox Worker myhost-23168-zYPsu058 starting: queues=['default'] concurrency=1 poll=1.0s schedules=0
Task id=9cb98ba6-46e0-46a7-8394-1046fa2ff1fd path=accounts.tasks.send_welcome_email state=RUNNING
Subject: Welcome
From: webmaster@localhost
To: alice@example.com

Hi alice, thanks for signing up.

-------------------------------------------------------------------------------
2026-09-19 07:55:55,467 INFO django_ox Task id=9cb98ba6-46e0-46a7-8394-1046fa2ff1fd path=accounts.tasks.send_welcome_email succeeded in 18ms
Task id=9cb98ba6-46e0-46a7-8394-1046fa2ff1fd path=accounts.tasks.send_welcome_email state=SUCCESSFUL
Task id=0c2d708b-47ed-42b1-b72c-9580baed75fc path=accounts.tasks.send_welcome_email state=RUNNING
Subject: Welcome
From: webmaster@localhost
To: bob@example.com

Hi bob, thanks for signing up.

-------------------------------------------------------------------------------
2026-09-19 07:55:55,471 INFO django_ox Task id=0c2d708b-47ed-42b1-b72c-9580baed75fc path=accounts.tasks.send_welcome_email succeeded in 1ms
Task id=0c2d708b-47ed-42b1-b72c-9580baed75fc path=accounts.tasks.send_welcome_email state=SUCCESSFUL
```

Reading it line by line:

- `Worker ... starting` names the worker (your hostname, the process id and
  a random suffix), the queues it serves, how many tasks it runs at once,
  how often it polls when idle, and how many recurring schedules it
  dispatches. All four are flags or settings you will meet below.
- `Task id=... state=RUNNING` and `state=SUCCESSFUL` are Django's own log
  lines from the Tasks framework. They show because `DEBUG = True` sends
  Django's INFO logging to the console.
- The email is `send_mail` writing to the console backend. The full MIME
  headers are trimmed here.
- `succeeded in 18ms` is django-ox recording the attempt. A failure logs
  at WARNING with the exception class, as Step 6 shows.

The task from the shell ran first, then the one from the form. Back in the
shell, read the result. If you closed it, look the result up by id:

```pycon
>>> result = send_welcome_email.get_result("9cb98ba6-46e0-46a7-8394-1046fa2ff1fd")
>>> result.status
TaskResultStatus.SUCCESSFUL
>>> result.is_finished
True
>>> result.return_value
>>> result.attempts
1
>>> result.errors
[]
>>> result.finished_at
datetime.datetime(2026, 9, 19, 7, 55, 55, 466285, tzinfo=datetime.timezone.utc)
```

`return_value` is `None` because the task returns nothing. A task that
returns a value stores it as JSON, and `refresh()` re-reads the row when
you already hold a result object.

Stop the worker with Ctrl-C:

```
2026-09-19 07:56:28,812 INFO django_ox Worker myhost-23168-zYPsu058 received SIGINT; draining in-flight tasks. Signal again to force exit.
2026-09-19 07:56:28,813 INFO django_ox Worker myhost-23168-zYPsu058 stopped
```

It stops claiming, finishes whatever is in flight, and exits 0. SIGTERM
does the same, which is what a process manager sends. A second signal
forces an immediate exit.

Two things about the worker to carry forward. It is a separate process:
tasks run only while one is up. And it does not reload code the way
`runserver` does: restart it after you change a task.

## Step 5: See the task in the Django admin

Create an admin user and sign in at <http://127.0.0.1:8000/admin/>:

```
python manage.py createsuperuser
```

Under **django-ox** there are two models: **Ox tasks** and **Ox
schedules**. Open **Ox tasks**. The change list shows one row per task:

| Id | Task path | Queue name | Status | Attempts | Enqueued at | Finished at |
| --- | --- | --- | --- | --- | --- | --- |
| 0c2d708b-47ed-42b1-b72c-9580baed75fc | accounts.tasks.send_welcome_email | default | Successful | 1 | Sept. 19, 2026, 7:55 a.m. | Sept. 19, 2026, 7:55 a.m. |
| 9cb98ba6-46e0-46a7-8394-1046fa2ff1fd | accounts.tasks.send_welcome_email | default | Successful | 1 | Sept. 19, 2026, 7:54 a.m. | Sept. 19, 2026, 7:55 a.m. |

The sidebar filters by status and by queue, and the search box takes an id
or a task path. The action menu holds **Retry selected tasks** and
**Discard selected tasks**: a retry gives a failed task one more attempt,
and a discard closes a queued or failed task without running it.

Click a row for the read-only detail page. It lays out the id, the task
path, the arguments, the status, the queue and priority, the attempts made
against the maximum, the ids of every worker that ran it, the return value,
every failed attempt's traceback, and the timing of enqueue, start and
finish. The admin never adds, edits or deletes task rows; the two actions
are the only writes it offers. The
[Monitoring](monitoring.md#the-admin-page) page has the permissions and
the rest.

## Step 6: Retries: make a task fail once

Add a second task that raises on its first attempt and succeeds on the
next, standing in for a remote system that was down for a moment:

```python
# accounts/tasks.py, below send_welcome_email
@task(takes_context=True)
def sync_to_crm(context, user_id):
    if context.attempt == 1:
        raise ConnectionError("CRM did not answer")
    return f"synced user {user_id} on attempt {context.attempt}"
```

`takes_context=True` asks the Tasks framework to pass a `TaskContext` as
the first argument. Its `attempt` is the number of the attempt now running.

Restart the worker in the second terminal, then enqueue the task from the
shell and read it a couple of seconds later:

```pycon
>>> from accounts.tasks import sync_to_crm
>>> result = sync_to_crm.enqueue(2)
>>> result.id
'0f68e1a7-17db-4f6f-87e5-6f05c6b3245e'
>>> result.refresh()
>>> result.status
TaskResultStatus.READY
>>> result.attempts
1
>>> result.errors[0].exception_class_path
'builtins.ConnectionError'
```

The first attempt has already failed. The task is `READY` again, waiting
for its retry, and the error is recorded. The worker's terminal says the
same:

```
Task id=0f68e1a7-17db-4f6f-87e5-6f05c6b3245e path=accounts.tasks.sync_to_crm state=RUNNING
2026-09-19 07:58:00,088 WARNING django_ox Task id=0f68e1a7-17db-4f6f-87e5-6f05c6b3245e path=accounts.tasks.sync_to_crm attempt 1/3 failed (ConnectionError); retrying in 5.0s
```

Wait five seconds and read the result again:

```pycon
>>> result.refresh()
>>> result.status
TaskResultStatus.SUCCESSFUL
>>> result.attempts
2
>>> result.return_value
'synced user 2 on attempt 2'
```

```
Task id=0f68e1a7-17db-4f6f-87e5-6f05c6b3245e path=accounts.tasks.sync_to_crm state=RUNNING
2026-09-19 07:58:05,143 INFO django_ox Task id=0f68e1a7-17db-4f6f-87e5-6f05c6b3245e path=accounts.tasks.sync_to_crm succeeded in 0ms
Task id=0f68e1a7-17db-4f6f-87e5-6f05c6b3245e path=accounts.tasks.sync_to_crm state=SUCCESSFUL
```

You did nothing to make this happen. A task that raises is retried with
exponential backoff: three attempts by default, with the delay starting at
five seconds and doubling each time, up to ten minutes. After the last
failure the task is `FAILED` and stays in the table with the traceback of
every attempt, where the admin's **Retry selected tasks** action can give
it another go. In the admin, this task's detail page now shows `Attempt 1:
builtins.ConnectionError` with its traceback under **Attempt errors**, and
the return value beside it. `MAX_ATTEMPTS`, `BACKOFF_INITIAL` and
`BACKOFF_MAX` on the [Configuration](configuration.md#options) page tune
the envelope.

An attempt is counted when a worker claims the task, so a worker that dies
mid-task uses one as well; after `LOCK_TIMEOUT` the reaper hands the task
to another worker. Execution is at-least-once, so write tasks that are safe
to run twice. [Common patterns](patterns.md#make-a-task-safe-to-run-twice)
shows the shape, and [Production](production.md#the-reaper) has the
mechanics.

## Step 7: Add a recurring task, with no scheduler process

Add a third task, a stand-in for a nightly report:

```python
# accounts/tasks.py, below sync_to_crm
@task
def signup_report():
    return {"users": User.objects.count()}
```

Schedules are declared in settings, in the `OPTIONS` of the backend they
enqueue through. This one fires every minute so you can watch it:

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "OPTIONS": {
            "SCHEDULES": {
                "signup-report": {
                    "task": "accounts.tasks.signup_report",
                    "cron": "* * * * *",
                },
            },
        },
    }
}
```

For a nightly report at three in the morning you would write
`"cron": "0 3 * * *"`; for a fixed interval, `"every": timedelta(hours=1)`
in place of `cron`. Times are wall-clock in your `TIME_ZONE`, which is UTC
in a fresh project.

`manage.py check` validates a schedule's declaration before it runs. It reports
a task path that does not import, or an expression that can never fire:

```
python manage.py check
```

```
System check identified no issues (0 silenced).
```

Change the expression to `0 0 30 2 *`, the 30th of February, and run it
again:

```
SystemCheckError: System check identified some issues:

ERRORS:
?: (django_ox.E002) Schedule 'signup-report': Cron expression '0 0 30 2 *' can never match: no listed month has any of the listed days.
	HINT: Fix the SCHEDULES entry of this backend's OPTIONS.

System check identified 1 issue (0 silenced).
```

The worker runs the same validation at startup, so these errors stop startup
rather than skipping dispatch in silence. The checks do not establish database
acceptance of arguments; schedule-scoped failures at dispatch are logged as
`schedule_dispatch_error`. Put `* * * * *` back and restart the worker.
The startup line now says `schedules=1`, and at the next minute boundary
the schedule fires:

```
2026-09-19 07:59:12,044 INFO django_ox Worker myhost-23929-gJ7A6QmA starting: queues=['default'] concurrency=1 poll=1.0s schedules=1
2026-09-19 08:00:00,399 INFO django_ox Dispatched schedule signup-report tick 2026-09-19T08:00:00+00:00 (task id=fe1388c3-bb4a-4f11-9e21-85449a4d50e0)
Task id=fe1388c3-bb4a-4f11-9e21-85449a4d50e0 path=accounts.tasks.signup_report state=RUNNING
2026-09-19 08:00:00,403 INFO django_ox Task id=fe1388c3-bb4a-4f11-9e21-85449a4d50e0 path=accounts.tasks.signup_report succeeded in 0ms
Task id=fe1388c3-bb4a-4f11-9e21-85449a4d50e0 path=accounts.tasks.signup_report state=SUCCESSFUL
```

There is no scheduler process to start. Every worker dispatches schedules,
and a unique constraint on the schedule name and the tick time means a due
tick is enqueued once however many workers you run. Each tick enqueues an
ordinary task, so retries, the result store and the admin page all apply
to it. Two rules to know. A new schedule waits for its next tick rather
than firing for a time before it existed; that is why the worker that
started at 07:59:12 first fired at 08:00:00. And if every worker is down
across several ticks, the most recent missed tick fires once on recovery
and older ones are skipped. The [Recurring tasks](recurring-tasks.md) page
has the cron syntax and the rest of the rules, and
[Schedules in the database](stored-schedules.md) covers schedules edited
in the admin without a deploy.

## Production notes

- **Run the worker under a process manager.** It is a foreground process
  that exits 0 on SIGTERM. The [Production](production.md#running-under-systemd)
  page has a systemd unit and a Compose service. In a container the
  command is the same one you ran, with the process count and the thread
  count you want:

    ```
    python manage.py ox_worker --processes 2 --concurrency 4
    ```

    ```
    2026-09-19 08:00:39,114 INFO django_ox Supervisor 24325 starting 2 worker process(es)
    2026-09-19 08:00:39,299 INFO django_ox Worker myhost-24327-glmGVHgP-0 starting: queues=['default'] concurrency=4 poll=1.0s schedules=1
    2026-09-19 08:00:39,299 INFO django_ox Worker myhost-24328-d7O05mDr-1 starting: queues=['default'] concurrency=4 poll=1.0s schedules=1
    ```

    That is eight tasks at once. With Django's PostgreSQL pool, budget
    connections per process; see
    [pool sizing](production.md#database-connections-and-postgresql-pooling).

    `--concurrency` is a thread pool, which suits email, HTTP and ORM work.
    For CPU-bound tasks run
    `--processes N --concurrency 1`. A worker process that dies is
    restarted by the supervisor. Give the process manager a stop grace
    period longer than your slowest task, so a drain finishes before it
    escalates to SIGKILL.
- **Probe with `ox_health`.** With no flags it checks that the database
  answers, which is what a container health check should test. With
  thresholds it turns queue depth and backlog age into an exit code for
  cron alerting:

    ```
    python manage.py ox_health --max-backlog 100 --max-age 300
    ```

    ```
    OK: backlog=0 oldest_age=none last_claim_age=37s
    ```

    `--format json` prints the same figures as one object. Which check
    belongs where is on the
    [Monitoring](monitoring.md#health-checks-ox_health) page, with the
    Prometheus endpoint and the log events.
- **Prune on a timer.** Finished rows stay in the table until you delete
  them, because the table is also the result store. Run `ox_prune` daily
  from cron or a systemd timer, with a retention that suits you. `--dry-run`
  reports what a cutoff would remove; on this demo database, whose rows
  are minutes old:

    ```
    python manage.py ox_prune --older-than 60s --dry-run
    ```

    ```
    Would delete 2 SUCCESSFUL/DISCARDED task row(s) finished before 2026-09-19T07:57:42.805703+00:00.
    Would delete 0 schedule tick row(s) scheduled before 2026-09-19T07:57:42.805703+00:00.
    ```

    Failed tasks are kept until you pass `--include-failed`, so their
    tracebacks survive until someone has read them. The
    [timer unit](production.md#pruning-on-a-timer) is on the Production page.
- **Use PostgreSQL.** SQLite is right for development and for a
  single-host deployment with one worker and threads. PostgreSQL and MySQL
  8 take a `SKIP LOCKED` claim path built for many workers. See
  [PostgreSQL, MySQL or SQLite](production.md#postgresql-mysql-or-sqlite).
- **On a read replica**, django-ox reads its own rows on the database it
  writes them to, so a lagging replica never hides a task from the worker,
  the result API or `ox_health`. The admin reads the primary too.
  `ox_worker`, `ox_prune` and `ox_health` take `--database` to name the
  alias they work on; leave it unset unless you mean it. Details under
  [Read replicas](configuration.md#read-replicas).
- **Run `migrate` before rolling workers**, as a deploy step or an init
  container, not from the worker itself.

## Next steps

- [Configuration](configuration.md): the `TASKS` entry, every `OPTIONS`
  key, and the flags of `ox_worker`, `ox_prune` and `ox_health`.
- [Recurring tasks](recurring-tasks.md): cron syntax, fixed intervals,
  missed ticks and time zones. [Schedules in the database](stored-schedules.md)
  for rows edited in the admin.
- [Common patterns](patterns.md): named queues and priorities, deferring
  work with `run_after`, enqueuing many tasks at once, and
  [testing without a worker](patterns.md#test-without-a-worker).
- [Production](production.md): systemd, containers, graceful shutdown,
  scaling out, the lease and the reaper.
- [Monitoring](monitoring.md): the stats API, `ox_health`, Prometheus, log
  events, retry and discard.
- [Migrating](migrating.md): from [Celery](migrating.md#from-celery), from
  [django-tasks-db](migrating.md#from-another-djangotasks-backend), from
  huey, and the switch-over order.
- [Choosing a task backend](choosing.md): how django-ox compares with the
  other backends a Django team shortlists, and when not to use it.
