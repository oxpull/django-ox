# For AI assistants

Exact steps for setting up django-ox in an existing Django project, the facts
to get right while doing it, and how to prove it works. Machine-readable
copies: [llms.txt](llms.txt) (facts and links) and
[llms-full.txt](llms-full.txt) (every page of this site in one file).
Context7 library id: `/oxpull/django-ox`.

## Set up django-ox in this project

Requires Python 3.12+ and Django 5.2+. Check before installing:

```
python -c "import django, sys; print(django.__version__, sys.version.split()[0])"
```

Django 6.0 and later ship the Tasks framework in core. On Django 5.2 LTS it
comes from the `django-tasks` backport, so install the `backport` extra there.

Install:

```
pip install django-ox
```

or, with uv:

```
uv add django-ox
```

On Django 5.2 LTS:

```
pip install "django-ox[backport]"
```

or, with uv:

```
uv add "django-ox[backport]"
```

Edit `settings.py`:

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

Create the table:

```
python manage.py migrate django_ox
```

Verify. The expected output is the second line:

```
python manage.py ox_health
OK: backlog=0 oldest_age=none last_claim_age=none
```

`manage.py check` also runs the django-ox system checks, so a bad schedule
or timeout option fails here, as `django_ox.E002` to `E005`, before anything
deploys.

Start a worker in its own process, next to the web server, under the same
supervisor:

```
python manage.py ox_worker
```

Settings with every option named, for when the defaults need changing:

```python
TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
        "QUEUES": ["default"],  # [] allows any queue name
        "OPTIONS": {
            "MAX_ATTEMPTS": 3,  # claims per task before FAILED
            "LOCK_TIMEOUT": 300,  # seconds a worker may stop renewing its lease
            "BACKOFF_INITIAL": 5,  # first retry delay, seconds; doubles each attempt
            "BACKOFF_MAX": 600,  # retry delay ceiling, seconds
            "TASK_TIMEOUT": None,  # seconds one attempt may run; None is no limit
            "TASK_TIMEOUTS": {},  # per-queue values, {"queue": seconds}
            "TASK_TIMEOUT_GRACE": 30,  # seconds a timed-out thread gets to stop
            "SCHEDULES": {},  # recurring tasks, see Recurring tasks
        },
    }
}
```

## Facts to get right

- `QUEUES` sits beside `OPTIONS`, not inside it. Inside `OPTIONS` it is
  ignored without warning; the symptom is `InvalidTask: Queue 'X' is not
  valid for backend.`
- Tasks are plain Tasks-framework tasks. `from django.tasks import task` on
  Django 6.0+, `from django_tasks import task` on 5.2 LTS,
  decorate with `@task`, call `.enqueue(...)`. Nothing is imported from
  `django_ox` in task code.
- The worker imports a task by its dotted path, so the module must be
  importable in the worker process and the worker runs the same code as the
  producer; nothing is registered and there is no autodiscovery. `async def`
  tasks run.
- Priority and deferral are Django API: `task.using(priority=N)` with N from
  -100 to 100, higher first, and `task.using(run_after=...)` with a timedelta
  or datetime.
- Tasks run only while `ox_worker` is running. It is a separate process.
- SIGTERM and SIGINT both drain and exit 0; a second signal forces an
  immediate exit with code 130.
- `enqueue()` is one INSERT on the default connection. Inside
  `transaction.atomic()` the task is visible to workers only after commit and
  is gone on rollback. Do not add `transaction.on_commit()` around it.
- Many calls of one task go through `django_ox.bulk.enqueue_many(task,
  [(args, kwargs), ...])`: one INSERT per 1,000 rows, one transaction, results
  in input order. Set queue, priority and `run_after` once with `.using(...)`.
- Execution is at-least-once. Write tasks to be safe to run twice: guard on
  state already in the database, not on a flag in memory.
- The task function runs outside any transaction. Open
  `transaction.atomic()` inside the task when it needs `select_for_update()`.
- An attempt is consumed at claim time, so a worker dying mid-run uses one.
  Retry delay after attempt n is `BACKOFF_INITIAL * 2 ** (n - 1)`, capped at
  `BACKOFF_MAX`.
- `LOCK_TIMEOUT` bounds an unresponsive worker, not task length. The lease is
  renewed every `LOCK_TIMEOUT / 3` seconds while the task runs.
- The worker polls; `--interval` (default 1.0 s) is the idle sleep, so a task
  starts within one interval of its commit. There is no LISTEN/NOTIFY.
- `TASK_TIMEOUT` bounds one attempt. At the deadline `TaskTimeout` is raised
  inside the task on its own thread (an async task is cancelled) and the
  attempt is recorded as failed and retried on the backoff. A thread that
  has not stopped `TASK_TIMEOUT_GRACE` seconds later is recorded as failed
  and the worker exits 75 so its supervisor restarts it. Per-queue values go
  in `TASK_TIMEOUTS`; there is no per-task value. `django_ox.remaining()`
  reads the seconds left from inside a task. On a thread a coverage tool or
  a debugger is watching (a `sys.settrace` hook, or a `sys.monitoring` tool
  with events enabled) nothing is raised inside a sync task: the worker logs
  `timeouts_backstop_only`, a task that returns within `TASK_TIMEOUT_GRACE`
  is recorded as whatever it did, and one still running then is recorded as
  failed and recycles the worker. An async task is cancelled at the deadline
  either way.
- Claiming: one `UPDATE ... SKIP LOCKED ... RETURNING` statement on
  PostgreSQL; `SELECT ... FOR UPDATE SKIP LOCKED` on MySQL 8+; an atomic
  compare-and-set UPDATE on SQLite and other databases without `SKIP LOCKED`.
- `--concurrency N` is a thread pool in one process. `--processes N` runs N
  such workers under one supervisor; a worker process that dies is restarted
  after one second, doubling to 30 s, and more than five deaths of one slot
  in a minute stops the supervisor with exit 1. CPU-bound work wants
  `--processes N --concurrency 1`.
- Recurring tasks go in `OPTIONS["SCHEDULES"]`. Every worker dispatches them;
  there is no scheduler process to start.
- `ox_prune --older-than 7d` deletes finished rows; FAILED rows stay unless
  `--include-failed`. READY and RUNNING rows are never deleted.
- `path("ox/", include("django_ox.urls"))` mounts `GET /ox/metrics`, the
  queue stats as Prometheus gauges. It has no authentication of its own;
  wrap it with `login_required` or restrict it by network.
- Run `migrate` before rolling workers, not from the worker.
- `django_ox.actions.retry(result_id)` requeues a FAILED or LOST task for
  one more attempt. `django_ox.actions.discard(result_id)` closes a READY,
  FAILED or LOST task without running it. Neither touches a RUNNING task.
  With `django.contrib.admin` installed, the task table appears in the admin
  with the same two actions.
- A particular running task cannot be interrupted on demand; `TASK_TIMEOUT`
  bounds every attempt. Tasks live on the default database.
- In tests use `django.tasks.backends.immediate.ImmediateBackend` or
  `django.tasks.backends.dummy.DummyBackend` for `TASKS`.
- Batches, unique tasks and rate limiting are in [Oxpull Pro](pro.md), a
  paid add-on that is not on sale yet. `django_ox.stats` and `ox_health`
  are in django-ox.

## How to verify it works

Define a task in any installed app:

```python
# myapp/tasks.py
from django.tasks import task  # Django 6.0+; on 5.2: from django_tasks import task


@task
def add(a, b):
    return a + b
```

Enqueue one from `python manage.py shell`:

```python
>>> from myapp.tasks import add
>>> result = add.enqueue(1, 2)
>>> result.status
TaskResultStatus.READY
```

Start `python manage.py ox_worker` in another terminal. The worker logs
`Worker <id> starting: queues=['default'] concurrency=1 poll=1.0s schedules=0`
to stderr, then `Task id=<id> path=myapp.tasks.add succeeded in <n>ms`.
With `DEBUG = True`, Django's own `Task id=... state=RUNNING` and
`state=SUCCESSFUL` lines appear between them.

Back in the shell:

```python
>>> result.refresh()
>>> result.status
TaskResultStatus.SUCCESSFUL
>>> result.return_value
3
```

`python manage.py ox_health --worker-timeout 60` now exits 0 and reports a
recent `last_claim_age`. Stop the worker with Ctrl-C; it drains and exits 0.

## Links

- [llms.txt](llms.txt): the facts above with links, in the llms.txt shape.
- [llms-full.txt](llms-full.txt): the whole site in one file.
- [Configuration](configuration.md), [Production](production.md),
  [Monitoring](monitoring.md), [Common patterns](patterns.md).
- Context7: `/oxpull/django-ox`. Source:
  [github.com/oxpull/django-ox](https://github.com/oxpull/django-ox).
