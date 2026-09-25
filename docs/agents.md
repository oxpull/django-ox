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

`manage.py check` also runs the django-ox system checks. Schedule declaration
and timeout errors covered by `django_ox.E002` to `E005` and `E010` fail here,
before anything deploys. The checks do not establish database acceptance of
schedule arguments; alert on `schedule_dispatch_error` and
`schedule_dispatch_failed` at runtime.
Review any `django_ox.W003` warning before deploying too.

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
- SIGTERM and SIGINT request a drain, followed by exit 0. A main loop
  hung in a database call cannot begin draining until that call returns.
  A second signal forces exit 130; a process that cannot act on signals
  needs SIGKILL.
- `enqueue()` is one INSERT on the database the router sends `OxTask` to,
  `default` unless you wrote a router. Two things make the commit joint: a
  `transaction.atomic()` opened on that database, because a bare `atomic()`
  opens on `default`; and the caller's own rows written there too. With both,
  the task and those rows commit or roll back together, and the task is
  visible to workers only after commit. Do not add `transaction.on_commit()`
  around it. Rows written on another connection give two transactions, not
  one.
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
- The worker schedules lease renewal every `LOCK_TIMEOUT / 3` seconds; task length is not bounded by `LOCK_TIMEOUT` while renewals succeed.
  Renewal needs a database connection: if the worker cannot refresh its lease for `LOCK_TIMEOUT`, the reaper can hand the task to another worker even while it is alive.
- With Django's PostgreSQL pool in 1.4.0, provide at least `concurrency + 1` pooled connections per worker process; add a spare pooled connection if fallback must work under full load.
  Budget up to `max_size + 1` server connections per process (`max_size + 2` with task timeouts), including any added spare; account for all processes, aliases, other clients, and reserved slots.
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
- Recurring tasks go in `OPTIONS["SCHEDULES"]`, with either `cron` or `every`,
  not both. Every worker dispatches them; there is no scheduler process to
  start. A tick that passes while every worker is down is enqueued once on
  recovery, and older missed ticks are skipped.
- Schedules can live in the database instead, editable in the admin. Set
  `OPTIONS["SCHEDULE_SOURCE"]` to `"django_ox.stored.DatabaseScheduleSource"`.
  A row names a key registered with `@schedulable`, never an import path, so
  admin access does not become permission to run arbitrary code. The decorator
  takes effect only when its module is imported, and django-ox imports each
  installed app's `tasks` module. Write rows
  with `django_ox.stored.create_schedule` and `update_schedule`, not
  `objects.create()`: `save()` runs no validation and leaves the activation
  boundary stale. Retiming a stored schedule reschedules it from the moment of
  the change, and re-enabling a paused one does not replay what it missed.
- `ox_prune --older-than 7d` deletes finished rows; FAILED rows stay unless
  `--include-failed`. READY, WAITING and RUNNING rows are never deleted.
- For per-container loop-liveness, pair `ox_worker --heartbeat-file PATH`
  with `ox_health --heartbeat-file PATH --processes N`, matching the
  worker's process count. Use an absolute path in an existing, writable
  directory private to the container. Never share it between replicas.
- A passing file probe means the expected controlling loops have advanced
  recently. It does not establish task progress, successful claims or a
  live renewal thread. All task slots can be stuck while the loop stays
  fresh; task timeouts are separate.
- File mode makes no database calls and runs no system or migration checks.
  Project Django startup must also avoid database access. Allow startup
  time before the first file update and replacement backoff while slot
  files are missing. Size freshness above the poll interval plus expected
  loop latency and scheduling margin.
- Heartbeats are updated at each poll and drain pass head and before every
  claim attempt. A busy pass can make up to `--concurrency` claims, but
  updates occur between claims. Budget for reap and dispatch work plus one
  claim, the poll interval and scheduling margin, not concurrency
  multiplied by claim latency. With Django's PostgreSQL connection pool,
  a refused database can hold a pass for the pool's `timeout`, 30 seconds
  by default. Include that wait in the freshness budget.
- Do not use bare `ox_health` or `--worker-timeout` for per-container
  liveness restarts. Database checks report dependencies; queue thresholds
  belong in fleet alerting. django-ox sets no overall timeout on a poll
  pass. A configured `OPTIONS["connect_timeout"]` bounds connects. On
  PostgreSQL and MySQL, statements and claims have no timeout by default.
  On SQLite, the busy timeout, 5 seconds by default, bounds each lock wait,
  not the whole poll pass. A hung database can make every file stale and
  trigger fleet-wide restarts. Omit that automatic restart trigger if the
  tradeoff is unacceptable. Plain Docker and Compose mark health without
  restarting merely on an unhealthy status.
- `path("ox/", include("django_ox.urls"))` mounts `GET /ox/metrics`, the
  queue stats as Prometheus gauges. It has no authentication of its own;
  wrap it with `login_required` or restrict it by network.
- Run `migrate` before rolling workers, not from the worker.
- `django_ox.actions.retry(result_id)` requeues a FAILED or LOST task for
  one more attempt. `django_ox.actions.discard(result_id)` closes a READY,
  WAITING, FAILED or LOST task without running it. Neither touches a RUNNING task.
  With `django.contrib.admin` installed, the task table appears in the admin
  with the same two actions.
- A particular running task cannot be interrupted on demand; `TASK_TIMEOUT`
  bounds every attempt. Every table lives on the database your router sends
  `OxTask` to.
- In tests use the framework's own backends for `TASKS`:
  `django.tasks.backends.immediate.ImmediateBackend` or
  `django.tasks.backends.dummy.DummyBackend` on Django 6.0+, and the same
  paths under `django_tasks.backends.` on Django 5.2 LTS.
- Batches, unique tasks, rate limiting and workflows are in
  [Oxpull Pro](pro.md), a paid add-on. `django_ox.stats` and `ox_health`
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
