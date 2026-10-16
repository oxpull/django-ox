# Choosing a task backend

This page compares django-ox with the task queues a Django team is most likely
to shortlist: django-tasks-db, huey, Celery, django-q2, dramatiq and
procrastinate, and, in a second table, with the other database-backed
`django.tasks` backends: steady-queue, dj-queue and django-database-task.
Every cell about another project comes from that project's own
documentation, source or issue tracker, with the link in the footnotes and the
date it was read. Where a project's pages do not say, the cell says so rather
than guessing.

The comparison is about fit. It does not rank the tools. A team that already
runs RabbitMQ and needs to stop running tasks has a different answer from one
that wants one fewer service. The last section says where django-ox is
not the right fit.

## The table

| | django-ox | django-tasks-db | huey | Celery | django-q2 | dramatiq | procrastinate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `django.tasks` backend | Yes, native | Yes, native [^tdb-readme] | Yes. `huey.contrib.djhuey` ships "a backend for the django.tasks framework (Django 6.0 and newer, or older Djangos using the django-tasks backport)" [^huey-contrib] | No. Issue #10062 closed 2026-02-03 [^celery-10062] | Pull request #315 open since 2026-02-04, last updated 2026-05-15 [^q2-315] | Not documented [^dq-guide] | Not documented; has its own Django integration [^proc-index] |
| Broker needed | None. The queue is a table in your database | None. Django ORM [^tdb-readme] | Redis, SQLite, PostgreSQL, file or memory storage [^huey-guide] | RabbitMQ, Redis or SQS (stable); Zookeeper, Kafka, Pub/Sub (experimental). SQL databases are result backends only [^celery-brokers]. Issue #5149, PostgreSQL as a broker, open since 2018-10-25 with 95 upvotes [^celery-5149] | Redis (default), IronMQ, SQS, MongoDB or Django ORM [^q2-brokers] | RabbitMQ or Redis [^dq-guide] | None. PostgreSQL is the queue [^proc-index] |
| Transactional enqueue | Yes. Enqueue is one INSERT on your default database; a task written inside `atomic()` commits or rolls back with the rows beside it | Not documented [^tdb-readme] | Not documented [^huey-guide] | No. Django's own docs name a background task as the case for `on_commit()` [^dj-oncommit] | Not documented [^q2-brokers] | No. Enqueue is a broker send [^dq-guide] | Not documented [^proc-django] |
| Retries and backoff | Exponential backoff by default; per-task `max_attempts` and `backoff` on Django 6.1 or Django 5.2 with django-tasks 0.12+. Backend defaults: `MAX_ATTEMPTS`, `BACKOFF_INITIAL`, `BACKOFF_MAX`. Every attempt's traceback kept. See [Per-task policy](configuration.md#per-task-policy) | No retry option in the README or the worker flags [^tdb-readme] [^tdb-worker] | `retries`, `retry_delay`, `retry_backoff` [^huey-guide] | `autoretry_for`, `retry_backoff`, `retry_backoff_max` (default 600 s), `retry_jitter` [^celery-tasks] | `max_attempts` (default 0, meaning unlimited) and a `retry` interval; backoff not documented [^q2-configure] | Exponential backoff; `max_retries` default 20, `min_backoff` 15 s, `max_backoff` 7 days [^dq-guide] | Retry strategy per task [^proc-index] |
| Recurring schedules | Cron or a fixed interval in settings, or rows created and edited in the Django admin, limited to the tasks the code exposes; every worker dispatches, no scheduler process. See [Schedules in the database](stored-schedules.md) | None. Issue #259 open since 2026-08-10 [^tasks-259] | `periodic_task(crontab(...))` [^huey-guide] | `celery beat`, a separate process; "ensure only a single scheduler is running" [^celery-beat]. With `django-celery-beat` the schedules are rows that "can be managed from the Django Admin interface", and the field naming the task has no list to choose from [^beat-admin] | `Schedule` model, editable in admin; cron via croniter [^q2-schedules] | None built in; APScheduler recommended [^dq-cookbook] | `@app.periodic` [^proc-index] |
| Unique tasks / deduplicated enqueue | `@unique` or `UNIQUE_TASKS` returns the existing `TaskResult` while a match is READY or RUNNING; not an execution-exclusivity guarantee. All four features below require the optional `oxpull` app and `OxpullBackend` on Django 6.0+ | Issue #164 closed as a duplicate of discussion #143; maintainer: "handled by your business logic" [^tdb-164] | huey's own `lock_task()` prevents concurrent runs, not duplicate enqueues [^huey-stable] | No general built-in enqueue deduplication. The cookbook uses a custom execution lock; `worker_deduplicate_successful_tasks` handles successful redeliveries, and SQS FIFO has a broker-specific deduplication mechanism [^celery-cookbook] [^celery-dedup-d3] [^celery-sqs-d3] | Not documented [^q2-tasks-d3] | Not documented [^dq-ref] | `queueing_lock` permits one job in `todo` per chosen lock string, but multiple jobs in `doing`; a duplicate defer raises `AlreadyEnqueued` [^proc-qlock] |
| Batch completion callback | `on_complete` is enqueued once every member's row is settled or gone, whatever the outcome; `on_success` requires all to succeed. Callbacks run at least once. Requires a once-a-minute reconciler schedule | "Task Groups" discussion #58, open since 2024-06-20, with 0 comments [^tasks-58] | huey's own `chord()` runs after members finish or fail permanently, passing failures as `huey.Error`; a member lost to abrupt consumer shutdown prevents the callback [^huey-stable] | `chord` runs its callback only if every member succeeds; a failed member produces `ChordError`, and an errback attached to the callback runs instead. Requires a result backend other than RPC and member results must not be ignored [^celery-canvas] | No group completion hook documented; groups label results for polling, while `hook` is per task [^q2-group] [^q2-tasks-d3] | `group.add_completion_callback()` requires optional `GroupCallbacks` middleware; the completion barrier is signalled only on success, so the callback requires every member to succeed [^dq-ref] | A contributor: "there is nothing out of the box"; issue #1219 closed in favour of #1198 [^proc-1219] |
| Rate limits shared across workers | Fixed start windows coordinated through one database row per limit name; requires `OxpullWorker`. Bounded overshoot is possible. Throttled tasks stay READY without spending an attempt; this is not a concurrency cap | Not documented in the README, worker flags or Django Tasks API [^tdb-readme-d3] [^tdb-worker-d3] [^dj-ref-tasks] | huey's own `rate_limit()` uses a fixed-window storage counter keyed by huey name and limit name; workers using the same shared storage use that counter. Checked inside the running task, with throttled calls rescheduled [^huey-stable] [^huey-ratelimit-src] | `Task.rate_limit` is per worker instance, not global; the docs advise restricting to a given queue for global limiting [^celery-ratelimit] | Not documented; pull request #350 proposes a per-cluster throttle and remains open [^q2-configure-d3] [^q2-350] | `WindowRateLimiter` and `BucketRateLimiter` use Redis or Memcached for shared limits, called from actor code [^dq-ref] [^dq-cookbook-d3] | Not documented; issue #1289 open since 2025-01-14 [^proc-1289] |
| Dependency workflows | Provisional. `depends_on` forms a DAG; a node runs only after every upstream succeeds. Requires `WORKFLOWS: True`, `OxpullWorker` and a reconciler schedule | Not documented in the Tasks API; discussion #58 proposes task groups and follow-up work [^dj-ref-tasks] [^tasks-58] | huey's own pipelines with `.then()` run tasks sequentially; a failure skips the remaining success pipeline [^huey-stable] | `chain` / `link` call the next task after success; `chord` provides fan-in [^celery-canvas] | Chains are not success-gated: later tasks are also enqueued after a failure. `save_task()` has no success check before enqueueing the next chain link. Issue #327, open since 2026-05-05 [^q2-327-d3] [^q2-monitor-d3] | `pipeline` passes results to the next actor; the next step is enqueued only after success [^dq-ref] | No built-in workflows; issue #1198 open since 2024-09-18. Execution locks order jobs whether the preceding job succeeds or not [^proc-1198] [^proc-locks-d3] |
| Priorities | -100 to 100 | Yes [^tdb-backend] | Yes; on Redis needs 5.0+ and `PriorityRedisHuey` [^huey-guide] | 0 to 255 on RabbitMQ and Redis [^celery-calling] | Not documented [^q2-configure] | Per actor; lower number runs first [^dq-guide] | Yes [^proc-index] |
| Time limit on a running task | Configured through backend `TASK_TIMEOUT`, per-queue `TASK_TIMEOUTS`, or a task's declared `timeout` on Django 6.1 or Django 5.2 with django-tasks 0.12+. `TaskTimeout` is raised inside the task at the deadline, followed by a worker recycle if the thread does not stop. The recycle is the whole enforcement on an interpreter with no facility for raising an exception in another thread, and for a thread a coverage tool or debugger is watching. See [Production](production.md#task-timeouts) | Not documented in the README or the worker flags [^tdb-readme] [^tdb-worker] | `timeout` per task or per call; "a `TaskTimeout` is raised and returned to the caller via the result handle" [^huey-guide] | `soft_time_limit` raises `SoftTimeLimitExceeded` inside the task; `time_limit` terminates the process running it, which is replaced. "Time limits don't currently work on platforms that don't support the `SIGUSR1` signal" [^celery-workers] | `timeout`, default `None`: "the number of seconds a worker is allowed to spend on a task before it's terminated" [^q2-configure] | `time_limit` per actor, default 10 minutes, raises `TimeLimitExceeded`. "Time limits are best-effort. They cannot cancel system calls or any function that doesn't currently hold the GIL under CPython" [^dq-guide] | Not documented on the page checked [^proc-index] |
| Worker dies mid-task | Lease renewed every `LOCK_TIMEOUT / 3`; a task whose lease goes stale for `LOCK_TIMEOUT` is put back to READY, or marked LOST when attempts are spent. See [Production](production.md#the-reaper) | Issue #5, open since 2024-06-11: the task 'remains marked as "PROCESSING", and thus is never picked up for re-processing nor marked as completed / failed' [^tdb-5] | "tasks that are mid-execution are lost and will not be retried automatically" [^huey-guide] | `acks_late` re-delivers; the worker still acknowledges "if the child process executing the task is terminated" [^celery-tasks] | Issue #327, open since 2026-05-05: worker death not reported to the monitor, `MAX_ATTEMPTS` ignored [^q2-327] | Not stated on the pages checked [^dq-guide] [^dq-cookbook] | Heartbeat every 10 s; jobs stay in `doing` until a `retry_stalled_jobs` periodic task you define picks them up [^proc-stalled] |
| Health and metrics | `ox_health` command, `django_ox.stats`, a Prometheus endpoint (`/ox/metrics`), structured log events, admin retry and discard | Issue #44, container healthchecks, open since 2026-06-08 [^tdb-44] | Signals [^huey-guide] | Flower, a separate process, with Prometheus integration [^flower] | `qmonitor`, `qinfo`, `Stat` [^q2-monitor] | Prometheus middleware; not in the default middleware list [^dq-prom] | Statistics via events [^proc-index] |
| Databases | PostgreSQL, MySQL 8 and SQLite, all run in CI; MariaDB 10.6+ takes the same claim path as MySQL and is not in CI | Any Django database [^tdb-readme] | Redis, SQLite, PostgreSQL, file, memory [^huey-guide] | Broker, not a database [^celery-brokers] | Any Django database through the ORM broker [^q2-brokers] | Broker, not a database [^dq-guide] | PostgreSQL 13+ [^proc-index] |
| Licence | BSD 3-Clause | BSD 3-Clause [^tdb-repo] | MIT [^huey-repo] | BSD 3-Clause [^celery-license] | MIT [^q2-pyproject] | LGPL 3.0 [^dq-repo] | MIT [^proc-repo] |

Async tasks: django-ox sets `supports_async_task`, so `async def` tasks
enqueue and run. Celery's most-upvoted open issue is #6552, "Support async
function", open since 2020-12-19 with 98 upvotes [^celery-6552].

## The database-backed `django.tasks` backends

steady-queue, dj-queue and django-database-task also keep the queue in the
database and implement the `django.tasks` backend API. The same rules apply:
every cell about another project comes from its README, its PyPI page or its
own issue tracker, with the link and the date in the footnotes, and a cell
says "not stated" when those pages do not say.

| | django-ox | steady-queue | dj-queue | django-database-task |
| --- | --- | --- | --- | --- |
| Latest release on PyPI | See the [changelog](changelog.md) | 0.2.1, 2026-09-06 [^sq-pypi] | 0.14.2, 2026-09-26 [^djq-pypi] | 0.6.0, 2026-09-30 [^ddt-pypi] |
| `django.tasks` backend | Yes, native | Yes, native [^sq-readme] | Yes, native [^djq-readme] | Yes, native [^ddt-readme] |
| Django versions | 5.2 LTS with the `django-tasks` backport, 6.0 and 6.1 | 6.0 and later, below 7 (`django>=6.0,<7`) [^sq-pypi] | 6.0 and 6.1 (`django>=6.0.0,<6.2`) [^djq-pypi] | 6.0 and later (`Django>=6.0`) [^ddt-pypi] |
| Databases | PostgreSQL, MySQL 8 and SQLite, all run in CI; MariaDB 10.6+ takes the same claim path as MySQL and is not in CI | MySQL, PostgreSQL or SQLite; `FOR UPDATE SKIP LOCKED` on MySQL 8+ and PostgreSQL 9.5+ [^sq-readme] | PostgreSQL first-class, with optional `LISTEN/NOTIFY`; MySQL and MariaDB supported; SQLite "supported with limits", polling only and no `SKIP LOCKED` [^djq-readme] | PostgreSQL 14+, MySQL 8.0.11+, MariaDB 10.6+, SQLite (no row-level locking; "development or single-worker deployments"), Oracle 19c+ ("not tested with this package") [^ddt-readme] |
| Transactional enqueue | Yes. Enqueue is one INSERT on your default database; a task written inside `atomic()` commits or rolls back with the rows beside it | The README says a shared database gives transactional integrity and recommends against relying on it: a separate database for the queue, and `transaction.on_commit` for tasks that depend on data [^sq-readme] | "`enqueue()` writes immediately and returns a real persisted task result ID"; for a task that depends on rows in the current transaction the README provides `enqueue_on_commit()` or `transaction.on_commit()` [^djq-readme] | Not stated as a feature. The PostgreSQL broker section says `pg_notify()` runs inside the same transaction as the INSERT and is delivered on commit [^ddt-readme] |
| Retries and backoff | Exponential backoff by default; per-task `max_attempts` and `backoff` on Django 6.1 or Django 5.2 with django-tasks 0.12+. Backend defaults: `MAX_ATTEMPTS`, `BACKOFF_INITIAL`, `BACKOFF_MAX`. Every attempt's traceback kept. See [Per-task policy](configuration.md#per-task-policy) | Not stated in the README or the configuration page; failed tasks are retried from the Django admin [^sq-readme] [^sq-config] | None built in: "`dj_queue` does not add an automatic retry policy or backoff engine". Failed jobs are retried from the admin or with `retry_failed_job()` [^djq-readme] | No automatic retry stated. A FAILED task is re-run from the admin action "Retry failed tasks" or with `run_task_by_id(..., allow_retry=True)` [^ddt-readme] |
| Recurring schedules | Cron or a fixed interval in settings, or rows edited in the Django admin; every worker dispatches, no scheduler process | Cron-style, with the `@recurring` decorator in code; a scheduler process under the supervisor [^sq-readme] | Static recurring tasks in `OPTIONS["recurring"]` and dynamic recurring tasks managed at runtime; a scheduler under the supervisor [^djq-readme] | None built in. The worker is run from cron, a systemd timer, JP1, Hinemos or Cloud Scheduler [^ddt-readme] |
| Unique tasks / deduplicated enqueue | `@unique` or `UNIQUE_TASKS` returns the existing `TaskResult` while a match is READY or RUNNING; not an execution-exclusivity guarantee. All four features below require the optional `oxpull` app and `OxpullBackend` on Django 6.0+ | Issue #24, open since 2026-02-10; owner: "For now ... I don't think this will get added to the package", but is "open to creating a `contrib` subpackage that could include this" [^sq-24] | `@concurrency(..., on_conflict="discard")` provides singleton-style work through a concurrency semaphore with a TTL; the discarded enqueue's return value is not stated [^djq-readme-d3] | Not stated; `SELECT FOR UPDATE SKIP LOCKED` prevents duplicate claims, not duplicate enqueues [^ddt-readme-d3] |
| Batch completion callback | `on_complete` is enqueued once every member's row is settled or gone, whatever the outcome; `on_success` requires all to succeed. Callbacks run at least once. Requires a once-a-minute reconciler schedule | Not stated [^sq-readme-d3] | Not stated [^djq-readme-d3] | Not stated [^ddt-readme-d3] |
| Rate limits shared across workers | Fixed start windows coordinated through one database row per limit name; requires `OxpullWorker`. Bounded overshoot is possible. Throttled tasks stay READY without spending an attempt; this is not a concurrency cap | Not stated; the README's concurrency controls limit simultaneous execution, not start rate [^sq-readme-d3] | Not stated; shared semaphores provide concurrency limits, not rate limits [^djq-readme-d3] | Not provided by the package; the README points to Google Cloud Tasks per-queue rate settings for `CloudTasksDatabaseBackend` [^ddt-readme-d3] |
| Dependency workflows | Provisional. `depends_on` forms a DAG; a node runs only after every upstream succeeds. Requires `WORKFLOWS: True`, `OxpullWorker` and a reconciler schedule | Not stated [^sq-readme-d3] | Not stated [^djq-readme-d3] | Not stated [^ddt-readme-d3] |
| Priorities | -100 to 100 | -100 to 100 [^sq-readme] | `priority` on the task call; higher claimed first within a queue [^djq-readme] | -100 to 100 [^ddt-readme] |
| Concurrency controls | No per-task concurrency limit. [Rate limiting](features/rate-limiting.md) caps how often a task starts, not how many run at once | "Limit how many instances of a specific task can run simultaneously" [^sq-readme] | `@concurrency` decorator with database-backed limits; semaphores shown in the admin dashboard [^djq-readme] | Not stated [^ddt-readme] |
| Worker dies mid-task | Lease renewed every `LOCK_TIMEOUT / 3`; a task whose lease goes stale for `LOCK_TIMEOUT` is put back to READY, or marked LOST when attempts are spent | Marked failed with `ProcessPrunedError` (heartbeat expired) or `ProcessExitError` (killed), to be inspected [^sq-readme] | Kept "as failed work that operators can inspect and retry": `ProcessExitError`, `ProcessPrunedError`, `ProcessMissingError` [^djq-readme] | Requeued by `requeue_stale_database_tasks --older-than`, a separate command run from cron or a systemd timer; `--max-attempts` (default 3) marks the task FAILED instead [^ddt-readme] |
| Health and metrics | `ox_health` command, `django_ox.stats`, a Prometheus endpoint (`/ox/metrics`), structured log events, admin retry and discard | Django admin: pause and resume queues, inspect tasks, retry or discard failed ones; operational signals from `steady_queue.signals` [^sq-readme] | Admin dashboard at `/admin/dj_queue/dashboard/`; `dj_queue_health` command; `/dj_queue/metrics` in Prometheus text format with the `prometheus` extra [^djq-readme] | Django admin; structured log fields; opt-in worker exit codes for job schedulers [^ddt-readme] |
| Async tasks and results | `async def` tasks run; `get_result()` supported | "Async task enqueueing is not supported"; "Result fetching is not supported" [^sq-readme] | `get_result()` documented; async task functions not stated [^djq-readme] | "Supports async task functions"; `get_result()` documented [^ddt-readme] |
| Licence | BSD 3-Clause | MIT [^sq-pypi] | MIT [^djq-pypi] | MIT [^ddt-pypi] |

## When not to use django-ox

- **You need to stop one chosen task while it runs.** django-ox supports
  configured attempt deadlines through `TASK_TIMEOUT`, per-queue
  `TASK_TIMEOUTS`, or a task's declared `timeout` on Django 6.1 or
  Django 5.2 with django-tasks 0.12+. Attempts are unbounded by default.
  Operator actions discard queued tasks and retry failed ones.
  For on-demand interruption, Celery can revoke and terminate a running
  task, from Flower or the control API [^flower].
- **The queue must live on a different database from your models.** Every django-ox table
  lives on the database `OxTask` routes to, and a queue on a different
  database from your models gives up the transactional enqueue.
- **Throughput beyond what one database comfortably serves.** The
  [benchmarks](benchmarks.md) page gives measured numbers with the method.
  If your workload is above them, a broker-based queue is the right tool, and
  the cost is the second datastore.
- **Celery-style chains, groups and chords.** django-ox does not implement
  these APIs. It provides [batches](features/batches.md) for grouped work
  and provisional [workflows](features/workflows.md) for dependency DAGs.
  Check their semantics before replacing an existing composition.
- **CPU-bound tasks in one process.** Worker concurrency is a thread pool.
  Run `ox_worker --processes N --concurrency 1` for N interpreters, or pick
  a queue with a process pool.

## Maintenance

Each footnote carries the date its row was last checked. If a cell is out
of date, open an issue with the link that shows it.

[^tdb-readme]: https://github.com/RealOrangeOne/django-tasks-db README, checked 2026-08-23.
[^tdb-worker]: https://github.com/RealOrangeOne/django-tasks-db/blob/master/django_tasks_db/management/commands/db_worker.py, `add_arguments`, checked 2026-08-23.
[^tdb-backend]: https://github.com/RealOrangeOne/django-tasks-db/blob/master/django_tasks_db/backend.py, `supports_priority = True`, checked 2026-08-23.
[^tdb-5]: https://github.com/RealOrangeOne/django-tasks-db/issues/5, open, checked 2026-08-23.
[^tdb-44]: https://github.com/RealOrangeOne/django-tasks-db/issues/44, open, checked 2026-08-23.
[^tdb-repo]: https://github.com/RealOrangeOne/django-tasks-db, licence field, checked 2026-08-23.
[^tasks-259]: https://github.com/RealOrangeOne/django-tasks/issues/259, open, checked 2026-08-23.
[^tdb-164]: https://github.com/RealOrangeOne/django-tasks/issues/164, closed as a duplicate of discussion #143; maintainer assigns deduplication to business logic, checked 2026-10-11.
[^tasks-58]: https://github.com/RealOrangeOne/django-tasks/discussions/58, "Task Groups", open since 2024-06-20 with 0 comments, checked 2026-10-11.
[^tdb-readme-d3]: https://github.com/RealOrangeOne/django-tasks-db, README at `8f79c7f`; no rate-limit option documented, checked 2026-10-11.
[^tdb-worker-d3]: https://github.com/RealOrangeOne/django-tasks-db/blob/master/django_tasks_db/management/commands/db_worker.py, `add_arguments`; no rate-limit flag documented, checked 2026-10-11.
[^huey-guide]: https://huey.readthedocs.io/en/latest/guide.html, checked 2026-08-23.
[^huey-contrib]: https://huey.readthedocs.io/en/latest/contrib.html, checked 2026-09-19.
[^huey-repo]: https://github.com/coleifer/huey, licence field, checked 2026-08-23.
[^huey-stable]: https://huey.readthedocs.io/en/stable/guide.html, huey 3.4.0; huey's own task locks, chords, fixed-window rate limits and pipelines, checked 2026-10-11.
[^huey-ratelimit-src]: https://github.com/coleifer/huey/blob/3.4.0/huey/api.py, `class RateLimit`; counter key includes huey name and limit name and uses `storage.incr()`, checked 2026-10-11.
[^celery-tasks]: https://docs.celeryq.dev/en/stable/userguide/tasks.html, checked 2026-08-23.
[^celery-brokers]: https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html, checked 2026-08-23.
[^celery-workers]: https://docs.celeryq.dev/en/stable/userguide/workers.html, Time Limits, checked 2026-08-23.
[^celery-beat]: https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html, checked 2026-08-23.
[^beat-admin]: django-celery-beat 2.9.0. The documentation says periodic tasks "can be managed from the Django Admin interface", and `django_celery_beat/models.py` declares `PeriodicTask.task` as `models.CharField(max_length=200)` with no `choices` and no validators. <https://django-celery-beat.readthedocs.io/en/latest/>, checked 2026-09-12.
[^celery-calling]: https://docs.celeryq.dev/en/stable/userguide/calling.html, checked 2026-08-23.
[^celery-5149]: https://github.com/celery/celery/issues/5149, open, checked 2026-08-23.
[^celery-6552]: https://github.com/celery/celery/issues/6552, open, checked 2026-08-23.
[^celery-10062]: https://github.com/celery/celery/issues/10062, closed, checked 2026-08-23.
[^celery-license]: https://github.com/celery/celery/blob/main/LICENSE, checked 2026-08-23.
[^celery-cookbook]: https://docs.celeryq.dev/en/stable/tutorials/task-cookbook.html, Celery 5.6.3; custom cache lock for execution exclusivity, checked 2026-10-11.
[^celery-dedup-d3]: https://docs.celeryq.dev/en/stable/userguide/configuration.html, Celery 5.6.3; `worker_deduplicate_successful_tasks` applies to successful redeliveries with the same task identifier and late acknowledgment, checked 2026-10-11.
[^celery-sqs-d3]: https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html, Celery 5.6.3; SQS FIFO message properties including `MessageDeduplicationId`, checked 2026-10-11.
[^celery-canvas]: https://docs.celeryq.dev/en/stable/userguide/canvas.html, Celery 5.6.3; chord result-backend requirements and failure behavior, including an errback attached to the chord callback running instead on failure; chains and links, checked 2026-10-11.
[^celery-ratelimit]: https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.rate_limit, Celery 5.6.3; per-worker rate limits and queue restriction advice, checked 2026-10-11.
[^flower]: https://flower.readthedocs.io/en/latest/features.html, checked 2026-08-23.
[^dj-oncommit]: https://docs.djangoproject.com/en/6.0/topics/db/transactions/#performing-actions-after-commit, checked 2026-08-23.
[^dj-ref-tasks]: https://docs.djangoproject.com/en/6.0/ref/tasks/, Django 6.0 Tasks API; no rate-limit or dependency option, checked 2026-10-11.
[^q2-brokers]: https://django-q2.readthedocs.io/en/master/brokers.html, checked 2026-08-23.
[^q2-configure]: https://django-q2.readthedocs.io/en/master/configure.html, checked 2026-08-23.
[^q2-schedules]: https://django-q2.readthedocs.io/en/master/schedules.html, checked 2026-08-23.
[^q2-monitor]: https://django-q2.readthedocs.io/en/master/monitor.html, checked 2026-08-23.
[^q2-315]: https://github.com/django-q2/django-q2/pull/315, open, checked 2026-08-23.
[^q2-327]: https://github.com/django-q2/django-q2/issues/327, open, checked 2026-08-23.
[^q2-pyproject]: https://github.com/django-q2/django-q2/blob/master/pyproject.toml, checked 2026-08-23.
[^q2-tasks-d3]: https://django-q2.readthedocs.io/en/master/tasks.html, Django Q2 1.11.1; `async_task()` options and per-task `hook`, checked 2026-10-11.
[^q2-group]: https://django-q2.readthedocs.io/en/master/group.html, result grouping and polling; no group completion hook documented, checked 2026-10-11.
[^q2-configure-d3]: https://django-q2.readthedocs.io/en/master/configure.html, Django Q2 1.11.1; no shared rate-limit option documented, checked 2026-10-11.
[^q2-350]: https://github.com/django-q2/django-q2/pull/350, proposed per-cluster throttle, open since 2026-09-16, checked 2026-10-11.
[^q2-327-d3]: https://github.com/django-q2/django-q2/issues/327, open since 2026-05-05; Bug 2 reports chains continuing after failure, checked 2026-10-11.
[^q2-monitor-d3]: https://github.com/django-q2/django-q2/blob/v1.11.1/django_q/monitor.py, `save_task()` has no success check before enqueueing the next chain link, checked 2026-10-11.
[^dq-guide]: https://dramatiq.io/guide.html, checked 2026-08-23.
[^dq-cookbook]: https://dramatiq.io/cookbook.html, checked 2026-08-23.
[^dq-prom]: https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/__init__.py, `default_middleware`, and `middleware/prometheus.py`, checked 2026-08-23.
[^dq-repo]: https://github.com/Bogdanp/dramatiq, licence field, checked 2026-08-23.
[^dq-ref]: https://dramatiq.io/reference.html, Dramatiq 2.2.0; composition and rate-limiter APIs; success gating also checked in `dramatiq/middleware/group_callbacks.py` and `dramatiq/middleware/pipelines.py` at `8534ffa`, checked 2026-10-11.
[^dq-cookbook-d3]: https://dramatiq.io/cookbook.html, rate limiters used inside actor code with a shared backend, checked 2026-10-11.
[^proc-index]: https://procrastinate.readthedocs.io/en/stable/, checked 2026-08-23.
[^proc-django]: https://procrastinate.readthedocs.io/en/stable/howto/django/basic_usage.html, checked 2026-08-23.
[^proc-stalled]: https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html, checked 2026-08-23.
[^proc-repo]: https://github.com/procrastinate-org/procrastinate, licence field, checked 2026-08-23.
[^proc-qlock]: https://procrastinate.readthedocs.io/en/stable/howto/advanced/queueing_locks.html, `queueing_lock`, `todo` versus `doing`, and `AlreadyEnqueued`, checked 2026-10-11.
[^proc-1219]: https://github.com/procrastinate-org/procrastinate/issues/1219, batch and workflow request closed in favour of #1198; a contributor, not a maintainer, says "there is nothing out of the box", checked 2026-10-11.
[^proc-1289]: https://github.com/procrastinate-org/procrastinate/issues/1289, rate-limit request, open since 2025-01-14, checked 2026-10-11.
[^proc-1198]: https://github.com/procrastinate-org/procrastinate/issues/1198, dependency workflow request, open since 2024-09-18, checked 2026-10-11.
[^proc-locks-d3]: https://procrastinate.readthedocs.io/en/stable/howto/advanced/locks.html, execution locks order jobs whether the preceding job succeeds or not, checked 2026-10-11.
[^sq-readme]: https://github.com/knifecake/steady-queue README, checked 2026-09-19.
[^sq-config]: https://steady-queue.readthedocs.io/en/latest/configuration.html, checked 2026-09-19.
[^sq-pypi]: https://pypi.org/project/steady-queue/ 0.2.1, uploaded 2026-09-06; `Requires-Dist: django<7,>=6.0`; licence MIT. Checked 2026-10-11.
[^sq-24]: https://github.com/knifecake/steady-queue/issues/24, enqueue uniqueness request, open since 2026-02-10; owner: "For now ... I don't think this will get added to the package", but is "open to creating a `contrib` subpackage that could include this", checked 2026-10-11.
[^sq-readme-d3]: https://github.com/knifecake/steady-queue, README at `db46170`; concurrency controls and documented feature scope, checked 2026-10-11.
[^djq-readme]: https://github.com/coriocactus/dj_queue README, checked 2026-09-19.
[^djq-pypi]: https://pypi.org/project/dj-queue/ 0.14.2, uploaded 2026-09-26; `Requires-Dist: django<6.2,>=6.0.0`; licence MIT. Checked 2026-10-11.
[^djq-readme-d3]: https://github.com/coriocactus/dj_queue, README at `086ce2a`; singleton-style concurrency controls and documented feature scope, checked 2026-10-11.
[^ddt-readme]: https://github.com/tokibito/django-database-task README, checked 2026-09-19.
[^ddt-pypi]: https://pypi.org/project/django-database-task/ 0.6.0, uploaded 2026-09-30; `Requires-Dist: Django>=6.0`; licence MIT. Checked 2026-10-11.
[^ddt-readme-d3]: https://github.com/tokibito/django-database-task, README at `9f4db93`; claim exclusivity, Google Cloud Tasks rate settings and documented feature scope, checked 2026-10-11.
