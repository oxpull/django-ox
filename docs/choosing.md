# Choosing a task backend

This page compares django-ox with the task queues a Django team is most likely
to shortlist: django-tasks-db, huey, Celery, django-q2, dramatiq and
procrastinate. Every cell about another project comes from that project's own
documentation, source or issue tracker, with the link in the footnotes and the
date it was read. Where a project's pages do not say, the cell says so rather
than guessing.

The comparison is about fit, not ranking. A team that already runs RabbitMQ and
needs to stop running tasks has a different answer from a team that wants one
fewer service. The last section says where django-ox is not the right fit.

## The table

| | django-ox | django-tasks-db | huey | Celery | django-q2 | dramatiq | procrastinate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `django.tasks` backend (Django 6) | Yes, native | Yes, native [^tdb-readme] | No. Issue #870 closed 2025-11-14 [^huey-870] | No. Issue #10062 closed 2026-02-03 [^celery-10062] | Pull request #315 open since 2026-02-04, last updated 2026-05-15 [^q2-315] | Not documented [^dq-guide] | Not documented; has its own Django integration [^proc-index] |
| Broker needed | None. The queue is a table in your database | None. Django ORM [^tdb-readme] | Redis, SQLite, PostgreSQL, file or memory storage [^huey-guide] | RabbitMQ, Redis or SQS (stable); Zookeeper, Kafka, Pub/Sub (experimental). SQL databases are result backends only [^celery-brokers]. Issue #5149, PostgreSQL as a broker, open since 2018-10-25 with 95 upvotes [^celery-5149] | Redis (default), IronMQ, SQS, MongoDB or Django ORM [^q2-brokers] | RabbitMQ or Redis [^dq-guide] | None. PostgreSQL is the queue [^proc-index] |
| Transactional enqueue | Yes. Enqueue is an INSERT on your connection; it commits or rolls back with your data | Not documented [^tdb-readme] | Not documented [^huey-guide] | No. Django's own docs name a background task as the case for `on_commit()` [^dj-oncommit] | Not documented [^q2-brokers] | No. Enqueue is a broker send [^dq-guide] | Not documented [^proc-django] |
| Retries and backoff | Exponential backoff, `MAX_ATTEMPTS`, `BACKOFF_INITIAL`, `BACKOFF_MAX`; every attempt's traceback kept | No retry option in the README or the worker flags [^tdb-readme] [^tdb-worker] | `retries`, `retry_delay`, `retry_backoff` [^huey-guide] | `autoretry_for`, `retry_backoff`, `retry_backoff_max` (default 600 s), `retry_jitter` [^celery-tasks] | `max_attempts` (default 0, meaning unlimited) and a `retry` interval; backoff not documented [^q2-configure] | Exponential backoff; `max_retries` default 20, `min_backoff` 15 s, `max_backoff` 7 days [^dq-guide] | Retry strategy per task [^proc-index] |
| Recurring schedules | Cron in settings; every worker dispatches, no scheduler process | None. Issue #259 open since 2026-08-10 [^tasks-259] | `periodic_task(crontab(...))` [^huey-guide] | `celery beat`, a separate process; "ensure only a single scheduler is running" [^celery-beat] | `Schedule` model, editable in admin; cron via croniter [^q2-schedules] | None built in; APScheduler recommended [^dq-cookbook] | `@app.periodic` [^proc-index] |
| Priorities | -100 to 100 | Yes [^tdb-backend] | Yes; on Redis needs 5.0+ and `PriorityRedisHuey` [^huey-guide] | 0 to 255 on RabbitMQ and Redis [^celery-calling] | Not documented [^q2-configure] | Per actor; lower number runs first [^dq-guide] | Yes [^proc-index] |
| Time limit on a running task | `TASK_TIMEOUT`, per queue in `TASK_TIMEOUTS`: `TaskTimeout` raised inside the task at the deadline, and a worker recycle when the thread does not stop. See [Production](production.md#task-timeouts) | Not documented in the README or the worker flags [^tdb-readme] [^tdb-worker] | `timeout` per task or per call; "a `TaskTimeout` is raised and returned to the caller via the result handle" [^huey-guide] | `soft_time_limit` raises `SoftTimeLimitExceeded` inside the task; `time_limit` terminates the process running it, which is replaced. "Time limits don't currently work on platforms that don't support the `SIGUSR1` signal" [^celery-workers] | `timeout`, default `None`: "the number of seconds a worker is allowed to spend on a task before it's terminated" [^q2-configure] | `time_limit` per actor, default 10 minutes, raises `TimeLimitExceeded`. "Time limits are best-effort. They cannot cancel system calls or any function that doesn't currently hold the GIL under CPython" [^dq-guide] | Not documented on the page checked [^proc-index] |
| Worker dies mid-task | Lease renewed every `LOCK_TIMEOUT / 3`; a task whose lease goes stale for `LOCK_TIMEOUT` is put back to READY, or marked LOST when attempts are spent. See [Production](production.md#the-reaper) | Issue #5, open since 2024-06-11: the task 'remains marked as "PROCESSING", and thus is never picked up for re-processing nor marked as completed / failed' [^tdb-5] | "tasks that are mid-execution are lost and will not be retried automatically" [^huey-guide] | `acks_late` re-delivers; the worker still acknowledges "if the child process executing the task is terminated" [^celery-tasks] | Issue #327, open since 2026-05-05: worker death not reported to the monitor, `MAX_ATTEMPTS` ignored [^q2-327] | Not stated on the pages checked [^dq-guide] [^dq-cookbook] | Heartbeat every 10 s; jobs stay in `doing` until a `retry_stalled_jobs` periodic task you define picks them up [^proc-stalled] |
| Health and metrics | `ox_health` command, `django_ox.stats`, structured log events | Issue #44, container healthchecks, open since 2026-06-08 [^tdb-44] | Signals [^huey-guide] | Flower, a separate process, with Prometheus integration [^flower] | `qmonitor`, `qinfo`, `Stat` [^q2-monitor] | Prometheus middleware; not in the default middleware list [^dq-prom] | Statistics via events [^proc-index] |
| Databases | PostgreSQL, SQLite and MySQL 8 tested in CI; MariaDB 10.6+ untested | Any Django database [^tdb-readme] | Redis, SQLite, PostgreSQL, file, memory [^huey-guide] | Broker, not a database [^celery-brokers] | Any Django database through the ORM broker [^q2-brokers] | Broker, not a database [^dq-guide] | PostgreSQL 13+ [^proc-index] |
| Licence | BSD 3-Clause | BSD 3-Clause [^tdb-repo] | MIT [^huey-repo] | BSD 3-Clause [^celery-license] | MIT [^q2-pyproject] | LGPL 3.0 [^dq-repo] | MIT [^proc-repo] |

Async tasks: django-ox sets `supports_async_task`, so `async def` tasks
enqueue and run. Celery's most-upvoted open issue is #6552, "Support async
function", open since 2020-12-19 with 98 upvotes [^celery-6552].

## When not to use django-ox

- **You need to stop one chosen task while it runs.** django-ox bounds every
  attempt with `TASK_TIMEOUT`, discards a queued task and retries a failed
  one, but has no call that interrupts a particular running task on demand.
  Celery can revoke and terminate a running task, from Flower or the control
  API [^flower].
- **The queue must live on a different database from your models.** Tasks are
  stored on the default database. Multi-database routing is outside the
  current scope, and a separate queue database would also give up the
  transactional enqueue.
- **Throughput beyond what one database comfortably serves.** The
  [benchmarks](benchmarks.md) page gives measured numbers with the method.
  If your workload is above them, a broker-based queue is the right tool, and
  the cost is the second datastore.
- **Chains, groups and chords.** Not in django-ox. Batches are in
  [Oxpull Pro](pro.md), a paid add-on that is not on sale yet; chains and
  workflows are on the Pro roadmap, undated.
- **CPU-bound tasks in one process.** Worker concurrency is a thread pool.
  Run `ox_worker --processes N --concurrency 1` for N interpreters, or pick
  a queue with a process pool.

## Maintenance

Rows are re-checked each release. If a cell is out of date, open an issue with
the link that shows it, and it will be corrected in the next release.

[^tdb-readme]: https://github.com/RealOrangeOne/django-tasks-db README, checked 2026-08-23.
[^tdb-worker]: https://github.com/RealOrangeOne/django-tasks-db/blob/master/django_tasks_db/management/commands/db_worker.py, `add_arguments`, checked 2026-08-23.
[^tdb-backend]: https://github.com/RealOrangeOne/django-tasks-db/blob/master/django_tasks_db/backend.py, `supports_priority = True`, checked 2026-08-23.
[^tdb-5]: https://github.com/RealOrangeOne/django-tasks-db/issues/5, open, checked 2026-08-23.
[^tdb-44]: https://github.com/RealOrangeOne/django-tasks-db/issues/44, open, checked 2026-08-23.
[^tdb-repo]: https://github.com/RealOrangeOne/django-tasks-db, licence field, checked 2026-08-23.
[^tasks-259]: https://github.com/RealOrangeOne/django-tasks/issues/259, open, checked 2026-08-23.
[^huey-guide]: https://huey.readthedocs.io/en/latest/guide.html, checked 2026-08-23.
[^huey-870]: https://github.com/coleifer/huey/issues/870, closed, checked 2026-08-23.
[^huey-repo]: https://github.com/coleifer/huey, licence field, checked 2026-08-23.
[^celery-tasks]: https://docs.celeryq.dev/en/stable/userguide/tasks.html, checked 2026-08-23.
[^celery-brokers]: https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html, checked 2026-08-23.
[^celery-workers]: https://docs.celeryq.dev/en/stable/userguide/workers.html, Time Limits, checked 2026-08-23.
[^celery-beat]: https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html, checked 2026-08-23.
[^celery-calling]: https://docs.celeryq.dev/en/stable/userguide/calling.html, checked 2026-08-23.
[^celery-5149]: https://github.com/celery/celery/issues/5149, open, checked 2026-08-23.
[^celery-6552]: https://github.com/celery/celery/issues/6552, open, checked 2026-08-23.
[^celery-10062]: https://github.com/celery/celery/issues/10062, closed, checked 2026-08-23.
[^celery-license]: https://github.com/celery/celery/blob/main/LICENSE, checked 2026-08-23.
[^flower]: https://flower.readthedocs.io/en/latest/features.html, checked 2026-08-23.
[^dj-oncommit]: https://docs.djangoproject.com/en/6.0/topics/db/transactions/#performing-actions-after-commit, checked 2026-08-23.
[^q2-brokers]: https://django-q2.readthedocs.io/en/master/brokers.html, checked 2026-08-23.
[^q2-configure]: https://django-q2.readthedocs.io/en/master/configure.html, checked 2026-08-23.
[^q2-schedules]: https://django-q2.readthedocs.io/en/master/schedules.html, checked 2026-08-23.
[^q2-monitor]: https://django-q2.readthedocs.io/en/master/monitor.html, checked 2026-08-23.
[^q2-315]: https://github.com/django-q2/django-q2/pull/315, open, checked 2026-08-23.
[^q2-327]: https://github.com/django-q2/django-q2/issues/327, open, checked 2026-08-23.
[^q2-pyproject]: https://github.com/django-q2/django-q2/blob/master/pyproject.toml, checked 2026-08-23.
[^dq-guide]: https://dramatiq.io/guide.html, checked 2026-08-23.
[^dq-cookbook]: https://dramatiq.io/cookbook.html, checked 2026-08-23.
[^dq-prom]: https://github.com/Bogdanp/dramatiq/blob/master/dramatiq/middleware/__init__.py, `default_middleware`, and `middleware/prometheus.py`, checked 2026-08-23.
[^dq-repo]: https://github.com/Bogdanp/dramatiq, licence field, checked 2026-08-23.
[^proc-index]: https://procrastinate.readthedocs.io/en/stable/, checked 2026-08-23.
[^proc-django]: https://procrastinate.readthedocs.io/en/stable/howto/django/basic_usage.html, checked 2026-08-23.
[^proc-stalled]: https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html, checked 2026-08-23.
[^proc-repo]: https://github.com/procrastinate-org/procrastinate, licence field, checked 2026-08-23.
