# Wagtail background tasks, step by step

Wagtail runs search-index updates, reference-index updates, file deletion, and other tasks inside the request by default. django-ox moves that work into a worker process, with the queue in your existing database and no broker to run.

django-ox adds automatic retries with backoff, recovery after worker death, recurring schedules, and health checks. See [Why django-ox](why-django-ox.md) for the backend comparison.

## Choose versions

Use Python 3.12 or later. PostgreSQL is recommended in production.

Tested combinations:

- Wagtail 7.3.1 / Django 5.2.17.
- Wagtail 7.3.4 / Django 6.0.8.
- Wagtail 7.4.3 / Django 5.2.17.
- Wagtail 7.4.3 / Django 6.0.8.
- Wagtail 7.4.3 / Django 6.1.1.
- Wagtail 8.0 / Django 5.2.17.
- Wagtail 8.0 / Django 6.1.1.

Wagtail 7.3.x declares Django 6.0 support. Wagtail 7.3.1-7.3.3 also resolve on Django 6.0, but those combinations were not run.

On Django 6.1, pip permits Wagtail 7.3.x, but Wagtail 7.3.4 fails in Wagtail's own search with `OperationalError: unable to use function MATCH`, both with and without django-ox.

Do not use Wagtail 7.3.0 with this setup on Django 5.2 or 6.0. Its dependency constraints resolve `django-tasks` 0.9.0. On Django 6.0, that version crashes during app loading (`django.setup()`) once `TASKS` selects a backend built on core Django's `Task`. Every `manage.py` command and the web process fail, not just migrations.

Upgrade Wagtail before installing django-ox into a Wagtail 7.3.0 environment.

## Install

On Django 5.2 LTS, install the extra for the `django-tasks` backport:

```
pip install "django-ox[backport]"
```

On Django 6.0 and later:

```
pip install django-ox
```

Wagtail imports tasks from the `django-tasks` backport even on Django 6.0 and later. If an existing environment pins it to 0.9.0, update that pin:

```
pip install "django-tasks>=0.10"
```

Refresh the lockfile if the project uses one, then check dependencies:

```
pip check
```

## Configure Django

Add django-ox to the installed apps and configure the `default` backend:

```
INSTALLED_APPS += ["django_ox"]

TASKS = {
    "default": {
        "BACKEND": "django_ox.backend.OxBackend",
    },
}
```

If `TASKS` already exists, update its `default` entry rather than replacing other backends. Wagtail's bare `@task()` binds to backend alias `default` and queue `default`.

Run migrations:

```
python manage.py migrate
```

Restart the web processes. Web and worker processes must use the same settings and database.

Wagtail now queues work in the database. The work waits until a worker runs.

## Start the worker

Leave the worker stopped and publish a page in Wagtail.

In a `wagtail start` project, Django admin is at `/django-admin/`. Open [the admin page](monitoring.md#the-admin-page) at `/django-admin/django_ox/oxtask/`, labelled "Ox tasks", to inspect the queued work.

Start a worker in another terminal, using the project's environment:

```
python manage.py ox_worker
```

Reload the admin list to see statuses such as "Successful", then search for the published page. Search and reference-index updates can lag behind saves until the worker processes them.

Without `--queues`, django-ox processes all queues configured for the backend. If restricting queues, include `default`:

```
python manage.py ox_worker --queues default
```

Keep the worker running alongside the web processes. In production, run it as a supervised service; see [Running under systemd](production.md#running-under-systemd) or [Running in containers](production.md#running-in-containers).

## What moves out of the request

On Wagtail 7.4 and 8.0, these operations can run through the worker:

- Search-index updates when indexed models are saved.
- Reference-index updates when tracked models are saved.
- File deletion after images, renditions, or documents are deleted, once the transaction commits.
- Image focal-point detection when `WAGTAILIMAGES_FEATURE_DETECTION_ENABLED` is enabled.
- Frontend-cache purges on publication or unpublication when `wagtail.contrib.frontend_cache` is installed.

Removing deleted objects from the search and reference indexes still happens inline. A task backend does not move every part of publishing into the background.

Verified through the worker: search-index updates, reference-index updates, document file deletion, and frontend-cache purges. Source-reviewed only: image focal-point detection and image/rendition file deletion.

## Handle delays and failures

If tasks are not draining, check that the worker uses the correct database and processes the `default` queue. Use [the admin page](monitoring.md#the-admin-page) to inspect attempts and tracebacks.

### Worker recovery

If a worker dies, its unfinished tasks return to the queue if they have attempts left. If the expired claim was the last attempt, the task becomes `LOST` instead.

Attempts count claims. The default `MAX_ATTEMPTS` is 3.

Every running worker runs a reaper every `min(30, max(LOCK_TIMEOUT/2, 1))` seconds. With the default 300-second lease and 30-second reaper interval, recovery can take about 330 seconds after the last lease renewal. Recovery requires at least one running worker.

`LOCK_TIMEOUT` and `MAX_ATTEMPTS` belong inside `TASKS["default"]["OPTIONS"]`, not at the top level of Django settings. See [Backend options](configuration.md#options).

### Timeouts

Timeouts are opt-in: `TASK_TIMEOUT` defaults to `None`, meaning no limit. `TASK_TIMEOUTS` sets per-queue limits. Both belong in the backend's [options](configuration.md#options).

Wagtail's frontend-cache purge makes outbound HTTP calls, so consider an appropriate task timeout rather than assuming one is already active.

### Objects deleted before their tasks run

Wagtail's search-index and reference-index task bodies raise `DoesNotExist` if the object is deleted before they run. This behavior applies on any task backend.

With django-ox's default three attempts, those tasks end `FAILED`. The file-deletion task still runs. Check the traceback and whether the object still exists before investigating a worker outage.

## Monitor and prune

For a per-container probe, check that the database answers:

```
python manage.py ox_health
```

`--max-backlog` and `--max-age` measure the whole queue. Put them in fleet-level alerting, from cron or a monitoring agent. Do not put them in a per-worker probe: a shared backlog would fail every worker's probe and restart healthy workers without shifting the backlog.

See [Health checks: ox_health](monitoring.md#health-checks-ox_health).

Task rows accumulate: one page publish writes about six rows. Run pruning from cron or a timer:

```
python manage.py ox_prune --older-than 7d
```

Seven days is the default retention threshold for finished rows. `FAILED` and `LOST` rows remain unless `--include-failed` is supplied. See [ox_prune](configuration.md#ox_prune).

## Continuous integration

The CI job pins Wagtail 7.4.3 / Django 5.2.17 and Wagtail 8.0 / Django 6.1.1, and runs on SQLite. It checks publishing, search before and after worker execution, task success, and clean worker shutdown on pull requests and pushes to `main`.
