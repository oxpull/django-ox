# Benchmarks

## Queue drain and worker recovery

### 2,000 of 2,000 tasks finished

Every django-ox kill-test trial finished with zero tasks stuck. django-tasks-db left 13 to 19 tasks stuck RUNNING per trial.

<small>Five trials, each with 20 worker kills and replacements, with LOCK_TIMEOUT set to 15 s.</small>

Execution is at-least-once. The trials recorded 7 repeated effects across 10,000 tasks. Write tasks that can run again safely.

### Up to 20% faster queue drain than django-tasks-db

Worker counts match in each row. Rates are tasks per second.

| Queued tasks | Workers per backend | django-ox, tasks/s | django-tasks-db, tasks/s | django-ox faster |
|---|---:|---:|---:|---:|
| 2,000 | 1 | 118.5 | 103.9 | **14.0%** |
| 2,000 | 4 | 426.3 | 387.1 | **10.1%** |
| 20,000* | 1 | 115.2 | 97.4 | **18.2%** |
| 20,000* | 4 | 461.8 | 401.6 | **15.0%** |

In each row, every django-ox run outperformed every django-tasks-db run. Results apply to these workloads, not every application.

### Enqueue 10,000 tasks in 0.71 s

`enqueue_many()` inserted all 10,000 rows in one transaction, including COMMIT. Run times ranged from 0.69 to 0.72 s.

### 20 of 20 tasks retried successfully

Each task raised an exception on its first attempt. django-ox retried them automatically, and all succeeded on attempt two, every run. django-tasks-db marked all 20 FAILED after the first exception.

### How we measured

Oxpull, 2026-09-19. django-ox 1.3.0 vs django-tasks-db 0.13.0.

PostgreSQL 16 ran locally in Docker on an Apple M1 Max. Task bodies were no-ops. Each cell used five interleaved runs per backend.

Package defaults applied except for the kill test, with LOCK_TIMEOUT set to 15 s.

<small>*The wallpaper process rendered during kill trials 2 and 3 only; kill counts were unaffected. The four drain cells (2,000 and 20,000 tasks) ran interleaved in one window; the 100,000-task run was one pair, not five.</small>

[**Download both raw JSON files**](https://github.com/oxpull/django-ox/tree/main/benchmarks)

[**Get started**](background-tasks.md)
