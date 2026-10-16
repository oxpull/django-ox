# Benchmarks

## 1.4 to 1.7× the throughput of django-tasks-db with retained connections

django-ox is a database-backed worker for Django's Tasks framework.

### More tasks per second at matched connection settings

With pooled or persistent connections, django-ox delivered 1.4 to 1.7× django-tasks-db's throughput across 4, 8 and 16 workers.

Every task finished in these runs, with zero duplicate starts or completions.

With persistent connections, worker CPU per task was 1.0 to 1.4 ms for django-ox versus 1.5 to 2.1 ms for django-tasks-db.

<small>PostgreSQL 16; no-op tasks; django-ox 1.9.0 vs django-tasks-db 0.13.0; medians of five runs.</small>

<small>Rates are medians of five interleaved runs; ratios are medians of per-round ratios. The test machine had background load; see How we measured.</small>

Retain worker connections to speed up both backends: `CONN_MAX_AGE = 600`, or `OPTIONS["pool"]` with `CONN_MAX_AGE = 0`. Django 6.0 rejects a pool with nonzero `CONN_MAX_AGE`; see [Retaining worker connections](production.md#retaining-worker-connections).

Both backends ran the same settings in each row. Rates are tasks per second.

| Workload | Setting | django-ox, tasks/s | django-tasks-db, tasks/s | Ratio |
|---|---|---:|---:|---:|
| 20,000 tasks, 4 workers | Persistent | 1,771 | 1,211 | **1.46×** |
| 20,000 tasks, 4 workers | Persistent, health checks | 1,590 | 1,123 | **1.42×** |
| 20,000 tasks, 4 workers | Pooled | 1,718 | 1,216 | **1.41×** |
| 20,000 tasks, 4 workers | Pooled, health checks | 1,561 | 1,127 | **1.40×** |
| 20,000 tasks, 8 workers | Persistent | 2,668 | 1,735 | **1.53×** |
| 20,000 tasks, 8 workers | Persistent, health checks | 2,324 | 1,594 | **1.45×** |
| 20,000 tasks, 8 workers | Pooled | 2,569 | 1,724 | **1.51×** |
| 20,000 tasks, 8 workers | Pooled, health checks | 2,269 | 1,556 | **1.47×** |
| 20,000 tasks, 16 workers | Persistent | 3,065 | 1,959 | **1.54×** |
| 20,000 tasks, 16 workers | Persistent, health checks | 2,693 | 1,787 | **1.50×** |
| 20,000 tasks, 16 workers | Pooled | 3,011 | 1,938 | **1.55×** |
| 20,000 tasks, 16 workers | Pooled, health checks | 2,625 | 1,786 | **1.46×** |
| 100,000 tasks, 16 workers | Persistent | 3,358 | 1,989 | **1.69×** |
| 100,000 tasks, 16 workers | Persistent, health checks | 2,884 | 1,816 | **1.59×** |
| 100,000 tasks, 16 workers | Pooled | 3,289 | 1,952 | **1.68×** |
| 100,000 tasks, 16 workers | Pooled, health checks | 2,833 | 1,815 | **1.57×** |

<small>Persistent: `CONN_MAX_AGE = 600`. Persistent, health checks: the same with `CONN_HEALTH_CHECKS = True`. Pooled: `OPTIONS["pool"]` with `CONN_MAX_AGE = 0`. Pooled, health checks: the same with `CONN_HEALTH_CHECKS = True`.</small>

Draining 100,000 tasks at 16 workers took about 30 s for django-ox (29.7 to 30.1 s over five runs) and about 50 s for django-tasks-db (49.3 to 51.2 s) with persistent connections; with a pool 30.2 to 30.5 s vs 49.9 to 51.6 s.

### Default connection settings

| Workload | Setting | django-ox, tasks/s | django-tasks-db, tasks/s | Ratio |
|---|---|---:|---:|---:|
| 20,000 tasks, 4 workers | Defaults | 430 | 373 | **1.1 to 1.2×** |

<small>`CONN_MAX_AGE = 0` and no pool. Spread is the standard deviation of the five rates divided by their mean.</small>

The spread between runs is 9% on both, so treat this as about 1.1×.

## Worker recovery, enqueue and retries (django-ox 1.3.0)

### 2,000 of 2,000 tasks finished

Every django-ox 1.3.0 kill-test trial finished with zero tasks stuck. django-tasks-db left 13 to 19 tasks stuck RUNNING per trial.

<small>Five trials, each with 20 worker kills and replacements, with LOCK_TIMEOUT set to 15 s. The wallpaper process rendered during kill trials 2 and 3 only; kill counts were unaffected.</small>

Execution is at-least-once. The trials recorded 7 repeated effects across 10,000 tasks. Write tasks that can run again safely.

### Enqueue 10,000 tasks in 0.71 s

In django-ox 1.3.0, `enqueue_many()` inserted all 10,000 rows in one transaction, including COMMIT. Run times ranged from 0.69 to 0.72 s.

### 20 of 20 tasks retried successfully

Each task raised an exception on its first attempt. django-ox 1.3.0 retried them automatically, and all succeeded on attempt two, every run. django-tasks-db marked all 20 FAILED after the first exception.

### How we measured

Matched-connection run: Oxpull, 2026-10-14. django-ox 1.9.0 vs django-tasks-db 0.13.0.

PostgreSQL 16 ran locally in Docker. Django 6.0.8, psycopg 3.3.6. Task bodies were no-ops with a 256-byte payload. Each cell used five interleaved runs per setting and backend, with order balanced across rounds. Both backends got identical Django database options; each package's own defaults applied otherwise.

The test machine had other load (one-minute load average 1.9 to 6.8 before a run), so absolute rates are lower than a quiet machine would give.

At one connection per task, the test machine runs out of ports at about 550 new connections a second, so we show no default-settings throughput above 4 workers.

Kill, enqueue and retry sections: Oxpull, 2026-09-19. django-ox 1.3.0 vs django-tasks-db 0.13.0. PostgreSQL 16 ran locally in Docker. Task bodies were no-ops. Each cell used five interleaved runs per backend. Package defaults applied except for the kill test, with LOCK_TIMEOUT set to 15 s.

[**Download the raw JSON files**](https://github.com/oxpull/django-ox/tree/main/benchmarks)

[**Get started**](background-tasks.md)
