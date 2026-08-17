# django-ox benchmarks

Reproducible comparison of two database-backed backends for Django's Tasks
API on identical workloads:

1. **django-ox** (this package, installed editable from the repo)
2. **django-tasks-db 0.12.0** (the incumbent ORM backend by Jake Howard,
   installed from PyPI), driven per its own README: `db_worker` management
   command, `django_tasks_db.DatabaseBackend` in `TASKS`.

The point of this harness is credibility, not marketing. It reports every
run, keeps both backends at defaults except where a flag is required to run
at all, and states its limitations plainly. If django-tasks-db wins a
metric, the results file says so.

## What is measured

All task bodies are the same no-op function (`return None`). The only
difference between the two workload modules is the decorator import:
django-ox tasks use Django core's `django.tasks.task`, django-tasks-db
tasks use the `django_tasks` package's `task` (that is how each backend is
meant to be driven; django-tasks-db predates and does not use the Django
6.0 core module). This means the comparison is between the two *stacks*
(framework + backend) rather than the backend classes in isolation. Both
stacks present the same public API.

| Metric | Definition |
| --- | --- |
| Enqueue throughput | Wall time for 2,000 sequential `noop.enqueue()` calls from a single producer process in autocommit mode. Reported as tasks/sec. |
| Enqueue latency in `transaction.atomic()` | 500 iterations; each opens its own `transaction.atomic()` block and times only the `enqueue()` call inside it (COMMIT excluded). Reported as p50/p95 milliseconds (nearest-rank on the sample). |
| End-to-end completion | 2,000 no-op tasks pre-loaded as READY. Clock starts immediately before the worker process(es) are spawned and stops when the database shows 2,000 SUCCESSFUL rows (polled every 50 ms over a separate connection). Includes worker process startup and Django initialization, identically for both backends. |

Each metric runs **3 times per backend** and all three runs are reported.
No best-of, no discarded runs. If a run errors or times out it appears in
the results as an error. Backends are interleaved (run 1 ox, run 1
tasksdb, run 2 ox, ...) so slow system drift cannot systematically favor
whichever ran last.

### Concurrency in the end-to-end metric

- django-ox: one worker process, `--concurrency 1` and `--concurrency 4`
  (its concurrency is a thread pool inside one process).
- django-tasks-db: its `db_worker` command is single-threaded and has **no
  concurrency option**, so "concurrency 4" is **4 worker processes**. This
  is its documented scaling model, but it is not the same thing as 4
  threads in one process: 4 processes get 4 CPUs' worth of Python and 4
  separate DB connections, while ox's threads share one interpreter (and
  its GIL). Read the concurrency-4 rows with that asymmetry in mind; it is
  the fairest mapping the two designs allow.

### Diagnostic row (ox only, clearly non-default)

The defaults-only end-to-end rows are the comparison. In addition, the
harness runs one extra ox configuration per run: concurrency 1 with
`--interval 0.1` instead of the default 1.0. The worker is designed to
wake on task completion whenever tasks are in flight, so `--interval`
should only govern how often an idle worker checks for new work; the
diagnostic row proves that property empirically on every run by matching
the default-interval cell. It is recorded under a separate
`e2e_diagnostic` key, labeled non-default, and is never presented as ox's
headline number.

### Warmup

Before each timed enqueue measurement, the producer performs 20 untimed
enqueues and then deletes those rows. This is identical for both backends
and exists to keep one-time lazy imports (model loading, connection setup)
out of the timed window for *both* sides equally. Every measurement runs in
a fresh Python process, so neither backend ever benefits from a process
that the other warmed up.

### Deviations from defaults (all of them)

- django-tasks-db workers run with `--no-startup-delay`, which disables a
  random sleep of up to 1 s at startup (a thundering-herd nicety the
  package exposes as a flag). Disabling it can only improve
  django-tasks-db's numbers.
- Both settings modules set `DEBUG = False` (also avoids `db_worker`'s
  auto-reload default, which is keyed to `DEBUG`).
- Console logging is capped at WARNING for both, so per-task INFO logging
  I/O is excluded for both.
- Everything else is at package defaults: poll interval 1 s on both
  workers, ox's `MAX_ATTEMPTS`/`LOCK_TIMEOUT`/backoff defaults, tasks-db's
  default queue and worker id.

## Hardware and software

Recorded automatically into the raw results JSON at run time
(`collect_environment()` in `bench.py`). The machine used for the published
results:

- Apple M1 Max, 10 logical cores (8 performance + 2 efficiency), 64 GiB RAM
  (from `sysctl -n machdep.cpu.brand_string`, `sysctl hw.memsize`)
- macOS 26.5.2
- Python 3.12.13, Django 6.0.8, psycopg 3.3.4 (binary)
- PostgreSQL 16 (official `postgres:16` image) in Docker Desktop,
  port-forwarded to localhost:54330, container `ox-bench`, default
  PostgreSQL configuration

## Reproducing

Prerequisites: Docker Desktop and a Python 3.12+ virtualenv with:

```
pip install -e ..            # django-ox, from the package root
pip install django-tasks-db psycopg[binary]
```

Then, from this `benchmarks/` directory:

```
python bench.py
```

The script starts Docker Desktop if needed, creates the `ox-bench`
container and the two databases (`bench_ox`, `bench_tasksdb`), runs
migrations for both backends, then runs the full 3-run matrix. Progress
prints as it goes; raw numbers checkpoint continuously to
`results-raw-<date>.json`, and per-process logs land in `logs/`.

Cleanup afterwards (the script leaves the container up so runs can be
repeated cheaply):

```
docker --context desktop-linux rm -f ox-bench
osascript -e 'quit app "Docker"'
```

The published `results-<date>.md` is derived from the raw JSON; the JSON
is the source of truth and ships alongside it.

## Limitations

Read these before quoting any number.

- **Single machine, single run day.** One Mac, one OS state, no controlled
  thermal or background-load environment. Numbers describe relative
  behavior on this hardware; absolute performance elsewhere will differ.
- **Localhost database.** PostgreSQL runs in Docker on the same machine
  with sub-millisecond round trips. Real deployments have network latency
  between app and database, which would compress the relative differences
  in per-call metrics and change end-to-end numbers materially.
- **Docker Desktop on macOS.** The database runs in a Linux VM with
  virtualized I/O and port forwarding. This is not a production database
  host; both backends face the same handicap.
- **Small N.** 2,000 tasks and 500 latency samples are enough to separate
  the backends here but not to characterize tail behavior (p99+) or
  sustained load. Queue-depth effects beyond 2,000 rows are not measured.
- **No-op task bodies.** Real tasks do work; with realistic task bodies the
  per-task framework overhead measured here shrinks as a fraction of total
  runtime. This benchmark isolates framework overhead deliberately.
- **Thread pool vs process model at concurrency 4.** See above; the two
  backends scale by different mechanisms and the concurrency-4 comparison
  maps them as fairly as their designs allow, which is not perfectly.
- **End-to-end timer includes startup.** Roughly half a second of Django
  boot per worker process is inside the timed window for both backends (4x
  for tasksdb's 4-process configuration, which is a real cost of a
  process-per-worker model, but worth knowing when reading the numbers).
- **Whole stacks compared.** ox rides Django 6.0 core
  `django.tasks`; tasks-db rides the external `django_tasks` package. Any
  overhead difference between those frameworks is included in the totals.
- **Single producer.** Enqueue metrics use one process on one connection.
  Concurrent-producer contention is not measured.

## Soak and chaos harness (soak.py)

`soak.py` is a separate harness that tests reliability rather than speed:
sustained mixed load from several producer and worker processes over tens
of minutes, repeated SIGKILL of workers mid-task with restarts, and a
forced crash-restart of a worker holding claimed tasks. Every task
execution writes phase rows with a per-execution nonce to a `soak_ledger`
side table, so at-least-once vs exactly-once behavior is measured from
side effects and asserted from the database afterwards. It runs against
the same PostgreSQL 16 container the test suite uses (`ox-pg`, port 54329;
see CONTRIBUTING.md for the docker run command) with its own `soak_ox`
database and the `soaksite/` settings module. Methodology, parameters, and results:
[SOAK-2026-08-16.md](SOAK-2026-08-16.md), raw data in
`soak-results-raw-<date>.json`.

## Files

- `bench.py`: orchestrator plus per-measurement subprocess roles. The
  orchestrator never imports Django; each measurement is a fresh process.
- `benchsite/`: minimal settings and task modules for each backend.
- `results-raw-<date>.json`: every number the harness produced, including
  all 500 individual latency samples per run.
- `results-<date>.md`: the human-written results document.
- `soak.py` / `soaksite/`: soak and chaos harness (see above).
- `SOAK-<date>.md` / `soak-results-raw-<date>.json`: its results.
- `logs/`: stdout/stderr of every producer and worker process.
