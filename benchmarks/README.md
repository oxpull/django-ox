# django-ox benchmarks

Reproducible comparison of two database-backed backends for Django's Tasks
API on identical workloads:

1. **django-ox** (this package, installed editable from the repo)
2. **django-tasks-db 0.13.0** (the incumbent ORM backend,
   installed from PyPI), driven per its own README: `db_worker` management
   command, `django_tasks_db.DatabaseBackend` in `TASKS`.

The harness reports every run, keeps both backends at defaults except where a
flag is required to run at all, and states its scope plainly.

## What is measured

All task bodies are no-op functions (`return None`), and both workload
modules decorate them with Django core's `django.tasks.task`.
django-tasks-db 0.13.0 runs on the core framework when it is present
(Django 6.0+) and no longer depends on the `django_tasks` backport, so the
two arms share one framework and differ in the backend package under test:
its backend class, its schema and its worker command. Framework overhead is
in both totals equally; what the numbers separate is the two backends.
(Runs before 2026-09-19 compared against django-tasks-db 0.12.0, which
enqueued through the backport, so those were whole-stack comparisons.)

| Cell | Workload | Clock | Raw key |
| --- | --- | --- | --- |
| Enqueue throughput | 2,000 sequential `noop.enqueue()` calls from one producer process in autocommit mode. | First call start to last call return, commits included. Reported as tasks/sec. Published only if it passes the stability gate below. | `enqueue_throughput` |
| Enqueue latency in `transaction.atomic()` | 500 iterations; each opens its own `transaction.atomic()` block. | Only the `enqueue()` call inside the block (COMMIT excluded). Reported as p50/p95 milliseconds, the nearest-rank value of the sorted sample (rank `round(q * n + 0.5)`, counted from 1). | `enqueue_latency` |
| Backlog drain, 1v1 and 4v4 | N no-op tasks preloaded as READY, N in `--depth` (default 2,000 and 20,000). One worker process per arm (1v1) or four per arm (4v4). | Starts immediately before the worker process(es) are spawned; stops on the poll that sees N SUCCESSFUL rows. Includes worker process startup and Django initialisation for both backends. Excludes the preload. | `e2e` |
| Depth probe | One 1v1 pair at `--probe-depth` (100,000 for the published run), run once after the matrix. | Same clock as the drain cells. | `e2e_probe` |
| Bulk enqueue | 10,000 payloads, one outer `transaction.atomic()`. Three arms: ox `enqueue_many()`, an ox `enqueue()` loop as the control, a tasks-db `enqueue()` loop. | Before the block is entered to after it exits, so COMMIT is inside. Row count validated after. Each arm is published only if it passes the stability gate below. | `bulk_enqueue` |
| Exception retry (behaviour) | 20 tasks that raise on their first execution and return on any later one, one worker per arm. | Observed until every task is terminal or 60 s have passed. Counts, not a rate; not ranked. | `retry` |
| Control row (ox only) | The 1v1 drain at the first depth with `--interval 0.1` instead of the default 1.0. | Same clock as the drain cells. Diagnostic, not part of the comparison. | `e2e_diagnostic` |
| Legacy threads cell | One ox process with `--concurrency 4` against four tasks-db processes, at the first depth. Runs only with `--legacy-threads`. | Same clock as the drain cells. Not ranked; kept so older raw files can be read against the current harness. | `e2e_legacy_threads` |

Each cell runs `--runs` times per arm (default 5) and every run is reported.
If a run errors or times out it appears in the results as an error.

### Process topology in the drain cells

- django-ox runs `ox_worker --processes P --concurrency 1`. At `P = 1` the
  command is the worker itself (`ox_worker` starts its supervisor only above
  1), so the 1v1 argv is the plain single-process worker spelled out. At
  `P = 4` the process the harness spawns is a supervisor: it boots Django,
  opens no database connection, and starts four children in sequence, each
  `ox_worker --processes 1 --concurrency 1` in a fresh interpreter. The
  clock starts before the supervisor is spawned, so its own startup is inside
  django-ox's 4v4 window, and five interpreters boot on that side against
  four on the other. The entry records the supervisor's pid, the children's
  pids and argv (read after the clock stopped) and one exit code, the
  supervisor's, which by its contract is the worst of its children's.
- django-tasks-db runs P copies of `db_worker --no-startup-delay`; its worker
  has no concurrency option, and P processes is its documented scaling
  model. The entry records P pids and P exit codes.
- The cell that ranked one ox process with four threads against four
  tasks-db processes left the default matrix on 2026-09-19. Four
  interpreters against one is not a like-for-like comparison. It remains
  runnable behind `--legacy-threads`.

### Queue depth and the probe

Every drain cell runs at every depth in `--depth`, 1v1 and 4v4, so the
default matrix has four drain cells per run. `--probe-depth N` adds one 1v1
pair after the matrix, at that depth, after the same preload and ANALYZE,
recorded under `e2e_probe` with `probe: true`. It is a probe, not a cell:
one pair, no repetition. The rule for it is fixed before the run. The probe is
published only if each arm's drain rate at the probe depth is within 20% of
the same arm's median rate at the deepest cell, and the load averages
recorded on its entry show the machine stayed quiet. Otherwise the raw
entry stays in the file and the page says the probe was not published, and
why. The harness computes the rate part of the rule into the raw file's
`probe` block (per arm: the probe rate, the median 1v1 rate at the deepest
cell, their ratio, whether it lies within the band, and the probe entry's
load averages); the quiet part is read from those load averages.

### Preload, ANALYZE and the observer

- The drain rows are preloaded outside the clock through each backend's
  bulk path: `django_ox.bulk.enqueue_many` for django-ox (which builds each
  row with the same code its `enqueue()` uses), and for django-tasks-db,
  which has no bulk API, `DBTaskResult.objects.bulk_create` with the columns
  its `DatabaseBackend.enqueue()` writes, `run_after` set to the value its
  `pre_save` receiver would set (`bulk_create` sends no signals). Both write
  1,000 rows per INSERT inside one transaction. The method and the preload's
  own duration are on every entry under `preload`. `--preload loop` restores
  the `enqueue()` loop used before 2026-09-19.
- `ANALYZE` runs on the arm's task table after every preload and before
  the timed spawn, from the orchestrator's own connection; the producer
  cells run it after their warm-up rows are deleted and before their timed
  loop, on the empty table. It never runs before a load, and never inside a
  timed window. Each entry records `analyze_at`.
- The observer counts rows by status with one `GROUP BY` query per poll on a
  separate connection. The poll interval is `max(0.05, depth / 200000)`
  seconds (50 ms at 2,000, 100 ms at 20,000, 500 ms at 100,000), so the
  observer's scans do not grow with the depth; it bounds the resolution of
  the stop time and is recorded as `poll_interval`.

### Bulk enqueue

Three arms, each writing 10,000 rows of `item(n)` (a no-op with one integer
argument) inside one outer `transaction.atomic()`; the payload list is
built before the clock. The arms:

- `ox-many`: `django_ox.bulk.enqueue_many(item, calls)`. It writes with
  `bulk_create` in chunks of `INSERT_CHUNK_SIZE` (1,000) rows; inside the
  outer block its own `atomic()` is a savepoint; after the insert it sends
  `task_enqueued` once per row, inside the window. The chunk size is on the
  entry.
- `ox-loop`: `item.enqueue(n)` once per payload, the control for the arm
  above. Without it a bulk API would be compared against a different
  transaction boundary.
- `tasksdb-loop`: `item.enqueue(n)` once per payload. django-tasks-db has no
  bulk API; a loop inside one transaction is its batch workload, and the
  cell reports it as a measurement, not as a missing number.

The producer counts the rows after the block, the orchestrator counts them
again on its own connection, and the entry carries both counts and a
`validated` flag. The three arms rotate one place per run.

### Exception retry (behaviour row)

Twenty tasks, each carrying an integer id, one worker per arm at package
defaults. The task body inserts a row into a side table
(`bench_retry_ledger`, created by the harness with plain SQL in each arm's
database) keyed by that id and reads back how many executions the id now
has; the first execution raises `RuntimeError`, any later one returns. The
ledger, not a backend's attempt counter, decides, so the body is the same
code under both backends. The rows are written on the worker's autocommit
connection and both workers call the body outside a transaction of their
own, so the row survives the exception.

The cell is observed until no task is READY or RUNNING, or 60 s have passed,
whichever comes first. The entry records per task the terminal status, the
backend's own attempt count (django-ox: the `attempts` column; django-tasks-db:
the length of `worker_ids`, one entry per claim) and the ledger's execution
count, a timeline of the status counts, `all_terminal`, `observed_seconds`,
the worker's pid, exit code and log tail. django-ox retries with its default
backoff (`BACKOFF_INITIAL` 5 s, `MAX_ATTEMPTS` 3); django-tasks-db has no
automatic retry and records the task FAILED. The row reports what each arm
did with the same 20 tasks. It has no rate and is not ranked.

### Order and interleaving

Within each run the arms are interleaved cell by cell, and the arm that goes
first alternates run by run (AB, BA, AB, BA, AB); `--seed` picks which arm
opens run 1 (even: django-ox, odd: django-tasks-db). The bulk arms rotate
one place per run from the same seed. The whole schedule is computed before
anything runs and written to `parameters.schedule`; every entry records its
`position` and `first_arm`.

### Stability gate

The raw file carries a `stability` block, recomputed at every checkpoint.
Each row holds every value of one metric, both arms together, the ratio
`max / min` over them, the threshold (1.15), whether the row is gated, and
`withheld_by_stability_gate`. A gated row whose ratio exceeds the threshold
is marked withheld with the reason; its values stay in the file and are not
summarised on the page. Enqueue throughput is gated by default, over the ten
values of a five-run block: the single-producer autocommit number has swung
by more than that between runs on this machine, and a median over a swing
like that would be a coin toss presented as a result. The three bulk-enqueue
arms are gated by default too, each over its own five values, because a
single-producer `enqueue()` loop has swung the same way. `--gate` widens the
gated set and `--stability-threshold` moves the bound; both are recorded. A run that fails the gate is not rerun to pass it; a rerun after a
documented environmental or harness correction is a separate file.

### Quiet gate and telemetry

`--require-quiet` refuses to start a scored run when the one-minute load
average is 2 or more, and prints the load averages it saw. `--smoke`
ignores it. The load at start and the verdict are recorded under
`parameters.quiet_gate`. Every entry records wall timestamps
(`started_at`, `finished_at`), the load averages before and after, the pids
of the processes it spawned, their exit codes (collected after the clock
stopped) and the last 20 lines of each process's output (`stderr_tail`;
worker stdout and stderr share one log file). The `environment` block adds
the harness commit (`harness.git_sha`, `harness.git_dirty`), the Docker VM's
CPU and memory allocation and image id, a few PostgreSQL settings in force
(`shared_buffers`, `synchronous_commit`, `fsync`, `autovacuum`,
`max_connections`) and, under `resolved`, each arm's settings in force: the
Task and backend classes it bound to, its backend `OPTIONS`, its worker
command's argument defaults and, for django-ox, the values the worker
derives from them (`lock_timeout` 300 s, `reap_interval`, `renew_interval`,
`backoff_initial` 5 s, `backoff_max`, `max_attempts` 3).

### Warmup

Before each timed producer measurement (throughput, latency, bulk), the
producer performs 20 untimed enqueues, deletes those rows, and runs
`ANALYZE`. This is identical for both backends and exists to keep one-time
lazy imports (model loading, connection setup) out of the timed window for
*both* sides equally. Every measurement runs in a fresh Python process, so
neither backend ever benefits from a process that the other warmed up. The
drain cells warm nothing: worker startup is part of what they measure.

### Deviations from defaults (all of them)

- django-tasks-db workers run with `--no-startup-delay`, which disables a
  random sleep of up to 1 s at startup (a thundering-herd nicety the
  package exposes as a flag). Disabling it can only improve
  django-tasks-db's numbers.
- Both settings modules set `DEBUG = False` (also avoids `db_worker`'s
  auto-reload default, which is keyed to `DEBUG`).
- Console logging is capped at WARNING for both, on the handler as well as
  the logger, so per-task INFO lines are excluded for both. The handler
  cap matters: `db_worker` raises its loggers to INFO at its default
  verbosity, so a cap on the logger alone does not hold, and runs before
  2026-09-19 wrote two INFO lines per task in the django-tasks-db worker
  logs while the django-ox worker logs stayed empty.
- The retry row's side table and the drain cells' bulk preload are harness
  additions outside both packages; neither changes a package setting.
- Everything else is at package defaults: poll interval 1 s on both
  workers, ox's `MAX_ATTEMPTS`/`LOCK_TIMEOUT`/backoff defaults, tasks-db's
  default queue and worker id. `--processes` is a documented `ox_worker`
  option.

## Hardware and software

Recorded automatically into the raw results JSON at run time
(`collect_environment()` in `bench.py`). The machine used for the published
results:

- Apple M1 Max, 10 logical cores, 64 GiB RAM
- macOS, Python, Django, psycopg and PostgreSQL versions: the `environment`
  block of the published raw file, recorded at run time
- PostgreSQL 16 (official `postgres:16` image) in Docker Desktop,
  port-forwarded to localhost:54330, container `ox-bench`, default
  PostgreSQL configuration

## Reproducing

Prerequisites: Docker Desktop and a Python 3.12+ virtualenv with:

```
pip install -e ..            # django-ox, from the package root
pip install "django-tasks-db==0.13.0" "psycopg[binary]"
```

Then, from this `benchmarks/` directory:

```
python bench.py --require-quiet --probe-depth 100000
```

The script starts Docker Desktop if needed, creates the `ox-bench`
container and the two databases (`bench_ox`, `bench_tasksdb`), runs
migrations for both backends, creates the retry ledger table, then runs the
matrix in the recorded order. Progress prints as it goes; raw numbers
checkpoint continuously to `results-raw-<date>.json`, and per-process logs
land in `logs/`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--runs N` | 5 | Repetitions per cell and arm. |
| `--depth A,B` | `2000,20000` | Preloaded READY rows for the drain cells; each depth runs 1v1 and 4v4. |
| `--probe-depth N` | off | One 1v1 pair at depth N after the matrix, under `e2e_probe`. |
| `--legacy-threads` | off | Also run the pre-2026-09-19 threads-vs-processes cell, under `e2e_legacy_threads`. |
| `--preload bulk\|loop` | `bulk` | How the drain rows are preloaded (outside the clock). |
| `--require-quiet` | off | Refuse to start when the one-minute load average is 2 or more; ignored under `--smoke`. |
| `--seed N` | 0 | Which arm opens run 1; recorded. |
| `--only a,b` | all | Run a subset of `throughput`, `latency`, `e2e`, `diagnostic`, `bulk`, `retry`, `legacy`, `probe`. |
| `--gate m1,m2` | `enqueue_throughput,bulk_enqueue` | Metrics the stability gate may withhold. |
| `--stability-threshold X` | 1.15 | `max / min` above which a gated row is withheld. |
| `--smoke` | off | Tiny counts (30 enqueues, depths 20 and 60, 300 bulk rows), one file suffixed `-SMOKE`; never for published numbers. |
| `--resume` | off | Load today's raw file and skip the cells it already holds. |

Cleanup afterwards (the script leaves the container up so runs can be
repeated cheaply):

```
docker rm -f ox-bench
```

The published results page is `docs/benchmarks.md`. It is written by hand;
the two raw JSON files are the source of truth and ship alongside it.
`render_results.py --check ../docs/benchmarks.md` finds every number on the
page and looks it up among the values it computes from those files, prints
any number it cannot source and exits 1; `tests/test_benchmarks_page.py` runs
it. Without `--check` the script prints the full evidence tables and the facts
behind them, a superset of the page. A load flag marks kill trials 2 and 3
and the 100,000-task probe (`--flag`, which takes `kill_trial_<n>` and
`probe`). The flag is an observation about the machine that the raw files do
not carry, and it covers timings, not counts. The drain cells take no flag:
all four ran interleaved, run by run, in one window.

## Scope

Read this before quoting any number.

- **Single machine, single run day.** One Mac, one OS state, no controlled
  thermal or background-load environment. Numbers describe relative
  behaviour on this hardware; absolute performance elsewhere will differ.
- **Localhost database.** PostgreSQL runs in Docker on the same machine
  with sub-millisecond round trips. Real deployments have network latency
  between app and database, which would compress the relative differences
  in per-call metrics and change end-to-end numbers materially.
- **Docker Desktop on macOS.** The database runs in a Linux VM with
  virtualized I/O and port forwarding. This is not a production database
  host; both backends face the same handicap.
- **N.** 2,000 enqueues and 500 latency samples per producer run; 2,000 and
  20,000 rows per drain, with a single 100,000-row probe. Enough to
  separate the backends here, not to characterise tail behaviour (p99+) or
  sustained load.
- **No-op task bodies.** Real tasks do work; with realistic task bodies the
  per-task framework overhead measured here shrinks as a fraction of total
  runtime. This benchmark isolates framework overhead deliberately.
- **Matched processes.** Both arms run the same number of worker processes
  in every ranked drain cell. The django-ox supervisor at 4v4 is one extra
  interpreter inside its window, as described above.
- **End-to-end timer includes startup.** Worker process startup and Django
  initialisation are inside the timed window for both backends, once per
  worker process, plus the supervisor on the django-ox side at 4v4.
- **One framework, two backends.** Both arms ride Django 6.0 core
  `django.tasks` (django-tasks-db 0.13.0 uses it when present). Framework
  overhead is in both totals equally; the numbers separate the backends,
  their schemas and their workers, not the framework.
- **Single producer.** Enqueue metrics use one process on one connection.
  Concurrent-producer contention is not measured.
- **The retry row is a behaviour observation.** It reports what each
  backend did with 20 tasks that raise once; it ranks nothing and says
  nothing about retry timing beyond the timeline it recorded.

## Soak and chaos harness (soak.py)

`soak.py` is a separate harness that tests reliability rather than speed:
sustained mixed load from several producer and worker processes over tens
of minutes, repeated SIGKILL of workers mid-task with restarts, and a
forced crash-restart of a worker holding claimed tasks. Every task
execution writes phase rows with a per-execution nonce to a `soak_ledger`
side table, so at-least-once vs exactly-once behaviour is measured from
side effects and asserted from the database afterwards. It runs against
the same PostgreSQL 16 container the test suite uses (`ox-pg`, port 54329;
see CONTRIBUTING.md for the docker run command) with its own `soak_ox`
database and the `soaksite/` settings module. Methodology, parameters, and results: [SOAK-2026-09-11.md](SOAK-2026-09-11.md),
raw data in `soak-results-raw-<date>.json`.

## Worker death (killbench.py)

`killbench.py` counts what each backend leaves behind when its worker
process is killed and restarted. It measures no speed. It reuses `bench.py`
for the container, the databases, the migrations and the truncate.

What runs:

- Two arms, one worker process each: `ox_worker --concurrency 1` for
  django-ox and `db_worker --no-startup-delay --worker-id <name>` for
  django-tasks-db. Neither arm runs under a supervisor. The harness restarts
  the worker itself, the same way in both arms.
- The harness has its own databases, `kill_ox` and `kill_tasksdb`, in the
  same `ox-bench` container, so a `bench.py` run cannot truncate a table
  under a trial in progress. Settings: `benchsite/settings_kill_ox.py` and
  `benchsite/settings_kill_tasksdb.py`, both derived from the `bench.py`
  settings modules.
- Per trial: the task table is truncated (`CASCADE`) together with the
  `kill_ledger` side table, 2,000 tasks are enqueued as due, `ANALYZE` runs
  on the task table, the worker is spawned.
- The task body (`benchsite/tasks_kill.py`, one module for both arms)
  writes a "started" row carrying a fresh execution nonce, sleeps 100 ms,
  writes an "effect" row with the same nonce, and returns. Both rows go
  through the process's own Django connection in autocommit, so each one is
  committed on its own.
- Once the first task is SUCCESSFUL, the worker is SIGKILLed 20 times at
  gaps drawn from a seeded RNG (uniform, 3 to 10 s), only while READY rows
  remain. A replacement worker starts immediately after each kill with the
  same command. Right after each kill the harness reads the RUNNING rows
  and attributes them to the killed process from the task table itself
  (ox: `locked_by` carries the pid; tasks-db: the last `worker_ids` entry is
  the `--worker-id` the harness passed).
- The drain ends when no status has changed for 30 s and READY is 0. The
  harness then observes for 120 s more with the worker alive, then sends
  SIGTERM and records the exit code.
- Five trials per arm, arms interleaved per trial (ox, then tasks-db).

What is reported per trial and per arm, from the task table and the ledger
and never from worker output:

- rows per status (READY, RUNNING, SUCCESSFUL, FAILED, LOST and any other
  value present) and rows never terminal;
- distinct execution nonces; logical tasks with more than one "effect" row
  (repeated effects) and with more than one "started" row (repeated
  starts); tasks whose last execution has a "started" row and no "effect"
  row (lost partials);
- per kill: the window it landed in (mid body; after the effect row and
  before the outcome write; after the claim and before the body; between
  tasks; before the first start), the offset from the killed worker's last
  "started" row, the rows it stranded, and for each stranded row the time
  to its next "started" row, DB clock to DB clock, or none within the
  window;
- the exit code of every worker process, load averages before and after
  each trial, the kill schedule, the seed, versions, the harness commit,
  pids and stderr tails.

Medians are derived from the per-trial records by `summarize()`. The trial
records in `results-kill-<date>.json` are the source of truth.

Self-checks fail the run when the ledger and the task table disagree in a
way that means the harness is wrong: a nonce with more than one "started"
row or more "effect" rows than "started" rows, a ledger row from a process
the harness did not spawn, a SUCCESSFUL row with no "effect" row, a ledger
row from a killed process dated after its kill, a mid-body kill whose task
was not RUNNING right after the kill.

Disclosure, all of it:

- django-ox `LOCK_TIMEOUT` is 15 s (`benchsite/settings_kill_ox.py`); the
  default is 300 s. The derived reap interval is 7.5 s. A stranded row is
  expected back in READY within `LOCK_TIMEOUT` plus one reap interval plus
  one poll interval (23.5 s) of the kill, on the replacement worker. The
  idle and observation windows together exceed that bound.
- django-tasks-db runs with `--no-startup-delay` and an explicit
  `--worker-id` per worker generation. The default is a random string; the
  value is only appended to the row's `worker_ids`.
- No supervisor in either arm. Restart is by the harness, in both arms,
  immediately after each kill, so restart latency is the same procedure on
  both sides and is recorded (kill to the replacement's first "started"
  row).
- The kill schedule is not tied to the task cycle: the observer's wait
  between reads is jittered (40 to 120 ms, never the body's 100 ms), and a
  due kill fires at its planned instant, not on a read boundary. The offset
  of every kill from the last "started" row is in the results file.
- Time to the next start is reported for django-ox only. A row that
  django-tasks-db leaves RUNNING has no next start to measure, and the
  results file records that as none within the window.
- `--smoke` runs N=100, K=2, one trial per arm, and writes
  `results-kill-<date>-SMOKE.json`. Never for published numbers.
- Same host, container and scope caveats as the sections above.

## Files

- `bench.py`: orchestrator plus per-measurement subprocess roles. The orchestrator imports Django only to record the environment; each measurement is a fresh process.
- `killbench.py`, `benchsite/settings_kill_ox.py`, `benchsite/settings_kill_tasksdb.py`, `benchsite/tasks_kill.py`: the worker-death harness (see above); its results are `results-kill-<date>.json` and its process logs `logs/kill-*.log`.
- `benchsite/`: minimal settings and task modules for each backend, and
  `retry_ledger.py`, the retry row's side-table helper.
- `results-raw-<date>.json`: every number the harness produced, including
  all 500 individual latency samples per run. Since 2026-09-19 it also
  carries `parameters.schedule` (the order everything ran in), the
  `stability` block, `environment.harness`, `environment.docker`,
  `environment.postgres_settings` and `environment.resolved` (each arm's
  bound classes and settings in force), and on every entry the telemetry
  listed above. Drain entries carry `depth`, `processes`, `topology`,
  `worker_command` (the argv as run, interpreter shortened to its
  basename), `spawned_processes`, `worker_processes`,
  `supervisor_process`, `child_processes` (django-ox 4v4), `preload`,
  `analyze_at`, `poll_interval`, `pids`, `worker_exit_codes` (collected
  after the clock stopped; 0 is a clean stop on SIGTERM) and
  `stderr_tail`. Files from before 2026-09-19 carry `concurrency` in
  place of `processes`, and their concurrency-4 cell is the legacy cell.
- `render_results.py`: recomputes the results page's figures from
  `results-raw-<date>.json` and `results-kill-<date>.json`; `--check PAGE`
  verifies every number on the page against them.
- `docs/benchmarks.md` (in the repository root's docs): the results page.
- `soak.py` / `soaksite/`: soak and chaos harness (see above).
- `SOAK-<date>.md` / `soak-results-raw-<date>.json`: its results.
- `logs/`: stdout/stderr of every producer and worker process.
