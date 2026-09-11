# Benchmarks

django-ox 1.1.0 against django-tasks-db 0.12.0 (the other database backend for
the Tasks API) on identical no-op workloads, PostgreSQL 16. Full methodology,
raw JSON with every sample, and per-process logs are in the `benchmarks/`
directory of the repository. Every run is reported.

## What was measured

The worker in 1.1.0, measured on 2026-09-11. Earlier raw files in the same
directory record 0.3.1; the tables here are 1.1.0.

Environment: Apple M1 Max, 10 logical CPUs, macOS 26.6.2, Python 3.12.13,
Django 6.0.8, PostgreSQL 16.14 in Docker on the same machine,
django-tasks-db 0.12.0, django-tasks 0.12.0, psycopg 3.3.5. Harness:
`benchmarks/bench.py --runs 5`. Raw data, every sample:
[`benchmarks/results-raw-2026-09-11.json`](https://github.com/oxpull/django-ox/blob/main/benchmarks/results-raw-2026-09-11.json).

## Results

Five runs per arm, interleaved within each run (django-ox first, then
django-tasks-db) so slow drift on the machine cannot systematically favour
whichever ran last. Zero
errors. Mean and one standard deviation over the five runs:

| Metric | django-ox | django-tasks-db |
| --- | --- | --- |
| Enqueue throughput (tasks/sec, higher better) | 1082 ± 261 | 873 ± 411 |
| Enqueue latency inside `transaction.atomic()`, commit excluded, p50 ms | 0.570 ± 0.009 | 0.562 ± 0.009 |
| Enqueue latency inside `transaction.atomic()`, commit excluded, p95 ms | 0.715 ± 0.055 | 0.698 ± 0.050 |
| End-to-end, 2,000 tasks, 1 worker (tasks/sec) | 124.5 ± 1.8 | 108.0 ± 2.3 |
| End-to-end, 2,000 tasks, concurrency 4 (tasks/sec) | 339 ± 4 | 357 ± 9 |

Every run behind those means:

| Metric | django-ox (r1 / r2 / r3 / r4 / r5) | django-tasks-db (r1 / r2 / r3 / r4 / r5) |
| --- | --- | --- |
| Enqueue throughput (tasks/sec) | 887 / 1222 / 808 / 1455 / 1040 | 1593 / 681 / 814 / 576 / 702 |
| Enqueue latency p50 (ms) | 0.56 / 0.56 / 0.57 / 0.58 / 0.58 | 0.56 / 0.58 / 0.56 / 0.55 / 0.56 |
| End-to-end, 1 worker (tasks/sec) | 121.3 / 125.9 / 124.8 / 125.3 / 125.1 | 104.1 / 109.3 / 110.1 / 107.9 / 108.7 |
| End-to-end, concurrency 4 (tasks/sec) | 342 / 342 / 338 / 334 / 341 | 340 / 360 / 363 / 358 / 362 |

Reading:

- **One worker: the two ranges do not overlap.** Every django-ox run
  finished the batch faster than every django-tasks-db run. The slowest
  django-ox run was 121.3 tasks/sec; the fastest django-tasks-db run was
  110.1.
- **Concurrency 4 compares two shapes.** django-tasks-db runs four processes
  and django-ox four threads in one process; the mapping and what it does to
  the numbers are explained below.
- **Enqueue throughput: django-ox ahead on the mean, no gap claimed.** About
  24 percent more enqueues per second on average, and both arms are
  noisy enough on this host that the ranges overlap. A mean separation that a
  single run can close is not a gap; the latency rows below are the steadier
  read of the same path.
- **Enqueue latency is a tie.** About six tenths of a millisecond at p50 for both, and the p95 is the same
  story.

A control cell at a non-default `--interval 0.1` produced the same
single-worker throughput as the defaults: 125.3 / 124.9 / 125.8 / 124.3 / 124.5
tasks/sec, a mean of 125.0 against 124.5 on the default interval. So the poll
interval does not bound throughput. With tasks in flight, the worker wakes on
task completion rather than on the polling clock.

## Where the numbers come from

Two properties of the worker's claim path drive the end-to-end results:

- **On PostgreSQL, claiming a task is a single statement**:
  `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`,
  with all per-attempt bookkeeping folded into the same UPDATE. Writing
  the outcome is a second single statement, and a test
  (`test_the_success_write_costs_one_statement`) fails if anything adds a
  read-back to it. A full claim-and-execute cycle costs 2 SQL statements
  per task. On a network link where every round trip costs real
  milliseconds, statement count matters more than it does on this
  localhost setup.
- **The worker wakes on task completion.** When executor slots are busy,
  the run loop waits on the in-flight futures rather than sleeping the
  poll interval, so `--interval` only governs how often an idle worker
  checks for new work.

The statement count is held by a test and the completion wake-up by the
diagnostic cell, so a regression in either shows up as a changed number
rather than a changed claim.

## How to read these numbers

- **No-op task bodies.** The tasks do nothing, so these numbers measure
  framework overhead only. Real tasks do work; with realistic bodies the
  differences here shrink as a fraction of total runtime. The single-worker gap here is about 1.2 ms per task; against a 100 ms task body it is about one percent.
- **Single machine, one day.** An Apple M1 Max running both the workers
  and the database; no controlled thermal or background-load environment.
  Interleaving the arms within each run is what protects the comparison
  from drift, not a clean room. The numbers are indicative of relative
  behaviour, not absolute claims.
- **Localhost database.** PostgreSQL 16 in Docker on the same machine,
  sub-millisecond round trips. Real deployments have network latency
  between app and database, which changes end-to-end numbers materially
  and increases the weight of per-task statement count.
- **Two different shapes at concurrency 4.** django-tasks-db's worker has no
  concurrency option, so its "concurrency 4" is four separate processes:
  four interpreters without a shared GIL, and four Django boots inside the
  timed window. django-ox's is four threads in one process and one boot.
  Separate interpreters help django-tasks-db on no-op bodies; the row is not a like-for-like comparison, and the mapping above is the closest the two workers allow.
- **Small N.** 2,000 tasks and 500 latency samples per run. Five runs
  separate the backends on the single-worker cell and say nothing about
  p99+ tails or sustained load. Sustained load and worker-failure
  behaviour are covered separately by the
  [soak and chaos run](#reliability-under-load) below.

## Reliability under load

A separate
soak and chaos harness ran django-ox 1.1.0 for 21.5 minutes of sustained
mixed load on PostgreSQL 16: 37,804 tasks across three scenarios,
including nine minutes in which a random worker was SIGKILLed every 20 to 45
seconds; 18 kills over the run, 27 interrupted executions. Forty assertions
ran and all forty passed. Every task reached a terminal state, every
interrupted execution was re-executed inside the reclaim bound (slowest reclaim 19.6 s against a bound of 37.5 s;
the harness runs `LOCK_TIMEOUT` at 15 s so that reclaims happen inside the
run, where the shipped default is 300 s), retry counts stayed bounded on every
row, and no row was marked LOST.

No task executed twice this run. Execution is at-least-once, and a worker
killed between finishing a task and recording the outcome leaves that task
to run again; whether a kill lands in that window is a matter of timing,
and the harness asserts the property that holds regardless: a second
execution is only ever attributable to a kill.

Latency under kill-chaos was within two milliseconds of the undisturbed
baseline at the median (p50 0.124 s against 0.126 s), and worker memory
stayed flat through the twelve-minute steady scenario.

The full report, including the harness design, every assertion, and the
caveats, is in
[`benchmarks/SOAK-2026-09-11.md`](https://github.com/oxpull/django-ox/blob/main/benchmarks/SOAK-2026-09-11.md),
written from the raw data beside it. The
[2026-09-01 run](https://github.com/oxpull/django-ox/blob/main/benchmarks/SOAK-2026-09-01.md)
on 0.3.1 used the same kill schedule.

## What to take from this

On the matrix above, django-ox finished the single-worker batch faster than
django-tasks-db in every run, tied it on in-transaction enqueue latency, and at
concurrency 4 the two worker shapes were within about five percent. Under sustained
load and repeated worker kills, django-ox 1.1.0 held its documented guarantees.

Throughput on no-op tasks is the floor, not the reason to choose django-ox. The
claim is the durability construction: transactional enqueue, at-least-once
execution with a reaper, bounded retries with per-attempt tracebacks. The
benchmark shows that the durability construction does not cost throughput: two statements per task, and 15 percent more throughput with one worker.
