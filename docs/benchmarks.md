# Benchmarks

django-ox against django-tasks-db 0.12.0 (the other database backend for
the Tasks API) on identical no-op workloads, PostgreSQL 16. Full
methodology, raw JSON with every sample, and per-process logs are in the
`benchmarks/` directory of the repository. Every run is reported; nothing
was discarded.

## What was measured

The worker path that ships as 1.0.0. It was measured on 2026-09-05, before
the release was tagged. The only source changes between the measurement and
the tag were the version string and the import re-routing through
`django_ox.compat`, neither of which touches the worker path.

Environment: Apple M1 Max, 10 logical CPUs, macOS 26.6.2, Python 3.12.13,
Django 6.0.8, PostgreSQL 16.14 in Docker on the same machine,
django-tasks-db 0.12.0, django-tasks 0.12.0, psycopg 3.3.4. Harness:
`benchmarks/bench.py --runs 5`. Raw data, every sample:
[`benchmarks/results-raw-2026-09-05.json`](https://github.com/oxpull/django-ox/blob/main/benchmarks/results-raw-2026-09-05.json).

One field in that raw file needs a note. `environment.packages` records
`django-ox: 0.3.1`. That is the editable install's distribution metadata,
which had not been refreshed since 0.3.1. The module the benchmark
imported was the working tree, not the 0.3.1 release.

## Results

Five runs per arm, interleaved within each run (django-ox first, then
django-tasks-db) so drift on the machine cannot favour one side. Zero
errors. Mean and one standard deviation over the five runs:

| Metric | django-ox | django-tasks-db |
| --- | --- | --- |
| Enqueue throughput (tasks/sec, higher better) | 1605 ± 110 | 1204 ± 160 |
| Enqueue latency in `transaction.atomic()`, p50 ms | 0.585 ± 0.044 | 0.623 ± 0.065 |
| Enqueue latency in `transaction.atomic()`, p95 ms | 0.753 ± 0.077 | 0.907 ± 0.159 |
| End-to-end, 2,000 tasks, 1 worker (tasks/sec) | 114.9 ± 1.7 | 103.6 ± 1.1 |
| End-to-end, 2,000 tasks, concurrency 4 (tasks/sec) | 328.5 ± 6.9 | 346.3 ± 12.8 |

Every run behind those means:

| Metric | django-ox (r1 / r2 / r3 / r4 / r5) | django-tasks-db (r1 / r2 / r3 / r4 / r5) |
| --- | --- | --- |
| Enqueue throughput (tasks/sec) | 1481 / 1674 / 1711 / 1666 / 1492 | 1134 / 1117 / 1118 / 1489 / 1163 |
| Enqueue latency p50 (ms) | 0.66 / 0.57 / 0.58 / 0.54 / 0.58 | 0.56 / 0.59 / 0.67 / 0.71 / 0.59 |
| End-to-end, 1 worker (tasks/sec) | 113.5 / 115.9 / 116.4 / 116.2 / 112.6 | 102.7 / 104.6 / 105.0 / 102.7 / 102.9 |
| End-to-end, concurrency 4 (tasks/sec) | 326 / 334 / 337 / 320 / 325 | 354 / 354 / 358 / 330 / 335 |

Reading:

- **One worker: the two ranges do not overlap.** Every django-ox run
  finished the batch faster than every django-tasks-db run. The slowest
  django-ox run was 112.6 tasks/sec; the fastest django-tasks-db run was
  105.0.
- **Concurrency 4 goes to django-tasks-db.** 346.3 tasks/sec against
  django-ox's 328.5, roughly 5% ahead. The two workers are shaped
  differently here, which is explained below, but the number is the
  number.
- **Enqueue throughput: no gap claimed.** django-ox is ahead on the mean,
  and both arms are noisy enough that the ranges touch. django-ox's
  slowest run was 1481 tasks/sec and django-tasks-db's fastest was 1489.
  A mean separation that a single run can close is not a gap.
- **Enqueue latency is a tie.** About six tenths of a millisecond at p50
  for both. Run-to-run drift on this machine is larger than the difference
  between the backends.

A diagnostic cell at a non-default `--interval 0.1` produced the same
single-worker throughput as the defaults: 117.4, 115.4, 115.4, 111.7 and
112.3 tasks/sec, a mean of 114.4 against 114.9 on the default interval. So
the poll interval does not bound throughput. With tasks in flight, the
worker wakes on task completion rather than on the polling clock.

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

The completion wake-up is shown by the diagnostic cell. The statement count is
read from the code path rather than isolated by a cell here: no run compares
this worker against itself with the read-back restored. A
regression in either shows up as a changed number rather than a changed
claim.

## How to read these numbers

- **No-op task bodies.** The tasks do nothing, so these numbers measure
  framework overhead only. Real tasks do work; with realistic bodies the
  differences here shrink as a fraction of total runtime. If your tasks
  average even 100 ms, both backends will feel identical.
- **Single machine, one day.** An Apple M1 Max running both the workers
  and the database; no controlled thermal or background-load environment.
  Interleaving the arms within each run is what protects the comparison
  from drift, not a clean room. The numbers are indicative of relative
  behaviour, not absolute claims.
- **Localhost database.** PostgreSQL 16 in Docker on the same machine,
  sub-millisecond round trips. Real deployments have network latency
  between app and database, which changes end-to-end numbers materially
  and increases the weight of per-task statement count.
- **Two different shapes at concurrency 4, and django-ox is behind.**
  django-tasks-db's worker has no concurrency option, so its "concurrency
  4" is four separate processes: four interpreters without a shared GIL,
  and four Django boots inside the timed window. django-ox's is four
  threads in one process and one boot. The two differences pull in
  opposite directions: separate interpreters help django-tasks-db, the
  extra boots cost it. On no-op bodies the interpreters win by more than
  the boots cost, and django-ox finishes about 5% behind. This is the
  fairest mapping the two workers allow, and the result stands as
  measured.
- **Small N.** 2,000 tasks and 500 latency samples per run. Five runs
  separate the backends on the single-worker cell and say nothing about
  p99+ tails or sustained load. Sustained load and worker-failure
  behaviour are covered separately by the
  [soak and chaos run](#reliability-under-load) below.

## Reliability under load

Speed is not the product claim; the durability construction is. A
separate soak and chaos harness ran django-ox 0.3.1 for 21.5 minutes of
sustained mixed load on PostgreSQL 16: 37,804 tasks across three
scenarios, including 9 minutes in which a random worker was SIGKILLed
every 20 to 45 seconds (18 kills total, 30 interrupted executions).
Thirty-seven assertions ran and all thirty-seven passed. Every task
reached a terminal state, every interrupted execution was re-executed
inside the documented bound (slowest reclaim 19.7 s against a bound of
37.5 s), and retry counts stayed bounded on every row.

One task executed twice, in the chaos scenario, after its worker was
killed between finishing the task and recording the outcome. That is what
at-least-once execution means, and it is the reason the harness asks
whether a second execution is attributable to a kill rather than whether
one happened: across 37,804 tasks, zero double executions could not be
traced to a kill, and no task ran twice without one.

Latency under kill-chaos was within a millisecond of the undisturbed
baseline at the median (p50 0.123 s against 0.122 s).

The full report, including the harness design, every assertion, and the
caveats, is in
[`benchmarks/SOAK-2026-09-01.md`](https://github.com/oxpull/django-ox/blob/main/benchmarks/SOAK-2026-09-01.md).
The [previous run](https://github.com/oxpull/django-ox/blob/main/benchmarks/SOAK-2026-08-16.md)
measured 0.1.0, which predates the lease fencing added in 0.2.0. Neither
soak covers the worker path measured in the tables above, which changed
after 0.3.1.

## What to take from this

On the matrix above, django-ox finished the single-worker batch faster
than django-tasks-db in every run, tied it on in-transaction enqueue
latency, and lost at concurrency 4. Under sustained load and repeated
worker kills, django-ox 0.3.1 held its documented guarantees.

Throughput on no-op tasks is not why you choose django-ox. The claim is
the durability construction: transactional enqueue, at-least-once
execution with a reaper, bounded retries with per-attempt tracebacks. The
benchmark exists to show what those semantics cost, and to publish the one
workload where they cost something.
