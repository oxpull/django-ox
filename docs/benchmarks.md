# Benchmarks

django-ox 0.1.0 against django-tasks-db 0.12.0 (the other database
backend for the Tasks API) on identical no-op workloads, PostgreSQL 16.
Full methodology, raw JSON with every sample, and per-process logs are in
the `benchmarks/` directory of the repository. Every run is reported;
nothing was discarded.

## Results

Final 3-run matrix, runs interleaved between backends, all runs shown.

| Metric | django-ox (r1 / r2 / r3) | django-tasks-db (r1 / r2 / r3) |
| --- | --- | --- |
| Enqueue throughput (tasks/sec, higher better) | 657 / 528 / 557 | 447 / 450 / 454 |
| Enqueue latency in `transaction.atomic()`, p50 ms | 0.53 / 0.43 / 0.55 | 0.55 / 0.54 / 0.43 |
| End-to-end, 2,000 tasks, concurrency 1 (tasks/sec) | 90.1 / 88.1 / 90.1 | 69.2 / 69.9 / 69.6 |
| End-to-end, 2,000 tasks, concurrency 4 (tasks/sec) | 393 / 373 / 397 | 366 / 373 / 376 |

Reading: ox wins enqueue throughput and concurrency-1 end-to-end in all
three runs, and edges or ties concurrency 4. In-transaction enqueue
latency is a tie at roughly half a millisecond for both; run-to-run drift
on the machine is larger than the difference between the backends.

A diagnostic cell at a non-default `--interval 0.1` produced the same throughput
as the defaults: 87.8, 86.6 and 88.5 tasks/sec. So the poll interval does not
bound throughput. With tasks in flight, the worker wakes on task completion
rather than on the polling clock.

## Where the numbers come from

Two properties of the worker's claim path drive the end-to-end results:

- **On PostgreSQL, claiming a task is a single statement**:
  `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`,
  with all per-attempt bookkeeping folded into the same UPDATE. A full
  claim-and-execute cycle costs 2 SQL statements per task. On a network
  link where every round trip costs real milliseconds, statement count
  matters even more than it does on this localhost setup.
- **The worker wakes on task completion.** When executor slots are busy,
  the run loop waits on the in-flight futures rather than sleeping the
  poll interval, so `--interval` only governs how often an idle worker
  checks for new work.

The harness gates releases. Both properties above are proven by published cells:
the single-statement claim by the concurrency-1 rows, the completion wake-up by
the diagnostic cell. A regression in either shows up as a changed number rather
than a changed claim.

## How to read these numbers

- **No-op task bodies.** The tasks do nothing, so these numbers measure
  framework overhead only. Real tasks do work; with realistic bodies the
  differences here shrink as a fraction of total runtime. If your tasks
  average even 100 ms, both backends will feel identical.
- **Single machine, one day.** An Apple M1 Max running both the workers
  and the database; no controlled thermal or background-load environment.
  The numbers are indicative of relative behavior, not absolute claims.
- **Localhost database.** PostgreSQL 16 in Docker on the same machine,
  sub-millisecond round trips. Real deployments have network latency
  between app and database, which changes end-to-end numbers materially
  and increases the weight of per-task statement count.
- **The concurrency-4 comparison is imperfect by construction.**
  django-tasks-db's worker has no concurrency option, so its
  "concurrency 4" is four separate processes: four interpreters without a
  shared GIL, four Django boots inside the timed window. ox's is four
  threads in one process. This is the fairest available mapping, though
  still an approximation. (It also means ox wins that cell while sharing
  one interpreter.)
- **Small N.** 2,000 tasks and 500 latency samples separate the backends
  here but say nothing about p99+ tails or sustained load. Sustained load
  and worker-failure behavior are covered separately by the
  [soak and chaos run](#reliability-under-load) below.

## Reliability under load

Speed is not the product claim; the durability construction is. A
separate soak and chaos harness ran django-ox 0.1.0 for 21.5 minutes of
sustained mixed load on PostgreSQL 16: 37,802 tasks across three
scenarios, including 9 minutes in which a random worker was SIGKILLed
every 20 to 45 seconds (16 kills total, 39 interrupted claims). Every
task reached a terminal state, every interrupted claim was reclaimed and
re-executed inside the documented bound, retry counts stayed bounded on
every row, and zero tasks were lost or double-completed. Latency under
kill-chaos was within noise of the undisturbed baseline (p50 0.217 s vs
0.211 s).

The full report, including the harness design, every assertion, and the
caveats, is in
[`benchmarks/SOAK-2026-08-16.md`](https://github.com/oxpull/django-ox/blob/main/benchmarks/SOAK-2026-08-16.md).

## What to take from this

The shipped worker beats the reference implementation on every measured
cell or ties it, and holds its documented guarantees under sustained load
and repeated worker kills. But throughput on no-op tasks is not why you
choose django-ox. The claim is the durability construction: transactional
enqueue, at-least-once execution with a reaper, bounded retries with
per-attempt tracebacks. The benchmark exists to show that choosing those
semantics costs you nothing on worker performance.
