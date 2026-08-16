# Benchmarks

django-ox 0.1.0 against django-tasks-db 0.12.0 (the other database
backend for the Tasks API) on identical no-op workloads, PostgreSQL 16.
Full methodology, raw JSON with every sample, and per-process logs are in
the `benchmarks/` directory of the repository. Every run is reported;
nothing was discarded.

This page leads with what the benchmark found wrong with django-ox,
because that is what made it worth running.

## The benchmark caught two real bugs

**Bug 1: the worker slept through completions.** In the first full run,
django-ox at concurrency 1 did not finish 2,000 no-op tasks inside the
900 second harness timeout, in any of three runs: 774 of 2,000 done, 0.86
tasks per second, against django-tasks-db's 70 per second. The mechanism:
after handing a task to its single executor slot, the main loop found the
slot busy and slept the full idle poll interval (1 second by default)
before checking again, capping throughput near one task per interval. The
fix has the loop wake as soon as any in-flight task completes. Those DNF
rows are preserved unchanged in the results file.

**Bug 2: eight SQL statements per task.** After the first fix, ox still
trailed at concurrency 1 (54 vs 71 tasks per second). Profiling showed 8
statements per task in the claim-and-execute path, each costing about 2.2
ms of round-trip overhead on this setup. The claim path was rewritten as a
single `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)
RETURNING` statement on PostgreSQL with the per-attempt bookkeeping folded
in, taking the path to 2 statements per task. The full test suite stayed
green on both databases through both fixes.

## Results after the fixes

Final 3-run matrix, runs interleaved between backends, all runs shown.

| Metric | django-ox (r1 / r2 / r3) | django-tasks-db (r1 / r2 / r3) |
| --- | --- | --- |
| Enqueue throughput (tasks/sec, higher better) | 657 / 528 / 557 | 447 / 450 / 454 |
| Enqueue latency in `transaction.atomic()`, p50 ms | 0.53 / 0.43 / 0.55 | 0.55 / 0.54 / 0.43 |
| End-to-end, 2,000 tasks, concurrency 1 (tasks/sec) | 90.1 / 88.1 / 90.1 | 69.2 / 69.9 / 69.6 |
| End-to-end, 2,000 tasks, concurrency 4 (tasks/sec) | 393 / 373 / 397 | 366 / 373 / 376 |

Reading: after the two fixes, ox wins enqueue throughput and
concurrency-1 end-to-end in all three runs, and edges or ties concurrency
4. In-transaction enqueue latency is a tie at roughly half a millisecond
for both; run-to-run drift on the machine is larger than the difference
between the backends.

A diagnostic cell at a non-default `--interval 0.1` produced the same
throughput as the defaults (87.8 / 86.6 / 88.5 tasks/sec), confirming the
poll interval no longer bounds anything.

## Caveats, all of them

- **No-op task bodies.** The tasks do nothing, so these numbers measure
  framework overhead only. Real tasks do work; with realistic bodies the
  differences here shrink as a fraction of total runtime. If your tasks
  average even 100 ms, both backends will feel identical.
- **Single machine, one day.** An Apple M1 Max running both the workers
  and the database; no controlled thermal or background-load environment.
  The numbers are indicative of relative behavior, not absolute claims.
- **Localhost database.** PostgreSQL 16 in Docker on the same machine,
  sub-millisecond round trips. Real deployments have network latency
  between app and database, which changes end-to-end numbers materially.
  Bug 2 above matters precisely because round trips dominate; on a
  higher-latency link, statement count matters even more.
- **The concurrency-4 comparison is imperfect by construction.**
  django-tasks-db's worker has no concurrency option, so its
  "concurrency 4" is four separate processes: four interpreters without a
  shared GIL, four Django boots inside the timed window. ox's is four
  threads in one process. This is the fairest available mapping, not a
  perfect one. (It also means ox wins that cell while sharing one
  interpreter.)
- **Small N.** 2,000 tasks and 500 latency samples separate the backends
  here but say nothing about p99+ tails or sustained load.
- One ox enqueue-throughput run in an earlier session measured 1,040
  tasks/sec. It was an outlier against its own siblings, we did not
  identify the cause, and it is not quoted here.

## What to take from this

The honest summary: django-ox's worker was measurably slow at launch
shape, the benchmark caught it, and after two targeted fixes it is faster
than the incumbent on every measured cell or tied. But throughput on no-op
tasks is not the product claim. The claim is the durability construction:
transactional enqueue, at-least-once execution with a reaper, bounded
retries with per-attempt tracebacks. The benchmark exists so that choosing
those semantics does not cost you worker performance, and to keep this
page honest.
