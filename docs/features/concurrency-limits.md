# Concurrency limits

A concurrency limit caps how many covered tasks are RUNNING at once. `OxpullWorker` applies it before each claim, from a count of RUNNING task rows. Its guarantee depends on the workers applying the limit; the bound and its assumptions are described below. Concurrency limits are provisional, as [API stability](../stability.md#provisional-concurrency-limits) lists.

See the [feature overview](index.md) for setup, requirements, and compatibility.

[Rate limiting](rate-limiting.md) caps how often a task starts. A concurrency
limit caps how many run at once. The two combine: a task blocked by either is
not claimed, and a workflow node must still pass the workflow gate.

## Behaviour

Give a limit a name, a `max`, and the tasks or queues it covers:

```python
TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", "exports", "video"],
        "OPTIONS": {
            "WORKER_CLASS": "oxpull.worker.OxpullWorker",
            "CONCURRENCY_LIMITS": {
                # Both tasks count against one ceiling of two.
                "warehouse": {
                    "max": 2,
                    "tasks": [
                        "myapp.exports.build_csv",
                        "myapp.exports.build_pdf",
                    ],
                },
                # A whole queue, whatever runs on it.
                "video": {"max": 4, "queues": ["video"]},
                # Paused: nothing it covers is claimed while max is 0.
                "backfill": {"max": 0, "tasks": ["myapp.backfill.rebuild"]},
            },
        },
    }
}
```

A limit covers a task when the task's path is in `tasks` or its queue is in
`queues`. At least one of the two is required. A queue limit counts every
task running on that queue. A task-path limit counts that task on any queue.

Before a claim, the worker counts the RUNNING task rows each limit covers. A
limit whose count is at or over its `max` has no room. Its tasks and queues
are left out of that claim's candidate select, on all three claim paths.
Uncovered tasks are still claimed in the same pass. The limit adds nothing
after the claim.

A limit is declared in settings. A worker imports no task modules, so a
limit written next to its task would be enforced only when something else
happened to import that module first. Declared here, it is visible to
every worker and to `manage.py check`.

`CONCURRENCY_LIMITS` takes effect only with `oxpull.backend.OxpullBackend`
and `OPTIONS["WORKER_CLASS"] = "oxpull.worker.OxpullWorker"`. Limits on an
alias whose worker is not `OxpullWorker` or a subclass, or whose
`WORKER_CLASS` is not a worker, are `oxpull.E011`, a hard system check
error. Every limit would be ignored silently. `max` is a whole number, zero
or more; `tasks` and `queues` are lists of strings; every queue must be one
the backend accepts. Anything else is `oxpull.E010`, and so is a `tasks`
entry that is not a task. The full rules are under
[`oxpull` options](../configuration.md#oxpull-options). `OxpullWorker`
refuses a malformed option when it is constructed, so one stops
`ox_worker --skip-checks` as well. Two faults only the system checks
catch: a `tasks` entry that is not a task (`oxpull.E010`), and limits on
an alias whose worker is not `OxpullWorker` (`oxpull.E011`). Under
`--skip-checks` the first starts a worker whose limit covers nothing
under that path, and the second starts a worker that applies no limit.

## Deployment guarantees

Concurrency limits are applied in `OxpullWorker` before the claim, from a count of RUNNING rows, and are not enforced by a database constraint. The base `Worker` claims covered tasks without applying the limit. So does an `OxpullWorker` from django-ox 1.9.x: it ignores `CONCURRENCY_LIMITS` and starts normally. A worker on a backend without the option is the same. The rows those workers claim add to the count, and no limit bounds them.

The settings can therefore deploy before the code. The bound holds once every claimer on the queues that carry covered tasks runs a version that applies it. `manage.py check` reads only its own process's settings and cannot verify which worker other processes run.

## The guarantee

> For a limit with ceiling `max`, with W claimers running `OxpullWorker`
> with the limit on the queues that carry covered tasks, the number of
> covered RUNNING rows is at most `max + W - 1`. One `ox_worker` process
> is one claimer; under `--processes N` each child is one. `--concurrency`
> adds no claimer: one thread claims per process. Each thread calling
> `run_once()` concurrently is one more. With a single claimer the limit
> is exact: at most `max`.

The W - 1 is structural. The count is read before the claim, because the
claim spends an attempt and a lease epoch and neither can be given back.
Two workers can each read `max - 1` at once, and each then claims.

W is a deployment fact. Size against the bound: for a ceiling of T,
configure **`max = T - (W - 1)`**. Adding claimers raises the bound of a
limit that is left alone. Under load the count goes over `max` routinely,
up to the bound; it is not a rare race.

That rule needs T to be at least W. With T = 1 and W = 2 it gives `max`
0, which is a pause. When T is smaller than W, or the ceiling must be
exact, put the covered tasks on a queue of their own and serve that queue
from one `ox_worker` process that no other worker claims from, as under
[routing a queue to its own worker](../configuration.md#routing-a-queue-to-its-own-worker).
The limit then has one claimer, so it is exact at `max`, whatever that
process's `--concurrency`.

The count is of RUNNING rows. A row and the body it stands for can part
ways:

- A dead worker's rows keep counting until the reaper takes them back:
  their lease expires (`LOCK_TIMEOUT`) and a reaper pass runs, up to one
  reap interval later. Until then those rows hold room under the limit.
- When the stuck-thread watchdog hands a row over, or the reaper requeues a
  row whose worker was only slow, the row leaves RUNNING while the old body
  may still be running. That body runs outside the cap until it ends.

Semantics, all deliberate:

- **A blocked task stays READY.** It is not claimed, spends no attempt and
  no lease epoch, and is indistinguishable from other queued work.
- **A blocked task does not hold up anything else.** The exclusion is
  applied inside the claim's candidate select, on all three claim paths. A
  worker with a full limit still claims uncovered tasks on the same pass.
- **A task may belong to several limits.** It is claimable only when every
  one of them has room.
- **Every RUNNING row counts.** The count covers every RUNNING row on the
  database the claim writes to, whichever backend enqueued it and whichever
  worker claimed it, on any queue. A worker that does not apply the limit
  adds to the count, and no limit bounds it.
- **`max: 0` pauses the covered tasks.** As with rate limiting's
  `limit: 0`, paused tasks hold unique keys open, hold batches open, and
  push up the ready-age metric for as long as it is set. A limit at 0
  needs no count.
- **A limit's name is a settings key.** It is never stored. Spelling is
  the identity on every database: two names that differ only by case are
  two limits, and there is no length cap. A rate-limit name is the primary
  key of its window row, which is why the rules there differ.
- **Two backends share a limit only when both declare it alike.** Each
  backend's workers count the RUNNING rows their own limits cover and stop
  at their own `max`. Two backends whose limits list the same `tasks` and
  `queues` under one `max` share one count and one ceiling. With the same
  coverage and different `max` values they allow up to the larger one.
  With coverage that overlaps but differs, a row only one limit covers
  counts for that one alone, so the rows one limit covers can reach the
  sum of both ceilings, equal or not. Both of those pairs are warning
  `oxpull.W008`, reported once for the installation, when both list a path
  under `tasks` or both list a queue under `queues`. Names play no part.
  Settings cannot see a `tasks` entry on one backend overlapping a
  `queues` entry on another, so that pair is not reported. Overlapping
  limits on one backend nest, for example 10 on a queue and 2 on one task
  in it, and are not reported.
- **Rate limits and concurrency limits combine.** A task blocked by either
  is not claimed. A workflow node must still pass the workflow gate.
- **Inside a caller's transaction the bound is not promised.** With
  `run_once()` inside `transaction.atomic`, `run_tasks()` under a
  `TestCase`, or `AUTOCOMMIT: False`, the count reads that transaction's
  view. On MySQL's default REPEATABLE READ that is a snapshot, which hides
  other processes' claims and completions since it was taken. The caller's
  own uncommitted claims are visible to its own count, and invisible to
  every other process until it commits.
- **A count that fails claims nothing in that pass.** `run()` logs
  `worker_poll_failed` and tries again on the next pass; `run_once()`
  raises.
- **Only blocked work left ends a batch run.** An `ox_worker --batch`
  worker stops when only blocked work is left, and `run_tasks()` returns,
  as with a rate-limited queue.

## When the count refreshes

The count runs before a claim and never on its own. Before a claim the
worker re-counts when its view is stale, or when `RATE_REFRESH_INTERVAL`
(0.25 s by default, the option rate limiting already uses) has passed
since its last count. The view goes stale on three things. This worker
claimed a covered task. It wrote the outcome of a covered task: success,
failure, a retry put back to READY, or the stuck-thread handover. Or its
reaper pass took rows back. So a slot this worker frees is reused on the
pass that the completion wakes.

A slot another worker frees is seen on this worker's next claim once a
refresh interval has passed since its last count. While this worker has
free slots and keeps finding work it claims again at once, so that is
within one refresh interval. Otherwise it claims again when one of its own
tasks settles, or after the poll interval (`ox_worker --interval`, 1.0 s
by default), so a worker with nothing to claim sees the slot within a poll
interval, or a refresh interval if that is longer. The refresh interval
bounds how stale a count can be when a claim runs. It does not start one.

A count that moves a limit across its `max` logs one DEBUG event on the
`oxpull` logger. `concurrency_limit_blocked` says the count reached `max`;
`concurrency_limit_unblocked` says it fell below. Both carry `limit`,
`running`, `max` and `worker_id`, and both are logged on the transition
only. A worker starts with nothing blocked, so its first count logs
`concurrency_limit_blocked` for every limit already at its `max`, `max: 0`
limits included. See [log events](../monitoring.md#log-events).

## Claim cost

Nothing when it is unused. With no `CONCURRENCY_LIMITS` configured, the
claim SQL is exactly what it was. With limits, the cost is one COUNT per
refresh: one SELECT counts every limit. A limit with `max: 0` needs no
count, and a configuration whose limits are all 0 runs no count.

Nothing is stored for a limit. The name is a settings key, and the count
reads task rows.

## Blocked work reads as backlog, because it is

A blocked task is READY, so `django_ox.stats.oldest_ready_age` and the
Prometheus ready-age gauge climb for as long as it waits. A queue-age alert
tuned before the limit existed will fire under a cap that stays full. Alert
on the age of queues that carry no covered tasks, or set the threshold with
the cap in mind.

The same holds for a blocked `@unique` task's key and for a batch with a
blocked member: both wait for the slot. Size batches against limits.

## Database aliases

The count runs on the database the claim writes to, never a read replica.
`ox_worker --database` moves the count with the claims, so the rows a
worker counts are the rows it claims against. See
[read replicas](../configuration.md#read-replicas) for what that flag does
and does not check.
