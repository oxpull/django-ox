# Batches

A batch groups tasks and fires a callback once every member's row is settled or gone. Completion describes the recorded task rows, not proof that every member has stopped executing.

See the [feature overview](index.md) for setup, requirements, and compatibility.

Batches derive completion from task rows rather than signals.
Batch state is derived by querying member task rows, never accumulated
from `task_finished`: a counter decremented by that signal hangs forever
whenever a worker dies mid-task, which is exactly what batches exist to
survive. The reconciler asks the rows instead, so it cannot miss a task
however it died. Completion is a compare-and-set on the batch status,
with the callbacks enqueued inside that same transaction. The rowcount
picks a single winner among racing reconcilers, so the callbacks are
enqueued once however many reconcilers race. They then run like any other
task, which is at least once: a callback that is retried runs again. A
crash mid-pass either leaves the batch sealed for the next tick or has
already committed the completion and its callbacks together.
These guarantees require the [database-routing constraints](unique-tasks.md#database-routing-constraints).

`OxpullBatchMember.task_id` is a bare UUID, not a ForeignKey, so task pruning
cannot cascade through membership or be blocked by a membership foreign-key constraint. The completion query uses the ORM rather than
SQL text, so `django_ox` table and column names come from model
metadata rather than hard-coded names.

## Behaviour

A batch groups tasks and fires a callback once every member's row is
settled (SUCCESSFUL, FAILED, LOST or DISCARDED) or gone; a row in any
other status, including one unrecognised by django-ox 1.9.0, holds it open:

```python
from oxpull import batches


batch = batches.create("nightly-invoices", on_complete=summarize_run)
batch.add(send_invoice, 42)
batch.add(send_invoice, 43)
batch.seal()

batch.progress()  # BatchProgress(total=2, pending=1, succeeded=1, failed=0)
batches.get(batch.id)
```

Completion is detected by a reconciler task, which runs once a minute through
django-ox's schedule dispatch (`OPTIONS["SCHEDULES"]`). Batches need this schedule; without it a batch never
completes:

```python
TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "OPTIONS": {
            "SCHEDULES": {
                "oxpull-batch-reconcile": {
                    "task": "oxpull.batches.reconcile",
                    "cron": "* * * * *",
                },
            },
        },
    }
}
```

The schedule may sit on any django-ox backend. Without one anywhere,
`manage.py check` reports `oxpull.W002`, and `create()` and `seal()`
refuse with `ImproperlyConfigured` naming the block above, so a batch
cannot be left sealed with nothing to complete it. The refusal reads
the settings of the process that calls, so the `SCHEDULES` block has to
be present in every process that calls `create()` or `seal()`, web
processes included, not only in the one that runs the scheduler. A
deployment whose web settings omit it, because only the worker's carry
the schedules, refuses every `create()` from the web;
declaring the same block there changes nothing about which process
runs the reconciler. `seal()` on a batch that is already SEALED or
COMPLETE raises `BatchSealed` whatever the settings say.

Semantics, all deliberate:

- **Sealing freezes membership.** `add()` enqueues the task and writes the
  membership row in one transaction, so the two commit or roll back
  together. `seal()` snapshots the member count; after that, `add()`
  raises `BatchSealed`. There is no un-seal and no reopen, and a batch
  that is never sealed never completes. On MySQL, an `add()` that loses
  an InnoDB deadlock reissues its own transaction, up to five times.
  Called inside your `atomic()` block it cannot: InnoDB has rolled your
  transaction back too, the error propagates, and it is your transaction
  to retry. A reissue enqueues again, so `task_enqueued` fires once per
  attempt, the first time for a row the rollback then discarded; a
  receiver deferred with `transaction.on_commit` sees only the row that
  committed, which is the rule for that signal in any case.
- **`on_complete` fires once every member's row is settled (SUCCESSFUL,
  FAILED, LOST or DISCARDED) or gone, whatever the outcome; a row in any
  other status holds the batch open. `on_success` fires in addition, and
  only when every member's outcome is known and none of them failed.**
  Finished is read off the row, not off the code; the bullet on what
  complete means says what that leaves open. There is no failed state
  for the batch itself: completion is completion, and `failed_count`
  plus the absent `on_success` is how a bad run reads.
- **Callbacks are ordinary tasks.** They are enqueued in the same
  transaction that completes the batch, then retry and fail like any
  other task. The batch stores their ids and does not supervise them.
  A callback must be a `@task` on a django-ox backend, and is stored as
  its dotted path: `create()` refuses a callback that carries `using()`
  overrides, because only the path is kept and the overrides would be
  dropped in silence. Set them on the decorator instead. A callback
  cannot be a unique task either: two batches finishing with the same
  callback and arguments would share one task, and one batch's callback
  would never run. A stored callback that became unique after
  `create()` is refused at completion instead, and its batch stays
  SEALED, logged at each tick, until the callback is made non-unique
  again; existing batches do not complete before then.
- **A retry is not a finish.** A member with attempts remaining goes back
  to READY, and a batch never completes past a member that will run
  again.
- **A member that outlives `LOCK_TIMEOUT` reads by its row, not by what
  it did.** When the worker running a member stops renewing its lease,
  the reaper takes the task back. With attempts remaining the row returns
  to READY and its lease epoch moves, which drops the old attempt's
  result. With no attempts left the row is recorded as LOST, which says
  the attempt went unobserved and nothing about its outcome, so the batch
  counts that member as unattributed rather than failed: `on_complete`
  fires, `on_success` is withheld, and `failed_count` is unchanged.
  Renewal keeps the lease only while it reaches the database on time.
  With Django's PostgreSQL pool, renewal needs a connection: the worker
  normally opens one outside the pool with a bounded deadline, with a
  short pooled fallback if needed. Sizing `LOCK_TIMEOUT` alone does not
  prevent a reclaim; a `task_reclaimed` warning in the worker log is
  the symptom.
- **Complete is a reading of the rows, not proof that every member has
  stopped running.** The reconciler completes a batch at the tick where
  it finds every member's row settled or gone. A member recorded LOST
  may still be executing: its worker stopped renewing the lease, the
  code kept going, and django-ox lets that execution write SUCCESSFUL
  or FAILED onto its own row afterwards. The worker's bounded
  recycle produces this on purpose when a member outlives `LOCK_TIMEOUT`
  on its last attempt. So `on_complete` is not a barrier against a
  member that is still running, and teardown in `on_complete` has to
  tolerate one.
  `on_success` fires only when every member's row is SUCCESSFUL, which
  is the one outcome a running member cannot still have. Two things
  follow. Member outcomes and the batch's totals are not final at
  COMPLETE: a straggler can turn a LOST member SUCCESSFUL after
  `failed_count` was written and the callback enqueued, and `progress()`
  shows the new count. And an operator retry that commits during the
  completing transaction can leave a READY member on a COMPLETE batch
  whose callback is already enqueued: the reconciler re-reads the counts
  under the batch row's lock, but `django_ox.actions.retry` writes the
  member's task row and takes no batch lock, so the two writes cannot be
  ordered.
- **A batch sealed with no members completes at the first tick**, and
  both callbacks fire.
- **The reconciler runs on the backend whose `SCHEDULES` name it.**
  django-ox rebinds a scheduled task to that backend at load. Its queue
  name comes from the default task backend's `QUEUES`: `"default"` when
  that list has it, otherwise the first entry. A queue the scheduling
  backend does not accept is refused at load with `ImproperlyConfigured`
  naming the schedule. Set `queue_name` in the schedule to put it
  somewhere specific.
- **Latency is one tick.** Cron granularity is one minute, so a callback
  lands up to about a minute after the last member finishes. Completion
  is decided by reading the member task rows, which is what makes it
  impossible to miss a member whose worker died without a word.
- **Pruned members read as finished, but not as successful.** By
  default `ox_prune` deletes SUCCESSFUL and DISCARDED rows that finished
  more than `--older-than` ago (7 days), and FAILED and LOST rows too
  under `--include-failed`. A membership row whose task is gone counts
  as finished, the sealed `total_count` keeps totals honest, and
  `BatchProgress.unattributed` reports how many are no longer
  attributable to success or failure. Those members are why `on_success`
  needs every outcome to be known: a pruned row could have been the one
  that failed, so the batch completes and `on_complete` fires while
  `on_success` is withheld. That includes the default prune. A batch
  that stays open longer than `--older-than`, because it is wide, its
  members are throttled, or the reconciler was down, loses its members'
  SUCCESSFUL rows and completes without `on_success`. Set `--older-than`
  above the longest batch you run. `BatchProgress.pruned` is deprecated
  and will be removed in 2.0. It returns the same number as
  `unattributed` and emits a `DeprecationWarning` when read.
- **A discarded member is finished, and the run is not clean.**
  `django_ox.actions.discard` closes a task an operator does not want
  run. That member is terminal, so it never holds the batch open, but it
  is cancelled work rather than work that was done. It counts as
  unattributed, not as failed, and `on_success` is withheld.
  `on_complete` still fires, so cleanup still runs.
- **Retrying a member reopens the wait, not the membership.**
  `django_ox.actions.retry` puts a FAILED or LOST member back on the
  queue. The batch stays SEALED until that attempt settles, and no
  callback fires in the meantime. Retrying a member of a batch that has
  already completed does not reopen it, and its callbacks have already
  fired.

## Batch API

Start with `oxpull.batches.create()`. It returns a `Batch` with an `id` and the methods `add()`, `seal()` and `progress()`. Use `oxpull.batches.get(batch.id)` to retrieve the batch.

`Batch.add()` raises `oxpull.exceptions.BatchMemberExists` if the task is already a member of that batch. Adding to a sealed batch raises `oxpull.exceptions.BatchSealed`.

`Batch.progress()` returns `BatchProgress(total, pending, succeeded, failed)`, read from the member task rows when called. Before sealing, `total` is the live member count. After sealing, it is the sealed snapshot.

The result also exposes:

- `unattributed`: `max(total - pending - succeeded - failed, 0)`. This covers members whose task rows `ox_prune` deleted, LOST members and DISCARDED members.
- `finished`: `total - pending`.
- `pruned`: a deprecated alias of `unattributed` that emits `DeprecationWarning`. Use `unattributed` in new code.

These batch entry points, result fields and exceptions are stable.

## How wide a batch can be

Measured to **1,000,000 members** in a single batch, on SQLite,
PostgreSQL 16 and MySQL 8, with every count checked against the task rows
rather than against what the API reports about itself. The completion
query is a subquery, not an expanded list of ids, so it binds one
parameter whatever the width, on every database and at every width
measured, and cannot run into SQLite's variable limit or PostgreSQL's
parameter ceiling.

At a million members the costs separate sharply by database:

| | SQLite | PostgreSQL 16 | MySQL 8 |
| --- | --- | --- | --- |
| seal, inside a caller's transaction | 34 ms | 0.7 s | **28 s** |
| reconcile | 4.9 s | 0.4 s | 7.3 s |
| finish all members | 4.8 s | 9.1 s | 27.6 s |

The seal row is the cost of a locking count: a row lock per member
wherever the database offers one, and a million InnoDB locks in one
transaction is where that shows (0.24 s at 100,000 members, 28 s at a
million). `seal()` now takes that count only when it is called inside a
caller's transaction, where the caller's snapshot could predate a member
committed since. Called outside a transaction it counts with a single
`COUNT(*)`: on a million-member batch, timing the two statements alone
with fresh statistics, the locking read took 12 s on MySQL 8 and 0.55 s
on PostgreSQL 16, the `COUNT(*)` 0.7 s and 0.03 s, and the whole `seal()`
call outside a transaction 0.74 s and 0.03 s. SQLite counts either
way, which is why it is the cheapest of the three at exactly the point
the other two are dearest. If you seal batches that wide on MySQL from
inside a transaction, budget for it, split them, or seal outside the
transaction.

Measured on an arm64 macOS host, with PostgreSQL 16 and MySQL 8 in
Docker on localhost and SQLite on a local file, one process at a time
and each width against an empty database. The individual-batch figures were measured on the earlier separate `oxpull` package: its 0.3.1 release on SQLite and PostgreSQL, and its unpublished 0.4.0 build on MySQL, with the same batch code. The fifty-batch sweep and the task-table figures below it are from the 2026-08-19 runs on pre-release code of that separate `oxpull` package labelled 0.1.0. These are historical measurements, not a new benchmark of the combined distribution.

Reconciler cost tracks the member count: from 100,000 members to a
million, the pass that completes a batch grows 7.5x on PostgreSQL, 14x on
SQLite and 13x on MySQL. The slowest reconciler pass measured at 100,000
members is the completing one: 0.56 s on MySQL, 0.34 s on SQLite and
55 ms on PostgreSQL, against a one-minute tick. Fifty sealed batches of
5,000 members each cost 667 ms per pass on PostgreSQL and 319 ms on
SQLite while their members are still pending, which is what every tick
costs until they finish.

One thing to know if your task table is large. On PostgreSQL the planner
hashes the batch's members and scans the task table, so a reconciler pass
costs roughly in proportion to that table rather than to the batch: about
2.3 ms against 5,000 task rows and about 38 ms against a million, per
sealed batch. Two levers, both yours: prune finished tasks with
`ox_prune`, which you want anyway, or lower `random_page_cost`. At 1.1
the planner picks a nested loop unaided and the same work took 6.4 ms
instead of 36 ms. SQLite probes the primary key per member, so the same
table growth costs it 3x rather than 16.6x.
