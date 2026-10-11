# Unique tasks

Unique tasks deduplicate enqueues while a matching task is pending. The key is held while the task is READY or RUNNING; this is not a guarantee that two executions of the same key cannot overlap.

See the [feature overview](index.md) for setup, requirements, and compatibility.

## Behaviour

A task marked unique is deduplicated while a matching task is pending:

```python
from django.tasks import task
from oxpull.unique import unique


@unique
@task
def send_invoice(order_id): ...


send_invoice.enqueue(42)  # enqueues
send_invoice.enqueue(42)  # returns the pending task's TaskResult; no new row
```

`enqueue_many()` raises `InvalidTask` for a unique task and names it.
django-ox writes that call as one bulk INSERT, which never reaches the
deduplicating path, so no uniqueness lock would be taken and the rows
would not be deduplicated. Refusing the call keeps the guarantee honest.
Enqueue unique tasks one call at a time; plain tasks keep the bulk path.

The lock is an enqueue-time gate. `django_ox.actions.retry` is an
operator instruction to run one named row, not an enqueue, so it does not
pass that gate. Retrying a unique task that a later `enqueue()` has
already superseded therefore runs that attempt beside the newer one, and
the key carries two READY rows until both settle. `oxpull` honours the
retry an operator asked for rather than discarding it.

Tasks can also be made unique from settings instead of the decorator:

```python
TASKS = {
    "default": {
        "BACKEND": "oxpull.backend.OxpullBackend",
        "QUEUES": ["default", ...],
        "OPTIONS": {
            "UNIQUE_TASKS": ["myapp.tasks.send_invoice"],
        },
    }
}
```

Semantics, all deliberate:

- **Window: pending-uniqueness.** The key is held while the matching task
  is READY or RUNNING. It releases as soon as
  the task leaves those two statuses (SUCCESSFUL, FAILED, LOST or
  DISCARDED), or when its row is pruned. The key follows the row's
  status, not the code: a row the reaper recorded LOST releases the key
  while the execution it belonged to may still be running, and django-ox
  lets that execution write SUCCESSFUL or FAILED onto its own row later.
  A RUNNING task whose worker has died still holds the key, so a duplicate
  enqueue returns that task's result and queues nothing. The reaper takes
  the task back after its lease expires. With attempts left, it puts the
  task back to READY, and the key stays held. On the task's last attempt,
  the reaper records it LOST instead. That frees the key, and the result the
  duplicate got back reads FAILED after `refresh()`.
  Uniqueness deduplicates pending rows. It does not stop two executions
  of one key overlapping, and a task that must never overlap itself
  needs its own guard.
- **A retry is not a duplicate.** A failed attempt with attempts remaining
  returns to READY, which is still inside the window; the key stays held
  through backoff, and at-least-once execution is unchanged.
- **Duplicates return the existing `TaskResult`** rather than raising, and
  `task_enqueued` is not sent for a suppressed duplicate. The result
  reflects the stored row (its priority, queue and run_after), not any
  `using()` overrides passed on the duplicate call.
- **Key derivation:** SHA-256 over `(backend_alias, task_path, queue_name,
  args, kwargs)` after `django.utils.json.normalize_json`, the same
  normalisation the task row stores. Uniqueness is scoped per backend
  alias. Dict order is irrelevant; tuples equal lists; `1` and `1.0`
  differ; int dict keys match their string form, exactly as JSON stores
  them. `queue_name` is in the key; `priority` and `run_after` are not.
  There is no signature binding: `add(1, 2)` and `add(1, b=2)` are
  different keys.
- **Enqueue cost:** a unique enqueue is one transaction containing three
  statements (lock INSERT under a savepoint, task INSERT, lock UPDATE)
  instead of one. Non-unique tasks take the parent code path untouched.

Signals: `task_enqueued` fires inside the enqueue transaction, before
commit, unchanged from django-ox. Receivers that trigger external
effects, including anything that wakes workers out of band, must defer
with `transaction.on_commit`: a pre-commit receiver cannot see the row
from another connection, and for unique tasks a suppressed duplicate
emits no signal at all.

Scheduling a unique task raises system check `oxpull.W001`: schedules
already dedup once per tick, and a unique scheduled task coalesces with
pending manual enqueues of the same key.

## Deployment guarantees

Uniqueness is enforced by the lock table's primary-key constraint. `OxpullBackend.enqueue()` writes the lock row and task row in one transaction and handles a competing insert's `IntegrityError`, rather than checking for the key first.

The constraint covers enqueues through `OxpullBackend`. An enqueue through `django_ox.backend.OxBackend` writes no lock row, so it can create a duplicate beside a task whose key is locked. Later unique enqueues return the task associated with the lock; they do not remove those extra rows.

Deploy django-ox 1.9.0 and configure `OxpullBackend` in every process that enqueues unique tasks, including web processes and cron jobs, before relying on this guarantee. `manage.py check` reads only the settings of the process running it; it cannot verify the rest of the fleet.

Before enabling uniqueness over existing traffic, drain or reconcile pending tasks enqueued without lock rows. A READY or RUNNING task without a lock row is not deduplicated against.

The dedup point is a unique constraint on `OxpullUniqueLock.key` plus an
`IntegrityError` loser path, the same mechanism used by `django_ox`
for schedule ticks. Check-then-insert is never used. The lock row and the
task row are written in one transaction, so enqueue keeps the parent
queue's durability guarantee (a `transaction.atomic()` opened on the
database these tables live on covers the enqueue too, and a rollback
discards task and lock along with every other row you wrote there).
These guarantees require the [database-routing constraints](#database-routing-constraints).

Lock release never depends on signals. The django-ox reaper settles an
abandoned task by writing LOST through a queryset update that emits no
signal, so any signal-driven release would strand keys exactly when a
worker dies. Instead, staleness is derived from the task row at the next
enqueue of the same key: when the holder has left READY and RUNNING, or
its row is gone (pruned), the lock is released and the enqueue proceeds.
A holder that is still READY or RUNNING is read without a lock: there
is nothing to release, and a row lock taken inside a caller's
transaction (`ATOMIC_REQUESTS`, any `atomic()` block, or a connection
with autocommit turned off) would live until that transaction commits
and hold off the holder's own lease renewal and outcome write for as
long as the request ran. On MySQL at REPEATABLE
READ that unlocked read, inside a caller's transaction, answers from the
transaction's snapshot, so a holder the snapshot calls live is read once
more on a connection of its own, which sees the committed status and
takes no lock; at READ COMMITTED, the level Django sets for MySQL unless
told otherwise, the plain read is current and no connection is opened.

At PostgreSQL's REPEATABLE READ or SERIALIZABLE, that read answers from
the snapshot. So the enqueue asks for a `FOR SHARE` lock on the holder's row
without waiting, inside a savepoint it rolls back at once. PostgreSQL
grants the lock when the row hasn't changed since the snapshot began, and
the enqueue returns the holder. If another transaction is writing the row
and hasn't committed, PostgreSQL refuses at once. The enqueue then returns
the last committed row, as a read at READ COMMITTED would. If the row has
changed, the enqueue reads the holder and the key's lock once more on a
new connection, in a READ COMMITTED transaction of its own, and closes it.
A task that's still pending and still holds the key is returned from that
read. The caller's transaction keeps its older snapshot. The level is read
in the same statement as the holder.

That read's connection waits at most 2 s to connect to each address the
host name resolves to. The read doesn't set that limit when the project
sets `connect_timeout` in `OPTIONS` or in `PGCONNECT_TIMEOUT`, or names a
connection service. Once connected, the read waits at most 100 ms for a
lock and runs for at most 2 s. It sets both with `SET LOCAL`, so a pooled
server session doesn't keep them. Nothing limits the host name lookup, or
a wait in a connection pooler's queue before the read reaches PostgreSQL.
The caller's transaction holds ACCESS SHARE locks on the task and lock
tables, from its own reads of them. A request such as a migration's
`ALTER TABLE` then waits for that transaction to end. The read queues
behind that request while the caller waits on the read, and PostgreSQL
can't see that cycle. Without those limits, the read would wait until that
request gave up. If the read stops, or its connection fails or times out,
the enqueue raises `UniqueEnqueueRace`.

Apart from that check, which keeps nothing, only the release reads the
holder with a locking read (`SELECT ... FOR
UPDATE` on PostgreSQL and MySQL), so that it acts on the committed
status rather than the enclosing transaction's snapshot and waits for an
operator retry still in flight; a retry that has just put the holder
back to READY keeps its lock instead of losing it to a stale read.
Inside a caller's transaction that lock, on a settled row, rides to the
caller's commit, and a retry of that row, a straggler's outcome write
onto a LOST row and `ox_prune` of the row wait behind it. The DELETE
itself carries the holder's status in its WHERE clause, which is the
whole of the guard on SQLite, where no row lock exists and a statement
is atomic against the single writer. `oxpull_prune` clears stale locks
for keys that are never enqueued again; it is hygiene, not correctness.

`task_id` on the lock is a bare UUID, not a ForeignKey, so `ox_prune`
deleting finished task rows cannot cascade through the lock or be blocked by a foreign-key constraint; a dangling id reads
as "finished". The `oxpull` migration creates only tables belonging to the
optional `oxpull` app and does not alter `django_ox` tables.

Portability note: MySQL has no partial unique indexes, so the lock-table
design is the one mechanism that is identical on SQLite, PostgreSQL and
MySQL, rather than a partial index over task status on the databases that
support one. The lock table keeps unique-task coordination within the
`oxpull` app's schema without adding an index to a `django_ox` table.

Known corner: inside a transaction at MySQL's REPEATABLE READ, or at
PostgreSQL's REPEATABLE READ or SERIALIZABLE, a unique enqueue can raise
`oxpull.exceptions.UniqueEnqueueRace` (a
`django.tasks.exceptions.TaskException`) where READ COMMITTED would
return a result. It never queues a duplicate instead, and it creates
nothing. On PostgreSQL that happens when the task holding the key
finished or was deleted after the transaction's snapshot began, or when a
settled holder was retried after that. It also happens there when the
read on a new connection stops or fails. On both databases it happens when
another enqueue took the key after the snapshot began, because the
transaction can't see the new lock. The message names the cause it
detects. Retry in a new, short transaction at READ COMMITTED, or outside a
transaction. READ COMMITTED on PostgreSQL or MySQL, and SQLite, don't hit
this.

Known limit: on MySQL at SERIALIZABLE, a duplicate enqueued inside a
transaction locks the holder's row until that transaction ends. Until
then the holder's lease renewal, its outcome write and the reaper wait.
They fail with error 1205 once `innodb_lock_wait_timeout` passes, 50
seconds by default. A worker won't claim a READY holder either. Keep
those transactions short, or enqueue unique tasks outside them.

A second corner, on every database: the enqueue joins the transaction the
caller opened on this database, so a slow request holds its key for as
long as it runs, and a duplicate enqueued meanwhile waits on it for as
long as the database allows. SQLite waits for the connection's
`OPTIONS["timeout"]` (5 s unless set), MySQL for
`innodb_lock_wait_timeout` (50 s unless set), PostgreSQL for as long as
the holder runs unless `lock_timeout` is set. Past that the duplicate
raises `UniqueEnqueueRace`, chained to the database's own error, rather
than a raw `OperationalError`. Nothing was written, and the message says
whether to retry the enqueue or, inside an open transaction, the whole
transaction. What that transaction is left as depends on the server.
MySQL's default rolls back the statement, so the enqueue rolls back to its
savepoint and the transaction is still usable; with
`innodb_rollback_on_timeout=ON` the server rolls the whole transaction
back, nothing done in it survives, and the message says so. SQLite in WAL
mode has a third shape: a transaction that read before it enqueued is
refused at once when another connection has committed since
(`SQLITE_BUSY_SNAPSHOT`), without waiting out the timeout; the message
names that too.

<a id="database-routing-constraints"></a>

Constraint: the `oxpull` and `django_ox` tables must live on the same database. The lock INSERT and its task INSERT share a transaction, the membership INSERT and its task INSERT share a transaction, and the completion query joins the two tables. None of these operations crosses a database alias. The rate window is neither co-written nor joined with task rows; it belongs on the same alias because `OxpullWorker` reads and writes it there, regardless of a router naming another database for it. At construction, the backend checks all seven `oxpull` models against the active router and refuses a split with `ImproperlyConfigured`. The batch API re-checks this placement on every call. Every read on these paths goes to the write database, so read-replica routing cannot serve a stale liveness answer, complete a batch early, or read capacity from a lagged window.

The check consults routers by model, without hints. `django_ox` saves each task row through the same router with the row as a hint, so a router that routes by model puts the task row on that database too. Hint-dependent routing that sends task rows elsewhere, such as one database per backend, is unsupported and may escape the startup check. With that routing, the lock or membership row would commit apart from its task, and an `add()` reissued after a deadlock would enqueue again on the other database.
