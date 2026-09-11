# Production

The worker is a plain foreground process: `manage.py ox_worker`, run
under whatever supervises your other processes. It rides out a database that goes away and comes back: a failed pass is
logged as `worker_poll_failed`, the connection is reopened, and the loop
carries on. A process manager is still what brings it back from a crash or a
recycle. Run it under `Restart=always` (as in the unit
below). This page covers systemd,
scaling, shutdown, the reaper, and monitoring.

## Running under systemd

```ini
# /etc/systemd/system/ox-worker.service
[Unit]
Description=django-ox worker
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=exec
User=app
Group=app
WorkingDirectory=/srv/myproject
Environment=DJANGO_SETTINGS_MODULE=myproject.settings
ExecStart=/srv/myproject/.venv/bin/python manage.py ox_worker --processes 2 --concurrency 4
Restart=always
RestartSec=5

# systemd sends SIGTERM on stop; the worker drains in-flight tasks and
# exits 0. Give the drain at least as long as your longest task before
# systemd escalates to SIGKILL. KillMode=mixed sends that SIGTERM to the
# supervisor alone, which forwards it once; the default of sending it to
# every process in the group would reach each worker twice, and a second
# signal is the force-exit.
KillSignal=SIGTERM
KillMode=mixed
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl enable --now ox-worker
journalctl -u ox-worker -f
```

There are two ways to put several worker processes on one host. The unit
above uses `--processes 2`: one unit, one supervisor, two workers, and one
place to set the flags. The other is a template unit (`ox-worker@.service`
with the same `[Service]` body and `--processes 1`), started as
`ox-worker@1`, `ox-worker@2`, and so on, which makes each worker its own
unit with its own journal entry and restart counter.

Use `--processes` unless you need to stop, restart or give flags to one
worker at a time. A supervisor that restarts a dead worker in a second, with
one command to edit, is the common case. The template unit is the right
shape when the workers differ, for instance one unit per queue with its own
`--lock-timeout`, and for that a queue flag per unit says more than a
process count.

## Running in containers

The worker is a foreground process that exits 0 on SIGTERM, so it needs no
special entrypoint. The setting that matters is the grace period: give the
runtime longer than your slowest task before it escalates to SIGKILL.

```yaml
services:
  worker:
    image: myapp:latest
    command: python manage.py ox_worker --processes 2 --concurrency 4
    stop_grace_period: 5m
    restart: unless-stopped
    depends_on:
      - db
    healthcheck:
      test: ["CMD", "python", "manage.py", "ox_health"]
      interval: 60s
      timeout: 15s
      start_period: 30s
```

Docker's default grace period is 10 seconds, which will kill a worker mid-task
and leave the reaper to clean up. `stop_grace_period` is the container
equivalent of `TimeoutStopSec`. On Kubernetes it is
`terminationGracePeriodSeconds` on the pod spec.

`--processes` inside one container, or one worker per container with the
replica count doing the scaling, both work. The runtime restarts a container,
the supervisor restarts a worker process, and each takes about a second. One
container per worker keeps the runtime's own health and restart accounting
per worker, which is worth having on an orchestrator; `--processes` keeps the
number of containers down on a single host.

With no flags, `ox_health` checks that the database answers, which is what a
per-container probe should test. Queue-wide checks belong in fleet alerting
rather than in a probe: see
[which check goes where](monitoring.md#health-checks-ox_health), and the
liveness probe example there for queues with steady traffic.

Run migrations before rolling workers, as an init container or a job, not from
the worker itself. Several workers starting at once would race the same
migration.

Roll every process before using a status the old version cannot read: migrate, finish the rollout, then use the new status.

## Graceful shutdown

On SIGTERM or SIGINT the worker:

1. Stops claiming new tasks immediately.
2. Waits for in-flight tasks to finish, however long they take.
3. Closes its database connections and exits with code 0.

A second signal during the drain forces an immediate exit, code 130. Whatever
was running is abandoned mid-flight. The reaper on a surviving worker reclaims
it later, and it counts as a failed attempt.

One other exit code exists. A worker exits 75 when it recycles itself after
a task thread that its timeout could not stop; see
[Task timeouts](#task-timeouts). A process manager on `Restart=always` or
`Restart=on-failure` restarts it either way.

This maps directly onto rolling deploys: send SIGTERM, wait, start the new
version. The only tuning point is the process manager's kill escalation
(`TimeoutStopSec` above) relative to your longest task.

With `--processes` above 1, the signal goes to the supervisor, and SIGHUP
counts as well as SIGTERM and SIGINT. The sequence is:

1. First signal: the supervisor forwards SIGTERM to every worker process
   and waits for each to drain. It exits 0 when all of them did, otherwise
   with the first non-zero code. A worker process cannot act on a signal
   until it has installed its handler, which is after Django is imported,
   so a stop that lands in that window kills it outright. It had claimed
   no work, so the supervisor logs `worker_process_stopped_early` and
   still exits 0. A restart, or a deploy that rolls twice, is not a
   failure to report to the process manager.
2. Second signal: forwarded again, which is the force-exit on each worker.
   A worker that cannot act on it (stopped, stuck in a C call) gets five
   seconds, then SIGKILL, logged as `supervisor_killed_workers` at ERROR.
3. A third signal sends the SIGKILL at once.

Send the signal to the supervisor only. A worker that also receives the
terminal's copy of a Ctrl-C has seen two signals. That is why each worker
runs in its own process group, and why the systemd unit above sets
`KillMode=mixed`.

A worker whose supervisor dies without signalling it (SIGKILL, an OOM kill)
does not run on as an orphan. On Linux the kernel sends it SIGTERM the
moment the supervisor exits (`PR_SET_PDEATHSIG`), so it drains through its
ordinary signal path. Everywhere else, and on Linux in the window before
that flag is set, the worker notices within one poll interval that its
parent pid has changed, logs `worker_orphaned` at WARNING, drains and
exits.

## Scaling out

Run as many workers as you need, on as many hosts as you need, pointed at
the same database. No coordinator, no leader election. Two things make
concurrent workers safe:

- **Claiming is atomic.** On PostgreSQL a claim is one
  `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`
  statement; on MySQL 8+ it is `SELECT ... FOR UPDATE SKIP LOCKED` inside a
  short transaction. Either way workers step past each other's rows rather
  than queueing behind them, so throughput scales with the workers you add.
  Databases without `SKIP LOCKED` claim through an optimistic
  compare-and-set, which is atomic everywhere and gives one worker at a time
  the head of the queue; [PostgreSQL, MySQL or SQLite](#postgresql-mysql-or-sqlite)
  says where to spend concurrency there.
- **Recurring schedules need no dedicated node.** Every worker dispatches;
  a unique constraint guarantees each tick fires once. See
  [Recurring tasks](recurring-tasks.md#many-workers-one-tick).

Workers can also be split by queue: run
`ox_worker --queues emails --concurrency 8` next to
`ox_worker --queues default` to isolate slow or bursty workloads.

### Threads and processes

`--concurrency N` is a thread pool inside one process. That fits the
common Django task profile: email, HTTP calls to third parties, ORM work.
For CPU-bound tasks the GIL makes threads the wrong tool; use
`--processes N` there, with `--concurrency 1`, and the same command on one
host gives you N interpreters.

```
python manage.py ox_worker --processes 4 --concurrency 1
```

Each process is a complete worker with its own database connections, lease
renewal and reaper, and its own worker id with the slot number on the end, so
`worker_ids` on a task row says which process ran it. Nothing is shared
across the processes except the database: the supervisor starts each one as a
fresh interpreter running `ox_worker --processes 1` with the same flags, so a
worker under the supervisor is the same code as a worker started by hand. The
supervisor itself never opens a database connection.

The children start the way the supervisor was started. `python
/srv/app/manage.py ox_worker --processes 2` from any working directory runs
`/srv/app/manage.py` again for each child; `django-admin` or `python -m
django` runs `python -m django` with `DJANGO_SETTINGS_MODULE` set in the
child's environment. `--settings` and `--pythonpath` are passed on, and the
children inherit the supervisor's working directory.

A worker process that exits, whatever the cause and whatever the code, is a
death: a crash, a kill, and a clean exit 0 all count the same, because a
worker is meant to run until told to stop. The supervisor restarts the slot
and logs `worker_process_restarted` with the exit code at WARNING. Its
in-flight tasks go through the ordinary lease path: the reaper on a
surviving process takes them back after `LOCK_TIMEOUT`. The restart policy
is per slot:

- The first restart comes after one second. Each further death within 60
  seconds of the slot's last start doubles the delay (1, 2, 4, 8, 16, 30
  seconds, capped at 30). A slot that has run for 60 seconds starts the
  sequence over at one second.
- More than five deaths of one slot inside one minute stops the supervisor:
  it logs `supervisor_restart_cap` at ERROR with the slot index, drains the
  other workers, and exits 1 whatever the children's own exit codes were, so
  a unit on `Restart=on-failure` restarts it too. A worker that cannot stay
  up hands the fault to the process manager and its restart policy rather
  than logging a restart a second forever.
- Because the count is per slot, every worker dying at once (a database
  restart, a deploy that changes a connection string) is one restart each,
  not a trip. Six workers that all die in the same second all come back a
  second later.

`--processes` is POSIX-only; on Windows run one `ox_worker` per process. It
does not replace a process manager: the supervisor is a foreground process
that expects to be restarted itself, like the single worker.

## The lease

A worker that claims a task takes a lease on it: the row records who holds
it, when the lock was last refreshed, and a lease number that goes up by one
every time the task changes hands. Three things follow from that number, and
they are worth understanding together because they are what makes recovery
safe.

**The worker keeps its own lease alive.** While a task is executing, its
worker refreshes the lock timestamp on the rows it is running, one statement
per interval however many are in flight, and it keeps doing so through a
graceful drain. So a task that takes an hour does not look abandoned after
five minutes. The reaper reclaims work from workers that stopped reporting,
not from tasks that are merely slow.

**A finish write only lands while the lease still holds.** When a worker
records success, failure or a retry, the UPDATE carries the lease number it
was given at claim time. If the task was taken off it in the meantime, that
number no longer matches and the write is dropped rather than applied, and no
completion is signalled for it. This is arithmetic rather than timing: no
pause is long enough to get around it, so a task that finished cannot be put
back on the queue by a straggler.

**Timestamps come from the database.** With `USE_TZ` on against PostgreSQL or
MySQL, the lock time is written by the database server and compared against the
database server's clock, so two hosts with drifting clocks do not produce false
reclaims. If you run workers on more than one host, that is the setting which
gives them one clock, and it is Django's default.

SQLite is the exception.
Django's `Now()` compiles there to `STRFTIME(..., 'NOW')`, which SQLite
evaluates inside the process that ran the statement. There is no server to
stamp it, so every worker uses its own clock whatever `USE_TZ` says. Run SQLite
on one host. It is the deployment SQLite is for, and a shared file over a
network filesystem does not give you working locking either.

With `USE_TZ` off the worker's clock is used on every database. Two workers
whose clocks differ by more than `LOCK_TIMEOUT` will then reclaim each other's
live leases, and the task runs twice. That is the property `USE_TZ` on buys you
on PostgreSQL and MySQL. A database's own clock also does not always match what
these columns hold under that setting: SQLite's is UTC while the columns carry
naive local time, and reading one against the other would make
`ox_prune --older-than` treat rows that finished seconds ago as hours old.
Under that setting, keep `TIME_ZONE` and the timezone your workers run in the
same, which is what Django assumes of it anyway.

### Task timeouts

`TASK_TIMEOUT` bounds how long one attempt may run. It is off by default.
With it set, the worker raises `django_ox.exceptions.TaskTimeout` inside the
task when the deadline passes:

- **A sync task** gets the exception on its own thread, at the next line of
  Python it executes. `finally` blocks run, an open `transaction.atomic()`
  rolls back, and the thread returns to the pool. The worker then drops the
  thread's database connections, since the exception may have landed inside
  the driver with a statement in flight, and records the outcome on a fresh
  one. The task may catch `TaskTimeout` to clean up and then re-raise it.
  Inside the task the exception is bare: `str(exc)` is empty and
  `exc.timeout` is `None`, and the worker fills both in when it records the
  attempt. A task that catches it and returns is allowed, and the attempt
  is recorded as whatever the task went on to do, provided it returns or
  raises within `TASK_TIMEOUT_GRACE`; one still running then is treated as
  a thread that did not stop, below. An exception raised while the task
  unwinds from `TaskTimeout` (a cleanup that fails, say) is recorded as the
  timeout, with that exception in the traceback. `raise ... from None`
  breaks that chain, and the attempt is then recorded as the exception it
  names, with no `task_timed_out` event.
- **An async task** is cancelled inside its event loop at the deadline. The
  coroutine sees `asyncio.CancelledError` at the `await` it was on, as any
  cancelled coroutine does, and should let it propagate; the worker records
  the attempt with `TaskTimeout`. `except TaskTimeout` inside an async task
  never fires, because nothing can raise another class at a running
  coroutine's `await`.

The attempt is recorded as failed with a `TaskTimeout` error that names the
timeout. The attempt was consumed at claim time, so the retry rule is the
ordinary one: back to READY on the backoff while attempts remain, FAILED
when they are spent. The worker logs `task_timed_out` at WARNING, then the
usual `task_retrying` or `task_failed`. `TaskTimeout` subclasses
`TimeoutError`, so code written for one treats it as one.

A long loop can check the clock instead of being interrupted between two
steps. `django_ox.deadline()` returns the attempt's deadline as a `datetime`,
and `django_ox.remaining()` the seconds left; both return `None` when no
timeout applies.

```python
import django_ox
from django.tasks import task  # Django 6.0+; on 5.2: from django_tasks import task


@task
def export(report_id):
    for chunk in chunks_of(report_id):
        left = django_ox.remaining()
        if left is not None and left < 5:
            return {"paused_at": chunk.offset}
        write(chunk)
    return {"done": True}
```

**A thread that does not stop.** The exception is delivered when the thread
next executes Python, so under a pool of CPU-bound threads it can lag the
deadline by a few multiples of the interpreter's 5 ms switch interval. A
thread blocked in a C call stays blocked until the call returns: a socket
read with no timeout, `time.sleep()`, a lock, a long statement waiting on the
database. The exception lands when the call returns, and if that is within
the grace the attempt is an ordinary timeout. `TASK_TIMEOUT_GRACE` (default
30 seconds) is how long the worker waits for the thread after the deadline.
A task that caught `TaskTimeout` and is still running then looks the same
from outside, and is treated the same way. If the thread is still running at
the grace, the worker:

1. Records the attempt as failed, with a `TaskTimeout` whose message says
   the thread did not stop within the grace, and moves the lease number in
   the same write, the
   way the reaper does when it takes a row off a worker that went quiet.
   The outcome the thread eventually reports is refused by that number.
2. Logs `task_stuck` at ERROR and `worker_recycling` at WARNING.
3. Stops claiming, drains its other in-flight tasks, and exits with code 75
   (`EX_TEMPFAIL`). The stuck thread dies with the process.

Under `--processes` (the unit above) the supervisor restarts the slot after
one second, logs `worker_process_recycled`, and does not count the exit
against the restart cap; systemd sees nothing. A single-process worker
under systemd comes back on `Restart=always`, and on `Restart=on-failure`,
since 75 is non-zero. A container runtime on `restart: unless-stopped` does
the same.

Between the stuck record and the process exit the task may run twice: its
retry is claimable the moment the record lands, and the stuck thread keeps
executing until the worker's other in-flight tasks have drained. The thread
cannot write its outcome to the row, but its side effects are real. That is
the at-least-once contract every task already lives under: write tasks to be
safe to run twice.

Put timeouts on sockets and HTTP clients where you can. A task that returns
to Python regularly is one the soft timeout stops cleanly; the recycle is the
backstop.

Set per-queue values where one number does not fit:

```python
"QUEUES": ["default", "exports", "webhooks"],
"OPTIONS": {
    "TASK_TIMEOUT": 60,
    "TASK_TIMEOUTS": {"exports": 3600, "webhooks": 10},
    "TASK_TIMEOUT_GRACE": 30,
},
```

A queue in `TASK_TIMEOUTS` uses its own value; `None` there exempts the
queue from the global limit. Every queue named there must be in `QUEUES`
(`django_ox.E005` otherwise), unless `QUEUES` is `[]`. A timeout longer than
`LOCK_TIMEOUT` is fine: the lease is renewed for as long as the task runs.

Timeouts use CPython's own facility for raising an exception in another
thread, which every supported Python has. On an interpreter without it, the
worker logs `timeouts_backstop_only` once at startup and enforces timeouts
by the grace backstop alone.

**Under a coverage tool or a debugger.** A tool that watches a thread runs a
callback between one bytecode and the next, and those callbacks hold locks
of their own. An exception raised into such a thread can land inside one,
leaving a lock held with nobody to release it. So the worker does not raise
into a watched thread. It asks on the task's own thread as it registers the
attempt, and two things count:

- a trace function on that thread (`sys.settrace`), which is what
  `coverage run` installs before Python 3.14, and what `pdb` and most
  debuggers install;
- a registered `sys.monitoring` tool with events enabled, which is what
  `coverage run` uses from Python 3.14 on.

A profile hook (`sys.setprofile`) is not consulted, so a sampling profiler
that installs one leaves timeouts alone.

For a **sync task** on a watched thread, `TaskTimeout` is not raised and the
grace backstop is the whole enforcement:

- A task that returns before `TASK_TIMEOUT` plus `TASK_TIMEOUT_GRACE` is
  recorded as whatever it did, however long it ran. There is no
  `task_timed_out` event, and nothing else says a deadline passed. With the
  default 30-second grace this is the common case.
- A task still running then is recorded as failed with `TaskTimeout`, and
  the worker recycles: `task_stuck` at ERROR, `worker_recycling` at WARNING,
  exit code 75. Every timeout that reaches the backstop costs a worker
  restart.

An **async task** is not affected. It is cancelled inside its event loop at
the deadline, watched or not.

The worker logs `timeouts_backstop_only` once, on the first attempt it
registers under the tool rather than at startup, with `reason=tracing_tool`
and `tracer` naming the mechanism. That line is the check: if it is in the
log, this is what is happening; if it is not, timeouts are being raised
inside tasks as usual.

A tool that starts watching a thread after that thread's attempt was
registered is not seen until the next attempt, since a thread's trace hook
cannot be read from outside it.

A test suite that measures coverage over its own task bodies sees this
shape. To run an attempt unwatched, take the calling thread's trace hook off
for the length of the call and put it back afterwards; a `sys.monitoring`
tool cannot be taken off a single thread.

## The reaper

Workers still die: OOM kills, node failures, `kill -9`. A dead worker stops
refreshing its lock, and the reaper picks the task up. It runs inside every
worker, on an interval derived from the lock timeout.

A RUNNING task whose lock has not been refreshed for `LOCK_TIMEOUT` (default
300 seconds, per-worker override `--lock-timeout`) is taken back, and what
happens next depends on whether the task has attempts left:

- **Attempts remaining.** The task goes back to READY and the lease number
  goes up, so the old worker cannot write to it again. This is the ordinary
  case, and at-least-once execution already covers it: at-least-once execution
  means the task may run twice, which is why task bodies must be idempotent.
- **No attempts remaining.** The row is marked LOST. LOST means what it says:
  the worker holding this task stopped reporting and nobody observed how the
  attempt ended. The reaper does not record a failure, because it did not see
  one. It has watched a lock go quiet, and that is all it writes down.

The attempt was already consumed when the task was claimed, so a
crash-looping task cannot retry forever; it stops after `MAX_ATTEMPTS` like
any other task.

The reclaim is a compare-and-set on the lease number, so a reaper running
late cannot stomp a task that finished or was already reclaimed.

### What LOST looks like from the outside

`django.tasks` has four result statuses and django-ox does not add a fifth to
them. A LOST task reads as `FAILED` through `get_result()`, and
`result.errors` ends with a `TaskAbandoned` record whose text says the lease
was lost and the outcome was never observed. `is_finished` is true, so code
that waits for a result terminates instead of waiting for a worker that is
not coming back.

The row keeps the distinction the four statuses cannot carry. Its status
column is LOST rather than FAILED, `queue_stats()` reports it in its own
`lost` column, and `ox_prune --include-failed` treats it like a failed row
for retention. Like a failed row it can be retried or discarded, from the
admin or with `django_ox.actions`; see
[Retrying and discarding](monitoring.md#retrying-and-discarding).

One case is worth knowing about before it surprises you. If the worker
holding a LOST task was starved rather than dead, and it comes back and
records a success, the row becomes SUCCESSFUL and a caller reading it twice
sees `FAILED` and then `SUCCESSFUL`. Only that one execution can do this, and
only while the row is still LOST. It is the cost of giving a
four-valued API an answer for a task whose outcome nobody saw, and the
alternative, reporting it as still running forever, hangs every caller that
waits on it.

It takes two things at once: the task's attempts spent, and its lease allowed
to lapse. Renewal holds the lease for as long as the worker is answering, so
what reaches this state is a worker that went unresponsive for longer than
`LOCK_TIMEOUT` and then came back, which is the case you asked the reaper to
act on in the first place. If your callers cannot tolerate seeing it, raise
`LOCK_TIMEOUT` until a merely slow worker is never reclaimed; the cost is that
a genuinely dead one takes that much longer to notice.

### Attempts count claims

`attempts` on a task row, the `attempt` key on every log record, and
`MAX_ATTEMPTS` all count **claims**, not invocations. The number goes up in the
same statement that hands the task to a worker, before the function is reached.

That is deliberate and it is what makes the bound hold. A worker that is killed
mid-run reports nothing, so a count that only moved on a reported failure would
never move for it, and a task that reliably kills its worker would be retried
without end, every reaper pass another start.

The cost of the choice is the case at the other end. A task that loses its
worker between the claim and the call has used an attempt without running, and
a task that does so as many times as `MAX_ATTEMPTS` allows reaches a terminal
state having never executed. The window is small: a worker claims only when it
has a free thread and hands the task straight to it. It is not zero.

Two consequences worth knowing:

- **Read `attempts` as "times this was handed out".** There is no column that
  counts successful runs: `errors` holds one entry per *failed* attempt, so a
  task that failed twice and then succeeded has three attempts and two
  entries.
- **A task that must not be retried on infrastructure loss** should be
  idempotent, the same as it must be under any at-least-once queue. The lease
  number stops a reaped worker writing its outcome over a later holder's; it
  does not stop the work that worker already did.

### Tuning LOCK_TIMEOUT

Set `LOCK_TIMEOUT` above the longest gap you expect between a worker's lease
renewals, not above your longest task. Renewal runs every `LOCK_TIMEOUT / 3`
seconds, so two consecutive renewals can be missed before anything is
reclaimed. The value is really a statement about how long a worker may be
unresponsive before you want its work handed to somebody else: too low and a
paused or overloaded worker loses tasks it was going to finish, too high and
recovery after a real crash is slow.

Watch for `task_lease_lost` in the logs. It records an attempt whose result
was discarded because the lease had already been reclaimed, and a steady
trickle of it means the timeout is short relative to how long your workers
go unresponsive.

**The value travels with the lease, not with the reaper.** A worker writes
`lease_expires_at` on the row when it claims, from its own `LOCK_TIMEOUT`, and
refreshes it on every renewal. Every reaper judges that column rather than
deriving a deadline from whatever it happens to be configured with, so you can
change `LOCK_TIMEOUT` in a rolling deploy: each row keeps the lease it was
granted and picks up the new value on its next claim.

The trade is that a lease outlives the configuration that granted it. If you
grant a very long one by mistake, changing the setting does not shorten the
leases already out; `django_ox.actions.expire_lease(result_id)` expires one so
the next reaper pass takes it. It does not stop the task, and the lease number
still refuses that task's finish write once somebody else holds the row, so it
is the ordinary reclaim brought forward rather than a cancellation. Use
`discard()` to close a task.


**Tasks must be idempotent.** Execution is at-least-once by design: a task
is retried both when it raises and when its worker dies mid-run. Write
task bodies so that running twice is harmless (upserts, idempotency keys,
"already sent?" checks).

### What the lease guarantees, precisely

One worker holds the lease on a task at a time, and a task runs at least once.
It is worth being exact about which of those the lease number enforces, because
the two are not the same guarantee.

**Two workers cannot write the same row.** Every claim increments
`lease_epoch`, and every write that ends an attempt carries the value the
worker was given in its `WHERE` clause. A worker whose lease was reclaimed
matches zero rows instead of overwriting whoever holds it now. That is
arithmetic, not timing: no pause is long enough to defeat it, which is why a
reclaimed worker cannot corrupt the record of a task it no longer owns.

**Two threads can run the same task body at the same time.** The lease fences
the row, not the function. There are two ways to get there:

- A task outlives `TASK_TIMEOUT`. The worker asks the thread to stop, and after
  `TASK_TIMEOUT_GRACE` it publishes the retry and recycles. The old thread is
  still running while the retry is claimed elsewhere, because nothing in
  CPython can stop a thread that is inside a call which never returns.
- A worker is partitioned from the database for longer than `LOCK_TIMEOUT`. It
  is still executing; the reaper cannot tell it apart from a dead one and gives
  the task to somebody else.

So the rule is the same one every at-least-once queue asks for, and it is worth
saying that it *is* every at-least-once queue rather than a property of this
one. Sidekiq's [reliability notes](https://github.com/sidekiq/sidekiq/wiki/Reliability)
say a job in flight is lost when a process is killed under the default fetch.
Oban's [rescue plugin](https://github.com/oban-bg/oban/blob/main/lib/oban/lifeline.ex)
documents that it "may transition jobs that are genuinely executing and cause
duplicate execution". Que's [README](https://github.com/que-rb/que/blob/master/docs/README.md)
describes a killed worker's job as unlocked and retried with its error count
untouched. Celery with [`acks_late`](https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-acks-late)
leaves redelivery to the broker and counts nothing. All read 2026-09-11.

Where a task must not overlap with itself at any cost, the options are the same
as anywhere else: make the body idempotent, take an application-level lock the
task checks on entry, or give the queue a timeout long enough that the backstop
is not reached in normal operation. What this package adds is that the *record*
of the task cannot be corrupted while you do it.

## PostgreSQL, MySQL or SQLite

All three run the full worker suite in CI. Guidance:

- **PostgreSQL** is the production recommendation. It gets the
  single-statement `SKIP LOCKED` claim path, and it handles many workers
  and high write concurrency the way you would expect.
- **MySQL 8** claims with `SELECT ... FOR UPDATE SKIP LOCKED` in a short
  transaction and runs the full suite in CI on the oldest and newest Python
  and Django corners.
- **SQLite** is the right choice wherever SQLite is already the right choice
  for your Django database: development, tests, and small single-host
  deployments. Run one worker and give it threads (`ox_worker
  --concurrency 8`) rather than several worker processes. A thread pool in one
  process is how a single-writer database wants to be driven, and it
  is enough for the IO-bound work a single-host deployment usually queues.
  Claiming stays correct with more processes than that: a worker that loses
  the head of the queue retries against the next few candidates rather than
  stepping past locked rows, so throughput stops scaling with processes well
  before it would on PostgreSQL.

The queue lives in your default database, inside your existing backup and
migration story. That is the point: one system of record, one thing to
operate.

## Monitoring

Monitoring has a [dedicated page](monitoring.md). The operational
summary:

- **The table is the queue.** `django_ox.stats` exposes queue depth,
  backlog age, throughput and failure rate as plain functions. Backlog
  depth and backlog age are the two numbers worth alerting on.
- **`manage.py ox_health`** turns thresholds on those numbers into an
  exit code, for cron alerting and container probes.
- **The Prometheus endpoint.** Mounting `django_ox.urls` serves the same
  numbers as gauges at `GET /ox/metrics`.
- **Logs.** The worker logs to the `django_ox` logger: lifecycle at
  INFO, retries and reaper reclaims at WARNING, terminal failures and
  unhandled worker errors at ERROR, each with stable extra keys for JSON
  log handlers. Under systemd this lands in the journal.
- **Per-task forensics.** Each row keeps its attempts count, the id of
  every worker that ran it, timestamps for enqueue/start/finish, and the
  full traceback of every failed attempt.

## Pruning on a timer

Finished rows accumulate; prune them on a schedule sized to how long you
need results and tracebacks to stay queryable. With systemd:

```ini
# /etc/systemd/system/ox-prune.service
[Unit]
Description=Prune finished django-ox tasks

[Service]
Type=oneshot
User=app
WorkingDirectory=/srv/myproject
Environment=DJANGO_SETTINGS_MODULE=myproject.settings
ExecStart=/srv/myproject/.venv/bin/python manage.py ox_prune --older-than 7d
```

```ini
# /etc/systemd/system/ox-prune.timer
[Unit]
Description=Daily django-ox prune

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Or from cron, or as a recurring task pointed at a small wrapper task of
your own. Flag reference on the
[Configuration](configuration.md#ox_prune) page. FAILED rows are kept by
default so tracebacks survive until you have looked at them; add
`--include-failed` once that is not needed.
